use anyhow::Result;
use assemblyline_models::{Readable, datastore};
use assemblyline_models::types::Sid;
use assemblyline_search::tables::init_database_tables;
use assemblyline_search::yugabyte::{Yugabyte, YugabyteConnectionManager, YugabytePool, bb8};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crate::Core;
use crate::elastic::{Elastic, create_empty_result_from_key};




pub async fn main(core: Core, leader: bool) -> Result<()> {
    let core = Arc::new(core);
    let manager = YugabyteConnectionManager::new(core.config.database.url.clone(), core.classification_parser.clone());
    let pool = bb8::Builder::new()
        .connection_timeout(Duration::from_mins(5))
        .build(manager).await?;
    // let mut db = Yugabyte::connect(&core.config.database.url, core.classification_parser.clone()).await?;
    {
        let db = pool.get().await?;
        init_database_tables(&db, false).await?;

        // println!("{}", db.count_submissions().await?);
        // return Ok(());
    }

    #[derive(Debug, Deserialize)]
    struct Resp {
        sid: Sid,
    }

    impl Readable for Resp { fn set_from_archive(&mut self, _from_archive: bool) {} }

    let mut task_pool = JoinSet::new();
    {
        let (queue, worker_queue) = flume::bounded(1000);
        for _ in 0..20 {
            task_pool.spawn(inserter(core.clone(), pool.clone(), worker_queue.clone()));
        }

        let mut cursor = core.datastore.submission.stream_search::<Resp>("*", "sid".to_owned(), vec!["state: completed".to_owned()], None, None, None).await?;
        let mut counter = 0;

        while let Some(row) = cursor.next().await? {
            if queue.send_async(row.sid).await.is_err() {
                break
            };
            counter += 1;
            if counter > 31_0 {
                break
            }
        }
    } // drop queue

    while let Some(val) = task_pool.join_next().await {
        val??;
    }

    Ok(())
    // // setup worker to stream data in a particular time window
    // todo!();

    // // start workers to process that stream of data
    // todo!();
}

async fn inserter(core: Arc<Core>, pool: YugabytePool, queue: flume::Receiver<Sid>) -> Result<()> {
    while let Ok(sid) = queue.recv_async().await {
        println!("examining: {}", sid);
        println!("{:?}", insert_submission(pool.clone(), &core, sid).await?);
    }
    Ok(())
}


#[derive(Debug, Default)]
pub struct InsertMetrics {
    db: assemblyline_search::yugabyte::InsertMetrics,
    fetching: std::time::Duration
}

#[derive(Debug)]
enum InsertResult {
    Inserted(InsertMetrics),
    AlreadyDone,
    NotFound,
}

async fn insert_submission(db: YugabytePool, core: &Core, sid: Sid) -> Result<InsertResult> {
    let ds = &core.datastore;

    // check if this sid is already present
    {
        let con = db.get().await?;
        if con.submission_exists(sid).await? {
            return Ok(InsertResult::AlreadyDone)
        }
    }
    let data_fetching_start = std::time::Instant::now();

    // Load submission
    let submission = match ds.submission.get(&sid.to_string(), None).await? {
        Some(sub) => sub,
        None => return Ok(InsertResult::NotFound),
    };

    // load errors
    let error_keys: Vec<&str> = submission.errors.iter().map(|r|r.as_str()).collect();
    // for key in &error_keys {
    //     println!("{}", ds.error.get(key, None).await?.is_some());
    // }
    let errors = ds.error.multiget::<datastore::Error>(&error_keys, Some(true), None).await?;
    // todo!();

    // load results
    let empty_result_keys: Vec<&str> = submission.results.iter().filter(|r|r.ends_with(".e")).map(|r|r.deref()).collect();
    let result_keys: Vec<&str> = submission.results.iter().filter(|r|!r.ends_with(".e")).map(|r|r.deref()).collect();
    let mut results = ds.result.multiget::<datastore::Result>(&result_keys, Some(true), None).await?;
    for empty_key in empty_result_keys {
        results.insert(empty_key.to_string(), create_empty_result_from_key(empty_key, submission.params.ttl as i64, &core.classification_parser)?);
    }

    // get list of related files
    let mut files = vec![submission.files[0].sha256.clone()];
    for result in results.values() {
        files.push(result.sha256.clone());
        for extract in &result.response.extracted {
            files.push(extract.sha256.clone());
        }
        for extract in &result.response.supplementary {
            files.push(extract.sha256.clone());
        }
    }
    files.sort_unstable();
    files.dedup();

    // load file info
    let ids: Vec<&str> = files.iter().map(|r| -> &str {r}).collect();
    let fileinfo = ds.file.multiget::<datastore::File>(&ids, Some(true), None).await?;
    let data_fetching_time = data_fetching_start.elapsed();

    // insert data
    let mut con = db.get().await?;
    let db_metrics = con.insert_submission(&submission, &results, &errors, &fileinfo).await?;
    Ok(InsertResult::Inserted(InsertMetrics { db: db_metrics, fetching: data_fetching_time }))
}