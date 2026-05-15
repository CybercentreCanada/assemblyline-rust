use anyhow::Result;
use assemblyline_models::datastore;
use assemblyline_models::types::Sid;
use assemblyline_search::yugabyte::Yugabyte;
use std::ops::Deref;
use std::sync::Arc;

use crate::Core;
use crate::elastic::Elastic;



pub async fn main(core: Core, leader: bool) -> Result<()> {
    // setup worker to stream data in a particular time window
    todo!();

    // start workers to process that stream of data
    todo!();
}


async fn insert_submission(db: &mut Yugabyte, ds: &Elastic, sid: Sid) -> Result<bool> {
    // check if this sid is already present
    if db.submission_exists(sid).await? {
        return Ok(true)
    }

    // Load submission
    let submission = match ds.submission.get(&sid.to_string(), None).await? {
        Some(sub) => sub,
        None => return Ok(false),
    };

    // load errors
    let error_keys: Vec<&str> = submission.errors.iter().map(|r|r.as_str()).collect();
    let errors = ds.error.multiget::<datastore::Error>(&error_keys, Some(true), None).await?;

    // load results
    let result_keys: Vec<&str> = submission.results.iter().map(|r|r.deref()).collect();
    let results = ds.result.multiget::<datastore::Result>(&result_keys, Some(true), None).await?;

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
    let ids: Vec<&str> = submission.errors.iter().map(|r|r.as_str()).collect();
    let fileinfo = ds.file.multiget::<datastore::File>(&ids, Some(true), None).await?;

    // insert data
    db.insert_submission(submission, results, errors, fileinfo).await?;
    Ok(true)
}