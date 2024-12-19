
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::user::User;
use assemblyline_models::datastore::Submission;
use assemblyline_models::messages::submission::{File, SubmissionParams};
use assemblyline_models::messages::submission::Submission as MessageSubmission;
use assemblyline_models::{ClassificationString, JsonMap, Sha256, Sid, UpperString};
use itertools::Itertools;
use rand::{thread_rng, Rng};
use serde_json::json;
use tokio::sync::mpsc;

use crate::constants::METRICS_CHANNEL;
use crate::ingester::IngestTask;
use crate::Core;

use super::Ingester;

/// A helper to fill in some fields that are largely invariant across tests.
struct MakeMessage {
    ce: Arc<ClassificationParser>,
    message: Option<JsonMap>,
    files: Option<JsonMap>,
    metadata: Option<JsonMap>,
    params: Option<JsonMap>,
}

impl MakeMessage {
    fn new(ce: Arc<ClassificationParser>) -> Self {
        Self {
            message: None,
            files: None,
            params: None,
            metadata: None,
            ce
        }
    }
    
    fn metadata(mut self, input: serde_json::Value) -> Self {
        let serde_json::Value::Object(input) = input else { panic!() };
        self.metadata = Some(input); self
    }
    
    fn params(mut self, input: serde_json::Value) -> Self {
        let serde_json::Value::Object(input) = input else { panic!() };
        self.params = Some(input); self
    }

    fn files(mut self, input: serde_json::Value) -> Self {
        let serde_json::Value::Object(input) = input else { panic!() };
        self.files = Some(input); self
    }

    fn message(mut self, input: serde_json::Value) -> Self {
        let serde_json::Value::Object(input) = input else { panic!() };
        self.message = Some(input); self
    }

    fn build(self) -> Vec<u8> {

        let mut params = SubmissionParams::new(ClassificationString::new(self.ce.unrestricted().to_string(), &self.ce).unwrap());
        params.description = Some("file abc".into());
        params.submitter = "user".to_string();
        params.groups = vec!["users".parse().unwrap()];

        let message = MessageSubmission {
            sid: thread_rng().r#gen(),
            files: vec![File {
                sha256: uniform_string('0', 64).parse().unwrap(),
                size: Some(100),
                name: "abc".to_string()
            }],
            metadata: Default::default(),
            time: chrono::Utc::now(),
            notification: Default::default(),
            params,
            scan_key: None,
        };

        let data = serde_json::to_value(&message).unwrap();
        let serde_json::Value::Object(mut data) = data else { panic!() };
        if let Some(mut message) = self.message {
            data.append(&mut message);
        }

        if let Some(mut input) = self.files {
            let file = data.get_mut("files").unwrap().as_array_mut().unwrap()[0].as_object_mut().unwrap();
            file.append(&mut input);
        }

        if let Some(mut input) = self.metadata {
            let metadata = data.get_mut("metadata").unwrap().as_object_mut().unwrap();
            metadata.append(&mut input);
        }

        if let Some(mut input) = self.params {
            let params = data.get_mut("params").unwrap().as_object_mut().unwrap();
            params.append(&mut input);
        }

        // println!("{}", serde_json::to_string(&data).unwrap());

        serde_json::to_vec(&data).unwrap()
    }
}

/// wait for metrics messages with the given fields
pub (crate) async fn assert_metrics(metrics: &mut mpsc::Receiver<Option<redis_objects::Msg>>, values: &[(&str, isize)]) {
    let start = std::time::Instant::now();
    let mut values: HashMap<&str, isize> = HashMap::from_iter(values.iter().cloned());
    while !values.is_empty() {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            panic!("Metrics failed {:?}", values);
        }

        let message = match tokio::time::timeout(std::time::Duration::from_secs(1), metrics.recv()).await {
            Ok(value) => value.unwrap().unwrap(),
            Err(_) => continue
        };
        let message: String = message.get_payload().unwrap();
        let message: JsonMap = serde_json::from_str(&message).unwrap();
        println!("{message:?}");

        for (key, value) in values.iter_mut() {
            if let Some(number) = message.get(*key) {
                if let Some(number) = number.as_number() {
                    if let Some(number) = number.as_i64() {
                        *value -= number as isize;
                    }
                }
            }
        }

        values.retain(|_, v| *v != 0);
    }
}

/// build a string that is the same character repeated a bunch of times
fn uniform_string(value: char, length: usize) -> String {
    let mut out = String::new();
    for _ in 0..length {
        out.push(value);
    }
    out
}

#[tokio::test]
async fn test_ingest_simple() {
    // setup the test environment
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // setup the test data
    let mut user: User = Default::default();
    let custom_user_groups = ["users", "the_user"];
    user.groups.extend(custom_user_groups.iter().map(|i|i.parse().unwrap()));
    core.datastore.user.save("user", &user, None, None).await.unwrap();

    // Send a message with a garbled sha, this should be dropped
    let in_queue = ingester.ingest_queue.raw();
    in_queue.push(&MakeMessage::new(core.classification_parser.clone()).files(json!({
        "sha256": uniform_string('0', 10)
    })).build()).await.unwrap();

    // Process garbled message
    ingester.ingest_once().await.unwrap();
    assert_metrics(&mut metrics, &[("error", 1)]).await;

    // Send a message that is fine, but has an illegal metadata field
    in_queue.push(&MakeMessage::new(core.classification_parser.clone())
        .metadata(json!({
            "tobig": uniform_string('a', core.config.submission.max_metadata_length as usize + 2),
            "small": "100"
        }))
        .params(json!({
            "submitter": "user", 
            "groups": custom_user_groups
        }))
        .build()
    ).await.unwrap();

    // Process those ok message
    ingester.ingest_once().await.unwrap();
    
    // The only task that makes it through though fit these parameters
    let task = ingester.unique_queue.blocking_pop(std::time::Duration::from_secs(2), false).await.unwrap().unwrap();
    assert_eq!(task.submission.files[0].sha256.to_string(), uniform_string('0', 64)); // Only the valid sha passed through
    assert!(!task.submission.metadata.contains_key("tobig")); // The bad metadata was stripped
    assert_eq!(task.submission.metadata.get("small"), Some(&json!("100"))); // The valid metadata is unchanged
    assert_eq!(task.submission.params.submitter, "user");
    assert_eq!(task.submission.params.groups, custom_user_groups.iter().map(|v| UpperString::from(*v)).collect_vec());

    // None of the other tasks should reach the end
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.ingest_queue.length().await.unwrap(), 0);
}

#[tokio::test]
async fn test_ingest_stale_score_exists() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());
 
    // Add a stale file score for this file
    let sha256: Sha256 = thread_rng().r#gen();
    let filescore_cache = assemblyline_models::datastore::filescore::FileScore { 
        psid: Some("0".parse().unwrap()), 
        expiry_ts: chrono::Utc::now(), 
        score: 10, 
        errors: 0, 
        sid: "0".parse().unwrap(), 
        time: 0.0 
    };
    let classification = ClassificationString::new(core.classification_parser.unrestricted().to_string(), &core.classification_parser).unwrap();
    let key = SubmissionParams::new(classification).create_filescore_key(&sha256, None);
    core.datastore.filescore.save(&key, &filescore_cache, None, None).await.unwrap();

    // Process a message that hits the stale score
    ingester.ingest_queue.raw().push(&MakeMessage::new(core.classification_parser.clone()).files(json!({"sha256": sha256})).build()).await.unwrap();
    ingester.ingest_once().await.unwrap();
    
    // The stale filescore was retrieved but expired
    assert_metrics(&mut metrics, &[("cache_hit", 1), ("cache_expired", 1), ("submissions_ingested", 1)]).await;

    // but message was ingested as a cache miss
    let task = ingester.unique_queue.blocking_pop(std::time::Duration::from_secs(2), false).await.unwrap().unwrap();
    assert_eq!(task.submission.files[0].sha256, sha256);

    // None of the other tasks should reach the end
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.ingest_queue.length().await.unwrap(), 0);
}

#[tokio::test]
async fn test_ingest_score_exists() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());
 
    // Add a valid file score for all files
    let sha256: Sha256 = thread_rng().r#gen();
    let filescore_cache = assemblyline_models::datastore::filescore::FileScore { 
        psid: Some("0".parse().unwrap()), 
        expiry_ts: chrono::Utc::now(), 
        score: 10, 
        errors: 0, 
        sid: "0".parse().unwrap(), 
        time: chrono::Utc::now().timestamp() as f64,
    };
    let classification = ClassificationString::new(core.classification_parser.unrestricted().to_string(), &core.classification_parser).unwrap();
    let key = SubmissionParams::new(classification).create_filescore_key(&sha256, None);
    core.datastore.filescore.save(&key, &filescore_cache, None, None).await.unwrap();

    // Ingest a file
    ingester.ingest_queue.raw().push(&MakeMessage::new(core.classification_parser.clone()).files(json!({"sha256": sha256})).build()).await.unwrap();
    ingester.ingest_once().await.unwrap();

    // wait for metrics
    assert_metrics(&mut metrics, &[("cache_hit", 1), ("duplicates", 1)]).await;

    // No file has made it into the internal buffer => cache hit and drop
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.ingest_queue.length().await.unwrap(), 0);
}

#[tokio::test]
async fn test_ingest_groups_custom() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    // let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // setup the test data
    let mut user: User = Default::default();
    let custom_user_groups = ["users", "the_user"];
    user.uname = "test_ingest_groups_custom".to_string();
    user.groups.extend(custom_user_groups.iter().map(|i|i.parse().unwrap()));
    core.datastore.user.save("test_ingest_groups_custom", &user, None, None).await.unwrap();
    
    // process a message
    ingester.ingest_queue.raw().push(&MakeMessage::new(core.classification_parser.clone())
        .params(json!({"submitter": "test_ingest_groups_custom", "groups": ["group_b"]})).build()
    ).await.unwrap();
    ingester.ingest_once().await.unwrap();

    // make sure it was processed as wanted
    let task = ingester.unique_queue.blocking_pop(std::time::Duration::from_secs(2), false).await.unwrap().unwrap();
    assert_eq!(task.submission.params.submitter, "test_ingest_groups_custom");
    assert_eq!(task.submission.params.groups, ["group_b"].iter().map(|v| UpperString::from(*v)).collect_vec());
}

#[tokio::test]
async fn test_ingest_size_error() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // Send a rather big file
    let submission = MakeMessage::new(core.classification_parser.clone())
        .files(json!({
            "size": core.config.submission.max_file_size + 1,
        }))
        .params(json!({
            "ignore_size": false,
            "never_drop": false
        }))
        .message(json!({
            "notification": {"queue": "test_ingest_size_error"}
        }))
        .build();
    // fo = random_minimal_obj(File)
    // fo.sha256 = submission['files'][0]['sha256']
    // datastore.file.save(submission['files'][0]['sha256'], fo)

    // process the message
    ingester.ingest_queue.raw().push(&submission).await.unwrap();
    ingester.ingest_once().await.unwrap();

    // wait for the error to be sent
    assert_metrics(&mut metrics, &[("error", 1)]).await;

    // No files in the internal buffer
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.ingest_queue.length().await.unwrap(), 0);

    // A file was dropped
    let queue = core.notification_queue("test_ingest_size_error");
    let task = queue.pop_timeout(std::time::Duration::from_secs(2)).await.unwrap().unwrap();
    assert!(!task.failure.is_empty());
}

#[tokio::test]
async fn test_ingest_always_create_submission() {
    let (core, _redis_lock) = Core::test_custom_setup(|config| {
        // Simulate configuration where we'll always create a submission
        config.core.ingester.always_create_submission = true;
    }).await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // Add a valid file score 
    let sha256: Sha256 = thread_rng().r#gen();
    let filescore_cache = assemblyline_models::datastore::filescore::FileScore { 
        psid: Some("000".parse().unwrap()), 
        expiry_ts: chrono::Utc::now(), 
        score: 10, 
        errors: 0, 
        sid: "001".parse().unwrap(), 
        time: chrono::Utc::now().timestamp() as f64
    };
    let classification = ClassificationString::new(core.classification_parser.unrestricted().to_string(), &core.classification_parser).unwrap();
    let key = SubmissionParams::new(classification).create_filescore_key(&sha256, None);
    core.datastore.filescore.save(&key, &filescore_cache, None, None).await.unwrap();

    // Create a submission for cache hit
    let mut old_sub: Submission = thread_rng().r#gen();
    old_sub.sid = "001".parse().unwrap();
    old_sub.params.psid = Some("000".parse().unwrap());
    core.datastore.submission.save(&old_sub.sid.to_string(), &old_sub, None, None).await.unwrap();

    // Ingest a file
    let sid = Sid::from_str("002").unwrap();
    let submission_msg = MakeMessage::new(core.classification_parser.clone())
        .message(json!({"sid": sid}))
        .files(json!({"sha256": sha256.to_string()}))
        .metadata(json!({"blah": "blah"}))
        .build();
    ingester.ingest_queue.raw().push(&submission_msg).await.unwrap();
    ingester.ingest_once().await.unwrap();

    // No file has made it into the internal buffer => cache hit and drop
    assert_metrics(&mut metrics, &[("cache_hit", 1), ("duplicates", 1)]).await;
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.ingest_queue.length().await.unwrap(), 0);

    // Check to see if new submission was created
    let wait_time = std::time::Instant::now();
    let new_sub = loop {
        if let Some(obj) = core.datastore.submission.get(&sid.to_string(), None).await.unwrap() {
            break obj
        }
        if wait_time.elapsed() > Duration::from_secs(60) {
            panic!();
        }
    };
    assert_eq!(new_sub.params.psid.unwrap(), old_sub.sid);

    // convert to json
    let serde_json::Value::Object(new_sub) = serde_json::to_value(&new_sub).unwrap() else { panic!() };
    let serde_json::Value::Object(old_sub) = serde_json::to_value(&old_sub).unwrap() else { panic!() };

    // Check to see if certain properties are same (anything relating to analysis)
    for attr in ["error_count", "errors", "file_count", "files", "max_score", "results", "state", "verdict"] {
        assert_eq!(old_sub.get(attr), new_sub.get(attr));
    }

    // Check to see if certain properties are different
    // (anything that isn't related to analysis but can be set at submission time)
    for attr in ["expiry_ts", "metadata", "params", "times"] {
        assert_ne!(old_sub.get(attr), new_sub.get(attr));
    }

    // Check to see if certain properties have been nullified
    // (properties that are set outside of submission)
    for attr in ["archived", "archive_ts", "to_be_deleted", "from_archive"] {
        let value = new_sub.get(attr).unwrap_or(&serde_json::Value::Null);
        // either null or boolean as false
        assert!(value.is_null() || !value.as_bool().unwrap(), "{} => {:?}", attr, value);
    }
}
        
fn create_ingest_task(ce: &Arc<ClassificationParser>) -> IngestTask {
    let classification = ClassificationString::new(ce.unrestricted().to_string(), ce).unwrap();
    let params = SubmissionParams::new(classification);
    let submission = MessageSubmission {
        params,
        files: vec![File {
            sha256: uniform_string('0', 64).parse().unwrap(),
            size: Some(100),
            name: "abc".to_string(),
        }],
        metadata: Default::default(),
        sid: thread_rng().r#gen(),
        time: chrono::Utc::now(),
        notification: Default::default(),
        scan_key: Default::default(),
    };
    IngestTask::new(submission)
}

#[tokio::test]
async fn test_submit_simple() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    // let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // Push a normal ingest task
    let task = create_ingest_task(&core.classification_parser);
    ingester.unique_queue.push(0.0, &task).await.unwrap();
    ingester.submit_once(true).await.unwrap();

    // The task has been passed to the submit tool and there are no other submissions
    ingester.submit_manager.dispatch_submission_queue.pop().await.unwrap();
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
}

#[tokio::test]
async fn test_submit_duplicate() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // a normal ingest task
    let mut task = create_ingest_task(&core.classification_parser);
    // Make sure the scan key is correct, this is normally done on ingest
    let scan_key = task.submission.params.create_filescore_key(&task.submission.files[0].sha256, Some(vec![]));
    task.submission.scan_key = Some(scan_key.clone());

    // Add this file to the scanning table, so it looks like it has already been submitted + ingest again
    ingester.scanning.add(&scan_key, &task).await.unwrap();
    ingester.unique_queue.push(0.0, &task).await.unwrap();
    ingester.submit_once(true).await.unwrap();

    assert_metrics(&mut metrics, &[("duplicates", 1)]).await;

    // No tasks should be left in the queue
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.submit_manager.dispatch_submission_queue.length().await.unwrap(), 0);

    // The task should have been pushed to the duplicates queue
    assert_eq!(ingester.duplicate_queue.length(&scan_key).await.unwrap(), 1);
}

#[tokio::test]
async fn test_existing_score() {
    let (core, _redis_lock) = Core::test_setup().await;
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    let mut metrics = core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned());

    // Set everything to have an existing filestore
    let sha256: Sha256 = thread_rng().r#gen();
    let filescore_cache = assemblyline_models::datastore::filescore::FileScore { 
        psid: Some("0".parse().unwrap()), 
        expiry_ts: chrono::Utc::now(), 
        score: 10, 
        errors: 0, 
        sid: "0".parse().unwrap(), 
        time: chrono::Utc::now().timestamp() as f64 
    };
    let classification = ClassificationString::new(core.classification_parser.unrestricted().to_string(), &core.classification_parser).unwrap();
    let key = SubmissionParams::new(classification).create_filescore_key(&sha256, None);
    core.datastore.filescore.save(&key, &filescore_cache, None, None).await.unwrap();
    
    // add task to internal queue
    let mut task = create_ingest_task(&core.classification_parser);
    task.submission.files[0].sha256 = sha256;
    task.submission.notification.queue = Some("test_existing_score".to_owned());
    ingester.unique_queue.push(0.0, &task).await.unwrap();

    ingester.submit_once(true).await.unwrap();
    assert_metrics(&mut metrics, &[("cache_hit", 1)]).await;

    // No tasks should be left in the queue
    assert_eq!(ingester.unique_queue.length().await.unwrap(), 0);
    assert_eq!(ingester.submit_manager.dispatch_submission_queue.length().await.unwrap(), 0);

    // We should have received a notification about our task, since it was already 'done'
    let queue = core.notification_queue("test_existing_score");
    assert_eq!(queue.length().await.unwrap(), 1);
}