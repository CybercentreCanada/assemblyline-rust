//! A test of ingest+dispatch running in one process.
//!
//! Needs the datastore and filestore to be running, otherwise these test are stand alone.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::config::PostprocessAction;
use assemblyline_models::datastore::submission::{SubmissionParams, SubmissionState};
use assemblyline_models::datastore::user::User;
use assemblyline_models::datastore::{Service, Submission};
use assemblyline_models::messages::changes::ServiceChange;
use assemblyline_models::messages::task::Task;
use assemblyline_models::types::{ClassificationString, ExpandingClassification, JsonMap, Sha256, Sid};
use log::{debug, error, info};
use parking_lot::Mutex;
use sha2::Digest;
use rand::Rng;
use serde_json::{from_value, json};
use tokio::sync::{mpsc, Notify};

use assemblyline_models::messages::submission::{File, Notification, Submission as MessageSubmission};

use crate::constants::{ServiceStage, INGEST_QUEUE_NAME, METRICS_CHANNEL};
use crate::dispatcher::Dispatcher;

use crate::ingester::{IngestTask, Ingester, SCANNING_TABLE_NAME};
use crate::plumber::Plumber;
use crate::postprocessing::SubmissionFilter;
use crate::service_api::helpers::APIResponse;
use crate::service_api::tests::tasking::TaskResp;
use crate::services::test::{dummy_service, setup_services};
use crate::{Core, Flag, TestGuard};


const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);


/// A fake service that works through the service server to do different test scenarios 
/// based on configuration stored in the target file
/// MARK: MockService
struct MockService {
    service_name: String,
    service_queue: redis_objects::PriorityQueue<Task>,
    server_address: String,
    classification_engine: Arc<ClassificationParser>,

    hits: Mutex<HashMap<String, u64>>,
    finish: Mutex<HashMap<String, u64>>,
    drops: Mutex<HashMap<String, u64>>,

    running: Arc<Flag>,
    local_running: Flag,
    stopped: Flag,
    signal: Arc<Notify>,
    local_signal: Notify,
}

impl MockService {
    async fn new(name: &str, signal: Arc<Notify>, core: &Core, server_address: String) -> Arc<Self> {

        Arc::new(Self {
            service_name: name.to_string(),
            service_queue: core.get_service_queue(name),
            local_running: Flag::new(true),
            stopped: Flag::new(false),
            running: core.running.clone(),
            classification_engine: core.classification_parser.clone(),
            hits: Mutex::new(Default::default()),
            finish: Mutex::new(Default::default()),
            drops: Mutex::new(Default::default()),
            signal,
            local_signal: Notify::new(),
            server_address,
        })
    }

    async fn run(self: Arc<Self>) {
        use reqwest::header::{HeaderMap, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert("Container-Id", HeaderValue::from_str(&self.service_name).unwrap());
        headers.insert("X-APIKey", HeaderValue::from_str(AUTH_KEY).unwrap());
        headers.insert("Service-Name", HeaderValue::from_str(&self.service_name).unwrap());
        headers.insert("Service-Version", HeaderValue::from_str("0").unwrap());
        headers.insert("Service-Tool-Version", HeaderValue::from_str("0").unwrap());
        headers.insert("Timeout", HeaderValue::from_str("3").unwrap());
        // ("X-Forwarded-For", "127.0.0.1".to_owned()),
        let address = self.server_address.clone();

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(90))
            .build().unwrap();

        while self.running.read() && self.local_running.read() {

            let response = client.get(format!("{address}/api/v1/task/")).send().await.unwrap();
            let body = response.error_for_status().unwrap().bytes().await.unwrap();
            let body: APIResponse<TaskResp> = serde_json::from_slice(&body).unwrap();

            let task = match body.api_response {
                TaskResp::Task { task } => task,
                TaskResp::None { task: _ } => continue,
            };

            info!("{} has received a job {}", self.service_name, task.sid);

            let response = client.get(format!("{address}/api/v1/file/{}/", task.fileinfo.sha256)).send().await.unwrap();
            let file = response.error_for_status().unwrap().bytes().await.unwrap();

            let mut instructions: JsonMap = serde_json::from_slice(&file).unwrap();
            let mut instructions: JsonMap = match instructions.remove(&self.service_name) {
                Some(serde_json::Value::Object(value)) => value,
                _ => Default::default()
            };

            info!("{} following instruction: {instructions:?}", self.service_name);
            let hits = {
                let mut hit_table = self.hits.lock();
                let hits = hit_table.entry(task.fileinfo.sha256.to_string()).or_default();
                *hits += 1;
                *hits
            };

            if let Some(hold) = instructions.get("hold") {
                if let Some(hold) = hold.as_i64() {
                    self.service_queue.push(task.priority as f64, &task).await.unwrap();
                    info!("{} Requeued task in {}, holding for {hold}", self.service_name, self.service_queue.name());
                    _ = tokio::time::timeout(Duration::from_secs(hold as u64), self.signal.notified()).await;
                    info!("{} Resuming from hold", self.service_name);
                    continue
                }
            }

            if let Some(lock) = instructions.get("lock") {
                if let Some(lock) = lock.as_i64() {
                    _ = tokio::time::timeout(Duration::from_secs(lock as u64), self.signal.notified()).await;
                }
            }

            if let Some(lock) = instructions.get("local_lock") {
                if let Some(lock) = lock.as_i64() {
                    _ = tokio::time::timeout(Duration::from_secs(lock as u64), self.local_signal.notified()).await;
                }
            }

            if let Some(drop) = instructions.get("drop") {
                if let Some(drop) = drop.as_i64() {
                    if drop >= hits as i64 {
                        *self.drops.lock().entry(task.fileinfo.sha256.to_string()).or_default() += 1;
                        continue
                    }
                }
            }

            if instructions.get("failure").and_then(|x|x.as_bool()).unwrap_or(false) {
                let mut error: assemblyline_models::datastore::Error = serde_json::from_value(instructions.get("error").unwrap().clone()).unwrap();
                error.sha256 = task.fileinfo.sha256.clone();
                // let key = rand::rng().random::<u128>().to_string();
                // self.dispatch_client.service_failed(task, &key, error).await.unwrap();
                let response = client.post(format!("{address}/api/v1/task/")).json(&json!({
                    "task": task,
                    "error": error
                })).send().await.unwrap();
                response.error_for_status().unwrap();
                continue
            }

            let mut partial = false;
            let temp_data: JsonMap = 
                task.temporary_submission_data.iter()
                .map(|item|(item.name.clone(), item.value.clone()))
                .collect();
            if let Some(requirments) = instructions.get("partial") {
                partial = true;
                if let Some(requirements) = requirments.as_object() {
                    for (key, value) in requirements.iter() {
                        let temp = temp_data.get(key).cloned().unwrap_or_default().to_string();
                        if temp.contains(value.as_str().unwrap()) {
                            partial = false;
                        } else {
                            partial = true;
                            break
                        }
                    }
                }
            }

            if partial {
                info!("{} will produce partial results", self.service_name);
            }

            let mut result_data = json!({
                "response": {
                    "service_version": "0",
                    "service_tool_version": "0",
                    "service_name": self.service_name,
                },
                "result": {},
                "partial": partial,
                "sha256": task.fileinfo.sha256,
                "expiry_ts": chrono::Utc::now() + chrono::TimeDelta::seconds(600)
            });

            if let Some(result_data) = result_data.as_object_mut() {
                ExpandingClassification::<false>::insert(&self.classification_engine, result_data, self.classification_engine.unrestricted()).unwrap();

                if let Some(result) = instructions.get_mut("result") {
                    if let Some(result) = result.as_object_mut() {
                        if let Some(serde_json::Value::String(cl)) = result.get("classification") {
                            ExpandingClassification::<false>::insert(&self.classification_engine, result_data, cl).unwrap();
                        }
                        result_data.append(result)
                    }
                }
                if let Some(response) = instructions.get_mut("response") {
                    if let Some(response) = response.as_object_mut() {
                        if let Some(serde_json::Value::Object(result_data)) = result_data.get_mut("response") {
                            result_data.append(response);
                        }
                    }
                }
            }

            let result: assemblyline_models::datastore::Result = from_value(result_data).unwrap();

            let result_key = if let Some(key) = instructions.get("result_key") {
                key.as_str().unwrap().to_owned()
            } else {
                result.build_key(Some(&task)).unwrap()
            };

            // let mut result_key: String = instructions
            //     .get("result_key")
            //     .and_then(|x|x.as_str())
            //     .map(|x|x.to_string())
            //     .unwrap_or_else(|| rand::rng().random::<u64>().to_string());
            // if !instructions.contains_key("result_key") && result.is_empty() {
            //     result_key = result_key + 
            // }

            let temporary_data = match instructions.get("temporary_data") {
                Some(data) => match data.as_object() {
                    Some(data) => data.clone(),
                    None => Default::default(),
                },
                None => Default::default()
            };

            debug!("result: {result_key} -> {result:?}");
            let sha = task.fileinfo.sha256.to_string();

            let serde_json::Value::Object(mut result) = serde_json::to_value(result).unwrap() else { panic!() };
            result.insert("temp_submission_data".to_owned(), serde_json::Value::Object(temporary_data));

            // self.dispatch_client.service_finished(task, result_key, result, Some(temporary_data), None, vec![]).await.unwrap();
            let response = client.post(format!("{address}/api/v1/task/")).json(&json!({
                "task": task,
                "freshen": false,
                "result": result
            })).send().await.unwrap();
            response.error_for_status().unwrap();

            *self.finish.lock().entry(sha).or_default() += 1;
        }
        self.stopped.set(true);
    }

}

fn test_services() -> HashMap<String, Service> {
    return [
        ("pre", dummy_service("pre", "pre", None, None, None, Some(true))),
        ("core-a", dummy_service("core-a", "core", None, None, None, None)),
        ("core-b", dummy_service("core-b", "core", None, None, None, Some(true))),
        ("finish", dummy_service("finish", "post", None, None, None, None)),
    ].into_iter().map(|(key, value)|(key.to_string(), value)).collect()
}

struct TestContext {
    core: Core,
    guard: TestGuard,
    metrics: MetricsWatcher,
    dispatcher: Arc<Dispatcher>,
    ingester: Arc<Ingester>,
    ingest_queue: redis_objects::Queue<MessageSubmission>,
    components: tokio::task::JoinSet<()>,
    signal: Arc<Notify>,
    services: Vec<Arc<MockService>>,
    service_server: tokio::task::JoinHandle<()>,
}

impl TestContext {
    pub fn hits(&self, service_name: &str, hash: &str) -> u64 {
        let mut hits = 0;
        for service in &self.services {
            if service.service_name == service_name {
                hits += service.hits.lock().get(hash).copied().unwrap_or_default()
            }
        }
        hits
    }

    pub fn get_a_service(&self, service_name: &str) -> Option<Arc<MockService>> {
        for service in &self.services {
            if service.service_name == service_name {
                return Some(service.clone())
            }
        }
        None
    }

    pub fn get_service(&self, service_name: &str) -> Vec<Arc<MockService>> {
        let mut out = vec![];
        for service in &self.services {
            if service.service_name == service_name {
                out.push(service.clone());
            }
        }
        out
    }
}

const AUTH_KEY: &str = "test_key_abc_123";

async fn start_api_server(core: Core) -> (tokio::task::JoinHandle<()>, String) {
    std::env::set_var("SERVICE_API_KEY", AUTH_KEY);
    let (port, server) = crate::service_api::tests::launch(Arc::new(core)).await;
    (server, format!("http://localhost:{port}"))
}


// MARK: setup
async fn setup() -> TestContext {
    setup_custom(|i| i).await
}

async fn setup_custom(ingest_op: impl FnOnce(Ingester) -> Ingester) -> TestContext {
    std::env::set_var("BIND_ADDRESS", "0.0.0.0:0");

    // Configure the services
    let mut service_configurations = test_services();
    service_configurations.get_mut("core-a").unwrap().timeout = 100;
    service_configurations.get_mut("core-b").unwrap().timeout = 100;
    let (core, guard) = setup_services(service_configurations).await;

    // launch the api Server
    let (service_server, api_address) = start_api_server(core.clone()).await;

    // launch the services
    let signal = Arc::new(Notify::new());
    let mut components = tokio::task::JoinSet::new();
    let mut services = vec![];
    let stages = core.services.get_service_stage_hash();
    for (name, service) in test_services() {
        let count = if name == "core-a" { 2 } else { 1 };

        core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
        core.datastore.service_delta.save_json(&name, json!({
            "name": name,
            "version": service.version,
            "enabled": true
        }).as_object_mut().unwrap(), None, None).await.unwrap();
        stages.set(&name, &ServiceStage::Running).await.unwrap();

        for _ in 0..count {
            let service_agent = MockService::new(&name, signal.clone(), &core, api_address.clone()).await;
            components.spawn(service_agent.clone().run());
            services.push(service_agent);
        }
    }

    // setup test user
    let user: User = User::create_test_user();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    core.datastore.user.commit(None).await.unwrap();

    // launch the ingester
    let ingester = Arc::new(ingest_op(Ingester::new(core.clone()).await.unwrap()));
    ingester.start(&mut components).await.unwrap();

    // launch the dispatcher
    let bind_address = "0.0.0.0:0".parse().unwrap();
    let tls_config = crate::config::TLSConfig::load().await.unwrap();
    let tcp = crate::http::create_tls_binding(bind_address, tls_config).await.unwrap();
    let dispatcher = Dispatcher::new(core.clone(), tcp).await.unwrap();
    dispatcher.start(&mut components);

    // launch the plumber
    let plumber_name = format!("plumber{}", rand::rng().random::<u32>());
    let plumber = Plumber::new(core.clone(), Some(Duration::from_secs(2)), Some(&plumber_name)).await.unwrap();
    plumber.start(&mut components).await.unwrap();


    TestContext {
        metrics: MetricsWatcher::new(core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned()).await),
        ingest_queue: core.redis_persistant.queue(INGEST_QUEUE_NAME.to_owned(), None),
        core,
        guard,
        dispatcher,
        ingester,
        components,
        signal,
        services,
        service_server
    }
}


async fn ready_body(core: &Core, mut body: serde_json::Value) -> (Sha256, usize) {
    let body = {
        let out = body.as_object_mut().unwrap();
        out.insert("salt".to_owned(), json!(rand::rng().random::<u64>().to_string()));
        bytes::Bytes::from(serde_json::to_string(&body).unwrap())
    };

    let mut hasher = sha2::Sha256::default();
    hasher.update(&body);
    let sha256 = Sha256::try_from(hasher.finalize().as_slice()).unwrap();
    core.filestore.put(&sha256, &body).await.unwrap();

    let temporary_file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(temporary_file.path(), &body).await.unwrap();
    let fileinfo = core.identify.fileinfo(temporary_file.path().to_owned(), None, None, None).await.unwrap();
    let serde_json::Value::Object(fileinfo) = serde_json::to_value(&fileinfo).unwrap() else { panic!() };
    let expiry = Some(chrono::Utc::now() + chrono::TimeDelta::seconds(500));
    core.datastore.save_or_freshen_file(&sha256, fileinfo, expiry, core.classification_parser.unrestricted().to_owned(), &core.classification_parser).await.unwrap();

    return (sha256, body.len())
}

async fn ready_extract(core: &Core, children: &[Sha256]) -> (Sha256, usize) {
    let mut extracted = vec![];
    for child in children {
        let mut entry = json!({
            "name": child,
            "sha256": child,
            "description": "abc",
        });
        if let Some(entry) = entry.as_object_mut() {
            ExpandingClassification::<false>::insert(&core.classification_parser, entry, core.classification_parser.unrestricted()).unwrap();
        }
        extracted.push(entry);
    }

    let body = json!({
        "pre": {
            "response": {
                "extracted": extracted
            }
        }
    });

    return ready_body(core, body).await
}

struct MetricsWatcher {
    counts: Arc<Mutex<HashMap<String, HashMap<String, i64>>>>,
}

impl MetricsWatcher {
    fn new(mut metrics: mpsc::Receiver<Option<redis_objects::Msg>>) -> Self {
        let counts = Arc::new(Mutex::new(HashMap::<String, HashMap<String, i64>>::new()));
        tokio::spawn({
            let counts = counts.clone();
            async move {
                while let Some(msg) = metrics.recv().await {
                    let message = match msg {
                        Some(message) => message,
                        None => continue,
                    };
                    let message: String = message.get_payload().unwrap();
                    let message: JsonMap = serde_json::from_str(&message).unwrap();

                    let message_type = if let Some(message_type) = message.get("type") {
                        if let Some(message_type) = message_type.as_str() {
                            message_type.to_string()
                        } else {
                            continue
                        }
                    } else {
                        continue
                    };

                    {
                        let mut counts = counts.lock();
                        let type_counts = counts.entry(message_type).or_default();
                        for (key, value) in message {
                            if let Some(number) = value.as_number() {
                                if let Some(number) = number.as_i64() {
                                    *type_counts.entry(key).or_default() += number;
                                }
                            }
                        }

                        type_counts.retain(|_, v| *v != 0);
                    }
                }
            }
        });
        Self { counts }
    }

    pub fn clear(&self) {
        self.counts.lock().clear();
    }

    /// wait for metrics messages with the given fields
    pub async fn assert_metrics(&self, service: &str, values: &[(&str, i64)]) {
        let start = std::time::Instant::now();
        let mut values: HashMap<&str, i64> = HashMap::from_iter(values.iter().cloned());
        while !values.is_empty() {
            if start.elapsed() > RESPONSE_TIMEOUT {
                match self.counts.lock().get(service) {
                    Some(count) => error!("Existing metrics for {service}: {count:?}"),
                    None => error!("No metrics for {service}"),
                };
                panic!("Metrics failed {service} {:?}", values);
            }

            {
                let mut count = self.counts.lock();
                let type_counts = match count.get_mut(service) {
                    Some(count) => count,
                    None => continue,
                };

                for (key, value) in values.iter_mut() {
                    let input_value = match type_counts.get_mut(*key) {
                        Some(input) => input,
                        None => continue
                    };

                    let change = *input_value.min(value);
                    *input_value -= change;
                    *value -= change;
                }

                values.retain(|_, v| *v != 0);
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

}


/// MARK: deduplication
/// Submit two identical jobs, check that they get deduped by ingester
#[tokio::test(flavor = "multi_thread")]
async fn test_deduplication() {
    let context = setup().await;

    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"lock": 60}
    })).await;

    for _ in 0..2 {
        context.ingest_queue.push(&MessageSubmission {
            sid: rand::rng().random(),
            metadata: Default::default(),
            params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
                .set_description("file abc123")
                .set_services_selected(&[])
                .set_submitter("user")
                .set_groups(&["user"]),
            notification: Notification {
                queue: Some("output-queue-one".to_string()),
                threshold: None,
            },
            files: vec![File {
                sha256: sha.clone(),
                size: Some(size as u64),
                name: "abc123".to_string()
            }],
            time: chrono::Utc::now(),
            scan_key: None,
        }).await.unwrap();
    }

    context.metrics.assert_metrics("ingester", &[("duplicates", 1)]).await;
    context.signal.notify_one();

    // notification_queue = NamedQueue('', core.redis)
    let notification_queue = context.core.notification_queue("output-queue-one");
    let first_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    // One of the submission will get processed fully
    let first_submission: Submission = context.core.datastore.submission.get(&first_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(first_submission.state, SubmissionState::Completed);
    assert_eq!(first_submission.files.len(), 1);
    assert_eq!(first_submission.errors.len(), 0);
    assert_eq!(first_submission.results.len(), 4);

    // The other will get processed as a duplicate
    // (Which one is the 'real' one and which is the duplicate isn't important for our purposes)
    let second_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();
    assert_eq!(second_task.submission.sid, first_task.submission.sid);

    // -------------------------------------------------------------------------------
    // Submit the same body, but change a parameter so the cache key misses,
    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"])
            .set_max_extracted(10000),
        notification: Notification {
            queue: Some("2".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    context.signal.notify_one();

    let notification_queue = context.core.notification_queue("2");
    let third_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    // The third task should not be deduplicated by ingester, so will have a different submission
    let third_submission: Submission = context.core.datastore.submission.get(&third_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(third_submission.state, SubmissionState::Completed);
    assert_ne!(first_submission.sid, third_submission.sid);
    assert_eq!(third_submission.files.len(), 1);
    assert_eq!(third_submission.results.len(), 4);

    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 3)]).await;
    context.metrics.assert_metrics("ingester", &[("submissions_completed", 2)]).await;
    context.metrics.assert_metrics("ingester", &[("files_completed", 2)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 2), ("files_completed", 2)]).await;
}

/// MARK: ingest retry
#[tokio::test(flavor = "multi_thread")]
async fn test_ingest_retry() {

    // let attempts = Arc::new(Mutex::new(vec![]));
    // let failures = Arc::new(Mutex::new(vec![]));

    // install a worker task between the ingester and the dispatcher that drops the first message it sees
    let context = setup_custom({
        // let attempts = attempts.clone();
        // let failures = failures.clone();
        |mut ingester| {
            ingester.set_retry_delay(chrono::Duration::seconds(1));

            *ingester.test_hook_fail_submit.lock() = 1;

            // let redis = ingester.submit_manager.dispatch_submission_queue.host();
            // let queue = redis.queue("replacement-dispatch-queue-".to_string() + &rand::rng().random::<u64>().to_string(), None);
            // let mut original_queue = queue.clone();
            // std::mem::swap(&mut original_queue, &mut ingester.submit_manager.dispatch_submission_queue);

            // tokio::spawn(async move {
            //     loop {
            //         let item = queue.pop_timeout(Duration::from_secs(60)).await.unwrap();
            //         if let Some(item) = item {
            //             println!("dispatcher proxy saw item {} + 1", attempts.lock().len());
            //             attempts.lock().push(item.clone());
            //             if attempts.lock().len() > 1 {
            //                 original_queue.push(&item).await.unwrap();
            //             } else {
            //                 println!("dispatcher proxy dropped message");
            //                 failures.lock().push(item);
            //             }
            //         } else {
            //             println!("dispatcher proxy empty message?");
            //         }
            //     }
            // });

            ingester
        }
    }).await;
    let (sha, size) = ready_body(&context.core, json!({})).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("ingest-retry".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("ingest-retry");
    let first_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    // One of the submission will get processed fully
    let first_submission: Submission = context.core.datastore.submission.get(&first_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    // assert_eq!(attempts.lock().len(), 2);
    // assert_eq!(failures.lock().len(), 1);
    assert_eq!(*context.ingester.test_hook_fail_submit.lock(), 0);
    assert_eq!(first_submission.state, SubmissionState::Completed);
    assert_eq!(first_submission.files.len(), 1);
    assert_eq!(first_submission.errors.len(), 0);
    assert_eq!(first_submission.results.len(), 4);

    // metrics.expect('ingester', 'duplicates', 0)
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1), ("retries", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;

}

/// MARK: ingest timeout
#[tokio::test(flavor = "multi_thread")]
async fn test_ingest_timeout() {
    // install a worker task between the ingester and the dispatcher that drops the first message it sees
    let context = setup_custom({
        |mut ingester| {
            ingester.set_timeout_delay(chrono::Duration::seconds(1));
            ingester
        }
    }).await;

    let (sha, size) = ready_body(&context.core, json!({
        "pre": {
            "hold": 60 
        } 
    })).await;

    let mut message = MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("ingest-timeout".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    };

    // this extra normalization is applied when the message is loaded, which changes the scan key
    // normally this doesn't matter because it would always be consistant _after_ message loading
    // but since we want the key before sending the message we apply it early
    message.params.classification = ClassificationString::new(message.params.classification.to_string(), &context.core.classification_parser).unwrap();

    let scan_key = message.params.create_filescore_key(&sha, None);

    context.ingest_queue.push(&message).await.unwrap();

    // Make sure the scanning table has been cleared
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let scanning = context.core.redis_persistant.hashmap::<IngestTask>(SCANNING_TABLE_NAME.to_owned(), None);
    for _ in 0..60 {
        if !scanning.exists(&scan_key).await.unwrap() {
            break
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(scanning.length().await.unwrap(), 0);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 0), ("timed_out", 1)]).await;
}

// MARK: service crash
#[tokio::test(flavor = "multi_thread")]
async fn test_service_crash_recovery() {
    // This time have the service 'crash'
    let context = setup().await;
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"drop": 1}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("watcher-recover".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("watcher-recover");
    let dropped_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&dropped_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.errors.len(), 0);  // No error raised if the service succeeds on retry
    assert_eq!(sub.results.len(), 4);
    assert_eq!(*context.get_a_service("pre").unwrap().drops.lock().get(&sha.to_string()).unwrap(), 1);
    assert_eq!(context.hits("pre", &sha), 2);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
    context.metrics.assert_metrics("service", &[("fail_recoverable", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("service_timeouts", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
}

// MARK: retry limit
#[tokio::test(flavor = "multi_thread")]
async fn test_service_retry_limit() {
    // This time have the service 'crash'
    let context = setup().await;
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"drop": 3}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("watcher-recover".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("watcher-recover");
    let dropped_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&dropped_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.errors.len(), 1);
    assert_eq!(sub.results.len(), 3);
    assert_eq!(*context.get_a_service("pre").unwrap().drops.lock().get(&sha.to_string()).unwrap(), 3);
    assert_eq!(context.hits("pre", &sha), 3);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
    context.metrics.assert_metrics("service", &[("fail_recoverable", 3), ("fail_nonrecoverable", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("service_timeouts", 3), ("submissions_completed", 1), ("files_completed", 1)]).await;
}

// MARK: dropping early
#[tokio::test(flavor = "multi_thread")]
async fn test_dropping_early() {
    // This time have a file get marked for dropping by a service
    let context = setup().await;
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"result": {"drop_file": true}}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("drop".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("drop");
    let dropped_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&dropped_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 1);

    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;
}

// MARK: service error
#[tokio::test(flavor = "multi_thread")]
async fn test_service_error() {
    // Have a service produce an error
    let context = setup().await;
    let (sha, size) = ready_body(&context.core, json!({
        "core-a": {
            "error": {
                "sha256": vec!["a"; 64].join(""),
                "response": {
                    "message": "words",
                    "status": "FAIL_NONRECOVERABLE",
                    "service_name": "core-a",
                    "service_tool_version": "0",
                    "service_version": "0"
                },
                "expiry_ts": chrono::Utc::now() + chrono::TimeDelta::seconds(500),
            },
            "failure": true,
        }
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("error".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("error");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 3);
    assert_eq!(sub.errors.len(), 1);

    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
}

// MARK: extracted file
#[tokio::test(flavor = "multi_thread")]
async fn test_extracted_file() {
    let context = setup().await;
    let (sha, size) = ready_extract(&context.core, &[ready_body(&context.core, json!({})).await.0]).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("text-extracted-file".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("text-extracted-file");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 8);
    assert_eq!(sub.errors.len(), 0);

    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 2)]).await;
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
}

// MARK: depth limit
#[tokio::test(flavor = "multi_thread")]
async fn test_depth_limit() {
    let context = setup().await;
    // Make a nested set of files that goes deeper than the max depth by one
    let (mut sha, mut size) = ready_body(&context.core, json!({})).await;
    for _ in 0..(context.core.config.submission.max_extraction_depth + 1) {
        (sha, size) = ready_extract(&context.core, &[sha]).await;
    }

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"])
            // Make sure we can extract enough files that we will definitely hit the depth limit first
            .set_max_extracted(context.core.config.submission.max_extraction_depth as i32 + 10),
        notification: Notification {
            queue: Some("test-depth-limit".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("test-depth-limit");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    // start = time.time()
    // task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    // print("notification time waited", time.time() - start)

    let sub: Submission = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    // We should only get results for each file up to the max depth
    assert_eq!(sub.results.len(), 4 * context.core.config.submission.max_extraction_depth as usize);
    assert_eq!(sub.errors.len(), 1);

    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", context.core.config.submission.max_extraction_depth.into())]).await;
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", context.core.config.submission.max_extraction_depth.into())]).await;
}

// MARK: extract in one
#[tokio::test(flavor = "multi_thread")]
async fn test_max_extracted_in_one() {
    let context = setup().await;
    // Make a set of files that is bigger than max_extracted (3 in this case)
    let mut children = vec![];
    for _ in 0..5 {
        children.push(ready_body(&context.core, json!({})).await.0);
    }
    let (sha, size) = ready_extract(&context.core, &children).await;
    let max_extracted = 3;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"])
            .set_max_extracted(max_extracted),
        notification: Notification {
            queue: Some("test-extracted-in-one".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("test-extracted-in-one");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub: Submission = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    // We should only get results for each file up to the max depth
    assert_eq!(sub.results.len(), 4 * (1 + 3));
    assert_eq!(sub.errors.len(), 2);  // The number of children that errored out

    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", max_extracted as i64 + 1)]).await;
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", max_extracted as i64 + 1)]).await;
}

// MARK: extract in N
#[tokio::test(flavor = "multi_thread")]
async fn test_max_extracted_in_several() {
    let context = setup().await;
    // Make a set of in a non trivial tree, that add up to more than 3 (max_extracted) files
    let (sha, size) = ready_extract(&context.core, &[
        ready_extract(&context.core, &[
            ready_body(&context.core, json!({})).await.0,
            ready_body(&context.core, json!({})).await.0
        ]).await.0,
        ready_extract(&context.core, &[
            ready_body(&context.core, json!({})).await.0,
            ready_body(&context.core, json!({})).await.0
        ]).await.0
    ]).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"])
            .set_max_extracted(3),
        notification: Notification {
            queue: Some("test-extracted-in-several".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("test-extracted-in-several");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub: Submission = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    // We should only get results for each file up to the max depth
    assert_eq!(sub.results.len(), 4 * (1 + 3));  // 4 services, 1 original file, 3 extracted files
    assert_eq!(sub.errors.len(), 3);  // The number of children that errored out

    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 4)]).await;
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 4)]).await;
}

// MARK: caching
#[tokio::test(flavor = "multi_thread")]
async fn test_caching() {
    let context = setup().await;
    let (sha, size) = ready_body(&context.core, json!({})).await;

    async fn run_once(context: &TestContext, sha: &Sha256, size: usize) -> Sid {
        context.ingest_queue.push(&MessageSubmission {
            sid: rand::rng().random(),
            metadata: Default::default(),
            params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
                .set_description("file abc123")
                .set_services_selected(&[])
                .set_submitter("user")
                .set_groups(&["user"]),
            notification: Notification {
                queue: Some("caching".to_string()),
                threshold: None,
            },
            files: vec![File {
                sha256: sha.clone(),
                size: Some(size as u64),
                name: "abc123".to_string()
            }],
            time: chrono::Utc::now(),
            scan_key: None,
        }).await.unwrap();

        let notification_queue = context.core.notification_queue("caching");
        let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

        // One of the submission will get processed fully
        let sub: Submission = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
        assert_eq!(sub.state, SubmissionState::Completed);
        assert_eq!(sub.files.len(), 1);
        assert_eq!(sub.errors.len(), 0);
        assert_eq!(sub.results.len(), 4);
        return sub.sid
    }

    let sid1 = run_once(&context, &sha, size).await;
    context.metrics.assert_metrics("ingester", &[("cache_miss", 1)]).await;
    context.metrics.clear();

    let sid2 = run_once(&context, &sha, size).await;
    context.metrics.assert_metrics("ingester", &[("cache_hit_local", 1)]).await;
    context.metrics.clear();
    assert_eq!(sid1, sid2);

    context.ingester.clear_local_cache();

    let sid3 = run_once(&context, &sha, size).await;
    context.metrics.assert_metrics("ingester", &[("cache_hit", 1)]).await;
    context.metrics.clear();
    assert_eq!(sid1, sid3);
}

// MARK: plumber
/// Have the plumber cancel tasks
#[tokio::test(flavor = "multi_thread")]
async fn test_plumber_clearing() {
    let context = setup().await;
    // terminate the core-b worker
    context.get_a_service("core-b").unwrap().local_running.set(false);
    context.get_a_service("core-b").unwrap().stopped.wait_for(true).await;

    let (sha, size) = ready_body(&context.core, json!({})).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("test_plumber_clearing".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1)]).await;
    let service_queue = context.core.get_service_queue("core-b");

    let start = std::time::Instant::now();
    while service_queue.length().await.unwrap() < 1 {
        if start.elapsed() > RESPONSE_TIMEOUT {
            panic!();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // actually disable the service so that plumber will handle the hanging task
    println!("------------ disabling core-b");
    let mut service_delta = context.core.datastore.service_delta.get("core-b", None).await.unwrap().unwrap();
    service_delta.enabled = Some(false);
    context.core.datastore.service_delta.save("core-b", &service_delta, None, None).await.unwrap();
    // context.core.services.get_service_stage_hash().set("core-b", )
    context.core.datastore.service_delta.commit(None).await.unwrap();
    context.core.redis_volatile.publish("changes.services.core-b", &serde_json::to_vec(&ServiceChange {
        name: "core-b".to_owned(),
        operation: assemblyline_models::messages::changes::Operation::Modified
    }).unwrap()).await.unwrap();

    let notification_queue = context.core.notification_queue("test_plumber_clearing");
    let dropped_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&dropped_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 3);
    assert_eq!(sub.errors.len(), 1);
    let error = context.core.datastore.error.get(&sub.errors[0], None).await.unwrap().unwrap();
    assert!(error.response.message.as_str().contains("disabled"));

    context.metrics.assert_metrics("ingester", &[("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;
}

// MARK: filter
#[tokio::test(flavor = "multi_thread")]
async fn test_filter() {
    let context = setup().await;
    let filter_string = "params.submitter: user";

    {
        let mut actions: HashMap<String, (_, _)> = Default::default();
        actions.insert("test_process".to_string(), (
            SubmissionFilter::new(filter_string).unwrap(), 
            PostprocessAction::new(filter_string.to_string()).enable().alert().on_completed()
        ));

        *context.dispatcher.postprocess_worker.actions.write().await = Arc::new(actions);
    }

    let (sha, size) = ready_extract(&context.core, &[ready_body(&context.core, json!({})).await.0]).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"])
            .set_generate_alert(true),
        notification: Notification {
            queue: Some("text-filter".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("text-filter");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 8);
    assert_eq!(sub.errors.len(), 0);

    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 2)]).await;

    let alert = context.dispatcher.postprocess_worker.alert_queue.pop_timeout(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(alert.as_object().unwrap().get("submission").unwrap().as_object().unwrap().get("sid").unwrap().as_str().unwrap(), sub.sid.to_string())
}

// MARK: tag filter
#[tokio::test(flavor = "multi_thread")]
async fn test_tag_filter() {
    let context = setup().await;
    let filter_string = "tags.file.behavior: exist";

    {
        let mut actions: HashMap<String, (_, _)> = Default::default();
        actions.insert("test_process".to_string(), (
            SubmissionFilter::new(filter_string).unwrap(), 
            PostprocessAction::new(filter_string.to_string()).enable().alert().on_completed()
        ));

        *context.dispatcher.postprocess_worker.actions.write().await = Arc::new(actions);
    }

    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"result": {"result": { "sections": [{
            "body": "info",
            "body_format": "TEXT",
            "classification": context.core.classification_parser.unrestricted(),
            "depth": 0,
            "tags": {
                "file": {
                    "behavior": ["exist"]
                }
            },
            "title_text": "title"
        }]}}}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"])
            .set_generate_alert(true),
        notification: Notification {
            queue: Some("tag-filter".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("tag-filter");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 4);
    assert_eq!(sub.errors.len(), 0);

    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;

    let alert = context.dispatcher.postprocess_worker.alert_queue.pop_timeout(Duration::from_secs(30)).await.unwrap().unwrap();
    assert_eq!(alert.as_object().unwrap().get("submission").unwrap().as_object().unwrap().get("sid").unwrap().as_str().unwrap(), sub.sid.to_string())
}


// MARK: partial
#[tokio::test(flavor = "multi_thread")]
async fn test_partial() {
    let context = setup().await;
    // Have pre produce a partial result, then have core-a update a monitored key
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"partial": {"passwords": "test_temp_data_monitoring"}}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("temp-data-monitor".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("temp-data-monitor");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();

    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.errors.len(), 0);
    assert_eq!(sub.results.len(), 4);
    assert_eq!(context.hits("pre", &sha), 1);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;

    let mut partial_results = 0;
    for res in sub.results {
        info!("loading result: {res}");
        let result = context.core.datastore.get_single_result(&res, 
            context.core.config.submission.emptyresult_dtl.into(),
            &context.core.classification_parser).await.unwrap().unwrap();
        if result.partial {
            partial_results += 1;
        }
    }
    assert_eq!(partial_results, 1, "partial_results");
}

// MARK: monitoring
#[tokio::test(flavor = "multi_thread")]
async fn test_temp_data_monitoring() {    
    let context = setup().await;

    // Have pre produce a partial result, then have core-a update a monitored key
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"partial": {"passwords": "test_temp_data_monitoring"}},
        "core-a": {"temporary_data": {"passwords": ["test_temp_data_monitoring"]}},
        "final": {"temporary_data": {"passwords": ["some other password"]}},
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("temp-data-monitor".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    let notification_queue = context.core.notification_queue("temp-data-monitor");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();

    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.errors.len(), 0);
    assert_eq!(sub.results.len(), 4);
    assert!(context.hits("pre", &sha) >= 2);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;

    let mut partial_results = 0;
    for res in sub.results {
        info!("loading result: {res}");
        let result = context.core.datastore.get_single_result(&res, 
            context.core.config.submission.emptyresult_dtl.into(),
            &context.core.classification_parser).await.unwrap().unwrap();
        if result.partial {
            partial_results += 1;
        }
    }
    assert_eq!(partial_results, 0, "partial_results");
}

// MARK: final-partial
#[tokio::test(flavor = "multi_thread")]
async fn test_final_partial() {
    let context = setup().await;

    // This test was written to cover an error where a partial result produced as the final
    // result of a submission would not trigger dispatching when it should due to data
    // that was produced while it was running.

    // Both services run at the same time, but one requires info from the other.
    // We lock down the timing of the service completion so that:
    // a) both run at the same time so that in its first run core-b does not have the
    //    temp data it wants and produces a partial result.
    // b) core-a finishes before core-b adding the temporary data to the dispatcher
    // c) core-b finishes and should trigger a rerun with the data to produce a full result
    let (sha, size) = ready_body(&context.core, json!({
        "core-a": {"local_lock": 10, "temporary_data": {"passwords": ["test_temp_data_monitoring"]}},
        "core-b": {"local_lock": 10, "partial": {"passwords": "test_temp_data_monitoring"}},
    })).await;

    let core_a = context.get_service("core-a");
    let core_b = context.get_service("core-b");

    assert_eq!(core_b.len(), 1);
    let core_b = core_b[0].clone();

    context.ingest_queue.push(&MessageSubmission {
        sid: rand::rng().random(),
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&["core-a", "core-b"])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("temp-final-partial".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    // Wait until both of the services have started (so service b doesn't get the temp data a produces on its first run)
    let start = std::time::Instant::now();
    while context.hits("core-a", &sha) + context.hits("core-b", &sha) != 2 {
        println!("hits: {} + {}", context.hits("core-a", &sha), context.hits("core-b", &sha));
        if start.elapsed() > RESPONSE_TIMEOUT {
            panic!();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Release a
    info!("Release core_a");
    for ser in &core_a { ser.local_signal.notify_one(); }

    // Let a finish so that the temporary data is added in the dispatcher
    while core_a.iter().map(|s|s.finish.lock().get(&*sha).copied().unwrap_or_default()).sum::<u64>() < 1 {
        if start.elapsed() > RESPONSE_TIMEOUT {
            panic!()
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Let b finish, it should produce a partial result then rerun right away
    core_b.local_signal.notify_one();
    while core_b.finish.lock().get(&*sha).copied().unwrap_or_default() < 1 {
        if start.elapsed() > RESPONSE_TIMEOUT {
            panic!()
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    core_b.local_signal.notify_one();

    let notification_queue = context.core.notification_queue("temp-final-partial");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();
    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();

    // The submission should produce no errors and two results
    assert_eq!(sub.errors.len(), 0);
    assert_eq!(sub.results.len(), 2);

    // b service should have run twice to produce the results
    assert!(core_b.hits.lock().get(&*sha).copied().unwrap_or_default() >= 2);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;

    // Verify thath there are no partial results in the final submission
    let mut partial_results = 0;
    for res in sub.results {
        info!("loading result: {res}");
        let result = context.core.datastore.get_single_result(&res, 
            context.core.config.submission.emptyresult_dtl.into(),
            &context.core.classification_parser).await.unwrap().unwrap();
        if result.partial {
            partial_results += 1;
        }
    }
    assert_eq!(partial_results, 0, "partial_results");
}

// MARK: tag filter
#[tokio::test(flavor = "multi_thread")]
async fn test_complex_extracted() {
    // stages to this processing when everything goes well
    // 1. extract a file that will process to produce a partial result
    // 2. hold a few seconds on the second stage of the root file to let child start
    // 3. on the last stage of the root file produce the password
    // dispatcher.TIMEOUT_EXTRA_TIME = 10
    let context = setup().await;

    let (child_sha, _) = ready_body(&context.core, json!({
        "pre": {"partial": {"passwords": "test_temp_data_monitoring"}}
    })).await;

    let (sha, size) = ready_body(&context.core, json!({
        "pre": {
            "response": {
                "extracted": [{
                    "name": child_sha,
                    "sha256": child_sha,
                    "description": "abc",
                    "classification": context.core.classification_parser.unrestricted()
                }]
            }
        },
        "core-a": {"lock": 500},
        "finish": {"temporary_data": {"passwords": ["test_temp_data_monitoring"]}},
    })).await;

    let sid = rand::rng().random();
    context.ingest_queue.push(&MessageSubmission {
        sid,
        metadata: Default::default(),
        params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
            .set_description("file abc123")
            .set_services_selected(&[])
            .set_submitter("user")
            .set_groups(&["user"]),
        notification: Notification {
            queue: Some("complex-extracted-file".to_string()),
            threshold: None,
        },
        files: vec![File {
            sha256: sha.clone(),
            size: Some(size as u64),
            name: "abc123".to_string()
        }],
        time: chrono::Utc::now(),
        scan_key: None,
    }).await.unwrap();

    // Wait for the extract file to finish
    context.metrics.assert_metrics("dispatcher", &[("files_completed", 1)]).await;
    // check that there is a pending result in the dispatcher
    // task = next(iter(core.dispatcher.tasks.values()))
    // assert 1 == sum(int(summary.partial) for summary in task.service_results.values())
    log::warn!("load test report for {sid}");
    let report = context.dispatcher.get_test_report(sid).await.unwrap();
    let partial_count: u32 = report.service_results.values().map(|row| if row.partial {1} else {0}).sum();
    assert_eq!(partial_count, 1);

    context.signal.notify_waiters();

    // Wait for the entire submission to finish
    let notification_queue = context.core.notification_queue("complex-extracted-file");
    let task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();
    let sub = context.core.datastore.submission.get(&task.submission.sid.to_string(), None).await.unwrap().unwrap();

    assert!(sub.errors.is_empty());
    assert_eq!(sub.results.len(), 8, "results");
    assert!(context.hits("pre", &sha) == 1);
    assert!(context.hits("pre", &child_sha) >= 1);

    // Wait until we get feedback from the metrics channel
    context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await; // only one file completed because we asserted one earlier

    let mut partial_results = 0;
    for res in sub.results {
        info!("loading result: {res}");
        let result = context.core.datastore.get_single_result(&res, 
            context.core.config.submission.emptyresult_dtl.into(),
            &context.core.classification_parser).await.unwrap().unwrap();
        if result.partial {
            partial_results += 1;
        }
    }
    assert_eq!(partial_results, 0, "partial_results");
}