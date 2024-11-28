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
use assemblyline_models::{ClassificationString, ExpandingClassification, JsonMap, Sha256, Sid};
use log::{debug, error, info};
use parking_lot::Mutex;
use sha2::Digest;
use rand::{thread_rng, Rng};
use serde_json::{from_value, json};
use tokio::sync::{mpsc, Notify};

use assemblyline_models::messages::submission::{File, Notification, Submission as MessageSubmission};

use crate::constants::{ServiceStage, INGEST_QUEUE_NAME, METRICS_CHANNEL};
use crate::dispatcher::client::DispatchClient;
use crate::dispatcher::Dispatcher;

use crate::filestore::FileStore;
use crate::ingester::Ingester;
use crate::plumber::Plumber;
use crate::postprocessing::SubmissionFilter;
use crate::services::test::{dummy_service, setup_services};
use crate::{Core, Flag, TestGuard};

// from __future__ import annotations
// import hashlib
// import json
// import typing
// import time
// import threading
// import logging
// from tempfile import NamedTemporaryFile
// from typing import TYPE_CHECKING, Any

// import pytest

// from assemblyline.common import forge
// from assemblyline.common.forge import get_service_queue
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.common.uid import get_random_id
// from assemblyline.datastore.helper import AssemblylineDatastore
// from assemblyline.odm.models.config import Config
// from assemblyline.odm.models.error import Error
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.service_delta import ServiceDelta
// from assemblyline.odm.models.submission import Submission
// from assemblyline.odm.models.user import User
// from assemblyline.odm.randomizer import random_model_obj
// from assemblyline.odm.messages.submission import Submission as SubmissionInput
// from assemblyline.remote.datatypes.queues.named import NamedQueue

// import assemblyline_core
// from assemblyline_core.plumber.run_plumber import Plumber
// from assemblyline_core.dispatching import dispatcher
// from assemblyline_core.dispatching.client import DispatchClient
// from assemblyline_core.dispatching.dispatcher import Dispatcher
// from assemblyline_core.ingester.ingester import IngestTask, Ingester
// from assemblyline_core.server_base import ServerBase, get_service_stage_hash, ServiceStage

// from test_scheduler import dummy_service

// if TYPE_CHECKING:
//     from redis import Redis

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);


/// Replaces everything past the dispatcher.
/// MARK: MockService
/// Including service API, in the future probably include that in this test.
struct MockService {
    service_name: String,
    service_queue: redis_objects::PriorityQueue<Task>,
    classification_engine: Arc<ClassificationParser>,
    // self.datastore = datastore
    filestore: Arc<FileStore>,
    // self.queue = get_service_queue(name, redis)

    hits: Mutex<HashMap<String, u64>>,
    drops: Mutex<HashMap<String, u64>>,

    running: Arc<Flag>,
    dispatch_client: DispatchClient,
    signal: Arc<Notify>,
}

impl MockService {
    async fn new(name: &str, signal: Arc<Notify>, core: &Core) -> Arc<Self> {
        // self.service_name = name
        // self.datastore = datastore
        // self.filestore = filestore
        // self.queue = get_service_queue(name, redis)
        // self.dispatch_client = DispatchClient(self.datastore, redis)
        Arc::new(Self {
            service_name: name.to_string(),
            service_queue: core.get_service_queue(name),
            running: core.running.clone(),
            classification_engine: core.classification_parser.clone(),
            filestore: core.filestore.clone(),
            dispatch_client: DispatchClient::new_from_core(core).await.unwrap(),
            hits: Mutex::new(Default::default()),
            drops: Mutex::new(Default::default()),
            signal,
        })
    }

    async fn run(self: Arc<Self>) {
        while self.running.read() {
            let task = self.dispatch_client.request_work("worker", &self.service_name, "0", Some(Duration::from_secs(3)), true, None).await.unwrap();
            let task = match task {
                Some(task) => task,
                None => continue,
            };
            info!("{} has received a job {}", self.service_name, task.sid);

            let file = self.filestore.get(&task.fileinfo.sha256).await.unwrap().unwrap();

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
                    self.service_queue.push(0.0, &task).await.unwrap();
                    info!("{} Requeued task, holding for {hold}", self.service_name);
                    _ = tokio::time::timeout(Duration::from_secs(hold as u64), self.signal.notified()).await;
                    continue
                }
            }

            if let Some(lock) = instructions.get("lock") {
                if let Some(lock) = lock.as_i64() {
                    _ = tokio::time::timeout(Duration::from_secs(lock as u64), self.signal.notified()).await;
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
                let key = thread_rng().gen::<u128>().to_string();
                self.dispatch_client.service_failed(task, &key, error).await.unwrap();
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
            debug!("result: {result:?}");
            let result_key: String = instructions
                .get("result_key")
                .and_then(|x|x.as_str())
                .map(|x|x.to_string())
                .unwrap_or_else(|| thread_rng().gen::<u64>().to_string());
            self.dispatch_client.service_finished(task, result_key, result, None, None).await.unwrap();
        }
    }
}

// class CoreSession:
//     def __init__(self, config, ingest):
//         self.ds: typing.Optional[AssemblylineDatastore] = None
//         self.filestore = None
//         self.redis = None
//         self.config: Config = config
//         self.ingest: Ingester = ingest
//         self.dispatcher: Dispatcher

//     @property
//     def ingest_queue(self):
//         return self.ingest.ingest_queue


// @pytest.fixture(autouse=True)
// def log_config(caplog):
//     caplog.set_level(logging.INFO, logger='assemblyline')


// class MetricsCounter:
//     def __init__(self, redis):
//         self.redis = redis
//         self.channel = None
//         self.data = {}

//     def clear(self):
//         self.data = {}

//     def sync_messages(self):
//         read = 0
//         for metric_message in self.channel.listen(blocking=False):
//             if metric_message is None:
//                 break
//             read += 1
//             try:
//                 existing = self.data[metric_message['type']]
//             except KeyError:
//                 existing = self.data[metric_message['type']] = {}

//             for key, value in metric_message.items():
//                 if isinstance(value, (int, float)):
//                     existing[key] = existing.get(key, 0) + value
//         return read

//     def __enter__(self):
//         self.channel = forge.get_metrics_sink(self.redis)
//         self.sync_messages()
//         return self

//     def __exit__(self, exc_type, exc_val, exc_tb):
//         self.sync_messages()
//         for key in list(self.data.keys()):
//             shortened = {k: v for k, v in self.data[key].items() if v > 0}
//             if shortened:
//                 self.data[key] = shortened
//             else:
//                 del self.data[key]
//         self.channel.close()

//         print("Metrics During Test")
//         for key, value in self.data.items():
//             print(key, value)

//     def expect(self, channel, name, value):
//         start_time = time.time()
//         while time.time() - start_time < RESPONSE_TIMEOUT:
//             if channel in self.data:
//                 if self.data[channel].get(name, 0) >= value:
//                     # self.data[channel][name] -= value
//                     return

//             if self.sync_messages() == 0:
//                 time.sleep(0.1)
//                 continue
//         pytest.fail(f"Did not get expected metric {name}={value} on metrics channel {channel}")


// @pytest.fixture(scope='function')
// def metrics(redis):
//     with MetricsCounter(redis) as counter:
//         yield counter




fn test_services() -> HashMap<String, Service> {
    return [
        ("pre", dummy_service("pre", "pre", None, None, None, None)),
        ("core-a", dummy_service("core-a", "core", None, None, None, None)),
        ("core-b", dummy_service("core-b", "core", None, None, None, None)),
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
    services: HashMap<String, Arc<MockService>>,
}

/// MARK: setup
async fn setup() -> TestContext {
    std::env::set_var("BIND_ADDRESS", "0.0.0.0:0");
    let (core, guard) = setup_services(test_services()).await;

    let signal = Arc::new(Notify::new());
    let mut components = tokio::task::JoinSet::new();

    // Register services
    let mut services = HashMap::new();
    let stages = core.services.get_service_stage_hash();
    for (name, _service) in test_services() {
        // ds.service.save(f'{svc}_0', dummy_service(svc, stage, docid=f'{svc}_0'))
        // ds.service_delta.save(svc, ServiceDelta({
        //     'name': svc,
        //     'version': '0',
        //     'enabled': True
        // }))
        stages.set(&name, &ServiceStage::Running).await.unwrap();
        let service_agent = MockService::new(&name, signal.clone(), &core).await;
        components.spawn(service_agent.clone().run());
        services.insert(name, service_agent);
    }

    // setup test user
    let user: User = Default::default();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    core.datastore.user.commit(None).await.unwrap();

    // launch the ingester
    let ingester = Arc::new(Ingester::new(core.clone()).await.unwrap());
    ingester.start(&mut components).await.unwrap();

    // launch the dispatcher
    let bind_address = "0.0.0.0:0".parse().unwrap();
    let tls_config = crate::config::TLSConfig::load().await.unwrap();
    let tcp = crate::http::create_tls_binding(bind_address, tls_config).await.unwrap();
    let dispatcher = Dispatcher::new(core.clone(), tcp).await.unwrap();
    dispatcher.start(&mut components);

    // launch the plumber
    let plumber_name = format!("plumber{}", thread_rng().gen::<u32>());
    let plumber = Plumber::new(core.clone(), Some(Duration::from_secs(2)), Some(&plumber_name)).await.unwrap();
    plumber.start(&mut components);

    TestContext {
        metrics: MetricsWatcher::new(core.redis_metrics.subscribe(METRICS_CHANNEL.to_owned())),
        ingest_queue: core.redis_persistant.queue(INGEST_QUEUE_NAME.to_owned(), None),
        core,
        guard,
        dispatcher,
        ingester,
        components,
        signal,
        services
    }
}


async fn ready_body(core: &Core, mut body: serde_json::Value) -> (Sha256, usize) {
    let body = {
        let out = body.as_object_mut().unwrap();
        out.insert("salt".to_owned(), json!(thread_rng().gen::<u64>().to_string()));
        serde_json::to_string(&body).unwrap()
    };

    let mut hasher = sha2::Sha256::default();
    hasher.update(body.as_bytes());
    let sha256 = Sha256::try_from(hasher.finalize().as_slice()).unwrap();
    core.filestore.put(&sha256, body.as_bytes()).await.unwrap();

    let temporary_file = tempfile::NamedTempFile::new().unwrap();
    tokio::fs::write(temporary_file.path(), body.as_bytes()).await.unwrap();
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
            if start.elapsed() > std::time::Duration::from_secs(20) {
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
            sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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
    todo!()
//     let context = setup().await;
//     let (sha, size) = ready_body(&context.core, json!({})).await;

//     // original_retry_delay = assemblyline_core.ingester.ingester._retry_delay
//     // assemblyline_core.ingester.ingester._retry_delay = 1

//     let attempts = vec![];
//     let failures = vec![];
//     original_submit = core.ingest.submit

//     def fail_once(task):
//         attempts.append(task)
//         if len(attempts) > 1:
//             original_submit(task)
//         else:
//             failures.append(task)
//             raise ValueError()
//     core.ingest.submit = fail_once

//     context.ingest_queue.push(&MessageSubmission {
//         sid: thread_rng().gen(),
//         metadata: Default::default(),
//         params: SubmissionParams::new(ClassificationString::unrestricted(&context.core.classification_parser))
//             .set_description("file abc123")
//             .set_services_selected(&[])
//             .set_submitter("user")
//             .set_groups(&["user"])
//         notification: Notification {
//             queue: Some("output-queue-one".to_string()),
//             threshold: None,
//         },
//         files: vec![File {
//             sha256: sha.clone(),
//             size: Some(size as u64),
//             name: "abc123".to_string()
//         }],
//         time: chrono::Utc::now(),
//         scan_key: None,
//     }).await.unwrap();

//     let notification_queue = context.core.notification_queue("output-queue-one");
//     let first_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

//     // One of the submission will get processed fully
//     let first_submission: Submission = context.core.datastore.submission.get(first_task.submission.sid, None).await.unwrap().unwrap();
//     assert len(attempts) == 2
//     assert len(failures) == 1
//     assert first_submission.state == 'completed'
//     assert len(first_submission.files) == 1
//     assert len(first_submission.errors) == 0
//     assert len(first_submission.results) == 4

//     // metrics.expect('ingester', 'duplicates', 0)
//     context.metrics.assert_metrics("ingester", &[("submissions_ingested", 1), ("submissions_completed", 1), ("files_completed", 1)]).await;
//     context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;

//     todo!()
}

/// MARK: ingest timeout
#[tokio::test(flavor = "multi_thread")]
async fn test_ingest_timeout() {
    todo!()
//     # -------------------------------------------------------------------------------
//     #
//     sha, size = ready_body(core)
//     original_max_time = assemblyline_core.ingester.ingester._max_time
//     assemblyline_core.ingester.ingester._max_time = 1

//     attempts = []
//     original_submit = core.ingest.submit_client.submit

//     def _fail(**args):
//         attempts.append(args)
//     core.ingest.submit_client.submit = _fail

//     try:
//         si = SubmissionInput(dict(
//             metadata={},
//             params=dict(
//                 description="file abc123",
//                 services=dict(selected=''),
//                 submitter='user',
//                 groups=['user'],
//             ),
//             notification=dict(
//                 queue='ingest-timeout',
//                 threshold=0
//             ),
//             files=[dict(
//                 sha256=sha,
//                 size=size,
//                 name='abc123'
//             )]
//         ))
//         core.ingest_queue.push(si.as_primitives())

//         sha256 = si.files[0].sha256
//         scan_key = si.params.create_filescore_key(sha256)

//         # Make sure the scanning table has been cleared
//         time.sleep(0.5)
//         for _ in range(60):
//             if not core.ingest.scanning.exists(scan_key):
//                 break
//             time.sleep(0.1)
//         assert not core.ingest.scanning.exists(scan_key)
//         assert len(attempts) == 1

//         # Wait until we get feedback from the metrics channel
//         metrics.expect('ingester', 'submissions_ingested', 1)
//         metrics.expect('ingester', 'timed_out', 1)

//     finally:
//         core.ingest.submit_client.submit = original_submit
//         assemblyline_core.ingester.ingester._max_time = original_max_time
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
        sid: thread_rng().gen(),
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
    assert_eq!(*context.services.get("pre").unwrap().drops.lock().get(&sha.to_string()).unwrap(), 1);
    assert_eq!(*context.services.get("pre").unwrap().hits.lock().get(&sha.to_string()).unwrap(), 2);

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
        sid: thread_rng().gen(),
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
    assert_eq!(*context.services.get("pre").unwrap().drops.lock().get(&sha.to_string()).unwrap(), 3);
    assert_eq!(*context.services.get("pre").unwrap().hits.lock().get(&sha.to_string()).unwrap(), 3);

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
        sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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
            sid: thread_rng().gen(),
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
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"hold": 60}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: thread_rng().gen(),
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
    let service_queue = context.core.get_service_queue("pre");

    let start = std::time::Instant::now();
    while service_queue.length().await.unwrap() < 1 {
        if start.elapsed() > RESPONSE_TIMEOUT {
            panic!();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut service_delta = context.core.datastore.service_delta.get("pre", None).await.unwrap().unwrap();
    service_delta.enabled = Some(false);
    context.core.datastore.service_delta.save("pre", &service_delta, None, None).await.unwrap();
    context.core.redis_volatile.publish("changes.services.pre", &serde_json::to_vec(&ServiceChange {
        name: "pre".to_owned(),
        operation: assemblyline_models::messages::changes::Operation::Modified
    }).unwrap()).await.unwrap();

    let notification_queue = context.core.notification_queue("test_plumber_clearing");
    let dropped_task = notification_queue.pop_timeout(RESPONSE_TIMEOUT).await.unwrap().unwrap();

    let sub = context.core.datastore.submission.get(&dropped_task.submission.sid.to_string(), None).await.unwrap().unwrap();
    assert_eq!(sub.files.len(), 1);
    assert_eq!(sub.results.len(), 3);
    assert_eq!(sub.errors.len(), 1);
    let error = context.core.datastore.error.get(&sub.errors[0], None).await.unwrap().unwrap();
    assert!(error.response.message.contains("disabled"));

    context.metrics.assert_metrics("ingester", &[("submissions_completed", 1)]).await;
    context.metrics.assert_metrics("dispatcher", &[("submissions_completed", 1), ("files_completed", 1)]).await;
    context.metrics.assert_metrics("service", &[("fail_recoverable", 1)]).await;
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
        sid: thread_rng().gen(),
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
        sid: thread_rng().gen(),
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

    let alert = context.dispatcher.postprocess_worker.alert_queue.pop_timeout(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(alert.as_object().unwrap().get("submission").unwrap().as_object().unwrap().get("sid").unwrap().as_str().unwrap(), sub.sid.to_string())
}


// MARK: partial
#[tokio::test(flavor = "multi_thread")]
async fn test_partial() {
    todo!();
    let context = setup().await;
    // Have pre produce a partial result, then have core-a update a monitored key
    let (sha, size) = ready_body(&context.core, json!({
        "pre": {"partial": {"passwords": "test_temp_data_monitoring"}}
    })).await;

    context.ingest_queue.push(&MessageSubmission {
        sid: thread_rng().gen(),
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

    // notification_queue = NamedQueue('nq-temp-data-monitor', core.redis)
    // dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    // assert dropped_task
    // dropped_task = IngestTask(dropped_task)
    // sub: Submission = core.ds.submission.get(dropped_task.submission.sid)
    // assert len(sub.errors) == 0
    // assert len(sub.results) == 4, 'results'
    // assert core.pre_service.hits[sha] == 1, 'pre_service.hits'

    // # Wait until we get feedback from the metrics channel
    // metrics.expect('ingester', 'submissions_ingested', 1)
    // metrics.expect('ingester', 'submissions_completed', 1)
    // metrics.expect('dispatcher', 'submissions_completed', 1)
    // metrics.expect('dispatcher', 'files_completed', 1)

    // partial_results = 0
    // for res in sub.results:
    //     result = core.ds.get_single_result(res, as_obj=True)
    //     assert result is not None, res
    //     if result.partial:
    //         partial_results += 1
    // assert partial_results == 1, 'partial_results'
}

// MARK: monitoring
#[tokio::test(flavor = "multi_thread")]
async fn test_temp_data_monitoring() {
    todo!()
    // # Have pre produce a partial result, then have core-a update a monitored key
    // sha, size = ready_body(core, {
    //     'pre': {'partial': {'passwords': 'test_temp_data_monitoring'}},
    //     'core-a': {'temporary_data': {'passwords': ['test_temp_data_monitoring']}},
    //     'final': {'temporary_data': {'passwords': ['some other password']}},
    // })

    // core.ingest_queue.push(SubmissionInput(dict(
    //     metadata={},
    //     params=dict(
    //         description="file abc123",
    //         services=dict(selected=[]),
    //         submitter='user',
    //         groups=['user'],
    //         max_extracted=10000
    //     ),
    //     notification=dict(
    //         queue='temp-data-monitor',
    //         threshold=0
    //     ),
    //     files=[dict(
    //         sha256=sha,
    //         size=size,
    //         name='abc123'
    //     )]
    // )).as_primitives())

    // notification_queue = NamedQueue('nq-temp-data-monitor', core.redis)
    // dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    // assert dropped_task
    // dropped_task = IngestTask(dropped_task)
    // sub: Submission = core.ds.submission.get(dropped_task.submission.sid)
    // assert len(sub.errors) == 0
    // assert len(sub.results) == 4, 'results'
    // assert core.pre_service.hits[sha] >= 2, f'pre_service.hits {core.pre_service.hits}'

    // # Wait until we get feedback from the metrics channel
    // metrics.expect('ingester', 'submissions_ingested', 1)
    // metrics.expect('ingester', 'submissions_completed', 1)
    // metrics.expect('dispatcher', 'submissions_completed', 1)
    // metrics.expect('dispatcher', 'files_completed', 1)

    // partial_results = 0
    // for res in sub.results:
    //     result = core.ds.get_single_result(res, as_obj=True)
    //     assert result is not None, res
    //     if result.partial:
    //         partial_results += 1
    // assert partial_results == 0, 'partial_results'
}

// MARK: tag filter
#[tokio::test(flavor = "multi_thread")]
async fn test_complex_extracted() {
    todo!();
    // # stages to this processing when everything goes well
    // # 1. extract a file that will process to produce a partial result
    // # 2. hold a few seconds on the second stage of the root file to let child start
    // # 3. on the last stage of the root file produce the password
    // dispatcher.TIMEOUT_EXTRA_TIME = 10

    // child_sha, _ = ready_body(core, {
    //     'pre': {'partial': {'passwords': 'test_temp_data_monitoring'}},
    // })

    // sha, size = ready_body(core, {
    //     'pre': {
    //         'response': {
    //             'extracted': [{
    //                 'name': child_sha,
    //                 'sha256': child_sha,
    //                 'description': 'abc',
    //                 'classification': 'U'
    //             }]
    //         }
    //     },
    //     'core-a': {'lock': 5},
    //     'finish': {'temporary_data': {'passwords': ['test_temp_data_monitoring']}},
    // })

    // core.ingest_queue.push(SubmissionInput(dict(
    //     metadata={},
    //     params=dict(
    //         description="file abc123",
    //         services=dict(selected=''),
    //         submitter='user',
    //         groups=['user'],
    //         max_extracted=10000
    //     ),
    //     notification=dict(
    //         queue='complex-extracted-file',
    //         threshold=0
    //     ),
    //     files=[dict(
    //         sha256=sha,
    //         size=size,
    //         name='abc123'
    //     )]
    // )).as_primitives())

    // # Wait for the extract file to finish
    // metrics.expect('dispatcher', 'files_completed', 1)
    // _global_semaphore.release()

    // # Wait for the entire submission to finish
    // notification_queue = NamedQueue('nq-complex-extracted-file', core.redis)
    // dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    // assert dropped_task
    // dropped_task = IngestTask(dropped_task)
    // sub: Submission = core.ds.submission.get(dropped_task.submission.sid)
    // assert len(sub.errors) == 0
    // assert len(sub.results) == 8, 'results'
    // assert core.pre_service.hits[sha] == 1, 'pre_service.hits[root]'
    // assert core.pre_service.hits[child_sha] >= 2, 'pre_service.hits[child]'

    // # Wait until we get feedback from the metrics channel
    // metrics.expect('ingester', 'submissions_ingested', 1)
    // metrics.expect('ingester', 'submissions_completed', 1)
    // metrics.expect('dispatcher', 'submissions_completed', 1)
    // metrics.expect('dispatcher', 'files_completed', 2)

    // partial_results = 0
    // for res in sub.results:
    //     result = core.ds.get_single_result(res, as_obj=True)
    //     assert result is not None, res
    //     if result.partial:
    //         partial_results += 1
    // assert partial_results == 0, 'partial_results'
}