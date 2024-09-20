//! Code for consistent interface with the dispatcher from other components.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};

// from typing import Optional, Any, cast

// from assemblyline.common import forge
// from assemblyline.common.constants import DISPATCH_RUNNING_TASK_HASH, SUBMISSION_QUEUE, \
//     make_watcher_list_name, DISPATCH_TASK_HASH
// from assemblyline.common.forge import CachedObject, get_service_queue
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.datastore.exceptions import VersionConflictException
// from assemblyline.odm.base import DATEFORMAT
// from assemblyline.odm.messages.dispatching import DispatcherCommandMessage, CREATE_WATCH, \
//     CreateWatch, LIST_OUTSTANDING, ListOutstanding, UPDATE_BAD_SID
// from assemblyline.odm.models.error import Error
// from assemblyline.odm.models.file import File
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.service import Service
// from assemblyline.odm.models.submission import Submission
// from assemblyline.remote.datatypes import get_client, reply_queue_name
// from assemblyline.remote.datatypes.hash import ExpiringHash, Hash
// from assemblyline.remote.datatypes.queues.named import NamedQueue
// from assemblyline.remote.datatypes.set import ExpiringSet, Set
// from assemblyline_core.dispatching.dispatcher import DISPATCH_START_EVENTS, DISPATCH_RESULT_QUEUE, \
//     DISPATCH_COMMAND_QUEUE, QUEUE_EXPIRY, BAD_SID_HASH, ServiceTask, Dispatcher


// MAX_CANCEL_RESPONSE_WAIT = 10
const UPDATE_INTERVAL: chrono::TimeDelta = chrono::TimeDelta::seconds(120);

// def weak_lru(maxsize=128, typed=False):
//     'LRU Cache decorator that keeps a weak reference to "self"'
//     def wrapper(func):

//         @functools.lru_cache(maxsize, typed)
//         def _func(_self, *args, **kwargs):
//             return func(_self(), *args, **kwargs)

//         @functools.wraps(func)
//         def inner(self, *args, **kwargs):
//             return _func(weakref.ref(self), *args, **kwargs)

//         return inner

//     return wrapper


// class RetryRequestWork(Exception):
//     pass

use assemblyline_models::datastore::{error, result, EmptyResult, Error};
use assemblyline_models::messages::dispatching::{SubmissionDispatchMessage, WatchQueueMessage};
use assemblyline_models::messages::task::{ResultSummary, ServiceError, ServiceResult, Task as ServiceTask};
use assemblyline_models::{JsonMap, Sid};
use log::{debug, error, info};
use serde_json::json;
use tokio::sync::Mutex;

use crate::constants::{make_watcher_list_name, service_queue_name, SUBMISSION_QUEUE};
use crate::elastic::{Elastic, Version};
use crate::Core;

use super::http::BasicStatus;
use super::ServiceStartMessage;


pub struct DispatchClient {
    datastore: Arc<Elastic>,
    redis_volatile: Arc<redis_objects::RedisObjects>,

    submission_queue: redis_objects::Queue<SubmissionDispatchMessage>,
    dispatcher_table: redis_objects::Hashmap<i64>,
    dispatcher_data: Mutex<DispatcherList>,

    http_client: reqwest::Client,
    emptyresult_dtl: chrono::TimeDelta,
    // running_tasks: redis_objects::Hashmap<ServiceTask>,

}

#[derive(Default)]
struct DispatcherList {
    last_update: chrono::DateTime<chrono::Utc>,
    alive: HashSet<String>,
    dead: HashSet<String>,
}

impl DispatchClient {
    pub async fn new_from_core(core: &Core) -> Result<Self> {
//     def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
//         self.config = forge.get_config()

//         self.redis = redis or get_client(
//             host=self.config.core.redis.nonpersistent.host,
//             port=self.config.core.redis.nonpersistent.port,
//             private=False,
//         )

//         self.redis_persist = redis_persist or get_client(
//             host=self.config.core.redis.persistent.host,
//             port=self.config.core.redis.persistent.port,
//             private=False,
//         )

//         self.ds = datastore or forge.get_datastore(self.config)
//         self.log = logger or logging.getLogger("assemblyline.dispatching.client")
//         self.results = self.ds.result
//         self.errors = self.ds.error
//         self.files = self.ds.file
//         self.submission_assignments = ExpiringHash(DISPATCH_TASK_HASH, host=self.redis_persist)
//         self.bad_sids = Set(BAD_SID_HASH, host=self.redis_persist)
//         self.service_data = cast(dict[str, Service], CachedObject(self._get_services))
//         self.dead_dispatchers = []

        let mut http_client = reqwest::Client::builder();
        match crate::config::get_cluster_ca_cert().await? {
            Some(cert) => {
                let cert = reqwest::Certificate::from_pem(cert.as_bytes())?;
                http_client = http_client.add_root_certificate(cert);
            }
            None => http_client = http_client.danger_accept_invalid_certs(true),
        }

        Ok(Self {
            datastore: core.datastore.clone(), 
            submission_queue: core.redis_volatile.queue(SUBMISSION_QUEUE.to_owned(), None),
            dispatcher_table: core.dispatcher_instances_table(),
            dispatcher_data: Mutex::new(Default::default()),
            redis_volatile: core.redis_volatile.clone(),
            http_client: http_client.build()?,
            emptyresult_dtl: chrono::TimeDelta::days(core.config.submission.emptyresult_dtl.into()),
            // running_tasks: core.redis_volatile.hashmap(DISPATCH_RUNNING_TASK_HASH.to_owned(), None),
        })
    }



//     @weak_lru(maxsize=128)
//     def _get_queue_from_cache(self, name):
//         return NamedQueue(name, host=self.redis, ttl=QUEUE_EXPIRY)

//     def _get_services(self):
//         # noinspection PyUnresolvedReferences
//         return {x.name: x for x in self.ds.list_all_services(full=True)}

    async fn is_dispatcher(&self, dispatcher_id: &str) -> Result<bool> {
        let mut dispatchers = self.dispatcher_data.lock().await;
        if dispatchers.dead.contains(dispatcher_id) {
            return Ok(false);
        }

        let data_stale = chrono::Utc::now() - dispatchers.last_update > UPDATE_INTERVAL;
        if data_stale || !dispatchers.alive.contains(dispatcher_id) {
            dispatchers.alive = self.dispatcher_table.keys().await?.into_iter().collect();
            dispatchers.last_update = chrono::Utc::now();
            debug!("dispatch_client refresh dispatcher list: {:?}", dispatchers.alive);
        }

        Ok(if dispatchers.alive.contains(dispatcher_id) {
            true
        } else {
            dispatchers.dead.insert(dispatcher_id.to_owned());
            false
        })
    }

    /// Insert a submission, potentially with other components, into the dispatching system.
    ///
    /// Prerequsits:
    ///     - submission and all other referenced objects should already be saved in the datastore
    ///     - files should already be in the datastore and filestore
    pub async fn dispatch_bundle(&self, message: &SubmissionDispatchMessage) -> Result<()> {
        self.submission_queue.push(message).await?; Ok(())
    }

//     def cancel_submission(self, sid):
//         """
//         If the submission is running make sure it is saved with the to_be_deleted flag set.
//         """
//         # Mark the sid as bad
//         self.bad_sids.add(sid)

//         # Tell all the known dispatchers that they need to update their bad list
//         queue_name = reply_queue_name(prefix="D", suffix="ResponseQueue")
//         queue: NamedQueue[dict[str, int]] = NamedQueue(queue_name, host=self.redis, ttl=30)
//         listed_dispatchers = set()

//         for dispatcher_id in Dispatcher.all_instances(self.redis_persist):
//             listed_dispatchers.add(dispatcher_id)
//             command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+dispatcher_id, ttl=QUEUE_EXPIRY, host=self.redis)
//             command_queue.push(DispatcherCommandMessage({
//                 'kind': UPDATE_BAD_SID,
//                 'payload_data': queue_name
//             }).as_primitives())

//         # Wait to hear back from the dispatchers that they have processed the changes to the bad sid list
//         wait_start_time = time.time()
//         while listed_dispatchers and time.time() - wait_start_time > MAX_CANCEL_RESPONSE_WAIT:
//             dispatcher_id = queue.pop(timeout=5)
//             listed_dispatchers.discard(dispatcher_id)

//     def queued_submissions(self) -> list[dict]:
//         return self.submission_queue.content()

//     def outstanding_services(self, sid) -> Optional[dict[str, int]]:
//         """
//         List outstanding services for a given submission and the number of file each
//         of them still have to process.

//         :param sid: Submission ID
//         :return: Dictionary of services and number of files
//                  remaining per services e.g. {"SERVICE_NAME": 1, ... }
//         """
//         dispatcher_id = self.submission_assignments.get(sid)
//         if dispatcher_id:
//             queue_name = reply_queue_name(prefix="D", suffix="ResponseQueue")
//             queue: NamedQueue[dict[str, int]] = NamedQueue(queue_name, host=self.redis, ttl=30)
//             command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+dispatcher_id, ttl=QUEUE_EXPIRY, host=self.redis)
//             command_queue.push(DispatcherCommandMessage({
//                 'kind': LIST_OUTSTANDING,
//                 'payload_data': ListOutstanding({
//                     'response_queue': queue_name,
//                     'submission': sid
//                 })
//             }).as_primitives())
//             return queue.pop(timeout=5)
//         return {}

    // Pull work from the service queue for the service in question.
    //
    // :param worker_id:
    // :param service_name: Which service needs work.
    // :param service_version: The version of the service that needs work
    // :param timeout: How many seconds to block before returning if blocking is true.
    // :param blocking: Whether to wait for jobs to enter the queue, or if false, return immediately
    // :return: The job found, and a boolean value indicating if this is the first time this task has
    //         been returned by request_work.
    pub async fn request_work(&self, worker_id: &str, service_name: &str, service_version: &str, timeout: Option<Duration>, blocking: bool, low_priority: Option<bool>) -> Result<Option<ServiceTask>> {
        let timeout = timeout.unwrap_or_else(|| Duration::from_secs(60));
        let low_priority = low_priority.unwrap_or_default();
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            let remaining = timeout.saturating_sub(start.elapsed());
            let work = self._request_work(worker_id, service_name, service_version, remaining, blocking, low_priority).await?;
            if blocking || work.is_some() {
                return Ok(work);
            }
        }
        return Ok(None)
    }

    async fn _request_work(&self, worker_id: &str, service_name: &str, service_version: &str,
                      timeout: Duration, blocking: bool, low_priority: bool) -> Result<Option<ServiceTask>> 
    {
        // For when we repeatedly retry on bad task dequeue-ing
        if timeout.is_zero() {
            info!("{service_name}:{worker_id} no task returned [timeout]");
            return Ok(None)
        }

        // Get work from the queue
        let work_queue = self.redis_volatile.priority_queue::<ServiceTask>(service_queue_name(service_name));
        let result = if blocking {
            work_queue.blocking_pop(timeout, low_priority).await?
        } else if low_priority {
            work_queue.unpush(1).await?.pop()
        } else {
            work_queue.pop(1).await?.pop()
        };

        let mut task = match result {
            Some(task) => task,
            None => {
                info!("{service_name}:{worker_id} no task returned: [empty message]");
                return Ok(None)
            }
        };
        task.metadata.insert("worker__".to_string(), json!(worker_id));

        let url = format!("https://{}/start", task.dispatcher_address);
        let message = ServiceStartMessage {
            sid: task.sid,
            sha: task.fileinfo.sha256.clone(),
            service_name: task.service_name.clone(),
            worker_id: worker_id.to_string(),
            task_id: Some(task.task_id),
        };

        loop {
            // Let the dispatcher know we want to start this task
            let response = self.http_client.post(&url).json(&message).send().await;

            match response {
                // if we got a complete response of any kind, treat
                Ok(response) => if response.status().is_success() {
                    return Ok(Some(task))
                } else {
                    return Ok(None)
                },
                Err(err) => {
                    error!("Error reaching dispatcher: {err:?}");
                    if !self.is_dispatcher(&task.dispatcher).await? {
                        info!("{service_name}:{worker_id} no task returned: [task from dead dispatcher]");
                        return Ok(None)
                    } else {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
            }
        }
        // let resp = match .send().await {
        //     Ok(resp) => resp,
        //     Err(err) => {
        //         info!("Couldn't reach dispatcher [{}] at [{}] {err}", task.dispatcher, task.dispatcher_address);
        //         return false;
        //     }
        // };
        // let resp: BasicStatus = match resp.json().await {
        //     Ok(resp) => resp,
        //     Err(err) => {
        //         info!("Couldn't understand dispatcher response [{}] at [{}] {err}", task.dispatcher, task.dispatcher_address);
        //         return false;
        //     }
        // };
        // resp.instance_id == task.dispatcher



        // if self.running_tasks.add(&task.key(), &task).await? {
        //     info!("[{}/{}] {service_name}:{worker_id} task found", task.sid, task.fileinfo.sha256);
        //     start_queue = self._get_queue_from_cache(DISPATCH_START_EVENTS + dispatcher);
        //     start_queue.push((task.sid, task.fileinfo.sha256, service_name, worker_id));
        //     return Ok(Some(task))
        // }
        // return Ok(None)
    }

    /// Notifies the dispatcher of service completion, and possible new files to dispatch.
    pub async fn service_finished(&self, mut task: ServiceTask, mut result_key: String, mut result: result::Result, temporary_data: Option<JsonMap>, version: Option<Version>) -> Result<()> {
        let mut version = Some(version.unwrap_or(Version::Create));

        // Make sure the dispatcher knows we were working on this task
        // task_key = ServiceTask.make_key(sid=sid, service_name=result.response.service_name, sha=result.sha256)
        // task = self.running_tasks.pop(task_key)
        // if not task:
        //     self.log.warning(f"[{sid}/{result.sha256}] {result.response.service_name} could not find the specified "
        //                      f"task in its set of running tasks while processing successful results.")
        //     return
        // task = ServiceTask(task)

        // Save or freshen the result, the CONTENT of the result shouldn't change, but we need to keep the
        // most distant expiry time to prevent pulling it out from under another submission too early
        if result.is_empty() {
            let expiry_ts = chrono::Utc::now() + self.emptyresult_dtl;
            result.expiry_ts = Some(expiry_ts);
            self.datastore.emptyresult.save(&result_key, &EmptyResult { expiry_ts }, None, None).await?;
        } else {
            loop {
                let save_res = self.datastore.result.save(&result_key, &result, version, None).await;

                match save_res {
                    Ok(_) => break,
                    Err(err) if err.is_version_conflict() => {
                        info!("Retrying to save results due to version conflict: {err}");
                        // A result already exists for this key
                        // Regenerate entire result key based on result and modified task (ignore caching)
                        version = Some(Version::Create);
                        result.created = chrono::Utc::now();
                        task.ignore_cache = true;
                        result_key = result.build_key(Some(&task))?;
                    }
                    Err(err) => {
                        return Err(err.into())
                    }
                }    
            }
        }

        // Send the result key to any watching systems
        let msg = WatchQueueMessage::ok(result_key.to_owned());
        for w in self._watcher_list(task.sid).members().await? {
            self.redis_volatile.queue(w, None).push(&msg).await?;
        }    

        // Save the tags and their score
        let tags = result.scored_tag_dict()?;

        // Pull out file names if we have them
        let mut file_names = HashMap::new();
        let mut dynamic_recursion_bypass = vec![];
        let mut children = vec![];
        for extracted_data in result.response.extracted {
            if !extracted_data.name.is_empty() {
                file_names.insert(extracted_data.sha256.clone(), extracted_data.name);
            }
            if extracted_data.allow_dynamic_recursion {
                dynamic_recursion_bypass.push(extracted_data.sha256.clone());
            }
            children.push((extracted_data.sha256, extracted_data.parent_relation));
        }

        // prepare report for the server
        let url = format!("https://{}/result", task.dispatcher_address);
        let dispatcher = task.dispatcher.to_string();
        let message = ServiceResult {
            dynamic_recursion_bypass,
            sid: task.sid,
            sha256: result.sha256,
            service_name: task.service_name,
            service_version: result.response.service_version,
            service_tool_version: result.response.service_tool_version,
            expiry_ts: result.expiry_ts,
            result_summary: ResultSummary{
                key: result_key,
                drop: result.drop_file,
                score: result.result.score,
                children,
            },
            tags,
            extracted_names: file_names,
            temporary_data: temporary_data.unwrap_or_default()
        };

        loop {
            // Let the dispatcher know we failed this task
            let response = self.http_client.post(&url).json(&message).send().await;

            match response {
                // if we got a complete response of any kind, treat
                Ok(response) => if response.status().is_success() {
                    return Ok(())
                } else {
                    bail!("Error refused by dispatcher");
                },
                Err(err) => {
                    error!("Error reaching dispatcher: {err}");
                    if !self.is_dispatcher(&dispatcher).await? {
                        return Ok(())
                    } else {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
            }
        }
    }   

    pub async fn service_failed(&self, task: ServiceTask, error_key: &str, error: Error) -> Result<()> {
        // task_key = ServiceTask.make_key(sid=sid, service_name=error.response.service_name, sha=error.sha256)
        // task = self.running_tasks.pop(task_key)
        // if not task {
        //     self.log.warning(f"[{sid}/{error.sha256}] {error.response.service_name} could not find the specified "
        //                      f"task in its set of running tasks while processing an error.")
        //     return
        // }
        // task = ServiceTask(task)

        debug!("[{}/{}] {} Failed with {} error.", task.sid, error.sha256, task.service_name, error.response.status);
        if error.response.status.is_nonrecoverable() {
            // This is a NON_RECOVERABLE error, error will be saved and transmitted to the user
            self.datastore.error.save(error_key, &error, None, None).await?;

            // Send the result key to any watching systems
            let msg = WatchQueueMessage::fail(error_key.to_owned());
            for w in self._watcher_list(task.sid).members().await? {
                self.redis_volatile.queue(w, None).push(&msg).await?;
            }    
        }

        // dispatcher = task.metadata['dispatcher__']
        // result_queue = self._get_queue_from_cache(DISPATCH_RESULT_QUEUE + dispatcher)
        // result_queue.push({

        // })
        let dispatcher = task.dispatcher.clone();
        let url = format!("https://{}/error", task.dispatcher_address);
        let message = ServiceError {
            sid: task.sid,
            service_task: task,
            error,
            error_key: error_key.to_owned()
        };

        loop {
            // Let the dispatcher know we failed this task
            let response = self.http_client
                .post(&url)
                .json(&message)
                .send().await;

            match response {
                // if we got a complete response of any kind, treat
                Ok(response) => if response.status().is_success() {
                    return Ok(())
                } else {
                    bail!("Error refused by dispatcher");
                },
                Err(err) => {
                    error!("Error reaching dispatcher: {err}");
                    if !self.is_dispatcher(&dispatcher).await? {
                        return Ok(())
                    } else {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                },
            }
        }

    }

//     def setup_watch_queue(self, sid: str) -> Optional[str]:
//         """
//         This function takes a submission ID as a parameter and creates a unique queue where all service
//         result keys for that given submission will be returned to as soon as they come in.

//         If the submission is in the middle of processing, this will also send all currently received keys through
//         the specified queue so the client that requests the watch queue is up to date.

//         :param sid: Submission ID
//         :return: The name of the watch queue that was created
//         """
//         dispatcher_id = self.submission_assignments.get(sid)
//         if dispatcher_id:
//             queue_name = reply_queue_name(prefix="D", suffix="WQ")
//             command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+dispatcher_id, host=self.redis)
//             command_queue.push(DispatcherCommandMessage({
//                 'kind': CREATE_WATCH,
//                 'payload_data': CreateWatch({
//                     'queue_name': queue_name,
//                     'submission': sid
//                 })
//             }).as_primitives())
//             return queue_name
//         return None

    fn _watcher_list(&self, sid: Sid) -> redis_objects::Set<String> {
        return self.redis_volatile.expiring_set(make_watcher_list_name(sid), None)
    }

}