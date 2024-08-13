use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use assemblyline_models::datastore::Submission;
use assemblyline_models::messages::ArchiveAction;
use assemblyline_models::{JsonMap, Sid};
use bytes::Bytes;
use anyhow::Result;
use rand::{thread_rng, Rng};
use tokio::sync::mpsc;
// use anyhow::Result;
use itertools::Itertools;
use log::{info, warn, error};
use redis_objects::{Hashmap, PriorityQueue, Queue, RedisObjects};
use serde_json::json;
use tokio::sync::RwLock;

use assemblyline_models::config::{default_postprocess_actions, Config, PostprocessAction, Webhook};
use assemblyline_models::messages::submission::Submission as MessageSubmission;

use crate::archive::ArchiveManager;
use crate::constants::{ALERT_QUEUE_NAME, CONFIG_HASH_NAME, INGEST_INTERNAL_QUEUE_NAME, POST_PROCESS_CONFIG_KEY};
use crate::Core;
// use crate::datastore::Datastore;
// use crate::models::JsonValue;
// use crate::models::action::{PostProcessAction, Webhook};
// use crate::models::config::Config;
// use crate::models::submission::SID;
// use crate::redis::{StructureStore, EventWatcher, PriorityQueue, Queue, Hashmap};

mod parsing;
mod search;
#[cfg(test)]
mod tests;

pub use self::search::{Query, parse};


const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct SubmissionFilter {
    operation: Query,
    pub cache_safe: bool,
}

impl SubmissionFilter {
    fn new(expression: &str) -> Result<Self, ParsingError> {
        let operation: Query = parse(expression)?;
        Ok(Self { cache_safe: operation.cache_safe().can_run(), operation })
    }

    pub fn test(&self, data: &serde_json::Value) -> Result<bool, ParsingError> {
        self.operation.test(data)
    }
}

fn should_resubmit(score: f64, shift: f64) -> bool {
//     # Resubmit:
//     #
//     # 100%     with a score above 400.
//     # 10%      with a score of 301 to 400.
//     # 1%       with a score of 201 to 300.
//     # 0.1%     with a score of 101 to 200.
//     # 0.01%    with a score of 1 to 100.
//     # 0.001%   with a score of 0.
//     # 0%       with a score below 0.

    if score < 0.0 {
        return false
    }

    if score >= shift {
        return true
    }

    let resubmit_probability = 1.0 / 10.0f64.powf((shift - score) / 100.0);

    return rand::thread_rng().gen::<f64>() < resubmit_probability
}

pub struct ActionWorker {
    // Configuration
    config: Arc<Config>,
    running_cache_tasks: bool,
    actions: RwLock<Arc<HashMap<String, (SubmissionFilter, PostprocessAction)>>>,

    // Redis information
    config_data: Hashmap<serde_json::Value>,
    unique_queue: PriorityQueue<serde_json::Value>,
    alert_queue: Queue<serde_json::Value>,
    archive_manager: ArchiveManager
}

impl ActionWorker {
    pub async fn new(cache: bool, core: &Core) -> Result<Arc<Self>> {
        // Setup the object
        let worker = Arc::new(Self {
            config: core.config.clone(),
            running_cache_tasks: cache,
            actions: Default::default(),
            unique_queue: core.redis_persistant.priority_queue(INGEST_INTERNAL_QUEUE_NAME.to_owned()),
            alert_queue: core.redis_persistant.queue(ALERT_QUEUE_NAME.to_owned(), None),
            config_data: core.redis_persistant.hashmap(CONFIG_HASH_NAME.to_owned(), None),
            archive_manager: ArchiveManager::new(core),
        });

        // Make sure we load any changed actions
        let reload_watcher = core.redis_volatile.subscribe("system.postprocess".to_owned());
        tokio::spawn(Self::watch_actions_pubsub(reload_watcher, Arc::downgrade(&worker)));

        // Load the current actions
        worker.load_actions().await?;
        return Ok(worker)
    }

    async fn watch_actions_pubsub(mut reload_watcher: mpsc::Receiver<Option<redis_objects::Msg>>, worker: Weak<Self>) {
        loop {
            if reload_watcher.recv().await.is_none() {
                return
            }

            let worker = match worker.upgrade() {
                Some(worker) => worker,
                None => return,
            };
            if let Err(err) = worker.load_actions().await {
                error!("Error loading postprocess actions: {err}");
            }
        }
    }

    async fn load_actions(&self) -> Result<()> {
        // Load the action data from redis
        let data = self.config_data.get_raw(POST_PROCESS_CONFIG_KEY).await?;

        // If nothing is in redis, fall back to legacy storage
        // if data is None:
        //     try:
        //         with CacheStore('system', config=self.config, datastore=self.datastore) as cache:
        //             byte_data = cache.get('postprocess_actions')
        //             if byte_data:
        //                 data = byte_data.decode()
        //     except Exception:
        //         logger.warn("Couldn't access system files")

        // Decode data
        let mut objects = default_postprocess_actions();
        if let Some(data) = data {
            match serde_yaml::from_slice(&data) {
                Ok(obj) => {
                    objects = obj;
                },
                Err(err) => {
                    error!("Couldn't load stored actions: {err}")
                }
            }
        }

        // Check which ones can be active
        let mut ready_objects: HashMap<String,(SubmissionFilter, PostprocessAction)> = Default::default();
        for (key, action) in objects {
            if !action.enabled {
                continue
            }

            let fltr = match SubmissionFilter::new(&action.filter) {
                Ok(fltr) => fltr,
                Err(err) => {
                    error!("Failed to load submission filter: {err}");
                    continue    
                }
            };

            if self.running_cache_tasks && action.run_on_cache {
                if !fltr.cache_safe {
                    error!("Tried to apply non-cache-safe filter to cached submissions.");
                    continue
                }
                ready_objects.insert(key, (fltr, action));
                continue
            }

            if !self.running_cache_tasks && action.run_on_completed {
                ready_objects.insert(key, (fltr, action));
                continue
            }
        }

        // Swap in the new actions
        *self.actions.write().await = Arc::new(ready_objects);
        Ok(())
    }

    pub async fn process_cachehit(self: &Arc<Self>, submission: &MessageSubmission, score: i32, force_archive: bool) -> Result<bool> {
        // convert submission data to searchable json data
        let mut data = json!(submission);
        if let Some(data) = data.as_object_mut() {
            data.insert("tags".to_string(), serde_json::Value::Object(Default::default()));
        };

        // do the search
        self._process(submission, data, score, force_archive).await
    }

    /// Handle any postprocessing events for a submission.
    /// Return bool indicating if a resubmission action has happened.
    pub async fn process(self: &Arc<Self>, submission: &Submission, tags: serde_json::Value, force_archive: bool) -> Result<bool> {
        // Add tags to submission
        let mut data = json!(submission);
        if let Some(data) = data.as_object_mut() {
            data.insert("tags".into(), tags);
        };

        // run the post-processing
        self._process(&submission.into(), data, submission.max_score, force_archive).await
    }

    async fn _process(self: &Arc<Self>, submission: &MessageSubmission, data: serde_json::Value, score: i32, force_archive: bool) -> Result<bool> {
        let mut archive_submission = force_archive;
        let mut create_alert = false;
        let mut resubmit: Option<HashSet<String>> = None;
        let mut webhooks: HashSet<Webhook> = HashSet::new();

        let actions = self.actions.read().await.clone();
        for (fltr, action) in actions.values() {
            if !fltr.test(&data)? {
                continue
            }

            // Check if we need to launch an alert
            create_alert |= action.raise_alert;

            // Check if we need to archive the submission
            archive_submission |= action.archive_submission;

            // Accumulate resubmit services
            if let Some(action_resubmit) = &action.resubmit {
                let mut do_resubmit = true;
                if let Some(random_below) = action_resubmit.random_below {
                    do_resubmit = should_resubmit(score as f64, random_below as f64);
                }

                if do_resubmit {
                    match &mut resubmit {
                        Some(resubmit) => resubmit.extend(action_resubmit.additional_services.iter().cloned()),
                        None => resubmit = Some(action_resubmit.additional_services.iter().cloned().collect()),
                    }
                }
            }

            // Accumulate hooks
            if let Some(webhook) = &action.webhook {
                webhooks.insert(webhook.clone());
            }
        }

        // Bail early if nothing is to be done
        if resubmit.is_none() && !create_alert && webhooks.is_empty() && !archive_submission {
            return Ok(false)
        }

        // Prepare a message formatted submission
        let submission_msg: MessageSubmission = submission.clone();
        let sid = submission_msg.sid;

        // Trigger resubmit
        let mut extended_scan = if submission_msg.params.psid.is_none() {
            "skipped"
        } else {
            // We are the extended scan
            "submitted"
        };
        let mut did_resubmit = false;

        if let Some(resubmit) = resubmit {
            let selected: HashSet<String> = submission_msg.params.services.selected.iter().cloned().collect();
            let resubmit_to: HashSet<String> = resubmit.union(&submission_msg.params.services.resubmit.iter().cloned().collect()).cloned().collect();

            if !selected.is_superset(&resubmit_to) {
                let submit_to = selected.union(&resubmit_to).cloned().collect_vec();
                extended_scan = "submitted";

                info!("[{sid} :: {}] Resubmitted for extended analysis", submission_msg.files[0].sha256);
                let mut resubmission = submission_msg.clone();
                resubmission.params.psid = Some(sid);
                resubmission.sid = thread_rng().gen();
                resubmission.scan_key = None;
                resubmission.params.services.resubmit.clear();
                resubmission.params.services.selected = submit_to;

                self.unique_queue.push(submission_msg.params.priority as f64, &json!({
                    "score": score,
                    "extended_scan": extended_scan,
                    "ingest_id": submission_msg.metadata.get("ingest_id"),
                    "submission": resubmission,
                })).await?;
                did_resubmit = true;
            }
        }

        // Raise alert
        if submission_msg.params.generate_alert && create_alert {
            info!("[{sid} :: {}] Notifying alerter to create or update an alert", submission_msg.files[0].sha256);

            self.alert_queue.push(&json!({
                "submission": submission_msg,
                "score": score,
                "extended_scan": extended_scan,
                "ingest_id": submission_msg.metadata.get("ingest_id")
            })).await?;
        }

        // Archive the submission
        if archive_submission {
            if self.config.datastore.archive.enabled {
                info!("[{sid} :: {}] Evaluating if the file can be moved to the malware archive", submission_msg.files[0].sha256);

                let archive_result = self.archive_manager.archive_submission(&submission_msg, Some(submission_msg.params.delete_after_archive)).await?;
                if let Some(archive_result) = archive_result {
                    if archive_result.action == ArchiveAction::Archive {
                        info!("[{sid} :: {}] Archiver was notified to copy the file in the malware archive", submission_msg.files[0].sha256);
                    } else {
                        info!("[{sid} :: {}] The file was re-submitted for analysis because it does not meet the minimum service requirement", submission_msg.files[0].sha256);
                    }
                }
            } else {
                warn!("[{sid} :: {}] Trying to archive a submission on a system where archiving is disabled", submission_msg.files[0].sha256);
            }
        }

        // Trigger webhooks
        let payload = Bytes::from(serde_json::to_vec(&json!({
            "is_cache": self.running_cache_tasks,
            "score": score,
            "submission": submission
        }))?);
        let mut pool = tokio::task::JoinSet::new();
        for hook in webhooks.into_iter() {
            pool.spawn(self.clone().process_hook(hook, sid, payload.clone()));
        }
        loop {
            if pool.join_next().await.is_none() { break }
        }

        return Ok(did_resubmit)
    }

    async fn process_hook(self: Arc<Self>, hook: Webhook, sid: Sid, payload: Bytes) {
        let mut attempts = 0;
        let mut backoff = Duration::from_millis(5);

        loop {
            match self._process_hook(&hook, payload.clone()).await {
                Ok(()) => return,
                Err(err) => {
                    error!("[{sid}] Error in webhook call ({}) {}", hook.uri, err);
                },
            };

            attempts += 1;
            if let Some(retries) = hook.retries {
                if attempts >= retries {
                    error!("[{sid}] Retry limit reached. Failed webhook call to {}.", hook.uri);
                    return
                }
                backoff = RETRY_MAX_BACKOFF.min(backoff * 2);
                tokio::time::sleep(backoff).await;
            }
        }
    }

    async fn _process_hook(self: &Arc<Self>, hook: &Webhook, payload: Bytes) -> anyhow::Result<()> {
        let mut builder = reqwest::ClientBuilder::new();

        // Setup ssl details
        if hook.ssl_ignore_errors {
            builder = builder.danger_accept_invalid_certs(true);
            builder = builder.danger_accept_invalid_hostnames(true);
        }
        if hook.ssl_ignore_hostname {
            builder = builder.danger_accept_invalid_hostnames(true);
        }
        if let Some(ca) = &hook.ca_cert {
            let cert = reqwest::Certificate::from_pem(ca.as_bytes())?;
            builder = builder.add_root_certificate(cert);
        }

        if let Some(proxy) = &hook.proxy {
            builder = builder.proxy(reqwest::Proxy::http(proxy)?);
        }

        // finalize configuration
        let client = builder.build()?;

        // Setup other headers
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        for header in &hook.headers {
            headers.insert(HeaderName::from_str(&header.name)?, HeaderValue::from_str(header.value.as_str())?);
        }

        // Setup setup http query details
        let method = reqwest::Method::from_str(&hook.method)?;
        let mut request = client.request(method, hook.uri.clone())
            .headers(headers)
            .timeout(std::time::Duration::from_secs(60))
            .body(payload);
        if let Some(username) = &hook.username {
            request = request.basic_auth(username, hook.password.clone())
        }

        // issue request
        request.send().await?.error_for_status()?;
        Ok(())
    }
}


// #[derive(Debug, PartialEq, Eq)]
// pub enum PostprocessingError {
//     NetworkError{
//         cause: Box<redis_objects::ErrorTypes>,
//     },
//     Processing{
//         cause: Box<ParsingError>
//     }
// }

// impl std::fmt::Display for PostprocessingError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             PostprocessingError::NetworkError {  } => todo!(),
//             PostprocessingError::Processing { cause } => todo!(),
//         }
//     }
// }

// impl From<redis_objects::ErrorTypes> for PostprocessingError {
//     fn from(value: redis_objects::ErrorTypes) -> Self {
//         Self::NetworkError { cause: Box::new(value) }
//     }
// }

/// Errors related to parsing or interpreting a post processing rule
#[derive(Debug, PartialEq, Eq)]
pub enum ParsingError {
    UnknownPrefixOperator(Box<String>),
    InvalidDate(Box<String>),
    InvalidTime(Box<String>),

    CouldNotParseSubmissionFilter(String),
    CouldNotParseSubmissionFilterTrailing(String),
    SubmissionFilterUsesUnknownFields(Vec<String>),
    // AlwaysTrue(String),
}

impl ParsingError {
    pub (crate) fn invalid_date<D: Display>(input: D) -> Self {
        Self::InvalidDate(Box::new(input.to_string()))
    }
}

impl std::fmt::Display for ParsingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParsingError::UnknownPrefixOperator(value) => write!(f, "An unknown prefix operator was used: {value}"),
            ParsingError::InvalidDate(value) => write!(f, "An invalid date string was provided: {value}"),
            ParsingError::InvalidTime(value) => write!(f, "An invalid time string was provided: {value}"),
            ParsingError::CouldNotParseSubmissionFilter(value) => write!(f, "The submission filter could not be parsed: {value}"),
            ParsingError::CouldNotParseSubmissionFilterTrailing(value) => write!(f, "There was trailing data after the filter was processed: {value}"),
            ParsingError::SubmissionFilterUsesUnknownFields(value) => {
                let fields = value.join(", ");
                write!(f, "Unknown fields were used in the submission filter: {fields}")
            },
        }
    }
}

impl std::error::Error for ParsingError {}
