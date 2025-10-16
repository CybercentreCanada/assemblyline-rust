mod http;
#[cfg(test)]
mod tests;
pub mod client;

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result, anyhow};
use assemblyline_models::config::TemporaryKeyType;
use assemblyline_models::datastore::submission::{SubmissionState, TraceEvent};
use assemblyline_models::datastore::tagging::TagValue;
use assemblyline_models::datastore::user::User;
use assemblyline_models::datastore::{error, Service, Submission};
use assemblyline_models::messages::dispatching::{CreateWatch, DispatcherCommand, DispatcherCommandMessage, FileTreeData, ListOutstanding, SubmissionDispatchMessage, WatchQueueMessage, WatchQueueStatus};
use assemblyline_models::messages::submission::SubmissionMessage;
use assemblyline_models::messages::task::{DataItem, FileInfo, ResultSummary, ServiceResponse, ServiceResult, TagEntry, TagItem, Task as ServiceTask};
use assemblyline_models::messages::KillContainerCommand;
use assemblyline_models::messages::service_heartbeat::Metrics as ServiceMetrics;
use assemblyline_models::messages::dispatcher_heartbeat::Metrics;
use assemblyline_models::types::{Wildcard, ExpandingClassification, JsonMap, Sha256, Sid};
use assemblyline_models::Readable;
use itertools::Itertools;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use poem::listener::Acceptor;
use redis_objects::quota::UserQuotaTracker;
use redis_objects::increment;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{mpsc, oneshot, RwLock};
use rand::Rng;

use crate::common::metrics::CPUTracker;
use crate::constants::{make_watcher_list_name, COMPLETE_QUEUE_NAME, DISPATCH_TASK_HASH, METRICS_CHANNEL, SCALER_TIMEOUT_QUEUE, SUBMISSION_QUEUE};
use crate::elastic::Elastic;
use crate::http::TlsAcceptor;
use crate::logging::FormattedList;
use crate::postprocessing::ActionWorker;
use crate::services::{get_schedule_names, ServiceHelper};
use crate::{Core, Flag};

// APM_SPAN_TYPE = 'handle_message'

const SUBMISSION_ATTEMPT_LIMIT: usize = 10;
const AL_SHUTDOWN_QUIT: u64 = 60;
const ONE_SECOND: Duration = Duration::from_secs(1);
const ONE_HOUR: Duration = Duration::from_secs(60 * 60);
const ONE_DAY: Duration = Duration::from_secs(ONE_HOUR.as_secs() * 24);
const DEFAULT_RESULT_BATCH_SIZE: usize = 50;
const USER_CACHE_TIME: Duration = Duration::from_secs(30);

/// How long to wait after verifying that a task is enqueued before checking again.
/// This prevents the dispatcher from:
/// a) repeatedly pushing the same task into queue when service instances 
///    are available causing a fast turn around 
/// b) repeatedly polling redis about the state of a queue in quick sucession 
///    when the queue is unlikely to have changed that quickly without
///    us getting notified via a task start query
const QUEUE_CHECK_INTERVAL: Duration = Duration::from_millis(500);

// ERROR_BATCH_SIZE = int(os.environ.get('DISPATCHER_ERROR_BATCH_SIZE', '50'))


pub const ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_secs(10);

const DISPATCH_TASK_ASSIGNMENT: &str = "dispatcher-tasks-assigned-to-";
const TASK_ASSIGNMENT_PATTERN: &str = "dispatcher-tasks-assigned-to-*";
const DISPATCH_START_EVENTS: &str = "dispatcher-start-events-";
const DISPATCH_RESULT_QUEUE: &str = "dispatcher-results-";
const DISPATCH_COMMAND_QUEUE: &str = "dispatcher-commands-";
const DISPATCH_DIRECTORY: &str = "dispatchers-directory";
const DISPATCH_DIRECTORY_FINALIZE: &str = "dispatchers-directory-finalizing";
const BAD_SID_HASH: &str = "bad-sid-hash";
const QUEUE_EXPIRY: Duration = Duration::from_secs(60 * 60);
const QUOTA_TIMEOUT: Duration = Duration::from_secs(60 * 60);
// SERVICE_VERSION_EXPIRY_TIME = 30 * 60  # How old service version info can be before we ignore it
const GUARD_TIMEOUT: i64 = 60 * 2;
const GLOBAL_TASK_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 10);
const TIMEOUT_GRACE: Duration = Duration::from_secs(5);
// TIMEOUT_TEST_INTERVAL = 5
// MAX_RESULT_BUFFER = 64
// RESULT_THREADS = max(1, int(os.getenv('DISPATCHER_RESULT_THREADS', '2')))
// FINALIZE_THREADS = max(1, int(os.getenv('DISPATCHER_FINALIZE_THREADS', '2')))

// After 20 minutes, check if a submission is still making progress.
// In the case of a crash somewhere else in the system, we may not have
// gotten a message we are expecting. This should prompt a retry in most
// cases.
const SUBMISSION_TOTAL_TIMEOUT: Duration = Duration::from_secs(60 * 20);

// This is a simple macro that wraps the given method in a retry loop
macro_rules! retry {
    ($name: expr, $dispatcher: ident, $method: ident) => {
        {
            let name = $name;
            let dispatcher: Arc<Dispatcher> = $dispatcher.clone();
            async move {
                while let Err(err) = dispatcher.clone().$method().await {
                    error!("Error in {name}: {err:?}");
                    dispatcher.core.sleep(ERROR_BACKOFF).await;
                }
                info!("Stopped {name} worker");
            }
        }
    };
}

enum DispatchAction {
    Start(ServiceStartMessage, Option<oneshot::Sender<Result<()>>>),
    Result(Box<ServiceResponse>),
    Check(Sid),
    BadSid(Sid),
    DescribeStatus(Sid, redis_objects::Queue<WatchQueueMessage>),
    ListOutstanding(Sid, oneshot::Sender<HashMap<String, u64>>),
    Terminate(Sid, oneshot::Sender<()>),
    DispatchFile(Sid, Sha256),
    TestReport(Sid, oneshot::Sender<TestReport>),
}

pub struct TestReport {
    pub queue_keys: HashMap<(Sha256, String), (ServiceTask, Vec<u8>, Instant)>,
    pub service_results: HashMap<(Sha256, String), ResultSummary>,
    pub service_errors: HashMap<(Sha256, String), String>,
}

impl DispatchAction {
    fn sid(&self) -> Sid {
        match self {
            DispatchAction::Start(message, _) => message.sid,
            DispatchAction::Result(message) => message.sid(),
            DispatchAction::Check(sid) => *sid,
            DispatchAction::BadSid(sid) => *sid,
            DispatchAction::DescribeStatus(sid, _) => *sid,
            DispatchAction::ListOutstanding(sid, _) => *sid,
            DispatchAction::Terminate(sid, _) => *sid,
            DispatchAction::DispatchFile(sid, _) => *sid,
            DispatchAction::TestReport(sid, _) => *sid,
        }
    }
}


#[derive(Serialize, Deserialize)]
struct ServiceStartMessage {
    sid: Sid,
    sha: Sha256,
    service_name: String,
    worker_id: String,
    dispatcher_id: String,
    task_id: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone)]
struct AncestoryEntry {
    #[serde(rename="type")]
    file_type: String,
    parent_relation: String,
    sha256: Sha256,
}

/// Tracks whether a task needs to be rerun based on
#[derive(Debug)]
struct MonitorTask {
    /// Service name
    service: String,
    /// sha256 of file in question
    sha: Sha256,
    /// The temporary values this task was last dispatached with
    values: HashMap<String, Option<serde_json::Value>>,
    /// Should aservice be dispatched again when possible
    dispatch_needed: bool, // = dataclasses.field(default=False)
}

// struct ActiveFileInfo {
//     info: Option<Arc<FileInfo>>,
//     name: String,
//     schedule: Vec<Vec<Arc<Service>>>,
//     tags: HashMap<String, TagEntry>, // = defaultdict(dict,
//     depth: u32,
//     ancestry: Vec<Vec<AncestoryEntry>>,
//     temporary_data: JsonMap, // = defaultdict(dict,
// }


/// Dispatcher internal model for submissions
struct SubmissionTask {
    submission: Submission,
    completed_queue: Option<String>,
    service_access_control: Option<String>,
    internal_task_queue: VecDeque<DispatchAction>,

    file_info: HashMap<Sha256, Option<Arc<FileInfo>>>,
    file_names: HashMap<Sha256, String>,
    file_schedules: HashMap<Sha256, Vec<Vec<String>>>,
    file_tags: HashMap<Sha256, HashMap<String, TagEntry>>, // = defaultdict(dict),
    file_depth: HashMap<Sha256, u32>,
    file_ancestry: HashMap<Sha256, Vec<Vec<AncestoryEntry>>>,
    file_temporary_data: HashMap<Sha256, TemporaryFileData>,

    /// files that are currently actively processing
    active_files: HashSet<Sha256>,
    /// files that are not going to be processed
    dropped_files: HashSet<Sha256>,
    /// files that are exempt from recursion prevention
    dynamic_recursion_bypass: HashSet<Sha256>,
    /// services that may want to be retried with more information from the shared temporary data
    monitoring: HashMap<(Sha256, String), MonitorTask>,

    // log and error information that may be passed through to the user
    service_logs: HashMap<(Sha256, String), Vec<String>>, // = defaultdict(list),
    extra_errors: Vec<String>,

    service_results: HashMap<(Sha256, String), ResultSummary>,
    service_errors: HashMap<(Sha256, String), String>,
    service_attempts: HashMap<(Sha256, String), u32>, //] = defaultdict(int),
    running_services: HashMap<(Sha256, String), ServiceTask>,
    queue_keys: HashMap<(Sha256, String), (ServiceTask, Vec<u8>, Instant)>,

    // mapping from file hash to a set of services that shouldn't be run on
    // any children (recursively) of that file
    _forbidden_services: HashMap<Sha256, HashSet<String>>,
    _parent_map: HashMap<Sha256, HashSet<Sha256>>,
}


impl SubmissionTask {

    fn new(args: SubmissionDispatchMessage, access_control: Option<String>, scheduler: &ServiceHelper) -> Self {
        let mut out = Self {
            submission: args.submission,
            completed_queue: args.completed_queue,
            service_access_control: access_control,
            internal_task_queue: Default::default(),

            file_info: Default::default(),
            file_names: Default::default(),
            file_schedules: Default::default(),
            file_tags: Default::default(),
            file_depth: Default::default(),
            file_temporary_data: Default::default(),
            file_ancestry: Default::default(),

            extra_errors: Default::default(),
            service_logs: Default::default(),

            active_files: Default::default(),
            dropped_files: Default::default(),
            dynamic_recursion_bypass: Default::default(),
            monitoring: Default::default(),

            service_results: Default::default(),
            service_errors: Default::default(),
            service_attempts: Default::default(),
            queue_keys: Default::default(),
            running_services: Default::default(),

            _forbidden_services: Default::default(),
            _parent_map: Default::default(),
        };

        // read cached file info into local storage
        for (hash, info) in args.file_infos {
            out.file_info.insert(hash, Some(Arc::new(info)));
        }

        // read information about file tree constructed by results
        if !args.file_tree.is_empty() {
            fn recurse_tree(out: &mut SubmissionTask, tree: HashMap<Sha256, FileTreeData>, depth: u32) {
                for (sha256, file_data) in tree {

                    out.file_depth.insert(sha256.clone(), depth);
                    let file_name = file_data.name.first().cloned().unwrap_or_else(|| sha256.to_string());
                    out.file_names.insert(sha256,  file_name);

                    recurse_tree(out, file_data.children, depth + 1)
                }
            }

            recurse_tree(&mut out, args.file_tree, 0);
        }

        if !args.results.is_empty() {
            let rescan = scheduler.expand_categories(out.submission.params.services.rescan.clone());

            // Replay the process of routing files for dispatcher internal state.
            for k in args.results.keys() {
                if let [sha256, service, _] = k.splitn(3, ".").collect_vec()[..] {
                    let sha256: Sha256 = match sha256.parse() {
                        Ok(sha) => sha,
                        Err(_) => continue,
                    };

                    let service = match scheduler.get(service) {
                        Some(service) => service,
                        None => continue,
                    };

                    let prevented_services = scheduler.expand_categories(service.recursion_prevention.clone());
                    for service_name in prevented_services {
                        out.forbid_for_children(sha256.clone(), service_name)
                    }
                }
            }
        
            // Replay the process of receiving results for dispatcher internal state
            for (k, result) in args.results {
                if let [sha256, service, _] = k.splitn(3, ".").collect_vec()[..] {
                    let sha256: Sha256 = match sha256.parse() {
                        Ok(sha) => sha,
                        Err(_) => continue,
                    };

                    if let Ok(tags) = result.scored_tag_dict() {
                        for (key, tag) in tags {
                            let file_tags = out.file_tags.entry(sha256.clone()).or_default();
                            match file_tags.entry(key) {
                                std::collections::hash_map::Entry::Occupied(mut occupied_entry) => {
                                    occupied_entry.get_mut().score += tag.score;
                                },
                                std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                                    vacant_entry.insert(tag);
                                },
                            }
                        }
                    }
                    
                    let service = service.to_owned();
                    if !rescan.contains(&service) {
                        let extracted = result.response.extracted;
                        out.register_children(&sha256, extracted.iter().map(|file| file.sha256.clone()));
                        let children_detail: Vec<(Sha256, String)> = extracted.into_iter().map(|file| (file.sha256, file.parent_relation.into())).collect();
                        out.service_results.insert((sha256, service), ResultSummary {
                            key: k, 
                            drop: result.drop_file, 
                            score: result.result.score,
                            children: children_detail,
                            partial: result.partial
                        });
                    }
                }
            }
        }

        // store errors that are already part of this submission
        for e in args.errors {
            if let [sha256, service, ..] = e.splitn(3, ".").collect_vec()[..] {
                if let Ok(sha256) = sha256.parse() {
                    let service = service.to_owned();
                    out.service_errors.insert((sha256, service), e);
                }
            }
        }

        out
    }

    pub fn send_dispatch_action(&mut self, action: DispatchAction) {
        self.internal_task_queue.push_back(action);
    }

    pub fn pop_internal_task(&mut self) -> Option<DispatchAction> {
        self.internal_task_queue.pop_front()
    }

    // pub fn update_temporary_data(&mut self, sha256: &Sha256, mut input: JsonMap, max_temp_data_length: usize) -> Result<()> {
    //     // unpack the data
    //     input.retain(|key, value| {
    //         let encoded_length = match serde_json::to_string(value) {
    //             Ok(text) => text.len(),
    //             Err(_) => return false,
    //         };

    //         if encoded_length <= max_temp_data_length {
    //             true
    //         } else {
    //             warn!("[{}] discarding temporary data for {key}", self.submission.sid);
    //             false
    //         }
    //     });

    //     // update the temporary data
    //     let temp_data = self.file_temporary_data.entry(sha256.clone()).or_default();
    //     temp_data.append(&mut input);
    //     Ok(())
    // }

    /// Mark that children of a given file should not be routed to a service.
    fn forbid_for_children(&mut self, sha256: Sha256, service_name: String) {
        self._forbidden_services.entry(sha256).or_default().insert(service_name);
    }

    /// Note for the purposes of dynamic recursion prevention which files extracted other files.
    fn register_children(&mut self, parent: &Sha256, children: impl Iterator<Item=Sha256>) {
        for child in children {
            self._parent_map.entry(child).or_default().insert(parent.clone());
        }
    }

    fn all_ancestors(&self, sha256: &Sha256) -> Vec<&Sha256> {
        let mut visited = HashSet::new();
        let mut to_visit = vec![sha256];
        while let Some(current) = to_visit.pop() {
            if let Some(parents) = self._parent_map.get(current) {
                for parent in parents {
                    if !visited.contains(parent) {
                        visited.insert(parent);
                        to_visit.push(parent);
                    }
                }
            }
        }
        return visited.into_iter().collect()
    }

    // Return a list of services that should be excluded for the given file.
    //
    // Note that this is computed dynamically from the parent map every time it is
    // called. This is to account for out of order result collection in unusual
    // circumstances like replay.
    fn find_recursion_excluded_services(&self, sha256: &Sha256) -> Vec<String> {
        let mut output = vec![];
        for parent in self.all_ancestors(sha256) {
            if let Some(forbidden) = self._forbidden_services.get(parent) {
                output.extend(forbidden.iter().cloned());
            }
        }
        output.sort_unstable();
        output
    }

    /// A service with monitoring has dispatched, keep track of the conditions.
    fn set_monitoring_entry(&mut self, sha256: Sha256, service_name: String, values: HashMap<String, Option<serde_json::Value>>) {
        self.monitoring.insert((sha256.clone(), service_name.clone()), MonitorTask {
            service: service_name,
            sha: sha256,
            values,
            dispatch_needed: false
        });
    }

    /// Note that a partial result has been recieved. If a dispatch was requested process that now.
    fn partial_result(&mut self, sha256: Sha256, service_name: String) -> bool {
        let monitoring_entry = match self.monitoring.get(&(sha256.clone(), service_name.clone())) {
            Some(entry) => entry,
            None => return false
        };

        if monitoring_entry.dispatch_needed {
            self.redispatch_service(sha256, service_name);
            true
        } else {
            false
        }
    }

    /// A service has completed normally. If the service is monitoring clear out the record.
    fn clear_monitoring_entry(&mut self, sha256: Sha256, service_name: String) {
        let key = (sha256, service_name);
        // We have an incoming non-partial result, flush out any partial monitoring
        self.monitoring.remove(&key);
        // If there is a partial result for this service flush that as well so we accept this new result
        if let Some(result) = self.service_results.get(&key) {
            if result.partial {
                self.service_results.remove(&key);
            }
        }
    }

    /// Check all of the monitored tasks on that key for changes. Redispatch as needed.
    fn temporary_data_changed(&mut self, key: &str) -> Vec<Sha256> {
        let mut changed = vec![];
        for ((sha256, service), entry) in self.monitoring.iter_mut() {
            // Check if this key is actually being monitored by this entry and
            // get whatever values (if any) were provided on the previous dispatch of this service
            let dispatched_value = match entry.values.get(key) {
                Some(val) => val,
                None => continue,
            };
            let value = match self.file_temporary_data.get(sha256) {
                Some(temp) => temp.read_key(key),
                None => continue
            };

            if &value != dispatched_value {
                let result = self.service_results.get(&(sha256.clone(), service.clone()));
                if result.is_some() {
                    // If there are results and there is a monitoring entry, the result was partial
                    // so redispatch it immediately (after the loop). If there are not partial results the monitoring
                    // entry will have been cleared.
                    changed.push((sha256.clone(), service.clone()));
                } else {
                    // If the value has changed since the last dispatch but results haven't come in yet
                    // mark this service to be disptached later. This will only happen if the service
                    // returns partial results, if there are full results the entry will be cleared instead.
                    entry.dispatch_needed = true;
                }
            }
        }

        let mut output = vec![];
        for (sha256, service) in changed {
            self.redispatch_service(sha256.clone(), service);
            output.push(sha256);
        }

        return output
    }

    fn redispatch_service(&mut self, sha256: Sha256, service_name: String) {
        // Clear the result if its partial or an error
        let key = (sha256.clone(), service_name);
        if let Some(result) = self.service_results.get(&key) {
            if !result.partial {
                return
            }
        }   
        self.service_results.remove(&key);
        self.service_errors.remove(&key);
        self.service_attempts.insert(key, 1);

        // Try to get the service to run again by reseting the schedule for that service
        self.file_schedules.remove(&sha256);
    }

    fn trace_event(&mut self, event_type: &str) {
        if self.submission.params.trace {
            self.submission.tracing_events.push(TraceEvent { 
                event_type: event_type.to_owned(), 
                service: None, 
                file: None, 
                message: None, 
                timestamp: chrono::Utc::now()
            })
        }
    }

}

macro_rules! option {
    () => { None };
    ($value:expr) => { Some($value.clone()) };
}

macro_rules! optional_format {
    () => { None };
    ($message:literal $(,$arg:expr)*) => { Some(format!($message $(,$arg)*)) };
}


macro_rules! trace_event {
    ($task:expr, $event:expr $(,file $file:expr)? $(,service $service:expr)? $(,$message:literal $(,$arg:expr)*)?) => {
        if $task.submission.params.trace {

            let mut __file: Option<Sha256> = option!($($file)?);
            let mut __service: Option<String> = option!($($service)?);
            let __message = optional_format!($($message $(,$arg)*)?);


            $task.submission.tracing_events.push(TraceEvent {
                timestamp: chrono::Utc::now(),
                event_type: $event.into(),
                file: __file,
                service: __service,
                message: __message,
            })
        }
    };
}

#[derive(Clone)]
struct TemporaryFileData {
    config: Arc<HashMap<String, TemporaryKeyType>>,
    shared: Arc<Mutex<JsonMap>>,
    local: JsonMap,
}

impl TemporaryFileData {

    pub fn new(config: HashMap<String, TemporaryKeyType>) -> Self {
        Self {
            config: Arc::new(config),
            shared: Arc::new(Mutex::new(Default::default())),
            local: Default::default(),            
        }
    }

    /// Create an entry for another file with reference to the shared values.
    pub fn child(&self) -> Self {
        Self {
            config: self.config.clone(),
            shared: self.shared.clone(), 
            local: self.local.clone(),
        }
    }

    /// Get a copy of the current data
    pub fn read(&self) -> JsonMap {
        // Start with a shallow copy of the local data
        let mut data = self.local.clone();

        // mix in whatever the latest submission wide values are values are
        let shared = self.shared.lock();
        data.append(&mut shared.clone());
        return data
    }

    /// Get a copy of the current data
    pub fn read_key(&self, key: &str) -> Option<serde_json::Value> {
        if let Some(value) = self.shared.lock().get(key) {
            return Some(value.clone())
        }
        return self.local.get(key).cloned()
    }

    /// Set the value of a temporary data key using the appropriate method for the key.
    ///
    /// Return true if this change could mean partial results should be reevaluated.
    pub fn set_value(&mut self, key: &str, value: serde_json::Value) -> bool {
        match self.config.get(key) {
            Some(TemporaryKeyType::Union) => {
                self.union_shared_value(key, value)    
            },
            Some(TemporaryKeyType::Overwrite) => {
                let mut shared = self.shared.lock();
                let change = shared.get(key) != Some(&value);
                shared.insert(key.to_owned(), value);
                change
            },
            None => {
                self.local.insert(key.to_owned(), value);
                false
            }
        }        
    }

    fn union_shared_value(&self, key: &str, values: serde_json::Value) -> bool {
        // Make sure the existing value is the right type
        let mut shared = self.shared.lock();        
        let existing = shared.entry(key.to_string()).or_insert(serde_json::Value::Array(vec![]));
        if !existing.is_array() {
            *existing = serde_json::Value::Array(vec![]);
        }
        let existing = existing.as_array_mut().unwrap();

        // make sure the input is the right type
        let serde_json::Value::Array(values) = values else {
            return false
        };

        // Add each value one at a time testing for new values
        // This is slower than using set intersection, but isn't type sensitive
        let mut changed = false;
        for new_item in values {
            if existing.contains(&new_item) {
                continue
            }
            existing.push(new_item);
            changed = true;
        }
        return changed
    }
}


pub async fn main(core: Core) -> Result<()> {
    // Bind the HTTP interface
    let bind_address = crate::config::load_bind_address().context("load_bind_address")?;
    let tls_config = crate::config::TLSConfig::load().await.context("load_tls_config")?;
    let tcp = crate::http::create_tls_binding(bind_address, tls_config).await.context("create_tls_binding")?;

    // Initialize Internal state
    let dispatcher = Dispatcher::new(core, tcp).await?;
    dispatcher.finalizing.install_terminate_handler(true)?;
    dispatcher.core.install_activation_handler("dispatcher").await?;

    let mut components = tokio::task::JoinSet::new();
    dispatcher.start(&mut components);

    // Wait for all of these components to terminate
    while components.join_next().await.is_some() {}

    // If the dispatcher is exiting cleanly remove as many tasks from the service queues as we can
    let mut completions = vec![];
    {
        let queues = dispatcher.process_queues.read().await;
        for (key, queue) in queues.iter() {
            let (send, recv) = oneshot::channel();
            if queue.send(DispatchAction::Terminate(*key, send)).is_ok() {
                completions.push(recv);
            }
        }
    }
    for channel in completions {
        _ = channel.await;
    }
    Ok(())
}

pub struct Dispatcher {
    core: Core,
    instance_id: String,
    instance_address: String,

    finalizing: Arc<Flag>,
    finalizing_start: Mutex<Option<Instant>>,
    shutdown_grace: u64,
    result_batch_size: usize,
    user_cache: UserCache,

    // Setup queues for work to be divided into
    process_queues: tokio::sync::RwLock<HashMap<Sid, mpsc::UnboundedSender<DispatchAction>>>,

    // Communications queues
    start_queue: redis_objects::Queue<ServiceStartMessage>,
    result_queue: redis_objects::Queue<ServiceResponse>,
    command_queue: redis_objects::Queue<DispatcherCommandMessage>,

    // Output. Duplicate our input traffic into this queue so it may be cloned by other systems
    traffic_queue: redis_objects::Publisher,
    quota_tracker: UserQuotaTracker,
    submission_queue: redis_objects::Queue<SubmissionDispatchMessage>,

    // Tables to track what submissions are running where
    submissions_assignments: redis_objects::Hashmap<String>,
    // ingester_scanning: redis_objects::Hashmap<>,
    active_submissions: redis_objects::Hashmap<SubmissionDispatchMessage>,

    // Table to track the running dispatchers
    dispatchers_directory: redis_objects::Hashmap<i64>,
    dispatchers_directory_finalize: redis_objects::Hashmap<i64>,
    running_dispatchers_estimate: std::sync::atomic::AtomicU32,

    // Build some utility classes
    // running_tasks: redis_objects::Hashmap<ServiceTask>,
    scaler_timeout_queue: redis_objects::Queue<KillContainerCommand>,

    // Publish counters to the metrics sink.
    counter: redis_objects::AutoExportingMetrics<Metrics>,

    // Update bad sid list
    redis_bad_sids: redis_objects::Set<Sid>,
    bad_sids: tokio::sync::RwLock<HashSet<Sid>>,

    // Utility object to handle post-processing actions
    pub postprocess_worker: Arc<ActionWorker>,
}


impl Dispatcher {
    pub async fn new(core: Core, tcp: TlsAcceptor) -> Result<Arc<Self>> {
        // generate an id for this instance
        let instance_id = format!("{:x}", rand::rng().random::<u128>());
        info!("Using dispatcher id {instance_id}");

        // load environment configured values
        let shutdown_grace: u64 = match std::env::var("AL_SHUTDOWN_GRACE") {
            Ok(grace) => grace.parse()?,
            Err(_) => 60,
        };

        let result_batch_size: usize = match std::env::var("DISPATCHER_RESULT_BATCH_SIZE") {
            Ok(grace) => grace.parse()?,
            Err(_) => DEFAULT_RESULT_BATCH_SIZE,
        };

        let redis_bad_sids = core.redis_persistant.set(BAD_SID_HASH.to_owned());

        // figure out the address we will present to the world
        let addr = tcp.local_addr();
        let port = match addr.first().and_then(|addr|addr.as_socket_addr()).map(|sock| sock.port()) {
            Some(port) => port,
            None => bail!("Could not determine own bound port")
        };

        let disp = Arc::new(Self {
            shutdown_grace,
            result_batch_size,
            finalizing: Arc::new(Flag::new(false)),
            finalizing_start: Mutex::new(None),
            user_cache: UserCache::new(core.datastore.clone()),

            // Communications queues
            start_queue: core.redis_volatile.queue(DISPATCH_START_EVENTS.to_owned() + &instance_id, Some(QUEUE_EXPIRY)),
            result_queue: core.redis_volatile.queue(DISPATCH_RESULT_QUEUE.to_owned() + &instance_id, Some(QUEUE_EXPIRY)),
            command_queue: core.redis_volatile.queue(DISPATCH_COMMAND_QUEUE.to_owned() + &instance_id, Some(QUEUE_EXPIRY)),

            // Output. Duplicate our input traffic into this queue so it may be cloned by other systems
            traffic_queue: core.redis_volatile.publisher("submissions".to_owned()),
            quota_tracker: core.redis_persistant.user_quota_tracker("submissions".to_string()).set_timeout(QUOTA_TIMEOUT),
            submission_queue: core.redis_volatile.queue(SUBMISSION_QUEUE.to_string(), None),

            // Tables to track what submissions are running where
            submissions_assignments: core.redis_persistant.hashmap(DISPATCH_TASK_HASH.to_owned(), None),
            // ingester_scanning: core.redis_persistant.hashmap("m-scanning-table", None),
            active_submissions: core.redis_persistant.hashmap(DISPATCH_TASK_ASSIGNMENT.to_owned() + &instance_id, None),

            // Setup queues for work to be divided into
            process_queues: RwLock::new(Default::default()),

            // Table to track the running dispatchers
            dispatchers_directory: core.dispatcher_instances_table(),
            dispatchers_directory_finalize: core.redis_persistant.hashmap(DISPATCH_DIRECTORY_FINALIZE.to_string(), None),
            running_dispatchers_estimate: AtomicU32::new(1),

            // Track the tasks that should be running right now
            // running_tasks: core.redis_volatile.hashmap(DISPATCH_RUNNING_TASK_HASH.to_string(), None),

            // message queue to tell scaler to kill a container
            scaler_timeout_queue: core.redis_persistant.queue(SCALER_TIMEOUT_QUEUE.to_string(), None),

            // Publish counters to the metrics sink.
            counter: core.redis_metrics.auto_exporting_metrics(METRICS_CHANNEL.to_owned(), "dispatcher".to_owned())
                .counter_name("dispatcher".to_owned())
                .export_interval(Duration::from_secs(core.config.core.metrics.export_interval as u64))
                .start(),

            // Update bad sid list
            bad_sids: RwLock::new(redis_bad_sids.members().await?.into_iter().collect()),
            redis_bad_sids,

            // Utility object to handle post-processing actions
            postprocess_worker: ActionWorker::new(false, &core).await?,

            core,
            instance_id,
            instance_address: format!("{}:{port}", crate::config::address()?),
        });

        info!("Dispatcher {} located at {}", disp.instance_id, disp.instance_address);

        tokio::spawn(http::start(tcp, disp.clone()));
        Ok(disp)
    }

    pub fn start(self: &Arc<Self>, components: &mut tokio::task::JoinSet<()>) {
        components.spawn(self.clone().on_terminate());

        // Pull in new submissions
        components.spawn(retry!("Pull Submissions", self, pull_submissions));

        // pull start messages
        components.spawn(retry!("Pull Service Start", self, pull_service_starts));

        // pull result messages
        components.spawn(retry!("Pull Service Result", self, pull_service_results));

        // Work guard/thief
        components.spawn(retry!("Guard Work", self, work_guard));
        components.spawn(retry!("Work Thief", self, work_thief));

        // Handle RPC commands
        components.spawn(retry!("Commands", self, handle_commands));

        // Process to protect against old dead tasks timing out
        components.spawn(retry!("Global Timeout Backstop", self, timeout_backstop));

        // Daemon to report CPU usage
        components.spawn(retry!("Metrics Reporter".to_string(), self, handle_metrics));
    }

    async fn on_terminate(self: Arc<Self>) {
        self.finalizing.wait_for(true).await;
        _ = self.dispatchers_directory_finalize.set(&self.instance_id, &chrono::Utc::now().timestamp()).await;
    }

    async fn handle_metrics(self: Arc<Self>) -> Result<()> {
        let mut tracker = CPUTracker::new().await;
        while self.core.is_running() {
            let value =  tracker.read().await;
            increment!(timer, self.counter, cpu_seconds, value);
            self.core.sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn get_test_report(&self, sid: Sid) -> Result<TestReport> {
        let (send, resp) = oneshot::channel();
        self.send_dispatch_action(DispatchAction::TestReport(sid, send)).await;
        Ok(resp.await?)
    }

    /// get how long it has been since finalization started
    /// if the counter hasn't started calling this method starts it
    fn get_finalizing_start(&self) -> Duration {
        let mut start = self.finalizing_start.lock();
        start.get_or_insert_with(Instant::now).elapsed()
    }

    /// get how long we will wait after getting a termination signal to allow for clean shutdown
    fn get_finalizing_window(&self) -> Duration {
        Duration::from_secs(self.shutdown_grace.saturating_sub(AL_SHUTDOWN_QUIT))
    }

    fn _watcher_list(&self, sid: Sid) -> redis_objects::Set<String> {
        return self.core.redis_volatile.expiring_set(make_watcher_list_name(sid), None)
    }

    async fn send_dispatch_action(&self, action: DispatchAction) {
        let sid = action.sid();
        let queue_table = self.process_queues.read().await;
        if let Some(queue) = queue_table.get(&sid) {
            if queue.send(action).is_err() {
                warn!("Message for non-active submission: {sid}");
            }
        }
    }

    async fn pull_submissions(self: &Arc<Self>) -> Result<()> {
        while self.core.is_running() {
            while !self.core.is_active() {
                // Dispatcher is disabled... waiting for it to be reactivated
                self.core.sleep(std::time::Duration::from_millis(100)).await;
            }

            if self.finalizing.read() {
                let finalizing_time = self.get_finalizing_start();
                if self.active_submissions.length().await? > 0 && finalizing_time < self.get_finalizing_window() {
                    self.core.sleep(ONE_SECOND).await;
                } else {
                    self.core.running.set(false);
                }
            } else {
                // Check if we are at the submission limit globally
                if self.submissions_assignments.length().await? >= self.core.config.core.dispatcher.max_inflight {
                    self.core.sleep(ONE_SECOND).await;
                    continue
                }

                // Check if we are maxing out our share of the submission limit
                let running = self.running_dispatchers_estimate.load(std::sync::atomic::Ordering::Acquire) as u64;
                let max_tasks = self.core.config.core.dispatcher.max_inflight / running;
                if self.active_submissions.length().await? >= max_tasks {
                    self.core.sleep(ONE_SECOND).await;
                    continue
                }

                // Grab a submission message
                let message = match self.submission_queue.pop_timeout(ONE_SECOND).await? {
                    Some(message) => message,
                    None => continue
                };

                // Fetch the access level of the submitter and attach it to the task.
                // This is for performance rather than security reasons.
                let mut access_control = None;
                if let Some(submitter) = self.user_cache.get(&message.submission.params.submitter).await? {
                    access_control = Some(submitter.classification.classification.clone())
                }

                // This is probably a complete task
                let task = SubmissionTask::new(message, access_control, &self.core.services);
                self.dispatch_submission(task).await.context("dispatch_submission")?;
            }
        }
        Ok(())
    }

    async fn pull_service_starts(self: &Arc<Self>) -> Result<()> {

        while self.core.is_running() {
            // get a batch of start messages
            let mut messages = self.start_queue.pop_batch(100).await?;

            // if the batch is empty block on getting more
            if messages.is_empty() {
                let message = self.start_queue.pop_timeout(ONE_SECOND).await?;
                if let Some(message) = message {
                    messages.push(message);
                }
            }

            // process all the messages we found
            for message in messages {
                self.send_dispatch_action(DispatchAction::Start(message, None)).await;
            }
        }
        Ok(())
    }

    async fn pull_service_results(self: &Arc<Self>) -> Result<()> {
        while self.core.is_running() {
            // Try to get a batch of results to process
            let mut messages = self.result_queue.pop_batch(self.result_batch_size).await?;

            // If there are no messages and no timeouts to process block for a second
            if messages.is_empty() {
                let message = self.result_queue.pop_timeout(ONE_SECOND).await?;
                if let Some(message) = message {
                    messages.push(message);
                }
            }

            // If we have any messages, schedule them to be processed by the right worker thread
            for message in messages {
                self.send_dispatch_action(DispatchAction::Result(Box::new(message))).await;
            }
        }
        Ok(())
    }

    async fn work_guard(self: &Arc<Self>) -> Result<()> {
        let check_interval = Duration::from_secs(GUARD_TIMEOUT as u64/8);
        let mut old_value = chrono::Utc::now().timestamp();
        let id = self.instance_id.to_string();
        self.dispatchers_directory.set(&id, &old_value).await?;

        while self.core.sleep(check_interval).await {
            // Increase the guard number
            let gap = chrono::Utc::now().timestamp() - old_value;
            let updated_value = self.dispatchers_directory.increment(&id, gap).await?;

            // If for whatever reason, there was a moment between the previous increment
            // and the one before that, that the gap reached the timeout, someone may have
            // started stealing our work. We should just exit.
            if gap > GUARD_TIMEOUT {
                warn!("Dispatcher closing due to guard interval failure: {gap} > {GUARD_TIMEOUT}");
                self.core.running.set(false);
                break
            }

            // Everything is fine, prepare for next round
            old_value = updated_value;
        }

        self.dispatchers_directory.pop(&id).await?;
        self.dispatchers_directory_finalize.pop(&id).await?;
        Ok(())
    }

    async fn work_thief(self: &Arc<Self>) -> Result<()> {
        // Clean up the finalize list once in a while, each time a dispatcher starts is fine
        for (id, timestamp) in self.dispatchers_directory_finalize.items().await? {
            let age = chrono::Utc::now().timestamp() - timestamp;
            if age > ONE_DAY.as_secs() as i64 {
                debug!("cleaning finialize record: {id} ({age} seconds)");
                self.dispatchers_directory_finalize.pop(&id).await?;
            }
        }

        // Keep a table of the last recorded status for other dispatchers
        let mut last_seen: HashMap<String, i64> = Default::default();

        while self.core.sleep(Duration::from_secs(GUARD_TIMEOUT as u64 / 4)).await {

            // Load guards
            let finalizing = self.dispatchers_directory_finalize.items().await?;
            for (id, time) in self.dispatchers_directory.items().await? {
                last_seen.insert(id.clone(), time);
            }

            // List all dispatchers with jobs assigned
            for raw_key in self.core.redis_persistant.keys(TASK_ASSIGNMENT_PATTERN).await? {
                if let Some(key) = raw_key.strip_prefix(DISPATCH_TASK_ASSIGNMENT){
                    if !last_seen.contains_key(key) {
                        last_seen.insert(key.to_string(), chrono::Utc::now().timestamp());
                    }
                }
            }

            // count the current dispatchers
            let mut active_ids = 0;
            for id in last_seen.keys() {
                if finalizing.contains_key(id) {
                    active_ids += 1;
                }
            }
            self.running_dispatchers_estimate.store(active_ids.max(1), std::sync::atomic::Ordering::Relaxed);

            // get the dispatcher that has gone the longest without reporting
            let oldest = last_seen.iter().reduce(|row, acc| {
                if row.1 < acc.1 { row } else { acc }
            });

            // Check if it has gone missing
            if let Some((oldest, last)) = oldest {
                if chrono::Utc::now().timestamp() - last > GUARD_TIMEOUT {
                    self.steal_work(oldest).await?;
                    last_seen.remove(&oldest.clone());
                }
            }
        }

        let id = self.instance_id.to_string();
        self.dispatchers_directory.pop(&id).await?;
        self.dispatchers_directory_finalize.pop(&id).await?;
        Ok(())
    }

    async fn steal_work(&self, target: &str) -> Result<()> {
        let target_jobs = self.core.redis_persistant.hashmap::<SubmissionDispatchMessage>(DISPATCH_TASK_ASSIGNMENT.to_owned() + target, None);
        info!("Starting to steal work from {target}");

        let mut keys = target_jobs.keys().await?;
        while !keys.is_empty() {
            let key: String = match keys.pop() {
                Some(key) => key,
                None => {
                    keys = target_jobs.keys().await?;
                    continue
                }
            };
            let message = match target_jobs.pop(&key).await? {
                Some(message) => message,
                None => continue,
            };

            if self.submissions_assignments.pop(&key).await?.is_some() {
                self.submission_queue.unpop(&message).await?;
            }
        }

        info!("Finished stealing work from {target}");
        self.dispatchers_directory.pop(target).await?;
        self.dispatchers_directory_finalize.pop(target).await?;
        Ok(())
    }

    async fn handle_commands(self: &Arc<Self>) -> Result<()> {
        while self.core.is_running() {

            let command = match self.command_queue.pop_timeout(Duration::from_secs(3)).await? {
                Some(message) => message,
                None => continue,
            };

            // Start of process dispatcher transaction
            match command.payload()? {
                DispatcherCommand::CreateWatch(CreateWatch { queue_name, submission }) => {
                    self.setup_watch_queue(submission, queue_name).await?;
                },
                DispatcherCommand::ListOutstanding(ListOutstanding { response_queue, submission }) => {
                    self.list_outstanding(submission, response_queue).await?;
                },
                DispatcherCommand::UpdateBadSid(response_queue) => {
                    self.update_bad_sids().await?;
                    self.core.redis_volatile.queue(response_queue, None).push(&self.instance_id.to_string()).await?;
                }
            }
        }
        Ok(())
    }


    async fn setup_watch_queue(&self, sid: Sid, queue_name: String) -> Result<()> {
        // Create a unique queue
        let watch_queue = self.core.redis_volatile.queue::<WatchQueueMessage>(queue_name.clone(), Some(Duration::from_secs(30)));
        watch_queue.push(&WatchQueueStatus::Start.into()).await?;

        //
        let queues = self.process_queues.read().await;
        let inner_queue = match queues.get(&sid) {
            Some(queue) => queue,
            None => {
                watch_queue.push(&WatchQueueStatus::Stop.into()).await?;
                return Ok(())
            }
        };

        // Add the newly created queue to the list of queues for the given submission
        self._watcher_list(sid).add(&queue_name).await?;

        // Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
        _ = inner_queue.send(DispatchAction::DescribeStatus(sid, watch_queue));
        Ok(())
    }

    async fn list_outstanding(&self, sid: Sid, queue_name: String) -> Result<()> {
        let response_queue = self.core.redis_volatile.queue::<HashMap<String, u64>>(queue_name, Some(Duration::from_secs(30)));

        let response = {
            let (send, recv) = oneshot::channel();
            let queues = self.process_queues.read().await;
            match queues.get(&sid) {
                Some(queue) => {
                    _ = queue.send(DispatchAction::ListOutstanding(sid, send));
                },
                None => {
                    response_queue.push(&Default::default()).await?;
                    return Ok(())
                }
            };
            recv
        };

        let outstanding = response.await.unwrap_or_default();
        response_queue.push(&outstanding).await?;
        Ok(())
    }

    async fn update_bad_sids(&self) -> Result<()> {
        // Pull new sid list
        let remote_sid_list = self.redis_bad_sids.members().await?;

        // Add new sids
        let mut new_sids = vec![];
        {
            let mut sid_list = self.bad_sids.write().await;
            for sid in remote_sid_list {
                if sid_list.insert(sid) {
                    new_sids.push(sid);
                }
            }
        }

        // Kick off updates for any new sids
        let queue_table = self.process_queues.read().await;
        for sid in new_sids {
            if let Some(queue) = queue_table.get(&sid) {
                _ = queue.send(DispatchAction::BadSid(sid));
            }
        }
        Ok(())
    }

    async fn timeout_backstop(self: Arc<Self>) -> Result<()> {
        while self.core.is_running() {
            // { // timeout_backstop
            //     let dispatcher_instances = self.core.dispatcher_instances().await?;
            //     let mut error_tasks = vec![];

            //     // iterate running tasks
            //     let tasks: HashSet<Sid> = self.process_queues.read().await.keys().cloned().collect();
            //     for (_task_key, task) in self.running_tasks.items().await? {
            //         // Its a bad task if it's dispatcher isn't running
            //         if !dispatcher_instances.contains(&task.dispatcher) {
            //             error_tasks.push(task);
            //             continue
            //         }
            //         // Its a bad task if its OUR task, but we aren't tracking that submission anymore
            //         if task.dispatcher == self.instance_id && !tasks.contains(&task.sid) {
            //             error_tasks.push(task)
            //         }
            //     }

            //     // Refresh our dispatcher list.
            //     let dispatcher_instances = self.core.dispatcher_instances().await?;
            //     let mut other_dispatcher_instances: HashSet<String> = dispatcher_instances.iter().cloned().collect();
            //     other_dispatcher_instances.remove(&self.instance_id);

            //     // The remaining running tasks (probably) belong to dead dispatchers and can be killed
            //     for task in error_tasks {
            //         // Check against our refreshed dispatcher list in case it changed during the previous scan
            //         if other_dispatcher_instances.contains(&task.dispatcher) {
            //             continue
            //         }

            //         // If its already been handled, we don't need to
            //         if self.running_tasks.pop(&task.key()).await?.is_none() {
            //             continue
            //         }

            //         let worker = match task.metadata.get("worker__") {
            //             Some(serde_json::Value::String(worker)) => worker,
            //             _ => continue
            //         };

            //         // Kill the task that would report to a dead dispatcher
            //         warn!("[{}]Task killed by backstop {} {}", task.sid, task.service_name, task.fileinfo.sha256);
            //         self.scaler_timeout_queue.push(&KillContainerCommand {
            //             service: task.service_name.clone(),
            //             container: worker.to_owned()
            //         }).await?;

            //         // Report to the metrics system that a recoverable error has occurred for that service
            //         self.core.export_metrics_once(
            //             &task.service_name,
            //             &ServiceMetrics {fail_recoverable: 1, ..Default::default()},
            //             Some(worker.as_str()),
            //             Some("service")
            //         ).await?;
            //     }
            // }

            #[derive(Debug, Deserialize)]
            struct FieldList {
                sid: Sid,
            }

            impl Readable for FieldList { fn set_from_archive(&mut self, _from_archive: bool) {} }

            // Look for unassigned submissions in the datastore if we don't have a
            // large number of outstanding things in the queue already.
            let assignments = self.submissions_assignments.items().await?;
            let mut recovered_from_database = vec![];
            if self.submission_queue.length().await? < 500 {
                // Get the submissions belonging to an dispatcher we don't know about
                let mut cursor = self.core.datastore.submission.stream_search::<FieldList>("state: submitted", "sid".to_owned(), vec![], None, None, None).await?;
                while let Some(item) = cursor.next().await? {
                    if assignments.contains_key(&item.sid.to_string()) {
                        continue
                    }
                    recovered_from_database.push(item.sid);
                }
            }

            // Look for instances that are in the assignment table, but the instance its assigned to doesn't exist.
            // We try to remove the instance from the table to prevent multiple dispatcher instances from
            // recovering it at the same time
            {
                // Get the submissions belonging to an dispatcher we don't know about
                let assignments = self.submissions_assignments.items().await?;
                let mut dispatcher_instances: HashSet<String> = self.core.dispatcher_instances().await?.into_iter().collect();
                // List all dispatchers with jobs assigned
                for raw_key in self.core.redis_persistant.keys(TASK_ASSIGNMENT_PATTERN).await? {
                    if let Some(key) = raw_key.strip_prefix(DISPATCH_TASK_ASSIGNMENT) {
                        dispatcher_instances.insert(key.to_owned());
                    }
                }

                // Submissions that didn't belong to anyone should be recovered
                for (sid, instance) in assignments {
                    if dispatcher_instances.contains(&instance) {
                        continue
                    }
                    if self.submissions_assignments.conditional_remove(&sid, &instance).await? {
                        self.recover_submission(&sid, "from assignment table").await?;
                    }
                }
            }

            // Go back over the list of sids from the database now that we have a copy of the
            // assignments table taken after our database scan
            for sid in recovered_from_database {
                if !assignments.contains_key(&sid.to_string()) {
                    self.recover_submission(&sid.to_string(), "from database scan").await?;
                }
            }

            self.core.sleep(GLOBAL_TASK_CHECK_INTERVAL).await;
        }
        Ok(())
    }

    async fn recover_submission(&self, sid: &str, message: &str) -> Result<bool> {
        // Make sure we can load the submission body
        let submission = match self.core.datastore.submission.get_if_exists(sid, None).await? {
            Some((submission, _)) => submission,
            None => return Ok(false)
        };
        if submission.state != SubmissionState::Submitted {
            return Ok(false)
        }

        warn!("Recovered dead submission: {sid} {message}");

        // Try to recover the completion queue value by checking with the ingest table
        let mut completed_queue = None;
        if submission.scan_key.is_some() {
            completed_queue = Some(COMPLETE_QUEUE_NAME.to_string());
        }

        // Put the file back into processing
        self.submission_queue.unpop(&SubmissionDispatchMessage::simple(submission, completed_queue)).await?;
        Ok(true)
    }

    // Find any files associated with a submission and dispatch them if they are
    // not marked as in progress. If all files are finished, finalize the submission.
    //
    // This version of dispatch submission doesn't verify each result, but assumes that
    // the dispatch table has been kept up to date by other components.
    //
    // Preconditions:
    //     - File exists in the filestore and file collection in the datastore
    //     - Submission is stored in the datastore
    async fn dispatch_submission(self: &Arc<Self>, mut task: SubmissionTask) -> Result<()> {
        // let submission = &task.submission;
        let sid = task.submission.sid.to_string();
        let sha256 = task.submission.files[0].sha256.clone();

        // Check the sid table
        if self.bad_sids.read().await.contains(&task.submission.sid) {
            task.submission.to_be_deleted = true;
        }

        if !self.submissions_assignments.add(&sid, &self.instance_id).await? {
            warn!("[{sid}] Received an assigned submission dropping");
            return Ok(())
        }

        if !self.active_submissions.exists(&sid).await? {
            info!("[{sid}] New submission received");
            task.trace_event("submission_start");
            self.active_submissions.add(&sid, &SubmissionDispatchMessage::simple(task.submission.clone(),  task.completed_queue.clone())).await?;

            // Write all new submissions to the traffic queue
            self.traffic_queue.publish(&SubmissionMessage::started((&task.submission).into())).await?;

        } else {
            trace_event!(task, "submission_start", "Received a pre-existing submission");
            warn!("[{sid}] Received a pre-existing submission, check if it is complete");
        }

        // Apply initial data parameter
        let mut temp_data_config = self.core.config.submission.default_temporary_keys.clone();
        temp_data_config.extend(&mut self.core.config.submission.temporary_keys.clone().into_iter());
        let mut temporary_data = TemporaryFileData::new(temp_data_config);
        let max_temp_data_length = self.core.config.submission.max_temp_data_length as usize;
        if let Some(initial) = &task.submission.params.initial_data {
            match serde_json::from_str::<JsonMap>(&initial.0) {
                Ok(init) => {
                    for (key, value) in init {
                        if estimate_size(&value) <= max_temp_data_length {
                            temporary_data.set_value(&key, value);
                        }
                    }
                },
                Err(err) => {
                    warn!("[{sid}] could not process initialization data: {err}")
                }
            }
        }
        task.file_temporary_data.insert(sha256.clone(), temporary_data);


        // setup file parameters for root
        task.file_depth.insert(sha256.clone(), 0);
        let file_name = task.submission.files[0].name.clone();
        task.file_names.insert(sha256.clone(), if file_name.is_empty() { sha256.to_string() } else { file_name });
        task.active_files.insert(sha256.clone());

        // Initialize ancestry chain by identifying the root file
        let file_info = self.get_fileinfo(&mut task, &sha256).await.context("get_fileinfo")?;
        let file_type = match file_info {
            Some(info) => info.file_type.clone(),
            None => "NOT_FOUND".to_owned(),
        };
        task.file_ancestry.insert(sha256.clone(), vec![vec![AncestoryEntry {
            file_type: file_type.clone(),
            parent_relation: "ROOT".to_owned(),
            sha256: sha256.clone()
        }]]);

        // create queue for processor
        let (sender, reciever) = tokio::sync::mpsc::unbounded_channel();
        {
            let mut queues = self.process_queues.write().await;
            match queues.entry(task.submission.sid) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if entry.get().is_closed() {
                        entry.insert(sender);
                    } else {
                        bail!("Submission already being processed: {sid}");
                    }
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(sender);
                },
            }
        }

        // Launch processor
        tokio::spawn(self.clone().submission_worker(task, reciever));
        Ok(())
    }

    async fn submission_worker(self: Arc<Self>, mut task: SubmissionTask, mut work_queue: mpsc::UnboundedReceiver<DispatchAction>) {
        for _ in 0..SUBMISSION_ATTEMPT_LIMIT {
            match self._submission_worker(&mut task, &mut work_queue).await {
                Ok(_) => return,
                Err(err) => {
                    error!("Error handling submission [{}]: {err:?}", task.submission.sid);
                },
            }
        }
    }

    async fn _submission_worker(self: &Arc<Self>, task: &mut SubmissionTask, work_queue: &mut mpsc::UnboundedReceiver<DispatchAction>) -> Result<()> {
        let sid = task.submission.sid;
        info!("Start service worker {sid}");

        // Refresh the quota hold
        let mut _quota_guard = None;
        if task.submission.params.quota_item && !task.submission.params.submitter.is_empty() {
            info!("[{sid}] Submission counts towards {} quota", task.submission.params.submitter.to_uppercase());
            _quota_guard = Some(QuotaGuard::new(sid, task.submission.params.submitter.clone(), self.quota_tracker.clone()));
        }

        // timer to detect when a submission has gone too long with events and needs to be
        let mut submission_timeout = Instant::now();

        // timers to detect when a service is late and needs to be timed out
        let mut timeouts: HashMap<(Sha256, String), (Instant, Duration, String)> = Default::default();

        // process the root file
        let mut finished = false;
        self.dispatch_file(task, &task.submission.files[0].sha256.clone()).await.context("initial dispatch")?;

        while self.core.is_running() && !finished {
            let mut message = task.pop_internal_task();

            // check for submission timeout
            if submission_timeout.elapsed() > SUBMISSION_TOTAL_TIMEOUT {
                message = Some(DispatchAction::Check(sid));
            }

            // check for service timeouts, make sure we have processed messages in the queue before checking timeouts
            if message.is_none() && work_queue.is_empty() {

                // Check for service timeouts
                let mut service_timeouts = vec![];
                for ((sha, service_name), (start, duration, worker)) in &timeouts {
                    if start.elapsed() > *duration {
                        let key = (sha.clone(), service_name.clone());
                        service_timeouts.push(key.clone());

                        let log = format!("Service timeout at {} on worker {worker}", chrono::Utc::now());
                        trace_event!(task, "service_timeout", file sha, service service_name, "{log}");
                        let logs = task.service_logs.entry(key).or_default();
                        logs.push(log);

                        self.timeout_service(task, sha, service_name, worker).await?;

                    }
                }

                // report timeouts and remove them from the table
                increment!(self.counter, service_timeouts, service_timeouts.len() as i32);
                for key in &service_timeouts {
                    timeouts.remove(key);
                }
            }

            // read messages
            let message = match message {
                Some(message) => message,
                None => {
                    match tokio::time::timeout(ONE_SECOND, work_queue.recv()).await {
                        Ok(Some(message)) => message,
                        Ok(None) => {
                            error!("submission processing terminated while incomplete {sid}");
                            return Ok(())
                        }
                        Err(_) => continue
                    }
                }
            };

            // process the message
            match message {
                DispatchAction::Start(message, started) => {
                    trace_event!(task, "service_start", file &message.sha, service &message.service_name, "{}", message.worker_id);

                    let finish = |message: Result<()>| {
                        started.map(|socket| socket.send(message))
                    };

                    let key = (message.sha, message.service_name.clone());
                    if let Some((service_task, queue_key, last_queue_check)) = task.queue_keys.remove(&key) {
                        // If this task is already finished (result message processed before start
                        // message) we can skip setting a timeout
                        if task.service_errors.contains_key(&key) || task.service_results.contains_key(&key) {
                            finish(Err(anyhow!("Task already finished")));
                            continue
                        }

                        // check if someone is trying to start an out of date task
                        if let Some(request_id) = message.task_id {
                            if request_id != service_task.task_id {
                                // trying to start the wrong task, put the queue key back and ignore this message
                                task.queue_keys.insert(key, (service_task, queue_key, last_queue_check));
                                finish(Err(anyhow!("This task has been replaced")));
                                continue
                            }
                        }

                        if let Some(service) = self.core.services.get(&message.service_name) {
                            timeouts.insert(key.clone(), (Instant::now(), Duration::from_secs(service.timeout as u64) + TIMEOUT_GRACE, message.worker_id.clone()));
                            task.service_logs.entry(key.clone()).or_default().push(format!("Popped from queue and running at {} on worker {}", chrono::Utc::now(), message.worker_id));
                            task.running_services.insert(key, service_task);
                            finish(Ok(()));
                            continue
                        }
                    } else {
                        finish(Err(anyhow!("Task not recorded as queued")));
                    }
                },
                DispatchAction::Result(message) => {
                    submission_timeout = Instant::now();
                    let key = (message.sha256(), message.service_name().to_owned());
                    timeouts.remove(&key);
                    task.running_services.remove(&key);

                    match *message {
                        ServiceResponse::Result(data) => self.process_service_result(task, data).await?,
                        ServiceResponse::Error(data) => self.process_service_error(task, &data.error_key, data.error).await?,
                    };
                },
                DispatchAction::DescribeStatus(_, watch_queue) => {
                    // Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
                    for result_data in task.service_results.values() {
                        let _ = watch_queue.push(&WatchQueueMessage {
                            status: WatchQueueStatus::Ok,
                            cache_key: Some(result_data.key.clone())
                        }).await;
                    }
                    for error_key in task.service_errors.values() {
                        let _ = watch_queue.push(&WatchQueueMessage {
                            status: WatchQueueStatus::Fail,
                            cache_key: Some(error_key.clone())
                        }).await;
                    }
                },
                DispatchAction::ListOutstanding(_, channel) => {
                    let mut outstanding: HashMap<String, u64> = Default::default();
                    for (_sha, service_name) in task.queue_keys.keys() {
                        *outstanding.entry(service_name.clone()).or_default() += 1;
                    }
                    for (_sha, service_name) in task.running_services.keys() {
                        *outstanding.entry(service_name.clone()).or_default() += 1;
                    }
                    _ = channel.send(outstanding);
                },
                DispatchAction::BadSid(_) => {
                    task.submission.to_be_deleted = true;
                    self.active_submissions.set(&sid.to_string(), &SubmissionDispatchMessage::simple(task.submission.clone(), task.completed_queue.clone())).await?;
                },
                DispatchAction::Check(_) => {
                    info!("[{sid}] checking dispatch status...");
                    finished = self.check_submission(task).await?;

                    // If we didn't finish the submission here, wait another 20 minutes
                    submission_timeout = Instant::now();
                },
                DispatchAction::Terminate(_, respond) => {
                    for ((_, service_name), (_, key, _)) in &task.queue_keys {
                        let service_queue = self.core.get_service_queue(service_name);
                        service_queue.remove(key).await?;
                    }
                    _ = respond.send(());
                    finished = true;
                },
                DispatchAction::DispatchFile(_, sha256) => {
                    self.dispatch_file(task, &sha256).await?;
                },
                DispatchAction::TestReport(_, respond) => {
                    _ = respond.send(TestReport {
                        queue_keys: task.queue_keys.clone(),
                        service_results: task.service_results.clone(),
                        service_errors: task.service_errors.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    // Dispatch to any outstanding services for the given file.
    // If nothing can be dispatched, check if the submission is finished.
    //
    // :param task: Submission task object.
    // :param sha256: hash of the file to check.
    // :param timed_out_host: Name of the host that timed out after maximum service attempts.
    // :return: true if submission is finished.
    async fn dispatch_file(&self, task: &mut SubmissionTask, sha256: &Sha256) -> Result<()> {
        // let submission = &task.submission;
        // let params = &submission.params;
        let sid = task.submission.sid;
        trace_event!(task, "dispatch_file", file sha256);

        // We are processing this file, load the file info
        let file_info = match self.get_fileinfo(task, sha256).await.context("get_fileinfo")? {
            Some(file_info) => file_info,
            None => return Ok(())
        };

        let file_depth = task.file_depth.get(sha256).cloned().unwrap_or(1);
        // If its the first time we've seen this file, we won't have a schedule for it
        let mut schedule = match task.file_schedules.get(sha256) {
            Some(schedule) => schedule.clone(),
            None => {

                let mut forbidden_services = vec![];

                // If Dynamic Recursion Prevention is in effect and the file is not part of the bypass list,
                // Find the list of services this file is forbidden from being sent to.
                // TODO: remove "or submission.params.ignore_dynamic_recursion_prevention" after assemblyline upgrade to version 4.6+
                let ignore_drop = task.submission.params.ignore_recursion_prevention;
                if !ignore_drop && !task.dynamic_recursion_bypass.contains(sha256) {
                    forbidden_services = task.find_recursion_excluded_services(sha256)
                }

                let schedule = self.core.services.build_schedule(
                    &task.submission.params,
                    &file_info.file_type,
                    file_depth,
                    Some(forbidden_services),
                    task.service_access_control.clone()
                ).context("build_schedule")?;
                let schedule_names = get_schedule_names(&schedule);
                trace_event!(task, "schedule_built", file sha256, "{:?}", schedule_names);
                task.file_schedules.insert(sha256.clone(), schedule_names.clone());
                schedule_names
            }
        };

        let deep_scan = task.submission.params.deep_scan;
        let ignore_filtering = task.submission.params.ignore_filtering;

        // Go through each round of the schedule removing complete/failed services
        // Break when we find a stage that still needs processing
        let mut outstanding: Vec<String> = Default::default();
        let mut started_stages = vec![];
        while !schedule.is_empty() && outstanding.is_empty() {
            let stage = schedule.remove(0);
            started_stages.push(stage.clone());

            for service_name in stage {
                // If the service terminated in an error, count the error and continue
                let key = (sha256.clone(), service_name.clone());
                if task.service_errors.contains_key(&key) {
                    continue
                }

                // If we have no error, and no result, its not finished
                let result = match task.service_results.get(&key) {
                    Some(result) => result,
                    None => {
                        outstanding.push(service_name);
                        continue
                    }
                };

                // if the service finished, count the score, and check if the file has been dropped
                if !ignore_filtering && result.drop {
                    // Clear out anything in the schedule after this stage
                    task.file_schedules.insert(sha256.clone(), started_stages.clone());
                    schedule.clear();
                }
            }
        }
        // debug!("[{sid}::{sha256}] outstanding services: {outstanding:?}");

        // Try to retry/dispatch any outstanding services
        if !outstanding.is_empty() {
            let mut sent = vec![];
            let mut enqueued = vec![];
            let mut running = vec![];
            let mut skipped = vec![];

            for service_name in outstanding {
                let service_queue = self.core.get_service_queue(&service_name);

                let key = (sha256.clone(), service_name.clone());
                // Check if the task is already running
                if task.running_services.contains_key(&key) {
                    running.push(service_name);
                    continue
                }

                // Check if this task is already sitting in queue
                if let Some((_, dispatch_key, last_check)) = task.queue_keys.get_mut(&key) {
                    if last_check.elapsed() < QUEUE_CHECK_INTERVAL {
                        enqueued.push(service_name);
                        continue
                    }
                    if service_queue.rank(dispatch_key).await?.is_some() {
                        *last_check = Instant::now();
                        enqueued.push(service_name);
                        continue
                    }
                }

                // If its not in queue already check we aren't dispatching anymore
                if task.submission.to_be_deleted {
                    skipped.push(service_name);
                    continue
                }

                // Check if we have attempted this too many times already.
                let attempts = task.service_attempts.entry(key.clone()).or_default();
                *attempts += 1;
                if *attempts > 3 {
                    self.retry_error(task, sha256, &service_name).await?;
                    continue
                }

                // check if the service is still enabled
                let service = match self.core.services.get(&service_name) {
                    Some(service) if service.enabled => service,
                    _ => continue
                };
                
                // Load the list of tags we will pass
                let mut tags = vec![];
                if service.uses_tags || service.uses_tag_scores {
                    if let Some(file_tags) = task.file_tags.get(sha256) {
                        for tag in file_tags.values() {
                            let short_type = match tag.tag_type.rsplit_once(".") {
                                Some((_, value)) => value.to_owned(),
                                None => tag.tag_type.clone(),
                            };

                            let score = if service.uses_tag_scores {
                                Some(tag.score)
                            } else {
                                None
                            };

                            tags.push(TagItem {
                                tag_type: tag.tag_type.clone(),
                                value: tag.value.clone(),
                                score,
                                short_type
                            });
                        }
                    }
                }

                // Load the temp submission data we will pass
                let mut temp_data = vec![];
                if service.uses_temp_submission_data {
                    // while we convert the map to a list of values capture any that we are monitoring
                    let mut monitored_keys: HashSet<&String> = HashSet::from_iter(service.monitored_keys.iter());
                    let mut monitored_values: HashMap<String, Option<serde_json::Value>> = Default::default();

                    // load a copy of the temporary data and convert it to a list of DataItems
                    if let Some(temporary) = task.file_temporary_data.get(sha256) {
                        for (key, value) in temporary.read() {
                            if monitored_keys.remove(&key) {
                                monitored_values.insert(key.clone(), Some(value.clone()));
                            }
                            temp_data.push(DataItem{
                                name: key,
                                value
                            });
                        }
                    }

                    // any keys that were not found in the temporary data should be filled with None to
                    // indicate their abscence from the temp_data list
                    for remaining_key in monitored_keys.drain() {
                        monitored_values.insert(remaining_key.clone(), None);
                    }

                    // mix in the ancestory data
                    if let Some(ancestry) = task.file_ancestry.get(sha256) {
                        temp_data.push(DataItem{
                            name: "ancestry".to_string(),
                            value: serde_json::json!(ancestry)
                        });
                    }

                    // if there are any monitored values create a monitoring entry
                    if !monitored_values.is_empty() {
                        task.set_monitoring_entry(sha256.clone(), service.name.clone(), monitored_values);
                    }
                }

                // Load the metadata we will pass
                let metadata = if service.uses_metadata {
                    task.submission.metadata.clone()
                } else {
                    Default::default()
                };

                // Mark this routing for the purposes of recursion prevention
                let prevented_services = self.core.services.expand_categories(service.recursion_prevention.clone());
                for service_name in prevented_services {
                    task.forbid_for_children(sha256.clone(), service_name);
                }

                let filename = task.file_names.get(sha256).cloned().unwrap_or_else(|| sha256.to_string());

                // Build the actual service dispatch message
                let config = Self::build_service_config(&service, &task.submission);
                let mut service_task = ServiceTask {
                    sid,
                    task_id: rand::rng().random(),
                    dispatcher: self.instance_id.clone(),
                    dispatcher_address: self.instance_address.clone(),
                    metadata,
                    min_classification: task.submission.classification.classification.clone(),
                    service_name: service_name.clone(),
                    service_config: config,
                    fileinfo: (*file_info).clone(),
                    filename,
                    depth: file_depth,
                    max_files: task.submission.params.max_extracted,
                    ttl: task.submission.params.ttl,
                    ignore_cache: task.submission.params.ignore_cache,
                    ignore_recursion_prevention: task.submission.params.ignore_recursion_prevention,
                    ignore_filtering,
                    tags,
                    temporary_submission_data: temp_data,
                    deep_scan,
                    priority: task.submission.params.priority as i32,
                    safelist_config: self.core.config.services.safelist.clone()
                };
                service_task.metadata.insert("dispatcher__".to_string(), self.instance_id.clone().into());
                service_task.metadata.insert("dispatcher_address__".to_string(), self.instance_address.clone().into());
                service_task.metadata.insert("task_id__".to_string(), service_task.task_id.to_string().into());

                // Its a new task, send it to the service
                let queue_key = service_queue.push(service_task.priority as f64, &service_task).await?;
                task.queue_keys.insert(key.clone(), (service_task, queue_key, Instant::now()));
                sent.push(service_name);
                task.service_logs.entry(key).or_default().push(format!("Submitted to queue at {}", chrono::Utc::now()));
            }

            if !sent.is_empty() || !enqueued.is_empty() || !running.is_empty() {
                // If we have confirmed that we are waiting, or have taken an action, log that.
                info!("[{sid}] File {sha256} sent to: {} already in queue for: {} running on: {}",
                      FormattedList(&sent), FormattedList(&enqueued), FormattedList(&running));
                trace_event!(task, "dispatch_file_result", file sha256, "sent to: {} already in queue for: {} running on: {}",
                             FormattedList(&sent), FormattedList(&enqueued), FormattedList(&running));
                return Ok(());
            } else if !skipped.is_empty() {
                // Not waiting for anything, and have started skipping what is left over
                // because this submission is terminated. Drop through to the base
                // case where the file is complete
                info!("[{sid}] File {sha256} skipping {}", FormattedList(&skipped));
            } else {
                // If we are not waiting, and have not taken an action, we must have hit the
                // retry limit on the only service running. In that case, we can move directly
                // onto the next stage of services, trigger disptach to run again ASAP.
                task.send_dispatch_action(DispatchAction::DispatchFile(task.submission.sid, sha256.clone()));
                return Ok(())
            }
        }

        increment!(self.counter, files_completed);
        if task.queue_keys.is_empty() && task.running_services.is_empty() {
            trace_event!(task, "file_finished", file sha256);
            info!("[{sid}] Finished processing file '{sha256}', checking if submission complete");
            task.send_dispatch_action(DispatchAction::Check(sid));
        } else {
            trace_event!(task, "file_finished", file sha256, "queued: {} running: {}", task.queue_keys.len(), task.running_services.len());
            info!("[{sid}] Finished processing file '{sha256}', submission incomplete (queued: {} running: {})",
                task.queue_keys.len(), task.running_services.len())
        }
        Ok(())
    }

    async fn get_fileinfo(&self, task: &mut SubmissionTask, sha256: &Sha256) -> Result<Option<Arc<FileInfo>>> {
        // First try to get the info from local cache
        if let Some(file_info) = task.file_info.get(sha256) {
            return Ok(file_info.clone());
        }

        // get the info from datastore
        let filestore_info = self.core.datastore.file.get(sha256, None).await.context("file.get")?;

        match filestore_info {
            None => {
                // Store an error and mark this file as unprocessable
                task.dropped_files.insert(sha256.clone());
                self._dispatching_error(task, &error::Error {
                    archive_ts: None,
                    created: chrono::Utc::now(),
                    expiry_ts: task.submission.expiry_ts,
                    response: error::Response {
                        message: format!("Couldn't find file info for {sha256} in submission {}", task.submission.sid).into(),
                        service_name: "Dispatcher".to_string(),
                        service_tool_version: None,
                        service_version: "4.0".to_string(),
                        status: error::Status::FailNonrecoverable,
                        service_debug_info: None,
                    },
                    sha256: sha256.clone(),
                    error_type: error::ErrorTypes::Unknown
                }).await?;
                task.file_info.insert(sha256.clone(), None);
                task.file_schedules.insert(sha256.clone(), vec![]);
                Ok(None)
            },
            Some(filestore_info) => {
                // Translate the file info format
                let file_info = Arc::new(FileInfo {
                    magic: filestore_info.magic,
                    md5: filestore_info.md5,
                    mime: filestore_info.mime,
                    sha1: filestore_info.sha1,
                    sha256: filestore_info.sha256,
                    size: filestore_info.size,
                    ssdeep: Some(filestore_info.ssdeep),
                    file_type: filestore_info.file_type,
                    tlsh: filestore_info.tlsh,
                    uri_info: filestore_info.uri_info
                });
                task.file_info.insert(sha256.clone(), Some(file_info.clone()));
                Ok(Some(file_info))
            }
        }
    }

    // Check if a submission is finished.
    //
    // :param task: Task object for the submission in question.
    // :return: true if submission has been finished.
    async fn check_submission(&self, task: &mut SubmissionTask) -> Result<bool> {
        let sid = task.submission.sid;

        // Track which files we have looked at already
        let mut checked = HashSet::<Sha256>::new();
        let mut unchecked: BTreeSet<Sha256> = task.file_depth.keys().cloned().collect();

        // Categorize files as pending/processing (can be both) all others are finished
        let mut pending_files = vec![];  // Files where we are missing a service and it is not being processed
        let mut processing_files = vec![];  // Files where at least one service is in progress/queued

        // Track information about the results as we hit them
        let mut file_scores = HashMap::<Sha256, i32>::new();

        // Make sure we have either a result or
        while let Some(sha256) = unchecked.pop_last() {
            checked.insert(sha256.clone());

            if task.dropped_files.contains(&sha256) {
                continue
            }

            let mut schedule = match task.file_schedules.get(&sha256) {
                Some(schedule) => schedule.clone(),
                None => {
                    pending_files.push(sha256);
                    continue
                }
            };

            while !schedule.is_empty() && !pending_files.contains(&sha256) && !processing_files.contains(&sha256) {
                let stage = schedule.remove(0);
                for service_name in stage {

                    // should be handled by plumber
                    // // Only active services should be in this dict, so if a service that was placed in the
                    // // schedule is now missing it has been disabled or taken offline.
                    // if self.core.services.get(service_name). {
                    // if not service:
                    //     continue
                    // }

                    // If there is an error we are finished with this service
                    let key = (sha256.clone(), service_name.clone());
                    if task.service_errors.contains_key(&key) {
                        continue
                    }

                    // if there is a result, then the service finished already
                    if let Some(result) = task.service_results.get(&key) {
                        if !task.submission.params.ignore_filtering && result.drop {
                            schedule.clear()
                        }

                        // Collect information about the result
                        *file_scores.entry(sha256.clone()).or_default() += result.score;
                        for (child, _) in &result.children {
                            if !checked.contains(child) {
                                unchecked.insert(child.clone());
                            }
                        }
                        continue
                    }

                    // If the file is in process, we may not need to dispatch it, but we aren't finished
                    // with the submission.
                    if task.running_services.contains_key(&key) {
                        processing_files.push(sha256.clone());
                        // another service may require us to dispatch it though so continue rather than break
                        continue
                    }

                    // Check if the service is in queue, and handle it the same as being in progress.
                    // Check this one last, since it can require a remote call to redis rather than checking a dict.
                    let service_queue = self.core.get_service_queue(&service_name);
                    if let Some((_, queue_key, last_check)) = task.queue_keys.get_mut(&key) {
                        if last_check.elapsed() < QUEUE_CHECK_INTERVAL {
                            processing_files.push(sha256.clone());
                            continue
                        }
                        if service_queue.rank(queue_key).await?.is_some() {
                            *last_check = Instant::now();
                            processing_files.push(sha256.clone());
                            continue
                        }
                    }

                    // Don't worry about pending files if we aren't dispatching anymore and they weren't caught
                    // by the prior checks for outstanding tasks
                    if task.submission.to_be_deleted {
                        break
                    }

                    // We check if it has already been dispatched before checking if its enabled 
                    // to let us catch any trailing results coming in. But we don't count the file as pending
                    // because we aren't going to _start_ processing on enabled. Plumber will handle the corner cases.
                    match self.core.services.get(&service_name) {
                        Some(service) if service.enabled => {},
                        _ => continue
                    };

                    // Since the service is not finished or in progress, it must still need to start
                    debug!("[{sid}] Not complete on account of {sha256} needing {service_name}");
                    pending_files.push(sha256.clone());
                    break
                }
            }
        }

        // Filter out things over the depth limit
        let depth_limit = self.core.config.submission.max_extraction_depth;
        pending_files.retain(|item| *task.file_depth.get(item).unwrap_or(&0) < depth_limit);

        // If there are pending files, then at least one service, on at least one
        // file isn't done yet, and hasn't been filtered by any of the previous few steps
        // poke those files.
        if !pending_files.is_empty() {
            trace_event!(task, "submission_check", "Dispatching {}", FormattedList(&pending_files));
            debug!("[{sid}] Dispatching {} files: {}", pending_files.len(), FormattedList(&pending_files));
            for file_hash in pending_files {
                task.send_dispatch_action(DispatchAction::DispatchFile(sid, file_hash));
            }
        } else if !processing_files.is_empty() {
            trace_event!(task, "submission_check", "Waiting for {}", FormattedList(&processing_files));
            debug!("[{sid}] Not finished waiting on {} files: {}", processing_files.len(), processing_files.len());
        } else {
            trace_event!(task, "submission_check", "Finished");
            debug!("[{sid}] Finalizing submission.");
            // accumulate the score, submissions with no results have no score
            // max_score = max(file_scores.values()) if file_scores else 0
            let max_score = file_scores.values().fold(0, |a, b| a.max(*b));
            self.finalize_submission(task, max_score, checked).await?;
            return Ok(true)
        }
        return Ok(false)
    }

    /// Prepare the service config that will be used downstream.
    ///
    /// v3 names: get_service_params get_config_data
    fn build_service_config(service: &Service, submission: &Submission) -> JsonMap {
        // Load the default service config
        let mut params = JsonMap::new();
        for x in &service.submission_params {
            params.insert(x.name.clone(), x.default.clone());
        }

        // Over write it with values from the submission
        if let Some(submission_params) = submission.params.service_spec.get(&service.name) {
            for (key, value) in submission_params {
                params.insert(key.clone(), value.clone());
            }
        }
        return params
    }

    /// All of the services for all of the files in this submission have finished or failed.
    ///
    /// Update the records in the datastore, and flush the working data from redis.
    async fn finalize_submission(&self, task: &mut SubmissionTask, max_score: i32, file_list: HashSet<Sha256>) -> Result<()> {
        let submission = &mut task.submission;
        let sid = submission.sid;
        // if self.tasks.pop(task.sid, None) {

        let results: Vec<Wildcard> = task.service_results.values().map(|summary| summary.key.clone().into()).collect();
        let mut errors: Vec<String> = task.service_errors.values().cloned().collect();
        errors.extend(task.extra_errors.clone());

        let error_count = errors.len() as i32;
        let file_count = file_list.len() as i32;
        let result_count = results.len();

        submission.classification = ExpandingClassification::new(submission.params.classification.as_str().to_string(), &self.core.classification_parser)?;
        submission.error_count = error_count;
        submission.errors = errors;
        submission.file_count = file_count;
        submission.results = results;
        submission.max_score = max_score;
        submission.state = SubmissionState::Completed;
        submission.times.completed = Some(chrono::Utc::now());
        self.core.datastore.submission.save(&sid.to_string(), submission, None, None).await?;

        self._cleanup_submission(task).await?;
        info!("[{sid}] Completed; files: {file_count} results: {result_count} errors: {error_count} score: {max_score}");
        Ok(())
    }

    /// Clean up code that is the same for canceled and finished submissions
    async fn _cleanup_submission(&self, task: &SubmissionTask) -> Result<()> {
        let submission = &task.submission;
        let sid = submission.sid;

        if let Some(queue_name) = &task.completed_queue {
            self.core.redis_volatile.queue(queue_name.clone(), None).push(submission).await?;
        }

        // Send complete message to any watchers.
        let watcher_list = self._watcher_list(sid);
        for w in watcher_list.members().await? {
            self.core.redis_volatile.queue(w, None).push(&WatchQueueMessage::stop()).await?;
        }

        // Don't run post processing and traffic notifications if the submission is terminated
        if !task.submission.to_be_deleted {
            // Pull the tags keys and values into a searchable form
            let mut tags = JsonMap::new();

            fn insert(target: &mut JsonMap, full_key: &str, key: &str, value: &TagValue) {
                match key.split_once(".") {
                    Some((root, key)) => {
                        let entry = target.entry(root).or_insert(serde_json::Value::Object(Default::default()));
                        if let Some(values) = entry.as_object_mut() {
                            insert(values, full_key, key, value);
                        } else {
                            error!("Unable to place tag [{full_key}]: scope conflict, expected object");
                        }
                    },
                    None => {
                        let entry = target.entry(key).or_insert(serde_json::Value::Array(Default::default()));
                        if let Some(values) = entry.as_array_mut() {
                            values.push(json!(value));
                        } else {
                            error!("Unable to place tag [{full_key}]: scope conflict, expected list");
                        }
                    }
                }
            }

            for file_tags in task.file_tags.values() {
                for _t in file_tags.values() {
                    insert(&mut tags, &_t.tag_type, &_t.tag_type, &_t.value);
            //         tags.push(serde_json::json!({
            //             "value": _t.value,
            //             "type": _t.tag_type
            //         }))
                }
            }
            let tags = serde_json::Value::Object(tags);

            // Send the submission for alerting or resubmission
            debug!("submission tags: {tags}");
            self.postprocess_worker.process(submission, tags, submission.params.auto_archive).await?;

            // Write all finished submissions to the traffic queue
            self.traffic_queue.publish(&SubmissionMessage::completed(submission.into(), "dispatcher".to_owned())).await?;
        }

        // Clear the timeout watcher
        watcher_list.delete().await?;
        let sid_str = sid.to_string();
        self.active_submissions.pop(&sid_str).await?;
        self.submissions_assignments.pop(&sid_str).await?;
        self.process_queues.write().await.remove(&sid);

        // Count the submission as 'complete' either way
        increment!(self.counter, submissions_completed);
        Ok(())
    }

    async fn retry_error(&self, task: &mut SubmissionTask, sha256: &Sha256, service_name: &str) -> Result<()> {
        let sid = task.submission.sid;
        warn!("[{sid}/{sha256}] {service_name} marking task failed: TASK PREEMPTED ");

        // Pull out any details to include in error message
        let mut error_details = "The number of retries has passed the limit.".to_string();
        let key = (sha256.clone(), service_name.to_string());
        if let Some(logs) = task.service_logs.get(&key) {
            error_details.push_str("\n\n");
            error_details.push_str(&logs.join("\n"));
        }

        let ttl = task.submission.params.ttl;
        let expiry_ts = if ttl != 0 {
            Some(chrono::Utc::now() + chrono::Duration::days(ttl as i64))
        } else {
            None
        };

        let error = error::Error {
            archive_ts: None,
            created: chrono::Utc::now(),
            expiry_ts,
            response: error::Response {
                message: error_details.into(),
                service_name: service_name.to_string(),
                service_version: "0".to_string(),
                status: error::Status::FailNonrecoverable,
                service_debug_info: None,
                service_tool_version: None
            },
            sha256: sha256.clone(),
            error_type: error::ErrorTypes::TaskPreempted,
        };

        let error_key = error.build_key(None, None)?;
        self.core.datastore.error.save(&error_key, &error, None, None).await?;

        task.queue_keys.remove(&key);
        task.running_services.remove(&key);
        task.service_errors.insert(key, error_key.clone());

        self.core.export_metrics_once(
            service_name,
            &ServiceMetrics{ fail_nonrecoverable: 1, ..Default::default()},
            Some("dispatcher"),
            Some("service")
        ).await?;

        // Send the result key to any watching systems
        let msg = WatchQueueMessage{ status: WatchQueueStatus::Fail, cache_key: Some(error_key) };
        for w in self._watcher_list(task.submission.sid).members().await? {
            self.core.redis_volatile.queue(w, None).push(&msg).await?;
        }
        Ok(())
    }


    async fn process_service_result(&self, task: &mut SubmissionTask, data: ServiceResult) -> Result<()> {
        // let submission: &Submission = &task.submission;
        let sid = task.submission.sid;
        let ServiceResult {
            sid: _,
            service_name,
            service_version,
            service_tool_version,
            expiry_ts,
            sha256,
            result_summary: summary,
            tags,
            temporary_data,
            extracted_names,
            dynamic_recursion_bypass,
            extra_errors,
        } = data;

        trace_event!(task, "process_result", file &sha256, service &service_name, "Processing result {}", summary.key);

        // add extra errors from the service the global error list for the submission
        task.extra_errors.extend(extra_errors);

        // check for/clear old partial results
        let mut force_redispatch = HashSet::new();
        if summary.partial {
            info!("[{sid}/{sha256}] {service_name} returned partial results");
            if task.partial_result(sha256.clone(), service_name.clone()) {
                force_redispatch.insert(sha256.clone());
            }
        } else {
            task.clear_monitoring_entry(sha256.clone(), service_name.clone());
        }

        // Add SHA256s of files that allowed to run regardless of Dynamic Recursion Prevention
        for item in dynamic_recursion_bypass {
            task.dynamic_recursion_bypass.insert(item);
        }

        // Clear logs we won't need to report
        let key = (sha256.clone(), service_name.clone());
        task.service_logs.remove(&key);

        // Don't process duplicates
        if task.service_results.contains_key(&key) {
            return self.dispatch_file(task, &sha256).await;
        }

        // remove pending messages related to this task
        if let Some((_, queue_key, _)) = task.queue_keys.remove(&key) {
            self.core.get_service_queue(&service_name).remove(&queue_key).await?;
        }

        // Let the logs know we have received a result for this task
        if summary.drop {
            debug!("[{sid}/{sha256}] {service_name} succeeded. Result will be stored in {} but processing will stop after this service.", summary.key);
        } else {
            debug!("[{sid}/{sha256}] {service_name} succeeded. Result will be stored in {}", summary.key);
        }

        // The depth is set for the root file, and for all extracted files whether we process them or not
        let file_depth = match task.file_depth.get(&sha256) {
            None => {
                warn!("[{sid}/{sha256}] {service_name} returned result for file that wasn't requested.");
                return self.dispatch_file(task, &sha256).await;
            }
            Some(depth) => *depth,
        };

        // Update score of tag as it moves through different services
        for (key, value) in tags {
            let tag_collection = task.file_tags.entry(sha256.clone()).or_default();
            match tag_collection.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().score += value.score;
                },
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }

        // Update the temporary data table for this file
        let max_temp_data_length = self.core.config.submission.max_temp_data_length as usize;
        let mut changed_keys = vec![];
        if let Some(existing) = task.file_temporary_data.get_mut(&sha256) {
            for (key, value) in temporary_data {
                if estimate_size(&value) <= max_temp_data_length && existing.set_value(&key, value) {
                    changed_keys.push(key);
                }
            }
        }

        for key in changed_keys {
            for hash in task.temporary_data_changed(&key) {
                force_redispatch.insert(hash);
            }
        }

        // // Update children to include parent_relation, likely EXTRACTED
        // if summary.children and isinstance(summary.children[0], str):
        //     old_children = typing.cast(list[str], summary.children)
        //     summary.children = [(c, 'EXTRACTED') for c in old_children]

        // Record the result as a summary
        if force_redispatch.is_empty() {
            task.service_results.insert(key, summary.clone());
        }
        task.register_children(&sha256, summary.children.iter().map(|row| row.0.clone()));

        // Set the depth of all extracted files, even if we won't be processing them
        let depth_limit = self.core.config.submission.max_extraction_depth;
        let new_depth = file_depth + 1;
        for (extracted_sha256, _) in &summary.children {
            task.file_depth.entry(extracted_sha256.clone()).or_insert(new_depth);
            if let Some(extracted_name) = extracted_names.get(extracted_sha256) {
                if !task.file_names.contains_key(extracted_sha256) {
                    task.file_names.insert(extracted_sha256.clone(), extracted_name.clone());
                }
            }
        }

        // Send the extracted files to the dispatcher
        if new_depth < depth_limit {
            for (extracted_sha256, parent_relation) in summary.children {

                if task.dropped_files.contains(&extracted_sha256) || task.active_files.contains(&extracted_sha256) {
                    continue
                }

                if task.active_files.len() as i32 > task.submission.params.max_extracted {
                    info!("[{sid}] hit extraction limit, dropping {extracted_sha256}");
                    task.dropped_files.insert(extracted_sha256.clone());
                    self._dispatching_error(task, &error::Error {
                        archive_ts: None,
                        created: chrono::Utc::now(),
                        expiry_ts,
                        response: error::Response {
                            message: format!("Too many files extracted for submission {sid} {extracted_sha256} extracted by {service_name} will be dropped").into(),
                            service_name: service_name.clone(),
                            service_tool_version: service_tool_version.clone(),
                            service_version: service_version.clone(),
                            status: error::Status::FailNonrecoverable,
                            service_debug_info: None
                        },
                        sha256: extracted_sha256,
                        error_type: error::ErrorTypes::MaxFilesReached
                    }).await?;
                    continue
                }

                task.active_files.insert(extracted_sha256.clone());

                let file_info = self.get_fileinfo(task, &extracted_sha256).await?;
                let parent_ancestry = task.file_ancestry.get(&sha256).cloned().unwrap_or_default();
                let existing_ancestry = task.file_ancestry.entry(extracted_sha256.clone()).or_default();

                let file_type = match file_info {
                    Some(file_info) => file_info.file_type.clone(),
                    None => "NOT_FOUND".to_owned()
                };
                let current_ancestry_node = AncestoryEntry {
                    file_type,
                    parent_relation,
                    sha256: extracted_sha256.to_owned()
                };

                if let Some(parent_data) = task.file_temporary_data.get(&sha256) {
                    task.file_temporary_data.insert(extracted_sha256.clone(), parent_data.child());
                    for mut ancestry in parent_ancestry {
                        ancestry.push(current_ancestry_node.clone());
                        existing_ancestry.push(ancestry);
                    }
                }

                task.send_dispatch_action(DispatchAction::DispatchFile(sid, extracted_sha256));
            }
        } else {
            for (extracted_sha256, _) in summary.children {
                task.dropped_files.insert(extracted_sha256.clone());
                self._dispatching_error(task, &error::Error {
                    archive_ts: None,
                    created: chrono::Utc::now(),
                    expiry_ts,
                    response: error::Response {
                        message: format!("{service_name} has extracted a file {extracted_sha256} beyond the depth limits").into(),
                        service_name: service_name.clone(),
                        service_tool_version: service_tool_version.clone(),
                        service_version: service_version.clone(),
                        status: error::Status::FailNonrecoverable,
                        service_debug_info: None,
                    },
                    sha256: extracted_sha256,
                    error_type: error::ErrorTypes::MaxDepthReached,
                }).await?;
            }
        }

        // Check if its worth trying to run the next stage
        // Not worth running if we know we are waiting for another service
        if task.running_services.keys().any(|(_s, _)| *_s == sha256) {
            let services: Vec<&String> = task.running_services.keys().filter(|(_s, _)| *_s == sha256).map(|(_, _s)|_s).collect();
            debug!("[{sid} :: {sha256}] Delaying dispatching, already being processed by {}", FormattedList(&services));
            return Ok(())
        }
        // Not worth running if we know we have services in queue
        if task.queue_keys.keys().any(|(_s, _)| *_s == sha256) {
            let services: Vec<&String> = task.queue_keys.keys().filter(|(_s, _)| *_s == sha256).map(|(_, _s)|_s).collect();
            debug!("[{sid} :: {sha256}] Delaying dispatching, already queued for {}", FormattedList(&services));
            return Ok(())
        }

        // Check if its worth trying to run the next stage
        // Not worth running if we know we are waiting for another service
        if !task.running_services.keys().any(|(hash, _)| hash == &sha256) {
            force_redispatch.insert(sha256.clone());
        }

        // Not worth running if we know we have services in queue
        if !task.queue_keys.keys().any(|(hash, _)| hash == &sha256) {
            force_redispatch.insert(sha256);
        }

        // Try to run the next stage
        for sha256 in force_redispatch {
            self.dispatch_file(task, &sha256).await?;
        }
        Ok(())
    }

    async fn _dispatching_error(&self, task: &mut SubmissionTask, error: &error::Error) -> Result<()> {
        let error_key = error.build_key(None, None)?;
        task.extra_errors.push(error_key.clone());
        self.core.datastore.error.save(&error_key, error, None, None).await?;
        let msg = WatchQueueMessage{
            status: WatchQueueStatus::Fail,
            cache_key: Some(error_key)
        };
        for w in self._watcher_list(task.submission.sid).members().await? {
            self.core.redis_volatile.queue(w, None).push(&msg).await?;
        }
        Ok(())
    }

    async fn process_service_error(&self, task: &mut SubmissionTask, error_key: &str, error: error::Error) -> Result<()> {
        info!("[{}] Error from service {} on {}", task.submission.sid, error.response.service_name, error.sha256);
        trace_event!(task, "process_error", file &error.sha256, service &error.response.service_name, "Service error {}", error_key);

        let key = (error.sha256.clone(), error.response.service_name);
        if matches!(error.response.status, error::Status::FailNonrecoverable) {
            task.service_logs.remove(&key);
            task.service_errors.insert(key, error_key.to_string());
        } else {
            task.service_logs.entry(key).or_default().push(format!("Service error: {}", error.response.message));
        }
        task.send_dispatch_action(DispatchAction::DispatchFile(task.submission.sid, error.sha256));
        Ok(())
    }

    async fn timeout_service(&self, task: &mut SubmissionTask, sha256: &Sha256, service_name: &str, worker_id: &str) -> Result<()> {
        // We believe a service task has timed out, try and read it from running tasks
        // If we can't find the task in running tasks, it finished JUST before timing out, let it go
        let sid = task.submission.sid;
        let key = (sha256.clone(), service_name.to_string());
        let mut service_task = None;
        if let Some((_t, _, _)) = task.queue_keys.remove(&key) {
            service_task = Some(_t);
        }
        if let Some(_t) = task.running_services.remove(&key) {
            service_task = Some(_t);
        }

        // let task_key = ServiceTask::make_key(sid, service_name, sha256);
        // task.running_services
        // let service_task = self.running_tasks.pop(&task_key).await?;
        // && !task.running_services.contains(&key)

        if service_task.is_none() {
            debug!("[{sid}] Service {service_name} timed out on {sha256} but task isn't running.");
            return Ok(())
        }

        // We can confirm that the task is ours now, even if the worker finished, the result will be ignored
        info!("[{sid}] Service {service_name} running on {worker_id} timed out on {sha256}.");
        task.send_dispatch_action(DispatchAction::DispatchFile(sid, sha256.clone()));

        // We push the task of killing the container off on the scaler, which already has root access
        // the scaler can also double check that the service name and container id match, to be sure
        // we aren't accidentally killing the wrong container
        self.scaler_timeout_queue.push(&KillContainerCommand{
            service: service_name.to_string(),
            container: worker_id.to_string()
        }).await?;

        // Report to the metrics system that a recoverable error has occurred for that service
        self.core.export_metrics_once(
            service_name,
            &ServiceMetrics {fail_recoverable: 1, ..Default::default()},
            Some(worker_id),
            Some("service")
        ).await?;
        Ok(())
    }

}

//     def stop(self):
//         super().stop()
//         self.service_change_watcher.stop()
//         self.postprocess_worker.stop()

//     def _handle_status_change(self, status: Optional[bool]):
//         super()._handle_status_change(status)

//         # If we may have lost redis connection check all of our submissions
//         if status is None:
//             for sid in self.tasks.keys():
//                 _q = self.find_process_queue(sid)
//                 _q.put(DispatchAction(kind=Action.check_submission, sid=sid))




struct QuotaGuard {
    sid: Sid,
    user: String,
    tracker: UserQuotaTracker
}

impl QuotaGuard {
    fn new(sid: Sid, user: String, tracker: UserQuotaTracker) -> Self {
        Self { sid, user, tracker }
    }
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        let sid = self.sid;
        let user = self.user.clone();
        let tracker = self.tracker.clone();
        tokio::spawn(async move {
            match tracker.end(&user).await {
                Ok(_) => info!("[{sid}] Submission no longer counts toward {user} quota"),
                Err(err) => error!("Error clearing quota entry for {user}: {err}"),
            }
        });
    }
}

struct UserCache {
    datastore: Arc<Elastic>,
    cache: Mutex<HashMap<String, (Arc<User>, Instant)>>,
}

impl UserCache {
    pub fn new(datastore: Arc<Elastic>) -> Self {
        Self {
            datastore,
            cache: Mutex::new(Default::default()),
        }
    }

    pub async fn get(&self, name: &str) -> Result<Option<Arc<User>>> {
        if let Some((user, time)) = self.cache.lock().get(name) {
            if time.elapsed() < USER_CACHE_TIME {
                return Ok(Some(user.clone()))
            }
        }

        if let Some((user, _)) = self.datastore.user.get_if_exists(name, None).await.context("fetching user data")? {
            let user = Arc::new(user);
            self.cache.lock().insert(name.to_string(), (user.clone(), Instant::now()));
            return Ok(Some(user))
        }
        return Ok(None)
    }
}


/// Estimate the size an object would take in json
/// this is for enforcing size limits and can undercount a little bit if it needs to 
/// should never be used when the accurate size of the data is important
pub fn estimate_size(value: &serde_json::Value) -> usize {
    match value {
        // keywords have a fixed length
        serde_json::Value::Null => 4,
        serde_json::Value::Bool(true) => 4,
        serde_json::Value::Bool(false) => 5,
        // if a number can fit in an i64 use log to figure out its written size, otherwise fallback to generating the string
        serde_json::Value::Number(number) => {
            if let Some(num) = number.as_i64() {
                if num < 0 {
                    2 + (-num).ilog10() as usize
                } else {
                    1 + num.checked_ilog10().unwrap_or_default() as usize
                }
            } else {
                number.to_string().len()
            }
        },
        // strings are their own length plus 2 (if we ignore escaping)
        serde_json::Value::String(value) => value.len() + 2,
        // array is 2 (open close brackets) + the sum of (item lengths + 1 each (commas)) - 1 for extra comma
        serde_json::Value::Array(values) => {
            values.iter().map(estimate_size).fold(1 + values.len(), |a, b| a + b)
        },
        // similar to array but add in key size same as string + 1 for the :
        serde_json::Value::Object(map) => {
            map.iter().map(|(k, v)| 3 + k.len() + estimate_size(v)).fold(1 + map.len(), |a, b| a + b)
        },
    }
}