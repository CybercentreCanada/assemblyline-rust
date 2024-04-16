pub mod interface;
mod processing;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use assemblyline_models::datastore::Submission;
use assemblyline_models::messages::task::{Task, TaskSignature, TagItem, TaskToken};
use assemblyline_models::{Sha256, Sid};
use assemblyline_models::config::Config;
use chrono::{DateTime, Utc};
use log::error;
use redis_objects::PriorityQueue;
use serde::{Serialize, Deserialize};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{oneshot, RwLock, mpsc, Mutex};
use tokio::time::MissedTickBehavior;

use crate::elastic::Elastic;
use crate::error::Result;

use self::interface::{start_interface, TaskResult, TaskError};
pub use self::interface::{DispatchStatusMessage, LoadInfo};

struct Flags {
    running: std::sync::atomic::AtomicBool,
    notification: tokio::sync::Notify,
}

impl Flags {
    pub fn new() -> Self {
        Self {
            running: std::sync::atomic::AtomicBool::new(true),
            notification: tokio::sync::Notify::new(),
        }
    }

    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Release);
        self.notification.notify_waiters();
    }
}

// enum StartMessage {
//     ExpectStart(TaskSignature, oneshot::Sender<String>),
//     Start{
//         task: TaskSignature,
//         worker: String,
//         response: oneshot::Sender<bool>,
//     }
// }

enum ResultResponse {
    Result(TaskResult),
    Error(TaskError),
}

// enum ResultMessage {
//     ExpectResponse(TaskSignature, String, oneshot::Sender<ResultResponse>),
//     Response(TaskSignature, String, ResultResponse)
// }

//     def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None,
//                  config=None, counter_name: str = 'dispatcher'):
//         super().__init__('assemblyline.dispatcher', config=config, datastore=datastore,
//                          redis=redis, redis_persist=redis_persist, logger=logger)

//         # Load the datastore collections that we are going to be using
//         self.instance_id = uuid.uuid4().hex
//         self.tasks: dict[str, SubmissionTask] = {}
//         self.finalizing = threading.Event()
//         self.finalizing_start = 0.0

//         # Build some utility classes
//         self.scheduler = Scheduler(self.datastore, self.config, self.redis)
//         self.running_tasks = Hash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)
//         self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)

//         self.classification_engine = get_classification()

//         # Output. Duplicate our input traffic into this queue so it may be cloned by other systems
//         self.traffic_queue = CommsQueue('submissions', self.redis)
//         self.quota_tracker = UserQuotaTracker('submissions', timeout=60 * 60, host=self.redis_persist)
//         self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)

//         # Table to track the running dispatchers
//         self.dispatchers_directory: Hash[int] = Hash(DISPATCH_DIRECTORY, host=self.redis_persist)
//         self.dispatchers_directory_finalize: Hash[int] = Hash(DISPATCH_DIRECTORY_FINALIZE, host=self.redis_persist)
//         self.running_dispatchers_estimate = 1

//         # Tables to track what submissions are running where
//         self.active_submissions = Hash(DISPATCH_TASK_ASSIGNMENT+self.instance_id, host=self.redis_persist)
//         self.submissions_assignments = Hash(DISPATCH_TASK_HASH, host=self.redis_persist)
//         self.ingester_scanning = Hash('m-scanning-table', self.redis_persist)

//         # Communications queues
//         self.start_queue: NamedQueue[tuple[str, str, str, str]] =\
//             NamedQueue(DISPATCH_START_EVENTS+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
//         self.result_queue: NamedQueue[dict] =\
//             NamedQueue(DISPATCH_RESULT_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
//         self.command_queue: NamedQueue[dict] =\
//             NamedQueue(DISPATCH_COMMAND_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)

//         # Publish counters to the metrics sink.
//         self.counter = MetricsFactory(metrics_type='dispatcher', schema=Metrics, name=counter_name,
//                                       redis=self.redis, config=self.config)

//         self.apm_client = None
//         if self.config.core.metrics.apm_server.server_url:
//             elasticapm.instrument()
//             self.apm_client = get_apm_client("dispatcher")

//         self._service_timeouts: TimeoutTable[tuple[str, str, str], str] = TimeoutTable()
//         self._submission_timeouts: TimeoutTable[str, None] = TimeoutTable()

//         # Setup queues for work to be divided into
//         self.process_queues: list[PriorityQueue[DispatchAction]] = [PriorityQueue() for _ in range(RESULT_THREADS)]
//         self.queue_ready_signals: list[threading.Semaphore] = [threading.Semaphore(MAX_RESULT_BUFFER)
//                                                                for _ in range(RESULT_THREADS)]

//         # Queue of finished submissions/errors waiting to be saved into elastic
//         self.finalize_queue = Queue()
//         self.error_queue: Queue[tuple[str, Error]] = Queue()

//         # Queue to hold of service timeouts that need to be processed
//         # They will be held in this queue until results in redis are
//         # already processed
//         self.timeout_queue: Queue[DispatchAction] = Queue()

//         # Utility object to handle post-processing actions
//         self.postprocess_worker = ActionWorker(cache=False, config=self.config, datastore=self.datastore,
//                                                redis_persist=self.redis_persist)

//         # Update bad sid list
//         self.redis_bad_sids = Set(BAD_SID_HASH, host=self.redis_persist)
//         self.bad_sids: set[str] = set(self.redis_bad_sids.members())

//         # Event Watchers
//         self.service_change_watcher = EventWatcher(self.redis, deserializer=ServiceChange.deserialize)
//         self.service_change_watcher.register('changes.services.*', self._handle_service_change_event)


struct DispatcherSession {
    config: Config,
    elastic: Elastic,
    redis_volatile: redis_objects::RedisObjects,
    redis_persistent: redis_objects::RedisObjects,
    flags: Flags,
    counters: redis_objects::counters::AutoExportingMetrics<>,
    // self.counter = MetricsFactory(metrics_type='dispatcher', schema=Metrics, name=counter_name, redis=self.redis, config=self.config)

    active: RwLock<HashMap<Sid, tokio::task::JoinHandle<()>>>,
    finished: RwLock<HashMap<Sid, Submission>>,

    // start_messages: mpsc::Sender<StartMessage>,
    // result_messages: mpsc::Sender<ResultMessage>,
}

impl DispatcherSession {
    pub fn get_service_queue(&self, service: &str) -> PriorityQueue<TaskToken> {
        // return PriorityQueue(service_queue_name(service), redis)
        todo!();
    }

    pub async fn register_task(&self, task: Task) -> oneshot::Receiver<ServiceStart> {
        todo!()
    }
}

struct ServiceStart {
    worker: String,
    result: oneshot::Receiver<ResultResponse> 
}

pub async fn launch(config: Config, elastic: Elastic) -> Result<()> {
    // create channels
    // let (start_messages, start_message_recv) = mpsc::channel(1024);
    // let (result_messages, result_message_recv) = mpsc::channel(1024);

    // create session
    let session = Arc::new(DispatcherSession {
        config,
        flags: Flags::new(),
        elastic,
        active: RwLock::new(Default::default()),
        finished: RwLock::new(Default::default()),
        // start_messages,
        // result_messages,
        redis_volatile: todo!(),
        redis_persistent: todo!(),
    });

    // setup exit conditions
    let mut term_signal = signal(SignalKind::terminate())?;
    let term_session = session.clone();
    tokio::spawn(async move {
        term_signal.recv().await;
        term_session.flags.stop();
    });

    // start API
    let api = tokio::spawn(start_interface(session.clone()));

    // launch worker that handles start messages
    tokio::spawn(start_message_broker(session.clone(), start_message_recv));

    // launch worker that handles message broker
    tokio::spawn(result_message_broker(session, result_message_recv));

    // wait for the api
    api.await.expect("API task failed")
}

async fn start_message_broker(session: Arc<DispatcherSession>, queue: mpsc::Receiver<StartMessage>) {
    let queue = Arc::new(Mutex::new(queue));
    loop {
        match tokio::spawn(_start_message_broker(session.clone(), queue.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in start_message_broker {err}"); },
            Err(err) => { error!("Task error on start_message_broker {err}"); },
        }
    }
}

async fn _start_message_broker(session: Arc<DispatcherSession>, queue: Arc<Mutex<mpsc::Receiver<StartMessage>>>) -> Result<()> {
    let mut queue = queue.lock().await;
    let mut cleanup_interval = tokio::time::interval(tokio::time::Duration::from_secs(60 * 5));
    let mut message_table: HashMap<TaskSignature, oneshot::Sender<String>> = Default::default();
    loop {
        tokio::select! {
            message = queue.recv() => {
                let message = match message {
                    Some(message) => message,
                    None => return Ok(()),
                };
                match message {
                    StartMessage::ExpectStart(task, response) => {
                        message_table.insert(task, response);
                    },
                    StartMessage::Start { task, worker, response } => {
                        match message_table.remove(&task) {
                            Some(worker_sink) => {
                                if let Ok(_) = worker_sink.send(worker) {
                                    response.send(true);
                                } else {
                                    response.send(false);
                                }
                            }
                            None => {
                                response.send(false);
                            }
                        }
                    },
                }
            }
            _ = cleanup_interval.tick() => {
                message_table.retain(|_key, value| !value.is_closed())
            }
        }
    }
}

async fn result_message_broker(session: Arc<DispatcherSession>, queue: mpsc::Receiver<ResultMessage>) {
    let queue = Arc::new(Mutex::new(queue));
    loop {
        match tokio::spawn(_result_message_broker(session.clone(), queue.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in result_message_broker {err}"); },
            Err(err) => { error!("Task error on result_message_broker {err}"); },
        }
    }
}

async fn _result_message_broker(session: Arc<DispatcherSession>, queue: Arc<Mutex<mpsc::Receiver<ResultMessage>>>) -> Result<()> {
    let mut queue = queue.lock().await;
    let mut cleanup_interval = tokio::time::interval(tokio::time::Duration::from_secs(60 * 5));
    let mut message_table: HashMap<TaskSignature, (String, oneshot::Sender<ResultResponse>)> = Default::default();
    loop {
        tokio::select! {
            message = queue.recv() => {
                let message = match message {
                    Some(message) => message,
                    None => return Ok(()),
                };
                match message {
                    ResultMessage::ExpectResponse(task, worker, response) => {
                        message_table.insert(task, (worker, response));
                    },
                    ResultMessage::Response(task, worker, response) => {
                        if let std::collections::hash_map::Entry::Occupied(entry) = message_table.entry(task) {
                            if entry.get().0 == worker {
                                let (_, sink) = entry.remove();
                                sink.send(response);
                            }
                        }
                    },
                }
            }
            _ = cleanup_interval.tick() => {
                message_table.retain(|_key, value| !value.1.is_closed())
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractionSummary {
    pub relation: String,
    pub sha256: Sha256,
    pub name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResultSummary {
    key: String,
    drop: bool,
    #[serde(default)]
    partial: bool,
    score: i32,
    children: Vec<ExtractionSummary>,
}

/// Only the uniquely countable parts of a tag, all the non-unique components (just score AFAIK)
/// are split out. This creates a key that we can use to capture all unique key/value tag pairs
/// and accumulate score over them. TagItem is the type for transmitting this data between 
/// processes as all the data is in one place
#[derive(PartialEq, Eq, Hash)]
pub struct TagData {
    /// Type of tag item
    pub tag_type: String,
    /// Value of tag item
    pub value: String,    
}

impl TagData {
    fn split(item: TagItem) -> (TagData, Option<i32>) {
        let TagItem{tag_type, value, score} = item;
        (TagData {tag_type, value}, score)
    }
}

/// A collection where we can accumulate tag information
pub struct TagCollection (HashMap<TagData, Option<i32>>);

impl TagCollection {

    pub fn update(&mut self, other: Vec<TagItem>) { 
        for tag in other.into_iter() {
            let (tag, score) = TagData::split(tag);
            let entry = self.0.entry(tag).or_default();
            if let Some(score) = score {
                *entry = Some(entry.unwrap_or_default() + score)
            }
        }
    }
}

//         # If the dispatcher is exiting cleanly remove as many tasks from the service queues as we can
//         service_queues = {}
//         for task in self.tasks.values():
//             for (_sha256, service_name), dispatch_key in task.queue_keys.items():
//                 try:
//                     s_queue = service_queues[service_name]
//                 except KeyError:
//                     s_queue = get_service_queue(service_name, self.redis)
//                     service_queues[service_name] = s_queue
//                 s_queue.remove(dispatch_key)


//     def work_guard(self):
//         check_interval = GUARD_TIMEOUT/8
//         old_value = int(time.time())
//         self.dispatchers_directory.set(self.instance_id, old_value)

//         try:
//             while self.sleep(check_interval):
//                 cpu_mark = time.process_time()
//                 time_mark = time.time()

//                 # Increase the guard number
//                 gap = int(time.time() - old_value)
//                 updated_value = self.dispatchers_directory.increment(self.instance_id, gap)

//                 # If for whatever reason, there was a moment between the previous increment
//                 # and the one before that, that the gap reached the timeout, someone may have
//                 # started stealing our work. We should just exit.
//                 if time.time() - old_value > GUARD_TIMEOUT:
//                     self.log.warning(f'Dispatcher closing due to guard interval failure: '
//                                      f'{time.time() - old_value} > {GUARD_TIMEOUT}')
//                     self.stop()
//                     return

//                 # Everything is fine, prepare for next round
//                 old_value = updated_value

//                 self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//                 self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)
//         finally:
//             if not self.running:
//                 self.dispatchers_directory.pop(self.instance_id)
//                 self.dispatchers_directory_finalize.pop(self.instance_id)

//     def work_thief(self):

//         # Clean up the finalize list once in a while
//         for id, timestamp in self.dispatchers_directory_finalize.items().items():
//             if int(time.time()) - timestamp > DAY_IN_SECONDS:
//                 self.dispatchers_directory_finalize.pop(id)

//         # Keep a table of the last recorded status for other dispatchers
//         last_seen = {}

//         try:
//             while self.sleep(GUARD_TIMEOUT / 4):
//                 cpu_mark = time.process_time()
//                 time_mark = time.time()

//                 # Load guards
//                 finalizing = self.dispatchers_directory_finalize.items()
//                 last_seen.update(self.dispatchers_directory.items())

//                 # List all dispatchers with jobs assigned
//                 for raw_key in self.redis_persist.keys(TASK_ASSIGNMENT_PATTERN):
//                     key: str = raw_key.decode()
//                     key = key[len(DISPATCH_TASK_ASSIGNMENT):]
//                     if key not in last_seen:
//                         last_seen[key] = time.time()
//                 self.running_dispatchers_estimate = len(set(last_seen.keys()) - set(finalizing.keys()))

//                 self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//                 self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//                 # Check if any of the dispatchers
//                 if last_seen:
//                     oldest = min(last_seen.keys(), key=lambda _x: last_seen[_x])
//                     if time.time() - last_seen[oldest] > GUARD_TIMEOUT:
//                         self.steal_work(oldest)
//                         last_seen.pop(oldest)

//         finally:
//             if not self.running:
//                 self.dispatchers_directory.pop(self.instance_id)
//                 self.dispatchers_directory_finalize.pop(self.instance_id)

//     def steal_work(self, target):
//         target_jobs = Hash(DISPATCH_TASK_ASSIGNMENT+target, host=self.redis_persist)
//         self.log.info(f'Starting to steal work from {target}')

//         # Start of process dispatcher transaction
//         if self.apm_client:
//             self.apm_client.begin_transaction(APM_SPAN_TYPE)

//         cpu_mark = time.process_time()
//         time_mark = time.time()

//         keys = target_jobs.keys()
//         while keys:
//             key = keys.pop()
//             message = target_jobs.pop(key)
//             if not keys:
//                 keys = target_jobs.keys()

//             if not message:
//                 continue

//             if self.submissions_assignments.pop(key):
//                 self.submission_queue.unpop(message)

//         self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//         self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//         if self.apm_client:
//             self.apm_client.end_transaction('submission_message', 'success')

//         self.log.info(f'Finished stealing work from {target}')
//         self.dispatchers_directory.pop(target)
//         self.dispatchers_directory_finalize.pop(target)



//     def pull_submissions(self):
//         sub_queue = self.submission_queue
//         cpu_mark = time.process_time()
//         time_mark = time.time()

//         while self.running:
//             while not self.active:
//                 # Dispatcher is disabled... waiting for it to be reactivated
//                 self.sleep(0.1)

//             if self.finalizing.is_set():
//                 finalizing_time = time.time() - self.finalizing_start
//                 if self.active_submissions.length() > 0 and finalizing_time < FINALIZING_WINDOW:
//                     self.sleep(1)
//                 else:
//                     self.stop()
//             else:
//                 self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//                 self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//                 # Check if we are at the submission limit globally
//                 if self.submissions_assignments.length() >= self.config.core.dispatcher.max_inflight:
//                     self.sleep(1)
//                     cpu_mark = time.process_time()
//                     time_mark = time.time()
//                     continue

//                 # Check if we are maxing out our share of the submission limit
//                 max_tasks = self.config.core.dispatcher.max_inflight / self.running_dispatchers_estimate
//                 if self.active_submissions.length() >= max_tasks:
//                     self.sleep(1)
//                     cpu_mark = time.process_time()
//                     time_mark = time.time()
//                     continue

//                 # Grab a submission message
//                 message = sub_queue.pop(timeout=1)
//                 cpu_mark = time.process_time()
//                 time_mark = time.time()

//                 if not message:
//                     continue

//                 # Start of process dispatcher transaction
//                 with apm_span(self.apm_client, 'submission_message'):
//                     # This is probably a complete task
//                     task = SubmissionTask(scheduler=self.scheduler, datastore=self.datastore, **message)

//                     # Check the sid table
//                     if task.sid in self.bad_sids:
//                         task.submission.to_be_deleted = True

//                     if self.apm_client:
//                         elasticapm.label(sid=task.submission.sid)
//                     self.dispatch_submission(task)
