use std::sync::Arc;

use assemblyline_models::datastore::Submission;
use assemblyline_models::{Sha256, JsonMap};
use tokio::sync::mpsc;

use super::Session;

use crate::error::Result;


#[derive(Clone)]
pub struct FileData {
    pub sha256: Sha256,
    pub depth: usize,
    pub name: String,
    pub temporary_data: JsonMap,
    pub file_info: assemblyline_models::datastore::File,
    // pub submission: Arc<Submission>,
    // pub ignore_dynamic_recursion_prevention: bool,
    pub schedule: Vec<
}


pub struct ExtractedFile {
    pub parent: FileData,
    pub sha256: Sha256,
    pub name: String,

    // By having the extract carry the sender back to the submission
    // main loop we ensure that the dispatcher only runs while there are
    // live files being processed.
    // the main loop is written to keep running if and only if there are
    // living references to this sender. This means that even if a file
    // crashes out terribly the dispatcher can never hang due to that crash.
    pub start_file: mpsc::Sender<ExtractedFile>
}

pub struct FileResult {
    pub sha256: Sha256,
    pub score: i32,
    pub errors: Vec<String>,
    pub results: Vec<String>,
}

pub async fn process_file(session: Arc<Session>, start_file: mpsc::Sender<ExtractedFile>, data: FileData) -> Result<FileResult> {
    let params = &data.submission.params;

    // Get the schedule for this file
    let forbidden_services = if !data.ignore_dynamic_recursion_prevention {
        task.find_recursion_excluded_services(data.sha256)
    } else {
        None
    };

    let schedule = self.scheduler.build_schedule(submission, file_info.type,
                                                                file_depth, forbidden_services,
                                                                task.service_access_control)

    deep_scan, ignore_filtering = submission.params.deep_scan, submission.params.ignore_filtering

    let results = vec![];

    for stage in schedule {
        let jobs = tokio::tasks::JoinSet::new();

        for service_name, service in stage {

        }

        jobs.c

//                 with elasticapm.capture_span('dispatch_task', labels={'service': service_name}):
//                     service_queue = get_service_queue(service_name, self.redis)

//                     key = (sha256, service_name)
//                     # Check if the task is already running
//                     if key in task.running_services:
//                         running.append(service_name)
//                         continue

//                     # Check if this task is already sitting in queue
//                     with elasticapm.capture_span('check_queue'):
//                         dispatch_key = task.queue_keys.get(key, None)
//                         if dispatch_key is not None and service_queue.rank(dispatch_key) is not None:
//                             enqueued.append(service_name)
//                             continue

//                     # If its not in queue already check we aren't dispatching anymore
//                     if task.submission.to_be_deleted:
//                         skipped.append(service_name)
//                         continue

//                     # Check if we have attempted this too many times already.
//                     task.service_attempts[key] += 1
//                     if task.service_attempts[key] > 3:
//                         self.retry_error(task, sha256, service_name)
//                         continue

//                     # Load the list of tags we will pass
//                     tags = []
//                     if service.uses_tags or service.uses_tag_scores:
//                         tags = list(task.file_tags.get(sha256, {}).values())

//                     # Load the temp submission data we will pass
//                     temp_data = {}
//                     if service.uses_temp_submission_data:
//                         temp_data = task.file_temporary_data[sha256]

//                     # Load the metadata we will pass
//                     metadata = {}
//                     if service.uses_metadata:
//                         metadata = submission.metadata

//                     tag_fields = ['type', 'value', 'short_type']
//                     if service.uses_tag_scores:
//                         tag_fields.append('score')

//                     # Mark this routing for the purposes of dynamic recursion prevention
//                     if service.category == DYNAMIC_ANALYSIS_CATEGORY:
//                         task.forbid_for_children(sha256, service_name)

//                     # Build the actual service dispatch message
//                     config = self.build_service_config(service, submission)
//                     service_task = ServiceTask(dict(
//                         sid=sid,
//                         metadata=metadata,
//                         min_classification=task.submission.classification,
//                         service_name=service_name,
//                         service_config=config,
//                         fileinfo=file_info,
//                         filename=task.file_names.get(sha256, sha256),
//                         depth=file_depth,
//                         max_files=task.submission.params.max_extracted,
//                         ttl=submission.params.ttl,
//                         ignore_cache=submission.params.ignore_cache,
//                         ignore_dynamic_recursion_prevention=submission.params.ignore_dynamic_recursion_prevention,
//                         ignore_filtering=ignore_filtering,
//                         tags=[{field: x[field] for field in tag_fields} for x in tags],
//                         temporary_submission_data=[
//                             {'name': name, 'value': value} for name, value in temp_data.items()
//                         ],
//                         deep_scan=deep_scan,
//                         priority=submission.params.priority,
//                         safelist_config=self.config.services.safelist
//                     ))
//                     service_task.metadata['dispatcher__'] = self.instance_id

//                     # Its a new task, send it to the service
//                     queue_key = service_queue.push(service_task.priority, service_task.as_primitives())
//                     task.queue_keys[key] = queue_key
//                     sent.append(service_name)
//                     task.service_logs[key].append(f'Submitted to queue at {now_as_iso()}')

    }


    self.counter.increment('files_completed')
    if len(task.queue_keys) > 0 or len(task.running_services) > 0:
        self.log.info(f"[{sid}] Finished processing file '{sha256}', submission incomplete "
                        f"(queued: {len(task.queue_keys)} running: {len(task.running_services)})")
    else:
        self.log.info(f"[{sid}] Finished processing file '{sha256}', checking if submission complete")
        return self.check_submission(task)

    return Ok(FileResult{

    })
}

// from __future__ import annotations
// import uuid
// import os
// import threading
// import time
// from collections import defaultdict
// from contextlib import contextmanager
// import typing
// from typing import Optional, Any, TYPE_CHECKING, Iterable
// import json
// import enum
// from queue import PriorityQueue, Empty, Queue
// import dataclasses

// import elasticapm

// from assemblyline.common import isotime
// from assemblyline.common.constants import make_watcher_list_name, SUBMISSION_QUEUE, \
//     DISPATCH_RUNNING_TASK_HASH, SCALER_TIMEOUT_QUEUE, DISPATCH_TASK_HASH
// from assemblyline.common.forge import get_service_queue, get_apm_client, get_classification
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.common.metrics import MetricsFactory
// from assemblyline.common.postprocess import ActionWorker
// from assemblyline.datastore.helper import AssemblylineDatastore
// from assemblyline.odm.messages.changes import ServiceChange, Operation
// from assemblyline.odm.messages.dispatcher_heartbeat import Metrics
// from assemblyline.odm.messages.service_heartbeat import Metrics as ServiceMetrics
// from assemblyline.odm.messages.dispatching import WatchQueueMessage, CreateWatch, DispatcherCommandMessage, \
//     CREATE_WATCH, LIST_OUTSTANDING, UPDATE_BAD_SID, ListOutstanding
// from assemblyline.odm.messages.submission import SubmissionMessage, from_datastore_submission
// from assemblyline.odm.messages.task import FileInfo, Task as ServiceTask
// from assemblyline.odm.models.error import Error
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.service import Service
// from assemblyline.odm.models.submission import Submission
// from assemblyline.odm.models.user import User
// from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
// from assemblyline.remote.datatypes.events import EventWatcher
// from assemblyline.remote.datatypes.hash import Hash
// from assemblyline.remote.datatypes.queues.comms import CommsQueue
// from assemblyline.remote.datatypes.queues.named import NamedQueue
// from assemblyline.remote.datatypes.set import ExpiringSet, Set
// from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
// from assemblyline_core.server_base import ThreadedCoreBase
// from assemblyline_core.alerter.run_alerter import ALERT_QUEUE_NAME


// if TYPE_CHECKING:
//     from assemblyline.odm.models.file import File


// from .schedules import Scheduler
// from .timeout import TimeoutTable
// from ..ingester.constants import COMPLETE_QUEUE_NAME

// APM_SPAN_TYPE = 'handle_message'

// AL_SHUTDOWN_GRACE = int(os.environ.get('AL_SHUTDOWN_GRACE', '60'))
// AL_SHUTDOWN_QUIT = 60
// FINALIZING_WINDOW = max(AL_SHUTDOWN_GRACE - AL_SHUTDOWN_QUIT, 0)
// RESULT_BATCH_SIZE = int(os.environ.get('DISPATCHER_RESULT_BATCH_SIZE', '50'))
// ERROR_BATCH_SIZE = int(os.environ.get('DISPATCHER_ERROR_BATCH_SIZE', '50'))
// DYNAMIC_ANALYSIS_CATEGORY = 'Dynamic Analysis'
// DAY_IN_SECONDS = 24 * 60 * 60


// class Action(enum.IntEnum):
//     start = 0
//     result = 1
//     dispatch_file = 2
//     service_timeout = 3
//     check_submission = 4
//     bad_sid = 5


// @dataclasses.dataclass(order=True)
// class DispatchAction:
//     kind: Action
//     sid: str = dataclasses.field(compare=False)
//     sha: Optional[str] = dataclasses.field(compare=False, default=None)
//     service_name: Optional[str] = dataclasses.field(compare=False, default=None)
//     worker_id: Optional[str] = dataclasses.field(compare=False, default=None)
//     data: Any = dataclasses.field(compare=False, default=None)
//     event: Optional[threading.Event] = dataclasses.field(compare=False, default=None)


// @contextmanager
// def apm_span(client, span_name: str):
//     try:
//         if client:
//             client.begin_transaction(APM_SPAN_TYPE)
//         yield None
//         if client:
//             client.end_transaction(span_name, 'success')
//     except Exception:
//         if client:
//             client.end_transaction(span_name, 'exception')
//         raise


// class ResultSummary:
//     def __init__(self, key, drop, score, children):
//         self.key: str = key
//         self.drop: bool = drop
//         self.score: int = score
//         self.children: list[tuple[str, str]] = children


// class SubmissionTask:
//     """Dispatcher internal model for submissions"""

//     def __init__(self, submission, completed_queue, scheduler, datastore: AssemblylineDatastore, results=None,
//                  file_infos=None, file_tree=None, errors: Optional[Iterable[str]] = None):
//         self.submission: Submission = Submission(submission)
//         submitter: Optional[User] = datastore.user.get_if_exists(self.submission.params.submitter)
//         self.service_access_control: Optional[str] = None
//         if submitter:
//             self.service_access_control = submitter.classification.value

//         self.completed_queue = None
//         if completed_queue:
//             self.completed_queue = str(completed_queue)

//         self.file_info: dict[str, Optional[FileInfo]] = {}
//         self.file_names: dict[str, str] = {}
//         self.file_schedules: dict[str, list[dict[str, Service]]] = {}
//         self.file_tags: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
//         self.file_depth: dict[str, int] = {}
//         self.file_temporary_data: dict[str, dict] = defaultdict(dict)
//         self.extra_errors: list[str] = []
//         self.active_files: set[str] = set()
//         self.dropped_files: set[str] = set()
//         self.dynamic_recursion_bypass: set[str] = set()
//         self.service_logs: dict[tuple[str, str], list[str]] = defaultdict(list)

//         # mapping from file hash to a set of services that shouldn't be run on
//         # any children (recursively) of that file
//         self._forbidden_services: dict[str, set[str]] = {}
//         self._parent_map: dict[str, set[str]] = {}

//         self.service_results: dict[tuple[str, str], ResultSummary] = {}
//         self.service_errors: dict[tuple[str, str], str] = {}
//         self.service_attempts: dict[tuple[str, str], int] = defaultdict(int)
//         self.queue_keys: dict[tuple[str, str], str] = {}
//         self.running_services: set[tuple[str, str]] = set()

//         if file_infos is not None:
//             self.file_info.update({k: FileInfo(v) for k, v in file_infos.items()})

//         if file_tree is not None:
//             def recurse_tree(tree, depth):
//                 for sha256, file_data in tree.items():
//                     self.file_depth[sha256] = depth
//                     self.file_names[sha256] = file_data['name'][0]
//                     recurse_tree(file_data['children'], depth + 1)

//             recurse_tree(file_tree, 0)

//         if results is not None:
//             rescan = scheduler.expand_categories(self.submission.params.services.rescan)

//             # Replay the process of routing files for dispatcher internal state.
//             for k, result in results.items():
//                 sha256, service, _ = k.split('.', 2)
//                 service = scheduler.services.get(service)
//                 if not service:
//                     continue
//                 if service.category == DYNAMIC_ANALYSIS_CATEGORY:
//                     self.forbid_for_children(sha256, service.name)

//             # Replay the process of receiving results for dispatcher internal state
//             for k, result in results.items():
//                 sha256, service, _ = k.split('.', 2)
//                 if service not in rescan:
//                     extracted = result['response']['extracted']
//                     children: list[str] = [r['sha256'] for r in extracted]
//                     self.register_children(sha256, children)
//                     children_detail: list[tuple[str, str]] = [(r['sha256'], r['parent_relation']) for r in extracted]
//                     self.service_results[(sha256, service)] = ResultSummary(
//                         key=k, drop=result['drop_file'], score=result['result']['score'],
//                         children=children_detail)

//                 tags = Result(result).scored_tag_dict()
//                 for key in tags.keys():
//                     if key in self.file_tags[sha256].keys():
//                         # Sum score of already known tags
//                         self.file_tags[sha256][key]['score'] += tags[key]['score']
//                     else:
//                         self.file_tags[sha256][key] = tags[key]

//         if errors is not None:
//             for e in errors:
//                 sha256, service, _ = e.split('.', 2)
//                 self.service_errors[(sha256, service)] = e

//     @property
//     def sid(self) -> str:
//         return self.submission.sid

//     def forbid_for_children(self, sha256: str, service_name: str):
//         """Mark that children of a given file should not be routed to a service."""
//         try:
//             self._forbidden_services[sha256].add(service_name)
//         except KeyError:
//             self._forbidden_services[sha256] = {service_name}

//     def register_children(self, parent: str, children: list[str]):
//         """
//         Note for the purposes of dynamic recursion prevention which
//         files extracted other files.
//         """
//         for child in children:
//             try:
//                 self._parent_map[child].add(parent)
//             except KeyError:
//                 self._parent_map[child] = {parent}

//     def all_ancestors(self, sha256: str) -> list[str]:
//         visited = set()
//         to_visit = [sha256]
//         while len(to_visit) > 0:
//             current = to_visit.pop()
//             for parent in self._parent_map.get(current, []):
//                 if parent not in visited:
//                     visited.add(parent)
//                     to_visit.append(parent)
//         return list(visited)

//     def find_recursion_excluded_services(self, sha256: str) -> list[str]:
//         """
//         Return a list of services that should be excluded for the given file.

//         Note that this is computed dynamically from the parent map every time it is
//         called. This is to account for out of order result collection in unusual
//         circumstances like replay.
//         """
//         return list(set().union(*[
//             self._forbidden_services.get(parent, set())
//             for parent in self.all_ancestors(sha256)
//         ]))


// DISPATCH_TASK_ASSIGNMENT = 'dispatcher-tasks-assigned-to-'
// TASK_ASSIGNMENT_PATTERN = DISPATCH_TASK_ASSIGNMENT + '*'
// DISPATCH_START_EVENTS = 'dispatcher-start-events-'
// DISPATCH_RESULT_QUEUE = 'dispatcher-results-'
// DISPATCH_COMMAND_QUEUE = 'dispatcher-commands-'
// DISPATCH_DIRECTORY = 'dispatchers-directory'
// DISPATCH_DIRECTORY_FINALIZE = 'dispatchers-directory-finalizing'
// BAD_SID_HASH = 'bad-sid-hash'
// QUEUE_EXPIRY = 60*60
// SERVICE_VERSION_EXPIRY_TIME = 30 * 60  # How old service version info can be before we ignore it
// GUARD_TIMEOUT = 60*2
// GLOBAL_TASK_CHECK_INTERVAL = 60*10
// TIMEOUT_EXTRA_TIME = 5
// TIMEOUT_TEST_INTERVAL = 5
// MAX_RESULT_BUFFER = 64
// RESULT_THREADS = max(1, int(os.getenv('DISPATCHER_RESULT_THREADS', '2')))
// FINALIZE_THREADS = max(1, int(os.getenv('DISPATCHER_FINALIZE_THREADS', '2')))

// # After 20 minutes, check if a submission is still making progress.
// # In the case of a crash somewhere else in the system, we may not have
// # gotten a message we are expecting. This should prompt a retry in most
// # cases.
// SUBMISSION_TOTAL_TIMEOUT = 60 * 20


// class Dispatcher(ThreadedCoreBase):
//     @staticmethod
//     def all_instances(persistent_redis):
//         return Hash(DISPATCH_DIRECTORY, host=persistent_redis).keys()

//     @staticmethod
//     def instance_assignment_size(persistent_redis, instance_id):
//         return Hash(DISPATCH_TASK_ASSIGNMENT + instance_id, host=persistent_redis).length()

//     @staticmethod
//     def instance_assignment(persistent_redis, instance_id) -> list[str]:
//         return Hash(DISPATCH_TASK_ASSIGNMENT + instance_id, host=persistent_redis).keys()

//     @staticmethod
//     def all_queue_lengths(redis, instance_id):
//         return {
//             'start': NamedQueue(DISPATCH_START_EVENTS + instance_id, host=redis).length(),
//             'result': NamedQueue(DISPATCH_RESULT_QUEUE + instance_id, host=redis).length(),
//             'command': NamedQueue(DISPATCH_COMMAND_QUEUE + instance_id, host=redis).length()
//         }

//     def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None,
//                  config=None, counter_name='dispatcher'):
//         super().__init__('assemblyline.dispatcher', config=config, datastore=datastore,
//                          redis=redis, redis_persist=redis_persist, logger=logger)

//         # Load the datastore collections that we are going to be using
//         self.instance_id = uuid.uuid4().hex
//         self.tasks: dict[str, SubmissionTask] = {}
//         self.finalizing = threading.Event()
//         self.finalizing_start = 0.0

//         #
//         # # Build some utility classes
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
//         self.start_queue = NamedQueue(DISPATCH_START_EVENTS+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
//         self.result_queue = NamedQueue(DISPATCH_RESULT_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
//         self.command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)

//         # Submissions that should have alerts generated
//         self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.redis_persist)

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

//     def stop(self):
//         super().stop()
//         self.service_change_watcher.stop()
//         self.postprocess_worker.stop()

//     def interrupt_handler(self, signum, stack_frame):
//         self.log.info("Instance caught signal. Beginning to drain work.")
//         self.finalizing_start = time.time()
//         self._shutdown_timeout = AL_SHUTDOWN_QUIT
//         self.finalizing.set()
//         self.dispatchers_directory_finalize.set(self.instance_id, int(time.time()))

//     def _handle_status_change(self, status: Optional[bool]):
//         super()._handle_status_change(status)

//         # If we may have lost redis connection check all of our submissions
//         if status is None:
//             for sid in self.tasks.keys():
//                 _q = self.find_process_queue(sid)
//                 _q.put(DispatchAction(kind=Action.check_submission, sid=sid))

//     def _handle_service_change_event(self, data: ServiceChange):
//         if data.operation == Operation.Removed:
//             # Remove all current instances of service from scheduler cache
//             [service_set.remove(data.name) for service_set in self.scheduler.c12n_services.values()
//              if data.name in service_set]
//         else:
//             # If Added/Modifed, pull the service information and modify cache
//             service: Service = self.datastore.get_service_with_delta(data.name)
//             for c12n, service_set in self.scheduler.c12n_services.items():
//                 if self.classification_engine.is_accessible(c12n, service.classification):
//                     # Classification group is allowed to use this service
//                     service_set.add(service.name)
//                 else:
//                     # Classification group isn't allowed to use this service
//                     if service.name in service_set:
//                         service_set.remove(service.name)

//     def process_queue_index(self, key: str) -> int:
//         return sum(ord(_x) for _x in key) % RESULT_THREADS

//     def find_process_queue(self, key: str):
//         return self.process_queues[self.process_queue_index(key)]

//     def service_worker_factory(self, index: int):
//         def service_worker():
//             return self.service_worker(index)
//         return service_worker

//     def try_run(self):
//         self.log.info(f'Using dispatcher id {self.instance_id}')
//         self.service_change_watcher.start()
//         threads = {
//             # Pull in new submissions
//             'Pull Submissions': self.pull_submissions,
//             # pull start messages
//             'Pull Service Start': self.pull_service_starts,
//             # pull result messages
//             'Pull Service Result': self.pull_service_results,
//             # Save errors to DB
//             'Save Errors': self.save_errors,
//             # Handle timeouts
//             'Process Timeouts': self.handle_timeouts,
//             # Work guard/thief
//             'Guard Work': self.work_guard,
//             'Work Thief': self.work_thief,
//             # Handle RPC commands
//             'Commands': self.handle_commands,
//             # Process to protect against old dead tasks timing out
//             'Global Timeout Backstop': self.timeout_backstop,
//         }

//         for ii in range(FINALIZE_THREADS):
//             # Finilize submissions that are done
//             threads[f'Save Submissions #{ii}'] = self.save_submission

//         for ii in range(RESULT_THREADS):
//             # Process results
//             threads[f'Service Update Worker #{ii}'] = self.service_worker_factory(ii)

//         self.maintain_threads(threads)

//         # If the dispatcher is exiting cleanly remove as many tasks from the service queues as we can
//         service_queues = {}
//         for task in self.tasks.values():
//             for (sha256, service_name), dispatch_key in task.queue_keys.items():
//                 try:
//                     s_queue = service_queues[service_name]
//                 except KeyError:
//                     s_queue = get_service_queue(service_name, self.redis)
//                     service_queues[service_name] = s_queue
//                 s_queue.remove(dispatch_key)

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


//     @classmethod
//     def build_service_config(cls, service: Service, submission: Submission) -> dict[str, str]:
//         """Prepare the service config that will be used downstream.

//         v3 names: get_service_params get_config_data
//         """
//         # Load the default service config
//         params = {x.name: x.default for x in service.submission_params}

//         # Over write it with values from the submission
//         if service.name in submission.params.service_spec:
//             params.update(submission.params.service_spec[service.name])
//         return params




//     def retry_error(self, task: SubmissionTask, sha256, service_name):
//         self.log.warning(f"[{task.submission.sid}/{sha256}] "
//                          f"{service_name} marking task failed: TASK PREEMPTED ")

//         # Pull out any details to include in error message
//         error_details = '\n'.join(task.service_logs[(sha256, service_name)])
//         if error_details:
//             error_details = '\n\n' + error_details

//         ttl = task.submission.params.ttl
//         error = Error(dict(
//             archive_ts=None,
//             created='NOW',
//             expiry_ts=now_as_iso(ttl * 24 * 60 * 60) if ttl else None,
//             response=dict(
//                 message='The number of retries has passed the limit.' + error_details,
//                 service_name=service_name,
//                 service_version='0',
//                 status='FAIL_NONRECOVERABLE',
//             ),
//             sha256=sha256, type="TASK PRE-EMPTED",
//         ))

//         error_key = error.build_key()
//         self.error_queue.put((error_key, error))

//         task.queue_keys.pop((sha256, service_name), None)
//         task.running_services.discard((sha256, service_name))
//         task.service_errors[(sha256, service_name)] = error_key

//         export_metrics_once(service_name, ServiceMetrics, dict(fail_nonrecoverable=1),
//                             counter_type='service', host='dispatcher', redis=self.redis)

//         # Send the result key to any watching systems
//         msg = {'status': 'FAIL', 'cache_key': error_key}
//         for w in self._watcher_list(task.submission.sid).members():
//             NamedQueue(w).push(msg)

//     def pull_service_results(self):
//         result_queue = self.result_queue

//         while self.running:
//             # Try to get a batch of results to process
//             messages = result_queue.pop_batch(RESULT_BATCH_SIZE)

//             # If there are no messages and no timeouts to process block for a second
//             if not messages and self.timeout_queue.empty():
//                 message = result_queue.pop(timeout=1)
//                 if message:
//                     messages = [message]

//             # If we have any messages, schedule them to be processed by the right worker thread
//             for message in messages:
//                 sid = message['sid']
//                 self.queue_ready_signals[self.process_queue_index(sid)].acquire()
//                 self.find_process_queue(sid).put(DispatchAction(kind=Action.result, sid=sid, data=message))

//             # If we got an incomplete batch, we have taken everything in redis
//             # and its safe to process timeouts, put some into the processing queues
//             if len(messages) < RESULT_BATCH_SIZE:
//                 for _ in range(RESULT_BATCH_SIZE):
//                     try:
//                         message = self.timeout_queue.get_nowait()
//                         self.find_process_queue(message.sid).put(message)
//                     except Empty:
//                         break

//     def service_worker(self, index: int):
//         self.log.info(f"Start service worker {index}")
//         work_queue = self.process_queues[index]
//         cpu_mark = time.process_time()
//         time_mark = time.time()

//         while self.running:
//             self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//             self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//             try:
//                 message = work_queue.get(timeout=1)
//             except Empty:
//                 cpu_mark = time.process_time()
//                 time_mark = time.time()
//                 continue

//             cpu_mark = time.process_time()
//             time_mark = time.time()

//             kind = message.kind

//             if kind == Action.start:
//                 with apm_span(self.apm_client, 'service_start_message'):
//                     task = self.tasks.get(message.sid)
//                     if not task:
//                         self.log.warning(f'[{message.sid}] Service started for finished task.')
//                         continue

//                     if not message.sha or not message.service_name:
//                         self.log.warning(f'[{message.sid}] Service started missing data.')
//                         continue

//                     key = (message.sha, message.service_name)
//                     if task.queue_keys.pop(key, None) is not None:
//                         # If this task is already finished (result message processed before start
//                         # message) we can skip setting a timeout
//                         if key in task.service_errors or key in task.service_results:
//                             continue
//                         self.set_timeout(task, message.sha, message.service_name, message.worker_id)
//                         task.service_logs[(message.sha, message.service_name)].append(
//                             f'Popped from queue and running at {now_as_iso()} on worker {message.worker_id}')

//             elif kind == Action.result:
//                 self.queue_ready_signals[self.process_queue_index(message.sid)].release()
//                 with apm_span(self.apm_client, "dispatcher_results"):
//                     task = self.tasks.get(message.sid)
//                     if not task:
//                         self.log.warning(f'[{message.sid}] Result returned for finished task.')
//                         continue
//                     self._submission_timeouts.set(message.sid, SUBMISSION_TOTAL_TIMEOUT, None)

//                     if 'result_summary' in message.data:
//                         self.process_service_result(task, message.data)
//                     elif 'error' in message.data:
//                         self.process_service_error(task, message.data['error_key'], Error(message.data['error']))

//             elif kind == Action.check_submission:
//                 with apm_span(self.apm_client, "check_submission_message"):
//                     task = self.tasks.get(message.sid)
//                     if task:
//                         self.log.info(f'[{message.sid}] submission timeout, checking dispatch status...')
//                         self.check_submission(task)

//                         # If we didn't finish the submission here, wait another 20 minutes
//                         if message.sid in self.tasks:
//                             self._submission_timeouts.set(message.sid, SUBMISSION_TOTAL_TIMEOUT, None)

//             elif kind == Action.service_timeout:
//                 task = self.tasks.get(message.sid)

//                 if not message.sha or not message.service_name:
//                     self.log.warning(f'[{message.sid}] Service timeout missing data.')
//                     continue

//                 if task:
//                     task.service_logs[(message.sha, message.service_name)].append(
//                         f'Service timeout at {now_as_iso()} on worker {message.worker_id}')
//                     self.timeout_service(task, message.sha, message.service_name, message.worker_id)

//             elif kind == Action.dispatch_file:
//                 task = self.tasks.get(message.sid)
//                 if task:
//                     self.dispatch_file(task, message.sha)

//             elif kind == Action.bad_sid:
//                 task = self.tasks.get(message.sid)
//                 if task:
//                     task.submission.to_be_deleted = True
//                     self.active_submissions.add(message.sid, {
//                         'completed_queue': task.completed_queue,
//                         'submission': task.submission.as_primitives()
//                     })

//                 if message.event:
//                     message.event.set()

//             else:
//                 self.log.warning(f'Invalid work order kind {kind}')

//     @elasticapm.capture_span(span_type='dispatcher')
//     def process_service_result(self, task: SubmissionTask, data: dict):
//         try:
//             submission: Submission = task.submission
//             sid = submission.sid
//             service_name = data['service_name']
//             service_version = data['service_version']
//             service_tool_version = data['service_tool_version']
//             expiry_ts = data['expiry_ts']

//             sha256 = data['sha256']
//             summary = ResultSummary(**data['result_summary'])
//             tags = data['tags']
//             temporary_data = data['temporary_data'] or {}
//             extracted_names = data['extracted_names']
//             dynamic_recursion_bypass = data.get('dynamic_recursion_bypass', [])

//         except KeyError as missing:
//             self.log.exception(f"Malformed result message, missing key: {missing}")
//             return

//         # Add SHA256s of files that allowed to run regardless of Dynamic Recursion Prevention
//         task.dynamic_recursion_bypass = task.dynamic_recursion_bypass.union(set(dynamic_recursion_bypass))

//         # Immediately remove timeout so we don't cancel now
//         self.clear_timeout(task, sha256, service_name)
//         task.service_logs.pop((sha256, service_name), None)

//         # Don't process duplicates
//         if (sha256, service_name) in task.service_results:
//             return

//         # Let the logs know we have received a result for this task
//         if summary.drop:
//             self.log.debug(f"[{sid}/{sha256}] {service_name} succeeded. "
//                            f"Result will be stored in {summary.key} but processing will stop after this service.")
//         else:
//             self.log.debug(f"[{sid}/{sha256}] {service_name} succeeded. "
//                            f"Result will be stored in {summary.key}")

//         # The depth is set for the root file, and for all extracted files whether we process them or not
//         if sha256 not in task.file_depth:
//             self.log.warning(f"[{sid}/{sha256}] {service_name} returned result for file that wasn't requested.")
//             return

//         # Account for the possibility of cache hits or services that aren't updated (tagged as compatible but not)
//         if isinstance(tags, list):
//             self.log.warning("Deprecation: Old format of tags found. "
//                              "This format changed with the release of 4.3 on 09-2022. "
//                              f"Rebuilding {service_name} may be required or the result of a cache hit. "
//                              "Proceeding with conversion to compatible format..")
//             alt_tags = {}
//             for t in tags:
//                 key = f"{t['type']}:{t['value']}"
//                 t.update({'score': 0})
//                 alt_tags[key] = t
//             tags = alt_tags

//         # Update score of tag as it moves through different services
//         for key, value in tags.items():
//             if key in task.file_tags[sha256].keys():
//                 task.file_tags[sha256][key]['score'] += value['score']
//             else:
//                 task.file_tags[sha256][key] = value

//         # Update the temporary data table for this file
//         for key, value in (temporary_data or {}).items():
//             if len(str(value)) <= self.config.submission.max_temp_data_length:
//                 task.file_temporary_data[sha256][key] = value

//         # Update children to include parent_relation, likely EXTRACTED
//         if summary.children and isinstance(summary.children[0], str):
//             old_children = typing.cast(list[str], summary.children)
//             summary.children = [(c, 'EXTRACTED') for c in old_children]

//         # Record the result as a summary
//         task.service_results[(sha256, service_name)] = summary
//         task.register_children(sha256, [c for c, _ in summary.children])

//         # Set the depth of all extracted files, even if we won't be processing them
//         depth_limit = self.config.submission.max_extraction_depth
//         new_depth = task.file_depth[sha256] + 1
//         for extracted_sha256, _ in summary.children:
//             task.file_depth.setdefault(extracted_sha256, new_depth)
//             extracted_name = extracted_names.get(extracted_sha256)
//             if extracted_name and extracted_sha256 not in task.file_names:
//                 task.file_names[extracted_sha256] = extracted_name

//         # Send the extracted files to the dispatcher
//         with elasticapm.capture_span('process_extracted_files'):
//             dispatched = 0
//             if new_depth < depth_limit:
//                 # Prepare the temporary data from the parent to build the temporary data table for
//                 # these newly extract files
//                 parent_data = task.file_temporary_data[sha256]

//                 for extracted_sha256, parent_relation in summary.children:

//                     if extracted_sha256 in task.dropped_files or extracted_sha256 in task.active_files:
//                         continue

//                     if len(task.active_files) > submission.params.max_extracted:
//                         self.log.info(f'[{sid}] hit extraction limit, dropping {extracted_sha256}')
//                         task.dropped_files.add(extracted_sha256)
//                         self._dispatching_error(task, Error({
//                             'archive_ts': None,
//                             'expiry_ts': expiry_ts,
//                             'response': {
//                                 'message': f"Too many files extracted for submission {sid} "
//                                            f"{extracted_sha256} extracted by "
//                                            f"{service_name} will be dropped",
//                                 'service_name': service_name,
//                                 'service_tool_version': service_tool_version,
//                                 'service_version': service_version,
//                                 'status': 'FAIL_NONRECOVERABLE'
//                             },
//                             'sha256': extracted_sha256,
//                             'type': 'MAX FILES REACHED'
//                         }))
//                         continue

//                     dispatched += 1
//                     task.active_files.add(extracted_sha256)
//                     try:
//                         parent_ancestry = parent_data['ancestry']
//                     except KeyError:
//                         self.log.warn(f"[{sid} :: {sha256}] missing ancestry data.")
//                         parent_ancestry = []
//                     existing_ancestry = task.file_temporary_data.get(extracted_sha256, {}).get('ancestry', [])
//                     file_info = self.get_fileinfo(task, extracted_sha256)
//                     file_type = file_info.type if file_info else 'NOT_FOUND'
//                     current_ancestry_node = dict(type=file_type, parent_relation=parent_relation,
//                                                  sha256=extracted_sha256)

//                     task.file_temporary_data[extracted_sha256] = dict(parent_data)
//                     task.file_temporary_data[extracted_sha256]['ancestry'] = existing_ancestry
//                     [task.file_temporary_data[extracted_sha256]['ancestry'].append(ancestry + [current_ancestry_node])
//                      for ancestry in parent_ancestry]
//                     self.find_process_queue(sid).put(DispatchAction(kind=Action.dispatch_file, sid=sid,
//                                                                     sha=extracted_sha256))
//             else:
//                 for extracted_sha256, _ in summary.children:
//                     task.dropped_files.add(sha256)
//                     self._dispatching_error(task, Error({
//                         'archive_ts': None,
//                         'expiry_ts': expiry_ts,
//                         'response': {
//                             'message': f"{service_name} has extracted a file "
//                                        f"{extracted_sha256} beyond the depth limits",
//                             'service_name': service_name,
//                             'service_tool_version': service_tool_version,
//                             'service_version': service_version,
//                             'status': 'FAIL_NONRECOVERABLE'
//                         },
//                         'sha256': extracted_sha256,
//                         'type': 'MAX DEPTH REACHED'
//                     }))

//         # Check if its worth trying to run the next stage
//         # Not worth running if we know we are waiting for another service
//         if any(_s == sha256 for _s, _ in task.running_services):
//             return
//         # Not worth running if we know we have services in queue
//         if any(_s == sha256 for _s, _ in task.queue_keys.keys()):
//             return
//         # Try to run the next stage
//         self.dispatch_file(task, sha256)

//     @elasticapm.capture_span(span_type='dispatcher')
//     def _dispatching_error(self, task: SubmissionTask, error):
//         error_key = error.build_key()
//         task.extra_errors.append(error_key)
//         self.error_queue.put((error_key, error))
//         msg = {'status': 'FAIL', 'cache_key': error_key}
//         for w in self._watcher_list(task.submission.sid).members():
//             NamedQueue(w).push(msg)

//     @elasticapm.capture_span(span_type='dispatcher')
//     def process_service_error(self, task: SubmissionTask, error_key, error: Error):
//         self.log.info(f'[{task.submission.sid}] Error from service {error.response.service_name} on {error.sha256}')
//         self.clear_timeout(task, error.sha256, error.response.service_name)
//         key = (error.sha256, error.response.service_name)
//         if error.response.status == "FAIL_NONRECOVERABLE":
//             task.service_errors[key] = error_key
//             task.service_logs.pop(key, None)
//         else:
//             task.service_logs[key].append(f"Service error: {error.response.message}")
//         self.dispatch_file(task, error.sha256)

//     def pull_service_starts(self):
//         start_queue = self.start_queue
//         cpu_mark = time.process_time()
//         time_mark = time.time()

//         while self.running:
//             self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//             self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//             messages = start_queue.pop_batch(100)
//             if not messages:
//                 message = start_queue.pop(timeout=1)
//                 if message:
//                     messages = [message]

//             cpu_mark = time.process_time()
//             time_mark = time.time()

//             for message in messages:
//                 sid, sha, service_name, worker_id = message
//                 self.find_process_queue(sid).put(DispatchAction(kind=Action.start, sid=sid, sha=sha,
//                                                                 service_name=service_name, worker_id=worker_id))

//     def handle_timeouts(self):
//         while self.sleep(TIMEOUT_TEST_INTERVAL):
//             with apm_span(self.apm_client, 'process_timeouts'):
//                 cpu_mark = time.process_time()
//                 time_mark = time.time()

//                 # Check for submission timeouts
//                 submission_timeouts = self._submission_timeouts.timeouts()
//                 for sid in submission_timeouts.keys():
//                     _q = self.find_process_queue(sid)
//                     _q.put(DispatchAction(kind=Action.check_submission, sid=sid))

//                 # Check for service timeouts
//                 service_timeouts = self._service_timeouts.timeouts()
//                 for (sid, sha, service_name), worker_id in service_timeouts.items():
//                     # Put our timeouts into special timeout queue so they are delayed
//                     # until redis results are processed
//                     self.timeout_queue.put(
//                         DispatchAction(kind=Action.service_timeout, sid=sid, sha=sha,
//                                        service_name=service_name, worker_id=worker_id)
//                     )

//                 self.counter.increment('service_timeouts', len(service_timeouts))
//                 self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//                 self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//     @elasticapm.capture_span(span_type='dispatcher')
//     def timeout_service(self, task: SubmissionTask, sha256, service_name, worker_id):
//         # We believe a service task has timed out, try and read it from running tasks
//         # If we can't find the task in running tasks, it finished JUST before timing out, let it go
//         sid = task.submission.sid
//         task.queue_keys.pop((sha256, service_name), None)
//         task_key = ServiceTask.make_key(sid=sid, service_name=service_name, sha=sha256)
//         service_task = self.running_tasks.pop(task_key)
//         if not service_task and (sha256, service_name) not in task.running_services:
//             self.log.debug(f"[{sid}] Service {service_name} "
//                            f"timed out on {sha256} but task isn't running.")
//             return False

//         # We can confirm that the task is ours now, even if the worker finished, the result will be ignored
//         task.running_services.discard((sha256, service_name))
//         self.log.info(f"[{sid}] Service {service_name} "
//                       f"running on {worker_id} timed out on {sha256}.")
//         self.dispatch_file(task, sha256)

//         # We push the task of killing the container off on the scaler, which already has root access
//         # the scaler can also double check that the service name and container id match, to be sure
//         # we aren't accidentally killing the wrong container
//         if worker_id is not None:
//             self.scaler_timeout_queue.push({
//                 'service': service_name,
//                 'container': worker_id
//             })

//             # Report to the metrics system that a recoverable error has occurred for that service
//             export_metrics_once(service_name, ServiceMetrics, dict(fail_recoverable=1),
//                                 host=worker_id, counter_type='service', redis=self.redis)
//         return True


//     def handle_commands(self):
//         while self.running:

//             message = self.command_queue.pop(timeout=3)
//             if not message:
//                 continue

//             cpu_mark = time.process_time()
//             time_mark = time.time()

//             # Start of process dispatcher transaction
//             with apm_span(self.apm_client, 'command_message'):

//                 command = DispatcherCommandMessage(message)
//                 if command.kind == CREATE_WATCH:
//                     watch_payload: CreateWatch = command.payload()
//                     self.setup_watch_queue(watch_payload.submission, watch_payload.queue_name)
//                 elif command.kind == LIST_OUTSTANDING:
//                     payload: ListOutstanding = command.payload()
//                     self.list_outstanding(payload.submission, payload.response_queue)
//                 elif command.kind == UPDATE_BAD_SID:
//                     self.update_bad_sids()
//                     NamedQueue(command.payload_data, host=self.redis).push(self.instance_id)
//                 else:
//                     self.log.warning(f"Unknown command code: {command.kind}")

//                 self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//                 self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

//     @elasticapm.capture_span(span_type='dispatcher')
//     def setup_watch_queue(self, sid, queue_name):
//         # Create a unique queue
//         watch_queue = NamedQueue(queue_name, ttl=30)
//         watch_queue.push(WatchQueueMessage({'status': 'START'}).as_primitives())

//         #
//         task = self.tasks.get(sid)
//         if not task:
//             watch_queue.push(WatchQueueMessage({"status": "STOP"}).as_primitives())
//             return

//         # Add the newly created queue to the list of queues for the given submission
//         self._watcher_list(sid).add(queue_name)

//         # Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
//         for result_data in list(task.service_results.values()):
//             watch_queue.push(WatchQueueMessage({"status": "OK", "cache_key": result_data.key}).as_primitives())

//         for error_key in list(task.service_errors.values()):
//             watch_queue.push(WatchQueueMessage({"status": "FAIL", "cache_key": error_key}).as_primitives())

//     @elasticapm.capture_span(span_type='dispatcher')
//     def list_outstanding(self, sid: str, queue_name: str):
//         response_queue = NamedQueue(queue_name, host=self.redis)
//         outstanding: defaultdict[str, int] = defaultdict(int)
//         task = self.tasks.get(sid)
//         if task:
//             for sha, service_name in list(task.queue_keys.keys()):
//                 outstanding[service_name] += 1
//             for sha, service_name in list(task.running_services):
//                 outstanding[service_name] += 1
//         response_queue.push(outstanding)

//     def timeout_backstop(self):
//         while self.running:
//             cpu_mark = time.process_time()
//             time_mark = time.time()

//             # Start of process dispatcher transaction
//             with apm_span(self.apm_client, 'timeout_backstop'):
//                 dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
//                 error_tasks = []

//                 # iterate running tasks
//                 for task_key, task_body in self.running_tasks:
//                     task = ServiceTask(task_body)
//                     # Its a bad task if it's dispatcher isn't running
//                     if task.metadata['dispatcher__'] not in dispatcher_instances:
//                         error_tasks.append(task)
//                     # Its a bad task if its OUR task, but we aren't tracking that submission anymore
//                     if task.metadata['dispatcher__'] == self.instance_id and task.sid not in self.tasks:
//                         error_tasks.append(task)

//                 # Refresh our dispatcher list.
//                 dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
//                 other_dispatcher_instances = dispatcher_instances - {self.instance_id}

//                 # The remaining running tasks (probably) belong to dead dispatchers and can be killed
//                 for task in error_tasks:
//                     # Check against our refreshed dispatcher list in case it changed during the previous scan
//                     if task.metadata['dispatcher__'] in other_dispatcher_instances:
//                         continue

//                     # If its already been handled, we don't need to
//                     if not self.running_tasks.pop(task.key()):
//                         continue

//                     # Kill the task that would report to a dead dispatcher
//                     self.log.warning(f"[{task.sid}]Task killed by backstop {task.service_name} {task.fileinfo.sha256}")
//                     self.scaler_timeout_queue.push({
//                         'service': task.service_name,
//                         'container': task.metadata['worker__']
//                     })

//                     # Report to the metrics system that a recoverable error has occurred for that service
//                     export_metrics_once(task.service_name, ServiceMetrics, dict(fail_recoverable=1),
//                                         host=task.metadata['worker__'], counter_type='service', redis=self.redis)

//             # Look for unassigned submissions in the datastore if we don't have a
//             # large number of outstanding things in the queue already.
//             with apm_span(self.apm_client, 'orphan_submission_check'):
//                 assignments = self.submissions_assignments.items()
//                 recovered_from_database = []
//                 if self.submission_queue.length() < 500:
//                     with apm_span(self.apm_client, 'abandoned_submission_check'):
//                         # Get the submissions belonging to an dispatcher we don't know about
//                         for item in self.datastore.submission.stream_search('state: submitted', fl='sid'):
//                             if item['sid'] in assignments:
//                                 continue
//                             recovered_from_database.append(item['sid'])

//             # Look for instances that are in the assignment table, but the instance its assigned to doesn't exist.
//             # We try to remove the instance from the table to prevent multiple dispatcher instances from
//             # recovering it at the same time
//             with apm_span(self.apm_client, 'orphan_submission_check'):
//                 # Get the submissions belonging to an dispatcher we don't know about
//                 assignments = self.submissions_assignments.items()
//                 dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
//                 # List all dispatchers with jobs assigned
//                 for raw_key in self.redis_persist.keys(TASK_ASSIGNMENT_PATTERN):
//                     key: str = raw_key.decode()
//                     dispatcher_instances.add(key[len(DISPATCH_TASK_ASSIGNMENT):])

//                 # Submissions that didn't belong to anyone should be recovered
//                 for sid, instance in assignments.items():
//                     if instance in dispatcher_instances:
//                         continue
//                     if self.submissions_assignments.conditional_remove(sid, instance):
//                         self.recover_submission(sid, 'from assignment table')

//             # Go back over the list of sids from the database now that we have a copy of the
//             # assignments table taken after our database scan
//             for sid in recovered_from_database:
//                 if sid not in assignments:
//                     self.recover_submission(sid, 'from database scan')

//             self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
//             self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)
//             self.sleep(GLOBAL_TASK_CHECK_INTERVAL)

//     def recover_submission(self, sid: str, message: str) -> bool:
//         # Make sure we can load the submission body
//         submission: Optional[Submission] = self.datastore.submission.get_if_exists(sid)
//         if not submission:
//             return False
//         if submission.state != 'submitted':
//             return False

//         self.log.warning(f'Recovered dead submission: {sid} {message}')

//         # Try to recover the completion queue value by checking with the ingest table
//         completed_queue = ''
//         if submission.scan_key:
//             completed_queue = COMPLETE_QUEUE_NAME

//         # Put the file back into processing
//         self.submission_queue.unpop(dict(
//             submission=submission.as_primitives(),
//             completed_queue=completed_queue,
//         ))
//         return True

//     def update_bad_sids(self):
//         # Pull new sid list
//         remote_sid_list = set(self.redis_bad_sids.members())
//         new_sid_events = []

//         # Kick off updates for any new sids
//         for bad_sid in remote_sid_list - self.bad_sids:
//             self.bad_sids.add(bad_sid)
//             event = threading.Event()
//             self.find_process_queue(bad_sid).put(DispatchAction(kind=Action.bad_sid, sid=bad_sid, event=event))
//             new_sid_events.append(event)

//         # Wait for those updates to finish
//         for event in new_sid_events:
//             event.wait()

// from __future__ import annotations
// from typing import Dict, Optional, Set, cast

// import logging
// import os
// import re

// from assemblyline.common.forge import CachedObject, get_classification
// from assemblyline.datastore.helper import AssemblylineDatastore
// from assemblyline.odm.models.config import Config
// from assemblyline.odm.models.service import Service
// from assemblyline.odm.models.submission import Submission
// from assemblyline_core.server_base import get_service_stage_hash, ServiceStage


// # If you are doing development and you want the system to route jobs ignoring the service setup/teardown
// # set an environment variable SKIP_SERVICE_SETUP to true for all dispatcher containers
// SKIP_SERVICE_SETUP = os.environ.get('SKIP_SERVICE_SETUP', 'false').lower() in ['true', '1']

// Classification = get_classification()


// class Scheduler:
//     """This object encapsulates building the schedule for a given file type for a submission."""

//     def __init__(self, datastore: AssemblylineDatastore, config: Config, redis):
//         self.datastore = datastore
//         self.config = config
//         self._services: Dict[str, Service] = {}
//         self.services = cast(Dict[str, Service], CachedObject(self._get_services))
//         self.service_stage = get_service_stage_hash(redis)
//         self.c12n_services: Dict[str, Set[str]] = {}

//     def build_schedule(self, submission: Submission, file_type: str, file_depth: int = 0,
//                        runtime_excluded: Optional[list[str]] = None,
//                        submitter_c12n: Optional[str] = Classification.UNRESTRICTED) -> list[dict[str, Service]]:
//         # Get the set of all services currently enabled on the system
//         all_services = dict(self.services)

//         # Retrieve a list of services that the classfication group is allowed to submit to
//         if submitter_c12n is None:
//             accessible = set(all_services.keys())
//         else:
//             accessible = self.get_accessible_services(submitter_c12n)

//         # Load the selected and excluded services by category
//         excluded = self.expand_categories(submission.params.services.excluded)
//         runtime_excluded = self.expand_categories(runtime_excluded or [])
//         if not submission.params.services.selected:
//             selected = [s for s in all_services.keys()]
//         else:
//             selected = self.expand_categories(submission.params.services.selected)

//         if submission.params.services.rescan:
//             selected.extend(self.expand_categories(submission.params.services.rescan))

//         # If we enable service safelisting, the Safelist service shouldn't run on extracted files unless:
//         #   - We're enforcing use of the Safelist service (we always want to run the Safelist service)
//         #   - We're running submission with Deep Scanning
//         #   - We want to Ignore Filtering (perform as much unfiltered analysis as possible)
//         if "Safelist" in selected and file_depth and self.config.services.safelist.enabled and \
//                 not self.config.services.safelist.enforce_safelist_service \
//                 and not (submission.params.deep_scan or submission.params.ignore_filtering):
//             # Alter schedule to remove Safelist, if scheduled to run
//             selected.remove("Safelist")

//         # Add all selected, accepted, and not rejected services to the schedule
//         schedule: list[dict[str, Service]] = [{} for _ in self.config.services.stages]
//         services = list(set(selected).intersection(accessible) - set(excluded) - set(runtime_excluded))
//         selected = []
//         skipped = []
//         for name in services:
//             service = all_services.get(name, None)

//             if not service:
//                 skipped.append(name)
//                 logging.warning(f"Service configuration not found: {name}")
//                 continue

//             accepted = not service.accepts or re.match(service.accepts, file_type)
//             rejected = bool(service.rejects) and re.match(service.rejects, file_type)

//             if accepted and not rejected:
//                 schedule[self.stage_index(service.stage)][name] = service
//                 selected.append(name)
//             else:
//                 skipped.append(name)

//         return schedule

//     def expand_categories(self, services: list[str]) -> list[str]:
//         """Expands the names of service categories found in the list of services.

//         Args:
//             services (list): List of service category or service names.
//         """
//         if services is None:
//             return []

//         services = list(services)
//         categories = self.categories()

//         found_services = []
//         seen_categories: set[str] = set()
//         while services:
//             name = services.pop()

//             # If we found a new category mix in it's content
//             if name in categories:
//                 if name not in seen_categories:
//                     # Add all of the items in this group to the list of
//                     # things that we need to evaluate, and mark this
//                     # group as having been seen.
//                     services.extend(categories[name])
//                     seen_categories.update(name)
//                 continue

//             # If it isn't a category, its a service
//             found_services.append(name)

//         # Use set to remove duplicates, set is more efficient in batches
//         return list(set(found_services))

//     def categories(self) -> Dict[str, list[str]]:
//         all_categories: dict[str, list[str]] = {}
//         for service in self.services.values():
//             try:
//                 all_categories[service.category].append(service.name)
//             except KeyError:
//                 all_categories[service.category] = [service.name]
//         return all_categories

//     def get_accessible_services(self, user_c12n: str) -> Set[str]:
//         if not self.c12n_services.get(user_c12n):
//             # Cache services that are accessible to a classification group
//             self.c12n_services[user_c12n] = {_ for _, service in dict(self.services).items()
//                                              if Classification.is_accessible(user_c12n, service.classification)}

//         return self.c12n_services[user_c12n]

//     def stage_index(self, stage):
//         return self.config.services.stages.index(stage)

//     def _get_services(self):
//         old, self._services = self._services, {}
//         stages = self.service_stage.items()
//         services: list[Service] = self.datastore.list_all_services(full=True)
//         for service in services:
//             if service.enabled:
//                 # Determine if this is a service we would wait for the first update run for
//                 # Assume it is set to running so that in the case of a redis failure we fail
//                 # on the side of waiting for the update and processing more, rather than skipping
//                 wait_for = service.update_config and (service.update_config.wait_for_update and not SKIP_SERVICE_SETUP)
//                 # This is a service that we wait for, and is new, so check if it has finished its update setup
//                 if wait_for and service.name not in old:
//                     if stages.get(service.name, ServiceStage.Running) == ServiceStage.Running:
//                         self._services[service.name] = service
//                 else:
//                     self._services[service.name] = service
//         return self._services
