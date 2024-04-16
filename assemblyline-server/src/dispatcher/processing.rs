use std::collections::{HashSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use assemblyline_models::datastore::user::User;
use assemblyline_models::{Sha256, JsonMap};
use assemblyline_models::datastore::submission::SubmissionState;
use assemblyline_models::datastore::{Submission, file, Service};
use assemblyline_models::datastore::error::Status as ErrorStatus;
use assemblyline_models::messages::task::{Task, TagItem};

use log::{info, error, debug};
use rand::Rng;
use redis_objects::increment;
use serde::{Serialize, Deserialize};
use serde_json::json;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::error::Result;

use super::{DispatcherSession, ResultSummary, TagCollection, ExtractionSummary};
use super::interface::TaskResult;
const REDIS_POLL_DELAY: Duration = Duration::from_secs(60 * 10);
const EXTRA_MESSAGE_TIME: Duration = Duration::from_secs(10);
// TODO
//             # Don't worry about pending files if we aren't dispatching anymore and they weren't caught
//             # by the prior checks for outstanding tasks
//             if task.submission.to_be_deleted:
//                 break


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


struct SubmissionProcessor {
    session: Arc<DispatcherSession>,
    submission: Arc<Submission>,
    user: Option<Arc<User>>,
    completion_queue: Option<String>,

    // Track files that are running, finished, or refused
    file_context: HashMap<Sha256, SharedFileData>,
    rejected_files: HashSet<Sha256>,
    file_results: HashMap<Sha256, FileResult>,

    // set of tokio tasks, each one responsible for one file
    file_tasks: JoinSet<Result<FileResult>>,

    // all of the non-service errors that occurred during processing
    error_keys: Vec<String>,

    // Set of files that can skip dynamic recursion prevention
    dynamic_recursion_bypass: HashSet<Sha256>,
}

impl SubmissionProcessor {

    pub fn new(session: Arc<DispatcherSession>, submission: Submission, completion_queue: Option<String>, user: Option<Arc<User>>) -> Result<Self> {
        Ok(Self {
            session,
            completion_queue,
            submission: Arc::new(submission),    
            file_context: Default::default(),
            rejected_files: Default::default(),
            file_results: Default::default(),
            file_tasks: JoinSet::new(),
            error_keys: Default::default(),
            dynamic_recursion_bypass: Default::default(),
            user,
        })
    }

    pub async fn run(self) -> Result<Submission> {
        match tokio::spawn(self._run()).await {
            Ok(result) => result,
            Err(err) => Err(err.into())
        }
    }

    async fn _run(&mut self) -> Result<Submission> {
        // some short (read only) aliases
        let submission = &self.submission;
        let params = &self.submission.params;
        let sid = self.submission.sid;

        if not self.submissions_assignments.add(sid, self.instance_id):
            self.log.warning(f"[{sid}] Received an assigned submission dropping")
            return

        if not self.active_submissions.exists(sid):
            self.log.info("[%s] New submission received", sid)
            self.active_submissions.add(sid, {
                'completed_queue': task.completed_queue,
                'submission': submission.as_primitives()
            })

            # Write all new submissions to the traffic queue
            self.traffic_queue.publish(SubmissionMessage({
                'msg': from_datastore_submission(task.submission),
                'msg_type': 'SubmissionStarted',
                'sender': 'dispatcher',
            }).as_primitives())

        else:
            self.log.warning(f"[{sid}] Received a pre-existing submission.")


        // Check if this submission is already complete in the database
        if let Some(db_sub) = self.session.elastic.submission.get(&sid.to_string()).await? {
            if db_sub.state != SubmissionState::Submitted {
                return Ok(db_sub)
            }
        }

        // Refresh the quota hold
        let _quote_guard = if params.quota_item && !params.submitter.is_empty() {
            info!("[{sid}] Submission counts towards {} quota", params.submitter.to_uppercase());
            Some(self.session.redis_volatile.quota(params.submitter).await?)
        } else {
            None
        };

        // Apply initial data parameter
        let root_temporary_data = JsonMap::new();
        if let Some(init) = params.initial_data {
            match serde_json::from_str::<JsonMap>(&init) {
                Ok(data) => {
                    for (key, value) in data.into_iter() {
                        if value.to_string().len() >= self.session.config.submission.max_temp_data_length as usize {
                            continue
                        }
                        root_temporary_data.insert(key, value);
                    }
                },
                Err(err) => {
                    self.save_dispatch_error(format!("Could not process initialization data: {err}")).await;
                },
            };
        }

        // Initialize ancestry chain
        let file_info = match self.get_fileinfo(&submission.file.sha256).await? {
            Some(info) => info,
            None => {
                return self.submission_failure().await
            }
        };
        root_temporary_data.insert("ancestry".to_owned(), json!([[{"type": file_info.file_type.clone(), "parent_relation": "ROOT", "sha256": submission.file.sha256}]]));

        // Initalize share data for the root
        self.file_context.insert(submission.file.sha256.clone(), SharedFileData {
            parents: Default::default(),
            children: Default::default(),
            depth: 0,
        });

        // Setup queue to allow files to dispatch more files
        let (task_request_send, task_request_recv) = mpsc::channel(32);

        // setup initial file data
        let root_file = FileProcessor {
            session: self.session.clone(),
            submission: self.submission.clone(),
            processor: task_request_send,
            schedule: self.build_schedule(&file_info),

            depth: 0,
            name: if submission.file.name.is_empty() { submission.file.sha256.to_string() } else { submission.file.name.clone() },
            file_info,
            temporary_data: root_temporary_data,
            tags: todo!(),
        };

        // dispatch initial file
        self.file_tasks.spawn(root_file.process_file());

        // Loop until all files are processed
        loop {
            tokio::select! {
                // make sure the branches run in order here
                biased;

                // wait for any new files to be suggested
                file_message = task_request_recv.recv() => {
                    // match file_message {
                    //     Some(file_message) => self.start_extracted_file(file_message).await?,
                    //     // TODO, the below isn't true anymore
                    //     // because all file tasks must hold the sender for this queue
                    //     // and a copy is passed back when a file is extracted we use the
                    //     // closing of this queue to signify processing being complete
                    //     None => break
                    // }
                }

                // wait for any files to finish
                finished = self.file_tasks.join_next(), if !self.file_tasks.is_empty() => {
                    match finished {
                        Some(Ok(Ok(finished))) => { self.file_finished(finished).await; },
                        Some(Ok(Err(err))) => { self.save_dispatch_error(err).await; },
                        Some(Err(err)) => { self.save_dispatch_error(err.into()).await; },
                        None => continue,
                    }
                }
            }

            // Check if there are 

            // Check if all the files are complete
            if self.file_tasks.is_empty() {
                break
            }
        }

        // Submissions with no results have no score (0)
        let max_score = self.file_results.into_values()
            .fold(0, |acc, result| acc.max(result.score));

        // collect results and update submission
        let results = vec![];
        let errors = self.error_keys;
        for result in self.file_results.values() {
            errors.extend(result.errors);
            results.extend(result.results);
        }

        let mut submission = self.submission;
        submission.classification = submission.params.classification;
        submission.error_count = errors.len() as i32;
        submission.errors = errors;
        submission.file_count = self.file_context.len() as i32;
        submission.results = results;
        submission.max_score = max_score;
        submission.state = SubmissionState::Completed;
        submission.times.completed = Some(chrono::Utc::now());

        // Don't run post processing and traffic notifications if the submission is terminated
        if not task.submission.to_be_deleted:
            # Pull the tags keys and values into a searchable form
            tags = [
                {'value': _t['value'], 'type': _t['type']}
                for file_tags in task.file_tags.values()
                for _t in file_tags.values()
            ]

            # Send the submission for alerting or resubmission
            self.postprocess_worker.process_submission(submission, tags)

            # Write all finished submissions to the traffic queue
            self.traffic_queue.publish(SubmissionMessage({
                'msg': from_datastore_submission(submission),
                'msg_type': 'SubmissionCompleted',
                'sender': 'dispatcher',
            }).as_primitives())

        // write submission to database and clear out records so it is not retried
        self.session.datastore.submission.save(sid, submission).await?;
        self.active_submissions.pop(sid)
        self.submissions_assignments.pop(sid)

        // Send complete message to any watchers.
        watcher_list = self._watcher_list(sid)
        for w in watcher_list.members():
            NamedQueue(w).push(WatchQueueMessage({'status': 'STOP'}).as_primitives())
        watcher_list.delete()

        // Count the submission as complete and write to complete queue
        increment!(self.counter, submissions_completed);
        if let Some(queue) = self.completion_queue {
            NamedQueue(task.completed_queue, self.redis).push(submission.as_primitives())
        }

        // Finished
        info!("[{sid}] Completed; files: {len(file_list)} results: {len(results)} errors: {len(errors)} score: {max_score}");
        return Ok(submission)
    }


    //     def _watcher_list(self, sid):
//         return ExpiringSet(make_watcher_list_name(sid), host=self.redis)

    async fn build_schedule(&self) -> Vec<Vec<Arc<Service>>> {
        todo!()
        
//         # If its the first time we've seen this file, we won't have a schedule for it
//         if sha256 not in task.file_schedules:
//             # We are processing this file, load the file info, and build the schedule
//             file_info = self.get_fileinfo(task, sha256)
//             if file_info is None:
//                 return False

//             forbidden_services = None

//             # If Dynamic Recursion Prevention is in effect and the file is not part of the bypass list,
//             # Find the list of services this file is forbidden from being sent to.
//             ignore_drp = submission.params.ignore_dynamic_recursion_prevention
//             if not ignore_drp and sha256 not in task.dynamic_recursion_bypass:
//                 forbidden_services = task.find_recursion_excluded_services(sha256)

//             task.file_schedules[sha256] = self.scheduler.build_schedule(submission, file_info.type,
//                                                                         file_depth, forbidden_services,
//                                                                         task.service_access_control)

//         file_info = task.file_info[sha256]
//         schedule: list = list(task.file_schedules[sha256])
    }

    async fn start_extracted_file(&mut self, start: ExtractedFile) -> Result<()> {
        // Check if this file has already been started or dropped
        todo!("Add parent when context already exists");
        if self.file_context.contains_key(&start.sha256) { return Ok(()) }
        if self.rejected_files.contains(&start.sha256) { return Ok(()) }

        // Enforce the max extracted limit
        if self.file_context.len() >= self.submission.params.max_extracted as usize {
            self.rejected_files.insert(start.sha256);
            self.save_max_extracted_error(start).await;
            return Ok(())
        }

        // Fetch the info about the parent of this file
        let parent = start.parent;

        // If Dynamic Recursion Prevention is in effect and the file is not part of the bypass list,
        // Find the list of services this file is forbidden from being sent to.
        let ignore_drp = self.submission.params.ignore_dynamic_recursion_prevention;
        let ignore_drp = ignore_drp || self.dynamic_recursion_bypass.contains(&start.sha256);

        // Create the info packet about this file
        let data = FileData {
            sha256: start.sha256.clone(),
            depth: parent.depth + 1,
            name: start.name,
            temporary_data: parent.temporary_data.clone(),
            file_info: match self.get_fileinfo(&start.sha256).await? {
                Some(info) => info,
                None => return Ok(())
            },
            submission: self.submission.clone(),
            ignore_dynamic_recursion_prevention: ignore_drp,
        };

        // Kick off this file for processing
        self.file_tasks.spawn(data.process_file());
        Ok(())
    }

    async fn file_finished(&mut self, result: FileResult) {
        use std::collections::hash_map::Entry::Vacant;
        if let Vacant(entry) = self.file_results.entry(result.sha256.clone()) {
            entry.insert(result);
        }
    }

    async fn get_fileinfo(&mut self, sha256: &Sha256) -> Result<Option<assemblyline_models::datastore::File>> {
        todo!()
//     @elasticapm.capture_span(span_type='dispatcher')
//     def get_fileinfo(self, task: SubmissionTask, sha256: str) -> Optional[FileInfo]:
//         """Read information about a file from the database, caching it locally."""
//         # First try to get the info from local cache
//         file_info = task.file_info.get(sha256, None)
//         if file_info:
//             return file_info

//         # get the info from datastore
//         filestore_info: Optional[File] = self.datastore.file.get(sha256)

//         if filestore_info is None:
//             # Store an error and mark this file as unprocessable
//             task.dropped_files.add(sha256)
//             self._dispatching_error(task, Error({
//                 'archive_ts': None,
//                 'expiry_ts': task.submission.expiry_ts,
//                 'response': {
//                     'message': f"Couldn't find file info for {sha256} in submission {task.sid}",
//                     'service_name': 'Dispatcher',
//                     'service_tool_version': '4.0',
//                     'service_version': '4.0',
//                     'status': 'FAIL_NONRECOVERABLE'
//                 },
//                 'sha256': sha256,
//                 'type': 'UNKNOWN'
//             }))
//             task.file_info[sha256] = None
//             task.file_schedules[sha256] = []
//             return None
//         else:
//             # Translate the file info format
//             file_info = task.file_info[sha256] = FileInfo(dict(
//                 magic=filestore_info.magic,
//                 md5=filestore_info.md5,
//                 mime=filestore_info.mime,
//                 sha1=filestore_info.sha1,
//                 sha256=filestore_info.sha256,
//                 size=filestore_info.size,
//                 ssdeep=filestore_info.ssdeep,
//                 type=filestore_info.type,
//                 tlsh=filestore_info.tlsh,
//                 uri_info=filestore_info.uri_info
//             ))
//         return file_info
    }

    async fn save_max_extracted_error(&self, start: ExtractedFile) {
        todo!()
        // self.log.info(f'[{sid}] hit extraction limit, dropping {extracted_sha256}')
        // task.dropped_files.add(extracted_sha256)
        // self._dispatching_error(task, Error({
        //     'archive_ts': None,
        //     'expiry_ts': expiry_ts,
        //     'response': {
        //         'message': f"Too many files extracted for submission {sid} "
        //                    f"{extracted_sha256} extracted by "
        //                    f"{service_name} will be dropped",
        //         'service_name': service_name,
        //         'service_tool_version': service_tool_version,
        //         'service_version': service_version,
        //         'status': 'FAIL_NONRECOVERABLE'
        //     },
        //     'sha256': extracted_sha256,
        //     'type': 'MAX FILES REACHED'
        // }))
    }

    async fn save_dispatch_error(&self, err: String) {
        todo!("prepend [sid] to the error message");
        todo!()
    }

    async fn submission_failure(self) -> Result<Submission> {
        todo!("prepend [sid] to the error message");
        todo!()
    }
}

pub struct FileResult {
    pub sha256: Sha256,
    pub score: i32,
    pub errors: HashMap<String, String>,
    pub results: HashMap<String, ResultSummary>,
    pub tags: TagCollection,
}

enum ContextRequest {
    ServiceFinished {
        parent: Sha256,
        extracted: Vec<ExtractionSummary>,
        dynamic_recursion_bypass: Vec<Sha256>,
        temporary_data: JsonMap,
    },
    ForbidForChildren {
        file: Sha256,
        forbidden_services: Vec<String>,
    }
}


// //     def _watcher_list(self, sid):
// //         return ExpiringSet(make_watcher_list_name(sid), host=self.redis)


// from .schedules import Scheduler
// from .timeout import TimeoutTable
// from ..ingester.constants import COMPLETE_QUEUE_NAME

// if TYPE_CHECKING:
//     from assemblyline.odm.models.file import File
//     from redis import Redis


// APM_SPAN_TYPE = 'handle_message'

// AL_SHUTDOWN_GRACE = int(os.environ.get('AL_SHUTDOWN_GRACE', '60'))
// AL_SHUTDOWN_QUIT = 60
// FINALIZING_WINDOW = max(AL_SHUTDOWN_GRACE - AL_SHUTDOWN_QUIT, 0)
// RESULT_BATCH_SIZE = int(os.environ.get('DISPATCHER_RESULT_BATCH_SIZE', '50'))
// ERROR_BATCH_SIZE = int(os.environ.get('DISPATCHER_ERROR_BATCH_SIZE', '50'))
// DYNAMIC_ANALYSIS_CATEGORY = 'Dynamic Analysis'
// DAY_IN_SECONDS = 24 * 60 * 60


// class KeyType(enum.Enum):
//     OVERWRITE = 'overwrite'
//     UNION = 'union'
//     IGNORE = 'ignore'


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


// @dataclasses.dataclass()
// class MonitorTask:
//     """Tracks whether a task needs to be rerun based on """
//     # Service name
//     service: str
//     # sha256 of file in question
//     sha: str
//     # The temporary values this task was last dispatached with
//     values: dict[str, Optional[str]]
//     # Should aservice be dispatched again when possible
//     dispatch_needed: bool = dataclasses.field(default=False)





// def merge_in_values(old_values: Any, new_values: set[str]) -> Optional[list[str]]:
//     """Merge in new values into a json list.
    
//     If there is no new values return None.
//     """
//     # Read out the old value set
//     if isinstance(old_values, (list, set)):
//         old_values = set(old_values)
//     else:
//         old_values = set()

//     # If we have no new values to merge in
//     if new_values <= old_values:
//         return None
    
//     # We have new values, build a new set
//     return list(new_values | old_values)

/// Information about files that needs to be syncronized for some operations
struct SharedFileData {
    parents: HashSet<Sha256>,
    children: HashSet<Sha256>,
    depth: u32,
}

/// A struct that forms the environment for a task that 
struct FileProcessor {
    session: Arc<DispatcherSession>,
    submission: Arc<Submission>,
    processor: mpsc::Sender<ContextRequest>,
    schedule: Vec<Vec<Arc<Service>>>,

    file_info: assemblyline_models::datastore::File,
    depth: u32,
    name: String,
    temporary_data: JsonMap,
    tags: TagCollection,
}

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
//         self.temporary_data: dict[str, TemporaryFileData] = {}
//         self.extra_errors: list[str] = []
//         self.active_files: set[str] = set()
//         self.dropped_files: set[str] = set()
//         self.dynamic_recursion_bypass: set[str] = set()
//         self.service_logs: dict[tuple[str, str], list[str]] = defaultdict(list)
//         self.monitoring: dict[tuple[str, str], MonitorTask] = {}

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
//                         children=children_detail, partial=result.get('partial', False))

//                 tags = Result(result).scored_tag_dict()
//                 for key, tag in tags.items():
//                     if key in self.file_tags[sha256].keys():
//                         # Sum score of already known tags
//                         self.file_tags[sha256][key]['score'] += tag['score']
//                     else:
//                         self.file_tags[sha256][key] = tag

//         if errors is not None:
//             for e in errors:
//                 sha256, service, _ = e.split('.', 2)
//                 self.service_errors[(sha256, service)] = e

//     @property
//     def sid(self) -> str:
//         """Shortcut to read submission SID"""
//         return self.submission.sid

//     def forbid_for_children(self, sha256: str, service_name: str):
//         """Mark that children of a given file should not be routed to a service."""
//         try:
//             self._forbidden_services[sha256].add(service_name)
//         except KeyError:
//             self._forbidden_services[sha256] = {service_name}

//     def register_children(self, parent: str, children: list[str]):
//         """
//         Note which files extracted other files.
//         _parent_map is for dynamic recursion prevention
//         temporary_data is for cascading the temp data to children
//         """
//         parent_temp = self.temporary_data[parent]
//         for child in children:
//             try:
//                 self.temporary_data[child].add_parent(parent_temp)
//             except KeyError:
//                 self.temporary_data[child] = parent_temp.new_child(child)
//             try:
//                 self._parent_map[child].add(parent)
//             except KeyError:
//                 self._parent_map[child] = {parent}

//     def all_ancestors(self, sha256: str) -> list[str]:
//         """Collect all the known ancestors of the given file within this submission."""
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

//     def set_monitoring_entry(self, sha256: str, service_name: str, values: dict[str, Optional[str]]):
//         """A service with monitoring has dispatched, keep track of the conditions."""
//         self.monitoring[(sha256, service_name)] = MonitorTask(
//             service=service_name,
//             sha=sha256,
//             values=values,
//         )

//     def partial_result(self, sha256, service_name):
//         """Note that a partial result has been recieved. If a dispatch was requested process that now."""
//         try:
//             entry = self.monitoring[(sha256, service_name)]
//         except KeyError:
//             return

//         if entry.dispatch_needed:
//             self.redispatch_service(sha256, service_name)

//     def clear_monitoring_entry(self, sha256, service_name):
//         """A service has completed normally. If the service is monitoring clear out the record."""
//         # We have an incoming non-partial result, flush out any partial monitoring
//         self.monitoring.pop((sha256, service_name), None)
//         # If there is a partial result for this service flush that as well so we accept this new result
//         result = self.service_results.get((sha256, service_name))
//         if result and result.partial:
//             self.service_results.pop((sha256, service_name), None)

//     def file_temporary_data_changed(self, changed_sha256: set[str], key: str) -> list[str]:
//         """Check all of the monitored tasks on that key for changes. Redispatch as needed."""
//         changed = []
//         for (sha256, service), entry in self.monitoring.items():
//             if sha256 not in changed_sha256:
//                 continue

//             value = self.temporary_data[sha256].read_key(key)
//             dispatched_value = entry.values.get(key)

//             if type(value) is not type(dispatched_value) or value != dispatched_value:
//                 result = self.service_results.get((sha256, service))
//                 if not result:
//                     entry.dispatch_needed = True
//                 else:
//                     self.redispatch_service(sha256, service)
//                     changed.append(sha256)
//         return changed

//     def redispatch_service(self, sha256, service_name):
//         # Clear the result if its partial or an error
//         result = self.service_results.get((sha256, service_name))
//         if result and not result.partial:
//             return
//         self.service_results.pop((sha256, service_name), None)
//         self.service_errors.pop((sha256, service_name), None)
//         self.service_attempts[(sha256, service_name)] = 1

//         # Try to get the service to run again by reseting the schedule for that service
//         self.file_schedules.pop(sha256, None)




// class Dispatcher(ThreadedCoreBase):
//     @staticmethod
//     def all_instances(persistent_redis: Redis):
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

//     def _handle_service_change_event(self, data: Optional[ServiceChange]):
//         if not data:
//             # We may have missed change messages, flush cache
//             self.scheduler.c12n_services.clear()
//             return
//         if data.operation == Operation.Removed:
//             # Remove all current instances of service from scheduler cache
//             for service_set in self.scheduler.c12n_services.values():
//                 if data.name in service_set:
//                     service_set.remove(data.name)
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



impl FileProcessor {

    /// Dispatch to any outstanding services for the given file.
    pub async fn process_file(self) -> Result<FileResult> {
        // local aliases
        let sid = self.submission.sid.clone();
//         deep_scan, ignore_filtering = submission.params.deep_scan, submission.params.ignore_filtering

        let mut results: HashMap<String, ResultSummary> = Default::default();
        let mut errors: HashMap<String, String> = Default::default();
        let mut score = 0;

        // go through each stage of services
        for stage in self.schedule {

            let mut futures = vec![];
            // launch each expected service
            for service in stage {

                // Load the list of tags we will pass
                let mut tags = TagCollection::new();
                if service.uses_tags || service.uses_tag_scores {
                    if service.uses_tag_scores {
                        tags = self.tags.read().unwrap().clone()
                    } else {
                        tags = self.tags.read().unwrap().without_scores()
                    }
                }

                // Kick off the service task
                futures.push((service.name.clone(), tokio::spawn(ServiceProcessor {
                    session: self.session.clone(),
                    submission: self.submission.clone(),
                    processor: self.processor.clone(),
                    service_logs: vec![],
                    depth: self.depth,
                    tags,
                    service,
                    temporary_data: self.temporary_data.clone(),
                }.run())))
            }

            // wait for all the services to finish
            let mut drop = false;
            for (service, future) in futures {
                match future.await?? {
                    ServiceResponse::Result(result, tags, mut data) => { 
                        self.tags.update(tags);
                        score += result.score;
                        results.insert(service, result); 
                        self.temporary_data.append(&mut data);
                        drop |= result.drop;
                    },
                    ServiceResponse::Error(error_key, _) => { errors.insert(service, error_key); },
                };
            }
            
            // break out of the loop if one of the services want to drop
            if !self.submission.params.ignore_filtering && drop { break }
        }

        // record this file as completed
        increment!(self.counter, files_completed);

        Ok(FileResult {
            sha256: self.file_info.sha256,
            score,
            errors,
            results,
            tags: self.tags,
        })
    }
}


struct ServiceProcessor {
    session: Arc<DispatcherSession>,
    submission: Arc<Submission>,
    processor: mpsc::Sender<ContextRequest>,
    service: Arc<Service>,

    tags: TagCollection,
    temporary_data: watch::Receiver<JsonMap>,
    service_logs: Vec<String>,
    depth: u32,
}

impl ServiceProcessor {

    async fn run(self) -> Result<ServiceResponse> {
        // Attempt to dispatch three times
        for attempt in 0..3 {
            if !self.session.flags.is_running() {
                return Err(crate::error::Error::RuntimeError("Unexpected shutdown.".to_owned()))
            }

            match self.dispatch_task().await {
                Ok(ServiceResponse::Result(result, tags, data)) => return Ok(ServiceResponse::Result(result, tags, data)),
                Ok(ServiceResponse::Error(key, message, status)) => if let ErrorStatus::FailNonrecoverable = status {
                    return Ok(ServiceResponse::Error(key, status))
                } else {
                    self.service_logs.push(format!("Recoverable error: {message}"));
                }
                Err(err) => {
                    self.service_logs.push(format!("Error in running service: {err}"))
                }
            }
        }

        // Dispatch failure
        return self.retry_error();
    }

    async fn dispatch_task(&mut self) -> Result<ServiceResponse> {

        // Load the temp submission data we will pass
        let mut temp_data = JsonMap::new();
        if self.service.uses_temp_submission_data {
            temp_data = self.temporary_data.borrow().clone()
        }

        // Load the metadata we will pass
        let metadata = if self.service.uses_metadata {
            self.submission.metadata.clone()
        } else {
            Default::default()
        };
    
        // Mark this routing for the purposes of dynamic recursion prevention
        if !self.service.recursion_prevention.is_empty() && !self.submission.params.ignore_dynamic_recursion_prevention {
            self.processor.send(ContextRequest::ForbidForChildren(self.sha256, self.service.recursion_prevention.clone())).await;
        }

        // Build the actual service dispatch message
        let config = self.build_service_config(self.service, self.submission);
        let service_task = Task {
            task_id: rand::thread_rng().gen(),
            dispatcher: self.session.instance_id,
            sid: self.submission.sid,
            metadata,
            min_classification: self.submission.classification,
            service_name: self.service.name,
            service_config: config,
            fileinfo: self.file_info,
            filename: self.file_name,
            depth: self.depth,
            max_files: self.submission.params.max_extracted,
            ttl: self.submission.params.ttl,
            ignore_cache: self.submission.params.ignore_cache,
            ignore_dynamic_recursion_prevention: self.submission.params.ignore_dynamic_recursion_prevention,
            ignore_filtering: self.submission.params.ignore_filtering,
            tags: self.tags.0,
            temporary_submission_data: temp_data,
            deep_scan: self.submission.params.deep_scan,
            priority: self.submission.params.priority,
            safelist_config: self.config.services.safelist
        };

        // register the task with the API callback
        let token = service_task.token();
        let future = self.session.register_task(service_task).await;

        // send it to the service
        let service_queue = self.session.get_service_queue(&self.service.name);
        let queue_key = service_queue.push(service_task.priority, &token).await?;
        self.service_logs.push(format!("Submitted to queue at {}", chrono::Utc::now()));

        // Wait for the service to start
        let start_message = loop {
            if let Ok(message) = tokio::time::timeout(REDIS_POLL_DELAY, &mut future).await {
                // item has been selected
                break message?
            }

            // check if the message is still in queue, wait forever if it is
            if service_queue.rank(&queue_key).await?.is_some() {
                continue
            }

            // item is not in redis anymore, give the extra time
            if let Ok(message) = tokio::time::timeout(EXTRA_MESSAGE_TIME, &mut future).await {
                break message?
            }

            // message seems to have gone missing. count this as a timeout
            return self.timeout_error()
        };

        // Wait for the service timeout time
        let response = match tokio::time::timeout(Duration::from_secs(self.service.timeout as u64) + EXTRA_MESSAGE_TIME, start_message.result).await {
            Ok(result) => result?,
            Err(_) => return self.timeout_error(),
        };

        match response {
            super::ResultResponse::Result(result) => self.process_service_result(result).await,
            super::ResultResponse::Error(error) => self.process_service_error(error).await,
        }
    }

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


    async fn process_service_result(&self, result: TaskResult) -> Result<ServiceResponse> {
        let sid = result.sid;
        let sha256 = result.sha256;
        let service_name = result.service_name;
        let summary = result.result_summary;

        if summary.partial {
            info!("[{}/{}] {} returned partial results", sid, sha256, service_name);
        } else if summary.drop {
            debug!("[{sid}/{sha256}] {service_name} succeeded. Result will be stored in {} but processing will stop after this service.", summary.key)
        } else {
            debug!("[{sid}/{sha256}] {service_name} succeeded. Result will be stored in {}", summary.key)
        }

        // Send the information we need to share to other services.
        self.processor.send(ContextRequest::ServiceFinished {
            parent: self.file_info.sha256.clone(),
            extracted: summary.children.clone(),
            dynamic_recursion_bypass: result.dynamic_recursion_bypass,
            temporary_data: result.temporary_data,
        }).await;

        todo!("Done in root");
        // Add SHA256s of files that allowed to run regardless of Dynamic Recursion Prevention
        // task.dynamic_recursion_bypass = task.dynamic_recursion_bypass.union(set(dynamic_recursion_bypass));

        todo!("Done in file/root");
        // Record the result as a summary
        // task.service_results[(sha256, service_name)] = summary
        // task.register_children(sha256, [c for c, _ in summary.children])

        todo!("Done in root when extracted children recieved");
        // Set the depth of all extracted files, even if we won't be processing them
        // depth_limit = self.config.submission.max_extraction_depth
        // new_depth = task.file_depth[sha256] + 1
        // for extracted_sha256, _ in summary.children:
        //     task.file_depth.setdefault(extracted_sha256, new_depth)
        //     extracted_name = extracted_names.get(extracted_sha256)
        //     if extracted_name and extracted_sha256 not in task.file_names:
        //         task.file_names[extracted_sha256] = extracted_name

//         # Send the extracted files to the dispatcher
//         with elasticapm.capture_span('process_extracted_files'):
//             dispatched = 0
//             if new_depth < depth_limit:
//                 # Prepare the temporary data from the parent to build the temporary data table for
//                 # these newly extract files
//                 parent_data = task.temporary_data[sha256]

//                 for extracted_sha256, parent_relation in summary.children:

//                     if extracted_sha256 in task.dropped_files or extracted_sha256 in task.active_files:
//                         continue

//                     if len(task.active_files) > submission.params.max_extracted:
//                         self.log.info('[%s] hit extraction limit, dropping %s', sid, extracted_sha256)
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

//                     # Get the new ancestory data
//                     file_info = self.get_fileinfo(task, extracted_sha256)
//                     file_type = file_info.type if file_info else 'NOT_FOUND'
//                     current_ancestry_node = dict(type=file_type, parent_relation=parent_relation,
//                                                  sha256=extracted_sha256)

//                     # Update ancestory data
//                     parent_ancestry = parent_data.read_key('ancestry') or []
//                     existing_ancestry = task.temporary_data[extracted_sha256].local_values.setdefault('ancestry', [])
//                     for ancestry in parent_ancestry:
//                         existing_ancestry.append(ancestry + [current_ancestry_node])

//                     # Trigger the processing of the extracted file
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
        
        return Ok(ServiceResponse::Result(
            summary,
            result.tags,
            result.temporary_data
        ))
    }

}

enum ServiceResponse {
    // a summary of information about the results needed for further dispatching and 
    // a collection of NEW tag information.
    Result(ResultSummary, Vec<TagItem>, JsonMap),
    Error{
        key: String, 
        status: ErrorStatus,
        message: String,
    },
}

//     @elasticapm.capture_span(span_type='dispatcher')
//     def check_submission(self, task: SubmissionTask) -> bool:
//         """
//         Check if a submission is finished.

//         :param task: Task object for the submission in question.
//         :return: true if submission has been finished.
//         """
//         # Track which files we have looked at already
//         checked: set[str] = set()
//         unchecked: set[str] = set(list(task.file_depth.keys()))

//         # Categorize files as pending/processing (can be both) all others are finished
//         pending_files = []  # Files where we are missing a service and it is not being processed
//         processing_files = []  # Files where at least one service is in progress/queued

//         # Track information about the results as we hit them
//         file_scores: dict[str, int] = {}

//         # Make sure we have either a result or
//         while unchecked:
//             sha256 = next(iter(unchecked))
//             unchecked.remove(sha256)
//             checked.add(sha256)

//             if sha256 in task.dropped_files:
//                 continue

//             if sha256 not in task.file_schedules:
//                 pending_files.append(sha256)
//                 continue
//             schedule = list(task.file_schedules[sha256])

//             while schedule and sha256 not in pending_files and sha256 not in processing_files:
//                 stage = schedule.pop(0)
//                 for service_name in stage:

//                     # Only active services should be in this dict, so if a service that was placed in the
//                     # schedule is now missing it has been disabled or taken offline.
//                     service = self.scheduler.services.get(service_name)
//                     if not service:
//                         continue

//                     # If there is an error we are finished with this service
//                     key = sha256, service_name
//                     if key in task.service_errors:
//                         continue

//                     # if there is a result, then the service finished already
//                     result = task.service_results.get(key)
//                     if result:
//                         if not task.submission.params.ignore_filtering and result.drop:
//                             schedule.clear()

//                         # Collect information about the result
//                         file_scores[sha256] = file_scores.get(sha256, 0) + result.score
//                         unchecked.update(set([c for c, _ in result.children]) - checked)
//                         continue

//                     # If the file is in process, we may not need to dispatch it, but we aren't finished
//                     # with the submission.
//                     if key in task.running_services:
//                         processing_files.append(sha256)
//                         # another service may require us to dispatch it though so continue rather than break
//                         continue

//                     # Check if the service is in queue, and handle it the same as being in progress.
//                     # Check this one last, since it can require a remote call to redis rather than checking a dict.
//                     service_queue = get_service_queue(service_name, self.redis)
//                     if key in task.queue_keys and service_queue.rank(task.queue_keys[key]) is not None:
//                         processing_files.append(sha256)
//                         continue

//                     # Don't worry about pending files if we aren't dispatching anymore and they weren't caught
//                     # by the prior checks for outstanding tasks
//                     if task.submission.to_be_deleted:
//                         break

//                     # Since the service is not finished or in progress, it must still need to start
//                     pending_files.append(sha256)
//                     break

//         # Filter out things over the depth limit
//         depth_limit = self.config.submission.max_extraction_depth
//         pending_files = [sha for sha in pending_files if task.file_depth[sha] < depth_limit]

//         # If there are pending files, then at least one service, on at least one
//         # file isn't done yet, and hasn't been filtered by any of the previous few steps
//         # poke those files.
//         if pending_files:
//             self.log.debug(f"[{task.submission.sid}] Dispatching {len(pending_files)} files: {list(pending_files)}")
//             for file_hash in pending_files:
//                 if self.dispatch_file(task, file_hash):
//                     return True
//         elif processing_files:
//             self.log.debug("[%s] Not finished waiting on %d files: %s",
//                            task.submission.sid, len(processing_files), list(processing_files))
//         else:
//             self.log.debug("[%s] Finalizing submission.", task.submission.sid)
//             max_score = max(file_scores.values()) if file_scores else 0  # Submissions with no results have no score
//             if self.tasks.pop(task.sid, None):
//                 self.finalize_queue.put((task, max_score, checked))
//             return True
//         return False

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

//     def save_submission(self):
//         while self.running:
//             self.counter.set('save_queue', self.finalize_queue.qsize())
//             try:
//                 task, max_score, checked = self.finalize_queue.get(block=True, timeout=3)
//                 self.finalize_submission(task, max_score, checked)
//             except Empty:
//                 pass

//     def save_errors(self):
//         while self.running:
//             self.counter.set('error_queue', self.error_queue.qsize())

//             try:
//                 errors = [self.error_queue.get(block=True, timeout=3)]
//             except Empty:
//                 continue

//             with apm_span(self.apm_client, 'save_error'):
//                 try:
//                     while len(errors) < ERROR_BATCH_SIZE:
//                         errors.append(self.error_queue.get_nowait())
//                 except Empty:
//                     pass

//                 plan = self.datastore.error.get_bulk_plan()
//                 for error_key, error in errors:
//                     plan.add_upsert_operation(error_key, error)
//                 self.datastore.error.bulk(plan)


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



//     def timeout_backstop(self):
//         while self.running:
//             cpu_mark = time.process_time()
//             time_mark = time.time()

//             # Start of process dispatcher transaction
//             with apm_span(self.apm_client, 'timeout_backstop'):
//                 dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
//                 error_tasks = []

//                 # iterate running tasks
//                 for _task_key, task_body in self.running_tasks:
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
