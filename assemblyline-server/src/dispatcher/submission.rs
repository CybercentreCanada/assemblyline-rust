use std::collections::{HashSet, HashMap};
use std::sync::Arc;

use assemblyline_models::Sha256;
use assemblyline_models::datastore::submission::SubmissionState;
use assemblyline_models::datastore::{Submission, file};
use log::error;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::dispatcher::file::{FileResult, process_file};
use crate::error::{Error, Result};

use super::{Session};
use super::file::{FileData, ExtractedFile};

/// Process a submission through to completion
pub async fn process_submission(session: Arc<Session>, submission: Submission) -> Result<Submission> {

    // submission: Submission,
    // session: Arc<Session>,
    // finished_files: HashMap<Sha256, FileResult>,
    // dropped_files: HashSet<Sha256>,

    // Check if its in the database
    if let Some(db_sub) = session.elastic.submission.get(&submission.sid.to_string()).await? {
        if db_sub.state != SubmissionState::Submitted {
            return Ok(db_sub)
        }
    }

    // # Refresh the quota hold
    // if submission.params.quota_item and submission.params.submitter:
    //     self.log.info(f"[{sid}] Submission counts towards {submission.params.submitter.upper()} quota")

    //     # Write all new submissions to the traffic queue
    //     self.traffic_queue.publish(SubmissionMessage({
    //         'msg': from_datastore_submission(task.submission),
    //         'msg_type': 'SubmissionStarted',
    //         'sender': 'dispatcher',
    //     }).as_primitives())

    // Setup context
    let mut processing_files: JoinSet<Result<FileResult>> = JoinSet::new();
    let mut file_info: HashMap<Sha256, FileData> = Default::default();
    let (start_file_send, start_file_recv) = mpsc::channel(32);


    // # Apply initial data parameter
    todo!();
    // if submission.params.initial_data:
    //     try:
    //         task.file_temporary_data[sha256] = {
    //             key: value
    //             for key, value in dict(json.loads(submission.params.initial_data)).items()
    //             if len(str(value)) <= self.config.submission.max_temp_data_length
    //         }

    //     except (ValueError, TypeError) as err:
    //         self.log.warning(f"[{sid}] could not process initialization data: {err}")

    // # Initialize ancestry chain by identifying the root file
    todo!();
    // file_info = self.get_fileinfo(task, sha256)
    // file_type = file_info.type if file_info else 'NOT_FOUND'
    // task.file_temporary_data[sha256]['ancestry'] = [[dict(type=file_type, parent_relation="ROOT", sha256=sha256)]]

    // dispatch initial file
    {
        let data = FileData {
            sha256: submission.file.sha256.clone(),
            depth: 0,
            name: submission.file.name.clone(),
        };
        file_info.insert(data.sha256.clone(), data.clone());
        processing_files.spawn(process_file(session.clone(), start_file_send, data));
    }

    // wait for changes
    loop {
        tokio::select! {
            biased;
            // wait for any new files to be suggested
            file_message = start_file_recv.recv() => {
                start_extracted_file(file_message).await
            }

            // wait for any files to finish
            finished = processing_files.join_next() => {
                match finished {
                    Some(Ok(Ok(finished))) => { self.file_finished(finished).await; },
                    Some(Ok(Err(err))) => { self.save_dispatch_error(err); },
                    Some(Err(err)) => { self.save_dispatch_error(err.into()); },
                    None => break,
                }
            }
        }
    }

    // collect results and update submission
    todo!();


    // post processing
    todo!();

    // Finish
    todo!("save to database");
    todo!("update finished/running structs in session");
}

async fn start_extracted_file(start: Option<ExtractedFile>) {
    // Unwrap the message
    let start = match start {
        Some(message) => message,
        None => return,
    };

    // Check if this file has already been dropped or excluded
    if self.dropped_files.contains(&start.sha256) { return }

    // Enforce the max extracted limit
    if self.file_info.len() >= self.submission.params.max_extracted as usize {
        self.dropped_files.insert(start.sha256);
        self.save_max_extracted_error(start).await;
        return
    }

    // Fetch the info about the parent of this file
    let parent = self.file_info.get(&start.parent).clone();

    // Create the info packet about this file
    let data = match self.file_info.entry(start.sha256.clone()) {
        // File already started
        std::collections::hash_map::Entry::Occupied(entry) => { return; },
        // create new data for this file
        std::collections::hash_map::Entry::Vacant(entry) => {
            let data = match parent {
                Some(parent) => FileData { sha256: start.sha256, depth: parent.depth + 1, name: start.name },
                None => FileData { sha256: start.sha256, depth: 1, name: start.name },
            };
            entry.insert(data.clone());
            data
        },
    };

    // Kick off this file for processing
    self.processing_files.spawn(process_file(self.session.clone(), self.start_file_send.clone(), data));
}

//     async fn file_finished(&self, result: FileResult) {
//         match finished_file {
//             Some(finished_file) => match finished_file {
//                 Ok(_) => todo!(),
//                 Err(_) => todo!(),
//             },
//             None => break,
//         }
//     }

//     async fn save_max_extracted_error(&self, start: ExtractedFile) {
//         todo!()
//         // self.log.info(f'[{sid}] hit extraction limit, dropping {extracted_sha256}')
//         // task.dropped_files.add(extracted_sha256)
//         // self._dispatching_error(task, Error({
//         //     'archive_ts': None,
//         //     'expiry_ts': expiry_ts,
//         //     'response': {
//         //         'message': f"Too many files extracted for submission {sid} "
//         //                    f"{extracted_sha256} extracted by "
//         //                    f"{service_name} will be dropped",
//         //         'service_name': service_name,
//         //         'service_tool_version': service_tool_version,
//         //         'service_version': service_version,
//         //         'status': 'FAIL_NONRECOVERABLE'
//         //     },
//         //     'sha256': extracted_sha256,
//         //     'type': 'MAX FILES REACHED'
//         // }))
//     }

//     async fn save_dispatch_error(&self, err: Error) {
//         todo!()
//     }
// }

    // """
    // Check if a submission is finished.

    // :param task: Task object for the submission in question.
    // :return: true if submission has been finished.
    // """
    // # Track which files we have looked at already
    // checked: set[str] = set()
    // unchecked: set[str] = set(list(task.file_depth.keys()))

    // # Categorize files as pending/processing (can be both) all others are finished
    // pending_files = []  # Files where we are missing a service and it is not being processed
    // processing_files = []  # Files where at least one service is in progress/queued

    // # Track information about the results as we hit them
    // file_scores: dict[str, int] = {}

    // # Make sure we have either a result or
    // while unchecked:
    //     sha256 = next(iter(unchecked))
    //     unchecked.remove(sha256)
    //     checked.add(sha256)

    //     if sha256 in task.dropped_files:
    //         continue

    //     if sha256 not in task.file_schedules:
    //         pending_files.append(sha256)
    //         continue
    //     schedule = list(task.file_schedules[sha256])

    //     while schedule and sha256 not in pending_files and sha256 not in processing_files:
    //         stage = schedule.pop(0)
    //         for service_name in stage:

    //             # Only active services should be in this dict, so if a service that was placed in the
    //             # schedule is now missing it has been disabled or taken offline.
    //             service = self.scheduler.services.get(service_name)
    //             if not service:
    //                 continue

    //             # If there is an error we are finished with this service
    //             key = sha256, service_name
    //             if key in task.service_errors:
    //                 continue

    //             # if there is a result, then the service finished already
    //             result = task.service_results.get(key)
    //             if result:
    //                 if not task.submission.params.ignore_filtering and result.drop:
    //                     schedule.clear()

    //                 # Collect information about the result
    //                 file_scores[sha256] = file_scores.get(sha256, 0) + result.score
    //                 unchecked.update(set([c for c, _ in result.children]) - checked)
    //                 continue

    //             # If the file is in process, we may not need to dispatch it, but we aren't finished
    //             # with the submission.
    //             if key in task.running_services:
    //                 processing_files.append(sha256)
    //                 # another service may require us to dispatch it though so continue rather than break
    //                 continue

    //             # Check if the service is in queue, and handle it the same as being in progress.
    //             # Check this one last, since it can require a remote call to redis rather than checking a dict.
    //             service_queue = get_service_queue(service_name, self.redis)
    //             if key in task.queue_keys and service_queue.rank(task.queue_keys[key]) is not None:
    //                 processing_files.append(sha256)
    //                 continue

    //             # Don't worry about pending files if we aren't dispatching anymore and they weren't caught
    //             # by the prior checks for outstanding tasks
    //             if task.submission.to_be_deleted:
    //                 break

    //             # Since the service is not finished or in progress, it must still need to start
    //             pending_files.append(sha256)
    //             break

    // # Filter out things over the depth limit
    // depth_limit = self.config.submission.max_extraction_depth
    // pending_files = [sha for sha in pending_files if task.file_depth[sha] < depth_limit]

    // # If there are pending files, then at least one service, on at least one
    // # file isn't done yet, and hasn't been filtered by any of the previous few steps
    // # poke those files.
    // if pending_files:
    //     self.log.debug(f"[{task.submission.sid}] Dispatching {len(pending_files)} files: {list(pending_files)}")
    //     for file_hash in pending_files:
    //         if self.dispatch_file(task, file_hash):
    //             return True
    // elif processing_files:
    //     self.log.debug(f"[{task.submission.sid}] Not finished waiting on {len(processing_files)} "
    //                    f"files: {list(processing_files)}")
    // else:
    //     self.log.debug(f"[{task.submission.sid}] Finalizing submission.")
    //     max_score = max(file_scores.values()) if file_scores else 0  # Submissions with no results have no score
    //     if self.tasks.pop(task.sid, None):
    //         self.finalize_queue.put((task, max_score, checked))
    //     return True
    // return False

//     @elasticapm.capture_span(span_type='dispatcher')
//     def finalize_submission(self, task: SubmissionTask, max_score, file_list):
//         """All of the services for all of the files in this submission have finished or failed.

//         Update the records in the datastore, and flush the working data from redis.
//         """
//         submission = task.submission
//         sid = submission.sid

//         results = list(task.service_results.values())
//         errors = list(task.service_errors.values())
//         errors.extend(task.extra_errors)

//         submission.classification = submission.params.classification
//         submission.error_count = len(errors)
//         submission.errors = errors
//         submission.file_count = len(file_list)
//         submission.results = [r.key for r in results]
//         submission.max_score = max_score
//         submission.state = 'completed'
//         submission.times.completed = isotime.now_as_iso()
//         self.datastore.submission.save(sid, submission)

//         self._cleanup_submission(task)
//         self.log.info(f"[{sid}] Completed; files: {len(file_list)} results: {len(results)} "
//                       f"errors: {len(errors)} score: {max_score}")

//     def _watcher_list(self, sid):
//         return ExpiringSet(make_watcher_list_name(sid), host=self.redis)

//     def _cleanup_submission(self, task: SubmissionTask):
//         """Clean up code that is the same for canceled and finished submissions"""
//         submission = task.submission
//         sid = submission.sid

//         # Now that a submission is finished, we can remove it from the timeout list
//         self._submission_timeouts.clear(task.sid)

//         if submission.params.quota_item and submission.params.submitter:
//             self.log.info(f"[{sid}] Submission no longer counts toward {submission.params.submitter.upper()} quota")
//             self.quota_tracker.end(submission.params.submitter)

//         if task.completed_queue:
//             NamedQueue(task.completed_queue, self.redis).push(submission.as_primitives())

//         # Send complete message to any watchers.
//         watcher_list = self._watcher_list(sid)
//         for w in watcher_list.members():
//             NamedQueue(w).push(WatchQueueMessage({'status': 'STOP'}).as_primitives())

//         # Don't run post processing and traffic notifications if the submission is terminated
//         if not task.submission.to_be_deleted:
//             # Pull the tags keys and values into a searchable form
//             tags = [
//                 {'value': _t['value'], 'type': _t['type']}
//                 for file_tags in task.file_tags.values()
//                 for _t in file_tags.values()
//             ]

//             # Send the submission for alerting or resubmission
//             self.postprocess_worker.process_submission(submission, tags)

//             # Write all finished submissions to the traffic queue
//             self.traffic_queue.publish(SubmissionMessage({
//                 'msg': from_datastore_submission(submission),
//                 'msg_type': 'SubmissionCompleted',
//                 'sender': 'dispatcher',
//             }).as_primitives())

//         # Clear the timeout watcher
//         watcher_list.delete()
//         self.active_submissions.pop(sid)
//         self.submissions_assignments.pop(sid)
//         self.tasks.pop(sid, None)

//         # Count the submission as 'complete' either way
//         self.counter.increment('submissions_completed')