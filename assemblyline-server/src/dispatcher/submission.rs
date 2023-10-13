use std::collections::{HashSet, HashMap};
use std::sync::Arc;

use assemblyline_models::{Sha256, JsonMap};
use assemblyline_models::datastore::submission::SubmissionState;
use assemblyline_models::datastore::{Submission, file};
use log::{info, error};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::dispatcher::file::{FileResult, process_file};
use crate::error::Result;

use super::Session;
use super::file::{FileData, ExtractedFile};

// TODO
//             # Don't worry about pending files if we aren't dispatching anymore and they weren't caught
//             # by the prior checks for outstanding tasks
//             if task.submission.to_be_deleted:
//                 break


struct SubmissionContext {
    session: Arc<Session>,
    submission: Submission,

    // Track files that are running, finished, or refused
    started_files: HashSet<Sha256>,
    rejected_files: HashSet<Sha256>,
    file_results: HashMap<Sha256, FileResult>,

    // set of tokio tasks, each one responsible for one file
    file_tasks: JoinSet<Result<FileResult>>,

    // all of the non-service errors that occurred during processing
    error_keys: Vec<String>,
}

impl SubmissionContext {

    pub fn new(session: Arc<Session>, submission: Submission) -> Result<Self> {

        todo!();
    }

    pub async fn run(self) -> Result<Submission> {
        match tokio::spawn(self._run()).await {
            Ok(result) => result,
            Err(err) => Err(err.into())
        }
    }

    async fn _run(&mut self) -> Result<Submission> {
        // some short aliases
        let submission = &self.submission;
        let params = &self.submission.params;
        let sid = self.submission.sid;

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

        // TODO?
        //     # Write all new submissions to the traffic queue
        //     self.traffic_queue.publish(SubmissionMessage({
        //         'msg': from_datastore_submission(task.submission),
        //         'msg_type': 'SubmissionStarted',
        //         'sender': 'dispatcher',
        //     }).as_primitives())

        // setup initial file data
        let root_file = FileData {
            sha256: submission.file.sha256.clone(),
            depth: 0,
            name: submission.file.name.clone(),
            temporary_data: Default::default(),
            file_info: match self.get_fileinfo(&submission.file.sha256).await? {
                Some(info) => info,
                None => {
                    return self.submission_failure().await
                }
            }
        };

        // Apply initial data parameter
        if let Some(init) = params.initial_data {
            match serde_json::from_str::<JsonMap>(&init) {
                Ok(data) => {
                    for (key, value) in data.into_iter() {
                        if value.to_string().len() >= self.session.config.submission.max_temp_data_length as usize {
                            continue
                        }
                        root_file.temporary_data.insert(key, value);
                    }
                },
                Err(err) => {
                    self.save_dispatch_error(format!("Could not process initialization data: {err}")).await;
                },
            };
        }

        // Initialize ancestry chain by identifying the root file
        let file_type = root_file.file_info.file_type;
        root_file.temporary_data.insert("ancestry".to_owned(), json!([[{"type": file_type, "parent_relation": "ROOT", "sha256": submission.file.sha256}]]));

        // Setup queue to allow files to dispatch more files
        let (start_file_send, start_file_recv) = mpsc::channel(32);

        // dispatch initial file
        self.started_files.insert(root_file.sha256.clone());
        self.file_tasks.spawn(process_file(self.session.clone(), start_file_send, root_file));

        // wait for changes
        loop {
            tokio::select! {
                // make sure the branches run in order here
                biased;

                // wait for any new files to be suggested
                file_message = start_file_recv.recv() => {
                    match file_message {
                        Some(file_message) => self.start_extracted_file(file_message).await?,
                        // because all file tasks must hold the sender for this queue
                        // and a copy is passed back when a file is extracted we use the
                        // closing of this queue to signify processing being complete
                        None => break
                    }
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
        }

        max_score = max(file_scores.values()) if file_scores else 0  # Submissions with no results have no score

        // collect results and update submission
        results = list(task.service_results.values())
        errors = list(task.service_errors.values())
        errors.extend(task.extra_errors)

        submission.classification = submission.params.classification
        submission.error_count = len(errors)
        submission.errors = error_keys;
        submission.file_count = len(file_list)
        submission.results = [r.key for r in results]
        submission.max_score = max_score
        submission.state = 'completed'
        submission.times.completed = isotime.now_as_iso()

        self._cleanup_submission(task)
        self.log.info(f"[{sid}] Completed; files: {len(file_list)} results: {len(results)} "
                        f"errors: {len(errors)} score: {max_score}")


        // post processing
        todo!();

        // Finish

        // //         # Send complete message to any watchers.
        // //         watcher_list = self._watcher_list(sid)
        // //         for w in watcher_list.members():
        // //             NamedQueue(w).push(WatchQueueMessage({'status': 'STOP'}).as_primitives())

        // //         # Don't run post processing and traffic notifications if the submission is terminated
        // //         if not task.submission.to_be_deleted:
        // //             # Pull the tags keys and values into a searchable form
        // //             tags = [
        // //                 {'value': _t['value'], 'type': _t['type']}
        // //                 for file_tags in task.file_tags.values()
        // //                 for _t in file_tags.values()
        // //             ]

        // //             # Send the submission for alerting or resubmission
        // //             self.postprocess_worker.process_submission(submission, tags)

        // //             # Write all finished submissions to the traffic queue
        // //             self.traffic_queue.publish(SubmissionMessage({
        // //                 'msg': from_datastore_submission(submission),
        // //                 'msg_type': 'SubmissionCompleted',
        // //                 'sender': 'dispatcher',
        // //             }).as_primitives())

        // //         # Clear the timeout watcher
        // //         watcher_list.delete()
        // //         self.active_submissions.pop(sid)
        // //         self.submissions_assignments.pop(sid)
        // //         self.tasks.pop(sid, None)

        // //         # Count the submission as 'complete' either way
        // //         self.counter.increment('submissions_completed')

        // //         if task.completed_queue:
        // //             NamedQueue(task.completed_queue, self.redis).push(submission.as_primitives())

        datastore.submission.save(sid, submission).await?;
        todo!("update finished/running structs in session");

    }


    async fn start_extracted_file(&mut self, start: ExtractedFile) -> Result<()> {
        // Check if this file has already been started or dropped
        if self.started_files.contains(&start.sha256) { return Ok(()) }
        if self.rejected_files.contains(&start.sha256) { return Ok(()) }

        // Enforce the max extracted limit
        if self.started_files.len() >= self.submission.params.max_extracted as usize {
            self.rejected_files.insert(start.sha256);
            self.save_max_extracted_error(start).await;
            return Ok(())
        }

        // Fetch the info about the parent of this file
        let parent = start.parent;

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
        };

        // Kick off this file for processing
        self.file_tasks.spawn(process_file(self.session.clone(), start.start_file, data));
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
//                 type=filestore_info.type,
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



// //     def _watcher_list(self, sid):
// //         return ExpiringSet(make_watcher_list_name(sid), host=self.redis)

