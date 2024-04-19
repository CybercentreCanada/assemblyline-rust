//! Ingester is responsible for monitoring for incoming submission requests,
//! sending submissions, waiting for submissions to complete, sending a message
//! to a notification queue as specified by the submission and, based on the
//! score received, possibly sending a message to indicate that an alert should
//! be created.

use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;

use assemblyline_models::datastore::filescore::FileScore;
use assemblyline_models::datastore::submission::{SubmissionParams, SubmissionState};
use assemblyline_models::datastore::user::User;
use assemblyline_models::{Sha256, Sid, UpperString};
use assemblyline_models::datastore::alert::ExtendedScanValues;
use assemblyline_models::messages::submission::{Submission as MessageSubmission, SubmissionMessage};
use assemblyline_models::datastore::submission::Submission as DatabaseSubmission;
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use rand::Rng;
use redis_objects::queue::MultiQueue;
use redis_objects::{increment, AutoExportingMetrics, Hashmap, PriorityQueue, Publisher, Queue};
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use tokio::sync::oneshot;

use crate::constants::{Priority, COMPLETE_QUEUE_NAME, INGEST_QUEUE_NAME, METRICS_CHANNEL};
use crate::{await_tasks, spawn_retry_forever, Core};

mod http;

const _DUP_PREFIX: &str = "w-m-";
const _NOTIFICATION_QUEUE_PREFIX: &str = "nq-";
const _MIN_PRIORITY: u16 = 1;
const _MAX_RETRIES: u32 = 10;
const _RETRY_DELAY: chrono::Duration = chrono::Duration::minutes(4); // Wait 4 minutes to retry
const _MAX_TIME: chrono::Duration = chrono::Duration::days(2); // Wait 2 days for responses.
const HOUR_IN_SECONDS: i64 = 60 * 60;

fn read_env_size(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(err) => if let std::env::VarError::NotPresent = err {
            return Ok(default);
        } else {
            return Err(err.into())
        },
    }
}

fn complete_threads() -> Result<usize> {read_env_size("INGESTER_COMPLETE_THREADS", 2)}
fn ingest_threads() -> Result<usize> {read_env_size("INGESTER_INGEST_THREADS", 2)}
fn submit_threads() -> Result<usize> {read_env_size("INGESTER_SUBMIT_THREADS", 8)}


#[derive(Serialize, Deserialize, Debug)]
struct IngestTask {
    // Submission Parameters
    submission: MessageSubmission,

    // Information about the ingestion itself, parameters irrelevant
    retries: u32,

    // Fields added after a submission is complete for notification/bookkeeping processes

    /// If the ingestion has failed for some reason, what is it?
    failure: String,
    // Score from previous processing of this file
    score: Option<i32>,
    // Status of the extended scan
    extended_scan: ExtendedScanValues,
    // Ingestion Identifier
    ingest_id: Sid,
    // Time at which the file was ingested
    ingest_time: DateTime<Utc>,
    // Time at which the user is notify the submission is finished
    notify_time: Option<DateTime<Utc>>,
}

impl IngestTask {
    fn new(submission: MessageSubmission) -> Self {
        Self {
            ingest_id: submission.sid,
            submission,
            failure: Default::default(),
            retries: 0,
            score: None,
            ingest_time: Utc::now(),
            notify_time: None,
            extended_scan: ExtendedScanValues::Skipped,
        }
    }

    // Shortcut for properties of the submission
    fn file_size(&self) -> u64 {
        self.submission.files.iter().fold(0, |a, b| a + b.size.unwrap_or(0))
    }
    
    fn params(&self) -> &SubmissionParams {
        &self.submission.params
    }

    fn sha256(&self) -> &Sha256 {
        &self.submission.files[0].sha256
    }
}

struct GroupCache {
    reset: i64, 
    cache: HashMap<String, Vec<UpperString>>
}

struct Ingester {
    core: Core,

    // Internal. Unique requests are placed in and processed from this queue.
    unique_queue: PriorityQueue<IngestTask>,

    // Internal, delay queue for retrying
    retry_queue: PriorityQueue<IngestTask>,

    // Internal, timeout watch queue
    timeout_queue: PriorityQueue<String>,

    // Internal, queue for processing duplicates
    //   When a duplicate file is detected (same cache key => same file, and same
    //   submission parameters) the file won't be ingested normally, but instead a reference
    //   will be written to a duplicate queue. Whenever a file is finished, in the complete
    //   method, not only is the original ingestion finalized, but all entries in the duplicate queue
    //   are finalized as well. This has the effect that all concurrent ingestion of the same file
    //   are 'merged' into a single submission to the system.
    duplicate_queue: MultiQueue<IngestTask>,

    // State. The submissions in progress are stored in Redis in order to
    // persist this state and recover in case we crash.
    scanning: Hashmap<IngestTask>,

    // Input. The dispatcher creates a record when any submission completes.
    complete_queue: Queue<DatabaseSubmission>,

    // Input. An external process places submission requests on this queue.
    ingest_queue: Queue<MessageSubmission>,

    // Metrics gathering factory
    counter: AutoExportingMetrics<assemblyline_models::messages::ingest_heartbeat::Metrics>,

    // Output. Duplicate our input traffic into this queue so it may be cloned by other systems
    traffic_queue: Publisher,

    // priority_value: HashMap<String, u16>,
    // priority_range: HashMap<String, (u16, u16)>,
    // threshold_value: HashMap<String, u16>,

    user_groups: tokio::sync::Mutex<GroupCache>,
    // self._user_groups_reset = time.time()//HOUR_IN_SECONDS

    // channel to pass data to the senders without going through redis
    queue_bypass: Mutex<VecDeque<oneshot::Sender<Box<IngestTask>>>>,

    cache: Mutex<HashMap<String, FileScore>>,
}

pub async fn main(core: Core) -> Result<()> {
    // Initialize ingester Internal state
    let ingester = Arc::new(Ingester {
        unique_queue: core.redis_persistant.priority_queue("m-unique".to_owned()),
        retry_queue: core.redis_persistant.priority_queue("m-retry".to_owned()),    
        timeout_queue: core.redis_volatile.priority_queue("m-timeout".to_owned()),
        complete_queue: core.redis_volatile.queue(COMPLETE_QUEUE_NAME.to_owned(), None),
        ingest_queue: core.redis_persistant.queue(INGEST_QUEUE_NAME.to_owned(), None),
        counter: core.redis_metrics.auto_exporting_metrics(METRICS_CHANNEL.to_owned(), "ingester".to_owned())
            .counter_name("ingester".to_owned())
            .export_interval(Duration::from_secs(core.config.core.metrics.export_interval as u64))
            .start(),
        traffic_queue: core.redis_volatile.publisher("submissions".to_owned()),
        duplicate_queue: core.redis_persistant.multiqueue(_DUP_PREFIX.to_owned()),
        scanning: core.redis_persistant.hashmap("m-scanning-table".to_owned(), None),
        core,
        cache: Mutex::new(Default::default()),
        user_groups: tokio::sync::Mutex::new(GroupCache{reset: current_hour(), cache: Default::default()}),
        queue_bypass: Mutex::new(Default::default()),
    });

    let mut components = vec![];

    // Launch the http interface
    components.push(("HTTP".to_owned(), http::start(ingester.clone())));

    // Launch the redis interface to pull in new submissions
    for n in 0..ingest_threads()? {
        spawn_retry_forever!(components, ingester, format!("Ingest {n}"), handle_ingest);
    }

    // Launch the redis interface to pull in complete submissions
    for n in 0..complete_threads()? {
        spawn_retry_forever!(components, ingester, format!("Complete {n}"), handle_complete);
    }

    // Launch the submission agents
    let submitters = submit_threads()?;
    let redis_only = (submitters/4).max(1);
    for n in 0..redis_only {
        spawn_retry_forever!(components, ingester, format!("Submit {n}"), handle_submit_redis);
    }
    for n in redis_only..submitters {
        spawn_retry_forever!(components, ingester, format!("Submit {n}"), handle_submit_internal);
    }

    // Launch the retry handler
    spawn_retry_forever!(components, ingester, "Retry Handler", handle_retries);

    // launch the timeout handler
    spawn_retry_forever!(components, ingester, "Timeout Handler", handle_timeouts);

    // Launch missing handler
    spawn_retry_forever!(components, ingester, "Missing Handler", handle_missing);

    // Wait for all of these components to terminate
    await_tasks(components).await; Ok(())
}

impl Ingester {

    #[must_use]
    pub fn is_running(&self) -> bool {
        todo!();
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        todo!();
    }

    pub async fn while_running(&self, duration: Duration) {
        todo!()
    }

    pub async fn sleep(&self, duration: Duration) {
        todo!();
    }

    async fn handle_ingest(self: Arc<Self>) -> Result<()> {
        // Move from ingest to unique and waiting queues.
        // While there are entries in the ingest queue we consume chunk_size
        // entries at a time and move unique entries to uniqueq / queued and
        // duplicates to their own queues / waiting.
        while self.is_running() {
            while !self.is_active() {
                // Ingester is disabled... waiting for it to be reactivated
                self.sleep(Duration::from_millis(100)).await;
            }

            let message = match self.ingest_queue.pop_timeout(Duration::from_secs(1)).await {
                Ok(message) => message,
                Err(err) => {
                    increment!(self.counter, error);
                    error!("Dropped ingest submission {err}");
                    continue
                },
            };

            let message = match message {
                Some(message) => message,
                None => continue,
            };

            // try:
                // if 'submission' in message:
                //     # A retried task
                //     task = IngestTask(message)
                // else:
                //     # A new submission
                //     sub = MessageSubmission(message)
            let mut task = Box::new(IngestTask::new(message));
            task.submission.sid = rand::thread_rng().gen(); // Reset to new random uuid
            
            // Write all input to the traffic queue
            self.traffic_queue.publish(&SubmissionMessage::ingested(task.submission.clone()));

            self.spawn_ingest(task);
        }
        Ok(())
    }

    async fn handle_complete(self: &Arc<Self>) -> Result<()> {
        while self.is_running() {
            let result = match self.complete_queue.pop_timeout(Duration::from_secs(3)).await? {
                Some(result) => result,
                None => continue
            };

            let session = self.clone();
            tokio::spawn(async move {
                if let Err(err) = session.completed(result).await {
                    error!("Error in completing submission: {err}");
                }
            });
        }
        Ok(())
    }

    async fn handle_submit_redis(self: &Arc<Self>) -> Result<()> {
        self.handle_submit(true).await
    }

    async fn handle_submit_internal(self: &Arc<Self>) -> Result<()> {
        self.handle_submit(false).await
    }
    
    async fn handle_submit(self: &Arc<Self>, block_on_redis: bool) -> Result<()> {
        while self.is_running() {
            // Check if there is room for more submissions
            let length = self.scanning.length().await?;
            if length >= self.core.config.core.ingester.max_inflight {
                self.sleep(Duration::from_millis(100)).await;
                continue
            }

            // try to get a new task
            let mut task = if block_on_redis {
                match self.unique_queue.blocking_pop(Duration::from_secs(3), false).await? {
                    Some(task) => Box::new(task),
                    None => continue,
                }
            } else {
                match self.unique_queue.pop(1).await?.pop() {
                    Some(task) => Box::new(task),
                    None => match self.pop_internal_unique_queue().await {
                        Some(task) => task,
                        None => continue,
                    },
                }
            };
            let sha256 = task.sha256().clone();

            // Check if we need to drop a file for capacity reasons, but only if the
            // number of files in flight is alreay over 80%
            if length >= (self.core.config.core.ingester.max_inflight as f64 * 0.8) as u64 && self.drop_task(&mut task).await? {
                // End of ingest message (dropped)
                continue
            }

            // if self.is_whitelisted(&task).await? {
            //     // End of ingest message (whitelisted)
            //     continue
            // }

            // Check if this file has been previously processed.
            let (pprevious, previous, score, scan_key) = if !task.submission.params.ignore_cache {
                self.check(&mut task, true).await?
            } else {
                (None, None, None, Self::stamp_filescore_key(&mut task, None))
            };

            // If it HAS been previously processed, we are dealing with a resubmission
            // finalize will decide what to do, and put the task back in the queue
            // rewritten properly if we are going to run it again
            if let Some(previous) = previous {
                let resubmit_empty = task.submission.params.services.resubmit.as_ref().map_or(true, |v|v.is_empty());
                if !resubmit_empty && pprevious.is_none() {
                    warn!("No psid for what looks like a resubmission of {sha256}: {scan_key}");
                }
                self.finalize(pprevious, previous, score, &mut task, true).await?;
                // End of ingest message (finalized)
                continue
            }

            // We have decided this file is worth processing

            // Add the task to the scanning table, this is atomic across all submit
            // workers, so if it fails, someone beat us to the punch, record the file
            // as a duplicate then.
            if !self.scanning.add(&scan_key, &task).await? {
                debug!("Duplicate {sha256}");
                increment!(self.counter, duplicates);
                self.duplicate_queue.push(&scan_key, &task).await?;
                // End of ingest message (duplicate)
                continue
            }

            // We have managed to add the task to the scan table, so now we go
            // ahead with the submission process
            let err = match self.submit(scan_key.clone(), task).await {
                Ok(()) => continue,
                Err(err) => err,
            };

            // For some reason (contained in err) we have failed the submission
            // The rest of this function is error handling/recovery
            increment!(self.counter, error);

            error!("Submission for file '{sha256}' failed due to: {err}");

            let mut task = match self.scanning.pop(&scan_key).await? {
                Some(task) => task,
                None => {
                    error!("No scanning entry for for {sha256}");
                    continue
                }
            };

            self.retry(&mut task, &scan_key, &err.to_string()).await?;
            // End of ingest message (retry)
        }
        Ok(())
    }

    async fn handle_retries(self: &Arc<Ingester>) -> Result<()> {
        while self.is_running() {
            let now = chrono::Utc::now().timestamp() as f64;
            let tasks = self.retry_queue.dequeue_range(None, Some(now), None, Some(100)).await?;
            let task_count = tasks.len();

            for task in tasks {
                self.spawn_ingest(Box::new(task));
            }
    
            if task_count == 0 {
                self.sleep(Duration::from_secs(3)).await;
            }
        }
        Ok(())
    }

    async fn handle_timeouts(self: Arc<Self>) -> Result<()> {
        while self.is_running() {
            let now = chrono::Utc::now().timestamp() as f64;
            let timeouts = self.timeout_queue.dequeue_range(None, Some(now), None, Some(100)).await?;
            let timeouts_count = timeouts.len();

            for scan_key in timeouts {
                if let Err(err) = self.timeout_single(&scan_key).await {
                    error!("Problem timing out {scan_key}: {err}")
                }
            }

            if timeouts_count == 0 {
                self.sleep(Duration::from_secs(3)).await;
            }
        }
        Ok(())
    }

    async fn timeout_single(&self, scan_key: &str) -> Result<()> {
        let mut actual_timeout = false;

        // Remove the entry from the hash of submissions in progress.
        if let Some(entry) = self.scanning.pop(scan_key).await? {
            actual_timeout = true;
            error!("Submission timed out for {scan_key}: {entry:?}");
        }

        let mut dup = self.duplicate_queue.pop_nonblocking(scan_key).await?;
        if dup.is_some() {
            actual_timeout = true;
        }

        while let Some(found) = dup {
            error!("Submission timed out for {scan_key}: {:?}", found);
            dup = self.duplicate_queue.pop_nonblocking(scan_key).await?;
        }

        if actual_timeout {
            increment!(self.counter, timed_out);
        }
        Ok(())
    }

    /// Messages get dropped or only partially processed when ingester and dispatcher containers scale up and down.
    ///
    /// This loop checks for submissions that are in two invalid states:
    ///     - finished but still listed as being scanned by ingester (message probably dropped by ingester)
    ///     - listed by ingester but unknown by dispatcher (message could have been dropped on either end)
    ///
    /// Loading all the info needed to do these checks is a bit slow, but doing them every 5 or 15 minutes
    /// per ingester shouldn't be noteworthy. While these missing messages are bound to happen from time to time
    /// they should be rare. With that in mind, a warning is raised whenever this worker processes something
    /// so that if a constant stream of items are falling through and getting processed here it might stand out.
    async fn handle_missing(self: &Arc<Self>) -> Result<()> {
        let mut last_round: HashSet<Sid> = Default::default();

        while self.is_running() {
            // Get the current set of outstanding tasks
            let mut outstanding = self.scanning.items().await?;

            // Get jobs being processed by dispatcher or in dispatcher queue
            let mut assignment: HashMap<Sid, String> = Default::default();
            for data in self.core.dispatch_submission_queue.content().await? {
                assignment.insert(data.submission.sid, "".to_owned());
            }
            for dis in self.core.dispatcher_instances().await? {
                for key in self.core.dispatcher_assignment(&dis).await? {
                    assignment.insert(key, dis.clone());
                }
            }

            // Filter out outstanding tasks currently assigned or in queue
            outstanding.retain(|_, doc| !assignment.contains_key(&doc.submission.sid));

            let mut unprocessed = vec![];
            for (scan_key, task) in outstanding {
                let sid = task.submission.sid;

                // Check if its already complete in the database
                let from_db = self.core.datastore.submission.get(&sid.to_string()).await?;
                if let Some(from_db) = from_db {
                    if from_db.state == SubmissionState::Completed {
                        warn!("Completing a hanging finished submission [{sid}]");
                        self.completed(from_db).await?;
                        continue
                    }
                }

                // Check for items that have been in an unknown state since the last round
                // and put it back in processing
                if last_round.contains(&sid) {
                    warn!("Recovering a submission dispatcher hasn't processed [{sid}]");
                    self.submit(scan_key, Box::new(task)).await?;
                    continue
                }

                // Otherwise defer looking at this until next iteration
                unprocessed.push(sid);
            }

            // store items for next round
            last_round = unprocessed.into_iter().collect();

            // wait a few minutes before checking again
            if last_round.is_empty() {
                self.sleep(Duration::from_secs(900)).await;
            } else {
                self.sleep(Duration::from_secs(300)).await;
            }
        }
        Ok(())
    }

    async fn spawn_ingest(self: &Arc<Self>, task: Box<IngestTask>) {
        if let Err(err) = self.ingest(task).await {
            error!("Error while ingesting a file: {err}");
        }
    }

    async fn ingest(self: &Arc<Self>, mut task: Box<IngestTask>) -> Result<()> {
        info!("[{} :: {}] Task received for processing", task.ingest_id, task.sha256());
        // Load a snapshot of ingest parameters as of right now.
        let max_file_size = self.core.config.submission.max_file_size;
        // let param = task.params();

        increment!(self.counter, bytes_ingested, task.file_size());
        increment!(self.counter, submissions_ingested);

        // if any(len(file.sha256) != 64 for file in task.submission.files):
        //     self.log.error(f"[{task.ingest_id} :: {task.sha256}] Invalid sha256, skipped")
        //     self.send_notification(task, failure="Invalid sha256", logfunc=self.log.warning)
        //     return

        // Clean up metadata strings, since we may delete some, iterate on a copy of the keys
        for (key, value) in task.submission.metadata.clone() {
            if value.len() > self.core.config.submission.max_metadata_length as usize {
                info!("[{} :: {}] Removing {key} from metadata because value is too big", task.ingest_id, task.sha256());
                task.submission.metadata.remove(&key);
            }
        }

        if task.file_size() > max_file_size && !task.params().ignore_size && !task.params().never_drop {
            task.failure = format!("File too large ({} > {max_file_size})", task.file_size());
            self._notify_drop(&mut task).await?;
            increment!(self.counter, skipped);
            error!("[{} :: {}] {}", task.ingest_id, task.sha256(), task.failure);
            return Ok(())
        }

        // Set the groups from the user, if they aren't already set
        if task.params().groups.is_empty() {
            let classification_string = task.params().classification.as_str().to_string();
            for g in self.get_groups_from_user(&task.params().submitter).await? {
                if classification_string.contains(g.deref()) {
                    task.submission.params.groups.push(g);
                }
            }
        }

        // Check if this file is already being processed
        Self::stamp_filescore_key(&mut task, None);
        let (pprevious, previous, score, _) = if !task.params().ignore_cache {
            self.check(&mut task, false).await?
        } else {
            (None, None, None, "".to_owned())
        };

        // Assign priority.
        // let low_priority = self.is_low_priority(&task);

        let mut priority = task.params().priority;
        // if priority < 0:
        //     priority = self.priority_value['medium']

        //     if score is not None:
        //         priority = self.priority_value['low']
        //         for level, threshold in self.threshold_value.items():
        //             if score >= threshold:
        //                 priority = self.priority_value[level]
        //                 break
        //     elif low_priority:
        //         priority = self.priority_value['low']

        // Reduce the priority by an order of magnitude for very old files.
        let current_time = Utc::now();
        if priority > 0 && self.expired((current_time - task.submission.time).num_seconds() as f64, 0) {
            priority = (priority / 10).max(1);
        }
        task.submission.params.priority = priority;

        // Do this after priority has been assigned.
        // (So we don't end up dropping the resubmission).
        if let Some(previous) = previous {
            increment!(self.counter, duplicates);
            self.finalize(pprevious, previous, score, &mut task, true).await?;

            // On cache hits of any kind we want to send out a completed message
            self.traffic_queue.publish(&SubmissionMessage::completed(task.submission.clone(), "ingester".to_owned())).await?;
            return Ok(())
        }

        if self.drop_task(&mut task).await? {
            info!("[{} :: {}] Dropped", task.ingest_id, task.sha256());
            return Ok(())
        }

        // if self.is_whitelisted(&task).await? {
        //     info!("[{} :: {}] Whitelisted", task.ingest_id, task.sha256());
        //     return Ok(())
        // }

        if let Some(task) = self.push_internal_unique_queue(task).await {
            self.unique_queue.push(priority as f64, &task).await?;
        }
        Ok(())
    }

    /// Invoked when notified that a submission has completed.
    async fn completed(self: &Arc<Self>, sub: DatabaseSubmission) -> Result<String> {
        // There is only one file in the submissions we have made
        let sha256 = sub.files[0].sha256.clone();
        let ingest_id = match sub.metadata.get("ingest_id") {
            Some(id) => id.to_owned(),
            None => "unknown".to_owned(),
        };
        let scan_key = match sub.scan_key {
            Some(key) => key,
            None => {
                warn!("[{ingest_id} :: {sha256}] Submission missing scan key");
                sub.params.create_filescore_key(&sha256, None)
            }
        };

        let mut task = match self.scanning.pop(&scan_key).await? {
            Some(task) => task,
            None => {
                // Some other worker has already popped the scanning queue?
                warn!("[{ingest_id} :: {sha256}] Submission completed twice");
                return Ok(scan_key)
            }
        };

        let psid = sub.params.psid;
        let score = sub.max_score;
        let sid = sub.sid;
        task.submission.sid = sid;

        let errors = sub.error_count;
        let file_count = sub.file_count;
        increment!(self.counter, submissions_completed);
        increment!(self.counter, files_completed, file_count as u32);
        increment!(self.counter, bytes_completed, task.file_size());

        let fs = FileScore{
            expiry_ts: Utc::now() + chrono::Duration::days(self.core.config.core.ingester.cache_dtl as i64),
            errors,
            psid,
            score,
            sid,
            time: Utc::now().timestamp() as f64,
        };
        self.cache.lock().insert(scan_key.clone(), fs.clone());
        self.core.datastore.filescore.save(&scan_key, &fs, None, None).await?;

        self.finalize(psid, sid, Some(score), &mut task, false).await?;


        // You may be tempted to remove the assignment to dups and use the
        // value directly in the for loop below. That would be a mistake.
        // The function finalize may push on the duplicate queue which we
        // are pulling off and so condensing those two lines creates a
        // potential infinite loop.
        let mut dups = vec![];
        loop {
            match self.duplicate_queue.pop_nonblocking(&scan_key).await? {
                None => break,
                Some(mut res) => {
                    res.submission.sid = sid;
                    dups.push(res);        
                }
            }
        }

        for mut dup in dups {
            self.finalize(psid, sid, Some(score), &mut dup, true).await?;
        }

        // def exhaust() -> Iterable[IngestTask]:
        //     while True:
        //         res = self.duplicate_queue.pop(scan_key, blocking=False)
        //         if res is None:
        //             break
        //         res = IngestTask(res)
        //         res.submission.sid = sid
        //         yield res

        // // You may be tempted to remove the assignment to dups and use the
        // // value directly in the for loop below. That would be a mistake.
        // // The function finalize may push on the duplicate queue which we
        // // are pulling off and so condensing those two lines creates a
        // // potential infinite loop.
        // dups = [dup for dup in exhaust()]
        // for dup in dups:
        //     self.finalize(psid, sid, score, dup, cache=True)

        return Ok(scan_key)
    }

    async fn drop_task(&self, task: &mut IngestTask) -> Result<bool> {
        let priority = task.submission.params.priority;
        let sample_threshold = &self.core.config.core.ingester.sampling_at;

        let mut dropped = false;
        if priority <= _MIN_PRIORITY {
            dropped = true;
        } else {
            for level in Priority::iter() {
                let (low, high) = level.range();
                if low <= priority && priority <= high {
                    if let Some(threshold) = sample_threshold.get(level) {
                        dropped = must_drop(self.unique_queue.count(*low as f64, *high as f64).await?, *threshold);
                        break
                    }
                }
            }

            let too_big = task.file_size() > self.core.config.submission.max_file_size && !task.submission.params.ignore_size;
            if too_big || task.file_size() == 0 {
                dropped = true;
            }
        }

        if task.submission.params.never_drop || !dropped {
            return Ok(false)
        }

        task.failure = "Skipped".to_owned();
        self._notify_drop(task).await?;
        increment!(self.counter, skipped);
        return Ok(true)
    }

    async fn _notify_drop(&self, task: &mut IngestTask) -> Result<()> {
        self.send_notification(&mut task, None, false).await?;

        let c12n = task.params().classification;
        let expiry = Utc::now() + chrono::Duration::seconds(86400);
        let sha256 = task.submission.files[0].sha256;

        self.core.datastore.save_or_freshen_file(sha256, {'sha256': sha256}, expiry, c12n, redis=self.redis).await?;
        Ok(())
    }

    // async fn is_whitelisted(&self, task: &IngestTask) -> Result<bool> {
        // the hardcoded whitelist was empty
        // reason, hit = self.get_whitelist_verdict(self.whitelist, task)
        // hit = {x: dotdump(safe_str(y)) for x, y in hit.items()}
        // sha256 = task.submission.files[0].sha256

        // if not reason:
        //     with self.whitelisted_lock:
        //         reason = self.whitelisted.get(sha256, None)
        //         if reason:
        //             hit = 'cached'

        // if reason:
        //     if hit != 'cached':
        //         with self.whitelisted_lock:
        //             self.whitelisted[sha256] = reason

        //     task.failure = "Whitelisting due to reason %s (%s)" % (dotdump(safe_str(reason)), hit)
        //     self._notify_drop(task)

        //     self.counter.increment('whitelisted')

        // return reason
    // }

    async fn get_groups_from_user(&self, username: &str) -> Result<Vec<UpperString>> {
        // Reset the group cache at the top of each hour
        let mut cache = self.user_groups.lock().await;
        let now = current_hour();

        if now > cache.reset {
            cache.cache.clear();
            cache.reset = now;
        }

        // Get the groups for this user if not known
        if let Some(groups) = cache.cache.get(username) {
            return Ok(groups.clone())
        }

        let user_data: Option<User> = self.core.datastore.user.get(username).await?;
        if let Some(user_data) = user_data {
            cache.cache.insert(username.to_owned(), user_data.groups.clone());
            Ok(user_data.groups)
        } else {
            cache.cache.insert(username.to_owned(), vec![]);
            Ok(vec![])
        }
    }

//     def check(self, task: IngestTask, count_miss=True) -> Tuple[Optional[str], Optional[str], Optional[float], str]:
    async fn check(&self, task: &mut IngestTask, count_miss: bool) -> Result<(Option<Sid>, Option<Sid>, Option<i32>, String)> {
        let key = Self::stamp_filescore_key(task, None);

        let result = match self.cache.lock().get(&key) {
            Some(result) => {
                increment!(self.counter, cache_hit_local);
                info!("[{} :: {}] Local cache hit", task.ingest_id, task.sha256());
                result.clone()
            },
            None => {
                let result = match self.core.datastore.filescore.get(&key).await? {
                    Some(result) => {
                        increment!(self.counter, cache_hit);
                        info!("[{} :: {}] Remote cache hit", task.ingest_id, task.sha256());
                        result
                    }
                    None => {
                        if count_miss {
                            increment!(self.counter, cache_miss);
                        }
                        return Ok((None, None, None, key));
                    }
                };

                self.cache.lock().insert(key, result.clone());
                result
            },
        };

        let current_time = Utc::now().timestamp() as f64;
        let age = current_time - result.time;
        let errors = result.errors;

        if self.expired(age, errors) {
            info!("[{} :: {}] Cache hit dropped, cache has expired", task.ingest_id, task.sha256());
            increment!(self.counter, cache_expired);
            self.cache.lock().remove(&key);
            self.core.datastore.filescore.delete(&key).await?;
            return Ok((None, None, None, key))
        } else if self.stale(age, errors) {
            info!("[{} :: {}] Cache hit dropped, cache is stale", task.ingest_id, task.sha256());
            increment!(self.counter, cache_stale);
            return Ok((None, None, Some(result.score), key))
        }

        return Ok((result.psid, Some(result.sid), Some(result.score), key))
    }

//     def stop(self):
//         super().stop()
//         if self.apm_client:
//             elasticapm.uninstrument()
//         self.submit_client.stop()
//         self.postprocess_worker.stop()

    fn expired(&self, delta: f64, errors: i32) -> bool {
        if errors > 0 {
            delta >= self.core.config.core.ingester.incomplete_expire_after_seconds
        } else {
            delta >= self.core.config.core.ingester.expire_after
        }
    }

    fn stale(&self, delta: f64, errors: i32) -> bool {
        if errors > 0 {
            delta >= self.core.config.core.ingester.incomplete_stale_after_seconds
        } else {
            delta >= self.core.config.core.ingester.stale_after_seconds
        }
    }

    fn stamp_filescore_key(task: &mut IngestTask, sha256: Option<Sha256>) -> String {
        let sha256 = match sha256 {
            Some(hash) => hash,
            None => task.submission.files[0].sha256
        };

        match task.submission.scan_key {
            Some(key) => key,
            None => {
                let key = task.submission.params.create_filescore_key(&sha256, None);
                task.submission.scan_key = Some(key);
                key
            }
        }
    }

    async fn send_notification(&self, task: &mut IngestTask, failure: Option<String>, warning: bool) -> Result<()> {
        if let Some(failure) = failure {
            task.failure = failure;
        }

        let failure = &task.failure;
        if !failure.is_empty() {
            if warning {
                warn!("{failure}: {task:?}");
            } else {
                info!("{failure}: {task:?}");
            }
        }

        let queue_name = match &task.submission.notification.queue {
            Some(queue) => queue,
            None => return Ok(()),
        };

        let queue_name = _NOTIFICATION_QUEUE_PREFIX.to_owned() + queue_name;
        let threshold = match task.submission.notification.threshold {
            Some(value) => value,
            None => return Ok(())
        };

        if let Some(score) = task.score {
            if score < threshold {
                return Ok(())
            }
        } else {
            return Ok(())
        }

        let queue = self.core.redis_persistant.queue::<IngestTask>(queue_name, None);

        // Mark at which time an item was queued
        task.notify_time = Some(Utc::now());

        queue.push(task).await?;
        Ok(())
    }

    async fn submit(&self, scan_key: String, task: Box<IngestTask>) -> Result<()> {
        let sha = task.submission.files[0].sha256.clone();
        self.core.submit_prepared(
            task.submission,
            Some(COMPLETE_QUEUE_NAME.to_owned()),
        ).await?;

        self.timeout_queue.push((Utc::now() + _MAX_TIME).timestamp() as f64, &scan_key).await?;
        info!("[{} :: {}] Submitted to dispatcher for analysis", task.ingest_id, sha);
        Ok(())
    }

    async fn retry(&self, task: &mut IngestTask, scan_key: &str, err: &str) -> Result<()> {
        let current_time = Utc::now();

        let retries = task.retries + 1;

        if retries > _MAX_RETRIES {
            error!("[{} :: {}] Max retries exceeded {err}", task.ingest_id, task.sha256());
            self.duplicate_queue.delete( scan_key).await?;
        } else if self.expired((current_time - task.ingest_time).num_seconds() as f64, 0) {
            info!("[{} :: {}] No point retrying expired submission", task.ingest_id, task.sha256());
            self.duplicate_queue.delete( scan_key).await?;
        } else {
            info!("[{} :: {}] Requeuing ({err})", task.ingest_id, task.sha256());
            task.retries = retries;
            self.retry_queue.push((current_time + _RETRY_DELAY).timestamp() as f64, &task).await?;
        }
        Ok(())
    }

    /// cache = False
    async fn finalize(&self, psid: Option<Sid>, sid: Sid, score: Option<i32>, task: &mut IngestTask, cache: bool) -> Result<()> {
        // let cache = cache.unwrap_or(false);
        info!("[{} :: {}] Completed", task.ingest_id, task.sha256());
        if let Some(psid) = psid {
            task.submission.params.psid = Some(psid);
        }
        task.score = score;
        task.submission.sid = sid;

        if cache {
            did_resubmit = self.postprocess_worker.process_cachehit(task.submission, score)

            if did_resubmit {
                task.extended_scan = ExtendedScanValues::Submitted;
                task.submission.params.psid = None;
            }
        }

        self.send_notification(&mut task, None, false).await?;
        Ok(())
    }

    async fn pop_internal_unique_queue(&self) -> Option<Box<IngestTask>> {
        // put our collector into the queue
        let (send, mut recv) = oneshot::channel();
        self.queue_bypass.lock().push_back(send);

        // wait for our queue to return
        tokio::select! {
            task = &mut recv => {
                if let Ok(task) = task {
                    return Some(task)
                }
            }
            _ = self.while_running(Duration::from_secs(120)) => {}
        }
        
        // if its a timeout close the collector and check for a last minute value
        recv.close();
        if let Ok(task) = recv.try_recv() {
            return Some(task)
        }

        // flush out extra items in the queue
        let mut queue = self.queue_bypass.lock();
        while let Some(front) = queue.front() {
            if front.is_closed() {
                queue.pop_front();
            } else {
                break
            }
        }
        None
    }

    /// Push task into the internal queue, return it if it couldn't be pushed
    async fn push_internal_unique_queue(&self, mut task: Box<IngestTask>) -> Option<Box<IngestTask>> {
        let mut queue = self.queue_bypass.lock();
        while let Some(front) = queue.pop_front() {
            task = match front.send(task) {
                Ok(()) => return None,
                Err(value) => value,
            }
        }
        Some(task)
    }

}


// import logging
// import threading
// import time
// from os import environ
// from random import random
// from typing import Any, Iterable, List, Optional, Tuple

// import elasticapm

// from assemblyline.common.postprocess import ActionWorker
// from assemblyline_core.server_base import ThreadedCoreBase
// from assemblyline.common.metrics import MetricsFactory
// from assemblyline.common.str_utils import dotdump, safe_str
// from assemblyline.common.exceptions import get_stacktrace_info
// from assemblyline.common.isotime import now, now_as_iso
// from assemblyline.common.importing import load_module_by_path
// from assemblyline.common import forge, exceptions, isotime
// from assemblyline.datastore.exceptions import DataStoreException
// from assemblyline.filestore import CorruptedFileStoreException, FileStoreException
// from assemblyline.odm.models.filescore import FileScore
// from assemblyline.odm.models.user import User
// from 
// from assemblyline.remote.datatypes.queues.named import NamedQueue
// from assemblyline.remote.datatypes.queues.priority import PriorityQueue
// from assemblyline.remote.datatypes.queues.comms import CommsQueue
// from assemblyline.remote.datatypes.queues.multi import MultiQueue
// from assemblyline.remote.datatypes.hash import Hash
// from assemblyline import odm
// from assemblyline.odm.models.submission import SubmissionParams, Submission as DatabaseSubmission
// from assemblyline.odm.models.alert import EXTENDED_SCAN_VALUES
// from assemblyline.odm.messages.submission import Submission as MessageSubmission, SubmissionMessage

// from assemblyline_core.dispatching.dispatcher import Dispatcher
// from assemblyline_core.submission_client import SubmissionClient



/// To calculate the probability of dropping an incoming submission we compare
/// the number returned by random() which will be in the range [0,1) and the
/// number returned by tanh() which will be in the range (-1,1).
///
/// If length is less than maximum the number returned by tanh will be negative
/// and so drop will always return False since the value returned by random()
/// cannot be less than 0.
///
/// If length is greater than maximum, drop will return False with a probability
/// that increases as the distance between maximum and length increases:
///
///   Length           Chance of Dropping
///
///     <= maximum       0
///     1.5 * maximum    0.76
///     2 * maximum      0.96
///     3 * maximum      0.999
fn must_drop(length: u64, maximum: i64) -> bool {
    rand::thread_rng().gen::<f64>() < drop_chance(length, maximum)
}

fn drop_chance(length: u64, maximum: i64) -> f64 {
    let length = length as f64;
    let maximum = maximum as f64;
    f64::max(0.0, f64::tanh((length - maximum) / maximum * 2.0))
}

// class Ingester(ThreadedCoreBase):
//     def __init__(self, datastore=None, logger: Optional[logging.Logger] = None,
//                  classification=None, redis=None, persistent_redis=None,
//                  , config=None):
//         super().__init__('assemblyline.ingester', logger, redis=redis, redis_persist=persistent_redis,
//                          datastore=datastore, config=config)

//         # Cache the user groups
//         self.notification_queues: dict[str, NamedQueue] = {}
//         self.whitelisted: dict[str, Any] = {}
//         self.whitelisted_lock = threading.RLock()

//         # Module path parameters are fixed at start time. Changing these involves a restart
//         self.is_low_priority = load_module_by_path(self.config.core.ingester.is_low_priority)
//         self.get_whitelist_verdict = load_module_by_path(self.config.core.ingester.get_whitelist_verdict)
//         self.whitelist = load_module_by_path(self.config.core.ingester.whitelist)

//         # Constants are loaded based on a non-constant path, so has to be done at init rather than load
//         constants = forge.get_constants(self.config)
//         self.priority_value: dict[str, int] = constants.PRIORITIES
//         self.priority_range: dict[str, Tuple[int, int]] = constants.PRIORITY_RANGES
//         self.threshold_value: dict[str, int] = constants.PRIORITY_THRESHOLDS

//         # Classification engine
//         self.ce = classification or forge.get_classification()



//         # Utility object to help submit tasks to dispatching
//         self.submit_client = SubmissionClient(datastore=self.datastore, redis=self.redis)
//         # Utility object to handle post-processing actions
//         self.postprocess_worker = ActionWorker(cache=True, config=self.config, datastore=self.datastore,
//                                                redis_persist=self.redis_persist)


fn current_hour() -> i64 {
    Utc::now().timestamp()/HOUR_IN_SECONDS
}
