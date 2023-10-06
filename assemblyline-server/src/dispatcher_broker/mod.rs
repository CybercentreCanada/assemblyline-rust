
//!
//! workers:
//!  - move invalid
//!  - monitor for finished jobs
//!

mod interface;
mod database;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use assemblyline_models::Sid;
use assemblyline_models::config::Config;
use assemblyline_models::datastore::Submission;
use assemblyline_models::datastore::submission::SubmissionState;
use futures::StreamExt;
use log::error;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{oneshot, mpsc, Mutex};
use tokio::task::{JoinHandle, JoinSet};

use self::database::Database;
use self::interface::{start_interface, StartSubmissionRequest};
use crate::dispatcher::{DispatchStatusMessage, LoadInfo};
use crate::error::Result;


struct BrokerFlags {
    pub running: AtomicBool,
}

impl Default for BrokerFlags {
    fn default() -> Self {
        Self {
            running: AtomicBool::new(true),
        }
    }
}

pub (crate) enum SubmissionWatchStatus {
    NotFound,
    SlotOccupied,
    Submission(Submission)
}

pub (crate) enum InternalMessage {
    SubmissionStart(Sid, SocketAddr, bool),
    InstallSubmissionWatch(Sid, oneshot::Sender<SubmissionWatchStatus>)
}

struct BrokerSession {
    config: Config,
    instance: usize,
    instance_count: usize,
    flags: BrokerFlags,
    flags_changes: tokio::sync::Notify,
    dispatcher_data: tokio::sync::RwLock<HashMap<SocketAddr, LoadInfo>>,
    database: Database,
    http_client: reqwest::Client,
    internal_sender: mpsc::Sender<InternalMessage>,
    current_move_job: Mutex<Option<JoinHandle<Result<()>>>>,
}

impl BrokerSession {

    pub fn peer_address(&self, instance: usize) -> String {
        format!("dispatcher-broker-{instance}")
    }

    pub fn peer_url(&self, instance: usize, route: &str) -> Result<url::Url> {
        let config = &self.config.core.dispatcher.broker_bind;
        let mut url = url::Url::parse(&format!("https://{}", self.peer_address(instance)))?;

        url.set_port(Some(config.address.port()))?;

        if let Some(path) = &config.path {
            if !path.is_empty() {
                url.set_path(if !path.ends_with("/") {
                    &format!("{path}/")
                } else {
                    path
                });
            }
        }

        Ok(url.join(route)?)
    }

    async fn move_misplaced_submissions(self: &Arc<BrokerSession>) {
        let mut job = self.current_move_job.lock().await;

        *job = match job.take() {
            Some(job) => {
                if job.is_finished() {
                    match job.await {
                        Ok(Ok(())) => {},
                        Ok(Err(err)) => { error!("Error in move_misplaced_submissions {err}"); },
                        Err(err) => { error!("Task error on move_misplaced_submissions {err}"); },
                    }
                    Some(tokio::spawn(_move_misplaced_submissions(self.clone())))
                } else {
                    Some(job)
                }
            },
            None => {
                Some(tokio::spawn(_move_misplaced_submissions(self.clone())))
            },
        }
    }

    pub async fn select_dispatcher(&self) -> SocketAddr {
        todo!()
    }

    pub async fn assign_submission(&self, submission: Submission, dispatcher: Option<SocketAddr>, retain: bool) -> Result<()> {
        let submission = Arc::new(submission);
        if let SubmissionState::Submitted = submission.state {
            // check if the submission is already present
            if self.database.have_submission(&submission.sid).await? {
                return Ok(())
            }

            // assign to a dispatcher
            let dispatcher = match dispatcher {
                None => self.select_dispatcher().await,
                Some(dispatcher) => dispatcher
            };

            // save to disk
            self.database.assign_submission(submission.clone(), dispatcher, retain).await?;

            // notify the dispatcher
            self.send_submission(&submission, dispatcher).await?;
            self.internal_sender.send(InternalMessage::SubmissionStart(submission.sid, dispatcher, retain)).await;
        } else {
            self.database.finish_submission(submission).await;
        }
        Ok(())
    }

    pub async fn send_submission(&self, submission: &Submission, dispatcher: SocketAddr) -> Result<()> {
        self.http_client.post(format!("https://{dispatcher}/submission")).json(submission).send().await?;
        Ok(())
    }
}

pub async fn launch(config: Config, instance: usize, instance_count: usize) -> Result<()> {
    // setup queues
    let (internal_sender, internal_recv) = tokio::sync::mpsc::channel(200);

    // connect to database
    let database = Database::open(&config).await?;

    // build an http client
    let http_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    // create session
    let session = Arc::new(BrokerSession {
        config,
        flags: Default::default(),
        flags_changes: tokio::sync::Notify::new(),
        current_move_job: tokio::sync::Mutex::new(None),
        // elastic,
        database,
        instance,
        instance_count,
        http_client,
        dispatcher_data: tokio::sync::RwLock::new(Default::default()),
        internal_sender,
    });

    // setup exit conditions
    let mut term_signal = signal(SignalKind::terminate())?;
    let term_session = session.clone();
    tokio::spawn(async move {
        term_signal.recv().await;
        term_session.flags.running.store(false, Ordering::Release);
        term_session.flags_changes.notify_waiters();
    });

    // start API
    let api = tokio::spawn(start_interface(session.clone()));

    // launch move worker
    session.move_misplaced_submissions().await;

    // Watch for dispatcher status
    tokio::spawn(monitor_dispatchers(session.clone()));

    // launch monitor worker
    tokio::spawn(monitor_submissions(session, internal_recv));

    // wait for the api
    api.await.expect("API task failed")
}

async fn _move_misplaced_submissions(session: Arc<BrokerSession>) -> Result<()> {
    // Get a list of all submissions on this broker
    let submissions = session.database.list_all_submissions().await?;

    // make sure the local flag receiver is seen
    if !session.flags.running.load(Ordering::Acquire) {
        return Ok(())
    }

    for (sid, status) in submissions {
        // if this submission belongs here continue
        let target_broker = sid.assign(session.instance_count);
        if target_broker != session.instance {
            continue
        }

        // if the submission doesn't belong here load its content
        let url = session.peer_url(target_broker, "submission")?;
        if let Some(submission) = session.database.get_submission(&sid).await? {

            let (assignment, retain) = match status {
                database::Status::Assigned(dispatcher, retain, _) => (Some(dispatcher), retain),
                database::Status::Finished(_) => (None, true),
            };

            // send it to the right broker
            let handled = session.http_client.post(url)
                .json(&StartSubmissionRequest { submission, assignment, refuse_forward: true, retain } )
                .send().await?;

            // if that worked clear the local data
            if handled.status() == reqwest::StatusCode::OK {
                session.database.delete_submission(&sid).await?;
            }
        }

        // check if the flags have changed
        if session.flags.running.load(Ordering::Acquire) {
            return Ok(())
        }
    }

    // if we get through all the listed submissions we are free to stop this worker
    return Ok(())
}

async fn monitor_submissions(session: Arc<BrokerSession>, queue: mpsc::Receiver<InternalMessage>) {
    let queue = Arc::new(Mutex::new(queue));
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_monitor_submissions(session.clone(), queue.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_submissions {err}"); },
            Err(err) => { error!("Task error on monitor_submissions {err}"); },
        }
    }
}

async fn _monitor_submissions(session: Arc<BrokerSession>, queue: Arc<Mutex<mpsc::Receiver<InternalMessage>>>) -> Result<()> {
    use database::Status;
    use std::collections::hash_map::Entry;

    let mut queue = queue.lock().await;
    let mut clear_too_old_interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
    let mut finish_slots: HashMap<Sid, oneshot::Sender<SubmissionWatchStatus>> = Default::default();
    let mut queries: JoinSet<Result<Submission>> = JoinSet::new();

    for _ in session.database.list_assigned_submissions().await? {
        todo!("Launch queries for all outstanding ");
    }

    loop {
        tokio::select! {
            _ = clear_too_old_interval.tick() => {
                session.database.clear_abandoned().await?;
                finish_slots.retain(|_, slot|!slot.is_closed());
            }

            _ = session.flags_changes.notified() => {
                if !session.flags.running.load(Ordering::Acquire) {
                    return Ok(())
                }
            }

            message = queue.recv() => {
                let message = match message {
                    Some(message) => message,
                    None => return Ok(())
                };

                match message {
                    // A submission has been posted to a dispatcher. start watching
                    // for it to complete
                    InternalMessage::SubmissionStart(sid, dispatcher, retain) => {
                        queries.spawn(wait_for_submission(session.clone(), sid, dispatcher, retain));
                    },

                    // Someone is asking to be notified when a submission finishes
                    InternalMessage::InstallSubmissionWatch(sid, resp) => {
                        // first check if we have a database entry for this submission
                        match session.database.get_status(&sid).await? {
                            // we do and it is being processed, drop through to the check for
                            // finish_slots below
                            Some(Status::Assigned(..)) => {},
                            // We do and the submission is done. Send them the submission
                            // and clear out our database.
                            Some(Status::Finished(..)) => {
                                if let Some(submission) = session.database.get_submission(&sid).await? {
                                    _ = resp.send(SubmissionWatchStatus::Submission(submission));
                                }
                                session.database.delete(sid).await?;
                                continue
                            }
                            // We have no record of that submission. Its already been processed.
                            // they'll find the result when they check the database.
                            None => {
                                _ = resp.send(SubmissionWatchStatus::NotFound);
                                continue
                            },
                        }

                        // We are currently processing that submission.
                        match finish_slots.entry(sid) {
                            // Someone is already waiting for a response to this submission.
                            // Refresh the waiter if needed.
                            Entry::Occupied(mut entry) => {
                                if entry.get().is_closed() {
                                    entry.insert(resp);
                                } else {
                                    resp.send(SubmissionWatchStatus::SlotOccupied);
                                }
                            },
                            // Empty entry, put the request in this slot
                            Entry::Vacant(entry) => {
                                entry.insert(resp);
                            },
                        }
                    },
                }
            }

            finished = queries.join_next(), if !queries.is_empty() => {
                let submission = match finished {
                    Some(Ok(Ok(submission))) => submission,
                    Some(Ok(Err(err))) => { error!("Error finishing submission: {err}"); continue }
                    Some(Err(err)) => { error!("Task error finishing submission: {err}"); continue }
                    None => continue,
                };

                let sid = submission.sid.clone();
                match finish_slots.entry(sid) {
                    Entry::Occupied(entry) => {
                        match entry.remove().send(SubmissionWatchStatus::Submission(submission)) {
                            Ok(_) => { session.database.delete(sid).await?; },
                            Err(SubmissionWatchStatus::Submission(sub)) => {
                                session.database.finish_submission(Arc::new(sub)).await?;
                            },
                            Err(_) => {}
                        }
                    },
                    Entry::Vacant(_) => {
                        session.database.finish_submission(Arc::new(submission)).await?;
                    },
                }
            }
        }
    }
}

async fn wait_for_submission(session: Arc<BrokerSession>, sid: Sid, dispatcher: SocketAddr, retain: bool) -> Result<Submission> {
    use crate::dispatcher::interface::GetSubmissionResponse;

    loop {
        // ask the dispatcher for the submission
        let err = match session.http_client.get(format!("https://{dispatcher}/submission/{sid}")).send().await {
            // got a response
            Ok(response) => {
                match response.json::<GetSubmissionResponse>().await {
                    Ok(resp) => return Ok(resp.submission),
                    Err(err) => err,
                }
            },
            Err(err) => err,
        };

        // If the dispatcher doesn't know about the submission send it
        if err.status() == Some(reqwest::StatusCode::NOT_FOUND) {
            match session.database.get_submission(&sid).await? {
                Some(submission) => {
                    session.send_submission(&submission, dispatcher).await?;
                    continue;
                }
                None => return Err(Error::)
            }
        }

        // connection error, check if the dispatcher is still around
        if session.is_dispatcher_available(dispatcher).await {
            error!("Transient error contacting dispatcher: {err}");
            continue
        }

        // Assign to a new dispatcher
        dispatcher = session.select_dispatcher().await;
        match session.database.get_submission(&sid).await? {
            Some(submission) => {
                session.database.assign_submission(Arc::new(submission), dispatcher, retain).await?;
                session.send_submission(&submission, dispatcher).await?;
                continue;
            }
            None => return Err(Error::)
        }
    }
}


async fn monitor_dispatchers(session: Arc<BrokerSession>) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_monitor_dispatchers(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_dispatchers {err}"); },
            Err(err) => { error!("Task error on monitor_dispatchers {err}"); },
        }
    }
}

async fn _monitor_dispatchers(session: Arc<BrokerSession>) -> Result<()> {
    loop {
        // launch dispatcher monitoring
        let mut data = session.dispatcher_data.write().await;
        for peer in  tokio::net::lookup_host("dispatcher").await? {
            if let std::collections::hash_map::Entry::Vacant(entry) = data.entry(peer) {
                entry.insert(LoadInfo::stale());
                tokio::spawn(monitor_dispatcher_instance(session.clone(), peer));
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await
    }
}

async fn monitor_dispatcher_instance(session: Arc<BrokerSession>, host: SocketAddr) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_monitor_dispatcher_instance(session.clone(), host.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_dispatcher_instance {err}"); },
            Err(err) => { error!("Task error on monitor_dispatcher_instance {err}"); },
        }
    }

    // clear out this dispatcher's entry
    session.dispatcher_data.write().await.remove(&host);
}

async fn _monitor_dispatcher_instance(session: Arc<BrokerSession>, host: SocketAddr) -> Result<()> {
    // connect to dispatcher instance
    let url = format!("wss://{host}/stats/");

    todo!("handle reconnect and stale data");

    let (mut client, _) = tokio_tungstenite::connect_async(&url).await?;

    while let Some(message) = client.next().await {
        if let Ok(message) = message {
            if let tokio_tungstenite::tungstenite::Message::Text(message) = message {
                let message: DispatchStatusMessage = match serde_json::from_str(&message) {
                    Ok(message) => message,
                    Err(err) => {
                        error!("Monitor ws error: {err}");
                        continue
                    },
                };

                match message {
                    DispatchStatusMessage::LoadInfo(load) => {
                        session.dispatcher_data.write().await.insert(host, load);
                    },
                }
            }
        }
    }
    return Ok(())
}
