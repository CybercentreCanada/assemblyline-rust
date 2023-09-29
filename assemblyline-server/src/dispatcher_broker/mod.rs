
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

use assemblyline_models::config::Config;
use assemblyline_models::datastore::Submission;
use assemblyline_models::datastore::submission::SubmissionState;
use futures::StreamExt;
use log::error;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast;

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

#[derive(Clone)]
pub enum PeerSubscribeMessage {
    Finished(Arc<Submission>),
    Lagged,
}

struct BrokerSession {
    config: Config,
    instance: usize,
    instance_count: usize,
    flags: BrokerFlags,
    flags_changes: tokio::sync::Notify,
    // elastic: Elastic,
    dispatcher_data: tokio::sync::RwLock<HashMap<SocketAddr, LoadInfo>>,
    database: Database,
    http_client: reqwest::Client,
    finished_submissions: broadcast::Receiver<PeerSubscribeMessage>,
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

    pub async fn select_dispatcher(&self) -> SocketAddr {
        todo!()
    }

    pub async fn assign_submission(&self, submission: Submission, dispatcher: Option<SocketAddr>) -> Result<()> {
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
            self.database.assign_submission(submission, dispatcher).await?;

            // notify the dispatcher
            todo!();
            // self.http_client.post();
        } else {
            self.database.finish_submission(submission).await;
        }
        Ok(())
    }
}

pub async fn launch(config: Config, instance: usize, instance_count: usize) -> Result<()> {
    // setup queues
    let (submission_sender, finished_submissions) = tokio::sync::broadcast::channel(2000);

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
        // elastic,
        database,
        instance,
        instance_count,
        http_client,
        finished_submissions,
        dispatcher_data: tokio::sync::RwLock::new(Default::default()),
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
    tokio::spawn(move_misplaced_submissions(session.clone()));

    // Watch for dispatchers
    tokio::spawn(monitor_dispatchers(session.clone(), submission_sender));
   
    // launch monitor worker
    tokio::spawn(monitor_finished_jobs(session));

    // wait for the api
    api.await.expect("API task failed")
}

async fn move_misplaced_submissions(session: Arc<BrokerSession>) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_move_misplaced_submissions(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in move_misplaced_submissions {err}"); },
            Err(err) => { error!("Task error on move_misplaced_submissions {err}"); },
        }
    }
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

            let assignment = match status {
                database::Status::Assigned(dispatcher, _) => Some(dispatcher),
                database::Status::Finished(_) => None,
            };

            // send it to the right broker
            let handled = session.http_client.post(url)
                .json(&StartSubmissionRequest { submission, assignment } )
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

async fn monitor_finished_jobs(session: Arc<BrokerSession>) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_monitor_finished_jobs(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_finished_jobs {err}"); },
            Err(err) => { error!("Task error on monitor_finished_jobs {err}"); },
        }
    }
}

async fn _monitor_finished_jobs(session: Arc<BrokerSession>) -> Result<()> {
    let mut stream = session.finished_submissions.resubscribe();
    let mut clear_too_old_interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
    tokio::spawn(refresh_submission_status(session.clone()));
    loop {
        tokio::select! {
            _ = clear_too_old_interval.tick() => {
                session.database.clear_abandoned().await?;
            }

            message = stream.recv() => {
                let message = match message {
                    Ok(message) => message,
                    Err(err) => match err {
                        broadcast::error::RecvError::Closed => return Ok(()),
                        broadcast::error::RecvError::Lagged(_) => PeerSubscribeMessage::Lagged,
                    },
                };

                match message {
                    PeerSubscribeMessage::Finished(submission) => {
                        // update finished jobs in local database
                        match session.database.get_assignment(&submission.sid).await {
                            Ok(Some(host)) => {
                                let url = format!("https://{host}/submission/{}", submission.sid);
                                session.http_client.delete(url).send().await?;
                                session.database.finish_submission(submission).await?;
                            }
                            Ok(None) => continue,
                            Err(err) => {
                                error!("Could not check submission assignment: {err}");
                            }
                        }
                        
                    }
                    PeerSubscribeMessage::Lagged => {
                        // kick off refresh
                        tokio::spawn(refresh_submission_status(session.clone()));
                    }
                }
            }
        }
    }
}

async fn monitor_dispatchers(session: Arc<BrokerSession>, submission_sink: broadcast::Sender<PeerSubscribeMessage>) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_monitor_dispatchers(session.clone(), submission_sink.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_dispatchers {err}"); },
            Err(err) => { error!("Task error on monitor_dispatchers {err}"); },
        }
    }
}

async fn _monitor_dispatchers(session: Arc<BrokerSession>, submission_sink: broadcast::Sender<PeerSubscribeMessage>) -> Result<()> {
    loop {
        // launch dispatcher monitoring
        let mut data = session.dispatcher_data.write().await;
        for peer in  tokio::net::lookup_host("dispatcher").await? {
            if let std::collections::hash_map::Entry::Vacant(entry) = data.entry(peer) {
                entry.insert(LoadInfo::stale());
                tokio::spawn(monitor_dispatcher_instance(session.clone(), peer, submission_sink.clone()));
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await
    }
}

async fn monitor_dispatcher_instance(session: Arc<BrokerSession>, host: SocketAddr, submission_sink: broadcast::Sender<PeerSubscribeMessage>) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_monitor_dispatcher_instance(session.clone(), host.clone(), submission_sink.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_dispatcher_instance {err}"); },
            Err(err) => { error!("Task error on monitor_dispatcher_instance {err}"); },
        }
    }

    // clear out this dispatcher's entry
    session.dispatcher_data.write().await.remove(&host);
}

async fn _monitor_dispatcher_instance(session: Arc<BrokerSession>, host: SocketAddr, submission_sink: broadcast::Sender<PeerSubscribeMessage>) -> Result<()> {
    // connect to dispatcher instance
    let url = format!("wss://{host}/updates/");

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
                    DispatchStatusMessage::Finished(submission) => { 
                        let _ = submission_sink.send(PeerSubscribeMessage::Finished(submission)); 
                    },
                }
            }
        }
    }
    return Ok(())
}

async fn refresh_submission_status(session: Arc<BrokerSession>) {
    while session.flags.running.load(Ordering::Acquire) {
        match tokio::spawn(_refresh_submission_status(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in refresh_submission_status {err}"); },
            Err(err) => { error!("Task error on refresh_submission_status {err}"); },
        }
    }
}

async fn _refresh_submission_status(session: Arc<BrokerSession>) -> Result<()> {
    // check every submission stored locally to see if they have been completed remotely
    for (sid, dispatcher) in session.database.list_assigned_submissions().await? {
        // get the submission
        let url = format!("https://{dispatcher}/submission/{sid}");
        let response = match session.http_client.get(&url).send().await {
            Ok(resp) => resp,
            Err(err) => {
                error!("Missing info from dispatcher: {err}");
                continue
            },
        };

        let data = match response.bytes().await {
            Ok(data) => data,
            Err(err) => {
                error!("Corrupt info from dispatcher: {err}");
                continue
            },
        };

        let submission: Arc<Submission> = match serde_json::from_slice(&data) {
            Ok(resp) => Arc::new(resp),
            Err(err) => {
                error!("Corrupt info from dispatcher: {err}");
                continue
            },
        };

        if let SubmissionState::Completed = submission.state {
            session.http_client.delete(url).send().await?;
            session.database.finish_submission(submission).await?;
        }
    }
    Ok(())
}
