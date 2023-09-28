
//!
//! workers:
//!  - move invalid
//!  - monitor for finished jobs
//!

mod interface;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use assemblyline_models::config::Config;
use assemblyline_models::datastore::Submission;
use log::error;
use tokio::signal::unix::{signal, SignalKind};

use self::interface::{start_interface, StartSubmissionRequest};
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

struct Elastic {

}

struct Database {

}

impl Database {
    pub async fn have_submission(&self, sid: &str) -> Result<bool> {
        todo!()
    }

    pub async fn get_submission(&self, sid: &str) -> Result<Option<Submission>> {
        todo!()
    }

    pub async fn assign_submission(&self, submission: &Submission, dispatcher: String) -> Result<()> {
        todo!()
    }
}

struct BrokerSession {
    config: Config,
    instance: usize,
    flags: BrokerFlags,
    flags_changes: tokio::sync::Notify,
    // elastic: Elastic,
    database: Database,
    http_client: reqwest::Client,
}

impl BrokerSession {
    pub fn assign_sid(&self, sid: &str) -> usize {
        todo!()
    }

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

    pub fn select_dispatcher(&self) -> String {
        todo!()
    }

    pub async fn assign_submission(&self, submission: Submission, dispatcher: Option<String>) -> Result<()> {
        // check if the submission is already present
        if self.database.have_submission(&submission.sid).await? {
            return Ok(())
        }

        // assign to a dispatcher
        let dispatcher = match dispatcher {
            None => self.select_dispatcher(),
            Some(dispatcher) => dispatcher
        };

        // save to disk
        self.database.assign_submission(&submission, dispatcher).await?;

        // notify the dispatcher
        todo!();
        // self.http_client.post();
        Ok(())
    }
}

pub async fn launch(config: Config, instance: usize) -> Result<()> {
    // connect to database
    let database = Database::open(config);

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
        http_client,
    });

    // setup exit conditions
    let term_signal = signal(SignalKind::terminate())?;
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
    let submissions = session.database.list_submissions().await?;

    // make sure the local flag receiver is seen
    if !session.flags.running.load(Ordering::Acquire) {
        return Ok(())
    }

    for (sid, dispatcher) in submissions {
        // if this submission belongs here continue
        let target_broker = session.assign_sid(sid);
        if target_broker != session.instance {
            continue
        }

        // if the submission doesn't belong here load its content
        let url = session.peer_url(target_broker, "submission")?;
        if let Some(submission) = session.database.get_submission(sid).await? {

            // send it to the right broker
            let handled = session.http_client.post(url)
                .json(&StartSubmissionRequest { submission, assignment: Some(dispatcher) } )
                .send().await?;

            // if that worked clear the local data
            if handled.status() == reqwest::StatusCode::OK {
                session.database.delete_submission(sid).await?;
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
    todo!();
}