pub mod interface;
mod submission;
mod file;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use assemblyline_models::datastore::Submission;
use assemblyline_models::{Sha256, Sid};
use assemblyline_models::config::Config;
use chrono::{DateTime, Utc};
use log::error;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{oneshot, RwLock, mpsc, Mutex};
use tokio::time::MissedTickBehavior;

use crate::elastic::Elastic;
use crate::error::Result;

use self::interface::{start_interface, TaskResult, TaskError};
pub use self::interface::{DispatchStatusMessage, LoadInfo};

struct Flags {
    pub running: bool,
}

#[derive(Hash, PartialEq, Eq)]
struct TaskSignature {
    pub sid: Sid,
    pub service: String,
    pub hash: Sha256,
}

enum StartMessage {
    ExpectStart(TaskSignature, oneshot::Sender<String>),
    Start{
        task: TaskSignature,
        worker: String,
        response: oneshot::Sender<bool>,
    }
}

enum ResultResponse {
    Result(TaskResult),
    Error(TaskError),
}

enum ResultMessage {
    ExpectResponse(TaskSignature, String, oneshot::Sender<ResultResponse>),
    Response(TaskSignature, String, ResultResponse)
}


struct Session {
    config: Config,
    flags: RwLock<Flags>,
    flags_changes: tokio::sync::Notify,
    elastic: Elastic,
    active: RwLock<HashMap<Sid, tokio::task::JoinHandle<()>>>,
    finished: RwLock<HashMap<Sid, Submission>>,
    start_messages: mpsc::Sender<StartMessage>,
    result_messages: mpsc::Sender<ResultMessage>,
}

pub async fn launch(config: Config, elastic: Elastic) -> Result<()> {
    // create channels
    let (start_messages, start_message_recv) = mpsc::channel(1024);
    let (result_messages, result_message_recv) = mpsc::channel(1024);

    // create session
    let session = Arc::new(Session {
        config,
        flags: RwLock::new(Flags { running: true }),
        flags_changes: tokio::sync::Notify::new(),
        elastic,
        active: RwLock::new(Default::default()),
        finished: RwLock::new(Default::default()),
        start_messages,
        result_messages,
    });

    // setup exit conditions
    let mut term_signal = signal(SignalKind::terminate())?;
    let term_session = session.clone();
    tokio::spawn(async move {
        term_signal.recv().await;
        term_session.flags.write().await.running = false;
        term_session.flags_changes.notify_waiters();
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

async fn start_message_broker(session: Arc<Session>, queue: mpsc::Receiver<StartMessage>) {
    let queue = Arc::new(Mutex::new(queue));
    loop {
        match tokio::spawn(_start_message_broker(session.clone(), queue.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in start_message_broker {err}"); },
            Err(err) => { error!("Task error on start_message_broker {err}"); },
        }
    }
}

async fn _start_message_broker(session: Arc<Session>, queue: Arc<Mutex<mpsc::Receiver<StartMessage>>>) -> Result<()> {
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

async fn result_message_broker(session: Arc<Session>, queue: mpsc::Receiver<ResultMessage>) {
    let queue = Arc::new(Mutex::new(queue));
    loop {
        match tokio::spawn(_result_message_broker(session.clone(), queue.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in result_message_broker {err}"); },
            Err(err) => { error!("Task error on result_message_broker {err}"); },
        }
    }
}

async fn _result_message_broker(session: Arc<Session>, queue: Arc<Mutex<mpsc::Receiver<ResultMessage>>>) -> Result<()> {
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