mod interface;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use assemblyline_models::config::Config;
use chrono::{DateTime, Utc};
use log::error;
use tokio::signal::unix::{signal, SignalKind};

use crate::elastic::Elastic;
use crate::error::Result;

use self::interface::start_interface;
pub use self::interface::{DispatchStatusMessage, LoadInfo};

struct Flags {
    pub running: bool,
}


struct Session {
    config: Config,
    flags: tokio::sync::RwLock<Flags>,
    flags_changes: tokio::sync::Notify,
    elastic: Elastic,
    active: tokio::sync::RwLock<HashMap<String, tokio::task::JoinHandle<()>>>,
    finished: tokio::sync::RwLock<BTreeSet<(DateTime<Utc>, String)>>,
}

pub async fn launch(config: Config, elastic: Elastic) -> Result<()> {
    // create session
    let session = Arc::new(Session {
        config,
        flags: tokio::sync::RwLock::new(Flags { running: true }),
        flags_changes: tokio::sync::Notify::new(),
        elastic,
        active: tokio::sync::RwLock::new(Default::default()),
        finished: tokio::sync::RwLock::new(Default::default()),
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
    tokio::spawn(start_message_broker(session.clone()));

    // launch worker that handles message broker
    tokio::spawn(result_message_broker(session));

    // wait for the api
    api.await.expect("API task failed")
}

async fn start_message_broker(session: Arc<Session>) {
    loop {
        match tokio::spawn(_start_message_broker(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in start_message_broker {err}"); },
            Err(err) => { error!("Task error on start_message_broker {err}"); },
        }
    }
}

async fn _start_message_broker(session: Arc<Session>) -> Result<()> {
    todo!()
}

async fn result_message_broker(session: Arc<Session>) {
    loop {
        match tokio::spawn(_result_message_broker(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in result_message_broker {err}"); },
            Err(err) => { error!("Task error on result_message_broker {err}"); },
        }
    }
}

async fn _result_message_broker(session: Arc<Session>) -> Result<()> {
    todo!()
}