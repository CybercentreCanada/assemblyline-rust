//! Interface for controlling the dispatch broker
//! 
//! POST /submission/
//! GET /submission/<sid>
//! POST /relocate/
//! GET /health/
//!
//! workers:
//!  - move invalid
//!  - monitor for finished jobs
//!  

mod interface;

use std::sync::Arc;

use log::error;
use poem::{post, get, EndpointExt};

use crate::logging::LoggerMiddleware;

use self::interface::start_interface;


struct BrokerConfig {
    pub replicas: usize,
}

struct BrokerFlags {
    pub running: bool,
}

struct Elastic {

}

struct Database {

}

impl Database {
    pub async fn have_submission(&self, sid: &str) -> Result<bool, Error> {
        todo!()
    }
}

struct BrokerSession {
    config: BrokerConfig,
    instance: usize,
    flags: tokio::sync::RwLock<BrokerFlags>,
    elastic: Elastic,
    database: Database,
}

#[derive(Debug)]
enum Error {

}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {

}

impl From<openssl::error::ErrorStack> for Error {
    fn from(value: openssl::error::ErrorStack) -> Self {
        todo!()
    }
}


pub async fn launch(config: BrokerConfig, elastic: Elastic) -> Result<(), Error> {
    // connect to database
    let database = Database::open(config);

    // create session
    let session = Arc::new(BrokerSession {
        config,
        flags: tokio::sync::RwLock::new(BrokerFlags { running: true }),
        elastic,
        database,
        instance: todo!(),
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
    loop { 
        match tokio::spawn(_move_misplaced_submissions(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in move_misplaced_submissions {err}"); },
            Err(err) => { error!("Task error on move_misplaced_submissions {err}"); },
        }
    }
}

async fn _move_misplaced_submissions(session: Arc<BrokerSession>) -> Result<(), Error> {
    todo!()
}

async fn monitor_finished_jobs(session: Arc<BrokerSession>) {
    loop { 
        match tokio::spawn(_monitor_finished_jobs(session.clone())).await {
            Ok(Ok(())) => return,
            Ok(Err(err)) => { error!("Error in monitor_finished_jobs {err}"); },
            Err(err) => { error!("Task error on monitor_finished_jobs {err}"); },
        }
    }
}

async fn _monitor_finished_jobs(session: Arc<BrokerSession>) -> Result<(), Error> {
    todo!()
}