//! Entrypoint to start any assemblyline server modules

#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return)]
// #![allow(clippy::needless_return, clippy::while_let_on_iterator, clippy::collapsible_else_if)]

use std::sync::atomic::AtomicBool;
use std::{path::PathBuf, process::ExitCode, sync::Arc};

use anyhow::Result;
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::config::Config;
use clap::{Command, Parser, Subcommand};
use elastic::Elastic;
use redis_objects::RedisObjects;
use log::error;
use services::ServiceHelper;
use tokio::sync::Notify;

use crate::logging::configure_logging;

// mod ingester;
mod submit;
mod core_dispatcher;
mod archive;
mod services;
// mod dispatcher;
mod postprocessing;
mod elastic;
mod logging;
mod tls;
mod error;
mod constants;


#[derive(Debug, Parser)]
#[command(name="assemblyline")]
#[command(bin_name="assemblyline")]
struct Args {
    config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
    Ingester {
        
    }
}


#[tokio::main]
async fn main() -> ExitCode {
    // Load CLI
    let args = Args::parse();

    // Load configuration
    let config = load_configuration(args.config).await.expect("Could not load configuration");

    // configure logging, the object returned here owns the log processing internals
    // and needs to be held until the program ends
    let log_manager = configure_logging(&config).expect("Could not configure logging");

    // Connect to all the supporting components
    let core = match Core::setup(config).await {
        Ok(core) => core,
        Err(err) => {
            error!("Startup error: {err}");
            return ExitCode::FAILURE;
        }
    };

    // pick the module to launch
    let result = match args.command {
        Commands::Ingester {  } => {
            todo!();
            anyhow::Ok(())
            // crate::ingester::main(core).await
        },
    };

    // log if the module failed
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            error!("{err}");
            return ExitCode::FAILURE;
        },
    }
}


async fn load_configuration(path: Option<PathBuf>) -> Result<Arc<Config>> {
    // figure out which file path to use
    let path = match path {
        Some(path) => path,
        None => match std::env::var("ASSEMBLYLINE_CONFIG_PATH") {
            Ok(path) => PathBuf::from(path),
            Err(std::env::VarError::NotPresent) => PathBuf::from("/etc/assemblyline/config.yml"),
            Err(err) => return Err(err.into())
        }
    };

    // load environment variables into config
    let body = tokio::fs::read_to_string(path).await?;
    let body = environment_template::apply_env(&body)?;

    // parse the configuration
    Ok(Arc::new(serde_yaml::from_str(&body)?))
}

/// Common components, connections, and utilities that every core daemon is going to end up needing
#[derive(Clone)]
struct Core {
    // flags
    pub running: Arc<Flag>,
    pub enabled: Arc<Flag>,

    // universal configuration and connection objects
    pub config: Arc<Config>,
    pub classification_parser: Arc<ClassificationParser>,
    pub datastore: Arc<Elastic>,
    pub redis_persistant: Arc<RedisObjects>,
    pub redis_volatile: Arc<RedisObjects>,
    pub redis_metrics: Arc<RedisObjects>,

    // interface to request service information
    pub services: ServiceHelper,
}

impl Core {
    /// Initialize connections to resources that everything uses
    pub async fn setup(config: Arc<Config>) -> Result<Self> {
        // connect to redis one
        let redis_persistant = RedisObjects::open_host(&config.core.redis.persistent.host, config.core.redis.persistent.port)?;

        // connect to redis two
        let redis_volatile = RedisObjects::open_host(&config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port)?;

        // connect to redis three
        let redis_metrics = RedisObjects::open_host(&config.core.metrics.redis.host, config.core.metrics.redis.port)?;

        // connect to elastic
        let datastore = Elastic::connect(&config.datastore.hosts[0], false).await?;

        Ok(Core {
            // start a daemon that keeps an up-to-date local cache of service info
            services: ServiceHelper::start(datastore.clone(), &redis_volatile).await?,
            config,
            datastore,
            redis_persistant,
            redis_volatile,
            redis_metrics,
            running: Arc::new(Flag::new(true)),
            enabled: Arc::new(Flag::new(true)),
            classification_parser: todo!(),
        })
    }
    
}

struct Flag {
    condition: tokio::sync::watch::Sender<bool>,
}

impl Flag {
    pub fn new(value: bool) -> Self {
        Flag { 
            condition: tokio::sync::watch::channel(value).0,
        }
    }

    pub fn read(&self) -> bool {
        *self.condition.borrow()
    }

    pub fn set(&self, value: bool) {
        self.condition.send_modify(|current| *current = value);
    }

    pub async fn wait_for(&self, value: bool) {
        let mut watcher = self.condition.subscribe();
        while *watcher.borrow_and_update() != value {
            _ = watcher.changed().await;
        }
    }
}