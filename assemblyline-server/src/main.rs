//! Entrypoint to start any assemblyline server modules

#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return)]
// #![allow(clippy::needless_return, clippy::while_let_on_iterator, clippy::collapsible_else_if)]

use std::{path::PathBuf, process::ExitCode, sync::Arc};

use anyhow::Result;
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_markings::config::ClassificationConfig;
use assemblyline_models::config::Config;
use clap::{Parser, Subcommand};
use elastic::Elastic;
use redis_objects::RedisObjects;
use log::error;
use services::ServiceHelper;

use crate::logging::configure_logging;

mod ingester;
mod submit;
mod core_dispatcher;
mod core_metrics;
mod archive;
mod services;
mod dispatcher;
mod postprocessing;
mod elastic;
mod logging;
mod tls;
mod error;
mod constants;
mod config;


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
    let _log_manager = configure_logging(&config).expect("Could not configure logging");

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
            crate::ingester::main(core).await
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
        let redis_persistant = RedisObjects::open_host(&config.core.redis.persistent.host, config.core.redis.persistent.port, config.core.redis.persistent.db)?;

        // connect to redis two
        let redis_volatile = RedisObjects::open_host(&config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, config.core.redis.nonpersistent.db)?;

        // connect to redis three
        let redis_metrics = RedisObjects::open_host(&config.core.metrics.redis.host, config.core.metrics.redis.port, config.core.metrics.redis.db)?;

        // connect to elastic
        // TODO Fill in ca parameter
        let datastore = Elastic::connect(&config.datastore.hosts[0], false, None, false).await?;

        // load classification from given config blob or file
        let mut classification_config = config.classification.config.clone();
        if classification_config.is_none() {
            if let Some(path) = &config.classification.path {
                classification_config = Some(tokio::fs::read_to_string(path).await?);
            }
        }
        let classification_config = match classification_config {
            Some(config) => serde_yaml::from_str(&config)?,
            None => ClassificationConfig::default(),
        };
        let classification_parser = Arc::new(ClassificationParser::new(classification_config)?);

        Ok(Core {
            // start a daemon that keeps an up-to-date local cache of service info
            services: ServiceHelper::start(datastore.clone(), &redis_volatile, classification_parser.clone(), config.services.clone()).await?,
            config,
            datastore,
            redis_persistant,
            redis_volatile,
            redis_metrics,
            running: Arc::new(Flag::new(true)),
            enabled: Arc::new(Flag::new(true)),
            classification_parser,
        })
    }
    
    #[cfg(test)]
    pub async fn test_custom_setup(callback: impl Fn(&mut Config)) -> (Self, RedisGuard) {
        use std::sync::LazyLock;
        use parking_lot::Mutex;

        static USED_DB: LazyLock<Arc<Mutex<Vec<i64>>>> = LazyLock::new(|| {
            let out = Vec::from_iter(1..16);
            Arc::new(Mutex::new(out))
        });

        let table = USED_DB.clone();
        let db = loop {
            {
                let mut table = table.lock();
                if let Some(value) = table.pop() {
                    break value
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };

        let host = "localhost";
        let port = 6379;
        let redis = RedisObjects::open_host(host, port, db).unwrap();
        redis.wipe().await.unwrap();

        let mut config: Config = serde_json::from_value(serde_json::json!({
            "core": {
                "redis": {
                    "persistent": {
                        "host": host,
                        "port": port,
                        "db": db,
                    },
                    "nonpersistent": {
                        "host": host,
                        "port": port,
                        "db": db,
                    }
                },
                "metrics": {
                    "redis": {
                        "host": host,
                        "port": port,
                        "db": db,
                    }
                }
            }
        })).unwrap();
        callback(&mut config);
        config.classification.config = Some(serde_json::to_string(&assemblyline_markings::classification::sample_config()).unwrap());
        (
            Self::setup(Arc::new(config)).await.unwrap(),
            RedisGuard { used: db, table }
        )
    }
    
    /// Produce a set of core resources suitable for testing
    #[cfg(test)]
    pub async fn test_setup() -> (Self, RedisGuard) {
        Self::test_custom_setup(|_| {}).await
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.running.read()
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.enabled.read()
    }

    // pub async fn while_running(&self, duration: Duration) {
        
    // }

    pub async fn sleep(&self, duration: std::time::Duration) -> bool {
        _ = tokio::time::timeout(duration, self.running.wait_for(false)).await;
        self.running.read()
    }
}

/// While this struct is held prevent 
#[cfg(test)]
struct RedisGuard {
    used: i64,
    table: Arc<parking_lot::Mutex<Vec<i64>>>,
}

#[cfg(test)]
impl Drop for RedisGuard {
    fn drop(&mut self) {
        let mut table = self.table.lock();
        table.push(self.used);
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

    pub fn install_terminate_handler(self: &Arc<Self>) {
        todo!()
    }
}