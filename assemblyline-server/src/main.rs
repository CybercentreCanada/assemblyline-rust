//! Entrypoint to start any assemblyline server modules

#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(
    // allow these as they sometimes improve clarity
    clippy::needless_return,
    clippy::collapsible_else_if
)]
#![allow(
    // remove after development, allow now so more important warnings can be seen
    dead_code 
)]

use std::{path::PathBuf, process::ExitCode, sync::Arc};

use anyhow::Result;
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_markings::config::ClassificationConfig;
use assemblyline_filestore::FileStore;
use assemblyline_models::config::Config;
use cachestore::CacheStore;
use clap::{Parser, Subcommand};
use elastic::Elastic;
use identify::Identify;
use redis_objects::RedisObjects;
use log::{error, info};
use services::ServiceHelper;

use crate::logging::configure_logging;

mod ingester;
mod submit;
mod http;
mod core_dispatcher;
mod core_metrics;
mod core_util;
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
mod identify;
mod cachestore;
mod string_utils;
mod plumber;
mod service_api;
mod common;

#[cfg(test)]
mod tests;


#[derive(Debug, Parser)]
#[command(name="assemblyline")]
#[command(bin_name="assemblyline")]
struct Args {
    /// Path to the assemblyline configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
    Ingester {
        
    },
    Dispatcher {

    },
    Plumber {

    }
}


#[tokio::main]
async fn main() -> ExitCode {
    // Load CLI
    let args = Args::parse();

    // Load configuration
    let (config, config_path) = load_configuration(args.config).await.expect("Could not load configuration");

    // configure logging, the object returned here owns the log processing internals
    // and needs to be held until the program ends
    let _log_manager = configure_logging(&config).expect("Could not configure logging");
    info!("Configuration loaded from: {config_path:?}");

    // Connect to all the supporting components
    let core = match Core::setup(config, "").await {
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
        Commands::Dispatcher {  } => {
            crate::dispatcher::main(core).await
        }
        Commands::Plumber {  } => {
            crate::plumber::main(core).await
        }
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


async fn load_configuration(path: Option<PathBuf>) -> Result<(Arc<Config>, PathBuf)> {
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
    let body = tokio::fs::read_to_string(&path).await?;
    let body = environment_template::apply_env(&body)?;

    // parse the configuration
    Ok((Arc::new(serde_yaml::from_str(&body)?), path))
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
    pub filestore: Arc<FileStore>,
    pub identify: Arc<Identify>,

    // interface to request service information
    pub services: ServiceHelper,
}

impl Core {
    /// Initialize connections to resources that everything uses
    pub async fn setup(config: Arc<Config>, elastic_prefix: &str) -> Result<Self> {
        // connect to redis one
        let redis_persistant = RedisObjects::open_host(&config.core.redis.persistent.host, config.core.redis.persistent.port, config.core.redis.persistent.db)?;

        // connect to redis two
        let redis_volatile = RedisObjects::open_host(&config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, config.core.redis.nonpersistent.db)?;

        // connect to redis three
        let redis_metrics = RedisObjects::open_host(&config.core.metrics.redis.host, config.core.metrics.redis.port, config.core.metrics.redis.db)?;

        // connect to elastic
        // TODO Fill in ca parameter
        let datastore = Elastic::connect(&config.datastore.hosts[0], false, None, false, elastic_prefix).await?;

        // connect to filestore
        let filestore = FileStore::open(&config.filestore.storage).await?;

        //
        let file_cache = FileStore::open(&config.filestore.cache).await?;
        let cachestore = CacheStore::new("system".to_owned(), datastore.clone(), file_cache)?;
        let identify = Identify::new(Some(cachestore), redis_volatile.clone()).await?;

        // load classification from given config blob or file
        let mut classification_config = config.classification.config.clone();
        if classification_config.is_none() {
            if let Some(path) = &config.classification.path {
                info!("Loading classification config from: {path:?}");
                classification_config = Some(tokio::fs::read_to_string(path).await?);
            }
        } else {
            info!("Loading classification configuration embedded in assemblyline configuration.");
        }
        let classification_config = match classification_config {
            Some(config) => serde_yaml::from_str(&config)?,
            None => {
                info!("Loading hardcoded default classification configuration.");
                ClassificationConfig::default()
            },
        };
        let classification_parser = Arc::new(ClassificationParser::new(classification_config)?);

        Ok(Core {
            // start a daemon that keeps an up-to-date local cache of service info
            services: ServiceHelper::start(datastore.clone(), &redis_volatile, classification_parser.clone(), &config.services).await?,
            config,
            datastore,
            redis_persistant,
            redis_volatile,
            redis_metrics,
            running: Arc::new(Flag::new(true)),
            enabled: Arc::new(Flag::new(true)),
            filestore,
            classification_parser,
            identify,
        })
    }
    
    #[cfg(test)]
    pub async fn test_custom_setup(callback: impl Fn(&mut Config)) -> (Self, TestGuard) {
        use rand::Rng;
        use std::sync::LazyLock;
        use parking_lot::Mutex;
        let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Debug).try_init();

        static USED_DB: LazyLock<Arc<Mutex<Vec<i64>>>> = LazyLock::new(|| {
            let out = Vec::from_iter(1..16);
            Arc::new(Mutex::new(out))
        });

        let table = USED_DB.clone();
        let db = loop {
            {
                if let Some(value) = table.lock().pop() {
                    break value
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };

        let host = "localhost";
        let port = 6379;
        let redis = RedisObjects::open_host(host, port, db).unwrap();
        redis.wipe().await.unwrap();

        let filestore = tempfile::TempDir::new().unwrap();

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
            },
            "filestore": {
                "storage": vec![format!("file://{}", filestore.path().to_string_lossy())],
                "cache": vec![format!("file://{}", filestore.path().to_string_lossy())],
                "archive": vec![format!("file://{}", filestore.path().to_string_lossy())],
            }
        })).unwrap();
        callback(&mut config);
        config.classification.config = Some(serde_json::to_string(&assemblyline_markings::classification::sample_config()).unwrap());
        let prefix = rand::thread_rng().r#gen::<u128>().to_string();
        let core = Self::setup(Arc::new(config), &prefix).await.unwrap();
        let elastic = core.datastore.clone();
        elastic.apply_test_settings().await.unwrap();
        let guard = TestGuard { used: db, table, elastic, filestore, running: core.running.clone() };
        (core, guard)
    }
    
    /// Produce a set of core resources suitable for testing
    #[cfg(test)]
    pub async fn test_setup() -> (Self, TestGuard) {
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


/// While this struct is held prevent temporary core resources from being collected
#[cfg(test)]
struct TestGuard {
    running: Arc<Flag>,
    used: i64,
    elastic: Arc<Elastic>,
    filestore: tempfile::TempDir,
    table: Arc<parking_lot::Mutex<Vec<i64>>>,
}

#[cfg(test)]
impl Drop for TestGuard {
    fn drop(&mut self) {
        self.running.set(false);

        self.table.lock().push(self.used);

        let elastic = self.elastic.clone();
        tokio::spawn(async move {
            if let Err(err) = elastic.wipe_all().await {
                error!("Could not clear test data: {err}");
            }
        });
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

    pub fn install_terminate_handler(self: &Arc<Self>, value: bool) {
        let flag = self.clone();
        tokio::spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Termination signal called");
                    flag.set(value);
                },
                Err(err) => error!("Error installing signal handler: {err}"),
            }
        });
    }
}

/// A convenience trait that lets you pass true, false, or None for boolean arguments
pub trait IBool: Into<Option<bool>> + Copy + Send {}
impl<T: Into<Option<bool>> + Copy + Send> IBool for T {}

