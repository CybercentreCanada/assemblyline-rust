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

use anyhow::{Context, Result};
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_markings::config::{ready_classification, ClassificationConfig};
use assemblyline_filestore::FileStore;
use assemblyline_models::config::Config;
use cachestore::CacheStore;
use clap::{Parser, Subcommand};
use common::flag::Flag;
use elastic::Elastic;
use identify::Identify;
use redis_objects::RedisObjects;
use log::{error, info};
use services::ServiceHelper;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use ort as _;

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

    /// Try to secure outgoing connections
    #[arg(short, long)]
    secure_connections: bool,

    /// Enable APM exporting
    #[arg(short, long)]
    enable_apm: bool,

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

    },
    ServiceAPI {

    }
}

impl Commands {
    pub fn label(&self) -> &str {
        match self {
            Commands::Ingester { .. } => "ingester",
            Commands::Dispatcher { .. } => "dispatcher",
            Commands::Plumber { .. } => "plumber",
            Commands::ServiceAPI { .. } => "service_server",
        }
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
    info!("Configuration loaded from: {}", config_path.to_string_lossy());

    // Configure APM
    if let Some(url) = &config.core.metrics.apm_server.server_url {
        if args.enable_apm {
            let config = tracing_elastic_apm::config::Config::new(url.to_string())
                .allow_invalid_certificates(true);

            let layer = tracing_elastic_apm::new_layer(args.command.label().to_string(), config).expect("Could not initialize APM");

            tracing_subscriber::registry().with(layer).init();
            info!("APM exporter configured and enabled");
        } else {
            info!("APM exporter configured but disabled");
        }
    } else {
        info!("APM collection not configured");
    }

    // Connect to all the supporting components
    let core = match Core::setup(config, "", args.secure_connections).await {
        Ok(core) => core,
        Err(err) => {
            error!("Startup error: {err:?}");
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
        Commands::ServiceAPI { } => {  
            crate::service_api::main(core).await
        }
    };

    // log if the module failed
    match result {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            error!("Module error: {err:?}");
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
    pub async fn setup(config: Arc<Config>, elastic_prefix: &str, secure: bool) -> Result<Self> {
        // connect to redis one
        let redis_persistant = if secure {
            RedisObjects::open_host_native_tls(&config.core.redis.persistent.host, config.core.redis.persistent.port, config.core.redis.persistent.db)?            
        } else {
            RedisObjects::open_host(&config.core.redis.persistent.host, config.core.redis.persistent.port, config.core.redis.persistent.db)?
        };

        // connect to redis two
        let redis_volatile = if secure { 
            RedisObjects::open_host_native_tls(&config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, config.core.redis.nonpersistent.db)?
        } else {
            RedisObjects::open_host(&config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, config.core.redis.nonpersistent.db)?
        };

        // connect to redis three
        let redis_metrics = if secure {
            RedisObjects::open_host_native_tls(&config.core.metrics.redis.host, config.core.metrics.redis.port, config.core.metrics.redis.db)?
        } else {
            RedisObjects::open_host(&config.core.metrics.redis.host, config.core.metrics.redis.port, config.core.metrics.redis.db)?
        };

        // connect to elastic
        let datastore_ca = get_datastore_ca().await?;
        let datastore_ca = datastore_ca.as_ref().map(|val|&val[..]);
        let datastore_verify = get_datastore_verify()?;
        let datastore = Elastic::connect(&config.datastore.hosts[0], false, datastore_ca, !datastore_verify, elastic_prefix).await?;

        // connect to filestore
        let filestore = FileStore::open(&config.filestore.storage).await.context("initializing filestore")?;

        //
        let file_cache = FileStore::open(&config.filestore.cache).await.context("initializing cache filestore")?;
        let cachestore = CacheStore::new("system".to_owned(), datastore.clone(), file_cache).context("initializing cachestore")?;
        let identify = Identify::new_with_cache(cachestore, redis_volatile.clone()).await.context("initializing identify")?;

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
            Some(config) => ready_classification(Some(&config))?,
            None => {
                info!("Loading hardcoded default classification configuration.");
                ClassificationConfig::default()
            },
        };
        let classification_parser = Arc::new(ClassificationParser::new(classification_config)?);
        assemblyline_models::types::classification::set_global_classification(classification_parser.clone());

        info!("Start service helper");
        let services = ServiceHelper::start(datastore.clone(), &redis_volatile, classification_parser.clone(), &config.services).await?;

        Ok(Core {
            // start a daemon that keeps an up-to-date local cache of service info
            services,
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
        let prefix = rand::rng().random::<u128>().to_string();
        let core = Self::setup(Arc::new(config), &prefix, false).await.unwrap();
        let elastic = core.datastore.clone();
        elastic.apply_test_settings().await.unwrap();
        let guard = TestGuard { used: db, table, elastic, filestore, running: core.running.clone(), cleaned: false };
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

const DEFAULT_DATASTORE_ROOT_CA_PATH: &str = "/etc/assemblyline/ssl/al_root-ca.crt";

async fn get_datastore_ca() -> Result<Option<Vec<u8>>> {
    let path = match std::env::var("DATASTORE_ROOT_CA_PATH") {
        Ok(path) => path,
        Err(std::env::VarError::NotPresent) => {
            if tokio::fs::try_exists(&DEFAULT_DATASTORE_ROOT_CA_PATH).await? {
                DEFAULT_DATASTORE_ROOT_CA_PATH.to_string()
            } else {
                return Ok(None)
            }
        },
        Err(err) => return Err(err.into())
    };

    Ok(Some(tokio::fs::read(path).await?))
}

fn get_datastore_verify() -> Result<bool> {
    match std::env::var("DATASTORE_VERIFY_CERTS") {
        Ok(value) => Ok(value.to_lowercase() == "true"),
        Err(std::env::VarError::NotPresent) => Ok(true),
        Err(err) => Err(err.into())
    }
}


// async fn get_redis_cert(kind: &str) -> Result<Option<Vec<u8>>> {
//     let path = match std::env::var(format!("REDIS_{name}_CERT_PATH")) {
//         Ok(path) => path,
//         Err(std::env::VarError::NotPresent) => return Ok(None),
//         Err(err) => return Err(err.into())
//     };

//     Ok(Some(tokio::fs::read(path).await?))
// }


/// While this struct is held prevent temporary core resources from being collected
#[cfg(test)]
struct TestGuard {
    running: Arc<Flag>,
    used: i64,
    elastic: Arc<Elastic>,
    filestore: tempfile::TempDir,
    table: Arc<parking_lot::Mutex<Vec<i64>>>,
    cleaned: bool
}

// #[cfg(test)]
// impl TestGuard {
//     pub async fn cleanup(mut self) -> Result<()> {
//         self.table.lock().push(self.used);
//         self.elastic.wipe_all().await?;
//         self.cleaned = true;
//         Ok(())
//     }
// }

#[cfg(test)]
impl Drop for TestGuard {
    fn drop(&mut self) {
        if !self.cleaned {
            self.running.set(false);
            // self.table.lock().push(self.used);
            let table = self.table.clone();
            let used = self.used;

            let elastic = self.elastic.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                runtime.block_on(async move {
                    if let Err(err) = elastic.wipe_all().await {
                        error!("Could not clear test data: {err}");
                    }
                });

                table.lock().push(used);
            });
        }
    }
}

/// A convenience trait that lets you pass true, false, or None for boolean arguments
pub trait IBool: Into<Option<bool>> + Copy + Send {}
impl<T: Into<Option<bool>> + Copy + Send> IBool for T {}

