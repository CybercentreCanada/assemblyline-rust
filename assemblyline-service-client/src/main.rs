//! Assemblyline service client that facilitates the communication between service server and service processes.

#![warn(
    missing_docs,
    non_ascii_idents,
    trivial_numeric_casts,
    unused_crate_dependencies,
    noop_method_call,
    single_use_lifetimes,
    trivial_casts,
    unused_lifetimes,
    nonstandard_style,
    variant_size_differences
)]
#![deny(
    keyword_idents,
    clippy::disallowed_methods
)]
#![allow(
    // allow these as they sometimes improve clarity
    clippy::needless_return,
    clippy::collapsible_else_if
)]

use anyhow::Result;
use assemblyline_models::config::Config;
use assemblyline_utilities::logging::configure_logging;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use signal_hook::iterator::Signals;
use std::{path::PathBuf, sync::Arc};

use itertools as _;
use nom as _;
use poem as _;
use rand as _;
use reqwest_retry as _;
use urlencoding as _;
use bytes as _;

use crate::{
    constants::DEFAULT_ASSEMBLYLINE_CONFIG_PATH,
    service_client::{ServiceClientBuilder},
    service_launcher::DefaultServiceLauncher,
    task_fetcher::single_thread_task_fetcher::SingleThreadTaskFetcher,
    task_uploader::TaskUploader,
};

pub mod constants;
pub mod service_client;
pub mod service_launcher;
pub mod task_fetcher;
pub mod task_uploader;
pub mod types;


#[cfg(test)]
pub(crate) mod tests;

#[tokio::main]
async fn main() {
    // Service handler is a messenger between the service process and the service API, it doesn't need to validate classification
    assemblyline_models::disable_global_classification();

    let mut sc_builder = ServiceClientBuilder::new();

    if let Ok(runtime_prefix) = std::env::var("RUNTIME_PREFIX") {
        sc_builder.set_runtime_prefix(runtime_prefix);
    }

    if let Ok(tasking_dir) = std::env::var("TASKING_DIR") {
        sc_builder.set_tasking_dir(tasking_dir);
    }

    if let Ok(server_host_string) = std::env::var("SERVICE_API_HOST") {
        sc_builder.set_server_host_string(server_host_string);
    }

    if let Ok(service_api_key) = std::env::var("SERVICE_API_KEY") {
        sc_builder.set_service_api_key(service_api_key);
    }

    if let Ok(container_id) = std::env::var("HOSTNAME") {
        sc_builder.set_container_id(container_id);
    }

    if let Ok(root_ca_path) = std::env::var("SERVICE_SERVER_ROOT_CA_PATH") {
        sc_builder.set_root_ca_path(root_ca_path);
    }

    if let Ok(manifest_dir) = std::env::var("MANIFEST_DIR") {
        sc_builder.set_manifest_dir(manifest_dir);
    }


    if let Ok(Ok(task_complete_limit)) = std::env::var("AL_SERVICE_TASK_LIMIT").map(|val| {
        val.parse::<i64>()
    }) {
        sc_builder.set_task_complete_limit(task_complete_limit);
    }

    if std::env::var("CONTAINER_MODE").is_ok_and(|m| matches!(m.to_lowercase().as_str(), "true")) {
        sc_builder.set_container_mode(true);
    }

    if std::env::var("REGISTER_ONLY").is_ok_and(|m| matches!(m.to_lowercase().as_str(), "true")) {
        sc_builder.set_register_only(true);
    }

    let service_dir = std::env::var("SERVICE_DIR").ok();

    let config_path = std::env::var("ASSEMBLYLINE_CONFIG_PATH")
        .unwrap_or(DEFAULT_ASSEMBLYLINE_CONFIG_PATH.to_owned());

    let config = load_configuration(config_path)
        .await
        .expect("Cannot load configuration.");
    let _log_manager = configure_logging(&config).expect("Cannot configure logging.");

    let sc_running = Arc::new(Mutex::new(true));

    let mut sc = sc_builder
        .build_service_client(sc_running.clone())
        .await
        .expect("Cannot initialize service client.");

    // setup different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = DefaultServiceLauncher { service_dir };

    // handler for termination signals
    let signals = Signals::new([signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM]);

    info!("Set up signal handlers.");
    let signal_handler = match signals {
        Ok(mut sig) => {
            let run = sc_running.clone();
            let handler = Arc::new(sig.handle());

            let signal_thread_handler = handler.clone();
            tokio::spawn(async move {
                while !signal_thread_handler.is_closed() {
                    if let Some(e) = sig.pending().next() {
                        if (e == signal_hook::consts::SIGTERM)
                            || (e == signal_hook::consts::SIGINT)
                            || (e == signal_hook::consts::SIGUSR1)
                        {
                            info!("Termination signal caught: Code {e}.");
                            *run.lock() = false;
                            break;
                        } else {
                            warn!("Unknown signal caught. Code ({})", e);
                        }
                    }

                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(2.0)).await;
                }
            });

            Some(handler)
        }
        Err(err) => {
            error!("Error setting up signal handling loop: {:?}", err);
            None
        }
    };

    // run service in a loop
    while *sc_running.lock() {
        info!("Start running service client...");
        let res = sc
            .run_service(&mut task_fetcher, &task_uploader, &service_launcher)
            .await;

        if let Err(err) = res {
            error!("Error running service client: {}", err);
        };

        if sc.container_mode {
            info!("We are in container mode. Stop running service.");
            break;
        } else {
            info!("Restart service client.");
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(2.0)).await;
        }
    }

    // Make sure to set service client to stop running.
    sc.stop_service_client();

    if let Some(handle) = signal_handler {
        debug!("Terminate signal handler loop.");
        handle.close();
    }

    info!("Service client ended.");
}

async fn load_configuration(config_path: String) -> Result<Arc<Config>> {
    let path = PathBuf::from(config_path);

    let data = if path.exists() {
        let body = tokio::fs::read_to_string(&path).await?;
        let body = environment_template::apply_env(&body)?;
        serde_yaml::from_str(&body)?
    } else {
        Config::default()
    };

    // parse the configuration
    Ok(Arc::new(data))
}
