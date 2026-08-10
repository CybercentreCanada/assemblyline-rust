use std::sync::Arc;

use log::{debug, error, info, warn};
use parking_lot::Mutex;
use signal_hook::iterator::Signals;

use crate::{
    constants::{
        DEFAULT_API_HOST, DEFAULT_CONTAINER_ID, DEFAULT_ROOT_CA_PATH, DEFAULT_RUNTIME_PREFIX,
        DEFAULT_SERVICE_API_KEY,
    },
    service_client::ServiceClient,
    service_launcher::DefaultServiceLauncher,
    task_fetcher::single_thread_task_fetcher::SingleThreadTaskFetcher,
    task_uploader::task_uploader::TaskUploader,
};

pub mod connection;
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
    // TODO: Revisit this block. Do we need to have a more dynamic logger?
    let _ = env_logger::builder()
        .target(env_logger::Target::Stdout)
        .try_init();

    // Service handler is a messenger between the service process and the service API, it doesn't need to validate classification
    assemblyline_models::disable_global_classification();

    // load environment variables to configure service client.
    let mut default_tmp_folder = std::env::temp_dir()
        .as_os_str()
        .to_str()
        .unwrap_or("/tmp")
        .to_string();
    default_tmp_folder.push('/');

    let runtime_prefix =
        std::env::var("RUNTIME_PREFIX").unwrap_or(DEFAULT_RUNTIME_PREFIX.to_owned());
    let tasking_dir = std::env::var("TASKING_DIR").unwrap_or(default_tmp_folder.clone());

    let manifest_folder = std::env::var("MANIFEST_FOLDER").unwrap_or("".to_owned());

    let server_host_string: String =
        std::env::var("SERVICE_API_HOST").unwrap_or(DEFAULT_API_HOST.to_string());
    let service_api_key =
        std::env::var("SERVICE_API_KEY").unwrap_or(DEFAULT_SERVICE_API_KEY.to_owned());
    let container_id = std::env::var("HOSTNAME").unwrap_or(DEFAULT_CONTAINER_ID.to_owned());
    let root_ca_path =
        std::env::var("SERVICE_SERVER_ROOT_CA_PATH").unwrap_or(DEFAULT_ROOT_CA_PATH.to_owned());

    let service_dir = std::env::var("SERVICE_DIR").map_or(None, |dir| Some(dir));

    let task_complete_limit = std::env::var("AL_SERVICE_TASK_LIMIT")
        .map_or(None, |val| val.parse::<i32>().map_or(None, |v| Some(v)));

    let sc_running = Arc::new(Mutex::new(true));

    let mut sc = ServiceClient::new(
        false,
        true,
        sc_running.clone(),
        container_id,
        runtime_prefix,
        default_tmp_folder,
        tasking_dir,
        manifest_folder,
        server_host_string,
        service_api_key,
        root_ca_path,
        task_complete_limit,
    )
    .await
    .unwrap();

    // setup different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = DefaultServiceLauncher {
        service_dir: service_dir,
    };

    // handler for termination signals
    let signals = Signals::new(&[signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM]);

    info!("Set up signal handlers.");
    let signal_handler = match signals {
        Ok(mut sig) => {
            let run = sc_running.clone();
            let handler = Arc::new(sig.handle());

            let signal_thread_handler = handler.clone();
            tokio::spawn(async move {
                while !signal_thread_handler.is_closed() {
                    match sig.pending().next() {
                        Some(e) => {
                            if (e == signal_hook::consts::SIGTERM)
                                || (e == signal_hook::consts::SIGINT)
                            {
                                *run.lock() = false;
                                break;
                            } else {
                                warn!("Unknown signal caught. Code ({})", e);
                            }
                        }
                        None => {}
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
        }
    }

    // Make sure to set service client to stop running.
    *sc_running.lock() = false;

    if let Some(handle) = signal_handler {
        debug!("Terminate signal handler loop.");
        handle.close();
    }

    info!("Service client ended.");
}
