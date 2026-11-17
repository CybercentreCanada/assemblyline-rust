//! The main service client class that facilitates communication between service server and service process

use std::{
    collections::HashMap,
    ffi::CString,
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Result};
use assemblyline_models::{
    messages::{
        service_api::{
            self,
            service_manifest::{Heuristic, ServiceManifest},
        },
        task::Task,
    },
    types::{strings::ServiceVersion, JsonMap},
};
use assemblyline_utilities::{
    connection::{
        self, convert_api_output_obj, Connection, ConnectionBuilder, ServerType, TLSSettings,
    },
    types::response::RegisterResponse,
};
use libc::mkfifo;
use log::{debug, error as log_error, info, warn};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::tempdir_in;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, Interest},
    net::unix::pipe::{self, Receiver, Sender},
    process::Child,
};

use crate::{
    constants::{
        get_version, DEFAULT_API_HOST, DEFAULT_CONTAINER_ID, DEFAULT_ROOT_CA_PATH,
        DEFAULT_RUNTIME_PREFIX, DEFAULT_SERVICE_API_KEY, DONE_FIFO_NAME,
        EXCEPTION_SERVICE_ERROR_TYPE, PLACEHOLDER_VERSION_TAG, TASK_DONE_ERROR, TASK_DONE_SUCCESS,
        TASK_FIFO_NAME, UNRECOVERABLE_ERROR_STATUS,
    },
    service_launcher::ServiceLauncher,
    task_fetcher::TaskFetcher,
    task_uploader::{TaskErrorData, TaskUploader},
    types::{errors::ServiceClientError, ServiceInfo},
};

/// Service client class that facilitates communication between service server and service process
pub struct ServiceClient {
    register_only: bool,
    /// is this service client running in container mode?
    pub container_mode: bool,
    runtime_prefix: String,
    container_id: String,
    file_required: bool,

    /// Service information
    pub service_info: ServiceInfo,
    service_timeout: i32,

    manifest_file_path: String,
    service_heuristics: Vec<Heuristic>,
    tool_version: Option<String>,

    service_api_host: url::Url,
    service_api_key: String,
    connection: Connection,

    tasking_dir: String,
    task_fifo_path: String,
    done_fifo_path: String,
    service_ready_path: String,
    running: Arc<Mutex<bool>>,
    task_complete_limit: Option<i64>,
    tasks_processed: i64,
}

struct TaskFifoPipes {
    task_fifo: Sender,
    done_fifo: Receiver,
}

// A simplified version of service manifest class for serializing manifest file
// only serialize the information needed for service client
#[derive(Serialize, Deserialize)]
struct SimplifiedServiceManifest {
    #[serde(default)]
    pub tool_version: Option<String>,
    #[serde(default = "default_file_required")]
    pub file_required: bool,
    #[serde(default)]
    pub heuristics: Vec<Heuristic>,
    #[serde(flatten)]
    pub service: ServiceInfo,
}

fn default_file_required() -> bool {
    true
}

/// builder for the service client struct
pub struct ServiceClientBuilder {
    register_only: bool,
    container_mode: bool,
    container_id: Option<String>,
    runtime_prefix: Option<String>,
    tmp_dir: Option<String>,
    tasking_dir: Option<String>,
    manifest_dir: Option<String>,
    server_host_string: Option<String>,
    service_api_key: Option<String>,
    root_ca_path: Option<String>,
    task_complete_limit: Option<i64>,
}

impl ServiceClientBuilder {
    /// create new service client builder instance with default values
    pub fn new() -> Self {
        ServiceClientBuilder {
            register_only: false,
            container_mode: false,
            container_id: None,
            runtime_prefix: None,
            tmp_dir: None,
            tasking_dir: None,
            manifest_dir: None,
            server_host_string: None,
            service_api_key: None,
            root_ca_path: None,
            task_complete_limit: None,
        }
    }

    /// set whether or not this service should only register the service
    pub fn set_register_only(&mut self, register_only: bool) -> &mut Self {
        self.register_only = register_only;
        self
    }

    /// set whether or not this service client should run in container mode
    pub fn set_container_mode(&mut self, container_mode: bool) -> &mut Self {
        self.container_mode = container_mode;
        self
    }

    /// set container ID
    pub fn set_container_id(&mut self, container_id: String) -> &mut Self {
        self.container_id = Some(container_id);
        self
    }

    /// set prefix for runtime files
    pub fn set_runtime_prefix(&mut self, runtime_prefix: String) -> &mut Self {
        self.runtime_prefix = Some(runtime_prefix);
        self
    }

    /// set location of temporary directory
    pub fn set_tmp_dir(&mut self, tmp_dir: String) -> &mut Self {
        self.tmp_dir = Some(tmp_dir);
        self
    }

    /// set tasking directory where the service use for creating temporary working directory and result files
    pub fn set_tasking_dir(&mut self, tasking_dir: String) -> &mut Self {
        self.tasking_dir = Some(tasking_dir);
        self
    }

    /// set directory of where the service manifest resides
    pub fn set_manifest_dir(&mut self, manifest_dir: String) -> &mut Self {
        self.manifest_dir = Some(manifest_dir);
        self
    }

    /// set service server host address
    pub fn set_server_host_string(&mut self, server_host_string: String) -> &mut Self {
        self.server_host_string = Some(server_host_string);
        self
    }

    /// set service server api key
    pub fn set_service_api_key(&mut self, service_api_key: String) -> &mut Self {
        self.service_api_key = Some(service_api_key);
        self
    }

    /// set root ca path file
    pub fn set_root_ca_path(&mut self, root_ca_path: String) -> &mut Self {
        self.root_ca_path = Some(root_ca_path);
        self
    }

    /// set task complete limit
    pub fn set_task_complete_limit(&mut self, task_complete_limit: i64) -> &mut Self {
        self.task_complete_limit = Some(task_complete_limit);
        self
    }

    /// create service client struct and load service manifest
    pub async fn build_service_client(self, running: Arc<Mutex<bool>>) -> Result<ServiceClient> {
        let container_id = self.container_id.unwrap_or(DEFAULT_CONTAINER_ID.to_owned());
        let runtime_prefix = self
            .runtime_prefix
            .unwrap_or(DEFAULT_RUNTIME_PREFIX.to_owned());
        let tmp_dir = self.tmp_dir.unwrap_or(
            std::env::temp_dir()
                .as_os_str()
                .to_string_lossy()
                .to_string(),
        );
        let tasking_dir = self.tasking_dir.unwrap_or(tmp_dir.clone());
        let manifest_dir = self
            .manifest_dir
            .unwrap_or(std::env::current_dir()?.to_string_lossy().to_string());
        let server_host_string = self
            .server_host_string
            .unwrap_or(DEFAULT_API_HOST.to_owned());
        let service_api_key = self
            .service_api_key
            .unwrap_or(DEFAULT_SERVICE_API_KEY.to_owned());
        let root_ca_path = self.root_ca_path.unwrap_or(DEFAULT_ROOT_CA_PATH.to_owned());

        let task_fifo_path = Path::new(&tmp_dir)
            .join(format!("{runtime_prefix}_task.fifo"))
            .to_string_lossy()
            .to_string();
        let done_fifo_path = Path::new(&tmp_dir)
            .join(format!("{runtime_prefix}_done.fifo"))
            .to_string_lossy()
            .to_string();
        let service_ready = Path::new(&tmp_dir)
            .join(format!("{runtime_prefix}_ready"))
            .to_string_lossy()
            .to_string();

        let runtime_manifest_path =
            Path::new(&tmp_dir).join(format!("{runtime_prefix}_manifest.yml"));

        debug!("Task fifo: {task_fifo_path}");
        debug!("Done fifo: {done_fifo_path}");
        debug!(
            "runtime manifest path: {}",
            runtime_manifest_path.clone().to_string_lossy()
        );

        info!("loading service_manifest from file.. .");
        // if there isn't a manifest file loaded for the current runtime, create one
        if !runtime_manifest_path.exists() {
            let manifest_path = Path::new(&manifest_dir).join("service_manifest.yml");
            // read the service manifest provided with the given service and write it to the runtime_manifest_path
            // which will be the loaded manifest for this run.
            debug!(
                "loading manifest path: {}",
                manifest_path.clone().to_string_lossy()
            );
            let mut manifest_file = std::fs::File::open(manifest_path)?;
            manifest_file.flush()?;
            let mut runtime_manifest_file = std::fs::File::create(&runtime_manifest_path)?;
            let _size = std::io::copy(&mut manifest_file, &mut runtime_manifest_file)?;
            runtime_manifest_file.flush()?;
        }

        let runtime_manifest_file = std::fs::File::open(&runtime_manifest_path)?;
        let mut service_manifest: SimplifiedServiceManifest =
            serde_yaml::from_reader(&runtime_manifest_file)?;

        // update service manifest version tag if it is the placeholder value
        if service_manifest.service.version.to_string() == PLACEHOLDER_VERSION_TAG {
            service_manifest.service.version = ServiceVersion::from_str(get_version().as_str())?;
            warn!(
                "Replacing placeholder version tag {PLACEHOLDER_VERSION_TAG} with {}",
                service_manifest.service.version
            );
        }

        let server_host_url = url::Url::parse(server_host_string.as_str())?;
        let key_text: &str = if service_api_key == DEFAULT_SERVICE_API_KEY {
            "'**default key** - You should consider setting SERVICE_API_KEY in your service containers"
        } else {
            "**custom key**"
        };

        info!("---- TaskHandler config ----");
        info!("SERVICE_API_HOST: {}", server_host_string);
        info!("SERVICE_APIKEY: {}", key_text);
        info!("CONTAINER-ID: {}", container_id);
        info!("----------------------------");

        info!(
            "Service: {} Version: {}",
            service_manifest.service.name, service_manifest.service.version
        );
        info!("HEURISTICS_COUNT: {}", service_manifest.heuristics.len());

        let tls_setting = if server_host_url.scheme().eq("https") {
            TLSSettings::CARootPath(root_ca_path.clone())
        } else {
            TLSSettings::Native
        };

        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert("X-APIKey".to_string(), service_api_key.clone());
        headers.insert("Container-ID".to_string(), container_id.clone());
        headers.insert(
            "Service-Name".to_string(),
            service_manifest.service.name.to_string(),
        );
        headers.insert(
            "Service-Version".to_string(),
            service_manifest.service.version.to_string(),
        );
        headers.insert(
            "Service-Tool-Version".to_string(),
            service_manifest
                .tool_version
                .clone()
                .unwrap_or("".to_string()),
        );

        let con = ConnectionBuilder::new(server_host_string.clone(), ServerType::ServiceServer)
            .set_raw_headers(headers.clone())
            .set_timeout(1000.0)
            .set_retry(5)
            .set_tls_settings(tls_setting.clone())
            .build_and_connect()
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(ServiceClient {
            register_only: self.register_only,
            container_mode: self.container_mode,
            runtime_prefix,

            service_info: service_manifest.service,
            // service_timeout will be updated again when register with service server.
            service_timeout: Default::default(),

            manifest_file_path: runtime_manifest_path.to_string_lossy().to_string(),
            file_required: service_manifest.file_required,
            service_heuristics: service_manifest.heuristics,
            tool_version: service_manifest.tool_version,

            service_api_host: server_host_url,
            service_api_key,
            connection: con,

            container_id,
            tasking_dir,
            task_fifo_path,
            done_fifo_path,
            service_ready_path: service_ready,
            running,
            task_complete_limit: self.task_complete_limit,
            tasks_processed: 0,
        })
    }
}

impl Default for ServiceClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceClient {
    async fn update_client_header(&mut self) -> Result<()> {
        let mut headers: HashMap<String, String> = HashMap::new();
        // need to update connection header to have the new tool version
        headers.insert("X-APIKey".to_string(), self.service_api_key.clone());
        headers.insert("Container-ID".to_string(), self.container_id.clone());
        headers.insert(
            "Service-Name".to_string(),
            self.service_info.name.to_string(),
        );
        headers.insert(
            "Service-Version".to_string(),
            self.service_info.version.to_string(),
        );
        headers.insert(
            "Service-Tool-Version".to_string(),
            self.tool_version.clone().unwrap_or("".to_string()),
        );

        let _ = self
            .connection
            .update_client_default_headers(headers)
            .await
            .map_err(|e| anyhow!(e));
        Ok(())
    }

    /// get connection struct to the service server
    #[cfg(test)]
    pub fn get_connection(&self) -> &Connection {
        &self.connection
    }

    /// the the path to the service ready path
    #[cfg(test)]
    pub fn get_service_ready_path(&self) -> String {
        self.service_ready_path.clone()
    }

    async fn _setup_fifo_pipes(&self) -> Result<TaskFifoPipes> {
        info!("Setting up task and result fifo pipe to communicate with service process.");
        // create named pipe to communicate with service
        let task_path_ptr: CString = CString::new(self.task_fifo_path.clone())?;
        let done_path_ptr: CString = CString::new(self.done_fifo_path.clone())?;

        debug!(
            "Trying to make fifo pipe at {} and at {}",
            self.task_fifo_path, self.done_fifo_path
        );

        // mkfifo is an unsafe operation
        unsafe {
            let status_code_task = mkfifo(task_path_ptr.as_ptr(), 0o644);
            let status_code_done = mkfifo(done_path_ptr.as_ptr(), 0o644);

            if status_code_task != 0 {
                return Err(anyhow!(
                    "Cannot create task named queue at {}",
                    self.task_fifo_path
                ));
            }

            if status_code_done != 0 {
                return Err(anyhow!(
                    "Cannot create done named queue at {}",
                    self.done_fifo_path
                ));
            }
        };

        let done_receiver = pipe::OpenOptions::new().open_receiver(self.done_fifo_path.clone())?;
        let mut open_sender = pipe::OpenOptions::new().open_sender(self.task_fifo_path.clone());

        let mut retry = 0;
        // retry opening sender pipe for a minute or fail.
        while open_sender.is_err() && retry < 15 {
            debug!(
                "Cannot find task sender pipe... waiting for reader. Retry #{}",
                retry
            );
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(4.0)).await;
            open_sender = pipe::OpenOptions::new().open_sender(self.task_fifo_path.clone());
            retry += 1;
        }

        debug!("Service server finished setting up fifo queues.");

        Ok(TaskFifoPipes {
            task_fifo: open_sender?,
            done_fifo: done_receiver,
        })
    }

    /// register service with the service server and update runtime service manifest
    pub async fn register_service(&mut self) -> Result<bool, ServiceClientError> {
        let register_url = self.connection.get_api_path("service", &["register"])?;

        // load runtime service manifest
        let manifest_file = std::fs::File::open(&self.manifest_file_path)?;
        let temp_service_manifest: JsonMap = serde_yaml::from_reader(manifest_file)?;
        info!("Send request to service server to register service.");

        let register_response: RegisterResponse = self
            .connection
            .post(
                register_url,
                connection::Body::Json(&temp_service_manifest),
                None,
                convert_api_output_obj,
            )
            .await?;

        if !register_response.new_heuristics.is_empty() {
            info!(
                "New heuristics registered: {}",
                register_response.new_heuristics.join(", ")
            );
        }

        // load new service configuration
        // update and write to manifest file with the updated manifest data
        let updated_manifest = ServiceManifest {
            service: register_response.service_config.to_owned(),
            tool_version: self.tool_version.clone(),
            file_required: self.file_required,
            heuristics: self.service_heuristics.clone(),
        };

        self.service_timeout = updated_manifest.service.timeout;

        let mut manifest_file = File::create(&self.manifest_file_path).await?;
        let manifest_data = serde_yaml::to_string(&updated_manifest).unwrap();
        manifest_file.write_all(manifest_data.as_bytes()).await?;
        let _ = manifest_file.flush().await;

        // update connection client header to have the update to date service information
        self.update_client_header().await?;

        Ok(register_response.keep_alive)
    }

    async fn process_task_done_message(
        &mut self,
        done_message: Vec<String>,
        task: Task,
        task_uploader: &TaskUploader,
    ) -> Result<(), ServiceClientError> {
        // done message should be in the form [FILE_PATH, DONE_STATUS]
        let done_file_path = done_message.first().ok_or(ServiceClientError::Default(
            "Cannot find result file path in message from service.".to_string(),
        ))?;

        info!(
            "[{}]-sid[{}] Task done status is: {:?}",
            task.task_id,
            task.sid,
            done_message.get(1)
        );

        // Done message format [filepath, STATUS]
        match done_message.get(1) {
            Some(status) => {
                match status.as_str() {
                    TASK_DONE_SUCCESS => {
                        let data = tokio::fs::read(done_file_path).await?;
                        let task_done_result: service_api::result::Result =
                            serde_json::from_slice(data.as_slice())?;

                        // update service tool version if there is a change
                        if let Some(service_tool_version) =
                            &task_done_result.response.service_tool_version
                        {
                            if self
                                .tool_version
                                .as_ref()
                                .is_some_and(|version| *service_tool_version != *version)
                            {
                                self.tool_version = Some(service_tool_version.clone());
                                self.update_client_header().await?;
                            }
                        }

                        // take the output result and do error handling....
                        let result = task_uploader
                            .upload_task_result(&task, task_done_result, &self.connection)
                            .await;

                        debug!("Finished uploading task result to server.");

                        match result {
                            Ok(_) => info!(
                                "[{}]-sid[{}] Successfully upload task results.",
                                task.task_id, task.sid
                            ),
                            Err(err) => {
                                // upload this error
                                warn!(
                                    "[{}]-sid[{}] Error uploading task results. {}",
                                    task.task_id, task.sid, err
                                );

                                let mut error_data = TaskErrorData::new(
                                    task.fileinfo.sha256.clone(),
                                    self.service_info.clone(),
                                );
                                error_data
                                    .set_message(err.to_string())
                                    .set_error_type(EXCEPTION_SERVICE_ERROR_TYPE.to_owned())
                                    .set_status(UNRECOVERABLE_ERROR_STATUS.to_owned());

                                let _ = task_uploader
                                    .upload_task_error(&task, &self.connection, error_data)
                                    .await;
                            }
                        }
                    }
                    TASK_DONE_ERROR => {
                        let data = tokio::fs::read(done_file_path).await?;
                        let mut error_data = TaskErrorData::new(
                            task.fileinfo.sha256.clone(),
                            self.service_info.clone(),
                        );
                        error_data.set_error_json(serde_json::from_slice(data.as_slice())?);
                        let _ = task_uploader
                            .upload_task_error(&task, &self.connection, error_data)
                            .await;
                    }
                    _ => {
                        return Err(ServiceClientError::Default(format!(
                            "Unknown task done status from done pipe {}",
                            status
                        )))
                    }
                }
            }
            None => {
                return Err(ServiceClientError::Default(
                    "Did not get task done status from service.".to_owned(),
                ))
            }
        };

        Ok(())
    }

    async fn create_task_file(&self, task: &Task, task_dir: &Path) -> Result<PathBuf> {
        let sid = task.sid;
        let sha256 = task.fileinfo.sha256.clone();
        // write task json file to tasking directory
        let task_json = serde_json::to_string(&task)?;
        let mut task_file_path = task_dir.to_path_buf();
        task_file_path.push(format!("{sid}_{sha256}_task.json"));
        let mut task_file = File::create(task_file_path.as_path()).await?;
        task_file.write_all(task_json.as_bytes()).await?;
        task_file.flush().await?;

        debug!(
            "[{}]-sid[{}]: Successfully written task file to: {}",
            task.task_id,
            task.sid,
            task_file_path.to_string_lossy()
        );

        Ok(task_file_path)
    }

    async fn write_task_to_fifo(
        &self,
        task_dir: &PathBuf,
        task_file: &PathBuf,
        fifo_pipes: &mut TaskFifoPipes,
    ) -> Result<(), ServiceClientError> {
        // task message should be in the form "[task_dir_path, task_file_path]\n"
        let task_message_data = json!([
            task_dir.to_owned().to_string_lossy().to_string(),
            task_file.to_owned().to_string_lossy().to_string()
        ]);

        let data = [task_message_data.to_string().as_bytes(), "\n".as_bytes()].concat();
        fifo_pipes
            .task_fifo
            .write_all(data.as_slice())
            .await
            .map_err(|e| ServiceClientError::PipeWriteError {
                pipe_name: TASK_FIFO_NAME.to_string(),
                message: e.to_string(),
            })?;

        fifo_pipes
            .task_fifo
            .flush()
            .await
            .map_err(|e| ServiceClientError::PipeWriteError {
                pipe_name: TASK_FIFO_NAME.to_string(),
                message: e.to_string(),
            })?;

        Ok(())
    }

    async fn read_result_from_fifo(
        &self,
        fifo_pipes: &mut TaskFifoPipes,
        service_process: &mut Child,
    ) -> Result<Vec<String>, ServiceClientError> {
        // wait for service to process task and listening for notification from the done fifo
        tokio::select! {
            pipe_timeout = tokio::time::timeout( Duration::from_secs(self.service_timeout as u64), fifo_pipes.done_fifo.readable()) => {
                match pipe_timeout {
                    Ok(_) => debug!("Done fifo is ready to read."),
                    Err(_) => {
                        // kill service process because it is taking too long to process the file.
                        let _ = service_process.start_kill();
                        return Err(ServiceClientError::PipeReadError { pipe_name: DONE_FIFO_NAME.to_owned(), message: format!("Read result pipe timeout at {} seconds.", self.service_timeout) });
                    },
                };
            },

            status_code = service_process.wait() => {
                log_error!("Service process terminated with status code: {:?}", status_code);
                return Err(ServiceClientError::PipeReadError { pipe_name: DONE_FIFO_NAME.to_owned(), message: format!("Service process terminated with status code: {:?}", status_code) });
            }
        }

        // try to read what is in done pipe. Make sure to capture any error in the fifo
        let pipe_ready = fifo_pipes
            .done_fifo
            .ready(Interest::READABLE)
            .await
            .map_err(|e| ServiceClientError::PipeReadError {
                pipe_name: DONE_FIFO_NAME.to_owned(),
                message: e.to_string(),
            })?;

        while pipe_ready.is_readable() {
            let mut msg = vec![0; 1024];
            match fifo_pipes.done_fifo.try_read(&mut msg) {
                Ok(data_size) => {
                    debug!("Start to read done_fifo data.");
                    msg.truncate(data_size);

                    let value = serde_json::from_slice::<Vec<String>>(msg.as_slice());
                    match value {
                        Ok(v) => {
                            return Ok(v);
                        }
                        Err(err) => {
                            return Err(ServiceClientError::PipeReadError {
                                pipe_name: DONE_FIFO_NAME.to_owned(),
                                message: format!(
                                    "Unknown message format received from done fifo. Err: {}",
                                    err
                                ),
                            });
                        }
                    }
                }
                // Just need to try to read pipe again if we get wouldblock error
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    debug!("Error::WouldBlock reading done fifo pipe. Sleep for 1 second and try again.");
                    // wait a second before trying to read the pipe again.
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
                }
                Err(err) => {
                    return Err(ServiceClientError::PipeReadError {
                        pipe_name: DONE_FIFO_NAME.to_owned(),
                        message: err.to_string(),
                    });
                }
            }
        }

        Err(ServiceClientError::Default(
            "Invalid service client state. Failed to read from done fifo.".to_string(),
        ))
    }

    /// start tasking fetching loop for the service
    pub async fn run_service(
        &mut self,
        task_fetcher: &mut impl TaskFetcher,
        task_uploader: &TaskUploader,
        service_launcher: &impl ServiceLauncher,
    ) -> Result<()> {
        let register_service_result = self.register_service().await;

        match register_service_result {
            Ok(keep_alive) => {
                if !keep_alive || self.register_only {
                    let mut running = self.running.lock();
                    *running = false;
                    info!("keep_alive is false or register only. Shut down now.");
                    return Ok(());
                } else {
                    info!("Finished registering service.");
                }
            }
            Err(e) => {
                log_error!("Error registering service: {:?}", e);
                return Err(anyhow!(e.to_string()));
            }
        };

        let mut service_process = service_launcher
            .launch_service()
            .await
            .map_err(|e| anyhow!(e))?;

        // setup fifo queue for service to use
        let mut fifo_pipes = self._setup_fifo_pipes().await?;

        // wait for service to be ready in task fetching
        info!("Start task fetching loop...");
        while self.is_running() && self.wait_for_service_ready(&mut service_process).await {
            let task_fetched = task_fetcher.get_task(&self.connection).await;

            match task_fetched {
                Ok(None) => {
                    debug!("Cannot find a task.");
                }

                Err(err) => {
                    log_error!("Error fetching task: {}", err);
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
                }

                Ok(Some(task)) => {
                    info!("[{}]-sid[{}] Got task.", task.task_id, task.sid);
                    // create a temporary directory for each task. Both service handler and service process reads and writes from.
                    let task_dir = tempdir_in(&self.tasking_dir)?;
                    let task_dir_path = task_dir.path().to_path_buf();

                    if self.file_required {
                        let download_result = task_fetcher
                            .download_file(
                                task.fileinfo.sha256.clone(),
                                task_dir_path.clone(),
                                &self.connection,
                            )
                            .await;

                        match download_result {
                            Ok(_f) => debug!(
                                "[{}]-sid[{}]: File {} successfully downloaded.",
                                task.task_id, task.sid, task.fileinfo.sha256
                            ),

                            Err(err) => {
                                log_error!(
                                    "[{}]-sid[{}] Error download file ({}): {}",
                                    task.task_id,
                                    task.sid,
                                    task.fileinfo.sha256,
                                    err
                                );

                                let mut error_data = TaskErrorData::new(
                                    task.fileinfo.sha256.clone(),
                                    self.service_info.clone(),
                                );
                                error_data
                                    .set_message(err.to_string())
                                    .set_error_type(EXCEPTION_SERVICE_ERROR_TYPE.to_string())
                                    .set_status(UNRECOVERABLE_ERROR_STATUS.to_string());
                                let _ = task_uploader
                                    .upload_task_error(&task, &self.connection, error_data)
                                    .await;
                                // failed to download the file required for the task. Continue to fetch the next task
                                continue;
                            }
                        }
                    }

                    let task_file_path = self.create_task_file(&task, &task_dir_path).await?;

                    // notify service process through task fifo queue that a task is ready
                    let write_task_fifo_result = self
                        .write_task_to_fifo(&task_dir_path, &task_file_path, &mut fifo_pipes)
                        .await;

                    if let Some(err) = write_task_fifo_result.err() {
                        log_error!(
                            "Error writing to task fifo. Terminating service handler. {:?}",
                            err
                        );

                        let _ = task_uploader
                            .upload_task_error(
                                &task,
                                &self.connection,
                                TaskErrorData::new(
                                    task.fileinfo.sha256.clone(),
                                    self.service_info.clone(),
                                ),
                            )
                            .await;

                        // if we failed to write to task fifo, the service process is likely dead.
                        self.stop_service_client();
                        continue;
                    }

                    // wait for service to process task and wait for notification from the done fifo
                    let read_done_fifo_result = self
                        .read_result_from_fifo(&mut fifo_pipes, &mut service_process)
                        .await;

                    match read_done_fifo_result {
                        Ok(done_message) => {
                            let process_result = self
                                .process_task_done_message(
                                    done_message,
                                    task.clone(),
                                    task_uploader,
                                )
                                .await;

                            if let Err(err) = process_result {
                                log_error!("{err}");
                            }
                        }
                        Err(e) => {
                            log_error!("{e}");
                            let _ = task_uploader
                                .upload_task_error(
                                    &task,
                                    &self.connection,
                                    TaskErrorData::new(
                                        task.fileinfo.sha256.clone(),
                                        self.service_info.clone(),
                                    ),
                                )
                                .await;
                        }
                    }
                    self.tasks_processed += 1;
                }
            }
        }

        info!("Service client terminated. Start clean up.");
        let _ = service_process.start_kill();
        let _ = self.cleanup_files();
        Ok(())
    }

    /// get runtime manifest path
    #[cfg(test)]
    pub fn get_runtime_manifest_path(&self) -> String {
        self.manifest_file_path.clone()
    }

    fn cleanup_files(&self) -> Result<()> {
        info!("Start service client cleanup!");

        let df = Path::new(&self.done_fifo_path);

        if df.exists() {
            info!("try to delete done fifo");
            let _ = fs::remove_file(df);
        }

        let tf = Path::new(&self.task_fifo_path);

        if tf.exists() {
            info!("try to delete task fifo");
            let _ = fs::remove_file(tf);
        }

        let mf = Path::new(&self.manifest_file_path);

        if mf.exists() {
            info!("try to delete runtime manifest file.");
            let _ = fs::remove_file(mf);
        }

        let sr = Path::new(&self.service_ready_path);
        if sr.exists() {
            info!("try to delete service ready");
            let _ = fs::remove_file(sr);
        }

        Ok(())
    }

    /// is this service client running?
    pub fn is_running(&self) -> bool {
        // service should stop running once the task limit is reached.
        if self
            .task_complete_limit
            .is_some_and(|v| v == self.tasks_processed)
        {
            *self.running.lock() = false;
        }

        *self.running.lock()
    }

    /// stop service client
    pub fn stop_service_client(&self) {
        *self.running.lock() = false;
    }

    /// wait for the service process to be in ready state to process tasks
    pub async fn wait_for_service_ready(&self, service_process: &mut Child) -> bool {
        debug!("Wait for service to be in ready state.");
        let service_ready_path = Path::new(&self.service_ready_path);

        if !service_ready_path.exists() {
            info!("Cannot find service ready file. Wait for service to be ready.");
        }

        async fn wait_for_ready_file(ready_path: &Path) {
            while !ready_path.exists() {
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(2.0)).await;
            }
        }

        async fn service_client_terminated(sc: &ServiceClient) {
            while sc.is_running() {
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(2.0)).await;
            }
        }

        let mut service_ready = false;

        tokio::select! {
            _ = wait_for_ready_file(service_ready_path) => {
                debug!("Service ready file found. Service in ready state.");

                // make sure that service is actually running.
                match service_process.try_wait() {
                    // service process is running and service ready found.
                    Ok(None) => service_ready = true,

                    // service terminated
                    Ok(Some(code)) => log_error!("Service process terminated with code {code}"),
                    Err(e) => log_error!("Error waiting for service process: {e}"),
                }
            },
            _ = service_client_terminated(self) => {
                info!("Service client terminated. Do not wait for service ready.");
            }
            status_code = service_process.wait() => {
                log_error!("Service process terminated with status code: {:?}. Do not wait for service ready.", status_code);
            }
        }

        service_ready
    }
}
