use std::{
    collections::HashMap,
    ffi::CString,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use assemblyline_models::{
    datastore::Service,
    messages::{
        service_api::{
            self,
            service_manifest::{Heuristic, ServiceManifest},
        },
        task::Task,
    },
};
use assemblyline_utilities::{
    connection::{self, convert_api_output_obj, Connection, ServerType, TLSSettings},
    types::{authentication::Authentication, response::RegisterResponse},
};
use libc::mkfifo;
use log::{debug, error as log_error, info, warn};
use ort::tensor::Utf8Data;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tempfile::tempdir_in;
use tokio::{
    fs::File,
    io::{AsyncWriteExt, Interest},
    net::unix::pipe::{self, Receiver, Sender},
    process::Child,
};

use crate::{
    constants::{
        get_version, DEFAULT_SERVICE_API_KEY, DONE_FIFO_NAME, EXCEPTION_SERVICE_ERROR_TYPE, PLACEHOLDER_VERSION_TAG, TASK_DONE_ERROR,
        TASK_DONE_SUCCESS, TASK_FIFO_NAME, UNRECOVERABLE_ERROR_STATUS,
    },
    service_launcher::ServiceLauncher,
    task_fetcher::task_fetcher::TaskFetcher,
    task_uploader::task_uploader::TaskUploader,
    types::errors::ServiceClientError,
};

pub struct ServiceClient {
    register_only: bool,
    pub container_mode: bool,
    pub service: Service,
    manifest_file_path: String,
    service_heuristics: Vec<Heuristic>,
    tool_version: Option<String>,
    service_api_host: url::Url,
    runtime_prefix: String,
    service_api_key: String,
    container_id: String,
    file_required: bool,
    tasking_dir: String,
    task_fifo_path: String,
    done_fifo_path: String,
    service_ready_path: String,
    running: Arc<Mutex<bool>>,
    task_complete_limit: Option<i32>,
    tasks_processed: i32,
    connection: Connection,
}

struct TaskFifoPipes {
    task_fifo: Sender,
    done_fifo: Receiver,
}

impl ServiceClient {
    pub async fn new(
        register_only: bool,
        container_mode: bool,
        running: Arc<Mutex<bool>>,
        container_id: String,
        runtime_prefix: String,
        tmp_folder: String,
        tasking_dir: String,
        manifest_folder: String,
        server_host_string: String,
        service_api_key: String,
        root_ca_path: String,
        task_complete_limit: Option<i32>,
    ) -> Result<Self> {
        let task_fifo_path = format!("{}{}_task.fifo", tmp_folder, runtime_prefix);
        let done_fifo_path = format!("{}{}_done.fifo", tmp_folder, runtime_prefix);
        let service_ready = format!("{}{}_ready", tmp_folder, runtime_prefix);
        let runtime_manifest_path = format!("{}{}_manifest.yml", tmp_folder, runtime_prefix);

        info!("-----------load_service_manifest from file-------");
        // if there isn't a manifest file loaded for the current runtime, create one
        if !Path::new(&runtime_manifest_path).exists() {
            let manifest_path = format!("{}service_manifest.yml", manifest_folder);
            // read the service manifest provided with the given service and write it to the runtime_manifest_path
            // which will be the loaded manifest for this run.
            fs::copy(Path::new(&manifest_path), Path::new(&runtime_manifest_path))?;
        }

        let runtime_manifest_file = std::fs::File::open(&runtime_manifest_path)?;
        let mut service_manifest: ServiceManifest = serde_yaml::from_reader(runtime_manifest_file)?;

        // update service manifest version tag if it is the placeholder value
        if service_manifest.service.version == PLACEHOLDER_VERSION_TAG {
            service_manifest.service.version = get_version().clone();
            warn!(
                "Replacing placeholder version tag {PLACEHOLDER_VERSION_TAG} with {}",
                &service_manifest.service.version
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

        info!("Service: {} Version: {}", service_manifest.service.name, service_manifest.service.version);
        info!("HEURISTICS_COUNT: {}", service_manifest.heuristics.len());

        let tls_setting = if server_host_url.scheme().eq("https") {
            TLSSettings::CARootPath(root_ca_path.clone())
        } else {
            TLSSettings::Native
        };

        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert("X-APIKey".to_string(), service_api_key.clone());
        headers.insert("Container-ID".to_string(), container_id.clone());
        headers.insert("Service-Name".to_string(), service_manifest.service.name.to_string());
        headers.insert("Service-Version".to_string(), service_manifest.service.version.clone());
        headers.insert(
            "Service-Tool-Version".to_string(),
            service_manifest.tool_version.clone().unwrap_or("".to_string()),
        );

        let con = Connection::connect(
            server_host_string,
            ServerType::ServiceServer,
            false,
            Authentication::None,
            Some(5),
            tls_setting.clone(),
            headers.clone(),
            Some(1000.0),
        )
        .await
        .map_err(|e| anyhow!(e))?;

        Ok(ServiceClient {
            register_only: register_only,
            container_mode: container_mode,
            runtime_prefix: runtime_prefix.to_owned(),
            service: service_manifest.service,
            file_required: service_manifest.file_required,
            service_heuristics: service_manifest.heuristics,
            tool_version: service_manifest.tool_version,
            service_api_host: server_host_url,
            manifest_file_path: runtime_manifest_path,
            service_api_key,
            container_id,
            tasking_dir,
            task_fifo_path,
            done_fifo_path,
            service_ready_path: service_ready,
            running,
            connection: con,
            task_complete_limit,
            tasks_processed: 0,
        })
    }

    async fn update_client_header(&mut self) -> Result<()> {
        let mut headers: HashMap<String, String> = HashMap::new();
        // need to update connection header to have the new tool version
        headers.insert("X-APIKey".to_string(), self.service_api_key.clone());
        headers.insert("Container-ID".to_string(), self.container_id.clone());
        headers.insert("Service-Name".to_string(), self.service.name.to_string());
        headers.insert("Service-Version".to_string(), self.service.version.clone());
        headers.insert("Service-Tool-Version".to_string(), self.tool_version.clone().unwrap_or("".to_string()));

        let _ = self.connection.update_client_default_headers(headers).await.map_err(|e| anyhow!(e));
        Ok(())
    }

    #[cfg(test)]
    pub fn get_connection(&self) -> &Connection {
        &self.connection
    }

    async fn _setup_fifo_pipes(&self) -> Result<TaskFifoPipes> {
        info!("Setting up task and result fifo pipe to communicate with service process.");
        // create named pipe to communicate with service
        let task_path_ptr: CString = CString::new(self.task_fifo_path.clone())?;
        let done_path_ptr: CString = CString::new(self.done_fifo_path.clone())?;

        debug!("Trying to make fifo pipe at {} and at {}", &self.task_fifo_path, &self.done_fifo_path);

        // mkfifo is an unsafe operation
        unsafe {
            let status_code_task = mkfifo(task_path_ptr.as_ptr(), 0o644);
            let status_code_done = mkfifo(done_path_ptr.as_ptr(), 0o644);

            if status_code_task != 0 {
                return Err(anyhow!("Cannot create task named queue at {}", self.task_fifo_path));
            }

            if status_code_done != 0 {
                return Err(anyhow!("Cannot create done named queue at {}", self.done_fifo_path));
            }
        };

        let done_receiver = pipe::OpenOptions::new().open_receiver(self.done_fifo_path.clone())?;
        let mut open_sender = pipe::OpenOptions::new().open_sender(self.task_fifo_path.clone());

        while open_sender.is_err() {
            debug!("Cannot find task sender pipe... waiting for reader");
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(4.0)).await;
            open_sender = pipe::OpenOptions::new().open_sender(self.task_fifo_path.clone());
        }

        debug!("Service server finished setting up fifo queues.");

        Ok(TaskFifoPipes {
            task_fifo: open_sender?,
            done_fifo: done_receiver,
        })
    }

    pub async fn register_service(&mut self) -> Result<bool, ServiceClientError> {
        let register_url = self.connection.get_api_path("service", &["register"])?;

        let mut temp_service_manifest = ServiceManifest {
            service: self.service.clone(),
            tool_version: self.tool_version.clone(),
            file_required: self.file_required,
            heuristics: self.service_heuristics.clone(),
        };

        info!("Send request to service server to register service.");

        let register_response: RegisterResponse = self
            .connection
            .post(register_url, connection::Body::Json(&temp_service_manifest), None, convert_api_output_obj)
            .await?;

        if !register_response.new_heuristics.is_empty() {
            info!("New heuristics registered: {}", register_response.new_heuristics.join(", "));
        }

        // load new service configuration
        self.service = register_response.service_config.to_owned();

        // update and write to manifest file with the updated manifest data
        temp_service_manifest.service = self.service.clone();
        let mut manifest_file = File::create(&self.manifest_file_path).await?;
        let manifest_data = serde_yaml::to_string(&temp_service_manifest).unwrap();
        manifest_file.write_all(manifest_data.as_bytes()).await?;

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
        let done_file_path = done_message.get(0).ok_or(ServiceClientError::Default(
            "Cannot find result file path in message from service.".to_string(),
        ))?;

        info!("[{}]-sid[{}] Task done status is: {:?}", &task.task_id, &task.sid, done_message.get(1));

        // Done message format [filepath, STATUS]
        match done_message.get(1) {
            Some(status) => {
                match status.as_str() {
                    TASK_DONE_SUCCESS => {
                        let data = tokio::fs::read(done_file_path).await?;
                        let task_done_result: service_api::result::Result = serde_json::from_slice(data.as_slice())?;

                        // update service tool version if there is a change
                        if let Some(service_tool_version) = &task_done_result.response.service_tool_version {
                            if self.tool_version.as_ref().is_some_and(|version| *service_tool_version != *version) {
                                self.tool_version = Some(service_tool_version.clone());
                                self.update_client_header().await?;
                            }
                        }

                        // take the output result and do error handling....
                        let result = task_uploader.upload_task_result(&task, task_done_result, &self.connection).await;

                        debug!("Finished uploading task result to server.");

                        match result {
                            Ok(_) => info!("[{}]-sid[{}] Successfully upload task results.", &task.task_id, &task.sid),
                            Err(err) => {
                                // upload this error
                                warn!("[{}]-sid[{}] Error uploading task results. {}", &task.task_id, &task.sid, err);
                                let _ = task_uploader
                                    .upload_task_error(
                                        &task,
                                        &self.service,
                                        &self.connection,
                                        None,
                                        Some(err.to_string()),
                                        Some(UNRECOVERABLE_ERROR_STATUS.to_owned()),
                                        Some(EXCEPTION_SERVICE_ERROR_TYPE.to_owned()),
                                    )
                                    .await;
                            }
                        }
                    }
                    TASK_DONE_ERROR => {
                        let data = tokio::fs::read(done_file_path).await?;
                        let error_json: Value = serde_json::from_slice(data.as_slice())?;

                        let _ = task_uploader
                            .upload_task_error(&task, &self.service, &self.connection, Some(error_json), None, None, None)
                            .await;
                    }
                    _ => return Err(ServiceClientError::Default(format!("Unknown task done status {}", status))),
                }
            }
            None => return Err(ServiceClientError::Default(format!("Did not get task done status from service."))),
        };

        Ok(())
    }

    async fn create_task_file(&self, task: &Task, task_dir: &PathBuf) -> Result<PathBuf> {
        let sid = task.sid.clone();
        let sha256 = task.fileinfo.sha256.clone();
        // write task json file to tasking directory
        let task_json = serde_json::to_string(&task)?;
        let mut task_file_path = task_dir.clone();
        task_file_path.push(format!("{sid}_{sha256}_task.json"));
        let mut task_file = File::create(task_file_path.as_path()).await?;
        let _ = task_file.write_all(task_json.as_bytes()).await?;

        Ok(task_file_path)
    }

    async fn write_task_to_fifo(&self, task_dir: &PathBuf, task_file: &PathBuf, fifo_pipes: &mut TaskFifoPipes) -> Result<(), ServiceClientError> {
        // task message should be in the form "[task_dir_path, task_file_path]\n"
        let task_message_data = json!([
            task_dir.to_owned().to_string_lossy().to_string(),
            task_file.to_owned().to_string_lossy().to_string()
        ]);

        let data = [task_message_data.to_string().as_bytes(), "\n".as_utf8_bytes()].concat();
        fifo_pipes
            .task_fifo
            .write_all(data.as_slice())
            .await
            .map_err(|e| ServiceClientError::PipeWriteError {
                pipe_name: TASK_FIFO_NAME.to_string(),
                message: e.to_string(),
            })?;

        fifo_pipes.task_fifo.flush().await.map_err(|e| ServiceClientError::PipeWriteError {
            pipe_name: TASK_FIFO_NAME.to_string(),
            message: e.to_string(),
        })?;

        Ok(())
    }

    async fn read_result_from_fifo(&self, fifo_pipes: &mut TaskFifoPipes, service_process: &mut Child) -> Result<Vec<String>, ServiceClientError> {
        // wait for service to process task and listening for notification from the done fifo
        tokio::select! {
            _ = fifo_pipes.done_fifo.readable() => {
                debug!("Done fifo is ready to read.");
            },

            status_code = service_process.wait() => {
                log_error!("Service process terminated with status code: {:?}", status_code);
                return Err(ServiceClientError::PipeWriteError { pipe_name: DONE_FIFO_NAME.to_owned(), message: format!("Service process terminated with status code: {:?}", status_code) });
            }
        }

        // try to read what is in done pipe. Make sure to capture any error in the fifo
        let pipe_ready = fifo_pipes
            .done_fifo
            .ready(Interest::READABLE)
            .await
            .map_err(|e| ServiceClientError::PipeWriteError {
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
                            return Err(ServiceClientError::PipeWriteError {
                                pipe_name: DONE_FIFO_NAME.to_owned(),
                                message: format!("Unknown message format received from done fifo. Err: {}", err),
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
                    return Err(ServiceClientError::PipeWriteError {
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
                    info!("Keep alive is false. Shut down now.");
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

        let mut service_process = service_launcher.launch_service().await.map_err(|e| anyhow!(e))?;

        // setup fifo queue for service to use
        let mut fifo_pipes = self._setup_fifo_pipes().await?;

        // wait for service to be ready and fifo pipes to be connected
        let service_ready_path = Path::new(&self.service_ready_path);

        while !service_ready_path.exists() && self.is_running() {
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(2.0)).await;
        }

        let mut is_service_running = true;

        info!("Service ready. Start task fetching loop...");
        while self.is_running() && is_service_running {
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
                    info!("[{}]-sid[{}] Got task.", &task.task_id, &task.sid);
                    // create a temporary directory for each task. Both service handler and service process reads and writes from.
                    let task_dir = tempdir_in(&self.tasking_dir)?;
                    let task_dir_path = task_dir.path().to_path_buf();

                    if self.file_required {
                        let download_result = task_fetcher
                            .download_file(task.fileinfo.sha256.clone(), task_dir_path.clone(), &self.connection)
                            .await;

                        match download_result {
                            Ok(_f) => debug!(
                                "[{}]-sid[{}]: File {} successfully downloaded.",
                                &task.task_id, &task.sid, &task.fileinfo.sha256
                            ),

                            Err(err) => {
                                log_error!(
                                    "[{}]-sid[{}] Error download file ({}): {}",
                                    &task.task_id,
                                    &task.sid,
                                    &task.fileinfo.sha256,
                                    err
                                );
                                let _ = task_uploader
                                    .upload_task_error(
                                        &task,
                                        &self.service,
                                        &self.connection,
                                        None,
                                        Some(err.to_string()),
                                        Some(EXCEPTION_SERVICE_ERROR_TYPE.to_string()),
                                        Some(UNRECOVERABLE_ERROR_STATUS.to_string()),
                                    )
                                    .await;
                                // failed to download the file required for the task. Continue to fetch the next task
                                continue;
                            }
                        }
                    }

                    // write task to a file for the service process to use
                    let task_file_path = self.create_task_file(&task, &task_dir_path).await?;

                    // notify service process through task fifo queue that a task is ready
                    let write_task_fifo_result = self.write_task_to_fifo(&task_dir_path, &task_file_path, &mut fifo_pipes).await;

                    if let Some(err) = write_task_fifo_result.err() {
                        log_error!("Error writing to task fifo. Terminating service handler. {:?}", err);
                        let _ = task_uploader
                            .upload_task_error(&task, &self.service, &self.connection, None, None, None, None)
                            .await;

                        // if we failed to write to task fifo, the service process is likely dead.
                        is_service_running = false;
                        continue;
                    }

                    // wait for service to process task and wait for notification from the done fifo
                    let read_done_fifo_result = self.read_result_from_fifo(&mut fifo_pipes, &mut service_process).await;

                    match read_done_fifo_result {
                        Ok(done_message) => {
                            let process_result = self.process_task_done_message(done_message, task.clone(), &task_uploader).await;

                            if let Err(err) = process_result {
                                log_error!("{err}");
                            }
                        }
                        Err(e) => {
                            log_error!("{e}");
                            let _ = task_uploader
                                .upload_task_error(&task, &self.service, &self.connection, None, None, None, None)
                                .await;
                        }
                    }

                    self.tasks_processed += 1;
                }
            }

            // check if service process terminated if so, terminate service handler
            if let Ok(Some(code)) = service_process.try_wait() {
                log_error!("Service process terminated with status code: {code}");
                is_service_running = false;
            }
        }

        info!("Service client terminated. Start clean up.");
        let _ = service_process.start_kill();
        let _ = self.cleanup_files();
        Ok(())
    }

    #[cfg(test)]
    pub fn get_service(&self) -> Service {
        self.service.clone()
    }

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

    pub fn is_running(&self) -> bool {
        // service should stop running once the task limit is reached.
        if self.task_complete_limit.is_some_and(|v| v == self.tasks_processed) {
            *self.running.lock() = false;
        }

        *self.running.lock()
    }
}
