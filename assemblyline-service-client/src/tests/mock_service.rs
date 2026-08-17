use std::{
    fs::File,
    io::{self, Write},
    path::Path,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use assemblyline_models::{
    datastore::{self, Service},
    messages::{service_api::service_manifest::ServiceManifest, task::Task},
};
use log::{debug, info};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::{
    io::AsyncWriteExt,
    net::unix::pipe::{self, Receiver, Sender},
};

use crate::{
    constants::{TASK_DONE_ERROR, TASK_DONE_SUCCESS},
    service_launcher::ServiceLauncher,
    tests::{create_random_service_result, init},
    types::errors::ServiceClientError,
};

pub struct MockService {
    service: Service,
    task_fifo_path: String,
    done_fifo_path: String,
    service_ready_path: String,
    running: Arc<Mutex<bool>>,
    task_done_success: bool,
    wait_forever: bool,
    service_result: Option<Value>,
}

pub struct DoNothingServiceLauncher {}

impl ServiceLauncher for DoNothingServiceLauncher {
    async fn launch_service(&self) -> Result<Child, ServiceClientError> {
        info!("LAUNCH Mock service...");
        let mut cmd = Command::new("sleep");
        cmd.args(["infinity"]);

        // make sure this process gets terminated when the reference to service_process is dropped.
        cmd.kill_on_drop(true);

        let service_process = cmd.spawn()?;
        Ok(service_process)
    }
}
pub struct TaskFifoPipes {
    pub(crate) task_fifo: Receiver,
    pub(crate) done_fifo: Sender,
}

pub struct MockServiceLauncher {
    pub service: Service,
    pub runtime_prefix: String,
    pub tasking_dir: String,
    pub running: Arc<Mutex<bool>>,
    pub task_done_success: bool,
    pub wait_forever: bool,
    pub service_result: Option<Value>,
}

impl ServiceLauncher for MockServiceLauncher {
    async fn launch_service(&self) -> Result<Child, ServiceClientError> {
        info!("LAUNCH Mock service...");
        let mut cmd = Command::new("sleep");
        cmd.args(["infinity"]);

        // make sure this process gets terminated when the reference to service_process is dropped.
        cmd.kill_on_drop(true);

        let service_process = cmd.spawn()?;

        let mut ms = MockService::create_mock_service(
            self.service.clone(),
            self.runtime_prefix.clone(),
            self.tasking_dir.clone(),
            self.task_done_success,
            self.running.clone(),
            self.wait_forever,
            self.service_result.clone(),
        );
        tokio::spawn(async move {
            let _ = ms.start_service().await.unwrap();
        });

        Ok(service_process)
    }
}

impl MockService {
    pub fn create_mock_service(
        service: Service,
        runtime_prefix: String,
        fifo_dir: String,
        task_done_success: bool,
        service_running: Arc<Mutex<bool>>,
        wait_forever: bool,
        service_result: Option<Value>,
    ) -> Self {
        let task_fifo_path = format!("{}{}_task.fifo", fifo_dir, runtime_prefix);
        let done_fifo_path = format!("{}{}_done.fifo", fifo_dir, runtime_prefix);
        let service_ready_path = format!("{}{}_ready", fifo_dir, runtime_prefix);

        MockService {
            service,
            task_fifo_path,
            done_fifo_path,
            service_ready_path,
            service_result,
            running: service_running,
            task_done_success,
            wait_forever: wait_forever,
        }
    }

    pub async fn setup_fifo(task_fifo_path: &String, done_fifo_path: &String) -> Result<TaskFifoPipes> {
        debug!("Open receiver......");
        let open_receiver = loop {
            let open_receiver = pipe::OpenOptions::new().open_receiver(task_fifo_path);

            if open_receiver.is_ok() {
                break open_receiver;
            }

            debug!("failed to open receiver...");
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
        };

        debug!("Open sender......");
        let open_sender = loop {
            let sender = pipe::OpenOptions::new().open_sender(done_fifo_path);

            if sender.is_ok() {
                break sender;
            } else {
                if let Some(e) = sender.err() {
                    debug!("Error creating sender pipe {:?}", e);
                }

                debug!("Cannot find done sender pipe... waiting for reader");
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;
            }
        };

        Ok(TaskFifoPipes {
            task_fifo: open_receiver?,
            done_fifo: open_sender?,
        })
    }

    pub async fn start_service(&mut self) -> Result<()> {
        // setup listening in fifo
        {
            let mut running = self.running.lock();
            *running = true;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs_f64(3.0)).await;
        let mut fifo_pipes = MockService::setup_fifo(&self.task_fifo_path, &self.done_fifo_path).await?;

        let _ = File::create(&self.service_ready_path)?;
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;

        loop {
            fifo_pipes.task_fifo.readable().await?;

            let mut msg = vec![0; 1024];
            match fifo_pipes.task_fifo.try_read(&mut msg) {
                Ok(n) => {
                    msg.truncate(n);
                    let value = serde_json::from_slice::<Vec<String>>(msg.as_slice())?;

                    let task_dir = value.get(0).unwrap().to_owned();
                    let task_file_path = value.get(1).unwrap().trim().to_string();

                    debug!("Data found in task fifo: {:?}", task_file_path);

                    if n < 1 {
                        debug!("Length of task is 0 continue");
                        tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;
                        continue;
                    }

                    //Get new task file.
                    if Path::new(&task_file_path).exists() {
                        debug!("The task at {:?} exists", task_file_path);
                    } else {
                        return Err(anyhow!("Task file is not at file path {:?}.", task_file_path));
                    }
                    let file = std::fs::File::open(&task_file_path)?;
                    let task: Task = serde_json::from_reader(file)?;

                    if self.wait_forever {
                        loop {
                            if *self.running.lock() {
                                tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;
                            } else {
                                debug!("Terminating service in the middle of a task.");
                                return Ok(());
                            }
                        }
                    }

                    let mut result_json_path = Path::new(&task_dir).to_path_buf();

                    let result_data = if let Some(res) = &self.service_result {
                        serde_json::to_string(res)?
                    } else if self.task_done_success {
                        let service_result = create_random_service_result(
                            task.fileinfo.sha256.clone(),
                            self.service.name.clone(),
                            self.service.version.clone(),
                            None,
                        );

                        serde_json::to_string(&service_result)?
                    } else {
                        let error_result: datastore::error::Error = rand::random();

                        serde_json::to_string(&error_result)?
                    };

                    if self.task_done_success {
                        result_json_path.push(format!("{}_{}_result.json", task.sid, task.fileinfo.sha256));
                    } else {
                        result_json_path.push(format!("{}_{}_error.json", task.sid, task.fileinfo.sha256));
                    }

                    let mut result_file = File::create(&result_json_path)?;
                    result_file.write_all(result_data.as_bytes())?;
                    result_file.flush()?;

                    debug!("Write task success to result fifo");
                    if let Some(path_str) = result_json_path.as_os_str().to_str() {
                        let task_done_type = if self.task_done_success { TASK_DONE_SUCCESS } else { TASK_DONE_ERROR };
                        let val = vec![path_str, task_done_type];
                        let msg = format!("{}\n", serde_json::to_string(&val)?);
                        debug!("Service done fifo message: {}", &msg);
                        fifo_pipes.done_fifo.write_all(msg.as_bytes()).await?;
                        fifo_pipes.done_fifo.flush().await?;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    debug!("Error::WouldBlock reading task fifo pipe. Sleep for 1 second and try again.");
                    // wait a second before trying to read the pipe again.
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }

            if !*self.running.lock() {
                info!("Service is terminating...");
                break;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
        info!("SERVICE ENDED!!");

        Ok(())
    }
}

#[tokio::test]
async fn test_service_launcher() {
    init();
    let sc_running = Arc::new(Mutex::new(true));
    let task_done_success = true;
    let service_manifest: ServiceManifest = rand::random();
    let service = service_manifest.service.clone();
    let tasking_dir = tempfile::tempdir().unwrap();
    let tasking_dir_string = tasking_dir.path().to_str().unwrap().to_string();
    let service_launcher = MockServiceLauncher {
        service,
        runtime_prefix: "test-a".to_string(),
        task_done_success: task_done_success,
        tasking_dir: tasking_dir_string,
        running: sc_running.clone(),
        wait_forever: false,
        service_result: None,
    };

    debug!("Launch mock service");
    let child = service_launcher.launch_service().await.unwrap();
    debug!("Sleep for 5 seconds");
    tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;
    debug!("Try to kill the child");
    *sc_running.lock() = false;
    std::mem::forget(child);

    debug!("Done!");
}
