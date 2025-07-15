//! A daemon that cleans up tasks from the service queues when a service is disabled/deleted.
//!
//! When a service is turned off by the orchestrator or deleted by the user, the service task queue needs to be
//! emptied. The status of all the services will be periodically checked and any service that is found to be
//! disabled or deleted for which a service queue exists, the dispatcher will be informed that the task(s)
//! had an error.

use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use assemblyline_models::datastore::Service;
use assemblyline_models::JsonMap;
use chrono::{TimeDelta, Utc};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::constants::{service_queue_name, ServiceStage, SERVICE_QUEUE_PREFIX};
use crate::dispatcher::client::DispatchClient;
use crate::elastic::Elastic;
use crate::{Core, Flag};

mod http;

const DAY: TimeDelta = TimeDelta::days(1);
const TASK_DELETE_CHUNK: u64 = 10000;

pub async fn main(core: Core) -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();
    let plumber = Plumber::new(core, None, None).await?;
    plumber.core.running.install_terminate_handler(false)?;
    plumber.start(&mut tasks).await?;
    while let Some(task) = tasks.join_next().await {
        task?;
    }
    Ok(())
}

pub struct Plumber {
    core: Core,
    datastore: Arc<Elastic>,
    delay: Duration,
    dispatch_client: DispatchClient,
    flush_tasks: Mutex<HashMap<String, ServiceWorker>>
}

struct ServiceWorker {
    stop: Arc<Flag>,
    service_limit: Arc<AtomicU32>,
    task: JoinHandle<()>,
}

impl Plumber {
    pub async fn new(core: Core, delay: Option<Duration>, user: Option<&str>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            datastore: core.datastore.switch_user(user.unwrap_or("plumber")).await?,
            delay: delay.unwrap_or(Duration::from_secs(60)),
            dispatch_client: DispatchClient::new_from_core(&core).await?,
            core,
            flush_tasks: Default::default(),
        }))
    }

    pub async fn start(self: &Arc<Self>, pool: &mut tokio::task::JoinSet<()>) -> Result<()> {
        // Launch the http interface
        let bind_address = crate::config::load_bind_address()?;
        let tls_config = crate::config::TLSConfig::load().await?;
        pool.spawn(http::start(bind_address, tls_config, self.clone()));

        // Start a task cleanup thread
        let this = self.clone();
        pool.spawn(async move {
            while let Err(err) = this.cleanup_old_tasks().await {
                error!("Error in datastore task cleanup: {err}");
            }
        });

        // Start a notification queue cleanup thread
        let this = self.clone();
        pool.spawn(async move {
            while let Err(err) = this.cleanup_notification_queues().await {
                error!("Error in redis notification queue cleanup: {err}");
            }
        });

        // Whatch for service queues that can be managed
        let this = self.clone();
        pool.spawn(async move {
            while let Err(err) = this.service_queue_plumbing().await {
                error!("Error in service queue cleanup: {err}");
            }
        });
        Ok(())
    }

    async fn service_queue_plumbing(self: &Arc<Self>) -> Result<()> {
        info!("Starting service queue plumbing.");
        // Get an initial list of all the service queues
        let mut service_queues: HashMap<String, Option<Service>> = Default::default();
        for queue_name in self.core.redis_volatile.keys(&service_queue_name("*")).await? {
            if let Some(name) = queue_name.strip_prefix(SERVICE_QUEUE_PREFIX) {
                service_queues.insert(name.to_string(), None);
            }
        }

        // get our own reference to the service stages
        let service_stage_hash = self.core.services.get_service_stage_hash().clone();

        while self.core.running.read() {
            // Reset the status of the service queues
            for row in service_queues.values_mut() {
                *row = None;
            }

            // Update the service queue status based on current list of services
            for service in self.datastore.list_all_services().await? {
                service_queues.insert(service.name.to_string(), Some(service));
            }

            for (service_name, service) in service_queues.iter() {
                // For disabled or othewise unavailable services purge the queue
                let current_stage = service_stage_hash.get(service_name).await?.unwrap_or(ServiceStage::Running);
                let disabled = service.as_ref().map(|s|!s.enabled).unwrap_or(true); // either enabled is false or the service info is none
                if disabled || current_stage != ServiceStage::Running {
                    let mut proccessed_tasks = 0;
                    loop {
                        let task = self.dispatch_client.request_work("plumber", service_name, "0", None, false, None).await?;
                        let task = match task { 
                            Some(task) => task,
                            None => break,
                        };

                        use assemblyline_models::datastore::error;

                        let error = error::Error {
                            archive_ts: None,
                            created: Utc::now(),
                            expiry_ts: if task.ttl > 0 { Some(Utc::now() + TimeDelta::days(task.ttl as i64)) } else { None },
                            response: error::Response {
                                message: "The service was disabled while processing this task.".into(),
                                service_name: task.service_name.clone(),
                                service_version: "0".to_string(),
                                service_tool_version: None,
                                service_debug_info: None,
                                status: error::Status::FailNonrecoverable,
                            },
                            sha256: task.fileinfo.sha256.clone(),
                            error_type: error::ErrorTypes::TaskPreempted,
                        };

                        let error_key = error.build_key(None, Some(&task))?;
                        self.dispatch_client.service_failed(task, &error_key, error).await?;
                        proccessed_tasks += 1
                    }
                    if proccessed_tasks > 0 {
                        debug!("plumber processed {proccessed_tasks} from {service_name}");
                    }
                }

                // clear out workers for services that are disabled or have no limit
                if disabled || service.as_ref().map(|s|s.max_queue_length == 0).unwrap_or_default() {
                    if let Some(worker) = self.flush_tasks.lock().remove(service_name) {
                        worker.stop.set(true);
                    }
                }
                
                // For services that are enabled but limited
                else if let Some(service) = service {
                     if service.enabled && service.max_queue_length > 0 {
                        if let Some(worker) = self.flush_tasks.lock().get_mut(service_name.as_str()) {
                            if !worker.task.is_finished() {
                                worker.service_limit.store(service.max_queue_length, std::sync::atomic::Ordering::Relaxed);
                                continue
                            }
                        }

                        let stop = Arc::new(Flag::new(false));
                        let service_limit = Arc::new(AtomicU32::new(service.max_queue_length));
                        self.flush_tasks.lock().insert(service_name.clone(), ServiceWorker {
                            task: tokio::spawn(self.clone().watch_service(service_name.clone(), stop.clone(), service_limit.clone())),
                            stop,
                            service_limit,
                        });
                    }
                }
            }

            // Wait a while before checking status of all services again
            self.core.sleep(self.delay).await;
        }
        Ok(())
    }

    async fn cleanup_notification_queues(&self) -> Result<()> {
        info!("Cleaning up notification queues for old messages...");
        while self.core.is_running() {
            // Finding all possible notification queues
            let keys = self.core.redis_persistant.keys("nq-*").await?;
            if keys.is_empty() {
                info!("There are no queues right now in the system");
            }
            for k in keys {
                info!("Checking for old message in queue: {k}");
                // Peek the first message of the queue
                let queue = self.core.redis_persistant.queue::<JsonMap>(k.to_owned(), None);

                let msg = match queue.peek_next().await {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        info!("There are no messages in the queue");
                        continue
                    },
                    Err(err) => {
                        // if something in the queue isn't a json object pop a value from the queue
                        // and drop it if we don't find json
                        if err.is_serialize_error() {
                            match queue.pop().await {
                                Ok(Some(msg)) => {
                                    // we got json this time, the non json content may have already be consumed
                                    // procede to handle the message like a peek
                                    queue.unpop(&msg).await?;
                                    msg
                                },
                                Ok(None) => {
                                    // we got nothing this time, the non json content may have already be consumed
                                    // this is fine I guess? Just move to next queue
                                    continue
                                }
                                Err(err) if err.is_serialize_error() => {
                                    // Non json content found, log a warning and move to next queue
                                    warn!("Non json content found in notification queue: {k}");
                                    continue
                                }
                                Err(err) => return Err(err.into())
                            }                            
                        } else {
                            return Err(err.into())
                        }
                    }
                };

                // examine the message to see if the queue content is too old
                use serde_json::Value;
                let current_time = Utc::now() - TimeDelta::seconds(self.core.config.core.plumber.notification_queue_max_age as i64);
                let mut task_time: Option<chrono::DateTime<Utc>> = None;
                for key in ["notify_time", "ingest_time"] {
                    if let Some(Value::String(time)) = msg.get(key) {
                        if let Ok(time) = time.parse() {
                            task_time = Some(time);
                            break
                        }
                    }
                }

                let mut submitter = "unknown".to_owned();
                if let Some(Value::Object(section)) = msg.get("submission") {
                    if let Some(Value::Object(params)) = section.get("params") {
                        if let Some(Value::String(name)) = params.get("submitter") {
                            submitter = name.clone();
                        }
                    }
                }

                // If the message too old, cleanup
                if task_time.map(|time| time <= current_time).unwrap_or(true) {
                    let task_time = task_time.map(|time|time.to_string()).unwrap_or("unknown".to_owned());
                    warn!("Messages on queue {k} by {submitter} are older then the maximum queue age ({task_time}), removing queue");
                    queue.delete().await?;
                } else {
                    info!("All messages are recent enough");
                }
            }

            // wait for next run
            self.core.sleep(Duration::from_secs(self.core.config.core.plumber.notification_queue_interval)).await;
        }
        info!("Done cleaning up notification queues");
        Ok(())
    }

    async fn cleanup_old_tasks(&self) -> Result<()> {
        info!("Cleaning up task index for old completed tasks...");
        while self.core.running.read() {
            let deleted = self.datastore.task_cleanup(Some(DAY), Some(TASK_DELETE_CHUNK)).await?;
            if deleted == 0 {
                self.core.sleep(self.delay).await;
            } else {
                info!("Cleaned up {deleted} tasks that were already completed");
            }
        }
        info!("Done cleaning up task index");
        Ok(())
    }

    async fn watch_service(self: Arc<Self>, service_name: String, stop_signal: Arc<Flag>, limit: Arc<AtomicU32>) {
        if let Err(err) = self._watch_service(service_name, stop_signal, limit).await {
            error!("service watch queue crashed with: {err}");
        }
    }

    async fn _watch_service(self: Arc<Self>, service_name: String, stop_signal: Arc<Flag>, limit: Arc<AtomicU32>) -> Result<()> {
        info!("Watching {service_name} service queue...");
        let service_queue = self.core.get_service_queue(&service_name);
        while self.core.is_running() && !stop_signal.read() {
            while service_queue.length().await? > limit.load(std::sync::atomic::Ordering::Relaxed) as u64 {
                let task = self.dispatch_client.request_work("plumber", &service_name, "0", None, false, Some(true)).await?;
                let task = match task {
                    Some(task) => task,
                    None => break
                };

                use assemblyline_models::datastore::error::{Error, Status, ErrorTypes, Response};
                let error = Error {
                    archive_ts: None,
                    created: Utc::now(),
                    expiry_ts: if task.ttl != 0 { Some(Utc::now() + TimeDelta::days(task.ttl as i64)) } else { None },
                    response: Response {
                        message: "Task canceled due to execesive queuing.".into(),
                        service_name: task.service_name.clone(),
                        service_version: "0".to_string(),
                        status: Status::FailNonrecoverable,
                        service_debug_info: None,
                        service_tool_version: None,
                    },
                    sha256: task.fileinfo.sha256.clone(),
                    error_type: ErrorTypes::TaskPreempted,
                };

                let error_key = error.build_key(None, Some(&task))?;
                self.dispatch_client.service_failed(task, &error_key, error).await?;
            }
            self.core.sleep(Duration::from_secs(2)).await;
        }
        info!("Done watching {service_name} service queue");
        Ok(())
    }


}