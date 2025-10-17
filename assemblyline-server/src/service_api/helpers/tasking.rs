use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::config::Config;
use assemblyline_models::datastore::heuristic::Heuristic;
use assemblyline_models::datastore::tagging::{get_tag_information, load_tags_from_object, TagValue};
use assemblyline_models::datastore::Service;
use assemblyline_models::messages::changes::{HeuristicChange, Operation, ServiceChange};
use assemblyline_models::messages::service_heartbeat::Metrics;
use assemblyline_models::messages::task::Task;
use assemblyline_models::types::strings::Keyword;
use assemblyline_models::types::{ExpandingClassification, JsonMap, ServiceName, Sha256};
use assemblyline_filestore::FileStore;
use chrono::{TimeDelta, Utc};
use itertools::Itertools;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use redis_objects::{increment, AutoExportingMetrics, Hashmap, RedisObjects};
use serde_json::{json, Value};
use thiserror::Error;

use crate::service_api::v1::task::models::Result as ApiResult;
use crate::common::heuristics::{HeuristicHandler, InvalidHeuristicException};
use crate::common::odm::value_to_string;
use crate::common::tagging::{tag_safelist_watcher, TagSafelister};
use crate::constants::{ServiceStatus, METRICS_CHANNEL, SERVICE_STATE_HASH};
use crate::dispatcher::client::DispatchClient;
use crate::elastic::bulk::TypedBulkPlan;
use crate::elastic::responses::BulkResult;
use crate::elastic::{create_empty_result_from_key, Elastic};
use crate::identify::Identify;
use crate::service_api::v1::service::RegisterResponse;
use crate::service_api::v1::task::FinishedBody;
use crate::services::ServiceHelper;
use crate::Core;

// {
//     use std::collections::HashMap;
//     use std::sync::{Arc, LazyLock};

//     ;
//     use parking_lot::Mutex;
//     use redis_objects::{AutoExportingMetrics, RedisObjects};

//     use crate::constants::METRICS_CHANNEL;


 
// }


/// A helper class to simplify tasking for privileged services and service-server.
///
/// This tool helps take care of interactions between the filestore,
/// datastore, dispatcher, and any sources of files to be processed.
pub struct TaskingClient {
    config: Arc<Config>,
    redis_volatile: Arc<RedisObjects>,
    redis_metrics: Arc<RedisObjects>,
    identify: Arc<Identify>,
    datastore: Arc<Elastic>,
    filestore: Arc<FileStore>,
    classification_engine: Arc<ClassificationParser>,
    dispatch_client: DispatchClient,
    status_table: Hashmap<(ServiceName, ServiceStatus, f64)>,
    services: ServiceHelper,
    heuristic_handler: HeuristicHandler,
    heuristics: Arc<Mutex<HashMap<String, Heuristic>>>,
    tag_safelister: Arc<Mutex<Arc<TagSafelister>>>,
    metrics_exporters: Mutex<HashMap<ServiceName, AutoExportingMetrics<Metrics>>>
}

impl TaskingClient {
    pub async fn new(core: &Core) -> Result<Self> {
//     def __init__(self, datastore: AssemblylineDatastore = None, filestore: FileStore = None,
//                  config=None, redis=None, redis_persist=None, identify=None, register_only=False):
        // self.config = config or forge.CachedObject(forge.get_config)
        // self.event_sender = EventSender('changes', redis)
        // self.event_listener = None

        // # If we're performing service registration, we only need a connection to the datastore
        // if not register_only:
        // self.dispatch_client = DispatchClient(self.datastore, redis=redis, redis_persist=redis_persist)
        
        // self.reload_heuristics({})
        
        
        let mut event_listener = core.redis_volatile.pubsub_json_listener::<JsonMap>()
            .subscribe("changes.heuristics".to_owned())
            .listen().await;

        // get the initial hearustic data
        let new_heuristics = core.datastore.list_all_heuristics().await?;
        let new_heuristics: HashMap<String, Heuristic> = new_heuristics.into_iter().map(|heur|(heur.heur_id.clone(), heur)).collect();
        let heuristics = Arc::new(Mutex::new(new_heuristics));

        tokio::spawn({
            let datastore = core.datastore.clone();
            let heuristics = heuristics.clone();
            
            async move {
                while Arc::strong_count(&heuristics) > 1 {
                    let message = match event_listener.recv().await {
                        Some(message) => message,
                        None => break,
                    };

                    let mut service_name = None;
                    if let Some(mut message) = message {
                        if let Some(Value::String(name)) = message.remove("service_name") {
                            service_name = Some(name);
                        }
                    };

                    if let Some(service_name) = service_name {
                        let updates = match datastore.list_service_heuristics(&service_name).await {
                            Ok(updates) => updates,
                            Err(err) => {
                                error!("Error loading heuristics: {err}");
                                continue
                            }
                        };
                        let mut heuristics = heuristics.lock();
                        for heur in updates {
                            heuristics.insert(heur.heur_id.clone(), heur);
                        }
                    } else {
                        let new_values = match datastore.list_all_heuristics().await {
                            Ok(updates) => updates,
                            Err(err) => {
                                error!("Error loading heuristics: {err}");
                                continue
                            }
                        };
                        let new_heuristics = new_values.into_iter().map(|heur|(heur.heur_id.clone(), heur)).collect();
                        *heuristics.lock() = new_heuristics;
                    }
                }
            }
        });   

        Ok(Self {
            config: core.config.clone(),
            redis_volatile: core.redis_volatile.clone(),
            redis_metrics: core.redis_metrics.clone(),
            identify: core.identify.clone(),            
            datastore: core.datastore.clone(),
            filestore: core.filestore.clone(),
            classification_engine: core.classification_parser.clone(),
            dispatch_client: DispatchClient::new_from_core(core).await?,
            services: core.services.clone(),
            status_table: core.redis_volatile.hashmap(SERVICE_STATE_HASH.to_owned(), Some(Duration::from_secs(60 * 20))),
            heuristic_handler: HeuristicHandler::new(core.datastore.clone()).await?,
            heuristics,
            tag_safelister: tag_safelist_watcher(core.config.clone(), core.datastore.clone(), None).await?,
            metrics_exporters: Mutex::new(Default::default()),
        })
    }

    pub async fn upload_file(&self, file: &Path, classification: &str, ttl: u32, is_section_image: bool, is_supplementary: bool, expected_sha256: Option<String>) -> Result<()> {
        // Identify the file info of the uploaded file
        let file_info = self.identify.fileinfo(file.to_path_buf(), true, None, None).await?;

        let sha256 = match &file_info.sha256 {
            Some(generated) => {
                if let Some(expected) = expected_sha256 {
                    if generated.to_string() != expected {
                        bail!("Uploaded file does not match expected file hash. [{generated} != {expected}]")
                    }
                };
                generated.clone()
            },
            None => bail!("Expected hash not found"),
        };

        // Validate SHA256 of the uploaded file
        let expiry_ts = if ttl > 0 {
            Some(chrono::Utc::now() + chrono::TimeDelta::days(ttl.into()))
        } else {
            None
        };
        let Value::Object(mut file_info) = serde_json::to_value(&file_info)? else {
            bail!("Unusable file info");
        };
        file_info.insert("is_section_image".to_string(), json!(is_section_image));
        file_info.insert("is_supplementary".to_string(), json!(is_supplementary));

        // Update the datastore with the uploaded file
        let classification = self.classification_engine.normalize_classification(classification)?;
        self.datastore.save_or_freshen_file(
            &sha256,
            file_info,
            expiry_ts,
            classification,
            &self.classification_engine,
        ).await?;

        // Upload file to the filestore (upload already checks if the file exists)
        self.filestore.upload(file, &sha256).await?;
        Ok(())
    }

    pub async fn register_service(&self, mut service_data: JsonMap, log_prefix: &str) -> Result<RegisterResponse, RegisterError> {
        debug!("Registring service: {:?}", service_data.get("name"));
        let mut keep_alive = true;

        // Initialize the classification strings
        if !service_data.contains_key("classification") {
            service_data.insert("classification".to_string(), json!(self.classification_engine.unrestricted()));
        }
        if !service_data.contains_key("default_result_classification") {
            service_data.insert("default_result_classification".to_string(), json!(self.classification_engine.unrestricted()));
        }
        
        // Get heuristics list
        let heuristics = service_data.remove("heuristics");

        // Patch update_channel, registry_type before Service registration object creation
        service_data.entry("update_channel").or_insert(json!(self.config.services.preferred_update_channel));

        // Normalize the docker objects
        let default_registry_type = json!(self.config.services.preferred_registry_type);
        if let Some(Value::Object(docker_config)) = service_data.get_mut("docker_config") {
            fix_docker_config(docker_config, &default_registry_type)?;
        }
        if !service_data.contains_key("privileged") {
            service_data.insert("privileged".to_owned(), json!(self.config.services.prefer_service_privileged));
        }
        if let Some(Value::Object(deps)) = service_data.get_mut("dependencies") {
            for dep in deps.values_mut() {
                if let Some(Value::Object(docker_config)) = dep.get_mut("container") {
                    fix_docker_config(docker_config, &default_registry_type)?;
                }
            }
        }

        // Pop unused registration service_data
        for x in ["file_required", "tool_version"] {
            service_data.remove(x);
        }

        // Create Service registration object
        let mut service: Service = match serde_json::from_value(Value::Object(service_data)) {
            Ok(service) => service,
            Err(err) => {
                return Err(RegisterError::Formatting(format!("Parsing Service: {err:?}")))
            }
        };
        if service.name.is_empty() || service.version.is_empty() {
            return Err(RegisterError::Formatting("Service name and version must be supplied".to_string()));
        }

        // Fix service version, we don't need to see the stable label
        service.version = service.version.replace("stable", "");

        // Save service if it doesn't already exist
        let key = format!("{}_{}", service.name, service.version);
        debug!("Registering service: storing version manifest");
        if !self.datastore.service.exists(&key, None).await? {
            debug!("Registering service: saving {key}");
            self.datastore.service.save(&key, &service, None, None).await?;
            self.datastore.service.commit(None).await?;
            info!("{log_prefix}{} registered", service.name);
            keep_alive = false;
        } else {
            debug!("Registering service: manifest exists, skipping");
        }

        // Save service delta if it doesn't already exist
        debug!("Registering service: setting version");
        if !self.datastore.service_delta.exists(&service.name, None).await? {
            let mut doc = [("version".to_string(), json!(service.version))].into_iter().collect();
            self.datastore.service_delta.save_json(&service.name, &mut doc, None, None).await?;
            self.datastore.service_delta.commit(None).await?;
            info!("{log_prefix}{} version ({}) registered", service.name, service.version);
        } else {
            debug!("Registering service: version already set");
        }

        let mut new_heuristics = vec![];
        if let Some(Value::Array(heuristics)) = heuristics {
            let mut plan = self.datastore.heuristic.get_bulk_plan(None)?;
            for (index, heuristic) in heuristics.into_iter().enumerate() {
                // Set heuristic id to it's position in the list for logging purposes
                let heuristic_id = format!("#{index}");

                async fn load_heuristic(plan: &mut TypedBulkPlan<Heuristic>, this: &TaskingClient, mut heuristic: Value, service_name: &str, ce: Arc<ClassificationParser>) -> anyhow::Result<(), RegisterError> {
                    // Attack_id field is now a list, make it a list if we receive otherwise
                    // attack_id = heuristic.get("attack_id", None)
                    // if isinstance(attack_id, str):
                    //     heuristic["attack_id"] = [attack_id]

                    if let Some(heuristic) = heuristic.as_object_mut() {
                        // Append service name to heuristic ID
                        let original_id = match heuristic.get("heur_id") {
                            Some(id) => if let Some(id) = id.as_str() {
                                id.to_owned()
                            } else {
                                id.to_string()
                            },
                            None => return Err(RegisterError::Formatting("heur_id field is required".to_string())),
                        };
                        let new_id = format!("{}.{original_id}", service_name.to_uppercase());
                        heuristic.insert("heur_id".to_string(), json!(new_id));

                        // Set default classification
                        if !heuristic.contains_key("classification") {
                            heuristic.insert("classification".to_string(), json!(ce.unrestricted()));
                        }
                
                    } else {
                        return Err(RegisterError::Formatting("Heuristic data must be an object".to_string()))
                    }

                    let mut heuristic: Heuristic = match serde_json::from_value(heuristic).context("parse Heuristic object") {
                        Ok(heuristic) => heuristic,
                        Err(err) => return Err(RegisterError::Formatting(format!("Parsing Heuristic: {err:?}")))
                    };

                    let heuristic_id = heuristic.heur_id.clone();
                    if let Some((existing_heuristic_obj, _)) = this.datastore.heuristic.get_if_exists(&heuristic_id, None).await? {
                        // Ensure statistics of heuristic are preserved
                        heuristic.stats = existing_heuristic_obj.stats
                    }
                    plan.add_upsert_operation(&heuristic_id, &heuristic, None)?;
                    Ok(())
                }

                if let Err(e) = load_heuristic(&mut plan, self, heuristic, &service.name, self.classification_engine.clone()).await {
                    let msg = format!("{} has an invalid heuristic ({heuristic_id}): {e:?}", service.name);
                    error!("{log_prefix}{msg}");
                    return Err(RegisterError::BadHeuristic(msg))
                }
            }

            for item in self.datastore.heuristic.bulk(plan).await?.items {
                if item.result != BulkResult::Noop {
                    info!("{log_prefix}{} heuristic {}: {}", service.name, item._id, item.result.to_string().to_uppercase());
                    new_heuristics.push(item._id.clone());
                }
            }

            self.datastore.heuristic.commit(None).await?;

            // Notify components watching for heuristic config changes
            self.redis_volatile.publish_json("heuristics", &HeuristicChange {
                operation: Operation::Modified,
                service_name: service.name.clone()
            }).await?;
        }

        let service_config = match self.datastore.get_service_with_delta(&service.name, None).await? {
            Some(config) => config,
            None => return Err(RegisterError::ServiceRemoved)
        };

        // Notify components watching for service config changes
        self.redis_volatile.publish_json(&("services.".to_owned() + &service.name), &ServiceChange{
            operation: Operation::Added,
            name: service.name
        }).await?;

        Ok(RegisterResponse{
            keep_alive, 
            new_heuristics, 
            service_config
        })
    }

    pub fn get_metrics_factory(&self, service_name: ServiceName) -> AutoExportingMetrics<Metrics> {
        let mut exporters = self.metrics_exporters.lock();
        if let Some(metrics) = exporters.get(&service_name) {
            return metrics.clone()
        }

        let metrics = self.redis_metrics.auto_exporting_metrics(METRICS_CHANNEL.to_owned(), "service".to_owned())
            .counter_name(service_name.to_string())
            .export_zero(false)
            .start();

        exporters.insert(service_name.to_owned(), metrics.clone());
        return metrics;
    }

}

fn fix_docker_config(docker_config: &mut JsonMap, registry_type: &Value) -> serde_json::Result<()> {
    if !docker_config.contains_key("registry_type") {
        docker_config.insert("registry_type".to_owned(), registry_type.clone());
    }
    
    if let Some(Value::Array(env)) = docker_config.get_mut("environment") {
        for row in env {
            if let Some(row) = row.as_object_mut() {
                match row.entry("value") {
                    serde_json::map::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert(Value::String("".to_string()));
                    },
                    serde_json::map::Entry::Occupied(mut occupied_entry) => {
                        occupied_entry.insert(Value::String(value_to_string(occupied_entry.get())?));
                    },
                }
            }
        }
    }
    Ok(())
}



// class TaskingClientException(Exception):
//     pass


// class ServiceMissingException(Exception):
//     pass

#[derive(Debug, Error)]
pub enum RegisterError {
    #[error("Could not complete json coversion: {0}")]
    Formatting(String),
    #[error("{0}")]
    BadHeuristic(String),
    #[error("Service was removed during registration.")]
    ServiceRemoved,
    #[error("Error occurred while registering service: {0}")]
    Other(Box<dyn std::error::Error>)
}

impl RegisterError {
    pub fn is_input_error(&self) -> bool {
        match self {
            RegisterError::Formatting(_) => true,
            RegisterError::BadHeuristic(_) => true,
            RegisterError::ServiceRemoved => false,
            RegisterError::Other(_) => false,
        }
    }
}

impl From<serde_json::Error> for RegisterError {
    fn from(value: serde_json::Error) -> Self {
        Self::Formatting(value.to_string())
    }
} 

impl From<crate::elastic::error::ElasticError> for RegisterError {
    fn from(value: crate::elastic::error::ElasticError) -> Self {
        if value.is_json() {
            Self::Formatting(value.to_string())
        } else {
            Self::Other(Box::new(value))
        }
    }
} 

impl From<redis_objects::ErrorTypes> for RegisterError {
    fn from(value: redis_objects::ErrorTypes) -> Self {
        if value.is_serialize_error() {
            Self::Formatting(value.to_string())
        } else {
            Self::Other(Box::new(value))
        }
    }
} 

impl TaskingClient {
    pub async fn get_task(&self, client_id: &str, service_name: ServiceName, service_version: &str, service_tool_version: Option<&str>, status_expiry: Option<f64>, timeout: Duration) -> Result<(Option<Task>, bool)> {
        let metric_factory = self.get_metrics_factory(service_name);
        let start_time = std::time::Instant::now();

        let status_expiry = match status_expiry {
            Some(expiry) => expiry,
            None => timestamp(timeout),
        };

        let service_data = match self.services.get(service_name) {
            Some(service) => service,
            None => return Err(ServiceMissing.into()),
        };

        // Set the service status to Idle since we will be waiting for a task
        self.status_table.set(client_id, &(service_name, ServiceStatus::Idle, status_expiry)).await?;

        // Getting a new task
        let task = self.dispatch_client.request_work(
            client_id, 
            service_name, 
            service_version, 
            Some(timeout), 
            true, 
            Some(false)
        ).await?;
        let task = match task {
            Some(task) => task,
            None => {
                // We've reached the timeout and no task found in service queue
                debug!("TaskingClient::get_task timeout for {client_id} running {service_name} after {:?}", start_time.elapsed());
                return Ok((None, false))
            }
        };
        debug!("TaskingClient::get_task found task for {client_id} running {service_name} after {:?}", start_time.elapsed());

        // We've got a task to process, consider us busy
        let timeout = timestamp(Duration::from_secs(service_data.timeout as u64));
        self.status_table.set(client_id, &(service_name.to_owned(), ServiceStatus::Running, timeout)).await?;
        increment!(metric_factory, execute);

        // If caching is disabled or ignored we can return the task right away
        if task.ignore_cache || service_data.disable_cache {
            increment!(metric_factory, cache_skipped);
            return Ok((Some(task), false))
        }

        // get the cache key for if a result exists for this task already
        let result_key = assemblyline_models::datastore::Result::help_build_key(
            &task.fileinfo.sha256,
            &service_name,
            service_version,
            false,
            false,
            service_tool_version,
            Some(&task)
        )?;

        // Checking for previous results for this key
        let possible_result = self.datastore.result.get_if_exists(&result_key, None).await?;
        if let Some((mut result, version)) = possible_result {
            increment!(metric_factory, cache_hit);
            if result.result.score != 0 {
                increment!(metric_factory, scored);
            } else {
                increment!(metric_factory, not_scored);
            }

            if task.ttl > 0 {
                if let Some(expiry) = result.expiry_ts {
                    result.expiry_ts = Some(expiry.max(Utc::now() + TimeDelta::days(task.ttl.into())));
                }
            }

            // Create a list of files to freshen
            let mut freshen_hashes = vec![task.fileinfo.sha256.clone()];

            // Test each extracted and supplementary files
            for file_item in result.response.extracted.iter().chain(result.response.supplementary.iter()) {
                if freshen_hashes.contains(&file_item.sha256) {
                    // We've already decided to freshen this file, moving on..
                    continue
                }

                freshen_hashes.push(file_item.sha256.clone());

                // Bail out if file does not exists
                if !self.filestore.exists(&file_item.sha256).await? {
                    info!("We have a cache hit with some related files missing, ignoring it...");
                    increment!(metric_factory, cache_miss);
                    return Ok((Some(task), false))
                }
            }

            // Freshen the files
            for sha256 in freshen_hashes {
                self.datastore.save_or_freshen_file(
                    &sha256, 
                    Default::default(), 
                    result.expiry_ts, 
                    result.classification.as_str().to_owned(),
                    &self.classification_engine
                ).await?;
            }

            self.dispatch_client.service_finished(task, result_key.clone(), result, None, Some(version), vec![]).await?;
            return Ok((None, true))
        }

        // Cache of full results has failed, so
        // Checking for previous empty results for this key
        let empty_key = format!("{result_key}.e");
        match self.datastore.emptyresult.get_if_exists(&empty_key, None).await {
            Ok(Some((_, version))) => {
                increment!(metric_factory, cache_hit);
                increment!(metric_factory, not_scored);
                let result = create_empty_result_from_key(&result_key, self.config.submission.emptyresult_dtl.into(), &self.classification_engine)?;
                self.dispatch_client.service_finished(task, empty_key, result, None, Some(version), vec![]).await?;
                return Ok((None, true))
            },
            Ok(None) => {},
            Err(err) if err.is_json() => {
                warn!("Got poisoned empty result cache record for key {result_key}.e, cleaning up...");
                self.datastore.emptyresult.delete(&empty_key, None).await?;
            },
            Err(err) => {
                return Err(err.into())
            }
        }

        // Both real and empty results found nothing, report this in the metrics
        increment!(metric_factory, cache_miss);

        // No luck with the cache, lets dispatch the task to a client
        return Ok((Some(task), false))
    }

}


#[derive(Debug, Error)]
#[error("The service you're asking task for does not exist, try later")]
pub struct ServiceMissing;

/// Some fields of the task object are new, in order to make the new code
/// compatable with older code those fields are coppied to metadata.
/// Here we will copy them back if they are missing
fn finish_parsing_task(mut data: JsonMap) -> Result<Task> {
    if !data.contains_key("task_id") {
        if let Some(Value::Object(metadata)) = data.get("metadata") {
            if let Some(id) = metadata.get("task_id__") {
                let id: u64 = if let Some(id) = id.as_u64() {
                    id
                } else if let Some(id) = id.as_str() {
                    id.to_string().parse()?
                } else  {
                    bail!("Could not parse id")
                };
                data.insert("task_id".to_string(), json!(id));
            }
        }
    }
    if !data.contains_key("dispatcher") {
        if let Some(Value::Object(metadata)) = data.get("metadata") {
            if let Some(id) = metadata.get("dispatcher__") {
                data.insert("dispatcher".to_string(), id.clone());
            }
        }
    }
    if !data.contains_key("dispatcher_address") {
        if let Some(Value::Object(metadata)) = data.get("metadata") {
            if let Some(id) = metadata.get("dispatcher_address__") {
                data.insert("dispatcher_address".to_string(), id.clone());
            }
        }
    }
    serde_json::from_value(Value::Object(data)).context("Couldn't parse Task")
}

impl TaskingClient {

    pub async fn task_finished(&self, service_task: FinishedBody, client_id: &str, service_name: ServiceName) -> Result<Value> {
        match service_task {
            FinishedBody::Success { task, exec_time, freshen, result } => {
                let task = finish_parsing_task(task)?;
                let missing_files = self._handle_task_result(exec_time, task, result, client_id, service_name, freshen).await.context("_handle_task_result")?;
                if !missing_files.is_empty() {
                    return Ok(json!({"success": false, "missing_files": missing_files}))
                }
                return Ok(json!({"success": true}))
            },
            FinishedBody::Error { task, exec_time, error } => {
                let task = finish_parsing_task(task)?;
                // let error = service_task['error']
                self._handle_task_error(exec_time, task, error, client_id, service_name).await?;
                return Ok(json!({"success": true}))    
            },
            FinishedBody::Other { content } => {
                error!("Malformed task result: {}", serde_json::to_string(&content)?);
                if let Some(Value::Object(task_data)) = content.get("task") {
                    finish_parsing_task(task_data.clone())?;
                }
                let cause = match content.get("result") {
                    Some(result_value) => if let Err(err) = serde_json::from_value::<ApiResult>(result_value.clone()) {
                        err.to_string()
                    } else {
                        "unknown".to_string()
                    },
                    None => {
                        "missing result".to_string()
                    },
                };
                anyhow::bail!("malformed task result: {cause}");
            }
        }
    }

    async fn _handle_task_result(&self, 
        exec_time: u64, 
        task: Task, 
        mut result: ApiResult,
        client_id: &str, 
        service_name: ServiceName,
        freshen: bool
    ) -> Result<Vec<Sha256>> {
        let sid = task.sid;

        let expiry_ts = if task.ttl != 0 {
            Some(Utc::now() + TimeDelta::days(task.ttl.into()))
        } else {
            None
        };

        async fn freshen_file(
            datastore: Arc<Elastic>, 
            cl_engine: Arc<ClassificationParser>, 
            mut file_info: assemblyline_models::datastore::file::File, 
            item: assemblyline_models::datastore::result::File,
            is_supplementary: bool,
        ) -> Result<()> {
            file_info.classification = ExpandingClassification::new(item.classification.into(), &cl_engine)?;
            file_info.archive_ts = None;
            file_info.is_section_image |= item.is_section_image;
            file_info.is_supplementary |= is_supplementary;
            
            let Value::Object(info) = serde_json::to_value(&file_info).context("freshen_file::to_value")? else {
                bail!("Object must serialize to object");
            };
    
            datastore.save_or_freshen_file(
                &item.sha256, 
                info,
                file_info.expiry_ts, 
                file_info.classification.as_str().to_owned(),
                &cl_engine,
            ).await.context("save_or_freshen_file")?;
            Ok(())
        }

        // Check if all files are in the filestore
        if freshen {
            // hashes = list(set([f['sha256'] for f in result['response']['extracted'] + result['response']['supplementary']]))
            let mut hashes: Vec<Sha256> =
                result.response.extracted.iter().chain(result.response.supplementary.iter())
                .map(|x| x.sha256.clone())
                .collect();
            hashes.sort_unstable();
            hashes.dedup();

            let hash_strings: Vec<String> = hashes.iter().map(|h|h.to_string()).collect();
            let hash_strings: Vec<&str> = hash_strings.iter().map(|h|h.as_str()).collect();

            // In the event of a result with duplicate files, let's cache file existence checks with the filestore
            // Pre-compute file existence checks before freshening files
            // let file_exists_check = {h: self.filestore.exists(h) for h in hashes}
            let mut file_exists_check: HashMap<Sha256, bool> = Default::default();
            for h in hashes {
                let hash_str = h.to_string();
                file_exists_check.insert(h, self.filestore.exists(&hash_str).await.context("exists")?);
            }

            let file_infos = self.datastore.file.multiget::<assemblyline_models::datastore::File>(&hash_strings, Some(false), None).await.context("multiget")?;
            let mut missing_files = vec![];

            let mut pool = tokio::task::JoinSet::new();
            let extracted = result.response.extracted.iter().map(|x|(x, false));
            let supplementary = result.response.supplementary.iter().map(|x|(x, true));
            for (file_entry, is_supplementary) in extracted.into_iter().chain(supplementary) {
                if !file_exists_check.get(&file_entry.sha256).copied().unwrap_or_default() {
                    missing_files.push(file_entry.sha256.clone());
                    continue
                }

                let mut file_info = match file_infos.get(&file_entry.sha256.to_string()) {
                    Some(info) => info.clone(),
                    None => {
                        missing_files.push(file_entry.sha256.clone());
                        continue
                    }
                };

                file_info.expiry_ts = expiry_ts;
                pool.spawn(freshen_file(self.datastore.clone(), self.classification_engine.clone(), file_info, file_entry.clone(), is_supplementary));
            }

            while let Some(res) = pool.join_next().await {
                res??;
            }

            if !missing_files.is_empty() {
                missing_files.sort_unstable();
                missing_files.dedup();
                return Ok(missing_files)
            }
        }

        // Add scores to the heuristics, if any section set a heuristic
        let mut total_score = 0;
        let mut service_sections = vec![];
        let mut all_dropped_tags = vec![];
        let mut section_heuristics = HashMap::new();
        for (index, mut section) in result.result.sections.into_iter().enumerate() {
            // let zeroize_on_sig_safe = section.zeroize_on_sig_safe;

            // Parse out and separate valid from invalid tags
            let (mut tags, dropped) = load_tags_from_object(section.tags);
            section.tags = Default::default();
            all_dropped_tags.extend(dropped);

            // if any heristics automatically create tags generate those tags now
            if let Some(mut heuristic) = section.heuristic.take() {
                let heur_id = format!("{}.{}", service_name.to_uppercase(), heuristic.heur_id);
                heuristic.heur_id = crate::service_api::v1::task::models::HeuristicId::Name(heur_id.clone());

                match self.heuristic_handler.service_heuristic_to_result_heuristic(heuristic, self.heuristics.clone()) {
                     Ok((heuristic, new_tags)) => {
                        total_score += heuristic.score;
                        section_heuristics.insert(index, heuristic);
                        for (tag_key, tag_value) in new_tags {
                            match get_tag_information(&tag_key) {
                                Some(tag_key) => {
                                    let entry = tags.entry(tag_key).or_default();
                                    let tag_value = TagValue::from(tag_value);
                                    if !entry.contains(&tag_value) {
                                        entry.push(tag_value)
                                    }
                                },
                                None => {
                                    all_dropped_tags.push((tag_key, tag_value))
                                },
                            }                            
                        }
                    },
                    Err(err) => {
                        if err.downcast_ref::<InvalidHeuristicException>().is_some() {
                            debug!("Dropped heuristic: {heur_id} {err:?}");
                            section.heuristic = None;
                        } else {
                            return Err(err.context("service_heuristic_to_result_heuristic"))
                        }
                    }
                }
            }
            service_sections.push((section, tags));
        }

        // Update the total score of the result
        result.result.score = total_score;

        // Add timestamps for creation, archive and expiry
        result.created = Utc::now();
        result.archive_ts = None;
        result.expiry_ts = expiry_ts;

        // Pop the temporary submission data
        let mut temp_submission_data = JsonMap::new();
        if !result.temp_submission_data.is_empty() {
            let old_submission_data: HashMap<String, _> = task.temporary_submission_data.iter().cloned().map(|row|(row.name, row.value)).collect();
            let mut big_temp_data = vec![];

            for (key, value) in result.temp_submission_data {
                if let Some(old) = old_submission_data.get(&key) {
                    if old == &value { continue }
                }

                let size = match serde_json::to_string(&value) {
                    Ok(data) => data.len(),
                    Err(_) => {
                        big_temp_data.push(format!("{key} is corrupt"));
                        continue
                    },
                };

                if size > self.config.submission.max_temp_data_length as usize {
                    big_temp_data.push(format!("{key} is {size} bytes"));
                } else {
                    temp_submission_data.insert(key, value);
                }
            }

            if !big_temp_data.is_empty() {
                warn!("[{sid}] The following temporary submission keys were ignored because they are bigger then the maximum data size allowed [{}]: {}", 
                      self.config.submission.max_temp_data_length, big_temp_data.join(" | "));
            }
        }

        // Process the tag values
        let mut sections = vec![];
        for (index, (section, tags)) in service_sections.into_iter().enumerate() {
            // if any heuristics have been saved for this section get them
            let mut heuristic = section_heuristics.remove(&index);

            // apply the safelister
            let safelister: Arc<TagSafelister> = self.tag_safelister.lock().clone();
            let (tags, safelisted_tags) = safelister.get_validated_tag_map(tags);

            // Set section score to zero and lower total score if service is set to zeroize score
            // and all tags were safelisted
            if let Some(heuristic) = &mut heuristic {
                if section.zeroize_on_tag_safe && tags.is_empty() && !safelisted_tags.is_empty() {
                    result.result.score -= heuristic.score;
                    heuristic.score = 0;
                }
            }

            // convert the different tag structures to the layouts that the database wants to see
            let formatted_tags = tags.to_tagging().context("to_tagging")?;

            let mut converted_safelist = HashMap::new();
            for (key, value) in safelisted_tags.into_iter() {
                converted_safelist.insert(key.full_path(), value.into_iter().map(|item| Keyword::from(item.to_string())).collect_vec());
            }

            // build a database suitable result section
            sections.push(assemblyline_models::datastore::result::Section {
                auto_collapse: section.auto_collapse,
                body: section.body,
                classification: section.classification,
                body_format: section.body_format,
                body_config: section.body_config,
                depth: section.depth,
                heuristic,
                tags: formatted_tags,
                safelisted_tags: converted_safelist,
                title_text: section.title_text,
                promote_to: section.promote_to,
            })
        }

        let mut tagging_error = vec![];
        if !all_dropped_tags.is_empty() {

            let error_message = all_dropped_tags
                .into_iter()
                .map(|(key, value)| key + "=" + &value)
                .join(" | ");

            warn!("[{sid}] Invalid tag data from {service_name}: {error_message}");
            use assemblyline_models::datastore::error::{Error, Response, ErrorTypes, Status};

            tagging_error.push(Error {
                archive_ts: None,
                created: Utc::now(),
                expiry_ts,
                response: Response { 
                    message: format!("The following tags were rejected: {error_message}").into(), 
                    service_debug_info: result.response.service_debug_info.clone(), 
                    service_name: result.response.service_name.clone(), 
                    service_tool_version: result.response.service_tool_version.clone(), 
                    service_version: result.response.service_version.clone(), 
                    status: Status::FailRecoverable,
                },
                sha256: result.sha256.clone(),
                error_type: ErrorTypes::Exception,
            })
        }

        let result = assemblyline_models::datastore::result::Result{
            archive_ts: None,
            classification: ExpandingClassification::new(result.classification.as_str().to_string(), &self.classification_engine)?,
            created: result.created,
            expiry_ts: result.expiry_ts,
            response: result.response,
            result: assemblyline_models::datastore::result::ResultBody {
                score: result.result.score,
                sections,
            },
            sha256: result.sha256,
            result_type: result.result_type,
            size: result.size,
            drop_file: result.drop_file,
            partial: result.partial,
            from_archive: false,
        };
        let score = result.result.score;
        let result_key = result.build_key(Some(&task))?;
        self.dispatch_client.service_finished(task, result_key, result, Some(temp_submission_data), None, tagging_error).await.context("service_finished")?;

        // Metrics
        let metric_factory = self.get_metrics_factory(service_name);
        if score > 0 {
            increment!(metric_factory, scored);
        } else {
            increment!(metric_factory, not_scored);
        }

        let exec_time = if exec_time > 0 { format!(" in {exec_time}ms") } else { String::new() };
        info!("[{sid}] {client_id} - {service_name} successfully completed task {exec_time}");

        self.status_table.set(client_id, &(service_name.to_owned(), ServiceStatus::Idle, timestamp(Duration::from_secs(5)))).await?;
        Ok(vec![])
    }

    async fn _handle_task_error(
        &self, 
        exec_time: u64, 
        task: Task, 
        mut error: assemblyline_models::datastore::error::Error,
        client_id: &str, 
        service_name: &str,
    ) -> Result<()> {
        info!("[{}] {client_id} - {service_name} failed to complete task in {exec_time}ms", task.sid);

        // Add timestamps for creation, archive and expiry
        error.created = Utc::now();
        error.archive_ts = None;
        if task.ttl != 0 {
            error.expiry_ts = Some(Utc::now() + TimeDelta::days(task.ttl.into()));
        }

        let tool_version_ref = error.response.service_tool_version.as_deref();
        let error_key = error.build_key(tool_version_ref, Some(&task))?;
        let status = error.response.status;
        self.dispatch_client.service_failed(task, &error_key, error).await?;

        // Metrics
        let metric_factory = self.get_metrics_factory(service_name);
        if status.is_recoverable() {
            increment!(metric_factory, fail_recoverable);
        } else {
            increment!(metric_factory, fail_nonrecoverable);
        }

        self.status_table.set(client_id, &(service_name.to_owned(), ServiceStatus::Idle, timestamp(Duration::from_secs(5)))).await?;
        Ok(())
    }
}


pub fn timestamp(offset: Duration) -> f64 {
    ((Utc::now() + offset).timestamp_millis() as f64)/1_000.0
}