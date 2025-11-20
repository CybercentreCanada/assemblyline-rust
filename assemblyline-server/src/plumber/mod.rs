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
use assemblyline_models::datastore::apikey::{Apikey, get_apikey_id};
use assemblyline_models::datastore::user::{AclCatagory, UserRole, UserType, load_roles, load_roles_form_acls};
use assemblyline_models::Readable;
use assemblyline_models::datastore::Service;
use assemblyline_models::messages::changes::ServiceChange;
use assemblyline_models::types::{JsonMap, ServiceName};
use chrono::{TimeDelta, Utc};
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::task::JoinHandle;

use crate::constants::{service_queue_name, ServiceStage, SERVICE_QUEUE_PREFIX};
use crate::dispatcher::client::{DispatchCapable, DispatchClient};
use crate::elastic::collection::OperationBatch;
use crate::elastic::Elastic;
use crate::{Core, Flag};

mod http;
#[cfg(test)]
mod tests;

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

pub struct Plumber<Dispatch: Send + Sync> {
    core: Core,
    datastore: Arc<Elastic>,
    delay: Duration,
    dispatch_client: Dispatch,
    flush_tasks: Mutex<HashMap<ServiceName, ServiceWorker>>
}

struct ServiceWorker {
    stop: Arc<Flag>,
    service_limit: Arc<AtomicU32>,
    task: JoinHandle<()>,
}

impl Plumber<DispatchClient> {
    pub async fn new(core: Core, delay: Option<Duration>, user: Option<&str>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            datastore: core.datastore.switch_user(user.unwrap_or("plumber")).await?,
            delay: delay.unwrap_or(Duration::from_secs(60)),
            dispatch_client: DispatchClient::new_from_core(&core).await?,
            core,
            flush_tasks: Default::default(),
        }))
    }
}

#[cfg(test)]
use crate::dispatcher::client::MockDispatchClient;

#[cfg(test)]
impl Plumber<MockDispatchClient> {
    pub async fn new_mocked(core: Core, delay: Option<Duration>, user: Option<&str>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            datastore: core.datastore.switch_user(user.unwrap_or("plumber")).await?,
            delay: delay.unwrap_or(Duration::from_secs(60)),
            dispatch_client: MockDispatchClient::new(core.clone()),
            core,
            flush_tasks: Default::default(),
        }))
    }
}

impl<Dispatch: DispatchCapable + Send + Sync> Plumber<Dispatch> {

    pub async fn start(self: &Arc<Self>, pool: &mut tokio::task::JoinSet<()>) -> Result<()> {
        // Launch the http interface
        let bind_address = crate::config::load_bind_address()?;
        let tls_config = crate::config::TLSConfig::load().await?;
        pool.spawn(http::start(bind_address, tls_config, self.clone()));

        // launch a cleanup task to fix old user api keys
        let this = self.clone();
        pool.spawn(async move {
            if let Err(err) = this.user_apikey_cleanup().await {
                error!("Error in API key cleanup: {err}");
            }
        });

        // launch a cleanup task to update old user profiles
        // let this = self.clone();
        // pool.spawn(async move {
        //     if let Err(err) = this.migrate_user_settings().await {
        //         error!("Error in user migration: {err}");
        //     }
        // });

        // Watch for services that have invalid configurations
        // TODO this can be removed after privileged services are removed from scaler
        let this = self.clone();
        pool.spawn(async move {
            while let Err(err) = this.remove_service_privilege().await {
                error!("Error in service config cleanup: {err}");
            }
        });

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
        let mut service_queues: HashMap<ServiceName, Option<Service>> = Default::default();
        for queue_name in self.core.redis_volatile.keys(&service_queue_name("*")).await? {
            if let Some(name) = queue_name.strip_prefix(SERVICE_QUEUE_PREFIX) {
                service_queues.insert(name.into(), None);
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
                service_queues.insert(service.name, Some(service));
            }

            for (service_name, service) in service_queues.iter() {
                // For disabled or othewise unavailable services purge the queue
                let current_stage = service_stage_hash.get(service_name).await?.unwrap_or(ServiceStage::Running);
                let disabled = service.as_ref().map(|s|!s.enabled).unwrap_or(true); // either enabled is false or the service info is none

                if disabled || current_stage != ServiceStage::Running {
                    let mut proccessed_tasks = 0;
                    loop {
                        let task = self.dispatch_client.request_work("plumber", *service_name, "0", None, false, None).await?;
                        let task = match task { 
                            Some(task) => task,
                            None => break,
                        };

                        use assemblyline_models::datastore::error;
                        let error = error::Error::from_task(&task)
                            .error_type(error::ErrorTypes::TaskPreempted)
                            .message("The service was disabled while processing this task.".into());

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
                        if let Some(worker) = self.flush_tasks.lock().get_mut(service_name) {
                            if !worker.task.is_finished() {
                                worker.service_limit.store(service.max_queue_length, std::sync::atomic::Ordering::Relaxed);
                                continue
                            }
                        }

                        let stop = Arc::new(Flag::new(false));
                        let service_limit = Arc::new(AtomicU32::new(service.max_queue_length));
                        self.flush_tasks.lock().insert(*service_name, ServiceWorker {
                            task: tokio::spawn(self.clone().watch_service(*service_name, stop.clone(), service_limit.clone())),
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

    async fn watch_service(self: Arc<Self>, service_name: ServiceName, stop_signal: Arc<Flag>, limit: Arc<AtomicU32>) {
        if let Err(err) = self._watch_service(service_name, stop_signal, limit).await {
            error!("service watch queue crashed with: {err}");
        }
    }

    async fn _watch_service(self: Arc<Self>, service_name: ServiceName, stop_signal: Arc<Flag>, limit: Arc<AtomicU32>) -> Result<()> {
        info!("Watching {service_name} service queue...");
        let service_queue = self.core.get_service_queue(&service_name);
        while self.core.is_running() && !stop_signal.read() {
            while service_queue.length().await? > limit.load(std::sync::atomic::Ordering::Relaxed) as u64 {
                let task = self.dispatch_client.request_work("plumber", service_name, "0", None, false, Some(true)).await?;
                let task = match task {
                    Some(task) => task,
                    None => break
                };

                use assemblyline_models::datastore::error::{Error, ErrorTypes};
                let error = Error::from_task(&task)
                    .error_type(ErrorTypes::TaskPreempted)
                    .message("Task canceled due to execesive queuing.".into());

                let error_key = error.build_key(None, Some(&task))?;
                self.dispatch_client.service_failed(task, &error_key, error).await?;
            }
            self.core.sleep(Duration::from_secs(2)).await;
        }
        info!("Done watching {service_name} service queue");
        Ok(())
    }


    async fn remove_service_privilege(&self) -> Result<()> {
        // subscribe to service changes
        let mut names_to_check = self.core.services.subscribe();

        // check current service values
        for (_, service) in self.core.services.list_all() {
            self.apply_service_config_change(service).await?
        }

        // wait for service changes and respond to them
        while let Ok(name) = names_to_check.recv().await {
            if let Some(service) = self.core.services.get(name) {
                self.apply_service_config_change(service).await?
            }
        }
        Ok(())
    }

    async fn apply_service_config_change(&self, service: Arc<Service>) -> Result<()> {
        // if not privledged we are done
        if !service.privileged { return Ok(()) }

        // update service in elastic
        let mut operations = OperationBatch::default();
        operations.set("privileged".to_owned(), serde_json::Value::Bool(false));
        self.datastore.service_delta.update(&service.name, operations, None, Some(5)).await?;

        // send notification of service change
        self.core.redis_volatile.publish(&("changes.services.".to_owned() + &service.name), &serde_json::to_vec(&ServiceChange {
            operation: assemblyline_models::messages::changes::Operation::Added,
            name: service.name,
            reason: Some("Disabling privileged service".to_owned())
        })?).await?;
        Ok(())
    }

    async fn user_apikey_cleanup(&self) -> Result<()> {
        let expiry_ts = self.core.config.auth.apikey_max_dtl.map(
            |days| chrono::Utc::now() + chrono::TimeDelta::days(days as i64)
        );

        let mut changes_made = false;

        #[derive(Deserialize, Debug)]
        pub struct LegacyApiKey {
            /// Access Control List for the API key
            pub acl: Vec<AclCatagory>,
            /// BCrypt hash of the password for the apikey
            pub password: String,
            /// List of roles tied to the API key
            #[serde(default)]
            pub roles: Vec<UserRole>,
        }
        
        #[derive(Deserialize, Debug)]
        struct PartialUser {
            uname: String,
            #[serde(default)]
            pub apikeys: HashMap<String, LegacyApiKey>,
            /// Type of user
            #[serde(rename="type", default="assemblyline_models::datastore::user::default_user_types")]
            pub user_types: Vec<UserType>,
            #[serde(default)]
            pub roles: Vec<UserRole>,
        }

        impl Readable for PartialUser {
            fn set_from_archive(&mut self, _from_archive: bool) { }
        }

        let mut search = self.datastore.user.stream_search::<PartialUser>("*", "uname,apikeys,type,roles".to_string(), vec![], None, None, None).await?;        

        while let Some(user) = search.next().await? {
            changes_made = true;
            let uname = user.uname;
            let user_roles = load_roles(&user.user_types, &user.roles);

            for (key, old_apikey) in user.apikeys {
                let key_id = get_apikey_id(&key, &uname);

                let mut roles = vec![];
                if old_apikey.acl == vec![AclCatagory::C] {
                    for r in old_apikey.roles {
                        if user_roles.contains(&r) {
                            roles.push(r);
                        }
                    }
                } else {
                    for r in load_roles_form_acls(&old_apikey.acl, &[]) {
                        if user_roles.contains(&r) {
                            roles.push(r)
                        }
                    }
                }
                let new_apikey = Apikey {
                    password: old_apikey.password,
                    acl: old_apikey.acl,
                    uname: uname.clone(),
                    key_name: key,
                    roles,
                    expiry_ts,
                    creation_date: Utc::now(),
                    last_used: None,
                };
                self.datastore.apikey.save(&key_id, &new_apikey, None, None).await?;
            }
        }

        if changes_made {
            // Commit changes made to indices
            self.datastore.apikey.commit(None).await?;

            // // Update permissions for API keys based on submission customization
            // self.datastore.apikey.update_by_query('roles:"submission_create" AND NOT roles:"submission_customize"',
            //                                       [(self.datastore.apikey.UPDATE_APPEND, 'roles', 'submission_customize')])
        }

        Ok(())
    }

    // async fn migrate_user_settings(&self) -> Result<()> {
    //     let service_list = self.datastore.list_all_services().await?;

    //     // Migrate user settings to the new format
    //     // for doc in self.datastore.user_settings.scan_with_search_after(query={
    //     //     "bool": {
    //     //         "must": {
    //     //             "query_string": {
    //     //                 "query": "*",
    //     //             }
    //     //         }
    //     //     }
    //     // }):

    //     let stream_cursor = self.datastore.user_settings

    //     while let Some(doc) = {
    //         user_settings = doc["_source"]
    //         if user_settings.get('submission_profiles'):
    //             # User settings already migrated, skip
    //             continue

    //         # Create the list of submission profiles
    //         submission_profiles = {
    //             # Grant everyone a default of which all settings from before 4.6 should be transferred
    //             "default": SubmissionProfileParams(user_settings).as_primitives(strip_null=True)
    //         }

    //         profile_error_checks = {}
    //         # Try to apply the original settings to the system-defined profiles
    //         for profile in self.config.submission.profiles:
    //             updates = deepcopy(user_settings)
    //             profile_error_checks.setdefault(profile.name, 0)
    //             validated_profile = profile.params.as_primitives(strip_null=True)

    //             updates.setdefault("services", {})
    //             updates["services"].setdefault("selected", [])
    //             updates["services"].setdefault("excluded", [])

    //             # Append the exclusion list set by the profile
    //             updates['services']['excluded'] = updates['services']['excluded'] + \
    //                 list(validated_profile.get("services", {}).get("excluded", []))

    //             # Ensure the selected services are not in the excluded list
    //             for service in service_list:
    //                 if service['name'] in updates['services']['selected'] and service['name'] in updates['services']['excluded']:
    //                     updates['services']['selected'].remove(service['name'])
    //                     profile_error_checks[profile.name] += 1
    //                 elif service['category'] in updates['services']['selected'] and service['category'] in updates['services']['excluded']:
    //                     updates['services']['selected'].remove(service['category'])
    //                     profile_error_checks[profile.name] += 1


    //             # Check the services parameters
    //             for param_type, list_of_params in profile.restricted_params.items():
    //                 # Check if there are restricted submission parameters
    //                 if param_type == "submission":
    //                     requested_params = (set(list_of_params) & set(updates.keys())) - set({'services', 'service_spec'})
    //                     if requested_params:
    //                         # Track the number of errors for each profile
    //                         profile_error_checks[profile.name] += len(requested_params)
    //                         for param in requested_params:
    //                             # Remove the parameter from the updates
    //                             updates.pop(param, None)

    //                 # Check if there are restricted service parameters
    //                 else:
    //                     service_spec = updates.get('service_spec', {}).get(param_type, {})
    //                     requested_params = set(list_of_params) & set(service_spec)
    //                     if requested_params:
    //                         # Track the number of errors for each profile
    //                         profile_error_checks[profile.name] += len(requested_params)
    //                         for param in requested_params:
    //                             # Remove the parameter from the updates
    //                             service_spec.pop(param, None)

    //                         if not service_spec:
    //                             # Remove the service spec if empty
    //                             updates['service_spec'].pop(param_type, None)
    //             submission_profiles[profile.name] = SubmissionProfileParams(updates).as_primitives(strip_null=True)

    //         # Assign the profile with the least number of errors
    //         user_settings['submission_profiles'] = submission_profiles
    //         user_settings['preferred_submission_profile'] = sorted(profile_error_checks.items(), key=lambda x: x[1])[0][0]

    //         self.datastore.user_settings.save(doc["_id"], user_settings)
    //     }
    // }
}