//! A daemon that keeps an up to date local cache of service information.
//! 
//! Nearly every core component needs service information so this is set up
//! at the level of the Core module.

use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::config::Services as ServiceConfig;
use assemblyline_models::datastore::submission::SubmissionParams;
use assemblyline_models::datastore::Service;
use assemblyline_models::messages::changes::ServiceChange;
use itertools::Itertools;
use log::{error, warn};
use parking_lot::{RwLock, Mutex};
use redis_objects::RedisObjects;
use tokio::sync::mpsc;

use crate::elastic::Elastic;

const REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);

type ChangeChannel = mpsc::Receiver<Option<ServiceChange>>;

/// Interface to service list daemon, should be the only public part of this module
#[derive(Clone)]
pub struct ServiceHelper{
    classification: Arc<ClassificationParser>,
    inner: Arc<RwLock<ServiceInfo>>,
    regex_cache: RegexCache,
    config: Arc<ServiceConfig>,
}

struct ServiceInfo {
    services: HashMap<String, Arc<Service>>,
    access_cache: HashMap<String, Vec<String>>,
}

impl ServiceHelper {
    pub async fn start(datastore: Arc<Elastic>, redis_volatile: &Arc<RedisObjects>, classification: Arc<ClassificationParser>, config: &ServiceConfig) -> Result<Self> {
        // register for change to services
        let changes: ChangeChannel = redis_volatile.pubsub_json_listener()
            .psubscribe("changes.services.*".to_owned())
            .listen();

        // Initialize the services
        let services = datastore.list_all_services().await?;
        let inner = Arc::new(RwLock::new( ServiceInfo {
            services: services.into_iter()
                        .map(|service|(service.name.clone(), Arc::new(service)))
                        .collect(),
            access_cache: Default::default(),
        }));

        // Launch agent that keeps watch for service updates
        tokio::spawn(service_daemon(datastore, changes, inner.clone()));

        // return shared reference
        Ok(Self { inner, classification, regex_cache: RegexCache::new(), config: Arc::new(config.clone()) })
    }

    pub fn get(&self, name: &str) -> Option<Arc<Service>> {
        self.inner.read().services.get(name).cloned()
    }

    pub fn list(&self) -> HashMap<String, Arc<Service>> {
        self.inner.read().services.clone()
    }

    /// get the list of services in each category
    pub fn categories(&self) -> HashMap<String, Vec<String>> {
        let mut output: HashMap<String, Vec<String>> = Default::default();
        for service in self.inner.read().services.values() {
            output.entry(service.category.clone()).or_default().push(service.name.clone());
        }
        output
    }

    /// Expands the names of service categories found in the list of services.
    ///
    /// Args:
    ///     services (list): List of service category or service names.
    pub fn expand_categories(&self, mut services: Vec<String>) -> Vec<String> {
        // handle null input quickly without having to get the data lock
        if services.is_empty() {
            return vec![]
        }

        // load catagory information from behind lock
        let categories = self.categories();

        // do the actual expansion into this new list
        let mut found_services = vec![];
        let mut seen_categories = HashSet::<String>::new();
        while let Some(name) = services.pop() {

            // If we found a new category mix in it's content
            if let Some(category_services) = categories.get(&name) {
                // Check if we have already proceses this item
                if seen_categories.contains(&name) { continue }

                // Add all of the items in this group to the list of
                // things that we need to evaluate, and mark this
                // group as having been seen.
                services.extend(category_services.iter().cloned());
                seen_categories.insert(name);
                continue
            }

            // If it isn't a category, its a service
            found_services.push(name)
        }

        // deduplicate the output
        found_services.sort_unstable();
        found_services.dedup();
        found_services
    }

    /// Build the expected sequence of 
    pub fn build_schedule(&self, 
        params: &SubmissionParams, 
        file_type: &str, 
        file_depth: u32, //int = 0,
        runtime_excluded: Option<Vec<String>>, // Optional[list[str]] = None,
        submitter_c12n: Option<String> // = Classification.UNRESTRICTED
    ) -> Result<Vec<Vec<Arc<Service>>>>
    {
        // Get the set of all services currently enabled on the system
        let all_services = self.list();

        // Retrieve a list of services that the classfication group is allowed to submit to
        let accessible: Vec<String> = match submitter_c12n {
            None => all_services.keys().cloned().collect(),
            Some(submitter_c12n) => self.get_accessible_services(&submitter_c12n)?,
        };

        // Load the selected and excluded services by category
        let excluded = self.expand_categories(params.services.excluded.clone());
        let runtime_excluded = self.expand_categories(runtime_excluded.unwrap_or_default());
        let mut selected = if params.services.selected.is_empty() {
             all_services.keys().cloned().collect()
        } else {
            self.expand_categories(params.services.selected.clone())
        };

        if !params.services.rescan.is_empty() {
            selected.extend(self.expand_categories(params.services.rescan.clone()))
        }

        // If we enable service safelisting, the Safelist service shouldn't run on extracted files unless:
        //   - We're enforcing use of the Safelist service (we always want to run the Safelist service)
        //   - We're running submission with Deep Scanning
        //   - We want to Ignore Filtering (perform as much unfiltered analysis as possible)
        let use_safelist = self.config.safelist.enabled && !self.config.safelist.enforce_safelist_service;
        let safelist = "Safelist".to_string();
        if selected.contains(&safelist) && file_depth > 0 && use_safelist && !(params.deep_scan || params.ignore_filtering) {
            // Alter schedule to remove Safelist, if scheduled to run
            selected.retain(|item| item != &safelist);
        }

        // Add all selected, accepted, and not rejected services to the schedule
        let mut schedule = vec![vec![]; self.config.stages.len()];
        selected.sort_unstable();
        selected.dedup();

        for name in selected {
            if !accessible.contains(&name) { continue }
            if excluded.contains(&name) { continue }
            if runtime_excluded.contains(&name) { continue }

            let service = match all_services.get(&name) {
                Some(service) => service,
                None => {
                    warn!("Service configuration not found: {name}");
                    continue    
                }
            };

            // let accepted = not service.accepts or re.match(service.accepts, file_type)
            let accepted = if service.accepts.trim().is_empty() {
                true
            } else {
                self.regex_cache.matches(&service.accepts, file_type)?
            };
            // let rejected = bool(service.rejects) and re.match(service.rejects, file_type)
            let rejected = match &service.rejects {
                Some(rejects) if !rejects.trim().is_empty() => self.regex_cache.matches(&rejects, file_type)?,
                _ => false,
            };

            if accepted && !rejected {
                schedule[self.stage_index(&service.stage)].push(service.clone());
            } 
        }

        return Ok(schedule)
    }
    
    fn stage_index(&self, stage: &str) -> usize {
        if let Some((index, _)) = self.config.stages.iter().find_position(|item| *item == stage) {
            index
        } else {
            self.config.stages.len() - 1
        }
    }

    fn get_accessible_services(&self, user_c12n: &str) -> Result<Vec<String>> {
        // try to load services from cache
        let mut info = self.inner.write();
        if let Some(data) = info.access_cache.get(user_c12n) {
            return Ok(data.clone())
        }

        // nothing in cache, recalculate the list of accessable services
        let mut data = vec![];
        for (name, service) in &info.services {
            if self.classification.is_accessible(user_c12n, &service.classification)? {
                data.push(name.clone());
            }
        }
        info.access_cache.insert(user_c12n.to_string(), data.clone());
        Ok(data)
    }
}


async fn service_daemon(datastore: Arc<Elastic>, mut changes: ChangeChannel, info: Arc<RwLock<ServiceInfo>>) {
    while let Err(err) = _service_daemon(datastore.clone(), &mut changes, info.clone()).await {
        error!("Error in service list daemon: {err}");
    }
}

async fn _service_daemon(datastore: Arc<Elastic>, changes: &mut ChangeChannel, info: Arc<RwLock<ServiceInfo>>) -> Result<()> {
    // 
    let mut refresh_interval = tokio::time::interval(REFRESH_INTERVAL);

    // load services as long as someone is holding a pointer to the service list
    while Arc::strong_count(&info) > 1 {
        tokio::select!{
            // wait for a change notification
            change = changes.recv() => {
                let change = change.expect("Redis event stream disconnect");
                if let Some(change) = change {
                    // update the service information based on the service specified
                    if change.operation.is_removed() {
                        info.write().services.remove(&change.name);
                        // don't worry about access_cache on this branch, extra service names will be ignored
                    } else {
                        let service_data = datastore.get_service_with_delta(&change.name, None).await?;
                        let mut info = info.write();
                        match service_data {
                            Some(service) => info.services.insert(change.name, Arc::new(service)),
                            None => info.services.remove(&change.name),
                        };
                        info.access_cache.clear();
                    }
                    continue
                }
                // if the message is none it means we may have missed data fall through to full refresh.
            }

            // Wait for our general refresh of the services, don't do anything, just fall through
            _ = refresh_interval.tick() => {}
        }

        // Refresh service list
        let new_services = datastore.list_all_services().await?;
        let mut info = info.write();
        info.services = new_services.into_iter()
            .map(|service|(service.name.clone(), Arc::new(service)))
            .collect();
        info.access_cache.clear();
    }
    Ok(())
}

struct RegexCache {
    cache: Mutex<HashMap<String, regex::Regex>>,
}

impl Clone for RegexCache {
    fn clone(&self) -> Self {
        Self { cache: Mutex::new(Default::default()) }
    }
}

impl RegexCache {
    fn new() -> Self {
        Self { cache: Mutex::new(Default::default()) }
    }

    fn matches(&self, pattern: &str, target: &str) -> Result<bool> {
        let mut cache = self.cache.lock();
        if let Some(regex) = cache.get(pattern) {
            Ok(regex.is_match(target))
        } else {
            let regex = regex::RegexBuilder::new(pattern).build()?;
            let result = regex.is_match(target);
            cache.insert(pattern.to_string(), regex);
            Ok(result)
        }
    }
}