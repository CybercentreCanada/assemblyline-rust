//! A daemon that keeps an up to date local cache of service information.
//! 
//! Nearly every core component needs service information so this is set up
//! at the level of the Core module.

use std::collections::HashSet;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use assemblyline_models::datastore::Service;
use assemblyline_models::messages::changes::ServiceChange;
use log::error;
use parking_lot::RwLock;
use redis_objects::RedisObjects;
use tokio::sync::mpsc;

use crate::elastic::Elastic;

const REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);

type ChangeChannel = mpsc::Receiver<Option<ServiceChange>>;

/// Interface to service list daemon, should be the only public part of this module
#[derive(Clone)]
pub struct ServiceHelper{
    services: Arc<RwLock<HashMap<String, Arc<Service>>>>
}

impl ServiceHelper {
    pub async fn start(datastore: Arc<Elastic>, redis_volatile: &Arc<RedisObjects>) -> Result<Self> {
        // register for change to services
        let changes: ChangeChannel = redis_volatile.pubsub_json_listener()
            .psubscribe("changes.services.*".to_owned())
            .listen();

        // Initialize the services
        let services = datastore.list_all_services().await?;
        let services = Arc::new(RwLock::new(
            services.into_iter()
                .map(|service|(service.name.clone(), Arc::new(service)))
                .collect()
        ));

        // Launch agent that keeps watch for service updates
        tokio::spawn(service_daemon(datastore, changes, services.clone()));

        // return shared reference
        Ok(Self { services })
    }

    pub fn get(&self, name: &str) -> Option<Arc<Service>> {
        self.services.read().get(name).cloned()
    }

    pub fn list(&self) -> HashMap<String, Arc<Service>> {
        self.services.read().clone()
    }

    pub fn categories(&self) -> HashMap<String, Vec<String>> {
        let mut output = Default::default();
        for service in self.services.read().values() {
            match output.entry(service.name.clone()) {

            }
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
                if !seen_categories.contains_key(&name) {
                    // Add all of the items in this group to the list of
                    // things that we need to evaluate, and mark this
                    // group as having been seen.
                    services.extend(category_services);
                    seen_categories.insert(name)
                }
                continue
            }

            // If it isn't a category, its a service
            found_services.append(name)
        }

        // deduplicate the output
        found_services.sort_unstable();
        found_services.dedup();
        found_services
    }
}


async fn service_daemon(datastore: Arc<Elastic>, mut changes: ChangeChannel, services: Arc<RwLock<HashMap<String, Arc<Service>>>>) {
    while let Err(err) = _service_daemon(datastore.clone(), &mut changes, services.clone()).await {
        error!("Error in service list daemon: {err}");
    }
}

async fn _service_daemon(datastore: Arc<Elastic>, changes: &mut ChangeChannel, services: Arc<RwLock<HashMap<String, Arc<Service>>>>) -> Result<()> {
    // 
    let mut refresh_interval = tokio::time::interval(REFRESH_INTERVAL);

    // load services as long as someone is holding a pointer to the service list
    while Arc::strong_count(&services) > 1 {
        tokio::select!{
            // wait for a change notification
            change = changes.recv() => {
                let change = change.expect("Redis event stream disconnect");
                if let Some(change) = change {
                    // update the service information based on the service specified
                    if change.operation.is_removed() {
                        services.write().remove(&change.name);
                    } else {
                        match datastore.get_service_with_delta(&change.name, None).await? {
                            Some(service) => services.write().insert(change.name, Arc::new(service)),
                            None => services.write().remove(&change.name),
                        };
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
        *services.write() = new_services.into_iter()
            .map(|service|(service.name.clone(), Arc::new(service)))
            .collect();
    }
    Ok(())
}