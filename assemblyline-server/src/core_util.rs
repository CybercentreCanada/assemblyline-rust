use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use assemblyline_models::config::Config;
use assemblyline_filestore::FileStore;
use log::{error, info};

use crate::cachestore::CacheStore;
use crate::common::tagging::{SafelistFile, TagSafelister};
use crate::elastic::Elastic;
use crate::Core;


async fn get_tag_safelist_defaults(config_path: Option<PathBuf>) -> Result<SafelistFile> {
    let config_path = match config_path {
        Some(path) => path,
        None => PathBuf::from("/etc/assemblyline/tag_safelist.yml")
    };

    // Initialize from the default data
    const DEFAULT_FILE_BODY: &str = include_str!("common/tag_safelist.yml");
    let mut tag_safelist_data: SafelistFile = serde_yaml::from_str(DEFAULT_FILE_BODY)?;

    // Load modifiers from the yaml config
    if tokio::fs::try_exists(&config_path).await? {
        let data = tokio::fs::read_to_string(config_path).await?;
        let yml_data: SafelistFile = serde_yaml::from_str(&data)?;
        tag_safelist_data.match_.extend(yml_data.match_);
        tag_safelist_data.regex.extend(yml_data.regex);
    }

    Ok(tag_safelist_data)
}

pub async fn get_tag_safelist_data(config: &Config, datastore: Arc<Elastic>, config_path: Option<PathBuf>) -> Result<SafelistFile> {
    let file_cache = FileStore::open(&config.filestore.cache).await?;
    let cache = CacheStore::new("system".to_owned(), datastore, file_cache)?;

    let tag_safelist_yml = cache.get("tag_safelist_yml").await?;
    let tag_safelist_data = if let Some(tag_safelist_yml) = tag_safelist_yml {
        serde_yaml::from_slice(&tag_safelist_yml)?
    } else {
        get_tag_safelist_defaults(config_path).await?
    };

    Ok(tag_safelist_data)
}


impl Core {
    
    pub async fn get_tag_safelister(&self, config_path: Option<PathBuf>) -> Result<TagSafelister> {
        let tag_safelist_data = get_tag_safelist_data(&self.config, self.datastore.clone(), config_path).await?;

        // if not tag_safelist_data:
        //     raise InvalidSafelist('Could not find any tag_safelist file to load.')

        return TagSafelister::new(self.datastore.clone(), tag_safelist_data).await;
    }

    pub async fn install_activation_handler(&self, component: &str) -> Result<()> {
        info!("Listening for status events on: system.{component}.active");
        let mut change_stream = self.redis_volatile.pubsub_json_listener::<bool>()
            .subscribe(format!("system.{component}.active"))
            .listen().await;

        // setup local copies of some values
        let enabled = self.enabled.clone();
        let component = component.to_owned();
        let active_hash = self.redis_persistant.clone().hashmap::<bool>("system".to_string(), None);
        let component_active_key = format!("{component}.active");

        // initialize our activity status
        match active_hash.get(&component_active_key).await? {
            Some(value) => {
                enabled.set(value);
            }
            None => {
                // Initialize state to be active if not set
                active_hash.set(&component_active_key, &true).await?;
                enabled.set(true);    
            }
        }

        // spawn a watcher that monitors the activity changes
        tokio::spawn(async move {
            while let Some(change) = change_stream.recv().await {
                match change {
                    None => {
                        info!("Refreshing status");
                        enabled.set(match active_hash.get(&component_active_key).await {
                            Ok(value) => value.unwrap_or(true),
                            Err(err) => {
                                error!("Error getting component active flag from redis: {err}");
                                tokio::time::sleep(Duration::from_secs(1)).await;
                                continue
                            },
                        });
                    }
                    Some(status) => {
                        info!("Status change detected: {status}");
                        enabled.set(status);
                    }
                }
            }
        });

        Ok(())
    }
}