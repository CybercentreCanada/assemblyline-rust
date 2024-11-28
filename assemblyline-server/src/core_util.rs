use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use assemblyline_models::config::Config;

use crate::cachestore::CacheStore;
use crate::common::tagging::{SafelistFile, TagSafelister};
use crate::elastic::Elastic;
use crate::filestore::FileStore;
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
}