use anyhow::{bail, Result};
// import re
// from typing import AnyStr, Optional

// from assemblyline.common import forge
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.filestore import FileStore

// DEFAULT_CACHE_LEN = 60 * 60  # 1 hour


static COMPONENT_VALIDATOR: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new("^[a-zA-Z0-9][a-zA-Z0-9_.]*$").unwrap()
});

use std::path::Path;
use std::sync::{Arc, LazyLock};

use assemblyline_filestore::FileStore;

use crate::elastic::Elastic;

#[derive(Clone)]
pub struct CacheStore {
    component: String,
    datastore: Arc<Elastic>,
    filestore: Arc<FileStore>,
}

impl CacheStore {
    pub fn new(component: String, datastore: Arc<Elastic>, filestore: Arc<FileStore>) -> Result<Self> {
        if component.is_empty() {
            bail!("Cannot instantiate a cachestore without providing a component name.")
        }

        if !COMPONENT_VALIDATOR.is_match(&component) {
            bail!("Invalid component name. (Only letters, numbers, underscores and dots allowed)")
        }

        Ok(Self{ component, datastore, filestore })
    }

//     def __enter__(self) -> 'CacheStore':
//         return self

//     def __exit__(self, ex_type, exc_val, exc_tb):
//         self.filestore.close()

//     def save(self, cache_key: str, data: AnyStr, ttl=DEFAULT_CACHE_LEN, force=False):
//         if not COMPONENT_VALIDATOR.match(cache_key):
//             raise ValueError("Invalid cache_key for cache item. "
//                              "(Only letters, numbers, underscores and dots allowed)")

//         new_key = f"{self.component}_{cache_key}" if self.component else cache_key

//         self.datastore.cached_file.save(new_key, {'expiry_ts': now_as_iso(ttl), 'component': self.component})
//         self.filestore.put(new_key, data, force=force)

//     def upload(self, cache_key: str, path: str, ttl=DEFAULT_CACHE_LEN):
//         if not COMPONENT_VALIDATOR.match(cache_key):
//             raise ValueError("Invalid cache_key for cache item. "
//                              "(Only letters, numbers, underscores and dots allowed)")

//         new_key = f"{self.component}_{cache_key}" if self.component else cache_key

//         self.datastore.cached_file.save(new_key, {'expiry_ts': now_as_iso(ttl), 'component': self.component})
//         self.filestore.upload(new_key, path, force=True)

//     def touch(self, cache_key: str, ttl=DEFAULT_CACHE_LEN):
//         if not COMPONENT_VALIDATOR.match(cache_key):
//             raise ValueError("Invalid cache_key for cache item. "
//                              "(Only letters, numbers, underscores and dots allowed)")
//         if not self.exists(cache_key):
//             raise KeyError(cache_key)

//         new_key = f"{self.component}_{cache_key}" if self.component else cache_key
//         self.datastore.cached_file.save(new_key, {'expiry_ts': now_as_iso(ttl), 'component': self.component})

    pub async fn get(&self, cache_key: &str) -> Result<Option<Vec<u8>>> {
        let new_key = format!("{}_{cache_key}", self.component);
        self.filestore.get(&new_key).await
    }

    pub async fn download(&self, cache_key: &str, path: &Path) -> Result<()> {
        let new_key = format!("{}_{cache_key}", self.component);
        self.filestore.download(&new_key, path).await
    }

//     def exists(self, cache_key: str) -> list:
//         new_key = f"{self.component}_{cache_key}" if self.component else cache_key
//         return self.filestore.exists(new_key)

//     def delete(self, cache_key: str, db_delete=True):
//         new_key = f"{self.component}_{cache_key}" if self.component else cache_key

//         self.filestore.delete(new_key)
//         if db_delete:
//             self.datastore.cached_file.delete(new_key)
}