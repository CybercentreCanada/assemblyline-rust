
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::datastore::{File, Submission, User, Error};
use chrono::{Duration, DateTime, Utc};
use elasticsearch::Elasticsearch;
use serde::de::DeserializeOwned;
use serde::{Serialize};

use crate::error::Result;

pub struct Elastic {
    es: Elasticsearch,
    pub file: Collection<File>,
    pub submission: Collection<Submission>,
    pub user: Collection<User>,
    pub error: Collection<Error>,
}

impl Elastic {
    fn connect(url: String) -> Arc<Self> {
        todo!()
    }
}

pub struct Collection<T: Serialize + DeserializeOwned> {
    database: Arc<Elasticsearch>,
    name: String,
    _data: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Collection<T> {

    pub fn new(es: Arc<Elasticsearch>, name: String) -> Self {
        Collection {
            database: es,
            name,
            _data: Default::default()
        }
    }


    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        todo!();
    }

    pub async fn save(&self, key: &str, value: &T) -> Result<()> {
        todo!();
    }

    pub async fn save_json(&self, key: &str, value: &serde_json::Value) -> Result<()> {
        todo!();
    }

}


pub struct CachedCollection<T: Serialize + DeserializeOwned> {
    database: Arc<Elastic>,
    cache_time: Duration,
    name: String,
    cached: HashMap<String, (DateTime<Utc>, Arc<T>)>,
}

impl<T: Serialize + DeserializeOwned> CachedCollection<T> {

    pub fn new(ds: Arc<Elastic>, name: String, expiry: Duration) -> Self {
        Self {
            database: ds,
            name,
            cache_time: expiry,
            cached: Default::default(),
        }
    }


    pub async fn get(&self, key: &str) -> Result<Option<Arc<T>>> {
        todo!();
    }

}
