use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Serialize};

use super::StructureStore;

pub struct Queue<T: Serialize + DeserializeOwned> {
    name: String,
    store: Arc<StructureStore>,
    _data: PhantomData<T>
}


impl<T: Serialize + DeserializeOwned> Queue<T> {
    pub fn new(name: String, store: Arc<StructureStore>) -> Self {
        Self {
            name,
            store,
            _data: PhantomData,
        }
    }

    pub async fn rank(&self, key: &Vec<u8>) -> Result<Option<usize>> {
        todo!();
    }

    pub async fn push(&self, data: &T) -> Result<Vec<u8>> {
        todo!();
    }
}

pub struct PriorityQueue<T: Serialize + DeserializeOwned> {
    name: String,
    store: Arc<StructureStore>,
    _data: PhantomData<T>
}


impl<T: Serialize + DeserializeOwned> PriorityQueue<T> {
    pub fn new(name: String, store: Arc<StructureStore>) -> Self {
        Self {
            name,
            store,
            _data: PhantomData,
        }
    }

    pub async fn rank(&self, key: &Vec<u8>) -> Result<Option<usize>> {
        todo!();
    }

    pub async fn push(&self, priority: i32, data: &T) -> Result<Vec<u8>> {
        todo!();
    }
}