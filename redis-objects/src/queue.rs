use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ErrorTypes;

use super::{RedisObjects, retry_call};

pub struct Queue<T: Serialize + DeserializeOwned> {
    name: String,
    store: Arc<RedisObjects>,
    ttl: Option<Duration>,
    last_expire_time: std::sync::Mutex<Option<std::time::Instant>>,
    _data: PhantomData<T>
}


impl<T: Serialize + DeserializeOwned> Queue<T> {
    pub (crate) fn new(name: String, store: Arc<RedisObjects>, ttl: Option<Duration>) -> Self {
        Self {
            name,
            store,
            ttl,
            last_expire_time: std::sync::Mutex::new(None),
            _data: PhantomData,
        }
    }

    async fn conditional_expire(&self) -> Result<(), ErrorTypes> {
        // load the ttl of this object has one set
        if let Some(ttl) = self.ttl {
            // the last expire time is behind a mutex so that the queue object is threadsafe
            let mut last_expire_time = self.last_expire_time.lock().unwrap();

            // figure out if its time to update the expiry, wait until we are half way through the
            // ttl to avoid resetting something only milliseconds old
            let call = match *last_expire_time {
                Some(time) => {
                    time.elapsed() > (ttl / 2)
                },
                None => true // always update the expiry if we haven't run it before on this object
            };

            if call {
                // update the time in the mutex then drop it so we aren't holding the lock 
                // while we make the call to the redis server
                *last_expire_time = Some(std::time::Instant::now());
                drop(last_expire_time);
                retry_call!(self.store.pool, expire, &self.name, ttl.as_secs() as usize)?;
            }
        }
        Ok(())
    }

    /// enqueue a single item
    pub async fn push(&self, data: &T) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, rpush, &self.name, serde_json::to_vec(data)?)?;
        self.conditional_expire().await
    }

    /// enqueue a sequence of items
    pub async fn push_batch(&self, data: &[T]) -> Result<(), ErrorTypes> {
        // todo do in pipeline
        // let pipe = redis::pipe();
        // for item in data {
        //     pipe.rpush(&self.name, serde_json::to_vec(item)?);
        // }

        for item in data {
            retry_call!(self.store.pool, rpush, &self.name, serde_json::to_vec(item)?)?;
        }
        self.conditional_expire().await
    }

    /// Put all messages passed back at the head of the FIFO queue.
    pub async fn unpop(&self, data: &T) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, lpush, &self.name, serde_json::to_vec(data)?)?;
        self.conditional_expire().await
    }

    /// Read the number of items in the queue
    pub async fn length(&self) -> Result<usize, ErrorTypes> {
        retry_call!(self.store.pool, llen, &self.name)
    }

    /// load the item that would be returned by the next call to pop
    pub async fn peek_next(&self) -> Result<Option<T>, ErrorTypes> {
        let response: Vec<Vec<u8>> = retry_call!(self.store.pool, lrange, &self.name, 0, 0)?;

        Ok(if response.len() > 0 {
            Some(serde_json::from_slice(&response[0])?)
        } else {
            None
        })
    }

    /// Load the entire content of the queue into memory
    pub async fn content(&self) -> Result<Vec<T>, ErrorTypes> {
        let response: Vec<Vec<u8>> = retry_call!(self.store.pool, lrange, &self.name, 0, -1)?;
        let mut out = vec![];
        for data in response {
            out.push(serde_json::from_slice(&data)?);
        }
        return Ok(out)

    }

    /// Clear all data for this object
    pub async fn delete(&self) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, del, &self.name)
    }

    /// dequeue an item from the front of the queue, returning immediately if empty
    pub async fn pop(&self) -> Result<Option<T>, ErrorTypes> {
        let response: Option<Vec<u8>> = retry_call!(self.store.pool, lpop, &self.name, None)?;

        Ok(match response {
            Some(value) => Some(serde_json::from_slice(&value)?),
            None => None
        })
    }

    pub async fn pop_timeout(&self, timeout: Duration) -> Result<Option<T>, ErrorTypes> {
        let response: Option<(String, Vec<u8>)> = retry_call!(self.store.pool, blpop, &self.name, timeout.as_secs() as usize)?;

        Ok(match response {
            Some((_, data)) => serde_json::from_slice(&data)?,
            None => None,
        })
    }

    pub async fn pop_batch(&self, limit: NonZeroUsize) -> Result<Vec<T>, ErrorTypes> {
        let response: Vec<Vec<u8>> = retry_call!(self.store.pool, lpop, &self.name, Some(limit))?;

        let mut out = vec![];
        for data in response {
            out.push(serde_json::from_slice(&data)?);
        }
        Ok(out)
    }

    pub async fn select(queues: &[&Queue<T>], timeout: Option<Duration>) -> Result<Option<(String, T)>, ErrorTypes> {
        let timeout = timeout.unwrap_or_default().as_secs();
        if queues.is_empty() {
            return Err(ErrorTypes::SelectRequiresQueues)
        }

        let store = &queues[0].store;        
        let mut names = vec![];
        for queue in queues {
            names.push(queue.name.as_str())
        }
        let response: Option<(String, Vec<u8>)> = retry_call!(store.pool, blpop, &names, timeout as usize)?;

        Ok(match response {
            Some((name, data)) => Some((name, serde_json::from_slice(&data)?)),
            None => None,
        })
    }
}



pub struct PriorityQueue<T: Serialize + DeserializeOwned> {
    name: String,
    store: Arc<RedisObjects>,
    _data: PhantomData<T>
}


impl<T: Serialize + DeserializeOwned> PriorityQueue<T> {
    pub fn new(name: String, store: Arc<RedisObjects>) -> Self {
        Self {
            name,
            store,
            _data: PhantomData,
        }
    }

    pub async fn rank(&self, key: &Vec<u8>) -> Result<Option<usize>, ErrorTypes> {
        todo!();
    }

    pub async fn push(&self, priority: i32, data: &T) -> Result<Vec<u8>, ErrorTypes> {
        todo!();
    }
}