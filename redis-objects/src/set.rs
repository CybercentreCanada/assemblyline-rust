
use std::i64;
use std::marker::PhantomData;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Duration;

use redis::{cmd, AsyncCommands};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{RedisObjects, ErrorTypes, retry_call};

const DROP_CARD_SCRIPT: &str = r#"
local set_name = KEYS[1]
local key = ARGV[1]

redis.call('srem', set_name, key)
return redis.call('scard', set_name)
"#;

const LIMITED_ADD: &str = r#"
local set_name = KEYS[1]
local key = ARGV[1]
local limit = tonumber(ARGV[2])

if redis.call('scard', set_name) < limit then
    redis.call('sadd', set_name, key)
    return true
end
return false
"#;


pub struct Set<T> {
    name: String,
    store: Arc<RedisObjects>,
    drop_card_script: redis::Script,
    limited_add: redis::Script,
    ttl: Option<i64>,
    last_expire_time: AtomicI64,
    _data: PhantomData<T>
}

impl<T: Serialize + DeserializeOwned> Set<T> {
    pub (crate) fn new(name: String, store: Arc<RedisObjects>, ttl: Option<Duration>) -> Self {
        Self {
            name,
            store,
            drop_card_script: redis::Script::new(DROP_CARD_SCRIPT),
            limited_add: redis::Script::new(LIMITED_ADD),
            ttl: ttl.map(|v| v.as_secs() as i64),
            last_expire_time: AtomicI64::new(i64::MIN),
            _data: PhantomData,
        }
    }

    async fn _conditional_expire(&self) -> Result<(), ErrorTypes> {
        if let Some(ttl) = self.ttl {
            let ctime = chrono::Utc::now().timestamp();
            let last_expire_time: i64 = self.last_expire_time.load(std::sync::atomic::Ordering::Acquire);
            if ctime > last_expire_time + (ttl / 2) {
                retry_call!(self.store.pool, expire, &self.name, ttl)?;
                self.last_expire_time.store(ctime, std::sync::atomic::Ordering::Release);
            }
        }
        Ok(())
    }

    pub async fn add(&self, value: &T) -> Result<bool, ErrorTypes> {
        let data = serde_json::to_vec(&value)?;
        let result = retry_call!(self.store.pool, sadd, &self.name, &data)?;
        self._conditional_expire().await?;
        Ok(result)
    }    
    
    pub async fn add_batch(&self, values: &[T]) -> Result<usize, ErrorTypes> {
        let mut data = vec![];
        for item in values {
            data.push(serde_json::to_vec(&item)?);
        }
        let result = retry_call!(self.store.pool, sadd, &self.name, &data)?;
        self._conditional_expire().await?;
        Ok(result)
    }

    /// Add a single value to the set, but only if that wouldn't make the set grow past a given size.
    pub async fn limited_add(&self, value: &T, size_limit: usize) -> Result<bool, ErrorTypes> {
        let data = serde_json::to_vec(&value)?;
        let result = retry_call!(method, self.store.pool, 
            self.limited_add.key(&self.name).arg(&data).arg(size_limit), invoke_async)?;
        self._conditional_expire().await?;
        Ok(result)
    }

    pub async fn exist(&self, value: &T) -> Result<bool, ErrorTypes> {
        let data = serde_json::to_vec(&value)?;
        retry_call!(self.store.pool, sismember, &self.name, &data)
    }

    pub async fn length(&self) -> Result<u64, ErrorTypes> {
        retry_call!(self.store.pool, scard, &self.name)
    }

    pub async fn members(&self) -> Result<Vec<T>, ErrorTypes> {
        let data: Vec<Vec<u8>> = retry_call!(self.store.pool, smembers, &self.name)?;
        Ok(data.into_iter()
            .map(|v| serde_json::from_slice::<T>(&v))
            .collect::<Result<Vec<T>, _>>()?)
    }

    /// Try to remove a given value from the set and return if any change has been made.
    pub async fn remove(&self, value: &T) -> Result<bool, ErrorTypes> {
        let data = serde_json::to_vec(&value)?;
        retry_call!(self.store.pool, srem, &self.name, &data)
    }

    pub async fn remove_batch(&self, values: &[T]) -> Result<usize, ErrorTypes> {
        let mut data = vec![];
        for item in values {
            data.push(serde_json::to_vec(&item)?);
        }
        retry_call!(self.store.pool, srem, &self.name, &data)
    }

    /// Remove a given value from the set and return the new size of the set.
    pub async fn drop(&self, value: &T) -> Result<usize, ErrorTypes> {
        let data = serde_json::to_vec(&value)?;
        let size: Option<usize> = retry_call!(method, self.store.pool, 
            self.drop_card_script.key(&self.name).arg(&data), invoke_async)?;
        Ok(size.unwrap_or_default())
    }

    /// Remove and return a random item from the set.
    pub async fn random(&self) -> Result<Option<T>, ErrorTypes>{
        let ret_val: Option<Vec<u8>> = retry_call!(self.store.pool, srandmember, &self.name)?;
        match ret_val {
            Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    // pub async fn random(&self, num=None) -> Result<Option<T>, ErrorTypes>{
    //     ret_val = retry_call(self.c.srandmember, self.name, num)
    //     if isinstance(ret_val, list):
    //         return [json.loads(s) for s in ret_val]
    //     else:
    //         return json.loads(ret_val)
    // }

    /// Remove and return a single value from the set.
    pub async fn pop(&self) -> Result<Option<T>, ErrorTypes> {
        let data: Option<Vec<u8>> = retry_call!(self.store.pool, spop, &self.name)?;
        match data {
            Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    /// Remove and return all values from the set.
    pub async fn pop_all(&self) -> Result<Vec<T>, ErrorTypes> {
        let length = self.length().await?;
        let mut command = cmd("SPOP");
        let command = command.arg(&self.name).arg(length);
        let data: Vec<Vec<u8>> = retry_call!(method, self.store.pool, command, query_async)?;
        Ok(data.into_iter()
            .map(|v| serde_json::from_slice::<T>(&v))
            .collect::<Result<Vec<T>, _>>()?)
    }

    /// Remove and drop all values from the set.
    pub async fn delete(&self) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, del, &self.name)
    }
}

