//! A hash map stored under a single redis key.
use std::{marker::PhantomData, sync::Arc, collections::HashMap, time::Duration};

use redis::AsyncCommands;
use serde::{de::DeserializeOwned, Serialize};

use crate::{RedisObjects, ErrorTypes, retry_call};

const POP_SCRIPT: &str = r#"
local result = redis.call('hget', ARGV[1], ARGV[2])
if result then redis.call('hdel', ARGV[1], ARGV[2]) end
return result
"#;



/// Hashmap opened by `RedisObjects::hashmap`
pub struct Hashmap<T> {
    name: String,
    store: Arc<RedisObjects>,
    pop_script: redis::Script,
    ttl: Option<Duration>,
    last_expire_time: std::sync::Mutex<Option<std::time::Instant>>,
    _data: PhantomData<T>
}

impl<T: Serialize + DeserializeOwned> Hashmap<T> {
    pub (crate) fn new(name: String, store: Arc<RedisObjects>, ttl: Option<Duration>) -> Self {
        Self {
            name,
            store,
            pop_script: redis::Script::new(POP_SCRIPT),
            ttl,
            last_expire_time: std::sync::Mutex::new(None),
            _data: PhantomData,
        }
    }

    // def __init__(self, name: str, host: Union[str, Redis] = None, port: int = None):
    //     self.c = get_client(host, port, False)
    //     self.name = name
    //     self._pop = self.c.register_script(h_pop_script)
    //     self._limited_add = self.c.register_script(_limited_add)
    //     self._conditional_remove = self.c.register_script(_conditional_remove_script)

    async fn conditional_expire(&self) -> Result<(), ErrorTypes> {
        // load the ttl of this object has one set
        if let Some(ttl) = self.ttl {
            let call = {
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
                }
                call
            };

            if call {
                retry_call!(self.store.pool, expire, &self.name, ttl.as_secs() as i64)?;
            }
        }
        Ok(())
    }

    // def __iter__(self):
    //     return HashIterator(self)


    /// Add the (key, value) pair to the hash for new keys.
    /// If a key already exists this operation doesn't add it.
    /// Returns true if key has been added to the table, False otherwise.
    pub async fn add(&self, key: &str, value: &T) -> Result<bool, ErrorTypes> {    
        let data = serde_json::to_vec(value)?;
        let result = retry_call!(self.store.pool, hset_nx, &self.name, &key, &data)?;
        self.conditional_expire().await?;
        Ok(result)
    }

    /// Increment a key within a hash by the given delta
    pub async fn increment(&self, key: &str, increment: i64) -> Result<i64, ErrorTypes> {
        let result = retry_call!(self.store.pool, hincr, &self.name, key, increment)?;
        self.conditional_expire().await?;
        Ok(result)
    }

    // def limited_add(self, key, value, size_limit):
    //     """Add a single value to the set, but only if that wouldn't make the set grow past a given size.

    //     If the hash has hit the size limit returns None
    //     Otherwise, returns the result of hsetnx (same as `add`)
    //     """
    //     self._conditional_expire()
    //     return retry_call(self._limited_add, keys=[self.name], args=[key, json.dumps(value), size_limit])

    /// Test if a given key is defind within this hash
    pub async fn exists(&self, key: &str) -> Result<bool, ErrorTypes> {
        retry_call!(self.store.pool, hexists, &self.name, key)
    }

    /// Read the value stored at the given key
    pub async fn get(&self, key: &str) -> Result<Option<T>, ErrorTypes> {
        let item: Option<Vec<u8>> = retry_call!(self.store.pool, hget, &self.name, key)?;
        Ok(match item {
            Some(data) => Some(serde_json::from_slice(&data)?),
            None => None,
        })
    }

    /// Read the value stored at the given key
    pub async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, ErrorTypes> {
        Ok(retry_call!(self.store.pool, hget, &self.name, key)?)
    }

    /// Load all keys from the hash
    pub async fn keys(&self) -> Result<Vec<String>, ErrorTypes> {
        retry_call!(self.store.pool, hkeys, &self.name)
    }

    /// Read the number of items in the hash
    pub async fn length(&self) -> Result<u64, ErrorTypes> {
        retry_call!(self.store.pool, hlen, &self.name)
    }

    /// Download the entire hash into memory
    pub async fn items(&self) -> Result<HashMap<String, T>, ErrorTypes> {
        let items: Vec<(String, Vec<u8>)> = retry_call!(self.store.pool, hgetall, &self.name)?;
        let mut out = HashMap::new();
        for (key, data) in items {
            out.insert(key, serde_json::from_slice(&data)?);
        }
        Ok(out)
    }

    // def conditional_remove(self, key: str, value) -> bool:
    //     return bool(retry_call(self._conditional_remove, keys=[self.name], args=[key, json.dumps(value)]))

    /// Remove and return the item in the hash if found
    pub async fn pop(&self, key: &str) -> Result<Option<T>, ErrorTypes> {
        let item: Option<Vec<u8>>  = retry_call!(method, self.store.pool, self.pop_script.arg(&self.name).arg(key), invoke_async)?;
        Ok(match item {
            Some(data) => Some(serde_json::from_slice(&data)?),
            None => None,
        })
    }

    /// Unconditionally overwrite the value stored at a given key
    pub async fn set(&self, key: &str, value: &T) -> Result<i64, ErrorTypes> {
        let data = serde_json::to_vec(value)?;
        let result = retry_call!(self.store.pool, hset, &self.name, key, &data)?;
        self.conditional_expire().await?;
        Ok(result)
    }

    // def multi_set(self, data: dict[str, T]):
    //     if any(isinstance(key, bytes) for key in data.keys()):
    //         raise ValueError("Cannot use bytes for hashmap keys")
    //     encoded = {key: json.dumps(value) for key, value in data.items()}
    //     self._conditional_expire()
    //     return retry_call(self.c.hset, self.name, mapping=encoded)

    /// Clear the content of this hash
    pub async fn delete(&self) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, del, &self.name)
    }

}


#[cfg(test)]
mod test {
    use crate::test::redis_connection;
    use crate::ErrorTypes;
    use std::time::Duration;

    #[tokio::test]
    async fn hash() -> Result<(), ErrorTypes> {
        let redis = redis_connection().await;
        let h = redis.hashmap("test-hashmap".to_string(), None);
        h.delete().await?;

        let value_string = "value".to_owned();
        let new_value_string = "new-value".to_owned();

        assert!(h.add("key", &value_string).await?);
        assert!(!h.add("key", &value_string).await?);
        assert!(h.exists("key").await?);
        assert_eq!(h.get("key").await?.unwrap(), value_string);
        assert_eq!(h.set("key", &new_value_string).await?, 0);
        assert!(!h.add("key", &value_string).await?);
        assert_eq!(h.keys().await?, ["key"]);
        assert_eq!(h.length().await?, 1);
        assert_eq!(h.items().await?, [("key".to_owned(), new_value_string.clone())].into_iter().collect());
        assert_eq!(h.pop("key").await?.unwrap(), new_value_string);
        assert_eq!(h.length().await?, 0);
        assert!(h.add("key", &value_string).await?);
        // assert h.conditional_remove("key", "value1") is False
        // assert h.conditional_remove("key", "value") is True
        // assert h.length(), 0

        // // Make sure we can limit the size of a hash table
        // assert h.limited_add("a", 1, 2) == 1
        // assert h.limited_add("a", 1, 2) == 0
        // assert h.length() == 1
        // assert h.limited_add("b", 10, 2) == 1
        // assert h.length() == 2
        // assert h.limited_add("c", 1, 2) is None
        // assert h.length() == 2
        // assert h.pop("a")

        // Can we increment integer values in the hash
        assert_eq!(h.increment("a", 1).await?, 1);
        assert_eq!(h.increment("a", 1).await?, 2);
        assert_eq!(h.increment("a", 10).await?, 12);
        assert_eq!(h.increment("a", -22).await?, -10);
        h.delete().await?;

        // // Load a bunch of items and test iteration
        // data_before = [''.join(_x) for _x in itertools.product('abcde', repeat=5)]
        // data_before = {_x: _x + _x for _x in data_before}
        // h.multi_set(data_before)

        // data_after = {}
        // for key, value in h:
        //     data_after[key] = value
        // assert data_before == data_after
        Ok(())
    }

    #[tokio::test] 
    async fn expiring_hash() -> Result<(), ErrorTypes> {
        let redis = redis_connection().await;
        let eh = redis.hashmap("test-expiring-hashmap".to_string(), Duration::from_secs(1).into());
        eh.delete().await?;
        assert!(eh.add("key", &"value".to_owned()).await?);
        assert_eq!(eh.length().await?, 1);
        tokio::time::sleep(Duration::from_secs_f32(1.1)).await;
        assert_eq!(eh.length().await?, 0);
        Ok(())
    }

}