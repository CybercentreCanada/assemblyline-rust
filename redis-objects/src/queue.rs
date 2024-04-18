use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ErrorTypes;

use super::{RedisObjects, retry_call};

/// A FIFO queue
/// Optionally has a server side time to live
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
                retry_call!(self.store.pool, expire, &self.name, ttl.as_secs() as i64)?;
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
        let mut pipe = redis::pipe();
        for item in data {
            pipe.rpush(&self.name, serde_json::to_vec(item)?);
        }
        retry_call!(method, self.store.pool, pipe, query_async)?;
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

    /// Make a blocking pop call with a timeout
    pub async fn pop_timeout(&self, timeout: Duration) -> Result<Option<T>, ErrorTypes> {
        let response: Option<(String, Vec<u8>)> = retry_call!(self.store.pool, blpop, &self.name, timeout.as_secs_f64())?;

        Ok(match response {
            Some((_, data)) => serde_json::from_slice(&data)?,
            None => None,
        })
    }

    /// Pop as many items as possible up to a certain limit
    pub async fn pop_batch(&self, limit: usize) -> Result<Vec<T>, ErrorTypes> {
        let limit = match NonZeroUsize::new(limit) {
            Some(value) => value,
            None => return Ok(Default::default()),
        };
        let response: Vec<Vec<u8>> = retry_call!(self.store.pool, lpop, &self.name, Some(limit))?;

        let mut out = vec![];
        for data in response {
            out.push(serde_json::from_slice(&data)?);
        }
        Ok(out)
    }

    /// Wait for up to the given timeout for any of the given queues to recieve a value
    pub async fn select(queues: &[&Queue<T>], timeout: Option<Duration>) -> Result<Option<(String, T)>, ErrorTypes> {
        let timeout = timeout.unwrap_or_default().as_secs_f64();
        if queues.is_empty() {
            return Ok(None)
        }

        let store = &queues[0].store;        
        let mut names = vec![];
        for queue in queues {
            names.push(queue.name.as_str())
        }
        let response: Option<(String, Vec<u8>)> = retry_call!(store.pool, blpop, &names, timeout)?;

        Ok(match response {
            Some((name, data)) => Some((name, serde_json::from_slice(&data)?)),
            None => None,
        })
    }
}

/// Work around for inconsistency between ZRANGEBYSCORE and ZREMRANGEBYSCORE
///   (No limit option available or we would just be using that directly)
///
/// args:
///   minimum score to pop
///   maximum score to pop
///   number of elements to skip before popping any
///   max element count to pop
const PQ_DEQUEUE_RANGE_SCRIPT: &str = r#"
local unpack = table.unpack or unpack
local min_score = tonumber(ARGV[1]);
if min_score == nil then min_score = -math.huge end
local max_score = tonumber(ARGV[2]);
if max_score == nil then max_score = math.huge end
local rem_offset = tonumber(ARGV[3]);
local rem_limit = tonumber(ARGV[4]);

local entries = redis.call("zrangebyscore", KEYS[1], -max_score, -min_score, "limit", rem_offset, rem_limit);
if #entries > 0 then redis.call("zrem", KEYS[1], unpack(entries)) end
return entries
"#;

/// The length of prefixes added to the entries in the priority queue
const SORTING_KEY_LEN: usize = 21;

/// A priority queue implemented on a redis sorted set
pub struct PriorityQueue<T: Serialize + DeserializeOwned> {
    name: String,
    store: Arc<RedisObjects>,
    dequeue_range: redis::Script,
    _data: PhantomData<T>,
}


impl<T: Serialize + DeserializeOwned> PriorityQueue<T> {
    pub (crate) fn new(name: String, store: Arc<RedisObjects>) -> Self {
        Self {
            name,
            store,
            dequeue_range: redis::Script::new(PQ_DEQUEUE_RANGE_SCRIPT),
            _data: PhantomData,
        }
    }

// class PriorityQueue(Generic[T]):
//     def __init__(self, name, host=None, port=None, private=False):
//         self.c = get_client(host, port, private)
//         self._deque_range = self.c.register_script(pq_dequeue_range_script)
//         self.name = name

    fn encode(item: &T) -> Result<Vec<u8>, ErrorTypes> {
        let vip = false;
        let vip = if vip { 0 } else { 9 };
        
        let now = chrono::Utc::now().timestamp_micros();
        let data = serde_json::to_string(&item)?;

        // let value = f"{vip}{f'{int(time.time()*1000000):020}'}{json.dumps(data)}"
        Ok(format!("{vip}{now:020}{data}").into_bytes())
    }
    fn decode(data: &[u8]) -> Result<T, ErrorTypes> {
        Ok(serde_json::from_slice(&data[SORTING_KEY_LEN..])?)
    }

    /// Return the number of items within the two priority values (inclusive on both ends)
    pub async fn count(&self, lowest: i64, highest: i64) -> Result<u64, ErrorTypes> {
        Ok(retry_call!(self.store.pool, zcount, &self.name, -highest, -lowest)?)
    }

    /// Remove all the data from this queue
    pub async fn delete(&self) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, del, &self.name)
    }

    /// Get the number of items in the queue
    pub async fn length(&self) -> Result<u64, ErrorTypes> {
        retry_call!(self.store.pool, zcard, &self.name)
    }

    /// Remove items from the front of the queue
    pub async fn pop(&self, num: isize) -> Result<Vec<T>, ErrorTypes> {
        if num <= 0 {
            return Ok(Default::default())
        };
        let items: Vec<(Vec<u8>, i32)> = retry_call!(self.store.pool, zpopmin, &self.name, num)?;
        let mut out = vec![];
        for (data, _priority) in items {
            out.push(Self::decode(&data)?);
        }
        Ok(out)
    }

    /// When only one item is requested, blocking is is possible.
    pub async fn blocking_pop(&self, timeout: Duration, low_priority: bool) -> Result<Option<T>, ErrorTypes> {
        let result: Option<(String, Vec<u8>, i64)> = if low_priority {
            retry_call!(self.store.pool, bzpopmax, &self.name, timeout.as_secs_f64())?
        } else {
            retry_call!(self.store.pool, bzpopmin, &self.name, timeout.as_secs_f64())?
        };
        match result {
            Some(result) => Ok(Some(Self::decode(&result.1)?)),
            None => Ok(None)
        }
    }

//     def blocking_pop(self, timeout=0, low_priority=False):
//         """When only one item is requested, blocking is is possible."""
//         if low_priority:
//             result = retry_call(self.c.bzpopmax, self.name, timeout)
//         else:
//             result = retry_call(self.c.bzpopmin, self.name, timeout)
//         if result:
//             return decode(result[1][SORTING_KEY_LEN:])
//         return None

    /// Dequeue a number of elements, within a specified range of scores.
    /// Limits given are inclusive, can be made exclusive, see redis docs on how to format limits for that.
    /// NOTE: lower/upper limit is negated+swapped in the lua script, no need to do it here
    /// :param lower_limit: The score of all dequeued elements must be higher or equal to this.
    /// :param upper_limit: The score of all dequeued elements must be lower or equal to this.
    /// :param skip: In the range of available items to dequeue skip over this many.
    /// :param num: Maximum number of elements to dequeue.
    pub async fn dequeue_range(&self, lower_limit: Option<i64>, upper_limit: Option<i64>, skip: Option<u32>, num: Option<u32>) -> Result<Vec<T>, ErrorTypes> {
        let skip = skip.unwrap_or(0);
        let num = num.unwrap_or(1);
        let mut call = self.dequeue_range.key(&self.name);
        let call = call.arg(lower_limit).arg(upper_limit).arg(skip).arg(num);
        let results: Vec<Vec<u8>> = retry_call!(method, self.store.pool, call, invoke_async)?;
        return results.iter()
            .map(|row| Self::decode(row))
            .collect()
        // results = retry_call(self._deque_range, keys=[self.name], args=[lower_limit, upper_limit, skip, num])
        // return [decode(res[SORTING_KEY_LEN:]) for res in results]
    }

    /// Place an item into the queue
    pub async fn push(&self, priority: i32, data: &T) -> Result<Vec<u8>, ErrorTypes> {
        let value = Self::encode(data)?;
        if retry_call!(self.store.pool, zadd, &self.name, &value, -priority)? {
            Ok(value)
        } else {
            Err(ErrorTypes::UnknownRedisError)
        }
    }

    /// Given the raw encoding of an item in queue get its position
    pub async fn rank(&self, raw_value: &[u8]) -> Result<Option<u64>, ErrorTypes> {
        retry_call!(self.store.pool, zrank, &self.name, raw_value)
    }

//     def remove(self, raw_value):
//         return retry_call(self.c.zrem, self.name, raw_value)

    /// Pop items from the low priority end of the queue
    pub async fn unpush(&self, num: isize) -> Result<Vec<T>, ErrorTypes> {
        if num <= 0 {
            return Ok(Default::default())
        };
        let items: Vec<(Vec<u8>, i32)> = retry_call!(self.store.pool, zpopmax, &self.name, num)?;
        let mut out = vec![];
        for (data, _priority) in items {
            out.push(Self::decode(&data)?);
        }
        Ok(out)
    }

    /// Pop the first item from any of the given queues within the given timeout
    pub async fn select(queues: &[&PriorityQueue<T>], timeout: Option<Duration>) -> Result<Option<T>, ErrorTypes> {
        if queues.is_empty() {
            return Ok(Default::default())
        }

        let _timeout = timeout.unwrap_or_default().as_secs();
        todo!("Waiting for deadpool-redis package to upgrade to redis-rs 0.24");
        // let names = vec![];
        // for queue in queues {
        //     names.push(queue.name);
        // }
        // let response = retry_call!(queues[0].store.pool, bzpopmin, names, timeout)

    // if not response:
    //     return response

    // return response[0].decode('utf-8'), json.loads(response[1][SORTING_KEY_LEN:])
    }

    /// Utility function for batch reading queue lengths.
    pub async fn all_length(queues: &[&PriorityQueue<T>]) -> Result<Vec<u64>, ErrorTypes> {
        if queues.is_empty() {
            return Ok(Default::default())
        }

        let mut pipe = redis::pipe();
        for que in queues {
            pipe.zcard(&que.name);
        }

        Ok(retry_call!(method, queues[0].store.pool, pipe, query_async)?)
    }

    
}


pub struct MultiQueue<Message: Serialize + DeserializeOwned> {
    store: Arc<RedisObjects>,
    prefix: String,
    _data: PhantomData<Message>,
}

impl<Message: Serialize + DeserializeOwned> MultiQueue<Message> {
    pub(crate) fn new(prefix: String, store: Arc<RedisObjects>) -> Self {
        Self {store, prefix, _data: Default::default()}
    }

    pub async fn delete(&self, name: &str) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, del, self.prefix.clone() + name)
    }

    pub async fn length(&self, name: &str) -> Result<u64, ErrorTypes> {
        retry_call!(self.store.pool, llen, self.prefix.clone() + name)
    }

    pub async fn pop_nonblocking(&self, name: &str) -> Result<Option<Message>, ErrorTypes> {
        let result: Option<String> = retry_call!(self.store.pool, lpop, self.prefix.clone() + name, None)?;
        match result {
            Some(result) => Ok(serde_json::from_str(&result)?),
            None => Ok(None)
        }
    }

    pub async fn pop(&self, name: &str, timeout: Duration) -> Result<Option<Message>, ErrorTypes> {
        let result: Option<(String, String)> = retry_call!(self.store.pool, blpop, self.prefix.clone() + name, timeout.as_secs_f64())?;
        match result {
            Some((_, result)) => Ok(serde_json::from_str(&result)?),
            None => Ok(None),
        }
    }

    pub async fn push(&self, name: &str, message: &Message) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, rpush, self.prefix.clone() + name, serde_json::to_string(message)?)?;
        Ok(())
    }
}