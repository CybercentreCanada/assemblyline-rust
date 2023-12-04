//! An object oriented wrapper around certain redis objects.

#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
// #![allow(clippy::needless_return)]
// #![allow(clippy::while_let_on_iterator, clippy::collapsible_else_if)]

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub use self::queue::PriorityQueue;
pub use self::queue::Queue;
pub use self::quota::QuotaGuard;
pub use self::events::EventWatcher;
pub use self::hashmap::Hashmap;

mod queue;
mod quota;
mod events;
mod hashmap;

/// Handle for a pool of connections to a redis server.
pub struct RedisObjects {
    pool: deadpool_redis::Pool,
}

impl RedisObjects {
    /// Open a connection pool
    pub fn open(config: redis::ConnectionInfo) -> Result<Arc<Self>, ErrorTypes> {
        let cfg = deadpool_redis::Config::from_connection_info(config);
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        Ok(Arc::new(Self{ pool }))
    }

    /// Open a priority queue under the given key
    pub fn priority_queue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String) -> PriorityQueue<T> {
        PriorityQueue::new(name, self.clone())
    }

    /// Open a FIFO queue under the given key
    pub fn queue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String, ttl: Option<Duration>) -> Queue<T> {
        Queue::new(name, self.clone(), ttl)
    }

    /// Open a hash map under the given key
    pub fn hashmap<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String, ttl: Option<Duration>) -> Hashmap<T> {
        Hashmap::new(name, self.clone(), ttl)
    }

}

#[derive(Debug)]
pub enum ErrorTypes {
    Configuration(deadpool_redis::CreatePoolError),
    Pool(deadpool_redis::PoolError),
    Redis(redis::RedisError),
    Serde(serde_json::Error),
    SelectRequiresQueues,
}

impl From<deadpool_redis::CreatePoolError> for ErrorTypes {
    fn from(value: deadpool_redis::CreatePoolError) -> Self { Self::Configuration(value) }
}

impl From<deadpool_redis::PoolError> for ErrorTypes {
    fn from(value: deadpool_redis::PoolError) -> Self { Self::Pool(value) }
}

impl From<redis::RedisError> for ErrorTypes {
    fn from(value: redis::RedisError) -> Self { Self::Redis(value) }
}

impl From<serde_json::Error> for ErrorTypes {
    fn from(value: serde_json::Error) -> Self { Self::Serde(value) }
}

// macro_rules! retry_call {
//     ($pool:expr, $method:ident, $($args:expr),+) => {
//         {
//             // track our backoff parameters
//             let mut exponent = -7.0;
//             let maximum = 2.0;
//             loop {
//                 // get a (fresh if needed) connection form the pool
//                 let mut con = $pool.get().await?;
//                 // execute the method given with the argments specified
//                 match con.$method($($args),+).await {
//                     Ok(val) => {
//                         if exponent > -7.0 {
//                             log::info!("Reconnected to Redis!")
//                         }
//                         break Ok(val)
//                     },
//                     Err(err) => {
//                         // If the error from redis is something not related to IO let the error propagate
//                         if !err.is_io_error() {
//                             break Result::<_, ErrorTypes>::Err(err.into())
//                         }
            
//                         // For IO errors print a warning and sleep
//                         log::warn!("No connection to Redis, reconnecting... [{}]", err);
//                         tokio::time::sleep(tokio::time::Duration::from_secs_f64(2f64.powf(exponent))).await;
//                         exponent = (exponent + 1.0).min(maximum);                    
//                     },
//                 }
//             }
//         }
//     };
// }

macro_rules! retry_call {
    (handle_output, $call:expr, $exponent:ident, $maximum:ident) => {
        {
            match $call {
                Ok(val) => {
                    if $exponent > -7.0 {
                        log::info!("Reconnected to Redis!")
                    }
                    break Ok(val)
                },
                Err(err) => {
                    // If the error from redis is something not related to IO let the error propagate
                    if !err.is_io_error() {
                        break Result::<_, ErrorTypes>::Err(err.into())
                    }
        
                    // For IO errors print a warning and sleep
                    log::warn!("No connection to Redis, reconnecting... [{}]", err);
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(2f64.powf($exponent))).await;
                    $exponent = ($exponent + 1.0).min($maximum);                    
                },
            }
        }
    };

    ($pool:expr, $method:ident, $($args:expr),+) => {
        {
            // track our backoff parameters
            let mut exponent = -7.0;
            let maximum = 2.0;
            loop {
                // get a (fresh if needed) connection form the pool
                let mut con = $pool.get().await?;
                // execute the method given with the argments specified
                retry_call!(handle_output, con.$method($($args),+).await, exponent, maximum)
            }
        }
    };

    (method, $pool:expr, $obj:expr, $method:ident) => {
        {
            // track our backoff parameters
            let mut exponent = -7.0;
            let maximum = 2.0;
            loop {
                // get a (fresh if needed) connection form the pool
                let mut con = $pool.get().await?;
                // execute the method given with the argments specified
                retry_call!(handle_output, $obj.$method(&mut con).await, exponent, maximum)
            }
        }
    };
}

pub (crate) use retry_call;

#[cfg(test)]
mod test {
    use std::{sync::Arc, num::NonZeroUsize, time::Duration};

    use redis::ConnectionInfo;

    use crate::{RedisObjects, Queue, ErrorTypes};


    async fn redis_connection() -> Arc<RedisObjects> {
        RedisObjects::open(ConnectionInfo{
            addr: redis::ConnectionAddr::Tcp("localhost".to_string(), 6379),
            redis: Default::default(),
        }).unwrap()
    }

    #[tokio::test]
    async fn queue() {
        let redis = redis_connection().await;

        let nq = redis.queue("test-named-queue".to_owned(), None);
        nq.delete().await.unwrap();

        assert!(nq.pop().await.unwrap().is_none());
        assert!(nq.pop_batch(NonZeroUsize::new(100).unwrap()).await.unwrap().is_empty());

        for x in 0..5 {
            nq.push(&x).await.unwrap();
        }

        assert_eq!(nq.content().await.unwrap(), [0, 1, 2, 3, 4]);

        assert_eq!(nq.pop_batch(NonZeroUsize::new(100).unwrap()).await.unwrap(), [0, 1, 2, 3, 4]);

        for x in 0..5 {
            nq.push(&x).await.unwrap();
        }

        assert_eq!(nq.length().await.unwrap(), 5);
        nq.push_batch(&(0..5).collect::<Vec<i32>>()).await.unwrap();
        assert_eq!(nq.length().await.unwrap(), 10);

        assert_eq!(nq.peek_next().await.unwrap(), nq.pop().await.unwrap());
        assert_eq!(nq.peek_next().await.unwrap(), Some(1));
        let v = nq.pop().await.unwrap().unwrap();
        assert_eq!(v, 1);
        assert_eq!(nq.peek_next().await.unwrap().unwrap(), 2);
        nq.unpop(&v).await.unwrap();
        assert_eq!(nq.peek_next().await.unwrap().unwrap(), 1);

        assert_eq!(Queue::select(&[&nq], None).await.unwrap().unwrap(), ("test-named-queue".to_owned(), 1));

        let nq1 = redis.queue("test-named-queue-1".to_owned(), None);
        nq1.delete().await.unwrap();
        let nq2 = redis.queue("test-named-queue-2".to_owned(), None);
        nq2.delete().await.unwrap();

        nq1.push(&1).await.unwrap();
        nq2.push(&2).await.unwrap();

        assert_eq!(Queue::select(&[&nq1, &nq2], None).await.unwrap().unwrap(), ("test-named-queue-1".to_owned(), 1));
        assert_eq!(Queue::select(&[&nq1, &nq2], None).await.unwrap().unwrap(), ("test-named-queue-2".to_owned(), 2));
    }


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
        assert_eq!(h.add("key", &value_string).await?, true);
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
    
    
    // # noinspection PyShadowingNames
    // def test_basic_counters(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.counters import Counters
    //         with Counters('test-counter') as ct:
    //             ct.delete()
    
    //             for x in range(10):
    //                 ct.inc('t1')
    //             for x in range(20):
    //                 ct.inc('t2', value=2)
    //             ct.dec('t1')
    //             ct.dec('t2')
    //             assert sorted(ct.get_queues()) == ['test-counter-t1',
    //                                                'test-counter-t2']
    //             assert ct.get_queues_sizes() == {'test-counter-t1': 9,
    //                                              'test-counter-t2': 39}
    //             ct.reset_queues()
    //             assert ct.get_queues_sizes() == {'test-counter-t1': 0,
    //                                              'test-counter-t2': 0}
    
    
    // # noinspection PyShadowingNames
    // def test_tracked_counters(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.counters import Counters
    //         with Counters('tracked-test-counter', track_counters=True) as ct:
    //             ct.delete()
    
    //             for x in range(10):
    //                 ct.inc('t1')
    //             for x in range(20):
    //                 ct.inc('t2', value=2)
    //             assert ct.tracker.keys() == ['t1', 't2']
    //             ct.dec('t1')
    //             ct.dec('t2')
    //             assert ct.tracker.keys() == []
    //             assert sorted(ct.get_queues()) == ['tracked-test-counter-t1',
    //                                                'tracked-test-counter-t2']
    //             assert ct.get_queues_sizes() == {'tracked-test-counter-t1': 9,
    //                                              'tracked-test-counter-t2': 39}
    //             ct.reset_queues()
    //             assert ct.get_queues_sizes() == {'tracked-test-counter-t1': 0,
    //                                              'tracked-test-counter-t2': 0}
    
    
    // # noinspection PyShadowingNames
    // def test_sets(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.set import Set
    //         with Set('test-set') as s:
    //             s.delete()
    
    //             values = ['a', 'b', 1, 2]
    //             assert s.add(*values) == 4
    //             assert s.length() == 4
    //             for x in s.members():
    //                 assert x in values
    //             assert s.random() in values
    //             assert s.exist(values[2])
    //             s.remove(values[2])
    //             assert not s.exist(values[2])
    //             pop_val = s.pop()
    //             assert pop_val in values
    //             assert not s.exist(pop_val)
    //             assert s.length() == 2
    
    //             assert s.limited_add('dog', 3)
    //             assert not s.limited_add('cat', 3)
    //             assert s.exist('dog')
    //             assert not s.exist('cat')
    //             assert s.length() == 3
    
    //             for pop_val in s.pop_all():
    //                 assert pop_val in values or pop_val in ['cat', 'dog']
    //             assert s.pop() is None
    //             assert s.length() == 0
    
    // # noinspection PyShadowingNames
    
    
    // def test_expiring_sets(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.set import ExpiringSet
    //         with ExpiringSet('test-expiring-set', ttl=1) as es:
    //             es.delete()
    
    //             values = ['a', 'b', 1, 2]
    //             assert es.add(*values) == 4
    //             assert es.length() == 4
    //             assert es.exist(values[2])
    //             for x in es.members():
    //                 assert x in values
    //             time.sleep(1.1)
    //             assert es.length() == 0
    //             assert not es.exist(values[2])
    
    
    // # noinspection PyShadowingNames
    // def test_lock(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.lock import Lock
    
    //         def locked_execution(next_thread=None):
    //             with Lock('test', 10):
    //                 if next_thread:
    //                     next_thread.start()
    //                 time.sleep(2)
    
    //         t2 = Thread(target=locked_execution)
    //         t1 = Thread(target=locked_execution, args=(t2,))
    //         t1.start()
    
    //         time.sleep(1)
    //         assert t1.is_alive()
    //         assert t2.is_alive()
    //         time.sleep(2)
    //         assert not t1.is_alive()
    //         assert t2.is_alive()
    //         time.sleep(2)
    //         assert not t1.is_alive()
    //         assert not t2.is_alive()
    
    
    // # noinspection PyShadowingNames,PyUnusedLocal
    // def test_priority_queue(redis_connection):
    //     from assemblyline.remote.datatypes.queues.priority import PriorityQueue, length, select
    //     with PriorityQueue('test-priority-queue') as pq:
    //         pq.delete()
    
    //         for x in range(10):
    //             pq.push(100, x)
    
    //         a_key = pq.push(101, 'a')
    //         z_key = pq.push(99, 'z')
    //         assert pq.rank(a_key) == 0
    //         assert pq.rank(z_key) == pq.length() - 1
    
    //         assert pq.pop() == 'a'
    //         assert pq.unpush() == 'z'
    //         assert pq.count(100, 100) == 10
    //         assert pq.pop() == 0
    //         assert pq.unpush() == 9
    //         assert pq.length() == 8
    //         assert pq.pop(4) == [1, 2, 3, 4]
    //         assert pq.unpush(3) == [8, 7, 6]
    //         assert pq.length() == 1  # Should be [<100, 5>] at this point
    
    //         for x in range(5):
    //             pq.push(100 + x, x)
    
    //         assert pq.length() == 6
    //         assert pq.dequeue_range(lower_limit=106) == []
    //         assert pq.length() == 6
    //         assert pq.dequeue_range(lower_limit=103) == [4]  # 3 and 4 are both options, 4 has higher score
    //         assert pq.dequeue_range(lower_limit=102, skip=1) == [2]  # 2 and 3 are both options, 3 has higher score, skip it
    //         assert pq.dequeue_range(upper_limit=100, num=10) == [5, 0]  # Take some off the other end
    //         assert pq.length() == 2
    
    //         with PriorityQueue('second-priority-queue') as other:
    //             other.push(100, 'a')
    //             assert length(other, pq) == [1, 2]
    //             select(other, pq)
    //             select(other, pq)
    //             select(other, pq)
    //             assert length(other, pq) == [0, 0]
    
    //         pq.push(50, 'first')
    //         pq.push(-50, 'second')
    
    //         assert pq.dequeue_range(0, 100) == ['first']
    //         assert pq.dequeue_range(-100, 0) == ['second']
    
    
    // # noinspection PyShadowingNames,PyUnusedLocal
    // def test_unique_priority_queue(redis_connection):
    //     from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
    //     with UniquePriorityQueue('test-priority-queue') as pq:
    //         pq.delete()
    
    //         for x in range(10):
    //             pq.push(100, x)
    //         assert pq.length() == 10
    
    //         # Values should be unique, this should have no effect on the length
    //         for x in range(10):
    //             pq.push(100, x)
    //         assert pq.length() == 10
    
    //         pq.push(101, 'a')
    //         pq.push(99, 'z')
    
    //         assert pq.pop() == 'a'
    //         assert pq.unpush() == 'z'
    //         assert pq.count(100, 100) == 10
    //         assert pq.pop() == 0
    //         assert pq.unpush() == 9
    //         assert pq.length() == 8
    //         assert pq.pop(4) == [1, 2, 3, 4]
    //         assert pq.unpush(3) == [8, 7, 6]
    //         assert pq.length() == 1  # Should be [<100, 5>] at this point
    
    //         for x in range(5):
    //             pq.push(100 + x, x)
    
    //         assert pq.length() == 6
    //         assert pq.dequeue_range(lower_limit=106) == []
    //         assert pq.length() == 6
    //         assert pq.dequeue_range(lower_limit=103) == [4]  # 3 and 4 are both options, 4 has higher score
    //         assert pq.dequeue_range(lower_limit=102, skip=1) == [2]  # 2 and 3 are both options, 3 has higher score, skip it
    //         assert sorted(pq.dequeue_range(upper_limit=100, num=10)) == [0, 5]  # Take some off the other end
    //         assert pq.length() == 2
    //         pq.pop(2)
    
    //         pq.push(50, 'first')
    //         pq.push(-50, 'second')
    
    //         assert pq.dequeue_range(0, 100) == ['first']
    //         assert pq.dequeue_range(-100, 0) == ['second']
    
    
    // # noinspection PyShadowingNames
    // def test_named_queue(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.queues.named import NamedQueue, select
    //         with NamedQueue('test-named-queue') as nq:
    //             nq.delete()
    
    //             for x in range(5):
    //                 nq.push(x)
    
    //             assert nq.pop_batch(100) == [0, 1, 2, 3, 4]
    
    //             for x in range(5):
    //                 nq.push(x)
    
    //             assert nq.length() == 5
    //             nq.push(*list(range(5)))
    //             assert nq.length() == 10
    
    //             assert nq.peek_next() == nq.pop()
    //             assert nq.peek_next() == 1
    //             v = nq.pop()
    //             assert v == 1
    //             assert nq.peek_next() == 2
    //             nq.unpop(v)
    //             assert nq.peek_next() == 1
    
    //             assert select(nq) == ('test-named-queue', 1)
    
    //         with NamedQueue('test-named-queue-1') as nq1:
    //             nq1.delete()
    
    //             with NamedQueue('test-named-queue-2') as nq2:
    //                 nq2.delete()
    
    //                 nq1.push(1)
    //                 nq2.push(2)
    
    //                 assert select(nq1, nq2) == ('test-named-queue-1', 1)
    //                 assert select(nq1, nq2) == ('test-named-queue-2', 2)
    
    
    // # noinspection PyShadowingNames
    // def test_multi_queue(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.queues.multi import MultiQueue
    //         mq = MultiQueue()
    //         mq.delete('test-multi-q1')
    //         mq.delete('test-multi-q2')
    
    //         for x in range(5):
    //             mq.push('test-multi-q1', x+1)
    //             mq.push('test-multi-q2', x+6)
    
    //         assert mq.length('test-multi-q1') == 5
    //         assert mq.length('test-multi-q2') == 5
    
    //         assert mq.pop('test-multi-q1') == 1
    //         assert mq.pop('test-multi-q2') == 6
    
    //         assert mq.length('test-multi-q1') == 4
    //         assert mq.length('test-multi-q2') == 4
    
    //         mq.delete('test-multi-q1')
    //         mq.delete('test-multi-q2')
    
    //         assert mq.length('test-multi-q1') == 0
    //         assert mq.length('test-multi-q2') == 0
    
    
    // # noinspection PyShadowingNames
    // def test_comms_queue(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.queues.comms import CommsQueue
    
    //         def publish_messages(message_list):
    //             time.sleep(0.1)
    //             with CommsQueue('test-comms-queue') as cq_p:
    //                 for message in message_list:
    //                     cq_p.publish(message)
    
    //         msg_list = ["bob", 1, {"bob": 1}, [1, 2, 3], None, "Nice!", "stop"]
    //         t = Thread(target=publish_messages, args=(msg_list,))
    //         t.start()
    
    //         with CommsQueue('test-comms-queue') as cq:
    //             x = 0
    //             for msg in cq.listen():
    //                 if msg == "stop":
    //                     break
    
    //                 assert msg == msg_list[x]
    
    //                 x += 1
    
    //         t.join()
    //         assert not t.is_alive()
    
    
    // # noinspection PyShadowingNames
    // def test_user_quota_tracker(redis_connection):
    //     if redis_connection:
    //         from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
    
    //         max_quota = 3
    //         timeout = 2
    //         name = get_random_id()
    //         uqt = UserQuotaTracker('test-quota', timeout=timeout)
    
    //         # First 0 to max_quota items should succeed
    //         for _ in range(max_quota):
    //             assert uqt.begin(name, max_quota) is True
    
    //         # All other items should fail until items timeout
    //         for _ in range(max_quota):
    //             assert uqt.begin(name, max_quota) is False
    
    //         # if you remove and item only one should be able to go in
    //         uqt.end(name)
    //         assert uqt.begin(name, max_quota) is True
    //         assert uqt.begin(name, max_quota) is False
    
    //         # if you wait the timeout, all items can go in
    //         time.sleep(timeout+1)
    //         for _ in range(max_quota):
    //             assert uqt.begin(name, max_quota) is True
    

//     from __future__ import annotations
// import uuid
// import time
// import enum
// import json
// from typing import Any, Optional
// from dataclasses import dataclass, asdict

// from assemblyline.remote.datatypes.events import EventSender, EventWatcher

// import pytest
// from redis import Redis


// def test_exact_event(redis_connection: Redis[Any]):
//     calls: list[dict[str, Any]] = []

//     def _track_call(data: Optional[dict[str, Any]]):
//         if data is not None:
//             calls.append(data)

//     watcher = EventWatcher(redis_connection)
//     try:
//         watcher.register('changes.test', _track_call)
//         watcher.start()
//         sender = EventSender('changes.', redis_connection)
//         start = time.time()

//         while len(calls) < 5:
//             sender.send('test', {'payload': 100})

//             if time.time() - start > 10:
//                 pytest.fail()
//         assert len(calls) >= 5

//         for row in calls:
//             assert row == {'payload': 100}

//     finally:
//         watcher.stop()


// def test_serialized_event(redis_connection: Redis[Any]):
//     import threading
//     started = threading.Event()

//     class Event(enum.IntEnum):
//         ADD = 0
//         REM = 1

//     @dataclass
//     class Message:
//         name: str
//         event: Event

//     def _serialize(message: Message):
//         return json.dumps(asdict(message))

//     def _deserialize(data: str) -> Message:
//         return Message(**json.loads(data))

//     calls: list[Message] = []

//     def _track_call(data: Optional[Message]):
//         if data is not None:
//             calls.append(data)
//         else:
//             started.set()

//     watcher = EventWatcher[Message](redis_connection, deserializer=_deserialize)
//     try:
//         watcher.register('changes.test', _track_call)
//         watcher.skip_first_refresh = False
//         watcher.start()
//         assert started.wait(timeout=5)
//         sender = EventSender[Message]('changes.', redis_connection, serializer=_serialize)
//         start = time.time()

//         while len(calls) < 5:
//             sender.send('test', Message(name='test', event=Event.ADD))

//             if time.time() - start > 10:
//                 pytest.fail()
//         assert len(calls) >= 5

//         expected = Message(name='test', event=Event.ADD)
//         for row in calls:
//             assert row == expected

//     finally:
//         watcher.stop()


// def test_pattern_event(redis_connection: Redis[Any]):
//     calls: list[dict[str, Any]] = []

//     def _track_call(data: Optional[dict[str, Any]]):
//         if data is not None:
//             calls.append(data)

//     watcher = EventWatcher(redis_connection)
//     try:
//         watcher.register('changes.*', _track_call)
//         watcher.start()
//         sender = EventSender('changes.', redis_connection)
//         start = time.time()

//         while len(calls) < 5:
//             sender.send(uuid.uuid4().hex, {'payload': 100})

//             if time.time() - start > 10:
//                 pytest.fail()
//         assert len(calls) >= 5

//         for row in calls:
//             assert row == {'payload': 100}

//     finally:
//         watcher.stop()


}