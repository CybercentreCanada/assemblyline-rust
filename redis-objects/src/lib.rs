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

use log::debug;
use queue::MultiQueue;
use quota::UserQuotaTracker;
use redis::AsyncCommands;
pub use redis::Msg;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

pub use self::queue::PriorityQueue;
pub use self::queue::Queue;
pub use self::quota::QuotaGuard;
pub use self::hashmap::Hashmap;
pub use self::counters::{AutoExportingMetrics, AutoExportingMetricsBuilder, MetricMessage};
pub use self::pubsub::{JsonListenerBuilder, ListenerBuilder, Publisher};
pub use self::set::Set;

pub mod queue;
pub mod quota;
pub mod hashmap;
pub mod counters;
pub mod pubsub;
pub mod set;

/// Handle for a pool of connections to a redis server.
pub struct RedisObjects {
    pool: deadpool_redis::Pool,
    client: redis::Client,
}

impl RedisObjects {
    /// Open given more limited connection info
    pub fn open_host(host: &str, port: u16, db: i64) -> Result<Arc<Self>, ErrorTypes> {
        Self::open(redis::ConnectionInfo{
            addr: redis::ConnectionAddr::Tcp(host.to_string(), port),
            redis: redis::RedisConnectionInfo {
                db,
                ..Default::default()
            },
        })
    }
   
    /// Open a connection pool
    pub fn open(config: redis::ConnectionInfo) -> Result<Arc<Self>, ErrorTypes> {
        debug!("Create redis connection pool.");
        let cfg = deadpool_redis::Config::from_connection_info(config.clone());
        let pool = cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        let client = redis::Client::open(config)?;
        Ok(Arc::new(Self{ 
            pool,
            client,
        }))
    }

    /// Open a priority queue under the given key
    pub fn priority_queue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String) -> PriorityQueue<T> {
        PriorityQueue::new(name, self.clone())
    }

    /// Open a FIFO queue under the given key
    pub fn queue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String, ttl: Option<Duration>) -> Queue<T> {
        Queue::new(name, self.clone(), ttl)
    }

    /// an object that represents a set of queues with a common prefix
    pub fn multiqueue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, prefix: String) -> MultiQueue<T> {
        MultiQueue::new(prefix, self.clone())
    }

    /// Open a hash map under the given key
    pub fn hashmap<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String, ttl: Option<Duration>) -> Hashmap<T> {
        Hashmap::new(name, self.clone(), ttl)
    }

    /// Create a sink to publish messages to a named channel
    pub fn publisher(self: &Arc<Self>, channel: String) -> Publisher {
        Publisher::new(self.clone(), channel)
    }

    /// Write a message directly to the channel given
    pub async fn publish(&self, channel: &str, data: &[u8]) -> Result<u32, ErrorTypes> {
        retry_call!(self.pool, publish, channel, data)
    }

    pub async fn publish_json<T: Serialize>(&self, channel: &str, value: &T) -> Result<u32, ErrorTypes> {
        self.publish(channel, &serde_json::to_vec(value)?).await
    }

    /// Start building a metrics exporter
    pub fn auto_exporting_metrics<T: MetricMessage>(self: &Arc<Self>, name: String, counter_type: String) -> AutoExportingMetricsBuilder<T> {
        AutoExportingMetricsBuilder::new(self.clone(), name, counter_type)
    }

    pub fn pubsub_json_listener<T: DeserializeOwned + Send + 'static>(self: &Arc<Self>) -> JsonListenerBuilder<T> {
        JsonListenerBuilder::new(self.clone())
    }

    pub fn subscribe_json<T: DeserializeOwned + Send + 'static>(self: &Arc<Self>, channel: String) -> mpsc::Receiver<Option<T>> {
        self.pubsub_json_listener()
            .subscribe(channel)
            .listen()
    }

    pub fn pubsub_listener(self: &Arc<Self>) -> ListenerBuilder {
        ListenerBuilder::new(self.clone())
    }

    pub fn subscribe(self: &Arc<Self>, channel: String) -> mpsc::Receiver<Option<Msg>> {
        self.pubsub_listener()
            .subscribe(channel)
            .listen()
    }

    pub fn user_quota_tracker(self: &Arc<Self>, prefix: String) -> UserQuotaTracker {
        UserQuotaTracker::new(self.clone(), prefix)
    }

    pub fn set<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String) -> Set<T> {
        Set::new(name, self.clone(), None)
    }

    pub fn expiring_set<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String, ttl: Option<Duration>) -> Set<T> {
        let ttl = ttl.unwrap_or_else(|| Duration::from_secs(86400));
        Set::new(name, self.clone(), Some(ttl))
    }

    pub async fn wipe(&self) -> Result<(), ErrorTypes> {
        let mut con = self.pool.get().await?;
        redis::cmd("FLUSHDB").arg("SYNC").query_async(&mut con).await?;
        Ok(())
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>, ErrorTypes> {
        Ok(retry_call!(self.pool, keys, pattern)?)
    }

}

/// Enumeration over all possible errors
#[derive(Debug)]
pub enum ErrorTypes {
    /// There is something wrong with the redis configuration
    Configuration(Box<deadpool_redis::CreatePoolError>),
    /// Could not get a connection from the redis connection pool
    Pool(Box<deadpool_redis::PoolError>),
    /// Returned by the redis server
    Redis(Box<redis::RedisError>),
    /// Unexpected result from the redis server
    UnknownRedisError,
    /// Could not serialize or deserialize a payload
    Serde(serde_json::Error),
}

impl ErrorTypes {
    /// Test if an error was created in serializing or deserializing data
    pub fn is_serialize_error(&self) -> bool {
        matches!(self, ErrorTypes::Serde(_))
    }
}


impl std::fmt::Display for ErrorTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorTypes::Configuration(err) => write!(f, "Redis configuration: {err}"),
            ErrorTypes::Pool(err) => write!(f, "Redis connection pool: {err}"),
            ErrorTypes::Redis(err) => write!(f, "Redis runtime error: {err}"),
            ErrorTypes::UnknownRedisError => write!(f, "Unexpected response from redis server"),
            ErrorTypes::Serde(err) => write!(f, "Encoding issue with message: {err}"),
        }
    }
}

impl From<deadpool_redis::CreatePoolError> for ErrorTypes {
    fn from(value: deadpool_redis::CreatePoolError) -> Self { Self::Configuration(Box::new(value)) }
}

impl From<deadpool_redis::PoolError> for ErrorTypes {
    fn from(value: deadpool_redis::PoolError) -> Self { Self::Pool(Box::new(value)) }
}

impl From<redis::RedisError> for ErrorTypes {
    fn from(value: redis::RedisError) -> Self { Self::Redis(Box::new(value)) }
}

impl From<serde_json::Error> for ErrorTypes {
    fn from(value: serde_json::Error) -> Self { Self::Serde(value) }
}

impl std::error::Error for ErrorTypes {}

/// A convenience trait that lets you pass an i32 value or None for arguments
pub trait Ii32: Into<Option<i32>> + Copy {}
impl<T: Into<Option<i32>> + Copy> Ii32 for T {}

/// A convenience trait that lets you pass an usize value or None for arguments
pub trait Iusize: Into<Option<usize>> + Copy {}
impl<T: Into<Option<usize>> + Copy> Iusize for T {}


/// A macro for retrying calls to redis when an IO error occurs
macro_rules! retry_call {

    (handle_error, $err:ident, $exponent:ident, $maximum:ident) => {
        {
            // If the error from redis is something not related to IO let the error propagate
            if !$err.is_io_error() {
                break Result::<_, ErrorTypes>::Err($err.into())
            }

            // For IO errors print a warning and sleep
            log::warn!("No connection to Redis, reconnecting... [{}]", $err);
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(2f64.powf($exponent))).await;
            $exponent = ($exponent + 1.0).min($maximum);
        }
    };

    (handle_output, $call:expr, $exponent:ident, $maximum:ident) => {
        {
            match $call {
                Ok(val) => {
                    if $exponent > -7.0 {
                        log::info!("Reconnected to Redis!")
                    }
                    break Ok(val)
                },
                Err(err) => retry_call!(handle_error, err, $exponent, $maximum),
            }
        }
    };

    ($pool:expr, $method:ident, $($args:expr),+) => {
        {
            // track our backoff parameters
            let mut exponent = -7.0;
            let maximum = 3.0;
            loop {
                // get a (fresh if needed) connection form the pool
                let mut con = match $pool.get().await {
                    Ok(connection) => connection,
                    Err(deadpool_redis::PoolError::Backend(err)) => {
                        retry_call!(handle_error, err, exponent, maximum);
                        continue
                    },
                    Err(err) => break Err(err.into())
                };

                // execute the method given with the argments specified
                retry_call!(handle_output, con.$method($($args),+).await, exponent, maximum)
            }
        }
    };

    (method, $pool:expr, $obj:expr, $method:ident) => {
        {
            // track our backoff parameters
            let mut exponent = -7.0;
            let maximum = 3.0;
            loop {
                // get a (fresh if needed) connection form the pool
                let mut con = match $pool.get().await {
                    Ok(connection) => connection,
                    Err(deadpool_redis::PoolError::Backend(err)) => {
                        retry_call!(handle_error, err, exponent, maximum);
                        continue
                    },
                    Err(err) => break Err(err.into())
                };
                
                // execute the method given with the argments specified
                retry_call!(handle_output, $obj.$method(&mut con).await, exponent, maximum)
            }
        }
    };
}

pub (crate) use retry_call;

#[cfg(test)]
pub (crate) mod test {
    use std::sync::Arc;

    use redis::ConnectionInfo;

    use crate::{ErrorTypes, PriorityQueue, Queue, RedisObjects};


    pub (crate) async fn redis_connection() -> Arc<RedisObjects> {
        RedisObjects::open(ConnectionInfo{
            addr: redis::ConnectionAddr::Tcp("localhost".to_string(), 6379),
            redis: Default::default(),
        }).unwrap()
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    // simple function to help test that reconnect is working. 
    // Enable and run this then turn your redis server on and off.
    // #[tokio::test]
    // async fn reconnect() {
    //     init();
    //     let connection = redis_connection().await;
    //     let mut listener = connection.subscribe("abc123".to_string());
    //     // launch a thread that sends messages every second forever
    //     tokio::spawn(async move {
    //         loop {
    //             tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    //             connection.publish("abc123", b"100").await.unwrap();
    //         }
    //     });
        
    //     while let Some(msg) = listener.recv().await {
    //         println!("{msg:?}");
    //     }
    // }
    
    #[tokio::test]
    async fn test_sets() {
        init();
        let redis = redis_connection().await;
        let s = redis.set::<String>("test-set".to_owned());

        s.delete().await.unwrap();

        let values = &["a", "b", "1", "2"];
        let owned = values.iter().map(|v|v.to_string()).collect::<Vec<_>>();
        assert_eq!(s.add_batch(&owned).await.unwrap(), 4);
        assert_eq!(s.length().await.unwrap(), 4);
        let members = s.members().await.unwrap();
        assert_eq!(members.len(), 4);
        for x in members {
            assert!(owned.contains(&x));
        }
        assert!(owned.contains(&s.random().await.unwrap().unwrap()));
        assert!(s.exist(&owned[2]).await.unwrap());
        s.remove(&owned[2]).await.unwrap();
        assert!(!s.exist(&owned[2]).await.unwrap());
        let pop_val = s.pop().await.unwrap().unwrap();
        assert!(owned.contains(&pop_val));
        assert!(!s.exist(&pop_val).await.unwrap());
        assert_eq!(s.length().await.unwrap(), 2);

        assert!(s.limited_add(&"dog".to_owned(), 3).await.unwrap());
        assert!(!s.limited_add(&"cat".to_owned(), 3).await.unwrap());
        assert!(s.exist(&"dog".to_owned()).await.unwrap());
        assert!(!s.exist(&"cat".to_owned()).await.unwrap());
        assert_eq!(s.length().await.unwrap(), 3);

        for pop_val in s.pop_all().await.unwrap() {
            assert!(values.contains(&pop_val.as_str()) || ["cat", "dog"].contains(&pop_val.as_str()));
        }
        assert!(s.pop().await.unwrap().is_none());
        assert_eq!(s.length().await.unwrap(), 0);
    }
    
    
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
    
    #[tokio::test]
    async fn priority_queue() -> Result<(), ErrorTypes> {
        let redis = redis_connection().await;
        let pq = redis.priority_queue("test-priority-queue".to_string());
        pq.delete().await?;

        for x in 0..10 {
            pq.push(100.0, &x.to_string()).await?;
        }

        let a_key = pq.push(101.0, &"a".to_string()).await?;
        let z_key = pq.push(99.0, &"z".to_string()).await?;
        assert_eq!(pq.rank(&a_key).await?.unwrap(), 0);
        assert_eq!(pq.rank(&z_key).await?.unwrap(), pq.length().await? - 1);
        assert!(pq.rank(b"onethuosentuh").await?.is_none());

        assert_eq!(pq.pop(1).await?, ["a"]);
        assert_eq!(pq.unpush(1).await?, ["z"]);
        assert_eq!(pq.count(100.0, 100.0).await?, 10);
        assert_eq!(pq.pop(1).await?, ["0"]);
        assert_eq!(pq.unpush(1).await?, ["9"]);
        assert_eq!(pq.length().await?, 8);
        assert_eq!(pq.pop(4).await?, ["1", "2", "3", "4"]);
        assert_eq!(pq.unpush(3).await?, ["8", "7", "6"]);
        assert_eq!(pq.length().await?, 1);
        // Should be [(100, 5)] at this point

        // for x in 0..5 {
        for x in 0..5 {
            pq.push(100.0 + x as f64, &x.to_string()).await?;
        }

        assert_eq!(pq.length().await?, 6);
        assert!(pq.dequeue_range(Some(106), None, None, None).await?.is_empty());
        assert_eq!(pq.length().await?, 6);
        // 3 and 4 are both options, 4 has higher score
        assert_eq!(pq.dequeue_range(Some(103), None, None, None).await?, vec!["4"]);
        // 2 and 3 are both options, 3 has higher score, skip it
        assert_eq!(pq.dequeue_range(Some(102), None, Some(1), None).await?, vec!["2"]);
        // Take some off the other end
        assert_eq!(pq.dequeue_range(None, Some(100), None, Some(10)).await?, vec!["5", "0"]);
        assert_eq!(pq.length().await?, 2);

        let other = redis.priority_queue("second-priority-queue".to_string());
        other.delete().await?;
        other.push(100.0, &"a".to_string()).await?;
        assert_eq!(PriorityQueue::all_length(&[&other, &pq]).await?, [1, 2]);
        assert!(PriorityQueue::select(&[&other, &pq], None).await?.is_some());
        assert!(PriorityQueue::select(&[&other, &pq], None).await?.is_some());
        assert!(PriorityQueue::select(&[&other, &pq], None).await?.is_some());
        assert_eq!(PriorityQueue::all_length(&[&other, &pq]).await?, [0, 0]);

        pq.push(50.0, &"first".to_string()).await?;
        pq.push(-50.0, &"second".to_string()).await?;

        assert_eq!(pq.dequeue_range(Some(0), Some(100), None, None).await?, ["first"]);
        assert_eq!(pq.dequeue_range(Some(-100), Some(0), None, None).await?, ["second"]);
        Ok(())
    }
    
    
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
    
    #[tokio::test]
    async fn named_queue() {
        let redis = redis_connection().await;

        let nq = redis.queue("test-named-queue".to_owned(), None);
        nq.delete().await.unwrap();

        assert!(nq.pop().await.unwrap().is_none());
        assert!(nq.pop_batch(100).await.unwrap().is_empty());

        for x in 0..5 {
            nq.push(&x).await.unwrap();
        }

        assert_eq!(nq.content().await.unwrap(), [0, 1, 2, 3, 4]);

        assert_eq!(nq.pop_batch(100).await.unwrap(), [0, 1, 2, 3, 4]);

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