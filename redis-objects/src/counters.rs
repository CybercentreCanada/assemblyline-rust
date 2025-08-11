//! Objects and helpers for publishing metrics in an efficent manner. 

use std::{borrow::BorrowMut, sync::Arc};
use std::marker::PhantomData;
use std::time::Duration;

use log::{error, info};
use rand::Rng;
use redis::AsyncCommands;
use serde::Serialize;
use parking_lot::Mutex;
use serde_json::json;

use crate::{retry_call, ErrorTypes, RedisObjects};

/// Trait for metric messages being exported 
pub trait MetricMessage: Serialize + Default + Send + Sync + 'static {}
impl<T: Serialize + Default + Send + Sync + 'static> MetricMessage for T {}

/// A builder to help configure a metrics counter that exports regularly to redis
/// 
/// This struct also acts as the internal config object for the counter once built.
pub struct AutoExportingMetricsBuilder<Message: MetricMessage> {
    channel_name: String,
    counter_name: Option<String>,
    counter_type: String,
    host: String,
    store: Arc<RedisObjects>,
    data_type: PhantomData<Message>,
    export_zero: bool,
    export_interval: Duration,

    /// Notification that wakes up the background task causing it to export early
    export_notify: tokio::sync::Notify,
}

impl<Message: MetricMessage> AutoExportingMetricsBuilder<Message> {

    pub (crate) fn new(store: Arc<RedisObjects>, channel_name: String, counter_type: String) -> Self {
        Self {
            channel_name,
            counter_name: None,
            counter_type,
            host: format!("{:x}", rand::rng().random::<u128>()),
            store,
            export_zero: true,
            export_interval: Duration::from_secs(5),
            data_type: Default::default(),
            export_notify: tokio::sync::Notify::new(),
        }
    }

    /// Set the name field for this counter
    pub fn counter_name(mut self, value: String) -> Self {
        self.counter_name = Some(value); self
    }

    /// Set the hostname, otherwise a random id is used.
    pub fn host(mut self, value: String) -> Self {
        self.host = value; self
    }

    /// Set the export interval 
    pub fn export_interval(mut self, value: Duration) -> Self {
        self.export_interval = value; self
    }

    /// Configure if messages should be sent when no content has been added
    pub fn export_zero(mut self, value: bool) -> Self {
        self.export_zero = value; self
    }

    /// Launch the auto exporting process and return a handle for incrementing metrics
    pub fn start(self) -> AutoExportingMetrics<Message> {
        let current = Arc::new(Mutex::new(Message::default()));
        let metrics = AutoExportingMetrics{
            config: Arc::new(self),
            current,
        };

        // start the background exporter
        metrics.clone().exporter();

        // return the original as metric interface 
        metrics
    }   

    // /// build an empty message with current exporter settings
    // fn empty_message(&self) -> Message {
    //     let counter_name = match &self.counter_name {
    //         Some(name) => name,
    //         None => &self.counter_type,
    //     };

    //     Message::new(&self.counter_type, counter_name, &self.host)
    // }
}

/// Increase the field given, by default incrementing by 1
#[macro_export]
macro_rules! increment {
    ($counter:expr, $field:ident) => {
        increment!($counter, $field, 1)
    };
    ($counter:expr, $field:ident, $value:expr) => {
        $counter.lock().$field += $value
    };
    (timer, $counter:expr, $field:ident) => {
        increment!(timer, $counter, $field, 0.0)
    };
    (timer, $counter:expr, $field:ident, $value:expr) => {
        $counter.lock().$field.increment($value)
    };
}
pub use increment;


/// A wrapper around a Message class that adds periodic backup.
///
/// At the specified interval and (best efforts) program exit, the current message will be 
/// exported to the given channel and reset with the message Default.
pub struct AutoExportingMetrics<Message: MetricMessage> {
    config: Arc<AutoExportingMetricsBuilder<Message>>,
    current: Arc<Mutex<Message>>
}

impl<Message: MetricMessage> Clone for AutoExportingMetrics<Message> {
    fn clone(&self) -> Self {
        Self { config: self.config.clone(), current: self.current.clone() }
    }
}

impl<Message: MetricMessage> AutoExportingMetrics<Message> {
    /// Launch the background export worker
    fn exporter(mut self) {
        tokio::spawn(async move {
            while let Err(err) = self.export_loop().await {
                error!("Error in metrics exporter {}: {}", self.config.counter_type, err);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    fn is_zero(&self, obj: &serde_json::Value) -> bool {
        if let Some(number) = obj.as_i64() {
            if number == 0 {
                return true
            }
        } 
        if let Some(number) = obj.as_u64() {
            if number == 0 {
                return true
            }
        } 
        if let Some(number) = obj.as_f64() {
            if number == 0.0 {
                return true
            }
        } 
        false 
    }

    fn is_all_zero(&self, obj: &serde_json::Value) -> bool {
        if let Some(obj) = obj.as_object() {
            for value in obj.values() {
                if !self.is_zero(value) {
                    return false
                }
            }
            true
        } else {
            false
        }
    }

    async fn export_once(&mut self) -> Result<(), ErrorTypes> {
        // Fetch the message that needs to be sent
        let outgoing = self.reset();

        // create mapping
        let mut outgoing = serde_json::to_value(&outgoing)?;

        // check if we will export this message
        if self.config.export_zero || !self.is_all_zero(&outgoing) {
            // add extra fields
            if let Some(obj) = outgoing.as_object_mut() {
                obj.insert("type".to_owned(), json!(self.config.counter_type));
                obj.insert("name".to_owned(), json!(self.config.counter_name));
                obj.insert("host".to_owned(), json!(self.config.host));
            }

            // send the message
            let data = serde_json::to_string(&outgoing)?;
            let _recievers: u32 = retry_call!(self.config.store.pool, publish, self.config.channel_name.as_str(), data.as_str())?;                
        }
        Ok(())
    }

    async fn export_loop(&mut self) -> Result<(), ErrorTypes> {
        loop {
            // wait for the configured duration (or we get notified to do it now)
            let _ = tokio::time::timeout(self.config.export_interval, self.config.export_notify.notified()).await;
            self.export_once().await?;

            // check if the public object has been dropped
            if Arc::strong_count(&self.current) == 1 {
                info!("Stopping metrics exporter: {}", self.config.channel_name);
                self.export_once().await?; // make sure we report any last minute messages that happened during/after the last export
                return Ok(())
            }
        }
    }

    /// Get a writeable guard holding the message that will next be exported 
    /// Rather than using this directly the increment macro can be used
    pub fn lock(&'_ self) -> parking_lot::MutexGuard<'_, Message> {
        self.current.lock()
    }

    /// Replace the current outgoing message with an empty one 
    /// returns the replaced message
    pub fn reset(&self) -> Message {
        let mut message: Message = Default::default();
        std::mem::swap(&mut message, self.current.lock().borrow_mut());
        message
    }

    /// Trigger the background task to export immediately
    pub fn export(&self) {
        self.config.export_notify.notify_one()
    }

//     def set(self, name, value):
//         try:
//             if name not in self.counter_schema:
//                 raise ValueError(f"{name} is not an accepted counter for this module: f{self.counter_schema}")
//             with self.lock:
//                 self.values[name] = value
//                 return value
//         except Exception:  # Don't let increment fail anything.
//             log.exception("Setting Metric")
//             return 0

//     def increment_execution_time(self, name, execution_time):
//         try:
//             if name not in self.timer_schema:
//                 raise ValueError(f"{name} is not an accepted counter for this module: f{self.timer_schema}")
//             with self.lock:
//                 self.counts[name + ".c"] += 1
//                 self.counts[name + ".t"] += execution_time
//                 return execution_time
//         except Exception:  # Don't let increment fail anything.
//             log.exception("Incrementing counter")
//             return 0



}

impl<M: MetricMessage> Drop for AutoExportingMetrics<M> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.current) <= 2 {
            self.export()
        }
    }
}


#[cfg(test)]
fn init() {
    let _ = env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init();
}

#[tokio::test]
async fn auto_exporting_counter() {
    use log::info;
    init();

    use serde::Deserialize;
    use crate::test::redis_connection;
    let connection = redis_connection().await;
    info!("redis connected");

    #[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
    struct MetricKind {
        started: u64,
        finished: u64,
    }

    // Subscribe on the pubsub being used
    let mut subscribe = connection.subscribe_json::<MetricKind>("test_metrics_channel".to_owned()).await;

    {   
        info!("Fast export");
        // setup an exporter that sends metrics automatically very fast
        let counter = connection.auto_exporting_metrics::<MetricKind>("test_metrics_channel".to_owned(), "component-x".to_owned())
            .export_interval(Duration::from_micros(10))
            .export_zero(false)
            .start();

        // Send a non default quantity via timer
        increment!(counter, started, 5);
        info!("Waiting for export");
        assert_eq!(subscribe.recv().await.unwrap().unwrap(), MetricKind{started: 5, finished: 0});
    }

    {   
        info!("slow export");
        // setup a slow export
        let counter = connection.auto_exporting_metrics::<MetricKind>("test_metrics_channel".to_owned(), "component-x".to_owned())
            .export_interval(Duration::from_secs(1000))
            .export_zero(false)
            .start();

        // set some quantities then erase them
        increment!(counter, started);
        increment!(counter, finished);
        counter.reset();

        // Send a default quantities explicity
        increment!(counter, started);
        increment!(counter, started);
        increment!(counter, finished);
        counter.export();
        assert_eq!(subscribe.recv().await.unwrap().unwrap(), MetricKind{started: 2, finished: 1});

        // send a message and let the drop signal an export
        increment!(counter, finished, 5);
        increment!(counter, finished);
    }

    let result = tokio::time::timeout(Duration::from_secs(10), subscribe.recv()).await.unwrap();
    assert_eq!(result.unwrap().unwrap(), MetricKind{started: 0, finished: 6});
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