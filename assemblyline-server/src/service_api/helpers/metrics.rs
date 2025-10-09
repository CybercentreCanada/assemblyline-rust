use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use assemblyline_models::messages::service_heartbeat::Metrics;
use parking_lot::Mutex;
use redis_objects::{AutoExportingMetrics, RedisObjects};

use crate::constants::METRICS_CHANNEL;


pub fn get_metrics_factory(redis: &Arc<RedisObjects>, service_name: &str) -> AutoExportingMetrics<Metrics> {
    static METRICS_FACTORIES: LazyLock<Mutex<HashMap<String, AutoExportingMetrics<Metrics>>>> = LazyLock::new(||{
        Mutex::new(Default::default())
    });
    let mut factories = METRICS_FACTORIES.lock();

    if let Some(metrics) = factories.get(service_name) {
        return metrics.clone()
    }

    let metrics = redis.auto_exporting_metrics(METRICS_CHANNEL.to_owned(), "service".to_owned())
        .counter_name(service_name.to_owned())
        .export_zero(false)
        .start();

    factories.insert(service_name.to_owned(), metrics.clone());
    return metrics;
}