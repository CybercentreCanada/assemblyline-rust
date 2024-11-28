use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use assemblyline_models::messages::service_heartbeat::Metrics;
use parking_lot::Mutex;
use redis_objects::{AutoExportingMetrics, RedisObjects};


pub fn get_metrics_factory(redis: &Arc<RedisObjects>, service_name: &str) -> AutoExportingMetrics<Metrics> {
    static METRICS_FACTORIES: OnceLock<Mutex<HashMap<String, AutoExportingMetrics<Metrics>>>> = OnceLock::new();
    let factories = METRICS_FACTORIES.get_or_init(|| Mutex::new(Default::default()));
    let mut factories = factories.lock();

    if let Some(metrics) = factories.get(service_name) {
        return metrics.clone()
    }

    let metrics = redis.auto_exporting_metrics(service_name.to_owned(), "service".to_owned())
        .export_zero(false)
        .start();
    
    factories.insert(service_name.to_owned(), metrics.clone());
    return metrics;
}