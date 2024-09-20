use serde::Serialize;
use serde_json::json;

use crate::constants::{METRICS_CHANNEL, NOTIFICATION_QUEUE_PREFIX};
use crate::ingester::IngestTask;
use crate::Core;




impl Core {
    //     Manually publish metric counts to the metrics system.
    //
    //     This was built for when the service server is reporting metrics for execution and caching
    //     on behalf of many services. At the moment the metrics system uses the hosts to count the number
    //     of instances of each service. This could be done with a single auto exporting counter for
    //     the service server, but that may require significant downstream changes in the metrics system.
    pub async fn export_metrics_once<T: Serialize>(&self, name: &str, metrics: &T, host: Option<&str>, counter_type: Option<&str>) -> Result<u32, redis_objects::ErrorTypes> {

        let mut counts = serde_json::to_value(metrics)?;

        if let Some(counts) = counts.as_object_mut() {
            counts.insert("type".to_owned(), json!(counter_type.unwrap_or(name)));
            counts.insert("name".to_owned(), json!(name));
            counts.insert("host".to_owned(), json!(host));
        }
        
        self.redis_metrics.publish(METRICS_CHANNEL, &serde_json::to_vec(&counts)?).await
    }

    pub fn notification_queue(&self, name: &str) -> redis_objects::Queue<IngestTask> {
        let queue_name = NOTIFICATION_QUEUE_PREFIX.to_owned() + name;
        self.redis_persistant.queue::<IngestTask>(queue_name, None)
    }
}