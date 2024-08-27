use assemblyline_markings::config::{ClassificationConfig, ClassificationGroup, ClassificationLevel, DynamicGroupType};
use assemblyline_models::Sid;
use strum_macros::{Display, EnumIter};


pub(crate) const INGEST_INTERNAL_QUEUE_NAME: &str = "m-unique";
pub(crate) const ALERT_QUEUE_NAME: &str = "m-alert";
pub(crate) const CONFIG_HASH_NAME: &str = "config-data";
pub(crate) const CONFIG_HASH: &str = "al-config";
pub(crate) const POST_PROCESS_CONFIG_KEY: &str = "post-process-actions";
pub(crate) const METRICS_CHANNEL: &str = "assemblyline_metrics";
pub(crate) const COMPLETE_QUEUE_NAME: &str = "m-complete";
pub(crate) const INGEST_QUEUE_NAME: &str = "m-ingest";
pub(crate) const SUBMISSION_QUEUE: &str = "dispatch-submission-queue";
pub(crate) const ARCHIVE_QUEUE_NAME: &str = "m-archive";
pub(crate) const DISPATCH_TASK_HASH: &str = "dispatch-active-submissions";
pub(crate) const DISPATCH_RUNNING_TASK_HASH: &str = "dispatch-active-tasks";
pub(crate) const SCALER_TIMEOUT_QUEUE: &str = "scaler-timeout-queue";


/// Take the name of a service, and provide the queue name to send tasks to that service.
pub fn service_queue_name(service: &str) -> String {
    format!("service-queue-{service}")
}

/// Get the name of the list dispatcher will pull for sending out submission events.
pub fn make_watcher_list_name(sid: Sid) -> String {    
    format!("dispatch-watcher-list-{sid}")
}


// # Queue priority values for each bucket in the ingester
// PRIORITIES = {
//     'low': 100,  # 0 -> 100
//     'medium': 200,  # 101 -> 200
//     'high': 300,
//     'critical': 400,
//     'user-low': 500,
//     'user-medium': 1000,
//     'user-high': 1500
// }
// MAX_PRIORITY = 2000

// # The above priority values presented as a range for consistency
// PRIORITY_RANGES = {}
// _start = -1
// for _end, _level in sorted((val, key) for key, val in PRIORITIES.items()):
//     PRIORITY_RANGES[_level] = (_start + 1, _end)
//     _start = _end


// # Score thresholds for determining which queue priority a reingested item
// # gets based on its previous score.
// # eg.: item with a previous score of 99 will get 'low' priority
// #      item with a previous score of 300 will get a 'high' priority
// PRIORITY_THRESHOLDS = {
//     'critical': 500,
//     'high': 100,
// }
