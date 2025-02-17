use serde::{Deserialize, Serialize};

// from assemblyline import odm
// from assemblyline.odm.messages import PerformanceTimer

// MSG_TYPES = {"DispatcherHeartbeat"}
// LOADER_CLASS = "assemblyline.odm.messages.dispatcher_heartbeat.DispatcherMessage"


// @odm.model(description="Queue Model")
// class Queues(odm.Model):
//     ingest = odm.Integer(description="Number of submissions in ingest queue")
//     start = odm.List(odm.Integer(), description="Number of submissions that started")
//     result = odm.List(odm.Integer(), description="Number of results in queue")
//     command = odm.List(odm.Integer(), description="Number of commands in queue")


// @odm.model(description="Inflight Model")
// class Inflight(odm.Model):
//     max = odm.Integer(description="Maximum number of submissions")
//     outstanding = odm.Integer(description="Number of outstanding submissions")
//     per_instance = odm.List(odm.Integer(), description="Number of submissions per Dispatcher instance")


/// Metrics Model
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Metrics {
    /// Number of files completed
    pub files_completed: i32,
    /// Number of submissions completed
    pub submissions_completed: i32,
    /// Number of service timeouts
    pub service_timeouts: i32,
    /// CPU time
    #[serde(flatten)]
    pub cpu_seconds: CPUSeconds,
    /// CPU count
    // pub cpu_seconds_count: i32,
    /// Busy CPU time
    #[serde(flatten)]
    pub busy_seconds: BusySeconds,
    // pub busy_seconds_count: i32,
    /// Processed submissions waiting to be saved
    pub save_queue: i32,
    /// Errors waiting to be saved
    pub error_queue: i32,
}


#[derive(Serialize, Deserialize, Default, Debug)]
pub struct CPUSeconds {
    #[serde(rename="cpu_seconds.c")]
    pub count: i32,
    #[serde(rename="cpu_seconds.t")]
    pub total: f64,
}

impl CPUSeconds {
    pub fn increment(&mut self, time: f64) {
        self.count += 1;
        self.total += time;
    }
}


#[derive(Serialize, Deserialize, Default, Debug)]
pub struct BusySeconds {
    #[serde(rename="busy_seconds.c")]
    pub count: i32,
    #[serde(rename="busy_seconds.t")]
    pub total: f64,
}


// @odm.model(description="Heartbeat Model")
// class Heartbeat(odm.Model):
//     inflight = odm.Compound(Inflight, description="Inflight submissions")
//     instances = odm.Integer(description="Number of instances")
//     metrics = odm.Compound(Metrics, description="Dispatcher metrics")
//     queues = odm.Compound(Queues, description="Dispatcher queues")
//     component = odm.Keyword(description="Component name")


// @odm.model(description="Model of Dispatcher Heartbeat Messages")
// class DispatcherMessage(odm.Model):
//     msg = odm.Compound(Heartbeat, description="Heartbeat message")
//     msg_loader = odm.Enum(values={LOADER_CLASS}, default=LOADER_CLASS, description="Loader class for message")
//     msg_type = odm.Enum(values=MSG_TYPES, default="DispatcherHeartbeat", description="Type of message")
//     sender = odm.Keyword(description="Sender of message")
