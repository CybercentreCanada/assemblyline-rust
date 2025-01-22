use serde::{Deserialize, Serialize};

// from assemblyline import odm
// from assemblyline.odm.messages import PerformanceTimer

// MSG_TYPES = {"IngestHeartbeat"}
// LOADER_CLASS = "assemblyline.odm.messages.ingest_heartbeat.IngestMessage"


// @odm.model(description="Queues")
// class Queues(odm.Model):
//     critical = odm.Integer(description="Size of the critical priority queue")
//     high = odm.Integer(description="Size of the high priority queue")
//     ingest = odm.Integer(description="Size of the ingest queue")
//     complete = odm.Integer(description="Size of the complete queue")
//     low = odm.Integer(description="Size of the low priority queue")
//     medium = odm.Integer(description="Size of the medium priority queue")



/// Metrics
#[derive(Serialize, Deserialize, Default)]
pub struct Metrics {
    /// Number of cache misses
    pub cache_miss: u32,
    /// Number of cache expires
    pub cache_expired: u32,
    /// Number of cache stales
    pub cache_stale: u32,
    /// Number of cache local hits
    pub cache_hit_local: u32,
    /// Number of cache hits
    pub cache_hit: u32,
    /// Number of bytes completed
    pub bytes_completed: u64,
    /// Number of bytes ingested
    pub bytes_ingested: u64,
    /// Number of duplicate submissions
    pub duplicates: u32,
    /// Number of errors
    pub error: u32,
    /// Number of completed files
    pub files_completed: u32,
    /// Number of skipped files
    pub skipped: u32,
    /// Number of completed submissions
    pub submissions_completed: u32,
    /// Number of ingested submissions
    pub submissions_ingested: u32,
    /// Number of timed_out submissions
    pub timed_out: u32,
    /// Number of safelisted submissions
    pub whitelisted: u32,

    /// Counter to track used cpu time
    #[serde(flatten)]
    pub cpu_seconds: CPUSeconds,

    // /// Used on metrics output to represent part cpu_seconds by the python module.
    // pub cpu_seconds_count: i32,

    /// Depricated
    #[serde(flatten)]
    pub busy_seconds: BusySeconds,
    // pub busy_seconds_count: i32,
}

#[derive(Serialize, Deserialize, Default)]
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


#[derive(Serialize, Deserialize, Default)]
pub struct BusySeconds {
    #[serde(rename="busy_seconds.c")]
    pub count: i32,
    #[serde(rename="busy_seconds.t")]
    pub total: f64,
}


// @odm.model(description="Processing")
// class Processing(odm.Model):
//     inflight = odm.Integer(description="Number of inflight submissions")



// @odm.model(description="Chance of Processing")
// class ProcessingChance(odm.Model):
//     critical = odm.Float(description="Chance of processing critical items")
//     high = odm.Float(description="Chance of processing high items")
//     low = odm.Float(description="Chance of processing low items")
//     medium = odm.Float(description="Chance of processing medium items")


// @odm.model(description="Heartbeat Model")
// class Heartbeat(odm.Model):
//     instances = odm.Integer(description="Number of ingest processes")
//     metrics = odm.Compound(Metrics, description="Metrics")
//     processing = odm.Compound(Processing, description="Inflight queue sizes")
//     processing_chance = odm.Compound(ProcessingChance, description="Chance of processing items")
//     queues = odm.Compound(Queues, description="Queue lengths block")


// @odm.model(description="Model of Ingester Heartbeat Message")
// class IngestMessage(odm.Model):
//     msg = odm.Compound(Heartbeat, description="Heartbeat message")
//     msg_loader = odm.Enum(values={LOADER_CLASS}, default=LOADER_CLASS, description="Loader class for message")
//     msg_type = odm.Enum(values=MSG_TYPES, default="IngestHeartbeat", description="Type of message")
//     sender = odm.Keyword(description="Sender of message")