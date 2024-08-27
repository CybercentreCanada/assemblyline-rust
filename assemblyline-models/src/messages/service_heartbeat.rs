use serde::{Deserialize, Serialize};

// from assemblyline import odm

// MSG_TYPES = {"ServiceHeartbeat"}
// LOADER_CLASS = "assemblyline.odm.messages.service_heartbeat.ServiceMessage"



/// Service Metrics
#[derive(Serialize, Deserialize, Default)]
pub struct Metrics {
    /// Number of cache hits
    pub cache_hit: i64,
    /// Number of cache misses
    pub cache_miss: i64,
    /// Number of cache skips
    pub cache_skipped: i64,
    /// Number of service executes
    pub execute: i64,
    /// Number of recoverable fails
    pub fail_recoverable: i64,
    /// Number of non-recoverable fails
    pub fail_nonrecoverable: i64,
    /// Number of tasks scored
    pub scored: i64,
    /// Number of tasks not scored
    pub not_scored: i64,
}

// @odm.model(description="Service Activity")
// class Activity(odm.Model):
//     busy = odm.Integer(description="Number of busy instances")
//     idle = odm.Integer(description="Number of idle instances")


// @odm.model(description="Heartbeat Model")
// class Heartbeat(odm.Model):
//     activity = odm.Compound(Activity, description="Service activity")
//     instances = odm.Integer(description="Service instances")
//     metrics = odm.Compound(Metrics, description="Service metrics")
//     queue = odm.Integer(description="Service queue")
//     service_name = odm.Keyword(description="Service name")


// @odm.model(description="Model of Service Heartbeat Message")
// class ServiceMessage(odm.Model):
//     msg = odm.Compound(Heartbeat, description="Heartbeat message")
//     msg_loader = odm.Enum(values={LOADER_CLASS}, default=LOADER_CLASS, description="Loader class for message")
//     msg_type = odm.Enum(values=MSG_TYPES, default="ServiceHeartbeat", description="Type of message")
//     sender = odm.Keyword(description="Sender of message")
