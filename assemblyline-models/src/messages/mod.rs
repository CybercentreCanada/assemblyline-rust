use serde::{Deserialize, Serialize};

pub mod task;
pub mod submission;
pub mod ingest_heartbeat;


#[derive(Serialize, Deserialize)]
pub struct SubmissionDispatchMessage {
    pub submission: crate::datastore::Submission,
    pub completed_queue: Option<String>,
}
