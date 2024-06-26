use serde::{Deserialize, Serialize};

use crate::Sid;

pub mod changes;
pub mod task;
pub mod submission;
pub mod ingest_heartbeat;


#[derive(Serialize, Deserialize)]
pub struct SubmissionDispatchMessage {
    pub submission: crate::datastore::Submission,
    pub completed_queue: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all="lowercase")]
pub enum ArchiveAction {
    Archive,
    Resubmit,
}

#[derive(Serialize, Deserialize)]
pub struct ArchivedMessage {
    pub action: ArchiveAction,
    pub sid: Option<Sid>,
}

impl ArchivedMessage {
    pub fn archive() -> Self {
        Self {action: ArchiveAction::Archive, sid: None}
    }
    pub fn resubmit(sid: Sid) -> Self {
        Self {action: ArchiveAction::Resubmit, sid: Some(sid)}
    }
}