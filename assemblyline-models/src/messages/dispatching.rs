use serde::{Deserialize, Serialize};

use crate::Sid;



#[derive(Serialize, Deserialize)]
pub struct SubmissionDispatchMessage {
    pub submission: crate::datastore::Submission,
    pub completed_queue: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all="UPPERCASE")]
pub enum WatchQueueStatus {
    Fail, 
    Ok, 
    Start, 
    Stop
}


/// These are messages sent by dispatcher on the watch queue
#[derive(Serialize, Deserialize)]
pub struct WatchQueueMessage {
    /// Cache key
    pub cache_key: Option<String>,
    /// Watch statuses
    pub status: WatchQueueStatus
}

impl WatchQueueMessage {
    pub fn stop() -> Self {
        Self {
            cache_key: None,
            status: WatchQueueStatus::Stop,
        }
    }
}

impl From<WatchQueueStatus> for WatchQueueMessage {
    fn from(value: WatchQueueStatus) -> Self {
        Self {
            cache_key: None,
            status: value
        }
    }
}


#[derive(Serialize, Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum MessageClasses {
    CrateWatch,
    ListOutstanding,
    UpdateBadSid
}


/// Create Watch Message
#[derive(Serialize, Deserialize)]
pub struct CreateWatch {
    /// Name of queue
    pub queue_name: String,
    /// Submission ID
    pub submission: Sid,
}

/// List Outstanding Message
#[derive(Serialize, Deserialize)]
pub struct ListOutstanding {
    /// Response queue
    pub response_queue: String,
    /// Submission ID
    pub submission: Sid,
}

/// Model of Dispatcher Command Message
#[derive(Serialize, Deserialize)]
pub struct DispatcherCommandMessage {
    /// Kind of message
    kind: MessageClasses,
    /// Message payload
    payload_data: serde_json::Value,
}

impl DispatcherCommandMessage {
    pub fn payload(&self) -> Result<DispatcherCommand, serde_json::Error> {
        Ok(match self.kind {
            MessageClasses::CrateWatch => DispatcherCommand::CreateWatch(serde_json::from_value(self.payload_data.clone())?),
            MessageClasses::ListOutstanding => DispatcherCommand::ListOutstanding(serde_json::from_value(self.payload_data.clone())?),
            MessageClasses::UpdateBadSid => DispatcherCommand::UpdateBadSid(serde_json::from_value(self.payload_data.clone())?),
        })
    }
}


/// Model of Dispatcher Command Message
#[derive(Serialize, Deserialize)]
pub enum DispatcherCommand {
    /// Create Watch Message
    CreateWatch(CreateWatch),

    /// List Outstanding Message
    ListOutstanding(ListOutstanding),

    /// let the dispatcher know that the bad sid list has changed
    UpdateBadSid(String),
}


