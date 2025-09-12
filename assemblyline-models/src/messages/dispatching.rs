use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::{Sha256, Sid};



#[derive(Serialize, Deserialize, Clone)]
pub struct SubmissionDispatchMessage {
    pub submission: crate::datastore::Submission,
    #[serde(default)]
    pub completed_queue: Option<String>,
    #[serde(default)]
    pub file_infos: HashMap<Sha256, super::task::FileInfo>,
    #[serde(default)]
    pub results: HashMap<String, crate::datastore::result::Result>,
    #[serde(default)]
    pub file_tree: HashMap<Sha256, FileTreeData>,
    #[serde(default)]
    pub errors: Vec<String>,
}

impl SubmissionDispatchMessage {
    pub fn simple(submission: crate::datastore::Submission, completed_queue: Option<String>) -> Self {
        Self {
            submission,
            completed_queue,
            file_infos: Default::default(),
            file_tree: Default::default(),
            errors: Default::default(),
            results: Default::default(),
        }
    }

    pub fn set_file_infos(mut self, file_infos: HashMap<Sha256, super::task::FileInfo>) -> Self {
        self.file_infos = file_infos.clone();
        self
    }

    pub fn set_file_tree(mut self, file_tree: HashMap<Sha256, FileTreeData>) -> Self {
        self.file_tree = file_tree.clone();
        self
    }


    pub fn set_results(mut self, results: HashMap<String, crate::datastore::result::Result>) -> Self {
        self.results = results.clone();
        self
    }

    pub fn set_errors(mut self, errors:  Vec<String>) -> Self {
        self.errors = errors.clone();
        self
    }


}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct FileTreeData {
    pub name: Vec<String>,
    pub children: HashMap<Sha256, FileTreeData>
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

    pub fn fail(cache_key: String) -> Self {
        Self {
            cache_key: Some(cache_key),
            status: WatchQueueStatus::Fail,
        }
    }

    pub fn ok(cache_key: String) -> Self {
        Self {
            cache_key: Some(cache_key),
            status: WatchQueueStatus::Ok,
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
    CreateWatch,
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
            MessageClasses::CreateWatch => DispatcherCommand::CreateWatch(serde_json::from_value(self.payload_data.clone())?),
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
