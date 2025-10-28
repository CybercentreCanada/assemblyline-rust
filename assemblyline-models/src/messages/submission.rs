use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::{Sid, Wildcard, Sha256};

use crate::messages::dispatching::FileTreeData;

pub use crate::datastore::submission::{File, SubmissionParams};

#[derive(Serialize, Deserialize)]
pub enum MessageType {
    SubmissionIngested,
    SubmissionReceived,
    SubmissionStarted,
    SubmissionCompleted
}

/// Notification Model
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default)]
pub struct Notification {
    /// Queue to publish the completion message
    pub queue: Option<String>,
    /// Notify only if this score threshold is met
    pub threshold: Option<i32>,
}

/// Submission Model
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Submission {
    /// Submission ID to use
    pub sid: Sid,
    /// Message time
    #[serde(default="chrono::Utc::now")]
    pub time: chrono::DateTime<chrono::Utc>,
    /// File block
    pub files: Vec<File>,
    /// Metadata submitted with the file
    pub metadata: HashMap<String, Wildcard>,
    /// Notification queue parameters
    #[serde(default)]
    pub notification: Notification,
    /// Parameters of the submission
    pub params: SubmissionParams,
    /// Key used to track groups of submissions ingester will see as duplicates
    pub scan_key: Option<String>,
    ///List of error keys
    #[serde(default)]
    pub errors: Vec<String>,
    /// Result key value mapping
    #[serde(default)]
    pub results: HashMap<String, crate::datastore::result::Result>,
    /// File sha256 map to File tree of the submission
    #[serde(default)]
    pub file_tree: HashMap<Sha256, FileTreeData>,
    /// File sha256 map to file info
    #[serde(default)]
    pub file_infos: HashMap<Sha256, super::task::FileInfo>,

}

impl Submission {

    pub fn new(sid: Sid,time: chrono::DateTime<chrono::Utc>, params: SubmissionParams) -> Self {
        Self {
            sid: sid,
            time: time,
            params: params,

            files: Default::default(),
            metadata: Default::default(),
            notification: Default::default(),
            scan_key: Default::default(),
            errors: Default::default(),
            results: Default::default(),
            file_infos: Default::default(),
            file_tree: Default::default()
        }
    }

    pub fn set_notification(mut self, notification: Notification) -> Self {
        self.notification = notification;
        self
    }

    pub fn set_files(mut self, files: Vec<File>) -> Self {
        self.files = files;
        self
    }

    pub fn set_errors(mut self, errors: Vec<String>) -> Self {
        self.errors = errors;
        self
    }

    pub fn set_metadata(mut self, metadata: HashMap<String, Wildcard>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn set_scan_key(mut self, scan_key: Option<String>) -> Self {
        self.scan_key = scan_key;
        self
    }

    pub fn set_results(mut self, results: HashMap<String, crate::datastore::result::Result>) -> Self {
        self.results = results;
        self
    }

    pub fn set_file_infos(mut self, file_infos: HashMap<Sha256, super::task::FileInfo>) -> Self {
        self.file_infos = file_infos;
        self
    }

    pub fn set_file_tree(mut self, file_tree: HashMap<Sha256, FileTreeData>) -> Self{
        self.file_tree = file_tree;
        self
    }
}

impl From<&crate::datastore::submission::Submission> for Submission {
    fn from(value: &crate::datastore::submission::Submission) -> Self {
        Self {
            sid: value.sid,
            files: value.files.clone(),
            metadata: value.metadata.clone(),
            params: value.params.clone(),
            scan_key: value.scan_key.clone(),
            time: chrono::Utc::now(),
            notification: Default::default(),
            errors: Default::default(),
            results: Default::default(),
            file_infos: Default::default(),
            file_tree: Default::default()
        }
    }
}

/// Model of Submission Message
#[derive(Serialize, Deserialize)]
pub struct SubmissionMessage {
    /// Body of the message
    pub msg: Submission,
    /// Class to use to load the message as an object
    #[serde(default="default_message_loader")]
    pub msg_loader: String,
        /// Type of message
    pub msg_type: MessageType,
    /// Sender of the message
    pub sender: String,
}

pub fn default_message_loader() -> String {"assemblyline.odm.messages.submission.SubmissionMessage".to_string()}

impl SubmissionMessage {
    pub fn ingested(sub: Submission) -> Self {
        Self {
            msg: sub,
            msg_loader: default_message_loader(),
            msg_type: MessageType::SubmissionIngested,
            sender: "ingester".to_owned()
        }
    }
    pub fn started(sub: Submission) -> Self {
        Self {
            msg: sub,
            msg_loader: default_message_loader(),
            msg_type: MessageType::SubmissionStarted,
            sender: "dispatcher".to_owned()
        }
    }
    pub fn completed(sub: Submission, sender: String) -> Self {
        Self {
            msg: sub,
            msg_loader: default_message_loader(),
            msg_type: MessageType::SubmissionCompleted,
            sender
        }
    }
    pub fn received(sub: Submission, sender: String) -> Self {
        Self {
            msg: sub,
            msg_loader: default_message_loader(),
            msg_type: MessageType::SubmissionReceived,
            sender
        }
    }
}
