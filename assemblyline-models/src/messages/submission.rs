use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::{Sid, Wildcard, ClassificationString};
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
}

impl Submission {
    // pub fn new(classification: ClassificationString) -> Self {
    //     Self { 
    //         sid: Sid(0), 
    //         time: chrono::Utc::now(), 
    //         files: Default::default(), 
    //         metadata: Default::default(), 
    //         notification: Default::default(), 
    //         params: SubmissionParams::new(classification), 
    //         scan_key: Default::default() 
    //     }
    // }
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