use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::Sid;

// from typing import List, Dict, Optional as Opt
// from assemblyline import odm
use crate::datastore::submission::{File, SubmissionParams};
// from assemblyline.odm.models.submission import SubmissionParams, File, Submission as DatabaseSubmission

// MSG_TYPES = {"SubmissionIngested", "SubmissionReceived", "SubmissionStarted", "SubmissionCompleted"}
// LOADER_CLASS = "assemblyline.odm.messages.submission.SubmissionMessage"


/// Notification Model
#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Notification {
    /// Queue to publish the completion message
    pub queue: Option<String>,
    /// Notify only if this score threshold is met
    pub threshold: Option<i32>,
}


/// Submission Model
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Submission {
    /// Submission ID to use
    pub sid: Sid,
    /// Message time
    pub time: chrono::DateTime<chrono::Utc>,
    /// File block
    pub files: Vec<File>, 
    /// Metadata submitted with the file
    pub metadata: HashMap<String, String>, 
    /// Notification queue parameters
    pub notification: Notification,
    /// Parameters of the submission
    pub params: SubmissionParams,
    /// Key used to track groups of submissions ingester will see as duplicates
    pub scan_key: Option<String>,
}

impl Default for Submission {
    fn default() -> Self {
        Self { 
            sid: Sid(0), 
            time: chrono::Utc::now(), 
            files: Default::default(), 
            metadata: Default::default(), 
            notification: Default::default(), 
            params: Default::default(), 
            scan_key: Default::default() 
        }
    }
}

// def from_datastore_submission(submission: DatabaseSubmission):
//     """
//     A helper to convert between database model version of Submission
//     and the message version of Submission.
//     """
//     return Submission({
//         'sid': submission.sid,
//         'files': submission.files,
//         'metadata': submission.metadata,
//         'params': submission.params,
//         'scan_key': submission.scan_key
//     })


// @odm.model(description="Model of Submission Message")
// class SubmissionMessage(odm.Model):
//     msg = odm.Compound(Submission, description="Body of the message")
//     msg_loader = odm.Enum(values={LOADER_CLASS}, default=LOADER_CLASS,
//                           description="Class to use to load the message as an object")   #
//     msg_type = odm.Enum(values=MSG_TYPES, description="Type of message")
//     sender = odm.Keyword(description="Sender of the message")