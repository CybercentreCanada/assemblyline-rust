use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

use crate::messages::task::{generate_conf_key, Task};
use crate::{ElasticMeta, Readable, Sha256};

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described)]
#[metadata_type(ElasticMeta)]
pub enum Status {
    #[strum(serialize = "FAIL_NONRECOVERABLE")]
    FailNonrecoverable,
    #[strum(serialize = "FAIL_RECOVERABLE")]
    FailRecoverable,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, Clone, Copy)]
#[metadata_type(ElasticMeta)]
pub enum ErrorTypes {
    #[strum(serialize = "UNKNOWN")]
    Unknown = 0,
    #[strum(serialize = "EXCEPTION")]
    Exception = 1,
    #[strum(serialize = "MAX DEPTH REACHED")]
    MaxDepthReached = 10,
    #[strum(serialize = "MAX FILES REACHED")]
    MaxFilesReached = 11,
    #[strum(serialize = "MAX RETRY REACHED")]
    MaxRetryReached = 12,
    #[strum(serialize = "SERVICE BUSY")]
    ServiceBusy = 20,
    #[strum(serialize = "SERVICE DOWN")]
    ServiceDown = 21,
    #[strum(serialize = "TASK PRE-EMPTED")]
    TaskPreempted = 30
}

/// Error Response from a Service
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Response {
    /// Error message
    #[metadata(copyto="__text__")]
    pub message: String,
    /// Information about where the service was processed
    pub service_debug_info: Option<String>,
    /// Service Name
    #[metadata(copyto="__text__")]
    pub service_name: String,
    /// Service Tool Version
    #[metadata(copyto="__text__")]
    pub service_tool_version: Option<String>,
    /// Service Version
    pub service_version: String,
    /// Status of error produced by service
    pub status: Status,
}

/// Error Model used by Error Viewer
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Error {
    /// Error creation timestamp
    pub created: DateTime<Utc>,
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// Response from the service
    pub response: Response,
    /// SHA256 of file related to service error
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
    /// Type of error
    #[serde(rename="type", default="default_error_type")]
    pub error_type: ErrorTypes,
}

impl Error {
    pub fn build_key(&self, service_tool_version: Option<&str>, task: Option<&Task>) -> Result<String, serde_json::Error> {
        let key_list = [
            self.sha256.to_string(),
            self.response.service_name.replace('.', "_"),
            format!("v{}", self.response.service_version.replace('.', "_")),
            format!("c{}", generate_conf_key(service_tool_version, task)?),
            format!("e{}", self.error_type as u64),
        ];

        Ok(key_list.join("."))
    }
}

fn default_error_type() -> ErrorTypes { ErrorTypes::Exception }

impl Readable for Error {
    fn set_from_archive(&mut self, from_archive: bool) {}
}