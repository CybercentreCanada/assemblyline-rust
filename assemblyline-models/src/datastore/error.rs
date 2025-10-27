use chrono::{DateTime, TimeDelta, Utc};
use rand::seq::IteratorRandom;
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;
use strum::IntoEnumIterator;

use crate::datastore::Submission;
use crate::messages::task::{generate_conf_key, Task};
use crate::{random_word, random_words, ElasticMeta, Readable};
use crate::types::{ServiceName, Sha256, Text};


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, Clone, Copy)]
#[metadata_type(ElasticMeta)]
pub enum Status {
    #[strum(serialize = "FAIL_NONRECOVERABLE")]
    FailNonrecoverable,
    #[strum(serialize = "FAIL_RECOVERABLE")]
    FailRecoverable,
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Status> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Status {
        if rng.random() {
            Status::FailNonrecoverable
        } else {
            Status::FailRecoverable
        }
    }
}

impl Status {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Status::FailRecoverable)
    }
    pub fn is_nonrecoverable(&self) -> bool {
        matches!(self, Status::FailNonrecoverable)
    }
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, strum::EnumIter, Described, Clone, Copy)]
#[strum(serialize_all="lowercase")]
#[metadata_type(ElasticMeta)]
pub enum ErrorSeverity {
    Error,
    Warning,
}

impl Default for ErrorSeverity {
    fn default() -> Self {
        Self::Error
    }
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<ErrorSeverity> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> ErrorSeverity {
        match ErrorSeverity::iter().choose(rng) {
            Some(value) => value,
            None => ErrorSeverity::Error,
        }
    }
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, strum::EnumIter, Described, Clone, Copy)]
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

#[cfg(feature = "rand")]
impl rand::distr::Distribution<ErrorTypes> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> ErrorTypes {
        match ErrorTypes::iter().choose(rng) {
            Some(value) => value,
            None => ErrorTypes::Unknown,
        }
    }
}

/// Error Response from a Service
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Response {
    /// Error message
    #[metadata(copyto="__text__")]
    pub message: Text,
    /// Information about where the service was processed
    pub service_debug_info: Option<String>,
    /// Service Name
    #[metadata(copyto="__text__")]
    pub service_name: ServiceName,
    /// Service Tool Version
    #[metadata(copyto="__text__")]
    pub service_tool_version: Option<String>,
    /// Service Version
    pub service_version: String,
    /// Status of error produced by service
    pub status: Status,
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Response> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Response {
        let word_count = rng.random_range(5..25);
        Response {
            message: Text(random_words(rng, word_count).join(" ")),
            service_debug_info: None,
            service_name: ServiceName::from_string(random_word(rng)),
            service_tool_version: None,
            service_version: "0.0".to_string(),
            status: rng.random(),
        }
    }
}


/// Error Model used by Error Viewer
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Error {
    /// Time at which the error was archived
    #[serde(default)]
    pub archive_ts: Option<DateTime<Utc>>,
    /// Error creation timestamp
    #[serde(default="chrono::Utc::now")]
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
    /// The severity of an error
    #[serde(default)]
    pub severity: ErrorSeverity
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Error> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Error {
        Error {
            archive_ts: None,
            created: chrono::Utc::now(),
            expiry_ts: None,
            response: rng.random(),
            sha256: rng.random(),
            error_type: rng.random(),
            severity: rng.random(),
        }
    }
}

impl Error {
    pub fn build_key(&self, service_tool_version: Option<&str>, task: Option<&Task>) -> Result<String, serde_json::Error> {
        let key_list = [
            self.sha256.to_string(),
            self.response.service_name.replace('.', "_"),
            format!("v{}", self.response.service_version.replace('.', "_")),
            format!("c{}", generate_conf_key(service_tool_version, task, None)?),
            format!("e{}", self.error_type as u64),
        ];

        Ok(key_list.join("."))
    }

    pub fn build_unique_key(&self, service_tool_version: Option<&str>, task: Option<&Task>) -> Result<String, serde_json::Error> {
        Ok(self.build_key(service_tool_version, task)? + "." + &rand::random::<u64>().to_string())
    }

    pub fn from_submission(context: &Submission) -> builder::NeedsService {
        builder::NeedsService { error: Self {
            archive_ts: None,
            created: chrono::Utc::now(),
            expiry_ts: context.expiry_ts,
            response: Response { 
                message: Text("".to_owned()), 
                service_debug_info: None, 
                service_name: "".into(), 
                service_tool_version: None, 
                service_version: "".to_owned(), 
                status: Status::FailNonrecoverable 
            },
            sha256: context.files[0].sha256.clone(),
            error_type: ErrorTypes::Exception,
            severity: ErrorSeverity::Error,
        } }
    }

    pub fn from_task(context: &crate::messages::task::Task) -> builder::NeedsMessage {
        builder::NeedsMessage { error: Self {
            archive_ts: None,
            created: chrono::Utc::now(),
            expiry_ts: if context.ttl > 0 { Some(Utc::now() + TimeDelta::days(context.ttl as i64)) } else { None },
            response: Response { 
                message: Text("".to_owned()), 
                service_debug_info: None, 
                service_name: context.service_name, 
                service_tool_version: None, 
                service_version: "0".to_owned(), 
                status: Status::FailNonrecoverable 
            },
            sha256: context.fileinfo.sha256.clone(),
            error_type: ErrorTypes::Exception,
            severity: ErrorSeverity::Error,
        } }
    }

    pub fn from_result(context: &crate::datastore::result::Result) -> builder::NeedsMessage {
        builder::NeedsMessage { error: Self {
            archive_ts: None,
            created: chrono::Utc::now(),
            expiry_ts: context.expiry_ts,
            response: Response { 
                message: Text("".to_owned()), 
                service_debug_info: context.response.service_debug_info.clone(), 
                service_name: context.response.service_name, 
                service_tool_version: context.response.service_tool_version.clone(), 
                service_version: context.response.service_version.clone(), 
                status: Status::FailNonrecoverable 
            },
            sha256: context.sha256.clone(),
            error_type: ErrorTypes::Exception,
            severity: ErrorSeverity::Error,
        } }
    }
}

fn default_error_type() -> ErrorTypes { ErrorTypes::Exception }

impl Readable for Error {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}

pub mod builder {
    use super::ErrorTypes;
    use crate::datastore::error::{ErrorSeverity, Status};
    use crate::types::{ServiceName, Sha256, Text};


    pub struct NeedsService {
        pub (crate) error: super::Error,
    }

    impl NeedsService {
        pub fn service(mut self, name: ServiceName, version: String) -> NeedsMessage {
            self.error.response.service_name = name;
            self.error.response.service_version = version;
            NeedsMessage { error: self.error }
        }
    }

    pub struct NeedsMessage {
        pub (crate) error: super::Error,
    }

    impl NeedsMessage {
        pub fn sha256(mut self, hash: Sha256) -> Self {
            self.error.sha256 = hash; self
        }

        pub fn error_type(mut self, error_type: ErrorTypes) -> Self {
            self.error.error_type = error_type; self
        }

        pub fn status(mut self, status: Status) -> Self {
            self.error.response.status = status; self
        }

        pub fn severity(mut self, severity: ErrorSeverity) -> Self {
            self.error.severity = severity; self
        }

        pub fn maybe_tool_version(mut self, version: Option<String>) -> Self {
            self.error.response.service_tool_version = version; self
        }

        pub fn tool_version(mut self, version: String) -> Self {
            self.error.response.service_tool_version = Some(version); self
        }

        pub fn message(mut self, message: String) -> super::Error {
            self.error.response.message = Text(message); self.error
        }
    }

}