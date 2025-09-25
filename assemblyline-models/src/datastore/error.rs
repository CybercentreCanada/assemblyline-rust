use chrono::{DateTime, Utc};
use rand::seq::IteratorRandom;
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;
use strum::IntoEnumIterator;

use crate::messages::task::{generate_conf_key, Task};
use crate::{random_word, random_words, ElasticMeta, Readable};
use crate::types::{Sha256, Text};


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
    pub service_name: String,
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
            service_name: random_word(rng),
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
}

fn default_error_type() -> ErrorTypes { ErrorTypes::Exception }

impl Readable for Error {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}