

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::{JsonMap, Sha256, Classification, Uuid, ElasticMeta};


/// Model of Submission
#[derive(Deserialize, Debug, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Submission {
    // pub archive_ts = odm.Optional(odm.Date(store=False, description="Archiving timestamp (Deprecated)"))
    /// Document is present in the malware archive
    pub archived: bool,
    /// Classification of the submission
    pub classification: Classification,
    /// Total number of errors in the submission
    pub error_count: i32,
    /// List of error keys
    #[metadata(store=false)]
    pub errors: Vec<String>,
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// Total number of files in the submission
    pub file_count: i32,
    /// List of files that were originally submitted
    pub files: Vec<File>,
    /// Maximum score of all the files in the scan
    pub max_score: i32,
    /// Metadata associated to the submission
    #[metadata(store=false)]
    pub metadata: HashMap<String, String>,
    /// Submission parameter details
    pub params: SubmissionParams,
    /// List of result keys
    #[metadata(store=false)]
    pub results: Vec<String>,
    /// Submission ID
    #[metadata(copyto="__text__")]
    pub sid: Uuid,
    /// Status of the submission
    pub state: SubmissionState,
    /// This document is going to be deleted as soon as it finishes
    pub to_be_deleted: bool,
    /// Submission-specific times
    pub times: Times,
    /// Malicious verdict details
    pub verdict: Verdict,
    /// Was loaded from the archive
    #[metadata(index=false)]
    pub from_archive: bool,

    /// the filescore key, used in deduplication. This is a non-unique key, that is
    /// shared by submissions that may be processed as duplicates.
    #[metadata(index=false, store=false)]
    pub scan_key: Option<String>,
}



/// Submission Parameters
#[derive(Serialize, Deserialize, Debug, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct SubmissionParams {
    /// classification of the submission
    #[serde(skip_serializing_if = "String::is_empty")]
    pub classification: Classification,
    /// Should a deep scan be performed?
    pub deep_scan: bool,
    /// Description of the submission
    #[serde(skip_serializing_if = "Option::is_none")]
    #[metadata(store=true, copyto="__text__")]
    pub description: Option<String>,
    /// Should this submission generate an alert?
    pub generate_alert: bool,
    /// List of groups related to this scan
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    /// Ignore the cached service results?
    pub ignore_cache: bool,
    /// Should we ignore dynamic recursion prevention?
    pub ignore_dynamic_recursion_prevention: bool,
    /// Should we ignore filtering services?
    pub ignore_filtering: bool,
    /// Ignore the file size limits?
    pub ignore_size: bool,
    /// Exempt from being dropped by ingester?
    pub never_drop: bool,
    /// Is the file submitted already known to be malicious?
    pub malicious: bool,
    /// Max number of extracted files
    pub max_extracted: u32,
    /// Max number of supplementary files
    pub max_supplementary: u32,
    /// Priority of the scan
    pub priority: u16,
    /// Should the submission do extra profiling?
    pub profile: bool,
    /// Service selection
    pub services: ServiceSelection,
    /// Service-specific parameters
    #[metadata(index=false, store=false)]
    pub service_spec: HashMap<String, JsonMap>,
    /// User who submitted the file
    #[metadata(store=true, copyto="__text__")]
    pub submitter: String,
    /// Time, in days, to live for this submission
    pub ttl: u32,
    /// Type of submission
    #[serde(rename="type")]
    pub submission_type: String,
    /// Initialization for temporary submission data
    #[serde(skip_serializing_if = "Option::is_none")]
    #[metadata(index=false)]
    pub initial_data: Option<String>,
    /// Does the submission automatically goes into the archive when completed?
    pub auto_archive: bool,
    /// When the submission is archived, should we delete it from hot storage right away?
    pub delete_after_archive: bool
}

impl Default for SubmissionParams {
    fn default() -> Self {
        Self {
            classification: "".to_owned(),
            deep_scan: false,
            description: None,
            generate_alert: false,
            groups: vec![],
            ignore_cache: false,
            ignore_dynamic_recursion_prevention: false,
            ignore_filtering: false,
            ignore_size: false,
            never_drop: false,
            malicious: false,
            max_extracted: 100,
            max_supplementary: 100,
            priority: 100,
            profile: false,
            services: Default::default(),
            service_spec: Default::default(),
            submitter: "USER".to_owned(),
            ttl: 30,
            submission_type: "USER".to_owned(),
            initial_data: None,
            auto_archive: false,
            delete_after_archive: false,
        }
    }
}


/// Service Selection Scheme
#[derive(Serialize, Deserialize, Default, Debug, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct ServiceSelection {
    /// List of selected services
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected: Option<Vec<String>>,
    /// List of excluded services
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded: Option<Vec<String>>,
    /// List of services to rescan when moving between systems
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescan: Option<Vec<String>>,
    /// Add to service selection when resubmitting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resubmit: Option<Vec<String>>,
}

/// Submission-Relevant Times
#[derive(Deserialize, Debug, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Times {
    /// Date at which the submission finished scanning
    #[metadata(store=false)]
    pub completed: Option<DateTime<Utc>>,
    /// Date at which the submission started scanning
    pub submitted: DateTime<Utc>,
}


/// Submission Verdict
#[derive(Deserialize, Debug, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Verdict {
    /// List of user that thinks this submission is malicious
    #[serde(default)]
    pub malicious: Vec<String>,
    /// List of user that thinks this submission is non-malicious
    #[serde(default)]
    pub non_malicious: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, strum::Display, Described)]
#[metadata_type(ElasticMeta)]
pub enum SubmissionState {
    Failed,
    Submitted,
    Completed,
}

impl<'de> Deserialize<'de> for SubmissionState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let string = String::deserialize(deserializer)?;
        match string.to_lowercase().as_str() {
            "failed" => Ok(Self::Failed),
            "submitted" => Ok(Self::Submitted),
            "completed" => Ok(Self::Completed),
            _ => Err(serde::de::Error::custom("unparsable submission state")),
        }
    }
}


/// File Model of Submission
#[derive(Deserialize, Debug, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct File {
    /// Name of the file
    #[metadata(copyto="__text__")]
    pub name: String,
    /// Size of the file in bytes
    pub size: Option<u64>,
    /// SHA256 hash of the file
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
}