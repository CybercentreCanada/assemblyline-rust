

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use struct_metadata::Described;

use crate::{ClassificationString, ElasticMeta, ExpandingClassification, JsonMap, Readable, Sha256, Sid, Text, UpperString};


/// Model of Submission
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Submission {
    // pub archive_ts = odm.Optional(odm.Date(store=False, description="Archiving timestamp (Deprecated)"))
    /// Document is present in the malware archive
    pub archived: bool,
    /// Classification of the submission
    #[serde(flatten)]
    pub classification: ExpandingClassification,
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
    // pub file: File,
    /// Maximum score of all the files in the scan
    pub max_score: i32,
    /// Metadata associated to the submission
    #[metadata(store=false, mapping="flattenedobject")]
    pub metadata: HashMap<String, serde_json::Value>,
    /// Submission parameter details
    pub params: SubmissionParams,
    /// List of result keys
    #[metadata(store=false)]
    pub results: Vec<String>,
    /// Submission ID
    #[metadata(copyto="__text__")]
    pub sid: Sid,
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

impl Readable for Submission {
    fn set_from_archive(&mut self, from_archive: bool) {
        self.from_archive = from_archive
    }
}


/// Submission Parameters
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct SubmissionParams {
    /// classification of the submission
    pub classification: ClassificationString,
    /// Should a deep scan be performed?
    pub deep_scan: bool,
    /// Description of the submission
    #[serde(skip_serializing_if = "Option::is_none")]
    #[metadata(store=true, copyto="__text__")]
    pub description: Option<Text>,
    /// Should this submission generate an alert?
    pub generate_alert: bool,
    /// List of groups related to this scan
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<UpperString>,
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
    pub max_extracted: i32,
    /// Max number of supplementary files
    pub max_supplementary: i32,
    /// Priority of the scan
    pub priority: u16,
    /// Should the submission do extra profiling?
    pub profile: bool,
    /// Does this submission count against quota?
    pub quota_item: bool,
    /// Service selection
    pub services: ServiceSelection,
    /// Service-specific parameters
    #[metadata(index=false, store=false)]
    pub service_spec: HashMap<String, JsonMap>,
    /// User who submitted the file
    #[metadata(store=true, copyto="__text__")]
    pub submitter: String,
    /// Time, in days, to live for this submission
    pub ttl: i32,
    /// Type of submission
    #[serde(rename="type")]
    pub submission_type: String,
    /// Initialization for temporary submission data
    #[serde(skip_serializing_if = "Option::is_none")]
    #[metadata(index=false)]
    pub initial_data: Option<Text>,
    /// Does the submission automatically goes into the archive when completed?
    pub auto_archive: bool,
    /// When the submission is archived, should we delete it from hot storage right away?
    pub delete_after_archive: bool,
    /// Parent submission ID
    pub psid: Option<Sid>,
    /// Should we use the alternate dtl while archiving?
    #[serde(default)]
    pub use_archive_alternate_dtl: bool,
}


impl SubmissionParams {
    pub fn new(classification: ClassificationString) -> Self {
        Self {
            classification,
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
            quota_item: false,
            services: Default::default(),
            service_spec: Default::default(),
            submitter: "USER".to_owned(),
            ttl: 30,
            submission_type: "USER".to_owned(),
            initial_data: None,
            auto_archive: false,
            delete_after_archive: false,
            psid: None,
            use_archive_alternate_dtl: false,
        }
    }

    /// Get the sections of the submission parameters that should be used in result hashes.
    fn get_hashing_keys(&self) -> Vec<(String, serde_json::Value)> {
        [
            ("classification", json!(self.classification)),
            ("deep_scan", json!(self.deep_scan)),
            ("ignore_cache", json!(self.ignore_cache)),
            ("ignore_dynamic_recursion_prevention", json!(self.ignore_dynamic_recursion_prevention)),
            ("ignore_filtering", json!(self.ignore_filtering)),
            ("ignore_size", json!(self.ignore_size)),
            ("max_extracted", json!(self.max_extracted)),
            ("max_supplementary", json!(self.max_supplementary)),
        ].into_iter().map(|(key, value)|(key.to_owned(), value)).collect()
    }


    /// This is the key used to store the final score of a submission for fast lookup.
    /// 
    /// This lookup is one of the methods used to check for duplication in ingestion process,
    /// so this key is fairly sensitive.
    pub fn create_filescore_key(&self, sha256: &Sha256, services: Option<Vec<String>>) -> String {
        // TODO do we need this version thing still be here?
        // One up this if the cache is ever messed up and we
        // need to quickly invalidate all old cache entries.
        let version = 0;

        let services = match services {
            Some(services) => services,
            None => self.services.selected.clone(),
        };

        let mut data = self.get_hashing_keys();
        data.push(("service_spec".to_owned(), {
            let mut spec = vec![];
            for (key, values) in self.service_spec.clone() {
                let mut values: Vec<(String, Value)> = values.into_iter().collect();
                values.sort_by(|a, b|a.0.cmp(&b.0));
                spec.push((key, values));
            }
            spec.sort_by(|a, b|a.0.cmp(&b.0));
            json!(spec)
        }));
        data.push(("sha256".to_owned(), json!(sha256)));
        data.push(("services".to_owned(), json!(services)));

        let s = data.into_iter().map(|(k, v)| format!("{k}: {v}")).collect::<Vec<String>>().join(", ");
        // s = ', '.join([f"{k}: {data[k]}" for k in sorted(data.keys())])

        use md5::{Md5, Digest};
        let mut hasher = Md5::new();
        hasher.update(s);
        let hash = hasher.finalize();
        let mut hex = String::new();
        for byte in hash {
            hex += &format!("{byte:x}");
        }

        format!("{hex}v{version}")
    }
}

/// Service Selection Scheme
#[derive(Serialize, Deserialize, Default, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct ServiceSelection {
    /// List of excluded services
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded: Vec<String>,
    /// List of services to rescan when moving between systems
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rescan: Vec<String>,
    /// Add to service selection when resubmitting
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resubmit: Vec<String>,
    /// List of selected services
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub selected: Vec<String>,
}

/// Submission-Relevant Times
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Times {
    /// Date at which the submission finished scanning
    #[metadata(store=false)]
    pub completed: Option<DateTime<Utc>>,
    /// Date at which the submission started scanning
    pub submitted: DateTime<Utc>,
}

impl Default for Times {
    fn default() -> Self {
        Self { 
            completed: None, 
            submitted: Utc::now() 
        }
    }
}

/// Submission Verdict
#[derive(Serialize, Deserialize, Debug, Described, Clone, Default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
#[serde(default)]
pub struct Verdict {
    /// List of user that thinks this submission is malicious
    pub malicious: Vec<String>,
    /// List of user that thinks this submission is non-malicious
    pub non_malicious: Vec<String>,
}

#[derive(Serialize, Debug, PartialEq, Eq, strum::Display, Described, Clone, Copy)]
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
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct File {
    /// Name of the file
    #[metadata(copyto="__text__")]
    pub name: String,
    /// Size of the file in bytes
    #[metadata(mapping="integer")]
    pub size: Option<u64>,
    /// SHA256 hash of the file
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
}