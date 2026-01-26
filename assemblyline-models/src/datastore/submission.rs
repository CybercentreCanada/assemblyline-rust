

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::types::{ClassificationString, ExpandingClassification, JsonMap, ServiceName, Sha256, Sid, Text, UpperString, Wildcard};
use crate::{ElasticMeta, Readable};


/// A logging event describing the processing of a submission
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct TraceEvent {
    #[serde(default="default_now")]
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub service: Option<ServiceName>,
    pub file: Option<Sha256>,
    pub message: Option<String>,
}

fn default_now() -> DateTime<Utc> { Utc::now() }

/// Model of Submission
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Submission {
    /// Time at which the submission was archived
    #[serde(default)]
    pub archive_ts: Option<DateTime<Utc>>,
    /// Document is present in the malware archive
    #[serde(default)]
    pub archived: bool,
    /// Classification of the submission
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// A log of events describing the processing sequence.
    #[serde(default)]
    #[metadata(store=false, index=false)]
    pub tracing_events: Vec<TraceEvent>,
    /// Total number of errors in the submission
    pub error_count: i32,
    /// List of error keys
    #[metadata(store=false)]
    pub errors: Vec<String>,
    /// Expiry timestamp
    #[serde(default)]
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
    #[serde(default)]
    #[metadata(store=false, mapping="flattenedobject", copyto="__text__")]
    pub metadata: HashMap<String, Wildcard>,
    /// Submission parameter details
    pub params: SubmissionParams,
    /// List of result keys
    #[metadata(store=false)]
    pub results: Vec<Wildcard>,
    /// Submission ID
    #[metadata(copyto="__text__")]
    pub sid: Sid,
    /// Status of the submission
    pub state: SubmissionState,
    /// This document is going to be deleted as soon as it finishes
    #[serde(default)]
    pub to_be_deleted: bool,
    /// Submission-specific times
    #[serde(default)]
    pub times: Times,
    /// Malicious verdict details
    #[serde(default)]
    pub verdict: Verdict,
    /// Was loaded from the archive
    #[serde(default)]
    #[metadata(index=false)]
    pub from_archive: bool,

    /// the filescore key, used in deduplication. This is a non-unique key, that is
    /// shared by submissions that may be processed as duplicates.
    #[serde(default)]
    #[metadata(index=false, store=false)]
    pub scan_key: Option<String>,
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Submission> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Submission {
        Submission {
            archive_ts: None,
            archived: rng.random(),
            classification: ExpandingClassification::try_unrestricted().unwrap(),
            tracing_events: Default::default(),
            error_count: 0,
            errors: vec![],
            expiry_ts: None,
            file_count: 1,
            files: vec![rng.random()],
            max_score: rng.random(),
            metadata: Default::default(),
            params: SubmissionParams::new(ClassificationString::try_unrestricted().unwrap()),
            results: vec![],
            sid: rng.random(),
            state: SubmissionState::Submitted,
            to_be_deleted: false,
            times: Times {
                completed: None,
                submitted: Utc::now(),
            },
            verdict: Verdict {
                malicious: vec![],
                non_malicious: vec![],
            },
            from_archive: false,
            scan_key: None,
        }
    }
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
    #[serde(default)]
    pub deep_scan: bool,
    /// Description of the submission
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[metadata(store=true, copyto="__text__")]
    pub description: Option<Text>,
    /// Should this submission generate an alert?
    #[serde(default)]
    pub generate_alert: bool,
    /// List of groups related to this scan
    // #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub groups: Vec<UpperString>,
    /// Ignore the cached service results?
    #[serde(default)]
    pub ignore_cache: bool,
    /// Should we ignore dynamic recursion prevention?
    #[serde(default)]
    pub ignore_recursion_prevention: bool,
    /// Should we ignore filtering services?
    #[serde(default)]
    pub ignore_filtering: bool,
    /// Ignore the file size limits?
    #[serde(default)]
    pub ignore_size: bool,
    /// Exempt from being dropped by ingester?
    #[serde(default)]
    pub never_drop: bool,
    /// Is the file submitted already known to be malicious?
    #[serde(default)]
    pub malicious: bool,
    /// Max number of extracted files
    #[serde(default="default_max_extracted")]
    pub max_extracted: i32,
    /// Max number of supplementary files
    #[serde(default="default_max_supplementary")]
    pub max_supplementary: i32,
    /// Priority of the scan
    #[serde(default="default_priority")]
    pub priority: u16,
    /// Does this submission count against quota?
    #[serde(default)]
    pub quota_item: bool,
    /// Service selection
    #[serde(default)]
    pub services: ServiceSelection,
    /// Service-specific parameters
    #[serde(default)]
    #[metadata(index=false, store=false)]
    pub service_spec: HashMap<ServiceName, JsonMap>,
    /// User who submitted the file
    #[metadata(store=true, copyto="__text__")]
    pub submitter: String,
    /// Collect extra logging information during dispatching
    #[serde(default)]
    pub trace: bool,
    /// Time, in days, to live for this submission
    #[serde(default)]
    pub ttl: i32,
    /// Type of submission
    #[serde(rename="type", default="default_type")]
    pub submission_type: String,
    /// Initialization for temporary submission data
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[metadata(index=false)]
    pub initial_data: Option<Text>,
    /// Does the submission automatically goes into the archive when completed?
    #[serde(default)]
    pub auto_archive: bool,
    /// When the submission is archived, should we delete it from hot storage right away?
    #[serde(default)]
    pub delete_after_archive: bool,
    /// Parent submission ID
    #[serde(default)]
    pub psid: Option<Sid>,
    /// Should we use the alternate dtl while archiving?
    #[serde(default)]
    pub use_archive_alternate_dtl: bool,
}

fn default_max_extracted() -> i32 { 100 }
fn default_max_supplementary() -> i32 { 100 }
fn default_priority() -> u16 { 1000 }
fn default_type() -> String { "USER".to_owned() }

impl SubmissionParams {
    pub fn new(classification: ClassificationString) -> Self {
        Self {
            classification,
            deep_scan: false,
            description: None,
            generate_alert: false,
            groups: vec![],
            ignore_cache: false,
            ignore_recursion_prevention: false,
            ignore_filtering: false,
            ignore_size: false,
            never_drop: false,
            malicious: false,
            max_extracted: default_max_extracted(),
            max_supplementary: default_max_supplementary(),
            priority: default_priority(),
            quota_item: false,
            services: Default::default(),
            service_spec: Default::default(),
            submitter: "USER".to_owned(),
            trace: false,
            ttl: 30,
            submission_type: default_type(),
            initial_data: None,
            auto_archive: false,
            delete_after_archive: false,
            psid: None,
            use_archive_alternate_dtl: false,
        }
    }

    pub fn set_description(mut self, text: &str) -> Self {
        self.description = Some(text.into()); self
    }

    pub fn set_ignore_cache(mut self, value: bool) -> Self {
        self.ignore_cache = value; self
    }

    pub fn set_services_selected(mut self, selected: &[&str]) -> Self {
        self.services.selected = selected.iter().map(|s|ServiceName::from_string(s.to_string())).collect(); self
    }

    pub fn set_submitter(mut self, submitter: &str) -> Self {
        self.submitter = submitter.to_owned(); self
    }

    pub fn set_groups(mut self, groups: &[&str]) -> Self {
        self.groups = groups.iter().map(|s|s.parse().unwrap()).collect(); self
    }

    pub fn set_max_extracted(mut self, max_extracted: i32) -> Self {
        self.max_extracted = max_extracted; self
    }

    pub fn set_generate_alert(mut self, alert: bool) -> Self {
        self.generate_alert = alert; self
    }

    pub fn set_initial_data(mut self, initial_data: Option<Text>) -> Self {
        self.initial_data = initial_data; self
    }

    /// Get the sections of the submission parameters that should be used in result hashes.
    fn get_hashing_keys(&self) -> Vec<(String, serde_json::Value)> {
        [
            ("classification", json!(self.classification)),
            ("deep_scan", json!(self.deep_scan)),
            ("ignore_cache", json!(self.ignore_cache)),
            ("ignore_recursion_prevention", json!(self.ignore_recursion_prevention)),
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
    pub fn create_filescore_key(&self, sha256: &Sha256, services: Option<Vec<ServiceName>>) -> String {
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

// pub struct SubmissionParamsBuilder {
//     params: SubmissionParams,
// }

// impl SubmissionParamsBuilder {
//     pub fn new(classification: ClassificationString) -> Self {
//         Self { params: SubmissionParams::new(classification) }
//     }


// }


/// Service Selection Scheme
#[derive(Serialize, Deserialize, Default, Debug, Described, Clone)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct ServiceSelection {
    /// List of excluded services
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded: Vec<ServiceName>,
    /// List of services to rescan when moving between systems
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rescan: Vec<ServiceName>,
    /// Add to service selection when resubmitting
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resubmit: Vec<ServiceName>,
    /// List of selected services
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub selected: Vec<ServiceName>,
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

#[derive(SerializeDisplay, DeserializeFromStr, Debug, PartialEq, Eq, strum::Display, strum::EnumString, Described, Clone, Copy)]
#[strum(ascii_case_insensitive, serialize_all = "lowercase")]
#[metadata_type(ElasticMeta)]
pub enum SubmissionState {
    Failed,
    Submitted,
    Completed,
}

#[test]
fn test_state_serialization() {
    assert_eq!(serde_json::to_string(&SubmissionState::Failed).unwrap(), "\"failed\"");
    assert_eq!(serde_json::from_str::<SubmissionState>("\"failed\"").unwrap(), SubmissionState::Failed);
    assert_eq!(serde_json::to_value(SubmissionState::Failed).unwrap(), serde_json::json!("failed"));
    assert_eq!(serde_json::from_str::<SubmissionState>("\"Failed\"").unwrap(), SubmissionState::Failed);
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
    #[metadata(mapping="long")]
    pub size: Option<u64>,
    /// SHA256 hash of the file
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<File> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> File {
        File {
            name: "readme.txt".to_string(),
            size: Some(rng.random_range(10..1_000_000)),
            sha256: rng.random()
        }
    }
}
