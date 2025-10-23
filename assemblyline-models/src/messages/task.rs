use std::collections::HashMap;

use md5::Digest;
use rand::Rng;
use serde::{Serialize, Deserialize};

use crate::datastore::tagging::TagValue;
use crate::random_word;
use crate::types::{JsonMap, SSDeepHash, ServiceName, Sha1, Sha256, Sid, Wildcard, MD5};
use crate::{datastore::file::URIInfo, config::ServiceSafelist};


/// File Information
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct FileInfo {
    /// The output from libmagic which was used to determine the tag
    pub magic: String,
    /// MD5 of the file
    pub md5: MD5,
    /// The libmagic mime type
    pub mime: Option<String>,
    /// SHA1 hash of the file
    pub sha1: Sha1,
    /// SHA256 hash of the file
    pub sha256: Sha256,
    /// Size of the file in bytes
    pub size: u64,
    /// SSDEEP hash of the file"
    pub ssdeep: Option<SSDeepHash>,
    /// TLSH hash of the file"
    pub tlsh: Option<String>,
    /// Type of file as identified by Assemblyline
    #[serde(rename="type")]
    pub file_type: String,
    /// URI structure to speed up specialty file searching
    pub uri_info: Option<URIInfo>,
}

/// Tag Item
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TagItem {
    /// Type of tag item
    #[serde(rename="type")]
    pub tag_type: String,
    ///Short version of tag type
    pub short_type: String,
    /// Value of tag item
    pub value: TagValue,
    /// Score of tag item
    #[serde(skip_serializing_if="Option::is_none")]
    pub score: Option<i32>,
}


/// Data Item
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DataItem {
    pub name: String,
    pub value: serde_json::Value,
}

/// Service Task Model
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Task {
    /// A random ID to differentiate this task
    pub task_id: u64,
    /// Id of the dispatcher that issued this task
    pub dispatcher: String,
    pub dispatcher_address: String,
    /// Submission ID
    pub sid: Sid,
    /// Metadata associated to the submission
    pub metadata: HashMap<String, Wildcard>,
    /// Minimum classification of the file being scanned
    pub min_classification: String,
    /// File info block
    pub fileinfo: FileInfo,
    /// File name
    pub filename: String,
    /// Service name
    pub service_name: ServiceName,
    /// Service specific parameters
    pub service_config: JsonMap,
    /// File depth relative to initital submitted file
    pub depth: u32,
    /// Maximum number of files that submission can have
    pub max_files: i32,
    /// Task TTL
    pub ttl: i32,

    /// List of tags
    pub tags: Vec<TagItem>,
    /// Temporary submission data
    pub temporary_submission_data: Vec<DataItem>,

    /// Perform deep scanning
    pub deep_scan: bool,

    /// Whether the service cache should be ignored during the processing of this task
    pub ignore_cache: bool, 

    /// Whether the service should ignore the dynamic recursion prevention or not
    pub ignore_recursion_prevention: bool,

    /// Should the service filter it's output?
    pub ignore_filtering: bool,

    /// Priority for processing order
    pub priority: i32,

    /// Safelisting configuration (as defined in global configuration)
    #[serde(default="task_default_safelist_config")]
    pub safelist_config: ServiceSafelist, // ", default={'enabled': False})
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Task> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Task {
        Task {
            task_id: rng.random(),
            dispatcher: random_word(rng),
            dispatcher_address: "localhost:8080".to_string(),
            sid: rng.random(),
            metadata: Default::default(),
            min_classification: Default::default(),
            fileinfo: FileInfo { 
                magic: "".to_string(), 
                md5: rng.random(), 
                mime: None, 
                sha1: rng.random(), 
                sha256: rng.random(), 
                size: rng.random(), 
                ssdeep: Some(rng.random()),
                tlsh: None, 
                file_type: "unknown".to_owned(), 
                uri_info: None 
            },
            filename: random_word(rng),
            service_name: ServiceName::from_string(random_word(rng)),
            service_config: Default::default(),
            depth: rng.random(),
            max_files: rng.random(),
            ttl: rng.random_range(0..100),
            tags: Default::default(),
            temporary_submission_data: Default::default(),
            deep_scan: rng.random(),
            ignore_cache: rng.random(),
            ignore_recursion_prevention: rng.random(),
            ignore_filtering: rng.random(),
            priority: rng.random(),
            safelist_config: Default::default(),
        }
    }
}

pub fn task_default_safelist_config() -> ServiceSafelist {
    ServiceSafelist {
        enabled: false,
        ..Default::default()
    }
}

impl Task {
    pub fn make_key(sid: Sid, service_name: &str, sha: &Sha256) -> String {
        format!("{sid}_{service_name}_{sha}")
    }

    pub fn key(&self) -> String {
        Self::make_key(self.sid, &self.service_name, &self.fileinfo.sha256)
    }

    pub fn signature(&self) -> TaskSignature {
        TaskSignature { 
            task_id: self.task_id, 
            sid: self.sid, 
            service: self.service_name, 
            hash: self.fileinfo.sha256.clone() 
        }
    } 
}

pub fn generate_conf_key(service_tool_version: Option<&str>, task: Option<&Task>, partial: Option<bool>) -> Result<String, serde_json::Error> {
    if let Some(task) = task {
        let service_config = serde_json::to_string(&{
            let mut pairs: Vec<_> = task.service_config.iter().collect();
            pairs.sort_unstable_by_key(|row| row.0);
            pairs
        })?;

        let submission_params_str = serde_json::to_string(&[
            ("deep_scan", serde_json::json!(task.deep_scan)),
            ("ignore_filtering", serde_json::json!(task.ignore_filtering)),
            ("max_files", serde_json::json!(task.max_files)),
            ("min_classification", serde_json::json!(task.min_classification)),
        ])?;

        let ignore_salt = if task.ignore_cache || partial.unwrap_or_default() {
             &rand::rng().random::<u128>().to_string()
        } else {
            "None"
        };

        let service_tool_version = service_tool_version.unwrap_or("None");
        let total_str = format!("{service_tool_version}_{service_config}_{submission_params_str}_{ignore_salt}");

        // get an md5 hash
        let mut hasher = md5::Md5::new();
        hasher.update(total_str);
        let hash = hasher.finalize();
        
        // truncate it to 8 bytes and interpret it as a number
        let number = u64::from_be_bytes(hash[0..8].try_into().unwrap());
        
        // encode it as a string
        Ok(base62::encode(number))
    } else {
        Ok("0".to_string())
    }
}


#[derive(Hash, PartialEq, Eq)]
pub struct TaskSignature {
    pub task_id: u64,
    pub sid: Sid,
    pub service: ServiceName,
    pub hash: Sha256,
}



/// Service Task Model
#[derive(Serialize, Deserialize)]
pub struct TaskToken {
    pub task_id: u64,
    pub dispatcher: String,
}

// ============================================================================
//MARK: Responses 

#[derive(Serialize, Deserialize, Clone)]
pub struct ResultSummary {
    pub key: String,
    pub drop: bool,
    pub score: i32,
    pub partial: bool,
    pub children: Vec<(Sha256, String)>
}

#[derive(Serialize, Deserialize)]
pub enum ServiceResponse {
    Result(Box<ServiceResult>),
    Error(Box<ServiceError>),
}

impl ServiceResponse {
    pub fn sid(&self) -> Sid {
        match self {
            ServiceResponse::Result(item) => item.sid,
            ServiceResponse::Error(item) => item.sid,
        }
    }

    pub fn sha256(&self) -> Sha256 {
        match self {
            ServiceResponse::Result(item) => item.sha256.clone(),
            ServiceResponse::Error(item) => item.service_task.fileinfo.sha256.clone(),
        }
    }

    pub fn service_name(&self) -> ServiceName {
        match self {
            ServiceResponse::Result(item) => item.service_name,
            ServiceResponse::Error(item) => item.service_task.service_name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TagEntry {
    pub score: i32,    
    #[serde(rename="type")]
    pub tag_type: String,
    pub value: TagValue,

}

#[derive(Serialize, Deserialize)]
pub struct ServiceResult {
    pub dynamic_recursion_bypass: Vec<Sha256>,
    pub sid: Sid,
    pub sha256: Sha256,
    pub service_name: ServiceName,
    pub service_version: String,
    pub service_tool_version: Option<String>,
    pub expiry_ts: Option<chrono::DateTime<chrono::Utc>>,
    pub result_summary: ResultSummary,
    pub tags: HashMap<String, TagEntry>,
    pub extracted_names: HashMap<Sha256, String>,
    pub temporary_data: JsonMap,
    pub extra_errors: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct ServiceError {
    pub sid: Sid,
    pub service_task: Task,
    pub error: crate::datastore::Error,
    pub error_key: String,
}
