use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::{MD5, Sha1, Sha256, Sid, JsonMap, SSDeepHash, datastore::file::URIInfo, config::ServiceSafelist};



/// File Information
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
pub struct TagItem {
    /// Type of tag item
    #[serde(rename="type")]
    pub tag_type: String,
    /// Value of tag item
    pub value: String,
    /// Score of tag item
    pub score: Option<i32>,
}


/// Data Item
#[derive(Serialize, Deserialize)]
pub struct DataItem {
    pub name: String,
    pub value: serde_json::Value,
}

/// Service Task Model
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub task_id: u64,
    pub dispatcher: String,
    /// Submission ID
    pub sid: Sid,
    /// Metadata associated to the submission
    pub metadata: HashMap<String, String>,
    /// Minimum classification of the file being scanned
    pub min_classification: String,
    /// File info block
    pub fileinfo: FileInfo,
    /// File name
    pub filename: String,
    /// Service name
    pub service_name: String,
    /// Service specific parameters
    pub service_config: JsonMap,
    /// File depth relative to initital submitted file
    pub depth: u32,
    /// Maximum number of files that submission can have
    pub max_files: u32,
    /// Task TTL
    pub ttl: u32,

    /// List of tags
    pub tags: Vec<TagItem>,
    /// Temporary submission data
    pub temporary_submission_data: Vec<DataItem>,

    /// Perform deep scanning
    pub deep_scan: bool,

    /// Whether the service cache should be ignored during the processing of this task
    pub ignore_cache: bool, 

    /// Whether the service should ignore the dynamic recursion prevention or not
    pub ignore_dynamic_recursion_prevention: bool,

    /// Should the service filter it's output?
    pub ignore_filtering: bool,

    /// Priority for processing order
    pub priority: i32,

    /// Safelisting configuration (as defined in global configuration)
    #[serde(default="task_default_safelist_config")]
    pub safelist_config: ServiceSafelist, // ", default={'enabled': False})
}

pub fn task_default_safelist_config() -> ServiceSafelist {
    ServiceSafelist {
        enabled: false,
        ..Default::default()
    }
}

impl Task {
    // @staticmethod
    // def make_key(sid, service_name, sha):
    //     return f"{sid}_{service_name}_{sha}"

    // def key(self):
    //     return Task.make_key(self.sid, self.service_name, self.fileinfo.sha256)

    pub fn signature(&self) -> TaskSignature {
        TaskSignature { 
            task_id: self.task_id, 
            sid: self.sid, 
            service: self.service_name.clone(), 
            hash: self.fileinfo.sha256.clone() 
        }
    } 
}

#[derive(Hash, PartialEq, Eq)]
pub struct TaskSignature {
    pub task_id: u64,
    pub sid: Sid,
    pub service: String,
    pub hash: Sha256,
}



/// Service Task Model
#[derive(Serialize, Deserialize)]
pub struct TaskToken {
    pub task_id: u64,
    pub dispatcher: String,
}