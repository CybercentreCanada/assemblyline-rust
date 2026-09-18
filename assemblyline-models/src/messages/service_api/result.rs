use std::collections::HashMap;

use crate::datastore;
use crate::datastore::error::ErrorTypes;
use crate::datastore::result::{default_file_parent_relation, BodyFormat, Milestone, PromoteTo};
use crate::types::{ClassificationString, JsonMap, ServiceName, Sha256, Text};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

/// Result Model
/// This struct is a nearly but not exactly identical to the datastore::result::Result struct
/// and should be kept in sync with each other.
/// This struct is used by the service_api and service_client to serialize
/// and process result json coming for the services.
/// The service_api processes the output of the service using structs in this file
/// and the output is transformed into datastore::result::Result struct
/// and stored in the database.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Result {
    /// Time at which the result was archived
    #[serde(default)]
    pub archive_ts: Option<DateTime<Utc>>,
    /// Aggregate classification for the result
    pub classification: ClassificationString,
    /// Date at which the result object got created
    #[serde(default = "chrono::Utc::now")]
    pub created: DateTime<Utc>,
    /// Expiry timestamp
    #[serde(default)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// The body of the response from the service
    pub response: ResponseBody,
    /// The result body
    #[serde(default)]
    pub result: ResultBody,
    /// SHA256 of the file the result object relates to
    pub sha256: Sha256,
    /// What type information is given along with this result
    #[serde(default, rename = "type")]
    pub result_type: Option<String>,
    /// ???
    #[serde(default)]
    pub size: Option<i32>,
    /// Use to not pass to other stages after this run
    #[serde(default)]
    pub drop_file: bool,
    /// Invalidate the current result cache creation
    #[serde(default)]
    pub partial: bool,
    #[serde(default)]
    pub temp_submission_data: JsonMap,
}

/// tracking local files with path
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct File {
    /// Name of the file
    pub name: String,
    /// SHA256 of the file
    pub sha256: Sha256,
    /// Description of the file
    pub description: Text,
    /// Classification of the file
    pub classification: ClassificationString,
    /// Is this an image used in an Image Result Section?
    #[serde(default)]
    pub is_section_image: bool,
    /// File relation to parent, if any.
    #[serde(default = "default_file_parent_relation")]
    pub parent_relation: Text,
    /// Allow file to be analysed during Dynamic Analysis even if Dynamic Recursion Prevention is enabled.
    #[serde(default)]
    pub allow_dynamic_recursion: bool,

    #[serde(default)]
    pub path: String,

    #[serde(default)]
    pub is_supplementary: bool,
}

impl From<File> for datastore::result::File {
    fn from(value: File) -> Self {
        datastore::result::File {
            name: value.name,
            sha256: value.sha256,
            description: value.description,
            classification: value.classification,
            is_section_image: value.is_section_image,
            parent_relation: value.parent_relation,
            allow_dynamic_recursion: value.allow_dynamic_recursion,
        }
    }
}

/// Response Body of Result
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ResponseBody {
    /// Milestone block
    #[serde(default)]
    pub milestones: Milestone,
    /// Version of the service
    pub service_version: String,
    /// Name of the service that scanned the file
    pub service_name: ServiceName,
    /// Tool version of the service
    #[serde(default)]
    pub service_tool_version: Option<String>,
    /// List of supplementary files
    #[serde(default)]
    pub supplementary: Vec<File>,
    /// List of extracted files
    #[serde(default)]
    pub extracted: Vec<File>,
    /// Context about the service
    #[serde(default)]
    pub service_context: Option<String>,
    /// Debug info about the service
    #[serde(default)]
    pub service_debug_info: Option<String>,
}

/// Result Body
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ResultBody {
    /// Aggregate of the score for all heuristics
    #[serde(default)]
    pub score: i32,
    /// List of sections
    #[serde(default)]
    pub sections: Vec<Section>,
}

/// Result Section
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Section {
    /// Should the section be collapsed when displayed?
    #[serde(default)]
    pub auto_collapse: bool,
    /// Text body of the result section
    #[serde(default)]
    pub body: Option<Text>,
    /// Classification of the section
    pub classification: ClassificationString,
    /// Type of body in this section
    pub body_format: BodyFormat,
    /// Configurations for the body of this section
    #[serde(default)]
    pub body_config: Option<HashMap<String, serde_json::Value>>,
    /// Depth of the section
    pub depth: i32,
    /// Heuristic used to score result section
    #[serde(default)]
    pub heuristic: Option<Heuristic>,
    /// List of tags associated to this section
    #[serde(default)]
    pub tags: JsonMap,
    /// List of safelisted tags
    #[serde(default)]
    pub safelisted_tags: HashMap<String, Vec<serde_json::Value>>,
    /// Title of the section
    pub title_text: Text,
    #[serde(default)]
    pub zeroize_on_sig_safe: bool,
    #[serde(default)]
    pub zeroize_on_tag_safe: bool,
    #[serde(default)]
    pub promote_to: Option<PromoteTo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Heuristic {
    pub heur_id: HeuristicId,
    #[serde(default, deserialize_with = "deserialize_array_or_str")]
    pub attack_ids: Vec<String>,
    #[serde(default)]
    pub signatures: HashMap<String, i32>,
    #[serde(default = "default_frequency")]
    pub frequency: i32,
    #[serde(default)]
    pub score_map: HashMap<String, i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum HeuristicId {
    Name(String),
    Code(u64),
}

impl std::fmt::Display for HeuristicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeuristicId::Name(str) => f.write_str(str),
            HeuristicId::Code(num) => f.write_fmt(format_args!("{num}")),
        }
    }
}

fn deserialize_array_or_str<'de, D>(deserializer: D) -> anyhow::Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Helper {
        List(Vec<String>),
        Value(String),
    }

    Ok(match Helper::deserialize(deserializer)? {
        Helper::List(items) => items,
        Helper::Value(value) => vec![value],
    })
}

fn default_frequency() -> i32 {
    1
}

#[derive(Serialize, Deserialize)]
pub struct ExtraError {
    pub message: String,
    #[serde(default = "unknown_error")]
    pub error_type: ErrorTypes,
}

#[derive(Serialize, Deserialize)]
pub struct ExtraWarning {
    pub message: String,
    #[serde(default = "unknown_error")]
    pub error_type: ErrorTypes,
}

fn unknown_error() -> ErrorTypes {
    ErrorTypes::Unknown
}
