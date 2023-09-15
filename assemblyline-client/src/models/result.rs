// from collections import defaultdict
// from typing import Any, Dict

// from assemblyline import odm
// from assemblyline.common import forge
// from assemblyline.common.caching import generate_conf_key
// from assemblyline.common.dict_utils import flatten
// from assemblyline.common.tagging import tag_dict_to_list
// from assemblyline.odm.models.tagging import Tagging

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};

use crate::Sha256;

use super::Classification;
use super::tagging::Tagging;

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Debug)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum BodyFormat {
    Text,
    MemoryDump,
    GraphData,
    Url,
    Json,
    KeyValue,
    ProcessTree,
    Table,
    Image,
    Multi,
    OrderedKeyValue,
    Timeline,
}

// constants = forge.get_constants()

#[derive(Serialize, Deserialize, Debug)]
pub struct Attack {
    /// ID
    pub attack_id: String,
    /// Pattern Name
    pub pattern: String,
    /// Categories
    pub categories: Vec<String>,
}

/// Heuristic Signatures
#[derive(Serialize, Deserialize, Debug)]
struct Signature {
    /// Name of the signature that triggered the heuristic
    pub name: String,
    /// Number of times this signature triggered the heuristic
    #[serde(default = "default_signature_frequency")]
    pub frequency: i64,
    /// Is the signature safelisted or not
    #[serde(default)]
    pub safe: bool,
}

fn default_signature_frequency() -> i64 { 1 }

/// Heuristic associated to the Section
#[derive(Serialize, Deserialize, Debug)]
struct Heuristic {
    /// ID of the heuristic triggered
    pub heur_id: String,
    /// Name of the heuristic
    pub name: String,
    /// List of Att&ck IDs related to this heuristic
    #[serde(default)]
    pub attack: Vec<Attack>,
    /// List of signatures that triggered the heuristic
    #[serde(default)]
    pub signature: Vec<Signature>,
    /// Calculated Heuristic score
    pub score: i64,
}

/// Result Section
#[derive(Serialize, Deserialize, Debug)]
struct Section {
    /// Should the section be collapsed when displayed?
    #[serde(default)]
    pub auto_collapse: bool,
    /// Text body of the result section
    pub body: Option<String>,
    /// Classification of the section
    pub classification: Classification,
    /// Type of body in this section
    pub body_format: BodyFormat,
    /// Configurations for the body of this section
    pub body_config: Option<HashMap<String, serde_json::Value>>,
    /// Depth of the section
    pub depth: i64,
    /// Heuristic used to score result section
    pub heuristic: Option<Heuristic>,
    /// List of tags associated to this section
    #[serde(default)]
    pub tags: Box<Tagging>,
    /// List of safelisted tags
    #[serde(default)]
    pub safelisted_tags: HashMap<String, Vec<serde_json::Value>>,
    /// Title of the section
    pub title_text: String,
}

/// Result Body
#[derive(Serialize, Deserialize, Debug, Default)]
struct ResultBody {
    /// Aggregate of the score for all heuristics
    #[serde(default)]
    pub score: i64,
    /// List of sections
    #[serde(default)]
    pub sections: Vec<Section>,
}

/// Service Milestones
#[derive(Serialize, Deserialize, Debug, Default)]
struct Milestone {
    /// Date the service started scanning
    pub service_started: DateTime<Utc>,
    /// Date the service finished scanning
    pub service_completed: DateTime<Utc>,
}

/// File related to the Response
#[derive(Serialize, Deserialize, Debug)]
struct File {
    /// Name of the file
    pub name: String,
    /// SHA256 of the file
    pub sha256: Sha256,
    /// Description of the file
    pub description: String,
    /// Classification of the file
    pub classification: Classification,
    /// Is this an image used in an Image Result Section?
    #[serde(default)]
    pub is_section_image: bool,
    /// File relation to parent, if any.
    #[serde(default = "default_file_parent_relation")]
    pub parent_relation: String,
    /// Allow file to be analysed during Dynamic Analysis even if Dynamic Recursion Prevention is enabled.
    #[serde(default)]
    pub allow_dynamic_recursion: bool,
}

fn default_file_parent_relation() -> String { "EXTRACTED".to_owned() }

/// Response Body of Result
#[derive(Serialize, Deserialize, Debug)]
struct ResponseBody {
    /// Milestone block
    #[serde(default)]
    pub milestones: Milestone,
    /// Version of the service
    pub service_version: String,
    /// Name of the service that scanned the file
    pub service_name: String,
    /// Tool version of the service
    pub service_tool_version: Option<String>,
    /// List of supplementary files
    #[serde(default)]
    pub supplementary: Vec<File>,
    /// List of extracted files
    #[serde(default)]
    pub extracted: Vec<File>,
    /// Context about the service
    pub service_context: Option<String>,
    /// Debug info about the service
    pub service_debug_info: Option<String>,
}

/// Result Model
#[derive(Serialize, Deserialize, Debug)]
pub struct Result {
    /// Aggregate classification for the result
    pub classification: Classification,
    /// Date at which the result object got created
    pub created: DateTime<Utc>,
    /// Expiry timestamp
    pub expiry_ts: Option<DateTime<Utc>>,
    /// The body of the response from the service
    pub response: ResponseBody,
    /// The result body
    #[serde(default)]
    pub result: ResultBody,
    /// SHA256 of the file the result object relates to
    pub sha256: Sha256,
    /// What type information is given along with this result
    #[serde(rename = "type")]
    pub result_type: Option<String>,
    /// ???
    pub size: Option<u64>,
    /// Use to not pass to other stages after this run
    #[serde(default)]
    pub drop_file: bool,
    /// Was loaded from the archive
    #[serde(default)]
    pub from_archive: bool,
}

    // def build_key(self, service_tool_version=None, task=None):
    //     return self.help_build_key(
    //         self.sha256,
    //         self.response.service_name,
    //         self.response.service_version,
    //         self.is_empty(),
    //         service_tool_version=service_tool_version,
    //         task=task
    //     )

    // @staticmethod
    // def help_build_key(sha256, service_name, service_version, is_empty, service_tool_version=None, task=None):
    //     key_list = [
    //         sha256,
    //         service_name.replace('.', '_'),
    //         f"v{service_version.replace('.', '_')}",
    //         f"c{generate_conf_key(service_tool_version=service_tool_version, task=task)}",
    //     ]

    //     if is_empty:
    //         key_list.append("e")

    //     return '.'.join(key_list)

    // def scored_tag_dict(self) -> Dict[str, Dict[str, Any]]:
    //     tags: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'score': 0})
    //     # Save the tags and their score
    //     for section in self.result.sections:
    //         tag_list = tag_dict_to_list(flatten(section.tags.as_primitives()))
    //         for tag in tag_list:
    //             key = f"{tag['type']}:{tag['value']}"
    //             tags[key].update(tag)
    //             tags[key]['score'] += section.heuristic.score if section.heuristic else 0

    //     return tags

    // def is_empty(self) -> bool:
    //     if len(self.response.extracted) == 0 and \
    //             len(self.response.supplementary) == 0 and \
    //             len(self.result.sections) == 0 and \
    //             self.result.score == 0:
    //         return True
    //     return False