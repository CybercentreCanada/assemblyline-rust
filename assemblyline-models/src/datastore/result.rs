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
use struct_metadata::Described;

use crate::datastore::tagging::LayoutError;
use crate::messages::task::{generate_conf_key, TagEntry, Task};
use crate::types::strings::Keyword;
use crate::{random_word, ElasticMeta, Readable};
use crate::types::{ClassificationString, ExpandingClassification, ServiceName, Sha256, Text};

use super::tagging::Tagging;

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
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

// This needs to match the PROMOTE_TO StringTable in
// assemblyline-v4-service/assemblyline_v4_service/common/result.py.
// Any updates here need to go in that StringTable also.
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum PromoteTo {
    Screenshot,
    Entropy,
    UriParams
}

// constants = forge.get_constants()

#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Attack {
    /// ID
    #[metadata(copyto="__text__")]
    pub attack_id: String,
    /// Pattern Name
    #[metadata(copyto="__text__")]
    pub pattern: String,
    /// Categories
    pub categories: Vec<String>,
}

/// Heuristic Signatures
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Signature {
    /// Name of the signature that triggered the heuristic
    #[metadata(copyto="__text__")]
    pub name: String,
    /// Number of times this signature triggered the heuristic
    #[serde(default = "default_signature_frequency")]
    pub frequency: i32,
    /// Is the signature safelisted or not
    #[serde(default)]
    pub safe: bool,
}

fn default_signature_frequency() -> i32 { 1 }

/// Heuristic associated to the Section
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Heuristic {
    /// ID of the heuristic triggered
    #[metadata(copyto="__text__")]
    pub heur_id: String,
    /// Name of the heuristic
    #[metadata(copyto="__text__")]
    pub name: String,
    /// List of Att&ck IDs related to this heuristic
    #[serde(default)]
    pub attack: Vec<Attack>,
    /// List of signatures that triggered the heuristic
    #[serde(default)]
    pub signature: Vec<Signature>,
    /// Calculated Heuristic score
    pub score: i32,
}

/// Result Section
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Section {
    /// Should the section be collapsed when displayed?
    #[serde(default)]
    pub auto_collapse: bool,
    /// Text body of the result section
    #[metadata(copyto="__text__")]
    pub body: Option<Text>,
    /// Classification of the section
    pub classification: ClassificationString,
    /// Type of body in this section
    #[metadata(index=false)]
    pub body_format: BodyFormat,
    /// Configurations for the body of this section
    #[metadata(index=false)]
    pub body_config: Option<HashMap<String, serde_json::Value>>,
    /// Depth of the section
    #[metadata(index=false)]
    pub depth: i32,
    /// Heuristic used to score result section
    pub heuristic: Option<Heuristic>,
    /// List of tags associated to this section
    #[serde(default)]
    pub tags: Tagging,
    /// List of safelisted tags
    #[serde(default)]
    #[metadata(store=false, mapping="flattenedobject")]
    pub safelisted_tags: HashMap<String, Vec<Keyword>>,
    /// Title of the section
    #[metadata(copyto="__text__")]
    pub title_text: Text,
    /// This is the type of data that the current section should be promoted to.
    pub promote_to: Option<PromoteTo>,
}

/// Result Body
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct ResultBody {
    /// Aggregate of the score for all heuristics
    #[serde(default)]
    pub score: i32,
    /// List of sections
    #[serde(default)]
    pub sections: Vec<Section>,
}

/// Service Milestones
#[derive(Serialize, Deserialize, Debug, Default, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct Milestone {
    /// Date the service started scanning
    pub service_started: DateTime<Utc>,
    /// Date the service finished scanning
    pub service_completed: DateTime<Utc>,
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Milestone> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Milestone {
        let service_started = chrono::Utc::now() - chrono::TimeDelta::hours(rng.random_range(1..200));
        let duration = chrono::TimeDelta::seconds(rng.random_range(1..900));
        Milestone {
            service_started,
            service_completed: service_started + duration
        }
    }
}


/// File related to the Response
#[derive(Serialize, Deserialize, Debug, Described, Clone, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct File {
    /// Name of the file
    #[metadata(copyto="__text__")]
    pub name: String,
    /// SHA256 of the file
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
    /// Description of the file
    #[metadata(copyto="__text__")]
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
}


impl PartialEq for File {
    fn eq(&self, other: &Self) ->bool {
        self.sha256 == other.sha256
    }
}

impl File {
    pub fn new(sha256: Sha256, name: String) -> Self {
        File {
            name,
            sha256,
            description: Default::default(),
            classification: ClassificationString::default_unrestricted(),
            is_section_image: false,
            parent_relation: Default::default(),
            allow_dynamic_recursion: false
        }
    }
}

fn default_file_parent_relation() -> Text { Text("EXTRACTED".to_owned()) }

/// Response Body of Result
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct ResponseBody {
    /// Milestone block
    #[serde(default)]
    pub milestones: Milestone,
    /// Version of the service
    #[metadata(store=false)]
    pub service_version: String,
    /// Name of the service that scanned the file
    #[metadata(copyto="__text__")]
    pub service_name: ServiceName,
    /// Tool version of the service
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub service_tool_version: Option<String>,
    /// List of supplementary files
    #[serde(default)]
    pub supplementary: Vec<File>,
    /// List of extracted files
    #[serde(default)]
    pub extracted: Vec<File>,
    /// Context about the service
    #[serde(default)]
    #[metadata(index=false, store=false)]
    pub service_context: Option<String>,
    /// Debug info about the service
    #[serde(default)]
    #[metadata(index=false, store=false)]
    pub service_debug_info: Option<String>,
}


#[cfg(feature = "rand")]
impl rand::distr::Distribution<ResponseBody> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> ResponseBody {
        ResponseBody {
            milestones: rng.random(),
            service_version: random_word(rng),
            service_name: ServiceName::from_string(random_word(rng)),
            service_tool_version: None,
            supplementary: Default::default(),
            extracted: Default::default(),
            service_context: Default::default(),
            service_debug_info: Default::default(),
        }
    }
}


/// Result Model
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Result {
    /// Timestamp indicating when the result was archived.
    pub archive_ts: Option<DateTime<Utc>>,
    /// Aggregate classification for the result
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Date at which the result object got created
    #[serde(default="chrono::Utc::now")]
    pub created: DateTime<Utc>,
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// The body of the response from the service
    pub response: ResponseBody,
    /// The result body
    #[serde(default)]
    pub result: ResultBody,
    /// SHA256 of the file the result object relates to
    #[metadata(store=false)]
    pub sha256: Sha256,
    /// What type information is given along with this result
    #[serde(rename = "type")]
    pub result_type: Option<String>,
    /// ???
    pub size: Option<i32>,
    /// Use to not pass to other stages after this run
    #[serde(default)]
    pub drop_file: bool,
    /// Invalidate the current result cache creation
    #[serde(default)]
    pub partial: bool,
    /// Was loaded from the archive
    #[serde(default)]
    #[metadata(index=false)]
    pub from_archive: bool,
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Result> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Result {
        Result {
            archive_ts: None,
            classification: ExpandingClassification::try_unrestricted().unwrap(),
            created: chrono::Utc::now(),
            expiry_ts: None,
            response: rng.random(),
            result: Default::default(),
            sha256: rng.random(),
            result_type: None,
            size: None,
            partial: Default::default(),
            drop_file: Default::default(),
            from_archive: Default::default(),
        }
    }
}

impl Readable for Result {
    fn set_from_archive(&mut self, from_archive: bool) {
        self.from_archive = from_archive;
    }
}

impl Result {
    pub fn build_key(&self, task: Option<&Task>) -> std::result::Result<String, serde_json::Error> {
        Self::help_build_key(
            &self.sha256,
            &self.response.service_name,
            &self.response.service_version,
            self.is_empty(),
            self.partial,
            self.response.service_tool_version.as_deref(),
            task
        )
    }

    pub fn help_build_key(sha256: &Sha256, service_name: &str, service_version: &str, is_empty: bool, partial: bool, service_tool_version: Option<&str>, task: Option<&Task>) -> std::result::Result<String, serde_json::Error> {
        let mut key_list = vec![
            sha256.to_string(),
            service_name.replace('.', "_"),
            format!("v{}", service_version.replace('.', "_")),
            format!("c{}", generate_conf_key(service_tool_version, task, Some(partial))?),
        ];

        if is_empty {
            key_list.push("e".to_owned())
        }

        Ok(key_list.join("."))
    }

    pub fn scored_tag_dict(&self) -> std::result::Result<HashMap<String, TagEntry>, LayoutError> {
        let mut tags: HashMap<String, TagEntry> = Default::default();
        // Save the tags and their score
        for section in &self.result.sections {
            for tag in section.tags.to_list(None)? {
                let key = format!("{}:{}", tag.tag_type, tag.value);
                let entry = tags.entry(key).or_insert(tag);
                if let Some(heuristic) = &section.heuristic {
                    entry.score += heuristic.score;
                }
            }
        }
        Ok(tags)
    }

    pub fn is_empty(&self) -> bool {
        self.response.extracted.is_empty() && self.response.supplementary.is_empty() && self.result.sections.is_empty() && self.result.score == 0 && !self.partial
    }
}
