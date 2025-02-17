
use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

use crate::{Domain, ElasticMeta, ExpandingClassification, Sha1, Sha256, Uri, Uuid, MD5};
use super::workflow::{Statuses, Priorities};


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, Debug)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum ExtendedScanValues {
    Submitted,
    Skipped,
    Incomplete,
    Complete,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum ItemVerdict {
    Safe,
    Info,
    Suspicious,
    Malicious,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum EntityType {
    User,
    Workflow,
}


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Subtype {
    Exp,
    Cfg,
    Ob,
    Imp,
    Ta,
}


/// Assemblyline Results Block
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct DetailedItem {
    /// Type of data that generated this item
    #[serde(rename = "type")]
    pub item_type: String,
    /// Value of the item
    pub value: String,
    /// Verdict of the item
    pub verdict: ItemVerdict,
    /// Sub-type of the item
    pub subtype: Option<Subtype>,
}

/// Assemblyline Detailed result block
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct DetailedResults {
    /// List of detailed Att&ck patterns
    #[serde(default)]
    pub attack_pattern: Vec<DetailedItem>,
    /// List of detailed Att&ck categories
    #[serde(default)]
    pub attack_category: Vec<DetailedItem>,
    /// List of detailed attribution
    #[serde(default)]
    pub attrib: Vec<DetailedItem>,
    /// List of detailed AV hits
    #[serde(default)]
    pub av: Vec<DetailedItem>,
    /// List of detailed behaviors for the alert
    #[serde(default)]
    pub behavior: Vec<DetailedItem>,
    /// List of detailed domains
    #[serde(default)]
    pub domain: Vec<DetailedItem>,
    /// List of detailed heuristics
    #[serde(default)]
    pub heuristic: Vec<DetailedItem>,
    /// List of detailed IPs
    #[serde(default)]
    pub ip: Vec<DetailedItem>,
    /// List of detailed URIs
    #[serde(default)]
    pub uri: Vec<DetailedItem>,
    /// List of detailed YARA rule hits
    #[serde(default)]
    pub yara: Vec<DetailedItem>,
}

/// Assemblyline Results Block
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct ALResults {
    /// List of attribution
    #[serde(default)]
    #[metadata(store=true, copyto="__text__")]
    pub attrib: Vec<String>,
    /// List of AV hits
    #[serde(default)]
    #[metadata(store=true, copyto="__text__")]
    pub av: Vec<String>,
    /// List of behaviors for the alert
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub behavior: Vec<String>,
    /// Assemblyline Detailed result block
    pub detailed: DetailedResults,
    /// List of all domains
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub domain: Vec<Domain>,
    /// List of domains found during Dynamic Analysis
    #[serde(default)]
    pub domain_dynamic: Vec<Domain>,
    /// List of domains found during Static Analysis
    #[serde(default)]
    pub domain_static: Vec<Domain>,
    /// List of all IPs
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub ip: Vec<std::net::IpAddr>,
    /// List of IPs found during Dynamic Analysis
    #[serde(default)]
    pub ip_dynamic: Vec<std::net::IpAddr>,
    /// List of IPs found during Static Analysis
    #[serde(default)]
    pub ip_static: Vec<std::net::IpAddr>,
    /// Finish time of the Assemblyline submission
    #[serde(default)]
    #[metadata(index=false)]
    pub request_end_time: DateTime<Utc>,
    /// Maximum score found in the submission
    #[serde(default)]
    #[metadata(store=true)]
    pub score: i32,
    /// List of all URIs
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub uri: Vec<Uri>,
    /// List of URIs found during Dynamic Analysis
    #[serde(default)]
    pub uri_dynamic: Vec<Uri>,
    /// List of URIs found during Static Analysis
    #[serde(default)]
    pub uri_static: Vec<Uri>,
    /// List of YARA rule hits
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub yara: Vec<String>,
}

/// File Block Associated to the Top-Level/Root File of Submission
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct File {
    /// MD5 hash of file
    #[metadata(copyto="__text__")]
    pub md5: MD5,
    /// Name of the file
    #[metadata(copyto="__text__")]
    pub name: String,
    /// SHA1 hash of the file
    #[metadata(copyto="__text__")]
    pub sha1: Sha1,
    /// SHA256 hash of the file
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
    /// Size of the file in bytes
    #[metadata(store=false)]
    pub size: i32,
    /// Type of file as identified by Assemblyline
    #[serde(rename = "type")]
    #[metadata(copyto="__text__")]
    pub file_type: String,
    /// Screenshots taken of the file during analysis, if applicable.
    #[serde(default)]
    pub screenshots: Vec<Screenshot>,
}

/// Stores information about screenshots taken during the analysis of the file. Each screenshot has a name, description, and the hashes of the image and its thumbnail, offering a visual reference that can aid in manual review processes.
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Screenshot {
    /// The name or title of the screenshot.
    pub name: String,
    /// A brief description of the screenshot's content.
    pub description: String,
    /// The SHA256 hash of the full-size screenshot image.
    pub img: Sha256,
    /// The SHA256 hash of the thumbnail version of the screenshot.
    pub thumb: Sha256,
}

/// Verdict Block of Submission
#[derive(Serialize, Deserialize, Default, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Verdict {
    /// List of users that claim submission as malicious
    #[serde(default)]
    pub malicious: Vec<String>,
    /// List of users that claim submission as non-malicious
    #[serde(default)]
    pub non_malicious: Vec<String>,
}

/// Heuristic Block
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Heuristic {
    /// List of related Heuristic names
    #[serde(default)]
    pub name: Vec<String>,
}

/// ATT&CK Block
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Attack {
    /// List of related ATT&CK patterns
    #[serde(default)]
    pub pattern: Vec<String>,
    /// List of related ATT&CK categories
    #[serde(default)]
    pub category: Vec<String>,
}

/// Model of Workflow Event
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Event {
    /// Type of entity associated to event
    pub entity_type: EntityType,
    /// ID of entity associated to event
    pub entity_id: String,
    /// Name of entity
    pub entity_name: String,
    /// Timestamp of event
    #[serde(default="chrono::Utc::now")]
    pub ts: DateTime<Utc>,
    /// Labels added during event
    #[serde(default)]
    pub labels: Vec<String>,
    /// Labels that were removed from the alert during the event.
    #[serde(default)]    
    pub labels_removed: Vec<String>, 
    /// Status applied during event
    #[serde(default)]    
    pub status: Option<Statuses>,
    /// Priority applied during event
    #[serde(default)]    
    pub priority: Option<Priorities>,
}

/// Describes the relationship between different submissions that are linked to the formation of the alert, highlighting parent-child connections.
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Relationship {
    /// The identifier of the child submission in the relationship.
    pub child: Uuid,
    /// The identifier of the parent submission, if applicable.
    #[serde(default)]
    pub parent: Option<Uuid>,
}

/// Model for Alerts
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Alert {
    /// ID of the alert
    #[metadata(copyto="__text__")]
    pub alert_id: String,
    /// Assemblyline Result Block
    pub al: ALResults,
    /// Timestamp indicating when the alert was archived in the system.
    pub archive_ts: Option<chrono::DateTime<chrono::Utc>>,
    /// ATT&CK Block
    pub attack: Attack,
    /// Classification of the alert
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// Status of the extended scan
    pub extended_scan: ExtendedScanValues,
    /// File Block
    pub file: File,
    /// Are the alert results filtered?
    #[serde(default)]
    pub filtered: bool,
    /// Heuristic Block
    pub heuristic: Heuristic,
    /// List of labels applied to the alert
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub label: Vec<String>,
    /// Metadata submitted with the file
    #[serde(default)]
    #[metadata(store=false, mapping="flattenedobject")]
    pub metadata: HashMap<String, String>,
    /// Owner of the alert
    pub owner: Option<String>,
    /// Priority applied to the alert
    pub priority: Option<Priorities>,
    /// Alert creation timestamp
    pub reporting_ts: DateTime<Utc>,
    /// Describes the hierarchical relationships between submissions that contributed to this alert.
    pub submission_relations: Vec<Relationship>,
    /// Submission ID related to this alert
    pub sid: String,
    /// Status applied to the alert
    pub status: Option<Statuses>,
    /// File submission timestamp
    pub ts: DateTime<Utc>,
    /// Type of alert
    #[serde(rename = "type")]
    pub alert_type: String,
    /// Verdict Block
    #[serde(default)]
    pub verdict: Verdict,
    /// An audit of events applied to alert
    #[serde(default)]
    pub events: Vec<Event>,
    /// Have all workflows ran on this alert?
    #[serde(default)]
    pub workflows_completed: bool,
}