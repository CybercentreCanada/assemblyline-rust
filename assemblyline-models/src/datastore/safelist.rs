use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{ClassificationString, ElasticMeta, ExpandingClassification, Readable, Sha1, Sha256, MD5};

use super::badlist::SourceTypes;

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, Debug, Clone, Copy, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum SafehashTypes {
    File, 
    Tag, 
    Signature
}

/// Hashes of a safelisted file
#[derive(Debug, Serialize, Deserialize, Described, Default, PartialEq, Eq)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Hashes {
    /// MD5
    #[metadata(copyto="__text__")]
    pub md5: Option<MD5>,
    /// SHA1
    #[metadata(copyto="__text__")]
    pub sha1: Option<Sha1>,
    /// SHA256
    #[metadata(copyto="__text__")]
    pub sha256: Option<Sha256>,
}

/// File Details
#[derive(Debug, Serialize, Deserialize, Described, Default, PartialEq, Eq)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct File {
    /// List of names seen for that file
    #[metadata(store=true, copyto="__text__")]    
    pub name: Vec<String>,
    /// Size of the file in bytes
    #[metadata(mapping="integer")]
    pub size: Option<u64>,
    /// Type of file as identified by Assemblyline
    #[serde(rename="type")]
    pub type_: Option<String>,
}

/// Safelist source
#[derive(Debug, Serialize, Deserialize, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Source {
    /// Classification of the source
    pub classification: ClassificationString,
    /// Name of the source
    #[metadata(store=true)]
    pub name: String,
    /// Reason for why file was safelisted
    pub reason: Vec<String>,
    /// Type of safelisting source
    #[serde(rename="type")]
    pub type_: SourceTypes
}

/// Tag associated to file
#[derive(Debug, Serialize, Deserialize, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Tag {
    /// Tag type
    #[serde(rename="type")]
    pub type_: String,
    /// Tag value
    #[metadata(copyto="__text__")]
    pub value: String,
}

/// Signature
#[derive(Debug, Serialize, Deserialize, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Signature {
    /// Name of the signature
    #[metadata(copyto="__text__")]
    pub name: String,
}

/// Safelist Model
#[derive(Debug, Serialize, Deserialize, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Safelist {
    /// Date when the safelisted hash was added
    pub added: DateTime<Utc>,
    /// Computed max classification for the safe hash
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Is safe hash enabled or not?
    #[serde(default="default_enabled")]
    pub enabled: bool,
    /// When does this item expire from the list?
    pub expiry_ts: Option<DateTime<Utc>>,
    /// List of hashes related to the safe hash
    #[serde(default)]
    pub hashes: Hashes,
    /// Information about the file
    pub file: Option<File>,
    /// List of reasons why hash is safelisted
    pub sources: Vec<Source>,
    /// Information about the tag
    pub tag: Option<Tag>,
    /// Information about the signature
    pub signature: Option<Signature>,
    /// Type of safe hash
    #[serde(rename="type")]
    pub type_: SafehashTypes,
    /// Last date when sources were added to the safe hash
    pub updated: DateTime<Utc>,
}

fn default_enabled() -> bool { true }

impl Readable for Safelist {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}