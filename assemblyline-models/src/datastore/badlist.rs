use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;
use crate::types::classification::unrestricted_classification_string;
use crate::{ClassificationString, ElasticMeta, ExpandingClassification, Readable, SSDeepHash, Sha1, Sha256, UpperString, MD5};

// from assemblyline import odm
// from assemblyline.common import forge

// Classification = forge.get_classification()
#[derive(SerializeDisplay, DeserializeFromStr, Clone, Copy, strum::Display, strum::EnumString, Described, Debug, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum BadhashTypes {
    File,
    Tag,
}

#[derive(SerializeDisplay, DeserializeFromStr, Clone, Copy, strum::Display, strum::EnumString, Described, Debug, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum SourceTypes {
    User,
    External,
}

// SOURCE_TYPES = ["user", "external"]

/// Attribution Tag Model
#[derive(Debug, Serialize, Deserialize, Clone, Described, Default, PartialEq, Eq)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Attribution {
    /// Attribution Actor
    #[metadata(copyto="__text__")]
    pub actor: Option<Vec<UpperString>>,
    /// Attribution Campaign
    #[metadata(copyto="__text__")]
    pub campaign: Option<Vec<UpperString>>,
    /// Attribution Category
    #[metadata(copyto="__text__")]
    pub category: Option<Vec<UpperString>>,
    /// Attribution Exploit
    #[metadata(copyto="__text__")]
    pub exploit: Option<Vec<UpperString>>,
    /// Attribution Implant
    #[metadata(copyto="__text__")]
    pub implant: Option<Vec<UpperString>>,
    /// Attribution Family
    #[metadata(copyto="__text__")]
    pub family: Option<Vec<UpperString>>,
    /// Attribution Network
    #[metadata(copyto="__text__")]
    pub network: Option<Vec<UpperString>>,
}

impl Attribution {
    pub fn update(&mut self, mut other: Attribution) {
        macro_rules! update_field {($self: ident, $other: ident, $key: ident) => {
            match &mut $self.$key {
                Some(data) => {
                    data.append(&mut $other.$key.take().unwrap_or_default());
                    data.sort_unstable();
                    data.dedup();                    
                },
                None => {
                    $self.$key = $other.$key;
                }
            }; 
        };}

        update_field!(self, other, actor);
        update_field!(self, other, campaign);
        update_field!(self, other, category);
        update_field!(self, other, exploit);
        update_field!(self, other, implant);
        update_field!(self, other, family);
        update_field!(self, other, network);
    }
}

/// Hashes of a badlisted file
#[derive(Debug, Serialize, Deserialize, Clone, Described, Default, PartialEq, Eq)]
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
    /// SSDEEP
    #[metadata(copyto="__text__")]
    pub ssdeep: Option<SSDeepHash>,
    /// TLSH
    #[metadata(copyto="__text__")]
    pub tlsh: Option<String>,
}

impl Hashes {
    pub fn update(&mut self, other: Hashes) {
        macro_rules! update_field {($self: ident, $other: ident, $key: ident) => {
            $self.$key = $other.$key.or($self.$key.take())
        };}

        update_field!(self, other, md5);
        update_field!(self, other, sha1);
        update_field!(self, other, sha256);
        update_field!(self, other, ssdeep);
        update_field!(self, other, tlsh);
    }

    pub fn label_hash(&self) -> Option<String> {
        macro_rules! read_field {($self: ident, $key: ident) => {
            if let Some(val) = &($self.$key) {
                return Some(val.to_string());
            }
        };}

        read_field!(self, sha256);
        read_field!(self, sha1);
        read_field!(self, md5);
        read_field!(self, tlsh);
        read_field!(self, ssdeep);
        None
    }
}

/// File Details
#[derive(Debug, Serialize, Deserialize, Clone, Described, Default, PartialEq, Eq)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct File {
    /// List of names seen for that file
    #[metadata(store=true, copyto="__text__")]
    pub name: Vec<String>,
    /// Size of the file in bytes
    pub size: Option<i32>,
    /// Type of file as identified by Assemblyline
    #[serde(rename="type")]
    pub file_type: Option<String>,
}


/// Badlist source
#[derive(Debug, Serialize, Deserialize, Clone, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Source {
    /// Classification of the source
    #[serde(default="unrestricted_classification_string")]
    pub classification: ClassificationString,
    /// Name of the source
    #[metadata(store=true)]
    pub name: String,
    /// Reason for why file was badlisted
    pub reason: Vec<String>,
    /// Type of badlisting source
    #[serde(rename="type")]
    pub source_type: SourceTypes,
}

/// Tag associated to file
#[derive(Debug, Serialize, Deserialize, Clone, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Tag {
    /// Tag type
    #[serde(rename="type")]    
    pub tag_type: String,
    /// Tag value
    #[metadata(copyto="__text__")]
    pub value: String,
}


/// Badlist Model
#[derive(Debug, Serialize, Deserialize, Clone, Described, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Badlist {
    /// Date when the badlisted hash was added
    #[serde(default="chrono::Utc::now")]
    pub added: DateTime<Utc>,
    /// Attribution related to the bad hash
    #[serde(default)]
    pub attribution: Option<Attribution>,
    /// Computed max classification for the bad hash
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Is bad hash enabled or not?
    #[serde(default="default_true")]
    pub enabled: bool,
    /// When does this item expire from the list?
    #[serde(default)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// List of hashes related to the bad hash
    #[serde(default)]
    pub hashes: Hashes,
    /// Information about the file
    #[serde(default)]
    pub file: Option<File>,
    /// List of reasons why hash is badlisted
    pub sources: Vec<Source>,
    /// Information about the tag
    #[serde(default)]
    pub tag: Option<Tag>,
    /// Type of bad hash
    #[serde(rename="type")]
    pub hash_type: BadhashTypes,
    /// Last date when sources were added to the bad hash
    #[serde(default="chrono::Utc::now")]
    pub updated: DateTime<Utc>,
}

fn default_true() -> bool { true }

impl Readable for Badlist {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}

// if __name__ == "__main__":
//     from pprint import pprint
//     from assemblyline.odm.randomizer import random_model_obj
//     pprint(random_model_obj(Badlist, as_json=True))
