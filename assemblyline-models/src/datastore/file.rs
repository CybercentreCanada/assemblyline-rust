use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use struct_metadata::Described;

use crate::{Classification, Sha256, MD5, SSDeepHash, Sha1, ElasticMeta};



/// File Seen Model
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Seen {
    /// How many times have we seen this file?
    #[serde(default = "default_seen_count")]
    pub count: u64,
    /// First seen timestamp
    #[serde(default = "default_now")]
    pub first: DateTime<Utc>,
    /// Last seen timestamp
    #[serde(default = "default_now")]
    pub last: DateTime<Utc>,
}

fn default_seen_count() -> u64 { 1 }
fn default_now() -> DateTime<Utc> { Utc::now() }

impl Default for Seen {
    fn default() -> Self {
        Self {
            count: default_seen_count(),
            first: default_now(),
            last: default_now()
        }
    }
}

/// Model of File
#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct File {
    /// Dotted ASCII representation of the first 64 bytes of the file
    #[metadata(index=false, store=false)]
    pub ascii: String,
    /// Classification of the file
    pub classification: Classification,
    /// Entropy of the file
    pub entropy: f64,
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// Is this an image from an Image Result Section?
    #[serde(default)]
    pub is_section_image: bool,
    /// Hex dump of the first 64 bytes of the file
    #[metadata(index=false, store=false)]
    pub hex: String,
    /// MD5 of the file
    #[metadata(copyto="__text__")]
    pub md5: MD5,
    /// Output from libmagic related to the file
    #[metadata(store=false)]
    pub magic: String,
    /// MIME type of the file as identified by libmagic
    #[metadata(store=false)]
    pub mime: Option<String>,
    /// Details about when the file was seen
    #[serde(default)]
    pub seen: Seen,
    /// SHA1 hash of the file
    #[metadata(copyto="__text__")]
    pub sha1: Sha1,
    /// SHA256 hash of the file
    #[metadata(copyto="__text__")]
    pub sha256: Sha256,
    /// Size of the file in bytes
    pub size: u64,
    /// SSDEEP hash of the file
    #[metadata(store=false)]
    pub ssdeep: SSDeepHash,
    /// Type of file as identified by Assemblyline
    #[serde(rename = "type")]
    #[metadata(copyto="__text__")]
    pub file_type: String,
    /// TLSH hash of the file"
    #[metadata(copyto="__text__")]
    pub tlsh: Option<String>,
    /// Was loaded from the archive
    #[serde(default)]
    #[metadata(index=false, store=false)]
    pub from_archive: bool,
}