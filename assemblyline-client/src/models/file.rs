use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

use crate::Sha256;

use super::{Classification, MD5, SSDeepHash, Sha1};



/// File Seen Model
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
pub struct File {
    /// Dotted ASCII representation of the first 64 bytes of the file
    pub ascii: String,
    /// Classification of the file
    pub classification: Classification,
    /// Entropy of the file
    pub entropy: f64,
    /// Expiry timestamp
    pub expiry_ts: Option<DateTime<Utc>>,
    /// Is this an image from an Image Result Section?
    #[serde(default)]
    pub is_section_image: bool,
    /// Hex dump of the first 64 bytes of the file
    pub hex: String,
    /// MD5 of the file
    pub md5: MD5,
    /// Output from libmagic related to the file
    pub magic: String,
    /// MIME type of the file as identified by libmagic
    pub mime: Option<String>,
    /// Details about when the file was seen
    #[serde(default)]
    pub seen: Seen,
    /// SHA1 hash of the file
    pub sha1: Sha1,
    /// SHA256 hash of the file
    pub sha256: Sha256,
    /// Size of the file in bytes
    pub size: u64,
    /// SSDEEP hash of the file
    pub ssdeep: SSDeepHash,
    /// Type of file as identified by Assemblyline
    #[serde(rename = "type")]
    pub file_type: String,
    /// TLSH hash of the file"
    pub tlsh: Option<String>,
    /// Was loaded from the archive
    #[serde(default)]
    pub from_archive: bool,
}