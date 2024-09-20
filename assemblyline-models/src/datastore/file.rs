
use chrono::{DateTime, Utc};
use md5::Digest;
use serde::{Serialize, Deserialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{ElasticMeta, ExpandingClassification, Readable, SSDeepHash, Sha1, Sha256, Text, MD5};

/// Model of File
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct File {
    /// Dotted ASCII representation of the first 64 bytes of the file
    #[metadata(index=false, store=false)]
    pub ascii: String,
    /// Classification of the file
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Entropy of the file
    pub entropy: f32,
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    /// Is this an image from an Image Result Section?
    #[serde(default)]
    pub is_section_image: bool,
    /// Is this a file generated by a service?
    #[serde(default)]
    pub is_supplementary: bool,
    /// Hex dump of the first 64 bytes of the file
    #[metadata(index=false, store=false)]
    pub hex: String,
    /// List of labels of the file
    #[serde(default)]
    #[metadata(copyto="__text__")]
    pub labels: Vec<String>,
    /// Categories of label
    #[serde(default)]
    pub label_categories: LabelCategories,
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
    #[metadata(mapping="integer")]
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
    /// URI structure to speed up specialty file searching
    pub uri_info: Option<URIInfo>,
    /// List of comments made on a file
    #[serde(default)]
    pub comments: Vec<Comment>,
}

#[cfg(feature = "rand")]
impl rand::distributions::Distribution<File> for rand::distributions::Standard {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> File {
        let mut data = vec![];
        for _ in 0..1000 {
            data.push(rng.gen());
        }
        File::gen_for_sample(&data, rng)
    }
}

impl File {
    pub fn gen_for_sample<R: rand::Rng + ?Sized>(data: &[u8], rng: &mut R) -> File {
        let sha256 = hex::encode(sha2::Sha256::new().chain_update(data).finalize());
        let sha1 = hex::encode(sha1::Sha1::new().chain_update(data).finalize());
        let md5 = hex::encode(md5::Md5::new().chain_update(data).finalize());

        File {
            ascii: String::from_iter(data.iter().take(64).map(|byte| if byte.is_ascii() { *byte as char } else { '.' })),
            classification: ExpandingClassification {
                classification: "".to_string(),
                __access_lvl__: 0,
                __access_req__: vec![],
                __access_grp1__: vec![],
                __access_grp2__: vec![],
            },
            entropy: rng.gen_range(0.0..1.0),
            expiry_ts: None,
            is_section_image: rng.r#gen(),
            is_supplementary: rng.r#gen(),
            hex: String::from_iter(data.iter().take(64).map(|byte| if byte.is_ascii() { *byte as char } else { '.' })),
            labels: vec![],
            label_categories: Default::default(),
            md5: md5.parse().unwrap(),
            magic: "Binary data".to_string(),
            mime: Some("application/octet-stream".to_string()),
            seen: Seen { count: 1, first: chrono::Utc::now(), last: chrono::Utc::now() },
            sha1: sha1.parse().unwrap(),
            sha256: sha256.parse().unwrap(),
            size: data.len() as u64,
            ssdeep: rng.gen(),
            file_type: "unknown".to_string(),
            tlsh: None,
            from_archive: false,
            uri_info: None,
            comments: vec![],
        }
    }
}

impl Readable for File {
    fn set_from_archive(&mut self, from_archive: bool) {
        self.from_archive = from_archive;
    }
}

/// URI Information Model
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct URIInfo {
    /// full URI
    pub uri: String,

    // https://www.rfc-editor.org/rfc/rfc1808.html#section-2.1
    pub scheme: String,
    pub netloc: String,
    pub path: Option<String>,
    pub params: Option<String>,
    pub query: Option<String>,
    pub fragment: Option<String>,

    // Ease-of-use elements
    pub username: Option<String>,
    pub password: Option<String>,
    pub hostname: String,
    pub port: Option<u16>,
}

/// File Seen Model
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Seen {
    /// How many times have we seen this file?
    #[serde(default = "default_seen_count")]
    #[metadata(mapping="integer")]
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


/// Label Categories Model
#[derive(Debug, Serialize, Deserialize, Described, Clone, Default)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct LabelCategories {
    /// List of extra informational labels about the file
    pub info: Vec<String>,
    /// List of labels related to the technique used by the file and the signatures that hits on it.
    pub technique: Vec<String>,
    /// List of labels related to attribution of this file (implant name, actor, campain...)
    pub attribution: Vec<String>,
}

/// Comment Model
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Comment {
    /// Comment ID
    pub cid: String,
    /// Username of the user who made the comment
    pub uname: String,
    /// Datetime the comment was made on
    #[serde(default="Utc::now")]
    #[metadata(store=true)]
    pub date: DateTime<Utc>,
    /// Text of the comment written by the author
    pub text: Text,
    /// List of reactions made on a comment
    #[serde(default)]
    pub reactions: Vec<Reaction>,
}

/// Reaction Model
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Reaction {
    /// Icon of the user who made the reaction
    pub icon: ReactionsTypes,
    /// Username of the user who made the reaction
    pub uname: String,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="keyword")]
#[strum(serialize_all = "snake_case")]
pub enum ReactionsTypes {
    ThumbsUp, 
    ThumbsDown, 
    Love, 
    Smile, 
    Surprised, 
    Party
}
