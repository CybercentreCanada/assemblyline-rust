use std::fmt::Display;
use std::str::FromStr;

use assemblyline_markings::classification::NormalizeOptions;
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

pub mod datastore;
pub mod config;
pub mod messages;
pub mod serialize;
mod meta;

pub use meta::ElasticMeta;

#[derive(Debug)]
pub enum ModelError {
    InvalidSha256(String),
    InvalidMd5(String),
    InvalidSha1(String),
    InvalidSid(String),
    ClassificationNotInitialized,
    InvalidClassification(Option<assemblyline_markings::errors::Errors>),
}

impl Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelError::InvalidSha256(content) => f.write_fmt(format_args!("Invalid value provided for a sha256: {content}")),
            ModelError::InvalidMd5(content) => f.write_fmt(format_args!("Invalid value provided for a md5: {content}")),
            ModelError::InvalidSha1(content) => f.write_fmt(format_args!("Invalid value provided for a sha1: {content}")),
            ModelError::InvalidSid(content) => f.write_fmt(format_args!("Invalid value provided for a sid: {content}")),
            ModelError::ClassificationNotInitialized => f.write_str("The classification engine has not been initialized."),
            ModelError::InvalidClassification(_) => f.write_str("An invalid classification string was provided."),
        }
    }
}

impl From<base62::DecodeError> for ModelError {
    fn from(value: base62::DecodeError) -> Self {
        Self::InvalidSid(value.to_string())
    }
}

impl From<assemblyline_markings::errors::Errors> for ModelError {
    fn from(value: assemblyline_markings::errors::Errors) -> Self {
        Self::InvalidClassification(Some(value))
    }
}

impl std::error::Error for ModelError {}

/// Short name for serde json's basic map type
pub type JsonMap = serde_json::Map<String, serde_json::Value>;

/// Uppercase String
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described)]
#[metadata_type(ElasticMeta)]
pub struct UpperString {
    value: String
}

impl std::fmt::Display for UpperString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

impl std::ops::Deref for UpperString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::str::FromStr for UpperString {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let value = s.trim().to_uppercase();
        Ok(UpperString{ value })
    }
}


/// sha256 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, Hash, PartialEq, Eq)]
#[metadata(mapping="keyword", normalizer="lowercase_normalizer")]
#[metadata_type(ElasticMeta)]
pub struct Sha256 {
    hex: String
}

impl std::fmt::Display for Sha256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex)
    }
}

impl std::ops::Deref for Sha256 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.hex
    }
}

impl FromStr for Sha256 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 64 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(ModelError::InvalidSha256(hex))
        }
        Ok(Sha256{ hex })
    }
}

/// MD5 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct MD5 {
    hex: String
}

impl std::fmt::Display for MD5 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex)
    }
}

impl std::ops::Deref for MD5 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.hex
    }
}

impl std::str::FromStr for MD5 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 32 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(ModelError::InvalidMd5(hex))
        }
        Ok(MD5{ hex })
    }
}


/// Sha1 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone)]
#[metadata_type(ElasticMeta)]
pub struct Sha1 {
    hex: String
}

impl std::fmt::Display for Sha1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex)
    }
}

impl std::ops::Deref for Sha1 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.hex
    }
}

impl std::str::FromStr for Sha1 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 40 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(ModelError::InvalidSha1(hex))
        }
        Ok(Sha1{ hex })
    }
}

/// Validated uuid type with base62 encoding
#[derive(SerializeDisplay, DeserializeFromStr, Debug, Described, Hash, PartialEq, Eq, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="keyword")]
pub struct Sid(u128);

impl std::fmt::Display for Sid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base62::encode(self.0))
    }
}

impl std::str::FromStr for Sid {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Sid(base62::decode(s)?))
    }
}

impl Sid {
    pub fn assign(&self, bins: usize) -> usize {
        (self.0 % bins as u128) as usize
    }
}

#[derive(Serialize, Deserialize, Described, PartialEq, Debug, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="text")]
pub struct Text(String);

/// Unvalidated uuid type
pub type Uuid = String;

/// Unvalidated domain type
pub type Domain = String;

/// Unvalidated ip type
pub type IP = String;

/// Unvalidated uri type
pub type Uri = String;

/// 
pub static DEFAULT_CLASSIFICATION_PARSER: std::sync::Mutex<Option<assemblyline_markings::classification::ClassificationParser>> = std::sync::Mutex::new(None);

/// Expanding classification type
#[derive(Serialize, PartialEq, Debug, Clone)]
// #[metadata_type(ElasticMeta)]
// #[metadata(mapping="classification")]
// #[metadata(index=true, store=true)]
pub struct ExpandingClassification<const USER: bool=false> { 
    classification: String, 
    __access_lvl__: i32,
    __access_req__: Vec<String>,
    __access_grp1__: Vec<String>,
    __access_grp2__: Vec<String>,
}

impl<const USER: bool> ExpandingClassification<USER> {
    fn new(classification: String) -> Result<Self, ModelError> {
        let parser = DEFAULT_CLASSIFICATION_PARSER.lock().or_else(|_| Err(ModelError::ClassificationNotInitialized))?;
        let parser = parser.as_ref().ok_or(ModelError::ClassificationNotInitialized)?;

        let parts = parser.get_classification_parts(&classification, false, true, !USER)?;
        let classification = parser.get_normalized_classification_text(parts.clone(), false, false)?;

        Ok(Self {
            classification,
            __access_lvl__: parts.level,
            __access_req__: parts.required,
            __access_grp1__: if parts.groups.is_empty() { vec!["__EMPTY__".to_owned()] } else { parts.groups },
            __access_grp2__: if parts.subgroups.is_empty() { vec!["__EMPTY__".to_owned()] } else { parts.subgroups },
        })
    }
}

impl<'de> Deserialize<'de> for ExpandingClassification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl<const USER: bool> Described<ElasticMeta> for ExpandingClassification<USER> {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {
        struct_metadata::Descriptor { 
            docs: None, 
            metadata: ElasticMeta{mapping: Some("classification"), ..Default::default()}, 
            kind: struct_metadata::Kind::Aliased { 
                name: "ExpandingClassification", 
                kind: Box::new(String::metadata())
            },
        }
    }
}

/// A classification value stored as a string
#[derive(Serialize, Described, PartialEq, Debug, Clone)]
#[metadata_type(ElasticMeta)]
pub struct ClassificationString(String);

impl ClassificationString {
    fn new(classification: String) -> Result<Self, ModelError> {
        let parser = DEFAULT_CLASSIFICATION_PARSER.lock().map_err(|_| ModelError::ClassificationNotInitialized)?;
        let parser = parser.as_ref().ok_or(ModelError::ClassificationNotInitialized)?;

        Ok(Self(parser.normalize_classification_options(&classification, NormalizeOptions::short())?))
    }
}

impl<'de> Deserialize<'de> for ClassificationString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}


/// Unvalidated platform type
pub type Platform = String;

/// Unvalidated processor type
pub type Processor = String;

/// Unvalidated ssdeep type
pub type SSDeepHash = String;

/// Unvalidated phone number type
pub type PhoneNumber = String;

/// Unvalidated MAC type
pub type Mac = String;

/// Unvalidated UNCPath type
pub type UNCPath = String;

/// Unvalidated UriPath type
pub type UriPath = String;

/// Unvalidated Email type
pub type Email = String;
