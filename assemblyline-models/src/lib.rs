use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;

use assemblyline_markings::classification::{ClassificationParser, NormalizeOptions};
use rand::{thread_rng, Rng};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

pub mod datastore;
pub mod config;
pub mod messages;
pub mod serialize;
pub mod meta;
pub mod types;

pub use meta::ElasticMeta;
pub use types::MD5;
pub use types::Sha1;

pub const HEXCHARS: [char; 16] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

pub trait Readable: for <'de> Deserialize<'de> {
    fn set_from_archive(&mut self, from_archive: bool);
}

impl Readable for JsonMap {
    fn set_from_archive(&mut self, from_archive: bool) {
        self.insert("from_json".to_owned(), serde_json::json!(from_archive));
    }
}

#[derive(Debug)]
pub enum ModelError {
    InvalidSha256(String),
    InvalidMd5(String),
    InvalidSha1(String),
    InvalidSid(String),
    InvalidSSDeep(String),
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
            ModelError::InvalidSSDeep(content) =>  f.write_fmt(format_args!("Invalid value provided for a ssdeep hash: {content}")),
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
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="keyword")]
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

impl From<&str> for UpperString {
    fn from(s: &str) -> Self {
        let value = s.trim().to_uppercase();
        UpperString{ value }
    }
}

/// sha256 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[metadata(mapping="keyword", normalizer="lowercase_normalizer")]
#[metadata_type(ElasticMeta)]
pub struct Sha256 {
    hex: String
}

// impl Described<ElasticMeta> for internment::ArcIntern<String> {
//     fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {
//         String::metadata()
//     }
// }

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

impl TryFrom<&[u8]> for Sha256 {
    type Error = ModelError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::from_str(&hex::encode(value))
    }
}

#[cfg(feature = "rand")]
pub fn random_hex<R: rand::prelude::Rng + ?Sized>(rng: &mut R, size: usize) -> String {
    let mut buffer = String::with_capacity(size);
    for _ in 0..size {
        let index = rng.gen_range(0..HEXCHARS.len());
        buffer.push(HEXCHARS[index]);
    }
    buffer
}

#[cfg(feature = "rand")]
impl rand::distributions::Distribution<Sha256> for rand::distributions::Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Sha256 {
        Sha256{hex: random_hex(rng, 64) }
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

#[cfg(feature = "rand")]
impl rand::distributions::Distribution<Sid> for rand::distributions::Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Sid {
        Sid(rng.gen())
    }
}

#[derive(Serialize, Deserialize, Described, PartialEq, Debug, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="text")]
pub struct Text(pub String);

impl From<&str> for Text {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for Text {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Unvalidated uuid type
pub type Uuid = String;

/// Unvalidated domain type
pub type Domain = String;

/// Unvalidated ip type
pub type IP = String;

/// Unvalidated uri type
pub type Uri = String;

/// Expanding classification type
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Eq)]
pub struct ExpandingClassification<const USER: bool=false> { 
    pub classification: String,
    __access_lvl__: i32,
    __access_req__: Vec<String>,
    __access_grp1__: Vec<String>,
    __access_grp2__: Vec<String>,
}

impl<const USER: bool> ExpandingClassification<USER> {
    pub fn new(classification: String, parser: &ClassificationParser) -> Result<Self, ModelError> {
        if parser.original_definition.enforce {
            let parts = parser.get_classification_parts(&classification, false, true, !USER)?;
            let classification = parser.get_normalized_classification_text(parts.clone(), false, false)?;

            Ok(Self {
                classification,
                __access_lvl__: parts.level,
                __access_req__: parts.required,
                __access_grp1__: if parts.groups.is_empty() { vec!["__EMPTY__".to_owned()] } else { parts.groups },
                __access_grp2__: if parts.subgroups.is_empty() { vec!["__EMPTY__".to_owned()] } else { parts.subgroups },
            })
        } else {
            Ok(Self {
                classification,
                __access_lvl__: Default::default(),
                __access_req__: Default::default(),
                __access_grp1__: vec!["__EMPTY__".to_owned()],
                __access_grp2__: vec!["__EMPTY__".to_owned()],
            })
        }
    }

    pub fn as_str(&self) -> &str {
        &self.classification
    }

    pub fn insert(parser: &ClassificationParser, output: &mut JsonMap, classification: &str) -> Result<(), ModelError> {
        use serde_json::json;
        if parser.original_definition.enforce {
            let parts = parser.get_classification_parts(classification, false, true, !USER)?;
            let classification = parser.get_normalized_classification_text(parts.clone(), false, false)?;

            output.insert("classification".to_string(), json!(classification));
            output.insert("__access_lvl__".to_string(), json!(parts.level));
            output.insert("__access_req__".to_string(), json!(parts.required));
            output.insert("__access_grp1__".to_string(), json!(if parts.groups.is_empty() { vec!["__EMPTY__".to_string()] } else { parts.groups }));
            output.insert("__access_grp2__".to_string(), json!(if parts.subgroups.is_empty() { vec!["__EMPTY__".to_string()] } else { parts.subgroups }));
            Ok(())
        } else {
            output.insert("classification".to_string(), json!(classification));
            output.insert("__access_lvl__".to_string(), json!(0));
            output.insert("__access_req__".to_string(), serde_json::Value::Array(Default::default()));
            output.insert("__access_grp1__".to_string(), json!(&["__EMPTY__"]));
            output.insert("__access_grp2__".to_string(), json!(&["__EMPTY__"]));
            Ok(())
        }
    }
}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct UncheckedClassification {
//     classification: String,
// }

// impl<const USER: bool> From<ExpandingClassification<USER>> for UncheckedClassification {
//     fn from(value: ExpandingClassification<USER>) -> Self {
//         UncheckedClassification { classification: value.classification }
//     }
// }

// impl<'de, const USER: bool> ValidatedDeserialize<'de, Arc<ClassificationParser>> for ExpandingClassification<USER> {
//     type ProxyType = UncheckedClassification;

//     fn validate(input: Self::ProxyType, validator: &Arc<ClassificationParser>) -> Result<Self, String> {
//         Self::new(input.classification, validator).map_err(|err| err.to_string())
//     }
// }

impl<const USER: bool> Described<ElasticMeta> for ExpandingClassification<USER> {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {

        // let group_meta = ElasticMeta {
        //     index: Some(true),
        //     store: None,
        //     ..Default::default()
        // };

        struct_metadata::Descriptor { 
            docs: None, 
            metadata: ElasticMeta{mapping: Some("classification"), ..Default::default()}, 
            kind: struct_metadata::Kind::new_struct("ExpandingClassification", vec![
                struct_metadata::Entry { label: "classification", docs: None, metadata: ElasticMeta{mapping: Some("classification"), ..Default::default()}, type_info: String::metadata(), has_default: false, aliases: &["classification"] },
                // struct_metadata::Entry { label: "__access_lvl__", docs: None, metadata: Default::default(), type_info: i32::metadata(), has_default: false, aliases: &["__access_lvl__"] },
                // struct_metadata::Entry { label: "__access_req__", docs: None, metadata: Default::default(), type_info: Vec::<String>::metadata(), has_default: false, aliases: &["__access_req__"] },
                // struct_metadata::Entry { label: "__access_grp1__", docs: None, metadata: Default::default(), type_info: Vec::<String>::metadata(), has_default: false, aliases: &["__access_grp1__"] },
                // struct_metadata::Entry { label: "__access_grp2__", docs: None, metadata: Default::default(), type_info: Vec::<String>::metadata(), has_default: false, aliases: &["__access_grp2__"] },
            ], &mut []),
            // kind: struct_metadata::Kind::Aliased { 
            //     name: "ExpandingClassification", 
            //     kind: Box::new(String::metadata())
            // },
        }
    }
}

/// A classification value stored as a string
#[derive(Serialize, Deserialize, Described, PartialEq, Debug, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="classification")]
pub struct ClassificationString(String);

impl From<ClassificationString> for String {
    fn from(value: ClassificationString) -> Self {
        value.0
    }
}

impl ClassificationString {
    pub fn new(classification: String, parser: &Arc<ClassificationParser>) -> Result<Self, ModelError> {
        Ok(Self(parser.normalize_classification_options(&classification, NormalizeOptions::short())?))
    }

    pub fn unrestricted(parser: &ClassificationParser) -> Self {
        Self(parser.unrestricted().to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// impl<'de> ValidatedDeserialize<'de, Arc<ClassificationParser>> for ClassificationString {
//     type ProxyType = String;

//     fn validate(input: Self::ProxyType, validator: &Arc<ClassificationParser>) -> Result<Self, String> {
//         Self::new(input, validator).map_err(|err| err.to_string())
//     }
// }


/// Unvalidated platform type
pub type Platform = String;

/// Unvalidated processor type
pub type Processor = String;

/// Validated ssdeep type
#[derive(SerializeDisplay, DeserializeFromStr, Described, PartialEq, Debug, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="text", analyzer="text_fuzzy")]
pub struct SSDeepHash(String);

impl std::fmt::Display for SSDeepHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn is_ssdeep_char(value: char) -> bool {
    value.is_ascii_alphanumeric() || value == '/' || value == '+'
}

impl std::str::FromStr for SSDeepHash {
    type Err = ModelError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // SSDEEP_REGEX = r"^[0-9]{1,18}:[a-zA-Z0-9/+]{0,64}:[a-zA-Z0-9/+]{0,64}$"
        let (numbers, hashes) = s.split_once(":").ok_or_else(||ModelError::InvalidSSDeep(s.to_owned()))?;
        let (hasha, hashb) = hashes.split_once(":").ok_or_else(||ModelError::InvalidSSDeep(s.to_owned()))?;
        if numbers.len() < 1 || numbers.len() > 18 || numbers.chars().any(|c|!c.is_ascii_digit()) {
            return Err(ModelError::InvalidSSDeep(s.to_owned()))
        }
        if hasha.len() > 64 || hasha.chars().any(|c|!is_ssdeep_char(c)) {
            return Err(ModelError::InvalidSSDeep(s.to_owned()))
        }
        if hashb.len() > 64 || hashb.chars().any(|c|!is_ssdeep_char(c)) {
            return Err(ModelError::InvalidSSDeep(s.to_owned()))
        }
        Ok(SSDeepHash(s.to_owned()))
    }
}

#[cfg(feature = "rand")]
impl rand::distributions::Distribution<SSDeepHash> for rand::distributions::Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> SSDeepHash {
        use rand::distributions::{Alphanumeric, DistString};
        let mut output = String::new();
        output += &rng.gen_range(0..10000).to_string();
        output += ":";
        let len = rng.gen_range(0..64);
        output += &Alphanumeric.sample_string(rng, len);
        output += ":";
        let len = rng.gen_range(0..64);
        output += &Alphanumeric.sample_string(rng, len);
        SSDeepHash(output)
    }
}

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

const WORDS: [&str; 187] = ["The", "Cyber", "Centre", "stays", "on", "the", "cutting", "edge", "of", "technology", "by", 
    "working", "with", "commercial", "vendors", "of", "cyber", "security", "technology", "to", "support", "their", 
    "development", "of", "enhanced", "cyber", "defence", "tools", "To", "do", "this", "our", "experts", "survey", 
    "the", "cyber", "security", "market", "evaluate", "emerging", "technologies", "in", "order", "to", "determine", 
    "their", "potential", "to", "improve", "cyber", "security", "across", "the", "country", "The", "Cyber", "Centre", 
    "supports", "innovation", "by", "collaborating", "with", "all", "levels", "of", "government", "private", "industry", 
    "academia", "to", "examine", "complex", "problems", "in", "cyber", "security", "We", "are", "constantly", 
    "engaging", "partners", "to", "promote", "an", "open", "innovative", "environment", "We", "invite", "partners", 
    "to", "work", "with", "us", "but", "also", "promote", "other", "Government", "of", "Canada", "innovation", 
    "programs", "One", "of", "our", "key", "partnerships", "is", "with", "the", "Government", "of", "Canada", "Build", 
    "in", "Canada", "Innovation", "Program", "BCIP", "The", "BCIP", "helps", "Canadian", "companies", "of", "all", 
    "sizes", "transition", "their", "state", "of", "the", "art", "goods", "services", "from", "the", "laboratory", 
    "to", "the", "marketplace", "For", "certain", "cyber", "security", "innovations", "the", "Cyber", "Centre", 
    "performs", "the", "role", "of", "technical", "authority", "We", "evaluate", "participating", "companies", 
    "new", "technology", "provide", "feedback", "in", "order", "to", "assist", "them", "in", "bringing", "their", 
    "product", "to", "market", "To", "learn", "more", "about", "selling", "testing", "an", "innovation", "visit", 
    "the", "BCIP", "website"];

#[cfg(feature = "rand")]
pub fn random_word<R: rand::Rng + ?Sized>(prng: &mut R) -> String {
    WORDS[prng.gen_range(0..WORDS.len())].to_string()
}

#[cfg(feature = "rand")]
pub fn random_words<R: rand::Rng + ?Sized>(prng: &mut R, count: usize) -> Vec<String> {
    let mut output = vec![];
    while output.len() < count {
        output.push(WORDS[prng.gen_range(0..WORDS.len())].to_string())
    }
    output
}


#[cfg(test)]
mod test {
    use rand::{thread_rng, Rng};

    use crate::{SSDeepHash, Sha1, Sha256, MD5};
    
    #[test]
    fn random_ssdeep() {
        let mut prng = thread_rng();
        for _ in 0..100 {
            let hash: SSDeepHash = prng.gen();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }

    #[test]
    fn random_sha256() {
        let mut prng = thread_rng();
        for _ in 0..100 {
            let hash: Sha256 = prng.gen();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }

    #[test]
    fn random_sha1() {
        let mut prng = thread_rng();
        for _ in 0..100 {
            let hash: Sha1 = prng.gen();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }

    #[test]
    fn random_md5() {
        let mut prng = thread_rng();
        for _ in 0..100 {
            let hash: MD5 = prng.gen();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }
}

