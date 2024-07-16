use std::fmt::Display;
use std::str::FromStr;

use assemblyline_markings::classification::{ClassificationParser, NormalizeOptions};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

pub mod datastore;
pub mod config;
pub mod messages;
pub mod serialize;
pub mod meta;

pub use meta::ElasticMeta;
use validation_boilerplate::ValidatedDeserialize;

pub const HEXCHARS: [char; 16] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

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
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone)]
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
        Sha256{hex: random_hex(rng, 64)}
    }
}

/// MD5 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(normalizer="lowercase_normalizer")]
pub struct MD5(String);

impl std::fmt::Display for MD5 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for MD5 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for MD5 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 32 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(ModelError::InvalidMd5(hex))
        }
        Ok(MD5(hex))
    }
}

#[cfg(feature = "rand")]
impl rand::distributions::Distribution<MD5> for rand::distributions::Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> MD5 {
        MD5(random_hex(rng, 32))
    }
}

/// Sha1 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(normalizer="lowercase_normalizer")]
pub struct Sha1(String);

impl std::fmt::Display for Sha1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for Sha1 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for Sha1 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 40 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(ModelError::InvalidSha1(hex))
        }
        Ok(Sha1(hex))
    }
}

#[cfg(feature = "rand")]
impl rand::distributions::Distribution<Sha1> for rand::distributions::Standard {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Sha1 {
        Sha1(random_hex(rng, 40))
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

/// Unvalidated uuid type
pub type Uuid = String;

/// Unvalidated domain type
pub type Domain = String;

/// Unvalidated ip type
pub type IP = String;

/// Unvalidated uri type
pub type Uri = String;

/// Expanding classification type
#[derive(Serialize, PartialEq, Debug, Clone, Eq)]
pub struct ExpandingClassification<const USER: bool=false> { 
    classification: String, 
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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UncheckedClassification {
    classification: String,
}

impl<const USER: bool> From<ExpandingClassification<USER>> for UncheckedClassification {
    fn from(value: ExpandingClassification<USER>) -> Self {
        UncheckedClassification { classification: value.classification }
    }
}

impl<'de, const USER: bool> ValidatedDeserialize<'de, ClassificationParser> for ExpandingClassification<USER> {
    type ProxyType = UncheckedClassification;

    fn validate(input: Self::ProxyType, validator: &ClassificationParser) -> Result<Self, String> {
        Self::new(input.classification, validator).map_err(|err| err.to_string())
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

impl From<ClassificationString> for String {
    fn from(value: ClassificationString) -> Self {
        value.0
    }
}

impl ClassificationString {
    pub fn new(classification: String, parser: &ClassificationParser) -> Result<Self, ModelError> {
        Ok(Self(parser.normalize_classification_options(&classification, NormalizeOptions::short())?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> ValidatedDeserialize<'de, ClassificationParser> for ClassificationString {
    type ProxyType = String;

    fn validate(input: Self::ProxyType, validator: &ClassificationParser) -> Result<Self, String> {
        Self::new(input, validator).map_err(|err| err.to_string())
    }
}


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