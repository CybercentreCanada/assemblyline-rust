use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{random_hex, ElasticMeta, ModelError};


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

impl TryFrom<&[u8]> for MD5 {
    type Error = ModelError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() == 16 {
            Ok(MD5(hex::encode(value)))
        } else {
            Err(ModelError::InvalidMd5(format!("Wrong number of bytes {}", value.len())))
        }
    }
}

impl From<[u8; 16]> for MD5 {
    fn from(value: [u8; 16]) -> Self {
        MD5(hex::encode(value))
    }
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<MD5> for rand::distr::StandardUniform {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> MD5 {
        MD5(random_hex(rng, 32))
    }
}


