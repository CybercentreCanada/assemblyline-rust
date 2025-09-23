use std::str::FromStr;

use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{ElasticMeta, ModelError};


/// sha256 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
#[metadata(normalizer="lowercase_normalizer")]
#[metadata_type(ElasticMeta)]
pub struct Sha256(String);

// impl Described<ElasticMeta> for internment::ArcIntern<String> {
//     fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {
//         String::metadata()
//     }
// }

impl std::fmt::Display for Sha256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for Sha256 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub fn is_sha256(hex: &str) -> bool {
    hex.len() == 64 && hex.chars().all(|c|c.is_ascii_hexdigit())
}

impl FromStr for Sha256 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if !is_sha256(&hex) {
            return Err(ModelError::InvalidSha256(hex))
        }
        Ok(Sha256(hex))
    }
}

impl TryFrom<&[u8]> for Sha256 {
    type Error = ModelError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Self::from_str(&hex::encode(value))
    }
}


#[cfg(feature = "rand")]
impl rand::distr::Distribution<Sha256> for rand::distr::StandardUniform {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Sha256 {
        use crate::random_hex;

        Sha256(random_hex(rng, 64))
    }
}
