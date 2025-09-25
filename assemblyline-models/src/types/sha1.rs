use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{random_hex, ElasticMeta, ModelError};



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

pub fn is_sha1(hex: &str) -> bool {
    hex.len() == 40 && hex.chars().all(|c|c.is_ascii_hexdigit())
}

impl std::str::FromStr for Sha1 {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if !is_sha1(&hex) {
            return Err(ModelError::InvalidSha1(hex))
        }
        Ok(Sha1(hex))
    }
}

#[cfg(feature = "rand")]
impl rand::distr::Distribution<Sha1> for rand::distr::StandardUniform {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Sha1 {
        Sha1(random_hex(rng, 40))
    }
}
