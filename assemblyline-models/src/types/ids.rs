use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{ElasticMeta, ModelError};



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
impl rand::distr::Distribution<Sid> for rand::distr::StandardUniform {
    fn sample<R: rand::prelude::Rng + ?Sized>(&self, rng: &mut R) -> Sid {
        Sid(rng.random())
    }
}

/// Uuid type
#[derive(SerializeDisplay, DeserializeFromStr, Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct Uuid(uuid::Uuid);

impl Described<ElasticMeta> for Uuid {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {
        struct_metadata::Descriptor {
            docs: None,
            metadata: ElasticMeta { mapping: Some("keyword"), ..Default::default() },
            kind: struct_metadata::Kind::String,
        }
    }
}

impl std::fmt::Display for Uuid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl std::str::FromStr for Uuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Uuid(s.parse()?))
    }
}