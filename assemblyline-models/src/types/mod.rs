mod ip;
mod md5;
mod sha1;
mod ja4;
pub mod classification;

pub use md5::MD5;
use serde::{Deserialize, Serialize};
pub use sha1::Sha1;
pub use ja4::JA4;
use struct_metadata::Described;

use crate::ElasticMeta;


#[derive(Serialize, Described, Debug, Clone, Copy, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
pub struct NonZeroInteger(u64);

impl<'de> Deserialize<'de> for NonZeroInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> 
    {
        let raw: i64 = i64::deserialize(deserializer)?;
        Ok(NonZeroInteger(raw.min(1) as u64))
    }
}