pub mod ip;
pub mod md5;
pub mod sha1;
pub mod sha256;
pub mod ja4;
pub mod ssdeep;
pub mod classification;
pub mod mapping_keys;
pub mod strings;
pub mod json_validation;
pub mod ids;
pub mod net_static;

use std::ops::Deref;

pub use ssdeep::{SSDeepHash};
pub use strings::{Wildcard, ServiceName, UpperString, Text, Domain, Email, Uri};
pub use md5::MD5;
pub use sha256::Sha256;
pub use sha1::Sha1;
pub use ja4::JA4;
pub use ids::{Sid, Uuid};
pub use classification::{ClassificationString, ExpandingClassification};

use struct_metadata::Described;
use serde::{Deserialize, Serialize};

use crate::{ElasticMeta, Readable};

/// Short name for serde json's basic map type
pub type JsonMap = serde_json::Map<String, serde_json::Value>;

impl Readable for JsonMap {
    fn set_from_archive(&mut self, from_archive: bool) {
        self.insert("from_json".to_owned(), serde_json::json!(from_archive));
    }
}

#[derive(Serialize, Described, Debug, Clone, Copy, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
pub struct NonZeroInteger(u64);

impl<'de> Deserialize<'de> for NonZeroInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>
    {
        let raw: i64 = i64::deserialize(deserializer)?;
        Ok(NonZeroInteger(raw.max(1) as u64))
    }
}

impl Deref for NonZeroInteger {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[test]
fn non_zero() {
    assert_eq!(*serde_json::from_str::<NonZeroInteger>("0").unwrap(), 1);
    assert_eq!(*serde_json::from_str::<NonZeroInteger>("-1").unwrap(), 1);
    assert_eq!(*serde_json::from_str::<NonZeroInteger>("100").unwrap(), 100);
}