use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use struct_metadata::Described;

use crate::{ElasticMeta, ModelError};


/// A string that maps to a keyword field in elasticsearch.
/// 
/// This is the default behaviour for a String in a mapped struct, the only reason
/// to use this over a standard String is cases where the 'mapping' field has been overwritten
/// by a container and the more explicit 'mapping' this provided is needed to reassert
/// the keyword type.
/// 
/// Example:
///         #[metadata(store=false, mapping="flattenedobject")]
///         pub safelisted_tags: HashMap<String, Vec<Keyword>>,
/// 
/// In that example, if the inner Keyword was String the entire HashMap would have its 
/// mapping set to 'flattenedobject', the inner Keyword more explicitly overrides this.
#[derive(Debug, Serialize, Deserialize, Described, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="keyword")]
pub struct Keyword(String);

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for Keyword {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Keyword {
    fn from(s: String) -> Self {
        Keyword(s)
    }
}

impl From<&str> for Keyword {
    fn from(s: &str) -> Self {
        Keyword(s.to_string())
    }
}


#[derive(Debug, Serialize, Deserialize, Described, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[metadata_type(ElasticMeta)]
#[metadata(mapping="wildcard")]
pub struct Wildcard(String);

impl std::fmt::Display for Wildcard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for Wildcard {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Wildcard {
    fn from(s: String) -> Self {
        Wildcard(s)
    }
}

impl From<&str> for Wildcard {
    fn from(s: &str) -> Self {
        Wildcard(s.to_string())
    }
}


/// Uppercase String
#[derive(Debug, SerializeDisplay, DeserializeFromStr, Described, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[metadata_type(ElasticMeta)]
pub struct UpperString(String);


impl std::fmt::Display for UpperString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for UpperString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for UpperString {
    type Err = ModelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let value = s.trim().to_uppercase();
        Ok(UpperString(value))
    }
}

impl From<&str> for UpperString {
    fn from(s: &str) -> Self {
        let value = s.trim().to_uppercase();
        UpperString(value)
    }
}
