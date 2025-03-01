use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

use crate::{Sha256, ElasticMeta, ClassificationString, Text, ExpandingClassification};

#[derive(SerializeDisplay, DeserializeFromStr, Debug, PartialEq, Eq, strum::Display, strum::EnumString, Described, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "snake_case")]
pub enum IndexCatagory {
    Hot = 1,
    Archive = 2,
    HotAndArchive = 3,
}

/// A search run on stored files.
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Retrohunt {
    /// Which archive catagories do we run on
    pub indices: IndexCatagory,
    /// Classification for the retrohunt job
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Maximum classification of results in the search
    #[metadata(mapping="keyword")]
    pub search_classification: ClassificationString,
    /// User who created this retrohunt job
    #[metadata(copyto="__text__")]
    pub creator: String,
    /// Human readable description of this retrohunt job
    #[metadata(copyto="__text__")]
    pub description: Text,
    /// Tags describing this retrohunt job"
    // pub tags = odm.Optional(odm.mapping(odm.sequence(odm.keyword(copyto="__text__")),)
    /// Expiry timestamp of this retrohunt job
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,

    /// Earliest expiry group this search will include
    pub start_group: u32,
    /// Latest expiry group this search will include
    pub end_group: u32,

    /// Start time for the search.
    pub created_time: DateTime<Utc>,
    /// Start time for the search.
    pub started_time: DateTime<Utc>,
    /// Time that the search finished
    #[metadata(store=false)]
    pub completed_time: Option<DateTime<Utc>>,

    /// Unique id identifying this retrohunt job
    pub key: String,
    /// Text of filter query derived from yara signature
    #[metadata(store=false)]
    pub raw_query: String,
    /// Text of original yara signature run
    #[metadata(store=false, copyto="__text__")]
    pub yara_signature: String,

    /// List of error messages that occured during the search
    #[metadata(store=false)]
    pub errors: Vec<String>,
    /// List of warning messages that occured during the search
    #[metadata(store=false)]
    pub warnings: Vec<String>,
    /// Boolean that indicates if this retrohunt job is finished
    pub finished: bool,
    /// Indicates if the list of hits been truncated at some limit
    pub truncated: bool,
}

/// A hit encountered during a retrohunt search.
#[derive(Serialize, Deserialize, Debug, Described, Clone, PartialEq, Eq)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct RetrohuntHit {
    /// Unique id identifying this retrohunt result
    pub key: String,
    /// Classification string for the retrohunt job and results list
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    pub sha256: Sha256,
    /// Expiry for this entry.
    #[metadata(store=false)]
    pub expiry_ts: Option<DateTime<Utc>>,
    pub search: String,
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use super::RetrohuntHit;
    use crate::{serialize::test::setup_classification, ExpandingClassification};

    #[test]
    fn hit_roundtrip(){
        let parser = setup_classification();
        let data = RetrohuntHit {
            key: "abc123".to_owned(),
            classification: ExpandingClassification::new("L0".to_owned(), &parser).unwrap(),
            sha256: "cb3f7b194d220004ffa6eef1305849bcef38033c49cb1b16c5ab3c3d60bd9d20".parse().unwrap(),
            expiry_ts: Utc::now().into(),
            search: "search".to_owned(),
        };

        let json = serde_json::to_string_pretty(&data).unwrap();
        println!("{json}");
        let data_copy = serde_json::from_str(&json).unwrap();
        assert_eq!(data, data_copy);
    }
}