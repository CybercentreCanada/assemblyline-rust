use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use struct_metadata::Described;

use crate::{Sha256, ElasticMeta, ClassificationString, Text, ExpandingClassification};



/// A search run on stored files.
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Retrohunt {
    /// Defines the indices used for this retrohunt job
    pub archive_only: bool,
    /// Classification for the retrohunt job
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Maximum classification of results in the search
    pub search_classification: ClassificationString,
    /// Start time for the search.
    pub created: DateTime<Utc>,
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
    /// Date corresponding to start_group (if any)
    pub start_date: Option<DateTime<Utc>>,
    /// Latest expiry group this search will include
    pub end_group: u32,
    /// Date corresponding to end_group (if any)
    pub end_date: Option<DateTime<Utc>>,

    /// Time that the search started
    pub run_start: Option<DateTime<Utc>>,
    /// Time that the search finished
    pub run_complete: Option<DateTime<Utc>>,

    /// Unique id identifying this retrohunt job
    pub code: String,
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
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
struct RetrohuntHit {
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
mod python {

    use serde_json::json;

    use crate::datastore::retrohunt::RetrohuntHit;
    use crate::{meta::Mappings, datastore::retrohunt::Retrohunt};

    use crate::meta::build_mapping;
    use pretty_assertions::assert_eq;

    fn load_mapping(module: &str, name: &str) -> Mappings {
        let output = std::process::Command::new("python").arg("-c").arg("from assemblyline.datastore.support.build import build_mapping; from assemblyline.odm.models. ".to_owned() + module + " import " + name + "; import json; print(json.dumps(build_mapping(" + name + ".fields().values())))").output().unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        if !output.status.success() || !stderr.is_empty() {
            println!("{stderr}");
            panic!();
        }

        let (properties, dynamic_templates): (serde_json::Value, serde_json::Value) = serde_json::from_str(&stdout).unwrap();
        let py_mapping = json!({
            "properties": properties,
            "dynamic_templates": dynamic_templates,
        });
        serde_json::from_value(py_mapping).unwrap()
    }

    #[test]
    fn retrohunt_schema() {
        let py_mappings = load_mapping("retrohunt", "Retrohunt");
        let mapping = build_mapping::<Retrohunt>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn retrohunt_hits_schema() {
        let py_mappings = load_mapping("retrohunt", "RetrohuntHit");
        let mapping = build_mapping::<RetrohuntHit>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    // // Test that the classification components get expanded as expected
    // #[test]
    // fn classification_serialize() {
    //     let time = DateTime::parse_from_rfc3339("2001-05-01T01:59:59Z").unwrap();
    //     let sample = RetrohuntHit {
    //         classification: "",
    //         sha256: Sha256::from_str("00000000000000000000000000000000").unwrap(),
    //         expiry_ts: Some(time.into()),
    //         search: "abc123".to_owned(),
    //     };
    //     assert_eq!(crate::serialize::to_string(&sample).unwrap(), "");
    // }

}