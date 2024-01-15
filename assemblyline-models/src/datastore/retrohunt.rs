use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use struct_metadata::Described;

use crate::{Classification, Sha256, ElasticMeta, ClassificationString, Text};



/// A search run on stored files.
#[derive(Serialize, Deserialize, Debug, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Retrohunt {
    /// Defines the indices used for this retrohunt job
    pub archive_only: bool,
    /// Classification for the retrohunt job
    pub classification: Classification,
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

    /// Unique id identifying this retrohunt job
    pub code: String,
    /// Text of filter query derived from yara signature"
    #[metadata(store=false)]
    pub raw_query: Option<String>,
    /// Text of original yara signature run
    #[metadata(store=false, copyto="__text__")]
    pub yara_signature: String,

    /// List of error messages that occured during the search
    #[metadata(store=false)]
    pub errors: Vec<String>,
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
    pub classification: Classification,
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
    fn retrohunt() {
        let py_mappings = load_mapping("retrohunt", "Retrohunt");
        let mapping = build_mapping::<Retrohunt>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

    #[test]
    fn retrohunt_hits() {
        let py_mappings = load_mapping("retrohunt", "RetrohuntHit");
        let mapping = build_mapping::<RetrohuntHit>().unwrap();
        assert_eq!(mapping, py_mappings);
    }

}