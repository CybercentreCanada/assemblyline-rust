
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::{ElasticMeta, ExpandingClassification, Readable, Text};


/// Model of Service Heuristics
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Heuristic {
    /// List of all associated ATT&CK IDs
    #[metadata(copyto="__text__")]
    #[serde(default)]
    pub attack_id: Vec<String>,
    /// Classification of the heuristic
    #[serde(flatten)]
    pub classification: ExpandingClassification,
    /// Description of the heuristic
    #[metadata(copyto="__text__")]
    pub description: Text,
    /// What type of files does this heuristic target?
    #[metadata(copyto="__text__")]
    pub filetype: String,
    /// ID of the Heuristic
    #[metadata(copyto="__text__")]
    pub heur_id: String,
    /// Name of the heuristic
    #[metadata(copyto="__text__")]
    pub name: String,
    /// Default score of the heuristic
    pub score: i32,
    /// Score of signatures for this heuristic
    #[serde(default)]
    pub signature_score_map: HashMap<String, i32>,
    /// Statistics related to the Heuristic
    #[serde(default)]
    pub stats: Statistics,
    /// Maximum score for heuristic
    pub max_score: Option<i32>,
}

impl Readable for Heuristic {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}


/// Statistics Model
#[derive(Debug, Serialize, Deserialize, Described, Default, Clone)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct Statistics {
    /// Count of statistical hits
    pub count: i32,
    /// Minimum value of all stastical hits
    pub min: i32,
    /// Maximum value of all stastical hits
    pub max: i32,
    /// Average of all stastical hits
    pub avg: i32,
    /// Sum of all stastical hits
    pub sum: i32,
    /// Date of first hit of statistic
    pub first_hit: Option<chrono::DateTime<chrono::Utc>>,
    /// Date of last hit of statistic
    pub last_hit: Option<chrono::DateTime<chrono::Utc>>,
}