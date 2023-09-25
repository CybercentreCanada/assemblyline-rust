// from assemblyline import odm
// from assemblyline.common import forge

// Classification = forge.get_classification()

use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};

use super::{Classification, Uuid};


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Priorities {
    Low, 
    Medium, 
    High, 
    Critical,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString)]
#[strum(serialize_all = "SCREAMING-KEBAB-CASE")]
pub enum Statuses {
    Malicious, 
    NonMalicious, 
    Assess, 
    Triage,
}

/// Model of Workflow
#[derive(Serialize, Deserialize)]
pub struct Workflow {
    /// Classification of the workflow
    pub classification: Classification,
    /// Creation date of the workflow
    pub creation_date: DateTime<Utc>,
    /// UID of the creator of the workflow
    pub creator: String,
    /// UID of the last user to edit the workflow
    pub edited_by: String,
    /// Date of first hit on workflow
    pub first_seen: Option<DateTime<Utc>>,
    /// Number of times there was a workflow hit
    #[serde(default)]
    pub hit_count: i64,
    /// Labels applied by the workflow
    #[serde(default)]
    pub labels: Vec<String>,
    /// Date of last edit on workflow
    pub last_edit: DateTime<Utc>,
    /// Date of last hit on workflow
    pub last_seen: Option<DateTime<Utc>>,
    /// Name of the workflow
    pub name: String,
    /// Which did this originate from?
    pub origin: Option<String>,
    /// Priority applied by the workflow
    pub priority: Option<Priorities>,
    /// Query that the workflow runs
    pub query: String,
    /// Status applied by the workflow
    pub status: Option<Statuses>,
    /// ID of the workflow
    pub workflow_id: Option<Uuid>,
}