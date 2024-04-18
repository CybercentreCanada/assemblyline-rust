use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::{ElasticMeta, Sid};


/// Model of Scoring related to a File
#[derive(Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct FileScore {
    /// Parent submission ID of the associated submission
    pub psid: Option<Sid>,
    /// Expiry timestamp, used for garbage collection
    #[metadata(index=true)]
    pub expiry_ts: DateTime<Utc>,
    /// Maximum score for the associated submission
    pub score: i64,
    /// Number of errors that occurred during the previous analysis
    pub errors: u32,
    /// ID of the associated submission
    pub sid: Sid,
    /// Epoch time at which the FileScore entry was created
    pub time: f64,
}