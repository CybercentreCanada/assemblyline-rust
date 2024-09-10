use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::{ElasticMeta, Readable};



/// Model for Empty Results
/// 
/// Empty results are gonna be an abstract construct
///  Only a record of the key is saved for caching purposes    
#[derive(Debug, Serialize, Deserialize, Described, Clone)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=true)]
pub struct EmptyResult {
    /// Expiry timestamp
    #[metadata(store=false)]
    pub expiry_ts: chrono::DateTime<chrono::Utc>,
}

impl Readable for EmptyResult {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}
