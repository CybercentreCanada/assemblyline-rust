use std::str::FromStr;

use assemblyline_models::JsonMap;
use serde::de::DeserializeOwned;
use serde::Deserialize;


#[derive(Deserialize)]
pub struct Get {
    /// The name of the index the document belongs to. 
    pub _index: String,
    /// The unique identifier for the document. 
    pub _id: String,
    /// The document version. Incremented each time the document is updated. 
    pub _version: u64,
    /// The sequence number assigned to the document for the indexing operation. Sequence numbers are used to ensure an older version of a document doesn’t overwrite a newer version. See Optimistic concurrency control. 
    pub _seq_no: i64,
    /// The primary term assigned to the document for the indexing operation. See Optimistic concurrency control. 
    pub _primary_term: i64,
    /// Indicates whether the document exists: true or false. 
    pub found: bool,
    /// The explicit routing, if set. 
    pub _routing: Option<String>,
    /// If found is true, contains the document data formatted in JSON. Excluded if the _source parameter is set to false or the stored_fields parameter is set to true. 
    pub _source: Option<JsonMap>,
    /// If the stored_fields parameter is set to true and found is true, contains the document fields stored in the index. 
    pub _fields: Option<JsonMap>,
}

#[derive(Deserialize)]
pub struct Multiget {
    pub docs: Vec<Get>
}

#[derive(Deserialize)]
pub struct OpenPit {
    pub id: String,
}


#[derive(Deserialize)]
pub struct Index {
    /// Provides information about the replication process of the index operation. 
    pub _shards: Shards,
    /// The name of the index the document was added to. 
    pub _index: String,
    /// The document type. Elasticsearch indices now support a single document type, _doc. 
    pub _type: Option<String>,
    /// The unique identifier for the added document. 
    pub _id: String,
    /// The document version. Incremented each time the document is updated. 
    pub _version: i64,
    /// The sequence number assigned to the document for the indexing operation. Sequence numbers are used to ensure an older version of a document doesn’t overwrite a newer version. See Optimistic concurrency control. 
    pub _seq_no: i64,
    /// The primary term assigned to the document for the indexing operation. See Optimistic concurrency control. 
    pub _primary_term: i64,
    /// The result of the indexing operation, created or updated. 
    pub result: IndexResult
}

#[derive(serde_with::DeserializeFromStr)]
pub enum IndexResult {
    Created,
    Updated,
    NoOp
}

impl FromStr for IndexResult {
    type Err = super::ElasticError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        if s.starts_with("create") {
            Ok(Self::Created)
        } else if s.starts_with("update") {
            Ok(Self::Updated)
        } else if s.starts_with("noop") {
            Ok(Self::NoOp)
        } else {
            Err(super::ElasticError::json(format!("Unknown index result: {s}")))
        }
    }
}

#[derive(Deserialize)]
pub struct Shards {
    /// Indicates how many shard copies (primary and replica shards) the index operation should be executed on. 
    pub total: i64,
    /// Indicates the number of shard copies the index operation succeeded on. When the index operation is successful, successful is at least 1.
    /// 
    /// Replica shards might not all be started when an indexing operation returns successfully—​by default, only the primary is required. Set wait_for_active_shards to change this default behavior. See Active shards.
    pub successful: i64,
    /// An array that contains replication-related errors in the case an index operation failed on a replica shard. 0 indicates there were no failures. 
    pub failed: ShardsFailure
}


#[derive(Deserialize)]
#[serde(untagged)]
enum ShardsFailure {
    NoFailures(i64),
    Failures(Vec<String>)
}

