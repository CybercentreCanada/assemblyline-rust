use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;

use assemblyline_models::types::JsonMap;
use serde::Deserialize;
use serde_json::Value;
use strum::Display;


#[derive(Debug, Deserialize)]
pub struct Get<Source, Field> {
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
    pub _source: Option<Source>,
    /// If the stored_fields parameter is set to true and found is true, contains the document fields stored in the index. 
    pub _fields: Option<Field>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum MaybeGet<Source, Field> {
    Get(Get<Source, Field>),
    Empty {
        /// The name of the index the document belongs to. 
        _index: String,
        /// The unique identifier for the document. 
        _id: String,
        /// Indicates whether the document exists: true or false. 
        found: bool,
    }
}

#[derive(Deserialize)]
pub struct Multiget<Source, Field> {
    pub docs: Vec<MaybeGet<Source, Field>>
}

#[derive(Deserialize)]
pub struct OpenPit {
    pub id: String,
}

#[derive(Deserialize)]
pub struct ClosePit {
    pub succeeded: bool,
    pub num_freed: u32,
}

/// json response for command queries
#[derive(Deserialize)]
pub struct Command {
    /// boolean field confirming the processing of the command
    pub acknowledged: bool,
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
    
#[derive(Debug, Deserialize)]
pub struct BulkShards {
    ///     (integer) Number of shards the operation attempted to execute on. 
    pub total: i64,
    //     (integer) Number of shards the operation succeeded on. 
    pub successful: i64,
    //     (integer) Number of shards the operation attempted to execute on but failed. 
    pub failed: i64,
}



#[derive(Deserialize)]
#[serde(untagged)]
pub enum ShardsFailure {
    NoFailures(i64),
    Failures(Vec<String>)
}

/// elasticsearch respons to status query
#[derive(Deserialize)]
#[allow(unused)]
pub(crate) struct Status {
    /// The name of the cluster. 
    pub cluster_name: String,
    /// Health status of the cluster, based on the state of its primary and replica shards. Statuses are:
    ///
    /// green: All shards are assigned.
    /// yellow: All primary shards are assigned, but one or more replica shards are unassigned. If a node in the cluster fails, some data could be unavailable until that node is repaired.
    /// red: One or more primary shards are unassigned, so some data is unavailable. This can occur briefly during cluster startup as primary shards are assigned.
    pub status: String,
    /// (Boolean) If false the response returned within the period of time that is specified by the timeout parameter (30s by default). 
    pub timed_out: bool,
    /// (integer) The number of nodes within the cluster. 
    pub number_of_nodes: i64,
    /// (integer) The number of nodes that are dedicated data nodes. 
    pub number_of_data_nodes: i64,
    /// (integer) The number of active primary shards. 
    pub active_primary_shards: i64,
    /// (integer) The total number of active primary and replica shards. 
    pub active_shards: i64,
    /// (integer) The number of shards that are under relocation. 
    pub relocating_shards: i64,
    /// (integer) The number of shards that are under initialization. 
    pub initializing_shards: i64,
    /// (integer) The number of shards that are not allocated. 
    pub unassigned_shards: i64,
    /// (integer) The number of shards whose allocation has been delayed by the timeout settings. 
    pub delayed_unassigned_shards: i64,
    /// (integer) The number of cluster-level changes that have not yet been executed. 
    pub number_of_pending_tasks: i64,
    /// (integer) The number of unfinished fetches. 
    pub number_of_in_flight_fetch: i64,
    /// (integer) The time expressed in milliseconds since the earliest initiated task is waiting for being performed. 
    pub task_max_waiting_in_queue_millis: i64,
    /// (float) The ratio of active shards in the cluster expressed as a percentage. 
    pub active_shards_percent_as_number: f64,
}

/// elasticsearch response for a search query
#[derive(Debug, Deserialize)]
pub struct Search<FieldType: Default + Debug, SourceType: Debug> {
    /// time taken to complete search call
    pub took: u64,
    /// flag indicating the search timed out rather than completed
    pub timed_out: bool,
    /// hits returned for the search
    pub hits: SearchHits<FieldType, SourceType>
}

/// hits section for an elasticsearch response to a search query
#[derive(Debug, Deserialize)]
pub struct SearchHits<FieldType: Default, SourceType> {
    /// information on the total number of matching documents as distinct from the potentially more limited set returned
    pub total: SearchHitTotals,
    /// Score indicating the quality of match in the search
    pub max_score: Option<f64>,
    /// list of items returned by the search
    pub hits: Vec<SearchHitItem<FieldType, SourceType>>,
}

/// total hit value with form of total for a search
#[derive(Debug, Deserialize)]
pub struct SearchHitTotals {
    /// number of or bound on the total size of the matching document set
    pub value: u64,
    /// operation decribing the nature of the bound given (is it the exact number or bound)
    /// TODO replace with enum
    pub relation: String,
}

/// entry returned for a single document matched by a search 
#[derive(Debug, Deserialize)]
pub struct SearchHitItem<FieldType, SourceType> {
    /// index document was returned from (search may be over many indices)
    pub _index: String,
    /// document id
    pub _id: String,
    /// score describing the match of this result to the search parameters
    pub _score: Option<f64>,
    /// the source document (or fields of the source document) requested in the search 
    #[serde(default="default_source")]
    pub _source: Option<SourceType>,
    /// entry describing this document's position in the sorting of the result set, useful for pagination 
    pub sort: serde_json::Value,
    /// Fields returned by the search from the indexed data (as opposed to source document)
    #[serde(default)]
    pub fields: FieldType,
}

/// helper function to handle empty source response when SourceType is not Default
fn default_source<T>() -> Option<T> { None }

#[derive(Deserialize)]
pub struct Delete {
    pub _index: String, 
    pub _id: String, 
    pub _version: i64, 
    pub result: DeleteResult, 
    pub _shards: Shards, 
    pub _seq_no: i64,
    pub _primary_term: i64,
}


#[derive(serde_with::DeserializeFromStr)]
pub enum DeleteResult {
    Deleted,
    NotFound,
}

impl FromStr for DeleteResult {
    type Err = super::ElasticError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        if s.starts_with("delete") {
            Ok(Self::Deleted)
        } else if s.starts_with("not_found") {
            Ok(Self::NotFound)
        } else {
            Err(super::ElasticError::json(format!("Unknown delete result: {s}")))
        }
    }
}

#[derive(Deserialize)]
pub struct Error {
    pub error: ErrorInner,
    pub status: u16,
}

#[derive(Deserialize)]
pub struct ErrorInner {
    pub root_cause: Vec<ErrorCause>,
    #[serde(rename="type")]
    pub _type: String,
    pub reason: String,
    #[serde(default)]
    pub index_uuid: Option<String>,
    #[serde(default)]
    pub shard: Option<String>,
    #[serde(default)]
    pub index: Option<String>,
}

#[derive(Deserialize)]
pub struct ErrorCause {
    #[serde(rename="type")]
    pub _type: String,
    pub reason: String,
    #[serde(default)]
    pub index_uuid: Option<String>,
    #[serde(default)]
    pub shard: Option<String>,
    #[serde(default)]
    pub index: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DescribeIndex {
    #[serde(flatten)]
    pub indices: HashMap<String, IndexDescription>,
}

#[derive(Debug, Deserialize)]
pub struct IndexDescription {
    #[serde(default)]
    pub aliases: HashMap<String, AliasDetails>,
    
    pub mappings: assemblyline_models::meta::Mappings,

    #[serde(flatten)]
    other: HashMap<String, Value>,    
}


#[derive(Debug, Deserialize)]
pub struct AliasDetails {}

#[derive(Debug, Deserialize)]
pub struct TaskId {
    pub task: String,
}

#[derive(Debug, Deserialize)]
pub struct TaskBody {
    pub response: TaskResponse, 
    pub completed: bool,
    pub task: Task
}

#[derive(Debug, Deserialize)]
pub struct TaskResponse {
    #[serde(flatten)]
    pub _status: TaskStatus,
    pub failures: Vec<serde_json::Value>,
    pub throttled: String,
    pub throttled_until: String,
    pub timed_out: bool,
    pub took: i64,
}


#[derive(Debug, Deserialize)]
pub struct Task {
    pub action: String,
    pub cancellable: bool,
    pub cancelled: bool,
    pub description: String,
    pub headers: JsonMap, 
    pub id: u64,
    pub node: String,
    pub running_time_in_nanos: u64,
    pub start_time_in_millis: u64,
    pub status: TaskStatus, 
    #[serde(rename="type")]
    pub type_: String,
}

#[derive(Debug, Deserialize)]
pub struct TaskStatus {
    pub batches: u64,
    pub created: u64,
    pub deleted: u64,
    pub noops: u64,
    pub requests_per_second: f64,
    pub retries: TaskRetries, 
    pub throttled_millis: u64,
    pub throttled_until_millis: u64,
    pub total: u64,
    pub updated: u64,
    pub version_conflicts: u64,
}


#[derive(Debug, Deserialize)]
pub struct TaskRetries {
    pub bulk: u64,
    pub search: u64,
}

#[derive(Debug, Deserialize)]
pub struct Bulk {
    /// (integer) How long, in milliseconds, it took to process the bulk request.     
    pub took: u64,
    /// (Boolean) If true, one or more of the operations in the bulk request did not complete successfully. 
    pub errors: bool,
    /// (array of objects) Contains the result of each operation in the bulk request, in the order they were submitted.
    pub items: Vec<BulkItem>,
}

/// (object) The parameter name is an action associated with the operation. Possible values are create, delete, index, and update.
#[derive(Debug, Deserialize)]
#[serde(rename_all="lowercase")]
pub enum BulkItem {
    Create(BulkItemData),
    Delete(BulkItemData),
    Index(BulkItemData),
    Update(BulkItemData),
}


impl std::ops::Deref for BulkItem {
    type Target = BulkItemData;

    fn deref(&self) -> &Self::Target {
        match self {
            BulkItem::Create(bulk_item_data) => bulk_item_data,
            BulkItem::Delete(bulk_item_data) => bulk_item_data,
            BulkItem::Index(bulk_item_data) => bulk_item_data,
            BulkItem::Update(bulk_item_data) => bulk_item_data,
        }
    }
}

/// The parameter value is an object that contains information for the associated operation.
#[derive(Debug, Deserialize)]
pub struct BulkItemData {
    /// (string) Name of the index associated with the operation. If the operation targeted a data stream, this is the backing index into which the document was written. 
    pub _index: String,
    /// The document ID associated with the operation. 
    pub _id: String,
    /// (integer) The document version associated with the operation. The document version is incremented each time the document is updated.
    ///
    /// This parameter is only returned for successful actions.
    #[serde(default)]
    pub _version: Option<i64>,
    /// (string) Result of the operation. Successful values are created, deleted, and updated. Other valid values are noop and not_found. 
    pub result: BulkResult,
    /// (object) Contains shard information for the operation.
    /// This parameter is only returned for successful operations.
    pub _shards: Option<BulkShards>,

    /// (integer) The sequence number assigned to the document for the operation. Sequence numbers are used to ensure an older version of a document doesn’t overwrite a newer version. See Optimistic concurrency control.
    ///
    /// This parameter is only returned for successful operations.
    #[serde(default)]
    pub _seq_no: Option<i64>,
    /// (integer) The primary term assigned to the document for the operation. See Optimistic concurrency control.
    ///
    /// This parameter is only returned for successful operations.
    #[serde(default)]
    pub _primary_term: Option<i64>,

    /// (integer) HTTP status code returned for the operation. 
    pub status: i32,
        
    // (object) Contains additional information about the failed operation.

    // The parameter is only returned for failed operations.
    // Properties of error

    // type
    //     (string) Error type for the operation. 
    // reason
    //     (string) Reason for the failed operation. 
    // index_uuid
    //     (string) The universally unique identifier (UUID) of the index associated with the failed operation. 
    // shard
    //     (string) ID of the shard associated with the failed operation. 
    // index
    //     (string) Name of the index associated with the failed operation. If the operation targeted a data stream, this is the backing index into which the document was attempted to be written. 
    #[serde(default)]
    pub error: serde_json::Value,
}

#[derive(Debug, Display, Deserialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum BulkResult {
    Created,
    Deleted,
    Updated,
    Noop,
    NotFound,
}

#[derive(Debug, Deserialize)]
pub struct CreateIndex {
    pub index: String,
    pub shards_acknowledged: bool,
    pub acknowledged: bool,    
}

