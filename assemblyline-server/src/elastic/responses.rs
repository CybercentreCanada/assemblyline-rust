use std::fmt::Debug;
use std::str::FromStr;

use assemblyline_models::JsonMap;
use serde::de::DeserializeOwned;
use serde::Deserialize;


#[derive(Deserialize)]
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
pub struct Multiget<Source, Field> {
    pub docs: Vec<Get<Source, Field>>
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


#[derive(Deserialize)]
#[serde(untagged)]
enum ShardsFailure {
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
    pub index_uuid: String,
    pub shard: String,
    pub index: String,
}

#[derive(Deserialize)]
pub struct ErrorCause {
    #[serde(rename="type")]
    pub _type: String,
    pub reason: String,
    pub index_uuid: String,
    pub shard: String,
    pub index: String,
}

#[derive(Debug, Deserialize)]
pub struct DescribeIndex {
    // Object {"aliases": Object {"112691228370903790308498296117861805943user": Object {}}, "mappings": Object {"dynamic": String("true"), "dynamic_templates": Array [Object {"strings_as_keywords": Object {"mapping": Object {"ignore_above": Number(8191), "type": String("keyword")}, "match_mapping_type": String("string")}}, Object {"refuse_all_implicit_mappings": Object {"mapping": Object {"ignore_malformed": Bool(true), "index": Bool(false)}, "match": String("*")}}], "properties": Object {"__access_grp1__": Object {"type": String("keyword")}, "__access_grp2__": Object {"type": String("keyword")}, "__access_lvl__": Object {"type": String("integer")}, "__access_req__": Object {"type": String("keyword")}, "__text__": Object {"type": String("text")}, "agrees_with_tos": Object {"doc_values": Bool(false), "format": String("date_optional_time||epoch_millis"), "index": Bool(false), "type": String("date")}, "api_quota": Object {"type": String("long")}, "apikeys": Object {"enabled": Bool(false), "type": String("object")}, "apps": Object {"enabled": Bool(false), "type": String("object")}, "can_impersonate": Object {"doc_values": Bool(false), "index": Bool(false), "type": String("boolean")}, "classification": Object {"store": Bool(true), "type": String("keyword")}, "dn": Object {"copy_to": Array [String("__text__")], "ignore_above": Number(8191), "type": String("keyword")}, "email": Object {"copy_to": Array [String("__text__")], "ignore_above": Number(8191), "store": Bool(true), "type": String("keyword")}, "groups": Object {"copy_to": Array [String("__text__")], "ignore_above": Number(8191), "store": Bool(true), "type": String("keyword")}, "id": Object {"store": Bool(true), "type": String("keyword")}, "is_active": Object {"store": Bool(true), "type": String("boolean")}, "name": Object {"copy_to": Array [String("__text__")], "ignore_above": Number(8191), "store": Bool(true), "type": String("keyword")}, "otp_sk": Object {"doc_values": Bool(false), "ignore_above": Number(8191), "index": Bool(false), "type": String("keyword")}, "password": Object {"doc_values": Bool(false), "ignore_above": Number(8191), "index": Bool(false), "type": String("keyword")}, "roles": Object {"ignore_above": Number(8191), "store": Bool(true), "type": String("keyword")}, "security_tokens": Object {"enabled": Bool(false), "type": String("object")}, "submission_quota": Object {"type": String("long")}, "type": Object {"ignore_above": Number(8191), "store": Bool(true), "type": String("keyword")}, "uname": Object {"copy_to": Array [String("__text__")], "ignore_above": Number(8191), "store": Bool(true), "type": String("keyword")}}}, "settings": Object {"index": Object {"analysis": Object {"analyzer": Object {"string_ci": Object {"filter": Array [String("lowercase")], "tokenizer": String("keyword"), "type": String("custom")}, "text_fuzzy": Object {"lowercase": String("false"), "pattern": String("\\s*:\\s*"), "type": String("pattern")}, "text_whitespace": Object {"type": String("whitespace")}, "text_ws_dsplit": Object {"filters": Array [String("text_ws_dsplit")], "tokenizer": String("whitespace"), "type": String("custom")}}, "filter": Object {"text_ws_dsplit": Object {"pattern": String("(\\.)"), "replacement": String(" "), "type": String("pattern_replace")}}, "normalizer": Object {"lowercase_normalizer": Object {"char_filter": Array [], "filter": Array [String("lowercase")], "type": String("custom")}}}, "creation_date": String("1724871445236"), "number_of_replicas": String("0"), "number_of_shards": String("1"), "provided_name": String("112691228370903790308498296117861805943user_hot"), "routing": Object {"allocation": Object {"include": Object {"_tier_preference": String("data_content")}}}, "uuid": String("1s5rUSinQ1qtYwSLejDUjg"), "version": Object {"created": String("8505000")}}}}}
}