use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

use crate::types::classification::{unrestricted_classification, unrestricted_classification_string};
use crate::types::{ClassificationString, JsonMap, NonZeroInteger, ServiceName, Text};
use crate::{ElasticMeta, Readable};

/// Environment Variable Model
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct EnvironmentVariable {
    /// Name of Environment Variable
    pub name: String,
    /// Value of Environment Variable
    pub value: String,
}


/// Docker Container Configuration
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct DockerConfig {
    /// Does the container have internet-access?
    #[serde(default)]
    pub allow_internet_access: bool,
    /// Command to run when container starts up.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// CPU allocation
    #[serde(default="default_cpu_cores")]
    pub cpu_cores: f32,
    /// Additional environemnt variables for the container
    #[serde(default)]
    pub environment: Vec<EnvironmentVariable>,
    /// Complete name of the Docker image with tag, may include registry
    pub image: String,
    /// The username to use when pulling the image
    #[serde(default)]
    pub registry_username: Option<String>,
    /// The password or token to use when pulling the image
    #[serde(default)]
    pub registry_password: Option<String>,
    /// The type of container registry
    #[serde(default="default_registry_type")]
    pub registry_type: RegistryType,
    /// What ports of container to expose?
    #[serde(default)]
    pub ports: Vec<String>,
    /// Container RAM limit
    #[serde(default="default_ram_mb")]
    pub ram_mb: i32,
    /// Container RAM request
    #[serde(default="default_ram_mb_min")]
    pub ram_mb_min: i32,
    /// Service account to use for pods in kubernetes
    #[serde(default)]
    pub service_account: Option<String>,
    /// Additional container labels.
    #[serde(default)]
    pub labels: Vec<EnvironmentVariable>,
}

fn default_cpu_cores() -> f32 { 1.0 }
fn default_registry_type() -> RegistryType { RegistryType::Docker }
fn default_ram_mb() -> i32 { 512 }
fn default_ram_mb_min() -> i32 { 256 }

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum RegistryType {
    Docker,
    Harbor,
}

/// Container's Persistent Volume Configuration
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct PersistentVolume {
    /// Path into the container to mount volume
    pub mount_path: String,
    /// The amount of storage allocated for volume
    pub capacity: String,
    /// Storage class used to create volume
    pub storage_class: String,
    /// Access mode for volume
    #[serde(default="default_access_mode")]
    pub access_mode: AccessMode,
}

fn default_access_mode() -> AccessMode { AccessMode::ReadWriteOnce }

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug, Clone, Copy)]
#[metadata_type(ElasticMeta)]
pub enum AccessMode {
    ReadWriteOnce, ReadWriteMany
}

/// Container's Dependency Configuration
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct DependencyConfig {
    /// Docker container configuration for dependency
    pub container: DockerConfig,
    /// Volume configuration for dependency
    #[serde(default)]
    pub volumes: HashMap<String, PersistentVolume>,
    /// Should this dependency run as other core components?
    #[serde(default)]
    pub run_as_core: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default, Described)]
#[metadata_type(ElasticMeta)]
#[serde(rename_all="UPPERCASE")]
pub enum FetchMethods {
    #[default]
    Get,
    Post,
    Git,
}


/// Update Source Configuration
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct UpdateSource {
    /// Is this source active for periodic fetching?
    #[serde(default="default_enabled")]
    pub enabled: bool,
    /// Name of source
    pub name: String,
    /// Password used to authenticate with source
    #[serde(default)]
    pub password: Option<String>,
    /// Pattern used to find files of interest from source
    #[serde(default)]
    pub pattern: Option<String>,
    /// Private key used to authenticate with source
    #[serde(default)]
    pub private_key: Option<String>,
    /// CA cert for source
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Ignore SSL errors when reaching out to source?
    #[serde(default)]
    pub ssl_ignore_errors: bool,
    /// Proxy server for source
    #[serde(default)]
    pub proxy: Option<String>,
    /// URI to source
    pub uri: String,
    /// Username used to authenticate with source
    #[serde(default)]
    pub username: Option<String>,
    /// Headers
    #[serde(default)]
    pub headers: Vec<EnvironmentVariable>,
    /// Default classification used in absence of one defined in files from source
    #[serde(default="unrestricted_classification_string")]
    pub default_classification: ClassificationString,
    /// Use managed identity for authentication with Azure DevOps
    #[serde(default)]
    pub use_managed_identity: bool,
    /// Branch to checkout from Git repository.
    #[serde(default)]
    pub git_branch: Option<String>,
    /// Synchronize signatures with remote source. Allows system to auto-disable signatures no longer found in source.
    #[serde(default)]
    pub sync: bool,
    /// Fetch method to be used with source
    #[serde(default)]
    pub fetch_method: FetchMethods,
    /// Should the source's classfication override the signature's self-defined classification, if any?
    #[serde(default)]
    pub override_classification: bool,
    /// Processing configuration for source
    #[serde(default)]
    pub configuration: HashMap<String, serde_json::Value>,
    /// Data that's sent in a POST request (`fetch_method="POST"`)
    #[serde(default)]
    pub data: Option<Text>,
    /// Update check interval, in seconds, for this source
    #[serde(default)]
    #[metadata(mapping="integer")]
    update_interval: Option<NonZeroInteger>,
    /// Ignore source caching and forcefully fetch from source
    #[serde(default)]
    pub ignore_cache: bool,
}

fn default_enabled() -> bool { true }

/// Update Configuration for Signatures
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct UpdateConfig {
    /// Does the updater produce signatures?
    #[metadata(index=true)]
    #[serde(default)]
    pub generates_signatures: bool,
    /// List of external sources
    #[serde(default)]
    pub sources: Vec<UpdateSource>,
    /// Update check interval, in seconds
    pub update_interval_seconds: i32,
    /// Should the service wait for updates first?
    #[serde(default)]
    pub wait_for_update: bool,
    /// Delimiter used when given a list of signatures
    #[serde(default="default_signature_delimiter")]
    pub signature_delimiter: SignatureDelimiter,
    /// Custom delimiter definition
    pub custom_delimiter: Option<String>,
    /// Default pattern used for matching files
    #[serde(default="default_default_pattern")]
    pub default_pattern: Text,
}

fn default_signature_delimiter() -> SignatureDelimiter { SignatureDelimiter::DoubleNewLine }
fn default_default_pattern() -> Text { ".*".into() }

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "snake_case")]
pub enum SignatureDelimiter {
    NewLine,
    DoubleNewLine,
    Pipe,
    Comma,
    Space,
    None,
    File,
    Custom,
}

impl SignatureDelimiter {
    pub fn token(&self) -> String {
        match self {
            SignatureDelimiter::NewLine => "\n".to_owned(),
            SignatureDelimiter::DoubleNewLine => "\n\n".to_owned(),
            SignatureDelimiter::Pipe => "|".to_owned(),
            SignatureDelimiter::Comma => ",".to_owned(),
            SignatureDelimiter::Space => " ".to_owned(),
            SignatureDelimiter::None => "".to_owned(),
            SignatureDelimiter::File => "".to_owned(),
            SignatureDelimiter::Custom => "".to_owned(),
        }
    }
}

/// Submission Parameters for Service
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct SubmissionParams {
    /// Default value (must match value in `value` field)
    pub default: serde_json::Value,
    /// Name of parameter
    pub name: String,
    /// Type of parameter
    #[serde(rename="type")]
    pub param_type: ParamKinds,
    /// Default value (must match value in `default` field)
    pub value: serde_json::Value,
    /// List of values if `type: list`
    #[serde(default)]
    pub list: Vec<serde_json::Value>,
    /// Should this parameter be hidden?
    #[serde(default)]
    pub hide: bool,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum ParamKinds {
    Str,
    Int,
    List,
    Bool,
}

/// Service Configuration
#[derive(Serialize, Deserialize, Clone, Described, PartialEq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct Service {
    /// Regex to accept files as identified by Assemblyline
    /// Regexes applied to assemblyline style file type string
    #[metadata(store=true)]
    #[serde(default="default_service_accepts")]
    pub accepts: String,
    /// Regex to reject files as identified by Assemblyline
    /// Regexes applied to assemblyline style file type string
    #[metadata(store=true)]
    #[serde(default="default_service_rejects")]
    pub rejects: Option<String>,
    /// Should the service be auto-updated?
    #[serde(default)]
    pub auto_update: Option<bool>,
    /// Which category does this service belong to?
    #[metadata(store=true, copyto="__text__")]
    #[serde(default="default_category")]
    pub category: ServiceName,
    /// Classification of the service
    #[serde(default="unrestricted_classification")]
    pub classification: String,
    /// Service Configuration
    #[metadata(index=false, store=false)]
    #[serde(default)]
    pub config: JsonMap,
    /// Description of service
    #[metadata(store=true, copyto="__text__")]
    #[serde(default="default_description")]
    pub description: Text,
    /// Default classification assigned to service results
    #[serde(default="unrestricted_classification")]
    pub default_result_classification: String,
    /// Is the service enabled
    #[metadata(store=true)]
    #[serde(default)]
    pub enabled: bool,
    /// Does this service perform analysis outside of Assemblyline?
    #[serde(default)]
    pub is_external: bool,
    /// How many licences is the service allowed to use?
    #[serde(default)]
    #[metadata(mapping="integer")]
    pub licence_count: u32,
    /// The minimum number of service instances. Overrides Scaler's min_instances configuration.
    #[serde(default)]
    #[metadata(mapping="integer")]
    pub min_instances: Option<u32>,
    /// If more than this many jobs are queued for this service drop those over this limit. 0 is unlimited.
    #[serde(default)]
    #[metadata(mapping="integer")]
    pub max_queue_length: u32,

    /// Does this service use tags from other services for analysis?
    #[serde(default)]
    pub uses_tags: bool,
    /// Does this service use scores of tags from other services for analysis?
    #[serde(default)]
    pub uses_tag_scores: bool,
    /// Does this service use temp data from other services for analysis?
    #[serde(default)]
    pub uses_temp_submission_data: bool,
    /// Does this service use submission metadata for analysis?
    #[serde(default)]
    pub uses_metadata: bool,
    /// This service watches these temporary keys for changes when partial results are produced.
    #[serde(default)]
    pub monitored_keys: Vec<String>,

    /// Name of service
    #[metadata(store=true, copyto="__text__")]
    pub name: ServiceName,
    /// Version of service
    #[metadata(store=true)]
    pub version: String,

    /// Should the service be able to talk to core infrastructure or just service-server for tasking?
    #[serde(default)]
    pub privileged: bool,
    /// Should the result cache be disabled for this service?
    #[serde(default)]
    pub disable_cache: bool,

    /// Which execution stage does this service run in?
    #[metadata(store=true, copyto="__text__")]
    #[serde(default="default_stage")]
    pub stage: String,
    /// Submission parameters of service
    #[metadata(index=false)]
    #[serde(default)]
    pub submission_params: Vec<SubmissionParams>,
    /// Service task timeout, in seconds
    #[serde(default="default_timeout")]
    pub timeout: i32,

    /// Docker configuration for service
    pub docker_config: DockerConfig,
    /// Dependency configuration for service
    #[serde(default)]
    pub dependencies: HashMap<String, DependencyConfig>,

    /// What channel to watch for service updates?
    #[serde(default="default_update_channel")]
    pub update_channel: ChannelKinds,
    /// Update configuration for fetching external resources
    pub update_config: Option<UpdateConfig>,

    /// List of service names/categories where recursion is prevented.
    #[serde(default)]
    pub recursion_prevention: Vec<ServiceName>,
}

fn default_category() -> ServiceName { ServiceName::from_string("Static Analysis".to_owned()) }
fn default_description() -> Text { Text("NA".to_owned()) }
fn default_stage() -> String { "CORE".to_owned() }
fn default_timeout() -> i32 { 60 }
fn default_update_channel() -> ChannelKinds { ChannelKinds::Stable }

impl Service {
    pub fn key(&self) -> String {
        format!("{}_{}", self.name, self.version)
    }
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug, Clone, Copy)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum ChannelKinds {
    Stable,
    RC,
    Beta,
    Dev,
}

fn default_service_accepts() -> String { ".*".to_string() }
fn default_service_rejects() -> Option<String> { Some("empty|metadata/.*".to_string()) }

impl Readable for Service {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}