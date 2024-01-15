use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use serde_with::{SerializeDisplay, DeserializeFromStr};
use struct_metadata::Described;

use crate::{JsonMap, ElasticMeta, Text};

/// Environment Variable Model
#[derive(Serialize, Deserialize, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct EnvironmentVariable {
    /// Name of Environment Variable
    pub name: String,
    /// Value of Environment Variable
    pub value: String,
}


/// Docker Container Configuration
#[derive(Serialize, Deserialize, Described, PartialEq, Debug)]
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
    pub cpu_cores: f64,
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
    pub ram_mb: u64,
    /// Container RAM request
    #[serde(default="default_ram_mb_min")]
    pub ram_mb_min: u64,
    /// Service account to use for pods in kubernetes
    #[serde(default)]
    pub service_account: Option<String>,
}

fn default_cpu_cores() -> f64 { 1.0 }
fn default_registry_type() -> RegistryType { RegistryType::Docker }
fn default_ram_mb() -> u64 { 512 }
fn default_ram_mb_min() -> u64 { 256 }

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum RegistryType {
    Docker,
    Harbor,
}

/// Container's Persistent Volume Configuration
#[derive(Serialize, Deserialize, Described, PartialEq, Eq, Debug)]
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

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
pub enum AccessMode {
    ReadWriteOnce, ReadWriteMany
}

/// Container's Dependency Configuration
#[derive(Serialize, Deserialize, Described, PartialEq, Debug)]
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


/// Update Source Configuration
#[derive(Serialize, Deserialize, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct UpdateSource {
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
    pub default_classification: String, // = odm.Classification(default=Classification.UNRESTRICTED,),
    /// Branch to checkout from Git repository.
    #[serde(default)]
    pub git_branch: Option<String>,
    /// Synchronize signatures with remote source. Allows system to auto-disable signatures no longer found in source.
    #[serde(default)]
    pub sync: bool,
}

/// Update Configuration for Signatures
#[derive(Serialize, Deserialize, Described, PartialEq, Eq, Debug)]
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
    pub update_interval_seconds: u32,
    /// Should the service wait for updates first?
    #[serde(default)]
    pub wait_for_update: bool,
    /// Delimiter used when given a list of signatures
    #[serde(default="default_signature_delimiter")]
    pub signature_delimiter: SignatureDelimiter,
    /// Custom delimiter definition
    pub custom_delimiter: Option<String>,
}

fn default_signature_delimiter() -> SignatureDelimiter { SignatureDelimiter::DoubleNewLine }

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug)]
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
#[derive(Serialize, Deserialize, Described, PartialEq, Eq, Debug)]
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
    pub list: Vec<serde_json::Value>,
    /// Should this parameter be hidden?
    #[serde(default)]
    pub hide: bool,
}

#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug)]
#[metadata_type(ElasticMeta)]
#[strum(serialize_all = "lowercase")]
pub enum ParamKinds {
    Str,
    Int,
    List,
    Bool,
}

/// Service Configuration
#[derive(Serialize, Deserialize, Described, PartialEq, Debug)]
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

    /// Which category does this service belong to?
    #[metadata(store=true, copyto="__text__")]
    #[serde(default="default_category")]
    pub category: String,
    /// Classification of the service
    pub classification: String, // = odm.ClassificationString(default=Classification.UNRESTRICTED, )
    /// Service Configuration
    #[metadata(index=false, store=false)]
    #[serde(default)]
    pub config: JsonMap,
    /// Description of service
    #[metadata(store=true, copyto="__text__")]
    #[serde(default="default_description")]
    pub description: Text,
    /// Default classification assigned to service results
    pub default_result_classification: String, // = odm.ClassificationString(default=Classification.UNRESTRICTED, )
    /// Is the service enabled
    #[metadata(store=true)]
    #[serde(default)]
    pub enabled: bool,
    /// Does this service perform analysis outside of Assemblyline?
    #[serde(default)]
    pub is_external: bool,
    /// How many licences is the service allowed to use?
    #[serde(default)]
    pub licence_count: u32,
    /// The minimum number of service instances. Overrides Scaler's min_instances configuration.
    #[serde(default)]
    pub min_instances: Option<u32>,
    /// If more than this many jobs are queued for this service drop those over this limit. 0 is unlimited.
    #[serde(default)]
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

    /// Name of service
    #[metadata(store=true, copyto="__text__")]
    pub name: String,
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
    pub timeout: u32,

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
}

fn default_category() -> String { "Static Analysis".to_owned() }
fn default_description() -> Text { Text("NA".to_owned()) }
fn default_stage() -> String { "CORE".to_owned() }
fn default_timeout() -> u32 { 60 }
fn default_update_channel() -> ChannelKinds { ChannelKinds::Stable }

impl Service {
    pub fn key(&self) -> String {
        format!("{}_{}", self.name, self.version)
    }
}


#[derive(SerializeDisplay, DeserializeFromStr, strum::Display, strum::EnumString, Described, PartialEq, Eq, Debug)]
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
