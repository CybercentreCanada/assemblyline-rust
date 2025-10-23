use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use struct_metadata::Described;

use crate::types::{NonZeroInteger, ServiceName};
use crate::{ElasticMeta, Readable, types::{ClassificationString, JsonMap, Text}};

use super::service::{AccessMode, ChannelKinds, EnvironmentVariable, FetchMethods, ParamKinds, RegistryType, SignatureDelimiter};

// from assemblyline import odm
// from assemblyline.odm.models.service import SIGNATURE_DELIMITERS


// REF_DEPENDENCY_CONFIG = "Refer to:<br>[Service - DependencyConfig](../service/#dependencyconfig)"
// REF_DOCKER_CONFIG = "Refer to:<br>[Service - DockerConfig](../service/#dockerconfig)"
// REF_ENVVAR = "Refer to:<br>[Service - Enviroment Variable](../service/#environmentvariable)"
// REF_PV = "Refer to:<br>[Service - PeristentVolume](../service/#persistentvolume)"
// REF_SERVICE = "Refer to:<br>[Service](../service/#service)"
// REF_SUBMISSION_PARAMS = "Refer to:<br>[Service - SubmissionParams](../service/#submissionparams)"
// REF_UPDATE_CONFIG = "Refer to:<br>[Service - UpdateConfig](../service/#updateconfig)"
// REF_UPDATE_SOURCE = "Refer to:<br>[Service - UpdateSource](../service/#updatesource)"


/// Docker Configuration Delta
#[derive(Serialize, Deserialize, Described, Default)]
#[metadata_type(ElasticMeta)]
#[serde(default)]
#[metadata(index=false, store=false)]
pub struct DockerConfigDelta {
    /// REF_DOCKER_CONFIG
    pub allow_internet_access: Option<bool>,
    /// REF_DOCKER_CONFIG
    pub command: Option<Vec<String>>,
    /// REF_DOCKER_CONFIG
    pub cpu_cores: Option<f32>,
    /// REF_DOCKER_CONFIG
    pub environment: Option<Vec<EnvironmentVariable>>,
    /// REF_DOCKER_CONFIG
    pub image: Option<String>,
    /// REF_DOCKER_CONFIG
    pub registry_username: Option<String>,
    /// REF_DOCKER_CONFIG
    pub registry_password: Option<String>,
    /// REF_DOCKER_CONFIG
    pub registry_type: Option<RegistryType>,
    /// REF_DOCKER_CONFIG
    pub ports: Option<Vec<String>>,
    /// REF_DOCKER_CONFIG
    pub ram_mb: Option<i32>,
    /// REF_DOCKER_CONFIG
    pub ram_mb_min: Option<i32>,
    /// REF_DOCKER_CONFIG
    pub service_account: Option<String>,
    /// REF_DOCKER_CONFIG
    pub labels: Option<Vec<EnvironmentVariable>>,
}


#[derive(Serialize, Deserialize, Described, Default)]
#[metadata_type(ElasticMeta)]
#[serde(default)]
#[metadata(index=false, store=false)]
pub struct UpdateSourceDelta {
    /// REF_UPDATE_SOURCE
    pub enabled: Option<bool>,
    /// REF_UPDATE_SOURCE
    pub name: Option<String>,
    /// REF_UPDATE_SOURCE
    pub password: Option<String>,
    /// REF_UPDATE_SOURCE
    pub pattern: Option<String>,
    /// REF_UPDATE_SOURCE
    pub private_key: Option<String>,
    /// REF_UPDATE_SOURCE
    pub ca_cert: Option<String>,
    /// REF_UPDATE_SOURCE
    pub ssl_ignore_errors: Option<bool>,
    /// REF_UPDATE_SOURCE
    pub proxy: Option<String>,
    /// REF_UPDATE_SOURCE
    pub uri: Option<String>,
    /// REF_UPDATE_SOURCE
    pub username: Option<String>,
    /// REF_UPDATE_SOURCE
    pub headers: Option<Vec<EnvironmentVariable>>,
    /// REF_UPDATE_SOURCE
    pub default_classification: Option<ClassificationString>,
    /// REF_UPDATE_SOURCE
    pub use_managed_identity: Option<bool>,
    /// REF_UPDATE_SOURCE
    pub git_branch: Option<String>,
    /// REF_UPDATE_SOURCE
    pub sync: Option<bool>,
    /// REF_UPDATE_SOURCE
    pub fetch_method: Option<FetchMethods>,
    /// REF_UPDATE_SOURCE
    pub override_classification: Option<bool>,
    /// REF_UPDATE_SOURCE
    pub configuration: Option<HashMap<String, serde_json::Value>>,
    /// REF_UPDATE_SOURCE
    pub data: Option<Text>,
    /// REF_UPDATE_SOURCE
    #[metadata(mapping="integer")]
    pub update_interval: Option<NonZeroInteger>,
    /// REF_UPDATE_SOURCE
    pub ignore_cache: Option<bool>,    
}

#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct PersistentVolumeDelta {
    /// REF_PV
    pub mount_path: Option<String>,
    /// REF_PV
    pub capacity: Option<String>,
    /// REF_PV
    pub storage_class: Option<String>,
    /// REF_PV
    pub access_mode: Option<AccessMode>,
}

#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct DependencyConfigDelta {
    /// REF_DEPENDENCY_CONFIG
    pub container: Option<DockerConfigDelta>,
    /// REF_DEPENDENCY_CONFIG
    pub volumes: Option<HashMap<String, PersistentVolumeDelta>>,
    /// REF_DEPENDENCY_CONFIG
    pub run_as_core: Option<bool>,
}

#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct UpdateConfigDelta {
    /// REF_UPDATE_CONFIG
    #[metadata[index=true]]
    pub generates_signatures: Option<bool>,
    /// REF_UPDATE_CONFIG
    pub sources: Option<Vec<UpdateSourceDelta>>,
    /// REF_UPDATE_CONFIG
    pub update_interval_seconds: Option<i32>,
    /// REF_UPDATE_CONFIG
    pub wait_for_update: Option<bool>,
    /// REF_UPDATE_CONFIG
    pub signature_delimiter: Option<SignatureDelimiter>,
    /// REF_UPDATE_CONFIG
    pub custom_delimiter: Option<String>,
    /// REF_UPDATE_CONFIG
    pub default_pattern: Option<Text>,
}

#[derive(Serialize, Deserialize, Described)]
#[metadata_type(ElasticMeta)]
#[metadata(index=false, store=false)]
pub struct SubmissionParamsDelta {
    /// REF_SUBMISSION_PARAMS
    pub default: Option<serde_json::Value>,
    /// REF_SUBMISSION_PARAMS
    pub name: Option<String>,
    /// REF_SUBMISSION_PARAMS
    #[serde(rename="type")]
    pub _type: Option<ParamKinds>,
    /// REF_SUBMISSION_PARAMS
    pub value: Option<serde_json::Value>,
    /// REF_SUBMISSION_PARAMS
    pub list: Option<serde_json::Value>,
    /// REF_SUBMISSION_PARAMS
    pub hide: Option<bool>,
}


/// Service Delta relative to Initial Service Configuration
#[derive(Serialize, Deserialize, Described, Default)]
#[serde(default)]
#[metadata_type(ElasticMeta)]
#[metadata(index=true, store=false)]
pub struct ServiceDelta {
    /// REF_SERVICE
    #[metadata(store=true)]
    pub accepts: Option<String>,
    /// REF_SERVICE
    #[metadata(store=true)]
    pub rejects: Option<String>,
    /// REF_SERVICE
    pub auto_update: Option<bool>,
    /// REF_SERVICE
    #[metadata(store=true)]
    pub category: Option<String>,
    /// REF_SERVICE
    #[metadata(mapping="keyword")]
    pub classification: Option<ClassificationString>,
    /// REF_SERVICE
    #[metadata(index=false)]
    pub config: Option<JsonMap>,
    /// REF_SERVICE
    #[metadata(store=true)]
    pub description: Option<Text>,
    /// REF_SERVICE
    #[metadata(mapping="keyword")]
    pub default_result_classification: Option<ClassificationString>,
    /// REF_SERVICE
    #[metadata(store=true)]
    pub enabled: Option<bool>,
    /// REF_SERVICE
    pub is_external: Option<bool>,
    /// REF_SERVICE
    pub licence_count: Option<i32>,
    /// REF_SERVICE
    pub max_queue_length: Option<i32>,
    /// REF_SERVICE
    pub min_instances: Option<i32>,

    /// REF_SERVICE
    pub uses_tags: Option<bool>,
    /// REF_SERVICE
    pub uses_tag_scores: Option<bool>,
    /// REF_SERVICE
    pub uses_temp_submission_data: Option<bool>,
    /// REF_SERVICE
    pub uses_metadata: Option<bool>,
    pub monitored_keys: Option<Vec<String>>,

    /// REF_SERVICE
    #[metadata(store=true)]
    pub name: Option<ServiceName>, 
    /// REF_SERVICE
    #[metadata(store=true)]
    pub version: String,

    /// REF_SERVICE
    pub privileged: Option<bool>,
    /// REF_SERVICE
    pub disable_cache: Option<bool>,

    /// REF_SERVICE
    #[metadata(store=true)]
    pub stage: Option<String>, 
    /// REF_SERVICE
    #[metadata(index=false)]
    pub submission_params: Option<Vec<SubmissionParamsDelta>>,
    /// REF_SERVICE
    pub timeout: Option<i32>,

    /// REF_SERVICE
    pub docker_config: Option<DockerConfigDelta>,
    /// REF_SERVICE
    pub dependencies: Option<HashMap<String, DependencyConfigDelta>>,

    /// REF_SERVICE
    pub update_channel: Option<ChannelKinds>,
    /// REF_SERVICE
    pub update_config: Option<UpdateConfigDelta>,

    /// REF_SERVICE
    pub recursion_prevention: Option<Vec<String>>,
}

impl Readable for ServiceDelta {
    fn set_from_archive(&mut self, _from_archive: bool) {}
}