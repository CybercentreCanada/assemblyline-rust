use assemblyline_models::types::ServiceName;
use assemblyline_models::types::strings::ServiceVersion;
use serde::{Deserialize, Serialize};

pub (crate) mod errors;
pub (crate) mod task;


#[derive(Serialize, Deserialize, Clone)]
pub struct ServiceInfo {
    pub name: ServiceName,
    pub version: ServiceVersion,
}
