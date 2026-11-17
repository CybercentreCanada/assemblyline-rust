//! Structs library

use assemblyline_models::types::{ServiceName, strings::ServiceVersion};
use serde::{Deserialize, Serialize};

pub (crate) mod errors;
pub (crate) mod task;


/// basic service information
#[derive(Serialize, Deserialize, Clone)]
pub struct ServiceInfo {
    /// name of service
    pub name: ServiceName,
    /// service version
    pub version: ServiceVersion,
}
