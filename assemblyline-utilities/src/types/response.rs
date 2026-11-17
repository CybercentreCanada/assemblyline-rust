//! Structs for different API response types


use assemblyline_models::{datastore::Service, types::Sha256};
use serde::{Deserialize, Serialize};

/// Default API response struct. The basic API response format
#[derive(Debug, Serialize, Deserialize)]
pub struct APIResponse<B: Send> {
    /// The response body
    pub api_response: B,
    /// Error messages from server response
    #[serde(default)]
    pub api_error_message: Option<String>,
    /// version of the API server
    pub api_server_version: String,
    /// status code of the HTTP response
    pub api_status_code: u16,
}

/// API response with errors
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorApiResponse {
    /// Error messages from server
    #[serde(default)]
    pub api_error_message: Option<String>,
    /// version of the API server
    #[serde(default)]
    pub api_server_version: Option<String>,
    /// status code of the HTTP response
    #[serde(default)]
    pub api_status_code: Option<u16>,
}

/// Server response for registering services
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// whether to keep the service alive after register
    #[serde(default)]
    pub keep_alive: bool,
    /// New heuristics that are registered with the server
    pub new_heuristics: Vec<String>,
    /// service configuration
    pub service_config: Service,
}

/// Server response for uploading task results
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskUploadResponse {
    /// whether the upload was successful
    pub success: bool,
    /// files that are missing from result upload
    #[serde(default)]
    pub missing_files: Option<Vec<Sha256>>,
}
