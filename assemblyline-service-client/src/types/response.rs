use assemblyline_models::{datastore::Service, types::Sha256};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct APIResponse<B: Send> {
    pub api_response: B,
    #[serde(default)]
    pub api_error_message: Option<String>,
    pub api_server_version: String,
    pub api_status_code: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorApiResponse {
    #[serde(default)]
    pub api_error_message: Option<String>,
    pub api_server_version: String,
    pub api_status_code: u16,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    #[serde(default)]
    pub keep_alive: bool,
    pub new_heuristics: Vec<String>,
    pub service_config: Service,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskUploadResponse {
    pub success: bool,
    #[serde(default)]
    pub missing_files: Option<Vec<Sha256>>,
}
