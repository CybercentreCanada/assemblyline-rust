use assemblyline_models::{
    messages::task::Task,
    types::{ServiceName, Sha256},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ErrorResponse {
    pub message: String,
    pub service_name: ServiceName,
    pub service_version: String,
    #[serde(default)]
    pub service_tool_version: Option<String>,
    pub status: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ErrorBody {
    pub sha256: Sha256,
    #[serde(rename = "type")]
    pub error_type: String,
    pub response: ErrorResponse,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TaskUploadBody {
    pub task: Task,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub result: Option<Value>,

    #[serde(default)]
    pub freshen: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub error: Option<Value>,
}
