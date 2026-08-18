use assemblyline_models::{messages::task::Task, types::Sha256, ModelError};
use assemblyline_utilities::types::errors::ApiClientError;

use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum ServiceClientError {
    ApiConnection {
        message: String,
        status_code: Option<u16>,
        server_version: Option<String>,
    },
    Api {
        message: String,
    },
    HashError(String),
    Default(String),
    SerializeError(String),
    FileHashMisMatch {
        requested_sha: Sha256,
        content_sha: Sha256,
    },
    TaskFileMissingError {
        message: String,
        file_sha: Sha256,
        task: Task,
    },
    PipeReadError {
        pipe_name: String,
        message: String,
    },
    PipeWriteError {
        pipe_name: String,
        message: String,
    },
    UrlParseError(String)
}

impl Display for ServiceClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceClientError::ApiConnection {
                status_code,
                message,
                server_version,
            } => f.write_fmt(format_args!(
                "Service API({}) error [{}]: {}",
                server_version.clone().unwrap_or("v?".to_string()),
                status_code.unwrap_or(0),
                message
            )),
            ServiceClientError::HashError(message) => f.write_fmt(format_args!("Error: {message}")),
            ServiceClientError::SerializeError(message) => f.write_fmt(format_args!("Error: {message}")),
            ServiceClientError::FileHashMisMatch { requested_sha, content_sha } => f.write_fmt(format_args!(
                "Error: Sha256 of content {content_sha} does not match the requested sha256 {requested_sha}"
            )),
            ServiceClientError::Default(message) => f.write_fmt(format_args!("Error: {message}")),
            ServiceClientError::TaskFileMissingError { message, file_sha, task } => {
                f.write_fmt(format_args!("File Missing Error [{}] - [{}]: {}", task.sid, file_sha, message))
            }
            ServiceClientError::PipeReadError { pipe_name, message } => f.write_fmt(format_args!("Error reading pipe({}): {} ", pipe_name, message)),
            ServiceClientError::PipeWriteError { pipe_name, message } => f.write_fmt(format_args!("Error writing pipe({}): {} ", pipe_name, message)),
            ServiceClientError::Api { message } => f.write_fmt(format_args!("Service API error: {message}")),
            ServiceClientError::UrlParseError(message) => f.write_fmt(format_args!("URL Parse Error: {message}"))
        }
    }
}

impl From<reqwest::Error> for ServiceClientError {
    fn from(value: reqwest::Error) -> Self {
        ServiceClientError::ApiConnection {
            status_code: value.status().and_then(|c| Some(c.as_u16())),
            message: format!("{}", value),
            server_version: None,
        }
    }
}

impl From<anyhow::Error> for ServiceClientError {
    fn from(value: anyhow::Error) -> Self {
        ServiceClientError::Default(value.to_string())
    }
}

impl From<std::io::Error> for ServiceClientError {
    fn from(value: std::io::Error) -> Self {
        ServiceClientError::Default(value.to_string())
    }
}

impl From<ModelError> for ServiceClientError {
    fn from(value: ModelError) -> Self {
        ServiceClientError::Default(value.to_string())
    }
}

impl From<serde_json::Error> for ServiceClientError {
    fn from(value: serde_json::Error) -> Self {
        ServiceClientError::SerializeError(value.to_string())
    }
}

impl From<ApiClientError> for ServiceClientError {
    fn from(value: ApiClientError) -> Self {
        match value {
            ApiClientError::ClientError {
                message,
                status_code,
                server_version,
            } => ServiceClientError::ApiConnection {
                message,
                status_code,
                server_version,
            },
            ApiClientError::Transport(e) => ServiceClientError::ApiConnection {
                message: e,
                status_code: None,
                server_version: None,
            },
            ApiClientError::Configuration(e) => ServiceClientError::Api { message: e },
            ApiClientError::IO(e) => ServiceClientError::Api { message: e },
            ApiClientError::MalformedResponse => ServiceClientError::ApiConnection {
                message: format!("{}", ApiClientError::MalformedResponse),
                status_code: None,
                server_version: None,
            },
            ApiClientError::InvalidHeader => ServiceClientError::ApiConnection {
                message: format!("{}", ApiClientError::InvalidHeader),
                status_code: None,
                server_version: None,
            },
            ApiClientError::Serialization(e) => ServiceClientError::Api { message: e },
            ApiClientError::UrlParseError(e) => ServiceClientError::Api { message: e },
        }
    }
}

impl From<url::ParseError> for ServiceClientError {
    fn from(value: url::ParseError) -> Self {
        Self::UrlParseError(value.to_string())
    }
}
