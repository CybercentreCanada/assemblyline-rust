use assemblyline_models::{messages::task::Task, types::Sha256, ModelError};

use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum ServiceHandlerError {
    ServiceApiConnectionError {
        message: String,
        status_code: Option<u16>,
        server_version: Option<String>,
    },
    /// An error that occured during a failed communication with the server
    Transport(String),
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
}

impl Display for ServiceHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceHandlerError::ServiceApiConnectionError { status_code, message, server_version } =>
                f.write_fmt(format_args!("Service API({}) error [{}]: {}", server_version.clone().unwrap_or("v?".to_string()), status_code.unwrap_or(0), message)),
            ServiceHandlerError::Transport(message) =>
                f.write_fmt(format_args!("Error: {message}")),
            ServiceHandlerError::HashError(message) =>
                f.write_fmt(format_args!("Error: {message}")),
            ServiceHandlerError::SerializeError(message) =>
                f.write_fmt(format_args!("Error: {message}")),
            ServiceHandlerError::FileHashMisMatch {requested_sha, content_sha} =>
                f.write_fmt(format_args!("Error: Sha256 of content {content_sha} does not match the requested sha256 {requested_sha}")),
            ServiceHandlerError::Default(message) =>
                f.write_fmt(format_args!("Error: {message}")),
            ServiceHandlerError::TaskFileMissingError { message, file_sha, task} =>
            f.write_fmt(format_args!("File Missing Error [{}] - [{}]: {}", task.sid, file_sha, message)),
            ServiceHandlerError::PipeReadError { pipe_name, message } => {
            f.write_fmt(format_args!("Error reading pipe({}): {} ", pipe_name, message))
            },
            ServiceHandlerError::PipeWriteError { pipe_name, message } => {
            f.write_fmt(format_args!("Error writing pipe({}): {} ", pipe_name, message))
            },
        }
    }
}

impl From<reqwest::Error> for ServiceHandlerError {
    fn from(value: reqwest::Error) -> Self {
        ServiceHandlerError::ServiceApiConnectionError {
            status_code: value.status().and_then(|c| Some(c.as_u16())),
            message: format!("{}", value),
            server_version: None,
        }
    }
}

impl From<anyhow::Error> for ServiceHandlerError {
    fn from(value: anyhow::Error) -> Self {
        ServiceHandlerError::Default(value.to_string())
    }
}

impl From<std::io::Error> for ServiceHandlerError {
    fn from(value: std::io::Error) -> Self {
        ServiceHandlerError::Default(value.to_string())
    }
}

impl From<ModelError> for ServiceHandlerError {
    fn from(value: ModelError) -> Self {
        ServiceHandlerError::Default(value.to_string())
    }
}

impl From<serde_json::Error> for ServiceHandlerError {
    fn from(value: serde_json::Error) -> Self {
        ServiceHandlerError::SerializeError(value.to_string())
    }
}
