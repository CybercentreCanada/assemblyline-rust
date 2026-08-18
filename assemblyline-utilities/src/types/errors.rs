use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum ApiClientError {
    ClientError {
        message: String,
        status_code: Option<u16>,
        server_version: Option<String>,
    },
    Transport(String),
    Configuration(String),
    IO(String),
    /// The server's response was truncated, corrupted, or malformed
    MalformedResponse,
    /// An invalid HTTP header name or value was provided
    InvalidHeader,
    /// An error caused by failing to serialize or deserialize a message
    Serialization(String),
    UrlParseError(String),
}

impl Display for ApiClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiClientError::ClientError {
                status_code,
                message,
                server_version,
            } => f.write_fmt(format_args!(
                "Service API({}) error [{}]: {}",
                server_version.clone().unwrap_or("v?".to_string()),
                status_code.unwrap_or(0),
                message
            )),
            ApiClientError::Transport(message) => f.write_fmt(format_args!("Error: {message}")),
            ApiClientError::Configuration(message) => f.write_fmt(format_args!("Cannot configure connection to service API: {message}")),
            ApiClientError::IO(message) => f.write_fmt(format_args!("An IO error ocurred: {message}")),
            ApiClientError::UrlParseError(message) => f.write_fmt(format_args!("Cannot parse string to URL: {message}")),
            ApiClientError::MalformedResponse => f.write_str("A server response was malformed"),
            ApiClientError::InvalidHeader => f.write_str("An invalid HTTP header name or value was encountered"),
            ApiClientError::Serialization(error) => f.write_fmt(format_args!("An error occurred serializing a body: {error}")),
        }
    }
}

impl From<std::io::Error> for ApiClientError {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value.to_string())
    }
}

impl From<rustls::Error> for ApiClientError {
    fn from(value: rustls::Error) -> Self {
        Self::Configuration(format!("Error loading tls certificates: {value}"))
    }
}

impl From<reqwest::Error> for ApiClientError {
    fn from(value: reqwest::Error) -> Self {
        if let Some(code) = value.status() {
            ApiClientError::ClientError {
                message: value.to_string(),
                status_code: Some(code.as_u16()),
                server_version: None,
            }
        } else {
            ApiClientError::Transport(value.to_string())
        }
    }
}

// value.to_string(), code.as_u16() as u32

impl From<reqwest::header::InvalidHeaderName> for ApiClientError {
    fn from(_value: reqwest::header::InvalidHeaderName) -> Self {
        Self::InvalidHeader
    }
}

impl From<reqwest::header::InvalidHeaderValue> for ApiClientError {
    fn from(_value: reqwest::header::InvalidHeaderValue) -> Self {
        Self::InvalidHeader
    }
}
impl From<serde_json::Error> for ApiClientError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value.to_string())
    }
}

impl From<url::ParseError> for ApiClientError {
    fn from(value: url::ParseError) -> Self {
        Self::UrlParseError(value.to_string())
    }
}
