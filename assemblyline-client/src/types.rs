use std::fmt::Display;

use assemblyline_utilities::types::errors::ApiClientError;

/// Short name for serde json's basic map type
pub type JsonMap = serde_json::Map<String, serde_json::Value>;

/// Set of possible errors returned by client
#[derive(Debug)]
pub enum Error {
    /// The server's response was truncated, corrupted, or malformed
    MalformedResponse,
    /// A path was provided for submission that couldn't be used
    InvalidSubmitFilePath,
    /// A url was provided for submission and a file name couldn't be parsed
    InvalidSubmitUrl,
    /// An error that has bubbled up from an IO call
    IO(std::io::Error),
    /// An error caused by failing to serialize or deserialize a message
    Serialization(serde_json::Error),
    /// An unexpected state was reached serializing submission parameters
    ParameterSerialization,
    // /// A configuration value which has caused errors
    // Configuration(String),
    ApiClientError(ApiClientError),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MalformedResponse => f.write_str("A server response was malformed"),
            // Error::InvalidSha256 =>
            //     f.write_str("An invalid SHA256 string was provided"),
            Error::InvalidSubmitFilePath => f.write_str("An invalid path was given for submission"),
            Error::InvalidSubmitUrl => f.write_str("An invalid URL was given for submission, try setting the file name explicitly"),
            Error::IO(error) => f.write_fmt(format_args!("An IO error ocurred: {error}")),
            Error::Serialization(error) => f.write_fmt(format_args!("An error occurred serializing a body: {error}")),
            Error::ParameterSerialization => f.write_str("Parameter serialization yielded unexpected type."),
            // Error::Configuration(message) =>
            //     f.write_fmt(format_args!("A configuration parameter caused an error: {message}")),
            Error::ApiClientError(e) => f.write_fmt(format_args!("Error with API connection: {e}")),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<url::ParseError> for Error {
    fn from(_value: url::ParseError) -> Self {
        Self::InvalidSubmitUrl
    }
}

impl From<ApiClientError> for Error {
    fn from(value: ApiClientError) -> Self {
        Self::ApiClientError(value)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// A convenience trait that lets you pass true, false, or None for boolean arguments
pub trait IBool: Into<Option<bool>> + Copy {}
impl<T: Into<Option<bool>> + Copy> IBool for T {}
