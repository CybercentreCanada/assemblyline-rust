use std::fmt::Display;
use std::str::FromStr;

use serde_with::{SerializeDisplay, DeserializeFromStr};



/// A value that contains one of the ways to authenticate to Assemblyline
pub enum Authentication {
    /// Authenticate with a password
    Password{
        /// The name of the user account connecting
        username: String,
        /// The password of the user connecting
        password: String
    },
    /// Authenticate with an api key
    ApiKey{
        /// The name of the user account connecting
        username: String,
        /// The API key of the user connecting
        key: String
    },
    /// Authenticate with an oauth token
    OAuth{
        /// Oauth provider
        provider: String,
        /// Oauth token
        token: String
    }
}

/// sha256 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr)]
pub struct Sha256 {
    hex: String
}

impl std::fmt::Display for Sha256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex)
    }
}

impl std::ops::Deref for Sha256 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.hex
    }
}

impl FromStr for Sha256 {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 64 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(Error::InvalidSha256)
        }
        return Ok(Sha256{ hex })
    }
}

/// Short name for serde json's basic map type
pub type JsonMap = serde_json::Map<String, serde_json::Value>;

/// Set of possible errors returned by client
#[derive(Debug)]
pub enum Error {
    /// An error produced by the client's communication with the server
    Client{
        /// A message describing the error
        message: String,
        /// HTTP status code associated
        status: u32,
        /// Server's API version if available
        api_version: Option<String>,
        /// Server's response details if available
        api_response: Option<String>
    },
    /// An error that occured during a failed communication with the server
    TransportError(String),
    /// An invalid HTTP header name or value was provided
    InvalidHeader,
    /// The server's response was truncated, corrupted, or malformed
    MalformedResponse,
    /// A string could not be converted into a sha256
    InvalidSha256,
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
}

impl Error {
    pub (crate) fn client_error(message: String, status: u32) -> Self {
        return Error::Client { message, status, api_response: None, api_version: None }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Client { message, status, .. } =>
                f.write_fmt(format_args!("Client error [{status}]: {message}")),
            Error::TransportError(message) =>
                f.write_fmt(format_args!("Error communicating with server: {message}")),
            Error::InvalidHeader =>
                f.write_str("An invalid HTTP header name or value was encountered"),
            Error::MalformedResponse =>
                f.write_str("A server response was malformed"),
            Error::InvalidSha256 =>
                f.write_str("An invalid SHA256 string was provided"),
            Error::InvalidSubmitFilePath =>
                f.write_str("An invalid path was given for submission"),
            Error::InvalidSubmitUrl =>
                f.write_str("An invalid URL was given for submission, try setting the file name explicitly"),
            Error::IO(error) =>
                f.write_fmt(format_args!("An IO error ocurred: {error}")),
            Error::Serialization(error) =>
                f.write_fmt(format_args!("An error occurred serializing a body: {error}")),
            Error::ParameterSerialization =>
                f.write_str("Parameter serialization yielded unexpected type."),
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        if let Some(code) = value.status() {
            Error::client_error(value.to_string(), code.as_u16() as u32)
        } else {
            Error::TransportError(value.to_string())
        }
    }
}

impl From<reqwest::header::InvalidHeaderName> for Error {
    fn from(_value: reqwest::header::InvalidHeaderName) -> Self {
        Self::InvalidHeader
    }
}

impl From<reqwest::header::InvalidHeaderValue> for Error {
    fn from(_value: reqwest::header::InvalidHeaderValue) -> Self {
        Self::InvalidHeader
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

impl std::error::Error for Error {

}

pub type Result<T> = std::result::Result<T, Error>;

/// A convenience trait that lets you pass true, false, or None for boolean arguments
pub trait IBool: Into<Option<bool>> + Copy {}
impl<T: Into<Option<bool>> + Copy> IBool for T {}
