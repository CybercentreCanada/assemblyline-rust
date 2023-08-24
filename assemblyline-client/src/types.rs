

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
        provider: String,
        token: String
    }
}

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

/// Short name for serde json's basic map type
pub type JsonMap = serde_json::Map<String, serde_json::Value>;


pub enum Error {
    Client{message: String, status: u32, api_version: Option<String>, api_response: Option<String>},
    TransportError(String),
    InvalidHeader,
    MalformedResponse,
}

impl Error {
    pub fn client_error(message: String, status: u32) -> Self {
        return Error::Client { message, status, api_response: None, api_version: None }
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