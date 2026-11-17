//! This module contains methods and structs for authentications with the server.

/// A value that contains one of the ways to authenticate to Assemblyline
#[derive(Clone, Default)]
pub enum Authentication {
    /// Authenticate with a password
    Password {
        /// The name of the user account connecting
        username: String,
        /// The password of the user connecting
        password: String,
    },
    /// Authenticate with an api key
    ApiKey {
        /// The name of the user account connecting
        username: String,
        /// The API key of the user connecting
        key: String,
    },
    /// Authenticate with an oauth token
    OAuth {
        /// Oauth provider
        provider: String,
        /// Oauth token
        token: String,
    },
    /// No authentication
    #[default]
    None,
}

impl Authentication {
    /// Whether the given authentication method requires logging in
    pub fn need_login(&self) -> bool {
        !matches!(self, Authentication::None)
    }
}
