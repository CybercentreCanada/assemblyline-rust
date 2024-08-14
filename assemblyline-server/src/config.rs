use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use anyhow::{bail, Result};


/// Information server can use to configure its tls binding
#[derive(Debug, Clone)]
pub struct TLSConfig {
    /// Private key for the server certificate
    pub key_pem: String,
    /// TLS certificate for the server to use
    pub certificate_pem: String
}

impl TLSConfig {
    /// Load the TLS information that should be used to serve content
    pub async fn load() -> Result<Option<Self>> {
        let server_key = load_file("SERVER_KEY", "SERVER_KEY_PATH", "/etc/ssl/key.pem").await?;
        let server_cert = load_file("SERVER_CERT", "SERVER_CERT_PATH", "/etc/ssl/cert.pem").await?;
        match (server_key, server_cert) {
            (Some(key_pem), Some(certificate_pem)) => Ok(Some(TLSConfig { key_pem, certificate_pem })),
            (None, Some(_)) => bail!("Initialization error, found certificate but no key found"),
            (Some(_), None) => bail!("Initialization error, found key but no certificate found"),
            (None, None) => Ok(None),
        }
    }
}

async fn load_file(direct_key: &str, path_key: &str, default_path: &str) -> Result<Option<String>> {
    // Try to load the certificates directly from env variables
    match std::env::var(direct_key) {
        Ok(key) => return Ok(Some(key)),
        Err(std::env::VarError::NotPresent) => {},
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse {direct_key} environment variable")
    }

    // See if there are env variables defining a path to load certificates from
    let path = match std::env::var(direct_key) {
        Ok(path) => PathBuf::from(path),
        Err(std::env::VarError::NotPresent) => PathBuf::from(default_path),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse {path_key} environment variable")
    };

    // Load the certificate file
    Ok(Some(tokio::fs::read_to_string(path).await?))
}

/// Load the address the server should bind to
pub fn load_bind_address() -> Result<SocketAddr> {
    match std::env::var("BIND_ADDRESS") {
        Ok(address) => Ok(address.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080))),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse BIND_ADDRESS environment variable")
    }
}