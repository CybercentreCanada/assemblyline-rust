use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use log::info;


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
        let server_key = load_file("SERVER_KEY", "SERVER_KEY_PATH").await?; // "/etc/ssl/key.pem"
        let server_cert = load_file("SERVER_CERT", "SERVER_CERT_PATH").await?; // "/etc/ssl/cert.pem"
        match (server_key, server_cert) {
            (Some(key_pem), Some(certificate_pem)) => Ok(Some(TLSConfig { key_pem, certificate_pem })),
            (None, Some(_)) => bail!("Initialization error, found certificate but no key found"),
            (Some(_), None) => bail!("Initialization error, found key but no certificate found"),
            (None, None) => Ok(None),
        }
    }
}

async fn load_file(direct_key: &str, path_key: &str) -> Result<Option<String>> {
    // Try to load the certificates directly from env variables
    match std::env::var(direct_key) {
        Ok(key) => return Ok(Some(key)),
        Err(std::env::VarError::NotPresent) => {},
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse {direct_key} environment variable")
    }

    // See if there are env variables defining a path to load certificates from
    let path = match std::env::var(path_key) {
        Ok(path) => PathBuf::from(path),
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse {path_key} environment variable")
    };

    // Load the certificate file
    info!("Loading {direct_key} from {path:?}");
    Ok(Some(tokio::fs::read_to_string(path).await?))
}

pub async fn get_cluster_ca_cert() -> Result<Option<String>> {
    load_file("CLUSTER_CA_CERT", "CLUSTER_CA_CERT_PATH").await
}


/// Load the address the server should bind to
pub fn load_bind_address() -> Result<SocketAddr> {
    match std::env::var("BIND_ADDRESS") {
        Ok(address) => Ok(address.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080))),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse BIND_ADDRESS environment variable")
    }
}

pub fn bind_address() -> Result<std::net::TcpListener> {
    Ok(std::net::TcpListener::bind(load_bind_address()?)?)
}

pub fn hostname() -> Result<String> {
    match std::env::var("HOSTNAME") {
        Ok(address) => Ok(address.to_owned()),
        Err(std::env::VarError::NotPresent) => Ok("localhost".to_string()),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("Could not parse HOSTNAME environment variable")
    }
}


pub fn generate_certificate() -> Result<poem::listener::OpensslTlsConfig> {
    info!("Generating self signed TLS certificate");
    use openssl::{rsa::Rsa, x509::X509, pkey::PKey, asn1::{Asn1Integer, Asn1Time}, bn::BigNum};

    // Generate our keypair
    let key = Rsa::generate(1 << 11)?;
    let pkey = PKey::from_rsa(key)?;

    // Use that keypair to sign a certificate
    let mut builder = X509::builder()?;

    // Set serial number to 1
    let one = BigNum::from_u32(1)?;
    let serial_number = Asn1Integer::from_bn(&one)?;
    builder.set_serial_number(&serial_number)?;

    // set subject/issuer name
    let mut name = openssl::x509::X509NameBuilder::new()?;
    name.append_entry_by_text("C", "CA")?;
    name.append_entry_by_text("ST", "ON")?;
    name.append_entry_by_text("O", "Inside the house")?;
    name.append_entry_by_text("CN", "localhost")?;
    let name = name.build();
    builder.set_issuer_name(&name)?;
    builder.set_subject_name(&name)?;

    // Set not before/after
    let not_before = Asn1Time::from_unix((chrono::Utc::now() - chrono::Duration::days(1)).timestamp())?;
    builder.set_not_before(&not_before)?;
    let not_after = Asn1Time::from_unix((chrono::Utc::now() + chrono::Duration::days(366)).timestamp())?;
    builder.set_not_after(&not_after)?;

    // set public key
    builder.set_pubkey(&pkey)?;

    // sign and build
    builder.sign(&pkey, openssl::hash::MessageDigest::sha256()).context("Could not sign certificate.")?;
    let cert = builder.build();

    Ok(poem::listener::OpensslTlsConfig::new()
        .cert_from_data(cert.to_pem().context("Could not extract self signed certificate")?)
        .key_from_data(pkey.rsa()?.private_key_to_pem()?))
}