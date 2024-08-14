use std::sync::Arc;

use crate::config::TLSConfig;
use crate::logging::LoggerMiddleware;

use super::Ingester;

use anyhow::{Context, Result};
use log::error;
use poem::listener::{Listener, OpensslTlsConfig, TcpListener};
use poem::{get, handler, EndpointExt, Route, Server};

/// API endpoint for null status that is always available
#[handler]
async fn get_status() -> Result<()> {
    return Ok(())
}

pub async fn start(bind_address: std::net::SocketAddr, tls: Option<TLSConfig>, ingester: Arc<Ingester>) {
    while let Err(err) = _start(bind_address, tls.clone(), ingester.clone()).await {
        error!("Error with http interface: {err} {}", err.root_cause());
    }
}


async fn _start(bind_address: std::net::SocketAddr, tls: Option<TLSConfig>, ingester: Arc<Ingester>) -> Result<()> {
    let app = Route::new()
        .at("/alive", get(get_status))
        .data(ingester)
        .with(LoggerMiddleware);

    let listener = TcpListener::bind(bind_address);
    let tls_config = match tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem)
                .key_from_data(tls.key_pem)
        },
        None => {
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

            OpensslTlsConfig::new()
                .cert_from_data(cert.to_pem().context("Could not extract self signed certificate")?)
                .key_from_data(pkey.rsa()?.private_key_to_pem()?)
        }
    };
    let listener = listener.openssl_tls(tls_config);

    Server::new(listener)
        .run(app)
        .await.context("Error in server runtime.")?;
    Ok(())
}

