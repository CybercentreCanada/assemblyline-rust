use std::sync::Arc;

use crate::config::TLSConfig;
use crate::logging::LoggerMiddleware;

use super::Dispatcher;

use anyhow::{Context, Result};
use log::error;
use poem::listener::{Listener, OpensslTlsConfig, TcpListener};
use poem::web::{Data, Json};
use poem::{get, handler, post, EndpointExt, Route, Server};

/// API endpoint for null status that is always available
#[handler]
async fn get_status() -> Result<()> {
    return Ok(())
}


pub async fn start(bind_address: std::net::SocketAddr, tls: Option<TLSConfig>, dispatcher: Arc<Dispatcher>) {
    while let Err(err) = _start(bind_address, tls.clone(), dispatcher.clone()).await {
        error!("Error with http interface: {err} {}", err.root_cause());
    }
}


async fn _start(bind_address: std::net::SocketAddr, tls: Option<TLSConfig>, dispatcher: Arc<Dispatcher>) -> Result<()> {
    let app = Route::new()
        .at("/alive", get(get_status))
        .data(dispatcher)
        .with(LoggerMiddleware);

    let listener = TcpListener::bind(bind_address);
    let tls_config = match tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem)
                .key_from_data(tls.key_pem)
        },
        None => crate::config::generate_certificate()?
    };
    let listener = listener.openssl_tls(tls_config);

    Server::new(listener)
        .run(app)
        .await.context("Error in server runtime.")?;
    Ok(())
}

