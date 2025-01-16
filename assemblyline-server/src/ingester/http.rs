use std::sync::Arc;

use crate::config::TLSConfig;
use crate::ingester::IngestTask;
use crate::logging::LoggerMiddleware;

use super::Ingester;

use anyhow::{Context, Result};
use log::{error, info};
use poem::listener::{Listener, OpensslTlsConfig, TcpListener};
use poem::web::{Data, Json};
use assemblyline_models::messages::submission::Submission as MessageSubmission;
use poem::{get, handler, post, EndpointExt, Route, Server};

/// API endpoint for null status that is always available
#[handler]
async fn get_status() -> Result<()> {
    return Ok(())
}

#[handler]
async fn start_ingest(ingester: Data<&Arc<Ingester>>, submission: Json<MessageSubmission>) -> Result<()> {
    let task = Box::new(IngestTask::new(submission.0));
    ingester.spawn_ingest(task).await?;
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
        .at("/ingest", post(start_ingest))
        .data(ingester.clone())
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
        .run_with_graceful_shutdown(app, ingester.core.running.wait_for(false), None)
        .await.context("Error in server runtime.")?;
    info!("HTTP interface stopped");
    Ok(())
}

