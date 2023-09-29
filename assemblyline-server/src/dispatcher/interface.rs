//! Interface for controlling submission processing
//!
//! POST /submission/
//! GET /submission/<sid>
//! DELETE /submission/<sid>
//! WS /updates/
//! POST /start/<task_key>
//! POST /result/<task_key>
//! POST /error/<task_key>
//! GET /health/
//! GET /stats/
//!

use std::sync::Arc;

use assemblyline_models::datastore::Submission;
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::{post, get, Server, delete, Response, handler, EndpointExt};
use serde::{Serialize, Deserialize};

use crate::logging::LoggerMiddleware;
use crate::tls::random_tls_certificate;
use crate::error::Error;

use super::Session;

pub async fn start_interface(session: Arc<Session>) -> Result<(), Error> {

    let config = &session.config.core.dispatcher.broker_bind;

    let app = poem::Route::new()
        .at("/submission/", post(post_submission))
        .at("/submission/:sid", get(get_submission))
        .at("/submission/:sid", delete(delete_submission))
        .at("/updates/", get(ws_finishing_submission_and_status))
        .at("/start/:task_key", post(post_start_task))
        .at("/result/:task_key", post(post_result_task))
        .at("/error/:task_key", post(post_error_task))
        .at("/health/", get(get_health))
        .at("/stats/", get(get_stats));

    let app = match &config.path {
        Some(route) => poem::Route::new().nest(route, app),
        None => app,
    };

    let app = app
        .data(session.clone())
        .with(LoggerMiddleware);

    let listener = TcpListener::bind(config.address);
    let tls_config = match &config.tls {
        Some(tls) => {
            OpensslTlsConfig::new()
                .cert_from_data(tls.certificate_pem.clone())
                .key_from_data(tls.key_pem.clone())
        },
        None => random_tls_certificate()?
    };
    let listener = listener.openssl_tls(tls_config);

    let exit = tokio::spawn(async move {
        while session.flags.read().await.running {
            session.flags_changes.notified().await
        }
    });

    Server::new(listener)
        .run_with_graceful_shutdown(app, async { exit.await; } , None)
        .await?;
    Ok(())
}


#[handler]
fn post_submission() -> Response {
    todo!()
}

#[handler]
fn get_submission() -> Response {
    todo!()
}

#[handler]
fn delete_submission() -> Response {
    todo!()
}

#[derive(Serialize, Deserialize)]
pub struct LoadInfo {
}

impl LoadInfo {
    pub fn stale() -> Self {
        Self {

        }
    }
}


#[derive(Serialize, Deserialize)]
pub enum DispatchStatusMessage {
    LoadInfo(LoadInfo),
    Finished(Arc<Submission>)
}

#[handler]
fn ws_finishing_submission_and_status() -> Response {
    todo!()
}

#[handler]
fn post_start_task() -> Response {
    todo!()
}

#[handler]
fn post_result_task() -> Response {
    todo!()
}

#[handler]
fn post_error_task() -> Response {
    todo!()
}

#[handler]
fn get_health() -> Response {
    todo!()
}

#[handler]
fn get_stats() -> Response {
    todo!()
}

