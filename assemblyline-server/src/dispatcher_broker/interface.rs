//! Interface for controlling the dispatch broker
//!
//! POST /submission/
//! GET /submission/<sid>
//! GET /health/
//!

use std::net::SocketAddr;
use std::sync::Arc;

use assemblyline_models::Sid;
use poem::http::StatusCode;
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::web::{Data, Json, Redirect, Path};
use poem::{EndpointExt, post, get, Server, handler, Response, IntoResponse};
use serde::{Deserialize, Serialize};

use crate::logging::LoggerMiddleware;
use crate::tls::random_tls_certificate;
use crate::error::Error;

use super::BrokerSession;


pub async fn start_interface(session: Arc<BrokerSession>) -> Result<(), Error> {

    let config = &session.config.core.dispatcher.broker_bind;

    let app = poem::Route::new()
        .at("/submission", post(post_submission))
        .at("/submission/:sid", get(get_submission))
        .at("/health", get(get_health));

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
        while session.flags.running.load(std::sync::atomic::Ordering::Acquire) {
            session.flags_changes.notified().await
        }
    });

    Server::new(listener)
        .run_with_graceful_shutdown(app, async { exit.await; } , None)
        .await?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct StartSubmissionRequest {
    pub submission: assemblyline_models::datastore::Submission,
    #[serde(default)]
    pub assignment: Option<SocketAddr>,
    #[serde(default)]
    pub refuse_forward: bool,
    #[serde(default)]
    pub retain: bool,
}

#[derive(Serialize, Deserialize)]
pub struct StartSubmissionResponse {
    pub success: bool,
    pub retrieve_path: Option<String>,
}

#[handler]
async fn post_submission(
    Data(session): Data<&Arc<BrokerSession>>,
    Json(mut request): Json<StartSubmissionRequest>,
) -> poem::Result<(StatusCode, Json<StartSubmissionResponse>)> {
    // check if this submission belongs here or should be forwarded
    let target_instance = request.submission.sid.assign(session.instance_count);
    if session.instance != target_instance {
        // its gone to the wrong host, but forwarding is not wanted
        if request.refuse_forward {
            return Ok((StatusCode::MISDIRECTED_REQUEST, Json(StartSubmissionResponse { success: false, retrieve_path: None })))
        }

        // forward the request to the right host
        let url = session.peer_url(target_instance, "submission")?;
        request.refuse_forward = true;
        let response = session.http_client.post(url).json(&request).send().await;
        return match response {
            Ok(response) => {
                match response.json().await {
                    Ok(resp) => Ok((StatusCode::OK, Json(resp))),
                    Err(err) => Err(poem::Error::new(err, StatusCode::BAD_GATEWAY))
                }
            },
            Err(err) => {
                let status = err.status().unwrap_or(StatusCode::BAD_GATEWAY);
                let error = poem::Error::new(err, status);
                Err(error)
            }
        }
    }

    session.assign_submission(request.submission, request.assignment, request.retain).await?;

    let retrieve_path = if request.retain {
        Some(session.peer_url(session.instance, "submission")?.to_string())
    } else {
        None
    };

    // Return success
    Ok((StatusCode::OK, Json(StartSubmissionResponse {
        success: true,
        retrieve_path
    })))
}

#[handler]
async fn get_submission(
    Data(session): Data<&Arc<BrokerSession>>,
    Path(sid): Path<Sid>
) -> poem::Result<Response> {
    // check if this submission belongs here or should be forwarded
    let target_instance = sid.assign(session.instance_count);
    if session.instance != target_instance {
        let url = session.peer_url(target_instance, &format!("submission/{sid}"))?;
        return Ok(Redirect::temporary(url).into_response())
    }

    todo!()
}

#[handler]
fn get_health() -> Response {
    todo!();
}
