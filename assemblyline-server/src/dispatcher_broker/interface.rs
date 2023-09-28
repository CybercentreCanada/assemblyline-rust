//! Interface for controlling the dispatch broker
//!
//! POST /submission/
//! GET /submission/<sid>
//! GET /health/
//! WS /submission/finished
//!

use std::sync::Arc;

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
    pub assignment: Option<String>
}


#[handler]
async fn post_submission(
    Data(session): Data<&Arc<BrokerSession>>,
    Json(request): Json<StartSubmissionRequest>,
) -> poem::Result<Response> {
    // check if this submission belongs here or should be forwarded
    let target_instance = session.assign_sid(&request.submission.sid);
    if session.instance != target_instance {
        let url = session.peer_url(target_instance, "submission")?;
        return Ok(Redirect::temporary(url).into_response());
    }

    session.assign_submission(request.submission, request.assignment).await?;

    // Return success
    Ok(StatusCode::OK.into())
}

#[handler]
async fn get_submission(
    Data(session): Data<&Arc<BrokerSession>>,
    Path(sid): Path<String>
) -> poem::Result<Response> {
    // check if this submission belongs here or should be forwarded
    let target_instance = session.assign_sid(&sid);
    if session.instance != target_instance {
        let url = session.peer_url(target_instance, &format!("submission/{sid}"))?;
        return Ok(Redirect::temporary(url).into_response())
    }

    // get submission data
    if let Some(submission) = session.database.get_submission(&sid).await? {
        Ok(Json(submission).into_response())
    } else {
        Ok(StatusCode::NOT_FOUND.into())
    }
}

#[handler]
fn get_health() -> Response {
    todo!();
}
