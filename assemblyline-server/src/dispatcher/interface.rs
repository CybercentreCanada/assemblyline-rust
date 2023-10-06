//! Interface for controlling submission processing
//!

use std::sync::Arc;

use assemblyline_models::datastore::submission::SubmissionState;
use assemblyline_models::{Sid, Sha256};
use assemblyline_models::datastore::Submission;
use poem::listener::{TcpListener, OpensslTlsConfig, Listener};
use poem::web::{Data, Json, Path};
use poem::http::StatusCode;
use poem::{post, get, Server, delete, Response, handler, EndpointExt, IntoResponse};
use serde::{Serialize, Deserialize};

use crate::dispatcher::submission::process_submission;
use crate::dispatcher::{TaskSignature, ResultMessage, ResultResponse};
use crate::logging::LoggerMiddleware;
use crate::tls::random_tls_certificate;
use crate::error::Error;

use super::Session;

pub async fn start_interface(session: Arc<Session>) -> Result<(), Error> {

    let config = &session.config.core.dispatcher.broker_bind;

    let app = poem::Route::new()
        .at("/submission/", post(post_submission))
        .at("/submission/:sid", get(get_submission))
        .at("/task/start", post(post_start_task))
        .at("/task/result", post(post_result_task))
        .at("/task/error", post(post_error_task))
        .at("/health/", get(get_health))
        .at("/stats/", get(ws_stats));

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

#[derive(Serialize, Deserialize)]
enum PostSubmissionStatus {
    Running,
    Finished,
}

#[derive(Serialize, Deserialize)]
pub struct PostSubmissionResponse {
    pub status: PostSubmissionStatus
}

#[handler]
async fn post_submission(
    Data(session): Data<&Arc<Session>>,
    Json(submission): Json<Submission>
) -> poem::Result<Json<PostSubmissionResponse>> {
    todo!()
//     // Check if its in the finished table
//     if session.finished.read().await.contains_key(&submission.sid) {
//         return Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Finished }))
//     }

//     // Test if the submission is already finished in elasticsearch
//     if let Some(sub) = session.elastic.submission.get(&submission.sid.to_string()).await? {
//         if let SubmissionState::Completed = sub.state {
//             session.finished.write().await.insert(sub.sid, sub);
//             return Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Finished }))
//         }
//     }

//     // See if we can launch this as a new submission
//     let mut active = session.active.write().await;
//     match active.entry(submission.sid) {
//         std::collections::hash_map::Entry::Occupied(_) => {
//             Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Running }))
//         },
//         std::collections::hash_map::Entry::Vacant(entry) => {
//             entry.insert(tokio::spawn(process_submission(session.clone(), submission)));
//             Ok(Json(PostSubmissionResponse { status: PostSubmissionStatus::Running }))
//         },
//     }
}

#[derive(Serialize, Deserialize)]
pub struct GetSubmissionResponse {
    pub submission: Submission
}

#[handler]
async fn get_submission(
    Data(session): Data<&Arc<Session>>,
    Path(sid): Path<Sid>,
) -> poem::Result<Json<GetSubmissionResponse>> {
    todo!();
//     if session.active.read().await.contains_key(&sid) {
//         return Ok(StatusCode::NO_CONTENT.into())
//     }
//     if let Some(submission) = session.finished.read().await.get(&sid) {
//         return Ok(Json(submission).into_response())
//     }
//     return Ok(StatusCode::NOT_FOUND.into())
}

// #[handler]
// async fn delete_submission(
//     Data(session): Data<&Arc<Session>>,
//     Path(sid): Path<Sid>
// ) {
//     session.finished.write().await.remove(&sid);
// }

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
}

// #[handler]
// fn ws_finishing_submission_and_status() -> Response {
//     todo!()
// }

// #[handler]
// fn post_start_task() -> Response {
//     todo!()
// }


// #[derive(Serialize, Deserialize)]
// pub struct TaskResult {
//     pub sid: Sid,
//     pub service_name: String,
//     pub sha256: Sha256,

//     pub worker: String,
// }

// #[handler]
// async fn post_result_task(
//     Data(session): Data<&Arc<Session>>,
//     Json(result): Json<TaskResult>
// ) {
//     let sig = TaskSignature {
//         hash: result.sha256.clone(),
//         service: result.service_name.clone(),
//         sid: result.sid,
//     };
//     session.result_messages.send(ResultMessage::Response(sig, result.worker.clone(), ResultResponse::Result(result))).await;
// }

// #[derive(Serialize, Deserialize)]
// pub struct TaskError {
//     pub sid: Sid,
//     pub service_name: String,
//     pub sha256: Sha256,

//     pub worker: String,

// }

// #[handler]
// async fn post_error_task(
//     Data(session): Data<&Arc<Session>>,
//     Json(result): Json<TaskError>
// ) {
//     let sig = TaskSignature {
//         hash: result.sha256.clone(),
//         service: result.service_name.clone(),
//         sid: result.sid,
//     };
//     session.result_messages.send(ResultMessage::Response(sig, result.worker.clone(), ResultResponse::Error(result))).await;
// }

// #[handler]
// fn get_health() -> Response {
//     todo!()
// }

// #[handler]
// fn get_stats() -> Response {
//     todo!()
// }

