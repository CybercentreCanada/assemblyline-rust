use std::sync::Arc;

use crate::dispatcher::ServiceStartMessage;
use crate::http::TlsAcceptor;
use crate::logging::LoggerMiddleware;
use tracing::instrument;

use super::Dispatcher;

use assemblyline_models::messages::task::{ServiceError, ServiceResponse, ServiceResult};
use log::error;
use poem::http::StatusCode;
use poem::web::{Data, Json};
use poem::{get, handler, post, EndpointExt, Route, Server};
use serde::{Serialize, Deserialize};


/// API endpoint for starting a task
#[instrument]
#[handler]
async fn start_task(
    Json(request): Json<ServiceStartMessage>,
    Data(dispatcher): Data<&Arc<Dispatcher>>
) -> (poem::http::StatusCode, String) {
    if request.dispatcher_id != dispatcher.instance_id {
        return (StatusCode::GONE, "This dispatcher id is no longer accepted at this IP address".to_string())
    }
    let (send, recv) = tokio::sync::oneshot::channel();
    dispatcher.send_dispatch_action(crate::dispatcher::DispatchAction::Start(request, Some(send))).await;
    match recv.await {
        Ok(Ok(())) => (poem::http::StatusCode::OK, "".to_string()),
        Ok(Err(err)) => (poem::http::StatusCode::BAD_REQUEST, err.to_string()),
        Err(_) => (poem::http::StatusCode::INTERNAL_SERVER_ERROR, "Connection dropped".to_string()),
    }
}

/// API endpoint for finishing a task (with an error)
#[instrument]
#[handler]
async fn handle_task_error (
    Json(request): Json<ServiceError>,
    Data(dispatcher): Data<&Arc<Dispatcher>>
) {
    dispatcher.send_dispatch_action(crate::dispatcher::DispatchAction::Result(Box::new(ServiceResponse::Error(request)))).await;
}

/// API endpoint for finishing a task
#[instrument]
#[handler]
async fn handle_task_result (
    Json(request): Json<ServiceResult>,
    Data(dispatcher): Data<&Arc<Dispatcher>>
) {
    dispatcher.send_dispatch_action(crate::dispatcher::DispatchAction::Result(Box::new(ServiceResponse::Result(request)))).await;
}


#[derive(Serialize, Deserialize)]
pub enum Component {
    Dispatcher
}


#[derive(Serialize, Deserialize)]
pub struct BasicStatus {
    pub component: Component,
    pub instance_id: String
}

/// API endpoint for null status that is always available
#[instrument]
#[handler]
fn get_status(Data(dispatcher): Data<&Arc<Dispatcher>>) -> Json<BasicStatus> {
    Json(BasicStatus{
        component: Component::Dispatcher,
        instance_id: dispatcher.instance_id.clone()
    })
}

pub async fn start(acceptor: TlsAcceptor, dispatcher: Arc<Dispatcher>) {
    let app = Route::new()
    .at("/alive", get(get_status))
    .at("/start", post(start_task))
    .at("/error", post(handle_task_error))
    .at("/result", post(handle_task_result))
    .data(dispatcher)
        .with(LoggerMiddleware);

    let result = Server::new_with_acceptor(acceptor)
        .run(app)
        .await;
    if let Err(err) = result {
        error!("http interface failed: {err}");
    }
}

