// from flask import request
// from werkzeug.exceptions import BadRequest

// from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
// from assemblyline_service_server.config import TASKING_CLIENT
// from assemblyline_service_server.helper.response import make_api_response

use std::sync::Arc;

use assemblyline_models::datastore::Service;
use assemblyline_models::JsonMap;
use poem::http::StatusCode;
use poem::web::{Data, Json};
use poem::{handler, put, Endpoint, EndpointExt, Response, Route};
use serde::Serialize;

use crate::service_api::helpers::auth::{ClientInfo, ServiceAuth};
use crate::service_api::helpers::{make_api_response, make_empty_api_error};
use crate::service_api::helpers::tasking::TaskingClient;
use crate::Core;

// SUB_API = 'service'
// service_api = make_subapi_blueprint(SUB_API, api_version=1)
// service_api._doc = "Perform operations on service"
pub fn api(core: Arc<Core>) -> impl Endpoint {
    Route::new()
    .at("/register", put(register_service).post(register_service))
    .with(ServiceAuth::new(core))
}


/// Data Block:
/// < SERVICE MANIFEST >
///
/// Result example:
/// {
///     'keep_alive': true,
///     'new_heuristics': [],
///     'service_config': < APPLIED SERVICE CONFIG >
/// }
#[handler]
async fn register_service(tasking: Data<&TaskingClient>, Json(body): Json<JsonMap>, client_info: Data<&ClientInfo>) -> Response {
    match tasking.register_service(body, &format!("{} - ", client_info.client_id)).await {
        Ok(output) => make_api_response(output),
        Err(err) if err.is_input_error() => make_empty_api_error(StatusCode::BAD_REQUEST, &err.to_string()),
        Err(err) => make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
    }
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub keep_alive: bool, 
    pub new_heuristics: Vec<String>, 
    pub service_config: Service 
}