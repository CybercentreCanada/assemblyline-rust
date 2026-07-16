// import time

// from flask import request
// from werkzeug.exceptions import BadRequest

// from assemblyline_core.tasking_client import ServiceMissingException
// from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
// from assemblyline_service_server.config import TASKING_CLIENT
// from assemblyline_service_server.helper.response import make_api_response
// from assemblyline_service_server.helper.metrics import get_metrics_factory

use std::sync::Arc;
use std::time::Duration;

use assemblyline_models::messages::service_api;
use assemblyline_models::types::JsonMap;
use log::{debug, error};
use poem::http::{HeaderMap, StatusCode};
use poem::web::{Data, Json};
use poem::{get, handler, Endpoint, EndpointExt, Response, Result, Route};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::service_api::helpers::auth::{ClientInfo, ServiceAuth};
use crate::service_api::helpers::tasking::{
    timestamp, MalformedResult, ServiceMissing, TaskingClient,
};
use crate::service_api::helpers::{make_api_error, make_api_response, make_empty_api_error};

use super::require_header;

/// Extra time added to the status duration to ensure it is stable between state changes
const EXTRA_STATUS_TIME: Duration = Duration::from_secs(1);

// SUB_API = 'task'
// task_api = make_subapi_blueprint(SUB_API, api_version=1)
// task_api._doc = "Perform operations on service tasks"
pub fn api(auth: ServiceAuth) -> impl Endpoint {
    Route::new()
        .at("/", get(get_task).post(task_finished))
        .with(auth)
}

/// Header:
/// {'Container-ID': abcd...123
///  'Service-Name': 'Extract',
///  'Service-Version': '4.0.1',
///  'Service-Tool-Version': '',
///  'Timeout': '30'}
///
/// Result example:
/// {'keep_alive': true}
#[handler]
async fn get_task(
    tasking: Data<&Arc<TaskingClient>>,
    headers: &HeaderMap,
    Data(client_info): Data<&ClientInfo>,
) -> Result<Response> {
    let ClientInfo {
        service_name,
        service_version,
        service_tool_version,
        client_id,
    } = client_info;

    debug!(
        "Getting task for {service_name} {service_version} [{}]",
        service_tool_version.as_deref().unwrap_or("None")
    );
    let timeout_string = require_header!(headers, "timeout", "30");

    let timeout = match timeout_string.parse() {
        Ok(timeout) => Duration::from_secs_f64(timeout),
        Err(_) => {
            return Err(make_empty_api_error(
                StatusCode::BAD_REQUEST,
                &format!("Could not parse [{timeout_string}] as number"),
            ))
        }
    };

    let status_expiry = timestamp(timeout + EXTRA_STATUS_TIME);
    let start_time = std::time::Instant::now();
    let mut attempts = 0;

    loop {
        let remaining = timeout.saturating_sub(start_time.elapsed());
        debug!("get_task {client_id}/{service_name} timeout ({remaining:?}/{timeout:?}) after {attempts} attempts");
        if remaining.is_zero() {
            break;
        }
        attempts += 1;

        let result = tasking
            .get_task(
                client_id,
                *service_name,
                service_version,
                service_tool_version.as_deref(),
                Some(status_expiry),
                remaining,
            )
            .await;

        match result {
            Ok((task, retry)) => {
                if let Some(task) = task {
                    debug!("get_task found task {client_id}/{service_name} timeout ({remaining:?}/{timeout:?}/{:?}) attempt {attempts} complete", start_time.elapsed());
                    return Ok(make_api_response(json!({"task": task})));
                } else if !retry {
                    return Ok(make_api_response(json!({"task": false})));
                }
                debug!("get_task none {client_id}/{service_name} timeout ({remaining:?}/{timeout:?}/{:?}) attempt {attempts} complete", start_time.elapsed());
            }
            Err(err) => {
                if err.downcast_ref::<ServiceMissing>().is_some() {
                    return Err(make_api_error(
                        StatusCode::NOT_FOUND,
                        &err.to_string(),
                        json!({}),
                    ));
                } else {
                    return Err(make_api_error(
                        StatusCode::BAD_REQUEST,
                        &err.to_string(),
                        json!({}),
                    ));
                }
            }
        }
    }

    // We've been processing cache hit for the length of the timeout... bailing out!
    return Ok(make_api_response(json!({"task": false})));
}

/// Header:
/// {'Container-ID': abcd...123
///  'Service-Name': 'Extract',
///  'Service-Version': '4.0.1',
///  'Service-Tool-Version': ''
/// }
///
/// Data Block:
/// {
///  "exec_time": 300,
///  "task": <Original Task Dict>,
///  "result": <AL Result Dict>,
///  "freshen": true
/// }
#[handler]
async fn task_finished(
    Data(client_info): Data<&ClientInfo>,
    tasking: Data<&Arc<TaskingClient>>,
    Json(body): Json<FinishedBody>,
) -> Result<Response> {
    let service_name = client_info.service_name;

    match tasking
        .task_finished(body, &client_info.client_id, service_name)
        .await
    {
        Ok(response) => Ok(make_api_response(response)),
        Err(err) => {
            if let Some(err) = err.downcast_ref::<MalformedResult>() {
                Err(make_empty_api_error(
                    StatusCode::BAD_REQUEST,
                    &format!("{err:?}"),
                ))
            } else if let Some(err) = err.downcast_ref::<serde_json::Error>() {
                Err(make_empty_api_error(
                    StatusCode::BAD_REQUEST,
                    &format!("json error: {err:?}"),
                ))
            } else {
                error!("task_finished error ({client_info:?}): {err:?}");
                Err(make_empty_api_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("{err:?}"),
                ))
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TaskSuccess {
    pub task: JsonMap,
    #[serde(default)]
    pub exec_time: u64,
    pub freshen: bool,
    pub result: service_api::result::Result,
    #[serde(default)]
    pub errors: Vec<service_api::result::ExtraError>,
    #[serde(default)]
    pub warnings: Vec<service_api::result::ExtraWarning>,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum FinishedBody {
    Success(Box<TaskSuccess>),
    Error {
        task: JsonMap,
        #[serde(default)]
        exec_time: u64,
        error: assemblyline_models::datastore::error::Error,
    },
    Other {
        #[serde(flatten)]
        content: JsonMap,
    },
}
