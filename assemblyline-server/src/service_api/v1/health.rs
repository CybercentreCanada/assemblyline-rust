// from flask import Blueprint, abort, make_response

// from assemblyline_service_server.config import STORAGE

use std::sync::Arc;

use poem::http::StatusCode;
use poem::web::Data;
use poem::{get, handler, Endpoint, EndpointExt, Route};

use crate::Core;

// API_PREFIX = "/healthz"
// healthz = Blueprint("healthz", __name__, url_prefix=API_PREFIX)
pub fn api(core: Arc<Core>) -> impl Endpoint {
    Route::new()
    .at("/live", get(liveness))
    .at("/ready", get(readyness))
    .data(core)
}

/// Check if the API is live
///
/// Variables:
/// None
///
/// Arguments:
/// None
///
/// Data Block:
/// None
///
/// Result example:
/// OK or FAIL
#[handler]
fn liveness() -> &'static str {
    "OK"
}


/// Check if the API is Ready
///
/// Variables:
/// None
///
/// Arguments:
/// None
///
/// Data Block:
/// None
///
/// Result example:
/// OK or FAIL
#[handler]
async fn readyness(Data(core): Data<&Arc<Core>>) -> (StatusCode, &'static str) {
    if core.datastore.ping().await {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "FAIL")
    }
}
