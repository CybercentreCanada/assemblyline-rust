// from flask import request

// from assemblyline_service_server.api.base import api_login, make_subapi_blueprint
// from assemblyline_service_server.config import SAFELIST_CLIENT
// from assemblyline_service_server.helper.response import make_api_response

use std::sync::Arc;

use poem::http::StatusCode;
use poem::web::{Data, Path, Query};
use poem::{get, handler, Endpoint, EndpointExt, Response, Route};
use serde::Deserialize;

use crate::common::safelist_client::SafelistClient;
use crate::service_api::helpers::auth::ServiceAuth;
use crate::service_api::helpers::{make_api_response, make_empty_api_error};
use crate::Core;

// SUB_API = 'safelist'
// safelist_api = make_subapi_blueprint(SUB_API, api_version=1)
// safelist_api._doc = "Query safelisted hashes"
pub fn api(core: Arc<Core>) -> impl Endpoint {
    Route::new()
    .at("/signatures", get(get_safelisted_signatures))
    .at("/:qhash", get(exists))
    .at("/", get(get_safelisted_tags))
    .data(Arc::new(SafelistClient::new(core.config.clone(), core.datastore.clone())))
    .with(ServiceAuth::new(core))
}



/// Check if a file exists in the safelist.
///
/// Variables:
/// qhash       => Hash to check
///
/// Arguments:
/// None
///
/// Data Block:
/// None
///
/// API call example:
/// GET /api/v1/safelist/123456...654321/
///
/// Result example:
/// <Safelisting object>
#[handler]
async fn exists(Path(qhash): Path<String>, safelist_client: Data<&Arc<SafelistClient>>) -> Response {
    match safelist_client.exists(&qhash).await {
        Ok(Some(safelist)) => make_api_response(safelist),
        Ok(None) => make_empty_api_error(StatusCode::NOT_FOUND, "The hash was not found in the safelist."),
        Err(err) => make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}


/// Get all the safelisted tags in the system
///
/// Variables:
/// tag_types       =>  List of tag types (comma seperated)
///
/// Arguments:
/// None
///
/// Data Block:
/// None
///
/// API call example:
/// GET /api/v1/safelist/?tag_types=network.static.domain,network.dynamic.domain
///
/// Result example:
/// {
///     "match": {  # List of direct matches by tag type
///         "network.static.domain": ["google.ca"],
///         "network.dynamic.domain": ["updates.microsoft.com"]
///     },
///     "regex": {  # List of regular expressions by tag type
///         "network.static.domain": ["*.cyber.gc.ca"],
///         "network.dynamic.domain": ["*.cyber.gc.ca"]
///     }
/// }
#[handler]
async fn get_safelisted_tags(safelist_client: Data<&Arc<SafelistClient>>, Query(query): Query<TagTypeQuery>) -> Response {
    match safelist_client.get_safelisted_tags(query.tag_types.as_deref()).await {
        Ok(response) => make_api_response(response),
        Err(err) => make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}

#[derive(Deserialize)]
struct TagTypeQuery {
    tag_types: Option<String>
}


/// Get all the signatures that were safelisted in the system.
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
/// API call example:
/// GET /api/v1/safelist/signatures/
///
/// Result example:
/// ["McAfee.Eicar", "Avira.Eicar", ...]
#[handler]
async fn get_safelisted_signatures(safelist_client: Data<&Arc<SafelistClient>>) -> Response {
    match safelist_client.get_safelisted_signatures().await {
        Ok(list) => make_api_response(list),
        Err(err) => make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    }
}
