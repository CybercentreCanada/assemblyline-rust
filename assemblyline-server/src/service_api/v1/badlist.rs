use std::collections::HashMap;
use std::sync::Arc;

use poem::http::StatusCode;
use poem::web::{Data, Json, Path};
use poem::{get, handler, post, Endpoint, EndpointExt, Result, Response, Route};
use serde::{Deserialize, Serialize};

use crate::service_api::helpers::badlist::BadlistClient;
use crate::service_api::helpers::{make_api_error, make_api_response, make_empty_api_error};
use crate::Core;
use super::super::helpers::auth::ServiceAuth;

const EMPTY: &[(); 0] = &[];


// SUB_API = 'badlist'
// badlist_api = make_subapi_blueprint(SUB_API, api_version=1)
// badlist_api._doc = "Query badlisted hashes"
pub fn api(core: Arc<Core>) -> impl Endpoint {
    Route::new()
    .at("/ssdeep", post(similar_ssdeep))
    .at("/tlsh", post(similar_tlsh))
    .at("/tags", post(tags_exists))
    .at("/:qhash", get(exists))
    .data(Arc::new(BadlistClient::new(core.datastore.clone(), core.config.clone(), core.classification_parser.clone())))
    .with(ServiceAuth::new(core))
}


/// Check if a file exists in the badlist.
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
/// GET /api/v1/badlist/123456...654321/
///
/// Result example:
/// <Badlisting object>
#[handler]
async fn exists(Path(qhash): Path<String>, client: Data<&Arc<BadlistClient>>) -> Result<Response> {
    log::debug!("looking for {qhash}");
    match client.exists(&qhash).await {
        Ok(Some(badlist)) => Ok(make_api_response(badlist)),
        Ok(None) => Err(make_empty_api_error(StatusCode::NOT_FOUND, "The hash was not found in the badlist.")),
        Err(err) => Err(make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()))
    }
}

/// Check if a file with a similar SSDeep exists.
/// 
/// Variables:
/// None
/// 
/// Arguments:
/// None
/// 
/// Data Block:
/// {
///     ssdeep : value    => Hash to check
/// }
/// 
/// API call example:
/// GET /api/v1/badlist/ssdeep/
/// 
/// Result example:
/// <Badlisting object>
#[handler]
async fn similar_ssdeep(Json(body): Json<SimilarSSDeepRequest>, client: Data<&Arc<BadlistClient>>) -> Result<Response> {
    match client.find_similar_ssdeep(&body.ssdeep).await {
        Ok(items) => if items.is_empty() {
            Err(make_api_error(StatusCode::NOT_FOUND, "The hash was not found in the badlist.", EMPTY))
        } else {
            Ok(make_api_response(items))
        },
        Err(err) => Err(make_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string(), EMPTY))
    }
}

#[derive(Serialize, Deserialize)]
pub struct SimilarSSDeepRequest {
    ssdeep: String
}

/// Check if a file with a similar TLSH exists.
/// 
/// Variables:
/// None
/// 
/// Arguments:
/// None
/// 
/// Data Block:
/// {
///     tlsh : value    => Hash to check
/// }
/// 
/// API call example:
/// GET /api/v1/badlist/tlsh/
/// 
/// Result example:
/// <Badlisting object>
#[handler]
async fn similar_tlsh(Json(body): Json<SimilarTlshRequest>, client: Data<&Arc<BadlistClient>>) -> Result<Response> {
    match client.find_similar_tlsh(&body.tlsh).await {
        Ok(items) => if items.is_empty() {
            Err(make_api_error(StatusCode::NOT_FOUND, "The hash was not found in the badlist.", EMPTY))
        } else {
            Ok(make_api_response(items))
        },
        Err(err) => Err(make_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string(), EMPTY))
    }
}

#[derive(Serialize, Deserialize)]
pub struct SimilarTlshRequest {
    tlsh: String
}


/// Check if the provided tags exists in the badlist
/// 
/// Variables:
/// None
/// 
/// Arguments:
/// None
/// 
/// Data Block:
/// { # Dictionary of types -> values to check if exists
///     "network.dynamic.domain": [...],
///     "network.static.ip": [...]
/// }
/// 
/// API call example:
/// GET /api/v1/badlist/tags/
/// 
/// Result example:
/// [ # List of existing objecs
///     <badlisting object>,
///     <Badlisting object>
/// ]
#[handler]
async fn tags_exists(
    Json(data): Json<HashMap<String, Vec<String>>>, 
    client: Data<&Arc<BadlistClient>>
) -> Result<Response> {
    match client.exists_tags(data).await {
        Ok(items) => Ok(make_api_response(items)),
        Err(err) => Err(make_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string(), EMPTY))
    }
}
