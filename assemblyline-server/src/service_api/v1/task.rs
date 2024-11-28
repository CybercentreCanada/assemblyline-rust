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

use assemblyline_models::messages::task::Task;
use poem::http::{HeaderMap, StatusCode};
use poem::{get, handler, Endpoint, EndpointExt, Response, Route};
use poem::web::{Data, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::service_api::helpers::auth::{ClientInfo, ServiceAuth};
use crate::service_api::helpers::{make_api_error, make_api_response, make_empty_api_error};
use crate::service_api::helpers::tasking::{ServiceMissing, TaskingClient};
use crate::Core;

use super::require_header;


// SUB_API = 'task'
// task_api = make_subapi_blueprint(SUB_API, api_version=1)
// task_api._doc = "Perform operations on service tasks"
pub fn api(core: Arc<Core>) -> impl Endpoint {
    Route::new()
    .at("/", get(get_task).post(task_finished))
    .with(ServiceAuth::new(core))
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
    tasking: Data<&TaskingClient>,
    headers: &HeaderMap,
    Data(client_info): Data<&ClientInfo>, 
) -> Response {
    let ClientInfo {
        service_name,
        service_version,
        service_tool_version,
        client_id
    } = client_info;
    let timeout_string = require_header!(headers, "timeout", "30");
    let timeout = match timeout_string.parse() {
        Ok(timeout) => Duration::from_secs_f64(timeout),
        Err(_) => return make_empty_api_error(StatusCode::BAD_REQUEST, &format!("Could not parse [{timeout_string}] as number"))
    };

    let status_expiry = (chrono::Utc::now() + timeout).timestamp();
    let start_time = std::time::Instant::now();
    
    loop {
        let remaining = start_time.elapsed().saturating_sub(timeout);
        if remaining.is_zero() {
            break
        }

        let result = tasking.get_task(
            client_id, 
            service_name, 
            service_version, 
            service_tool_version, 
            Some(status_expiry), 
            remaining
        ).await;

        match result {
            Ok((task, retry)) => {
                if let Some(task) = task {
                    return make_api_response(json!({"task": task}))
                } else if !retry {
                    return make_api_response(json!({"task": false}))
                }
            },
            Err(err) => if err.downcast_ref::<ServiceMissing>().is_some() {
                return make_api_error(StatusCode::NOT_FOUND, &err.to_string(), json!({}))
            } else {
                return make_api_error(StatusCode::BAD_REQUEST, &err.to_string(), json!({}))
            }
        }
    }

    // We've been processing cache hit for the length of the timeout... bailing out!
    return make_api_response(json!({"task": false}))
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
    tasking: Data<&TaskingClient>,
    Json(body): Json<FinishedBody>,
) -> Response {
    let service_name = &client_info.service_name;
        
    match tasking.task_finished(body, &client_info.client_id, service_name).await {
        Ok(response) => make_api_response(response),
        Err(err) => make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum FinishedBody {
    Success {
        task: Task,
        exec_time: u64,
        freshen: bool,
        result: models::Result,
    },
    Error {
        task: Task,
        exec_time: u64,
        error: assemblyline_models::datastore::error::Error,
    }
}

pub mod models {
    use std::collections::HashMap;

    use assemblyline_models::datastore::result::{BodyFormat, ResponseBody};
    use assemblyline_models::{ClassificationString, ExpandingClassification, JsonMap, Sha256};
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    /// Result Model
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Result {
        /// Time at which the result was archived
        pub archive_ts: Option<DateTime<Utc>>,
        /// Aggregate classification for the result
        #[serde(flatten)]
        pub classification: ExpandingClassification,
        /// Date at which the result object got created
        #[serde(default="chrono::Utc::now")]
        pub created: DateTime<Utc>,
        /// Expiry timestamp
        pub expiry_ts: Option<DateTime<Utc>>,
        /// The body of the response from the service
        pub response: ResponseBody,
        /// The result body
        #[serde(default)]
        pub result: ResultBody,
        /// SHA256 of the file the result object relates to
        pub sha256: Sha256,
        /// What type information is given along with this result
        #[serde(rename = "type")]
        pub result_type: Option<String>,
        /// ???
        pub size: Option<u64>,
        /// Use to not pass to other stages after this run
        #[serde(default)]
        pub drop_file: bool,
        /// Invalidate the current result cache creation
        #[serde(default)]
        pub partial: bool,

        pub temp_submission_data: JsonMap,
    }

    /// Result Body
    #[derive(Serialize, Deserialize, Debug, Default, Clone)]
    pub struct ResultBody {
        /// Aggregate of the score for all heuristics
        #[serde(default)]
        pub score: i32,
        /// List of sections
        #[serde(default)]
        pub sections: Vec<Section>,
    }

    /// Result Section
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Section {
        /// Should the section be collapsed when displayed?
        #[serde(default)]
        pub auto_collapse: bool,
        /// Text body of the result section
        pub body: Option<String>,
        /// Classification of the section
        pub classification: ClassificationString,
        /// Type of body in this section
        pub body_format: BodyFormat,
        /// Configurations for the body of this section
        pub body_config: Option<HashMap<String, serde_json::Value>>,
        /// Depth of the section
        pub depth: i64,
        /// Heuristic used to score result section
        pub heuristic: Option<Heuristic>,
        /// List of tags associated to this section
        #[serde(default)]
        pub tags: JsonMap,
        /// List of safelisted tags
        #[serde(default)]
        pub safelisted_tags: HashMap<String, Vec<serde_json::Value>>,
        /// Title of the section
        pub title_text: String,
        #[serde(default)]
        pub zeroize_on_sig_safe: bool,
        #[serde(default)]
        pub zeroize_on_tag_safe: bool,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Heuristic {
        pub heur_id: String,
        #[serde(default)]
        pub attack_ids: Vec<String>,
        #[serde(default)]
        pub signatures: HashMap<String, i32>,
        #[serde(default="default_frequency")]
        pub frequency: i32,
        #[serde(default)]
        pub score_map: HashMap<String, i32>,
    }

    fn default_frequency() -> i32 { 1 }
}