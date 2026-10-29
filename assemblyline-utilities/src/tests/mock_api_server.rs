use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{Result, anyhow};
use assemblyline_models::types::JsonMap;
use itertools::Itertools;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use poem::{
    Endpoint, EndpointExt, FromRequest, IntoResponse, Middleware, Request, RequestBody, Response, Route, Server, get, handler,
    http::{HeaderMap, HeaderName},
    listener::{Acceptor, TcpAcceptor},
    middleware::{AddData, NormalizePath},
    web::{Data, Json},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

use crate::types::response::{APIResponse, ErrorApiResponse};

pub const TEST_API_VERSION: &str = "v1.0.0";

pub struct MockServiceServer {}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestDataResponse {
    pub header: HashMap<String, String>,
    pub query: HashMap<String, Value>,
    pub error: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MockServerConfig {
    pub retry: Option<Arc<Mutex<i32>>>,
    pub error_response: Option<ErrorApiResponse>,
}

impl MockServerConfig {
    pub fn default() -> Self {
        MockServerConfig {
            retry: None,
            error_response: None,
        }
    }

    pub fn with_retry(retry: Arc<Mutex<i32>>) -> Self {
        MockServerConfig {
            retry: Some(retry),
            error_response: None,
        }
    }

    pub fn with_error_response(error_response: Option<ErrorApiResponse>) -> Self {
        MockServerConfig {
            retry: None,
            error_response: error_response,
        }
    }
}

impl MockServiceServer {
    pub async fn launch_with_test_endpoints(config: MockServerConfig) -> Result<(u16, JoinHandle<()>)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let acceptor = TcpAcceptor::from_tokio(listener).unwrap();
        let port = acceptor.local_addr()[0].as_socket_addr().unwrap().port();
        // let app = api;
        let app_test = Route::new()
            .nest(format!("/test"), MockServiceServer::test_api())
            .with(NormalizePath::new(poem::middleware::TrailingSlash::Trim))
            .with(AddData::new(config));

        let handle = tokio::spawn(async move {
            info!("Starting test api server on {:?}", acceptor.local_addr());
            let result = Server::new_with_acceptor(acceptor).run(app_test).await;

            if let Err(err) = result {
                error!("test api server crashed: {err}");
            } else {
                info!("test api server stopped.");
            }
        });

        Ok((port, handle))
    }

    pub fn make_api_error(code: poem::http::StatusCode, err: &str, response: impl Serialize + Send) -> poem::error::Error {
        let mut response = Json(APIResponse {
            api_response: response,
            api_error_message: Some(err.to_owned()),
            api_server_version: TEST_API_VERSION.to_string(),
            api_status_code: code.as_u16(),
        })
        .into_response();
        response.set_status(code);
        let mut error = poem::error::Error::from_response(response);
        error.set_error_message(format!("[{code}] {err}"));
        error
    }

    pub fn make_api_response(response: impl Serialize + Send) -> Response {
        Json(APIResponse {
            api_response: response,
            api_error_message: None,
            api_server_version: TEST_API_VERSION.to_string(),
            api_status_code: 200,
        })
        .into_response()
    }

    pub fn make_empty_api_error(code: poem::http::StatusCode, err: &str) -> poem::error::Error {
        MockServiceServer::make_api_error(code, err, Option::<()>::None)
    }

    fn test_api() -> impl Endpoint {
        Route::new()
            .at(format!("index"), get(index))
            .at(format!("get_request_data"), get(get_request_data))
            .at(format!("test_retry"), get(test_retry))
            .at(format!("error_response"), get(test_error_response))
    }
}

#[handler]
async fn index() -> &'static str {
    "hello"
}

#[handler]
async fn get_request_data(header: RawHeaderMap, query: poem::web::Query<JsonMap>) -> Result<Response> {
    let mut header_data: HashMap<String, String> = HashMap::new();
    let mut error_data: Vec<String> = Vec::new();
    for (k, v) in header.map {
        if let Some(key) = k {
            match v.to_str() {
                Ok(val) => {
                    header_data.insert(key.to_string(), val.to_string());
                }
                Err(_) => error_data.push(format!("Error loading header key as string: {key}")),
            }
        }
    }

    let mut query_data: HashMap<String, serde_json::Value> = query
        .0
        .iter()
        .map(|(k, v)| (k.to_owned(), serde_json::to_value(v).unwrap().to_owned()))
        .collect();

    let response_data = RequestDataResponse {
        header: header_data,
        error: error_data,
        query: query_data,
    };

    Ok(Json(response_data).into_response())
}

#[handler]
async fn test_retry(config: Data<&MockServerConfig>) -> Result<Response> {
    debug!("Got test_retry request.");
    let retry_arc = config.retry.clone().unwrap();
    let mut retry_value = { retry_arc.lock().to_owned() };

    if retry_value > 1 {
        retry_value -= 1;

        *retry_arc.lock() = retry_value;
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(3.0)).await;

        return Err(anyhow!("Retry will error out for {retry_value} more times."));
    }

    Ok(Json(json!({
        "data": "OK",
    }))
    .into_response())
}

#[handler]
async fn test_error_response(config: Data<&MockServerConfig>) -> Result<Response, poem::error::Error> {
    match &config.error_response {
        Some(data) => {
            let mut response = Json(data.to_owned()).into_response();
            response.set_status(StatusCode::from_u16(data.api_status_code.unwrap_or(0)).unwrap());
            let mut error = poem::error::Error::from_response(response);
            error.set_error_message(format!(
                "[{}] {}",
                data.api_status_code.unwrap_or(0),
                data.api_error_message.to_owned().unwrap_or("no error message.".to_string())
            ));
            return Err(error);
        }
        None => {
            let mut response = Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("This is test internal server error.".to_string());
            Err(poem::error::Error::from_response(response))
        }
    }
}

#[derive(Debug, Clone)]
pub struct RawHeaderMap {
    pub map: HeaderMap,
}

impl<'a> FromRequest<'a> for RawHeaderMap {
    async fn from_request(req: &'a Request, _body: &mut RequestBody) -> Result<Self, poem::error::Error> {
        let headers: HeaderMap = req.headers().to_owned();

        Ok(RawHeaderMap { map: headers })
    }
}
