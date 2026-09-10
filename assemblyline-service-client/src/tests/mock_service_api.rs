use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::Result;
use assemblyline_models::types::JsonMap;
use assemblyline_utilities::{
    connection::ServerType,
    types::response::{APIResponse, ErrorApiResponse},
};
use itertools::Itertools;
use log::{error, info, warn};
use parking_lot::Mutex;
use poem::{
    get, handler,
    http::{HeaderMap, HeaderName},
    listener::{Acceptor, TcpAcceptor},
    middleware::{AddData, NormalizePath},
    web::Json,
    Endpoint, EndpointExt, FromRequest, IntoResponse, Middleware, Request, RequestBody, Response, Route, Server,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{net::TcpListener, task::JoinHandle};

pub const TEST_AUTH_KEY: &str = "test_key_abc_123";
pub const TEST_SERVER_VERSION: &str = "test_server_version";

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

    pub async fn launch_with_custom_endpoints(api: impl Endpoint + 'static) -> Result<(u16, JoinHandle<()>)> {
        let listener = TcpListener::bind("0.0.0.0:0").await?;
        let acceptor = TcpAcceptor::from_tokio(listener).unwrap();
        let port = acceptor.local_addr()[0].as_socket_addr().unwrap().port();
        let app = Route::new()
            .nest(format!("/api/{}/", ServerType::ServiceServer.supported_version()), api)
            .with(NormalizePath::new(poem::middleware::TrailingSlash::Trim))
            // .with(AddData::new(self.config.clone()));
            .with(ServiceAuth {
                auth_key: TEST_AUTH_KEY.to_string(),
            });

        let handle = tokio::spawn(async move {
            info!("Starting test api server on {:?}", acceptor.local_addr());
            let result = Server::new_with_acceptor(acceptor).run(app).await;

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

pub struct ServiceAuth {
    auth_key: String,
}

impl<E: Endpoint> Middleware<E> for ServiceAuth {
    type Output = ServiceAuthImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        ServiceAuthImpl {
            auth_key: self.auth_key.clone(),
            endpoint: ep,
        }
    }
}

pub struct ServiceAuthImpl<E> {
    auth_key: String,
    endpoint: E,
}

impl<E: Endpoint> Endpoint for ServiceAuthImpl<E> {
    type Output = Response;

    async fn call(&self, mut req: Request) -> Result<Self::Output, poem::error::Error> {
        // normalize headers, they are already case insensitive, but lets also normalize _
        let mut new_headers = vec![];
        for (name, value) in req.headers() {
            if name.as_str().contains("_") {
                new_headers.push((name.as_str().replace("_", "-"), value.clone()));
            }
        }
        for (name, value) in new_headers {
            let name = match HeaderName::from_str(&name) {
                Ok(name) => name,
                _ => continue,
            };
            req.headers_mut().insert(name, value);
        }

        // Before anything else, check that the API key is set
        let apikey = match req.header("X-APIKEY") {
            Some(key) => key,
            None => {
                return Err(MockServiceServer::make_empty_api_error(
                    StatusCode::BAD_REQUEST,
                    "missing required key X-APIKEY",
                ))
            }
        };

        if self.auth_key != apikey {
            let client_id = req.header("CONTAINER-ID").unwrap_or("Unknown Client");
            let header_dump = req.headers().iter().map(|(k, v)| format!("{k}={v:?}")).join("; ");
            warn!("Client [{client_id}] provided wrong api key [{apikey}] headers: {header_dump}");
            return Err(MockServiceServer::make_empty_api_error(
                StatusCode::UNAUTHORIZED,
                "Unauthorized access denied",
            ));
        }

        Ok(self.endpoint.call(req).await?.into_response())
    }
}
