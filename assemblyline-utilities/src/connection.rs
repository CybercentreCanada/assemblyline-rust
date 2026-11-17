//! The connection module is an interface for creating and communicating with
//! Assemblyline servers.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use assemblyline_models::types::JsonMap;
use base64::Engine;
use futures::future::BoxFuture;
use log::{debug, error, warn};
use poem::http::HeaderValue;
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::bytes::Bytes;
use url::Url;

use crate::constants::{
    ASSEMBLYLINE_UI_SERVER_SUPPORTED_VERSION, SERVICE_SERVER_SUPPORTED_VERSION,
};
use crate::types::authentication::Authentication;
use crate::types::errors::ApiClientError;
use crate::types::response::{APIResponse, ErrorApiResponse};

/// Specify the server type to interface with
///
#[derive(Clone)]
pub enum ServerType {
    /// Assemblyline service server for facilitating service tasks and results
    ServiceServer,
    /// Assemblyline API server for user interactions with an Assemblyline system
    AssemblylineUiServer,
}

impl ServerType {
    /// Get the server version that is supported by this library
    ///
    /// :return: Version string of the supported server version
    pub fn supported_version(&self) -> String {
        match self {
            ServerType::ServiceServer => SERVICE_SERVER_SUPPORTED_VERSION.to_owned(),
            ServerType::AssemblylineUiServer => ASSEMBLYLINE_UI_SERVER_SUPPORTED_VERSION.to_owned(),
        }
    }
}

impl std::fmt::Display for ServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ServerType::ServiceServer => write!(f, "Service Server"),
            ServerType::AssemblylineUiServer => write!(f, "Assemblyline UI Server"),
        }
    }
}

struct OnlyCAVerify {
    preferred: rustls::RootCertStore,
}

impl OnlyCAVerify {
    fn new(preferred: rustls::RootCertStore) -> Self {
        Self { preferred }
    }
}

impl rustls::client::ServerCertVerifier for OnlyCAVerify {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::Certificate,
        intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
        use rustls::client::verify_server_cert_signed_by_trust_anchor;

        let cert = rustls::server::ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(&cert, &self.preferred, intermediates, now)?;
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

/// Specifiy how the client should perform its tls verification
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub enum TLSSettings {
    /// Use the native set of certificates
    #[default]
    Native,
    /// Use the native set of certificates and the given certificate
    CARoot(String),
    /// Use the native st of certificates and the given certificate at the given path
    CARootPath(String),
    /// Use the given certificate and only verify against it doing no other checks
    UnsafeClusterCARoot(String),
    /// Just let any old certificate work
    UnsafeNoVerify,
}

async fn unpack_cert_from_path(cert_path: String) -> Result<Vec<u8>, ApiClientError> {
    let cert_data = tokio::fs::read_to_string(cert_path).await?;

    return unpack_certs(cert_data);
}

fn unpack_certs(cert: String) -> Result<Vec<u8>, ApiClientError> {
    let cert = cert.trim();
    let cert = if cert.contains("-----BEGIN ") {
        cert.as_bytes().to_vec()
    } else {
        match base64::prelude::BASE64_STANDARD.decode(cert) {
            Ok(cert) => cert,
            Err(_) => {
                return Err(ApiClientError::Configuration(format!(
                    "Couldn't understand the ca cert value: {cert}"
                )));
            }
        }
    };

    Ok(cert)
}

fn to_rustls_certs(cert: Vec<u8>) -> Result<rustls::RootCertStore, ApiClientError> {
    let mut reader = std::io::BufReader::new(&cert[..]);
    let mut store = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader)? {
        store.add(&rustls::Certificate(cert))?;
    }
    return Ok(store);
}

/// A connection abstraction to handle queries
pub struct Connection {
    /// request client for interacting with the server
    pub client: reqwest::Client,
    /// URL of the server host
    pub server_host_url: Url,
    api_version: String,
    max_retries: Option<u32>,
    authentication: Authentication,
    default_timeout: Option<f64>,
    session_header_label: reqwest::header::HeaderName,
    session_token: tokio::sync::RwLock<Option<reqwest::header::HeaderValue>>,
    tls_setting: TLSSettings,
}

/// Configure connection settings to Assemblyline server and create the connection client
pub struct ConnectionBuilder {
    server_host: String,
    server_type: ServerType,
    check_api_version: bool,
    authentication: Option<Authentication>,
    retry: Option<u32>,
    tls_settings: Option<TLSSettings>,
    raw_headers: Option<HashMap<String, String>>,
    timeout: Option<f64>,
}

impl ConnectionBuilder {
    /// create a new connection builder class
    pub fn new(server_host: String, server_type: ServerType) -> Self {
        ConnectionBuilder {
            server_host,
            server_type,
            check_api_version: false,
            authentication: None,
            retry: None,
            tls_settings: None,
            raw_headers: None,
            timeout: None,
        }
    }

    /// whether or not to check assemblyline server API version matches the supported API version
    pub fn check_api_version(&mut self, check: bool) -> &mut Self {
        self.check_api_version = check;
        self
    }

    /// set the authentication method to the Assemblyline server
    pub fn set_authentication(&mut self, auth: Authentication) -> &mut Self {
        self.authentication = Some(auth);
        self
    }

    /// set the default raw headers per request
    pub fn set_raw_headers(&mut self, headers: HashMap<String, String>) -> &mut Self {
        self.raw_headers = Some(headers);
        self
    }

    /// set the tls connection settings
    pub fn set_tls_settings(&mut self, tls_settings: TLSSettings) -> &mut Self {
        self.tls_settings = Some(tls_settings);
        self
    }

    /// set default connection timeout
    pub fn set_timeout(&mut self, timeout: f64) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// set default number of request retries
    pub fn set_retry(&mut self, retry: u32) -> &mut Self {
        self.retry = Some(retry);
        self
    }

    /// Build a Connection object and connect to the Assemblyline server
    pub async fn build_and_connect(&mut self) -> Result<Connection, ApiClientError> {
        let tls_settings = self.tls_settings.clone().unwrap_or_default();
        let raw_headers = self.raw_headers.clone().unwrap_or_default();

        let client = Connection::create_client(&tls_settings, raw_headers).await?;

        let server_url = Url::parse(&self.server_host)?;

        let auth = self.authentication.clone().unwrap_or_default();

        let con = Connection {
            client,
            server_host_url: server_url,
            api_version: self.server_type.clone().supported_version(),
            max_retries: self.retry,
            authentication: auth,
            default_timeout: self.timeout,
            tls_setting: tls_settings,
            session_header_label: reqwest::header::HeaderName::from_lowercase(b"x-xsrf-token")?,
            session_token: tokio::sync::RwLock::new(None),
        };

        // check API version
        if self.check_api_version {
            debug!("Check API version of {}", self.server_type);
            Connection::check_api_version(self.server_type.clone(), &con).await?;
        }

        // Login
        let _auth_details = con.authenticate().await?;
        // session.timeout = auth_session_detail['session_duration']

        return Ok(con);
    }
}

impl Connection {
    /// update the default headers used for each request
    pub async fn update_client_default_headers(
        &mut self,
        raw_headers: HashMap<String, String>,
    ) -> Result<(), ApiClientError> {
        self.client = Connection::create_client(&self.tls_setting, raw_headers).await?;

        let _ = self.authenticate().await?;
        Ok(())
    }

    async fn create_client(
        verify: &TLSSettings,
        raw_headers: HashMap<String, String>,
    ) -> Result<Client, ApiClientError> {
        let builder = match verify {
            TLSSettings::Native => reqwest::Client::builder().use_native_tls(),
            TLSSettings::CARoot(root) => reqwest::Client::builder()
                .use_rustls_tls()
                .add_root_certificate(reqwest::Certificate::from_pem(&unpack_certs(
                    root.to_owned(),
                )?)?),
            TLSSettings::CARootPath(path) => reqwest::Client::builder()
                .use_rustls_tls()
                .add_root_certificate(reqwest::Certificate::from_pem(
                    &unpack_cert_from_path(path.to_owned()).await?,
                )?),
            TLSSettings::UnsafeClusterCARoot(root) => {
                let store = to_rustls_certs(unpack_certs(root.to_owned())?)?;
                let verify = OnlyCAVerify::new(store);

                let connector = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_custom_certificate_verifier(Arc::new(verify))
                    .with_no_client_auth();

                reqwest::Client::builder().use_preconfigured_tls(connector)
            }
            TLSSettings::UnsafeNoVerify => reqwest::Client::builder()
                .danger_accept_invalid_certs(true)
                .use_rustls_tls(),
        };

        // build headers
        let mut headers = HeaderMap::new();
        for (name, value) in raw_headers.into_iter() {
            let name: reqwest::header::HeaderName = name.parse()?;
            headers.insert(name, value.parse()?);
        }

        // finalize client
        Ok(builder
            .cookie_store(true)
            .default_headers(headers)
            .build()?)
    }

    async fn check_api_version(
        server_type: ServerType,
        con: &Connection,
    ) -> Result<(), ApiClientError> {
        let mut api_url = con.server_host_url.clone();

        match api_url.path_segments_mut() {
            Ok(mut u) => u.push("api"),
            Err(_) => {
                return Err(ApiClientError::Configuration(
                    "Failed to create API checking URL.".to_owned(),
                ));
            }
        };

        let versions = con.get(api_url, None, convert_api_output_list).await?;

        let found = versions.into_iter().any(|version| match version.as_str() {
            Some(v) if v == server_type.supported_version() => return true,
            _ => return false,
        });

        if !found {
            return Err(ApiClientError::ClientError {
                message: format!(
                    "Supported APIS ({}) are not available",
                    server_type.supported_version()
                ),
                status_code: Some(400),
                server_version: None,
            });
        }

        Ok(())
    }

    /// Login to the assemblyline system
    #[async_recursion::async_recursion]
    async fn authenticate(&self) -> Result<JsonMap, ApiClientError> {
        debug!("login");
        let body = match &self.authentication {
            Authentication::Password { username, password } => Some(json!({
                "user": username,
                "password": password,
            })),
            Authentication::ApiKey { username, key } => Some(json!({
                "user": username,
                "apikey": key,
            })),
            Authentication::OAuth { provider, token } => Some(json!({
                "oauth_provider": provider,
                "oauth_token": token
            })),
            Authentication::None => None,
        };

        if let Some(data) = body {
            let login_url = self.get_api_path("auth", &["login"]).map_err(|_e| {
                ApiClientError::Configuration("Cannot create login URL.".to_string())
            })?;
            return self
                .get_with(login_url, Some(data), None, convert_api_output_map)
                .await;
        } else {
            debug!("No authentication.");
            return Ok(JsonMap::default());
        }
    }

    // def delete(self, path, **kw):
    //     return self.request(self.session.delete, path, convert_api_output, **kw)

    // def download(self, path, process, **kw):
    //     return self.request(self.session.get, path, process, **kw)

    // pub fn get(self: &Arc<Self>, path: &str) -> RequestBuilder {
    //     // return self.request(self.session.get, path, convert_api_output, **kw)
    //     todo!()
    // }

    /// Perform HTTP get request with the specified URL params
    ///
    /// :type Resp: response type
    /// :param path: The request URL
    /// :param params: URL parameters in key value pairs
    /// :param headers: Optional added header values
    /// :param con: Conversion function that converts the output of the request to the given response type
    /// :return: Result of response data in the given response type
    pub async fn get_params<Resp, F>(
        &self,
        path: Url,
        params: Vec<(String, String)>,
        headers: Option<HashMap<String, String>>,
        con: F,
    ) -> Result<Resp, ApiClientError>
    where
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        let params = if params.is_empty() {
            None
        } else {
            Some(params)
        };

        return con(self
            .request::<()>(
                reqwest::Method::GET,
                path,
                Body::None,
                None,
                params,
                headers,
            )
            .await?)
        .await;
    }

    /// Perform HTTP get request
    ///
    /// :type Resp: response type
    /// :param path: The request URL
    /// :param headers: Optional added header values
    /// :param con: Conversion function that converts the output of the request to the given Type
    /// :return: Result of response data in the given response type
    pub async fn get<Resp, F>(
        &self,
        path: Url,
        headers: Option<HashMap<String, String>>,
        con: F,
    ) -> Result<Resp, ApiClientError>
    where
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        return con(self
            .request::<()>(reqwest::Method::GET, path, Body::None, None, None, headers)
            .await?)
        .await;
    }

    /// Perform HTTP get request with request body
    ///
    /// :type Resp: response type
    /// :type Req: request body type
    /// :param path: The request URL
    /// :param body: value of request body
    /// :param headers: Optional added header values
    /// :param con: Conversion function that converts the output of the request to the given Type
    /// :return: Result of response data in the given response type
    pub async fn get_with<Req, Resp, F>(
        &self,
        path: Url,
        body: Req,
        headers: Option<HashMap<String, String>>,
        con: F,
    ) -> Result<Resp, ApiClientError>
    where
        Req: serde::Serialize,
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        return con(self
            .request(
                reqwest::Method::GET,
                path,
                Body::Json(body),
                None,
                None,
                headers,
            )
            .await?)
        .await;
    }

    /// Perform HTTP POST request with the specified URL params
    ///
    /// :type Resp: response type
    /// :type Req: request body type
    /// :param path: The request URL
    /// :param body: The request body
    /// :param headers: Optional added header values
    /// :param con: Conversion function that converts the output of the request to the given response type
    /// :return: Result of response data in the given response type
    pub async fn post<Req, Resp, F>(
        &self,
        path: Url,
        body: Body<Req>,
        headers: Option<HashMap<String, String>>,
        con: F,
    ) -> Result<Resp, ApiClientError>
    where
        Req: serde::Serialize,
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        return con(self
            .request(reqwest::Method::POST, path, body, None, None, headers)
            .await?)
        .await;
    }

    /// Perform HTTP PUT request with the specified URL params
    ///
    /// :type Resp: response type
    /// :type Req: request body type
    /// :param path: The request URL
    /// :param body: The request body
    /// :param headers: Optional added header values
    /// :param con: Conversion function that converts the output of the request to the given response type
    /// :return: Result of response data in the given response type
    pub async fn put<Req, Resp, F>(
        &self,
        path: Url,
        body: Body<Req>,
        headers: Option<HashMap<String, String>>,
        con: F,
    ) -> Result<Resp, ApiClientError>
    where
        Req: serde::Serialize,
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        return con(self
            .request(reqwest::Method::PUT, path, body, None, None, headers)
            .await?)
        .await;
    }

    /// Perform HTTP POST request with the specified URL params
    ///
    /// :type Resp: response type
    /// :type Req: request body type
    /// :param path: The request URL
    /// :param body: The request body
    /// :param params: The URL parameters
    /// :param headers: Optional added header values
    /// :param con: Conversion function that converts the output of the request to the given response type
    /// :return: Result of response data in the given response type
    pub async fn post_params<Req, Resp, F>(
        &self,
        path: Url,
        body: Body<Req>,
        params: Vec<(String, String)>,
        headers: Option<HashMap<String, String>>,
        con: F,
    ) -> Result<Resp, ApiClientError>
    where
        Req: serde::Serialize,
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        let params = if params.is_empty() {
            None
        } else {
            Some(params)
        };

        return con(self
            .request(reqwest::Method::POST, path, body, None, params, headers)
            .await?)
        .await;
    }

    // def put(self, path, **kw):
    //     return self.request(self.session.put, path, convert_api_output, **kw)

    /// Construct the full server URL path with the give prefix and arguments.
    ///
    /// :param prefix: the prefix segment before arguments
    /// :param args: ordered list of arguments to add to the URL
    /// :return: Result of the full URL path.
    pub fn get_api_path(&self, prefix: &str, args: &[&str]) -> Result<Url, url::ParseError> {
        // Calculate the API path using the prefix as shown:
        //     /api/v1/<prefix>/[arg1/[arg2/[...]]]

        let mut result_url = self.server_host_url.clone();

        match result_url.path_segments_mut() {
            Ok(mut path_segment) => path_segment
                .push("api")
                .push(&self.api_version)
                .push(prefix)
                .extend(args),
            Err(_) => return Err(url::ParseError::SetHostOnCannotBeABaseUrl),
        };

        Ok(result_url)
    }

    /// Construct the full server URL path extended with the given path.
    ///
    /// :param path: the path to append to the server URL
    /// :return: Result of the full URL path.
    pub fn get_server_path(&self, path: &str) -> Result<Url, url::ParseError> {
        let mut server_path_url = self.server_host_url.clone();

        match server_path_url.path_segments_mut() {
            Ok(mut u) => {
                u.push(path);
            }
            Err(_) => return Err(url::ParseError::SetHostOnCannotBeABaseUrl),
        }

        return Ok(server_path_url);
    }

    /// Detailed method to make an http request
    ///
    /// :type Req: the specified request type
    /// :param method: HTTP request method
    /// :param url: URL to send the HTTP request to
    /// :param body: HTTP request body
    /// :param timeout: Optional request timeout value
    /// :param params: Optional URL parameters
    /// :param headers: Optional additional header values
    /// :return: Result of HTTP request
    pub(crate) async fn request<Req>(
        &self,
        method: reqwest::Method,
        url: Url,
        mut body: Body<Req>,
        timeout: Option<f64>,
        params: Option<Vec<(String, String)>>,
        headers: Option<HashMap<String, String>>,
    ) -> Result<reqwest::Response, ApiClientError>
    where
        Req: serde::Serialize,
    {
        // Apply default timeout parameter if not passed elsewhere
        let timeout = match timeout {
            Some(time) => Some(time),
            None => self.default_timeout,
        };
        let mut retries = 0;
        while self.max_retries.is_none_or(|max| retries <= max) {
            if retries > 0 {
                let seconds = 2.0_f64.min(2.0_f64.powf(retries as f64 - 7.0));
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(seconds)).await;
                debug!(
                    "Failed to connect to URL:[{}] {}, retry # {}",
                    method, url, retries
                );
            } else {
                debug!("Sending request to URL: {} with header: {:?}", url, method);
            }

            let mut request = self.client.request(method.clone(), url.clone());

            match body {
                Body::None => {}
                Body::Json(json) => {
                    request = request.json(&json);
                    body = Body::Json(json);
                }
                Body::Multipart(form) => {
                    request = request.multipart(form);
                    body = Body::None;
                }
                Body::Prepared(data) => {
                    request = request.body(data);
                    body = Body::None;
                }
            }

            // set timeout
            if let Some(timeout) = timeout {
                request = request.timeout(std::time::Duration::from_secs_f64(timeout));
            }

            // attach the session header
            if let Some(token) = self.session_token.read().await.as_ref() {
                request = request.header(self.session_header_label.clone(), token);
            }

            if let Some(params) = &params {
                request = request.query(params);
            }

            if let Some(headers) = &headers {
                let header_iter = headers.iter();
                for (key, value) in header_iter {
                    if let Ok(v) = HeaderValue::from_str(value) {
                        request = request.header(key, v);
                    }
                }
            }

            // issue the request
            retries += 1;
            let response = match request.send().await {
                Ok(response) => response,
                Err(err) => {
                    // for connection errors continue, previously this would not include
                    // ssl or proxy errors
                    if err.is_connect() {
                        warn!("TLS Error: {}", err);
                        continue;
                    }

                    if err.is_timeout() {
                        warn!("Connection timeout. Retrying {retries} times.");
                        continue;
                    }

                    // for other non-http errors break the loop
                    return Err(err.into());
                }
            };

            // Capture session cookie
            for cookie in response.cookies() {
                if cookie.name() == "XSRF-TOKEN" {
                    *self.session_token.write().await = Some(cookie.value().parse()?)
                }
            }

            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }

            // try to parse and recover different error types
            let body = response.text().await?;

            // check if the response body is in the standard service api error response format
            let error_body =
                if let Ok(resp) = serde_json::from_str::<ErrorApiResponse>(&body.clone()) {
                    // if we get unauthorized error from the service server, try to authenticate and redo retry.
                    if status == StatusCode::UNAUTHORIZED
                        && self.authentication.need_login()
                        && let Some(msg) = &resp.api_error_message
                        && is_session_error(msg.as_str())
                    {
                        self.authenticate().await?;
                        continue;
                    }

                    ApiClientError::ClientError {
                        message: resp
                            .api_error_message
                            .unwrap_or("Unknown Error".to_string()),
                        status_code: resp.api_status_code,
                        server_version: resp.api_server_version,
                    }
                } else {
                    ApiClientError::ClientError {
                        message: body.clone(),
                        status_code: Some(status.as_u16()),
                        server_version: None,
                    }
                };

            if status == StatusCode::GATEWAY_TIMEOUT {
                error!("Status({}): {}", status, body);
            } else {
                // return error immediately and we don't need to retry
                return Err(error_body);
            }
        }

        return Err(ApiClientError::ClientError {
            message: "Max retry reached, could not perform the request.".to_owned(),
            status_code: Some(429),
            server_version: None,
        });
    }
}

/// HTTP Request body type
pub enum Body<T: serde::Serialize> {
    /// No request body
    None,
    /// Json
    Json(T),
    /// Multipart form format
    Multipart(reqwest::multipart::Form),
    /// Request already prepared in request::Body format
    Prepared(reqwest::Body),
}

fn is_session_error(error: &str) -> bool {
    matches!(
        error,
        "Session rejected"
            | "Session not found"
            | "Session expired"
            | "Invalid source IP for this session"
            | "Invalid user agent for this session"
    )
}

/// Helper function that converts the Response object to String
///
/// :param resp: The response object to convert
/// :return: Result of conversion
pub fn convert_api_output_string(
    resp: reqwest::Response,
) -> BoxFuture<'static, Result<String, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<String>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            _ => Err(ApiClientError::MalformedResponse),
        }
    })
}

/// Helper function that converts the Response object to a JsonMap
///
/// :param resp: The response object to convert
/// :return: Result of conversion
pub fn convert_api_output_map(
    resp: reqwest::Response,
) -> BoxFuture<'static, Result<JsonMap, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<JsonMap>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            _ => Err(ApiClientError::MalformedResponse),
        }
    })
}

/// Helper function that converts the Response object to a Specified object type
///
/// :type T: the type for the response to be convert to
/// :param resp: The response object to convert
/// :return: Result of conversion
pub fn convert_api_output_obj<T: DeserializeOwned + Send>(
    resp: reqwest::Response,
) -> BoxFuture<'static, Result<T, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<T>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            _ => Err(ApiClientError::MalformedResponse),
        }
    })
}

/// Helper function that converts the Response object to a Vec of Value object
///
/// :param resp: The response object to convert
/// :return: Result of conversion
pub fn convert_api_output_list(
    resp: reqwest::Response,
) -> BoxFuture<'static, Result<Vec<Value>, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<Vec<Value>>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            Err(_) => Err(ApiClientError::MalformedResponse),
        }
    })
}

/// Helper function that converts the Response object to a byte stream
///
/// :param resp: The response object to convert
/// :return: Result of conversion
pub fn convert_output_stream(
    resp: reqwest::Response,
) -> BoxFuture<
    'static,
    Result<impl futures::Stream<Item = Result<Bytes, reqwest::Error>>, ApiClientError>,
> {
    Box::pin(async { Ok(resp.bytes_stream()) })
}

/// Helper function that converts the Response object to a json Value object
///
/// :param resp: The response object to convert
/// :return: Result of conversion
pub fn convert_output_map(
    resp: reqwest::Response,
) -> BoxFuture<'static, Result<Value, ApiClientError>> {
    Box::pin(async {
        resp.json::<Value>()
            .await
            .map_err(|_| ApiClientError::MalformedResponse)
    })
}
