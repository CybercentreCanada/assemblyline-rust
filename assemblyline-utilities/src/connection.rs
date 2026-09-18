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

use crate::constants::{ASSEMBLYLINE_UI_SERVER_SUPPORTED_VERSION, SERVICE_SERVER_SUPPORTED_VERSION};
use crate::types::authentication::Authentication;
use crate::types::errors::ApiClientError;
use crate::types::response::{APIResponse, ErrorApiResponse};

#[derive(Clone)]
pub enum ServerType {
    ServiceServer,
    AssemblylineUiServer,
}

impl ServerType {
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TLSSettings {
    /// Use the native set of certificates
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

impl Default for TLSSettings {
    fn default() -> Self {
        Self::Native
    }
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
                return Err(ApiClientError::Configuration(format!("Couldn't understand the ca cert value: {cert}")));
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
    pub client: reqwest::Client,
    pub server_string: Url,
    api_version: String,
    max_retries: Option<u32>,
    authentication: Authentication,
    //     self.debug = debug
    //     self.is_v4 = False
    //     self.silence_warnings = silence_warnings
    default_timeout: Option<f64>,
    session_header_label: reqwest::header::HeaderName,
    session_token: tokio::sync::RwLock<Option<reqwest::header::HeaderValue>>,
    tls_setting: TLSSettings,
}

impl Connection {
    /// Connect to an assemblyline system
    pub async fn connect(
        server: String,
        server_type: ServerType,
        check_api_version: bool,
        auth: Authentication,
        retry: Option<u32>,
        verify: TLSSettings,
        raw_headers: HashMap<String, String>,
        timeout: Option<f64>,
    ) -> Result<Self, ApiClientError> {
        let client = Connection::create_client(&verify, raw_headers).await?;

        let server_url = Url::parse(&server)?;

        let con = Connection {
            client,
            server_string: server_url,
            api_version: server_type.supported_version(),
            max_retries: retry,
            authentication: auth,
            default_timeout: timeout,
            tls_setting: verify.to_owned(),
            session_header_label: reqwest::header::HeaderName::from_lowercase(b"x-xsrf-token")?,
            session_token: tokio::sync::RwLock::new(None),
        };

        // check API version
        if check_api_version {
            debug!("Check API version of {}", server_type);
            Connection::check_api_version(server_type, &con).await?;
        }

        // Login
        let _auth_details = con.authenticate().await?;
        // session.timeout = auth_session_detail['session_duration']

        return Ok(con);
    }

    pub async fn update_client_default_headers(&mut self, raw_headers: HashMap<String, String>) -> Result<(), ApiClientError> {
        self.client = Connection::create_client(&self.tls_setting, raw_headers).await?;

        let _ = self.authenticate().await?;
        Ok(())
    }

    async fn create_client(verify: &TLSSettings, raw_headers: HashMap<String, String>) -> Result<Client, ApiClientError> {
        let builder = match verify {
            TLSSettings::Native => reqwest::Client::builder().use_native_tls(),
            TLSSettings::CARoot(root) => reqwest::Client::builder()
                .use_rustls_tls()
                .add_root_certificate(reqwest::Certificate::from_pem(&unpack_certs(root.to_owned())?)?),
            TLSSettings::CARootPath(path) => reqwest::Client::builder()
                .use_rustls_tls()
                .add_root_certificate(reqwest::Certificate::from_pem(&unpack_cert_from_path(path.to_owned()).await?)?),
            TLSSettings::UnsafeClusterCARoot(root) => {
                let store = to_rustls_certs(unpack_certs(root.to_owned())?)?;
                let verify = OnlyCAVerify::new(store);

                let connector = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_custom_certificate_verifier(Arc::new(verify))
                    .with_no_client_auth();

                reqwest::Client::builder().use_preconfigured_tls(connector)
            }
            TLSSettings::UnsafeNoVerify => reqwest::Client::builder().danger_accept_invalid_certs(true).use_rustls_tls(),
        };

        // build headers
        let mut headers = HeaderMap::new();
        for (name, value) in raw_headers.into_iter() {
            let name: reqwest::header::HeaderName = name.parse()?;
            headers.insert(name, value.parse()?);
        }

        // finalize client
        Ok(builder.cookie_store(true).default_headers(headers).build()?)
    }

    async fn check_api_version(server_type: ServerType, con: &Connection) -> Result<(), ApiClientError> {
        let api_url = con
            .server_string
            .join("api/")
            .map_err(|_e| ApiClientError::Configuration(format!("Failed to create API checking URL.")))?;
        let versions = con.get(api_url, None, convert_api_output_list).await?;

        let found = versions.into_iter().any(|version| match version.as_str() {
            Some(v) if v.to_owned() == server_type.supported_version() => return true,
            _ => return false,
        });

        if !found {
            return Err(ApiClientError::ClientError {
                // message: "Supported APIS (v4) are not available".to_owned(),
                message: format!("Supported APIS ({}) are not available", server_type.supported_version()),
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
            let login_url = self
                .get_api_path("auth", &["login"])
                .map_err(|_e| ApiClientError::Configuration("Cannot create login URL.".to_string()))?;
            return self.get_with(login_url, Some(data), None, convert_api_output_map).await;
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
        let params = if params.is_empty() { None } else { Some(params) };

        return con(self.request::<()>(reqwest::Method::GET, path, Body::None, None, params, headers).await?).await;
    }

    pub async fn get<Resp, F>(&self, path: Url, headers: Option<HashMap<String, String>>, con: F) -> Result<Resp, ApiClientError>
    where
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        return con(self.request::<()>(reqwest::Method::GET, path, Body::None, None, None, headers).await?).await;
    }

    pub async fn get_with<Req, Resp, F>(&self, path: Url, body: Req, headers: Option<HashMap<String, String>>, con: F) -> Result<Resp, ApiClientError>
    where
        Req: serde::Serialize,
        F: Fn(reqwest::Response) -> BoxFuture<'static, Result<Resp, ApiClientError>>,
    {
        return con(self.request(reqwest::Method::GET, path, Body::Json(body), None, None, headers).await?).await;
    }

    // TODO: TEST
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
        return con(self.request(reqwest::Method::POST, path, body, None, None, headers).await?).await;
    }

    // TODO: TEST
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
        return con(self.request(reqwest::Method::PUT, path, body, None, None, headers).await?).await;
    }

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
        let params = if params.is_empty() { None } else { Some(params) };

        return con(self.request(reqwest::Method::POST, path, body, None, params, headers).await?).await;
    }

    // def put(self, path, **kw):
    //     return self.request(self.session.put, path, convert_api_output, **kw)

    pub fn get_api_path(&self, prefix: &str, args: &[&str]) -> Result<Url, url::ParseError> {
        // Calculate the API path using the prefix as shown:
        //     /api/v1/<prefix>/[arg1/[arg2/[...]]]

        let mut result_url = self
            .server_string
            .join("api/")
            .and_then(|u| u.join(&format!("{}/", self.api_version)))
            .and_then(|u| u.join(&format!("{prefix}/")));

        for arg in args {
            result_url = result_url.and_then(|u| u.join(&format!("{}/", *arg)));
        }

        Ok(result_url?)
    }

    pub fn get_server_path(&self, path: &str) -> Result<Url, url::ParseError> {
        return self.server_string.clone().join(path);
    }

    /// Detailed method to make an http request
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
                debug!("Failed to connect to URL:[{}] {}, retry # {}", &method, &url.to_string(), retries);
            } else {
                debug!("Sending request to URL: {} with header: {:?}", &url.to_string(), method.clone());
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
                let mut header_iter = headers.iter();
                while let Some((key, value)) = header_iter.next() {
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
            let error_body = if let Ok(resp) = serde_json::from_str::<ErrorApiResponse>(&body.clone()) {
                // if we get unauthorized error from the service server, try to authenticate and redo retry.
                if status == StatusCode::UNAUTHORIZED && self.authentication.need_login() {
                    if let Some(msg) = &resp.api_error_message {
                        if is_session_error(msg.as_str()) {
                            self.authenticate().await?;
                            continue;
                        }
                    }
                }
                ApiClientError::ClientError {
                    message: resp.api_error_message.unwrap_or("Unknown Error".to_string()),
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
                error!("Status({}): {}", status, &body);
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

pub enum Body<T: serde::Serialize> {
    None,
    Json(T),
    Multipart(reqwest::multipart::Form),
    Prepared(reqwest::Body),
}

fn is_session_error(error: &str) -> bool {
    matches!(
        error,
        "Session rejected" | "Session not found" | "Session expired" | "Invalid source IP for this session" | "Invalid user agent for this session"
    )
}

pub fn convert_api_output_string(resp: reqwest::Response) -> BoxFuture<'static, Result<String, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<String>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            _ => Err(ApiClientError::MalformedResponse),
        }
    })
}

pub fn convert_api_output_map(resp: reqwest::Response) -> BoxFuture<'static, Result<JsonMap, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<JsonMap>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            _ => Err(ApiClientError::MalformedResponse),
        }
    })
}

pub fn convert_api_output_obj<T: DeserializeOwned + Send>(resp: reqwest::Response) -> BoxFuture<'static, Result<T, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<T>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            _ => Err(ApiClientError::MalformedResponse),
        }
    })
}

pub fn convert_api_output_list(resp: reqwest::Response) -> BoxFuture<'static, Result<Vec<Value>, ApiClientError>> {
    Box::pin(async {
        let body = resp.json::<APIResponse<Vec<Value>>>().await;
        match body {
            Ok(data) => return Ok(data.api_response),
            Err(_) => Err(ApiClientError::MalformedResponse),
        }
    })
}

pub fn convert_output_stream(
    resp: reqwest::Response,
) -> BoxFuture<'static, Result<impl futures::Stream<Item = Result<Bytes, reqwest::Error>>, ApiClientError>> {
    Box::pin(async { Ok(resp.bytes_stream()) })
}

pub fn convert_output_map(resp: reqwest::Response) -> BoxFuture<'static, Result<Value, ApiClientError>> {
    Box::pin(async { resp.json::<Value>().await.map_err(|_| ApiClientError::MalformedResponse) })
}
