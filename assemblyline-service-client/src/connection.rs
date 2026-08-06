use std::collections::HashMap;

use base64::Engine;
use log::{debug, info, warn};

use anyhow::Result;
use poem::http::{HeaderMap, HeaderValue};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};

use url::Url;

use crate::types::{errors::ServiceHandlerError, response::ErrorApiResponse};

struct OnlyCAVerify {
    preferred: rustls::RootCertStore,
}

impl OnlyCAVerify {
    fn new(preferred: rustls::RootCertStore) -> Self {
        Self { preferred }
    }
}

impl Default for TLSSettings {
    fn default() -> Self {
        Self::Native
    }
}

pub(crate) enum Body {
    None,
    Json(serde_json::Value),
    Prepared(reqwest::Body),
}

fn unpack_certs(cert: String) -> Result<Vec<u8>> {
    let cert = cert.trim();
    let cert = if cert.contains("-----BEGIN ") {
        cert.as_bytes().to_vec()
    } else {
        match base64::prelude::BASE64_STANDARD.decode(cert) {
            Ok(cert) => cert,
            Err(err) => return Err(err.into()),
        }
    };

    Ok(cert)
}

async fn unpack_cert_from_path(cert_path: String) -> Result<Vec<u8>> {
    let cert_data = tokio::fs::read_to_string(cert_path).await?;
    return unpack_certs(cert_data);
}

fn to_rustls_certs(cert: Vec<u8>) -> Result<rustls::RootCertStore> {
    let mut reader = std::io::BufReader::new(&cert[..]);
    let mut store = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut reader)? {
        store.add(&rustls::Certificate(cert))?;
    }
    return Ok(store);
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
    /// Just let any old certificate work
    UnsafeNoVerify,
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub client: reqwest::Client,
    pub server: Url,
    max_retries: Option<u32>,
    default_timeout: Option<f64>,
    tls_setting: TLSSettings,
}

impl Connection {
    pub async fn connect(
        server_string: String,
        retry: Option<u32>,
        verify: TLSSettings,
        raw_headers: HashMap<String, String>,
        timeout: Option<f64>,
    ) -> Result<Self> {
        let client = Connection::create_client(&verify, raw_headers).await?;

        let con = Connection {
            client,
            server: Url::parse(server_string.as_str())?,
            max_retries: retry,
            default_timeout: timeout,
            tls_setting: verify,
        };

        return Ok(con);
    }

    pub async fn update_client(&mut self, raw_headers: HashMap<String, String>) -> Result<()> {
        self.client = Connection::create_client(&self.tls_setting, raw_headers).await?;
        Ok(())
    }

    async fn create_client(
        verify: &TLSSettings,
        raw_headers: HashMap<String, String>,
    ) -> Result<Client> {
        let builder = match verify {
            TLSSettings::Native => reqwest::Client::builder()
                .tls_built_in_root_certs(true)
                .use_rustls_tls(),
            TLSSettings::CARoot(root) => reqwest::Client::builder()
                .tls_built_in_root_certs(true)
                .add_root_certificate(reqwest::Certificate::from_pem(
                    &unpack_cert_from_path(root.clone()).await?,
                )?)
                .use_rustls_tls(),
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
        let client = builder.default_headers(headers).connection_verbose(true).build()?;

        Ok(client)
    }

    pub fn get_api_path(&self, api_version: &str, prefix: &str, args: &[&str]) -> Result<Url> {
        // Calculate the API path using the prefix as shown:
        //     /api/v1/<prefix>/[arg1/[arg2/[...]]]

        let mut result_url = self
            .server
            .join("api/")
            .and_then(|u| u.join(&format!("{api_version}/")))
            .and_then(|u| u.join(&format!("{prefix}/")));

        for arg in args {
            result_url = result_url.and_then(|u| u.join(&format!("{}/", *arg)));
        }

        Ok(result_url?)
    }

    pub(crate) async fn request(
        &self,
        method: reqwest::Method,
        url: Url,
        mut body: Body,
        timeout: Option<f64>,
        params: Option<Vec<(String, String)>>,
        headers: Option<HashMap<String, String>>,
    ) -> Result<Response, ServiceHandlerError> {
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
                info!(
                    "Failed to connect to URL:[{}] {}, retry # {}",
                    &method,
                    url.to_string(),
                    retries
                );
            } else {
                info!(
                    "Sending request to URL: {} with header: {:?}",
                    url.to_string(),
                    method.clone()
                );
            }

            let mut request = self.client.request(method.clone(), url.clone());

            match body {
                Body::None => {}
                Body::Json(json) => {
                    request = request.json(&json);
                    body = Body::Json(json);
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

            // // attach the session header
            // if let Some(token) = self.session_token.read().await.as_ref() {
            //     request = request.header(self.session_header_label.clone(), token);
            // }

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

                    debug!("Error sending request: {}", err);
                    // for other non-http errors break the loop
                    return Err(ServiceHandlerError::ServiceApiConnectionError {
                        status_code: err.status().and_then(|code| Some(code.as_u16())),
                        message: err.to_string(),
                        server_version: None,
                    });
                }
            };

            if response.status().is_success() {
                return Ok(response);
            } else {
                // if we get an error response, parse the JSON to get the detailed error messages and service version
                let status = response.status();
                let error_body = response.json::<ErrorApiResponse>().await;

                match error_body {
                    Ok(body) => {
                        return Err(ServiceHandlerError::ServiceApiConnectionError {
                            message: body.api_error_message.unwrap_or(format!(
                                "Error code ({}) connecting to url {}",
                                status, url
                            )),
                            status_code: Some(body.api_status_code),
                            server_version: Some(body.api_server_version),
                        });
                    }
                    Err(_) => {
                        return Err(ServiceHandlerError::ServiceApiConnectionError {
                            status_code: Some(status.as_u16()),
                            message: format!("Error code ({}) connecting to url {}", status, url),
                            server_version: None,
                        });
                    }
                }
            }
        }

        Err(ServiceHandlerError::ServiceApiConnectionError {
            status_code: None,
            message: "Max retry reached.".to_string(),
            server_version: None,
        })
    }
}
