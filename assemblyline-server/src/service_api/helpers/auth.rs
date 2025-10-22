use std::str::FromStr;
use std::sync::Arc;

use assemblyline_models::types::ServiceName;
use itertools::Itertools;
use log::{debug, warn};
use poem::http::HeaderName;
use poem::IntoResponse;
use poem::{Endpoint, Middleware, Request, Response, Result, http::StatusCode};

use crate::Core;

use super::make_empty_api_error;



pub struct ServiceAuth {
    core: Arc<Core>,
    auth_key: String,
}

impl ServiceAuth {
    pub fn new(core: Arc<Core>) -> Self {
        let auth_key = match std::env::var("SERVICE_API_KEY"){
            Ok(key) => key,
            Err(_) => {
                warn!("SERVICE_API_KEY not set. If this is not a testing environment please set a key.");
                "ThisIsARandomAuthKey...ChangeMe!".to_string()
            }
        };
        Self {
            core,
            auth_key,
        }
    }
}

impl<E: Endpoint> Middleware<E> for ServiceAuth {
    type Output = ServiceAuthImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        ServiceAuthImpl{
            core: self.core.clone(),
            auth_key: self.auth_key.clone(),
            endpoint: ep
        }
    }
}

pub struct ServiceAuthImpl<E> {
    core: Arc<Core>,
    auth_key: String,
    endpoint: E,
}

impl<E: Endpoint> Endpoint for ServiceAuthImpl<E> {
    type Output = Response;

    async fn call(&self, mut req: Request) -> Result<Self::Output> {
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
            None => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, "missing required key X-APIKEY")),
        };

        if self.auth_key != apikey {
            let client_id = req.header("CONTAINER-ID").unwrap_or("Unknown Client");
            let header_dump = req.headers().iter().map(|(k, v)| format!("{k}={v:?}")).join("; ");
            warn!("Client [{client_id}] provided wrong api key [{apikey}] headers: {header_dump}");
            return Err(make_empty_api_error(StatusCode::UNAUTHORIZED, "Unauthorized access denied"));
        }

        let client_info = match ClientInfo::new(&req) {
            Ok(info) => info,
            Err(key) => {
                let client_id = req.header("CONTAINER-ID").unwrap_or("Unknown Client");
                let header_dump = req.headers().iter().map(|(k, v)| format!("{k}={v:?}")).join("; ");
                debug!("Client [{client_id}] missing required header [{key}] headers: {header_dump}");        
                return Err(make_empty_api_error(StatusCode::BAD_REQUEST, &format!("missing required key {key}")))
            },
        };
        req.extensions_mut().insert(client_info);
        req.extensions_mut().insert(self.core.clone());


        // if config.core.metrics.apm_server.server_url is not None {
        //     elasticapm.set_user_context(username=client_info['service_name'])
        // }

        Ok(self.endpoint.call(req).await?.into_response())
    }
}

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub service_name: ServiceName,
    pub service_version: String,
    pub service_tool_version: Option<String>,
}

impl ClientInfo {
    fn new(req: &Request) -> Result<Self, &'static str> {
        let service_tool_version = match req.header("SERVICE-TOOL-VERSION") {
            None | Some("") => None,
            Some(header) => Some(header.to_owned())
        };

        Ok(ClientInfo {
            client_id: read_required_header(req, "CONTAINER-ID")?.to_owned(),
            service_name: read_required_header(req, "SERVICE-NAME")?.to_owned().as_str().into(),
            service_version: read_required_header(req, "SERVICE-VERSION")?.replace("stable", ""),
            service_tool_version,
        })
    }
}

fn read_required_header<'a>(req: &'a Request, name: &'static str) -> Result<&'a str, &'static str> {
    match req.header(name) {
        Some(header) => Ok(header),
        None => Err(name)
    }
}
