use std::sync::Arc;

use itertools::Itertools;
use log::{debug, warn};
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

#[async_trait::async_trait]
impl<E: Endpoint> Endpoint for ServiceAuthImpl<E> {
    type Output = Response;

    async fn call(&self, mut req: Request) -> Result<Self::Output> {
        debug!("authenicating request");
        // Before anything else, check that the API key is set
        let apikey = match req.header("X-APIKEY") {
            Some(key) => key,
            None => return Ok(make_empty_api_error(StatusCode::BAD_REQUEST, "missing required key X-APIKEY")),
        };

        if self.auth_key != apikey {
            let client_id = req.header("CONTAINER-ID").unwrap_or("Unknown Client");
            let header_dump = req.headers().iter().map(|(k, v)| format!("{k}={v:?}")).join("; ");
            // let wsgi_dump = "; ".join(format!("{k}={v}") for k, v in request.environ.items());
            warn!("Client [{client_id}] provided wrong api key [{apikey}] headers: {header_dump}");
            return Ok(make_empty_api_error(StatusCode::UNAUTHORIZED, "Unauthorized access denied"));
        }

        let client_info = match ClientInfo::new(&req) {
            Ok(info) => info,
            Err(key) => return Ok(make_empty_api_error(StatusCode::BAD_REQUEST, &format!("missing required key {key}"))),
        };
        req.extensions_mut().insert(client_info);
        req.extensions_mut().insert(self.core.clone());


        // if config.core.metrics.apm_server.server_url is not None {
        //     elasticapm.set_user_context(username=client_info['service_name'])
        // }

        Ok(self.endpoint.call(req).await?.into_response())
    }
}

pub struct ClientInfo {
    pub client_id: String,
    pub service_name: String,
    pub service_version: String,
    pub service_tool_version: String,
}

impl ClientInfo {
    fn new(req: &Request) -> Result<Self, &'static str> {
        Ok(ClientInfo {
            client_id: read_required_header(req, "CONTAINER_ID")?.to_owned(),
            service_name: read_required_header(req, "SERVICE_NAME")?.to_owned(),
            service_version: read_required_header(req, "SERVICE_VERSION")?.to_owned(),
            service_tool_version: read_required_header(req, "SERVICE_TOOL_VERSION")?.to_owned(),
        })
    }
}

fn read_required_header<'a>(req: &'a Request, name: &'static str) -> Result<&'a str, &'static str> {
    match req.header(name) {
        Some(header) => Ok(header),
        None => Err(name)
    }
}
