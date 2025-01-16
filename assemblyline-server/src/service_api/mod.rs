use std::sync::Arc;

use anyhow::Result;
use helpers::tasking::TaskingClient;
use poem::middleware::NormalizePath;
use poem::{Endpoint, EndpointExt, Route, Server};

use crate::logging::LoggerMiddleware;
use crate::Core;

pub mod helpers;
pub mod v1;
#[cfg(test)]
mod tests;

pub async fn api(core: Arc<Core>) -> Result<impl Endpoint> {
    let tasking_client = Arc::new(TaskingClient::new(&core).await?);

    Ok(
        Route::new()
        .nest("/api/v1/badlist", v1::badlist::api(core.clone()))
        .nest("/api/v1/file", v1::file::api(core.clone()))
        .nest("/api/v1/service", v1::service::api(core.clone()))
        .nest("/api/v1/task", v1::task::api(core.clone()))
        .nest("/api/v1/safelist", v1::safelist::api(core.clone()))
        .nest("/healthz", v1::health::api(core.clone()))
        .data(tasking_client)
        .with(LoggerMiddleware)
        .with(NormalizePath::new(poem::middleware::TrailingSlash::Trim))
    )
}

pub async fn main(core: Core) -> Result<()> {
    // Bind the HTTP interface
    let bind_address = crate::config::load_bind_address()?;
    let tls_config = crate::config::TLSConfig::load().await?;
    let tcp = crate::http::create_tls_binding(bind_address, tls_config).await?;
    
    // Build the interface
    let running = core.running.clone();
    let core = Arc::new(core);
    let app = api(core).await?;

    // launch the interface
    Server::new_with_acceptor(tcp)
        .run_with_graceful_shutdown(app, running.wait_for(false), None)
        .await?;
    Ok(())
}


// import logging

// from elasticapm.contrib.flask import ElasticAPM
// from flask import Flask
// from flask.logging import default_handler
// from os import environ, path

// from assemblyline.common import forge, log as al_log
// from assemblyline_service_server.api.v1.badlist import badlist_api
// from assemblyline_service_server.api.v1.file import file_api
// from assemblyline_service_server.api.v1.service import service_api
// from assemblyline_service_server.api.v1.task import task_api
// from assemblyline_service_server.api.v1.safelist import safelist_api
// from assemblyline_service_server.healthz import healthz

// config = forge.get_config()
// CERT_BUNDLE = (
//     # If internal encryption is enabled on deployment
//     environ.get('SERVICE_SERVER_CLIENT_CERT_PATH', '/etc/assemblyline/ssl/service-server/tls.crt'),
//     environ.get('SERVICE_SERVER_CLIENT_KEY_PATH', '/etc/assemblyline/ssl/service-server/tls.key')
// )

// # Prepare the logger
// al_log.init_logging('svc')
// LOGGER = logging.getLogger('assemblyline.svc_server')
// LOGGER.info("Service server ready to receive connections...")
// ssl_context = None
// if all([path.exists(fp) for fp in CERT_BUNDLE]):
//     # If all files required are present, start up encrypted comms
//     ssl_context = CERT_BUNDLE

// # Prepare the app
// app = Flask('svc-server')
// app.config['SECRET_KEY'] = config.ui.secret_key


// # Setup logging
// app.logger.setLevel(LOGGER.getEffectiveLevel())
// app.logger.removeHandler(default_handler)
// for ph in LOGGER.parent.handlers:
//     app.logger.addHandler(ph)

// # Setup APMs
// if config.core.metrics.apm_server.server_url is not None:
//     app.logger.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
//     ElasticAPM(app, client=forge.get_apm_client("al_svc_server"))


// if __name__ == '__main__':
//     wlog = logging.getLogger('werkzeug')
//     wlog.setLevel(LOGGER.getEffectiveLevel())
//     for h in LOGGER.parent.handlers:
//         wlog.addHandler(h)

//     app.run(host='0.0.0.0', port=5003, debug=config.logging.log_level == 'DEBUG', ssl_context=ssl_context)
