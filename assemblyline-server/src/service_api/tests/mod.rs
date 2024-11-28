use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_models::HEXCHARS;
use log::{error, info};
use poem::listener::{Acceptor, TcpAcceptor};
use poem::Server;
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::{Core, TestGuard};

mod badlist;
mod file;
mod safelist;
mod service;
mod tasking;

const AUTH_KEY: &str = "test_key_abc_123";

pub fn launch(core: Arc<Core>) -> u16 {
    let listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let acceptor = TcpAcceptor::from_std(listener).unwrap();
    let port = acceptor.local_addr()[0].as_socket_addr().unwrap().port();

    let app = crate::service_api::api(core.clone());

    tokio::spawn(async move {
        info!("Starting test server on {:?}", acceptor.local_addr());
        let result = Server::new_with_acceptor(acceptor)
            .run(
                app, 

            // .run_with_graceful_shutdown(
            //     app, 
            //     async move {
            //         core.running.wait_for(false).await
            //     }, None
            ).await;
        if let Err(err) = result {
            error!("test server crashed: {err}");
        } else {
            info!("test server stopped");
        }
    });

    port
}



pub async fn setup(headers: http::HeaderMap) -> (reqwest::Client, Arc<Core>, TestGuard, String) {
    std::env::set_var("SERVICE_API_KEY", AUTH_KEY);
    let (core, guard) = Core::test_setup().await;
    let core = Arc::new(core);
    let port = launch(core.clone());
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(30))
        .build().unwrap();
    (client, core, guard, format!("http://localhost:{port}"))
}

pub fn random_hash(length: usize) -> String {
    let mut rng = thread_rng();
    let mut out = String::new();
    while out.len() < length {
        out.push(*HEXCHARS.choose(&mut rng).unwrap());
    }
    out
}