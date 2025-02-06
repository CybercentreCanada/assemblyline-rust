use assemblyline_models::datastore::service::{DockerConfig, RegistryType};
use assemblyline_models::datastore::{Service, ServiceDelta};
use rand::seq::IndexedRandom;
use tokio::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::HeaderMap;
use assemblyline_models::HEXCHARS;
use log::{error, info};
use poem::listener::{Acceptor, TcpAcceptor};
use poem::Server;

use crate::{Core, TestGuard};

mod badlist;
mod file;
mod safelist;
mod service;
mod tasking;

const AUTH_KEY: &str = "test_key_abc_123";

pub async fn launch(core: Arc<Core>) -> u16 {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let acceptor = TcpAcceptor::from_tokio(listener).unwrap();
    let port = acceptor.local_addr()[0].as_socket_addr().unwrap().port();

    let app = crate::service_api::api(core.clone()).await.unwrap();

    tokio::spawn(async move {
        info!("Starting test server on {:?}", acceptor.local_addr());
        let result = Server::new_with_acceptor(acceptor)
            // .run(
            //     app, 
            .run_with_graceful_shutdown(
                app, 
                async move {
                    core.running.wait_for(false).await
                }, None
            ).await;
        if let Err(err) = result {
            error!("test server crashed: {err}");
        } else {
            info!("test server stopped");
        }
    });

    port
}



pub async fn setup(headers: HeaderMap) -> (reqwest::Client, Arc<Core>, TestGuard, String) {
    std::env::set_var("SERVICE_API_KEY", AUTH_KEY);
    let (core, guard) = Core::test_setup().await;
    let core = Arc::new(core);
    let port = launch(core.clone()).await;
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(30))
        .build().unwrap();
    (client, core, guard, format!("http://localhost:{port}"))
}

pub fn random_hash(length: usize) -> String {
    let mut rng = rand::rng();
    let mut out = String::new();
    while out.len() < length {
        out.push(*HEXCHARS.choose(&mut rng).unwrap());
    }
    out
}


fn build_service() -> Service {
    Service {
        accepts: Default::default(),
        rejects: Some("empty|metadata/.*".to_owned()),
        category: Default::default(),
        classification: Default::default(),
        config: Default::default(),
        description: "A service".into(),
        default_result_classification: Default::default(),
        enabled: Default::default(),
        is_external: Default::default(),
        licence_count: Default::default(),
        min_instances: Default::default(),
        max_queue_length: Default::default(),
        uses_tags: Default::default(),
        uses_tag_scores: Default::default(),
        uses_temp_submission_data: Default::default(),
        uses_metadata: Default::default(),
        monitored_keys: Default::default(),
        name: "TestSvice".to_string(),
        version: "100".to_string(),
        privileged: Default::default(),
        disable_cache: Default::default(),
        stage: Default::default(),
        submission_params: Default::default(),
        timeout: Default::default(),
        docker_config: DockerConfig {
            allow_internet_access: Default::default(),
            command: Default::default(),
            cpu_cores: Default::default(),
            environment: Default::default(),
            image: Default::default(),
            registry_username: Default::default(),
            registry_password: Default::default(),
            registry_type: RegistryType::Docker,
            ports: Default::default(),
            ram_mb: Default::default(),
            ram_mb_min: Default::default(),
            service_account: Default::default(),
        },
        dependencies: Default::default(),
        update_channel: assemblyline_models::datastore::service::ChannelKinds::Beta,
        update_config: Default::default(),
        recursion_prevention: Default::default(),
    }
}

fn empty_delta(service: &Service) -> ServiceDelta {
    serde_json::from_value(serde_json::json!({
        "version": service.version,
    })).unwrap()
}
