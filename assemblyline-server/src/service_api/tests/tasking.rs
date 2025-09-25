// from unittest.mock import patch, MagicMock

// import pytest

// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.error import Error
// from assemblyline.odm.messages.task import Task
// from assemblyline.odm.randomizer import random_minimal_obj, random_model_obj
// from assemblyline.common.constants import SERVICE_STATE_HASH
// from assemblyline.remote.datatypes.hash import ExpiringHash
// from assemblyline.common import forge
// from assemblyline.odm import randomizer
// from assemblyline.remote.datatypes import get_client

// from assemblyline_service_server import app
// from assemblyline_service_server.api.v1 import task
// from assemblyline_service_server.config import AUTH_KEY

use std::sync::Arc;
use std::time::Duration;

use assemblyline_models::datastore::{EmptyResult, Error, Service};
use assemblyline_models::messages::changes::ServiceChange;
use assemblyline_models::messages::task::Task;
use assemblyline_models::types::{ClassificationString, ExpandingClassification, JsonMap, Sha256};
use log::{debug, info};
use poem::http::StatusCode;
use poem::listener::Acceptor;
use poem::EndpointExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc;
use reqwest::header::HeaderMap;

use crate::http::create_tls_binding;
use crate::service_api::helpers::APIResponse;
use crate::Core;

use super::{build_service, empty_delta, random_hash, setup, AUTH_KEY};

const TIMEOUT: Duration = Duration::from_secs(30);

// @pytest.fixture(scope='function')
// def redis():
//     config = forge.get_config()
//     client = get_client(
//         config.core.metrics.redis.host,
//         config.core.metrics.redis.port,
//         False
//     )
//     client.flushdb()
//     yield client
//     client.flushdb()


// service_name = 'Extract'
const TOOL_VERSION: &str = "89goecru";

fn container_id() -> String {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        random_hash(12)
    }).to_owned()
}

fn headers() -> HeaderMap {
    [
        ("Container-Id", container_id()),
        ("X-APIKey", AUTH_KEY.to_owned()),
        ("Service-Name", "TestSvice".to_owned()),
        ("Service-Version", "100".to_owned()),
        ("Service-Tool-Version", TOOL_VERSION.to_owned()),
        ("Timeout", "1".to_owned()),
        ("X-Forwarded-For", "127.0.0.1".to_owned()),
    ].into_iter()
    .map(|(name, value)|(reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}

type MockItem = (String, String);
async fn mock_dispatcher() -> (mpsc::Receiver<MockItem>, String) {
    use poem::{handler, Server, Body};
    use poem::web::Data;

    let (send, recv) = mpsc::channel(32);
    #[handler]
    async fn capture(
        Data(send): Data<&mpsc::Sender<MockItem>>,
        request: &poem::Request,
        body: Body,
    ) -> StatusCode {
        let path = request.uri().path();
        let body = body.into_string().await.unwrap();
        info!("Captured query: {path}");
        _ = send.send((path.to_owned(), body)).await;
        StatusCode::OK
    }
    let app = capture.data(send);

    let acceptor = create_tls_binding("0.0.0.0:0".parse().unwrap(), None).await.unwrap();
    // let listener = TcpListener::bind("0.0.0.0:0");
    // let acceptor = listener.into_acceptor().await.unwrap();
    let port = acceptor.local_addr()[0].as_socket_addr().unwrap().port();

    tokio::spawn(async move {
        Server::new_with_acceptor(acceptor).run(app).await
    });
    (recv, format!("localhost:{port}"))
}

// @pytest.fixture(scope='function')
// def storage():
//     ds = MagicMock()
//     with patch('assemblyline_service_server.config.TASKING_CLIENT.datastore', ds):
//         yield ds


// @pytest.fixture(scope='function')
// def heuristics():
//     ds = MagicMock()
//     with patch('assemblyline_service_server.config.TASKING_CLIENT.heuristics', ds):
//         yield ds


// @pytest.fixture(scope='function')
// def dispatch_client():
//     ds = MagicMock()
//     with patch('assemblyline_service_server.config.TASKING_CLIENT.dispatch_client', ds):
//         yield ds


// @pytest.fixture()
// def client(redis, storage, heuristics, dispatch_client):
//     client = app.app.test_client()
//     task.status_table = ExpiringHash(SERVICE_STATE_HASH, ttl=60 * 30, host=redis)
//     yield client

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum TaskResp {
    Task {
        task: Task
    },
    None {
        task: bool
    },
}

impl TaskResp {
    fn unwrap(self) -> Task {
        match self {
            TaskResp::Task { task } => task,
            _ => panic!(),
        }
    }
    fn is_none(&self) -> bool {
        matches!(self, TaskResp::None{ task: false })
    }
}

async fn setup_service(core: &Core) -> Service {
    let mut service = build_service();
    let name = service.name.clone();
    service.timeout = 100;
    service.disable_cache = false;
    let service_delta = empty_delta(&service);
    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();

    core.datastore.service.commit(None).await.unwrap();
    core.datastore.service_delta.commit(None).await.unwrap();
    core.redis_volatile.publish_json(&format!("changes.services.{name}"), &ServiceChange {
        name: name.clone(),
        operation: assemblyline_models::messages::changes::Operation::Added,
    }).await.unwrap();

    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > TIMEOUT { panic!(); }
        if core.services.get(&name).is_some() { break }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    service
}

fn build_task() -> Task {
    let mut task: Task = rand::rng().random();
    task.ignore_cache = false;
    task
}

#[tokio::test]
async fn test_task_timeout() {
    let (client, core, _guard, address) = setup(headers()).await;
    let _service = setup_service(&core).await;

    let response = client.get(format!("{address}/api/v1/task/")).send().await.unwrap();

    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    let body: APIResponse<TaskResp> = serde_json::from_slice(&body).unwrap();
    assert!(body.api_response.is_none());
}

#[tokio::test]
async fn test_task_ignored_then_timeout() {
    let (client, core, _guard, address) = setup(headers()).await;
    // prepare a service record and a fake dispatcher server
    let service = setup_service(&core).await;
    let (mut mock_result, mock_address) = mock_dispatcher().await;
    let mut task = build_task();
    task.dispatcher_address = mock_address;

    // put a task for that service in a queue
    let queue = core.get_service_queue(&service.name);
    queue.push(0.0, &task).await.unwrap();

    // add an empty result for that task to cache hit on
    let result_key = assemblyline_models::datastore::Result::help_build_key(
        &task.fileinfo.sha256,
        &service.name,
        &service.version,
        true,
        false,
        Some(TOOL_VERSION),
        Some(&task)
    ).unwrap();
    debug!("Saving empty as {result_key}");
    core.datastore.emptyresult.save(&result_key, &EmptyResult{expiry_ts: chrono::Utc::now()}, None, None).await.unwrap();

    // ask for a task
    let response = client.get(format!("{address}/api/v1/task/")).send().await.unwrap();

    // we expect a successful http response
    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    // our response body should be empty
    let body: APIResponse<TaskResp> = serde_json::from_slice(&body).unwrap();
    assert!(body.api_response.is_none());

    // there should not be any hanging queue content
    assert_eq!(queue.length().await.unwrap(), 0);

    // we should have seen the task starting and finishing
    assert_eq!(mock_result.try_recv().unwrap().0, "/start");
    assert_eq!(mock_result.try_recv().unwrap().0, "/result");
    assert!(mock_result.try_recv().is_err());
}

#[tokio::test]
async fn test_task_dispatch() {
    let (client, core, _guard, address) = setup(headers()).await;
    // prepare a service record and a fake dispatcher server
    let service = setup_service(&core).await;
    let (mut mock_result, mock_address) = mock_dispatcher().await;
    let mut task = build_task();
    task.dispatcher_address = mock_address;

    // put a task for that service in a queue
    let queue = core.get_service_queue(&service.name);
    queue.push(0.0, &task).await.unwrap();

    // ask for a task
    let response = client.get(format!("{address}/api/v1/task/")).send().await.unwrap();

    // we expect a successful http response
    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    // our response body should be that task
    let body: APIResponse<TaskResp> = serde_json::from_slice(&body).unwrap();
    let mut read_task = body.api_response.unwrap();
    let read_worker = read_task.metadata.remove("worker__").unwrap();
    assert_eq!(read_task, task);
    assert_eq!(*read_worker, container_id());

    // there should not be any hanging queue content
    assert_eq!(queue.length().await.unwrap(), 0);

    // we should have seen the task starting, but not finishing
    assert_eq!(mock_result.try_recv().unwrap().0, "/start");
    assert!(mock_result.try_recv().is_err());
}

#[tokio::test]
async fn test_finish_error() {
    let (client, core, _guard, address) = setup(headers()).await;
    // prepare a service record and a fake dispatcher server
    let service = setup_service(&core).await;
    let (mut mock_result, mock_address) = mock_dispatcher().await;
    let mut task = build_task();
    task.dispatcher_address = mock_address;

    // task = random_minimal_obj(Task)
    let mut error: Error = rand::rng().random();
    error.response.service_name = service.name;
    error.response.service_version = service.version;
    error.response.service_tool_version = Some(TOOL_VERSION.to_owned());
    let error_key = error.build_key(Some(TOOL_VERSION), Some(&task)).unwrap();

    let response = client.post(format!("{address}/api/v1/task/")).json(&json!({
        "task": task,
        "error": error
    })).send().await.unwrap();

    // we expect a successful http response
    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    let body: APIResponse<JsonMap> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response.get("success").unwrap(), true);

    // we should have seen the error get reported
    let (path, upload) = mock_result.try_recv().unwrap();
    assert_eq!(path, "/error");
    let posted_body: JsonMap = serde_json::from_str(&upload).unwrap();
    info!("{}", serde_json::to_string_pretty(&posted_body).unwrap());
    assert_eq!(posted_body.get("error_key").unwrap().as_str().unwrap(), error_key);

    assert!(mock_result.try_recv().is_err());
}

#[tokio::test]
async fn test_finish_minimal() {
    let (client, core, _guard, address) = setup(headers()).await;

    // prepare a service record and a fake dispatcher server
    let service = setup_service(&core).await;
    let (mut mock_result, mock_address) = mock_dispatcher().await;
    let mut task = build_task();
    task.dispatcher_address = mock_address;

    // create a result for that task
    let mut result: assemblyline_models::datastore::Result = rand::rng().random();
    result.response.service_name = service.name;
    result.response.service_version = service.version;
    result.response.service_tool_version = Some(TOOL_VERSION.to_owned());
    let result_key = result.build_key(Some(&task)).unwrap();

    let response = client.post(format!("{address}/api/v1/task/")).json(&json!({
        "task": task,
        "freshen": false,
        "result": result
    })).send().await.unwrap();

    // we expect a successful http response
    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    let body: APIResponse<JsonMap> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response.get("success").unwrap(), true);

    // we should have seen the result get reported
    let (path, upload) = mock_result.try_recv().unwrap();
    assert_eq!(path, "/result");
    let posted_body: JsonMap = serde_json::from_str(&upload).unwrap();
    info!("{}", serde_json::to_string_pretty(&posted_body).unwrap());
    assert_eq!(posted_body.get("result_summary").unwrap().as_object().unwrap().get("key").unwrap().as_str().unwrap(), result_key);

    assert!(mock_result.try_recv().is_err());
}

#[tokio::test]
async fn test_finish_heuristic() {
    let (client, core, _guard, address) = setup(headers()).await;

    // prepare a service record and a fake dispatcher server
    let service = setup_service(&core).await;
    let (mut mock_result, mock_address) = mock_dispatcher().await;
    let mut task = build_task();
    task.dispatcher_address = mock_address;

    // create a result for that task
    let mut result: assemblyline_models::datastore::Result = rand::rng().random();
    result.response.service_name = service.name.clone();
    result.response.service_version = service.version;
    result.response.service_tool_version = Some(TOOL_VERSION.to_owned());

    result.result.sections.push(assemblyline_models::datastore::result::Section {
        auto_collapse: Default::default(),
        body: Default::default(),
        classification: ClassificationString::unrestricted(&core.classification_parser),
        body_format: assemblyline_models::datastore::result::BodyFormat::Json,
        body_config: Default::default(),
        depth: Default::default(),
        heuristic: Some(assemblyline_models::datastore::result::Heuristic {
            heur_id: "HEUR1".to_owned(),
            name: "HEUR".to_owned(),
            attack: vec![],
            signature: vec![],
            score: 5500
        }),
        tags: Default::default(),
        safelisted_tags: Default::default(),
        title_text: Default::default(),
        promote_to: None,
    });

    let heur_id = format!("{}.HEUR1", service.name.to_uppercase());
    core.datastore.heuristic.save(&heur_id, &assemblyline_models::datastore::heuristic::Heuristic { 
        attack_id: Default::default(), 
        classification: ExpandingClassification::unrestricted(&core.classification_parser), 
        description: "".into(), 
        filetype: Default::default(), 
        heur_id: heur_id.clone(), 
        name: Default::default(), 
        score: 5000, 
        signature_score_map: Default::default(), 
        stats: Default::default(), 
        max_score: None 
    }, None, None).await.unwrap();
    core.datastore.heuristic.commit(None).await.unwrap();
    core.redis_volatile.publish_json("changes.heuristics", &json!({})).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    result.result.score = 99999;
    let result_key = result.build_key(Some(&task)).unwrap();

    let serde_json::Value::Object(mut encoded) = serde_json::to_value(&result).unwrap() else { panic!() };
    encoded
        .get_mut("result").unwrap()
        .as_object_mut().unwrap().get_mut("sections").unwrap()
        .as_array_mut().unwrap().get_mut(0).unwrap()
        .as_object_mut().unwrap().remove("tags");

    let response = client.post(format!("{address}/api/v1/task/")).json(&json!({
        "task": task,
        "freshen": false,
        "result": result
    })).send().await.unwrap();

    // we expect a successful http response
    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    let body: APIResponse<JsonMap> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response.get("success").unwrap(), true);

    // check that the result is in the database
    let db_result = core.datastore.result.get(&result_key, None).await.unwrap().unwrap();
    info!("{db_result:?}");

    // we should have seen the result get reported
    let (path, upload) = mock_result.try_recv().unwrap();
    assert_eq!(path, "/result");
    let posted_body: JsonMap = serde_json::from_str(&upload).unwrap();
    info!("{}", serde_json::to_string_pretty(&posted_body).unwrap());
    assert_eq!(posted_body.get("result_summary").unwrap().as_object().unwrap().get("key").unwrap().as_str().unwrap(), result_key);
    assert_eq!(posted_body.get("result_summary").unwrap().as_object().unwrap().get("score").unwrap(), &json!(5000));

    assert!(mock_result.try_recv().is_err());
}

#[tokio::test]
async fn test_finish_missing_file() {
    let (client, core, _guard, address) = setup(headers()).await;

    // prepare a service record and a fake dispatcher server
    let service = setup_service(&core).await;
    let (mut mock_result, mock_address) = mock_dispatcher().await;
    let mut task = build_task();
    task.dispatcher_address = mock_address;

    // create a result for that task
    let mut result: assemblyline_models::datastore::Result = rand::rng().random();
    result.response.service_name = service.name;
    result.response.service_version = service.version;
    result.response.service_tool_version = Some(TOOL_VERSION.to_owned());
    
    let missing_hash: Sha256 = rand::rng().random();
    result.response.extracted.push(assemblyline_models::datastore::result::File{ 
        name: Default::default(), 
        sha256: missing_hash.clone(), 
        description: Default::default(), 
        classification: ClassificationString::unrestricted(&core.classification_parser), 
        is_section_image: Default::default(), 
        parent_relation: Default::default(), 
        allow_dynamic_recursion: Default::default() 
    });
    
    let result_key = result.build_key(Some(&task)).unwrap();

    let response = client.post(format!("{address}/api/v1/task/")).json(&json!({
        "task": task,
        "freshen": true,
        "result": result
    })).send().await.unwrap();

    // we expect a successful http response
    let status = response.status();
    let body = response.bytes().await.unwrap();
    debug!("{}", String::from_utf8_lossy(&body));
    assert_eq!(status.as_u16(), 200);

    // but a failed API response, with the missing file reported
    let body: APIResponse<JsonMap> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response.get("success").unwrap(), false);
    assert_eq!(body.api_response.get("missing_files").unwrap().as_array().unwrap(), &vec![json!(missing_hash)]);

    // Nothing should reach the dispatcher
    assert!(mock_result.try_recv().is_err());

    // nothing should be saved in database
    assert!(!core.datastore.result.exists(&result_key, None).await.unwrap());
}


#[tokio::test]
async fn parse_sample_result() {
    let data = include_str!("data/sample_result_1.json");
    
    let config = assemblyline_markings::classification::sample_config();
    assemblyline_models::set_global_classification(Arc::new(assemblyline_markings::classification::ClassificationParser::new(config).unwrap()));

    #[derive(Serialize, Deserialize)]
    struct Success {
        task: assemblyline_models::types::JsonMap,
        #[serde(default)]
        exec_time: u64,
        freshen: bool,
        result: crate::service_api::v1::task::models::Result,
    }

    let _output: Success = serde_json::from_str(data).unwrap();
}