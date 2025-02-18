use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_models::datastore::user::User;
use assemblyline_models::datastore::{result, submission, Error, File, Service, Submission};
use assemblyline_models::messages::dispatching::SubmissionDispatchMessage;
use assemblyline_models::messages::task::{DataItem, ServiceResult, Task};
use assemblyline_models::{ClassificationString, Sha256};
use log::{debug, info};
use poem::listener::Acceptor;
use poem::EndpointExt;
use rand::Rng;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::dispatcher::client::DispatchClient;
use crate::dispatcher::{Dispatcher, SubmissionTask};
use crate::services::test::{dummy_service, setup_services};
use crate::{Core, TestGuard};


// import logging
// import time
// from unittest import mock

// import json
// import pytest

// from assemblyline.common.forge import get_service_queue, get_classification
// from assemblyline.odm.models.error import Error
// from assemblyline.odm.models.file import File
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.user import User
// from assemblyline.odm.randomizer import random_model_obj, random_minimal_obj, get_random_hash
// from assemblyline.odm import models
// from assemblyline.common.metrics import MetricsFactory

// from assemblyline_core.dispatching.client import DispatchClient, DISPATCH_RESULT_QUEUE
// from assemblyline_core.dispatching.dispatcher import Dispatcher, ServiceTask, Submission
// from assemblyline_core.dispatching.schedules import Scheduler as RealScheduler

// # noinspection PyUnresolvedReferences
// from assemblyline_core.dispatching.timeout import TimeoutTable
// from mocking import ToggleTrue
// from test_scheduler import dummy_service

fn test_services() -> HashMap<String, Service> {
    let mut bonus = dummy_service("bonus", "pre", None, None, Some("unknown"), Some(true));
    bonus.enabled = false;

    let mut sandbox = dummy_service("sandbox", "core", Some("Dynamic Analysis"), None, Some("unknown"), Some(true));
    sandbox.recursion_prevention.push("Dynamic Analysis".to_string());
    return [
        ("bonus", bonus),
        ("extract", dummy_service("extract", "pre", None, None, None, Some(true))),
        ("sandbox", sandbox),
        ("wrench", dummy_service("wrench", "pre", None, None, None, None)),
        ("av-a", dummy_service("av-a", "core", None, None, None, None)),
        ("av-b", dummy_service("av-b", "core", None, None, None, None)),
        ("frankenstrings", dummy_service("frankenstrings", "core", None, None, None, None)),
        ("xerox", dummy_service("xerox", "post", None, None, None, None)),
    ].into_iter().map(|(key, value)|(key.to_string(), value)).collect()
}

pub async fn setup() -> (Core, TestGuard) {
    setup_services(test_services()).await
}

fn make_result(file_hash: Sha256, service: String) -> result::Result {
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash;
    new_result.response.service_name = service;
    new_result
}

// recoverable=true
fn make_error(file_hash: Sha256, service: &str, recoverable: bool) -> Error {
    use assemblyline_models::datastore::error::Status;
    let mut new_error: Error = rand::rng().random();
    new_error.response.service_name = service.to_string();
    new_error.sha256 = file_hash;
    if recoverable {
        new_error.response.status = Status::FailRecoverable;
    } else {
        new_error.response.status = Status::FailNonrecoverable;
    }
    return new_error
}

// def wait_result(task, file_hash, service):
//     for _ in range(10):
//         if (file_hash, service) in task.service_results:
//             return True
//         time.sleep(0.05)


// def wait_error(task, file_hash, service):
//     for _ in range(10):
//         if (file_hash, service) in task.service_errors:
//             return True
//         time.sleep(0.05)


// @pytest.fixture(autouse=True)
// def log_config(caplog):
//     caplog.set_level(logging.INFO, logger='assemblyline')
//     from assemblyline.common import log as al_log
//     al_log.init_logging = lambda *args: None

async fn start_test_dispatcher(core: Core) -> anyhow::Result<Arc<Dispatcher>> {
    // Bind the HTTP interface
    let bind_address: SocketAddr = "0.0.0.0:0".parse()?;
    let tls_config = crate::config::TLSConfig::load().await?;
    let tcp = crate::http::create_tls_binding(bind_address, tls_config).await?;
    
    // Initialize Internal state
    let dispatcher = Dispatcher::new(core, tcp).await?;

    // launch components that we need running in test
    tokio::spawn({ let dispatcher = dispatcher.clone(); async move { dispatcher.work_guard().await }} );
    Ok(dispatcher)
}

#[tokio::test]
async fn test_simple() {
    let (core, _guard) = setup().await;
    debug!("Core setup");

    // create a test file to dispatch
    let mut file: File = rand::rng().random();
    let file_hash = file.sha256.clone();
    file.file_type = "unknown".to_string();
    core.datastore.file.save(&file_hash, &file, None, None).await.unwrap();
    debug!("File created");

    // create a user who will submit the file
    let user: User = Default::default();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    debug!("User created");

    // create the submission to dispatch
    let mut sub: Submission = rand::rng().random();
    let sid = sub.sid;
    sub.params.ignore_cache = false;
    sub.params.max_extracted = 5;
    sub.to_be_deleted = false;
    sub.params.classification = ClassificationString::unrestricted(&core.classification_parser);
    sub.params.initial_data = Some(serde_json::to_string(&serde_json::json!({"cats": "big"})).unwrap().into());
    sub.params.submitter = user.uname.clone();
    sub.files = vec![submission::File{ sha256: file_hash.clone(), name: "file".to_string(), size: None }];

    // start up dispatcher
    let disp = start_test_dispatcher(core.clone()).await.unwrap();
    let client = DispatchClient::new_from_core(&core).await.unwrap();
    // client.add_dispatcher(disp.instance_id.clone());
    debug!("Dispatcher ready");

    // Submit a problem, and check that it gets added to the dispatch hash
    // and the right service queues
    info!("==== first dispatch");
    let task = SubmissionDispatchMessage::simple(sub.clone(), Some("some-completion-queue".to_string()));

    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services)).await.unwrap();
    // client.dispatch_bundle(&task).await.unwrap();
    // disp.pull_submissions();
    let task = disp.get_test_report(sid).await.unwrap();

    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "extract".to_owned())));
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "wrench".to_owned())));
    assert_eq!(core.get_service_queue("extract").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("wrench").length().await.unwrap(), 1);

    // Making the same call again will queue it up again
    info!("==== second dispatch");
    disp.send_dispatch_action(crate::dispatcher::DispatchAction::DispatchFile(sid, file_hash.clone())).await;
    let task = disp.get_test_report(sid).await.unwrap();

    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "extract".to_owned())));
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "wrench".to_owned())));
    // note that the queue doesn't pile up
    assert_eq!(core.get_service_queue("extract").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("wrench").length().await.unwrap(), 1);

    info!("==== third dispatch");
    let job = client.request_work("0", "extract", "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.temporary_submission_data, &[
        DataItem{ name: "cats".to_string(), value: json!("big") },
        DataItem{ name: "ancestry".to_string(), value: json!([[{"type": "unknown", "parent_relation": "ROOT", "sha256": file.sha256}]]) }
    ]);
    let service_task = task.queue_keys.get(&(file_hash.clone(), "extract".to_string())).unwrap().0.clone();
    client.service_failed(service_task, "abc123", make_error(file_hash.clone(), "extract", true)).await.unwrap();
    
    let task = disp.get_test_report(sid).await.unwrap();
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "extract".to_owned())));
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "wrench".to_owned())));
    assert_eq!(core.get_service_queue("extract").length().await.unwrap(), 1);

    // Mark extract as finished, wrench as failed
    info!("==== fourth dispatch");
    let task_extract = client.request_work("0", "extract", "0", None, true, None).await.unwrap().unwrap();
    let task_wrench = client.request_work("0", "wrench", "0", None, true, None).await.unwrap().unwrap();
    client.service_finished(task_extract, "extract-result".to_string(), make_result(file_hash.clone(), "extract".to_owned()), None, None).await.unwrap();
    client.service_failed(task_wrench, "wrench-error", make_error(file_hash.clone(), "wrench", false)).await.unwrap();
    
    let task = disp.get_test_report(sid).await.unwrap();
    assert!(task.service_errors.contains_key(&(file_hash.clone(), "wrench".to_string())));
    assert!(task.service_results.contains_key(&(file_hash.clone(), "extract".to_string())));
    assert_eq!(core.get_service_queue("av-a").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("av-b").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("frankenstrings").length().await.unwrap(), 1);

    // Have the AVs fail, frankenstrings finishes
    info!("==== fifth dispatch");
    let task_av_a = client.request_work("0", "av-a", "0", None, true, None).await.unwrap().unwrap();
    let task_av_b = client.request_work("0", "av-b", "0", None, true, None).await.unwrap().unwrap();
    let task_frankenstrings = client.request_work("0", "frankenstrings", "0", None, true, None).await.unwrap().unwrap();
    client.service_failed(task_av_a, "av-a-error", make_error(file_hash.clone(), "av-a", false)).await.unwrap();
    client.service_failed(task_av_b, "av-b-error", make_error(file_hash.clone(), "av-b", false)).await.unwrap();
    client.service_finished(task_frankenstrings, "f-result".to_owned(), make_result(file_hash.clone(), "frankenstrings".to_owned()), None, None).await.unwrap();

    let task = disp.get_test_report(sid).await.unwrap();
    assert!(task.service_results.contains_key(&(file_hash.clone(), "frankenstrings".to_owned())));
    assert!(task.service_errors.contains_key(&(file_hash.clone(), "av-a".to_owned())));
    assert!(task.service_errors.contains_key(&(file_hash.clone(), "av-b".to_owned())));
    assert_eq!(core.get_service_queue("xerox").length().await.unwrap(), 1);

    // Finish the xerox service and check if the submission completion got checked
    info!("==== sixth dispatch");
    let task_xerox = client.request_work("0", "xerox", "0", None, true, None).await.unwrap().unwrap();
    client.service_finished(task_xerox, "xerox-result-key".to_owned(), make_result(file_hash, "xerox".to_owned()), None, None).await.unwrap();
    // disp.pull_service_results()
    // disp.service_worker(disp.process_queue_index(sid))
    // disp.save_submission()

    // assert!(wait_result(task, file_hash, "xerox"));
    assert!(disp.get_test_report(sid).await.is_err());
}

#[tokio::test]
async fn test_dispatch_extracted() {
    let (core, _guard) = setup().await;
    debug!("Core setup");

    // create a test file to dispatch
    let mut file_one: File = rand::rng().random();
    let mut file_two: File = rand::rng().random();
    for file in [&mut file_one, &mut file_two] {
        let file_hash = file.sha256.clone();
        file.file_type = "exe".to_string();
        core.datastore.file.save(&file_hash, file, None, None).await.unwrap();
    }
    let file_hash = file_one.sha256.clone();
    let second_file_hash = file_two.sha256.clone();
    debug!("File created");

    // create a user who will submit the file
    let user: User = Default::default();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    debug!("User created");

    // create the submission to dispatch
    let mut sub: Submission = rand::rng().random();
    // let sid = sub.sid;
    sub.params.ignore_cache = false;
    sub.params.max_extracted = 5;
    sub.params.ignore_recursion_prevention = false;
    sub.to_be_deleted = false;
    sub.params.services.selected = vec!["extract".to_string(), "sandbox".to_string()];
    sub.params.classification = ClassificationString::unrestricted(&core.classification_parser);
    // sub.params.initial_data = Some(serde_json::to_string(&serde_json::json!({"cats": "big"})).unwrap().into());
    sub.params.submitter = user.uname.clone();
    sub.files = vec![submission::File{ sha256: file_hash.clone(), name: "./file".to_string(), size: None }];

    // start up dispatcher
    let disp = start_test_dispatcher(core.clone()).await.unwrap();
    let client = DispatchClient::new_from_core(&core).await.unwrap();
    // client.add_dispatcher(disp.instance_id.clone());
    debug!("Dispatcher ready");

    // Launch the submission
    let task = SubmissionDispatchMessage::simple(sub.clone(), Some("some-completion-queue".to_string()));
    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services)).await.unwrap();

    // Finish one service extracting a file
    let job = client.request_work("0", "extract", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash.clone();
    new_result.response.service_name = "extract".to_string();
    new_result.response.extracted = vec![result::File{
        sha256: second_file_hash.clone(), 
        name: "second-*".to_owned(),
        description: "abc".into(), 
        classification: ClassificationString::unrestricted(&core.classification_parser),
        allow_dynamic_recursion: false,
        is_section_image: false,
        parent_relation: "EXTRACTED".into(),
    }];
    client.service_finished(job, "extracted-done".to_string(), new_result, None, None).await.unwrap();

    // see that the job has reached 
    let job = client.request_work("0", "sandbox", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash;
    new_result.response.service_name = "sandbox".to_string();
    client.service_finished(job, "sandbox-done".to_string(), new_result, None, None).await.unwrap();

    // 
    let job = client.request_work("0", "extract", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, second_file_hash);
    assert_eq!(job.filename, "second-*");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = second_file_hash;
    new_result.response.service_name = "extract".to_string();
    client.service_finished(job, "extracted-done-2".to_string(), new_result, None, None).await.unwrap();

    // see that the job doesn't reach sandbox
    assert!(client.request_work("0", "sandbox", "0", Some(Duration::from_secs(20)), true, None).await.unwrap().is_none());
}

/// Dynamic Recursion Prevention is to prevent services belonging to the 'Dynamic Analysis'
/// from analyzing the children of files they've analyzed.
///
/// The bypass should allow services to specify files to run through Dynamic Analysis regardless of the
/// Dynamic Recursion Prevention parameter.
#[tokio::test]
async fn test_dispatch_extracted_bypass_drp()  {
    let (core, _guard) = setup().await;
    debug!("Core setup");

    // create a test file to dispatch
    let mut file_one: File = rand::rng().random();
    let mut file_two: File = rand::rng().random();
    for file in [&mut file_one, &mut file_two] {
        let file_hash = file.sha256.clone();
        file.file_type = "exe".to_string();
        core.datastore.file.save(&file_hash, file, None, None).await.unwrap();
    }
    let file_hash = file_one.sha256.clone();
    let second_file_hash = file_two.sha256.clone();
    debug!("File created");

    // create a user who will submit the file
    let user: User = Default::default();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    debug!("User created");

    // create the submission to dispatch
    let mut sub: Submission = rand::rng().random();
    // let sid = sub.sid;
    sub.params.ignore_cache = false;
    sub.params.ignore_recursion_prevention = true;
    sub.params.services.selected = vec!["extract".to_string(), "sandbox".to_string()];
    sub.params.max_extracted = 5;
    sub.to_be_deleted = false;
    sub.params.classification = ClassificationString::unrestricted(&core.classification_parser);
    // sub.params.initial_data = Some(serde_json::to_string(&serde_json::json!({"cats": "big"})).unwrap().into());
    sub.params.submitter = user.uname.clone();
    sub.files = vec![submission::File{ sha256: file_hash.clone(), name: "./file".to_string(), size: None }];


    // start up dispatcher
    let disp = start_test_dispatcher(core.clone()).await.unwrap();
    let client = DispatchClient::new_from_core(&core).await.unwrap();
    // client.add_dispatcher(disp.instance_id.clone());
    debug!("Dispatcher ready");

    // Launch the submission
    let task = SubmissionDispatchMessage::simple(sub.clone(), Some("some-completion-queue".to_string()));
    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services)).await.unwrap();

    // Finish one service extracting a file
    let job = client.request_work("0", "extract", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash.clone();
    new_result.response.service_name = "extract".to_string();
    // This extracted file should be able to bypass Dynamic Recursion Prevention
    new_result.response.extracted = vec![result::File{
        sha256: second_file_hash.clone(), 
        name: "second-*".to_owned(),
        description: "abc".into(), 
        classification: ClassificationString::unrestricted(&core.classification_parser),
        allow_dynamic_recursion: true,
        is_section_image: false,
        parent_relation: "EXTRACTED".into(),
    }];
    client.service_finished(job, "extracted-done".to_string(), new_result, None, None).await.unwrap();  

    // Then 'sandbox' service will analyze the same file, give result
    let job = client.request_work("0", "sandbox", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash;
    new_result.response.service_name = "sandbox".to_string();
    client.service_finished(job, "sandbox-done".to_string(), new_result, None, None).await.unwrap();

    // "extract" service should have a task for the extracted file, give results to move onto next stage
    let job = client.request_work("0", "extract", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, second_file_hash);
    assert_eq!(job.filename, "second-*");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = second_file_hash.clone();
    new_result.response.service_name = "extract".to_string();
    client.service_finished(job, "extract-done".to_string(), new_result, None, None).await.unwrap();

    // "sandbox" should have a task for the extracted file
    // disp.dispatch_file(disp.tasks.get(sid), second_file_hash)
    let job = client.request_work("0", "sandbox", "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, second_file_hash);
    assert_eq!(job.filename, "second-*");
}


// mock_time = mock.Mock()
// mock_time.return_value = 0


// @mock.patch('time.time', mock_time)
#[tokio::test]
async fn test_timeout() {
    let (core, _guard) = setup().await;
    debug!("Core setup");

    // create a test file to dispatch
    let mut file: File = rand::rng().random();
    let file_hash = file.sha256.clone();
    file.file_type = "unknown".to_string();
    core.datastore.file.save(&file_hash, &file, None, None).await.unwrap();
    debug!("File created");

    // create a user who will submit the file
    let user: User = Default::default();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    debug!("User created");

    // create the submission to dispatch
    let mut sub: Submission = rand::rng().random();
    let sid = sub.sid;
    sub.params.ignore_cache = false;
    sub.params.max_extracted = 5;
    sub.to_be_deleted = false;
    sub.params.classification = ClassificationString::unrestricted(&core.classification_parser);
    sub.params.initial_data = Some(serde_json::to_string(&serde_json::json!({"cats": "big"})).unwrap().into());
    sub.params.submitter = user.uname.clone();
    sub.files = vec![submission::File{ sha256: file_hash.clone(), name: "file".to_string(), size: None }];

    // start up dispatcher
    let disp = start_test_dispatcher(core.clone()).await.unwrap();
    let client = DispatchClient::new_from_core(&core).await.unwrap();
    // client.add_dispatcher(disp.instance_id.clone());
    debug!("Dispatcher ready");

    // Submit a problem, and check that it gets added to the dispatch hash
    // and the right service queues
    let task = SubmissionDispatchMessage::simple(sub.clone(), Some("some-completion-queue".to_string()));
    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services)).await.unwrap();

    let job = client.request_work("0", "extract", "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "file");

    let job = client.request_work("0", "extract", "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "file");

    let job = client.request_work("0", "extract", "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "file");

    let key = (file_hash, "extract".to_string());
    for _ in 0..10 {
        let task = disp.get_test_report(sid).await.unwrap();
        if task.service_errors.contains_key(&key) {
            return
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!();    
}

#[poem::handler]
async fn handle_task_result(poem::web::Json(request): poem::web::Json<ServiceResult>, channel: poem::web::Data<&mpsc::Sender<ServiceResult>>) {
    channel.send(request).await.unwrap();
}

pub async fn fake_dispatcher(port: oneshot::Sender<u16>, channel: mpsc::Sender<ServiceResult>) {
    let app = poem::Route::new()
    // .at("/alive", poem::get(get_status))
    // .at("/start", poem::post(start_task))
    // .at("/error", poem::post(handle_task_error))
    .at("/result", poem::post(handle_task_result))
    .data(channel);

    let tcp = crate::http::create_tls_binding("0.0.0.0:0".parse().unwrap(), None).await.unwrap();
    port.send(tcp.local_addr()[0].as_socket_addr().unwrap().port()).unwrap();

    let result = poem::Server::new_with_acceptor(tcp)
        .run(app)
        .await;
    if let Err(err) = result {
        panic!("http interface failed: {err}");
    }
}



#[tokio::test]
async fn test_prevent_result_overwrite() {
    let (core, _guard) = setup().await;
    let client = DispatchClient::new_from_core(&core).await.unwrap();
    let dispatcher_name = "test";

    let mut pool = tokio::task::JoinSet::new();
    let (send_port, recv_port) = oneshot::channel();
    let (send_data, mut result_queue) = mpsc::channel(6);
    pool.spawn(fake_dispatcher(send_port, send_data));
    let port = recv_port.await.unwrap();
    // result_queue = client._get_queue_from_cache(DISPATCH_RESULT_QUEUE + dispatcher_name)

    // Create a task and add it to set of running tasks
    let mut task: Task = rand::rng().random();
    task.dispatcher = dispatcher_name.to_owned();
    task.dispatcher_address = format!("localhost:{port}");

    // Create a result that's not "empty"
    let mut result: result::Result = rand::rng().random();
    result.response.service_name = task.service_name.clone();
    result.sha256 = task.fileinfo.sha256.clone();
    result.result.score = 1;
    let result_key = result.build_key(None).unwrap();

    // Submit result to be saved
    // client.running_tasks.add(task.key(), task.as_primitives())
    client.service_finished(task.clone(), result_key.clone(), result.clone(), None, None).await.unwrap();

    // Pop result from queue, we expect to get the same result key as earlier
    let message = result_queue.recv().await.unwrap();
    assert_eq!(message.result_summary.key, result_key);

    // Save the same result again but we expect to be saved under another key
    // client.running_tasks.add(task.key(), task.as_primitives())
    client.service_finished(task, result_key.clone(), result, None, None).await.unwrap();
    let message = result_queue.recv().await.unwrap();
    assert_ne!(message.result_summary.key, result_key);

}
