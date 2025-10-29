use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use assemblyline_models::config::{Config, TemporaryKeyType};
use assemblyline_models::datastore::result::{ResponseBody, File as ResponseFile};
use assemblyline_models::datastore::submission::SubmissionState;
use assemblyline_models::datastore::user::User;
use assemblyline_models::datastore::{result, submission, Error, File, Service, Submission};

use assemblyline_models::messages::dispatching::{FileTreeData, SubmissionDispatchMessage};
use assemblyline_models::messages::submission::{File as MessageFile, SubmissionParams};
use assemblyline_models::messages::task::{DataItem, FileInfo, ServiceResult, Task};
use assemblyline_models::types::{ClassificationString, ServiceName, Sha256, ExpandingClassification,  Text, Wildcard};
use log::{debug, info};
use poem::listener::Acceptor;
use poem::EndpointExt;
use rand::Rng;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::dispatcher::client::DispatchClient;
use crate::dispatcher::{Dispatcher, SubmissionTask};
use crate::services::test::{dummy_service, setup_core_with_config, setup_services, setup_services_and_core};
use crate::{Core, TestGuard};



fn test_services() -> HashMap<ServiceName, Service> {
    let mut bonus = dummy_service("bonus", "pre", None, None, Some("unknown"), Some(true));
    bonus.enabled = false;

    let mut sandbox = dummy_service("sandbox", "core", Some("Dynamic Analysis"), None, Some("unknown"), Some(true));
    sandbox.recursion_prevention.push("Dynamic Analysis".into());
    return [
        ("bonus", bonus),
        ("extract", dummy_service("extract", "pre", None, None, None, Some(true))),
        ("sandbox", sandbox),
        ("wrench", dummy_service("wrench", "pre", None, None, None, None)),
        ("av-a", dummy_service("av-a", "core", None, None, None, None)),
        ("av-b", dummy_service("av-b", "core", None, None, None, None)),
        ("frankenstrings", dummy_service("frankenstrings", "core", None, None, None, None)),
        ("xerox", dummy_service("xerox", "post", None, None, None, None)),
    ].into_iter().map(|(key, value)|(key.into(), value)).collect()
}

pub async fn setup() -> (Core, TestGuard) {
    setup_services_and_core(test_services()).await
}

pub async fn setup_with_config(callback: impl Fn(&mut Config)) -> (Core, TestGuard) {
    let(core, redis) = setup_core_with_config(callback).await;
    return setup_services(core, redis, test_services()).await;
}

/// build a string that is the same character repeated a bunch of times
fn uniform_string(value: char, length: usize) -> String {
    let mut out = String::new();
    for _ in 0..length {
        out.push(value);
    }
    out
}

fn make_result(file_hash: Sha256, service: ServiceName) -> result::Result {
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash;
    new_result.response.service_name = service;
    new_result
}

// recoverable=true
fn make_error(file_hash: Sha256, service: ServiceName, recoverable: bool) -> Error {
    use assemblyline_models::datastore::error::Status;
    let mut new_error: Error = rand::rng().random();
    new_error.response.service_name = service;
    new_error.sha256 = file_hash;
    if recoverable {
        new_error.response.status = Status::FailRecoverable;
    } else {
        new_error.response.status = Status::FailNonrecoverable;
    }
    return new_error
}

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

//MARK: simple
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
    let user: User = User::create_test_user();
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
    let task = SubmissionDispatchMessage::new(sub.clone(), Some("some-completion-queue".to_string()));

    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services, &core.config)).await.unwrap();
    // client.dispatch_bundle(&task).await.unwrap();
    // disp.pull_submissions();
    let task = disp.get_test_report(sid).await.unwrap();

    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "extract".into())));
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "wrench".into())));
    assert_eq!(core.get_service_queue("extract").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("wrench").length().await.unwrap(), 1);

    // Making the same call again will queue it up again
    info!("==== second dispatch");
    disp.send_dispatch_action(crate::dispatcher::DispatchAction::DispatchFile(sid, file_hash.clone())).await;
    let task = disp.get_test_report(sid).await.unwrap();

    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "extract".into())));
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "wrench".into())));
    // note that the queue doesn't pile up
    assert_eq!(core.get_service_queue("extract").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("wrench").length().await.unwrap(), 1);

    info!("==== third dispatch");
    let job = client.request_work("0", "extract".into(), "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.temporary_submission_data, &[
        DataItem{ name: "cats".to_string(), value: json!("big") },
        DataItem{ name: "ancestry".to_string(), value: json!([[{"type": "unknown", "parent_relation": "ROOT", "sha256": file.sha256}]]) }
    ]);

    let service_task = task.queue_keys.get(&(file_hash.clone(), "extract".into())).unwrap().0.clone();
    client.service_failed(service_task, "abc123", make_error(file_hash.clone(), "extract".into(), true)).await.unwrap();

    let task = disp.get_test_report(sid).await.unwrap();
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "extract".into())));
    assert!(task.queue_keys.contains_key(&(file_hash.clone(), "wrench".into())));
    assert_eq!(core.get_service_queue("extract").length().await.unwrap(), 1);

    // Mark extract as finished, wrench as failed
    info!("==== fourth dispatch");
    let task_extract = client.request_work("0", "extract".into(), "0", None, true, None).await.unwrap().unwrap();
    let task_wrench = client.request_work("0", "wrench".into(), "0", None, true, None).await.unwrap().unwrap();
    client.service_finished(task_extract, "extract-result".to_string(), make_result(file_hash.clone(), "extract".into()), None, None, vec![]).await.unwrap();
    client.service_failed(task_wrench, "wrench-error", make_error(file_hash.clone(), "wrench".into(), false)).await.unwrap();

    let task = disp.get_test_report(sid).await.unwrap();
    assert!(task.service_errors.contains_key(&(file_hash.clone(), "wrench".into())));
    assert!(task.service_results.contains_key(&(file_hash.clone(), "extract".into())));
    assert_eq!(core.get_service_queue("av-a").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("av-b").length().await.unwrap(), 1);
    assert_eq!(core.get_service_queue("frankenstrings").length().await.unwrap(), 1);

    // Have the AVs fail, frankenstrings finishes
    info!("==== fifth dispatch");
    let task_av_a = client.request_work("0", "av-a".into(), "0", None, true, None).await.unwrap().unwrap();
    let task_av_b = client.request_work("0", "av-b".into(), "0", None, true, None).await.unwrap().unwrap();
    let task_frankenstrings = client.request_work("0", "frankenstrings".into(), "0", None, true, None).await.unwrap().unwrap();
    client.service_failed(task_av_a, "av-a-error", make_error(file_hash.clone(), "av-a".into(), false)).await.unwrap();
    client.service_failed(task_av_b, "av-b-error", make_error(file_hash.clone(), "av-b".into(), false)).await.unwrap();
    client.service_finished(task_frankenstrings, "f-result".to_owned(), make_result(file_hash.clone(), "frankenstrings".into()), None, None, vec![]).await.unwrap();

    let task = disp.get_test_report(sid).await.unwrap();
    assert!(task.service_results.contains_key(&(file_hash.clone(), "frankenstrings".into())));
    assert!(task.service_errors.contains_key(&(file_hash.clone(), "av-a".into())));
    assert!(task.service_errors.contains_key(&(file_hash.clone(), "av-b".into())));
    assert_eq!(core.get_service_queue("xerox").length().await.unwrap(), 1);

    // Finish the xerox service and check if the submission completion got checked
    info!("==== sixth dispatch");
    let task_xerox = client.request_work("0", "xerox".into(), "0", None, true, None).await.unwrap().unwrap();
    client.service_finished(task_xerox, "xerox-result-key".to_owned(), make_result(file_hash, "xerox".into()), None, None, vec![]).await.unwrap();
    // disp.pull_service_results()
    // disp.service_worker(disp.process_queue_index(sid))
    // disp.save_submission()

    // assert!(wait_result(task, file_hash, "xerox"));
    assert!(disp.get_test_report(sid).await.is_err());
}

// MARK: extracted
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
    let user: User = User::create_test_user();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    debug!("User created");

    // create the submission to dispatch
    let mut sub: Submission = rand::rng().random();
    // let sid = sub.sid;
    sub.params.ignore_cache = false;
    sub.params.max_extracted = 5;
    sub.params.ignore_recursion_prevention = false;
    sub.to_be_deleted = false;
    sub.params.services.selected = vec!["extract".into(), "sandbox".into()];
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
    let task = SubmissionDispatchMessage::new(sub.clone(), Some("some-completion-queue".to_string()));
    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services, &core.config)).await.unwrap();

    // Finish one service extracting a file
    let job = client.request_work("0", "extract".into(), "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash.clone();
    new_result.response.service_name = "extract".into();
    new_result.response.extracted = vec![result::File{
        sha256: second_file_hash.clone(),
        name: "second-*".to_owned(),
        description: "abc".into(),
        classification: ClassificationString::unrestricted(&core.classification_parser),
        allow_dynamic_recursion: false,
        is_section_image: false,
        parent_relation: "EXTRACTED".into(),
    }];
    client.service_finished(job, "extracted-done".to_string(), new_result, None, None, vec![]).await.unwrap();

    // see that the job has reached
    let job = client.request_work("0", "sandbox".into(), "0", None, true, None).await.unwrap().unwrap();

    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash;
    new_result.response.service_name = "sandbox".into();
    client.service_finished(job, "sandbox-done".to_string(), new_result, None, None, vec![]).await.unwrap();

    //
    let job = client.request_work("0", "extract".into(), "0", None, true, None).await.unwrap().unwrap();

    assert_eq!(job.fileinfo.sha256, second_file_hash);
    assert_eq!(job.filename, "second-*");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = second_file_hash;
    new_result.response.service_name = "extract".into();
    client.service_finished(job, "extracted-done-2".to_string(), new_result, None, None, vec![]).await.unwrap();

    // see that the job doesn't reach sandbox
    assert!(client.request_work("0", "sandbox".into(), "0", Some(Duration::from_secs(20)), true, None).await.unwrap().is_none());
}

// MARK: extracted bypass drp
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
    let user: User = User::create_test_user();
    core.datastore.user.save(&user.uname, &user, None, None).await.unwrap();
    debug!("User created");

    // create the submission to dispatch
    let mut sub: Submission = rand::rng().random();
    // let sid = sub.sid;
    sub.params.ignore_cache = false;
    sub.params.ignore_recursion_prevention = true;
    sub.params.services.selected = vec!["extract".into(), "sandbox".into()];
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
    let task = SubmissionDispatchMessage::new(sub.clone(), Some("some-completion-queue".to_string()));
    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services, &core.config)).await.unwrap();

    // Finish one service extracting a file
    let job = client.request_work("0", "extract".into(), "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash.clone();
    new_result.response.service_name = "extract".into();
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
    client.service_finished(job, "extracted-done".to_string(), new_result, None, None, vec![]).await.unwrap();

    // Then 'sandbox' service will analyze the same file, give result
    let job = client.request_work("0", "sandbox".into(), "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "./file");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = file_hash;
    new_result.response.service_name = "sandbox".into();
    client.service_finished(job, "sandbox-done".to_string(), new_result, None, None, vec![]).await.unwrap();

    // "extract" service should have a task for the extracted file, give results to move onto next stage
    let job = client.request_work("0", "extract".into(), "0", None, true, None).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, second_file_hash);
    assert_eq!(job.filename, "second-*");
    let mut new_result: result::Result = rand::rng().random();
    new_result.sha256 = second_file_hash.clone();
    new_result.response.service_name = "extract".into();
    client.service_finished(job, "extract-done".to_string(), new_result, None, None, vec![]).await.unwrap();

    // "sandbox" should have a task for the extracted file
    // disp.dispatch_file(disp.tasks.get(sid), second_file_hash)
    let job = client.request_work("0", "sandbox".into(), "0", None, true, None).await.unwrap().unwrap();
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
    let user: User = User::create_test_user();
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
    let task = SubmissionDispatchMessage::new(sub.clone(), Some("some-completion-queue".to_string()));
    disp.dispatch_submission(SubmissionTask::new(task, None, &core.services, &core.config)).await.unwrap();

    let job = client.request_work("0", "extract".into(), "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "file");

    let job = client.request_work("0", "extract".into(), "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "file");

    let job = client.request_work("0", "extract".into(), "0", None, true, Some(false)).await.unwrap().unwrap();
    assert_eq!(job.fileinfo.sha256, file_hash);
    assert_eq!(job.filename, "file");

    let key = (file_hash, "extract".into());
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
    result.response.service_name = task.service_name;
    result.sha256 = task.fileinfo.sha256.clone();
    result.result.score = 1;
    let result_key = result.build_key(None).unwrap();

    // Submit result to be saved
    // client.running_tasks.add(task.key(), task.as_primitives())
    client.service_finished(task.clone(), result_key.clone(), result.clone(), None, None, vec![]).await.unwrap();

    // Pop result from queue, we expect to get the same result key as earlier
    let message = result_queue.recv().await.unwrap();
    assert_eq!(message.result_summary.key, result_key);

    // Save the same result again but we expect to be saved under another key
    // client.running_tasks.add(task.key(), task.as_primitives())
    client.service_finished(task, result_key.clone(), result, None, None, vec![]).await.unwrap();
    let message = result_queue.recv().await.unwrap();
    assert_ne!(message.result_summary.key, result_key);

}

// This test is for creating submission task when the DispatcherSubmissionMessage object is populated with information in file_tree, file_infos, results, errors
#[tokio::test]
async fn test_create_submission_task() {

    let (core, _guard) = setup_with_config(|config| {
        config.services.stages = vec!["pre".to_string(), "core".to_string(), "post".to_string()];
        config.submission.default_temporary_keys = [("key1".to_owned(), TemporaryKeyType::Overwrite)].into_iter().collect();
        // setup temporary keys
        config.submission.max_temp_data_length = 32;
        config.submission.temporary_keys = [("key2".to_owned(), TemporaryKeyType::Union)].into_iter().collect()
    }).await;

    debug!("Core setup");

    let mut metadata: HashMap<String, Wildcard> = HashMap::new();
    metadata.insert(String::from("bundle.source"), Wildcard::from("test_bundle"));

    let test_service_name1 = "extract".to_string();

    let shas : Vec<Sha256> = vec![uniform_string('0', 64).parse().unwrap(), uniform_string('1', 64).parse().unwrap(), uniform_string('2', 64).parse().unwrap(), uniform_string('3', 64).parse().unwrap()];

    let errors :Vec<String> = shas.iter().map(|s| format!("{}.serviceName.v0_0_0.c0.e0", s)).collect();

    //  create files and results with file tree:
    //      root1
    //        |
    //      middle1
    //      |     |
    //    leaf1 leaf2

    let files =  vec![
            MessageFile {
            sha256: shas[0].clone(),
            size: Some(100),
            name: "root1".to_string()
            },
            MessageFile {
            sha256: shas[1].clone(),
            size: Some(100),
            name: "middle1".to_string()
            },
            MessageFile {
            sha256: shas[2].clone(),
            size: Some(100),
            name: "leaf1".to_string()
            },
            MessageFile {
            sha256: shas[3].clone(),
            size: Some(100),
            name: "leaf2".to_string()
            },
        ];


    let mut results : HashMap<String, result::Result> = HashMap::new();

    // root file result extracted one child
    let mut root_file_result = make_result(shas[0].clone(), ServiceName::from_string(test_service_name1.clone()));
    root_file_result.response = {
        let mut res = rand::random::<ResponseBody>();
        res.extracted = vec![ResponseFile::new(files[1].sha256.clone(), files[1].name.clone())];
        res
    };

    let mut middle_file_result = make_result(shas[1].clone(), ServiceName::from_string(test_service_name1.clone()));
    middle_file_result.response = {
        let mut res = rand::random::<ResponseBody>();
        res.extracted = vec![ResponseFile::new(files[2].sha256.clone(), files[2].name.clone()),
        ResponseFile::new(files[3].sha256.clone(), files[3].name.clone())];
        res
    };

    results.insert(format!("{}.{}.v0_0_0.c0", files[0].sha256, test_service_name1), root_file_result);
    results.insert(format!("{}.{}.v0_0_0.c0", files[1].sha256, test_service_name1), middle_file_result);

    let file_infos: HashMap<Sha256, FileInfo> = shas.iter().map(|k| (k.clone(), rand::random::<FileInfo>())).collect();

    let file_tree_middle =
        HashMap::from([
            (files[1].sha256.clone(),
                FileTreeData{
                    name: vec![files[1].name.clone()],
                    children:
                    HashMap::from([
                        (   files[2].sha256.clone(),
                            FileTreeData {
                            name: vec![files[2].name.clone()],
                            children: Default::default()
                        }),
                        (   files[3].sha256.clone(),
                            FileTreeData {
                            name: vec![files[3].name.clone()],
                            children: Default::default()
                        })
                    ])
                }
            )
    ]);

    let file_tree = HashMap::from(
        [
            (
                files[0].sha256.clone(),
                FileTreeData {
                    name: vec![files[0].name.clone()],
                    children: file_tree_middle
                }
            )
        ]
    );

    let initial_data: Text = json!({
            "key1": ["value1"],
            "key2": ["value2"]
        }).to_string().into();

    // set up submission object
    let submission = Submission {
        archive_ts: Default::default(),
        archived: false,
        classification: ExpandingClassification::try_unrestricted().unwrap(),
        tracing_events: Default::default(),
        error_count: 0,
        expiry_ts: Default::default(),
        file_count: 1,
        files: files.clone(),
        max_score: 500,
        metadata,
        params: SubmissionParams::new(ClassificationString::default_unrestricted()).set_initial_data(Some(initial_data)),
        results: Default::default(),
        sid: rand::rng().random(),
        state: SubmissionState::Submitted,
        to_be_deleted: false,
        times:  Default::default(),
        verdict:  Default::default(),
        from_archive: false,
        scan_key: None,
        errors: errors.clone()
    };

    let dispatch_message: SubmissionDispatchMessage = SubmissionDispatchMessage {
        submission: submission.clone(),
        completed_queue: None,
        file_infos: file_infos.clone(),
        results: results.clone(),
        file_tree: file_tree.clone(),
        errors: errors.clone() };


    let task = SubmissionTask::new(dispatch_message, None, &core.services, &core.config);

    assert_eq!(task.submission.sid, submission.sid);

    for f in files.clone() {

        // check if file_names are stored properly
        let file_name = task.file_names.get(&f.sha256).expect(format!("Cannot find file with sha {} in task.file_names", &f.sha256).as_str());
        assert_eq!(*file_name, f.name, "task.file_names error on sha 256 {}. Expect {} and got {}", f.sha256, f.name, file_name);

        // check if file_info are stored properly
        let file_info = task.file_info.get(&f.sha256).expect(format!("Cannot find file with sha {} in task._file_info", &f.sha256).as_str()).as_ref().unwrap().as_ref();

        assert_eq!(*file_info, *file_infos.get(&f.sha256).expect("Did not initialize test data file_infos correctly."));

        // check if temporary_data is present for all file
        let temporary_data = task.file_temporary_data.get(&f.sha256).expect(format!("Cannot find file with sha {} in task.file_temporary_data", &f.sha256).as_str());

        assert_eq!(*temporary_data.config.get(&"key1".to_string()).expect(format!("No key1 in file_temporary_data config for sha {}", &f.sha256).as_str()), TemporaryKeyType::Overwrite );
        assert_eq!(*temporary_data.config.get(&"key2".to_string()).expect(format!("No key2 in file_temporary_data config for sha {}", &f.sha256).as_str()), TemporaryKeyType::Union );

        let shared_temp_data = temporary_data.shared.as_ref().lock();
        assert_eq!(*shared_temp_data.get(&"key1".to_string()).expect(format!("No key1 in file_temporary_data shared for sha {}", &f.sha256).as_str()),json!(["value1"]));

        assert_eq!(*shared_temp_data.get(&"key2".to_string()).expect(format!("No key1 in file_temporary_data shared for sha {}", &f.sha256).as_str()),json!(["value2"]));

    }

    // check file depth
    assert_eq!(*task.file_depth.get(&files[0].sha256).expect(format!("Cannot find sha {} in task.file_depth.", &files[0].sha256).as_str()), 0);
    assert_eq!(*task.file_depth.get(&files[1].sha256).expect(format!("Cannot find sha {} in task.file_depth.", &files[1].sha256).as_str()), 1);
    assert_eq!(*task.file_depth.get(&files[2].sha256).expect(format!("Cannot find sha {} in task.file_depth.", &files[2].sha256).as_str()), 2);
    assert_eq!(*task.file_depth.get(&files[3].sha256).expect(format!("Cannot find sha {} in task.file_depth.", &files[3].sha256).as_str()), 2);

    // check parent map elements
    // the root file doesn't have a parent
    assert!(!task._parent_map.contains_key(&files[0].sha256));
    assert!(task._parent_map.get(&files[1].sha256).expect(format!("Cannot find sha {} in task._parent_map.", &files[0].sha256).as_str()).contains(&files[0].sha256));
    assert_eq!(task._parent_map.get(&files[1].sha256).expect(format!("Cannot find sha {} in task._parent_map.", &files[0].sha256).as_str()).len(), 1);
    assert!(task._parent_map.get(&files[2].sha256).expect(format!("Cannot find sha {} in task._parent_map.", &files[0].sha256).as_str()).contains(&files[1].sha256));
    assert_eq!(task._parent_map.get(&files[2].sha256).expect(format!("Cannot find sha {} in task._parent_map.", &files[0].sha256).as_str()).len(), 1);
    assert!(task._parent_map.get(&files[3].sha256).expect(format!("Cannot find sha {} in task._parent_map.", &files[0].sha256).as_str()).contains(&files[1].sha256));
    assert_eq!(task._parent_map.get(&files[3].sha256).expect(format!("Cannot find sha {} in task._parent_map.", &files[0].sha256).as_str()).len(), 1);

    // check service results
    let task_results = task.service_results.clone();
    for k in results.keys() {
        if let [sha, service, _] = k.splitn(3, ".").collect::<Vec<_>>()[..] {
            let sha256: Sha256 = match sha.parse() {
                            Ok(sha) => sha,
                            Err(_) => continue,
            };

            let res_summary = task_results.get(&(sha256.clone(), ServiceName::from(service))).expect(format!("Cannot find sha {sha256} and service {service} in task.service_results.").as_str());

            assert_eq!(res_summary.key, *k);

        }
    }

    let task_errors = task.service_errors.clone();

    for e in errors {
        if let [sha, service, _] = e.splitn(3, ".").collect::<Vec<_>>()[..] {
            let sha256: Sha256 = match sha.parse() {
                            Ok(sha) => sha,
                            Err(_) => continue,
            };
            let error = task_errors.get(&(sha256.clone(), ServiceName::from(service))).expect(format!("Cannot find sha {sha256} and service {service} in task.service_errors.").as_str());
            assert_eq!(*error, *error);
        };
    }


}
