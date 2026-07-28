use assemblyline_models::{
    datastore::{self, Service},
    messages::{self, service_api::service_manifest::ServiceManifest, task::Task},
    types::{JsonMap, Sha256},
};
use log::{debug, info};
use std::{collections::HashMap, io::Write, path::Path, str::FromStr, sync::Arc};

use parking_lot::Mutex;

use poem::{
    get, handler,
    middleware::AddData,
    put,
    web::{Data, Json},
    Endpoint, EndpointExt, IntoResponse, Route,
};
use rand::RngExt;
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use url::Url;

use crate::{
    connection::{self},
    constants::{RECOVERABLE_ERROR_STATUS, UNKNOWN_SERVICE_ERROR_TYPE},
    service_client::ServiceClient,
    task_fetcher::single_thread_task_fetcher::SingleThreadTaskFetcher,
    task_uploader::task_uploader::TaskUploader,
    tests::{
        create_random_service_result, init,
        mock_service::{DoNothingServiceLauncher, MockService, MockServiceLauncher},
        mock_service_api::{
            MockServerConfig, MockServiceServer, RequestDataResponse, TEST_API_VERSION,
            TEST_AUTH_KEY, TEST_SERVER_VERSION,
        },
        test_sha_file,
    },
    types::{
        response::{APIResponse, RegisterResponse, TaskUploadResponse},
        task::{ErrorBody, ErrorResponse},
    },
};

pub const TESTING_PREFIX: &str = "test";

#[derive(Clone, Debug)]
pub struct RegisterServiceData {
    pub updated_service: Option<Service>,
}

#[derive(Clone, Debug)]
pub struct RunServiceData {
    pub service: Service,
    pub task: Option<Task>,
    pub num_get_task_called: Arc<Mutex<usize>>,
    pub num_upload_called: Arc<Mutex<usize>>,
    pub upload_value: Value,
    pub task_done_success: bool,
}

fn register_api(service_data: RegisterServiceData) -> impl Endpoint {
    Route::new()
        .at(
            format!("/service/register"),
            put(register_service).post(register_service),
        )
        .with(AddData::new(service_data))
}

fn run_service_api(data: RunServiceData) -> impl Endpoint {
    Route::new()
        .at(
            format!("/service/register"),
            put(simple_register).post(simple_register),
        )
        .at(format!("/task"), get(get_task).post(upload_task_result))
        .at(format!("/file/:sha256"), get(download_file))
        .at(format!("/file"), put(upload_file))
        .with(AddData::new(data))
}

#[handler]
pub async fn get_task(data: Data<&RunServiceData>) -> Result<poem::Response, poem::error::Error> {
    *data.num_get_task_called.lock() += 1;

    if let Some(task) = &data.task {
        let data = json!({"task": task});
        Ok(Json(APIResponse {
            api_response: Some(data),
            api_error_message: None,
            api_server_version: TEST_SERVER_VERSION.to_string(),
            api_status_code: StatusCode::OK.as_u16(),
        })
        .into_response())
    } else {
        Ok(Json(APIResponse {
            api_response: json!({"task": false}),
            api_error_message: None,
            api_server_version: TEST_SERVER_VERSION.to_string(),
            api_status_code: StatusCode::OK.as_u16(),
        })
        .into_response())
    }
}

#[handler]
async fn upload_task_result(
    Json(body): Json<JsonMap>,
    data: Data<&RunServiceData>,
) -> poem::Result<poem::Response, poem::error::Error> {
    *data.num_upload_called.lock() += 1;

    if !data.upload_value.is_null() {
        if data.task_done_success {
            let result_data = body.get("result").expect("Result should be uploaded.");
            assert_eq!(data.upload_value, *result_data);
        } else {
            let error_data = body.get("error").expect("Error should be uploaded.");
            assert_eq!(data.upload_value, *error_data);
        }
    }
    if data.task_done_success {
        return Ok(MockServiceServer::make_api_response(TaskUploadResponse {
            success: true,
            missing_files: None,
        }));
    } else {
        return Ok(Json(json!({
            "data": "OK",
        }))
        .into_response());
    }
}

#[handler]
async fn simple_register(
    Json(_body): Json<JsonMap>,
    data: Data<&RunServiceData>,
) -> poem::Result<poem::Response> {
    let new_heuristics: Vec<String> = Vec::new();
    let register_response = RegisterResponse {
        keep_alive: true,
        new_heuristics,
        service_config: data.service.to_owned(),
    };

    let api_response = APIResponse {
        api_response: register_response,
        api_error_message: None,
        api_server_version: TEST_API_VERSION.to_string(),
        api_status_code: 200,
    };

    return Ok(Json(api_response).into_response());
}

#[handler]
async fn register_service(
    Json(body): Json<JsonMap>,
    service_data: Data<&RegisterServiceData>,
) -> poem::Result<poem::Response> {
    // Data sent from service client should serialize to service
    let mut service: Service =
        serde_json::from_value::<Service>(serde_json::Value::Object(body)).unwrap();

    // register service either return the service that we got from the request body or
    // return an updated version of the service
    if let Some(data) = service_data.updated_service.to_owned() {
        service = data.clone();
    }

    let new_heuristics: Vec<String> = Vec::new();
    let register_response = RegisterResponse {
        keep_alive: true,
        new_heuristics,
        service_config: service,
    };

    let api_response = APIResponse {
        api_response: register_response,
        api_error_message: None,
        api_server_version: TEST_API_VERSION.to_string(),
        api_status_code: 200,
    };

    return Ok(Json(api_response).into_response());
}

#[handler]
async fn upload_file(_body: poem::Body) -> Result<poem::Response, poem::error::Error> {
    return Ok(Json(json!({
        "data": "OK",
    }))
    .into_response());
}

#[handler]
async fn download_file(
    poem::web::Path(_sha256): poem::web::Path<String>,
) -> Result<poem::Response, poem::error::Error> {
    // let hash: Sha256 = sha256.parse().unwrap();
    let (file_hash, file_data) = test_sha_file();
    let file_size = file_data.len();
    // let body = Body::from_vec(data.clone());
    let filename = format!("UTF-8''{}", urlencoding::encode(&file_hash));

    return Ok(poem::Response::builder()
        .content_type("application/octet-stream")
        .header("Content-Length", file_size.to_string())
        .header(
            "Content-Disposition",
            format!("attachment; filename=file.bin; filename*={filename}"),
        )
        .body(file_data));
}

async fn make_run_service_data(
    base_folder: String,
    file_required: bool,
) -> (ServiceManifest, Service, Task) {
    let mut base_manifest: ServiceManifest = rand::rng().random();
    base_manifest.file_required = file_required;
    let base_service = base_manifest.service.clone();
    let manifest_path = format!("{}service_manifest.yml", &base_folder);

    let mut manifest_file = tokio::fs::File::create(&manifest_path).await.unwrap();
    let data = serde_yaml::to_string(&base_manifest).unwrap();
    manifest_file.write_all(data.as_bytes()).await.unwrap();
    let mut task: Task = rand::rng().random();

    let (file_hash, _) = test_sha_file();
    task.fileinfo.sha256 =
        Sha256::from_str(&file_hash).expect("File hash should be hex representation of sha256.");

    task.service_name = base_manifest.service.name;

    (base_manifest, base_service, task)
}

async fn make_test_service_client(
    running: Arc<Mutex<bool>>,
    service_api_host: String,
    base_folder: String,
) -> ServiceClient {
    let sc = ServiceClient::new(
        false,
        true,
        running,
        "test_container_id".to_string(),
        TESTING_PREFIX.to_string(),
        base_folder.clone(),
        base_folder.clone(),
        base_folder.clone(),
        service_api_host,
        TEST_AUTH_KEY.to_string(),
        "".to_string(),
    )
    .await
    .expect("Failed to make create service client.");

    sc
}

#[tokio::test]
async fn test_service_client_connection() {
    init();

    let (port, _) = MockServiceServer::launch_with_test_endpoints(MockServerConfig::default())
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    let test_manifest: ServiceManifest = rand::rng().random();

    let temp_dir = tempfile::tempdir().unwrap();
    let mut base_dir_string = temp_dir.path().to_string_lossy().to_string();
    base_dir_string.push('/');
    // let base_manifest: ServiceManifest = rand::rng().random();
    let manifest_path = format!("{}service_manifest.yml", &base_dir_string);

    // let manifest_path = Path::new("service_manifest.yml");
    let mut manifest_file = std::fs::File::create(&manifest_path).unwrap();
    let data = serde_yaml::to_string(&test_manifest).unwrap();
    manifest_file.write_all(data.as_bytes()).unwrap();

    let headers: HashMap<String, String> = HashMap::from([
        ("x-apikey".to_string(), TEST_AUTH_KEY.to_string()),
        ("container-id".to_string(), "test_container_id".to_string()),
        (
            "service-name".to_string(),
            test_manifest.service.name.to_string(),
        ),
        (
            "service-tool-version".to_string(),
            test_manifest.tool_version.unwrap_or("".to_string()),
        ),
        ("service-version".to_string(), test_manifest.service.version),
    ]);

    let sc_running = Arc::new(Mutex::new(true));

    let sc = ServiceClient::new(
        false,
        true,
        sc_running.clone(),
        "test_container_id".to_string(),
        "test".to_string(),
        base_dir_string.clone(),
        base_dir_string.clone(),
        base_dir_string.clone(),
        service_api_address.clone(),
        TEST_AUTH_KEY.to_string(),
        "".to_string(),
    )
    .await
    .unwrap();

    // make sure service connection has the correct header setup
    let get_request_data =
        Url::parse(format!("{service_api_address}/test/get_request_data/").as_str()).unwrap();
    let response = sc
        .connection
        .request(
            reqwest::Method::GET,
            get_request_data.clone(),
            connection::Body::None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let response_data: RequestDataResponse = response.json().await.unwrap();

    let mut missing_headers: Vec<String> = Vec::new();
    let mut incorrect_headers: Vec<String> = Vec::new();

    for (k, v) in headers {
        match response_data.header.get(&k) {
            Some(header_val) => {
                if v != *header_val {
                    incorrect_headers.push(k)
                }
            }
            None => missing_headers.push(k),
        }
    }

    assert!(
        missing_headers.is_empty(),
        "Missing request headers: {}",
        missing_headers.join(",")
    );
    assert!(
        incorrect_headers.is_empty(),
        "Incorrect request headers values for: {}",
        incorrect_headers.join(",")
    );
}

#[tokio::test]
async fn test_register_service() {
    init();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut base_dir_string = temp_dir.path().to_string_lossy().to_string();
    base_dir_string.push('/');
    let base_manifest: ServiceManifest = rand::rng().random();

    let manifest_path = format!("{}service_manifest.yml", &base_dir_string);

    let mut manifest_file = tokio::fs::File::create(&manifest_path).await.unwrap();
    let data = serde_yaml::to_string(&base_manifest).unwrap();
    manifest_file.write_all(data.as_bytes()).await.unwrap();

    let mut updated_service = base_manifest.service.clone();
    let update_config = json!({
        "key_a": "a",
        "key_b": ["b"],
        "key_c": {"key_d": "d"}
    });

    updated_service.config = serde_json::from_value(update_config).unwrap();

    let (port, _) =
        MockServiceServer::launch_with_custom_endpoints(register_api(RegisterServiceData {
            updated_service: Some(updated_service.clone()),
        }))
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    let sc_running = Arc::new(Mutex::new(true));

    let mut sc =
        make_test_service_client(sc_running.clone(), service_api_address, base_dir_string).await;

    let _ = sc
        .register_service()
        .await
        .expect("Register service should pass.");

    // the updated manifest should be written to file
    let file = tokio::fs::File::open(&sc.get_runtime_manifest_path()).await.unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).await.unwrap();

    let updated_manifest: ServiceManifest = serde_yaml::from_str(&contents).unwrap();

    assert_eq!(
        &updated_manifest.service, &updated_service,
        "The service should be updated after register service."
    );
}

#[tokio::test]
async fn test_run_service_write_task_pipe() {
    init();
    let tasking_dir = tempfile::tempdir().unwrap();
    let mut tasking_dir_string = tasking_dir.path().to_string_lossy().to_string();
    tasking_dir_string.push('/');

    let (_, base_service, task) = make_run_service_data(tasking_dir_string.clone(), true).await;

    let (port, _) =
        MockServiceServer::launch_with_custom_endpoints(run_service_api(RunServiceData {
            service: base_service.clone(),
            task: Some(task.clone()),
            num_get_task_called: Arc::new(Mutex::new(0)),
            num_upload_called: Arc::new(Mutex::new(0)),
            upload_value: Value::Null,
            task_done_success: false,
        }))
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    //  build a test core
    let sc_running = Arc::new(Mutex::new(true));

    let mut sc = make_test_service_client(
        sc_running.clone(),
        service_api_address,
        tasking_dir_string.clone(),
    )
    .await;

    // different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = DoNothingServiceLauncher {};

    let handler = tokio::spawn(async move {
        return sc
            .run_service(&mut task_fetcher, &task_uploader, &service_launcher)
            .await;
    });

    let task_fifo_path = format!(
        "{}{}_task.fifo",
        tasking_dir_string,
        TESTING_PREFIX.to_owned()
    );
    let done_fifo_path = format!(
        "{}{}_done.fifo",
        tasking_dir_string,
        TESTING_PREFIX.to_owned()
    );
    let service_ready_path = format!("{}{}_ready", tasking_dir_string, TESTING_PREFIX.to_owned());

    // start the task fifo setup process
    let fifo_pipes = MockService::setup_fifo(&task_fifo_path, &done_fifo_path)
        .await
        .expect("Setup FIFO pipe should pass.");
    // create service ready file so that sevice handler can continue
    let _ = tokio::fs::File::create(&service_ready_path).await;

    fifo_pipes
        .task_fifo
        .readable()
        .await
        .expect("Task fifo pipe should be readable for task.");

    loop {
        let mut msg = vec![0; 1024];
        match fifo_pipes.task_fifo.try_read(&mut msg) {
            Ok(n) => {
                if n < 1 {
                    info!("Length of task is 0. Try reading the task fifo pipe again.");
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(2.0)).await;
                    continue;
                }
                msg.truncate(n);
                let value = serde_json::from_slice::<Vec<String>>(msg.as_slice())
                    .expect("The message from task_fifo should be a vector of string");

                let task_dir_path = value
                    .get(0)
                    .expect("The task fifo_message should have task_dir at index 0")
                    .to_owned();
                let task_file_path = value
                    .get(1)
                    .expect("The task fifo_message should have task_file path at index 1")
                    .trim()
                    .to_string();

                debug!("Task dir: {task_dir_path}");
                debug!("task file: {task_file_path}");

                // the task_dir and task_file path should be in /{tasking_dir}/{temporary_dir}/
                let task_dir = Path::new(&task_dir_path);

                assert!(task_dir.exists(), "Task directory should exist.");
                assert!(task_dir.is_dir(), "Task directory should be a directory");
                assert_eq!(
                    task_dir.parent().expect("Task_dir should have a parent."),
                    Path::new(&tasking_dir_string),
                    "The parent directory of the task dir should be tasking_dir"
                );

                let task_file = Path::new(&task_file_path);
                assert!(task_file.exists(), "The task file should exist.");
                assert_eq!(
                    task_file.parent().expect("Task file should have a parent."),
                    task_dir,
                    "The parent directory of the task file should be tasking_dir"
                );

                let mut task_download_file_path = Path::new(&task_dir_path).to_path_buf();
                task_download_file_path.push(format!("{}", task.fileinfo.sha256));

                assert!(
                    task_download_file_path.exists(),
                    "Task file should be downloaded."
                );

                assert_eq!(
                    task_download_file_path.parent().expect("Downloaded file should have a parent."),
                    task_dir,
                    "The parent directory of the downloaded file should be tasking_dir"
                );


                let file =
                    std::fs::File::open(&task_file_path).expect("Task file should be openable.");
                let output_task: Task = serde_json::from_reader(file)
                    .expect("The data in the task file should deserialize to a task object.");

                assert_eq!(
                    output_task, task,
                    "The task given from service API should be written to task_file."
                );

                break;
            }
            Err(e) if e.kind() == tokio::io::ErrorKind::WouldBlock => {
                debug!("Error::WouldBlock writing to task fifo pipe. Sleep for 1 second and try again.");
                // wait a second before trying to read the pipe again.
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
            }
            Err(e) => {
                panic!("Reading task pipe caused error: {e}");
            }
        }
    }

    // terminate service handler process. Wait for it to finish clean up.
    *sc_running.lock() = false;
    drop(fifo_pipes);

    let _ = handler.await;

    // Task fifo, done fifo, service ready should all be removed from tasking_dir, each individual folder from tasking_dir should be removed too.
    let task_dir_path = tasking_dir.path().to_path_buf();
    assert!(
        task_dir_path.exists(),
        "The base tasking_dir should still exists."
    );
    let task_dir_iterator = task_dir_path
        .read_dir()
        .expect("Should be able to read from tasking dir.");
    // everything should be deleted except for the test service manifest file
    assert_eq!(task_dir_iterator.count(), 1)
}

#[tokio::test]
async fn test_run_service_task_done_with_result() {
    init();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut base_dir_string = temp_dir.path().to_owned().to_str().unwrap().to_string();
    base_dir_string.push('/');

    let (_, base_service, task) = make_run_service_data(base_dir_string.clone(), false).await;

    let num_get_task_called = Arc::new(Mutex::new(0));
    let num_upload_called = Arc::new(Mutex::new(0));
    let task_done_success = true;
    let result_value: messages::service_api::result::Result = create_random_service_result(
        task.fileinfo.sha256.clone(),
        base_service.name.clone(),
        base_service.version.clone(),
        None,
    );

    let (port, _) =
        MockServiceServer::launch_with_custom_endpoints(run_service_api(RunServiceData {
            service: base_service.clone(),
            task: Some(task.clone()),
            num_get_task_called: num_get_task_called.clone(),
            num_upload_called: num_upload_called.clone(),
            upload_value: serde_json::to_value(&result_value).expect("Result should serialize."),
            task_done_success: task_done_success,
        }))
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    //  build a test core
    let sc_running = Arc::new(Mutex::new(true));
    let service_running = Arc::new(Mutex::new(true));

    let mut sc = make_test_service_client(
        sc_running.clone(),
        service_api_address,
        base_dir_string.clone(),
    )
    .await;

    // different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = MockServiceLauncher {
        service: base_service.clone(),
        runtime_prefix: TESTING_PREFIX.to_string(),
        tasking_dir: base_dir_string.clone(),
        running: service_running.clone(),
        task_done_success: task_done_success,
        wait_forever: false,
        service_result: Some(
            serde_json::to_value(&result_value).expect("Result should serialize."),
        ),
    };

    let handler = tokio::spawn(async move {
        return sc
            .run_service(&mut task_fetcher, &task_uploader, &service_launcher)
            .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_secs_f64(10.0)).await;
    *sc_running.lock() = false;

    assert!(
        *num_get_task_called.lock() > 0,
        "Service client should call get task at least once."
    );
    assert!(
        *num_upload_called.lock() > 0,
        "Service client should call upload task at least once."
    );

    let res = handler.await;
    assert!(res.is_ok(), "Service handler should return with no error.");
}

#[tokio::test]
async fn test_run_service_task_done_with_error() {
    init();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut base_dir_string = temp_dir.path().to_owned().to_str().unwrap().to_string();
    base_dir_string.push('/');

    let (_, base_service, task) = make_run_service_data(base_dir_string.clone(), false).await;

    let num_get_task_called = Arc::new(Mutex::new(0));
    let num_upload_called = Arc::new(Mutex::new(0));
    let task_done_success = false;
    let result_value: datastore::error::Error = rand::random();

    let (port, _) =
        MockServiceServer::launch_with_custom_endpoints(run_service_api(RunServiceData {
            service: base_service.clone(),
            task: Some(task.clone()),
            num_get_task_called: num_get_task_called.clone(),
            num_upload_called: num_upload_called.clone(),
            upload_value: serde_json::to_value(&result_value).expect("Error should serialize."),
            task_done_success: task_done_success,
        }))
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    //  build a test core
    let sc_running = Arc::new(Mutex::new(true));
    let service_running = Arc::new(Mutex::new(true));

    let mut sc = make_test_service_client(
        sc_running.clone(),
        service_api_address,
        base_dir_string.clone(),
    )
    .await;

    // different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = MockServiceLauncher {
        service: base_service.clone(),
        runtime_prefix: TESTING_PREFIX.to_string(),
        tasking_dir: base_dir_string.clone(),
        running: service_running.clone(),
        task_done_success: task_done_success,
        wait_forever: false,
        service_result: Some(serde_json::to_value(&result_value).expect("Error should serialize.")),
    };

    let handler = tokio::spawn(async move {
        return sc
            .run_service(&mut task_fetcher, &task_uploader, &service_launcher)
            .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_secs_f64(10.0)).await;

    *sc_running.lock() = false;

    assert!(
        *num_get_task_called.lock() > 0,
        "Service client should call get task at least once."
    );
    assert!(
        *num_upload_called.lock() > 0,
        "Service client should call upload task at least once."
    );

    let res = handler.await;
    assert!(res.is_ok(), "Service handler should return with no error.");
}

#[tokio::test]
async fn test_run_service_task_get_no_task() {
    init();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut base_dir_string = temp_dir.path().to_owned().to_str().unwrap().to_string();
    base_dir_string.push('/');

    let (_, base_service, _) = make_run_service_data(base_dir_string.clone(), false).await;

    let num_get_task_called = Arc::new(Mutex::new(0));
    let num_upload_called = Arc::new(Mutex::new(0));

    let (port, _) =
        MockServiceServer::launch_with_custom_endpoints(run_service_api(RunServiceData {
            service: base_service.clone(),
            task: None,
            num_get_task_called: num_get_task_called.clone(),
            num_upload_called: num_upload_called.clone(),
            upload_value: Value::Null,
            task_done_success: false,
        }))
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    //  build a test core
    let sc_running = Arc::new(Mutex::new(true));
    let service_running = Arc::new(Mutex::new(true));

    let mut sc = make_test_service_client(
        sc_running.clone(),
        service_api_address,
        base_dir_string.clone(),
    )
    .await;

    // different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = MockServiceLauncher {
        service: base_service.clone(),
        runtime_prefix: TESTING_PREFIX.to_string(),
        tasking_dir: base_dir_string.clone(),
        running: service_running.clone(),
        task_done_success: true,
        wait_forever: false,
        service_result: None,
    };

    let handler = tokio::spawn(async move {
        return sc
            .run_service(&mut task_fetcher, &task_uploader, &service_launcher)
            .await;
    });

    tokio::time::sleep(tokio::time::Duration::from_secs_f64(11.0)).await;
    *sc_running.lock() = false;

    assert!(
        *num_get_task_called.lock() > 1,
        "Service client should continuously request task when no task received."
    );
    assert!(
        *num_upload_called.lock() == 0,
        "There should be nothing uploaded when there is no task."
    );

    let res = handler.await;
    assert!(res.is_ok(), "Service handler should return with no error.");
}

#[tokio::test]
async fn test_run_service_task_service_terminated() {
    init();
    let temp_dir = tempfile::tempdir().unwrap();
    let mut base_dir_string = temp_dir.path().to_owned().to_str().unwrap().to_string();
    base_dir_string.push('/');

    let (_, base_service, task) = make_run_service_data(base_dir_string.clone(), false).await;

    let num_get_task_called = Arc::new(Mutex::new(0));
    let num_upload_called = Arc::new(Mutex::new(0));

    let error_body = ErrorBody {
        sha256: task.fileinfo.sha256.clone(),
        error_type: UNKNOWN_SERVICE_ERROR_TYPE.to_string(),
        response: ErrorResponse {
            message: "The service instance processing this task has terminated unexpectedly."
                .to_owned(),
            service_name: base_service.name.clone(),
            service_version: base_service.version.clone(),
            service_tool_version: None,
            status: RECOVERABLE_ERROR_STATUS.to_string(),
        },
    };

    let (port, _) =
        MockServiceServer::launch_with_custom_endpoints(run_service_api(RunServiceData {
            service: base_service.clone(),
            task: Some(task),
            num_get_task_called: num_get_task_called.clone(),
            num_upload_called: num_upload_called.clone(),
            upload_value: serde_json::to_value(error_body)
                .expect("ErrorBody type should be serializable."),
            task_done_success: false,
        }))
        .await
        .unwrap();
    let service_api_address: String = format!("http://localhost:{}", port).to_string();

    //  build a test core
    let sc_running = Arc::new(Mutex::new(true));
    let service_running = Arc::new(Mutex::new(true));

    let mut sc = make_test_service_client(
        sc_running.clone(),
        service_api_address,
        base_dir_string.clone(),
    )
    .await;

    // different possible service client modes.
    let mut task_fetcher = SingleThreadTaskFetcher {};
    let task_uploader = TaskUploader {};
    let service_launcher = MockServiceLauncher {
        service: base_service.clone(),
        runtime_prefix: TESTING_PREFIX.to_string(),
        tasking_dir: base_dir_string.clone(),
        running: service_running.clone(),
        task_done_success: true,
        wait_forever: true,
        service_result: None,
    };

    let handler = tokio::spawn(async move {
        return sc
            .run_service(&mut task_fetcher, &task_uploader, &service_launcher)
            .await;
    });

    // terminate service
    tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;
    *service_running.lock() = false;
    // give service client some time to upload error to service api
    tokio::time::sleep(tokio::time::Duration::from_secs_f64(5.0)).await;

    assert!(
        *num_get_task_called.lock() > 0,
        "Service client should call get task at least once."
    );
    assert!(
        *num_upload_called.lock() > 0,
        "Service client should call upload task at least once."
    );

    let res = handler.await;
    assert!(res.is_ok(), "Service handler should return with no error.");
}
