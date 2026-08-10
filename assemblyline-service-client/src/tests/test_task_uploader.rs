use std::{
    collections::{HashMap, HashSet},
    io::Write,
    sync::Arc,
};

use anyhow::Context;
use assemblyline_models::{
    messages::{
        service_api::{self, service_manifest::ServiceManifest},
        task::Task,
    },
    types::{ClassificationString, JsonMap, Sha256, Text},
};

use log::info;
use nom::AsBytes;
use parking_lot::Mutex;
use poem::{
    handler,
    middleware::AddData,
    post, put,
    web::{Data, Json},
    Endpoint, EndpointExt, IntoResponse, Route,
};
use rand::RngExt;
use reqwest::StatusCode;
use serde_json::{json, Value};

use crate::{
    connection::{Connection, TLSSettings},
    constants::{
        DEFAULT_SERVICE_ERROR_MESSAGE, RECOVERABLE_ERROR_STATUS, UNKNOWN_SERVICE_ERROR_TYPE,
    },
    task_uploader::task_uploader::TaskUploader,
    tests::{
        create_random_service_result, init,
        mock_service_api::{MockServiceServer, RawHeaderMap, TEST_API_VERSION, TEST_AUTH_KEY},
        sha256_data,
    },
    types::{
        errors::ServiceHandlerError,
        task::{ErrorBody, ErrorResponse, TaskUploadBody},
    },
};

async fn get_test_connection(port: u16) -> Connection {
    let service_api_address: String = format!("http://localhost:{}", port).to_string();
    let tls_setting = TLSSettings::Native;

    let mut headers: HashMap<String, String> = HashMap::new();
    headers.insert("X-APIKey".to_string(), TEST_AUTH_KEY.to_string());
    Connection::connect(
        service_api_address.clone(),
        None,
        tls_setting.clone(),
        headers,
        Some(1000.0),
    )
    .await
    .unwrap()
}

fn initialize_upload_error_test_data() -> (ServiceManifest, Task) {
    let test_manifest: ServiceManifest = rand::rng().random();
    let mut initial_task: Task = rand::rng().random();

    let service_name = test_manifest.service.name.clone();

    initial_task.service_name = service_name.clone();

    (test_manifest, initial_task)
}

fn create_upload_task_data(
    tasking_dir_string: String,
) -> (
    ServiceManifest,
    Task,
    service_api::result::Result,
    HashMap<Sha256, service_api::result::File>,
) {
    let test_manifest: ServiceManifest = rand::rng().random();
    let mut initial_task: Task = rand::rng().random();

    let sha256: Sha256 = initial_task.fileinfo.sha256.clone();
    let service_name = test_manifest.service.name.clone();
    let service_version = test_manifest.service.version.clone();
    let tool_version = test_manifest.tool_version.clone();

    initial_task.service_name = service_name.clone();

    let mut service_result = create_random_service_result(
        sha256.clone(),
        service_name.clone(),
        service_version.clone(),
        tool_version.clone(),
    );

    let mut supplementary_files: Vec<service_api::result::File> = Vec::new();
    let mut extracted_files: Vec<service_api::result::File> = Vec::new();

    info!("tasking dir: {}", tasking_dir_string);

    let test_data = "aaaaaaaa".as_bytes();
    let file_sha256 = sha256_data(test_data);
    let test_file_path = format!("{}/{}", &tasking_dir_string, &file_sha256);
    {
        let mut test_file = std::fs::File::create(&test_file_path).unwrap();
        test_file.write_all(test_data).unwrap();
        test_file.flush().unwrap();
    }

    supplementary_files.push(service_api::result::File {
        name: "file_a".to_string(),
        sha256: file_sha256.parse().unwrap(),
        description: Text("file_a description".to_string()),
        classification: ClassificationString::default_unrestricted(),
        is_section_image: false,
        parent_relation: Default::default(),
        allow_dynamic_recursion: false,
        path: test_file_path,
        is_supplementary: true,
    });

    let test_data = "bbbbbbbb".as_bytes();
    let file_sha256 = sha256_data(test_data);
    let test_file_path = format!("{}/{}", &tasking_dir_string, &file_sha256);
    {
        let mut test_file = std::fs::File::create(&test_file_path).unwrap();
        test_file.write_all(test_data).unwrap();
        test_file.flush().unwrap();
    }

    extracted_files.push(service_api::result::File {
        name: "file_b".to_string(),
        sha256: file_sha256.parse().unwrap(),
        description: Text("file_b description".to_string()),
        classification: ClassificationString::default_unrestricted(),
        is_section_image: false,
        parent_relation: Default::default(),
        allow_dynamic_recursion: false,
        path: test_file_path,
        is_supplementary: false,
    });

    service_result.response.extracted = extracted_files.clone();
    service_result.response.supplementary = supplementary_files.clone();

    let mut files: HashMap<Sha256, service_api::result::File> = HashMap::new();

    files.extend(
        extracted_files
            .iter()
            .map(|f| (f.sha256.clone(), f.to_owned())),
    );
    files.extend(
        supplementary_files
            .iter()
            .map(|f| (f.sha256.clone(), f.to_owned())),
    );

    (test_manifest, initial_task, service_result, files)
}

#[derive(Clone, Debug)]
struct TaskErrorData {
    pub task: Task,
    pub error_value: Value,
}

#[derive(Clone, Debug)]
struct TaskResultData {
    pub task: Task,
    pub call_number: Arc<Mutex<usize>>,
    pub freshen_sequence: Vec<bool>,
    pub response_sequence: Vec<Value>,
    pub response_code_sequence: Vec<StatusCode>,
    pub result_sequence: Vec<Value>,
    pub files: HashMap<Sha256, service_api::result::File>,
    pub missing_files: Arc<Mutex<HashSet<Sha256>>>,
}


fn upload_error_api(task_error_data: TaskErrorData) -> impl Endpoint {
    Route::new()
        .at(format!("/task"), post(upload_error))
        .with(AddData::new(task_error_data))
}

fn upload_result_api(task_result_data: TaskResultData) -> impl Endpoint {
    Route::new()
        .at(format!("/file"), put(upload_file))
        .at(format!("/task"), post(upload_task_result))
        .with(AddData::new(task_result_data))
}

#[handler]
async fn upload_task_result(
    Json(body): Json<JsonMap>,
    task_result_data: Data<&TaskResultData>,
) -> Result<poem::Response, poem::error::Error> {
    let upload_task_body: TaskUploadBody = serde_json::from_value(serde_json::Value::Object(body))
        .with_context(|| "deserializer request body to TaskUploadBody")
        .unwrap();

    let mut call_value = *task_result_data.call_number.lock();

    assert!(
        call_value < task_result_data.response_sequence.len(),
        "Making more calls to API server than expected."
    );

    let freshen = task_result_data.freshen_sequence.get(call_value).unwrap();

    assert_eq!(
        upload_task_body.freshen, *freshen,
        "The {}st call to upload task result should set freshen to {}",
        call_value, freshen
    );

    assert_eq!(
        task_result_data.task, upload_task_body.task,
        "The uploaded task object is incorrect."
    );

    assert!(
        upload_task_body.result.is_some(),
        "Upload task result should upload Result."
    );
    assert!(
        upload_task_body.error.is_none(),
        "Only one of Error or Result should be uploaded."
    );

    let expected_result = task_result_data.result_sequence.get(call_value).unwrap();
    if let Some(result) = upload_task_body.result {
        assert_eq!(
            *expected_result, result,
            "The value of uploaded result is incorrect."
        );
    }

    let response = task_result_data.response_sequence.get(call_value).unwrap();
    let status_code = task_result_data
        .response_code_sequence
        .get(call_value)
        .unwrap();
    *task_result_data.call_number.lock() = call_value + 1;

    match status_code.to_owned() {
        StatusCode::OK => Ok(MockServiceServer::make_api_response(response)),
        code => Err(MockServiceServer::make_empty_api_error(
            code,
            response.to_string().as_str(),
        )),
    }
}

#[handler]
async fn upload_file(
    body: poem::Body,
    header: RawHeaderMap,
    task_result_data: Data<&TaskResultData>,
) -> Result<poem::Response, poem::error::Error> {
    let mut call_value = *task_result_data.call_number.clone().lock();
    assert!(
        call_value < task_result_data.response_sequence.len(),
        "Making more calls to API server than expected."
    );
    let response = task_result_data.response_sequence.get(call_value).unwrap();
    let status_code = task_result_data
        .response_code_sequence
        .get(call_value)
        .unwrap();
    let body_sha: Sha256 = sha256_data(body.into_bytes().await.unwrap().as_bytes())
        .parse()
        .expect("Convert String hash to SHA256");

    let return_response = match status_code.to_owned() {
        StatusCode::OK => {
            let missing_files_mutex = task_result_data.missing_files.clone();
            assert!(
                !missing_files_mutex.lock().is_empty(),
                "No missing files. Upload file endpoint should not be called."
            );

            assert!(
                missing_files_mutex.lock().get(&body_sha).is_some(),
                "The uploaded file {} is not part of requested missing files.",
                body_sha
            );

            let valid_file = task_result_data
                .files
                .get(&body_sha)
                .expect("File sha should exist.");

            let request_sha = header
                .map
                .get("Sha256")
                .expect("Request should container header 'sha256'")
                .to_str()
                .unwrap();

            assert_eq!(
                request_sha,
                body_sha.to_string().as_str(),
                "Request header value for sha256 does not match the sha of the file uploaded."
            );
            assert_eq!(request_sha, valid_file.sha256.to_string().as_str(), "Request header value for sha256 does not match the does not match task result.sha256 value");

            let request_classification = header
                .map
                .get("Classification")
                .expect("Request should container header 'classification'")
                .to_str()
                .unwrap();

            assert_eq!(
                valid_file.classification.as_str(),
                request_classification,
                "Request header value for classification does not match file classification"
            );
            let request_ttl = header
                .map
                .get("Ttl")
                .expect("Request should container header 'ttl'")
                .to_str()
                .unwrap();
            assert_eq!(
                task_result_data.task.ttl.to_string().as_str(),
                request_ttl,
                "Request header value for ttl does not match task ttl."
            );

            let request_is_section_image = header
                .map
                .get("Is-Section-Image")
                .expect("Request should container header 'Is-Section-Image'")
                .to_str()
                .unwrap();

            assert_eq!(valid_file.is_section_image.to_string().as_str(), request_is_section_image, "Request header value for Is-Section-Image does not match task result.is_section_image value.");

            let request_is_supplementary = header
                .map
                .get("Is-Supplementary")
                .expect("Request should container header 'Is-Supplementary'")
                .to_str()
                .unwrap();

            assert_eq!(valid_file.is_supplementary.to_string().as_str(), request_is_supplementary, "Request header value for Is-Supplementary does not match task result.is_supplementary value.");

            missing_files_mutex.lock().remove(&body_sha);
            Ok(MockServiceServer::make_api_response(response))
        }
        code => Err(MockServiceServer::make_empty_api_error(
            code,
            &response.as_str().unwrap().to_string(),
        )),
    };

    *task_result_data.call_number.lock() = call_value + 1;

    return_response
}

#[handler]
async fn upload_error(
    Json(body): Json<JsonMap>,
    task_error_data: Data<&TaskErrorData>,
) -> Result<poem::Response, poem::error::Error> {
    let upload_task_body: TaskUploadBody = serde_json::from_value(serde_json::Value::Object(body))
        .expect("deserializer request body to TaskUploadBody");
    let expected_task = task_error_data.task.clone();
    let expected_data = task_error_data.error_value.clone();

    assert_eq!(expected_task, upload_task_body.task);

    if let Some(error) = upload_task_body.error {
        let expected_err: ErrorBody = serde_json::from_value(expected_data).unwrap();
        let cur_err: ErrorBody =
            serde_json::from_value(error).expect("Deserialize request body to ErrorBody");
        assert_eq!(
            expected_task.fileinfo.sha256, cur_err.sha256,
            "The error sha256 should be the task sha256"
        );
        assert_eq!(expected_err, cur_err, "Uploaded Error data is incorrect.");

        assert!(
            upload_task_body.result.is_none(),
            "Only one of Error or Result should be uploaded."
        );

        return Ok(Json(json!({
            "data": "OK",
        }))
        .into_response());
    }

    if upload_task_body.result.is_some() {
        return Err(MockServiceServer::make_empty_api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "This testing function is for testing uploading errors only.",
        ));
    }

    Err(MockServiceServer::make_empty_api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Task upload body should contain error or result.",
    ))
}

#[tokio::test]
async fn test_upload_error_default_values() {
    init();
    let (test_manifest, initial_task) = initialize_upload_error_test_data();

    let sha256: Sha256 = initial_task.fileinfo.sha256.clone();
    let service_name = test_manifest.service.name.clone();
    let service_version = test_manifest.service.version.clone();

    // task uploader should upload task with default values when None are given
    let task_error = ErrorBody {
        sha256: sha256.clone(),
        error_type: UNKNOWN_SERVICE_ERROR_TYPE.to_string(),
        response: ErrorResponse {
            message: DEFAULT_SERVICE_ERROR_MESSAGE.to_string(),
            service_name: service_name.clone(),
            service_version: service_version.clone(),
            service_tool_version: None,
            status: RECOVERABLE_ERROR_STATUS.to_string(),
        },
    };

    let task_error_data = TaskErrorData {
        task: initial_task.clone(),
        error_value: serde_json::to_value(task_error).unwrap(),
    };

    let (port, server) =
        MockServiceServer::launch_with_custom_endpoints(upload_error_api(task_error_data))
            .await
            .unwrap();
    let connection = get_test_connection(port).await;

    let task_uploader = TaskUploader {};

    let res = task_uploader
        .upload_task_error(
            &initial_task,
            &test_manifest.service,
            &connection,
            None,
            None,
            None,
            None,
        )
        .await;

    assert!(res.is_ok(), "Task Uploader should complete with no error.");
    server.abort();
}

#[tokio::test]
async fn test_upload_error_default_custom_values() {
    init();
    let (test_manifest, initial_task) = initialize_upload_error_test_data();

    let sha256: Sha256 = initial_task.fileinfo.sha256.clone();
    let service_name = test_manifest.service.name.clone();
    let service_version = test_manifest.service.version.clone();

    // task uploader should use the custom values given
    let error_type = "test type";
    let message = "test message";
    let status = "service error status";

    // task uploader should upload the json given and ignore type, message, and status parameter.
    let task_error = serde_json::to_value(ErrorBody {
        sha256: sha256.clone(),
        error_type: error_type.to_string(),
        response: ErrorResponse {
            message: message.to_string(),
            service_name: service_name.clone(),
            service_version: service_version.clone(),
            service_tool_version: None,
            status: status.to_string(),
        },
    })
    .unwrap();

    let test_string = "test".to_string();

    let task_error_data = TaskErrorData {
        task: initial_task.clone(),
        error_value: task_error.clone(),
    };
    let (port, server) =
        MockServiceServer::launch_with_custom_endpoints(upload_error_api(task_error_data))
            .await
            .unwrap();
    let connection = get_test_connection(port).await;
    let task_uploader = TaskUploader {};
    let _ = task_uploader
        .upload_task_error(
            &initial_task,
            &test_manifest.service,
            &connection,
            Some(task_error),
            Some(test_string.clone()),
            Some(test_string.clone()),
            Some(test_string.clone()),
        )
        .await
        .unwrap();

    server.abort();
}

#[tokio::test]
async fn test_upload_error_json() {
    init();
    let (test_manifest, initial_task) = initialize_upload_error_test_data();

    let sha256: Sha256 = initial_task.fileinfo.sha256.clone();
    let service_name = test_manifest.service.name.clone();
    let service_version = test_manifest.service.version.clone();

    // task uploader should use the custom values given
    let error_type = "test type";
    let message = "test message";
    let status = "service error status";

    let task_error = ErrorBody {
        sha256: sha256.clone(),
        error_type: error_type.to_string(),
        response: ErrorResponse {
            message: message.to_string(),
            service_name: service_name.clone(),
            service_version: service_version.clone(),
            service_tool_version: None,
            status: status.to_string(),
        },
    };

    let task_error_data = TaskErrorData {
        task: initial_task.clone(),
        error_value: serde_json::to_value(task_error).unwrap(),
    };

    let (port, server) =
        MockServiceServer::launch_with_custom_endpoints(upload_error_api(task_error_data))
            .await
            .unwrap();
    let connection = get_test_connection(port).await;
    let task_uploader = TaskUploader {};
    let res = task_uploader
        .upload_task_error(
            &initial_task,
            &test_manifest.service,
            &connection,
            None,
            Some(message.to_string()),
            Some(error_type.to_string()),
            Some(status.to_string()),
        )
        .await;

    assert!(res.is_ok(), "Task Uploader should complete with no error.");
    server.abort();
}

#[tokio::test]
async fn test_upload_task_result_no_missing_files() {
    init();

    let tasking_dir = tempfile::tempdir().unwrap();
    let tasking_dir_string = tasking_dir.path().to_str().unwrap().to_string();
    let (_, initial_task, service_result, _) = create_upload_task_data(tasking_dir_string);

    let task_uploader = TaskUploader {};

    // Upload task when no missing file
    // The uploader should set freshen as True for service server to check for missing file
    // with no missing files, uploader expects "success:true" from service server.
    // no more action and return.
    let call_number: Arc<Mutex<usize>> = Arc::new(0.into());
    let response_sequence = vec![json!({"success": true})];
    let freshen_sequence = vec![true];
    let result_sequence = vec![serde_json::to_value(&service_result).unwrap()];
    let response_code_sequence = vec![StatusCode::OK];

    let upload_data = TaskResultData {
        task: initial_task.clone(),
        call_number: call_number.clone(),
        freshen_sequence: freshen_sequence,
        result_sequence: result_sequence,
        response_sequence: response_sequence,
        response_code_sequence: response_code_sequence,
        files: HashMap::new(),
        missing_files: Arc::new(Mutex::new(HashSet::new())),
    };

    let (port, server) =
        MockServiceServer::launch_with_custom_endpoints(upload_result_api(upload_data))
            .await
            .unwrap();
    let connection = get_test_connection(port).await;

    // The task upload should be complete successfully. It does not need to call file upload endpoint
    task_uploader
        .upload_task_result(&initial_task, service_result.clone(), &connection)
        .await
        .unwrap();

    let call_value = call_number.lock();
    // there should only be one call to the service_api to upload task result
    assert_eq!(*call_value, 1);

    server.abort();
}

#[tokio::test]
async fn test_upload_task_result_with_missing_files() {
    init();

    let tasking_dir = tempfile::tempdir().unwrap();
    let tasking_dir_string = tasking_dir.path().to_str().unwrap().to_string();
    let (_, initial_task, service_result, files) = create_upload_task_data(tasking_dir_string);

    let task_uploader = TaskUploader {};

    // Upload task with missing files
    // The uploader should set freshen as True for service server to check for missing file
    // with missing files, there should be subsquent files to upload all the missing files
    // service client at the end sends a final task upload request with freshen=false, to complete the task upload.
    let call_number: Arc<Mutex<usize>> = Arc::new(0.into());
    let missing_files: HashSet<Sha256> = HashSet::from_iter(files.keys().map(|k| k.to_owned()));

    let missing_files_ref = Arc::new(Mutex::new(missing_files.clone()));
    let service_result_value = serde_json::to_value(&service_result).unwrap();
    let response_sequence = vec![
        json!({"success": false, "missing_files": &missing_files}),
        json!({"success": true}),
        json!({"success": true}),
        json!({"success": true}),
    ];

    let freshen_sequence = vec![true, true, true, false];
    let result_sequence = vec![
        service_result_value.clone(),
        Value::Null,
        Value::Null,
        service_result_value.clone(),
    ];
    let response_code_sequence = vec![
        StatusCode::OK,
        StatusCode::OK,
        StatusCode::OK,
        StatusCode::OK,
    ];
    let upload_data = TaskResultData {
        task: initial_task.clone(),
        call_number: call_number.clone(),
        freshen_sequence: freshen_sequence,
        result_sequence: result_sequence,
        response_sequence: response_sequence,
        response_code_sequence: response_code_sequence,
        files: files.clone(),
        missing_files: missing_files_ref.clone(),
    };

    let (port, server) =
        MockServiceServer::launch_with_custom_endpoints(upload_result_api(upload_data))
            .await
            .unwrap();

    let connection = get_test_connection(port).await;

    // The task upload should be complete successfully. It does not need to call file upload endpoint
    task_uploader
        .upload_task_result(&initial_task, service_result.clone(), &connection)
        .await
        .unwrap();

    let call_value = call_number.lock();
    assert_eq!(
        *call_value, 4,
        "There should be total of 4 calls to the api-server to upload task and missing files."
    );
    assert!(
        missing_files_ref.lock().is_empty(),
        "There are still files missing at the end of task upload."
    );
    server.abort();
}

#[tokio::test]
async fn test_upload_task_result_with_task_upload_file_error() {
    init();

    let tasking_dir = tempfile::tempdir().unwrap();
    let tasking_dir_string = tasking_dir.path().to_str().unwrap().to_string();
    let (_, initial_task, service_result, files) = create_upload_task_data(tasking_dir_string);
    let error_message = format!("TEST ERROR FAIL MESSAGE");

    let task_uploader = TaskUploader {};

    // Upload task and handler error with upload file.
    // errors should be returned immediately when the server failed to upload a file to the API.
    let call_number: Arc<Mutex<usize>> = Arc::new(0.into());
    let missing_files: HashSet<Sha256> = HashSet::from_iter(files.keys().map(|k| k.to_owned()));

    let missing_files_ref = Arc::new(Mutex::new(missing_files.clone()));
    let service_result_value = serde_json::to_value(&service_result).unwrap();

    let response_sequence = vec![
        json!({"success": false, "missing_files": &missing_files}),
        Value::String(error_message.clone()),
    ];
    let freshen_sequence = vec![true, true];
    let result_sequence = vec![service_result_value.clone(), Value::Null];
    let response_code_sequence = vec![StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR];
    let upload_data = TaskResultData {
        task: initial_task.clone(),
        call_number: call_number.clone(),
        freshen_sequence: freshen_sequence,
        result_sequence: result_sequence,
        response_sequence: response_sequence,
        response_code_sequence: response_code_sequence,
        files: files.clone(),
        missing_files: missing_files_ref.clone(),
    };

    let (port, server) =
        MockServiceServer::launch_with_custom_endpoints(upload_result_api(upload_data))
            .await
            .unwrap();
    let connection = get_test_connection(port).await;

    // The task upload should be complete successfully. It does not need to call file upload endpoint
    let res = task_uploader
        .upload_task_result(&initial_task, service_result.clone(), &connection)
        .await;

    let error = res.expect_err("This task upload should return error.");

    match error {
        ServiceHandlerError::ServiceApiConnectionError {
            message,
            status_code,
            server_version,
        } => {
            // make sure error message from the service api gets propagated out of the function
            assert_eq!(error_message, message);
            assert_eq!(
                StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                status_code.expect("Status code should exist.")
            );
            assert_eq!(
                server_version.expect("Server version should exist."),
                TEST_API_VERSION
            );
        }
        e => {
            panic!("Connection returns the wrong type of error {e}");
        }
    };

    let call_value = call_number.lock();
    // there should be 2 calls to the service api, since upload file errored out in the first try.
    assert_eq!(*call_value, 2);

    server.abort();
}
