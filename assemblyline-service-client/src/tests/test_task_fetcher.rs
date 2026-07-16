use std::collections::HashMap;

use assemblyline_models::{messages::task::Task, types::Sha256};
use log::info;
use md5::Digest;
use poem::{
    get, handler,
    middleware::AddData,
    web::{Data, Json},
    Body, Endpoint, EndpointExt, IntoResponse, Route,
};
use rand::Rng;
use reqwest::StatusCode;
use serde_json::json;

use crate::{
    connection::{Connection, TLSSettings},
    task_fetcher::{
        single_thread_task_fetcher::SingleThreadTaskFetcher, task_fetcher::TaskFetcher,
    },
    tests::{
        init,
        mock_service_api::{MockServiceServer, TEST_AUTH_KEY, TEST_SERVER_VERSION},
    },
    types::{errors::ServiceHandlerError, response::APIResponse},
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

#[handler]
async fn download_file(
    poem::web::Path(sha256): poem::web::Path<String>,
    fetcher_data: Data<&TaskFetcherData>,
) -> Result<poem::Response, poem::error::Error> {
    let hash: Sha256 = sha256.parse().unwrap();
    match fetcher_data.file_hashes.get(&hash) {
        Some(data) => {
            let file_size = data.len();
            let body = Body::from_vec(data.clone());
            let filename = format!("UTF-8''{}", urlencoding::encode(&hash));
            info!("The file name of the file is {} ", &sha256);
            return Ok(poem::Response::builder()
                .content_type("application/octet-stream")
                .header("Content-Length", file_size.to_string())
                .header(
                    "Content-Disposition",
                    format!("attachment; filename=file.bin; filename*={filename}"),
                )
                .body(body));
        }

        None => {
            return Err(MockServiceServer::make_empty_api_error(
                StatusCode::NOT_FOUND,
                "Cannot find file sha.",
            ))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaskFetcherData {
    pub file_hashes: HashMap<Sha256, Vec<u8>>,
    pub task: Option<Task>,
}

#[handler]
pub async fn get_task(
    fetcher_data: Data<&TaskFetcherData>,
) -> Result<poem::Response, poem::error::Error> {
    if let Some(task) = &fetcher_data.task {
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

fn fetcher_api(data: TaskFetcherData) -> impl Endpoint {
    Route::new()
        .at(format!("/file/:sha256"), get(download_file))
        .at(format!("/task"), get(get_task))
        .with(AddData::new(data))
}

fn initialize_download_task_file_data() -> (Sha256, Vec<u8>, HashMap<Sha256, Vec<u8>>) {
    // create test file data and sha
    let file_size = 1000;
    let file_data = b"x".repeat(file_size);
    let mut hasher = sha2::Sha256::default();
    std::io::Write::write_all(&mut hasher, &file_data).unwrap();
    let good_sha: Sha256 = hex::encode(hasher.finalize()).parse().unwrap();

    let file_hashes = HashMap::from([(good_sha.clone(), file_data.clone())]);

    (good_sha, file_data, file_hashes)
}

#[tokio::test]
async fn test_download_task_file_success() {
    init();

    let tasking_dir = tempfile::tempdir().unwrap();
    let tasking_dir_string = tasking_dir.path().to_str().unwrap().to_string();
    let (good_sha, file_data, file_hashes) = initialize_download_task_file_data();

    // initialize server connection
    let fetcher_data = TaskFetcherData {
        file_hashes: file_hashes.clone(),
        task: None,
    };

    let (port, _) = MockServiceServer::launch_with_custom_endpoints(fetcher_api(fetcher_data))
        .await
        .unwrap();
    let connection = get_test_connection(port).await;

    let task_fetcher = SingleThreadTaskFetcher {};

    // Task fetcher should download file to disk
    let file_path = task_fetcher
        .download_file(
            good_sha.clone(),
            tasking_dir.path().to_path_buf(),
            &connection,
        )
        .await
        .expect(format!("Cannot find {good_sha}.").as_str());

    let path_string = file_path.to_string_lossy().to_string();
    assert_eq!(
        path_string,
        format!("{tasking_dir_string}/{}", good_sha.clone()),
        "Returned file path should be in the format of tasking_dir/sha256"
    );

    assert!(
        file_path.exists(),
        "The downloaded file should exist on disk."
    );

    // make sure the file requested is downloaded with correct data
    let data = tokio::fs::read(file_path).await.unwrap();
    assert_eq!(file_data, data, "Incorrect data write to disk.");
}

#[tokio::test]
async fn test_download_task_file_mismatched_sha() {
    init();

    let tasking_dir = tempfile::tempdir().unwrap();
    let (good_sha, file_data, mut file_hashes) = initialize_download_task_file_data();

    let bad_sha: Sha256 = rand::rng().random();
    file_hashes.insert(bad_sha.clone(), file_data.clone());

    // initialize server connection
    let fetcher_data = TaskFetcherData {
        file_hashes: file_hashes.clone(),
        task: None,
    };

    let (port, _) = MockServiceServer::launch_with_custom_endpoints(fetcher_api(fetcher_data))
        .await
        .unwrap();
    let connection = get_test_connection(port).await;

    let mut task_fetcher = SingleThreadTaskFetcher {};
    // throw error when downloaded file sha doesn't match
    let response_error = task_fetcher
        .download_file(
            bad_sha.clone(),
            tasking_dir.path().to_path_buf(),
            &connection,
        )
        .await
        .expect_err(
            "Task fetcher should throw error when the data body does not match that requested sha",
        );

    match response_error {
        ServiceHandlerError::FileHashMisMatch {
            requested_sha,
            content_sha,
        } => {
            assert_eq!(requested_sha, bad_sha);
            assert_eq!(content_sha, good_sha);
        }
        e => panic!("Requesting bad sha results in unexpected error: {e}"),
    }
}

#[tokio::test]
async fn test_download_task_file_unknown_sha() {
    init();

    let tasking_dir = tempfile::tempdir().unwrap();
    let (_, _, file_hashes) = initialize_download_task_file_data();

    // initialize server connection
    let fetcher_data = TaskFetcherData {
        file_hashes: file_hashes.clone(),
        task: None,
    };

    let (port, _) = MockServiceServer::launch_with_custom_endpoints(fetcher_api(fetcher_data))
        .await
        .unwrap();
    let connection = get_test_connection(port).await;

    let task_fetcher = SingleThreadTaskFetcher {};

    // throw error when server cannot find file
    let unknown_sha: Sha256 = rand::rng().random();
    let response_error = task_fetcher
        .download_file(
            unknown_sha.clone(),
            tasking_dir.path().to_path_buf(),
            &connection,
        )
        .await
        .expect_err(
            "Task fetcher should throw error when it cannot find a file with the given sha.",
        );

    match response_error {
        ServiceHandlerError::ServiceApiConnectionError {
            message,
            status_code,
            server_version,
        } => {
            assert_eq!(
                status_code.expect("Status code should exist."),
                StatusCode::NOT_FOUND.as_u16()
            );
        }
        e => panic!("Requesting unknown sha results in unexpected error: {e}"),
    }
}

#[tokio::test]
async fn test_request_task_none() {
    init();

    // return none if api server have no task
    // initialize server connection
    let task_fetcher = SingleThreadTaskFetcher {};
    // let api_server = MockServiceServer::new(MockServerConfig::with_task(None));
    let fetcher_data = TaskFetcherData {
        file_hashes: HashMap::new(),
        task: None,
    };
    let (port, _) = MockServiceServer::launch_with_custom_endpoints(fetcher_api(fetcher_data))
        .await
        .unwrap();
    let connection = get_test_connection(port).await;

    let response = task_fetcher
        .get_task(&connection)
        .await
        .expect("Get task should not return error.");

    assert_eq!(response, None);
}

#[tokio::test]
async fn test_request_task_some() {
    // return task object if we have a response
    init();
    let task: Task = rand::rng().random();
    let fetcher_data = TaskFetcherData {
        file_hashes: HashMap::new(),
        task: Some(task.clone()),
    };

    let task_fetcher = SingleThreadTaskFetcher {};

    let (port, server) = MockServiceServer::launch_with_custom_endpoints(fetcher_api(fetcher_data))
        .await
        .unwrap();
    let connection = get_test_connection(port).await;

    let response = task_fetcher
        .get_task(&connection)
        .await
        .expect("Get task should not return error.");
    assert!(response.is_some());

    let data = response.expect("Server should return a task.");
    assert_eq!(data, task);
}
