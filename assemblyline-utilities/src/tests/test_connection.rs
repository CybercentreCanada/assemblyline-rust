use std::{collections::HashMap, sync::Arc};

use log::debug;
use parking_lot::Mutex;
use reqwest::StatusCode;
use rustls::Side::Server;
use url::Url;

use crate::{
    connection::{self, Connection, ServerType, TLSSettings},
    tests::{
        init,
        mock_api_server::{MockServerConfig, MockServiceServer, RequestDataResponse},
    },
    types::{authentication::Authentication, errors::ApiClientError, response::ErrorApiResponse},
};

#[tokio::test]
async fn test_connection_retry() {
    init();
    let max_retry = 3;

    // The test_retry endpoint for return max_retry times
    let retry_value: Arc<Mutex<i32>> = Arc::new(Mutex::new(max_retry));
    let (port, _) = MockServiceServer::launch_with_test_endpoints(MockServerConfig::with_retry(retry_value.clone()))
        .await
        .unwrap();

    let service_api_address: String = format!("http://localhost:{}", port).to_string();
    let tls_setting = TLSSettings::Native;

    // make sure that the connection class will retry max_retry + 1 times
    let total_retry = max_retry + 1;
    let connection = Connection::connect(
        service_api_address.clone(),
        ServerType::ServiceServer,
        false,
        Authentication::None,
        Some(total_retry.try_into().unwrap()),
        tls_setting.clone(),
        HashMap::new(),
        Some(1.0),
    )
    .await
    .unwrap();

    let test_retry_url = Url::parse(format!("{service_api_address}/test/test_retry/").as_str()).unwrap();

    debug!("Send data to test_retry endpoint. URL: {:?}", test_retry_url);

    let response = connection
        .request::<()>(reqwest::Method::GET, test_retry_url.clone(), connection::Body::None, None, None, None)
        .await
        .unwrap();

    // response should be ok after retrying four times.
    assert_eq!(response.status(), StatusCode::OK, "Response status code should be OK after retry.");

    // reset server retry to 3 times.
    *retry_value.lock() = max_retry;

    // The connection should fail and do no retry
    // connection should return error
    let connection = Connection::connect(
        service_api_address.clone(),
        ServerType::ServiceServer,
        false,
        Authentication::None,
        Some(0),
        tls_setting.clone(),
        HashMap::new(),
        Some(1.0),
    )
    .await
    .unwrap();

    let test_retry_url = Url::parse(format!("{service_api_address}/test/test_retry/").as_str()).unwrap();
    let response = connection
        .request::<()>(reqwest::Method::GET, test_retry_url.clone(), connection::Body::None, None, None, None)
        .await;

    assert!(response.is_err(), "Connection should return error response without retry.");
    let err = response.err().unwrap();

    assert!(err.to_string().contains("Max retry reached"));

    let num_retry = max_retry - *retry_value.lock();
    assert_eq!(num_retry, 1, "The connection function should only connect once.");
}

#[tokio::test]
async fn test_connection_headers_params() {
    init();
    let (port, _) = MockServiceServer::launch_with_test_endpoints(MockServerConfig::default()).await.unwrap();

    let service_api_address: String = format!("http://localhost:{}", port).to_string();
    let tls_setting = TLSSettings::Native;

    let connection = Connection::connect(
        service_api_address.clone(),
        ServerType::ServiceServer,
        false,
        Authentication::None,
        Some(1),
        tls_setting.clone(),
        HashMap::new(),
        Some(1000.0),
    )
    .await
    .unwrap();

    let get_request_data = Url::parse(format!("{service_api_address}/test/get_request_data/").as_str()).unwrap();

    let params: Vec<(String, String)> = vec![("a".to_string(), "b".to_string()), ("c".to_string(), "d".to_string())];
    let headers: HashMap<String, String> = HashMap::from([("a".to_string(), "b".to_string()), ("c".to_string(), "d".to_string())]);

    debug!("Sending request to : {}", get_request_data.to_string());
    let response = connection
        .request::<()>(
            reqwest::Method::GET,
            get_request_data.clone(),
            connection::Body::None,
            None,
            Some(params.clone()),
            Some(headers.clone()),
        )
        .await
        .unwrap();

    let response_data: RequestDataResponse = response.json().await.unwrap();

    for (k, v) in headers {
        assert_eq!(&v, response_data.header.get(&k).unwrap(), "{}: {} not in request header.", k, v);
    }

    for (k, v) in params {
        assert_eq!(&v, response_data.query.get(&k).unwrap(), "{}: {} not in request query.", k, v);
    }
}

#[tokio::test]
async fn test_get_api_path() {
    init();
    let service_api_address: String = "http://testest.test:1234".to_string();
    let tls_setting = TLSSettings::Native;

    let server_type = ServerType::ServiceServer;
    let connection = Connection::connect(
        service_api_address.clone(),
        server_type.clone(),
        false,
        Authentication::None,
        None,
        tls_setting.clone(),
        HashMap::new(),
        Some(1000.0),
    )
    .await
    .unwrap();

    let prefix = "test_prefix";
    let args = vec!["a", "b"];
    let result_url = connection.get_api_path(prefix, &args).unwrap();

    let expected_url = format!("{}/api/{}/test_prefix/a/b/", &service_api_address, &server_type.supported_version());
    assert_eq!(
        expected_url,
        result_url.as_str(),
        "URL should be: {} and we got {} instead",
        expected_url,
        result_url.as_str()
    );

    let server_type = ServerType::AssemblylineUiServer;
    let connection = Connection::connect(
        service_api_address.clone(),
        server_type.clone(),
        false,
        Authentication::None,
        None,
        tls_setting.clone(),
        HashMap::new(),
        Some(1000.0),
    )
    .await
    .unwrap();

    let prefix = "test_prefix";
    let args = vec!["a", "b"];
    let result_url = connection.get_api_path(prefix, &args).unwrap();

    let expected_url = format!("{}/api/{}/test_prefix/a/b/", &service_api_address, &server_type.supported_version());
    assert_eq!(
        expected_url,
        result_url.as_str(),
        "URL should be: {} and we got {} instead",
        expected_url,
        result_url.as_str()
    );
}

#[tokio::test]
async fn test_serializable_connection_error() {
    init();

    // serialize error in ErrorApiResponse if possible.
    let error_response = ErrorApiResponse {
        api_error_message: Some("test error message".to_string()),
        api_server_version: Some("vtest".to_string()),
        api_status_code: Some(404),
    };
    let (port, server) = MockServiceServer::launch_with_test_endpoints(MockServerConfig::with_error_response(Some(error_response.clone())))
        .await
        .unwrap();

    let service_api_address: String = format!("http://localhost:{}", port).to_string();
    let tls_setting = TLSSettings::Native;

    let connection = Connection::connect(
        service_api_address.clone(),
        ServerType::ServiceServer,
        false,
        Authentication::None,
        Some(1),
        tls_setting.clone(),
        HashMap::new(),
        Some(1000.0),
    )
    .await
    .unwrap();

    let test_response_url = Url::parse(format!("{service_api_address}/test/error_response/").as_str()).unwrap();
    let response = connection
        .request::<()>(reqwest::Method::GET, test_response_url.clone(), connection::Body::None, None, None, None)
        .await;

    let error = response.expect_err("This mock service api endpoint only returns errors.");

    match error {
        ApiClientError::ClientError {
            message,
            status_code,
            server_version,
        } => {
            assert_eq!(message, error_response.api_error_message.expect("Error message should exist."));
            assert_eq!(status_code.expect("Status code should exist."), error_response.api_status_code.unwrap());
            assert_eq!(
                server_version.expect("Server version should exist."),
                error_response.api_server_version.unwrap()
            );
        }
        e => {
            panic!("Connection returns the wrong type of error {e}");
        }
    };

    server.abort();
}

#[tokio::test]
async fn test_text_connection_error() {
    init();

    let (port, _) = MockServiceServer::launch_with_test_endpoints(MockServerConfig::with_error_response(None))
        .await
        .unwrap();

    let service_api_address: String = format!("http://localhost:{}", port).to_string();
    let tls_setting = TLSSettings::Native;

    let connection = Connection::connect(
        service_api_address.clone(),
        ServerType::ServiceServer,
        false,
        Authentication::None,
        Some(1),
        tls_setting.clone(),
        HashMap::new(),
        Some(1000.0),
    )
    .await
    .unwrap();

    let test_response_url = Url::parse(format!("{service_api_address}/test/error_response/").as_str()).unwrap();
    let response = connection
        .request::<()>(reqwest::Method::GET, test_response_url.clone(), connection::Body::None, None, None, None)
        .await;

    let error = response.expect_err("This mock service api endpoint only returns errors.");

    match error {
        ApiClientError::ClientError {
            status_code,
            message,
            server_version,
        } => {
            let code = status_code.expect("Service API should return the status code.");
            assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR);
        }
        e => {
            panic!("Connection returns the wrong type of error {e}");
        }
    };
}
