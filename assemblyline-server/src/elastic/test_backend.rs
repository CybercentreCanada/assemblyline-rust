use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use assemblyline_models::config::DatastoreType;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::{convert_wildcards_to_keywords, mapping_for_backend, Backend, ElasticHelper, Request};

async fn mock_root(
    status: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let server_hits = hits.clone();
    let status = status.to_owned();
    let headers = headers
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<Vec<_>>();
    let body = body.to_owned();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        server_hits.fetch_add(1, Ordering::SeqCst);
        let extra_headers = headers
            .iter()
            .map(|(key, value)| format!("{key}: {value}\r\n"))
            .collect::<String>();
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    (format!("http://{address}/"), hits)
}

#[tokio::test]
async fn explicit_backends_bypass_detection() {
    let elasticsearch = ElasticHelper::connect_with_backend(
        "http://127.0.0.1:1",
        false,
        None,
        false,
        DatastoreType::Elasticsearch,
    )
    .await
    .unwrap();
    assert_eq!(elasticsearch.backend, Backend::Elasticsearch);

    let opensearch = ElasticHelper::connect_with_backend(
        "http://127.0.0.1:1",
        false,
        None,
        false,
        DatastoreType::Opensearch,
    )
    .await
    .unwrap();
    assert_eq!(opensearch.backend, Backend::Opensearch);
}

#[tokio::test]
async fn auto_detects_elasticsearch_once() {
    let (url, hits) = mock_root(
        "200 OK",
        &[("X-Elastic-Product", "Elasticsearch")],
        r#"{"version":{"number":"8.19.3"},"tagline":"You Know, for Search"}"#,
    )
    .await;
    let helper = ElasticHelper::connect_with_backend(&url, false, None, false, DatastoreType::Auto)
        .await
        .unwrap();
    assert_eq!(helper.backend, Backend::Elasticsearch);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(helper.backend, Backend::Elasticsearch);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn auto_detects_opensearch_once() {
    let (url, hits) = mock_root(
        "200 OK",
        &[],
        r#"{"version":{"distribution":"opensearch","number":"2.19.1"},"tagline":"The OpenSearch Project: https://opensearch.org/"}"#,
    ).await;
    let helper = ElasticHelper::connect_with_backend(&url, false, None, false, DatastoreType::Auto)
        .await
        .unwrap();
    assert_eq!(helper.backend, Backend::Opensearch);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn auto_rejects_unsupported_and_malformed_responses() {
    let (url, _) = mock_root("200 OK", &[], r#"{"version":{"number":"1.2.3"}}"#).await;
    let error = ElasticHelper::connect_with_backend(&url, false, None, false, DatastoreType::Auto)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Unsupported or ambiguous"));

    let (url, _) = mock_root(
        "200 OK",
        &[("X-Elastic-Product", "Elasticsearch")],
        r#"{"version":{"distribution":"opensearch"}}"#,
    )
    .await;
    let error = ElasticHelper::connect_with_backend(&url, false, None, false, DatastoreType::Auto)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Unsupported or ambiguous"));

    let (url, _) = mock_root("200 OK", &[], "not-json").await;
    let error = ElasticHelper::connect_with_backend(&url, false, None, false, DatastoreType::Auto)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Malformed"));
}

#[tokio::test]
async fn auto_reports_authentication_and_connection_failures() {
    let (url, _) = mock_root("401 Unauthorized", &[], r#"{"error":"unauthorized"}"#).await;
    let error = ElasticHelper::connect_with_backend(&url, false, None, false, DatastoreType::Auto)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("authentication failure"));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let error = ElasticHelper::connect_with_backend(
        &format!("http://{address}/"),
        false,
        None,
        false,
        DatastoreType::Auto,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("connection failure"));
}

#[test]
fn backend_specific_pit_requests_are_compatible() {
    let host = url::Url::parse("http://localhost:9200/").unwrap();
    let elastic_create = Request::create_pit(&host, "files", "5m", Backend::Elasticsearch).unwrap();
    let open_create = Request::create_pit(&host, "files", "5m", Backend::Opensearch).unwrap();
    assert_eq!(elastic_create.url.path(), "/files/_pit");
    assert_eq!(open_create.url.path(), "/files/_search/point_in_time");
    assert_eq!(
        Request::delete_pit(&host, Backend::Elasticsearch)
            .unwrap()
            .url
            .path(),
        "/_pit"
    );
    assert_eq!(
        Request::delete_pit(&host, Backend::Opensearch)
            .unwrap()
            .url
            .path(),
        "/_search/point_in_time"
    );
    assert_eq!(
        super::pit::close_body(Backend::Elasticsearch, "elastic-pit".to_owned()),
        serde_json::json!({"id": "elastic-pit"})
    );
    assert_eq!(
        super::pit::close_body(Backend::Opensearch, "opensearch-pit".to_owned()),
        serde_json::json!({"pit_id": ["opensearch-pit"]})
    );
    let elastic_open: super::responses::OpenPit =
        serde_json::from_value(serde_json::json!({"id": "elastic-pit"})).unwrap();
    let open_search_open: super::responses::OpenPit =
        serde_json::from_value(serde_json::json!({"pit_id": "opensearch-pit"})).unwrap();
    assert_eq!(elastic_open.id, "elastic-pit");
    assert_eq!(open_search_open.id, "opensearch-pit");
    serde_json::from_value::<super::responses::ClosePit>(
        serde_json::json!({"succeeded": true, "num_freed": 1}),
    )
    .unwrap();
    serde_json::from_value::<super::responses::ClosePit>(
        serde_json::json!({"pits": [{"successful": true, "pit_id": "opensearch-pit"}]}),
    )
    .unwrap();
}

#[test]
fn backend_specific_task_requests_are_compatible() {
    let host = url::Url::parse("http://localhost:9200/").unwrap();
    assert_eq!(
        Request::get_task(&host, "node:123", true, "5s")
            .unwrap()
            .url
            .path(),
        "/_tasks/node:123"
    );
    assert!(Backend::Elasticsearch.supports_task_index_cleanup());
    assert!(!Backend::Opensearch.supports_task_index_cleanup());
}

#[test]
fn elasticsearch_security_provisioning_requests_are_unchanged() {
    let host = url::Url::parse("http://localhost:9200/").unwrap();
    let role = Request::put_role(&host, "manage_tasks").unwrap();
    assert_eq!(role.method, reqwest::Method::POST);
    assert_eq!(role.url.path(), "/_security/role/manage_tasks");

    let user = Request::post_user(&host, "plumber").unwrap();
    assert_eq!(user.method, reqwest::Method::POST);
    assert_eq!(user.url.path(), "/_security/user/plumber");
}

#[test]
fn wildcard_conversion_is_recursive_and_opensearch_only() {
    let original = serde_json::json!({
        "properties": {
            "top": {"type": "wildcard"},
            "nested": {"properties": {"child": {"type": "wildcard"}}},
            "stable": {"type": "keyword"}
        }
    });
    let mut opensearch = original.clone();
    convert_wildcards_to_keywords(&mut opensearch);
    assert_eq!(
        opensearch.pointer("/properties/top/type").unwrap(),
        "keyword"
    );
    assert_eq!(
        opensearch
            .pointer("/properties/nested/properties/child/type")
            .unwrap(),
        "keyword"
    );
    assert_eq!(
        opensearch.pointer("/properties/stable/type").unwrap(),
        "keyword"
    );
    assert_eq!(
        original.pointer("/properties/top/type").unwrap(),
        "wildcard"
    );
    assert_eq!(
        mapping_for_backend(&original, Backend::Elasticsearch).unwrap(),
        original
    );
}
