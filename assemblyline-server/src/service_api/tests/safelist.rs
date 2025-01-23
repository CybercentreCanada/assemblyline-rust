
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::safelist::Safelist;
use assemblyline_models::ExpandingClassification;
use chrono::Utc;
use reqwest::header::HeaderMap;

use crate::common::tagging::SafelistFile;
use crate::service_api::helpers::APIResponse;

use super::{setup, AUTH_KEY, random_hash};

fn headers() -> HeaderMap {
    [
        ("Container-Id", random_hash(12)),
        ("X-APIKey", AUTH_KEY.to_owned()),
        ("Service-Name", "Badlist".to_owned()),
        ("Service-Version", random_hash(2)),
        ("Service-Tool-Version", random_hash(64)),
        ("Timeout", "1".to_owned()),
        ("X-Forwarded-For", "127.0.0.1".to_owned()),
    ].into_iter()
    .map(|(name, value)|(reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}

fn build_safelist(ce: &ClassificationParser) -> Safelist {
    Safelist {
        added: Utc::now(),
        classification: ExpandingClassification::unrestricted(ce),
        enabled: true,
        expiry_ts: None,
        hashes: Default::default(),
        file: None,
        sources: vec![],
        tag: None,
        signature: None,
        type_: assemblyline_models::datastore::safelist::SafehashTypes::File,
        updated: Utc::now(),
    }
}

#[tokio::test]
async fn test_safelist_exist() {
    let (client, core, _guard, address) = setup(headers()).await;

    let valid_hash = random_hash(64);
    let mut valid_resp = build_safelist(&core.classification_parser);
    valid_resp.hashes.sha256 = Some(valid_hash.parse().unwrap());
    core.datastore.safelist.save(&valid_hash, &valid_resp, None, None).await.unwrap();

    let resp = client.get(format!("{address}/api/v1/safelist/{valid_hash}/")).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<Safelist> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response, valid_resp);
}

#[tokio::test]
async fn test_safelist_missing() {
    let (client, _core, _guard, address) = setup(headers()).await;

    let invalid_hash = random_hash(64);

    let resp = client.get(format!("{address}/api/v1/safelist/{invalid_hash}/")).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<Option<Safelist>> = serde_json::from_slice(&body).unwrap();
    assert!(body.api_response.is_none());
}

#[tokio::test]
async fn test_get_full_safelist() {
    let (client, _core, _guard, address) = setup(headers()).await;

    let resp = client.get(format!("{address}/api/v1/safelist/")).send().await.unwrap();

    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<SafelistFile> = serde_json::from_slice(&body).unwrap();

    assert!(!body.api_response.match_.is_empty());
    assert!(!body.api_response.regex.is_empty());
}

#[tokio::test]
async fn test_get_full_safelist_specific() {
    let (client, _core, _guard, address) = setup(headers()).await;

    const TAG_TYPE: &str = "network.dynamic.domain";
    let resp = client.get(format!("{address}/api/v1/safelist/?tag_types={TAG_TYPE}")).send().await.unwrap();

    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<SafelistFile> = serde_json::from_slice(&body).unwrap();

    assert!(!body.api_response.match_.is_empty());
    assert!(!body.api_response.regex.is_empty());

    for key in body.api_response.match_.keys() {
        assert_eq!(key, TAG_TYPE);
    }
    for key in body.api_response.regex.keys() {
        assert_eq!(key, TAG_TYPE);
    }
}

#[tokio::test]
async fn test_get_signature_safelist() {
    let (client, core, _guard, address) = setup(headers()).await;

    let key = random_hash(16);
    let mut resp = build_safelist(&core.classification_parser);
    resp.signature = Some(assemblyline_models::datastore::safelist::Signature { name: "test".to_string() });
    resp.type_ = assemblyline_models::datastore::safelist::SafehashTypes::Signature;
    core.datastore.safelist.save(&key, &resp, None, None).await.unwrap();
    core.datastore.safelist.commit(None).await.unwrap();

    let resp = client.get(format!("{address}/api/v1/safelist/signatures")).send().await.unwrap();

    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<Vec<String>> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response, vec!["test"]);
}