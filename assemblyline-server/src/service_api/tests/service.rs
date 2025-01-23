
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::heuristic::Heuristic;
use assemblyline_models::ExpandingClassification;
use reqwest::header::{HeaderMap, HeaderValue};

use crate::service_api::helpers::APIResponse;
use crate::service_api::tests::{build_service, empty_delta};
use crate::service_api::v1::service::RegisterResponse;

use super::{setup, AUTH_KEY, random_hash};

fn headers() -> HeaderMap {
    [
        ("Container-Id", random_hash(12)),
        ("X-APIKey", AUTH_KEY.to_owned()),
        ("Service-Tool-Version", random_hash(64)),
        ("X-Forwarded-For", "127.0.0.1".to_owned()),
    ].into_iter()
    .map(|(name, value)|(reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}

fn build_heuristic(name: String, ce: &ClassificationParser) -> Heuristic {
    Heuristic {
        attack_id: Default::default(),
        classification: ExpandingClassification::unrestricted(ce),
        description: "heuristic".into(),
        filetype: Default::default(),
        heur_id: name.clone(),
        name,
        score: Default::default(),
        signature_score_map: Default::default(),
        stats: Default::default(),
        max_score: Default::default(),
    }
}

#[tokio::test]
async fn test_register_service_auth_fail() {
    let (client, core, _guard, address) = setup(headers()).await;

    let service = build_service();
    let service_delta = empty_delta(&service);
    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();
    
    let mut headers = headers();
    headers.insert("X-APIKEY", HeaderValue::from_static("10"));
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());

    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service).send().await.unwrap();
    assert_eq!(result.status().as_u16(), 401);
}

#[tokio::test]
async fn test_register_existing_service() {
    let (client, core, _guard, address) = setup(headers()).await;

    let service = build_service();
    let service_delta = empty_delta(&service);
    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();

    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());

    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service).send().await.unwrap();
    let status = result.status();
    let body = result.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200);

    let body: APIResponse<RegisterResponse> = serde_json::from_slice(&body).unwrap();
    assert!(body.api_response.keep_alive);
    assert!(body.api_response.new_heuristics.is_empty());
    assert_eq!(body.api_response.service_config, service);
}

#[tokio::test]
async fn test_register_bad_service() {
    let (client, _core, _guard, address) = setup(headers()).await;

    let mut service = build_service();
   
    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());

    service.name = "".to_owned();

    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service).send().await.unwrap();
    assert_eq!(result.status().as_u16(), 400);
}

#[tokio::test]
async fn test_register_new_service() {
    let (client, core, _guard, address) = setup(headers()).await;

    let service = build_service();
    let service_delta = empty_delta(&service);

    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());

    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service).send().await.unwrap();

    let status = result.status();
    let body = result.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{}", String::from_utf8_lossy(&body));

    let body: APIResponse<RegisterResponse> = serde_json::from_slice(&body).unwrap();
    assert!(!body.api_response.keep_alive);
    assert!(body.api_response.new_heuristics.is_empty());
    assert_eq!(body.api_response.service_config, service);

    assert_eq!(core.datastore.service.get(&service.key(), None).await.unwrap().unwrap(), service);
    assert_eq!(serde_json::to_string(&core.datastore.service_delta.get(&service.name, None).await.unwrap()).unwrap(), serde_json::to_string(&service_delta).unwrap());
}

#[tokio::test]
async fn test_register_new_service_version() {
    let (client, core, _guard, address) = setup(headers()).await;

    let mut service = build_service();
    let service_delta = empty_delta(&service);

    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();

    let old_service = service.clone();
    service.version = "101".to_string();

    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());

    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service).send().await.unwrap();

    let status = result.status();
    let body = result.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{}", String::from_utf8_lossy(&body));

    let body: APIResponse<RegisterResponse> = serde_json::from_slice(&body).unwrap();
    assert!(!body.api_response.keep_alive);
    assert!(body.api_response.new_heuristics.is_empty());
    assert_eq!(body.api_response.service_config, old_service);

    assert_eq!(core.datastore.service.get(&old_service.key(), None).await.unwrap().unwrap(), old_service);
    assert_eq!(core.datastore.service.get(&service.key(), None).await.unwrap().unwrap(), service);
    assert_eq!(serde_json::to_string(&core.datastore.service_delta.get(&service.name, None).await.unwrap()).unwrap(), serde_json::to_string(&service_delta).unwrap());
}

#[tokio::test]
async fn test_register_new_heuristics() {
    let (client, core, _guard, address) = setup(headers()).await;

    let service = build_service();
    let service_delta = empty_delta(&service);

    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();

    let serde_json::Value::Object(mut service_request) = serde_json::to_value(&service).unwrap() else { panic!() };
    let new_heuristic = build_heuristic("test-heuristic".to_string(), &core.classification_parser);
    service_request.insert("heuristics".to_string(), serde_json::json!([
        new_heuristic
    ]));
    let heur_id = format!("{}.{}", service.name.to_uppercase(), new_heuristic.heur_id);

    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());


    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service_request).send().await.unwrap();

    let status = result.status();
    let body = result.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{}", String::from_utf8_lossy(&body));

    let body: APIResponse<RegisterResponse> = serde_json::from_slice(&body).unwrap();

    assert!(body.api_response.keep_alive);
    assert_eq!(body.api_response.new_heuristics.len(), 1);
    assert_eq!(body.api_response.new_heuristics[0], heur_id);
    assert_eq!(body.api_response.service_config, service);
}

#[tokio::test]
async fn test_register_existing_heuristics() {
    let (client, core, _guard, address) = setup(headers()).await;

    let service = build_service();
    let service_delta = empty_delta(&service);

    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();

    let serde_json::Value::Object(mut service_request) = serde_json::to_value(&service).unwrap() else { panic!() };
    let mut new_heuristic = build_heuristic("test-heuristic".to_string(), &core.classification_parser);
    service_request.insert("heuristics".to_string(), serde_json::json!([
        new_heuristic
    ]));

    // add the heuristic to the database in the form it will be there before restoring the service form of it
    let base_id = new_heuristic.heur_id.clone();
    let heur_id = format!("{}.{}", service.name.to_uppercase(), new_heuristic.heur_id);
    new_heuristic.heur_id = heur_id.clone();
    core.datastore.heuristic.save(&heur_id, &new_heuristic, None, None).await.unwrap();
    new_heuristic.heur_id = base_id;

    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());


    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service_request).send().await.unwrap();

    let status = result.status();
    let body = result.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{}", String::from_utf8_lossy(&body));

    let body: APIResponse<RegisterResponse> = serde_json::from_slice(&body).unwrap();

    assert!(body.api_response.keep_alive);
    assert_eq!(body.api_response.new_heuristics.len(), 0);
    assert_eq!(body.api_response.service_config, service);
}

#[tokio::test]
async fn test_register_bad_heuristics() {
    let (client, core, _guard, address) = setup(headers()).await;

    let service = build_service();
    let service_delta = empty_delta(&service);

    core.datastore.service.save(&service.key(), &service, None, None).await.unwrap();
    core.datastore.service_delta.save(&service.name, &service_delta, None, None).await.unwrap();

    let serde_json::Value::Object(mut service_request) = serde_json::to_value(&service).unwrap() else { panic!() };
    let new_heuristic = build_heuristic("test-heuristic".to_string(), &core.classification_parser);
    service_request.insert("heuristics".to_string(), serde_json::json!([
        new_heuristic
    ]));
    service_request.get_mut("heuristics").unwrap().as_array_mut().unwrap().get_mut(0).unwrap().as_object_mut().unwrap().insert("description".to_string(), serde_json::Value::Null);

    let mut headers = headers();
    headers.insert("Service-Name", HeaderValue::from_str(&service.name).unwrap());
    headers.insert("Service-Version", HeaderValue::from_str(&service.version).unwrap());

    let result = client.post(format!("{address}/api/v1/service/register/")).headers(headers).json(&service_request).send().await.unwrap();

    let status = result.status();
    assert_eq!(status.as_u16(), 400);
}