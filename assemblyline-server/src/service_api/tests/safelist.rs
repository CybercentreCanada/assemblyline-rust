
// import pytest

// from unittest.mock import MagicMock, patch

// from assemblyline.odm import randomizer
// from assemblyline.odm.models.safelist import Safelist
// from assemblyline_service_server import app
// from assemblyline_service_server.config import AUTH_KEY

use super::{setup, AUTH_KEY, random_hash};

fn headers() -> http::HeaderMap {
    [
        ("Container-Id", random_hash(12)),
        ("X-APIKey", AUTH_KEY.to_owned()),
        ("Service-Name", "Badlist".to_owned()),
        ("Service-Version", random_hash(2)),
        ("Service-Tool-Version", random_hash(64)),
        ("Timeout", "1".to_owned()),
        ("X-Forwarded-For", "127.0.0.1".to_owned()),
    ].into_iter()
    .map(|(name, value)|(reqwest::header::HeaderName::from_static(name), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}


// @pytest.fixture(scope='function')
// def storage():
//     ds = MagicMock()
//     with patch('assemblyline_service_server.config.SAFELIST_CLIENT.datastore', ds):
//         yield ds


// @pytest.fixture()
// def client():
//     client = app.app.test_client()
//     yield client


#[tokio::test]
async fn test_safelist_exist() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // valid_hash = randomizer.get_random_hash(64)
    // valid_resp = randomizer.random_model_obj(Safelist, as_json=True)
    // valid_resp['hashes']['sha256'] = valid_hash
    // storage.safelist.get_if_exists.return_value = valid_resp

    // resp = client.get(f'/api/v1/safelist/{valid_hash}/', headers=headers)
    // assert resp.status_code == 200
    // assert resp.json['api_response'] == valid_resp
}

#[tokio::test]
async fn test_safelist_missing() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // invalid_hash = randomizer.get_random_hash(64)
    // storage.safelist.get_if_exists.return_value = None

    // resp = client.get(f'/api/v1/safelist/{invalid_hash}/', headers=headers)
    // assert resp.status_code == 404
    // assert resp.json['api_response'] is None
}

#[tokio::test]
async fn test_get_full_safelist() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // storage.safelist.stream_search.return_value = []

    // resp = client.get('/api/v1/safelist/', headers=headers)
    // assert resp.status_code == 200
    // assert 'match' in resp.json['api_response']
    // assert 'regex' in resp.json['api_response']
    // assert isinstance(resp.json['api_response']['match'], dict)
    // assert isinstance(resp.json['api_response']['regex'], dict)
}

#[tokio::test]
async fn test_get_full_safelist_specific() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // storage.safelist.stream_search.return_value = []

    // tag_type = "network.dynamic.domain"
    // resp = client.get(f'/api/v1/safelist/?tag_types={tag_type}', headers=headers)
    // assert resp.status_code == 200
    // assert 'match' in resp.json['api_response']
    // assert 'regex' in resp.json['api_response']
    // assert isinstance(resp.json['api_response']['match'], dict)
    // assert isinstance(resp.json['api_response']['regex'], dict)

    // for k in resp.json['api_response']['match']:
    //     assert k == tag_type

    // for k in resp.json['api_response']['regex']:
    //     assert k == tag_type
}

#[tokio::test]
async fn test_get_signature_safelist() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // storage.safelist.stream_search.return_value = [{"signature": {"name": "test"}}]

    // resp = client.get('/api/v1/safelist/signatures/', headers=headers)
    // assert resp.status_code == 200
    // assert isinstance(resp.json['api_response'], list)
    // assert resp.json['api_response'] == ['test']
}