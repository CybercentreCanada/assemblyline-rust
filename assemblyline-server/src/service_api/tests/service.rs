// from unittest.mock import patch, MagicMock

// import pytest
// from assemblyline.odm.models.heuristic import Heuristic

// from assemblyline.odm.randomizer import random_minimal_obj

// from assemblyline.odm.models.service import Service

// from assemblyline.odm import randomizer
// from assemblyline_service_server.config import AUTH_KEY
// from assemblyline_service_server import app
// from assemblyline_core.tasking_client import TaskingClient

use super::{setup, AUTH_KEY, random_hash};

fn headers() -> http::HeaderMap {
    [
        ("Container-Id", random_hash(12)),
        ("X-APIKey", AUTH_KEY.to_owned()),
        ("Service-Tool-Version", random_hash(64)),
        ("X-Forwarded-For", "127.0.0.1".to_owned()),
    ].into_iter()
    .map(|(name, value)|(reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}


// @pytest.fixture()
// def client():
//     client = app.app.test_client()
//     yield client


// @pytest.fixture(scope='function')
// def storage():
//     ds = MagicMock()
//     with patch('assemblyline_service_server.config.TASKING_CLIENT.datastore', ds):
//         yield ds


#[tokio::test]
async fn test_register_service_auth_fail() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // config_block = 'A CONFIG OBJECT FOR SURE'
    // storage.get_service_with_delta.return_value = config_block
    // service = random_minimal_obj(Service)

    // _headers = dict(headers)
    // _headers['X-APIKEY'] = '10'
    // _headers['Service-Name'] = service.name
    // _headers['Service-Version'] = service.version

    // result = client.post("/api/v1/service/register/", headers=_headers, json=service.as_primitives())
    // assert result.status_code == 401
}

#[tokio::test]
async fn test_register_existing_service() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // config_block = 'A CONFIG OBJECT FOR SURE'
    // storage.get_service_with_delta.return_value = config_block
    // service = random_minimal_obj(Service)

    // headers['Service-Name'] = service.name
    // headers['Service-Version'] = service.version

    // result = client.post("/api/v1/service/register/", headers=headers, json=service.as_primitives())
    // assert result.status_code == 200
    // assert storage.heuristic.save.call_count == 0

    // assert result.json['api_response']['keep_alive'] is True
    // assert len(result.json['api_response']['new_heuristics']) == 0
    // assert result.json['api_response']['service_config'] == config_block
}

#[tokio::test]
async fn test_register_bad_service() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
//     service = random_minimal_obj(Service).as_primitives()

//     headers['Service-Name'] = service['name']
//     headers['Service-Version'] = service['version']
//     del service['name']

//     result = client.post("/api/v1/service/register/", headers=headers, json=service)
//     assert result.status_code == 400
}

#[tokio::test]
async fn test_register_new_service() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // config_block = 'A CONFIG OBJECT FOR SURE'
    // storage.get_service_with_delta.return_value = config_block
    // storage.service.exists.return_value = False
    // storage.service_delta.exists.return_value = False

    // service = random_minimal_obj(Service)

    // headers['Service-Name'] = service.name
    // headers['Service-Version'] = service.version

    // result = client.post("/api/v1/service/register/", headers=headers, json=service.as_primitives())
    // assert result.status_code == 200
    // assert storage.heuristic.save.call_count == 0
    // assert storage.service.save.call_count == 1
    // assert storage.service_delta.save.call_count == 1

    // assert result.json['api_response']['keep_alive'] is False
    // assert len(result.json['api_response']['new_heuristics']) == 0
    // assert result.json['api_response']['service_config'] == config_block
}

#[tokio::test]
async fn test_register_new_service_version() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // config_block = 'A CONFIG OBJECT FOR SURE'
    // storage.get_service_with_delta.return_value = config_block
    // storage.service_delta.exists.return_value = False

    // service = random_minimal_obj(Service)

    // headers['Service-Name'] = service.name
    // headers['Service-Version'] = service.version

    // result = client.post("/api/v1/service/register/", headers=headers, json=service.as_primitives())
    // assert result.status_code == 200
    // assert storage.heuristic.save.call_count == 0
    // assert storage.service.save.call_count == 0
    // assert storage.service_delta.save.call_count == 1

    // assert result.json['api_response']['keep_alive'] is True
    // assert len(result.json['api_response']['new_heuristics']) == 0
    // assert result.json['api_response']['service_config'] == config_block
}

#[tokio::test]
async fn test_register_new_heuristics() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // config_block = 'A CONFIG OBJECT FOR SURE'
    // storage.get_service_with_delta.return_value = config_block
    // storage.heuristic.get_if_exists.return_value = None

    // service = random_minimal_obj(Service)
    // service = service.as_primitives()
    // service['heuristics'] = [random_minimal_obj(Heuristic).as_primitives()]

    // storage.heuristic.bulk.return_value = {"items": [{"update": {"result": "created",
    //                                                              "_id": service['heuristics'][0]['heur_id']}}]}

    // headers['Service-Name'] = service['name']
    // headers['Service-Version'] = service['version']

    // result = client.post("/api/v1/service/register/", headers=headers, json=service)
    // assert result.status_code == 200
    // assert storage.heuristic.bulk.call_count == 1

    // assert result.json['api_response']['keep_alive'] is True
    // assert len(result.json['api_response']['new_heuristics']) == 1
    // assert service['heuristics'][0]['heur_id'] in result.json['api_response']['new_heuristics'][0]
    // assert result.json['api_response']['service_config'] == config_block
}

#[tokio::test]
async fn test_register_existing_heuristics() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // config_block = 'A CONFIG OBJECT FOR SURE'
    // storage.get_service_with_delta.return_value = config_block

    // service = random_minimal_obj(Service)
    // service = service.as_primitives()
    // service['heuristics'] = [random_minimal_obj(Heuristic).as_primitives()]

    // headers['Service-Name'] = service['name']
    // headers['Service-Version'] = service['version']

    // result = client.post("/api/v1/service/register/", headers=headers, json=service)
    // assert result.status_code == 200
    // assert storage.heuristic.save.call_count == 0

    // assert result.json['api_response']['keep_alive'] is True
    // assert len(result.json['api_response']['new_heuristics']) == 0
    // assert result.json['api_response']['service_config'] == config_block
}

#[tokio::test]
async fn test_register_bad_heuristics() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // service = random_minimal_obj(Service)
    // service = service.as_primitives()
    // service['heuristics'] = [random_minimal_obj(Heuristic).as_primitives()]
    // service['heuristics'][0]['description'] = None

    // headers['Service-Name'] = service['name']
    // headers['Service-Version'] = service['version']

    // result = client.post("/api/v1/service/register/", headers=headers, json=service)
    // assert result.status_code == 400
}