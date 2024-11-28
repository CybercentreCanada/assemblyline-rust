

use std::time::Duration;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::badlist::{BadhashTypes, Badlist, Hashes, Tag};
use assemblyline_models::{ExpandingClassification, JsonMap};
use chrono::Utc;
use log::info;
use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::service_api::helpers::APIResponse;

use super::{setup, random_hash, AUTH_KEY};

// import pytest

// from unittest.mock import MagicMock, patch

// from assemblyline.odm import randomizer
// from assemblyline.odm.models.badlist import Badlist
// from assemblyline_service_server import app
// from assemblyline_service_server.config import AUTH_KEY


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
    .map(|(name, value)|(reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}

// @pytest.fixture(scope='function')
// def storage():
//     ds = MagicMock()
//     with patch('assemblyline_service_server.config.BADLIST_CLIENT.datastore', ds):
//         yield ds

fn make_badlist(hash: &str, ce: &ClassificationParser) -> Badlist {
    Badlist { 
        added: Utc::now(), 
        attribution: None, 
        classification: ExpandingClassification::new(ce.unrestricted().to_owned(), ce).unwrap(), 
        enabled: true, 
        expiry_ts: None, 
        hashes: Hashes {
            sha256: Some(hash.parse().unwrap()),
            ..Default::default()
        }, 
        file: None, 
        sources: vec![], 
        tag: None, 
        hash_type: BadhashTypes::Tag,
        updated: Utc::now() 
    }
}


#[tokio::test]
async fn test_badlist_exist() {
    let (client, core, _guard, address) = setup(headers()).await;

    let valid_hash = random_hash(64);
    let valid_resp = make_badlist(&valid_hash, &core.classification_parser);
    core.datastore.badlist.save(&valid_hash, &valid_resp, None, None).await.unwrap();

    let resp = client.get(format!("{address}/api/v1/badlist/{valid_hash}/")).send().await.unwrap();
    assert!(resp.status().is_success());
    let body = resp.bytes().await.unwrap();
    assert_eq!(serde_json::from_slice::<APIResponse<Badlist>>(&body).unwrap().api_response, valid_resp);
}

#[tokio::test]
async fn test_badlist_missing() {
    let (client, core, _guard, address) = setup(headers()).await;
    let invalid_hash = random_hash(64);
    // storage.badlist.get_if_exists.return_value = None

    let resp = client.get(format!("{address}/api/v1/badlist/{invalid_hash}/")).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let body = resp.bytes().await.unwrap();
    assert!(serde_json::from_slice::<APIResponse<Option<Badlist>>>(&body).unwrap().api_response.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_badlist_tags_exists() {
    let (client, core, _guard, address) = setup(headers()).await;

    const TAG_TYPE: &str = "network.dynamic.domain";
    const TAG_VALUES: [&str; 2] = ["cse-cst.gc.ca", "cyber.gc.ca"];

    let mut items = vec![];
    while items.len() < 12 {
        let hash = random_hash(64);
        let mut item = make_badlist(&hash, &core.classification_parser);
        item.tag = Some(Tag { tag_type: TAG_TYPE.to_owned(), value: TAG_VALUES.choose(&mut thread_rng()).unwrap().to_string() });
        core.datastore.badlist.save(&hash, &item, None, None).await.unwrap();
        items.push(item);
    }

    let data = serde_json::json!({TAG_TYPE: TAG_VALUES});
    let url = format!("{address}/api/v1/badlist/tags/");
    // for _ in 0..3 {
    //     tokio::time::sleep(Duration::from_secs(30)).await;
    //     info!("holding...");
    // }
    info!("requesting {url}");
    let resp = tokio::time::timeout(Duration::from_secs(10), client.post(url).json(&data).send()).await.unwrap().unwrap();
    info!("verifying results");
    assert!(resp.status().is_success());
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<Vec<Badlist>> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response.len(), 12);

    for item in body.api_response {
        assert!(items.contains(&item));
    }
}

#[tokio::test]
async fn test_badlist_similar_tlsh() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // response_item = randomizer.random_model_obj(Badlist, as_json=True)
    // valid_resp = {'items': [response_item]}
    // storage.badlist.search.return_value = valid_resp

    // data = {"tlsh": "TLSH_HASH_FAKE"}
    // resp = client.post('/api/v1/badlist/tlsh/', headers=headers, json=data)
    // assert resp.status_code == 200
    // assert isinstance(resp.json['api_response'], list)

    // for item in resp.json['api_response']:
    //     assert item == response_item
}

#[tokio::test]
async fn test_badlist_similar_ssdeep() {
    let (client, core, _guard, address) = setup(headers()).await;
    todo!();
    // response_item = randomizer.random_model_obj(Badlist, as_json=True)
    // valid_resp = {'items': [response_item]}
    // storage.badlist.search.return_value = valid_resp

    // data = {"ssdeep": "0:fake:ssdeep"}
    // resp = client.post('/api/v1/badlist/ssdeep/', headers=headers, json=data)
    // assert resp.status_code == 200
    // assert isinstance(resp.json['api_response'], list)

    // for item in resp.json['api_response']:
    //     assert item == response_item
}