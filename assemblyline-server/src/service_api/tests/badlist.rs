
use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::badlist::{BadhashTypes, Badlist, Hashes, Source, SourceTypes, Tag};
use assemblyline_models::{ClassificationString, ExpandingClassification};
use chrono::Utc;
use log::info;
use rand::seq::IndexedRandom;

use crate::service_api::helpers::badlist::{BadlistClient, CommonRequestBadlist, RequestBadlist};
use crate::service_api::helpers::APIResponse;

use super::{setup, random_hash, AUTH_KEY};


fn headers() -> reqwest::header::HeaderMap {
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

fn make_sha256_badlist(hash: &str, ce: &ClassificationParser) -> Badlist {
    make_badlist(Hashes {
        sha256: Some(hash.parse().unwrap()),
        ..Default::default()
    }, ce)
}

fn make_tlsh_badlist(hash: &str, ce: &ClassificationParser) -> Badlist {
    make_badlist(Hashes {
        tlsh: Some(hash.parse().unwrap()),
        ..Default::default()
    }, ce)
}

fn make_ssdeep_badlist(hash: &str, ce: &ClassificationParser) -> Badlist {
    make_badlist(Hashes {
        ssdeep: Some(hash.parse().unwrap()),
        ..Default::default()
    }, ce)
}

fn make_badlist(hashes: Hashes, ce: &ClassificationParser) -> Badlist {
    Badlist { 
        added: Utc::now(), 
        attribution: None, 
        classification: ExpandingClassification::unrestricted(ce), 
        enabled: true, 
        expiry_ts: None, 
        hashes, 
        file: None, 
        sources: vec![
            Source { classification: ClassificationString::unrestricted(ce), name: "test".to_owned(), reason: vec![], source_type: SourceTypes::User }
        ], 
        tag: None, 
        hash_type: BadhashTypes::Tag,
        updated: Utc::now() 
    }
}


fn into_request(value: Badlist) -> RequestBadlist {
    let body = CommonRequestBadlist { 
        classification: Some(value.classification.classification), 
        enabled: value.enabled, 
        dtl: Some(0), 
        attribution: value.attribution, 
        sources: value.sources 
    };

    match value.hash_type {
        BadhashTypes::File => RequestBadlist::File { 
            body, 
            file: value.file.unwrap(), 
            hashes: value.hashes 
        },
        BadhashTypes::Tag => RequestBadlist::Tag { body, tag: value.tag.unwrap() }
    }
}

// #[test]
// fn test_badlist_exist() {    
//     let runtime = tokio::runtime::Runtime::new().unwrap();
//     let _guard = runtime.block_on(async {
//         let (client, core, (_guard, _server), address) = setup(headers()).await;

//         let valid_hash = random_hash(64);
//         let valid_resp = make_sha256_badlist(&valid_hash, &core.classification_parser);
//         core.datastore.badlist.save(&valid_hash, &valid_resp, None, None).await.unwrap();

//         let url = format!("{address}/api/v1/badlist/{valid_hash}/");
//         let resp = client.get(url).send().await.unwrap();
//         let status = resp.status();
//         let body = resp.bytes().await.unwrap();
//         assert!(status.is_success(), "{:?} {:?}", status, body);
//         assert_eq!(serde_json::from_slice::<APIResponse<Badlist>>(&body).unwrap().api_response, valid_resp);


//         _guard.running.set(false);
//         _ = _server.await;
//         _guard
//     });
//     runtime.block_on(_guard.cleanup()).unwrap();
// }


#[tokio::test(flavor = "multi_thread")]
async fn test_badlist_exist() {    
    let (client, core, _guard, address) = setup(headers()).await;

    let valid_hash = random_hash(64);
    let valid_resp = make_sha256_badlist(&valid_hash, &core.classification_parser);
    core.datastore.badlist.save(&valid_hash, &valid_resp, None, None).await.unwrap();

    let url = format!("{address}/api/v1/badlist/{valid_hash}/");
    let resp = client.get(url).send().await.unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    assert!(status.is_success(), "{status:?} {body:?}");
    assert_eq!(serde_json::from_slice::<APIResponse<Badlist>>(&body).unwrap().api_response, valid_resp);
}

#[tokio::test]
async fn test_badlist_missing() {
    let (client, _core, _guard, address) = setup(headers()).await;
    let invalid_hash = random_hash(64);
    // storage.badlist.get_if_exists.return_value = None

    let resp = client.get(format!("{address}/api/v1/badlist/{invalid_hash}/")).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let body = resp.bytes().await.unwrap();
    assert!(serde_json::from_slice::<APIResponse<Option<Badlist>>>(&body).unwrap().api_response.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_badlist_exists_tags() {
    let (client, core, _guard, address) = setup(headers()).await;

    const TAG_TYPE: &str = "network.dynamic.domain";
    const TAG_VALUES: [&str; 2] = ["cse-cst.gc.ca", "cyber.gc.ca"];
    let badlist_client = BadlistClient::new(core.datastore.clone(), core.config.clone(), core.classification_parser.clone());

    let mut items = vec![];
    while items.len() < 12 {
        let hash = random_hash(64);
        let mut item = make_sha256_badlist(&hash, &core.classification_parser);
        item.tag = Some(Tag { tag_type: TAG_TYPE.to_owned(), value: TAG_VALUES.choose(&mut rand::rng()).unwrap().to_string() });
        let (key, _) = badlist_client.add_update(into_request(item),None).await.unwrap();
        items.push(core.datastore.badlist.get(&key, None).await.unwrap().unwrap());
    }
    core.datastore.badlist.commit(None).await.unwrap();

    let data = serde_json::json!({TAG_TYPE: TAG_VALUES});
    let url = format!("{address}/api/v1/badlist/tags/");
    let resp = client.post(url).json(&data).send().await.unwrap();
    assert!(resp.status().is_success());
    let body = resp.bytes().await.unwrap();
    let body: APIResponse<Vec<Badlist>> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response.len(), 2);

    for item in body.api_response {
        assert!(items.contains(&item), "{item:?}");
    }
}

#[tokio::test]
async fn test_badlist_similar_tlsh() {
    let (client, core, _guard, address) = setup(headers()).await;
    
    const SAMPLE_HASH: &str = "T1B6D1C6DBE5187047FA85B6F161A7F04E7D6B6C1FD8C4C250F145E24039973AABB4A01A";
    let item = make_tlsh_badlist(SAMPLE_HASH, &core.classification_parser);
    core.datastore.badlist.save(SAMPLE_HASH, &item, None, None).await.unwrap();    
    core.datastore.badlist.commit(None).await.unwrap();

    let data = serde_json::json!({"tlsh": SAMPLE_HASH});
    let url = format!("{address}/api/v1/badlist/tlsh/");
    let resp = client.post(url).json(&data).send().await.unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    
    assert!(status.is_success(), "{status} -- {}", String::from_utf8_lossy(&body));
    let body: APIResponse<Vec<Badlist>> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response, vec![item.clone()]);
}

#[tokio::test]
async fn test_badlist_similar_ssdeep() {
    let (client, core, _guard, address) = setup(headers()).await;
    
    const SAMPLE_HASH: &str = "192:y1Jz0DoFJuq5f6SgNmq9CndaktcHkk1JYE:yConuqf7gNmHdakKHJ1JYE";
    let hash = SAMPLE_HASH.replace("/", "\\/");
    let item = make_ssdeep_badlist(SAMPLE_HASH, &core.classification_parser);
    info!("Saving hash");
    core.datastore.badlist.save(&hash, &item, None, None).await.unwrap();    
    core.datastore.badlist.commit(None).await.unwrap();

    info!("Do query");
    let data = serde_json::json!({"ssdeep": SAMPLE_HASH});
    let url = format!("{address}/api/v1/badlist/ssdeep/");
    let resp = client.post(url).json(&data).send().await.unwrap();
    let status = resp.status();
    let body = resp.bytes().await.unwrap();
    
    assert!(status.is_success(), "{status} -- {}", String::from_utf8_lossy(&body));
    let body: APIResponse<Vec<Badlist>> = serde_json::from_slice(&body).unwrap();
    assert_eq!(body.api_response, vec![item.clone()]);
}