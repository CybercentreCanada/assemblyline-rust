use std::sync::Arc;

use assemblyline_models::datastore::File;
use bytes::Bytes;
use rand::thread_rng;
use reqwest::header::HeaderMap;

use crate::common::sha256_data;
use crate::Core;

use super::{setup, AUTH_KEY, random_hash};

fn headers() -> HeaderMap {
    [
        ("Container-Id", random_hash(12)),
        ("X-APIKey", AUTH_KEY.to_owned()),
        ("Service-Name", "Badlist".to_owned()),
        ("Service-Version", random_hash(2)),
        ("Service-Tool-Version", random_hash(64)),
        ("X-Forwarded-For", "127.0.0.1".to_owned()),
    ].into_iter()
    .map(|(name, value)|(reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(), reqwest::header::HeaderValue::from_str(&value).unwrap()))
    .collect()
}



async fn setup_file(core: &Arc<Core>, body: &[u8]) -> String {
    let sha = sha256_data(body);
    core.filestore.put(&sha, &Bytes::copy_from_slice(body)).await.unwrap();

    core.datastore.file.save(&sha, &File::gen_for_sample(body, &mut thread_rng()), None, None).await.unwrap();
    sha
}

#[tokio::test]
async fn test_download_file() {
    let (client, core, _guard, address) = setup(headers()).await;

    // Put the file in place
    let file_size = 12345;
    let hash = setup_file(&core, &b"x".repeat(file_size)).await;

    // check that we can fetch it
    let response = client.get(format!("{address}/api/v1/file/{hash}/")).send().await.unwrap();
    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(response.bytes().await.unwrap(), (b"x".repeat(file_size)));

    core.filestore.delete(&hash).await.unwrap();

    // Try getting it again where the datastore thinks its there but its missing from the filestore
    let response = client.get(format!("{address}/api/v1/file/{hash}/")).send().await.unwrap();
    assert_eq!(response.status().as_u16(), 404);

    // Have the datastore say it doesn"t exist
    assert!(core.datastore.file.delete(&hash, None).await.unwrap());
    let response = client.get(format!("{address}/api/v1/file/{hash}/")).send().await.unwrap();
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn test_upload_new_file() {
    let (client, core, _guard, address) = setup(headers()).await;

    let file_size = 10003;
    let file_data = b"x".repeat(file_size);
    let file_hash = sha256_data(&file_data);

    use reqwest::header::HeaderValue;
    let mut file_headers = headers();
    file_headers.insert("sha256", HeaderValue::from_str(&file_hash).unwrap());
    file_headers.insert("classification", HeaderValue::from_str(core.classification_parser.unrestricted()).unwrap());
    file_headers.insert("ttl", HeaderValue::from_static("1"));
    file_headers.insert("Content-Type", HeaderValue::from_static("application/octet-stream"));

    let response = client.put(format!("{address}/api/v1/file/")).headers(file_headers).body(file_data).send().await.unwrap();
    let status = response.status();
    let body = response.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{status} - {}", String::from_utf8_lossy(&body));
    assert!(core.filestore.exists(&file_hash).await.unwrap());
    assert!(core.datastore.file.exists(&file_hash, None).await.unwrap());
}

#[tokio::test]
async fn test_upload_section_image() {
    let (client, core, _guard, address) = setup(headers()).await;

    let file_size = 10003;
    let file_data = b"x".repeat(file_size);
    let file_hash = sha256_data(&file_data);

    use reqwest::header::HeaderValue;
    let mut file_headers = headers();
    file_headers.insert("sha256", HeaderValue::from_str(&file_hash).unwrap());
    file_headers.insert("classification", HeaderValue::from_str(core.classification_parser.unrestricted()).unwrap());
    file_headers.insert("ttl", HeaderValue::from_static("1"));
    file_headers.insert("Content-Type", HeaderValue::from_static("application/octet-stream"));
    file_headers.insert("Is-Section-Image", HeaderValue::from_static("true"));

    let response = client.put(format!("{address}/api/v1/file/")).headers(file_headers).body(file_data).send().await.unwrap();
    let status = response.status();
    let body = response.bytes().await.unwrap();
    assert_eq!(status.as_u16(), 200, "{status} - {}", String::from_utf8_lossy(&body));
    assert!(core.filestore.exists(&file_hash).await.unwrap());
    assert!(core.datastore.file.get(&file_hash, None).await.unwrap().unwrap().is_section_image);
}

#[tokio::test]
async fn test_upload_file_bad_hash() {
    let (client, core, _guard, address) = setup(headers()).await;

    let file_size = 10003;
    let file_data = b"x".repeat(file_size);
    let file_hash = sha256_data(&file_data);
    let bad_hash = "0000".to_string() + &file_hash[4..];

    use reqwest::header::HeaderValue;
    let mut file_headers = headers();
    file_headers.insert("sha256", HeaderValue::from_str(&bad_hash).unwrap());
    file_headers.insert("classification", HeaderValue::from_str(core.classification_parser.unrestricted()).unwrap());
    file_headers.insert("ttl", HeaderValue::from_static("1"));
    file_headers.insert("Content-Type", HeaderValue::from_static("application/octet-stream"));

    let response = client.put(format!("{address}/api/v1/file/")).headers(file_headers).body(file_data).send().await.unwrap();
    let status = response.status();
    
    assert!(status.is_client_error());
    assert!(!core.filestore.exists(&file_hash).await.unwrap());
    assert!(!core.filestore.exists(&bad_hash).await.unwrap());
    assert!(!core.datastore.file.exists(&file_hash, None).await.unwrap());
    assert!(!core.datastore.file.exists(&bad_hash, None).await.unwrap());
}