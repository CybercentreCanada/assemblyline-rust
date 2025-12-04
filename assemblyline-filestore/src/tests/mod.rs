
use std::sync::Arc;

use bytes::Bytes;

mod ftp_server;
mod sftp_server;

use crate::FileStore;
use crate::tests::ftp_server::{random_tls_certificate, start_temp_ftp_server};
use crate::tests::sftp_server::start_temp_sftp_server;

const TEMP_BODY_A: &[u8] = b"temporary file string";

fn init() {
    let _ = env_logger::builder()
        .filter_module("suppaftp", log::LevelFilter::Warn)
        .filter_module("libunftp", log::LevelFilter::Warn)
        .filter_level(log::LevelFilter::Debug)
        .is_test(true).try_init();
}

/// test Azure filestore by downloading a file from our public storage blob
#[tokio::test]
async fn test_azure() {
    init();
    let fs = FileStore::with_limit_retries("azure://alpytest.blob.core.windows.net/pytest/").await.unwrap();
    println!("{fs:?}");

    assert!(fs.get("__missing_file__").await.unwrap().is_none());
    assert!(fs.exists("test").await.unwrap());
    assert!(fs.get("test").await.unwrap().is_some());
    assert!(fs.put("bob", &Bytes::copy_from_slice(b"bob")).await.is_err());
}

/// test Azure filestore against the local emulator container
#[tokio::test(flavor = "multi_thread")]
async fn test_azure_emulator() {
    init();
    let fs = FileStore::with_limit_retries("azure://localhost/?emulator=true&allow_directory_access=true").await.unwrap();
    println!("{fs:?}");

    common_actions(fs).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sftp() {
    init();
    let server = start_temp_sftp_server().await;
    let fs = FileStore::with_limit_retries(&server).await.unwrap();
    common_actions(fs).await;
}

/// Run some operations against an in-process ftp server
#[tokio::test(flavor = "multi_thread")]
async fn ftp() {
    init();
    let temp_ftp_server = start_temp_ftp_server(None).await;
    let fs = FileStore::with_limit_retries(&format!("ftp://{temp_ftp_server}")).await.unwrap();
    common_actions(fs).await;
}

/// Run some operations against an in-process ftp server
#[tokio::test(flavor = "multi_thread")]
async fn ftps() {
    init();
    let certs = random_tls_certificate().unwrap();
    let temp_ftps_server = start_temp_ftp_server(Some(certs)).await;
    let fs = FileStore::with_limit_retries(&format!("ftps://{temp_ftps_server}")).await.unwrap();
    common_actions(fs).await;
}

/// Run many parallel operations against an in-process ftp server
#[tokio::test(flavor = "multi_thread")]
async fn ftp_parallel() {
    init();
    let temp_ftp_server = start_temp_ftp_server(None).await;
    let fs = FileStore::open(&[format!("ftp://{temp_ftp_server}")]).await.unwrap();
    parallel_activity(fs).await;
}

/// Run many parallel operations against an in-process ftp server
#[tokio::test(flavor = "multi_thread")]
async fn ftps_parallel() {
    init();
    let certs = random_tls_certificate().unwrap();
    let temp_ftps_server = start_temp_ftp_server(Some(certs)).await;
    let fs = FileStore::open(&[format!("ftps://{temp_ftps_server}")]).await.unwrap();
    parallel_activity(fs).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_file() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!("file://{}", directory.path().to_string_lossy());
    println!("{url}");
    let fs = FileStore::with_limit_retries(&url).await.unwrap();
    common_actions(fs).await;
}

/// Test S3 FileStore using Minio by pushing and fetching back content from it.
#[tokio::test(flavor = "multi_thread")]
async fn test_s3() {
    let content = Bytes::copy_from_slice(b"THIS IS A MINIO TEST");

    let fs = FileStore::with_limit_retries("s3://al_storage_key:Ch@ngeTh!sPa33w0rd@localhost:9000/?s3_bucket=test&use_ssl=False").await.unwrap();
    assert!(fs.delete("al4_minio_pytest.txt").await.is_ok());
    assert!(fs.put("al4_minio_pytest.txt", &content).await.is_ok());
    assert!(fs.exists("al4_minio_pytest.txt").await.unwrap());
    assert_eq!(fs.get("al4_minio_pytest.txt").await.unwrap().unwrap(), content);
    assert!(fs.delete("al4_minio_pytest.txt").await.is_ok());
    common_actions(fs).await;
}

async fn common_actions(fs: Arc<FileStore>) {
    let temp_dir = tempfile::tempdir().unwrap();

    // Make sure a missing file returns None
    assert!(!fs.exists("__missing_file__").await.unwrap());
    assert!(fs.get("__missing_file__").await.unwrap().is_none());
    assert!(fs.download("__missing_file__", &temp_dir.path().join("local_copy")).await.is_err());
    assert!(fs.stream("__missing_file__").await.is_err());
    assert!(fs.upload(&temp_dir.path().join("__missing_file__"), "not-to-be-created").await.is_err());
    assert!(!fs.exists("not-to-be-created").await.unwrap());
    fs.delete("__missing_file__").await.unwrap();

    // Write and read file body directly
    let temp_body_a = Bytes::copy_from_slice(TEMP_BODY_A);
    assert!(fs.put("put", &temp_body_a).await.is_ok());
    assert_eq!(fs.get("put").await.unwrap().unwrap(), TEMP_BODY_A);

    // Write a file body by batch upload
    {
        let temp_file_a = temp_dir.path().join("a");
        tokio::fs::write(&temp_file_a, TEMP_BODY_A).await.unwrap();

        let temp_file_b = temp_dir.path().join("b");
        tokio::fs::write(&temp_file_b, TEMP_BODY_A).await.unwrap();

        let failures = fs.upload_batch(&[
            (&temp_file_a, "upload/a"),
            (&temp_file_b, "upload/b")
        ]).await;
        assert!(failures.is_empty(), "{failures:?}");
        assert!(fs.exists("upload/a").await.unwrap());
        assert!(fs.exists("upload/b").await.unwrap());

        // Read a file body by download
        let temp_file_name = temp_dir.path().join("scratch");
        assert!(!temp_file_name.exists());
        fs.download("upload/b", &temp_file_name).await.unwrap();
        assert_eq!(tokio::fs::read(temp_file_name).await.unwrap(), TEMP_BODY_A);
    }

    assert!(fs.exists("put").await.unwrap());
    fs.delete("put").await.unwrap();
    fs.delete("put").await.unwrap();
    assert!(!fs.exists("put").await.unwrap());

    // Try to create a file above the root of the file store, but see that it lands at the root instead
    fs.put("../illigal-file", &temp_body_a).await.unwrap();
    assert!(fs.exists("illigal-file").await.unwrap());

    // if check_listing {
    // let hello = Bytes::copy_from_slice("hello");
    // fs.put(&"0".repeat(64), hello).await.unwrap();
    // fs.put("0" + "1" * 63, hello).await.unwrap();
    // fs.put("01" + "2" * 62, hello).await.unwrap();
    // fs.put("012" + "3" * 61, hello).await.unwrap();
    // fs.put("0123" + "4" * 60, hello).await.unwrap();
    // fs.put("01-file", hello).await.unwrap();
    // fs.put("012-file", hello).await.unwrap();
    // fs.put("0123-file", hello).await.unwrap();
    // fs.put("01234-file", hello).await.unwrap();

    //     assert len(set(fs.transports[0].list("0"))) == 9
    //     assert len(set(fs.transports[0].list("01"))) == 8
    //     assert len(set(fs.transports[0].list("012"))) == 6
    //     assert len(set(fs.transports[0].list("0123"))) == 4
    //     assert set(fs.transports[0].list("01234")) == {"01234-file", "0123" + "4" * 60}
    // }
}


async fn parallel_activity(fs: Arc<FileStore>) {

    let mut file_names = vec![];
    for _ in 0..1000 {
        file_names.push(uuid::Uuid::new_v4().to_string());
    }
    let file_names = Arc::new(file_names);

    let mut handles = vec![];
    for _ in 0..10 {
        let file_names = file_names.clone();
        let fs = fs.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..1_000 {
                let index = rand::random_range(0..file_names.len());
                fs.put(&file_names[index], &Bytes::copy_from_slice(file_names[index].as_bytes())).await.unwrap()
            }
        }))
    }
    for _ in 0..10 {
        let file_names = file_names.clone();
        let fs = fs.clone();
        handles.push(tokio::spawn(async move {
            'outer: for _ in 0..1_000 {
                let index = rand::random_range(0..file_names.len());
                for _ in 0..3 {
                    if let Some(data) = fs.get(&file_names[index]).await.unwrap() { 
                        if data == Bytes::copy_from_slice(file_names[index].as_bytes()) {
                            continue 'outer
                        }
                    } else {
                        continue 'outer
                    }
                }
                panic!()
            }
        }))
    }

    for handle in handles {
        handle.await.unwrap();
        println!("finish");
    }
}