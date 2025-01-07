
use std::sync::Arc;

use bytes::Bytes;

use crate::FileStore;

// import os
// import tempfile
// import threading
// import traceback

// import pytest

// from assemblyline.filestore.transport.base import TransportException
// from assemblyline.filestore import FileStore

const TEMP_BODY_A: &[u8] = b"temporary file string";

fn init() {
    let _ = env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init();
}

// def _temp_ftp_server(start: threading.Event, stop: threading.Event, user, password, port, secure):
//     try:
//         from pyftpdlib.authorizers import DummyAuthorizer
//         from pyftpdlib.handlers import FTPHandler, TLS_FTPHandler
//         from pyftpdlib.servers import FTPServer

//         with tempfile.TemporaryDirectory() as temp_dir:
//             authorizer = DummyAuthorizer()
//             authorizer.add_user(user, password, temp_dir, perm="elradfmwMT")
//             authorizer.add_anonymous(temp_dir)

//             if secure:
//                 handler = TLS_FTPHandler
//                 handler.certfile = os.path.join(os.path.dirname(__file__), 'key.pem')
//             else:
//                 handler = FTPHandler

//             handler.authorizer = authorizer
//             server = FTPServer(("127.0.0.1", port), handler)
//             while not stop.is_set():
//                 start.set()
//                 server.serve_forever(timeout=1, blocking=False)
//     except Exception:
//         traceback.print_exc()


// @pytest.fixture
// def temp_ftp_server():
//     start = threading.Event()
//     stop = threading.Event()
//     thread = threading.Thread(target=_temp_ftp_server, args=[start, stop, "user", "12345", 21111, False])
//     try:
//         thread.start()
//         start.wait(5)
//         yield 'user:12345@localhost:21111'
//     finally:
//         stop.set()
//         thread.join()


// @pytest.fixture
// def temp_ftps_server():
//     start = threading.Event()
//     stop = threading.Event()
//     thread = threading.Thread(target=_temp_ftp_server, args=[start, stop, "user", "12345", 21112, True])
//     try:
//         thread.start()
//         start.wait(5)
//         yield 'user:12345@localhost:21112'
//     finally:
//         stop.set()
//         thread.join()

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

// #[tokio::test]
// async fn test_azure_list() {
//     init();
//     let fs = FileStore::with_limit_retries("azure://alpytest.blob.core.windows.net/pytest/").await.unwrap();
//     println!("{fs:?}");

//     todo!("list");
//     // assert "test" in list(fs.transports[0].list())
//     // assert list(fs.transports[0].list("abc")) == []
// }

/// test Azure filestore against the local emulator container
#[tokio::test]
async fn test_azure_emulator() {
    init();
    let fs = FileStore::with_limit_retries("azure://localhost/?emulator=true&allow_directory_access=true").await.unwrap();
    println!("{fs:?}");

    common_actions(fs).await
}

// def test_http():
//     """
//     Test HTTP FileStore by fetching the assemblyline page on
//     CSE's cyber center page.
//     """
//     fs = FileStore('http://github.com/CybercentreCanada/')
//     assert 'github.com' in str(fs)
//     httpx_tests(fs)


// def test_https():
//     """
//     Test HTTPS FileStore by fetching the assemblyline page on
//     CSE's cyber center page.
//     """
//     fs = FileStore('https://github.com/CybercentreCanada/')
//     assert 'github.com' in str(fs)
//     httpx_tests(fs)


// def httpx_tests(fs):
//     assert fs.get('__missing_file__') is None
//     assert fs.exists('assemblyline-base') != []
//     assert fs.get('assemblyline-base') is not None
//     with tempfile.TemporaryDirectory() as temp_dir:
//         local_base = os.path.join(temp_dir, 'base')
//         fs.download('assemblyline-base', local_base)
//         assert os.path.exists(local_base)


// def test_sftp():
//     """
//     Test SFTP FileStore by fetching the readme.txt file from
//     Rebex test server.
//     """
//     fs = FileStore('sftp://user:password@localhost:2222')
//     common_actions(fs, check_listing=False)


// def test_ftp(temp_ftp_server):
//     """
//     Run some operations against an in-process ftp server
//     """
//     with FileStore(f'ftp://{temp_ftp_server}') as fs:
//         assert 'localhost' in str(fs)
//         common_actions(fs)


// def test_ftps(temp_ftps_server):
//     """
//     Run some operations against an in-process ftp server
//     """
//     with FileStore(f'ftps://{temp_ftps_server}') as fs:
//         assert 'localhost' in str(fs)
//         common_actions(fs)

/// Test Local FileStore by fetching the README.md file from
/// the assemblyline core repo directory.
///
/// Note: This test will fail if pytest is not ran from the root
///       of the assemblyline core repo.
#[tokio::test]
async fn test_file() {
    // fs = FileStore("file://%s" % os.path.dirname(__file__))
    // assert fs.exists(os.path.basename(__file__)) != []
    // assert fs.get(os.path.basename(__file__)) is not None

    let directory = tempfile::tempdir().unwrap();
    let url = format!("file://{}", directory.path().to_string_lossy());
    println!("{url}");
    let fs = FileStore::with_limit_retries(&url).await.unwrap();
    common_actions(fs).await
}

// def test_s3():
//     """
//     Test S3 FileStore using Minio by pushing and fetching back content from it.
//     """
//     content = b"THIS IS A MINIO TEST"

//     fs = FileStore('s3://al_storage_key:Ch@ngeTh!sPa33w0rd@localhost:9000/?s3_bucket=test&use_ssl=False')
//     assert fs.delete('al4_minio_pytest.txt') is None
//     assert fs.put('al4_minio_pytest.txt', content) != []
//     assert fs.exists('al4_minio_pytest.txt') != []
//     assert fs.get('al4_minio_pytest.txt') == content
//     assert fs.delete('al4_minio_pytest.txt') is None
//     common_actions(fs)


async fn common_actions(fs: Arc<FileStore>) {
    let temp_dir = tempfile::tempdir().unwrap();

    // Make sure a missing file returns None
    assert!(!fs.exists("__missing_file__").await.unwrap());
    assert!(fs.get("__missing_file__").await.unwrap().is_none());
    assert!(fs.download("__missing_file__", &temp_dir.path().join("local_copy")).await.is_err());
    assert!(fs.stream("__missing_file__").await.is_err());
    assert!(fs.upload(&temp_dir.path().join("__missing_file__"), "not-to-be-created").await.is_err());
    assert!(!fs.exists("not-to-be-created").await.unwrap());

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
    assert!(!fs.exists("put").await.unwrap());

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