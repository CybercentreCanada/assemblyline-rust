
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use libunftp::auth::{AuthenticationError, Authenticator, Credentials, DefaultUser};
use unftp_sbe_fs::ServerExt;

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
    let _ = env_logger::builder()
        .filter_module("suppaftp", log::LevelFilter::Warn)
        .filter_module("libunftp", log::LevelFilter::Warn)
        .filter_level(log::LevelFilter::Debug)
        .is_test(true).try_init();
}

pub fn random_tls_certificate() -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    use openssl::{rsa::Rsa, x509::X509, pkey::PKey, asn1::{Asn1Integer, Asn1Time}, bn::BigNum};

    // Generate our keypair
    let key = Rsa::generate(1 << 11)?;
    let pkey = PKey::from_rsa(key)?;

    // Use that keypair to sign a certificate
    let mut builder = X509::builder()?;

    // Set serial number to 1
    let one = BigNum::from_u32(1)?;
    let serial_number = Asn1Integer::from_bn(&one)?;
    builder.set_serial_number(&serial_number)?;
    builder.set_version(2)?;

    // set subject/issuer name
    let mut name = openssl::x509::X509NameBuilder::new()?;
    name.append_entry_by_text("C", "CA")?;
    name.append_entry_by_text("ST", "ON")?;
    name.append_entry_by_text("O", "Inside the house")?;
    name.append_entry_by_text("CN", "localhost")?;
    name.append_entry_by_text("subjectAltName", "DNS:localhost,IP:127.0.0.1")?;
    let name = name.build();
    builder.set_issuer_name(&name)?;
    builder.set_subject_name(&name)?;

    // Set not before/after
    let not_before = Asn1Time::from_unix((chrono::Utc::now() - chrono::Duration::days(1)).timestamp())?;
    builder.set_not_before(&not_before)?;
    let not_after = Asn1Time::from_unix((chrono::Utc::now() + chrono::Duration::days(366)).timestamp())?;
    builder.set_not_after(&not_after)?;

    // set public key
    builder.set_pubkey(&pkey)?;

    // sign and build
    builder.sign(&pkey, openssl::hash::MessageDigest::sha256())?;
    let cert = builder.build();
    println!("{:?}", cert.subject_alt_names());

    Ok((cert.to_pem()?, pkey.rsa()?.private_key_to_pem()?))
}

async fn start_temp_ftp_server(tls_cert: Option<(Vec<u8>, Vec<u8>)>) -> String {

    #[derive(Debug)]
    struct StaticAuth;

    #[async_trait]
    impl Authenticator<DefaultUser> for StaticAuth {
        async fn authenticate(&self, username: &str, creds: &Credentials) -> Result<DefaultUser, AuthenticationError> {
            if username.eq_ignore_ascii_case("bozo") {
                if let Some(password) = &creds.password {
                    if password == "theclown" {
                        return Ok(DefaultUser{})
                    }
                }
            }
            Err(AuthenticationError::BadUser)
        }
    }

    let dir = tempfile::tempdir().unwrap();

    let certs_file = tempfile::NamedTempFile::new().unwrap();
    let key_file = tempfile::NamedTempFile::new().unwrap();
    if let Some((public, private)) = &tls_cert {
        certs_file.as_file().write_all(public).unwrap();
        key_file.as_file().write_all(private).unwrap();
    }

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();

    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let dir_path = dir.path().to_owned();
        // let mut server_builder = libunftp::Server::with_fs(dir_path.clone())
        //     .authenticator(Arc::new(StaticAuth{}))
        //     .passive_ports(50000..=65535);
        // if tls_cert.is_some() {
        //     server_builder = server_builder.ftps(certs_file.path(), key_file.path());
        // }
        // let server = server_builder.build().unwrap();
        // server.listen(addr);

        loop {
            let mut server_builder = libunftp::Server::with_fs(dir_path.clone())
                .authenticator(Arc::new(StaticAuth{}))
                .passive_ports(50000..=65535);
            if tls_cert.is_some() {
                server_builder = server_builder.ftps(certs_file.path(), key_file.path());
            }
            let server = server_builder.build().unwrap();

            let (sock, _) = listener.accept().await.unwrap();
            println!("test server accepted connection");
            tokio::spawn(async move {
                server.service(sock).await.unwrap();                
            });
        }
    });

    return format!("bozo:theclown@127.0.0.1:{}", addr.port())
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

/// test Azure filestore against the local emulator container
#[tokio::test(flavor = "multi_thread")]
async fn test_azure_emulator() {
    init();
    let fs = FileStore::with_limit_retries("azure://localhost/?emulator=true&allow_directory_access=true").await.unwrap();
    println!("{fs:?}");

    common_actions(fs).await;
}

// def test_sftp():
//     """
//     Test SFTP FileStore by fetching the readme.txt file from
//     Rebex test server.
//     """
//     fs = FileStore('sftp://user:password@localhost:2222')
//     common_actions(fs, check_listing=False)

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

/// Test Local FileStore by fetching the README.md file from
/// the assemblyline core repo directory.
///
/// Note: This test will fail if pytest is not ran from the root
///       of the assemblyline core repo.
#[tokio::test(flavor = "multi_thread")]
async fn test_file() {
    // fs = FileStore("file://%s" % os.path.dirname(__file__))
    // assert fs.exists(os.path.basename(__file__)) != []
    // assert fs.get(os.path.basename(__file__)) is not None

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