
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use libunftp::auth::{AuthenticationError, Authenticator, Credentials, DefaultUser};
use unftp_sbe_fs::ServerExt;


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

pub async fn start_temp_ftp_server(tls_cert: Option<(Vec<u8>, Vec<u8>)>) -> String {

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

