
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;

use super::Transport;


// import boto3
// import logging
// import os
// import tempfile
// import threading

// from typing import Iterable, Optional

// from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError
// from io import BytesIO

// from assemblyline.common.exceptions import ChainAll
// from assemblyline.filestore.transport.base import Transport, TransportException

// try:
//     from botocore.vendored.requests.packages.urllib3 import disable_warnings
// except ImportError:
//     from urllib3 import disable_warnings


// disable_warnings()
// boto3_client_lock = threading.Lock()

// """
// This class assumes a flat file structure in the S3 bucket.  This is due to the way the AL datastore currently handles
// file paths for local/ftp datastores not playing nicely with s3 constraints.
// """


const DEFAULT_HOST: &str = "s3.amazonaws.com";
const MIN_BACKOFF: Duration = Duration::ZERO;
const MAX_BACKOFF: Duration = Duration::from_secs(5);


#[derive(Debug)]
pub struct S3Parameters {
    pub aws_region: Option<String>,
    pub s3_bucket: String,
    pub use_ssl: bool,
    pub verify: bool,
    pub boto_defaults: bool,
}

impl Default for S3Parameters {
    fn default() -> Self {
        Self { 
            aws_region: None, 
            s3_bucket: "al-storage".to_string(), 
            use_ssl: true, 
            verify: true,
            boto_defaults: false 
        }
    }
}

pub struct TransportS3 {
    parameters: S3Parameters,
    retry_limit: Option<usize>,
    client: aws_sdk_s3::Client,

    base: String,
    accesskey: Option<String>,
    host: String,
    port: u16,
}


impl TransportS3 {
    // base=None, , aws_region=None, host=None, port=None, ):

    pub async fn new(base: String, host: Option<String>, port: Option<u16>, accesskey: Option<String>, secretkey: Option<String>, connection_attempts: Option<usize>, parameters: S3Parameters) -> Result<Self> {
        let host = host.unwrap_or_else(|| DEFAULT_HOST.to_owned());

        let port = match port {
            Some(port) => port,
            None => if parameters.use_ssl { 443 } else { 80 }
        };

        let scheme = if parameters.use_ssl { "https" } else { "http" };

        let endpoint_url = format!("{scheme}://{host}:{port}");

        // Ok(S3BlobStore { client: bucket })
        let mut loader = aws_config::defaults(BehaviorVersion::v2025_08_07());

        // Override the region
        if let Some(region) = parameters.aws_region.clone() {
            loader = loader.region(aws_types::region::Region::new(region));
        } else {
            loader = loader.region(aws_types::region::Region::from_static("ca-central-1"))
        }

        // configure endpoint
        loader = loader.endpoint_url(endpoint_url);

        // Configure keys
        if let Some(key) = &accesskey {
            std::env::set_var("AWS_ACCESS_KEY_ID", key);
        }
        if let Some(secret) = secretkey {
            std::env::set_var("AWS_SECRET_ACCESS_KEY", secret);
        }

        // Configure the use of ssl
        loader = loader.http_client({
            use legacy_hyper_rustls as hyper_rustls;
            use legacy_rustls as rustls;

            let https_connector = if parameters.verify {
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http1()
                    .enable_http2()
                    .build()
            } else {
                let root_store = rustls::RootCertStore::empty();
                let mut tls_config = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_store.clone())
                    .with_no_client_auth();
                                    
                tls_config
                    .dangerous()
                    .set_certificate_verifier(Arc::new(verifier::NoCertificateVerification::new()));

                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_tls_config(tls_config)
                    .https_or_http()
                    .enable_http1()
                    .enable_http2()
                    .build()
            };

            // this is for a later version of rustls, the current non-legacy code in aws
            // doesn't support custimizing the tls configurations
            // let https_connector = if !parameters.verify {
            //     let root_store = rustls::RootCertStore::empty();
            //     let mut tls_config = rustls::ClientConfig::builder()    
            //         .with_root_certificates(root_store.clone())
            //         .with_no_client_auth();
            //     let verifier = rustls::client::WebPkiServerVerifier::builder(Arc::new(root_store)).build()?;
            //     tls_config
            //         .dangerous()
            //         .set_certificate_verifier(Arc::new(verifier::NoCertificateVerification::new(verifier)));

            //     hyper_rustls::HttpsConnectorBuilder::new()
            //         .with_tls_config(tls_config)
            //         .https_or_http()
            //         .enable_http1()
            //         .enable_http2()
            //         .build()
            // } else {
            //     hyper_rustls::HttpsConnectorBuilder::new()
            //         .with_native_roots()?
            //         .https_or_http()
            //         .enable_http1()
            //         .enable_http2()
            //         .build()
            // };

            aws_smithy_http_client::hyper_014::HyperClientBuilder::new()
                .build(https_connector)
        });

        // Build the client
        let sdk_config = loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(s3_config);

        // make sure the bucket exists
        let head_result = retry!(connection_attempts, { 
            client.head_bucket().bucket(&parameters.s3_bucket).send().await
        });

        if let Err(err) = head_result {
            let err = err.downcast::<SdkError<aws_sdk_s3::operation::head_bucket::HeadBucketError>>()?;
            let err = err.into_service_error();
            if err.is_not_found() {
                // if the bucket does not exist, create it
                let create_result = retry!(connection_attempts, {
                    client.create_bucket().bucket(&parameters.s3_bucket).send().await
                });
                if let Err(err) = create_result {
                    let err = err.downcast::<SdkError<aws_sdk_s3::operation::create_bucket::CreateBucketError>>()?;
                    let x = err.into_service_error();
                    // Maybe someone else created the bucket in the tibe between us calling head and create.
                    if !x.is_bucket_already_exists() && !x.is_bucket_already_owned_by_you() {
                        return Err(x.into())
                    }
                }
            } else {
                return Err(anyhow::Error::new(err).context("head error"))
            }
        }

        Ok(Self {
            base,
            parameters,
            accesskey,
            retry_limit: connection_attempts,
            client,
            host,
            port,
        })
    }


    fn normalize(&self, path: &str) -> Result<String> {
        // flatten path to just the basename
        match Path::new(path).file_name() {
            Some(path) => Ok(path.to_string_lossy().to_string()),
            None => Err(anyhow::anyhow!("Could not normalize path to file name: {path}")),
        }
    }

}

impl std::fmt::Debug for TransportS3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("s3://")?;
        if let Some(access) = &self.accesskey {
            f.write_str(access)?;
            f.write_str("@")?;
        }
        f.write_fmt(format_args!("{}:{}/{}", self.host, self.port, self.parameters.s3_bucket))?;
        if !self.base.is_empty() {
            if !self.base.starts_with("/") {
                f.write_str("/")?;
            }
            f.write_str(&self.base)?;
        }
        Ok(())
    }
}


#[async_trait]
impl Transport for TransportS3 {

    async fn put(&self, name: &str, body: &Bytes) -> Result<()> {
        let label = self.normalize(name)?;
        retry!(ignore_result, self.retry_limit, {
            self.client
                .put_object()
                .content_type("application/octet-stream")
                .content_length(body.len() as i64)
                .bucket(&self.parameters.s3_bucket)
                .key(label.clone())
                .body(body.clone().into())
                .send().await
        })
    }

    async fn upload(&self, path: &Path, name: &str) -> Result<()> {
        let label = self.normalize(name)?;
        retry!(ignore_result, self.retry_limit, {
            self.client
                .put_object()
                .content_type("application/octet-stream")
                .bucket(&self.parameters.s3_bucket)
                .key(label.clone())
                .body(ByteStream::from_path(path).await?)
                .send().await
        })
    }

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let label = self.normalize(name)?;
        
        fn is_not_found(err: &SdkError<aws_sdk_s3::operation::get_object::GetObjectError>) -> bool {
            if let Some(err) = err.as_service_error() {
                if err.is_no_such_key() {
                    return true
                }
            }
            return false
        }

        retry!(self.retry_limit, {
            let request = self.client
                .get_object()
                .bucket(&self.parameters.s3_bucket)
                .key(label.clone())
                .send().await;
            match request {
                Ok(request) => {
                    let bytes = request.body.collect().await?;
                    Ok(Some(bytes.to_vec()))        
                },
                Err(err) if is_not_found(&err) => Ok(None),
                Err(err) => Err(err)
            }
        })
    }
    async fn exists(&self, name: &str) -> Result<bool> {
        let label = self.normalize(name)?;

        fn is_not_found(err: &SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>) -> bool {
            if let Some(err) = err.as_service_error() {
                if err.is_not_found() {
                    return true
                }
            }
            return false
        }

        retry!(self.retry_limit, {
            let request = self.client
                .head_object()
                .bucket(&self.parameters.s3_bucket)
                .key(label.clone())
                .send().await;
            match request {
                Ok(_) => Ok(true),
                Err(err) if is_not_found(&err) => Ok(false),
                Err(err) => Err(err)
            }
        })
    }

    /// read blob into stream
    /// The api already provides block based reading, so just spawn a task
    /// to read from the respones and shovel data into the channel
    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)> {
        let label = self.normalize(name)?;
        let mut request = self.client
            .get_object()
            .bucket(&self.parameters.s3_bucket)
            .key(label)
            .send().await?;
        let length = match request.content_length() {
            Some(length) => length,
            None => anyhow::bail!("S3 did not return blob size"),
        };

        let (send, recv) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            // let mut chunks = request.body.chunks(1 << 20);
            while let Some(buffer) = request.body.next().await {
                _ = match buffer {
                    Ok(data) => send.send(Ok(data)).await,
                    Err(err) => send.send(Err(std::io::Error::other(err))).await,
                };
            }
        });

        return Ok((length as u64, recv))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let label = self.normalize(name)?;

        retry!(ignore_result, self.retry_limit, {
            self.client
                .delete_object()
                .bucket(&self.parameters.s3_bucket)
                .key(label.clone())
                .send().await
        })
    }
}


//     def list(self, prefix: Optional[str] = None) -> Iterable[str]:
//         args = {
//             'Bucket': self.bucket,
//             'Prefix': prefix or '',
//             'MaxKeys': 50000,
//         }
//         while args.get('ContinuationToken', None) != '':
//             data = self.client.list_objects_v2(**args)
//             args['ContinuationToken'] = data.get("NextContinuationToken", '')
//             for chunk in data.get('Contents', []):
//                 yield chunk['Key']


mod verifier {
    use legacy_rustls::client::{ServerCertVerified, ServerCertVerifier};

    /// A dummy certificate verifier that just accepts anything
    #[derive(Debug)]
    pub struct NoCertificateVerification { }

    impl NoCertificateVerification {
        pub fn new() -> Self {
            Self { }
        }
    }

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &legacy_rustls::Certificate,
            _intermediates: &[legacy_rustls::Certificate],
            _server_name: &legacy_rustls::ServerName,
            _scts: &mut dyn Iterator<Item = &[u8]>,
            _ocsp_response: &[u8],
            _now: std::time::SystemTime,
        ) -> Result<legacy_rustls::client::ServerCertVerified, legacy_rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
    }

    // Dummy verifiier implementations for newer rustls versions

    // use legacy_rustls as rustls;
    // use legacy_rustls::{
    //     client::{
    //         danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    //         WebPkiServerVerifier,
    //     },
    //     pki_types::{CertificateDer, ServerName, UnixTime},
    //     DigitallySignedStruct,
    // };
    // use std::sync::Arc;

    // /// A dummy certificate verifier that just accepts anything
    // #[derive(Debug)]
    // pub struct NoCertificateVerification {
    //     inner: Arc<WebPkiServerVerifier>,
    // }

    // impl NoCertificateVerification {
    //     pub fn new(inner: Arc<WebPkiServerVerifier>) -> Self {
    //         Self { inner }
    //     }
    // }

    // impl ServerCertVerifier for NoCertificateVerification {

    //     fn verify_server_cert(
    //         &self,
    //         _end_entity: &CertificateDer<'_>,
    //         _intermediates: &[CertificateDer<'_>],
    //         _server_name: &ServerName<'_>,
    //         _ocsp: &[u8],
    //         _now: UnixTime,
    //     ) -> Result<ServerCertVerified, rustls::Error> {
    //         Ok(ServerCertVerified::assertion())
    //     }

    //     fn verify_tls12_signature(
    //         &self,
    //         message: &[u8],
    //         cert: &CertificateDer<'_>,
    //         dss: &DigitallySignedStruct,
    //     ) -> Result<HandshakeSignatureValid, rustls::Error> {
    //         self.inner.verify_tls12_signature(message, cert, dss)
    //     }

    //     fn verify_tls13_signature(
    //         &self,
    //         message: &[u8],
    //         cert: &CertificateDer<'_>,
    //         dss: &DigitallySignedStruct,
    //     ) -> Result<HandshakeSignatureValid, rustls::Error> {
    //         self.inner.verify_tls13_signature(message, cert, dss)
    //     }

    //     fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
    //         self.inner.supported_verify_schemes()
    //     }
    // }

    // /// A weak certificate verifier that performs certificate verification but doesn't require hostnames to match
    // #[derive(Debug)]
    // pub struct NoServerNameVerification {
    //     inner: Arc<WebPkiServerVerifier>,
    // }

    // impl NoServerNameVerification {
    //     pub fn new(inner: Arc<WebPkiServerVerifier>) -> Self {
    //         Self { inner }
    //     }
    // }

    // impl ServerCertVerifier for NoServerNameVerification {
    //     fn verify_server_cert(
    //         &self,
    //         _end_entity: &CertificateDer<'_>,
    //         _intermediates: &[CertificateDer<'_>],
    //         _server_name: &ServerName<'_>,
    //         _ocsp: &[u8],
    //         _now: UnixTime,
    //     ) -> Result<ServerCertVerified, rustls::Error> {
    //         match self
    //             .inner
    //             .verify_server_cert(_end_entity, _intermediates, _server_name, _ocsp, _now)
    //         {
    //             Ok(scv) => Ok(scv),
    //             Err(rustls::Error::InvalidCertificate(cert_error)) => {
    //                 if let rustls::CertificateError::NotValidForName = cert_error {
    //                     Ok(ServerCertVerified::assertion())
    //                 } else {
    //                     Err(rustls::Error::InvalidCertificate(cert_error))
    //                 }
    //             }
    //             Err(e) => Err(e),
    //         }
    //     }

    //     fn verify_tls12_signature(
    //         &self,
    //         message: &[u8],
    //         cert: &CertificateDer<'_>,
    //         dss: &DigitallySignedStruct,
    //     ) -> Result<HandshakeSignatureValid, rustls::Error> {
    //         self.inner.verify_tls12_signature(message, cert, dss)
    //     }

    //     fn verify_tls13_signature(
    //         &self,
    //         message: &[u8],
    //         cert: &CertificateDer<'_>,
    //         dss: &DigitallySignedStruct,
    //     ) -> Result<HandshakeSignatureValid, rustls::Error> {
    //         self.inner.verify_tls13_signature(message, cert, dss)
    //     }

    //     fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
    //         self.inner.supported_verify_schemes()
    //     }
    // }

}


//     def with_retries(self, func, *args, **kwargs):
//         retries = 0
//         while self.retry_limit is None or retries <= self.retry_limit:
//             try:
//                 ret_val = func(*args, **kwargs)

//                 if retries:
//                     self.log.info('Reconnected to S3 transport!')

//                 return ret_val

//             except (EndpointConnectionError, ConnectionClosedError):
//                 self.log.warning(f"No connection to S3 transport {self.endpoint_url}, retrying...")
//                 retries += 1
//         raise ConnectionError(f"Couldn't connect to the requested S3 endpoint {self.endpoint_url} inside retry limit")


macro_rules! retry {
    (ignore_result, $connection_attempts: expr, $body: expr) => {
        {
            match retry!($connection_attempts, $body) {
                Ok(_) => Ok(()),
                Err(err) => Err(err)
            }
        }
    };

    ($connection_attempts: expr, $body: expr) => {
        {
            let mut backoff = MIN_BACKOFF;
            let mut retries = 0;
            'outer: loop {
                if retries > 0 {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }

                if let Some(limit) = $connection_attempts {
                    if retries > limit {
                        break Err(anyhow::Error::from(crate::errors::ConnectionError))
                    }
                }

                let ret_val = $body;
                retries += 1;

                match ret_val {
                    Ok(value) => {
                        if retries > 1 {
                            log::info!("Reconnected to S3 transport!")
                        }
                        break Ok(value)
                    },
                    Err(err) => {
                        let mut error: Box<&(dyn std::error::Error + 'static)> = Box::new(&err);
                        loop {
                            if error.downcast_ref::<std::io::Error>().is_some() {
                                log::warn!("Filestore IO error: {err:?}");
                                continue 'outer
                            }    
                            match error.source() {
                                Some(parent) => error = Box::new(parent),
                                None => break,
                            }
                        }

                        break Err(err.into())
                    }
                }
            }
        }
    };
}
pub (crate) use retry;

