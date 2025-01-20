// import logging
// import os
// import time
// from io import BytesIO
// from typing import Iterable, Optional

// from assemblyline.common.exceptions import ChainAll
// from assemblyline.filestore.transport.base import Transport, TransportException
// from azure.core.exceptions import (
//     ClientAuthenticationError,
//     DecodeError,
//     ODataV4Error,
//     ResourceExistsError,
//     ResourceModifiedError,
//     ResourceNotFoundError,
//     ResourceNotModifiedError,
//     ServiceRequestError,
//     TooManyRedirectsError,
// )
// from azure.identity import ClientSecretCredential, DefaultAzureCredential, WorkloadIdentityCredential
// from azure.storage.blob import BlobServiceClient

// """
// This class assumes a flat file structure in the Azure storage blob.
// """

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredentialBuilder;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobServiceClient, ClientBuilder, ContainerClient};
use bytes::Bytes;
use log::info;
use tokio_stream::StreamExt;

use super::Transport;
use crate::errors::ReadOnlyError;

const MIN_BACKOFF: Duration = Duration::ZERO;
const MAX_BACKOFF: Duration = Duration::from_secs(5);


#[derive(Debug, Default)]
pub struct AzureParameters {
    pub access_key: String, 
    pub tenant_id: String,
    pub client_id: String, 
    pub client_secret: String,
    pub emulator: bool,
    pub allow_directory_access: bool, 
    pub use_default_credentials: bool,
}

pub struct TransportAzure {
    /// An azure blob storage client
    container_client: ContainerClient,

    allow_directory_access: bool,

    base_path: Option<PathBuf>,

    connection_attempts: Option<usize>,

    read_only: bool,

    host: String,
    container: String,
}


impl TransportAzure {
    pub async fn new(host: String, base: String, parameters: AzureParameters, connection_attempts: Option<usize>) -> Result<Self> {
//     def __init__(self, base=None, access_key=None, tenant_id=None, client_id=None, client_secret=None,
//                 host=None, connection_attempts=None, allow_directory_access=False, use_default_credentials=False):
        let mut read_only = false;
        let AzureParameters {
            access_key,
            use_default_credentials,
            tenant_id,
            client_id,
            client_secret,
            emulator,
            allow_directory_access,
        } = parameters;

        // Get URL
        // self.host = host
        // let endpoint_url = format!("https://{host}");

        // Get container and base_path
        let (blob_container, base_path) = match base.trim_matches('/').split_once("/") {
            Some((a, b)) => (a, Some(PathBuf::from(b))),
            None => (base.trim_matches('/'), None)
        };

        let host = host.to_lowercase();

        let account = match host.strip_suffix(".blob.core.windows.net") {
            Some(account) => account,
            None => &host,
        };

        let container_client = if emulator {
            ClientBuilder::emulator().container_client(account)
        } else {
            // Get credentials
            let credentials: StorageCredentials = if use_default_credentials {
                if (!tenant_id.is_empty() && !client_id.is_empty()) && (client_secret.is_empty()) {
                    todo!()
                    // Arc::new(WorkloadIdentityCredential::new(
                    //     reqwest::Client::new(),                    
                    //     tenant_id=tenant_id,
                    //                                             client_id=client_id
                    // ))
                } else {
                    // Service accounts will by default create the enviromental variables, and use them as params
                    let credentials: Arc<dyn TokenCredential> = Arc::new(DefaultAzureCredentialBuilder::new().build()?);
                    credentials.into()
                }
            } else if !access_key.is_empty() {
                let account = if client_id.is_empty() { account } else { &client_id };
                StorageCredentials::access_key(account, access_key)
            } else if !tenant_id.is_empty() && !client_id.is_empty() && !client_secret.is_empty() {
                todo!()
                // self.credential = ClientSecretCredential(tenant_id=tenant_id,
                //                                          client_id=client_id,
                //                                          client_secret=client_secret)
            } else {
                StorageCredentials::anonymous()
            };

            // open clients
            let service_client = BlobServiceClient::builder(account, credentials).blob_service_client();
            service_client.container_client(blob_container)
        };

        // Init
        if let Err(err) = retry!(connection_attempts, container_client.get_properties().await) {
            if !any_is_not_found(&err) {
                return Err(err)
            }

            if let Err(err) = retry!(connection_attempts, container_client.create().await) {
                if !any_is_not_found(&err) {
                    return Err(err)
                }
                info!("Failed to create container, we're most likely in read only mode");
                read_only = true;
            }    
        }

        Ok(Self {
            container_client,
            allow_directory_access,
            connection_attempts,
            base_path,
            read_only,
            host,
            container: blob_container.to_owned()
        })
    }

    fn normalize<'a>(&'a self, path: &'a str) -> Cow<'a, str> {
        // flatten path to just the basename
        let path = if !self.allow_directory_access {
            match std::path::Path::new(path).file_name() {
                Some(name) => match name.to_str() {
                    Some(name) => name,
                    None => path,
                },
                None => path,
            }
        } else {
            path
        };

        if let Some(base) = &self.base_path {
            Cow::Owned(base.join(path).to_string_lossy().to_string())
        } else {
            Cow::Borrowed(path)
        }
    }

}

#[async_trait]
impl Transport for TransportAzure {
    async fn put(&self, name: &str, body: &Bytes) -> Result<()> {
        if self.read_only {
            return Err(ReadOnlyError.into())
        }

        let key = self.normalize(name);
        let client = self.container_client.blob_client(key);

        retry!(ignore_result, self.connection_attempts, {
            client.put_block_blob(body.clone()).await
        })
    }
    
    async fn upload(&self, source: &Path, dest: &str) -> Result<()> {
        if self.read_only {
            return Err(ReadOnlyError.into())
        }

        let key = self.normalize(dest);
        let client = self.container_client.blob_client(key);

        retry!(ignore_result, self.connection_attempts, {
            let source = tokio::fs::File::open(source).await?;
            let source = azure_core::tokio::fs::FileStreamBuilder::new(source).build().await?;
            client.put_block_blob(source).await    
        })

//         # if file exists already, it will be overwritten
//         with open(src_path, "rb") as data:
//             blob_client = self.service_client.get_blob_client(self.blob_container, key)
//             try:
//                 self.with_retries(blob_client.upload_blob, data, overwrite=True)
//             except TransportException as error:
//                 if not isinstance(error.cause, ResourceExistsError):
//                     raise
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let key = self.normalize(name);
        let client = self.container_client.blob_client(key);
        retry!(self.connection_attempts, {
            match client.exists().await {
                Ok(exists) => Ok(exists),
                Err(err) if is_not_found(&err) => Ok(false),
                Err(err) => Err(err),
            }
        })
    }

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        // there are some errors azure library that stop it from using get_content on empty files.
        // we will use our own stream method, and explicity check for zero length.
        let (size, mut stream) = match self.stream(name).await {
            Ok((size, stream)) => (size, stream),
            Err(err) if any_is_not_found(&err) => return Ok(None),
            Err(err) => return Err(err),
        };

        if size == 0 {
            return Ok(Some(vec![]))
        }

        let mut buffer = vec![];
        while let Some(chunk) = stream.recv().await {
            buffer.extend(&chunk?[..]);
        }
        return Ok(Some(buffer))


        // let key = self.normalize(name);
        // let client = self.container_client.blob_client(key);
        // retry!(self.connection_attempts, {
        //     // match client.get_content().await {
        //     //     Ok(body) => Ok(Some(body)),
        //     //     Err(err) if is_not_found(&err) => Ok(None),
        //     //     Err(err) => Err(err),
        //     // }
        // })
    }

    // async fn download(&self, name: &str, dest: &Path) -> Result<()> {
    //     // create dst_path if it doesn't exist
    //     if let Some(parent) = dest.parent() {
    //         if !tokio::fs::try_exists(parent).await.context("download::try_exists")? {
    //             tokio::fs::create_dir_all(parent).await.context("download::create_dir_all")?;
    //         }
    //     }

    //     // download the key from azure
    //     let (_, mut stream) = self.stream(name).await.context("download::stream")?;
    //     let dest = dest.to_owned();
    //     tokio::task::spawn_blocking(move || {
    //         let mut file = std::fs::File::options().write(true).truncate(true).create(true).open(dest)?;
    //         while let Some(data) = stream.blocking_recv() {
    //             file.write_all(&data?)?;
    //         }
    //         return anyhow::Ok(())
    //     }).await?
    // }

    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)> {
        let key = self.normalize(name);
        let client = self.container_client.blob_client(key);
        let mut stream = client.get().into_stream();
        let (send, recv) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(err) => {
                        _ = send.send(Err(std::io::Error::new(std::io::ErrorKind::Other, err))).await;
                        return;
                    },
                };

                let mut body = chunk.data;
                while let Some(data) = body.next().await {
                    let data = match data {
                        Ok(data) => data,
                        Err(err) => {
                            _ = send.send(Err(std::io::Error::new(std::io::ErrorKind::Other, err))).await;
                            return;
                        },
                    };
                    if send.send(Ok(data)).await.is_err() {
                        return;
                    }
                };
            }
        });
        
        let properties = client.get_properties().await?;
        Ok((properties.blob.properties.content_length, recv))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        if self.read_only {
            return Err(ReadOnlyError.into())
        }

        let key = self.normalize(name);
        let client = self.container_client.blob_client(key);
        retry!(self.connection_attempts, {
            match client.delete().await {
                Ok(_) => Ok(()),
                Err(err) if is_not_found(&err) => Ok(()),
                Err(err) => Err(err),
            }
        })
//         key = self.normalize(path)
//         blob_client = self.service_client.get_blob_client(self.blob_container, key)
//         try:
//             self.with_retries(blob_client.delete_blob)
//         except TransportException as error:
//             # If its already not found, then consider it deleted.
//             if not isinstance(error.cause, ResourceNotFoundError):
//                 raise
    }
}

impl std::fmt::Debug for TransportAzure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("azure://{}/{}", self.host, self.container))
        // f.debug_struct("TransportAzure").field("container_client", &self.container_client).field("allow_directory_access", &self.allow_directory_access).field("base_path", &self.base_path).field("connection_attempts", &self.connection_attempts).field("read_only", &self.read_only).finish()
    }
}
//     def __str__(self):
//         return f"azure://{self.host}/{self.blob_container}/"


//     def makedirs(self, path):
//         # Does not need to do anything as azurestorage blob has a flat layout.
//         pass


//     def list(self, prefix: Optional[str] = None) -> Iterable[str]:
//         for blob in self.container_client.list_blobs(name_starts_with=prefix):
//             yield blob['name']



// macro_rules! retry {
//     ($body: expr) => {
//         {
//             let mut backoff = MIN_BACKOFF;
//             loop {
//                 let result = $body;
//                 match result {
//                     Ok(val) => return Ok(val),
//                     Err(err) => {
//                     }
//                 }
//             }
//         }
//     };
// }
// pub (crate) use retry;

fn is_not_found(err: &azure_core::Error) -> bool { 
    if let Some(err) = err.as_http_error() {
        if err.status() == azure_core::StatusCode::NotFound {
            return true
        }
    }
    false
}

fn any_is_not_found(err: &anyhow::Error) -> bool { 
    if let Some(err) = err.downcast_ref() {
        return is_not_found(err)
    }
    false
}

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
            loop {
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
                            log::info!("Reconnected to Azure transport!")
                        }

                        break Ok(value)
                    },
                    Err(err) => {
                        if matches!(err.kind(), azure_core::error::ErrorKind::Io) {
                            log::warn!("Filestore IO error: {err:?}");
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                            continue
                        }
                        break Err(err.into())


                        // except (ServiceRequestError, DecodeError, ResourceExistsError, ResourceNotFoundError,
                        //     ClientAuthenticationError, ResourceModifiedError, ResourceNotModifiedError,
                        //     TooManyRedirectsError, ODataV4Error):
                        //     // These errors will be wrapped by TransportException
                        //     raise
    
                        // log::warn!("No connection to Azure transport, retrying... [{err:?}]");
                        // tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        // retries += 1;    
                    }
                }
            }
        }
    };
}
pub (crate) use retry;

