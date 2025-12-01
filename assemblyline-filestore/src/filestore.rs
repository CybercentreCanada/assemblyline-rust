use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::warn;

use crate::transport::Transport;
use crate::transport::ftp::TransportFtp;
use crate::transport::local::LocalTransport;

/// An abstract interface over one or more storage transports.
#[derive(Debug)]
pub struct FileStore {
    transports: Vec<Box<dyn Transport>>,
}


impl FileStore {
    /// Open all urls with default parameters
    pub async fn open(urls: &[String]) -> Result<Arc<FileStore>> {
        let mut transports = vec![];
        for url in urls {
            transports.push(Self::create_transport(url, None).await?)
        }
        Ok(Arc::new(Self { 
            transports 
        }))
    }

    /// Open a single url with retrying disabled
    pub async fn with_limit_retries(url: &str) -> Result<Arc<FileStore>> {
        Ok(Arc::new(Self { 
            transports: vec![Self::create_transport(url, Some(1)).await?]
        }))
    }

    async fn create_transport(address: &str, connection_attempts: Option<usize>) -> Result<Box<dyn Transport>> {
        let url: url::Url = address.parse()?;

        let mut base = match url.path() {
            "" => "/",
            other => other,
        }.to_string();

        let host = url.host_str();
        if host == Some(".") {
            base = format!(".{base}");
        }
        let port = url.port();

        let password = url.password().map(|password|{
            percent_encoding::percent_decode_str(password).decode_utf8_lossy().to_string()
        });

        // user = parsed.username or ''
    

        match url.scheme() {
            "file" => {
                if url.has_host() {
                    bail!("Local file connections can't specify a host.");
                }

                // for (name, value) in url.query_pairs() {

                // }
                // valid_bool_keys = ['normalize']
                // extras = _get_extras(parse_qs(parsed.query), valid_bool_keys=valid_bool_keys)

                // t = TransportLocal(base=base, **extras)

                let path = base.parse()?;
                Ok(Box::new(LocalTransport::new(path)))
            }
            "azure" => {
                use crate::transport::azure::{AzureParameters, TransportAzure};
                let mut parameters = AzureParameters::default();
                for (name, value) in url.query_pairs() {
                    match name.as_ref() {
                        "allow_directory_access" => parameters.allow_directory_access = read_bool(&value), 
                        "use_default_credentials" => parameters.use_default_credentials = read_bool(&value),
                        "emulator" => parameters.emulator = read_bool(&value),
                        "access_key" => parameters.access_key = value.to_string(), 
                        "tenant_id" => parameters.tenant_id = value.to_string(), 
                        "client_id" => parameters.client_id = value.to_string(), 
                        "client_secret" => parameters.client_secret = value.to_string(),
                        _ => {}
                    }
                }

                // t = TransportAzure(base=base, host=host, connection_attempts=connection_attempts, **extras)
                let host = match url.host_str() {
                    Some(host) => host.to_owned(),
                    None => bail!("a host must be provided for azure connections"),
                };
                let base = url.path().to_owned();

                Ok(Box::new(TransportAzure::new(host, base, parameters, connection_attempts).await?))
            }
            "s3" => {
                use crate::transport::s3::{S3Parameters, TransportS3};
                let mut parameters = S3Parameters::default();
                for (name, value) in url.query_pairs() {
                    match name.as_ref() {
                        "use_ssl" => parameters.use_ssl = read_bool(&value), 
                        "verify" => parameters.verify = read_bool(&value),
                        "boto_defaults" => parameters.boto_defaults = read_bool(&value),
                        "aws_region" => parameters.aws_region = Some(value.to_string()), 
                        "s3_bucket" => parameters.s3_bucket = value.to_string(), 
                        _ => {}
                    }
                }
        
                // If user/password not specified, access might be dictated by IAM roles
                let user = match url.username() {
                    "" => None,
                    value => Some(value.to_owned())
                };
        
                Ok(Box::new(TransportS3::new(base, host.map(str::to_string), port, user, password, connection_attempts, parameters).await?))
            }
            "ftp" | "ftps" => {
                let host = match host {
                    Some(host) => host.to_owned(),
                    None => bail!("A host must be provided for ftp transport")
                };

                let user = match url.username() {
                    "" => None,
                    value => Some(value.to_owned())
                };
                
                if url.scheme().eq_ignore_ascii_case("ftps") {
                    Ok(Box::new(TransportFtp::new_secure(connection_attempts, &base, host, port.unwrap_or(21), user, password).await?))
                } else {
                    Ok(Box::new(TransportFtp::new(connection_attempts, &base, host, port.unwrap_or(21), user, password).await?))
                }
            }
            _ => {
                bail!("Not an accepted filestore scheme: {}", url.scheme());
            }
        }
    }

    /// Upload a buffer to all transports
    pub async fn put(&self, name: &str, body: &Bytes) -> Result<()> {
        for transport in &self.transports {
            transport.put(name, body).await?;
        }
        Ok(())
    }

    /// Check if a given blob is defined in any transport.
    /// Errors will be supressed as long as any transport contains the file.
    pub async fn exists(&self, name: &str) -> Result<bool> {
        let mut last_error = None;
        for transport in &self.transports {
            match transport.exists(name).await {
                Ok(true) => return Ok(true),
                Ok(false) => continue,
                Err(err) => {
                    last_error = Some(err);
                    continue
                },
            }
        }
        if let Some(error) = last_error {
            return Err(error).context("Transport errors");
        }
        return Ok(false)
    }

    /// Pull blob to in memory buffer.
    /// Returns errors only if all transports fail, otherwise errors will be logged as warnings.
    pub async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let mut last_error = None;
        for transport in &self.transports {
            match transport.get(name).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    warn!("error fetching blob [{name}] from transport {transport:?}: {err}");
                    last_error = Some(err);
                    continue
                },
            }
        }
        match last_error {
            Some(error) => Err(error).context("All transports failed to fetch"),
            None => Ok(None)
        }
    }

    /// Download a blob and write it to a local file.
    /// If the file does not exist it will be created. If it does exist it will be replaced.
    pub async fn download(&self, name: &str, path: &Path) -> Result<()> {
        let mut errors = vec![];
        for transport in &self.transports {
            match transport.download(name, path).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    errors.push(format!("Could not download file: [{name}] from {transport:?}: {err}"));
                    continue
                },
            }
        }
        if errors.is_empty() {
            bail!("All transports failed to fetch [{name}]")
        }
        Err(anyhow::anyhow!(errors.join("\n")).context("All transports failed to fetch"))
    }

    /// Upload a local file as a named blob.
    pub async fn upload(&self, path: &Path, name: &str) -> Result<()> {
        let mut last_error = None;
        for transport in &self.transports {
            if let Err(err) = transport.upload(path, name).await {
                last_error = Some(err);
            }
        }
        match last_error {
            Some(error) => Err(error).context("A transport failed to upload"),
            None => Ok(())
        }
    }

    /// Upload a collection of local files.
    pub async fn upload_batch(&self, local_remote_tuples: &[(&Path, &str)]) -> Vec<(PathBuf, String, String)> {
        let mut failed_tuples = vec![];
        for (src_path, dst_path) in local_remote_tuples {
            if let Err(error) = self.upload(src_path, dst_path).await {
                failed_tuples.push((src_path.to_path_buf(), dst_path.to_string(), format!("{error:?}")));
            }
        }
        return failed_tuples
    }

    /// Stream the content of a blob.
    /// Returns the total expected length of the stream and a message receiver of data buffers.
    pub async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)> {
        let mut last_error = None;
        for transport in &self.transports {
            match transport.stream(name).await {
                Ok((size, stream)) => return Ok((size, stream)),
                Err(err) => last_error = Some(err),
            }
        }
        match last_error {
            Some(err) => Err(err),
            None => bail!("No transports could stream file"),
        }
    }

    /// Remove a blob from the storage
    pub async fn delete(&self, name: &str) -> Result<()> { 
        for transport in &self.transports {
            transport.delete(name).await?;
        }
        Ok(())
    }
}

fn read_bool(value: &str) -> bool {
    matches!(value.to_ascii_lowercase().as_str(), "true" | "1")
}