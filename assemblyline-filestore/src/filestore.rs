use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::warn;

use crate::transport::Transport;
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

        // base = parsed.path or '/'
        // host = parsed.hostname
        // if host == ".":
        //     base = "%s%s" % (host, base)
        // port = parsed.port
        // if parsed.password:
        //     password = unquote(parsed.password)
        // else:
        //     password = ''
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

                let path = url.path().parse()?;
                Ok(Box::new(LocalTransport::new(path)))
            }
            "azure" => {
                use crate::transport::azure::{AzureParameters, TransportAzure};
                let mut parameters = AzureParameters::default();
                for (name, value) in url.query_pairs() {
                    match name.as_ref() {
                        "allow_directory_access" => parameters.allow_directory_access = read_bool(&value), 
                        "use_default_credentials" => parameters.use_default_credentials = read_bool(&value),
                        "access_key" => parameters.access_key = value.to_string(), 
                        "tenant_id" => parameters.tenant_id = value.to_string(), 
                        "emulator" => parameters.emulator = read_bool(&value),
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

                println!("host/base: {host}  {base}");

                Ok(Box::new(TransportAzure::new(host, base, parameters, connection_attempts).await?))
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
        let mut last_error = None;
        for transport in &self.transports {
            match transport.download(name, path).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    warn!("Could not download file: [{name}] from {transport:?}");
                    last_error = Some(err);
                    continue
                },
            }
        }
        match last_error {
            Some(error) => Err(error).context("All transports failed to fetch"),
            None => bail!("All transports failed to fetch [{name}]")
        }
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
                failed_tuples.push((src_path.to_path_buf(), dst_path.to_string(), error.to_string()));
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