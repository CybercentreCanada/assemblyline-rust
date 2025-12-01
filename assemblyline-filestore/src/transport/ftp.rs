use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use log::debug;
use suppaftp::tokio::{AsyncNativeTlsStream, AsyncNoTlsStream, ImplAsyncFtpStream, TokioTlsStream};
use suppaftp::{FtpError, Status};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::transport::{Transport, normalize_srl_path};

const BUFFER_SIZE: usize = 1 << 12;
const FTP_POOL_WAIT: Duration = Duration::from_millis(2);
const MIN_BACKOFF: Duration = Duration::from_millis(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// An FTP connection can only be used for a single transfer at a time. 
/// To overcome this we will use a pool of connections
struct FtpConnectionPool<T: TokioTlsStream + Send> {
    user: Option<String>,
    password: Option<String>,
    base: PathBuf,
    tls: bool,
    host: String,
    port: u16,
    
    pool: parking_lot::Mutex<Vec<Client<T>>>,
    signal: tokio::sync::Notify,
}

/// A single connection owned by the pool, loaned to a function call
struct Client<T: TokioTlsStream + Send> {
    pool: Weak<FtpConnectionPool<T>>,
    client: Option<ImplAsyncFtpStream<T>>,
}

/// Return the loaned connection to the pool when the function call drops the Client handle
impl<T: TokioTlsStream + Send> Drop for Client<T> {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.pool.lock().push(Client { pool: Arc::downgrade(&pool), client: self.client.take() });
            pool.signal.notify_one();
        }
    }
}

impl<T: TokioTlsStream + Send> std::ops::Deref for Client<T> {
    type Target = ImplAsyncFtpStream<T>;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}

impl<T: TokioTlsStream + Send> std::ops::DerefMut for Client<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}


impl<T: TokioTlsStream + Send> FtpConnectionPool<T> {
    pub async fn new(tls: bool, base: &str, host: String, port: u16, user: Option<String>, password: Option<String>) -> Result<Arc<Self>> {
        let new = Arc::new(Self {
            user,
            password,
            base: PathBuf::from_str(base)?,
            tls,
            host,
            port,

            pool: parking_lot::Mutex::new(vec![]),
            signal: tokio::sync::Notify::new(),
        });

        // initiaize the pool with some connections
        for _ in 0..5 {
            let connection = new.new_connection().await?;
            new.pool.lock().push(connection);
        }

        Ok(new)
    }

    /// Form a new connection to the FTP server
    async fn new_connection(self: &Arc<Self>) -> Result<Client<T>> {
        let addr = format!("{}:{}", self.host, self.port);
        
        let mut client = ImplAsyncFtpStream::<T>::connect(addr).await?;

        if let Some(user) = &self.user {
            client.login(user.as_ref(), self.password.as_deref().unwrap_or_default()).await?;
        }

        Ok(Client { pool: Arc::downgrade(self), client: Some(client) })
    }

    /// Borrow an existing connection from the pool, or after a timeout, form a new one
    async fn get_connection(self: &Arc<Self>) -> Result<Client<T>> {
        let timeout = tokio::time::Instant::now() + FTP_POOL_WAIT;
        loop {
            {
                let mut pool = self.pool.lock();
                if let Some(item) = pool.pop() {
                    return Ok(item)
                }
            }

            if tokio::time::timeout_at(timeout, self.signal.notified()).await.is_err() {
                return self.new_connection().await;
            }
        }
    }
}

pub struct TransportFtp<T: TokioTlsStream + Send> {
    pool: Arc<FtpConnectionPool<T>>,
    retry_limit: Option<usize>,
}

impl<T: TokioTlsStream + Send> std::fmt::Debug for TransportFtp<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scheme = if self.pool.tls { "ftps" } else { "ftp" };
        let url = format!("{scheme}://{}:{}/{}", self.pool.host, self.pool.port, self.pool.base.to_string_lossy());
        f.debug_struct("TransportFtp").field("url", &url).finish()
    }
}

impl TransportFtp<AsyncNoTlsStream> {

    pub async fn new(retry_limit: Option<usize>, base: &str, host: String, port: u16, user: Option<String>, password: Option<String>) -> Result<Self> {
        Ok(Self {
            pool: FtpConnectionPool::new(false, base, host, port, user, password).await?,
            retry_limit
        })
    }
}

impl TransportFtp<AsyncNativeTlsStream> {

    pub async fn new_secure(retry_limit: Option<usize>, base: &str, host: String, port: u16, user: Option<String>, password: Option<String>) -> Result<Self> {
        Ok(Self {
            pool: FtpConnectionPool::new(true, base, host, port, user, password).await?,
            retry_limit
        })
    }
}

impl<T: TokioTlsStream + Send + 'static> TransportFtp<T> {


    fn normalize(&self, path: &str) -> Result<String> {
        // If they've provided an absolute path. Leave it a is.
        let s = if path.starts_with('/') {
            PathBuf::from_str(path)?
        } else if path.contains("/") || path.len() != 64 { // Relative paths
            safe_path::scoped_join(&self.pool.base, path)?
        } else {
            safe_path::scoped_join(&self.pool.base, normalize_srl_path(path))?
        }.to_string_lossy().to_string();
        debug!("ftp normalized: {} -> {}", path, s);
        return Ok(s)
    }

    async fn upload_stream(&self, name: &str, mut stream: mpsc::Receiver<Bytes>) -> Result<()> {
        let mut client = self.pool.get_connection().await?;

        let mut upload_stream = {
            let path = self.normalize(name)?;
            
            // make sure any directories in 
            let mut dirs: Vec<&str> = path.split("/").collect();
            dirs.pop(); // exclude the last path item as that is probably the file name
            let mut build_path = String::new();
            for dir in dirs {
                if dir.is_empty() { continue }
                build_path += dir;
                match client.mkdir(&build_path).await {
                    Ok(_) => {},
                    Err(err) if is_file_unavailable(&err) => {},
                    Err(err) => return Err(err.into())
                }
                build_path += "/";
            }

            client.put_with_stream(path).await?
        };

        while let Some(chunk) = stream.recv().await {
            upload_stream.write_all(&chunk).await?;
        }

        client.finalize_put_stream(upload_stream).await?;
        Ok(())
    }

    async fn _get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let (size, mut stream) = match self.stream(name).await {
            Ok((size, stream)) => (size, stream),
            Err(err) => {
                match err.downcast() {
                    Ok(FtpError::UnexpectedResponse(resp)) if resp.status == Status::FileUnavailable => return Ok(None),
                    Ok(other) => return Err(other.into()),
                    Err(other) => return Err(other)
                }
            }
        };

        let mut buffer = Vec::with_capacity(size as usize);
        while let Some(chunk) = stream.recv().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);
        }
        Ok(Some(buffer))
    }

    async fn _put(&self, name: &str, body: &Bytes) -> Result<()> {
        let (send, recv) = mpsc::channel(8);
        send.send(body.clone()).await?;
        drop(send); 
        self.upload_stream(name, recv).await?;
        Ok(())
    }

    async fn _upload(&self, src: &Path, dest: &str) -> Result<()> {
        let (send, recv) = mpsc::channel(8);
        let mut read_handle = tokio::fs::OpenOptions::new().read(true).create(false).open(src).await?;
        let handle = tokio::spawn(async move {
            loop {
                let mut chunk = BytesMut::zeroed(BUFFER_SIZE);
                let size = read_handle.read(&mut chunk).await?;
                if size == 0 { return Ok(()) }
                chunk.resize(size, 0);
                send.send(chunk.freeze()).await?;
            }
        });
        
        self.upload_stream(dest, recv).await?;
        handle.await?
    }

}

#[async_trait]
impl<T: TokioTlsStream + Send + 'static> Transport for TransportFtp<T> { 
    async fn put(&self, name: &str, body: &Bytes) -> Result<()> {
        retry!(self.retry_limit, self._put(name, body).await)
    }

    async fn upload(&self, src: &Path, dest: &str) -> Result<()> {
        retry!(self.retry_limit, self._upload(src, dest).await)
    }

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        retry!(self.retry_limit, self._get(name).await)
    }
    
    async fn exists(&self, name: &str) -> Result<bool> {
        let path = self.normalize(name)?;
        let size = retry!(until_missing, self.retry_limit, {
            let mut client = self.pool.get_connection().await?;
            client.size(&path).await
        })?;
        return Ok(size.is_some())
    }

    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)> {
        let path = self.normalize(name)?;
        let mut client = self.pool.get_connection().await?;
        let size = client.size(&path).await? as u64;
        let mut stream = client.retr_as_stream(path).await?;

        let (output_stream, channel) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            loop {
                let mut buffer = BytesMut::zeroed(BUFFER_SIZE);
                let len = match stream.read(&mut buffer[..]).await {
                    Ok(0) => break,
                    Ok(len) => len,
                    Err(error) => {
                        _ = output_stream.send(Err(std::io::Error::other(error))).await;
                        break
                    },
                };

                buffer.resize(len, 0);
                _ = output_stream.send(Ok(buffer.freeze())).await;
            }
            if let Err(err) = client.finalize_retr_stream(stream).await {
                _ = output_stream.send(Err(std::io::Error::other(err))).await;
            }
        });

        Ok((size, channel))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let name = self.normalize(name)?;
        retry!(until_missing, self.retry_limit, {
            let mut client = self.pool.get_connection().await?;
            client.rm(&name).await
        })?;
        Ok(())
    }
}

fn is_file_unavailable(err: &FtpError) -> bool {
    if let FtpError::UnexpectedResponse(resp) = err {
        if resp.status == Status::FileUnavailable {
            return true
        }
    }
    false
}

macro_rules! retry {
    (until_missing, $connection_attempts: expr, $body: expr) => {
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
                            log::info!("Reconnected to FTP Transport!")
                        }

                        break Ok(Some(value))
                    },
                    Err(err) if is_file_unavailable(&err) => {
                        if retries > 1 {
                            log::info!("Reconnected to FTP Transport!")
                        }

                        break Ok(None)
                    },
                    Err(err) => {
                        log::warn!("Filestore error: {err:?}");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue
                    }
                }
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
                            log::info!("Reconnected to FTP Server!")
                        }

                        break Ok(value)
                    },
                    Err(err) => {
                        log::warn!("Filestore error: {err:?}");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue
                    }
                }
            }
        }
    };
}

pub (crate) use retry;

