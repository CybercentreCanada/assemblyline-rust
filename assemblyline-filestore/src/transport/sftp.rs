use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use log::debug;
use russh::client::Handler;
use russh::keys::PrivateKeyWithHashAlg;
use russh_sftp::client::SftpSession;
use russh_sftp::client::error::Error;
use russh_sftp::protocol::{OpenFlags, StatusCode};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::transport::{Transport, normalize_srl_path};

const MIN_BACKOFF: Duration = Duration::from_millis(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const BUFFER_SIZE: usize = 1 << 14;

pub struct TransportSftp {
    host: String,
    port: u16,
    user: String,
    base: String,
    client: SftpSession,
    retry_limit: Option<usize>,
}

impl std::fmt::Debug for TransportSftp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let url = format!("sftp://{}@{}:{}{}", self.user, self.host, self.port, self.base);
        f.debug_struct("TransportSFTP").field("url", &url).finish()
    }
}

struct ConnectionHandler {
    host: String,
    port: u16,
    validate_host: bool
}

impl Handler for ConnectionHandler {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, server_public_key: &russh::keys::ssh_key::PublicKey) -> Result<bool, Self::Error> {
        if self.validate_host {
            Ok(russh::keys::check_known_hosts(&self.host, self.port, server_public_key)?)
        } else {
            Ok(true)
        }
    }
}

#[derive(Default)]
pub struct SftpParameters {
    pub private_key: Option<String>, 
    pub private_key_password: Option<String>, 
    pub validate_host: bool
}


impl TransportSftp {
    pub async fn new(base: String, host: String, password: Option<String>, user: String, port: u16, retry_limit: Option<usize>, params: SftpParameters) -> Result<Self> {
        // let base = if base == "/" { "".to_owned() } else { base };

        debug!("SFTP to {user}@{host}:{port}{base}; password ({}), private_key ({})", password.is_some(), params.private_key.is_some());

        let config = Arc::new(russh::client::Config::default());
        let handler = ConnectionHandler{ 
            validate_host: params.validate_host,
            host: host.clone(),
            port
        };
        let mut session = russh::client::connect(config, (host.clone(), port), handler).await?;

        if let Some(password) = password {
            session.authenticate_password(&user, password).await?;
        }

        if let Some(key) = params.private_key {
            let mut private_key = russh::keys::PrivateKey::from_openssh(key)?;
            if let Some(pass) = params.private_key_password {
                if private_key.is_encrypted() {
                    private_key = private_key.decrypt(pass)?;
                }
            }

            let hash_alg = match session.best_supported_rsa_hash().await? {
                Some(Some(hash)) => Some(hash),
                Some(None) => bail!("No supported hash for private key login"), // Server does tell us, it doesn't support anything
                None => None, // server doesn't tell us anything, might still be fine
            };

            session.authenticate_publickey(&user, PrivateKeyWithHashAlg::new(Arc::new(private_key), hash_alg)).await?;
        }
        
        let channel = session.channel_open_session().await.unwrap();
        channel.request_subsystem(true, "sftp").await.unwrap();
        let sftp = SftpSession::new(channel.into_stream()).await.unwrap();

        Ok(Self {
            client: sftp,
            base,
            host,
            port,
            user,
            retry_limit,
        })
    }

}

impl TransportSftp {
    fn normalize(&self, path: &str) -> Result<String> {
        // If they've provided an absolute path. Leave it a is.
        let s = String::from(".") + &if path.starts_with('/') {
            PathBuf::from_str(path)?
        // Relative paths
        } else if path.contains('/') || path.len() != 64 {
            safe_path::scoped_join(&self.base, path)?
        } else {
            safe_path::scoped_join(&self.base, normalize_srl_path(path))?
        }.to_string_lossy();
        debug!("sftp normalized: {} -> {}", path, s);
        return Ok(s)
    }

    async fn make_dirs(&self, dest_path: &str) -> Result<()> {
        let mut dirs: Vec<&str> = dest_path.split("/").collect();
        dirs.pop(); // exclude the last path item as that is probably the file name
        let mut build_path = String::new();
        for dir in dirs {
            if dir.is_empty() { continue }
            build_path += dir;
            _ = self.client.create_dir(&build_path).await;
            build_path += "/";
        }
        Ok(())
    }

    async fn _put(&self, dest_path: &str, body: &Bytes) -> Result<()> {
        let dest_path = self.normalize(dest_path)?;
        self.make_dirs(&dest_path).await?;
        let mut handle = self.client.open_with_flags(&dest_path, OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE).await?;
        handle.write_all(body).await?;
        Ok(())
    }

    async fn _upload(&self, src: &mut tokio::fs::File, dest_path: &str) -> Result<()> {
        let dest_path = self.normalize(dest_path)?;
        self.make_dirs(&dest_path).await?;
        let mut dest = self.client.open_with_flags(dest_path, OpenFlags::WRITE | OpenFlags::CREATE | OpenFlags::TRUNCATE).await?;

        src.seek(std::io::SeekFrom::Start(0)).await?;
        let mut buffer = vec![0; 1 << 14];

        loop {
            let len = src.read(&mut buffer).await?;
            if len == 0 { break }
            dest.write_all(&buffer[0..len]).await?;
        }

        Ok(())
    }

    async fn _get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let path = self.normalize(path)?;
        match self.client.open_with_flags(path, OpenFlags::READ).await {
            Ok(mut file) => {
                let mut buffer = vec![];
                file.read_to_end(&mut buffer).await?;
                Ok(Some(buffer))
            },
            Err(err) => {
                if let Error::Status(status) = &err {
                    if status.status_code == StatusCode::NoSuchFile {
                        return Ok(None)
                    }
                }
                return Err(err.into())
            },
        }
    }

    async fn _exists(&self, path: &str) -> Result<bool> {
        let path = self.normalize(path)?;
        println!("_exists: {path}");
        Ok(self.client.try_exists(path).await?)
    }

    async fn _delete(&self, path: &str) -> Result<()> {
        let path = self.normalize(path)?;
        match self.client.remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) => {
                if let Error::Status(status) = &err {
                    if status.status_code == StatusCode::NoSuchFile {
                        return Ok(())
                    }
                }
                Err(err.into())
            },
        }
    }
}

#[async_trait]
impl Transport for TransportSftp {
    async fn put(&self, name: &str, body: &Bytes) -> Result<()> {
        retry!(self.retry_limit, self._put(name, body).await)
    }

    async fn upload(&self, src: &Path, dest: &str) -> Result<()> {
        let mut src = tokio::fs::OpenOptions::new().read(true).create(false).open(src).await?;
        retry!(self.retry_limit, self._upload(&mut src, dest).await)
    }

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        retry!(self.retry_limit, self._get(name).await)
    }
    
    async fn exists(&self, name: &str) -> Result<bool> {
        retry!(self.retry_limit, self._exists(name).await)
    }

    async fn stream(&self, path: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)> {
        let path = self.normalize(path)?;
        let mut file = self.client.open_with_flags(path, OpenFlags::READ).await?;
        let metadata = file.metadata().await?;

        let (output_stream, channel) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            loop {
                let mut buffer = BytesMut::zeroed(BUFFER_SIZE);
                let len = match file.read(&mut buffer[..]).await {
                    Ok(0) => break,
                    Ok(len) => len,
                    Err(err) => {
                        _ = output_stream.send(Err(std::io::Error::other(err))).await;
                        break
                    },
                };

                buffer.resize(len, 0);
                _ = output_stream.send(Ok(buffer.freeze())).await;
            }
        });

        Ok((metadata.len(), channel))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        retry!(self.retry_limit, self._delete(name).await)
    }
}




macro_rules! retry {
    ($retry_limit: expr, $body: expr) => {
        {
            let mut backoff = MIN_BACKOFF;
            let mut retries = 0;
            loop {
                if let Some(limit) = $retry_limit {
                    if retries > limit {
                        break Err(anyhow::Error::from(crate::errors::ConnectionError))
                    }
                }

                let ret_val = $body;
                retries += 1;

                match ret_val {
                    Ok(value) => {
                        if retries > 1 {
                            log::info!("Reconnected to SFTP Transport!")
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

