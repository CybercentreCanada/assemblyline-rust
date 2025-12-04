use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use log::debug;
use russh::client::Handler;
use russh::keys::PrivateKeyWithHashAlg;
use russh_sftp::client::SftpSession;
use russh_sftp::client::error::Error;
use russh_sftp::protocol::{OpenFlags, Status, StatusCode};
// use rusftp::client::SftpClient;
// use rusftp::message::PFlags;
// use rusftp::russh::client::Handler;
// use rusftp::russh;
// use rusftp::russh::keys::key::KeyPair;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::transport::{Transport, normalize_srl_path};

const MIN_BACKOFF: Duration = Duration::from_millis(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const BUFFER_SIZE: usize = 1 << 14;

// import logging
// import os
// import posixpath
// import tempfile
// import warnings


// # Stop Blowfish deprecation warning
// with warnings.catch_warnings():
//     warnings.simplefilter("ignore")

//     import pysftp

// from io import BytesIO
// from paramiko import SSHException

// from assemblyline.common.exceptions import ChainAll
// from assemblyline.common.uid import get_random_id
// from assemblyline.filestore.transport.base import Transport, TransportException, normalize_srl_path


// def reconnect_retry_on_fail(func):
//     def new_func(self, *args, **kwargs):
//         with warnings.catch_warnings():
//             warnings.simplefilter("ignore")

//             try:
//                 if not self.sftp:
//                     self.sftp = pysftp.Connection(self.host,
//                                                   username=self.user,
//                                                   password=self.password,
//                                                   private_key=self.private_key,
//                                                   private_key_pass=self.private_key_pass,
//                                                   port=self.port,
//                                                   cnopts=self.cnopts)
//                 return func(self, *args, **kwargs)
//             except SSHException:
//                 pass

//             # The previous attempt at calling original func failed.
//             # Reset the connection and try again (one time).
//             if self.sftp:
//                 self.sftp.close()   # Just best effort.

//             # The original func will reconnect automatically.
//             self.sftp = pysftp.Connection(self.host,
//                                           username=self.user,
//                                           password=self.password,
//                                           private_key=self.private_key,
//                                           private_key_pass=self.private_key_pass,
//                                           cnopts=self.cnopts)
//             return func(self, *args, **kwargs)

//     new_func.__name__ = func.__name__
//     new_func.__doc__ = func.__doc__
//     return new_func



// @ChainAll(TransportException)
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

impl TransportSftp {
    pub async fn new(base: String, host: String, password: Option<String>, user: String, port: u16, private_key: Option<String>, private_key_pass: Option<String>, validate_host: bool, retry_limit: Option<usize>) -> Result<Self> {
        // let base = if base == "/" { "".to_owned() } else { base };

        debug!("SFTP to {user}@{host}:{port}{base}; password ({}), private_key ({})", password.is_some(), private_key.is_some());

        let config = Arc::new(russh::client::Config::default());
        let handler = ConnectionHandler{ 
            validate_host,
            host: host.clone(),
            port
        };
        let mut session = russh::client::connect(config, (host.clone(), port), handler).await?;

        if let Some(password) = password {
            session.authenticate_password(&user, password).await?;
        }

        if let Some(key) = private_key {
            let mut private_key = russh::keys::PrivateKey::from_openssh(key)?;
            if let Some(pass) = private_key_pass {
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
        

//         self.host = host
//         self.port = int(port or 22)
//         self.password = password
//         self.user = user
//         self.private_key = private_key
//         self.private_key_pass = private_key_pass
//         if not validate_host:
//             self.cnopts = pysftp.CnOpts()
//             self.cnopts.hostkeys = None
//         else:
//             self.cnopts = None

//         # Connect on create
//         self.sftp = pysftp.Connection(self.host,
//                                       username=self.user,
//                                       password=self.password,
//                                       private_key=self.private_key,
//                                       private_key_pass=self.private_key_pass,
//                                       port=self.port,
//                                       cnopts=self.cnopts)

        Ok(Self {
            client: sftp,
            base,
            host,
            port,
            user,
            retry_limit,
        })
    }

//     def close(self):
//         if self.sftp:
//             self.sftp.close()

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

