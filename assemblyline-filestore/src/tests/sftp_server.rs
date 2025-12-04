
use std::collections::HashMap;
use std::io::{ErrorKind, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use log::info;
use parking_lot::Mutex;

use russh_sftp::protocol::{FileAttributes, Handle, OpenFlags, StatusCode};
use russh::{Channel, ChannelId};
use russh::server::{Auth, Msg, Server, Session};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone)]
struct SshServer {
    file_root: Arc<tempfile::TempDir>,
}

impl russh::server::Server for SshServer {
    type Handler = SshSession;

    fn new_client(&mut self, _: Option<SocketAddr>) -> Self::Handler {
        info!("New client");
        SshSession::new(self.file_root.path().to_owned())
    }    
}

struct SshSession {
    clients: Arc<Mutex<HashMap<ChannelId, Channel<Msg>>>>,
    directory: PathBuf,
}

impl SshSession {
    fn new(directory: PathBuf) -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            directory
        }
    }

    pub async fn get_channel(&mut self, channel_id: ChannelId) -> Channel<Msg> {
        let mut clients = self.clients.lock();
        clients.remove(&channel_id).unwrap()
    }
}

impl russh::server::Handler for SshSession {
    type Error = anyhow::Error;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        if user == "bozo" && password == "theclown" {
            info!("Bozo login accepted");
            Ok(Auth::Accept)
        } else {
            info!("Login refused");
            Ok(Auth::reject())
        }
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &russh::keys::PublicKey,
    ) -> Result<Auth, Self::Error> {
        info!("Reject key auth");
        Ok(Auth::reject())
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        info!("New session");
        let mut clients = self.clients.lock();
        clients.insert(channel.id(), channel);
        Ok(true)
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        // After a client has sent an EOF, indicating that they don't want
        // to send more data in this session, the channel can be closed.
        session.close(channel)?;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        info!("subsystem: {}", name);

        if name == "sftp" {
            let channel = self.get_channel(channel_id).await;
            let sftp = SftpSession {
                directory: self.directory.clone(),
                handles: Default::default(),
            };
            session.channel_success(channel_id)?;
            russh_sftp::server::run(channel.into_stream(), sftp).await;
        } else {
            session.channel_failure(channel_id)?;
        }

        Ok(())
    }
}

struct SftpSession {
    directory: PathBuf,
    handles: HashMap<String, tokio::fs::File>,
}


impl russh_sftp::server::Handler for SftpSession {
    type Error = Error;

    fn unimplemented(&self) -> Self::Error {
        "Unimplemented".into()
    }

    async fn open(&mut self, id: u32, filename: String, pflags: OpenFlags, attrs: FileAttributes) -> Result<Handle, Self::Error> {
        info!("Open {filename}");
        let path = safe_path::scoped_join(&self.directory, filename)?;
        let file = tokio::fs::OpenOptions::new()
            .read(pflags.contains(OpenFlags::READ))
            .write(pflags.contains(OpenFlags::WRITE))
            .append(pflags.contains(OpenFlags::APPEND))
            .create(pflags.contains(OpenFlags::CREATE))
            .truncate(pflags.contains(OpenFlags::TRUNCATE))
            .open(path).await?;

        let handle = Handle { id, handle: uuid::Uuid::new_v4().to_string() };
        self.handles.insert(handle.handle.clone(), file);
        return Ok(handle)
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<russh_sftp::protocol::Attrs, Self::Error> {
        info!("stat {path}");
        let path = safe_path::scoped_join(&self.directory, path)?;
        let meta = tokio::fs::metadata(path).await?;
        Ok(russh_sftp::protocol::Attrs {
            id,
            attrs: (&meta).into(),
        })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<russh_sftp::protocol::Attrs, Self::Error> {
        info!("fstat");
        let handle = match self.handles.get_mut(&handle) {
            Some(ok) => ok,
            None => {
                return Err("File handle invalid".into())
            }
        };
        let meta = handle.metadata().await?;
        Ok(russh_sftp::protocol::Attrs {
            id,
            attrs: (&meta).into(),
        })        
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<russh_sftp::protocol::Status, Self::Error> {
        info!("stat {filename}");
        let path = safe_path::scoped_join(&self.directory, filename)?;
        tokio::fs::remove_file(path).await?;
        Ok(russh_sftp::protocol::Status{
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn mkdir(&mut self, id: u32, path: String, attrs: FileAttributes) -> Result<russh_sftp::protocol::Status, Self::Error> {
        info!("mkdir {path}");
        let path = safe_path::scoped_join(&self.directory, path)?;
        tokio::fs::create_dir(path).await?;
        Ok(russh_sftp::protocol::Status{
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn write(&mut self, id: u32, handle: String, offset: u64, data: Vec<u8>) -> Result<russh_sftp::protocol::Status, Self::Error> {
        info!("write");
        let handle = match self.handles.get_mut(&handle) {
            Some(ok) => ok,
            None => {
                return Err("File handle invalid".into())
            }
        };
        handle.seek(std::io::SeekFrom::Start(offset)).await?;
        handle.write_all(&data).await?;
        Ok(russh_sftp::protocol::Status{
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn read(&mut self, id: u32, handle: String, offset: u64, len: u32) -> Result<russh_sftp::protocol::Data, Self::Error> {
        info!("read");
        let handle = match self.handles.get_mut(&handle) {
            Some(ok) => ok,
            None => {
                return Err("File handle invalid".into())
            }
        };
        let mut buffer = vec![0u8; (len as usize).min(512)];
        handle.seek(std::io::SeekFrom::Start(offset)).await?;
        let len = handle.read(&mut buffer[..]).await?;
        buffer.resize(len, 0);
        Ok(russh_sftp::protocol::Data{
            id,
            data: buffer
        })        
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<russh_sftp::protocol::Status, Self::Error> {
        info!("Close");
        let handle = match self.handles.remove(&handle) {
            Some(ok) => ok,
            None => {
                return Err("File handle invalid".into())
            }
        };
        handle.sync_all().await?;        
        Ok(russh_sftp::protocol::Status{
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })        
    }
}

struct Error(StatusCode, String);

impl From<Error> for StatusCode {
    fn from(val: Error) -> Self {
        val.0
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        if value.kind() == ErrorKind::NotFound {
            Error(StatusCode::NoSuchFile, value.to_string())
        } else {
            value.to_string().into()
        }
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Error(StatusCode::Failure, value.to_string())
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error(StatusCode::Failure, value)
    }
}


pub async fn start_temp_sftp_server() -> String {
    let config = russh::server::Config {
        keys: vec![
            russh::keys::PrivateKey::random(&mut russh::keys::ssh_key::rand_core::OsRng, russh::keys::Algorithm::Ed25519).unwrap(),
        ],
        ..Default::default()
    };
    
    let mut server = SshServer {
        file_root: Arc::new(tempfile::TempDir::new().unwrap())
    };

    let socket = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();

    tokio::spawn(async move {
        server.run_on_socket(Arc::new(config), &socket).await.unwrap();
    });

    format!("sftp://bozo:theclown@localhost:{port}")
}
