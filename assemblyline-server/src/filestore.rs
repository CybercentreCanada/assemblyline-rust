use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use anyhow::{bail, Context, Result};
use bytes::Bytes;


pub struct FileStore {
    transports: Vec<Box<dyn Transport>>,
}


impl FileStore {
    
    pub async fn open(urls: &[String]) -> Result<Arc<FileStore>> {
        let mut transports = vec![];
        for url in urls {
            transports.push(Self::create_transport(url)?)
        }
        Ok(Arc::new(Self { 
            transports 
        }))
    }

    fn create_transport(address: &str) -> Result<Box<dyn Transport>> {
        let url: url::Url = address.parse()?;
        match url.scheme() {
            "file" => {
                if url.has_host() {
                    bail!("Local file connections can't specify a host.");
                }
                let path = url.path().parse()?;
                Ok(Box::new(LocalTransport::new(path)))
            }
            _ => {
                bail!("Not an accepted filestore scheme: {}", url.scheme());
            }
        }
    }

    pub async fn put(&self, name: &str, body: &[u8]) -> Result<()> {
        for transport in &self.transports {
            transport.put(name, body).await?;
        }
        Ok(())
    }

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

    pub async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let mut last_error = None;
        for transport in &self.transports {
            match transport.get(name).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
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

    pub async fn download(&self, name: &str, path: &Path) -> Result<()> {
        let mut last_error = None;
        for transport in &self.transports {
            match transport.download(name, path).await {
                Ok(()) => return Ok(()),
                Err(err) => {
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

    pub async fn delete(&self, name: &str) -> Result<()> { 
        for transport in &self.transports {
            transport.delete(name).await?;
        }
        Ok(())
    }
}

#[async_trait]
trait Transport: Send + Sync {

    async fn put(&self, name: &str, body: &[u8]) -> Result<()>;
    async fn upload(&self, path: &Path, name: &str) -> Result<()>;

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>>;
    async fn exists(&self, name: &str) -> Result<bool>;
    async fn download(&self, name: &str, path: &Path) -> Result<()>;
    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)>;

    async fn delete(&self, name: &str) -> Result<()>;
}

struct LocalTransport {
    path: PathBuf,
}

impl LocalTransport {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn make_path(&self, mut name: &str) -> Result<PathBuf> {
        while let Some(tail) = name.strip_prefix("/") {
            name = tail;
        }

        let prefixed = if name.len() >= 4 {
            let mut chars = name.chars();
            let a = chars.next().unwrap();
            let b = chars.next().unwrap();
            let c = chars.next().unwrap();
            let d = chars.next().unwrap();
            &format!("{a}/{b}/{c}/{d}/{name}")
        } else {
            name
        };
        
        Ok(safe_path::scoped_join(&self.path, prefixed)?)
    }
}

#[async_trait]
impl Transport for LocalTransport {
    async fn put(&self, name: &str, body: &[u8]) -> Result<()> {
        let path = self.make_path(name)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(tokio::fs::write(path, body).await?)
    }

    async fn upload(&self, source: &Path, name: &str) -> Result<()> {
        let path = self.make_path(name)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tokio::fs::hard_link(source, &path).await.is_ok() {
            return Ok(())
        }
        let body = tokio::fs::read(source).await?;
        tokio::fs::write(path, body).await?;
        Ok(())
    }

    async fn exists(&self, name: &str) -> Result<bool> {
        let path = self.make_path(name)?;
        Ok(tokio::fs::try_exists(path).await?)
    }

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let path = self.make_path(name)?;
        match tokio::fs::read(path).await {
            Ok(body) => Ok(Some(body)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into())
        }
    }

    async fn download(&self, name: &str, dest: &Path) -> Result<()> {
        let path = self.make_path(name)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        _ = tokio::fs::remove_file(dest).await;
        if tokio::fs::hard_link(path, dest).await.is_ok() {
            return Ok(())
        }
        match self.get(name).await? {
            Some(data) => self.put(name, &data).await,
            None => Err(std::io::Error::from(std::io::ErrorKind::NotFound).into())
        }
    }

    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)> {
        let path = self.make_path(name)?;
        let metadata = tokio::fs::metadata(&path).await?;
        let (send, recv) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let mut file = match std::fs::OpenOptions::new().read(true).open(path) {
                Ok(file) => file,
                Err(err) => {
                    _ = send.send(Err(err)).await;
                    return
                }
            };

            loop {
                let mut buf = vec![0u8; 1 << 14];
                let size = match file.read(&mut buf) {
                    Ok(size) => size,
                    Err(err) => {
                        _ = send.send(Err(err)).await;
                        return    
                    }
                };

                if size == 0 { break }                
                buf.truncate(size);
                
                if send.send(Ok(buf.into())).await.is_err() {
                    break
                }
            }
        });
        Ok((metadata.len(), recv))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let path = self.make_path(name)?;
        if let Err(err) = tokio::fs::remove_file(path).await {
            if err.kind() == ErrorKind::NotFound {
                return Ok(())
            }
            return Err(err.into())
        }
        Ok(())
    }
}