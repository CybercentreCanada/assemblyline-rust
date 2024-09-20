use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use anyhow::{bail, Context, Result};
use log::error;


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

    pub async fn get(&self, name: &str) -> Result<Vec<u8>> {
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
            None => bail!("All transports failed to fetch [{name}]")
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

}

#[async_trait]
trait Transport: Send + Sync {
    async fn put(&self, name: &str, body: &[u8]) -> Result<()>;
    async fn get(&self, name: &str) -> Result<Vec<u8>>;
    async fn download(&self, name: &str, path: &Path) -> Result<()>;
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

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = self.make_path(name)?;
        Ok(tokio::fs::read(path).await?)
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
        self.put(name, &self.get(name).await?).await
    }
}