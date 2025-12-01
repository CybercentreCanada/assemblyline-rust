use std::io::Write;
use std::path::Path;

use async_trait::async_trait;
use anyhow::{Context, Result};
use bytes::Bytes;

pub mod local;
pub mod azure;
pub mod s3;
pub mod ftp;

#[async_trait]
pub trait Transport: Send + Sync + std::fmt::Debug {

    async fn put(&self, name: &str, body: &Bytes) -> Result<()>;
    async fn upload(&self, path: &Path, name: &str) -> Result<()>;

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>>;
    async fn exists(&self, name: &str) -> Result<bool>;
    async fn download(&self, name: &str, dest: &Path) -> Result<()> {
        // create dst_path if it doesn't exist
        if let Some(parent) = dest.parent() {
            if !tokio::fs::try_exists(parent).await.context("download::try_exists")? {
                tokio::fs::create_dir_all(parent).await.context("download::create_dir_all")?;
            }
        }

        // download the key from azure
        let (_, mut stream) = self.stream(name).await.context("download::stream")?;
        let dest = dest.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut file = std::fs::File::options().write(true).truncate(true).create(true).open(dest)?;
            while let Some(data) = stream.blocking_recv() {
                file.write_all(&data?)?;
            }
            return anyhow::Ok(())
        }).await?
    }
    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)>;

    async fn delete(&self, name: &str) -> Result<()>;
}

fn normalize_srl_path(path: &str) -> String {
    if path.contains("/") {
        return path.to_owned()
    }

    if path.len() > 4 {
        let mut chars = path.chars();
        let a = chars.next().unwrap();
        let b = chars.next().unwrap();
        let c = chars.next().unwrap();
        let d = chars.next().unwrap();
        format!("{a}/{b}/{c}/{d}/{path}")
    } else {
        path.to_string()
    }
}