use std::path::Path;

use async_trait::async_trait;
use anyhow::Result;
use bytes::Bytes;

pub mod local;
pub mod azure;

#[async_trait]
pub trait Transport: Send + Sync + std::fmt::Debug {

    async fn put(&self, name: &str, body: &Bytes) -> Result<()>;
    async fn upload(&self, path: &Path, name: &str) -> Result<()>;

    async fn get(&self, name: &str) -> Result<Option<Vec<u8>>>;
    async fn exists(&self, name: &str) -> Result<bool>;
    async fn download(&self, name: &str, path: &Path) -> Result<()>;
    async fn stream(&self, name: &str) -> Result<(u64, tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>)>;

    async fn delete(&self, name: &str) -> Result<()>;
}

