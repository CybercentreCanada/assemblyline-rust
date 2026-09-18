use std::path::PathBuf;

use anyhow::Result;
use assemblyline_models::{messages::task::Task, types::Sha256};
use assemblyline_utilities::connection::Connection;

use crate::types::errors::ServiceClientError;

pub trait TaskFetcher {
    fn get_task(&self, connection: &Connection) -> impl std::future::Future<Output = Result<Option<Task>, ServiceClientError>> + Send;

    fn download_file(
        &self,
        sha256: Sha256,
        task_dir: PathBuf,
        con: &Connection,
    ) -> impl std::future::Future<Output = Result<PathBuf, ServiceClientError>> + Send;
}
