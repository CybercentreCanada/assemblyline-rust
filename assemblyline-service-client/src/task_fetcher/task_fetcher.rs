use std::path::PathBuf;

use anyhow::Result;
use assemblyline_models::{messages::task::Task, types::Sha256};

use crate::{connection::Connection, types::errors::ServiceHandlerError};

pub trait TaskFetcher {
    fn get_task(
        &self,
        connection: &Connection,
    ) -> impl std::future::Future<Output = Result<Option<Task>, ServiceHandlerError>> + Send;

    fn download_file(
        &self,
        sha256: Sha256,
        task_dir: PathBuf,
        con: &Connection,
    ) -> impl std::future::Future<Output = Result<PathBuf, ServiceHandlerError>> + Send;
}
