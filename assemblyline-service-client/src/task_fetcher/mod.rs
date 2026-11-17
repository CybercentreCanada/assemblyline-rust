//! Module for task fetcher that requests tasks from service server
pub mod single_thread_task_fetcher;

use std::path::PathBuf;

use anyhow::Result;
use assemblyline_models::{messages::task::Task, types::Sha256};
use assemblyline_utilities::connection::Connection;

use crate::types::errors::ServiceClientError;

/// Interface for fetching tasks from the service server
pub trait TaskFetcher {
    /// Request task from service server
    /// :param connection: Connection client to the service server
    /// :return: Result of task fetch. Either a task is returned or no task is found
    fn get_task(
        &self,
        connection: &Connection,
    ) -> impl std::future::Future<Output = Result<Option<Task>, ServiceClientError>> + Send;

    /// Download task file from service server
    /// :param sha256: Sha256 of the file to download
    /// :param task_dir: The task directory to download the file to
    /// :return: The file path to the downloaded file
    fn download_file(
        &self,
        sha256: Sha256,
        task_dir: PathBuf,
        con: &Connection,
    ) -> impl std::future::Future<Output = Result<PathBuf, ServiceClientError>> + Send;
}
