use assemblyline_models::{messages::task::Task, types::Sha256};
use assemblyline_utilities::connection::{convert_api_output_map, convert_output_stream, Connection};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Result;
use futures::StreamExt;

use log::{debug, info};
use sha2::Digest;

use tokio::{fs::File, io::AsyncWriteExt};

use crate::{constants::DEFAULT_REQUEST_TASK_TIMEOUT, task_fetcher::task_fetcher::TaskFetcher, types::errors::ServiceClientError};
use std::io::Write;

#[derive(Debug, Clone)]
pub struct SingleThreadTaskFetcher {}

impl SingleThreadTaskFetcher {
    /// Calculate the sha256 of a buffer
    pub fn sha256_data(body: &[u8]) -> String {
        let mut hasher = sha2::Sha256::default();
        hasher.write_all(body).unwrap();
        hex::encode(hasher.finalize())
    }
}

impl TaskFetcher for SingleThreadTaskFetcher {
    async fn get_task(
        &self,
        // file_required: bool,
        // tasking_dir: String,
        con: &Connection,
    ) -> Result<Option<Task>, ServiceClientError> {
        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert("Timeout".to_string(), DEFAULT_REQUEST_TASK_TIMEOUT.to_string());
        let request_task_url = con.get_api_path("task", &[])?;

        info!("Requesting a task with {}s timeout...", DEFAULT_REQUEST_TASK_TIMEOUT);
        let data = con.get(request_task_url, Some(headers), convert_api_output_map).await?;

        if let Some(value) = data.get("task") {
            if value.is_boolean() {
                //service server returns a boolean when no task if found
                return Ok(None);
            } else if value.is_object() {
                let task = serde_json::from_value::<Task>(value.clone())?;
                return Ok(Some(task));
            }

            return Err(ServiceClientError::Default(
                "Unknown response from get_task. Should be boolean or task.".into(),
            ));
        } else {
            return Err(ServiceClientError::Default(
                "Error response from get_task. Cannot find key 'task' in response.".into(),
            ));
        }
    }

    async fn download_file(&self, sha256: Sha256, task_dir: PathBuf, con: &Connection) -> Result<PathBuf, ServiceClientError> {
        let donwload_file_url = con.get_api_path("file", &[sha256.to_string().as_str()])?;

        let mut data_stream = con.get(donwload_file_url, None, convert_output_stream).await?;

        let file_path = task_dir.join(Path::new(&sha256.to_string()));

        let mut out_file = File::create(&file_path).await?;
        let mut hasher = sha2::Sha256::new();

        while let Some(data) = data_stream.next().await {
            let chunk = &data?;
            out_file.write_all(chunk).await?;
            hasher.write_all(chunk)?;
        }

        let file_sha256: Sha256 = (&hasher.finalize()[..]).try_into()?;

        debug!("Download task file. File sha requested: {sha256} and content sha is: {file_sha256}");

        if file_sha256 != sha256 {
            return Err(ServiceClientError::FileHashMisMatch {
                requested_sha: sha256,
                content_sha: file_sha256,
            });
        }

        Ok(file_path)
    }
}
