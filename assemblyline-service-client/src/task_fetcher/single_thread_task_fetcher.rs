use assemblyline_models::{
    messages::task::Task,
    types::{JsonMap, Sha256},
};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use anyhow::Result;
use futures::StreamExt;

use log::debug;
use sha2::Digest;

use tokio::{fs::File, io::AsyncWriteExt};

use crate::{
    connection::{self, Connection},
    constants::{DEFAULT_REQUEST_TASK_TIMEOUT, SUPPORTED_API},
    task_fetcher::task_fetcher::TaskFetcher,
    types::{errors::ServiceHandlerError, response::APIResponse},
};
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
    ) -> Result<Option<Task>, ServiceHandlerError> {
        let mut headers: HashMap<String, String> = HashMap::new();
        headers.insert(
            "Timeout".to_string(),
            DEFAULT_REQUEST_TASK_TIMEOUT.to_string(),
        );
        let request_task_url = con.get_api_path(SUPPORTED_API, "task", &[])?;

        let response = con
            .request(
                reqwest::Method::GET,
                request_task_url,
                connection::Body::None,
                None,
                None,
                Some(headers),
            )
            .await?;

        let data = response.json::<APIResponse<JsonMap>>().await?;

        if let Some(value) = data.api_response.get("task") {
            if value.is_boolean() {
                //service server returns a boolean when no task if found
                return Ok(None);
            } else if value.is_object() {
                let task = serde_json::from_value::<Task>(value.clone())?;
                return Ok(Some(task));
            }

            return Err(ServiceHandlerError::Default(
                "Unknown response from get_task. Should be boolean or task.".into(),
            ));
        } else {
            return Err(ServiceHandlerError::Default(
                "Error response from get_task. Cannot find key 'task' in response.".into(),
            ));
        }
    }

    async fn download_file(
        &self,
        sha256: Sha256,
        task_dir: PathBuf,
        con: &Connection,
    ) -> Result<PathBuf, ServiceHandlerError> {
        let donwload_file_url =
            con.get_api_path(SUPPORTED_API, "file", &[sha256.to_string().as_str()])?;

        let response = con
            .request(
                reqwest::Method::GET,
                donwload_file_url,
                connection::Body::None,
                None,
                None,
                None,
            )
            .await?;

        let mut data_stream = response.bytes_stream();
        // os.path.join(self.tasking_dir, sha256)

        let file_path = task_dir.join(Path::new(&sha256.to_string()));

        let mut out_file = File::create(&file_path).await?;
        let mut hasher = sha2::Sha256::new();

        while let Some(data) = data_stream.next().await {
            let chunk = &data?;
            out_file.write_all(chunk).await?;
            hasher.write_all(chunk)?;
        }

        let file_sha256: Sha256 = (&hasher.finalize()[..]).try_into()?;

        debug!(
            "Download task file. File sha requested: {sha256} and content sha is: {file_sha256}"
        );

        if file_sha256 != sha256 {
            return Err(ServiceHandlerError::FileHashMisMatch {
                requested_sha: sha256,
                content_sha: file_sha256,
            });
        }

        Ok(file_path)
    }
}
