//! Task uploader that uploads task errors and results to service server
use std::collections::HashMap;

use crate::{
    constants::{
        DEFAULT_SERVICE_ERROR_MESSAGE, RECOVERABLE_ERROR_STATUS, UNKNOWN_SERVICE_ERROR_TYPE,
    },
    types::{
        errors::ServiceClientError,
        task::{ErrorBody, ErrorResponse, TaskUploadBody},
        ServiceInfo,
    },
};

use anyhow::Result;
use assemblyline_models::{
    messages::{service_api, task::Task},
    types::Sha256,
};
use assemblyline_utilities::{
    connection::{convert_api_output_obj, convert_output_map, Body, Connection},
    types::response::TaskUploadResponse,
};
use log::{debug, info, warn};
use serde_json::Value;

use tokio_util::codec::{BytesCodec, FramedRead};

/// Task uploader for uploading task files to service server
pub struct TaskUploader {}

/// Error data to upload to service server
pub struct TaskErrorData {
    sha256: Sha256,
    service_info: ServiceInfo,
    error_json: Option<Value>,
    message: Option<String>,
    error_type: Option<String>,
    status: Option<String>,
}

impl TaskErrorData {
    /// create new default task error object
    pub fn new(sha256: Sha256, service_info: ServiceInfo) -> Self {
        TaskErrorData {
            sha256,
            service_info,
            error_json: None,
            message: None,
            error_type: None,
            status: None,
        }
    }

    /// set error json
    pub fn set_error_json(&mut self, error_json: Value) -> &mut Self {
        self.error_json = Some(error_json);
        self
    }

    /// set error message
    pub fn set_message(&mut self, message: String) -> &mut Self {
        self.message = Some(message);
        self
    }

    /// set error type
    pub fn set_error_type(&mut self, error_type: String) -> &mut Self {
        self.error_type = Some(error_type);
        self
    }

    /// set error status
    pub fn set_status(&mut self, status: String) -> &mut Self {
        self.status = Some(status);
        self
    }

    /// convert task error data to json body
    pub fn to_json_body(self) -> Result<Value> {
        Ok(self.error_json.unwrap_or({
            let error_body = ErrorBody {
                sha256: self.sha256,
                error_type: self
                    .error_type
                    .unwrap_or(UNKNOWN_SERVICE_ERROR_TYPE.to_string()),
                response: ErrorResponse {
                    service_name: self.service_info.name,
                    service_version: self.service_info.version,
                    service_tool_version: None,
                    status: self.status.unwrap_or(RECOVERABLE_ERROR_STATUS.to_string()),
                    message: self
                        .message
                        .unwrap_or(DEFAULT_SERVICE_ERROR_MESSAGE.to_string()),
                },
            };
            serde_json::to_value(error_body)?
        }))
    }
}

impl TaskUploader {
    /// Upload task error to service server
    /// :param task: Task object
    /// :param connection: Connection client to the service server
    /// :param error_data: Task error data
    /// :return: Result of task fetch. Either a task is returned or no task is found
    pub async fn upload_task_error(
        &self,
        task: &Task,
        connection: &Connection,
        error_data: TaskErrorData,
    ) -> Result<(), ServiceClientError> {
        let task_upload_body = TaskUploadBody {
            task: task.clone(),
            result: None,
            freshen: false,
            error: Some(error_data.to_json_body()?),
        };

        let task_error_url = connection.get_api_path("task", &[])?;

        let _ = connection
            .post(
                task_error_url,
                Body::Json(task_upload_body),
                None,
                convert_output_map,
            )
            .await;

        Ok(())
    }

    /// Upload task result and result files to service server
    /// :param task: Task object
    /// :param task_done_result: Task result data
    /// :param connection: Connection client to the service server
    /// :return: Result of task fetch. Either a task is returned or no task is found
    pub async fn upload_task_result(
        &self,
        task: &Task,
        task_done_result: service_api::result::Result,
        connection: &Connection,
    ) -> Result<(), ServiceClientError> {
        info!(" [{}]-sid[{}] upload task result.", task.task_id, task.sid);

        let mut file_map: HashMap<Sha256, service_api::result::File> = task_done_result
            .response
            .supplementary
            .iter()
            .map(|r| (r.sha256.to_owned(), r.to_owned()))
            .collect();

        file_map.extend(
            task_done_result
                .response
                .extracted
                .iter()
                .map(|r| (r.sha256.to_owned(), r.to_owned())),
        );

        let mut request_body = TaskUploadBody {
            task: task.clone(),
            result: Some(serde_json::to_value(task_done_result)?),
            freshen: true, // ask service-api to check in database for missing files
            error: None,
        };

        debug!(
            "[{}]-sid[{}] Uploading task results.",
            task.task_id, task.sid
        );
        let task_result_url = connection.get_api_path("task", &[])?;

        let result_response: TaskUploadResponse = connection
            .post(
                task_result_url.clone(),
                Body::Json(request_body.clone()),
                None,
                convert_api_output_obj,
            )
            .await?;

        if !result_response.success && result_response.missing_files.is_none() {
            return Err(ServiceClientError::Default(
                "Invalid state. Result response should NOT be successful with missing files "
                    .to_string(),
            ));
        }

        let file_upload_url = connection.get_api_path("file", &[])?;

        if let Some(missing_files) = result_response.missing_files {
            for file_sha in missing_files {
                debug!(
                    "[{}]-sid[{}] Uploading missing file: {}",
                    task.task_id, task.sid, file_sha
                );
                if let Some(upload_file) = file_map.get(&file_sha) {
                    let mut headers: HashMap<String, String> = HashMap::new();

                    info!(
                        "Trying to upload file: {:?}",
                        upload_file.sha256.to_string()
                    );

                    headers.insert("Sha256".to_string(), upload_file.sha256.to_string());
                    headers.insert(
                        "Classification".to_string(),
                        upload_file.classification.to_string(),
                    );
                    headers.insert("Ttl".to_string(), task.ttl.to_string());
                    headers.insert(
                        "Is-Section-Image".to_string(),
                        upload_file.is_section_image.to_string(),
                    );
                    headers.insert(
                        "Is-Supplementary".to_string(),
                        upload_file.is_supplementary.to_string(),
                    );

                    let file = tokio::fs::File::open(upload_file.path.clone()).await?;
                    let stream = FramedRead::new(file, BytesCodec::new());
                    let stream_body = reqwest::Body::wrap_stream(stream);

                    let _ = connection
                        .put(
                            file_upload_url.clone(),
                            Body::<()>::Prepared(stream_body),
                            Some(headers),
                            convert_output_map,
                        )
                        .await?;
                } else {
                    warn!(
                        "Service server requesting file sha {} that is not part of the task.",
                        file_sha
                    );
                }
            }

            // finished uploading files. Upload task again with freshen = false to tell service-api there shouldn't be any missing files
            request_body.freshen = false;
            let _ = connection
                .post(
                    task_result_url.clone(),
                    Body::Json(request_body.clone()),
                    None,
                    convert_output_map,
                )
                .await?;
        }

        info!(
            "[{}]-sid[{}] finishing uploading task result and missing files for task.",
            task.task_id, task.sid
        );

        Ok(())
    }
}
