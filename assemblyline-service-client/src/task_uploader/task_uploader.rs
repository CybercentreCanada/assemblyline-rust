use std::collections::HashMap;

use crate::{
    connection::{Body, Connection},
    constants::{
        DEFAULT_SERVICE_ERROR_MESSAGE, RECOVERABLE_ERROR_STATUS, SUPPORTED_API,
        UNKNOWN_SERVICE_ERROR_TYPE,
    },
    types::{
        errors::ServiceHandlerError,
        response::{APIResponse, TaskUploadResponse},
        task::{ErrorBody, ErrorResponse, TaskUploadBody},
    },
};

use anyhow::Result;
use assemblyline_models::{
    datastore::Service,
    messages::{service_api, task::Task},
    types::Sha256,
};
use log::{debug, info, warn};
use serde_json::Value;

use tokio_util::codec::{BytesCodec, FramedRead};

pub struct TaskUploader {}

impl TaskUploader {
    pub async fn upload_task_error(
        &self,
        task: &Task,
        service: &Service,
        connection: &Connection,
        error_json: Option<Value>,
        message: Option<String>,
        error_type: Option<String>,
        status: Option<String>,
    ) -> Result<(), ServiceHandlerError> {
        let error_data = error_json.unwrap_or({
            let error_body = ErrorBody {
                sha256: task.fileinfo.sha256.clone(),
                error_type: error_type.unwrap_or(UNKNOWN_SERVICE_ERROR_TYPE.to_string()),
                response: ErrorResponse {
                    message: message.unwrap_or(DEFAULT_SERVICE_ERROR_MESSAGE.to_string()),
                    service_name: service.name.clone(),
                    service_version: service.version.clone(),
                    service_tool_version: None,
                    status: status.unwrap_or(RECOVERABLE_ERROR_STATUS.to_string()),
                },
            };

            serde_json::to_value(error_body)?
        });

        let task_upload_body = TaskUploadBody {
            task: task.clone(),
            result: None,
            freshen: false,
            error: Some(error_data),
        };

        let task_error_url = connection.get_api_path(SUPPORTED_API, "task", &[])?;

        let _ = connection
            .request(
                reqwest::Method::POST,
                task_error_url,
                Body::Json(serde_json::to_value(task_upload_body)?),
                None,
                None,
                None,
            )
            .await?;

        Ok(())
    }

    pub async fn upload_task_result(
        &self,
        task: &Task,
        task_done_result: service_api::result::Result,
        connection: &Connection,
    ) -> Result<(), ServiceHandlerError> {
        info!(
            " [{}]-sid[{}] upload task result.",
            &task.task_id, &task.sid
        );

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
            &task.task_id, &task.sid
        );
        let task_result_url = connection.get_api_path(SUPPORTED_API, "task", &[])?;

        let response = connection
            .request(
                reqwest::Method::POST,
                task_result_url.clone(),
                Body::Json(serde_json::to_value(request_body.clone())?),
                None,
                None,
                None,
            )
            .await?;

        let result_response = response.json::<APIResponse<TaskUploadResponse>>().await?;

        if !result_response.api_response.success
            && result_response.api_response.missing_files.is_none()
        {
            return Err(ServiceHandlerError::Default(
                "Invalid state. Result response should NOT be successful with missing files "
                    .to_string(),
            ));
        }

        let file_upload_url = connection.get_api_path(SUPPORTED_API, "file", &[])?;

        if let Some(missing_files) = result_response.api_response.missing_files {
            for file_sha in missing_files {
                debug!(
                    "[{}]-sid[{}] Uploading missing file: {}",
                    &task.task_id, &task.sid, &file_sha
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

                    // if this upload failed there is big issue. Need to upload error to server
                    let _ = connection
                        .request(
                            reqwest::Method::PUT,
                            file_upload_url.clone(),
                            Body::Prepared(stream_body),
                            None,
                            None,
                            Some(headers),
                        )
                        .await?;
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(1.0)).await;
                } else {
                    warn!(
                        "Service server requesting file sha {} that is not part of the task.",
                        file_sha
                    );
                    // SHOULD I ERROR OUT HERE?
                }
            }

            // finished uploading files. Upload task again with freshen = false to tell service-api there shouldn't be any
            // missing files
            request_body.freshen = false;
            let _ = connection
                .request(
                    reqwest::Method::POST,
                    task_result_url.clone(),
                    Body::Json(serde_json::to_value(request_body.clone())?),
                    None,
                    None,
                    None,
                )
                .await?;
        }

        info!(
            "[{}]-sid[{}] finishing uploading task result and missing files for task.",
            &task.task_id, &task.sid
        );

        Ok(())
    }
}
