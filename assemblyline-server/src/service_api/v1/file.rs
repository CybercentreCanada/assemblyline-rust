// import os
// import shutil
// import tempfile
// from assemblyline_core.tasking_client import TaskingClientException

// from flask import request

// from assemblyline.filestore import FileStoreException
// from assemblyline_service_server.helper.response import make_api_response, stream_file_response
// from assemblyline_service_server.api.base import make_subapi_blueprint, api_login
// from assemblyline_service_server.config import FILESTORE, LOGGER, TASKING_CLIENT


use std::sync::Arc;

use log::{error, info, warn};
use poem::http::{HeaderMap, StatusCode};
use poem::web::{Data, Multipart, Path};
use poem::{get, handler, put, Body, Endpoint, EndpointExt, Result, Response, Route};
use serde_json::json;
use tokio_stream::wrappers::ReceiverStream;

use crate::service_api::helpers::auth::{ClientInfo, ServiceAuth};
use crate::service_api::helpers::tasking::TaskingClient;
use crate::service_api::helpers::{copy_to_file, make_api_error, make_api_response, make_empty_api_error};
use crate::service_api::v1::require_header;
use crate::string_utils::safe_str;
use crate::Core;

// SUB_API = 'file'
// file_api = make_subapi_blueprint(SUB_API, api_version=1)
// file_api._doc = "Perform operations on file"
pub fn api(core: Arc<Core>) -> impl Endpoint {
    Route::new()
    .at("/:sha256", get(download_file))
    .at("/", put(upload_file))
    .with(ServiceAuth::new(core))
}


/// Download a file.
///
/// Variables:
/// sha256       => A resource locator for the file (sha256)
///
/// Arguments:
/// None
///
/// Data Block:
/// None
///
/// API call example:
/// GET /api/v1/file/123456...654321/
///
/// Result example:
/// <THE FILE BINARY>
#[handler]
async fn download_file(Path(sha256): Path<String>, client_info: Data<&ClientInfo>, core: Data<&Arc<Core>>) -> Result<Response> {
    match core.filestore.stream(&sha256).await {
        Ok((size, stream)) => {
            let body = Body::from_bytes_stream(ReceiverStream::new(stream));
            let filename = format!("UTF-8''{}", urlencoding::encode(&safe_str(&sha256)));
            Ok(Response::builder()
                .content_type("application/octet-stream")
                .header("Content-Length", size.to_string())
                .header("Content-Disposition", format!("attachment; filename=file.bin; filename*={filename}"))
                .body(body))
        }, 
        Err(err) => {
            error!("[{}] {} couldn't find file {sha256} requested by service: {err}", 
                client_info.client_id, client_info.service_name);
            Err(make_empty_api_error(StatusCode::NOT_FOUND, "The file was not found in the system."))
        }
    }
}

/// Upload a single file.
///
/// Variables:
/// None
///
/// Arguments:
/// None
///
/// Data Block:
/// None
///
/// Files:
/// Multipart file obj stored in the "file" key.
///
/// API call example:
/// PUT /api/v1/file/
///
/// Result example:
/// {"success": true}
#[handler]
async fn upload_file(
    client_info: Data<&ClientInfo>, 
    tasking: Data<&Arc<TaskingClient>>,
    headers: &HeaderMap,
    multipart_body: Option<Multipart>,
    stream_body: Option<poem::Body>
) -> Result<Response> {
    let sha256 = require_header!(headers, "sha256");
    let classification = require_header!(headers, "classification");
    let ttl: u32 = match require_header!(headers, "ttl").parse() {
        Ok(value) => value,
        Err(_) => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, "Could not parse ttl header as a number"))
    };
    let is_section_image = require_header!(headers, "is-section-image", "false").trim().to_ascii_lowercase() == "true";
    let is_supplementary = require_header!(headers, "is-supplementary", "false").trim().to_ascii_lowercase() == "true";

    let temp_file = match multipart_body {
        Some(mut body) => {
            loop {
                let field = match body.next_field().await {
                    Ok(Some(field)) => field,
                    Ok(None) => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, "expected multipart with file named 'file'")),
                    Err(err) => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, &format!("Error reading multipart body: {err}"))),
                };

                if field.file_name() != Some("file") {
                    continue
                }

                let body = field.into_async_read();
                break copy_to_file(body).await
            }
        },
        None => {
            match stream_body {
                Some(stream) => copy_to_file(stream.into_async_read()).await,
                None => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, "expected a file upload in body")),
            }
        }
    };

    let temp_file = match temp_file {
        Ok(file) => file,
        Err(err) => return Err(make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Could not move file to temporary storage: {err}"))),
    };

    let upload_result = tasking.upload_file(temp_file.path(), classification, ttl, is_section_image, is_supplementary, Some(sha256.to_owned())).await;

    if let Err(err) = upload_result {
        warn!("{} - {}: {err}", client_info.client_id, client_info.service_name);
        return Err(make_api_error(StatusCode::BAD_REQUEST, &err.to_string(), json!({"success": false})));
    }

    info!("{} - {}: Successfully uploaded file (SHA256: {sha256})", 
        client_info.client_id, client_info.service_name);

    return Ok(make_api_response(json!({"success": true})))
}

