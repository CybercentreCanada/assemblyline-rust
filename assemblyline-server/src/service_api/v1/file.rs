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
use poem::{get, handler, put, Body, Endpoint, EndpointExt, Response, Route};
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
async fn download_file(Path(sha256): Path<String>, client_info: Data<&ClientInfo>, core: Data<&Arc<Core>>) -> Response {
    match core.filestore.stream(&sha256).await {
        Ok((size, stream)) => {
            let body = Body::from_bytes_stream(ReceiverStream::new(stream));
            let filename = format!("UTF-8''{}", urlencoding::encode(&safe_str(&sha256)));
            Response::builder()
                .content_type("application/octet-stream")
                .header("Content-Length", size.to_string())
                .header("Content-Disposition", format!("attachment; filename=file.bin; filename*={filename}"))
                .body(body)
        }, 
        Err(err) => {
            error!("[{}] {} couldn't find file {sha256} requested by service: {err}", 
                client_info.client_id, client_info.service_name);
            make_empty_api_error(StatusCode::NOT_FOUND, "The file was not found in the system.")
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
    sha256: Path<String>, 
    client_info: Data<&ClientInfo>, 
    // core: Data<&Arc<Core>>, 
    tasking: Data<&TaskingClient>,
    headers: &HeaderMap,
    mut body: Multipart
) -> Response {

    let sha256 = require_header!(headers, "sha256");
    let classification = require_header!(headers, "classification");
    let ttl: u32 = match require_header!(headers, "ttl").parse() {
        Ok(value) => value,
        Err(_) => return make_empty_api_error(StatusCode::BAD_REQUEST, "Could not parse ttl header as a number")
    };
    let is_section_image = require_header!(headers, "is_section_image", "false").trim().to_ascii_lowercase() == "true";
    let is_supplementary = require_header!(headers, "is_supplementary", "false").trim().to_ascii_lowercase() == "true";

    // Try reading multipart data from "files" or a single file post from stream
    // if request.content_type.startswith("multipart") {
    //     let file = request.files["file"];
    //     file.save(temp_file.name)
    // } else if request.stream.is_exhausted {
    //     if request.stream.limit == len(request.data) {
    //         temp_file.write(request.data)
    //     } else {
    //         return make_empty_api_error(StatusCode::BAD_REQUEST, "Cannot find the uploaded file...");
    //     }
    // } else {
    //     shutil.copyfileobj(request.stream, temp_file)
    // }
    let temp_file = loop {
        let field = match body.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => return make_empty_api_error(StatusCode::BAD_REQUEST, "expected multipart with file named 'file'"),
            Err(err) => return make_empty_api_error(StatusCode::BAD_REQUEST, &format!("Error reading multipart body: {err}")),
        };

        if field.file_name() != Some("file") {
            continue
        }

        let body = field.into_async_read();
        break match copy_to_file(body).await {
            Ok(file) => file,
            Err(err) => return make_empty_api_error(StatusCode::INTERNAL_SERVER_ERROR, &format!("Could not move file to temporary storage: {err}")),
        };
    };

    let upload_result = tasking.upload_file(temp_file.path(), classification, ttl, is_section_image, is_supplementary, Some(sha256.to_owned())).await;

    if let Err(err) = upload_result {
        warn!("{} - {}: {err}", client_info.client_id, client_info.service_name);
        return make_api_error(StatusCode::BAD_REQUEST, &err.to_string(), json!({"success": false}));
    }

    info!("{} - {}: Successfully uploaded file (SHA256: {sha256})", 
        client_info.client_id, client_info.service_name);

    return make_api_response(json!({"success": true}))
}

