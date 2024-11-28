use std::io::Write;

use anyhow::Result;
use poem::http::StatusCode;
use poem::web::Json;
use poem::IntoResponse;
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tokio::io::{AsyncRead, AsyncReadExt};

pub mod auth;
pub mod badlist;
pub mod tasking;
pub mod metrics;

pub fn get_version() -> &'static String {
    todo!()
    // VERSION = os.environ.get('ASSEMBLYLINE_VERSION', f"{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{BUILD_MINOR}.dev0")
}

#[derive(Debug, Serialize, Deserialize)]
pub struct APIResponse<'a, B: Send> {
    pub api_response: B,
    pub api_error_message: Option<&'a str>,
    pub api_server_version: &'a str,
    pub api_status_code: u16
}


pub fn make_api_error(code: poem::http::StatusCode, err: &str, response: impl Serialize + Send) -> poem::Response {
    let mut response = Json(APIResponse {
        api_response: response,
        api_error_message: Some(err),
        api_server_version: get_version().as_str(),
        api_status_code: code.as_u16()
    }).into_response();
    response.set_status(code);
    response
}

pub fn make_empty_api_error(code: poem::http::StatusCode, err: &str) -> poem::Response {
    make_api_error(code, err, Option::<()>::None)
}


pub fn make_api_response<B: Serialize + Send>(body: B) -> poem::Response {
    let response = Json(APIResponse {
        api_response: Some(body),
        api_error_message: None,
        api_server_version: get_version().as_str(),
        api_status_code: StatusCode::OK.as_u16()
    }).into_response();
    response
}


pub async fn copy_to_file(mut data: impl AsyncRead + Send) -> Result<NamedTempFile> {
    // start the writer in a blocking thread
    let (send, mut recv) = tokio::sync::mpsc::channel::<Vec<u8>>(8);
    let writer = tokio::task::spawn_blocking(move || {
        let mut temp_file = tempfile::NamedTempFile::new()?;
        while let Some(data) = recv.blocking_recv() {
            temp_file.write_all(&data)?;
        }
        anyhow::Ok(temp_file)
    });

    // read data into that writing thread
    let mut data = core::pin::pin!(data);
    loop {
        let mut buffer = vec![0u8; 1 << 14];
        let size = data.read(&mut buffer).await?;
        if size == 0 { break }
        buffer.truncate(size);
        send.send(buffer).await?;
    }
    
    // wait for writing to finish
    drop(send);
    writer.await?
}
