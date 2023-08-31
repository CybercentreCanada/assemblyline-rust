
// import os

// from json import dumps

// from assemblyline_client.v4_client.common.utils import api_path, api_path_by_module, ClientError
// from assemblyline_client.v4_client.common.submit_utils import get_file_handler

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use serde_json::json;
use tokio::io::AsyncSeekExt;
use tokio_util::codec::{FramedRead, BytesCodec};
use url::Url;

use crate::{JsonMap, Error, Sha256};
use crate::connection::{Connection, convert_api_output_obj, Body};
use crate::models::submission::{SubmissionParams, File};

use super::api_path;

const INGEST_PATH: &str = "ingest";

#[derive(Deserialize, Debug)]
pub struct IngestResponse {
    pub ingest_id: String
}

#[derive(Debug)]
pub enum ExtendedScan {
    Submitted,
    Skipped,
    Incomplete,
    Completed,
}

impl FromStr for ExtendedScan {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "submitted" => Ok(Self::Submitted),
            "skipped" => Ok(Self::Skipped),
            "incomplete" => Ok(Self::Incomplete),
            "completed" => Ok(Self::Completed),
            _ => Err(Error::MalformedResponse)
        }
    }
}

impl<'de> Deserialize<'de> for ExtendedScan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let string = String::deserialize(deserializer)?;
        string.parse().map_err(serde::de::Error::custom)
    }
}

/// Notification Model
#[derive(Deserialize, Debug)]
pub struct Notification {
    /// Queue to publish the completion message
    pub queue: Option<String>,
    /// Notify only if this score threshold is met
    pub threshold: Option<i64>,
}

/// Submission Model
#[derive(Deserialize, Debug)]
pub struct MessageSubmission {
    /// Submission ID to use
    pub sid: String,
    /// Message time
    pub time: DateTime<Utc>,
    /// File block
    #[serde(default)]
    pub files: Vec<File>,
    /// Metadata submitted with the file
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    /// Notification queue parameters
    pub notification: Notification,
    /// Parameters of the submission
    pub params: SubmissionParams,
    pub scan_key: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct IngestMessage {
    pub extended_scan: ExtendedScan,
    pub failure: String,
    pub ingest_id: String,
    pub ingest_time: DateTime<Utc>,
    pub retries: u64,
    pub score: Option<i64>,
    pub submission: MessageSubmission
}


pub struct Ingest {
    connection: Arc<Connection>,
}

impl Ingest {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }

    pub fn single(&self) -> IngestBuilder {
        IngestBuilder::new(self.connection.clone())
    }

    /// Return a single message from the given notification queue.
    pub async fn get_message(&self, nq: &str) -> Result<Option<IngestMessage>, Error> {
        return self.connection.get(&api_path!(INGEST_PATH, "get_message", nq), convert_api_output_obj).await
    }

    /// Return all messages from the given notification queue.
    pub async fn get_message_list(&self, nq: &str) -> Result<Vec<IngestMessage>, Error> {
        return self.connection.get(&api_path!(INGEST_PATH, "get_message_list", nq), convert_api_output_obj).await
    }
}


pub struct IngestBuilder {
    connection: Arc<Connection>,
    metadata: HashMap<String, String>,
    params: Option<SubmissionParams>,
    extra_params: JsonMap,
    alert: bool,
    notification_queue: Option<String>,
    notification_threshold: Option<i64>,
    ingest_type: String
}

impl IngestBuilder {

    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
            metadata: Default::default(),
            params: Default::default(),
            extra_params: Default::default(),
            alert: false,
            notification_queue: None,
            notification_threshold: None,
            ingest_type: "AL_CLIENT".to_owned()
        }
    }

    /// fname   : Name of the file to scan
    pub fn fname(self, fname: String) -> NamedIngestBuilder {
        NamedIngestBuilder { parent: self, fname }
    }

    // metadata   : Metadata to include with submission. (dict)
    pub fn metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata.extend(metadata.into_iter()); self
    }
    pub fn metadata_item(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value); self
    }

    // params  : Additional submission parameters. (dict)
    pub fn params(mut self, params: SubmissionParams) -> Self {
        self.params = Some(params); self
    }
    pub fn parameter(mut self, name: String, value: serde_json::Value) -> Self {
        self.extra_params.insert(name, value); self
    }

    /// Create an alert if score above alert threshold. (boolean)
    pub fn alert(mut self, alert: bool) -> Self {
        self.alert = alert; self
    }

    /// Notification queue name. (string)
    pub fn notification_queue(mut self, name: String) -> Self {
        self.notification_queue = Some(name); self
    }
    pub fn notification_threshold(mut self, threshold: i64) -> Self {
        self.notification_threshold = Some(threshold); self
    }

    /// Ingestion type, one word to describe how the data is ingested. Default: AL_CLIENT (string)
    pub fn ingest_type(mut self, name: String) -> Self {
        self.ingest_type = name; self
    }

    // path    : Path/name of file. (string)
    pub async fn path(self, path: &std::path::Path) -> Result<IngestResponse, Error> {
        if let Some(name) = path.file_name() {
            if let Some(name) = name.to_str() {
                return self.fname(name.to_string()).path(path).await
            }
        }
        return Err(Error::InvalidSubmitFilePath)
    }

    // sha256  : Sha256 of the file to scan (string)
    pub async fn sha256(self, hash: Sha256) -> Result<IngestResponse, Error> {
        self.fname(hash.to_string()).sha256(hash).await
    }

    // url     : Url to scan (string)
    pub async fn url(self, url: String) -> Result<IngestResponse, Error> {
        let parsed = Url::parse(&url)?;

        if let Some(path_parts) = parsed.path_segments() {
            if let Some(name) = path_parts.last() {
                if !name.is_empty() {
                    return self.fname(name.to_owned()).url(url).await
                }
            }
        }
        return Err(Error::InvalidSubmitUrl)
    }
}

pub struct NamedIngestBuilder {
    parent: IngestBuilder,
    fname: String,
}

impl NamedIngestBuilder {

    fn prepare_request(&self) -> Result<JsonMap, Error> {
        let mut request: JsonMap = [
            ("name".to_owned(), self.fname.clone().into()),
            ("type".to_owned(), self.parent.ingest_type.clone().into()),
            ("generate_alert".to_owned(), self.parent.alert.into()),
        ].into_iter().collect();

        if let Some(queue) = &self.parent.notification_queue {
            request.insert("notification_queue".to_owned(), queue.clone().into());
        }
        if let Some(threshold) = &self.parent.notification_threshold {
            request.insert("notification_threshold".to_owned(), json!(threshold));
        }

        if self.parent.params.is_some() || !self.parent.extra_params.is_empty() {
            let params = if let Some(params) = &self.parent.params {
                if let serde_json::Value::Object(mut obj) = serde_json::to_value(params)? {
                    obj.extend(self.parent.extra_params.clone().into_iter());
                    obj
                } else {
                    return Err(Error::ParameterSerialization)
                }
            } else {
                self.parent.extra_params.clone()
            };

            request.insert("params".to_owned(), serde_json::Value::Object(params));
        }

        if !self.parent.metadata.is_empty() {
            request.insert("metadata".to_owned(), serde_json::to_value(self.parent.metadata.clone())?);
        }

        Ok(request)
    }

    async fn submit_file(self, body: reqwest::Body) -> Result<IngestResponse, Error> {
        let request = self.prepare_request()?;

        // build multipart, adding our file and the submission details as parts
        let multipart = Form::new();
        let multipart = multipart.part("json", Part::text(serde_json::to_string(&request)?));
        let multipart = multipart.part("bin", Part::stream(body).file_name(self.fname));

        // println!("{multipart:?}");

        let url: String = api_path!(INGEST_PATH);
        return self.parent.connection.post(&url, Body::<()>::Multipart(multipart), convert_api_output_obj).await
    }

    // fh      : Opened file handle to a file to scan
    pub async fn file_handle(self, mut file: tokio::fs::File) -> Result<IngestResponse, Error> {
        // prepare file handle for reading
        file.seek(std::io::SeekFrom::Start(0)).await?;

        // turn it into a stream
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream_body = reqwest::Body::wrap_stream(stream);

        return self.submit_file(stream_body).await
    }

    // content : Content of the file to scan (byte array)
    pub async fn content(self, data: Vec<u8>) -> Result<IngestResponse, Error> {
        let body = reqwest::Body::from(data);
        return self.submit_file(body).await
    }

    // path    : Path/name of file. (string)
    pub async fn path(self, path: &std::path::Path) -> Result<IngestResponse, Error> {
        // prepare file handle for reading
        let file = tokio::fs::File::open(path).await?;

        // turn it into a stream
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream_body = reqwest::Body::wrap_stream(stream);

        return self.submit_file(stream_body).await
    }

    // sha256  : Sha256 of the file to scan (string)
    pub async fn sha256(self, hash: Sha256) -> Result<IngestResponse, Error> {
        let mut request = self.prepare_request()?;
        request.insert("sha256".to_owned(), hash.to_string().into());

        let path = api_path!(INGEST_PATH);
        return self.parent.connection.post(&path, Body::Json(request), convert_api_output_obj).await
    }

    // url     : Url to scan (string)
    pub async fn url(self, url: String) -> Result<IngestResponse, Error> {
        let mut request = self.prepare_request()?;
        request.insert("url".to_owned(), url.into());

        let path = api_path!(INGEST_PATH);
        return self.parent.connection.post(&path, Body::Json(request), convert_api_output_obj).await
    }

}


