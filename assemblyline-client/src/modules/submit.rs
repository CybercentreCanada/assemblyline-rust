// import os

// from json import dumps

// from assemblyline_client.v4_client.common.utils import api_path, api_path_by_module, get_function_kwargs, ClientError
// from assemblyline_client.v4_client.common.submit_utils import get_file_handler

use std::collections::HashMap;
use std::sync::Arc;

use assemblyline_models::Sha256;
use reqwest::multipart::{Form, Part};
use tokio::io::AsyncSeekExt;
use tokio_util::codec::{FramedRead, BytesCodec};
use url::Url;
use assemblyline_models::datastore::submission::{SubmissionParams, Submission};

use crate::{JsonMap, types::Error};
use crate::connection::{Connection, Body, convert_api_output_obj};

use super::api_path;

const SUBMIT_PATH: &str = "submit";

pub struct Submit {
    connection: Arc<Connection>,
}

impl Submit {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self { connection }
    }

    /// Submit a file to be dispatched.
    pub fn single(&self) -> SubmitBuilder {
        SubmitBuilder::new(self.connection.clone())
    }


    /// Resubmit a file for dynamic analysis
    pub async fn dynamic(&self, sha256: Sha256, copy_sid: Option<String>, name: Option<String>) -> Result<Submission, Error> {
        let mut params = vec![];
        if let Some(copy_sid) = copy_sid {
            params.push(("copy_sid".to_owned(), copy_sid));
        }
        if let Some(name) = name {
            params.push(("name".to_owned(), name));
        }
        let path = api_path!(SUBMIT_PATH, "dynamic", sha256);
        return self.connection.get_params(&path, params, convert_api_output_obj).await
    }

    /// Resubmit a file for analysis with the exact same parameters.
    pub async fn resubmit(&self, sid: String) -> Result<Submission, Error> {
        return self.connection.get(&api_path!(SUBMIT_PATH, "resubmit", sid), convert_api_output_obj).await
    }
}

pub struct SubmitBuilder {
    connection: Arc<Connection>,
    metadata: HashMap<String, String>,
    params: Option<SubmissionParams>,
    extra_params: JsonMap,
}

impl SubmitBuilder {

    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
            metadata: Default::default(),
            params: Default::default(),
            extra_params: Default::default()
        }
    }

    /// fname   : Name of the file to scan
    pub fn fname(self, fname: String) -> NamedSubmitBuilder {
        NamedSubmitBuilder { parent: self, fname }
    }

    // metadata   : Metadata to include with submission. (dict)
    pub fn metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata.extend(metadata); self
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

    // path    : Path/name of file. (string)
    pub async fn path(self, path: &std::path::Path) -> Result<Submission, Error> {
        if let Some(name) = path.file_name() {
            if let Some(name) = name.to_str() {
                return self.fname(name.to_string()).path(path).await
            }
        }
        return Err(Error::InvalidSubmitFilePath)
    }

    // sha256  : Sha256 of the file to scan (string)
    pub async fn sha256(self, hash: Sha256) -> Result<Submission, Error> {
        self.fname(hash.to_string()).sha256(hash).await
    }

    // url     : Url to scan (string)
    pub async fn url(self, url: String) -> Result<Submission, Error> {
        let parsed = Url::parse(&url)?;

        if let Some(mut path_parts) = parsed.path_segments() {
            if let Some(name) = path_parts.next_back() {
                if !name.is_empty() {
                    return self.fname(name.to_owned()).url(url).await
                }
            }
        }
        return Err(Error::InvalidSubmitUrl)
    }
}

pub struct NamedSubmitBuilder {
    parent: SubmitBuilder,
    fname: String,
}

impl NamedSubmitBuilder {

    // // metadata   : Metadata to include with submission. (dict)
    // pub fn metadata(self, metadata: HashMap<String, String>) -> Self {
    //     Self { parent: self.parent.metadata(metadata), fname: self.fname }
    // }
    // pub fn metadata_item(self, key: String, value: String) -> Self {
    //     Self { parent: self.parent.metadata_item(key, value), fname: self.fname }
    // }

    // // params  : Additional submission parameters. (dict)
    // pub fn params(self, params: SubmissionParams) -> Self {
    //     Self { parent: self.parent.params(params), fname: self.fname }
    // }
    // pub fn parameter(self, name: String, value: serde_json::Value) -> Self {
    //     Self { parent: self.parent.parameter(name, value), fname: self.fname }
    // }

    fn prepare_request(&self) -> Result<JsonMap, Error> {
        let mut request: JsonMap = [
            ("name".to_owned(), self.fname.clone().into())
        ].into_iter().collect();


        if self.parent.params.is_some() || !self.parent.extra_params.is_empty() {
            let params = if let Some(params) = &self.parent.params {
                if let serde_json::Value::Object(mut obj) = serde_json::to_value(params)? {
                    obj.extend(self.parent.extra_params.clone());
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

    async fn submit_file(self, body: reqwest::Body) -> Result<Submission, Error> {
        let request = self.prepare_request()?;

        // build multipart, adding our file and the submission details as parts
        let multipart = Form::new();
        let multipart = multipart.part("json", Part::text(serde_json::to_string(&request)?));
        let multipart = multipart.part("bin", Part::stream(body).file_name(self.fname));

        // println!("{multipart:?}");

        let url: String = api_path!(SUBMIT_PATH);
        return self.parent.connection.post(&url, Body::<()>::Multipart(multipart), convert_api_output_obj).await
    }

    // fh      : Opened file handle to a file to scan
    pub async fn file_handle(self, mut file: tokio::fs::File) -> Result<Submission, Error> {
        // prepare file handle for reading
        file.seek(std::io::SeekFrom::Start(0)).await?;

        // turn it into a stream
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream_body = reqwest::Body::wrap_stream(stream);

        return self.submit_file(stream_body).await
    }

    // content : Content of the file to scan (byte array)
    pub async fn content(self, data: Vec<u8>) -> Result<Submission, Error> {
        let body = reqwest::Body::from(data);
        return self.submit_file(body).await
    }

    // path    : Path/name of file. (string)
    pub async fn path(self, path: &std::path::Path) -> Result<Submission, Error> {
        // prepare file handle for reading
        let file = tokio::fs::File::open(path).await?;

        // turn it into a stream
        let stream = FramedRead::new(file, BytesCodec::new());
        let stream_body = reqwest::Body::wrap_stream(stream);

        return self.submit_file(stream_body).await
    }

    // sha256  : Sha256 of the file to scan (string)
    pub async fn sha256(self, hash: Sha256) -> Result<Submission, Error> {
        let mut request = self.prepare_request()?;
        request.insert("sha256".to_owned(), hash.to_string().into());

        let path = api_path!(SUBMIT_PATH);
        return self.parent.connection.post(&path, Body::Json(request), convert_api_output_obj).await
    }

    // url     : Url to scan (string)
    pub async fn url(self, url: String) -> Result<Submission, Error> {
        let mut request = self.prepare_request()?;
        request.insert("url".to_owned(), url.into());

        let path = api_path!(SUBMIT_PATH);
        return self.parent.connection.post(&path, Body::Json(request), convert_api_output_obj).await
    }

}


