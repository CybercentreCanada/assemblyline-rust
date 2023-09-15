// import os

// from assemblyline_client.v4_client.common.utils import api_path, stream_output, raw_output

use std::collections::HashMap;
use std::sync::Arc;

use crate::JsonMap;
use crate::connection::{Connection, convert_api_output_stream, Body, convert_api_output_map};
use crate::types::{Error, IBool};

use super::api_path;

const BUNDLE_PATH: &str = "bundle";

pub struct Bundle {
    connection: Arc<Connection>,
}

impl Bundle {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {connection}
    }

    /// Creates a bundle containing the submission results and the associated files
    /// Required:
    /// sid    : Submission ID (string)
    /// Optional:
    /// output     : Path or file handle. (string or file-like object)
    /// use_alert  : The ID provided is an alert ID and will be used for bundle creation. (bool)
    /// If output is not specified the content is returned by the function
    pub async fn create(self, sid: &str, use_alert: bool) -> Result<impl futures::Stream, Error> {
        let path = api_path!(BUNDLE_PATH, sid);

        let params: HashMap<String, String> = if use_alert {
            [("use_alert".to_string(), "".to_string())].into_iter().collect()
        } else {
            Default::default()
        };

        return self.connection.get_params(&path, params, convert_api_output_stream).await

//         if output:
//             return self._connection.download(path, stream_output(output))
//         return self._connection.download(path, raw_output)
    }

    // Import a submission bundle into the system
    // Required:
    // bundle              : bundle to import (string, bytes or file_handle)
    // Optional:
    // allow_incomplete    : allow importing incomplete submission. (bool)
    // exist_ok            : Do not throw an exception if the submission already exists (bool)
    // min_classification  : Minimum classification at which the bundle is imported. (string)
    // rescan_services     : List of services to rescan after import. (Comma seperated strings)
    // Returns {'success': True/False } depending if it was imported or not
    pub async fn import_bundle(&self, 
        bundle: impl Into<reqwest::Body>, 
        min_classification: Option<String>, 
        rescan_services: Option<Vec<String>>, 
        exist_ok: impl IBool, 
        allow_incomplete: impl IBool, 
        complete_queue: Option<String>
    ) -> Result<JsonMap, Error> {
        let path = api_path!(BUNDLE_PATH);

        let exist_ok = exist_ok.into().unwrap_or(false);
        let allow_incomplete = allow_incomplete.into().unwrap_or(false);

        let mut params = HashMap::<String, String>::new();
        if exist_ok {
            params.insert("exist_ok".to_string(), "".to_string());
        }
        if let Some(classification) = min_classification {
            params.insert("min_classification".to_string(), classification);
        }
        if let Some(services) = rescan_services {
            params.insert("rescan_services".to_string(), services.join(","));
        }
        if allow_incomplete {
            params.insert("allow_incomplete".to_string(), "".to_string());
        }

        self.connection.post_params(&path, Body::<()>::Prepared(bundle.into()), params, convert_api_output_map).await
//         return self._connection.post(api_path('bundle', **kw), data=contents)
    }
}
