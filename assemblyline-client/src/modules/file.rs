use std::collections::HashMap;
use std::sync::Arc;

use assemblyline_models::types::Sha256;
use assemblyline_models::datastore::file::File as FileModel;
use assemblyline_models::datastore::result::Result as ResultModel;
use serde::Deserialize;

use crate::connection::{Connection, convert_api_output_string, convert_api_output_obj, convert_api_output_stream};
use crate::types::IBool;
use crate::types::Result;

use super::api_path;

pub const FILE_INDEX: &str = "file";

pub struct File {
    connection: Arc<Connection>,
}

#[derive(Deserialize)]
pub struct Child {
    pub name: String,
    pub sha256: Sha256,
}

#[derive(Deserialize)]
pub struct FileResults {
    /// File info Block
    pub file_info: FileModel,
    /// Full result list
    pub results: Vec<ResultModel>,
    /// Full error list
    pub errors: Vec<String>,
    /// List of possible parents
    pub parents: Vec<String>,
    /// List of children files
    pub childrens: Vec<Child>,
    /// List tags generated
    pub tags: HashMap<String, Vec<(String, String, bool, String)>>,
    /// Metadata facets results
    // pub metadata: HashMap<String, FacetResult>,
    /// UI switch to disable features
    pub file_viewer_only: bool,
}

#[derive(Deserialize)]
pub struct FileResultForService {
    /// File info Block
    pub file_info: FileModel,
    /// Full result list
    pub results: Vec<ResultModel>,
}

#[derive(Deserialize)]
pub struct FileScore {
    /// File info Block
    pub file_info: FileModel,
    /// List of keys used to compute the score
    pub result_keys: Vec<String>,
    /// Latest score for the file
    pub score: i64,
}


impl File {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {connection}
    }

    /// Return an ascii representation of the file.
    /// Throws a Client exception if the file does not exist.
    pub async fn ascii(&self, hash: &Sha256) -> Result<String> {
        let path = api_path!(FILE_INDEX, "ascii", hash);
        self.connection.get(&path, convert_api_output_string).await
    }

    // Return the list of children for the file with the given sha256.
    // Throws a Client exception if the file does not exist.
    pub async fn children(&self, sha256: &Sha256) -> Result<Vec<Child>> {
        self.connection.get(&api_path!(FILE_INDEX, "children", sha256), convert_api_output_obj).await
    }

    // Download the file with the given sha256.
    //
    // Required:
    // sha256     : File key (string)
    //
    // Optional:
    // encoding : Which file encoding do you want for the file (string)
    // output   : Path or file handle (string or file-like object)
    // sid      : ID of the submission the download is for
    //            If carted the file will inherit the submission metadata (string)
    //
    // Throws a Client exception if the file does not exist.
    pub async fn download(&self, sha256: &Sha256, encoding: Option<String>, sid: Option<String>, password: Option<String>) -> Result<impl futures::Stream> {
        let mut params = vec![];
        if let Some(encoding) = encoding {
            params.push(("encoding".to_owned(), encoding));
        }
        if let Some(sid) = sid {
            params.push(("sid".to_owned(), sid));
        }
        if let Some(password) = password {
            params.push(("password".to_owned(), password));
        }
        let path = api_path!(FILE_INDEX, "download", sha256);
        return self.connection.get_params(&path, params, convert_api_output_stream).await
    }

    /// Return an hexadecimal representation of the file.
    pub async fn hex(&self, sha256: &Sha256, bytes_only: impl IBool, length: Option<usize>) -> Result<String> {
        let bytes_only = bytes_only.into().unwrap_or(false);
        let mut params = vec![];
        if bytes_only {
            params.push(("bytes_only".to_owned(), "".to_owned()));
        }
        if let Some(length) = length {
            params.push(("length".to_owned(), length.to_string()));
        }
        self.connection.get_params(&api_path!(FILE_INDEX, "hex", sha256), params, convert_api_output_string).await
    }

    // Return info for the the file with the given sha256.
    //
    // Required:
    // sha256     : File key (string)
    //
    // Throws a Client exception if the file does not exist.
    pub async fn info(&self, sha256: &Sha256) -> Result<FileModel> {
        return self.connection.get(&api_path!(FILE_INDEX, "info", sha256), convert_api_output_obj).await
    }

    // Return all the results for the given sha256.
    //
    // Required:
    // sha256     : File key (string)
    pub async fn result(&self, sha256: &Sha256) -> Result<FileResults> {
        return self.connection.get(&api_path!(FILE_INDEX, "result", sha256), convert_api_output_obj).await
    }

    // Return all the results for the given sha256.
    //
    // Required:
    // sha256     : File key (string)
    // service : Service name (string)
    pub async fn result_for_service(&self, sha256: &Sha256, service: &str) -> Result<FileResultForService> {
        return self.connection.get(&api_path!(FILE_INDEX, "result", sha256, service), convert_api_output_obj).await
    }

    // Return the latest score for the given sha256.
    //
    // Required:
    // sha256     : File key (string)
    pub async fn score(&self, sha256: &Sha256) -> Result<FileScore> {
        return self.connection.get(&api_path!(FILE_INDEX, "score", sha256), convert_api_output_obj).await
    }

    // Return all strings found in the file.
    //
    // Required:
    // sha256     : File key (string)
    pub async fn strings(&self, sha256: &Sha256) -> Result<impl futures::Stream> {
        return self.connection.get(&api_path!(FILE_INDEX, "strings", sha256), convert_api_output_stream).await
    }

}


