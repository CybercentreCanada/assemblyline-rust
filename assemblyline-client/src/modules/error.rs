use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::connection::{Connection, convert_api_output_obj};

use crate::models::error::Error as ErrorModel;
use crate::types::Result;

use super::api_path;
use super::search::SearchResult;

const ERROR_PATH: &str = "error";

pub struct Error {
    connection: Arc<Connection>,
}

impl Error {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {connection}
    }

    // Get the error details for a given error key
    // Required:
    // error_key:  Error key to get the details for (string)
    // Throws a Client exception if the error does not exist.
    pub async fn get(&self, error_key: &str) -> Result<ErrorModel> {
        let path = api_path!(ERROR_PATH, error_key);
        self.connection.get(&path, convert_api_output_obj).await
    }

    /// List all errors in the system (per page)
    pub fn list(&self) -> ListingBuilder<ErrorModel> {
        ListingBuilder {
            connection: self.connection.clone(),
            _pd: Default::default(),
            query: None,
            offset: 0,
            rows: 10,
            sort: None,
            use_archive: false,
            track_total_hits: None,
        }
    }
}

pub struct ListingBuilder<Type> {
    connection: Arc<Connection>,
    _pd: PhantomData<Type>,

    /// Offset at which we start giving errors
    offset: u64,
    /// Query to apply to the error list
    query: Option<String>,
    /// Number of errors to return
    rows: u64,
    /// Sort order
    sort: Option<String>,
    /// Also query the archive
    use_archive: bool,
    /// Number of hits to track (default: 10k)
    track_total_hits: Option<u64>,
}

impl<Type> ListingBuilder<Type> {

    /// Offset at which we start giving errors
    pub fn offset(mut self, value: u64) -> Self {
        self.offset = value; self
    }
    /// Query to apply to the error list
    pub fn query(mut self, value: String) -> Self {
        self.query = Some(value); self
    }
    /// Number of errors to return
    pub fn rows(mut self, value: u64) -> Self {
        self.rows = value; self
    }
    /// Sort order
    pub fn sort(mut self, value: String) -> Self {
        self.sort = Some(value); self
    }
    /// Also query the archive
    pub fn use_archive(mut self, value: bool) -> Self {
        self.use_archive = value; self
    }
    /// Number of hits to track (default: 10k)
    pub fn track_total_hits(mut self, value: u64) -> Self {
        self.track_total_hits = Some(value); self
    }

    ///
    pub async fn fetch(self) -> Result<SearchResult<ErrorModel>> {

        let mut params: HashMap<String, String> = [
            ("offset", self.offset.to_string()),
            ("query", self.query.unwrap_or_default()),
            ("rows", self.rows.to_string()),
            ("sort", self.sort.unwrap_or_default())
        ].into_iter().map(|(k, v)|(k.to_owned(), v)).collect();

        if self.use_archive {
            params.insert("use_archive".to_string(), "".to_string());
        }
        if let Some(hits) = self.track_total_hits {
            params.insert("track_total_hits".to_string(), hits.to_string());
        }
        let path = api_path!(ERROR_PATH, "list");
        self.connection.get_params(&path, params, convert_api_output_obj).await
    }


}