// import json

// from assemblyline_client.v4_client.common.utils import SEARCHABLE, ClientError, api_path
// from assemblyline_client.v4_client.module.search.facet import Facet
// from assemblyline_client.v4_client.module.search.fields import Fields
// from assemblyline_client.v4_client.module.search.grouped import Grouped
// from assemblyline_client.v4_client.module.search.histogram import Histogram
// from assemblyline_client.v4_client.module.search.stats import Stats
// from assemblyline_client.v4_client.module.search.stream import Stream

use std::sync::Arc;

use serde::Deserialize;
use serde_json::json;

use crate::connection::{Connection, Body, convert_api_output_obj};
use crate::types::{JsonMap, Error};

use super::api_path;

#[derive(Deserialize, Debug)]
pub struct SearchResult {
    pub items: Vec<JsonMap>,
    pub offset: i64,
    pub rows: i64,
    pub total: i64,
}


enum Searchable {
    Alert,
    File,
    Heuristic,
    Result,
    Safelist,
    Signature,
    Submission,
    Workflow,
}

impl std::fmt::Display for Searchable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Searchable::Alert => "alert",
            Searchable::File => "file",
            Searchable::Heuristic => "heuristic",
            Searchable::Result => "result",
            Searchable::Safelist => "safelist",
            Searchable::Signature => "signature",
            Searchable::Submission => "submission",
            Searchable::Workflow => "workflow",
        })
    }
}


pub struct Search {
    connection: Arc<Connection>,

//         self.facet = Facet(connection)
//         self.fields = Fields(connection)
//         self.grouped = Grouped(connection)
//         self.histogram = Histogram(connection)
//         self.stats = Stats(connection)
//         self.stream = Stream(connection, self._do_search)

}

impl Search {
    pub (crate) fn new(connection: Arc<Connection>) -> Self {
        Self {connection}
    }

    /// Search alerts with a lucene query.
    /// query   : lucene query (string)
    pub fn alert(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Alert, query)
    }

    /// Search files with a lucene query.
    /// query   : lucene query (string)
    pub fn file(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::File, query)
    }

    /// Search heuristics with a lucene query.
    /// query   : lucene query (string)
    pub fn heuristic(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Heuristic, query)
    }

    /// Search result with a lucene query.
    /// query   : lucene query (string)
    pub fn result(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Result, query)
    }

    /// Search safelist with a lucene query.
    /// query   : lucene query (string)
    pub fn safelist(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Safelist, query)
    }

    /// Search signature with a lucene query.
    /// query   : lucene query (string)
    pub fn signature(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Signature, query)
    }

    /// Search submission with a lucene query.
    /// query   : lucene query (string)
    pub fn submission(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Submission, query)
    }

    /// Search workflow with a lucene query.
    /// query   : lucene query (string)
    pub fn workflow(&self, query: String) -> SearchBuilder {
        SearchBuilder::new(self.connection.clone(), Searchable::Workflow, query)
    }
}

pub struct SearchBuilder {
    connection: Arc<Connection>,
    index: Searchable,
    query: String,
    filters: Vec<String>,
    field_list: Option<String>,
    offset: usize,
    rows: usize,
    sort: Option<String>,
    timeout: Option<usize>,
    use_archive: bool,
    track_total_hits: Option<usize>,
}

impl SearchBuilder {
    fn new(connection: Arc<Connection>, index: Searchable, query: String) -> Self {
        Self {
            connection,
            index,
            query,
            filters: vec![],
            field_list: None,
            offset: 0,
            rows: 25,
            sort: None,
            timeout: None,
            use_archive: false,
            track_total_hits: None,
        }
    }

    /// Additional lucene query used to filter the data
    pub fn filter(mut self, filter: String) -> Self {
        self.filters.push(filter); self
    }

    /// List of fields to return (comma separated string of fields)
    pub fn field_list(mut self, fields: String) -> Self {
        self.field_list = Some(fields); self
    }
    /// List of fields to return (comma separated string of fields)
    pub fn fl(mut self, fields: String) -> Self {
        self.field_list = Some(fields); self
    }
    /// Offset at which the query items should start
    pub fn offset(mut self, value: usize) -> Self {
        self.offset = value; self
    }
    /// Number of records to return
    pub fn rows(mut self, value: usize) -> Self {
        self.rows = value; self
    }
    /// Field used for sorting with direction (string: ex. 'id desc')
    pub fn sort(mut self, value: String) -> Self {
        self.sort = Some(value); self
    }
    /// Max amount of miliseconds the query will run
    pub fn timeout(mut self, value: usize) -> Self {
        self.timeout = Some(value); self
    }
    /// Also query the archive
    pub fn use_archive(mut self, value: bool) -> Self {
        self.use_archive = value; self
    }
    /// Number of hits to track (default: 10k)
    pub fn track_total_hits(mut self, value: usize) -> Self {
        self.track_total_hits = Some(value); self
    }

    pub async fn search(self) -> Result<SearchResult, Error> {
        let mut data = json!({
            "query": self.query,
            "filters": self.filters,
            "fl": self.field_list,
            "offset": self.offset,
            "rows": self.rows,
            "sort": self.sort,
            "timeout": self.timeout,
            "use_archive": self.use_archive,
            "track_total_hits": self.track_total_hits,
        });

        if let Some(data) = data.as_object_mut() {
            data.retain(|_k, v|!v.is_null());
        }

        let path = api_path!("search", self.index.to_string());
        self.connection.post(&path, Body::Json(data), convert_api_output_obj).await
    }
}
