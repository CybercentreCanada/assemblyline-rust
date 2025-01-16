use std::borrow::Cow;
use std::fmt::Debug;
use std::time::Duration;

use assemblyline_models::{ElasticMeta, JsonMap, Readable};
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use struct_metadata::Described;

use crate::elastic::DEFAULT_SEARCH_FIELD;

use super::collection::{Collection, PitGuard, DEFAULT_ROW_SIZE, DEFAULT_SORT};
use super::{parse_sort, responses, Index, Request, Result};



impl<T: Serialize + Readable + Described<ElasticMeta> + Debug> Collection<T> {

    /// Do a search and return the full source data
    pub fn search<'a>(&'a self, query: &'a str) -> SearchBuilder<'a, T> {
        SearchBuilder {
            collection: self,
            query,
            target: Target::IndexType(Index::Hot),
            offset: None,
            rows: DEFAULT_ROW_SIZE,
            sort: DEFAULT_SORT.to_owned(),
            filters: vec![],
            access_control: None,
            field_list: "*".to_owned(),
            timeout: None,
            key_space: None,
            search_after: None,
        }
    }        
}

/// Build a search query that will run a single search call
pub struct SearchBuilder<'a, T: Serialize + Readable + Described<ElasticMeta>> {
    collection: &'a Collection<T>,
    query: &'a str,
    target: Target,
    offset: Option<u64>, 
    rows: u64, 
    sort: String, 
    filters: Vec<String>,
    access_control: Option<String>,
    field_list: String,
    key_space: Option<&'a [String]>,
    timeout: Option<Duration>,
    search_after: Option<serde_json::Value>,
}

impl<'a, T: Serialize + Readable + Described<ElasticMeta> + Debug> SearchBuilder<'a, T> {
    
    pub fn fields(mut self, fields: &str) -> Self {
        self.field_list = fields.to_owned(); self
    }

    pub fn rows(mut self, rows: u64) -> Self {
        self.rows = rows; self
    }

    pub fn key_space(mut self, keys: &'a [String]) -> Self {
        self.key_space = Some(keys); self
    }

    pub (super) async fn execute_raw<Field: Default + Debug + DeserializeOwned>(&self) -> Result<responses::Search<Field, T>> {
        let mut filters = self.filters.clone();

        if let Some(access_control) = self.access_control.clone() {
            filters.push(access_control);
        }

        let mut formatted_filters = vec![];
        for ff in filters {
            formatted_filters.push(json!({"query_string": {"query": ff}}))
        }

        if let Some(key_space) = self.key_space {
            formatted_filters.push(json!({"ids": {"values": key_space}}));
        }


        let query_expression = json!({
            "bool": {
                "must": {
                    "query_string": {
                        "query": self.query,
                        "default_field": DEFAULT_SEARCH_FIELD
                    }
                },
                "filter": formatted_filters
            }
        });

        let source = if self.field_list.is_empty() || self.field_list == "*" {
            "true".to_owned()
        } else {
            self.field_list.clone()
        };

        let sort = parse_sort(&self.sort)?;
        let sort = sort.iter().map(|(name, direction)|format!("{name}:{direction}")).collect_vec();

        let mut params: Vec<(&str, Cow<str>)> = vec![];
        params.push(("size", self.rows.to_string().into()));
        params.push(("sort", sort.join(",").into()));
        params.push(("_source", source.as_str().into()));

        if let Some(offset) = self.offset {
            params.push(("from", offset.to_string().into()));
        }

        if let Some(timeout) = &self.timeout {
            params.push(("timeout", format!("{}ms", timeout.as_millis()).into()));
        }

        let mut body = JsonMap::new();
        body.insert("query".to_string(), query_expression);

        if let Some(search_after) = &self.search_after {
            body.insert("search_after".to_string(), search_after.clone());
        }

        let request = match &self.target {
            Target::Pit{pit, keep_alive} => {
                body.insert("pit".to_string(), json!({
                    "id": pit.id,
                    "keep_alive": keep_alive,
                }));
                Request::get_search(&self.collection.database.host, params)?
            },
            Target::Index(index) => {
                Request::get_search_on(&self.collection.database.host, index, params)?
            },
            Target::IndexType(index) => {
                let index = self.collection.get_joined_index(Some(*index))?;
                Request::get_search_on(&self.collection.database.host, &index, params)?
            }
        };

        let response = self.collection.database.make_request_json(&mut 0, &request, &body).await?;
        let response: responses::Search<Field, T> = response.json().await?;
        Ok(response)
    }

    pub async fn execute<Field: Default + Debug + DeserializeOwned>(self) -> Result<SearchResult<Field, T>> {
        let mut response = self.execute_raw().await?;
        // match response.hits.hits.last() {
        //     Some(row) => {
        //         self.search_after = Some(row.sort.clone());
        //     },
        //     None => {
        //         self.finished = true;
        //         return Ok(None)
        //     },
        // }

        // Load any result fields
        let mut field_items = vec![];
        let mut source_items = vec![];
        while let Some(row) = response.hits.hits.pop() {
            if let Some(mut source) = row._source {
                source.set_from_archive(self.collection.is_archive_index(&row._index));
                source_items.push(source);
            }

            field_items.push(row.fields);
        }

        Ok(SearchResult {
            offset: self.offset.unwrap_or_default(),
            rows: self.rows,
            total: response.hits.total.value,
            field_items,
            source_items
        })
    }

}

pub struct SearchResult<Field, Source> {
    pub offset: u64,
    pub rows: u64,
    pub total: u64,
    pub field_items: Vec<Field>,
    pub source_items: Vec<Source>
}

enum Target {
    Pit{pit: PitGuard, keep_alive: String},
    Index(String),
    IndexType(Index),
}