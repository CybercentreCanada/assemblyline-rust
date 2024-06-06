use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::JsonMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;

use crate::error::{Result, Error as ErrorKind};

use super::{ElasticHelper, Index, with_retries_raise_confict};

const DEFAULT_SORT: &str = "_id: asc";

enum Version {
    Create,
    Expected(i64, i64),
}

pub struct Collection<T: Serialize + DeserializeOwned> {
    database: Arc<ElasticHelper>,
    name: String,
    archive_name: Option<String>,
    _data: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned> Collection<T> {

    fn new(es: Arc<ElasticHelper>, name: String, archive_name: Option<String>) -> Self {
        Collection {
            database: es,
            name,
            archive_name,
            _data: Default::default()
        }
    }

    fn get_index_list(&self, index_type: Option<Index>) -> Result<Vec<String>> {
        Ok(match index_type {
            // Default value
            None => {
                // If has an archive: hot + archive
                if self.database.archive_access {
                    if let Some(archive_name) = &self.archive_name {
                        return Ok(vec![self.name.clone(), archive_name.clone()])
                    }
                }
                // Otherwise just hot
                vec![self.name.clone()]
            }

            // If specified index is HOT
            Some(Index::Hot) => {
                vec![self.name.clone()]
            },

            // If only archive asked
            Some(Index::Archive) => {
                // Crash if index has no archive
                match &self.archive_name {
                    None => {
                        return Err(ErrorKind::ArchiveDisabled(format!("Index {} does not have an archive", self.name.to_uppercase())))
                    }

                    Some(archive_name) => {
                        // Crash if no archive access
                        if !self.database.archive_access {
                            return Err(ErrorKind::ArchiveDisabled("Trying to get access to the archive on a datastore where archive_access is disabled".to_owned()))
                        }

                        // Return only archive index
                        vec![archive_name.clone()]
                    }
                }
            }

            Some(Index::HotAndArchive) => {
                // Crash if no archive access
                if !self.database.archive_access {
                    return Err(ErrorKind::ArchiveDisabled("Trying to get access to the archive on a datastore where archive_access is disabled".to_owned()))
                }

                // Return HOT if asked for both but only has HOT
                match &self.archive_name {
                    None => vec![self.name.clone()],
                    Some(archive_name) => vec![self.name.clone(), archive_name.clone()]
                }
            }
        })
    }

    fn get_joined_index(&self, index_type: Option<Index>) -> Result<String> {
        Ok(self.get_index_list(index_type)?.join(","))
    }

    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        todo!();
    }
    
    pub async fn get_version(&self, key: &str) -> Result<Option<(T, Version)>> {
        todo!();
    }

    pub async fn save(&self, key: &str, value: &T, version: Option<Version>, index_type: Option<Index>) -> Result<()> {
        if key.contains(' ') {
            return Err(ErrorKind::DataStoreException("You are not allowed to use spaces in datastore keys."))
        }

        let mut saved_data = serde_json::to_value(value)?;
        let data = if let Some(data) = saved_data.as_object_mut() {
            data
        } else {
            return Err(ErrorKind::DataStoreException("Types saved in elastic must serialize to a json document."))
        };
        data.insert("id".to_owned(), json!(key));

        self.save_json(key, data, version, index_type).await
    }

    pub async fn save_json(&self, key: &str, value: &JsonMap, version: Option<Version>, index_type: Option<Index>) -> Result<()> {
        let mut operation = elasticsearch::params::OpType::Index;
        let mut version_parts = None;

        if let Some(version) = version {
            match version {
                Version::Create => {
                    operation = elasticsearch::params::OpType::Create;
                },
                Version::Expected(seq, prim) => {
                    version_parts = Some((seq, prim));
                },
            }
        }

        let index_list = self.get_index_list(index_type)?;
        for index in index_list {
            with_retries_raise_confict!(self.database, index.clone(), {
                let es = self.database.es.read().await;
                let mut request = es.index(elasticsearch::IndexParts::IndexId(&index, key))
                    .body(&value)
                    .op_type(operation);
                if let Some((seq, prim)) = version_parts {
                    request = request.if_seq_no(seq).if_primary_term(prim);
                }
                request.send().await
            })?;
        }

        Ok(())
    }


    /// This function should perform a search through the datastore and stream
    /// all related results as a dictionary of key value pair where each keys
    /// are one of the field specified in the field list parameter.
    ///
    /// >>> # noinspection PyUnresolvedReferences
    /// >>> {
    /// >>>     fl[0]: value,
    /// >>>     ...
    /// >>>     fl[x]: value
    /// >>> }
    ///
    /// :param query: lucene query to search for
    /// :param fl: list of fields to return from the search
    /// :param filters: additional queries to run on the original query to reduce the scope
    /// :param access_control: access control parameters to run the query with
    /// :param item_buffer_size: number of items to buffer with each search call
    /// :param as_obj: Return objects instead of dictionaries
    /// :param index_type: Type of indices to target
    /// :return: a generator of dictionary of field list results
    async fn stream_search<RT: DeserializeOwned>(&self,
        query: String,
        fl: String,
        mut filters: Vec<String>,
        access_control: Option<String>,
        item_buffer_size: Option<i64>,
        index_type: Option<Index>,
    ) -> Result<ScrollCursor<RT>> {
        let item_buffer_size = item_buffer_size.unwrap_or(200);
        let index_type = index_type.unwrap_or(Index::Hot);

        if item_buffer_size > 2000 || item_buffer_size < 50 {
            return Err(ErrorKind::SearchException("Variable item_buffer_size must be between 50 and 2000.".to_string()))
        }

        let index = self.get_joined_index(Some(index_type))?;

        if let Some(access_control) = access_control {
            filters.push(access_control);
        }

        let mut formatted_filters = vec![];
        for ff in filters {
            formatted_filters.push(json!({"query_string": {"query": ff}}))
        }

        let query_expression = json!({
            "bool": {
                "must": {
                    "query_string": {
                        "query": query,
                        "default_field": DEFAULT_SEARCH_FIELD
                    }
                },
                "filter": formatted_filters
            }
        });

        let sort = vec![DEFAULT_SORT.to_owned()]; //parse_sort(DEFAULT_SORT)?;
        let source = if fl.is_empty() || fl == "*" {
            "_source".to_owned()
        } else {
            fl
        };
        // let source = match fl {
        //     Some(fl) => fl,
        //     None => list(self.stored_fields.keys())
        // };

        Ok(ScrollCursor::new(self.database.clone(), index, query_expression, sort, source, None, Some(item_buffer_size), None).await?)
    }
}


struct ScrollCursor<T: DeserializeOwned> {
    helper: Arc<ElasticHelper>,
    pit_id: serde_json::Value,
    batch_size: i64,
    keep_alive: String,
    batch: Vec<(serde_json::Value, T)>,
    sort: Vec<String>,
    query: serde_json::Value,
    index: String,
    search_after: Option<serde_json::Value>,
    timeout: Option<String>,
    source: String,
}

impl<T: DeserializeOwned> ScrollCursor<T> {

    async fn new(helper: Arc<ElasticHelper>,
        index: String,
        query: serde_json::Value,
        mut sort: Vec<String>,
        source: String,
        keep_alive: Option<String>,
        size: Option<i64>,
        timeout: Option<String>,
    ) -> Result<Self> {
        let keep_alive = keep_alive.unwrap_or(KEEP_ALIVE.to_owned());
        let batch_size = size.unwrap_or(1000);

        // Generate the point in time
        let mut pit: JsonMap = with_retries_on!(helper, index.clone(), {
            let es = helper.es.read().await;
            es.open_point_in_time(elasticsearch::OpenPointInTimeParts::Index(&[&index])).keep_alive(&keep_alive).send().await
        })?;

        println!("{pit:?}");

        let pit = match pit.remove("id") {
            Some(value) => value,
            None => return Err(ErrorKind::SearchException(format!("Could not establish search point in time.")))
        };

        // Add tie_breaker sort using _shard_doc ID
        sort.push("_shard_doc desc".to_owned());

    //     pit = {'id': self.with_retries(self.datastore.client.open_point_in_time,
    //                                    index=index, keep_alive=keep_alive)['id'],
    //            'keep_alive': keep_alive}

        //     # initial search
    //     resp = self.with_retries(self.datastore.client.search, query=query, pit=pit,
    //                              size=size, timeout=timeout, sort=sort, _source=source)


        Ok(ScrollCursor {
            helper,
            pit_id: pit,
            batch_size,
            keep_alive,
            batch: Default::default(),
            sort,
            index,
            query,
            search_after: None,
            timeout,
            source,
        })
    }

    async fn next(&mut self) -> Result<Option<T>> {
        if self.batch.is_empty() {
            let response: JsonMap = with_retries_on!(self.helper, self.index.clone(), {
                let es = self.helper.es.read().await;
                let sort = self.sort.iter().map(|x|x.as_str()).collect_vec();
                let index = [self.index.as_str()];
                let mut request = es.search(elasticsearch::SearchParts::Index(&index))
                    .body(json!({
                        "query": self.query,
                        "pit": {
                            "id": self.pit_id,
                            "keep_alive": self.keep_alive,
                        },
                        "search_after": self.search_after
                    }))
                    .size(self.batch_size)
                    .sort(&sort)
                    .source(&self.source);
                if let Some(timeout) = &self.timeout {
                    request = request.timeout(&timeout);
                }
                request.send().await
            })?;

            for mut item in extract_results_backwards(response).ok_or(ErrorKind::SearchException(format!("Incomplete search result")))? {
                let sort = item.remove("sort").ok_or(ErrorKind::SearchException(format!("Incomplete search result")))?;
                let body: T = serde_json::from_value(item.remove("_source").ok_or(ErrorKind::SearchException(format!("Incomplete search result")))?)?;
                self.batch.push((sort, body))
            }
        }

        match self.batch.pop() {
            Some((sort, body)) => {
                self.search_after = Some(sort);
                Ok(Some(body))
            },
            None => Ok(None),
        }
    //         while resp["hits"]["hits"]:
    //             search_after = resp['hits']['hits'][-1]['sort']
    //             for hit in resp["hits"]["hits"]:
    //                 yield hit


    //     try:


    //     finally:
    //         try:
    //             self.with_retries(self.datastore.client.close_point_in_time, id=pit['id'])
    //         except elasticsearch.exceptions.NotFoundError:
    //             pass
    // }


        // for value in self.scan_with_search_after(query=query_expression, sort=sort, source=source,
        //     index=index, size=item_buffer_size):
        // # Unpack the results, ensure the id is always set
        // yield self._format_output(value, fl, as_obj=as_obj)
    }

    async fn collect(&mut self) -> Result<Vec<T>> {
        todo!()
    }
}



fn extract_results_backwards(mut response: JsonMap) -> Option<Vec<JsonMap>> {
    let mut hits = if let serde_json::Value::Object(obj) = response.remove("hits")? {
        obj
    } else {
        return None
    };

    if let serde_json::Value::Array(hits) = hits.remove("hits")? {
        let mut output = vec![];
        for item in hits {
            if let serde_json::Value::Object(obj) = item {
                output.push(obj)
            }
        }
        Some(output)
    } else {
        None
    }
}
