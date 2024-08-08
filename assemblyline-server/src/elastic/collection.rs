use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::meta::flatten_fields;
use assemblyline_models::{ElasticMeta, JsonMap, Readable};
use itertools::Itertools;
use log::{debug, error, warn};
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use struct_metadata::Described;
use thiserror::Error;

use crate::elastic::{responses, DEFAULT_SEARCH_FIELD, KEEP_ALIVE};

use super::responses::DeleteResult;
use super::{parse_sort, CopyMethod, ElasticError, ElasticHelper, Index, Request, Result, SortDirection, Version};
use super::error::{ElasticErrorInner, WithContext};

const DEFAULT_SORT: &str = "_id asc";
const PIT_KEEP_ALIVE: &str = "5m";

pub struct Collection<T: Serialize + DeserializeOwned> {
    database: Arc<ElasticHelper>,
    name: String,
    archive_name: Option<String>,
    _data: PhantomData<T>,
}

impl<T: Serialize + Readable + Described<ElasticMeta>> Collection<T> {

    pub async fn new(es: Arc<ElasticHelper>, name: String, archive_name: Option<String>) -> Result<Self> {
        let collection = Collection {
            database: es,
            name,
            archive_name,
            _data: Default::default()
        };
        collection.ensure_collection().await?;
        Ok(collection)
    }

    /// This function should test if the collection that you are trying to access does indeed exist
    /// and should create it if it does not.
    pub async fn ensure_collection(&self) -> Result<()> {
        for alias in self.get_index_list(None)? {
            let index = format!("{alias}_hot");

            // Create HOT index
            if !self.database.does_index_exist(&alias).await.context("does_index_exist")? {
                debug!("Index {} does not exist. Creating it now...", alias.to_uppercase());

                let mut mapping = assemblyline_models::meta::build_mapping::<T>().map_err(ElasticError::fatal)?;
                mapping.apply_defaults();

                let body = json!({
                    "mappings": mapping,
                    "settings": self.database.get_index_settings(&self.name, self.is_archive_index(&index))
                });

                if let Err(err) = self.database.make_request_json(&mut 0, &(Method::PUT, self.database.host.join(&index)?).into(), &body).await {
                    if err.is_resource_already_exists() {
                        warn!("Tried to create an index template that already exists: {}", alias.to_uppercase());    
                    } else {
                        return Err(err).context("put index bad request")
                    }
                };

                self.database.put_alias(&index, &alias).await.context("put_alias")?;
            } else if !self.database.does_index_exist(&index).await? && !self.database.does_alias_exist(&alias).await.context("does_alias_exist")? {
                // Hold a write block for the rest of this section
                // self.with_retries(self.datastore.client.indices.put_settings, index=alias, settings=write_block_settings)
                let settings_url = self.database.host.join(&format!("{index}/_settings"))?;
                let settings_request = (Method::PUT, settings_url).into();
                self.database.make_request_json(&mut 0, &settings_request, &json!({"index.blocks.write": true})).await.context("create write block")?;
        
                // Create a copy on the result index
                self.database.safe_index_copy(CopyMethod::Clone, &alias, &index, None, None).await?;

                // Make the hot index the new clone
                // self.with_retries(self.datastore.client.indices.update_aliases, actions=actions)
                self.database.make_request_json(&mut 0, &(Method::POST, self.database.host.join("_aliases")?).into(), &json!({
                    "actions": [
                        {"add":  {"index": index, "alias": alias}}, 
                        {"remove_index": {"index": alias}}
                    ]
                })).await?;

                // self.with_retries(self.datastore.client.indices.put_settings, index=alias, settings=write_unblock_settings)
                self.database.make_request_json(&mut 0, &settings_request, &json!({"index.blocks.write": null})).await?;
            }
        }

        // todo!("self._check_fields()")
        Ok(())
    }

    /// Make an http request with no body
    async fn make_request(&self, request: &Request) -> Result<reqwest::Response> {
        let mut attempt = 0;
        loop {
            match self.database.make_request(&mut attempt, request).await {
                Ok(response) => break Ok(response),
                Err(err) if err.is_index_not_found() => {
                    self.ensure_collection().await?;
                    continue    
                },
                Err(err) => break Err(err)    
            }
        }     
    }

    /// Make an http request with a json body
    async fn make_request_json<R: Serialize>(&self, request: &Request, body: &R) -> Result<reqwest::Response> {
        let mut attempt = 0;
        loop {
            match self.database.make_request_json(&mut attempt, request, body).await {
                Ok(response) => break Ok(response),
                Err(err) if err.is_index_not_found() => {
                    self.ensure_collection().await?;
                    continue    
                },
                Err(err) => break Err(err)    
            }
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
                        return Err(ElasticErrorInner::ArchiveNotFound(self.name.to_uppercase()).into())
                    }

                    Some(archive_name) => {
                        // Crash if no archive access
                        if !self.database.archive_access {
                            return Err(ElasticErrorInner::ArchiveDisabled.into())
                        }

                        // Return only archive index
                        vec![archive_name.clone()]
                    }
                }
            }

            Some(Index::HotAndArchive) => {
                // Crash if no archive access
                if !self.database.archive_access {
                    return Err(ElasticErrorInner::ArchiveDisabled.into())
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

    fn is_archive_index(&self, index: &str) -> bool {
        if let Some(archive_name) = &self.archive_name {
            index.starts_with(archive_name)
        } else {
            false
        }
    }

    /// Get a list of documents from the datastore and make sure they are normalized using the model class
    ///
    /// :param index_type: Type of indices to target
    /// :param error_on_missing: Should it raise a key error when keys are missing
    /// :param as_dictionary: Return a disctionary of items or a list
    /// :param as_obj: Return objects or not
    /// :param key_list: list of keys of documents to get
    /// :return: list of instances of the model class
    pub async fn multiget<RT: Readable>(&self, ids: &[&str], error_on_missing: Option<bool>, index_type: Option<Index>) -> Result<HashMap<String, RT>> {
        if ids.is_empty() { return Ok(Default::default()) }

        let error_on_missing = error_on_missing.unwrap_or(true);
        let index_list = self.get_index_list(index_type)?;

        // where to collect output
        let mut out: HashMap<String, RT> = Default::default();

        // track which documents are outstanding
        let mut outstanding = vec![];

        for index in index_list {
            // prepare the url
            let mut url = self.database.host.join(&format!("{index}/_mget"))?;
            url.query_pairs_mut().append_pair("_source", "true");

            // prepare the request body
            let body = if outstanding.is_empty() {
                json!({ "ids": ids })
            } else {
                json!({ "ids": outstanding })
            };

            // fetch all the documents
            let response = self.make_request_json(&(Method::GET, url).into(), &body).await.context("mget request")?;
            let response: responses::Multiget<RT, ()> = response.json().await?;

            // track which ones we have found
            outstanding.clear();
            for row in response.docs {
                let _id = row._id.clone();
                if let Some(mut body) = row._source {
                    // If this index has an archive, check is the document was found in it.
                    if self.archive_name.is_some() {
                        body.set_from_archive(self.is_archive_index(&index));
                    }

                    if out.insert(_id, body).is_some() {
                        error!("MGet returned multiple documents for id: {}", row._id);
                    }
                } else {
                    outstanding.push(_id)
                }
            }

            // finish if we have found everything we want
            if outstanding.is_empty() {
                break
            }
        }

        if !outstanding.is_empty() && error_on_missing {
            return Err(ElasticError::multi_key_error(outstanding))
        }

        return Ok(out)
    }

    /// fetch an object from elastic, retrying on missing
    pub async fn get(&self, key: &str, index_type: Option<Index>) -> Result<Option<T>> {
        Ok(self._get_version(key, index_type).await?.map(|(doc, _)| doc))
    }

    pub async fn get_json(&self, key: &str, index_type: Option<Index>) -> Result<Option<JsonMap>> {
        Ok(self._get_version(key, index_type).await?.map(|(doc, _)| doc))
    }

    /// fetch an object from elastic, retrying on missing, returning document version info
    pub async fn get_version(&self, key: &str, index_type: Option<Index>) -> Result<Option<(T, Version)>> {
        self._get_version(key, index_type).await
    }

    pub async fn _get_version<RT: Readable>(&self, key: &str, index_type: Option<Index>) -> Result<Option<(RT, Version)>> {
        const RETRY_NORMAL: usize = 1;
        for _attempt in 0..=RETRY_NORMAL {
            match self._get_if_exists(key, index_type).await? {
                Some(data) => return Ok(Some(data)),
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                },
            }
        }
        Ok(None)
    }

    /// fetch an object from elastic, no retry on missing, returning document version info
    /// 
    /// Versioned get-save for atomic update has three paths:
    ///   1. Document doesn't exist at all. Create token will be returned for version.
    /// This way only the first query to try and create the document will succeed.
    ///   2. Document exists. A version string with the info needed to do a versioned save is returned.
    ///
    /// The create token is needed to differentiate between "I'm saving a new
    /// document non-atomic (version=None)" and "I'm saving a new document
    /// atomically (version=CREATE_TOKEN)".
    pub async fn get_if_exists(&self, key: &str, index_type: Option<Index>) -> Result<Option<(T, Version)>> {
        self._get_if_exists(key, index_type).await
    }
    
    async fn _get_if_exists<RT: Readable>(&self, key: &str, index_type: Option<Index>) -> Result<Option<(RT, Version)>> {
        let index_list = self.get_index_list(index_type)?;

        for index in index_list {
            // prepare the url
            let mut url = self.database.host.join(&format!("{index}/_doc/{key}"))?;
            url.query_pairs_mut().append_pair("_source", "true");

            // fetch all the documents
            let mut response: responses::Get<RT, ()> = match self.make_request(&(Method::GET, url).into()).await {
                Ok(response) => response.json().await?,
                Err(err) if err.is_document_not_found() => continue,
                Err(err) => return Err(err)
            };
           
            // If this index has an archive, check is the document was found in it.
            if self.archive_name.is_some() {
                if let Some(source) = &mut response._source {
                    source.set_from_archive(self.is_archive_index(&index));
                }
            }

            if let Some(source) = response._source {
                return Ok(Some((source, Version::Expected{primary_term: response._primary_term, sequence_number: response._seq_no})))
            }
        }
        Ok(None)
    }

    pub async fn save(&self, key: &str, value: &T, version: Option<Version>, index_type: Option<Index>) -> Result<()> {
        if key.contains(' ') {
            return Err(ElasticError::json("You are not allowed to use spaces in datastore keys."))
        }

        let mut saved_data = serde_json::to_value(value)?;
        let data = if let Some(data) = saved_data.as_object_mut() {
            data
        } else {
            return Err(ElasticError::json("Types saved in elastic must serialize to a json document."))
        };

        self.save_json(key, data, version, index_type).await
    }

    pub async fn save_json(&self, key: &str, data: &mut JsonMap, version: Option<Version>, index_type: Option<Index>) -> Result<()> {
        let index_type = index_type.unwrap_or(Index::Hot);
        if key.contains(' ') {
            return Err(ElasticError::json("You are not allowed to use spaces in datastore keys."))
        }
        data.insert("id".to_owned(), json!(key));

        let mut operation = "index";
        let mut version_parts = None;

        if let Some(version) = version {
            match version {
                Version::Create => {
                    operation = "create";
                },
                Version::Expected { primary_term, sequence_number } => {
                    version_parts = Some((primary_term, sequence_number));
                },
            }
        }

        let index_list = self.get_index_list(Some(index_type))?;
        for index in index_list {
            // build the url for the operation type
            let mut url = if operation == "index" {
                self.database.host.join(&format!("{}/_doc/{key}", index))?
            } else {
                self.database.host.join(&format!("{}/_create/{key}", index))?
            };

            url.query_pairs_mut()
                .append_pair("op_type", operation)
                .append_pair("require_alias", "true");

            if let Some((primary_term, seq_no)) = version_parts {
                url.query_pairs_mut()
                    .append_pair("if_seq_no", &seq_no.to_string())
                    .append_pair("if_primary_term", &primary_term.to_string());
            }

            self.make_request_json(&Request::with_raise_conflict(Method::PUT, url), &data).await?;
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
    pub async fn stream_search<RT: DeserializeOwned + Debug>(&self,
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
            return Err(ElasticError::fatal("Variable item_buffer_size must be between 50 and 2000."))
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

        let sort = parse_sort(DEFAULT_SORT)?;
        let source = if fl.is_empty() || fl == "*" {
            "true".to_owned()
        } else {
            fl
        };
        // let source = match fl {
        //     Some(fl) => fl,
        //     None => list(self.stored_fields.keys())
        // };

        ScrollCursor::new(self.database.clone(), index, query_expression, sort, source, None, Some(item_buffer_size), None).await
    }


    pub async fn update(&self, id: &str, mut operations: OperationBatch, index_type: Option<Index>, retry_on_conflict: Option<i64>) -> Result<bool> {
        let index_type = index_type.unwrap_or(Index::Hot);
        operations.validate_operations::<T>()?;
        let operations = operations.to_script();

        for index in self.get_index_list(Some(index_type))? {
            let mut url = self.database.host.join(&format!("/{index}/_update/{id}"))?;
            if let Some(retry_on_conflict) = retry_on_conflict {
                url.query_pairs_mut().append_pair("retry_on_conflict", &retry_on_conflict.to_string());
            }

            let result = self.make_request_json(&(Method::POST, url).into(), &operations).await;
            let body: responses::Index = match result {
                Ok(response) => response.json().await?,
                Err(err) => {
                    if err.is_document_not_found() {
                        continue
                    }
                    return Err(err)
                }
            };

            return Ok(matches!(body.result, responses::IndexResult::Updated))
        }

        Ok(false)
    }

    /// This function should be overloaded to perform a commit of the index data of all the different hosts
    /// specified in self.datastore.hosts.
    ///
    /// :param index_type: Type of indices to target
    /// :return: Should return True of the commit was successful on all hosts
    pub async fn commit(&self, index_type: Option<Index>) -> Result<()> {
        for index in self.get_index_list(index_type)? {
            self.make_request(&(Method::POST, self.database.host.join(&format!("{index}/_refresh"))?).into()).await?;
            self.make_request(&(Method::POST, self.database.host.join(&format!("{index}/_cache/clear"))?).into()).await?;
        }
        Ok(())
    }

    /// This function should delete the underlying document referenced by the key.
    /// It should return true if the document was in fact properly deleted.
    /// 
    /// :param index_type: Type of indices to target
    /// :param key: id of the document to delete
    /// :return: True is delete successful
    pub async fn delete(&self, key: &str, index_type: Option<Index>) -> Result<bool> {
        let index_list = self.get_index_list(index_type)?;

        let mut deleted = false;
        for index in index_list {
            let url = self.database.host.join(&format!("/{index}/_doc/{key}"))?;
            let result = self.make_request(&(Method::DELETE, url).into()).await;
            match result {
                Ok(response) => {
                    let response: responses::Delete = response.json().await?;
                    if matches!(response.result, DeleteResult::Deleted) {
                        deleted = true;
                    }
                },
                Err(err) if err.is_document_not_found() => { deleted = true; }
                Err(err) => return Err(err)
            }
        }

        return Ok(deleted)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum UpdateOperation {
    Append,
    AppendIfMissing,
    Dec,
    Inc,
    Max,
    Min,
    Prepend,
    PrependIfMissing,
    Remove,
    Set,
    Delete,
}

#[derive(Error, Debug)]
pub enum InvalidOperationError {
    #[error("field {field} not found in {model}")]
    InvalidField { field: String, model: String },
    #[error("Invalid operation for field {field}: {operation:?}")]
    InvalidOperation { field: String, operation: UpdateOperation },
    #[error("Invalid value for field {field}: {value}")]
    Value { field: String, value: String }
}

#[derive(Default)]
pub struct OperationBatch {
    operations: Vec<(UpdateOperation, String, serde_json::Value)>,
}

impl OperationBatch {

    pub fn set(&mut self, field: String, value: serde_json::Value) {
        self.operations.push((UpdateOperation::Set, field, value))
    }

    pub fn increment(&mut self, field: String, value: serde_json::Value) {
        self.operations.push((UpdateOperation::Inc, field, value))
    }

    pub fn max(&mut self, field: String, value: serde_json::Value) {
        self.operations.push((UpdateOperation::Max, field, value))
    }

    // Validate the different operations received for a partial update
    //
    // TODO: When the field is of type Mapping, the validation/check only works for depth 1. A full recursive
    //       solution is needed to support multi-depth cases.
    pub fn validate_operations<Model: Described<ElasticMeta>>(&mut self) -> Result<(), InvalidOperationError> {

        // if self.model_class:
        //     fields = self.model_class.flat_fields(show_compound=True)
        //     if 'classification in fields':
        //         fields.update({"__access_lvl__": Integer(),
        //                        "__access_req__": List(Keyword()),
        //                        "__access_grp1__": List(Keyword()),
        //                        "__access_grp2__": List(Keyword())})
        // else:
        //     fields = None
        let metadata = Model::metadata();
        let model_name = metadata.kind.name().to_owned();
        let fields = flatten_fields(&metadata);
        let fields: HashMap<String, _> = fields.into_iter()
            .map(|(key, value)|(key.join("."), value))
            .collect();

        for (op, doc_key, value) in &mut self.operations {

            let (field, multivalued) = match fields.get(doc_key) {
                Some(field) => *field,
                None => {
                    // let prev_key = None;
                    if let Some((prev_key, _)) = doc_key.rsplit_once('.') {
                        if let Some((field, multivalued)) = fields.get(prev_key) {
                            if let struct_metadata::Kind::Mapping(kind, ..) = &field.kind {
                                (kind.as_ref(), *multivalued)
                            } else {
                                return Err(InvalidOperationError::InvalidField{ field: doc_key.to_owned(), model: model_name })
                            }
                        } else {
                            return Err(InvalidOperationError::InvalidField{ field: doc_key.to_owned(), model: model_name })
                        }        
                    } else {
                        return Err(InvalidOperationError::InvalidField{ field: doc_key.to_owned(), model: model_name })
                    }    
                }
            };

            match op {
                UpdateOperation::Append | UpdateOperation::AppendIfMissing |
                UpdateOperation::Prepend | UpdateOperation::PrependIfMissing |
                UpdateOperation::Remove => {
                    if !multivalued {
                        return Err(InvalidOperationError::InvalidOperation{ field: doc_key.to_owned(), operation: *op });
                    }

                    if check_type(&field.kind, value).is_err() {
                        return Err(InvalidOperationError::Value { field: doc_key.to_owned(), value: value.to_string() })
                    }
                }

                UpdateOperation::Dec |
                UpdateOperation::Inc => {
                    if check_type(&field.kind, value).is_err() {
                        return Err(InvalidOperationError::Value { field: doc_key.to_owned(), value: value.to_string() })
                    }
                }

                UpdateOperation::Set => {
                    if multivalued && value.is_array() {
                        for value in value.as_array_mut().unwrap() {
                            if check_type(&field.kind, value).is_err() {
                                return Err(InvalidOperationError::Value { field: doc_key.to_owned(), value: value.to_string() })
                            }        
                        }
                    } else if check_type(&field.kind, value).is_err() {
                        return Err(InvalidOperationError::Value { field: doc_key.to_owned(), value: value.to_string() })
                    }
                }

                UpdateOperation::Max => {},
                UpdateOperation::Min => {},
                UpdateOperation::Delete => {},
            }

            // if isinstance(value, Model):
            //     value = value.as_primitives()
            // elif isinstance(value, datetime):
            //     value = value.isoformat()
            // elif isinstance(value, ClassificationObject):
            //     value = str(value)
        }
        Ok(())
    }

    pub fn to_script(&self) -> serde_json::Value {
        let mut op_sources = vec![];
        let mut op_params = HashMap::<String, serde_json::Value>::new();
        let mut val_id = 0;
        for (op, doc_key, value) in &self.operations {
            match op {
                UpdateOperation::Set => {
                    op_sources.push(format!("ctx._source.{doc_key} = params.value{val_id}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Delete => {
                    op_sources.push(format!("ctx._source.{doc_key}.remove(params.value{val_id})"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Append => {
                    op_sources.push(format!("
                        if (ctx._source.{doc_key} == null) {{ctx._source.{doc_key} = new ArrayList()}} 
                        ctx._source.{doc_key}.add(params.value{val_id})"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::AppendIfMissing => {
                    op_sources.push(format!("
                        if (ctx._source.{doc_key} == null) {{ctx._source.{doc_key} = new ArrayList()}} 
                        if (ctx._source.{doc_key}.indexOf(params.value{val_id}) == -1) 
                            {{ctx._source.{doc_key}.add(params.value{val_id})}}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Prepend => {
                    op_sources.push(format!("ctx._source.{doc_key}.add(0, params.value{val_id})"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::PrependIfMissing => {
                    op_sources.push(format!("
                        if (ctx._source.{doc_key}.indexOf(params.value{val_id}) == -1) 
                            {{ctx._source.{doc_key}.add(0, params.value{val_id})}}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Remove => {
                    op_sources.push(format!("
                        if (ctx._source.{doc_key}.indexOf(params.value{val_id}) != -1) 
                            {{ctx._source.{doc_key}.remove(ctx._source.{doc_key}.indexOf(params.value{val_id}))}}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Inc => {
                    op_sources.push(format!("ctx._source.{doc_key} += params.value{val_id}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Dec => {
                    op_sources.push(format!("ctx._source.{doc_key} -= params.value{val_id}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Max => {
                    op_sources.push(format!("
                        if (ctx._source.{doc_key} == null || ctx._source.{doc_key}.compareTo(params.value{val_id}) < 0) 
                            {{ctx._source.{doc_key} = params.value{val_id}}}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                },
                UpdateOperation::Min => {
                    op_sources.push(format!("
                        if (ctx._source.{doc_key} == null || ctx._source.{doc_key}.compareTo(params.value{val_id}) > 0) 
                            {{ctx._source.{doc_key} = params.value{val_id}}}"));
                    op_params.insert(format!("value{val_id}"), value.clone());
                }
            }

            val_id += 1;
        }

        let joined_sources = op_sources.into_iter().join(";\n");

        json!({
            "lang": "painless",
            "source": joined_sources.replace("};\n", "}\n"),
            "params": op_params
        })
    }

}

#[derive(thiserror::Error, Debug)]
pub enum CheckError {
    #[error("{type_name} expected an {expected_type} but got {received_type}")]
    Type { type_name: &'static str, expected_type: &'static str, received_type: &'static str },

    #[error("{type_name} requires field {field}")]
    MissingField{ type_name: &'static str, field: &'static str },

    #[error("JSON object keys must be strings, {key_type} provided instead")]
    JsonKeys{ key_type: &'static str },

    #[error("{value} is not a known varient of enum {type_name}")]
    NonEnumVarient{ type_name: &'static str, value: &'static str },

    #[error("{type_name} is unknown and unhandled")]
    Unhandled{ type_name: &'static str },
}

impl CheckError {
    fn expected_object(name: &'static str, received: &'static str) -> Self {
        Self::Type { type_name: name, expected_type: "Object", received_type: received }
    }
    fn expected_array(name: &'static str, received: &'static str) -> Self {
        Self::Type { type_name: name, expected_type: "Array", received_type: received }
    }
    fn rename(self, name: &'static str) -> Self {
        match self {
            Self::Type { type_name: _, expected_type, received_type } => Self::Type { type_name: name, expected_type, received_type },
            Self::MissingField { type_name: _, field } => Self::MissingField { type_name: name, field },
            Self::JsonKeys { key_type } => Self::JsonKeys { key_type },
            Self::Unhandled { type_name: _ } => Self::Unhandled { type_name: name },
            Self::NonEnumVarient { type_name: _, value } => Self::NonEnumVarient { type_name: name, value },
        }
    }
}

fn describe_value(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "Null",
        serde_json::Value::Bool(_) => "Bool",
        serde_json::Value::Number(_) => "Number",
        serde_json::Value::String(_) => "String",
        serde_json::Value::Array(_) => "Array",
        serde_json::Value::Object(_) => "Object",
    }
}

pub fn check_type(kind: &struct_metadata::Kind<ElasticMeta>, value: &mut serde_json::Value) -> Result<(), CheckError> {
    match kind {
        struct_metadata::Kind::Struct { name, children } => {
            // check that the value being read is an object
            let map = match value.as_object_mut() {
                Some(value) => value,
                None => return Err(CheckError::expected_object(name, describe_value(value)))
            };

            let mut viewed = HashSet::new();

            // ensure require fields are found
            'fields: for field in children {
                // try to find the field under each possible name for it
                for alias in field.aliases.iter() {
                    if viewed.contains(alias) { continue }
                    if let Some(entry) = map.get_mut(*alias) {
                        check_type(&field.type_info.kind, entry)?;
                        viewed.insert(*alias);
                        continue 'fields
                    }
                }

                // field not found, thats ok if the field has a default value
                if field.has_default {
                    continue
                }

                // neither value nor default value can be found
                return Err(CheckError::MissingField { type_name: name, field: field.label })
            }

            // remove extra fields
            map.retain(|key, _| viewed.contains(key.as_str()));
            Ok(())
        },
        struct_metadata::Kind::Aliased { kind, name } => check_type(&kind.kind, value).map_err(|err|err.rename(name)),
        struct_metadata::Kind::Enum { name, variants } => {
            if let Some(value) = value.as_str() {
                for var in variants {
                    for name in var.aliases.iter() {
                        if value == *name {
                            return Ok(())
                        }
                    }
                }
            }
            Err(CheckError::NonEnumVarient{ type_name: name, value: describe_value(value)})
        },
        struct_metadata::Kind::Sequence(child) => {
            // try the case where value is directly a sequence of type `child` items
            let result = (|| {
                let array = match value.as_array_mut() {
                    Some(array) => array,
                    None => return Err(CheckError::expected_array(kind.name(), describe_value(value)))
                };

                for item in array {
                    check_type(&child.kind, item)?;
                }
                Ok(())
            })();
            
            // if that failed, test if value is an instance of `child` directly and if so wrap it in an array
            #[allow(clippy::collapsible_if)]
            if result.is_err() {
                if check_type(&child.kind, value).is_ok() {
                    *value = serde_json::Value::Array(vec![value.clone()]);
                    return Ok(())
                }
            }
            result
        },
        struct_metadata::Kind::Option(inner) => {
            if value.is_null() {
                return Ok(())
            } 
            
            check_type(&inner.kind, value)
        },
        struct_metadata::Kind::Mapping(key_type, value_type) => {
            // check that the value being read is an object
            let map = match value.as_object_mut() {
                Some(value) => value,
                None => return Err(CheckError::expected_object(kind.name(), describe_value(value)))
            };

            if !matches!(key_type.kind, struct_metadata::Kind::String) {
                return Err(CheckError::JsonKeys { key_type: key_type.kind.name() })
            }

            for value in map.values_mut() {
                check_type(&value_type.kind, value)?;
            }
            Ok(())
        },
        struct_metadata::Kind::DateTime => try_cast::<chrono::DateTime<chrono::Utc>>("DateTime", "String", value),
        struct_metadata::Kind::String => try_cast::<String>("String", "String", value),
        struct_metadata::Kind::U128 => try_cast::<u128>("U128", "Number", value),
        struct_metadata::Kind::I128 => try_cast::<i128>("I128", "Number", value),
        struct_metadata::Kind::U64 => try_cast::<u64>("U64", "Number", value),
        struct_metadata::Kind::I64 => try_cast::<i64>("I64", "Number", value),
        struct_metadata::Kind::U32 => try_cast::<u32>("U32", "Number", value),
        struct_metadata::Kind::I32 => try_cast::<i32>("I32", "Number", value),
        struct_metadata::Kind::U16 => try_cast::<u16>("U16", "Number", value),
        struct_metadata::Kind::I16 => try_cast::<i16>("I16", "Number", value),
        struct_metadata::Kind::U8 => try_cast::<u8>("U8", "Number", value),
        struct_metadata::Kind::I8 => try_cast::<i8>("I8", "Number", value),
        struct_metadata::Kind::F64 => try_cast::<f64>("F64", "Number", value),
        struct_metadata::Kind::F32 => try_cast::<f32>("F32", "Number", value),
        struct_metadata::Kind::Bool => try_cast::<bool>("bool", "Boolean", value),
        struct_metadata::Kind::Any => Ok(()),
        _ => Err(CheckError::Unhandled { type_name: kind.name() }),
    }
}

fn try_cast<Type: serde::de::DeserializeOwned>(name: &'static str, expected: &'static str, value: &serde_json::Value) -> Result<(), CheckError> {
    if serde_json::from_value::<Type>(value.clone()).is_ok() {
        Ok(())
    } else {
        Err(CheckError::Type { type_name: name, expected_type: expected, received_type: describe_value(value) })
    }
}

struct PitGuard {
    helper: Arc<ElasticHelper>,
    id: String,
}

impl PitGuard {
    async fn open(helper: Arc<ElasticHelper>, index: &str) -> Result<Self> {
        let mut url = helper.host.join(&format!("{}/_pit", index))?;
        url.query_pairs_mut().append_pair("keep_alive", PIT_KEEP_ALIVE);
        let response = helper.make_request(&mut 0, &(Method::POST, url).into()).await?;
        let pit: responses::OpenPit = response.json().await?;
        Ok(Self { helper, id: pit.id })
    }

    async fn close(helper: Arc<ElasticHelper>, id: String) -> Result<()> {
        let url = helper.host.join("/_pit")?;
        let response = helper.make_request_json(&mut 0, &(Method::DELETE, url).into(), &json!({
            "id": id
        })).await?;
        let _body: responses::ClosePit = response.json().await?;
        Ok(())
    }
}

impl Drop for PitGuard {
    fn drop(&mut self) {
        let mut id = String::new();
        std::mem::swap(&mut self.id, &mut id);
        if !id.is_empty() {
            let helper = self.helper.clone();
            tokio::spawn(async move {
                if let Err(err) = Self::close(helper, id).await {
                    error!("Error closing pit: {err}");
                }
            });
        }
    }
}

pub struct ScrollCursor<T: DeserializeOwned> {
    helper: Arc<ElasticHelper>,
    pit: PitGuard,
    batch_size: i64,
    keep_alive: String,
    batch: Vec<T>,
    sort: Vec<(String, SortDirection)>,
    query: serde_json::Value,
    search_after: Option<serde_json::Value>,
    timeout: Option<std::time::Duration>,
    source: String,
    finished: bool,
}

impl<T: DeserializeOwned + std::fmt::Debug> ScrollCursor<T> {

    async fn new(helper: Arc<ElasticHelper>,
        index: String,
        query: serde_json::Value,
        mut sort: Vec<(String, SortDirection)>,
        source: String,
        keep_alive: Option<String>,
        size: Option<i64>,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self> {
        let keep_alive = keep_alive.unwrap_or(KEEP_ALIVE.to_owned());
        let batch_size = size.unwrap_or(1000);

        // Add tie_breaker sort using _shard_doc ID
        sort.push(("_shard_doc".to_string(), SortDirection::Descending));

        //     # initial search
    //     resp = self.with_retries(self.datastore.client.search, query=query, pit=pit,
    //                              size=size, timeout=timeout, sort=sort, _source=source)

        Ok(ScrollCursor {
            pit: PitGuard::open(helper.clone(), &index).await?,
            helper,
            batch_size,
            keep_alive,
            batch: Default::default(),
            sort,
            query,
            search_after: None,
            timeout,
            source,
            finished: false,
        })
    }

    async fn next(&mut self) -> Result<Option<T>> {
        if self.finished {
            return Ok(None)
        }

        if self.batch.is_empty() {
            let sort = self.sort.iter().map(|(name, direction)|format!("{name}:{direction}")).collect_vec();

            let mut url = self.helper.host.join("/_search")?;
            url.query_pairs_mut()
            .append_pair("size", &self.batch_size.to_string())
            .append_pair("sort", &sort.join(","))
            .append_pair("_source", &self.source.to_string());

            // if let Some(search_after) = &self.search_after {
            //     url.query_pairs_mut().append_pair("search_after", &search_after.to_string());
            // }

            if let Some(timeout) = &self.timeout {
                url.query_pairs_mut().append_pair("timeout", &format!("{}ms", timeout.as_millis()));
            }

            let mut body = JsonMap::new();
            body.insert("query".to_string(), self.query.clone());
            body.insert("pit".to_string(), json!({
                "id": self.pit.id,
                "keep_alive": self.keep_alive,
            }));

            if let Some(search_after) = &self.search_after {
                body.insert("search_after".to_string(), search_after.clone());
            }

            let response = self.helper.make_request_json(&mut 0, &(Method::GET, url).into(), &body).await?;
            let mut response: responses::Search<(), T> = response.json().await?;

            match response.hits.hits.last() {
                Some(row) => {
                    self.search_after = Some(row.sort.clone());
                },
                None => {
                    self.finished = true;
                    return Ok(None)
                },
            }

            // move the results into self.batch in reverse
            while let Some(row) = response.hits.hits.pop() {
                if let Some(source) = row._source {
                    self.batch.push(source);
                }
            }
        }

        // take results from back to front from the batch buffer (already reversed above so this is forward order)
        Ok(self.batch.pop())
    }

    pub async fn collect(mut self) -> Result<Vec<T>> {
        let mut output = vec![];
        while let Some(value) = self.next().await? {
            output.push(value);
        }
        Ok(output)
    }
}



// fn extract_results_backwards(mut response: JsonMap) -> Option<Vec<JsonMap>> {
//     let mut hits = if let serde_json::Value::Object(obj) = response.remove("hits")? {
//         obj
//     } else {
//         return None
//     };

//     if let serde_json::Value::Array(hits) = hits.remove("hits")? {
//         let mut output = vec![];
//         for item in hits {
//             if let serde_json::Value::Object(obj) = item {
//                 output.push(obj)
//             }
//         }
//         Some(output)
//     } else {
//         None
//     }
// }
