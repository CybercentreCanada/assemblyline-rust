use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::meta::flatten_fields;
use assemblyline_models::{ElasticMeta, JsonMap};
use itertools::Itertools;
use log::error;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use struct_metadata::Described;
use thiserror::Error;

use crate::elastic::{responses, DEFAULT_SEARCH_FIELD, KEEP_ALIVE};

use super::{with_retries_on, with_retries_raise_confict, ElasticError, ElasticHelper, Index, Result, Version};

const DEFAULT_SORT: &str = "_id:asc";

pub struct Collection<T: Serialize + DeserializeOwned> {
    database: Arc<ElasticHelper>,
    name: String,
    archive_name: Option<String>,
    _data: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Described<ElasticMeta>> Collection<T> {

    pub fn new(es: Arc<ElasticHelper>, name: String, archive_name: Option<String>) -> Self {
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
                        return Err(ElasticError::ArchiveNotFound(Box::new(self.name.to_uppercase())))
                    }

                    Some(archive_name) => {
                        // Crash if no archive access
                        if !self.database.archive_access {
                            return Err(ElasticError::ArchiveDisabled)
                        }

                        // Return only archive index
                        vec![archive_name.clone()]
                    }
                }
            }

            Some(Index::HotAndArchive) => {
                // Crash if no archive access
                if !self.database.archive_access {
                    return Err(ElasticError::ArchiveDisabled)
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
    pub async fn multiget<RT: for<'de> serde::Deserialize<'de>>(&self, mut key_list: Vec<&str>, error_on_missing: Option<bool>, index_type: Option<Index>) -> Result<HashMap<String, RT>> {
        let error_on_missing = error_on_missing.unwrap_or(true);
        let index_list = self.get_index_list(index_type)?;

        let mut out: HashMap<String, RT> = Default::default();

        for index in index_list {
            let index: &str = &index;
            if key_list.is_empty() {
                break
            }

            // let data = self.with_retries(self.datastore.client.mget, ids=key_list, index=index)
            let result: responses::Multiget = with_retries_on!(self.database, index, {
                let es = self.database.es.read().await;
                let parts = elasticsearch::MgetParts::Index(index);
                es.mget(parts).body(json!({
                    "ids": key_list
                })).send().await    
            })?;

            for mut row in result.docs {
                if !row.found {
                    continue
                }

                // If this index has an archive, check is the document was found in it.
                if self.archive_name.is_some() {
                    if let Some(source) = &mut row._source {
                        source.insert("from_archive".to_owned(), json!(self.is_archive_index(&index)));
                    }
                }

                if let Some(source) = row._source {
                    let value = serde_json::Value::Object(source);
                    if out.insert(row._id.clone(), serde_json::from_value(value)?).is_some() {
                        error!("MGet returned multiple documents for id: {}", row._id);
                    }
                } else {
                    error!("MGet returned a document without any data {}", row._id);
                }
            }

            // only keep keys that are not in the output already
            key_list.retain(|key| !out.contains_key(*key));
        }

        if !key_list.is_empty() && error_on_missing {
            return Err(ElasticError::multi_key_error(&key_list))
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

    pub async fn _get_version<RT: for <'de> serde::Deserialize<'de>>(&self, key: &str, index_type: Option<Index>) -> Result<Option<(RT, Version)>> {
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
    
    async fn _get_if_exists<RT: for <'de> serde::Deserialize<'de>>(&self, key: &str, index_type: Option<Index>) -> Result<Option<(RT, Version)>> {
        let index_list = self.get_index_list(index_type)?;

        for index in index_list {
            let mut doc: responses::Get = with_retries_on!(self.database, index.as_str(), {
                let es = self.database.es.read().await;
                let parts = elasticsearch::GetParts::IndexId(&index, key);
                es.get(parts)._source(&["true"]).send().await
            })?;
            
            // If this index has an archive, check is the document was found in it.
            if self.archive_name.is_some() {
                if let Some(source) = &mut doc._source {
                    source.insert("from_archive".to_owned(), json!(self.is_archive_index(&index)));
                }
            }

            if let Some(source) = doc._source {
                let object = serde_json::from_value(json!(source))?;
                return Ok(Some((object, Version::Expected(doc._primary_term, doc._seq_no))))
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
            let _response: responses::Index = with_retries_raise_confict!(self.database, index.clone(), {
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
    pub async fn stream_search<RT: DeserializeOwned>(&self,
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

        let sort = vec![DEFAULT_SORT.to_owned()]; //parse_sort(DEFAULT_SORT)?;
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
            let result: Result<responses::Index> = with_retries_on!(self.database, index.as_str(), {
                let es = self.database.es.read().await;
                let parts = elasticsearch::UpdateParts::IndexId(&index, id);
                let mut request = es.update(parts)
                    .body(&operations);
                if let Some(retry_on_conflict) = retry_on_conflict {
                    request = request.retry_on_conflict(retry_on_conflict);
                }
                request.send().await
            });

            match result {
                Ok(result) => return Ok(matches!(result.result, responses::IndexResult::Updated)),
                Err(err) => {
                    if err.is_not_found() {
                        continue
                    }
                    return Err(err)
                }
            }
        }

        Ok(false)
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

pub struct ScrollCursor<T: DeserializeOwned> {
    helper: Arc<ElasticHelper>,
    pit_id: String,
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
        let pit: responses::OpenPit = with_retries_on!(helper, index.clone(), {
            let es = helper.es.read().await;
            es.open_point_in_time(elasticsearch::OpenPointInTimeParts::Index(&[&index])).keep_alive(&keep_alive).send().await
        })?;

        // Add tie_breaker sort using _shard_doc ID
        sort.push("_shard_doc:desc".to_owned());

        //     # initial search
    //     resp = self.with_retries(self.datastore.client.search, query=query, pit=pit,
    //                              size=size, timeout=timeout, sort=sort, _source=source)


        Ok(ScrollCursor {
            helper,
            pit_id: pit.id,
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
                    request = request.timeout(timeout);
                }
                request.send().await
            })?;

            todo!()
            // for mut item in extract_results_backwards(response).ok_or(ErrorKind::SearchException(format!("Incomplete search result")))? {
            //     let sort = item.remove("sort").ok_or(ErrorKind::SearchException(format!("Incomplete search result")))?;
            //     let body: T = serde_json::from_value(item.remove("_source").ok_or(ErrorKind::SearchException(format!("Incomplete search result")))?)?;
            //     self.batch.push((sort, body))
            // }
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

    pub async fn collect(mut self) -> Result<Vec<T>> {
        let mut output = vec![];
        while let Some(value) = self.next().await? {
            output.push(value);
        }
        Ok(output)
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
