
use std::collections::VecDeque;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

use assemblyline_models::datastore::filescore::FileScore;
use assemblyline_models::datastore::user::User;
use assemblyline_models::{JsonMap, Sha256};
use assemblyline_models::datastore::{File, Submission, Error as ErrorModel, Service};
use chrono::{Duration, DateTime, Utc};
use elasticsearch::{Elasticsearch, http};
use itertools::Itertools;
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::{Serialize};
use serde_json::json;

use crate::error::{Result, Error as ErrorKind};

enum Index {
    Hot = 1,
    Archive = 2,
    HotAndArchive = 3
}

const DEFAULT_SEARCH_FIELD: &str = "__text__";
const KEEP_ALIVE: &str = "5m";
const DEFAULT_SORT: &str = "_id: asc";
const MAX_RETRY_BACKOFF: u64 = 10;


fn parse_sort(sort: &str) -> Result<Vec<String>> {
    todo!()
//     """
//     This function tries to do two things at once:
//         - convert AL sort syntax to elastic,
//         - convert any sorts on the key _id to id
//     """
//     if sort is None:
//         return sort

//     if isinstance(sort, list):
//         return [parse_sort(row, ret_list=False) for row in sort]
//     elif isinstance(sort, dict):
//         return {('id' if key == '_id' else key): value for key, value in sort.items()}
//     elif "," in sort:
//         return [parse_sort(row.strip(), ret_list=False) for row in sort.split(',')]

//     parts = sort.split(' ')
//     if len(parts) == 1:
//         if parts == '_id':
//             if ret_list:
//                 return ['id']
//             return 'id'
//         if ret_list:
//             return [parts]
//         return parts
//     elif len(parts) == 2:
//         if parts[1] not in ['asc', 'desc']:
//             raise SearchException('Unknown sort parameter ' + sort)
//         if parts[0] == '_id':
//             if ret_list:
//                 return [{'id': parts[1]}]
//             return {'id': parts[1]}
//         if ret_list:
//             return [{parts[0]: parts[1]}]
//         return {parts[0]: parts[1]}
//     raise SearchException('Unknown sort parameter ' + sort)
}

macro_rules! with_retries_on {
    ($helper:expr, $index_name:expr, $expression:expr) => {{
        with_retries_detail!($helper, Some($index_name), false, $expression)
    }}
}

macro_rules! with_retries_raise_confict {
    ($helper:expr, $index_name:expr, $expression:expr) => {{
        with_retries_detail!($helper, Some($index_name), true, $expression)
    }}
}

/// This function evaluates the passed expression and reconnect if it fails
macro_rules! with_retries_detail {
    ($helper:expr, $index_name:expr, $raise_conflicts:expr, $expression:expr) => {{
        let mut retries = 0;
        // let updated = 0;
        // let deleted = 0;
        loop {
            // If this isn't the first time we have tried, wait and reset the connection
            if retries > 0 {
                let sleep_seconds = MAX_RETRY_BACKOFF.min(retries);
                tokio::time::sleep(tokio::time::Duration::from_secs(sleep_seconds)).await;
                $helper.connection_reset().await?;
            }
            retries += 1;

            // run the actual code being retried
            let result = $expression;

            // Unify server and client errors
            let response = match result {
                // valid response or server error
                Ok(response) => response.error_for_status_code(),
                // client error
                Err(err) => Err(err)
            };

            // handle non errors, extract errors
            let original_err = match response {
                Ok(response) => {
                    if retries > 0 {
                        info!("Reconnected to elasticsearch!");
                    }

                    // if updated:
                    //     ret_val['updated'] += updated

                    // if deleted:
                    //     ret_val['deleted'] += deleted

                    break Ok(response.json().await?)
                },
                Err(err) => err
            };

            // Internal library errors are terminal and we stop here
            let source_err = match original_err.source() {
                Some(err) => err,
                None => break Err(ErrorKind::SearchException(original_err.to_string()))
            };

            let hosts = $helper.get_hosts_safe().join(" | ");

            // Some manner of io error, just print a warning and retry, lots of ephemeral errors
            // will resolve to this type where socket methods have failed
            if let Some(err) = source_err.downcast_ref::<std::io::Error>() {
                warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, err, stringify!($expression));
                continue
            }

            // Json decoding/encoding error, will probably repeat if we retry, break with error
            if let Some(err) = source_err.downcast_ref::<serde_json::error::Error>() {
                break Err(ErrorKind::SearchException(err.to_string()))
            }

            // HTTP library error
            if let Some(err) = source_err.downcast_ref::<reqwest::Error>() {
                // A timeout
                if err.is_timeout() {
                    warn!("Elasticsearch connection timeout, server(s): {}, retrying...", hosts);
                    continue
                }

                // a connection error
                if err.is_connect() {
                    warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, err, stringify!($expression));
                    continue
                }

                // handle error types that have HTTP status codes associated
                if let Some(status) = err.status() {

                    if http::StatusCode::NOT_FOUND == status {
                        let err_message = err.to_string();

                        // Validate exception type
                        if $index_name.is_some() || !err_message.contains("No search context found") {
                            break Err(ErrorKind::SearchException(err_message))
                        }

                        let index = $index_name.unwrap_or("".to_owned()).to_uppercase();
                        warn!("Index {} was removed while a query was running, retrying...", index);
                        continue
                    } else if http::StatusCode::CONFLICT == status {
                        if $raise_conflicts {
                            // De-sync potential treads trying to write to the index
                            tokio::time::sleep(tokio::time::Duration::from_secs_f64(rand::random::<f64>() * 0.1)).await;
                            break Err(ErrorKind::VersionConflictException(err.to_string()))
                        }
                        // updated += ce.info.get('updated', 0)
                        // deleted += ce.info.get('deleted', 0)
                        continue

                    } else if http::StatusCode::FORBIDDEN == status {
                        match $index_name {
                            None => break Err(original_err.into()),
                            Some(index) => {
                                log::warn!("Elasticsearch cluster is preventing writing operations on index {}, retrying...", index);
                            }
                        }
                        continue
                    } else if http::StatusCode::SERVICE_UNAVAILABLE == status {
                        let index = $index_name.unwrap_or("".to_owned()).to_uppercase();

                        // Display proper error message
                        log::warn!("Looks like index {} is not ready yet, retrying...", index);
                        continue
                    } else if http::StatusCode::TOO_MANY_REQUESTS == status {
                        let index = $index_name.unwrap_or("".to_owned()).to_uppercase();
                        log::warn!("Elasticsearch is too busy to perform the requested task on index {}, retrying...", index);
                        continue
                    } else if http::StatusCode::UNAUTHORIZED == status {
                        // authentication errors
                        warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, err, stringify!($expression));
                        continue
                    }
                }
            }

            // any other error we can't identify, break out
            break Err(original_err.into())
        }
    }}
}



/// Wrapper around the elasticsearch client for helper methods used across contexts
/// This struct is deliberately private to this module
struct ElasticHelper {
    pub es: tokio::sync::RwLock<Elasticsearch>,
    pub hosts: Vec<url::Url>,
    pub archive_access: bool,
}

impl ElasticHelper {
    async fn connect(url: &str, archive_access: bool) -> Result<Self> {
        let host = url::Url::parse(url)?;
        let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(host.clone());
        let transport = elasticsearch::http::transport::TransportBuilder::new(conn_pool).build()?;
        Ok(ElasticHelper{
            es: tokio::sync::RwLock::new(Elasticsearch::new(transport)),
            hosts: vec![host],
            archive_access,
        })
    }

    async fn connection_reset(&self) -> Result<()> {
        todo!("{:?}", self.hosts)
    }

    fn get_hosts_safe(&self) -> Vec<String> {
        self.hosts.iter().map(|url|format!("{}:{}", url.host_str().unwrap_or_default(), url.port_or_known_default().unwrap_or(80))).collect()
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

/// Public interface to our elastic datastore.
/// details are actually in Collection and ElasticHelper classes.
pub struct Elastic {
    es: Arc<ElasticHelper>,
    pub file: Collection<File>,
    pub submission: Collection<Submission>,
    pub user: Collection<User>,
    pub error: Collection<ErrorModel>,

    /// Unmodified default service data classes
    pub service: Collection<Service>,
    pub filescore: Collection<FileScore>,

    /// Modifications to service data for this system
    pub service_delta: Collection<JsonMap>,
}

impl Elastic {
    pub async fn connect(url: &str, archive_access: bool) -> Result<Arc<Self>> {
        let helper = Arc::new(ElasticHelper::connect(url, archive_access).await?);
        Ok(Arc::new(Self {
            es: helper.clone(),
            file: Collection::new(helper.clone(), "file".to_owned(), Some("file-ma".to_owned())),
            submission: Collection::new(helper.clone(), "submission".to_owned(), Some("submission-ma".to_owned())),
            error: Collection::new(helper.clone(), "error".to_owned(), None),
            service: Collection::new(helper.clone(), "service".to_owned(), None),
            service_delta: Collection::new(helper.clone(), "service_delta".to_owned(), None),
        }))
    }

    pub async fn update_service_delta(&self, name: &str, delta: &JsonMap) -> Result<()> {
        todo!();
    }

    pub async fn get_service_with_delta(&self, service_name: &str, version: Option<String>) -> Result<Option<Service>> {
        todo!()
    //     svc = self.service_delta.get(service_name)
    //     if svc is None:
    //         return None

    //     if version is not None:
    //         svc.version = version

    //     svc_version_data = self.service.get(f"{service_name}_{svc.version}")
    //     if svc_version_data is None:
    //         return None

    //     svc_version_data = recursive_update(svc_version_data.as_primitives(strip_null=True),
    //                                         svc.as_primitives(strip_null=True),
    //                                         stop_keys=['config'])
    //     if as_obj:
    //         return Service(svc_version_data)
    //     else:
    //         return svc_version_data
    }

    pub async fn list_all_services(&self) -> Result<Vec<Service>> {
        todo!()
        // List all services from service delta (Return all fields if full is true)
        // let service_deltas = self.service_delta.stream_search::<JsonMap>("id:*".to_owned(), vec!["*".to_owned()], vec![], None, None, None).await?.collect().await?;

        // // service_delta = list(self.service_delta.stream_search("id:*", fl="*" if full else None))

        // // Gather all matching services and apply a mask if we don't want the full source object
        // service_data = [Service(s, mask=mask)
        //                 for s in self.service.multiget([f"{item.id}_{item.version}" for item in service_delta],
        //                                                as_obj=False, as_dictionary=False)]

        // // Recursively update the service data with the service delta while stripping nulls
        // services = [recursive_update(data.as_primitives(strip_null=True), delta.as_primitives(strip_null=True),
        //                              stop_keys=['config'])
        //             for data, delta in zip(service_data, service_delta)]

        // // Return as an objet if needs be...
        // return [Service(s, mask=mask) for s in services]
    }

    pub async fn list_enabled_services(&self) -> Result<Vec<Service>> {
        let mut services = self.list_all_services().await?;
        services.retain(|service|service.enabled);
        Ok(services)
    }

        
    pub async fn save_or_freshen_file(&self, sha256: &Sha256, mut fileinfo: JsonMap, expiry: Option<DateTime<Utc>>, classification: ParsedClassification, cl_engine: ClassificationParser, is_section_image: Option<bool>) -> Result<()> {
        let is_section_image = is_section_image.unwrap_or(false);
        // Remove control fields from new file info
        for x in ["classification", "expiry_ts", "seen", "archive_ts", "labels", "label_categories", "comments"] {
            fileinfo.pop(x, None)
        }

        loop {
            let (current_fileinfo, version) = self.file.get_version(sha256).await?;

            if current_fileinfo is None {
                current_fileinfo = {}
            } else {
                // If the freshen we are doing won't change classification, we can do it via an update operation
                classification = cl_engine.min_classification(
                    str(current_fileinfo.get('classification', classification)),
                    str(classification)
                )
                if classification == current_fileinfo.get('classification', None) {
                    operations = [
                        (self.file.UPDATE_SET, key, value)
                        for key, value in fileinfo.items()
                    ]
                    operations.extend([
                        (self.file.UPDATE_INC, 'seen.count', 1),
                        (self.file.UPDATE_MAX, 'seen.last', now_as_iso()),
                    ])
                    if expiry:
                        operations.append((self.file.UPDATE_MAX, 'expiry_ts', expiry))
                    if self.file.update(sha256, operations):
                        return
                }
            }

            # Add new fileinfo to current from database
            current_fileinfo.update(fileinfo)
            current_fileinfo['archive_ts'] = None

            # Update expiry time
            current_expiry = current_fileinfo.get('expiry_ts', expiry)
            if current_expiry and expiry:
                current_fileinfo['expiry_ts'] = max(current_expiry, expiry)
            else:
                current_fileinfo['expiry_ts'] = None

            # Update seen counters
            now = now_as_iso()
            current_fileinfo['seen'] = seen = current_fileinfo.get('seen', {})
            seen['count'] = seen.get('count', 0) + 1
            seen['last'] = now
            seen['first'] = seen.get('first', now)

            # Update Classification
            current_fileinfo['classification'] = classification

            # Update section image status
            current_fileinfo['is_section_image'] = current_fileinfo.get('is_section_image', False) or is_section_image

            try:
                self.file.save(sha256, current_fileinfo, version=version)
                return
            except VersionConflictException as vce:
                log.info(f"Retrying save or freshen due to version conflict: {str(vce)}")
        }
    }
}

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


#[cfg(test)]
mod test {
    use assemblyline_models::datastore::Service;

    use super::Elastic;

    fn create_service(name: &str) -> Service {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "classification": "U",
            "default_result_classification": "U",
            "version": rand::random::<u8>().to_string(),
            "docker_config": {
                "image": "abc:123"
            },
        })).unwrap()
    }

    #[tokio::test]
    async fn list_services() {
        use serde_json::json;
        // connect to database
        let elastic = Elastic::connect("https://localhost:9200", true).await.unwrap();

        let mut aa = create_service("servicea");
        let bb = create_service("serviceb");

        elastic.service.save(&aa.key(), &aa, None, None).await.unwrap();
        elastic.service.save(&bb.key(), &bb, None, None).await.unwrap();
        elastic.service_delta.save_json(&aa.name, &[("version".to_owned(), json!(aa.version))].into_iter().collect(), None, None).await.unwrap();
        elastic.service_delta.save_json(&bb.name, &[("version".to_owned(), json!(bb.version))].into_iter().collect(), None, None).await.unwrap();

        // fetch the services without changes
        assert_eq!(elastic.get_service_with_delta(&aa.name, None).await.unwrap().unwrap(), aa);
        assert_eq!(elastic.get_service_with_delta(&bb.name, None).await.unwrap().unwrap(), bb);
        {
            let listed = elastic.list_all_services().await.unwrap();
            assert!(listed.contains(&aa));
            assert!(listed.contains(&bb));
        }

        // change one of the services
        aa.category = "DOGHOUSE".to_owned();
        aa.enabled = false;
        elastic.update_service_delta(&aa.name, &[
            ("category".to_owned(), json!("DOGHOUSE")),
            ("enabled".to_owned(), json!(false)),
        ].into_iter().collect()).await.unwrap();

        // fetch them again and ensure the changes have been applied
        assert_eq!(elastic.get_service_with_delta(&aa.name, None).await.unwrap().unwrap(), aa);
        assert_eq!(elastic.get_service_with_delta(&bb.name, None).await.unwrap().unwrap(), bb);
        {
            let listed = elastic.list_all_services().await.unwrap();
            assert!(listed.contains(&aa));
            assert!(listed.contains(&bb));

            let listed = elastic.list_enabled_services().await.unwrap();
            assert!(!listed.contains(&aa));
            assert!(listed.contains(&bb));
        }
    }

}
