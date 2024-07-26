
use std::sync::Arc;

pub mod responses;
pub mod collection;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::filescore::FileScore;
use assemblyline_models::datastore::user::User;
use assemblyline_models::{JsonMap, Sha256};
use assemblyline_models::datastore::{Error as ErrorModel, File, Service, ServiceDelta, Submission};
use chrono::{DateTime, Utc};
use collection::{Collection, OperationBatch};
use elasticsearch::Elasticsearch;
use itertools::Itertools;
use log::info;
use serde_json::json;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    Hot = 1,
    Archive = 2,
    HotAndArchive = 3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Create,
    Expected(i64, i64),
}

const DEFAULT_SEARCH_FIELD: &str = "__text__";
const KEEP_ALIVE: &str = "5m";
const MAX_RETRY_BACKOFF: u64 = 10;


fn strip_nulls(d: serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::Object(d) = d {
        let mut out = JsonMap::new();
        for (key, value) in d {
            if value.is_null() { continue }
            out.insert(key, strip_nulls(value));
        }
        json!(out)
    } else {
        d
    }
}


fn recursive_update(mut d: serde_json::Value, u: serde_json::Value, stop_keys: Option<&[&str]>, allow_recursion: Option<bool>) -> serde_json::Value {
    let stop_keys = stop_keys.unwrap_or_default();
    let allow_recursion = allow_recursion.unwrap_or(true);

    if d.is_null() {
        return u;
    }

    if u.is_null() {
        return d;
    }

    if let Some(d) = d.as_object_mut() {
        if let serde_json::Value::Object(u) = u {
            for (k, v) in u {
                if v.is_object() && allow_recursion {
                    let old_value = d.remove(&k).unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                    let new_value = recursive_update(
                        old_value, 
                        v, 
                        Some(stop_keys), 
                        Some(!stop_keys.contains(&k.as_str()))
                    );
                    d.insert(k, new_value);
                } else {
                    d.insert(k, v);
                }
            }
        }
    }

    return d
}


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
        crate::elastic::with_retries_detail!($helper, Some($index_name), false, $expression)
    }}
}
use with_retries_on;

macro_rules! with_retries_raise_confict {
    ($helper:expr, $index_name:expr, $expression:expr) => {{
        crate::elastic::with_retries_detail!($helper, Some($index_name), true, $expression)
    }}
}
use with_retries_raise_confict;

/// This function evaluates the passed expression and reconnect if it fails
macro_rules! with_retries_detail {
    ($helper:expr, $index_name:expr, $raise_conflicts:expr, $expression:expr) => {{
        use log::{info, warn};
        use std::error::Error;
        use elasticsearch::http;

        let mut attempts = 0;
        // let updated = 0;
        // let deleted = 0;
        loop {
            // If this isn't the first time we have tried, wait and reset the connection
            if attempts > 0 {
                let sleep_seconds = crate::elastic::MAX_RETRY_BACKOFF.min(attempts);
                tokio::time::sleep(tokio::time::Duration::from_secs(sleep_seconds)).await;
                $helper.connection_reset().await?;
            }
            attempts += 1;

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
                    if attempts > 1 {
                        info!("Reconnected to elasticsearch!");
                    }

                    // if updated:
                    //     ret_val['updated'] += updated

                    // if deleted:
                    //     ret_val['deleted'] += deleted

                    match response.json().await {
                        Ok(doc) => break Ok(doc),
                        Err(err) => err,
                    }
                },
                Err(err) => err
            };

            // Internal library errors are terminal and we stop here
            let source_err = match original_err.source() {
                Some(err) => err,
                None => break Err(ElasticError::fatal(original_err))
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
                break Err(ElasticError::json(err))
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
                            break Err(ElasticError::NotFound(Box::new(err_message)))
                        }

                        let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();
                        warn!("Index {} was removed while a query was running, retrying...", index);
                        continue
                    } else if http::StatusCode::CONFLICT == status {
                        if $raise_conflicts {
                            // De-sync potential treads trying to write to the index
                            tokio::time::sleep(tokio::time::Duration::from_secs_f64(rand::random::<f64>() * 0.1)).await;
                            break Err(ElasticError::VersionConflict(Box::new(err.to_string())))
                        }
                        // updated += ce.info.get('updated', 0)
                        // deleted += ce.info.get('deleted', 0)
                        continue

                    } else if http::StatusCode::FORBIDDEN == status {
                        match $index_name {
                            None => break Err(ElasticError::fatal(original_err)),
                            Some(index) => {
                                log::warn!("Elasticsearch cluster is preventing writing operations on index {}, retrying...", index);
                            }
                        }
                        continue
                    } else if http::StatusCode::SERVICE_UNAVAILABLE == status {
                        let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();

                        // Display proper error message
                        log::warn!("Looks like index {} is not ready yet, retrying...", index);
                        continue
                    } else if http::StatusCode::TOO_MANY_REQUESTS == status {
                        let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();
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
            break Err(ElasticError::fatal(original_err))
        }
    }}
}
use with_retries_detail;

fn get_transport_timeout() -> std::time::Duration {
    let seconds = match std::env::var("AL_DATASTORE_TRANSPORT_TIMEOUT") {
        Ok(value) => value.parse().unwrap_or(90),
        Err(_) => 90,
    };
    std::time::Duration::from_secs(seconds)
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
        Ok(ElasticHelper{
            es: tokio::sync::RwLock::new(Self::_create_connection(host.clone())?),
            hosts: vec![host],
            archive_access,
        })
    }

    async fn connection_reset(&self) -> Result<()> {
        *self.es.write().await = Self::_create_connection(self.hosts[0].clone())?;
        Ok(())
    }

    fn _create_connection(host: url::Url) -> Result<Elasticsearch> {
        let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(host);
        let transport = elasticsearch::http::transport::TransportBuilder::new(conn_pool)
            .timeout(get_transport_timeout())
            .build()?;
        Ok(Elasticsearch::new(transport))
    }

    fn get_hosts_safe(&self) -> Vec<String> {
        self.hosts.iter().map(|url|format!("{}:{}", url.host_str().unwrap_or_default(), url.port_or_known_default().unwrap_or(80))).collect()
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
    pub service_delta: Collection<ServiceDelta>,
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
            user: Collection::new(helper.clone(), "user".to_owned(), None),
            filescore: Collection::new(helper.clone(), "filescore".to_owned(), None),
        }))
    }

    pub async fn update_service_delta(&self, name: &str, delta: &JsonMap) -> Result<()> {
        todo!();
    }

    pub async fn get_service_with_delta(&self, service_name: &str, version: Option<String>) -> Result<Option<Service>> {
        let svc = self.service_delta.get_json(service_name, None).await?;
        let mut svc = match svc {
            Some(svc) => svc,
            None => return Ok(None),
        };

        if let Some(version) = version {
            svc.insert("version".to_owned(), json!(version));
        }

        let version = match svc.get("version") {
            Some(version) => match version.as_str() {
                Some(value) => value.to_owned(),
                None => return Ok(None),
            },
            None => return Ok(None),
        };

        let svc_version_data = self.service.get_json(&format!("{service_name}_{version}"), None).await?;
        let svc_version_data = match svc_version_data {
            Some(svc_version_data) => svc_version_data,
            None => return Ok(None),
        };

        let svc_version_data = recursive_update(
            strip_nulls(json!(svc_version_data)),
            strip_nulls(json!(svc)),
            Some(&["config"]),
            None,
        );

        Ok(serde_json::from_value(svc_version_data)?)
    }

    pub async fn list_all_services(&self) -> Result<Vec<Service>> {
        // List all services from service delta (Return all fields if full is true)
        // // service_delta = list(self.service_delta.stream_search("id:*", fl="*" if full else None))
        let service_deltas = self.service_delta.stream_search::<JsonMap>("id:*".to_owned(), "*".to_owned(), vec![], None, None, None).await?.collect().await?;

        // // Gather all matching services and apply a mask if we don't want the full source object
        // service_data = [Service(s, mask=mask)
        //                 for s in self.service.multiget([f"{item.id}_{item.version}" for item in service_delta],
        //                                                as_obj=False, as_dictionary=False)]
        let mut service_keys = vec![];
        for delta in service_deltas.iter() {
            service_keys.push(format!("{}_{}", delta.get("id").unwrap(), delta.get("version").unwrap()))
        }
        let key_refs: Vec<&str> = service_keys.iter().map(|val|val.as_str()).collect();
        let mut service_data = self.service.multiget::<JsonMap>(key_refs, None, None).await?;

        // // Recursively update the service data with the service delta while stripping nulls
        // services = [recursive_update(data.as_primitives(strip_null=True), delta.as_primitives(strip_null=True),
        //                              stop_keys=['config'])
        //             for data, delta in zip(service_data, service_delta)]
        let mut services = vec![];
        for (key, delta) in service_keys.into_iter().zip(service_deltas) {
            if let Some(data) = service_data.remove(&key) {
                let data = recursive_update(
                    strip_nulls(json!(data)), 
                    strip_nulls(json!(delta)), 
                    Some(&["config"]), 
                    None
                );
                services.push(serde_json::from_value(data)?);
            }
        }

        return Ok(services);
    }

    pub async fn list_enabled_services(&self) -> Result<Vec<Service>> {
        let mut services = self.list_all_services().await?;
        services.retain(|service|service.enabled);
        Ok(services)
    }

    pub async fn save_or_freshen_file(&self, sha256: &Sha256, mut fileinfo: JsonMap, expiry: Option<DateTime<Utc>>, mut classification: String, cl_engine: &ClassificationParser) -> Result<()> {

        // Remove control fields from new file info
        for x in ["classification", "expiry_ts", "seen", "archive_ts", "labels", "label_categories", "comments"] {
            fileinfo.remove(x);
        }

        // Reset archive_ts field
        fileinfo.insert("archive_ts".to_owned(), serde_json::Value::Null);

        // # Clean up and prepare timestamps
        // if isinstance(expiry, datetime):
        //     expiry = expiry.strftime(DATEFORMAT)

        loop {
            let current = self.file.get_if_exists(sha256, None).await?;

            let (mut current_fileinfo, version) = match current {
                None => (JsonMap::new(), Version::Create),
                Some((current_fileinfo, version)) => {
                    // If the freshen we are doing won't change classification, we can do it via an update operation
                    let server_classification = cl_engine.normalize_classification(&current_fileinfo.classification.classification)?;

                    classification = cl_engine.min_classification(
                        &server_classification,
                        &classification,
                        None
                    )?;

                    if classification == server_classification {
                        
                        let mut batch = OperationBatch::default();

                        for (key, value) in &fileinfo {
                            batch.set(key.clone(), value.clone());
                        }

                        batch.increment("seen.count".to_owned(), json!(1));
                        batch.max("seen.last".to_owned(), json!(chrono::Utc::now().to_rfc3339()));

                        if current_fileinfo.expiry_ts.is_some() {
                            if let Some(expiry) = expiry {
                                batch.max("expiry_ts".to_owned(), json!(expiry.to_rfc3339()));
                            } 
                        }
                        
                        if self.file.update(sha256, batch, None, Some(8)).await.is_ok() {
                            return Ok(())
                        }
                    }

                    let value = serde_json::to_value(current_fileinfo)?;
                    let serde_json::Value::Object(obj) = value else { panic!("impossible to reach") };
                    (obj, version)
                }
            };

            // Update expiry time
            match (current_fileinfo.get("expiry_ts"), expiry) {
                (Some(a), Some(b)) => {
                    let a: DateTime<Utc> = serde_json::from_value(a.clone())?;
                    current_fileinfo.insert("expiry_ts".to_owned(), json!(a.max(b)));
                },
                _ => {
                    current_fileinfo.insert("expiry_ts".to_owned(), serde_json::Value::Null);
                }
            }
                
            // Add new fileinfo to current from database
            current_fileinfo.append(&mut fileinfo);

            // Update seen counters
            let now = Utc::now().to_rfc3339();
            let seen = current_fileinfo.entry("seen").or_insert(json!(JsonMap::new()));
            if !seen.is_object() { *seen = json!(JsonMap::new()); }
            let seen = seen.as_object_mut().unwrap();

            // seen.entry("count") = seen.get('count', 0) + 1
            seen.insert("count".to_string(), json!(extract_number(&seen, "count") + 1));
            seen.insert("last".to_owned(), json!(now));
            seen.insert("first".to_owned(), seen.get("first").unwrap_or(&json!(now)).clone());

            // Update Classification
            current_fileinfo.insert("classification".to_owned(), json!(classification));

            // write the file
            let current_fileinfo: File = serde_json::from_value(json!(current_fileinfo))?;
            let result = self.file.save(sha256, &current_fileinfo, Some(version), None).await;
            match result {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if err.is_version_conflict() {
                        info!("Retrying save or freshen due to version conflict: {err}");
                        continue
                    }
                }
            }
        }
    }
}

fn extract_number(container: &JsonMap, name: &str) -> u64 {
    match container.get(name) {
        Some(value) => serde_json::from_value(value.clone()).unwrap_or(0),
        None => 0,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ElasticError {
    // start out with a range of errors we may want to recover from specifically
    #[error("Index {0} does not have an archive")]
    ArchiveNotFound(Box<String>),
    #[error("Trying to get access to the archive on a datastore where archive_access is disabled")]
    ArchiveDisabled,
    #[error("Keys couldn't be found during multiget: {0:?}")]
    MultiKeyError(Box<Vec<String>>),
    #[error("Document not found: {0}")]
    NotFound(Box<String>),
    #[error("Version conflict: {0}")]
    VersionConflict(Box<String>),

    // bundle all of our non-recoverable/fatal errors under broad catagories
    #[error("Network error: {0}")]
    NetworkError(Box<dyn std::error::Error + Send + Sync>),
    #[error("Json format error: {0}")]
    JsonError(Box<String>),
    #[error("Datastore error: {0}")]
    Fatal(Box<String>),
}


impl ElasticError {
    pub fn multi_key_error(keys: &[&str]) -> Self {
        ElasticError::MultiKeyError(Box::new(keys.into_iter().map(|key| key.to_string()).collect_vec()))
    }

    pub fn json(error: impl std::fmt::Display) -> Self {
        ElasticError::JsonError(Box::new(error.to_string()))
    }

    pub fn fatal(error: impl std::fmt::Display) -> Self {
        ElasticError::Fatal(Box::new(error.to_string()))
    }

    pub fn is_version_conflict(&self) -> bool {
        matches!(self, Self::VersionConflict(_))
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
    }
}

impl From<serde_json::Error> for ElasticError {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonError(Box::new(value.to_string()))
    }
} 

impl From<url::ParseError> for ElasticError {
    fn from(value: url::ParseError) -> Self {
        Self::NetworkError(Box::new(value))
    }
}

impl From<elasticsearch::http::transport::BuildError> for ElasticError {
    fn from(value: elasticsearch::http::transport::BuildError) -> Self {
        Self::NetworkError(Box::new(value))
    }
}

impl From<assemblyline_markings::errors::Errors> for ElasticError {
    fn from(value: assemblyline_markings::errors::Errors) -> Self {
        Self::Fatal(Box::new(value.to_string()))
    }
}

impl From<collection::InvalidOperationError> for ElasticError {
    fn from(value: collection::InvalidOperationError) -> Self {
        Self::Fatal(Box::new(value.to_string()))
    }
}

type Result<T, E=ElasticError> = std::result::Result<T, E>;

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

    fn init() {
        let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Debug).try_init();
    }

    #[tokio::test]
    async fn list_services() {
        init();
        use serde_json::json;
        // connect to database
        let elastic = Elastic::connect("http://elastic:devpass@localhost:9200", true).await.unwrap();

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
        aa.category = "DOGHOUSE".to_string();
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

    
// import hashlib
// from assemblyline.common.isotime import now_as_iso
// from assemblyline.odm.models.file import File
// import pytest
// import random

// from retrying import retry

// from assemblyline.common import forge
// from assemblyline.datastore.helper import AssemblylineDatastore, MetadataValidator
// from assemblyline.odm.base import DATEFORMAT, KeyMaskException
// from assemblyline.odm.models.config import Config, Metadata
// from assemblyline.odm.models.result import Result
// from assemblyline.odm.models.service import Service
// from assemblyline.odm.models.submission import Submission
// from assemblyline.odm.randomizer import SERVICES, random_minimal_obj
// from assemblyline.odm.random_data import create_signatures, create_submission, create_heuristics, create_services


// class SetupException(Exception):
//     pass


// @retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=500)
// def setup_store(al_datastore: AssemblylineDatastore, request):
//     try:
//         ret_val = al_datastore.ds.ping()
//         if ret_val:

//             # Create data
//             fs = forge.get_filestore()
//             for _ in range(3):
//                 create_submission(al_datastore, fs)
//             create_heuristics(al_datastore)
//             create_signatures(al_datastore)
//             create_services(al_datastore)

//             # Wipe all on finalize
//             def cleanup():
//                 for index_name in al_datastore.ds.get_models():
//                     al_datastore.enable_archive_access()
//                     collection = al_datastore.get_collection(index_name)
//                     collection.wipe(recreate=False)
//             request.addfinalizer(cleanup)

//             return al_datastore
//     except ConnectionError:
//         pass
//     raise SetupException("Could not setup Datastore: %s" % al_datastore)


// @pytest.fixture(scope='module')
// def config():
//     config = forge.get_config()
//     config.datastore.archive.enabled = True
//     return config


// @pytest.fixture(scope='module')
// def ds(request, config):
//     try:
//         return setup_store(forge.get_datastore(config=config), request)
//     except SetupException:
//         pass

//     return pytest.skip("Connection to the Elasticsearch server failed. This test cannot be performed...")


// def test_index_archive_status(ds: AssemblylineDatastore, config: Config):
//     """Save a new document atomically, then try to save it again and detect the failure."""
//     ds.enable_archive_access()
//     try:
//         indices = ds.ds.get_models()
//         archiveable_indices = config.datastore.archive.indices

//         for index in indices:
//             collection = ds.get_collection(index)
//             if index in archiveable_indices:
//                 assert collection.archive_name == f"{index}-ma"
//             else:
//                 assert collection.archive_name is None

//     finally:
//         ds.disable_archive_access()


// def test_get_stats(ds: AssemblylineDatastore):
//     stats = ds.get_stats()
//     assert "cluster" in stats
//     assert "nodes" in stats
//     assert "indices" in stats
//     assert stats['cluster']['status'] in ["green", "yellow"]


// def test_create_empty_result(ds: AssemblylineDatastore):
//     cl_engine = forge.get_classification()

//     # Set expected values
//     classification = cl_engine.normalize_classification(cl_engine.UNRESTRICTED)
//     svc_name = "TEST"
//     svc_version = "4"
//     sha256 = "a123" * 16

//     # Build result key
//     result_key = Result.help_build_key(sha256=sha256, service_name=svc_name, service_version=svc_version, is_empty=True)

//     # Create an empty result from the key
//     empty_result = ds.create_empty_result_from_key(result_key, cl_engine=cl_engine)

//     # Test the empty result
//     assert empty_result.is_empty()
//     assert empty_result.response.service_name == svc_name
//     assert empty_result.response.service_version == svc_version
//     assert empty_result.sha256 == sha256
//     assert empty_result.classification.long() == classification


// DELETE_TREE_PARAMS = [
//     (True, "bulk"),
//     (False, "direct"),
// ]


// # noinspection PyShadowingNames
// @pytest.mark.parametrize("bulk", [f[0] for f in DELETE_TREE_PARAMS], ids=[f[1] for f in DELETE_TREE_PARAMS])
// def test_delete_submission_tree(ds: AssemblylineDatastore, bulk):
//     # Reset the data
//     fs = forge.get_filestore()

//     # Create a random submission
//     submission: Submission = create_submission(ds, fs)
//     files = set({submission.files[0].sha256})
//     files = files.union([x[:64] for x in submission.results])
//     files = files.union([x[:64] for x in submission.errors])
//     # Validate the submission is there
//     assert ds.submission.exists(submission.sid)
//     for f in files:
//         assert ds.file.exists(f)
//     for r in submission.results:
//         if r.endswith(".e"):
//             assert ds.emptyresult.exists(r)
//         else:
//             assert ds.result.exists(r)
//     for e in submission.errors:
//         assert ds.error.exists(e)

//     # Delete the submission
//     if bulk:
//         ds.delete_submission_tree_bulk(submission.sid, transport=fs)
//     else:
//         ds.delete_submission_tree(submission.sid, transport=fs)

//     # Make sure delete operation is reflected in the DB
//     ds.submission.commit()
//     ds.error.commit()
//     ds.emptyresult.commit()
//     ds.result.commit()
//     ds.file.commit()

//     # Make sure submission is completely gone
//     assert not ds.submission.exists(submission.sid)
//     for f in files:
//         assert not ds.file.exists(f)
//     for r in submission.results:
//         if r.endswith(".e"):
//             assert not ds.emptyresult.exists(r)
//         else:
//             assert not ds.result.exists(r)
//     for e in submission.errors:
//         assert not ds.error.exists(e)


// def test_get_all_heuristics(ds: AssemblylineDatastore):
//     # Get a list of all services
//     all_services = set([x.upper() for x in SERVICES.keys()])

//     # List all heuristics
//     heuristics = ds.get_all_heuristics()

//     # Test each heuristics
//     for heur in heuristics.values():
//         assert heur['heur_id'].split(".")[0] in all_services


// def test_get_results(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get all results for that submission
//     results = ds.get_multiple_results(submission.results)
//     assert len(results) == len(submission.results)

//     # Get results one by one
//     single_res = {}
//     for r in submission.results:
//         single_res[r] = ds.get_single_result(r)

//     # Compare results
//     for r_key in results:
//         assert r_key in single_res
//         if not r_key.endswith(".e"):
//             assert single_res[r_key] == results[r_key]


// def test_get_file_submission_meta(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get submission meta
//     submission_meta = ds.get_file_submission_meta(submission.files[0].sha256, ['params.submitter'])

//     # check if current submission values are in submission meta
//     assert submission.params.submitter in submission_meta['submitter']


// def test_get_file_list_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get related file list
//     file_list = [sha256 for sha256, supplementary, in ds.get_file_list_from_keys(submission.results)]

//     # Check if all files that are obvious from the results are there
//     for f in submission.files:
//         if not [r for r in submission.results if r.startswith(f.sha256) and not r.endswith('.e')]:
//             # If this file has no actual results, we can't this file to show up in the file list
//             continue
//         assert f.sha256 in file_list
//     for r in submission.results:
//         if r.endswith('.e'):
//             # We can't expect a file tied to be in the file list
//             continue
//         assert r[:64] in file_list


// def test_get_file_scores_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get scores
//     file_scores = ds.get_file_scores_from_keys(submission.results)

//     # Check if all files that are obvious from the results are there
//     for f in submission.files:
//         if not [r for r in submission.results if r.startswith(f.sha256) and not r.endswith('.e')]:
//             # If this file has no actual results, we can't expect there to be a file score
//             continue
//         assert f.sha256 in file_scores
//     for r in submission.results:
//         if r.endswith('.e'):
//             # We can't expect a file tied to an empty_result to have a file score
//             continue
//         assert r[:64] in file_scores

//     for s in file_scores.values():
//         assert isinstance(s, int)


// def test_get_signature_last_modified(ds: AssemblylineDatastore):
//     last_mod = ds.get_signature_last_modified()

//     assert isinstance(last_mod, str)
//     assert "T" in last_mod
//     assert last_mod.endswith("Z")


// def test_get_or_create_file_tree(ds: AssemblylineDatastore, config: Config):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get file tree
//     tree = ds.get_or_create_file_tree(submission, config.submission.max_extraction_depth)

//     # Check if all files that are obvious from the results are there
//     for x in ['tree', 'classification', 'filtered', 'partial', 'supplementary']:
//         assert x in tree

//     for f in submission.files:
//         assert f.sha256 in tree['tree']


// def test_get_summary_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get the summary
//     summary = ds.get_summary_from_keys(submission.results)

//     # Get the summary with heuristics
//     summary_heur = ds.get_summary_from_keys(submission.results, keep_heuristic_sections=True)

//     assert summary['tags'] == summary_heur['tags']
//     assert summary['attack_matrix'] == summary_heur['attack_matrix']
//     assert summary['heuristics'] == summary_heur['heuristics']
//     assert summary['classification'] == summary_heur['classification']
//     assert summary['filtered'] == summary_heur['filtered']
//     assert summary['heuristic_sections'] == {}
//     assert summary['heuristic_name_map'] == {}

//     heuristics = ds.get_all_heuristics()

//     for h in summary_heur['heuristic_sections']:
//         assert h in heuristics

//     for heur_list in summary_heur['heuristic_name_map'].values():
//         for h in heur_list:
//             assert h in heuristics


// def test_get_tag_list_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get the list of tags
//     tags = ds.get_tag_list_from_keys(submission.results)

//     assert len(tags) > 0
//     for t in tags:
//         assert t['key'] in submission.results


// def test_get_attack_matrix_from_keys(ds: AssemblylineDatastore):
//     # Get a random submission
//     submission: Submission = ds.submission.search("id:*", rows=1, fl="*")['items'][0]

//     # Get the list of tags
//     attacks = ds.get_attack_matrix_from_keys(submission.results)

//     for a in attacks:
//         assert a['key'] in submission.results


// def test_get_service_with_delta(ds: AssemblylineDatastore):
//     # Get a random service delta
//     service_delta: Service = ds.service_delta.search("id:*", rows=1, fl="*")['items'][0]
//     service_key = f"{service_delta.id}_{service_delta.version}"
//     service_delta.category = "TEST"

//     # Save fake service category
//     ds.service_delta.save(service_delta.id, service_delta)
//     ds.service_delta.commit()

//     # Get the associated service
//     service: Service = ds.service.get(service_key)

//     # Get the full service with its delta
//     full_service = ds.get_service_with_delta(service_delta.id)

//     assert full_service.as_primitives() != service.as_primitives()
//     assert full_service.category == "TEST"


// def test_calculate_heuristic_stats(ds: AssemblylineDatastore):
//     default_stats = {'count': 0, 'min': 0, 'max': 0, 'avg': 0, 'sum': 0, 'first_hit': None, 'last_hit': None}

//     # Reset original heuristics stats
//     for heur_id in ds.get_all_heuristics():
//         ds.heuristic.update(heur_id, [(ds.heuristic.UPDATE_SET, 'stats', default_stats)])
//     ds.heuristic.commit()

//     # Make sure stats did get reset
//     heuristics = ds.get_all_heuristics()
//     assert all([heur['stats'] == default_stats for heur in heuristics.values()])

//     # Do heuristics stat calculation for all
//     ds.calculate_heuristic_stats()
//     ds.heuristic.commit()

//     # Get heuristics with calculated stats
//     updated_heuristics = ds.get_all_heuristics()

//     assert heuristics != updated_heuristics
//     assert any([heur['stats'] != default_stats for heur in updated_heuristics.values()])


// def test_calculate_signature_stats(ds: AssemblylineDatastore):
//     default_stats = {'count': 0, 'min': 0, 'max': 0, 'avg': 0, 'sum': 0, 'first_hit': None, 'last_hit': None}

//     def get_all_signatures():
//         return {s['id']: s for s in ds.signature.stream_search("id:*", as_obj=False)}

//     # Reset original signature stats
//     for sig_id in get_all_signatures():
//         ds.signature.update(sig_id, [(ds.signature.UPDATE_SET, 'stats', default_stats)])
//     ds.signature.commit()

//     # Make sure stats did get reset
//     signatures = get_all_signatures()
//     assert all([sig['stats'] == default_stats for sig in signatures.values()])

//     # Do signature stat calculation for all
//     ds.calculate_signature_stats(lookback_time="now-1y")
//     ds.signature.commit()

//     # Get signatures with calculated stats
//     updated_signatures = get_all_signatures()

//     assert signatures != updated_signatures
//     assert any([sig['stats'] != default_stats for sig in updated_signatures.values()])


// def test_task_cleanup(ds: AssemblylineDatastore):
//     assert ds.ds.client.search(index='.tasks',
//                                q="completed:true",
//                                track_total_hits=True,
//                                size=0)['hits']['total']['value'] != 0

//     if ds.ds.es_version.major == 7:
//         # Superusers are allowed to interact with .tasks index
//         assert ds.task_cleanup()

//     elif ds.ds.es_version.major == 8:
//         # Superusers are NOT allowed to interact with .tasks index because it's a restricted index

//         # Attempt cleanup using the default user, assert that the cleanup didn't happen
//         assert ds.task_cleanup() == 0

//         # Switch to user that can perform task cleanup
//         ds.ds.switch_user("plumber")

//         assert ds.task_cleanup()


// def test_list_all_services(ds: AssemblylineDatastore):
//     all_svc: Service = ds.list_all_services()
//     all_svc_full: Service = ds.list_all_services(full=True)

//     # Make sure service lists are different
//     assert all_svc != all_svc_full

//     # Check that all services are there in the normal list
//     for svc in all_svc:
//         assert svc.name in SERVICES

//     # Check that all services are there in the full list
//     for svc in all_svc_full:
//         assert svc.name in SERVICES

//     # Make sure non full list raises exceptions
//     for svc in all_svc:
//         with pytest.raises(KeyMaskException):
//             svc.timeout

//     # Make sure the full list does not
//     for svc in all_svc_full:
//         assert svc.timeout is not None


// def test_list_service_heuristics(ds: AssemblylineDatastore):
//     # Get a random service
//     svc_name = random.choice(list(SERVICES.keys()))

//     # Get the service heuristics
//     heuristics = ds.list_service_heuristics(svc_name)

//     # Validate the heuristics
//     for heur in heuristics:
//         assert heur.heur_id.startswith(svc_name.upper())


// def test_list_all_heuristics(ds: AssemblylineDatastore):
//     # Get a list of all services
//     all_services = set([x.upper() for x in SERVICES.keys()])

//     # List all heuristics
//     heuristics = ds.list_all_heuristics()

//     # Test each heuristics
//     for heur in heuristics:
//         assert heur.heur_id.split(".")[0] in all_services

// def test_metadata_validation(ds: AssemblylineDatastore):
//     validator = MetadataValidator(ds)

//     # Run validator with no submission metadata validation configured
//     assert not validator.check_metadata({'blah': 'blee'}, validation_scheme={})

//     # Run validation using validator parameters
//     meta_config = {
//         'blah': Metadata({
//             'required': True,
//             'validator_type': 'regex',
//             'validator_params': {
//                 'validation_regex': 'blee'
//             }
//         })
//     }
//     assert not validator.check_metadata({'blah': 'blee'}, validation_scheme=meta_config)

//     # Run validator with validation configured but is missing metadata
//     assert validator.check_metadata({'bloo': 'blee'}, validation_scheme=meta_config)

//     # Run validation using invalid metadata
//     assert validator.check_metadata({'blah': 'blee'}, validation_scheme={
//         'blah': Metadata({
//             'required': True,
//             'validator_type': 'integer',
//         })
//     })

//     # Run validation on field that's not required (but still provided and is invalid)
//     assert validator.check_metadata({'blah': 'blee'}, validation_scheme={
//         'blah': Metadata({
//             'validator_type': 'integer',
//         })
//     })

// def test_save_or_freshen_file(ds: AssemblylineDatastore):
//     classification = forge.get_classification()

//     # Generate random data
//     data = b"asfd"*64
//     expiry_create = now_as_iso(60 * 60 * 24 * 14)
//     expiry_freshen = now_as_iso(60 * 60 * 24 * 15)

//     # Generate file info for random file
//     f = random_minimal_obj(File)
//     f.sha256 = hashlib.sha256(data).hexdigest()
//     f.sha1 = hashlib.sha1(data).hexdigest()
//     f.md5 = hashlib.md5(data).hexdigest()

//     # Make sure file does not exists
//     ds.file.delete(f.sha256)

//     # Save the file
//     ds.save_or_freshen_file(f.sha256, f.as_primitives(), expiry_create, classification.RESTRICTED)

//     # Validate created file
//     saved_file = ds.file.get_if_exists(f.sha256)
//     assert saved_file.sha256 == f.sha256
//     assert saved_file.sha1 == f.sha1
//     assert saved_file.md5 == f.md5
//     assert saved_file.expiry_ts.strftime(DATEFORMAT) == expiry_create
//     assert saved_file.seen.count == 1
//     assert saved_file.seen.first == saved_file.seen.last
//     assert saved_file.classification.long() == classification.normalize_classification(classification.RESTRICTED)

//     # Freshen the file
//     ds.save_or_freshen_file(f.sha256, f.as_primitives(), expiry_freshen, classification.UNRESTRICTED)

//     # Validate freshened file
//     freshened_file = ds.file.get_if_exists(f.sha256)
//     assert freshened_file.sha256 == f.sha256
//     assert freshened_file.sha1 == f.sha1
//     assert freshened_file.md5 == f.md5
//     assert freshened_file.expiry_ts.strftime(DATEFORMAT) == expiry_freshen
//     assert freshened_file.seen.count == 2
//     assert freshened_file.seen.first < freshened_file.seen.last
//     assert freshened_file.classification.long() == classification.normalize_classification(classification.UNRESTRICTED)


// def test_switch_user(ds: AssemblylineDatastore):
//     # Attempt to switch to another random user
//     ds.ds.switch_user("test")

//     # Confirm that user switch didn't happen
//     assert list(ds.ds.client.security.get_user().keys()) != ["test"]

//     # Switch to recognized plumber user
//     ds.ds.switch_user("plumber")

//     # Confirm that user switch did happen
//     assert list(ds.ds.client.security.get_user().keys()) != ["plumber"]

}
