
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use log::error;

pub mod responses;
pub mod collection;
pub mod error;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::filescore::FileScore;
use assemblyline_models::datastore::user::User;
use assemblyline_models::{JsonMap, Sha256};
use assemblyline_models::datastore::{Error as ErrorModel, File, Service, ServiceDelta, Submission};
use chrono::{DateTime, Utc};
use collection::{Collection, OperationBatch};
use error::{ElasticErrorInner, WithContext};
use itertools::Itertools;
use log::info;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use self::error::{ElasticError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    Hot = 1,
    Archive = 2,
    HotAndArchive = 3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    Create,
    Expected{primary_term: i64, sequence_number: i64},
}


/// Methods supported for copying indices
enum CopyMethod {
    /// Copy the index by cloning it
    Clone,
}

impl std::fmt::Display for CopyMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyMethod::Clone => f.write_str("_clone")
        }
    }
}


/// Maximum time between retries when an error occurs
const MAX_RETRY_SECONDS: u64 = 10;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(MAX_RETRY_SECONDS);

const DEFAULT_SEARCH_FIELD: &str = "__text__";
const KEEP_ALIVE: &str = "5m";


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

/// The header section and local parameters to a request to elasticsearch.
/// 
/// Not to be confused with the actual HTTP request constructed to make a query.
/// This is just some of the things you need to build that HTTP request and information
/// needed to handle its outcome locally
#[derive(Debug, Clone)]
struct Request {
    method: reqwest::Method, 
    url: reqwest::Url,
    raise_conflicts: bool,
}

impl From<(reqwest::Method, reqwest::Url)> for Request {
    fn from(value: (reqwest::Method, reqwest::Url)) -> Self {
        Request {
            method: value.0,
            url: value.1,
            raise_conflicts: false
        }
    }
}

impl Request {
    pub fn with_raise_conflict(method: reqwest::Method, url: reqwest::Url) -> Self {
        Self {
            method,
            url,
            raise_conflicts: true
        }
    }
}

// This function tries to do two things at once:
//  - convert AL sort syntax to elastic,
//  - convert any sorts on the key _id to id
fn parse_sort(sort: &str) -> Result<Vec<(String, SortDirection)>> {
    if sort.is_empty() {
        return Ok(vec![])
    }

    // if isinstance(sort, list) {
    //     return [parse_sort(row, ret_list=False) for row in sort]
    // } elif isinstance(sort, dict) {
    //     return {('id' if key == '_id' else key): value for key, value in sort.items()}
    // } elif "," in sort {
    //     return [parse_sort(row.strip(), ret_list=False) for row in sort.split(',')]
    // }
    if sort.contains(',') {
        let mut out = vec![];
        for part in sort.split(',') {
            out.extend(parse_sort(part)?)
        }
        return Ok(out)
    }

    Ok(if let Some((left, right)) = sort.split_once(' ') {
        if left == "_id" {
            vec![("id".to_owned(), right.parse()?)]
        } else {
            vec![(left.to_owned(), right.parse()?)]
        }
    } else if sort == "_id" {
        vec![("id".to_owned(), SortDirection::Ascending)]
    } else {
        vec![(sort.to_string(), SortDirection::Ascending)]
    })
}

enum SortDirection {
    Ascending,
    Descending,
}

impl Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Ascending => "asc",
            Self::Descending => "desc",
        })
    }
}

impl FromStr for SortDirection {
    type Err = ElasticError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        if s.starts_with("asc") {
            Ok(Self::Ascending)
        } else if s.starts_with("desc") {
            Ok(Self::Descending)
        } else {
            Err(ElasticError::fatal(format!("Unknown sort parameter {s}")))
        }
    }
}

// macro_rules! with_retries {
//     ($helper:expr, $expression:expr) => {{
//         crate::elastic::with_retries_detail!($helper, None, false, $expression)
//     }}
// }
// use with_retries;

// macro_rules! with_retries_on {
//     ($helper:expr, $index_name:expr, $expression:expr) => {{
//         crate::elastic::with_retries_detail!($helper, Some($index_name), false, $expression)
//     }}
// }
// use with_retries_on;

// macro_rules! with_retries_raise_confict {
//     ($helper:expr, $index_name:expr, $expression:expr) => {{
//         crate::elastic::with_retries_detail!($helper, Some($index_name), true, $expression)
//     }}
// }
// use with_retries_raise_confict;

// /// This function evaluates the passed expression and reconnect if it fails
// macro_rules! with_retries_detail {
//     ($helper:expr, $index_name:expr, $raise_conflicts:expr, $expression:expr) => {{
//         use log::{info, warn};
//         use std::error::Error;
//         use elasticsearch::http;
//         let hosts = $helper.get_hosts_safe().join(" | ");

//         let handle_error = |original_err: elasticsearch::Error| -> Option<ElasticError> {
//             // Internal library errors are terminal and we stop here
//             let source_err = match original_err.source() {
//                 Some(err) => err,
//                 None => return Some(ElasticError::fatal(original_err))
//             };

//             // Some manner of io error, just print a warning and retry, lots of ephemeral errors
//             // will resolve to this type where socket methods have failed
//             if let Some(err) = source_err.downcast_ref::<std::io::Error>() {
//                 warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, err, stringify!($expression));
//                 return None
//             }

//             // Json decoding/encoding error, will probably repeat if we retry, break with error
//             if let Some(err) = source_err.downcast_ref::<serde_json::error::Error>() {
//                 return Some(ElasticError::json(err))
//             }

//             // HTTP library error
//             if let Some(err) = source_err.downcast_ref::<reqwest::Error>() {
//                 // A timeout
//                 if err.is_timeout() {
//                     warn!("Elasticsearch connection timeout, server(s): {}, retrying...", hosts);
//                     return None
//                 }

//                 // a connection error
//                 if err.is_connect() {
//                     warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, err, stringify!($expression));
//                     return None
//                 }

//             }

//             // any other error we can't identify, break out
//             Some(ElasticError::fatal(original_err))
//         };

//         let mut attempts = 0;
//         // let updated = 0;
//         // let deleted = 0;
//         loop {
//             // If this isn't the first time we have tried, wait and reset the connection
//             if attempts > 0 {
//                 let sleep_seconds = crate::elastic::MAX_RETRY_SECONDS.min(attempts);
//                 tokio::time::sleep(tokio::time::Duration::from_secs(sleep_seconds)).await;
//                 $helper.connection_reset().await?;
//             }
//             attempts += 1;

//             // run the actual code being retried
//             let result = $expression;


//             let response = match result {
//                 // we have managed to load response headers (may still be an error)
//                 Ok(response) => response,
//                 // error that stopped us from getting a response
//                 Err(err) => match handle_error(err) {
//                     Some(err) => break Err(err),
//                     None => continue
//                 },
//             };

//             // at this point we know we have a respones, even if its an error
//             if attempts > 1 {
//                 info!("Reconnected to elasticsearch!");
//             }

//             // if updated:
//             //     ret_val['updated'] += updated

//             // if deleted:
//             //     ret_val['deleted'] += deleted

//             let status = response.status_code();

//             if status.is_success() {
//                 match response.json().await {
//                     Ok(doc) => break Ok(doc),
//                     Err(err) => match handle_error(err) {
//                         Some(err) => break Err(err),
//                         None => continue
//                     },
//                 }
//             }

//             let mut message = match response.text().await {
//                 Ok(message) => message,
//                 Err(err) => match handle_error(err) {
//                     Some(err) => break Err(err),
//                     None => continue
//                 }
//             };
//             if message.is_empty() {
//                 message = status.to_string()
//             }

//             // handle specific HTTP status codes we want particular actions for
//             if http::StatusCode::NOT_FOUND == status {
//                 // let err_message = err.to_string();

//                 // Validate exception type
//                 if $index_name.is_some() || !message.contains("No search context found") {
//                     break Err(ElasticError::NotFound(Box::new(message)))
//                 }

//                 let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();
//                 warn!("Index {} was removed while a query was running, retrying...", index);
//                 continue
//             } else if http::StatusCode::CONFLICT == status {
//                 if $raise_conflicts {
//                     // De-sync potential treads trying to write to the index
//                     tokio::time::sleep(tokio::time::Duration::from_secs_f64(rand::random::<f64>() * 0.1)).await;
//                     break Err(ElasticError::VersionConflict(Box::new(message)))
//                 }
//                 // updated += ce.info.get('updated', 0)
//                 // deleted += ce.info.get('deleted', 0)
//                 continue

//             } else if http::StatusCode::FORBIDDEN == status {
//                 match $index_name {
//                     None => break Err(ElasticError::fatal(message)),
//                     Some(index) => {
//                         log::warn!("Elasticsearch cluster is preventing writing operations on index {}, retrying...", index);
//                     }
//                 }
//                 continue
//             } else if http::StatusCode::SERVICE_UNAVAILABLE == status {
//                 let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();

//                 // Display proper error message
//                 log::warn!("Looks like index {} is not ready yet, retrying...", index);
//                 continue
//             } else if http::StatusCode::TOO_MANY_REQUESTS == status {
//                 let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();
//                 log::warn!("Elasticsearch is too busy to perform the requested task on index {}, retrying...", index);
//                 continue
//             } else if http::StatusCode::UNAUTHORIZED == status {
//                 // authentication errors
//                 let hosts = $helper.get_hosts_safe().join(" | ");
//                 warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, message, stringify!($expression));
//                 continue
//             } else {
//                 break Err(ElasticError::fatal(message))
//             }
//         }
//     }}
// }
// use with_retries_detail;


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
    pub client: reqwest::Client,
    // pub es: tokio::sync::RwLock<elasticsearch::Elasticsearch>,
    pub host: url::Url,
    pub archive_access: bool,
}

impl ElasticHelper {
    async fn connect(url: &str, archive_access: bool, ca_cert: Option<&str>, connect_unsafe: bool) -> Result<Self> {
        let host: url::Url = url.parse()?;
        let mut builder = reqwest::Client::builder()
            .timeout(get_transport_timeout());

        if let Some(ca_cert) = ca_cert {
            let cert = reqwest::Certificate::from_pem(ca_cert.as_bytes()).map_err(ElasticError::fatal)?;
            builder = builder.add_root_certificate(cert);
        }

        if connect_unsafe {
            builder = builder.danger_accept_invalid_certs(true);
        }

        Ok(ElasticHelper{
            // es: tokio::sync::RwLock::new(Self::_create_connection(host.clone())?),
            client: builder.build().map_err(ElasticError::fatal)?,
            host,
            archive_access,
        })
    }

    // async fn connection_reset(&self) -> Result<()> {
    //     *self.es.write().await = Self::_create_connection(self.host.clone())?;
    //     Ok(())
    // }

    // fn _create_connection(host: url::Url) -> Result<Elasticsearch> {
    //     let conn_pool = elasticsearch::http::transport::SingleNodeConnectionPool::new(host);
    //     let transport = elasticsearch::http::transport::TransportBuilder::new(conn_pool)
    //         .timeout(get_transport_timeout())
    //         .build()?;
    //     Ok(Elasticsearch::new(transport))
    // }

    /// start an index copy operation and wait for it to complete
    async fn safe_index_copy(&self, copy_method: CopyMethod, src: &str, target: &str, settings: Option<serde_json::Value>, min_status: Option<&str>) -> Result<()> {
        let min_status = min_status.unwrap_or("yellow");
        let mut url = self.host.join(&format!("{src}/{copy_method}/{target}"))?;
        url.query_pairs_mut().append_pair("timeout", "60s");
        let body = settings.map(|value| json!({"settings": value}));
        let response = match body {
            Some(body) => self.make_request_json(&mut 0, &(Method::POST, url).into(), &body).await?,
            None => self.make_request(&mut 0, &(Method::POST, url).into()).await?
        };

        let ret: responses::Command = response.json().await?;

        if !ret.acknowledged {
            return Err(ElasticErrorInner::FailedToCreateIndex{ src: src.to_owned(), target: target.to_owned()}.into())
        }

        self.wait_for_status(target, Some(min_status)).await
    }

    /// Wait for an index responds with a given status level
    async fn wait_for_status(&self, index: &str, min_status: Option<&str>) -> Result<()> {
        let min_status = min_status.unwrap_or("yellow");
        let mut url = self.host.join("_cluster/health/")?.join(index)?;
        url.query_pairs_mut().append_pair("timeout", "5s").append_pair("wait_for_status", min_status);

        loop {
            match self.client.request(Method::GET, url.clone()).send().await {
                Ok(response) => {
                    if response.status() == reqwest::StatusCode::REQUEST_TIMEOUT {
                        continue
                    } else if response.status() != reqwest::StatusCode::OK {
                        return Err(ElasticError::fatal("unexpected response status"))
                    }
                    let response: responses::Status = response.json().await?;
                    if !response.timed_out {
                        return Ok(())
                    }
                }
                Err(err) => {
                    if err.is_connect() || err.is_timeout() {
                        continue
                    }
                    return Err(err.into())
                }
            }
        }
    }

    /// Given an http query result decide whether to retry or extract the response
    async fn handle_result(attempt: &mut u64, request: &Request, result: reqwest::Result<reqwest::Response>) -> Result<Option<reqwest::Response>> {
        match Self::_handle_result(result, request).await? {
            Some(value) => Ok(Some(value)),
            None => {
                let delay = MAX_RETRY_DELAY.min(Duration::from_secs_f64((*attempt as f64).powf(2.0)/5.0));
                tokio::time::sleep(delay).await;
                Ok(None)
            },
        }
    }

    /// Given an http query result decide whether to retry or extract the response
    async fn _handle_result(result: reqwest::Result<reqwest::Response>, request: &Request) -> Result<Option<reqwest::Response>> {
        // Handle connection errors with a retry, let other non http errors bubble up
        let response = match result {
            Ok(response) => response,
            Err(err) => {
                // always retry for connect and timeout errors
                if err.is_connect() || err.is_timeout() {
                    error!("Error connecting to datastore: {err}");
                    return Ok(None)
                }

                return Err(err.into())
            },
        };

        // At this point we have a response from the server, but it may be describing an error.
        let status = response.status();
        
        // handle non-errors
        if status.is_success() {
            return Ok(Some(response))
        }

        // Since we know we have an error, load the body, for some errors this will have more information
        let headers = response.headers().clone();
        let url = response.url().clone();
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                // always retry for connect and timeout errors
                if err.is_request() || err.is_connect() || err.is_timeout() {
                    error!("Error connecting to datastore: {err}");
                    return Ok(None)
                }

                return Err(err.into())
            }
        };

        if StatusCode::NOT_FOUND == status {
            if body.is_empty() {
                return Err(ElasticErrorInner::IndexNotFound("".to_string()).into())
            }

            /// An agregate of several similar responess that indicate a missing document under different cercumstances.
            #[derive(Deserialize)]
            struct NotFoundResponse<'a> {
                _index: String,
                _id: String,
                // If a delete or update is directed to a missing document this should be "not_found"
                #[serde(default)]
                result: Option<&'a str>,
                // If a get is directed to a missing document this should be false
                #[serde(default)]
                found: Option<bool>,
            }

            if let Ok(body) = serde_json::from_str::<NotFoundResponse>(&body) {
                if body.result == Some("not_found") || body.found == Some(false) {
                    return Err(ElasticErrorInner::DocumentNotFound{
                        index: body._index,
                        id: body._id,
                    }.into())
                }    
            }
        } else if StatusCode::CONFLICT == status {
            if request.raise_conflicts {
                // De-sync potential treads trying to write to the index
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(rand::random::<f64>() * 0.1)).await;

                // try to pull out a sensible error message from the response
                if let Ok(response) = serde_json::from_str::<responses::Error>(&body) {
                    return Err(ElasticErrorInner::VersionConflict(response.error.reason).into())
                }

                // couldn't get an error message, who knows what happened
                return Err(ElasticErrorInner::VersionConflict("unknown".to_owned()).into())
            }
            // updated += ce.info.get('updated', 0)
            // deleted += ce.info.get('deleted', 0)
            return Ok(None)
        }

        todo!("{url} {status:?} {body:?} {headers:?}");
                
        // // handle specific HTTP status codes we want particular actions for
        // if StatusCode::NOT_FOUND == status {
            
        //     // let err_message = err.to_string();

        //     // Validate exception type
        //     if $index_name.is_some() || !body.contains("No search context found") {
        //         break Err(ElasticError::NotFound(Box::new(message)))
        //     }

        //     let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();
        //     warn!("Index {} was removed while a query was running, retrying...", index);
        //     continue


        // } else if http::StatusCode::FORBIDDEN == status {
        //     match $index_name {
        //         None => break Err(ElasticError::fatal(message)),
        //         Some(index) => {
        //             log::warn!("Elasticsearch cluster is preventing writing operations on index {}, retrying...", index);
        //         }
        //     }
        //     continue
        // } else if http::StatusCode::SERVICE_UNAVAILABLE == status {
        //     let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();

        //     // Display proper error message
        //     log::warn!("Looks like index {} is not ready yet, retrying...", index);
        //     continue
        // } else if http::StatusCode::TOO_MANY_REQUESTS == status {
        //     let index = $index_name.map(|x|x.to_string()).unwrap_or_default().to_uppercase();
        //     log::warn!("Elasticsearch is too busy to perform the requested task on index {}, retrying...", index);
        //     continue
        // } else if http::StatusCode::UNAUTHORIZED == status {
        //     // authentication errors
        //     let hosts = $helper.get_hosts_safe().join(" | ");
        //     warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, message, stringify!($expression));
        //     continue
        // } else {
        //     break Err(ElasticError::fatal(message))
        // }

        // return if status.is_server_error() {
        //     let body = response.text().await.unwrap_or(status.to_string());
        //     error!("Server error in datastore: {body}");
        //     let delay = MAX_RETRY_DELAY.min(Duration::from_secs_f64((*attempt as f64).powf(2.0)/5.0));
        //     tokio::time::sleep(delay).await;
        //     return Ok(None)                        
        // } else if status.is_client_error() {
        //     let path = response.url().path().to_owned();
        //     let body = response.text().await.unwrap_or(status.to_string());
        //     Err(ElasticError::HTTPError{path: Some(path), code: status, message: body})
        // } else {
        //     Ok(Some(response))
        // }
    }

    /// Start an http request with an empty body
    async fn make_request(&self, attempt: &mut u64, request: &Request) -> Result<reqwest::Response> {
        loop {
            *attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(request.method.clone(), request.url.clone())
                .send().await;
            
            // Handle connection errors with a retry, let other non http errors bubble up
            match Self::handle_result(attempt, request, result).await? {
                Some(response) => return Ok(response),
                None => continue,
            }
        }     
    }

    /// start an http request with a json body
    async fn make_request_json<R: Serialize>(&self, attempt: &mut u64, request: &Request, body: &R) -> Result<reqwest::Response> {
        loop {
            *attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(request.method.clone(), request.url.clone())
                .json(body)
                .send().await;
            
            // Handle connection errors with a retry, let other non http errors bubble up
            match Self::handle_result(attempt, request, result).await? {
                Some(response) => return Ok(response),
                None => continue,
            }
        }     
    }

    /// start an http request with a binary body
    async fn make_request_data(&self, attempt: &mut u64, request: &Request, body: &[u8]) -> Result<reqwest::Response> {
        // TODO: body can probably be a boxed stream of some sort which will be faster to clone
        loop {
            *attempt += 1;

            // Build and dispatch the request
            let result = self.client.request(request.method.clone(), request.url.clone())
                .header("Content-Type", "application/x-ndjson")
                .body(body.to_owned())
                .send().await;
            
            // Handle connection errors with a retry, let other non http errors bubble up
            match Self::handle_result(attempt, request, result).await? {
                Some(response) => return Ok(response),
                None => continue,
            }
        }     
    }

    /// checking if an index of the given name exists
    pub async fn does_index_exist(&self, name: &str) -> Result<bool> {
        let url = self.host.join(name)?;
        match self.make_request(&mut 0, &(Method::HEAD, url).into()).await {
            Ok(result) => {
                Ok(result.status() == reqwest::StatusCode::OK)
            },
            Err(err) => if err.is_index_not_found() {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }

    /// Check if an alias with the given name is defined
    pub async fn does_alias_exist(&self, name: &str) -> Result<bool> {
        // self.with_retries(self.datastore.client.indices.exists_alias, name=alias)
        let url = self.host.join("_alias/")?.join(name)?;
        let result = self.make_request(&mut 0, &(reqwest::Method::HEAD, url).into()).await?;
        Ok(result.status() == reqwest::StatusCode::OK)
    }

    /// Create an index alias
    pub async fn put_alias(&self, index: &str, name: &str) -> Result<()> {
        // self.with_retries(self.datastore.client.indices.put_alias, index=index, name=alias)
        let url = self.host.join(&format!("{index}/_alias/{name}"))?;
        self.make_request(&mut 0, &(Method::PUT, url).into()).await?;
        Ok(())
    }

    /// Get the settings map for creating a new index
    fn get_index_settings(&self, index: &str, archive: bool) -> serde_json::Value {
        default_settings(json!({
            "number_of_shards": index_shards(index, archive), // self.shards if not archive else self.archive_shards,
            "number_of_replicas": index_replicas(index, archive), // self.replicas if not archive else self.archive_replicas    
        }))
    }

    fn get_hosts_safe(&self) -> Vec<String> {
        // self.hosts.iter().map(|url|format!("{}:{}", url.host_str().unwrap_or_default(), url.port_or_known_default().unwrap_or(80))).collect()
        vec![format!("{}:{}", self.host.host_str().unwrap_or_default(), self.host.port_or_known_default().unwrap_or(80))]
    }
}


/// Get the number of shards defined for this index
pub fn index_shards(name: &str, archive: bool) -> Option<u32> {
    let name = name.to_uppercase();
    let shards: u32 = match std::env::var(format!("ELASTIC_{name}_SHARDS")) {
        Ok(var) => var.parse().ok()?,
        Err(_) => match std::env::var("ELASTIC_DEFAULT_SHARDS") {
            Ok(var) => var.parse().ok()?,
            Err(_) => 1
        },
    };

    if archive {
        match std::env::var(format!("ELASTIC_{name}_ARCHIVE_SHARDS")) {
            Ok(var) => var.parse().ok(),
            Err(_) => Some(shards)
        }
    } else {
        Some(shards)
    }
}

/// Get the number of replicas defined for this index
fn index_replicas(name: &str, archive: bool) -> Option<u32> {
    let name = name.to_uppercase();
    let replicas: u32 = match std::env::var(format!("ELASTIC_{name}_REPLICAS")) {
        Ok(var) => var.parse().ok()?,
        Err(_) => match std::env::var("ELASTIC_DEFAULT_REPLICAS") {
            Ok(var) => var.parse().ok()?,
            Err(_) => 0
        },
    };

    if archive {
        match std::env::var(format!("ELASTIC_{name}_ARCHIVE_REPLICAS")) {
            Ok(var) => var.parse().ok(),
            Err(_) => Some(replicas)
        }
    } else {
        Some(replicas)
    }
}




pub fn default_settings(index: serde_json::Value) -> serde_json::Value {
    json!({
       "analysis": {
           "filter": {
               "text_ws_dsplit": {
                   "type": "pattern_replace",
                   "pattern": r"(\.)",
                   "replacement": " "
               }
           },
           "analyzer": {
               "string_ci": {
                   "type": "custom",
                   "tokenizer": "keyword",
                   "filter": ["lowercase"]
               },
               "text_fuzzy": {
                   "type": "pattern",
                   "pattern": r"\s*:\s*",
                   "lowercase": false
               },
               "text_whitespace": {
                   "type": "whitespace"
               },
               "text_ws_dsplit": {
                   "type": "custom",
                   "tokenizer": "whitespace",
                   "filters": ["text_ws_dsplit"]
               }
           },
           "normalizer": {
               "lowercase_normalizer": {
                   "type": "custom",
                   "char_filter": [],
                   "filter": ["lowercase"]
               }
           }
       },
       "index": index,
   })
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
    pub async fn connect(url: &str, archive_access: bool, ca_cert: Option<&str>, connect_unsafe: bool) -> Result<Arc<Self>> {
        let helper = Arc::new(ElasticHelper::connect(url, archive_access, ca_cert, connect_unsafe).await?);
        Ok(Arc::new(Self {
            es: helper.clone(),
            file: Collection::new(helper.clone(), "file".to_owned(), Some("file-ma".to_owned())).await?,
            submission: Collection::new(helper.clone(), "submission".to_owned(), Some("submission-ma".to_owned())).await?,
            error: Collection::new(helper.clone(), "error".to_owned(), None).await?,
            service: Collection::new(helper.clone(), "service".to_owned(), None).await?,
            service_delta: Collection::new(helper.clone(), "service_delta".to_owned(), None).await?,
            user: Collection::new(helper.clone(), "user".to_owned(), None).await?,
            filescore: Collection::new(helper.clone(), "filescore".to_owned(), None).await?,
        }))
    }

    // pub async fn update_service_delta(&self, name: &str, delta: &JsonMap) -> Result<()> {
    //     todo!();
    // }

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
        let service_deltas = self.service_delta.stream_search::<JsonMap>("id:*", "*".to_owned(), vec![], None, None, None).await?.collect().await?;

        // // Gather all matching services and apply a mask if we don't want the full source object
        // service_data = [Service(s, mask=mask)
        //                 for s in self.service.multiget([f"{item.id}_{item.version}" for item in service_delta],
        //                                                as_obj=False, as_dictionary=False)]

        let mut service_keys = vec![];

        fn get_id(data: &JsonMap) -> Option<String> {
            Some(format!("{}_{}", data.get("id")?.as_str()?, data.get("version")?.as_str()?))
        }

        for delta in service_deltas.iter() {
            service_keys.push(get_id(delta).ok_or_else(|| ElasticError::fatal("version not found in service delta"))?)
        }
        let key_refs: Vec<&str> = service_keys.iter().map(|val|val.as_str()).collect();
        let mut service_data = self.service.multiget::<JsonMap>(&key_refs, None, None).await.context("multiget_json")?;

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
                None => (JsonMap::from_iter([("expiry_ts".to_owned(), json!(expiry))]), Version::Create),
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
                Err(err) if err.is_version_conflict() => {
                    info!("Retrying save or freshen due to version conflict: {err}");
                    continue
                },
                Err(err) => return Err(err)
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use assemblyline_markings::classification;
    use assemblyline_models::datastore::{File, Service};
    use assemblyline_models::JsonMap;
    use log::debug;
    use rand::{thread_rng, Rng};
    use sha2::Digest;

    use super::Elastic;

    fn create_service(name: &str) -> Service {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "enabled": true,
            "classification": "U",
            "default_result_classification": "U",
            "version": rand::random::<u8>().to_string(),
            "docker_config": {
                "image": "abc:123"
            },
        })).unwrap()
    }

    async fn init() -> Arc<Elastic> {
        let _ = env_logger::builder().is_test(true).filter_level(log::LevelFilter::Debug).try_init();
        Elastic::connect("http://elastic:devpass@localhost:9200", false, None, false).await.unwrap()
    }

    #[tokio::test]
    async fn list_services() {
        let elastic = init().await;
        use serde_json::json;
        // connect to database

        let mut aa = create_service("servicea");
        let bb = create_service("serviceb");

        debug!("save service");
        elastic.service.save(&aa.key(), &aa, None, None).await.unwrap();
        elastic.service.save(&bb.key(), &bb, None, None).await.unwrap();
        debug!("save service delta");
        elastic.service_delta.save_json(&aa.name, &mut [("version".to_owned(), json!(aa.version))].into_iter().collect(), None, None).await.unwrap();
        elastic.service_delta.save_json(&bb.name, &mut [("version".to_owned(), json!(bb.version))].into_iter().collect(), None, None).await.unwrap();
        debug!("commit service_delta");
        elastic.service_delta.commit(None).await.unwrap();

        // fetch the services without changes
        debug!("get service with delta");
        assert_eq!(elastic.get_service_with_delta(&aa.name, None).await.unwrap().unwrap(), aa);
        assert_eq!(elastic.get_service_with_delta(&bb.name, None).await.unwrap().unwrap(), bb);
        {
            debug!("list_all_services");
            let listed = elastic.list_all_services().await.unwrap();
            assert!(listed.contains(&aa));
            assert!(listed.contains(&bb));
        }

        // change one of the services
        aa.category = "DOGHOUSE".to_string();
        aa.enabled = false;
        {
            debug!("update delta");
            let mut delta = elastic.service_delta.get(&aa.name, None).await.unwrap().unwrap();
            delta.category = Some("DOGHOUSE".to_owned());
            delta.enabled = Some(false);
            elastic.service_delta.save(&aa.name, &delta, None, None).await.unwrap();
            elastic.service_delta.commit(None).await.unwrap();
        }

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

    #[tokio::test]
    async fn test_save_or_freshen_file() {
        let ds = init().await;

        let classification = assemblyline_markings::classification::sample_config();
        let ce = assemblyline_markings::classification::ClassificationParser::new(classification).unwrap();


        // Generate random data
        let mut data: Vec<u8> = vec![]; 
        for _ in 0..64 {
            data.extend(b"asfd");
        }
        let expiry_create = chrono::Utc::now() + chrono::Duration::days(14).to_std().unwrap();
        let expiry_freshen = chrono::Utc::now() + chrono::Duration::days(15).to_std().unwrap();

        // Generate file info for random file
        let mut f = File::gen_for_sample(&data, &mut thread_rng());
        f.expiry_ts = Some(chrono::Utc::now());

        // Make sure file does not exists
        ds.file.delete(&f.sha256.to_string(), None).await.unwrap();

        // Save the file
        let raw = if let serde_json::Value::Object(raw) = serde_json::to_value(&f).unwrap() { raw } else { panic!(); };
        ds.save_or_freshen_file(&f.sha256, raw.clone(), Some(expiry_create), ce.restricted().to_owned(), &ce).await.unwrap();

        // Validate created file
        let (saved_file, _) = ds.file.get_if_exists(&f.sha256.to_string(), None).await.unwrap().unwrap();
        assert_eq!(saved_file.sha256, f.sha256);
        assert_eq!(saved_file.sha1, f.sha1);
        assert_eq!(saved_file.md5, f.md5);
        assert_eq!(saved_file.expiry_ts, Some(expiry_create));
        assert_eq!(saved_file.seen.count, 1);
        assert_eq!(saved_file.seen.first, saved_file.seen.last);
        assert_eq!(saved_file.classification.classification, ce.restricted());

        // Freshen the file
        ds.save_or_freshen_file(&f.sha256, raw, Some(expiry_freshen), ce.unrestricted().to_owned(), &ce).await.unwrap();

        // Validate freshened file
        let (freshened_file, _) = ds.file.get_if_exists(&f.sha256.to_string(), None).await.unwrap().unwrap();
        assert_eq!(freshened_file.sha256, f.sha256);
        assert_eq!(freshened_file.sha1, f.sha1);
        assert_eq!(freshened_file.md5, f.md5);
        assert_eq!(freshened_file.expiry_ts, Some(expiry_freshen));
        assert_eq!(freshened_file.seen.count, 2);
        assert!(freshened_file.seen.first < freshened_file.seen.last);
        assert_eq!(freshened_file.classification.classification, ce.unrestricted());
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
