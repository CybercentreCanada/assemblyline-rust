
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use assemblyline_models::datastore::badlist::Badlist;
use assemblyline_models::datastore::heuristic::Heuristic;
use assemblyline_models::datastore::safelist::Safelist;
use log::{debug, error, warn};

pub mod responses;
pub mod collection;
pub mod error;
pub mod search;
pub mod bulk;
pub mod pit;
pub mod request;

#[cfg(test)]
mod test_datastore;
#[cfg(test)]
mod test_mapping;
#[cfg(test)]
mod test_helper;

use assemblyline_markings::classification::ClassificationParser;
use assemblyline_models::datastore::filescore::FileScore;
use assemblyline_models::datastore::user::User;
use assemblyline_models::{ExpandingClassification, JsonMap, Sha256};
use assemblyline_models::datastore::{EmptyResult, Error as ErrorModel, Result as ResultModel, File, Service, ServiceDelta, Submission};
use chrono::{DateTime, TimeDelta, Utc};
use collection::{Collection, OperationBatch};
use error::{ElasticErrorInner, WithContext};
use log::info;
use rand::Rng;
use reqwest::{Method, StatusCode};
use responses::DescribeIndex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use self::error::{ElasticError, Result};
use self::request::Request;

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

const ALT_ELASTICSEARCH_USERS: &[&str] = &["plumber"];

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
    match d {
        serde_json::Value::Array(vec) => {
            let mut out = vec![];
            for item in vec {
                out.push(strip_nulls(item));
            }
            json!(out)
        },
        serde_json::Value::Object(d) => {
            let mut out = JsonMap::new();
            for (key, value) in d {
                if value.is_null() { continue }
                out.insert(key, strip_nulls(value));
            }
            json!(out)    
        },
        value => value
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Result key could not be broken into components [{0}]")]
pub struct InvalidResultKey(String);

pub fn create_empty_result_from_key(key: &str, dtl: i64, cl_engine: &ClassificationParser) -> anyhow::Result<assemblyline_models::datastore::Result> {
    let mut parts = key.split(".");
    let sha256 = parts.next().ok_or(InvalidResultKey(key.to_owned()))?;
    let svc_name = parts.next().ok_or(InvalidResultKey(key.to_owned()))?;
    let svc_version = parts.next().ok_or(InvalidResultKey(key.to_owned()))?;
    let svc_version = &svc_version[1..];

    Ok(assemblyline_models::datastore::Result {
        archive_ts: None,
        expiry_ts: Some(Utc::now() + TimeDelta::days(dtl)),
        classification: ExpandingClassification::new(cl_engine.unrestricted().to_owned(), cl_engine)?,
        response: assemblyline_models::datastore::result::ResponseBody {
            service_name: svc_name.to_owned(),
            service_version: svc_version.to_owned(),
            milestones: Default::default(),
            service_tool_version: Default::default(),
            supplementary: Default::default(),
            extracted: Default::default(),
            service_context: Default::default(),
            service_debug_info: Default::default(),
        },
        sha256: Sha256::from_str(sha256)?,
        created: Utc::now(),
        result: Default::default(),
        result_type: Default::default(),
        size: Default::default(),
        drop_file: Default::default(),
        partial: Default::default(),
        from_archive: Default::default(),
    })
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

// MARK: ElasticHelper

/// Wrapper around the elasticsearch client for helper methods used across contexts
/// This struct is deliberately private to this module
pub struct ElasticHelper {
    pub client: reqwest::Client,
    // pub es: tokio::sync::RwLock<elasticsearch::Elasticsearch>,
    pub host: url::Url,
    pub archive_access: bool,
}

impl ElasticHelper {
    async fn connect(url: &str, archive_access: bool, ca_cert: Option<&[u8]>, connect_unsafe: bool) -> Result<Self> {
        let host: url::Url = url.parse()?;
        let mut builder = reqwest::Client::builder()
            .timeout(get_transport_timeout());

        if let Some(ca_cert) = ca_cert {
            info!("Datastore connecting with a configured CA certificate");
            let cert = reqwest::Certificate::from_pem(ca_cert).map_err(ElasticError::fatal)?;
            builder = builder.add_root_certificate(cert);
        }

        if connect_unsafe {
            info!("Datastore connecting without certificate verification");
            builder = builder.danger_accept_invalid_certs(true);
        }

        Ok(ElasticHelper{
            // es: tokio::sync::RwLock::new(Self::_create_connection(host.clone())?),
            client: builder.build().map_err(ElasticError::fatal)?,
            host,
            archive_access,
        })
    }

    fn change_host(&self, url: url::Url) -> Self {
        ElasticHelper{
            client: self.client.clone(),
            host: url,
            archive_access: self.archive_access,
        }
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
        let body = settings.map(|value| json!({"settings": value}));
        let request = Request::index_copy(&self.host, src, target, copy_method)?;
        let response = match body {
            Some(body) => self.make_request_json(&mut 0, &request, &body).await?,
            None => self.make_request(&mut 0, &request).await?
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
        // debug!("elastic response status: {status}");
        
        // handle non-errors
        if status.is_success() {
            return Ok(Some(response))
        }

        // Since we know we have an error, load the body, for some errors this will have more information
        // let headers = response.headers().clone();
        let body = match response.bytes().await {
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

        // handle specific HTTP status codes we want particular actions for
        if StatusCode::NOT_FOUND == status {
            if request.method == Method::HEAD {
                if let Some(index) = &request.index_name {
                    if let Some(doc) = &request.document_key {
                        return Err(ElasticErrorInner::DocumentNotFound{
                            index: index.to_string(),
                            id: doc.to_string(),
                        }.into())
                    } 
                    return Err(ElasticErrorInner::IndexNotFound(index.to_string()).into());
                }
            }

            // debug!("not found {} {} {} {:?}", request.url, String::from_utf8_lossy(&body), body.len(), headers);
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

            if let Ok(body) = serde_json::from_slice::<NotFoundResponse>(&body) {
                if body.result == Some("not_found") || body.found == Some(false) {
                    return Err(ElasticErrorInner::DocumentNotFound{
                        index: body._index,
                        id: body._id,
                    }.into())
                }    
            }

            #[derive(Deserialize)]
            struct Error<'a> {
                index: &'a str,
                r#type: &'a str,
                // "root_cause": [
                //     {
                //         "type": "index_not_found_exception",
                //         "reason": "no such index [test_field_upgrade_ok_hot]",
                //         "resource.type": "index_or_alias",
                //         "resource.id": "test_field_upgrade_ok_hot",
                //         "index_uuid": "_na_",
                //         "index": "test_field_upgrade_ok_hot"
                //     }
                // ],
                // "reason": "no such index [test_field_upgrade_ok_hot]",
                // "resource.type": "index_or_alias",
                // "resource.id": "test_field_upgrade_ok_hot",
                // "index_uuid": "_na_",
            }

            // Response we might get for missing indices 
            #[derive(Deserialize)]
            struct IndexNotFoundResponse<'a> {
                #[serde(borrow = "'a")]
                error: Error<'a>,
                status: i32
            }

            if let Ok(body) = serde_json::from_slice::<IndexNotFoundResponse>(&body) {
                if body.error.r#type == "index_not_found_exception" {
                    return Err(ElasticErrorInner::IndexNotFound(body.error.index.to_string()).into())
                }    
            }

        } else if StatusCode::CONFLICT == status {
            if request.raise_conflicts {
                // De-sync potential treads trying to write to the index
                tokio::time::sleep(tokio::time::Duration::from_secs_f64(rand::random::<f64>() * 0.1)).await;

                // try to pull out a sensible error message from the response
                if let Ok(response) = serde_json::from_slice::<responses::Error>(&body) {
                    return Err(ElasticErrorInner::VersionConflict(response.error.reason).into())
                }

                // couldn't get an error message, who knows what happened
                return Err(ElasticErrorInner::VersionConflict("unknown".to_owned()).into())
            }
            // updated += ce.info.get('updated', 0)
            // deleted += ce.info.get('deleted', 0)
            return Ok(None)
        } else if StatusCode::SERVICE_UNAVAILABLE == status {
            if let Some(index_name) = &request.index_name {
                warn!("Looks like index {index_name} is not ready yet, retrying...");
                return Ok(None)
            }
            return Err(ElasticErrorInner::Fatal("Database not available".to_string()).into())
        } else if StatusCode::REQUEST_TIMEOUT == status {
            return Err(ElasticErrorInner::Timeout.into())
        } else if StatusCode::INTERNAL_SERVER_ERROR == status {
            // some errors don't have a status code assigned to them, try to read them from the body
            if let Ok(body) = serde_json::from_slice::<responses::Error>(&body) {
                if body.error._type == "timeout_exception" {
                    return Err(ElasticErrorInner::Timeout.into())
                }
                return Err(ElasticError::fatal("server error: ".to_owned() + &body.error._type))
            }
            return Err(ElasticError::fatal("server error"))
        } else if StatusCode::FORBIDDEN == status {
            if let Some(index_name) = &request.index_name {
                log::warn!("Elasticsearch cluster is preventing writing operations on index {index_name}, retrying...");
                return Ok(None)
            }
            return Err(ElasticError::fatal("request forbidden"))
        } else if StatusCode::TOO_MANY_REQUESTS == status {
            let index = request.index_name.as_deref().unwrap_or("UNKNOWN").to_uppercase();
            log::warn!("Elasticsearch is too busy to perform the requested task on index {}, retrying...", index);
            return Ok(None)
        } else if StatusCode::UNAUTHORIZED == status {
            // authentication errors
            // let hosts = $helper.get_hosts_safe().join(" | ");
            // warn!("No connection to Elasticsearch server(s): {}, because [{}] retrying [{}]...", hosts, message, stringify!($expression));
            warn!("No connection to Elasticsearch server(s) retrying...");
            return Ok(None)
        }

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

        error!("unexpected elastic error: {}", String::from_utf8_lossy(&body));
        return Err(ElasticError::fatal(format!("Unexpected elastic error [status: {status}]")))
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
        match self.make_request(&mut 0, &Request::head_index(&self.host, name)?).await {
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
        let request = Request::head_alias(&self.host, name)?;
        let result = self.make_request(&mut 0, &request).await?;
        Ok(result.status() == reqwest::StatusCode::OK)
    }

    /// Create an index alias
    pub async fn put_alias(&self, index: &str, name: &str) -> Result<()> {
        // self.with_retries(self.datastore.client.indices.put_alias, index=index, name=alias)
        let request = Request::put_alias(&self.host, index, name)?;
        self.make_request(&mut 0, &request).await?;
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

    pub async fn remove_index(&self, name: &str) -> Result<()> {
        debug!("Removing index: {name}");
        let request = Request::delete_index(&self.host, name)?;
        match self.make_request(&mut 0, &request).await {
            Ok(_) => Ok(()),
            Err(error) => if error.is_index_not_found() {
                Ok(())
            } else {
                Err(error)
            }
        }
    } 

    // retry_function=None
    async fn get_task_results(&self, task: &str) -> Result<responses::TaskResponse> {
        // This function is only used to wait for a asynchronous task to finish in a graceful manner without
        //  timing out the elastic client. You can create an async task for long running operation like:
        //   - update_by_query
        //   - delete_by_query
        //   - reindex ...
        // if retry_function is None:
        //     retry_function = self.with_retries

        let res: responses::TaskBody = loop {
            let request = Request::get_task(&self.host, task, true, "5s")?;
            let res = self.make_request(&mut 0, &request).await;

            match res {
                Ok(ok) => break ok.json().await?,
                Err(err) if err.is_timeout() => { continue }
                Err(err) => return Err(err)
            }
        };

        Ok(res.response)
    }

    pub async fn list_indices(&self, prefix: &str) -> Result<Vec<String>> {
        let request = Request::get_indices(&self.host, prefix)?;
        let response = self.make_request(&mut 0, &request).await?;
        let body: DescribeIndex = response.json().await?;
        Ok(body.indices.into_keys().collect())
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

// MARK: Elastic

/// Public interface to our elastic datastore.
/// details are actually in Collection and ElasticHelper classes.
pub struct Elastic {
    es: Arc<ElasticHelper>,
    prefix: String,

    pub file: Collection<File>,
    pub submission: Collection<Submission>,
    pub user: Collection<User>,
    pub error: Collection<ErrorModel>,
    pub safelist: Collection<Safelist>,
    pub badlist: Collection<Badlist>,
    pub heuristic: Collection<Heuristic>,

    pub result: Collection<assemblyline_models::datastore::result::Result>,
    pub emptyresult: Collection<EmptyResult>,
    pub filescore: Collection<FileScore>,

    /// Unmodified default service data classes
    pub service: Collection<Service>,

    /// Modifications to service data for this system
    pub service_delta: Collection<ServiceDelta>,
}

impl Elastic {
    pub async fn connect(url: &str, archive_access: bool, ca_cert: Option<&[u8]>, connect_unsafe: bool, prefix: &str) -> Result<Arc<Self>> {
        let helper = Arc::new(ElasticHelper::connect(url, archive_access, ca_cert, connect_unsafe).await?);
        Self::setup(helper, prefix).await
    }

    async fn setup(helper: Arc<ElasticHelper>, prefix: &str) -> Result<Arc<Self>> {
        macro_rules! collection {
            ($name: expr) => {
                Collection::new(helper.clone(), $name.to_owned(), None, prefix.to_string(), true).await?
            };
            (archive, $name: expr) => {
                Collection::new(helper.clone(), $name.to_owned(), Some($name.to_owned() + "-ma"), prefix.to_string(), true).await?
            };
        }

        Ok(Arc::new(Self {
            es: helper.clone(),
            file: collection!(archive, "file"),
            submission: collection!(archive, "submission"),
            error: collection!("error"),
            safelist: collection!("safelist"),
            badlist: collection!("badlist"),
            result: collection!(archive, "result"),
            emptyresult: collection!("emptyresult"),
            heuristic: collection!("heuristic"),
            service: collection!("service"),
            service_delta: collection!("service_delta"),
            user: collection!("user"),
            filescore: collection!("filescore"),
            prefix: prefix.to_string(),
        }))
    }

    pub async fn list_indices(&self) -> Result<Vec<String>> {
        self.es.list_indices(&self.prefix).await
    } 

    #[cfg(test)]
    pub async fn wipe_all(&self) -> Result<()> {
        for index in self.list_indices().await? {
            self.es.remove_index(&index).await?;
        }
        Ok(())
    } 

    pub async fn switch_user(&self, username: &str) -> Result<Arc<Elastic>> {
        if !ALT_ELASTICSEARCH_USERS.contains(&username){
            warn!("Unknown alternative user '{username}' to switch to for Elasticsearch");
        }
        // generate a format safe random password that is just 16 characters of hex
        let password = hex::encode(rand::rng().random::<u128>().to_be_bytes());

        if username.starts_with("plumber") {
            // Ensure roles for "plumber" user are created
            let request = Request::put_role(&self.es.host, "manage_tasks")?;
            self.es.make_request_json(&mut 0, &request, &json!({
                "indices": [{
                    "names": [".tasks"], 
                    "privileges": ["all"], 
                    "allow_restricted_indices": true,
                }]
            })).await?;
            // self.with_retries(
            //     self.client.security.put_role,
            //     name="manage_tasks",
            //     indices=[{}])

            // Initialize/update 'plumber' user in Elasticsearch to perform cleanup
            let request = Request::post_user(&self.es.host, username)?;
            self.es.make_request_json(&mut 0, &request, &json!({
                "password": password,
                "roles": ["manage_tasks", "superuser"]
            })).await?;
        }

        // Modify the client details for next reconnect
        let mut url = self.es.host.clone();

        url.set_username(username).map_err(|_| ElasticErrorInner::Fatal("Could not set username when switching user".to_owned()))?;
        url.set_password(Some(&password)).map_err(|_| ElasticErrorInner::Fatal("Could not set password when switching user".to_owned()))?;

        // self._hosts = [h.replace(f"{urlparse(h).username}:{urlparse(h).password}",
        //                         f"{username}:{password}") for h in self._hosts]
        // self.client.close()
        // self.connection_reset()
        let helper = Arc::new(self.es.change_host(url));
        Self::setup(helper, &self.prefix).await
    }

    pub async fn task_cleanup(&self, deleteable_task_age: Option<chrono::TimeDelta>, max_tasks: Option<u64>) -> Result<u64> {
        let deleteable_task_age = deleteable_task_age.unwrap_or(chrono::TimeDelta::zero());

        // Create the query to delete the tasks
        //   NOTE: This will delete up to 'max_tasks' completed tasks older then a 'deleteable_task_age'
        let q = format!("completed:true AND task.start_time_in_millis:<{}", (Utc::now() - deleteable_task_age).timestamp_millis());

        // Create a new task to delete expired tasks
        let request = Request::delete_by_query(&self.es.host, ".tasks", false, "proceed", max_tasks)?;
        let task: responses::TaskId = self.es.make_request_json(&mut 0, &request, &json!({
            "query": {"bool": {"must": {"query_string": {"query": q}}}},
        })).await?.json().await?;

        // Wait until the tasks deletion task is over
        let res = self.es.get_task_results(&task.task).await?;

        // return the number of deleted items
        return Ok(res._status.deleted)
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
                let error_string = format!("Could not convert json into service: {data}");
                services.push(serde_json::from_value(data).map_err(ElasticError::from).context(&error_string)?);
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
                        
                        match self.file.update(sha256, batch, None, Some(8)).await {
                            Ok(true) => return Ok(()),
                            Ok(false) => {
                                warn!("fast save_or_freshen failed");
                            },
                            Err(err) => {
                                error!("fast save_or_freshen failed: {err}");
                            }
                        }
                    } else {
                        debug!("Skipping fast save_or_freshen {classification} != {server_classification}");
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
            seen.insert("count".to_string(), json!(extract_number(seen, "count") + 1));
            seen.insert("last".to_owned(), json!(now));
            seen.insert("first".to_owned(), seen.get("first").unwrap_or(&json!(now)).clone());

            // Update Classification
            // current_fileinfo.insert("classification".to_owned(), json!(classification));
            ExpandingClassification::<false>::insert(cl_engine, &mut current_fileinfo, &classification)?;

            // write the file
            // let current_fileinfo: File = serde_json::from_value(json!(current_fileinfo))?;
            // let result = self.file.save_json(sha256, &mut current_fileinfo, Some(version), None).await;
            // match result {
            //     Ok(_) => return Ok(()),
            //     Err(err) if err.is_version_conflict() => {
            //         info!("Retrying save or freshen due to version conflict: {err}");
            //         continue
            //     },
            //     Err(err) => return Err(err)
            // }
            // let file: File = serde_json::from_value(serde_json::Value::Object(current_fileinfo))?;
            let result = self.file.save_json(sha256, &mut current_fileinfo, Some(version), None).await;
            match result {
                Ok(_) => return Ok(()),
                Err(err) if err.is_version_conflict() => {
                    debug!("Retrying save or freshen due to version conflict: {err}");
                    continue
                },
                Err(err) => return Err(err)
            }
        }
    }

    pub async fn list_service_heuristics(&self, service_name: &str) -> Result<Vec<Heuristic>> {
        let mut heuristics = vec![];
        let mut cursor = self.heuristic.stream_search(&format!("id:{}.*", service_name.to_uppercase()), "*".to_string(), vec![], None, None, None).await?;
        while let Some(row) = cursor.next().await? {
            heuristics.push(row);
        }
        Ok(heuristics)
    }

    pub async fn list_all_heuristics(&self) -> Result<Vec<Heuristic>> {
        let mut heuristics = vec![];
        let mut cursor = self.heuristic.stream_search("id:*", "*".to_string(), vec![], None, None, None).await?;
        while let Some(row) = cursor.next().await? {
            heuristics.push(row);
        }
        Ok(heuristics)
    }

    pub async fn ping(&self) -> bool {
        match self.es.client.head(self.es.host.clone()).send().await {
            Ok(res) => res.status().is_success(),
            _ => false
        }
    }

    #[cfg(test)]
    pub async fn apply_test_settings(&self) -> Result<()> {
        let body = serde_json::json!({
            "persistent": {
                "cluster.max_shards_per_node": 1000000
            }
        });
        self.es.client.put(self.es.host.join("/_cluster/settings")?).json(&body).send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn get_single_result(&self, key: &str, dtl: i64, cl_engine: &ClassificationParser) -> anyhow::Result<Option<ResultModel>> {
        if key.ends_with(".e") {
            Ok(Some(create_empty_result_from_key(key, dtl, cl_engine)?))
        } else {
            Ok(self.result.get(key, None).await?)
        }
    }

}


fn extract_number(container: &JsonMap, name: &str) -> u64 {
    match container.get(name) {
        Some(value) => serde_json::from_value(value.clone()).unwrap_or(0),
        None => 0,
    }
}

