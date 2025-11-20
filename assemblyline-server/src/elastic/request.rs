use std::borrow::Cow;

use reqwest::Method;
use super::{CopyMethod, Result};

/// The header section and local parameters to a request to elasticsearch.
/// 
/// Not to be confused with the actual HTTP request constructed to make a query.
/// This is just some of the things you need to build that HTTP request and information
/// needed to handle its outcome locally
#[derive(Debug, Clone)]
pub struct Request {
    pub method: reqwest::Method, 
    pub url: reqwest::Url,
    pub index_name: Option<String>,
    pub document_key: Option<String>,
    pub raise_conflicts: bool,
    // raise_not_found: bool,
}

impl std::fmt::Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{} {}://{}:{}{}", self.method, self.url.scheme(), self.url.host_str().unwrap_or_default(), self.url.port_or_known_default().unwrap_or(80), self.url.path()))
    }
}

// impl From<(reqwest::Method, reqwest::Url)> for Request {
//     fn from(value: (reqwest::Method, reqwest::Url)) -> Self {
//         Request {
//             method: value.0,
//             url: value.1,
//             raise_conflicts: false
//         }
//     }
// }wait_for_completion

impl Request {
    pub fn new(method: Method, url: reqwest::Url, index: Option<String>) -> Self {
        Self {
            method,
            url,
            index_name: index,
            document_key: None,
            raise_conflicts: false,
            // raise_not_found: false,
        }
    }

    // fn new_on_index(method: Method, url: reqwest::Url) -> Self {
    //     Self {
    //         method,
    //         url,
    //         index_name: Some(index),
    //         // document_key: None,
    //         raise_conflicts: false,
    //         // raise_not_found: false,
    //     }
    // }

    pub fn new_on_document(method: Method, url: reqwest::Url, index: String, document: String) -> Self {
        Self {
            method,
            url,
            index_name: Some(index),
            document_key: Some(document),
            raise_conflicts: false,
            // raise_not_found: false,
        }
    }

    pub fn get_task(host: &reqwest::Url, task_id: &str, wait_for_completion: bool, timeout: &str) -> Result<Self> {
        let mut url = host.join(&format!("/_tasks/{task_id}"))?;

        url.query_pairs_mut()
            .append_pair("wait_for_completion", &wait_for_completion.to_string().to_lowercase())
            .append_pair("timeout", timeout);

        Ok(Self::new(Method::GET, url, None))
    }

    pub fn delete_by_query(host: &reqwest::Url, name: &str, wait_for_completion: bool, conflicts: &str, max_docs: Option<u64>) -> Result<Self> {
        let mut url = host.join(&format!("/{name}/_delete_by_query"))?;

        url.query_pairs_mut()
            .append_pair("wait_for_completion", &wait_for_completion.to_string().to_lowercase())
            .append_pair("conflicts", conflicts);
        if let Some(max_docs) = max_docs {
            url.query_pairs_mut().append_pair("max_docs", &max_docs.to_string());
        }

        Ok(Self::new(Method::POST, url, Some(name.to_owned())))
    }
    
    pub fn post_user(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, host.join(&format!("_security/user/{name}"))?, None))
    }

    pub fn put_role(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, host.join(&format!("_security/role/{name}"))?, None))
    }

    pub fn put_index_settings(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, host.join(&format!("{index}/_settings"))?, Some(index.to_owned())))
    }

    pub fn put_index_mapping(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, host.join(&format!("{index}/_mapping"))?, Some(index.to_owned())))
    }
    
    pub fn put_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, host.join(index)?, Some(index.to_owned())))
    }

    pub fn get_index(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::GET, host.join(name)?, None))
    }

    pub fn get_indices(host: &reqwest::Url, prefix: &str) -> Result<Self> {
        Ok(Self::new(Method::GET, host.join(&(prefix.to_string() + "*"))?, None))
    }

    pub fn head_doc(host: &reqwest::Url, index: &str, id: &str) -> Result<Self> {
        Ok(Self::new_on_document(Method::HEAD, host.join(&format!("{index}/_doc/{id}"))?, index.to_owned(), id.to_owned()))
    }    

    pub fn head_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::HEAD, host.join(index)?, Some(index.to_owned())))
    }    
    
    pub fn delete_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::DELETE, host.join(index)?, Some(index.to_owned())))
    }

    pub fn get_search_on(host: &reqwest::Url, target: &str, params: Vec<(&str, Cow<str>)>) -> Result<Self> {
        let mut url = host.join(&format!("{target}/_search"))?;  
        url.query_pairs_mut().extend_pairs(params);
        Ok(Self::new(Method::GET, url, None))
    }

    pub fn get_search(host: &reqwest::Url, params: Vec<(&str, Cow<str>)>) -> Result<Self> {
        let mut url = host.join("_search")?;  
        url.query_pairs_mut().extend_pairs(params);
        Ok(Self::new(Method::GET, url, None))
    }

    pub fn post_aliases(host: &reqwest::Url) -> Result<Self> {
        Ok(Self::new(Method::POST, host.join("_aliases")?, None))
    }

    pub fn post_reindex(host: &reqwest::Url, wait_for_completion: bool) -> Result<Self> {
        let mut url = host.join("_reindex")?;

        url.query_pairs_mut()
        .append_pair("wait_for_completion", &wait_for_completion.to_string().to_lowercase());

        Ok(Self::new(Method::POST, url, None))
    }

    pub fn head_alias(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::HEAD, host.join("_alias/")?.join(name)?, None))
    }    
    
    pub fn put_alias(host: &reqwest::Url, index: &str, alias: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, host.join(&format!("{index}/_alias/{alias}"))?, None))
    }

    pub fn post_refresh_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, host.join(&format!("{index}/_refresh"))?, Some(index.to_owned())))
    }

    pub fn post_clear_index_cache(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, host.join(&format!("{index}/_cache/clear"))?, Some(index.to_owned())))
    }

    pub fn bulk(host: &reqwest::Url) -> Result<Self> {
        Ok(Self::new(Method::POST, host.join("_bulk")?, None))
    }

    pub fn create_pit(host: &reqwest::Url, index: &str, keep_alive: &str) -> Result<Self> {
        let mut url = host.join(&format!("{index}/_pit"))?;
        url.query_pairs_mut().append_pair("keep_alive", keep_alive);
        Ok(Self::new(Method::POST, url, Some(index.to_owned())))
    }

    pub fn delete_doc(host: &reqwest::Url, index: &str, key: &str) -> Result<Self> {
        Ok(Self::new(Method::DELETE, host.join(&format!("{index}/_doc/{key}"))?, Some(index.to_owned())))
    }

    pub fn get_doc(host: &reqwest::Url, index: &str, key: &str) -> Result<Self> {
        // prepare the url
        let mut url = host.join(&format!("{index}/_doc/{key}"))?;
        url.query_pairs_mut().append_pair("_source", "true");
        Ok(Self::new(Method::GET, url, Some(index.to_owned())))
    }

    pub fn mget_doc(host: &reqwest::Url, index: &str) -> Result<Self> {
        let mut url = host.join(&format!("{index}/_mget"))?;
        url.query_pairs_mut().append_pair("_source", "true");
        Ok(Self::new(Method::GET, url, Some(index.to_owned())))
    }    
    
    pub fn update_doc(host: &reqwest::Url, index: &str, key: &str, retry_on_conflict: Option<i64>) -> Result<Self> {
        let mut url = host.join(&format!("/{index}/_update/{key}"))?;
        if let Some(retry_on_conflict) = retry_on_conflict {
            url.query_pairs_mut().append_pair("retry_on_conflict", &retry_on_conflict.to_string());
        }
        Ok(Self::new(Method::POST, url, Some(index.to_owned())))
    }

    pub fn index_copy(host: &reqwest::Url, src: &str, target: &str, copy_method: CopyMethod) -> Result<Self> {    
        let mut url = host.join(&format!("{src}/{copy_method}/{target}"))?;
        url.query_pairs_mut().append_pair("timeout", "60s");

        Ok(Self::new(Method::POST, url, None))
    }    

    pub fn delete_pit(host: &reqwest::Url) -> Result<Self> {
        Ok(Self::new(Method::DELETE, host.join("/_pit")?, None))
    }

    pub fn with_raise_conflict(method: reqwest::Method, url: reqwest::Url, index: String) -> Self {
        let mut item = Self::new(method, url, Some(index));
        item.raise_conflicts = true;
        item
    }
}