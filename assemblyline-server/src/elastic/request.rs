use std::borrow::Cow;

use reqwest::Method;
use super::{Backend, CopyMethod, ElasticError, Result};

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

    /// Build the URL for an operation with safe path building.
    ///
    /// Each path segment given must stay a single path segment of the request URL.
    /// `path_segments_mut().extend()` percent-encodes `/`, `?`, `#` and `%`
    /// within each segment (so pre-encoded sequences like `%2e%2e` cannot be
    /// smuggled through either), which keeps a hostile key from rewriting the
    /// request path, query or fragment the way interpolating it into a string
    /// handed to `Url::join` would. The literal keys `.` and `..` (and the
    /// empty string) are rejected outright: they contain nothing to encode,
    /// yet would still resolve as dot segments or vanish from the path.
    ///
    /// Any base path configured on the datastore URL is preserved.
    pub fn build_url(host: &reqwest::Url, path: &[&str]) -> Result<reqwest::Url> {
        let mut url = host.clone();
        url.set_query(None);
        url.set_fragment(None);
        if path.iter().any(|&p| p == "." || p == ".." || p.is_empty()) {
            return Err(ElasticError::fatal("datastore path component is empty"))
        }
        url.path_segments_mut()
            .map_err(|_| ElasticError::fatal("datastore URL must be a scheme that includes path components"))?
            .pop_if_empty()
            .extend(path);
        Ok(url)
    }

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
        let mut url = Self::build_url(host, &["_tasks", task_id])?;

        url.query_pairs_mut()
            .append_pair("wait_for_completion", &wait_for_completion.to_string().to_lowercase())
            .append_pair("timeout", timeout);

        Ok(Self::new(Method::GET, url, None))
    }

    pub fn delete_by_query(host: &reqwest::Url, name: &str, wait_for_completion: bool, conflicts: &str, max_docs: Option<u64>) -> Result<Self> {
        let mut url = Self::build_url(host, &[name, "_delete_by_query"])?;

        url.query_pairs_mut()
            .append_pair("wait_for_completion", &wait_for_completion.to_string().to_lowercase())
            .append_pair("conflicts", conflicts);
        if let Some(max_docs) = max_docs {
            url.query_pairs_mut().append_pair("max_docs", &max_docs.to_string());
        }

        Ok(Self::new(Method::POST, url, Some(name.to_owned())))
    }

    pub fn post_user(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, Self::build_url(host, &["_security", "user", name])?, None))
    }

    pub fn put_role(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, Self::build_url(host, &["_security", "role", name])?, None))
    }

    pub fn put_index_settings(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, Self::build_url(host, &[index, "_settings"])?, Some(index.to_owned())))
    }

    pub fn put_index_mapping(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, Self::build_url(host, &[index, "_mapping"])?, Some(index.to_owned())))
    }

    pub fn put_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, Self::build_url(host, &[index])?, Some(index.to_owned())))
    }

    pub fn get_index(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::GET, Self::build_url(host, &[name])?, None))
    }

    pub fn get_indices(host: &reqwest::Url, prefix: &str) -> Result<Self> {
        Ok(Self::new(Method::GET, Self::build_url(host, &[&(prefix.to_string() + "*")])?, None))
    }

    pub fn head_doc(host: &reqwest::Url, index: &str, id: &str) -> Result<Self> {
        let url = Self::build_url(host, &[index, "_doc", id])?;
        Ok(Self::new_on_document(Method::HEAD, url, index.to_owned(), id.to_owned()))
    }

    pub fn head_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::HEAD, Self::build_url(host, &[index])?, Some(index.to_owned())))
    }

    pub fn delete_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::DELETE, Self::build_url(host, &[index])?, Some(index.to_owned())))
    }

    pub fn get_search_on(host: &reqwest::Url, target: &str, params: Vec<(&str, Cow<str>)>) -> Result<Self> {
        let mut url = Self::build_url(host, &[target, "_search"])?;
        url.query_pairs_mut().extend_pairs(params);
        Ok(Self::new(Method::GET, url, None))
    }

    pub fn get_search(host: &reqwest::Url, params: Vec<(&str, Cow<str>)>) -> Result<Self> {
        let mut url = Self::build_url(host, &["_search"])?;
        url.query_pairs_mut().extend_pairs(params);
        Ok(Self::new(Method::GET, url, None))
    }

    pub fn post_aliases(host: &reqwest::Url) -> Result<Self> {
        Ok(Self::new(Method::POST, Self::build_url(host, &["_aliases"])?, None))
    }

    pub fn post_reindex(host: &reqwest::Url, wait_for_completion: bool) -> Result<Self> {
        let mut url = Self::build_url(host, &["_reindex"])?;

        url.query_pairs_mut()
        .append_pair("wait_for_completion", &wait_for_completion.to_string().to_lowercase());

        Ok(Self::new(Method::POST, url, None))
    }

    pub fn head_alias(host: &reqwest::Url, name: &str) -> Result<Self> {
        Ok(Self::new(Method::HEAD, Self::build_url(host, &["_alias", name])?, None))
    }

    pub fn put_alias(host: &reqwest::Url, index: &str, alias: &str) -> Result<Self> {
        Ok(Self::new(Method::PUT, Self::build_url(host, &[index, "_alias", alias])?, None))
    }

    pub fn post_refresh_index(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, Self::build_url(host, &[index, "_refresh"])?, Some(index.to_owned())))
    }

    pub fn post_clear_index_cache(host: &reqwest::Url, index: &str) -> Result<Self> {
        Ok(Self::new(Method::POST, Self::build_url(host, &[index, "_cache", "clear"])?, Some(index.to_owned())))
    }

    pub fn bulk(host: &reqwest::Url) -> Result<Self> {
        Ok(Self::new(Method::POST, Self::build_url(host, &["_bulk"])?, None))
    }

    pub fn create_pit(host: &reqwest::Url, index: &str, keep_alive: &str, backend: Backend) -> Result<Self> {
        let mut url = match backend {
            Backend::Elasticsearch => Self::build_url(host, &[index, "_pit"])?,
            Backend::Opensearch => Self::build_url(host, &[index, "_search", "point_in_time"])?,
        };
        url.query_pairs_mut().append_pair("keep_alive", keep_alive);
        Ok(Self::new(Method::POST, url, Some(index.to_owned())))
    }

    pub fn delete_doc(host: &reqwest::Url, index: &str, key: &str) -> Result<Self> {
        Ok(Self::new(Method::DELETE, Self::build_url(host, &[index, "_doc", key])?, Some(index.to_owned())))
    }

    pub fn get_doc(host: &reqwest::Url, index: &str, key: &str) -> Result<Self> {
        // prepare the url
        let mut url = Self::build_url(host, &[index, "_doc", key])?;
        url.query_pairs_mut().append_pair("_source", "true");
        Ok(Self::new(Method::GET, url, Some(index.to_owned())))
    }

    pub fn mget_doc(host: &reqwest::Url, index: &str) -> Result<Self> {
        let mut url = Self::build_url(host, &[index, "_mget"])?;
        url.query_pairs_mut().append_pair("_source", "true");
        Ok(Self::new(Method::GET, url, Some(index.to_owned())))
    }

    pub fn update_doc(host: &reqwest::Url, index: &str, key: &str, retry_on_conflict: Option<i64>) -> Result<Self> {
        let mut url = Self::build_url(host, &[index, "_update", key])?;
        if let Some(retry_on_conflict) = retry_on_conflict {
            url.query_pairs_mut().append_pair("retry_on_conflict", &retry_on_conflict.to_string());
        }
        Ok(Self::new(Method::POST, url, Some(index.to_owned())))
    }

    pub fn index_copy(host: &reqwest::Url, src: &str, target: &str, copy_method: CopyMethod) -> Result<Self> {
        let mut url = Self::build_url(host, &[src, &copy_method.to_string(), target])?;
        url.query_pairs_mut().append_pair("timeout", "60s");

        Ok(Self::new(Method::POST, url, None))
    }

    pub fn delete_pit(host: &reqwest::Url, backend: Backend) -> Result<Self> {
        let url = match backend {
            Backend::Elasticsearch => Self::build_url(host, &["_pit"])?,
            Backend::Opensearch => Self::build_url(host, &["_search", "point_in_time"])?,
        };
        Ok(Self::new(Method::DELETE, url, None))
    }

    pub fn with_raise_conflict(method: reqwest::Method, url: reqwest::Url, index: String) -> Self {
        let mut item = Self::new(method, url, Some(index));
        item.raise_conflicts = true;
        item
    }
}

#[cfg(test)]
mod tests {
    use super::Request;

    #[test]
    fn document_keys_stay_in_one_path_segment() {
        let host = reqwest::Url::parse("http://localhost:9200/").unwrap();

        // legitimate key shapes pass through byte-identical
        for key in [
            "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730",
            "sample_service_4.0.0",
            "7d86.ServiceName.v4_0_0.cABCDEF.e",
            "admin",
        ] {
            let url = Request::build_url(&host, &["index", "_doc", key]).unwrap();
            assert_eq!(url.path(), format!("/index/_doc/{key}"));
        }

        // '+' (apikey ids) is a legal path character and passes through
        // unchanged (unlike urlencoding::encode, which would use %2B; both
        // forms resolve to the same elasticsearch document id)
        let url = Request::build_url(&host, &["apikey", "_doc", "devkey+admin"]).unwrap();
        assert_eq!(url.path(), "/apikey/_doc/devkey+admin");

        // separators and pre-encoded dot segments cannot escape {index}/{endpoint}/
        for key in [
            "../../user/_doc/admin",
            "1/%2e%2e/%2e%2e/%2e%2e/apikey/_doc/devkey+admin#",
            "x/../../y",
            "x?pretty",
            "x#frag",
            "%2e%2e",
        ] {
            let url = Request::build_url(&host, &["emptyresult", "_doc", key]).unwrap();
            assert!(url.path().starts_with("/emptyresult/_doc/"), "{key} escaped: {url}");
            assert_eq!(url.query(), None, "{key} injected query: {url}");
            assert_eq!(url.fragment(), None, "{key} injected fragment: {url}");
            // the key stayed a single segment
            assert_eq!(url.path_segments().unwrap().count(), 3, "{key} split: {url}");
        }

        // a base path on the datastore URL is preserved
        let based = reqwest::Url::parse("http://localhost:9200/es/").unwrap();
        let url = Request::build_url(&based, &["index", "_doc", "key"]).unwrap();
        assert_eq!(url.path(), "/es/index/_doc/key");

        // bare dot segments and empty keys are rejected on every doc-key builder
        for key in [".", "..", ""] {
            assert!(Request::get_doc(&host, "badlist", key).is_err());
            assert!(Request::head_doc(&host, "badlist", key).is_err());
            assert!(Request::delete_doc(&host, "badlist", key).is_err());
            assert!(Request::update_doc(&host, "badlist", key, None).is_err());
        }

        // the raw (unencoded) key is what lands in the error-reporting metadata
        let request = Request::head_doc(&host, "apikey", "devkey+admin").unwrap();
        assert_eq!(request.document_key.as_deref(), Some("devkey+admin"));
    }
}
