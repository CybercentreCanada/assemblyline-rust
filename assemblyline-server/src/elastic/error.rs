use std::fmt::Display;

use itertools::Itertools;


#[derive(Debug)]
pub struct ElasticError {
    inner: Box<ElasticErrorInner>
}

impl Display for ElasticError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.inner))
    }
}

impl ElasticError {
    pub fn multi_key_error(keys: impl IntoIterator<Item=impl Display>) -> Self {
        ElasticErrorInner::MultiKeyError(keys.into_iter().map(|key| key.to_string()).collect_vec()).into()
    }

    pub fn json(error: impl std::fmt::Display) -> Self {
        ElasticErrorInner::JsonError(error.to_string()).into()
    }

    pub fn fatal(error: impl std::fmt::Display) -> Self {
        ElasticErrorInner::Fatal(error.to_string()).into()
    }

    pub fn is_version_conflict(&self) -> bool {
        self.inner.is_version_conflict()
    }

    pub fn is_document_not_found(&self) -> bool {
        self.inner.is_document_not_found()
    }

    pub fn is_index_not_found(&self) -> bool {
        self.inner.is_index_not_found()
    }

    pub fn is_resource_already_exists(&self) -> bool {
        self.inner.is_resource_already_exists()
    }
}

impl<T: Into<ElasticErrorInner>> From<T> for ElasticError {
    fn from(value: T) -> Self {
        Self{ inner: Box::new(value.into()) }
    }
}

impl std::error::Error for ElasticError {
    
}

// match &err {
//     ElasticError::HTTPError{code: StatusCode::BAD_REQUEST, message, ..} => {
//         if message.contains("resource_already_exists_exception") {
//             warn!("Tried to create an index template that already exists: {}", alias.to_uppercase());    
//         } else {
//             return Err(err).context("put index bad request")
//         }
//     },
//     _ => return Err(err).context("put index other error")
// };


#[derive(Debug, thiserror::Error)]
pub enum ElasticErrorInner {
    // start out with a range of errors we may want to recover from specifically
    #[error("Keys couldn't be found during multiget: {0:?}")]
    MultiKeyError(Vec<String>),
    #[error("Document [{id}] not found in index [{index}]")]
    DocumentNotFound{index: String, id: String},
    #[error("Index not found: {0}")]
    IndexNotFound(String),
    #[error("version conflict -- {0}")]
    VersionConflict(String),
    #[error("Resource conflict, already exists")]
    ResourceAlreadyExists,

    // Some errors that handle specific AL runtime conditions, but probably won't be handled outright
    #[error("Index {0} does not have an archive")]
    ArchiveNotFound(String),
    #[error("Trying to get access to the archive on a datastore where archive_access is disabled")]
    ArchiveDisabled,
    #[error("Failed to create index {target} from {src}")]
    FailedToCreateIndex { src: String, target: String },

    // errors we expect to mostly be handled within a retry loop
    #[error("Network error: {source}")]
    NetworkError{ 
        source: Box<dyn std::error::Error + Send + Sync>
    },

    // bundle all of our lower level non-recoverable/fatal errors under broad catagories
    #[error("Json format error: {0}")]
    JsonError(String),
    #[error("Datastore error: {0}")]
    Fatal(String),

    // A chained error to provide better error context
    #[error(" - {context}\n{inner}")]
    Context{ 
        context: String, 
        inner: Box<ElasticErrorInner>
    },
}

impl ElasticErrorInner {
    pub fn is_version_conflict(&self) -> bool {
        match self {
            Self::VersionConflict(_) => true,
            Self::Context{inner, ..} => inner.is_version_conflict(),
            _ => false,
        }
    }

    pub fn is_document_not_found(&self) -> bool {
        match self {
            Self::DocumentNotFound{..} => true,
            Self::Context{inner, ..} => inner.is_document_not_found(),
            _ => false,
        }
    }

    pub fn is_index_not_found(&self) -> bool {
        match self {
            Self::IndexNotFound(_) => true,
            Self::Context{inner, ..} => inner.is_index_not_found(),
            _ => false,
        }
    }

    pub fn is_resource_already_exists(&self) -> bool {
        match self {
            Self::ResourceAlreadyExists => true,
            Self::Context{inner, ..} => inner.is_resource_already_exists(),
            _ => false,
        }
    }
}


impl From<serde_json::Error> for ElasticErrorInner {
    fn from(value: serde_json::Error) -> Self {
        Self::JsonError(value.to_string())
    }
} 

impl From<url::ParseError> for ElasticErrorInner {
    fn from(value: url::ParseError) -> Self {
        Self::NetworkError{ source: Box::new(value) }
    }
}

// impl From<elasticsearch::http::transport::BuildError> for ElasticErrorInner {
//     fn from(value: elasticsearch::http::transport::BuildError) -> Self {
//         Self::NetworkError{ source: Box::new(value) }
//     }
// }

impl From<assemblyline_markings::errors::Errors> for ElasticErrorInner {
    fn from(value: assemblyline_markings::errors::Errors) -> Self {
        Self::Fatal(value.to_string())
    }
}

impl From<super::collection::InvalidOperationError> for ElasticErrorInner {
    fn from(value: super::collection::InvalidOperationError) -> Self {
        Self::Fatal(value.to_string())
    }
}

impl From<reqwest::Error> for ElasticErrorInner {
    fn from(value: reqwest::Error) -> Self {
        if value.is_connect() || value.is_timeout() {
            return Self::NetworkError{ source: Box::new(value) }
        }

        Self::Fatal(value.to_string())
    }
}

pub type Result<T, E=ElasticError> = std::result::Result<T, E>;

pub trait WithContext {
    fn context(self, message: impl Display) -> Self;
}

impl<T> WithContext for Result<T, ElasticError> {
    fn context(self, message: impl Display) -> Self {
        self.map_err(|err| ElasticErrorInner::Context{ 
            context: message.to_string(), 
            inner: err.inner
        }.into())
    }
}

impl<T> WithContext for Result<T, ElasticErrorInner> {
    fn context(self, message: impl Display) -> Self {
        self.map_err(|err| ElasticErrorInner::Context{
            context: message.to_string(), 
            inner: Box::new(err)
        })
    }
}
