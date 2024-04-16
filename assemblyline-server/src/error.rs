use poem::http::StatusCode;



#[derive(Debug)]
pub enum Error {
    SearchException(String),
    ArchiveDisabled(String),
    VersionConflictException(String),
    DataStoreException(&'static str),
    /// An error with the runtime or execution environment itself
    RuntimeError(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl std::error::Error for Error {

}

impl poem::error::ResponseError for Error {
    fn status(&self) -> poem::http::StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

impl From<openssl::error::ErrorStack> for Error {
    fn from(value: openssl::error::ErrorStack) -> Self {
        todo!()
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        todo!()
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        todo!()
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        todo!()
    }
}

impl From<url::ParseError> for Error {
    fn from(value: url::ParseError) -> Self {
        todo!()
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        todo!()
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::RuntimeError(value.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(value: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::RuntimeError(value.to_string())
    }
}

impl From<elasticsearch::Error> for Error {
    fn from(value: elasticsearch::Error) -> Self {
        todo!()
    }
}

impl From<elasticsearch::http::transport::BuildError> for Error {
    fn from(value: elasticsearch::http::transport::BuildError) -> Self {
        todo!()
    }
}

impl From<()> for Error {
    fn from(value: ()) -> Self {
        todo!()
    }
}

pub type Result<T> = std::result::Result<T, Error>;