//! Error types and error handling functions

/// An enumeration of all errors that can occur in the library
#[derive(Debug)]
pub enum Errors {
    /// An invalid classification string was provided
    InvalidClassification(String),
    /// An invalid classification config was provided
    InvalidDefinition(String),
    /// A name given in a classification definition is an empty string
    ClassificationNameEmpty
}

impl std::fmt::Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Errors::InvalidClassification(message) => f.write_fmt(format_args!("An invalid classification string was provided: {message}")),
            Errors::InvalidDefinition(message) => f.write_fmt(format_args!("An invalid classification config was provided: {message}")),
            Errors::ClassificationNameEmpty => f.write_str("A name given in the classification definition is an empty string"),
        }
    }
}

// make the Errors struct into an error type
impl std::error::Error for Errors {}

// Capture certain errors with a fairly direct conversion to InvalidDefinition
// impl From<serde_json::Error> for Errors {
//     fn from(value: serde_json::Error) -> Self {
//         Self::InvalidDefinition(value.to_string())
//     }
// }
impl From<serde_yaml::Error> for Errors {
    fn from(value: serde_yaml::Error) -> Self {
        Self::InvalidDefinition(value.to_string())
    }
}
impl From<std::io::Error> for Errors {
    fn from(value: std::io::Error) -> Self {
        Self::InvalidDefinition(value.to_string())
    }
}