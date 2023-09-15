use serde_with::{SerializeDisplay, DeserializeFromStr};

use crate::types::Error;

pub mod submission;
pub mod error;
pub mod alert;
pub mod workflow;
pub mod result;
pub mod tagging;


/// Uppercase String
#[derive(Debug, SerializeDisplay, DeserializeFromStr)]
pub struct UpperString {
    value: String
}

impl std::fmt::Display for UpperString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value)
    }
}

impl std::ops::Deref for UpperString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::str::FromStr for UpperString {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let value = s.trim().to_uppercase();
        return Ok(UpperString{ value })
    }
}


/// MD5 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr)]
pub struct MD5 {
    hex: String
}

impl std::fmt::Display for MD5 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex)
    }
}

impl std::ops::Deref for MD5 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.hex
    }
}

impl std::str::FromStr for MD5 {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 32 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(Error::MalformedResponse)
        }
        return Ok(MD5{ hex })
    }
}


/// Sha1 hash of a file
#[derive(Debug, SerializeDisplay, DeserializeFromStr)]
pub struct Sha1 {
    hex: String
}

impl std::fmt::Display for Sha1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.hex)
    }
}

impl std::ops::Deref for Sha1 {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.hex
    }
}

impl std::str::FromStr for Sha1 {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let hex = s.trim().to_ascii_lowercase();
        if hex.len() != 40 || !hex.chars().all(|c|c.is_ascii_hexdigit()) {
            return Err(Error::MalformedResponse)
        }
        return Ok(Sha1{ hex })
    }
}

/// Unvalidated uuid type
pub type Uuid = String;

/// Unvalidated domain type
pub type Domain = String;

/// Unvalidated ip type
pub type IP = String;

/// Unvalidated uri type
pub type Uri = String;

/// Unvalidated classification type
pub type Classification = String;

/// Unvalidated platform type
pub type Platform = String;

/// Unvalidated processor type
pub type Processor = String;

/// Unvalidated ssdeep type
pub type SSDeepHash = String;

/// Unvalidated phone number type
pub type PhoneNumber = String;

/// Unvalidated MAC type
pub type Mac = String;

/// Unvalidated UNCPath type
pub type UNCPath = String;

/// Unvalidated UriPath type
pub type UriPath = String;

/// Unvalidated Email type
pub type Email = String;
