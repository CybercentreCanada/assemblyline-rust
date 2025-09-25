#![allow(clippy::needless_return)]
use std::fmt::Display;

use serde::Deserialize;

pub mod datastore;
pub mod config;
pub mod messages;
pub mod serialize;
pub mod meta;
pub mod types;

pub use meta::ElasticMeta;
pub use types::classification::{disable_global_classification, set_global_classification};

pub const HEXCHARS: [char; 16] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

pub trait Readable: for <'de> Deserialize<'de> {
    fn set_from_archive(&mut self, from_archive: bool);
}

#[derive(Debug)]
pub enum ModelError {
    InvalidSha256(String),
    InvalidMd5(String),
    InvalidSha1(String),
    InvalidSid(String),
    InvalidSSDeep(String),
    ClassificationNotInitialized,
    InvalidClassification(Option<assemblyline_markings::errors::Errors>),
}

impl Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModelError::InvalidSha256(content) => f.write_fmt(format_args!("Invalid value provided for a sha256: {content}")),
            ModelError::InvalidMd5(content) => f.write_fmt(format_args!("Invalid value provided for a md5: {content}")),
            ModelError::InvalidSha1(content) => f.write_fmt(format_args!("Invalid value provided for a sha1: {content}")),
            ModelError::InvalidSid(content) => f.write_fmt(format_args!("Invalid value provided for a sid: {content}")),
            ModelError::ClassificationNotInitialized => f.write_str("The classification engine has not been initialized."),
            ModelError::InvalidClassification(_) => f.write_str("An invalid classification string was provided."),
            ModelError::InvalidSSDeep(content) =>  f.write_fmt(format_args!("Invalid value provided for a ssdeep hash: {content}")),
        }
    }
}

impl From<base62::DecodeError> for ModelError {
    fn from(value: base62::DecodeError) -> Self {
        Self::InvalidSid(value.to_string())
    }
}

impl From<assemblyline_markings::errors::Errors> for ModelError {
    fn from(value: assemblyline_markings::errors::Errors) -> Self {
        Self::InvalidClassification(Some(value))
    }
}

impl std::error::Error for ModelError {}


const WORDS: [&str; 187] = ["The", "Cyber", "Centre", "stays", "on", "the", "cutting", "edge", "of", "technology", "by", 
    "working", "with", "commercial", "vendors", "of", "cyber", "security", "technology", "to", "support", "their", 
    "development", "of", "enhanced", "cyber", "defence", "tools", "To", "do", "this", "our", "experts", "survey", 
    "the", "cyber", "security", "market", "evaluate", "emerging", "technologies", "in", "order", "to", "determine", 
    "their", "potential", "to", "improve", "cyber", "security", "across", "the", "country", "The", "Cyber", "Centre", 
    "supports", "innovation", "by", "collaborating", "with", "all", "levels", "of", "government", "private", "industry", 
    "academia", "to", "examine", "complex", "problems", "in", "cyber", "security", "We", "are", "constantly", 
    "engaging", "partners", "to", "promote", "an", "open", "innovative", "environment", "We", "invite", "partners", 
    "to", "work", "with", "us", "but", "also", "promote", "other", "Government", "of", "Canada", "innovation", 
    "programs", "One", "of", "our", "key", "partnerships", "is", "with", "the", "Government", "of", "Canada", "Build", 
    "in", "Canada", "Innovation", "Program", "BCIP", "The", "BCIP", "helps", "Canadian", "companies", "of", "all", 
    "sizes", "transition", "their", "state", "of", "the", "art", "goods", "services", "from", "the", "laboratory", 
    "to", "the", "marketplace", "For", "certain", "cyber", "security", "innovations", "the", "Cyber", "Centre", 
    "performs", "the", "role", "of", "technical", "authority", "We", "evaluate", "participating", "companies", 
    "new", "technology", "provide", "feedback", "in", "order", "to", "assist", "them", "in", "bringing", "their", 
    "product", "to", "market", "To", "learn", "more", "about", "selling", "testing", "an", "innovation", "visit", 
    "the", "BCIP", "website"];

#[cfg(feature = "rand")]
pub fn random_word<R: rand::Rng + ?Sized>(prng: &mut R) -> String {
    WORDS[prng.random_range(0..WORDS.len())].to_string()
}

#[cfg(feature = "rand")]
pub fn random_words<R: rand::Rng + ?Sized>(prng: &mut R, count: usize) -> Vec<String> {
    let mut output = vec![];
    while output.len() < count {
        output.push(WORDS[prng.random_range(0..WORDS.len())].to_string())
    }
    output
}

#[cfg(feature = "rand")]
pub fn random_hex<R: rand::prelude::Rng + ?Sized>(rng: &mut R, size: usize) -> String {
    let mut buffer = String::with_capacity(size);
    for _ in 0..size {
        let index = rng.random_range(0..HEXCHARS.len());
        buffer.push(HEXCHARS[index]);
    }
    buffer
}

#[cfg(test)]
mod test {
    use rand::Rng;

    use crate::types::{SSDeepHash, Sha1, Sha256, MD5};
    
    #[test]
    fn random_ssdeep() {
        let mut prng = rand::rng();
        for _ in 0..100 {
            let hash: SSDeepHash = prng.random();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }

    #[test]
    fn random_sha256() {
        let mut prng = rand::rng();
        for _ in 0..100 {
            let hash: Sha256 = prng.random();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }

    #[test]
    fn random_sha1() {
        let mut prng = rand::rng();
        for _ in 0..100 {
            let hash: Sha1 = prng.random();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }

    #[test]
    fn random_md5() {
        let mut prng = rand::rng();
        for _ in 0..100 {
            let hash: MD5 = prng.random();
            assert_eq!(hash, hash.to_string().parse().unwrap());
        }
    }
}

