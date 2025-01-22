use std::io::Write;

use md5::Digest;

pub mod heuristics;
pub mod attack_map;
pub mod tagging;
pub mod odm;
pub mod safelist_client;
pub mod version;
pub mod metrics;

#[derive(Debug, thiserror::Error)]
#[error("Permission error: {0}")]
pub struct PermissionError(pub String);

/// Calculate the sha256 of a buffer
pub fn sha256_data(body: &[u8]) -> String {
    let mut hasher = sha2::Sha256::default();
    hasher.write_all(body).unwrap();
    hex::encode(hasher.finalize().as_slice())
}