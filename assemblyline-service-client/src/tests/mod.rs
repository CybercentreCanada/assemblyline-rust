#[cfg(test)]
pub(crate) mod mock_service;
#[cfg(test)]
pub mod test_service_client;

#[cfg(test)]
pub mod test_task_fetcher;

#[cfg(test)]
pub mod test_task_uploader;

#[cfg(test)]
pub(crate) mod test_connection;

#[cfg(test)]
pub(crate) mod mock_service_api;

use assemblyline_models::{
    messages::service_api::{self, result::Result as ServiceResult},
    types::{ClassificationString, ServiceName, Sha256},
};
use md5::Digest;
use nom::AsBytes;

/// Calculate the sha256 of a buffer
pub fn sha256_data(body: &[u8]) -> String {
    let mut hasher = sha2::Sha256::default();
    std::io::Write::write_all(&mut hasher, body).unwrap();
    hex::encode(hasher.finalize())
}

pub fn test_sha_file() -> (String, Vec<u8>) {
    let mut data: Vec<u8> = "abcdefg".to_string().into_bytes();

    let sha_hash = sha256_data(data.as_bytes());
    (sha_hash, data)
}

pub fn create_random_service_result(
    sha256: Sha256,
    service_name: ServiceName,
    service_version: String,
    service_tool_version: Option<String>,
) -> ServiceResult {
    let response = service_api::result::ResponseBody {
        milestones: Default::default(),
        service_version,
        service_name,
        service_tool_version,
        supplementary: Default::default(),
        extracted: Default::default(),
        service_context: Default::default(),
        service_debug_info: Default::default(),
    };

    ServiceResult {
        archive_ts: Default::default(),
        classification: ClassificationString::default_unrestricted(),
        created: Default::default(),
        expiry_ts: Default::default(),
        response: response,
        result: Default::default(),
        sha256: sha256,
        result_type: Default::default(),
        size: Default::default(),
        drop_file: Default::default(),
        partial: Default::default(),
        temp_submission_data: Default::default(),
    }
}

fn init() {
    // let _ = env_logger::builder()
    //     .target(env_logger::Target::Stdout)
    //     .filter_level(log::LevelFilter::Info)
    //     .is_test(true)
    //     .try_init();

    assemblyline_models::disable_global_classification();
}
