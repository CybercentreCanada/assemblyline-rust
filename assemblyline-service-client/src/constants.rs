//! Constants and default values in this module

use std::sync::OnceLock;

/// Default value for service api key for communication with the service server
pub const DEFAULT_SERVICE_API_KEY: &str = "ThisIsARandomAuthKey...ChangeMe!";
/// Default container ID
pub const DEFAULT_CONTAINER_ID: &str = "dev-service";
/// Default prefix on runtime files created by service client
pub const DEFAULT_RUNTIME_PREFIX: &str = "service";
/// Default service server host address
pub const DEFAULT_API_HOST: &str = "http://localhost:5003";
/// Default ca cert file path
pub const DEFAULT_ROOT_CA_PATH: &str = "/etc/assemblyline/ssl/al_root-ca.crt";
/// Default assemblyline configuration file path
pub const DEFAULT_ASSEMBLYLINE_CONFIG_PATH: &str = "/etc/assemblyline/config.yml";
/// Task fifo pipe name
pub const TASK_FIFO_NAME: &str = "task_fifo";
/// Done fifo pipe name
pub const DONE_FIFO_NAME: &str = "done_fifo";
/// Default request time out for communication with service server
pub const DEFAULT_REQUEST_TASK_TIMEOUT: &str = "30";
/// Placeholder text in service manifest
pub const PLACEHOLDER_VERSION_TAG: &str = "$SERVICE_TAG";

/// Service task done message type success
pub const TASK_DONE_SUCCESS: &str = "RESULT_FOUND";
/// Service task done message type error
pub const TASK_DONE_ERROR: &str = "ERROR_FOUND";

/// Default error message
pub const DEFAULT_SERVICE_ERROR_MESSAGE: &str = "The service instance processing this task has terminated unexpectedly.";
/// Service task error type unknown
pub const UNKNOWN_SERVICE_ERROR_TYPE: &str = "UNKNOWN";
/// Service task error type exception
pub const EXCEPTION_SERVICE_ERROR_TYPE: &str = "EXCEPTION";
/// Task status recoverable error
pub const RECOVERABLE_ERROR_STATUS: &str = "FAIL_RECOVERABLE";
/// Task status unrecoverable error
pub const UNRECOVERABLE_ERROR_STATUS: &str = "FAIL_NONRECOVERABLE";

/// Assemblyline framework version
pub const FRAMEWORK_VERSION: u32 = 4;
/// Assemblyline system version
pub const SYSTEM_VERSION: u32 = 7;
/// Assemblyline minor build version
pub const BUILD_MINOR: u32 = 0;

/// Get Assemblyline release version
pub fn get_version() -> &'static String {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| match std::env::var("SERVICE_TAG") {
        Ok(value) => value,
        _ => format!("{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{BUILD_MINOR}.dev0"),
    })
}
