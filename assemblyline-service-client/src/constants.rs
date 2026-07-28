use std::sync::OnceLock;

pub const DEFAULT_SERVICE_API_KEY: &str = "ThisIsARandomAuthKey...ChangeMe!";
pub const DEFAULT_CONTAINER_ID: &str = "dev-service";
pub const DEFAULT_RUNTIME_PREFIX: &str = "service";
pub const DEFAULT_API_HOST: &str = "http://localhost:5003";
pub const DEFAULT_ROOT_CA_PATH: &str = "/etc/assemblyline/ssl/al_root-ca.crt";
pub const SUPPORTED_API: &str = "v1";

pub const TASK_DONE_SUCCESS: &str = "RESULT_FOUND";
pub const TASK_DONE_ERROR: &str = "ERROR_FOUND";

pub const FRAMEWORK_VERSION: u32 = 4;
pub const SYSTEM_VERSION: u32 = 7;
pub const BUILD_MINOR: u32 = 0;

pub const PLACEHOLDER_VERSION_TAG: &str = "$SERVICE_TAG";

pub const MANIFEST_FILE_NAME: &str = "service_manifest.yml";

pub const DEFAULT_SERVICE_ERROR_MESSAGE: &str =
    "The service instance processing this task has terminated unexpectedly.";
pub const UNKNOWN_SERVICE_ERROR_TYPE: &str = "UNKNOWN";
pub const EXCEPTION_SERVICE_ERROR_TYPE: &str = "EXCEPTION";
pub const RECOVERABLE_ERROR_STATUS: &str = "FAIL_RECOVERABLE";
pub const UNRECOVERABLE_ERROR_STATUS: &str = "FAIL_NONRECOVERABLE";


pub const DEFAULT_REQUEST_TASK_TIMEOUT: &str = "10";

pub const TASK_FIFO_NAME: &str = "task_fifo";
pub const DONE_FIFO_NAME: &str = "done_fifo";

pub fn get_version() -> &'static String {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| match std::env::var("SERVICE_TAG") {
        Ok(value) => value,
        _ => format!("{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{BUILD_MINOR}.dev0"),
    })
}
