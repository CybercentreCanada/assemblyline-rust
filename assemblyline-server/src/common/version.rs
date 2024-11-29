use std::sync::OnceLock;


pub const FRAMEWORK_VERSION: u32 = 4;
pub const SYSTEM_VERSION: u32 = 5;
pub const BUILD_MINOR: u32 = 0;


pub fn get_version() -> &'static String {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| {
        match std::env::var("ASSEMBLYLINE_VERSION") {
            Ok(value) => value,
            _ => format!("{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{BUILD_MINOR}.dev0")
        }
    })
}
