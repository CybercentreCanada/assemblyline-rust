pub mod badlist;
pub mod file;
pub mod health;
pub mod safelist;
pub mod service;
pub mod task;

macro_rules! require_header {
    ($headers:ident, $name:expr) => {{
        use crate::service_api::helpers::make_empty_api_error;
        match $headers.get($name) {
            Some(value) => match value.to_str() {
                Ok(value) => value,
                Err(_) => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, &format!("Could not parse value of header: {}", $name)))
            },
            None => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, &format!("Missing required header: {}", $name)))
        }
    }};

    ($headers:ident, $name:expr, $default:expr) => {{
        use crate::service_api::helpers::make_empty_api_error;
        match $headers.get($name) {
            Some(value) => match value.to_str() {
                Ok(value) => value,
                Err(_) => return Err(make_empty_api_error(StatusCode::BAD_REQUEST, &format!("Could not parse value of header: {}", $name)))
            },
            None => $default
        }
    }}
}

pub (crate) use require_header;

