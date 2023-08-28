pub mod file;
pub mod search;
pub mod submit;

use serde::Serialize;

macro_rules! api_path {
    ($component:expr) => {
        format!("api/v4/{}", $component)
    };
    ($component:expr, $($part:expr),+) => {
        {
            let mut root = format!("api/v4/{}", $component);
            api_path!(add_parts root, $($part),+)
        }
    };
    (add_parts $builder:ident) => (
        $builder
    );
    (add_parts $builder:ident, $part:expr) => {
        {
            $builder.push('/');
            $builder.push_str(&$part);
            $builder
        }
    };
    (add_parts $builder:ident, $part:expr, $($tail:expr),*) => {
        {
            $builder.push('/');
            $builder.push_str(&$part);
            api_path!(add_parts $builder, $($tail),*)
        }
    };
}

pub (crate) use api_path;

#[derive(Serialize)]
struct Params {

}