#![warn(
    missing_docs,
    non_ascii_idents,
    trivial_numeric_casts,
    unused_crate_dependencies,
    noop_method_call,
    single_use_lifetimes,
    trivial_casts,
    unused_lifetimes,
    nonstandard_style,
    variant_size_differences
)]
#![deny(
    keyword_idents,
    clippy::disallowed_methods
)]
#![allow(
    // allow these as they sometimes improve clarity
    clippy::needless_return,
    clippy::collapsible_else_if
)]


pub mod connection;
pub mod constants;
pub mod logging;
pub mod types;

#[cfg(test)]
pub(crate) mod tests;
