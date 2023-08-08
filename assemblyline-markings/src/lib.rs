#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
#![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return, clippy::while_let_on_iterator, clippy::collapsible_else_if)]

//! Library for manipulating and comparing classification strings based on configuration from Assemblyline.

pub mod errors;
pub mod config;
pub mod classification;