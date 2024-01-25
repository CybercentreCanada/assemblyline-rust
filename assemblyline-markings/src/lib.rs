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

use std::sync::{Arc, Mutex};


/// Mutex to hold a default parser the loaded system uses. 
/// The classification engine is often treated as an aspect of the execution environment 
/// that should be globally accessable. Rather than reloading a configuration file repeatedly or having 
/// several different modules all track the same parser redundantly we will create a common mutex here.
pub static DEFAULT_PARSER: Mutex<Option<Arc<classification::ClassificationParser>>> = Mutex::new(None);

/// Load the assigned default parser
pub fn get_default() -> Option<Arc<classification::ClassificationParser>> {
    DEFAULT_PARSER.lock().unwrap().clone()
}

/// Set the default parser 
pub fn set_default(parser: Arc<classification::ClassificationParser>)  {
    *DEFAULT_PARSER.lock().unwrap() = Some(parser);
}