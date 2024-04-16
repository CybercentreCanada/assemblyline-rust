#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return)]
// #![allow(clippy::needless_return, clippy::while_let_on_iterator, clippy::collapsible_else_if)]


// mod dispatcher;
mod postprocessing;
mod elastic;
mod logging;
mod tls;
mod error;
mod constants;

fn main() {
    println!("Hello, world!");
}
