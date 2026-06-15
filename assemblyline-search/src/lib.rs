#![allow(
    // allow these as they sometimes improve clarity
    clippy::needless_return,
    clippy::collapsible_if,
    clippy::collapsible_else_if
)]

pub mod tables;
pub mod lucene;
pub mod json;
pub mod yugabyte;
pub mod search;
pub mod tidb;

#[cfg(test)]
mod tests;