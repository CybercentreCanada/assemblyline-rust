#![allow(
    // allow these as they sometimes improve clarity
    clippy::needless_return,
    clippy::collapsible_else_if
)]

pub mod index;
pub mod lucene;
pub mod json;
#[cfg(test)]
mod tests;