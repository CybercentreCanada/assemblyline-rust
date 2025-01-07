//! Errors potentially used by multiple transports

/// A generic error raised when connection attempt limits are reached
#[derive(thiserror::Error, Debug)]
#[error("Couldn't reach the requested endpoint inside retry limit")]
pub struct ConnectionError;

/// An error produced when a transport is in read only mode and a write operation is attempted
#[derive(thiserror::Error, Debug)]
#[error("Attempted a write operation on a read only endpoint")]
pub struct ReadOnlyError;

