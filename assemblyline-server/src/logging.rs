//!
//! Utilities for logging.
//!
use std::time::Instant;

use log::{debug, error};
use poem::{Endpoint, Middleware, Request};

/// Middleware for poem http server to add query logging
///
/// Successful queries are logged at the debug level. Errors at the error level.
/// All logs include the time elapsed processing the request.
pub struct LoggerMiddleware;

impl<E: Endpoint> Middleware<E> for LoggerMiddleware {
    type Output = LoggerMiddlewareImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        LoggerMiddlewareImpl { ep }
    }
}

/// Endpoint wrapper that implements the details of `LoggerMiddleware`
pub struct LoggerMiddlewareImpl<E> {
    /// Inner endpoint wrapped by this object
    ep: E,
}


#[poem::async_trait]
impl<E: Endpoint> Endpoint for LoggerMiddlewareImpl<E> {
    type Output = E::Output;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        let start = Instant::now();
        let uri = req.uri().clone();
        match self.ep.call(req).await {
            Ok(resp) => {
                debug!("request for {uri} handled ({} ms)", start.elapsed().as_millis());
                Ok(resp)
            },
            Err(err) => {
                error!("error handling {uri} ({} ms) {err}", start.elapsed().as_millis());
                Err(err)
            },
        }
    }
}