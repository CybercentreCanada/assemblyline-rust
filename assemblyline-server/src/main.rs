//! Entrypoint to start any assemblyline server modules

#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return)]
// #![allow(clippy::needless_return, clippy::while_let_on_iterator, clippy::collapsible_else_if)]

use std::sync::Arc;

use assemblyline_models::config::Config;
use elastic::Elastic;
use redis_objects::{Queue, RedisObjects};

mod ingester;
mod submission_common;
mod dispatcher_common;
// mod dispatcher;
// mod postprocessing;
mod elastic;
mod logging;
mod tls;
mod error;
mod constants;

const SUBMISSION_QUEUE: &str = "dispatch-submission-queue";

struct Core {
    pub config: Config,
    pub datastore: Arc<Elastic>,
    pub redis_persistant: Arc<RedisObjects>,
    pub redis_volatile: Arc<RedisObjects>,
    pub redis_metrics: Arc<RedisObjects>,

    // entrypoint to the dispatcher
    pub dispatch_submission_queue: Queue<assemblyline_models::messages::SubmissionDispatchMessage>
}

#[tokio::main]
async fn main() {
    // Load configuration
    todo!();

    // configure logging
    todo!();

    // connect to redis one
    let redis_persistant = RedisObjects::open_host(&config.core.redis.persistent.host, config.core.redis.persistent.port)?;

    // connect to redis two
    let redis_volatile = RedisObjects::open_host(&config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port)?;

    // connect to redis two
    let redis_metrics = RedisObjects::open_host(&config.metrics.redis.host, config.metrics.redis.port)?;

    // connect to elastic
    let datastore = Elastic::connect(&config.datastore.hosts[0], false).await?;

    // Initialize connections to resources that everything uses
    let core = Core {
        dispatch_submission_queue: redis_volatile.queue(SUBMISSION_QUEUE.to_owned(), None),
        config,
        datastore,
        redis_persistant,
        redis_volatile,
        redis_metrics,
    };

    // pick the module to launch
    todo!();
}

const ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_secs(10);

// spawn a task
macro_rules! spawn_retry_forever {
    ($source:ident, $name:expr, $method_name:ident) => {
        tokio::spawn(async move {
            while let Err(err) = $source.$method_name().await {
                error!("Error in {}: {err}", $name);
                $source.sleep(crate::ERROR_BACKOFF).await;
            }
        })
    };
    ($container:ident, $source:ident, $name:expr, $method_name:ident) => {
        $container.push(($name.to_owned(), spawn_retry_forever!($source, $name, $method_name)))
    }
}
pub(crate) use spawn_retry_forever;


async fn await_tasks(tasks: Vec<(String, tokio::task::JoinHandle<()>)>) {
    todo!()
}
