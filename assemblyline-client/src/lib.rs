#![warn(missing_docs, non_ascii_idents, trivial_numeric_casts,
    unused_crate_dependencies, noop_method_call, single_use_lifetimes, trivial_casts,
    unused_lifetimes, nonstandard_style, variant_size_differences)]
#![deny(keyword_idents)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::needless_return)]
// #![allow(clippy::needless_return, clippy::while_let_on_iterator, clippy::collapsible_else_if)]

//! A library to help access the Assemblyline API from rust

mod types;
mod connection;
mod modules;

use std::sync::Arc;

use modules::file::File;
use modules::help::Help;
use modules::alert::Alert;
use modules::bundle::Bundle;
use modules::error::Error;
use modules::search::Search;
use modules::ingest::Ingest;
use modules::submit::Submit;
pub use types::{Authentication, JsonMap};
pub use connection::{Connection, TLSSettings};


/// A client to communicate with the Assemblyline API
pub struct Client {
    // Connection handler
    // _connection: Arc<Connection>,

    /// alert specific API endpoints
    pub alert: Alert,
    /// bundle specific API endpoints
    pub bundle: Bundle,
    /// error specific API endpoints
    pub error: Error,
    /// file specific API endpoints
    pub file: File,
    // self.hash_search = HashSearch(self._connection)
    /// Help API endpoints
    pub help: Help,
    // self.heuristics = Heuristics(self._connection)
    /// Ingest API endpoints
    pub ingest: Ingest,
    // self.live = Live(self._connection)
    // self.ontology = Ontology(self._connection)
    // self.replay = Replay(self._connection)
    // self.result = Result(self._connection)
    // self.safelist = Safelist(self._connection)
    /// search API endpoints
    pub search: Search,
    // self.service = Service(self._connection)
    // self.signature = Signature(self._connection)
    // self.socketio = SocketIO(self._connection)
    // self.submission = Submission(self._connection)
    /// File submission API endpoints
    pub submit: Submit,
    // self.system = System(self._connection)
    // self.user = User(self._connection)
    // self.workflow = Workflow(self._connection)
}

impl Client {
    /// Connect to an assemblyline system
    pub async fn connect(server: String, auth: Authentication) -> Result<Self, types::Error> {
        let connection = Arc::new(Connection::connect(server, auth, None, connection::TLSSettings::Native, Default::default(), None).await?);
        Ok(Self {
            alert: Alert::new(connection.clone()),
            bundle: Bundle::new(connection.clone()),
            error: Error::new(connection.clone()),
            file: File::new(connection.clone()),
            help: Help::new(connection.clone()),
            ingest: Ingest::new(connection.clone()),
            search: Search::new(connection.clone()),
            submit: Submit::new(connection)
            // _connection,
        })
    }

    /// Connect to an assemblyline system
    pub async fn from_connection(connection: Arc<Connection>) -> Result<Self, types::Error> {
        // let connection = Arc::new(Connection::connect(server, auth, None, true, Default::default(), Some(cert), None).await?);
        Ok(Self {
            alert: Alert::new(connection.clone()),
            bundle: Bundle::new(connection.clone()),
            error: Error::new(connection.clone()),
            file: File::new(connection.clone()),
            help: Help::new(connection.clone()),
            ingest: Ingest::new(connection.clone()),
            search: Search::new(connection.clone()),
            submit: Submit::new(connection)
            // _connection,
        })
    }
}


#[cfg(test)]
mod tests {

    use assemblyline_models::datastore::submission::{SubmissionParams, SubmissionState, ServiceSelection};
    use assemblyline_models::ClassificationString;
    use rand::Rng;

    use crate::{Authentication, Client};

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    pub (crate) async fn prepare_client() -> Client {
        init();
        let url = std::env::var("ASSEMBLYLINE_URL").unwrap();
        let username = std::env::var("ASSEMBLYLINE_USER").unwrap();
        let key = std::env::var("ASSEMBLYLINE_KEY").unwrap();
        Client::connect(url, Authentication::ApiKey { username, key }).await.unwrap()
    }

    fn random_body() -> Vec<u8> {
        let mut out = vec![];
        let mut prng = rand::rng();
        let length = 128 + prng.random_range(0..256);
        while out.len() < length {
            out.push(prng.random());
        }
        out
    }

    #[tokio::test]
    async fn submit_content() {
        let client = prepare_client().await;

        let result = client.submit.single()
            .metadata_item("testbatch".to_owned(), "0".to_owned())
            .params(SubmissionParams{ ttl: 1, ..SubmissionParams::new(ClassificationString::try_unrestricted().unwrap())})
            .fname("test-file".to_owned())
            .content(random_body()).await.unwrap();

        assert_eq!(result.state, SubmissionState::Submitted);
    }


    #[tokio::test]
    async fn search_single_page() {
        let client = prepare_client().await;
        let batch: u64 = rand::rng().random();
        let batch: String = batch.to_string();

        let _result = client.ingest.single()
            .metadata_item("testbatch".to_owned(), batch.clone())
            .notification_queue(batch.clone())
            .params(SubmissionParams{ priority: 300, ttl: 1, services: ServiceSelection{ selected: vec!["Characterize".to_owned()], ..Default::default()}, ..SubmissionParams::new(ClassificationString::try_unrestricted().unwrap())})
            .fname("test-file".to_owned())
            .content(random_body()).await.unwrap();

        for _ in 0..100 {
            match client.ingest.get_message(&batch).await.unwrap() {
                Some(message) => {
                    assert_eq!(message.submission.metadata.get("testbatch").unwrap(), &batch);
                    break;
                },
                None => { tokio::time::sleep(tokio::time::Duration::from_secs(1)).await; },
            }
        }

        for _ in 0..10 {
            let result = client.search.submission(format!("metadata.testbatch: {batch}"))
                .search().await.unwrap();

            if result.items.len() == 1 {
                return
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
        panic!()
    }



}