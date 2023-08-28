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

use connection::Connection;
use modules::file::File;
use modules::search::Search;
use modules::submit::Submit;
pub use types::{Authentication, Error, Sha256, JsonMap};


/// A client to communicate with the Assemblyline API
pub struct Client {
    // Connection handler
    // _connection: Arc<Connection>,

    // self.alert = Alert(self._connection)
    // self.bundle = Bundle(self._connection)
    // self.error = Error(self._connection)
    /// file specific API endpoints
    pub file: File,
    // self.hash_search = HashSearch(self._connection)
    // self.help = Help(self._connection)
    // self.heuristics = Heuristics(self._connection)
    // self.ingest = Ingest(self._connection)
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
    pub submit: Submit,
    // self.system = System(self._connection)
    // self.user = User(self._connection)
    // self.workflow = Workflow(self._connection)
}

impl Client {
    /// Connect to an assemblyline system
    pub async fn connect(server: String, auth: Authentication) -> Result<Self, Error> {
        let connection = Arc::new(Connection::connect(server, auth, None, true, Default::default(), None, None).await?);
        Ok(Self {
            file: File::new(connection.clone()),
            search: Search::new(connection.clone()),
            submit: Submit::new(connection)
            // _connection,
        })
    }
}

