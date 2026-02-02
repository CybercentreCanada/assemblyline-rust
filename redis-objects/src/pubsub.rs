//! objects for interacting with pubsubs
use std::{marker::PhantomData, sync::Arc};

use futures::StreamExt;
use log::error;
use redis::{AsyncCommands, Msg};
use serde::Serialize;
use tokio::sync::mpsc;
use serde::de::DeserializeOwned;
use tracing::instrument;

use crate::{retry_call, ErrorTypes, RedisObjects};

/// Struct to setup a stream reading from a pubsub
/// The content of the pubsub is not processed
pub struct ListenerBuilder {
    store: Arc<RedisObjects>,
    channels: Vec<String>,
    patterns: Vec<String>,
}

impl ListenerBuilder {

    pub (crate) fn new(store: Arc<RedisObjects>) -> Self {
        ListenerBuilder { store, channels: vec![], patterns: vec![] }
    }

    /// Subscribe to a fixed channel
    pub fn subscribe(mut self, channel: String) -> Self {
        self.channels.push(channel); self
    }

    /// Subscribe to all channels matching this pattern
    pub fn psubscribe(mut self, channel: String) -> Self {
        self.patterns.push(channel); self
    }

    /// Launch the task reading from the pubsub
    pub async fn listen(self) -> mpsc::Receiver<Option<Msg>> {

        let (message_sender, message_receiver) = mpsc::channel(64);
        let started = Arc::new(tokio::sync::Notify::new());
        let notify_started = started.clone();

        tokio::spawn(async move {
            const STARTING_EXPONENT: f64 = -8.0;
            let mut exponent = STARTING_EXPONENT;
            let maximum = 3.0;

            'reconnect: loop {
                if exponent > STARTING_EXPONENT {
                    log::warn!("No connection to Redis, reconnecting...");
                    tokio::time::sleep(tokio::time::Duration::from_secs_f64(2f64.powf(exponent))).await;
                }
                exponent = (exponent + 1.0).min(maximum);

                let mut pubsub = match self.store.client.get_async_pubsub().await {
                    Ok(connection) => connection,
                    Err(connection_error) => {
                        error!("Error connecting to pubsub: {connection_error}");
                        continue 'reconnect;
                    }
                };

                for channel in &self.channels {
                    if pubsub.subscribe(self.store.pubsub_prefix.clone() + channel).await.is_err() {
                        continue 'reconnect;
                    }
                }
                for pattern in &self.patterns {
                    if pubsub.psubscribe(self.store.pubsub_prefix.clone() + pattern).await.is_err() {
                        continue 'reconnect;
                    }
                }
                notify_started.notify_one();

                let mut stream = pubsub.on_message();
                while let Some(message) = stream.next().await {
                    // if the send fails it means the other end of the channel has dropped
                    // and we can stop listening
                    if message_sender.send(Some(message)).await.is_err() {
                        break 'reconnect
                    }
                    exponent = STARTING_EXPONENT + 1.0;
                }
                if message_sender.send(None).await.is_err() {
                    break 'reconnect
                }
            }
        });

        started.notified().await;
        message_receiver
    }
}



/// Struct to setup a stream reading from a pubsub
/// The content of the Pubsub must be a JSON serialized object
pub struct JsonListenerBuilder<Message: DeserializeOwned> {
    store: Arc<RedisObjects>,
    channels: Vec<String>,
    patterns: Vec<String>,
    _data: PhantomData<Message>
}

impl<Message: DeserializeOwned + Send + 'static> JsonListenerBuilder<Message> {

    pub (crate) fn new(store: Arc<RedisObjects>) -> Self {
        JsonListenerBuilder { store, channels: vec![], patterns: vec![], _data: Default::default() }
    }

    /// Subscribe to a fixed channel
    pub fn subscribe(mut self, channel: String) -> Self {
        self.channels.push(channel); self
    }

    /// Subscribe to all channels matching this pattern
    pub fn psubscribe(mut self, channel: String) -> Self {
        self.patterns.push(channel); self
    }

    /// Launch the task reading from the pubsub
    pub async fn listen(self) -> mpsc::Receiver<Option<Message>> {

        let (parsed_sender, parsed_receiver) = mpsc::channel(2);

        let mut message_reciever = ListenerBuilder {
            store: self.store,
            channels: self.channels,
            patterns: self.patterns
        }.listen().await;

        tokio::spawn(async move {
            while let Some(message) = message_reciever.recv().await {
                let message = match message {
                    Some(message) => message,
                    None => {
                        if parsed_sender.send(None).await.is_err() {
                            break
                        }
                        continue
                    }
                };

                let result = match serde_json::from_slice(message.get_payload_bytes()) {
                    Ok(message) => parsed_sender.send(Some(message)).await,
                    Err(err) => {
                        error!("Could not process pubsub message: {err}");
                        parsed_sender.send(None).await
                    }
                };

                if result.is_err() {
                    break
                }
            }
        });

        parsed_receiver
    }
}

/// Hold connection and channel name for publishing to a pubsub
pub struct Publisher {
    store: Arc<RedisObjects>,
    channel: String,
}

impl std::fmt::Debug for Publisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Publisher").field("store", &self.store).field("channel", &self.channel).finish()
    }
}

impl Publisher {
    pub (crate) fn new(store: Arc<RedisObjects>, channel: String) -> Self {
        Publisher { store, channel }
    }

    /// Publish a message in a serializable type
    #[instrument(skip(data))]
    pub async fn publish<T: Serialize>(&self, data: &T) -> Result<(), ErrorTypes> {
        self.publish_data(&serde_json::to_vec(data)?).await
    }

    /// Publish raw data as a pubsub message
    #[instrument(skip(data))]
    pub async fn publish_data(&self, data: &[u8]) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, publish, self.store.pubsub_prefix.clone() + &self.channel, data)
    }
}