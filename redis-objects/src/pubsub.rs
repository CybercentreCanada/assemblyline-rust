use std::{marker::PhantomData, sync::Arc};

use futures::StreamExt;
use log::error;
use redis::{AsyncCommands, Msg, RedisError};
use serde::Serialize;
use tokio::sync::mpsc;
use serde::de::DeserializeOwned;

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
    pub fn listen(self) -> mpsc::Receiver<Msg> {

        let (message_sender, message_receiver) = mpsc::channel(64);

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

                let connection = match self.store.client.get_async_connection().await {
                    Ok(connection) => connection,
                    Err(connection_error) => {
                        error!("Error connecting to pubsub: {connection_error}");
                        continue 'reconnect;
                    }
                };

                let mut pubsub = connection.into_pubsub();
                for channel in &self.channels {
                    if pubsub.subscribe(channel).await.is_err() {
                        continue 'reconnect;
                    }
                }
                for pattern in &self.patterns {
                    if pubsub.psubscribe(pattern).await.is_err() {
                        continue 'reconnect;
                    }
                }

                let mut stream = pubsub.on_message();
                while let Some(message) = stream.next().await {
                    // if the send fails it means the other end of the channel has dropped 
                    // and we can stop listening 
                    if message_sender.send(message).await.is_err() {
                        break 'reconnect
                    }
                    exponent = STARTING_EXPONENT + 1.0;
                }
            }
        });

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
    pub fn listen(self) -> mpsc::Receiver<Result<Message, ErrorTypes>> {

        let (parsed_sender, parsed_receiver) = mpsc::channel(2);

        let mut message_reciever = ListenerBuilder {
            store: self.store,
            channels: self.channels,
            patterns: self.patterns
        }.listen();

        tokio::spawn(async move {
            while let Some(message) = message_reciever.recv().await {
                let result = match serde_json::from_slice(message.get_payload_bytes()) {
                    Ok(message) => parsed_sender.send(Ok(message)).await,
                    Err(err) => parsed_sender.send(Err(err.into())).await,
                };

                if result.is_err() {
                    break
                }
            }
        });

        parsed_receiver
    }
}


pub struct Publisher {
    store: Arc<RedisObjects>,
    channel: String,
}

impl Publisher {
    pub (crate) fn new(store: Arc<RedisObjects>, channel: String) -> Self {
        Publisher { store, channel }
    }

    pub async fn publish<T: Serialize>(&self, data: &T) -> Result<(), ErrorTypes> {
        self.publish_data(&serde_json::to_vec(data)?).await
    }

    pub async fn publish_data(&self, data: &[u8]) -> Result<(), ErrorTypes> {
        retry_call!(self.store.pool, publish, &self.channel, data)
    }
}