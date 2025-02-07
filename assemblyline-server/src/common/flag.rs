use std::sync::Arc;

use anyhow::Result;
use log::{error, info};


pub struct Flag {
    condition: tokio::sync::watch::Sender<bool>,
}

impl Flag {
    pub fn new(value: bool) -> Self {
        Flag { 
            condition: tokio::sync::watch::channel(value).0,
        }
    }

    pub fn read(&self) -> bool {
        *self.condition.borrow()
    }

    pub fn set(&self, value: bool) {
        self.condition.send_modify(|current| *current = value);
    }

    pub async fn wait_for(&self, value: bool) {
        let mut watcher = self.condition.subscribe();
        while *watcher.borrow_and_update() != value {
            _ = watcher.changed().await;
        }
    }

    pub fn install_terminate_handler(self: &Arc<Self>, value: bool) -> Result<()> {
        use tokio::signal::{self, unix::SignalKind};

        let flag = self.clone();
        tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => {
                    info!("Termination signal called");
                    flag.set(value);
                },
                Err(err) => error!("Error installing signal handler: {err}"),
            }
        });

        let flag = self.clone();
        let mut stream = signal::unix::signal(SignalKind::terminate())?;
        tokio::spawn(async move {
            stream.recv().await;
            info!("Termination signal called");
            flag.set(value);
        });
        Ok(())
    }
}
