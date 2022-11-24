mod config;
mod proxy;

use crate::config::get_app_config;
use crate::proxy::Proxy;
use std::error::Error;
use tokio::signal::ctrl_c;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::warn;
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
pub struct Shutdown {
    sender: Sender<()>,
}

impl Shutdown {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1);

        Self { sender }
    }

    pub fn subscribe(&self) -> Receiver<()> {
        self.sender.subscribe()
    }

    pub fn shutdown(&self) {
        let _ = self.sender.send(());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .finish(),
    )
    .expect("setting default subscriber failed");

    let app_config = get_app_config().expect("invalid config");

    let shutdown = Shutdown::new();

    let shutdown_signal = shutdown.clone();

    let mut proxies = vec![];
    for app in app_config.apps {
        let proxy = Proxy::from_app(app).expect("failed to create proxy for app");
        proxy
            .run(shutdown.clone())
            .await
            .expect("start listening ports");
        proxies.push(proxy);
    }

    ctrl_c().await.expect("couldn't listen to ctrl c event!");

    warn!("received graceful shutdown request, signaling all relays to stop...");

    shutdown_signal.shutdown();

    Ok(())
}
