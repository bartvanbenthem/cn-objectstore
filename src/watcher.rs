use std::collections::HashSet;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::client::ObjectStoreClient;

/// Watches an object store prefix for changes by periodically listing its contents
/// and comparing results against the previous poll.
///
/// Sends a `()` signal on `tx` whenever a change is detected (objects added or removed).
///
/// # Example
/// ```no_run
/// use kube_objstore::{ObjectStoreClient, ObjectStoreConfig, ObjectStoreWatcher};
/// use tokio::sync::mpsc;
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let config = ObjectStoreConfig::from_env()?;
///     let client = ObjectStoreClient::new(config).await?;
///
///     let (tx, mut rx) = mpsc::channel(8);
///     let _watcher = ObjectStoreWatcher::new(client, "my-prefix/")
///         .with_interval(Duration::from_secs(15))
///         .spawn(tx);
///
///     while let Some(()) = rx.recv().await {
///         println!("Change detected!");
///     }
///     Ok(())
/// }
/// ```
pub struct ObjectStoreWatcher {
    client: ObjectStoreClient,
    prefix: String,
    interval: Duration,
}

impl ObjectStoreWatcher {
    /// Create a new watcher for the given client and prefix.
    /// Default poll interval is 30 s.
    pub fn new(client: ObjectStoreClient, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
            interval: Duration::from_secs(30),
        }
    }

    /// Override the polling interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Start the watcher as a background Tokio task.
    ///
    /// Returns a [`JoinHandle`] — dropping it does **not** abort the task.
    /// The task stops automatically when `tx` is closed (all receivers dropped).
    pub fn spawn(self, tx: mpsc::Sender<()>) -> JoinHandle<()> {
        tokio::spawn(run_watcher(self.client, self.prefix, self.interval, tx))
    }
}

async fn run_watcher(
    client: ObjectStoreClient,
    prefix: String,
    interval: Duration,
    tx: mpsc::Sender<()>,
) {
    info!(%prefix, ?interval, "Object store watcher started");

    let mut last_paths: Option<HashSet<String>> = None;

    loop {
        sleep(interval).await;

        debug!(%prefix, "Polling object store");

        let current_paths = match client.list(&prefix).await {
            Ok(objects) => objects.into_iter().map(|m| m.path).collect::<HashSet<_>>(),
            Err(e) => {
                warn!(%prefix, error = %e, "Poll failed — will retry");
                continue;
            }
        };

        let changed = match &last_paths {
            // First successful poll — establish baseline, no signal.
            None => {
                info!(%prefix, count = current_paths.len(), "Baseline established");
                false
            }
            Some(prev) => prev != &current_paths,
        };

        last_paths = Some(current_paths);

        if changed {
            info!(%prefix, "Change detected, sending signal");
            if tx.send(()).await.is_err() {
                info!("Receiver dropped — watcher shutting down");
                break;
            }
        } else {
            debug!(%prefix, "No change detected");
        }
    }

    info!(%prefix, "Object store watcher stopped");
}