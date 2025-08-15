use crate::encryption::EncryptedDb;
use crate::fs::errors::FsError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::interval_at;

#[derive(Debug, Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<oneshot::Sender<Result<(), FsError>>>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<EncryptedDb>) -> Self {
        let (sender, mut receiver) =
            mpsc::unbounded_channel::<oneshot::Sender<Result<(), FsError>>>();

        tokio::spawn(async move {
            let mut pending_senders = Vec::new();
            let mut last_flush = Instant::now();

            tracing::info!("Starting automatic flush task (flushes every 30 seconds if needed)");
            let mut auto_flush_interval = interval_at(
                tokio::time::Instant::now() + Duration::from_secs(30),
                Duration::from_secs(30),
            );

            loop {
                tokio::select! {
                    Some(sender) = receiver.recv() => {
                        pending_senders.push(sender);

                        while let Ok(sender) = receiver.try_recv() {
                            pending_senders.push(sender);
                        }

                        let result = db.flush().await.map_err(|_| FsError::IoError);

                        if result.is_ok() {
                            last_flush = Instant::now();
                        }

                        for sender in pending_senders.drain(..) {
                            let _ = sender.send(result);
                        }
                    }
                    _ = auto_flush_interval.tick() => {
                        if last_flush.elapsed() >= Duration::from_secs(30) {
                            tracing::debug!("Auto-flush triggered");

                            let result = db.flush().await.map_err(|_| FsError::IoError);

                            if result.is_ok() {
                                last_flush = Instant::now();
                                tracing::debug!("Automatic flush completed successfully");
                            } else {
                                tracing::error!("Automatic flush failed: {:?}", result);
                            }
                        }
                        // If not enough time has passed, just continue - the next tick will come in 30s
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        self.sender.send(tx).map_err(|_| FsError::IoError)?;

        rx.await.map_err(|_| FsError::IoError)?
    }
}
