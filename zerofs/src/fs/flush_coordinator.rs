use crate::encryption::EncryptedDb;
use crate::fs::errors::FsError;
use crate::task::spawn_named;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<oneshot::Sender<Result<(), FsError>>>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<EncryptedDb>) -> Self {
        let (sender, mut receiver) =
            mpsc::unbounded_channel::<oneshot::Sender<Result<(), FsError>>>();

        spawn_named("flush-coordinator", async move {
            let mut pending_senders = Vec::new();

            while let Some(sender) = receiver.recv().await {
                pending_senders.push(sender);

                while let Ok(sender) = receiver.try_recv() {
                    pending_senders.push(sender);
                }

                let result = db.flush().await.map_err(|_| FsError::IoError);

                for sender in pending_senders.drain(..) {
                    let _ = sender.send(result);
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
