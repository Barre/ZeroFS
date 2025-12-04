use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Represents a filesystem operation with its associated parameters.
#[derive(Clone, Debug)]
pub enum FileOperation {
    Read { offset: u64, length: u64 },
    Write { offset: u64, length: u64 },
    Create { mode: u32 },
    Remove,
    Rename { new_path: String },
    Mkdir { mode: u32 },
    Readdir { count: u32 },
    Lookup { filename: String },
    Setattr { mode: Option<u32> },
    Link { new_path: String },
    Symlink { target: String },
    Mknod { mode: u32 },
    Trim { offset: u64, length: u64 },
}

impl std::fmt::Display for FileOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FileOperation::Read { .. } => "read   ",
            FileOperation::Write { .. } => "write  ",
            FileOperation::Create { .. } => "create ",
            FileOperation::Remove => "remove ",
            FileOperation::Rename { .. } => "rename ",
            FileOperation::Mkdir { .. } => "mkdir  ",
            FileOperation::Readdir { .. } => "readdir",
            FileOperation::Lookup { .. } => "lookup ",
            FileOperation::Setattr { .. } => "setattr",
            FileOperation::Link { .. } => "link   ",
            FileOperation::Symlink { .. } => "symlink",
            FileOperation::Mknod { .. } => "mknod  ",
            FileOperation::Trim { .. } => "trim   ",
        };
        write!(f, "{}", s)
    }
}

/// A file access event emitted when a filesystem operation occurs.
#[derive(Clone, Debug)]
pub struct FileAccessEvent {
    pub timestamp: u64,
    pub operation: FileOperation,
    pub path: String,
}

/// Traces filesystem operations and broadcasts them to subscribers.
///
/// `AccessTracer` provides a publish-subscribe mechanism for monitoring
/// filesystem operations in real-time. Subscribers receive `FileAccessEvent`s
/// for each operation that occurs.
///
/// The tracer is designed to have zero overhead when no subscribers are
/// connected - the `has_subscribers()` check should be used before doing
/// any expensive work like path resolution.
#[derive(Clone)]
pub struct AccessTracer {
    sender: broadcast::Sender<FileAccessEvent>,
}

impl AccessTracer {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self { sender }
    }

    /// Returns true if there are any active subscribers.
    ///
    /// Use this to avoid expensive operations (like path resolution)
    /// when no one is listening.
    pub fn has_subscribers(&self) -> bool {
        self.sender.receiver_count() > 0
    }

    /// Subscribe to the event stream.
    pub fn subscribe(&self) -> broadcast::Receiver<FileAccessEvent> {
        self.sender.subscribe()
    }

    /// Emit a file access event with lazy path resolution.
    ///
    /// The path is only resolved if there are active subscribers,
    /// ensuring zero overhead when no one is listening.
    pub async fn emit<F, Fut>(&self, resolve_path: F, operation: FileOperation)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = String>,
    {
        if !self.has_subscribers() {
            return;
        }

        let path = resolve_path().await;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let event = FileAccessEvent {
            timestamp,
            operation,
            path,
        };
        // Ignore send errors (no receivers is OK)
        let _ = self.sender.send(event);
    }
}

impl Default for AccessTracer {
    fn default() -> Self {
        Self::new()
    }
}
