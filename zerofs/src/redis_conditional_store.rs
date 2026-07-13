//! Conditional-put coordination for S3-compatible stores that lack native
//! `If-Match` / `If-None-Match` support.
//!
//! This is a decorator [`ObjectStore`] that adds atomic create/update semantics
//! on top of any inner store by serialising the HEAD + PUT sequence behind a
//! distributed lock. It replaces the `S3ConditionalPut::Commit` variant that
//! previously lived in our `object_store` fork — by living one layer up, over
//! the public [`ObjectStore`] trait, it keeps Redis entirely out of the fork.
//!
//! Build the inner S3 store with `S3ConditionalPut::Disabled` (so it never
//! emits precondition headers) and wrap it with [`RedisConditionalStore`].
//!
//! Correctness relies on *every* writer to the affected keys going through the
//! same lock — the lock is the sole source of mutual exclusion.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

const STORE: &str = "RedisConditionalStore";

// ---------------------------------------------------------------------------
// Lock abstraction
// ---------------------------------------------------------------------------

/// External coordination for conditional put operations.
///
/// Implementations provide mutual exclusion for writes to a given path, so a
/// HEAD + PUT sequence on a store without native conditional support is atomic.
#[async_trait]
pub trait PutCommit: Send + Sync + Debug {
    /// Acquire an exclusive lock for `path`.
    ///
    /// Returns a [`CommitLock`] carrying a deadline by which the guarded
    /// operation must complete. The lock is released when the guard is dropped.
    async fn lock(&self, path: &Path) -> Result<CommitLock>;
}

/// An acquired lock with a deadline.
///
/// The caller must finish the guarded operation before [`Self::deadline`]; past
/// it the lock may have expired and another writer could proceed, so the caller
/// must abort. Released on drop.
pub struct CommitLock {
    /// The instant by which the guarded operation must complete.
    pub deadline: tokio::time::Instant,
    /// Dropping this releases the lock.
    _guard: Box<dyn Send>,
}

impl Debug for CommitLock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitLock")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl CommitLock {
    /// Create a `CommitLock` with the given deadline and release guard.
    pub fn new(deadline: tokio::time::Instant, guard: Box<dyn Send>) -> Self {
        Self {
            deadline,
            _guard: guard,
        }
    }
}

/// Wraps an [`ObjectStore`] and provides conditional-put semantics via an
/// external [`PutCommit`] lock.
///
/// `PutMode::Overwrite` writes pass straight through. `Create` and
/// `Update(version)` acquire a lock, perform a HEAD to check the precondition,
/// then issue an unconditional overwrite PUT — all bounded by the lock deadline.
#[derive(Debug)]
pub struct RedisConditionalStore {
    inner: Arc<dyn ObjectStore>,
    commit: Arc<dyn PutCommit>,
}

impl RedisConditionalStore {
    /// Wrap `inner`, coordinating conditional writes through `commit`.
    pub fn new(inner: Arc<dyn ObjectStore>, commit: Arc<dyn PutCommit>) -> Self {
        Self { inner, commit }
    }

    /// Run `fut` (the guarded HEAD + PUT) under the lock's deadline. The lock is
    /// released when this returns. A deadline overrun is reported as an error
    /// rather than risking a race with a writer that acquired the expired lock.
    async fn within_deadline<F>(lock: CommitLock, fut: F) -> Result<PutResult>
    where
        F: std::future::Future<Output = Result<PutResult>>,
    {
        match tokio::time::timeout_at(lock.deadline, fut).await {
            Ok(result) => result,
            Err(_) => Err(Error::Generic {
                store: STORE,
                source: "conditional put timed out — lock may have expired".into(),
            }),
        }
    }
}

impl Display for RedisConditionalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RedisConditionalStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for RedisConditionalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        match opts.mode.clone() {
            // Unconditional write: nothing to coordinate.
            PutMode::Overwrite => self.inner.put_opts(location, payload, opts).await,

            // Create: must not already exist.
            PutMode::Create => {
                let lock = self.commit.lock(location).await?;
                let fut = async move {
                    match self.inner.head(location).await {
                        Ok(_) => Err(Error::AlreadyExists {
                            path: location.to_string(),
                            source: "object already exists (checked under lock)".into(),
                        }),
                        Err(Error::NotFound { .. }) => {
                            let mut opts = opts;
                            opts.mode = PutMode::Overwrite;
                            self.inner.put_opts(location, payload, opts).await
                        }
                        Err(e) => Err(e),
                    }
                };
                Self::within_deadline(lock, fut).await
            }

            // Update: current object must exist with the expected ETag.
            PutMode::Update(version) => {
                let lock = self.commit.lock(location).await?;
                let fut = async move {
                    let meta = match self.inner.head(location).await {
                        Ok(meta) => meta,
                        Err(Error::NotFound { path, .. }) => {
                            return Err(Error::Precondition {
                                path,
                                source: "object does not exist (checked under lock)".into(),
                            });
                        }
                        Err(e) => return Err(e),
                    };
                    if meta.e_tag != version.e_tag {
                        return Err(Error::Precondition {
                            path: location.to_string(),
                            source: format!(
                                "ETag mismatch: expected {:?}, found {:?}",
                                version.e_tag, meta.e_tag
                            )
                            .into(),
                        });
                    }
                    let mut opts = opts;
                    opts.mode = PutMode::Overwrite;
                    self.inner.put_opts(location, payload, opts).await
                };
                Self::within_deadline(lock, fut).await
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// Lua script for atomic check-and-delete: only deletes the key when its value
/// matches the caller's token, so a client can never release another's lock.
const UNLOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

/// Default lock TTL (safety net for crash recovery).
const DEFAULT_LOCK_TTL: Duration = Duration::from_secs(300);

/// Margin subtracted from the TTL to produce the operation deadline, covering
/// network round-trip, scheduling delays, and clock skew.
const DEADLINE_SAFETY_MARGIN: Duration = Duration::from_secs(10);

/// Default timeout for acquiring a lock before erroring out.
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);

/// Interval between acquire retries while the lock is held by another client.
const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// A [`PutCommit`] backed by [Redis](https://redis.io/).
///
/// Locks via `SET key token NX PX ttl` and releases with a token-checked Lua
/// script, so only the holder can unlock. The TTL auto-expires the lock if the
/// holder crashes. Operations must complete within the TTL or they are aborted.
#[derive(Debug)]
pub struct RedisCommit {
    client: redis::Client,
    key_prefix: String,
    lock_ttl: Duration,
    acquire_timeout: Duration,
}

impl RedisCommit {
    /// Connect to the given Redis URL (`redis://[user:pass@]host:port[/db]` or
    /// `rediss://...` for TLS). No connection is opened until [`lock`] is called.
    ///
    /// [`lock`]: PutCommit::lock
    pub fn new(url: impl redis::IntoConnectionInfo) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|e| Error::Generic {
            store: STORE,
            source: format!("failed to create Redis client: {e}").into(),
        })?;
        Ok(Self {
            client,
            key_prefix: "s3-lock".into(),
            lock_ttl: DEFAULT_LOCK_TTL,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        })
    }
}

#[async_trait]
impl PutCommit for RedisCommit {
    async fn lock(&self, path: &Path) -> Result<CommitLock> {
        let key = format!(
            "{}:{}",
            self.key_prefix,
            hex_encode(path.as_ref().as_bytes())
        );
        let token = generate_token();

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Generic {
                store: STORE,
                source: format!("failed to connect to Redis: {e}").into(),
            })?;

        let ttl_ms = self.lock_ttl.as_millis() as u64;
        let acquire_deadline = tokio::time::Instant::now() + self.acquire_timeout;

        loop {
            let result: redis::Value = redis::cmd("SET")
                .arg(&key)
                .arg(&token)
                .arg("NX")
                .arg("PX")
                .arg(ttl_ms)
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::Generic {
                    store: STORE,
                    source: format!("Redis SET error: {e}").into(),
                })?;

            if matches!(result, redis::Value::Okay) {
                let deadline = tokio::time::Instant::now()
                    + self.lock_ttl.saturating_sub(DEADLINE_SAFETY_MARGIN);
                let guard = Box::new(RedisLockGuard { conn, key, token });
                return Ok(CommitLock::new(deadline, guard));
            }

            if tokio::time::Instant::now() >= acquire_deadline {
                return Err(Error::Generic {
                    store: STORE,
                    source: format!("timeout acquiring lock for {path}").into(),
                });
            }

            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }
}

/// Releases a Redis lock on drop via a fire-and-forget unlock. The TTL is the
/// safety net if the spawn cannot run (e.g. during runtime shutdown).
struct RedisLockGuard {
    conn: redis::aio::MultiplexedConnection,
    key: String,
    token: String,
}

impl Drop for RedisLockGuard {
    fn drop(&mut self) {
        let mut conn = self.conn.clone();
        let key = std::mem::take(&mut self.key);
        let token = std::mem::take(&mut self.token);

        tokio::spawn(async move {
            let script = redis::Script::new(UNLOCK_SCRIPT);
            let _: std::result::Result<i32, _> =
                script.key(&key).arg(&token).invoke_async(&mut conn).await;
        });
    }
}

/// Generate a random 32-character hex token for lock ownership.
fn generate_token() -> String {
    let bytes: [u8; 16] = rand::random();
    hex_encode(&bytes)
}

/// Lowercase hex-encode a byte slice.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encode_roundtrip() {
        assert_eq!(hex_encode(b"foo/bar"), "666f6f2f626172");
        assert_eq!(hex_encode(b""), "");
    }

    #[test]
    fn tokens_are_unique_32_hex() {
        let (a, b) = (generate_token(), generate_token());
        assert_eq!(a.len(), 32);
        assert_eq!(b.len(), 32);
        assert_ne!(a, b);
    }

    #[test]
    fn invalid_url_errors() {
        assert!(RedisCommit::new("not-a-valid-url").is_err());
    }
}
