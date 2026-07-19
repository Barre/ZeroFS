//! Async, path-based client for a ZeroFS server, speaking the private
//! `9P2000.L.Z` dialect over TCP or a unix socket natively, and over
//! WebSocket in browser WASM builds.
//!
//! The primary surface is one-shot path operations on a shared [`Client`]
//! (read, write, stat, rename, mkdir), with [`File`] and [`Dir`] handles
//! for chunked I/O, incremental listing, and openat-style child operations.
//! Paths are byte-oriented. Native APIs accept non-UTF-8 paths; browser paths
//! originate as UTF-8 JavaScript strings. The API uses owned data, an
//! exhaustive error enum, `Arc` handles, explicit close, and cancellation
//! cleanup for FFI compatibility.
//!
//! The client asserts its uid at connect time and assumes a trusted network.
//!
//! ```no_run
//! use zerofs_client::{Client, OpenOptions};
//!
//! # async fn demo() -> Result<(), zerofs_client::ZeroFsError> {
//! let fs = Client::connect("unix:/tmp/zerofs.9p.sock").await?;
//!
//! fs.create_dir_all("/projects/demo", 0o755).await?;
//! fs.write("/projects/demo/hello.txt", b"hello from zerofs").await?;
//!
//! let data = fs.read("/projects/demo/hello.txt").await?;
//! assert_eq!(&data[..], b"hello from zerofs");
//!
//! for entry in fs.read_dir("/projects/demo").await? {
//!     let size = entry.metadata.size;
//!     println!("{size:>8}  {}", entry.name);
//! }
//!
//! let file = fs
//!     .open("/projects/demo/big.bin", OpenOptions::read_write().create(true))
//!     .await?;
//! file.write_at(0, &vec![0u8; 4 << 20]).await?;
//! file.sync_all().await?;
//! file.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Lifecycles
//!
//! **Close and drop.** `close()` marks the handle closed and is idempotent and
//! non-blocking. Dropping the handle schedules its fid for release and recycling.
//!
//! **Cancellation.** Dropping a Rust operation future releases any temporary
//! fids it owns. If the future is dropped while a fid-state request is dispatched
//! and unsettled, the connection that carried the request is retired. If it is
//! still current, the session reconnects and replays before other operations
//! resume. A dispatched mutation may still complete, leaving an ambiguous
//! outcome. Callers supply per-operation timeouts when this ambiguity is
//! acceptable. `zerofs-ffi` timeouts abandon a timed-out wait without cancelling
//! the Rust future.
//!
//! **Connection loss.** The session reconnects with backoff and restores state
//! before accepting requests. In-flight mutations retain their op-id across
//! resends. The retry horizon bounds automatic resends; expiry returns an
//! ambiguous connection failure. An open-unlinked handle cannot be replayed and
//! returns `ESTALE` after connection loss; other handles remain usable. The
//! negotiated `msize` is fixed for the logical session; every reconnect target
//! must accept that value.

#![warn(missing_docs)]

mod client;
mod dir;
mod error;
mod file;
#[cfg(feature = "tokio-io")]
pub mod io;
mod linux;
mod path;
mod runtime;
mod session;
#[cfg(feature = "stream")]
pub mod stream;
mod types;

pub use client::Client;
pub use dir::Dir;
pub use error::ZeroFsError;
pub use file::File;
#[cfg(feature = "stream")]
pub use stream::DirStream;
pub use types::{
    Capabilities, ConnectOptions, DirEntry, FileType, Metadata, NodeKind, OpenOptions, SetAttrs,
    SetTime, StatFs, Timestamp,
};

/// Re-exported so callers need not depend on `bytes` directly; it is the return
/// type of every read (cheap to clone and slice, derefs to `&[u8]`).
pub use bytes::Bytes;
pub use ninep_client::TrafficStats;
