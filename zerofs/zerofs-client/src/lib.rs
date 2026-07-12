//! Async, path-based client for a ZeroFS server, speaking 9P2000.L (plus the
//! ZeroFS fast-path extensions, used automatically when the server offers
//! them) over TCP or a unix socket natively, and over WebSocket in browser
//! WASM builds.
//!
//! The primary surface is one-shot path operations on a shared [`Client`] (
//! read, write, stat, rename, mkdir), with [`File`] and [`Dir`] handles only
//! where statefulness pays (chunked I/O, incremental listing, openat-style
//! child operations). Paths are bytes, as on POSIX and the 9P wire: every
//! path parameter is `impl AsRef<Path>`, so non-UTF-8 names need nothing
//! special on Unix. Browser paths come from JavaScript strings and are UTF-8.
//! The API otherwise stays FFI-friendly (owned buffers, a flat
//! exhaustive error enum, `Arc` handles, explicit `close`, cancel-safe
//! futures) for the `zerofs-ffi` bindings layer built on top.
//!
//! This is a trusted-network client: you assert your uid at connect time,
//! NFS-style.
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
//!     let size = entry.metadata.as_ref().map_or(0, |m| m.size);
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
//! **Close & drop.** `close()` marks the handle closed immediately (later calls
//! return [`ZeroFsError::Closed`]); it always succeeds, is idempotent, and never
//! hangs. A handle's server-side fid is released and its number recycled when
//! the handle is dropped (for scope-bound use, right after `close()`); the
//! janitor performs the clunk in the background. Dropping a handle you never
//! closed does the same. Fid numbers are always reused, so a long-running client
//! never exhausts them.
//!
//! **Cancellation.** Every public future is cancel-safe: dropping it at any
//! await point leaks nothing (in-flight fids are reclaimed in the background).
//! There is deliberately no per-operation timeout parameter; callers can bound
//! waits with their runtime's timeout facility.
//!
//! **Connection loss.** The underlying session reconnects forever with backoff
//! and replays its state; while the server is unreachable, calls block rather
//! than fail. An operation in flight at the instant of a disconnect is resent,
//! so a non-idempotent op (rename, create, unlink) can apply twice across a
//! reconnect.

#![warn(missing_docs)]

mod client;
mod dir;
mod error;
pub mod failover;
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
pub use failover::FailoverClient;
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
