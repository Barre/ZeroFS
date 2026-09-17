#![cfg_attr(not(feature = "std"), no_std)]

//! 9P2000.L wire protocol with owned and allocation-free codecs.
//!
//! Shared by the ZeroFS 9P server and the `ninep-client` crate so both sides
//! speak the exact same messages. Includes the ZeroFS-private `Trebind`/`Rrebind`
//! reconnect extension.
//!
//! All supported messages use [`slice_codec`]. It encodes into caller-owned
//! buffers and decodes borrowed views without allocating. The owned API uses
//! `Bytes` storage, so decoded strings and payloads share the received frame.
//! With default features disabled, the codec requires only `core`. The `owned`
//! feature adds the userspace API in `no_std + alloc` environments.

#[cfg(feature = "owned")]
extern crate alloc;
#[cfg(all(test, not(feature = "std")))]
extern crate std;

#[cfg(feature = "owned")]
mod lock_range;
#[cfg(feature = "owned")]
mod protocol;
pub mod retry;
pub mod slice_codec;
mod wire_messages;
pub use slice_codec::{CodecError, LockType};
mod wire_types;

#[cfg(feature = "owned")]
pub use lock_range::*;
#[cfg(feature = "owned")]
pub use protocol::*;
pub use wire_types::*;
