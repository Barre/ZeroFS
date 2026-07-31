#![cfg_attr(not(feature = "std"), no_std)]

//! 9P2000.L wire protocol with owned and allocation-free codecs.
//!
//! Shared by the ZeroFS 9P server and the `ninep-client` crate so both sides
//! speak the exact same messages. Includes the ZeroFS-private `Trebind`/`Rrebind`
//! reconnect extension.
//!
//! The default API uses Deku with `Vec`/`Bytes` ownership. [`slice_codec`]
//! encodes into caller-owned buffers and decodes borrowed response views
//! without allocation. Both codecs share message identifiers, limits, fixed
//! structures, and storage-neutral [`WireString`]/[`WireBytes`] values. With
//! default features disabled, the slice codec and shared wire layer
//! require only `core`. Enabling `owned` adds the Deku API in `no_std + alloc`
//! environments; the default `std` feature enables its standard-library I/O
//! adapters.

#[cfg(feature = "owned")]
extern crate alloc;
#[cfg(all(test, not(feature = "std")))]
extern crate std;

#[cfg(feature = "owned")]
mod deku_bytes;
#[cfg(feature = "owned")]
mod lock_range;
#[cfg(feature = "owned")]
mod protocol;
pub mod retry;
pub mod slice_codec;
mod wire_requests;
mod wire_types;

#[cfg(feature = "owned")]
pub use deku_bytes::*;
#[cfg(feature = "owned")]
pub use lock_range::*;
#[cfg(feature = "owned")]
pub use protocol::*;
pub use wire_types::*;
