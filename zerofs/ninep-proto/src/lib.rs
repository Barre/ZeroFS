//! 9P2000.L wire-protocol types, Deku-(de)serialized.
//!
//! Shared by the ZeroFS 9P server and the `ninep-client` crate so both sides
//! speak the exact same messages. Includes the ZeroFS-private `Trebind`/`Rrebind`
//! reconnect extension.

mod deku_bytes;
mod protocol;

pub use deku_bytes::*;
pub use protocol::*;
