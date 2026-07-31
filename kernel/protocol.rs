//! Kernel-facing import of the canonical allocation-free 9P codec.

#[path = "../zerofs/ninep-proto/src/slice_codec.rs"]
mod shared;

#[allow(unused_imports)]
pub(crate) use shared::*;

/// Retry timings imported rather than copied. The client's stop point and the
/// server's result retention are one pair of numbers, tied together by a const
/// assert inside the imported file; a second copy here would decouple them.
#[allow(dead_code, unreachable_pub)]
#[path = "../zerofs/ninep-proto/src/retry.rs"]
pub(crate) mod retry;
