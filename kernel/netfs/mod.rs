//! Rust bridge to Linux netfslib.
//!
//! Netfslib is a normal in-kernel library, but its implementation-facing ABI
//! is not yet part of the Rust-for-Linux bindings. `Kbuild` therefore runs
//! bindgen against the target kernel's `<linux/netfs.h>` and includes only the
//! netfs types, constants, and functions used by this module.  Kernel types
//! referenced by that ABI resolve to the target's canonical Rust bindings.

pub(crate) mod abi {
    #![allow(
        dead_code,
        improper_ctypes,
        non_camel_case_types,
        non_snake_case,
        non_upper_case_globals,
        unreachable_pub,
        unsafe_op_in_unsafe_fn
    )]

    use kernel::bindings::*;

    /// Opaque FS-Cache cookie; netfslib owns every object behind this pointer.
    #[repr(C)]
    pub struct fscache_cookie {
        _private: [u8; 0],
    }

    include!(concat!(env!("ZEROFS_KBUILD_OUTPUT"), "/netfs/bindings.rs"));
}

pub(crate) mod compat;
pub(crate) mod io;
