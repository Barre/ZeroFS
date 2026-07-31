//! Request schema consumed by the allocation-free native-client codec.
//!
//! The allocation-free codec expands this table for request type IDs, tag
//! rules, encoding, and size arithmetic. A cross-codec test checks the result
//! against the owned Deku codec.
//!
//! Each row is
//!
//! ```text
//! Variant, TYPE_ID_CONST, tag rule, { field: kind, ... };
//! ```
//!
//! The tag rule is `notag` for `Tversion`, which must use [`NOTAG`], and `tag`
//! for everything else, which must not. Field kinds:
//!
//! - `u8`, `u16`, `u32`, `u64`: fixed-width little-endian scalars
//! - `str`: `u16` length prefix followed by that many bytes
//! - `names`: `u16` count followed by that many `str`
//! - `payload`: `u32` count followed by that many bytes, always last
//! - `envelope`: the ZeroFS mutation envelope, always first
//!
//! [`NOTAG`]: crate::slice_codec::NOTAG

/// Expand `$emit` over every row. See the module docs for the row shape.
macro_rules! for_each_request {
    ($emit:ident) => {
        $emit! {
            Tversion, TVERSION, notag, { msize: u32, version: str };
            Tgetlineage, TGETLINEAGE, tag, { };
            Trebind, TREBIND, tag, {
                fid: u32,
                inode_id: u64,
                root_inode: u64,
                flags: u8,
                uname: str,
                n_uname: u32
            };
            Twalkgetattr, TWALKGETATTR, tag, { fid: u32, newfid: u32, names: names };
            Tgetattr, TGETATTR, tag, { fid: u32, request_mask: u64 };
            Tsetattrattr, TSETATTRATTR, tag, {
                envelope: envelope,
                fid: u32,
                valid: u32,
                mode: u32,
                uid: u32,
                gid: u32,
                size: u64,
                atime_sec: u64,
                atime_nsec: u64,
                mtime_sec: u64,
                mtime_nsec: u64
            };
            Tfallocate, TFALLOCATE, tag, {
                envelope: envelope,
                fid: u32,
                offset: u64,
                length: u64,
                mode: u32
            };
            Tlopenat, TLOPENAT, tag, { fid: u32, newfid: u32, flags: u32 };
            Tlcreateattr, TLCREATEATTR, tag, {
                envelope: envelope,
                dfid: u32,
                newfid: u32,
                name: str,
                flags: u32,
                mode: u32,
                gid: u32
            };
            Tmkdirattr, TMKDIRATTR, tag, {
                envelope: envelope,
                dfid: u32,
                name: str,
                mode: u32,
                gid: u32
            };
            Tsymlinkattr, TSYMLINKATTR, tag, {
                envelope: envelope,
                dfid: u32,
                name: str,
                target: str,
                gid: u32
            };
            Tmknodattr, TMKNODATTR, tag, {
                envelope: envelope,
                dfid: u32,
                name: str,
                mode: u32,
                major: u32,
                minor: u32,
                gid: u32
            };
            Tlinkattr, TLINKATTR, tag, {
                envelope: envelope,
                dfid: u32,
                fid: u32,
                name: str
            };
            Trenameat, TRENAMEAT, tag, {
                envelope: envelope,
                olddirfid: u32,
                oldname: str,
                newdirfid: u32,
                newname: str
            };
            Tunlinkat, TUNLINKAT, tag, {
                envelope: envelope,
                dirfid: u32,
                name: str,
                flags: u32
            };
            Treadlink, TREADLINK, tag, { fid: u32 };
            Tflush, TFLUSH, tag, { oldtag: u16 };
            Tread, TREAD, tag, { fid: u32, offset: u64, count: u32 };
            Twrite, TWRITE, tag, {
                envelope: envelope,
                fid: u32,
                offset: u64,
                data: payload
            };
            Tfsyncdur, TFSYNCDUR, tag, { fid: u32, datasync: u32, token: u64 };
            Treaddirattr, TREADDIRATTR, tag, { fid: u32, offset: u64, count: u32 };
            Tclunk, TCLUNK, tag, { fid: u32 };
            Tstatfs, TSTATFS, tag, { fid: u32 };
            Tlock, TLOCK, tag, {
                fid: u32,
                lock_type: u8,
                flags: u32,
                start: u64,
                length: u64,
                proc_id: u32,
                client_id: str
            };
            Tgetlock, TGETLOCK, tag, {
                fid: u32,
                lock_type: u8,
                start: u64,
                length: u64,
                proc_id: u32,
                client_id: str
            };
        }
    };
}

pub(crate) use for_each_request;
