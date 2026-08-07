//! Directory iteration and remote readdir batching.

use core::{cmp, sync::atomic::Ordering};

use kernel::{
    bindings,
    error::{Result, code::ENOTDIR, from_result},
    ffi,
};

use crate::{client::OwnedPayload, protocol};

use super::{
    MountState, READ_REPLY_OVERHEAD, READDIR_BATCH,
    attributes::{inode_number, protocol_error, validate_stat},
    io::{DirectoryEmitContext, OpenFileRef},
};

#[derive(Clone, Copy)]
enum DirectoryBatch {
    Eof,
    BufferFull,
    Continue(u64),
}

pub(super) unsafe extern "C" fn zerofs_iterate_shared(
    file: *mut bindings::file,
    context: *mut bindings::dir_context,
) -> ffi::c_int {
    from_result(|| {
        // SAFETY: VFS retains this open directory for the callback.
        let file = unsafe { OpenFileRef::from_raw(file) }?;
        // SAFETY: VFS lends iterate_shared exclusive control of this context.
        let mut context = unsafe { DirectoryEmitContext::from_raw(context) }?;
        let initial_position = context.position();
        if file.inode().file_type() != bindings::S_IFDIR {
            return Err(ENOTDIR);
        }
        let fid = file.state().fid();
        let file_credentials = file.state().credentials().clone();

        let state = file.inode().mount()?;
        let parent_inode = file.inode().remote_id()?;

        let maximum = state
            .client
            .negotiated_msize()
            .saturating_sub(READ_REPLY_OVERHEAD);
        let batch_size = cmp::min(maximum, READDIR_BATCH);
        let mut cookie = initial_position as u64;

        loop {
            let outcome = (|| -> Result<DirectoryBatch> {
                // Capture both cache generations before the RPC so a concurrent
                // namespace mutation cannot reintroduce this reply as a hint.
                let hint_generation = state.hint_generation.load(Ordering::Acquire);
                let observation = state.begin_cache_observation();
                let buffer = match remote_readdir(state, fid, cookie, batch_size)? {
                    Some(buffer) => buffer,
                    None => return Ok(DirectoryBatch::Eof),
                };
                let reply = protocol::decode_readdirattr_payload(buffer.as_slice())
                    .map_err(|_| protocol_error())?;

                let mut next_cookie = cookie;
                for decoded in reply.entries() {
                    let entry = decoded.map_err(|_| protocol_error())?;
                    validate_stat(&entry.stat)?;
                    if entry.qid != entry.stat.qid
                        || entry.offset <= next_cookie
                        || entry.offset > bindings::loff_t::MAX as u64
                        || entry.name.is_empty()
                        || entry.name.len() > bindings::NAME_MAX as usize
                        || entry.name.contains(&b'/')
                        || entry.name.contains(&b'\0')
                    {
                        return Err(protocol_error());
                    }

                    let directory_type = match entry.stat.mode & bindings::S_IFMT {
                        bindings::S_IFDIR => bindings::DT_DIR,
                        bindings::S_IFREG => bindings::DT_REG,
                        bindings::S_IFLNK => bindings::DT_LNK,
                        bindings::S_IFCHR => bindings::DT_CHR,
                        bindings::S_IFBLK => bindings::DT_BLK,
                        bindings::S_IFIFO => bindings::DT_FIFO,
                        bindings::S_IFSOCK => bindings::DT_SOCK,
                        _ => return Err(errno!(EOPNOTSUPP)),
                    };
                    if entry.type_ as u32 != directory_type {
                        return Err(protocol_error());
                    }
                    let inode_number = inode_number(entry.qid)? as u64;

                    // The actor receives the current cookie as d_off, exactly like
                    // dir_emit(). Advance ctx.pos to the server's resume cookie
                    // only after acceptance. If the userspace buffer is full, the
                    // rejected record is fetched again.
                    let accepted = context.emit(entry.name, inode_number, directory_type)?;
                    if !accepted {
                        return Ok(DirectoryBatch::BufferFull);
                    }

                    context.advance(entry.offset)?;
                    next_cookie = entry.offset;
                    if entry.name != b"." && entry.name != b".." {
                        // The stat came from the same credential-bound open fid.
                        // Allocation failure only loses an optimization; lookup
                        // remains correct and falls back to Twalkgetattr.
                        let _ = state.remember_readdir_hint(
                            parent_inode,
                            entry.name,
                            entry.stat,
                            observation,
                            hint_generation,
                            &file_credentials,
                        );
                    }
                }
                Ok(DirectoryBatch::Continue(next_cookie))
            })();

            match outcome {
                Ok(DirectoryBatch::Eof | DirectoryBatch::BufferFull) => return Ok(0),
                Ok(DirectoryBatch::Continue(next_cookie)) => {
                    cookie = next_cookie;
                }
                // POSIX directory iteration reports records already copied in this
                // invocation before deferring a later transport error.
                Err(_) if context.position() != initial_position => return Ok(0),
                Err(error) => return Err(error),
            }
        }
    })
}

fn remote_readdir<'a>(
    state: &'a MountState,
    fid: u32,
    cookie: u64,
    count: u32,
) -> Result<Option<OwnedPayload<'a>>> {
    let reply = state.client.readdirattr(fid, cookie, count)?;
    if reply.is_empty() {
        return Ok(None);
    }
    Ok(Some(reply))
}
