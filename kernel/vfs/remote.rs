//! Credential-bound remote inode and namespace operations.

use kernel::{alloc::flags::GFP_KERNEL, error::Result};

use crate::{
    client::{Client, DeviceNumber, OwnedPayload, RebindCredentials},
    protocol::{self, GETATTR_ALL, QID_TYPE_DIR, QID_TYPE_SYMLINK, Stat},
};

use super::attributes::{protocol_error, validate_stat};
use super::{
    BOUND_FID_CACHE_CAPACITY, BoundFidEntry, BoundFidUse, LookupAttributes, OpenedCreation,
    io::InodeRef,
};

pub(super) fn acquire_bound_fid(
    client: &Client,
    inode: &InodeRef<'_>,
    credentials: &RebindCredentials,
    expected_qid_type: u8,
) -> Result<BoundFidUse> {
    let inode_id = inode.remote_id()?;
    let cache = &inode.state()?.bound_fids;

    if let Some(fid) = cache.lock().find(credentials) {
        return Ok(BoundFidUse { fid, cached: true });
    }

    // A cache full of other access=user identities still gets this operation a
    // transient correctly credentialed fid, it just cannot retain it.
    let fid = client.allocate_fid()?;
    let rebound = match client.rebind(fid, inode_id, credentials) {
        Ok(rebound) => rebound,
        Err(error) => {
            // Even a resolved Tflush does not prove that a mutating request
            // lacked side effects. Tclunk is idempotent for an absent fid and
            // is the only safe point at which the number can be recycled.
            let _ = client.clunk(fid);
            return Err(error);
        }
    };
    if rebound.qid.path != inode_id || rebound.qid.type_ != expected_qid_type {
        let _ = client.clunk(fid);
        return Err(protocol_error());
    }

    let mut winner = None;
    let mut installed = false;
    {
        let mut cached = cache.lock();
        // Another task may have installed one for this identity while the
        // rebind was outstanding.
        if let Some(existing) = cached.find(credentials) {
            winner = Some(existing);
        } else if cached.entries.len() < BOUND_FID_CACHE_CAPACITY {
            installed = cached
                .entries
                .push(
                    BoundFidEntry {
                        fid,
                        credentials: credentials.clone(),
                    },
                    GFP_KERNEL,
                )
                .is_ok();
        }
    }

    if let Some(cached_fid) = winner {
        let _ = client.clunk(fid);
        return Ok(BoundFidUse {
            fid: cached_fid,
            cached: true,
        });
    }
    // If capacity or an allocation failure prevented installation, the new
    // capability is transient and this caller owns its clunk. Cache pressure
    // must never make a filesystem operation fail.
    Ok(BoundFidUse {
        fid,
        cached: installed,
    })
}

pub(super) fn retain_bound_fid(
    client: &Client,
    inode: &InodeRef<'_>,
    credentials: &RebindCredentials,
    fid: u32,
) {
    let Ok(inode_state) = inode.state() else {
        let _ = client.clunk(fid);
        return;
    };
    let cache = &inode_state.bound_fids;
    let mut keep = false;
    {
        let mut cached = cache.lock();
        if cached.find(credentials).is_none() && cached.entries.len() < BOUND_FID_CACHE_CAPACITY {
            keep = cached
                .entries
                .push(
                    BoundFidEntry {
                        fid,
                        credentials: credentials.clone(),
                    },
                    GFP_KERNEL,
                )
                .is_ok();
        }
    }
    if !keep {
        let _ = client.clunk(fid);
    }
}

pub(super) fn lookup_attributes(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
) -> Result<LookupAttributes> {
    let parent_inode = parent.remote_id()?;
    let parent_fid = acquire_bound_fid(client, parent, credentials, QID_TYPE_DIR)?;

    let child_fid = match client.allocate_fid() {
        Ok(fid) => fid,
        Err(error) => {
            let _ = parent_fid.cleanup(client);
            return Err(error);
        }
    };
    let result = {
        let names = [name];
        match client.walk_getattr(parent_fid.fid, child_fid, &names) {
            Ok(reply)
                if reply.qid_count == names.len() && reply.final_qid == Some(reply.stat.qid) =>
            {
                Ok(reply.stat)
            }
            Ok(_) => Err(protocol_error()),
            Err(error) => Err(error),
        }
    };
    let parent_cleanup = parent_fid.cleanup(client);
    let attributes = match result {
        Ok(attributes) => attributes,
        Err(error) => {
            // Twalkgetattr may install newfid before a later server-side
            // failure, and an interrupted request is likewise ambiguous.
            let _ = client.clunk(child_fid);
            let _ = parent_cleanup;
            return Err(error);
        }
    };
    let _ = parent_cleanup;
    if let Err(error) = validate_stat(&attributes) {
        let _ = client.clunk(child_fid);
        return Err(error);
    }
    if attributes.qid.path == parent_inode {
        let _ = client.clunk(child_fid);
        return Err(protocol_error());
    }
    Ok(LookupAttributes {
        stat: attributes,
        child_fid,
    })
}

pub(super) fn getattr_attributes(
    client: &Client,
    inode: &InodeRef<'_>,
    credentials: &RebindCredentials,
    expected_qid_type: u8,
) -> Result<Stat> {
    let fid = acquire_bound_fid(client, inode, credentials, expected_qid_type)?;
    let result = (|| {
        let reply = client.getattr(fid.fid)?;
        if reply.valid & GETATTR_ALL != GETATTR_ALL {
            return Err(protocol_error());
        }
        Ok(reply.stat)
    })();
    let cleanup = fid.cleanup(client);
    let attributes = result?;
    cleanup?;
    Ok(attributes)
}

pub(super) struct OpenedInode<'a> {
    pub(super) fid: u32,
    pub(super) iounit: u32,
    pub(super) prefetch: Option<(u64, u64, OwnedPayload<'a>)>,
}

pub(super) fn open_inode<'a>(
    client: &'a Client,
    inode: &InodeRef<'_>,
    credentials: &RebindCredentials,
    expected_qid_type: u8,
    flags: u32,
    prefetch_count: u32,
    prefetch_observed_ns: u64,
    prefetch_content_generation: u64,
) -> Result<OpenedInode<'a>> {
    let inode_id = inode.remote_id()?;
    let bound_fid = acquire_bound_fid(client, inode, credentials, expected_qid_type)?;

    let opened_fid = match client.allocate_fid() {
        Ok(fid) => fid,
        Err(error) => {
            let _ = bound_fid.cleanup(client);
            return Err(error);
        }
    };
    let opened = if prefetch_count == 0 {
        client
            .openat(bound_fid.fid, opened_fid, flags)
            .map(|open| (open, None))
    } else {
        client
            .openat_read(bound_fid.fid, opened_fid, flags, prefetch_count)
            .map(|reply| {
                (
                    reply.open,
                    reply.eof.then_some((
                        prefetch_observed_ns,
                        prefetch_content_generation,
                        reply.payload,
                    )),
                )
            })
    };
    match opened {
        Ok((opened, prefetch))
            if opened.qid.path == inode_id && opened.qid.type_ == expected_qid_type =>
        {
            match bound_fid.cleanup(client) {
                Ok(()) => Ok(OpenedInode {
                    fid: opened_fid,
                    iounit: opened.iounit,
                    prefetch,
                }),
                Err(error) => {
                    let _ = client.clunk(opened_fid);
                    Err(error)
                }
            }
        }
        Ok(_) => {
            let _ = client.clunk(opened_fid);
            let _ = bound_fid.cleanup(client);
            Err(protocol_error())
        }
        Err(error) => {
            let _ = client.clunk(opened_fid);
            let _ = bound_fid.cleanup(client);
            Err(error)
        }
    }
}

pub(super) fn create_opened_regular_file(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
    gid: u32,
    mode: u32,
    flags: u32,
) -> Result<OpenedCreation> {
    let parent_fid = acquire_bound_fid(client, parent, credentials, QID_TYPE_DIR)?;
    let child_fid = match client.allocate_fid() {
        Ok(fid) => fid,
        Err(error) => {
            let _ = parent_fid.cleanup(client);
            return Err(error);
        }
    };

    let result = client.lcreateattr(parent_fid.fid, child_fid, name, flags, mode, gid);
    let parent_cleanup = parent_fid.cleanup(client);
    let created = match result {
        Ok(created) => created,
        Err(error) => {
            // Tlcreateattr may install child_fid before a later fallible reply
            // step. Tclunk is idempotent when it remained absent.
            let _ = client.clunk(child_fid);
            let _ = parent_cleanup;
            return Err(error);
        }
    };
    // A successful Rlcreateattr confirms the namespace mutation. Return the
    // raw response so its caller invalidates committed local cache state
    // before performing fallible protocol validation.
    Ok(OpenedCreation {
        stat: created.stat,
        fid: child_fid,
        iounit: created.iounit,
        parent_cleanup,
    })
}

pub(super) fn create_regular_file(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
    gid: u32,
    mode: u32,
) -> Result<Stat> {
    let created = create_opened_regular_file(client, parent, name, credentials, gid, mode, 0)?;
    // The non-atomic VFS create callback does not own a struct file. Retire the
    // opened child capability; atomic_open transfers it directly instead.
    let _ = client.clunk(created.fid);
    // Rlcreateattr is the namespace commit point. As with every mutation-only
    // callback, a later cleanup failure cannot undo it.
    let _ = created.parent_cleanup;
    Ok(created.stat)
}

fn create_child_with_parent_fid(
    client: &Client,
    parent: &InodeRef<'_>,
    credentials: &RebindCredentials,
    operation: impl FnOnce(u32) -> Result<Stat>,
) -> Result<Stat> {
    let parent_fid = acquire_bound_fid(client, parent, credentials, QID_TYPE_DIR)?;
    let result = operation(parent_fid.fid);
    let cleanup = parent_fid.cleanup(client);
    let stat = result?;
    // The successful response is the namespace commit point. A later
    // transient-parent clunk failure cannot roll it back.
    let _ = cleanup;
    Ok(stat)
}

pub(super) fn create_directory(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
    gid: u32,
    mode: u32,
) -> Result<Stat> {
    create_child_with_parent_fid(client, parent, credentials, |parent_fid| {
        client.mkdirattr(parent_fid, name, mode, gid)
    })
}

pub(super) fn create_symlink(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    target: &[u8],
    credentials: &RebindCredentials,
    gid: u32,
) -> Result<Stat> {
    create_child_with_parent_fid(client, parent, credentials, |parent_fid| {
        client.symlinkattr(parent_fid, name, target, gid)
    })
}

pub(super) fn create_special_node(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
    gid: u32,
    mode: u32,
    device: DeviceNumber,
) -> Result<Stat> {
    create_child_with_parent_fid(client, parent, credentials, |parent_fid| {
        client.mknodattr(parent_fid, name, mode, device, gid)
    })
}

pub(super) fn create_hard_link(
    client: &Client,
    parent: &InodeRef<'_>,
    target: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
    expected_target_type: u8,
) -> Result<Stat> {
    let parent_fid = acquire_bound_fid(client, parent, credentials, QID_TYPE_DIR)?;
    let target_fid = match acquire_bound_fid(client, target, credentials, expected_target_type) {
        Ok(fid) => fid,
        Err(error) => {
            let _ = parent_fid.cleanup(client);
            return Err(error);
        }
    };
    let result = client.linkattr(parent_fid.fid, target_fid.fid, name);
    let target_cleanup = target_fid.cleanup(client);
    let parent_cleanup = parent_fid.cleanup(client);
    let stat = result?;
    let _ = target_cleanup;
    let _ = parent_cleanup;
    Ok(stat)
}

pub(super) fn rename_entry(
    client: &Client,
    old_parent: &InodeRef<'_>,
    old_name: &[u8],
    new_parent: &InodeRef<'_>,
    new_name: &[u8],
    credentials: &RebindCredentials,
) -> Result<()> {
    let old_parent_fid = acquire_bound_fid(client, old_parent, credentials, QID_TYPE_DIR)?;
    let new_parent_fid = match acquire_bound_fid(client, new_parent, credentials, QID_TYPE_DIR) {
        Ok(fid) => fid,
        Err(error) => {
            let _ = old_parent_fid.cleanup(client);
            return Err(error);
        }
    };
    let result = client.renameat(old_parent_fid.fid, old_name, new_parent_fid.fid, new_name);
    let new_cleanup = new_parent_fid.cleanup(client);
    let old_cleanup = old_parent_fid.cleanup(client);
    result?;
    // Rrenameat is authoritative; cleanup failure cannot undo the rename.
    let _ = new_cleanup;
    let _ = old_cleanup;
    Ok(())
}

pub(super) fn remove_entry(
    client: &Client,
    parent: &InodeRef<'_>,
    name: &[u8],
    credentials: &RebindCredentials,
    remove_directory: bool,
) -> Result<()> {
    let parent_fid = acquire_bound_fid(client, parent, credentials, QID_TYPE_DIR)?;
    let result = client.unlinkat(
        parent_fid.fid,
        name,
        if remove_directory {
            protocol::AT_REMOVEDIR
        } else {
            0
        },
    );
    let cleanup = parent_fid.cleanup(client);
    result?;
    // Runlinkat is authoritative; cleanup failure cannot undo the removal.
    let _ = cleanup;
    Ok(())
}

pub(super) fn read_symlink<'a>(
    client: &'a Client,
    inode: &InodeRef<'_>,
    credentials: &RebindCredentials,
) -> Result<OwnedPayload<'a>> {
    let fid = acquire_bound_fid(client, inode, credentials, QID_TYPE_SYMLINK)?;
    let result = client.readlink(fid.fid);
    let cleanup = fid.cleanup(client);
    let target = result?;
    // The target lives in an owned response frame. A cleanup failure already
    // tears down the session and does not invalidate these bytes.
    let _ = cleanup;
    Ok(target)
}
