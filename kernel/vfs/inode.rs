//! VFS inode lookup, allocation, and initialization.

use core::sync::atomic::AtomicU64;

use kernel::{
    alloc::{KBox, flags::GFP_KERNEL},
    bindings,
    error::{Result, code::ENOMEM},
    init::InPlaceInit,
    new_mutex,
    prelude::pin_init,
};

use crate::protocol::Stat;

use super::{
    BoundFidCache, CacheObservation, CachedAttributes, DataCacheBaseline, InodeState,
    WritebackGroupCache,
    attributes::{cache_inode_attributes_at, inode_number, protocol_error, validate_stat},
    io::{IgetInode, OwnedInode, SuperBlockRef},
};

pub(super) fn get_inode(
    super_block: &SuperBlockRef<'_>,
    attributes: &Stat,
    observation: CacheObservation,
) -> Result<OwnedInode> {
    validate_stat(attributes)?;
    let inode_number = inode_number(attributes.qid)?;

    // SAFETY: super_block is live; iget_locked returns null or a referenced,
    // locked new inode / stable existing inode from this superblock.
    let inode = unsafe { bindings::iget_locked(super_block.as_ptr(), inode_number as _) };
    // SAFETY: Classify this exact iget_locked result before exposing it as a
    // general owned reference.
    let inode = unsafe { IgetInode::from_iget_locked(inode) }.map_err(|_| ENOMEM)?;
    let inode = match inode {
        IgetInode::Existing(inode) => {
            let inode_ref = inode.as_ref();
            // A qid path maps to exactly one VFS inode. Refuse a server
            // response that changes its object type under an existing
            // identity.
            let existing_type = inode_ref.file_type();
            let remote_type = attributes.mode & bindings::S_IFMT;
            if existing_type != remote_type {
                return Err(protocol_error());
            }
            // Publish the safe shared subset under the target's i_lock rather
            // than nesting its i_rwsem below the parent directory lock.
            cache_inode_attributes_at(&inode_ref, attributes, observation);
            return Ok(inode);
        }
        IgetInode::New(inode) => inode,
    };

    // From this point, every early return drops NewInode and therefore calls
    // iget_failed exactly once.
    let inode_state = KBox::pin_init(
        pin_init!(InodeState {
            cached_attributes <- new_mutex!(CachedAttributes {
                stat: *attributes,
                data_baseline: DataCacheBaseline::from_stat(attributes),
                metadata_observed_ns: observation.observed_ns,
                data_observed_ns: observation.observed_ns,
                metadata_generation: observation.generation,
                data_generation: observation.generation,
                mapping_generation: observation.generation,
                content_generation: observation.generation,
                metadata_valid: true,
                data_valid: true,
            }),
            metadata_fresh_ns: AtomicU64::new(observation.observed_ns),
            bound_fids <- new_mutex!(BoundFidCache::new()),
            writeback_groups <- new_mutex!(WritebackGroupCache::new()),
            data_revalidate <- new_mutex!(()),
            // This inode was just populated from an authoritative Stat, so
            // both relaxed-consistency windows begin at its observation time.
            last_data_revalidate_ns: AtomicU64::new(observation.observed_ns),
        }),
        GFP_KERNEL,
    )
    .map_err(|_| ENOMEM)?;

    inode.initialize(attributes, inode_state)
}
