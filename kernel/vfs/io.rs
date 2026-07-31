//! Typed contexts for raw VFS and netfslib callback objects.
//!
//! VFS passes raw C pointers, while its callback contracts define object
//! lifetimes, exclusive lifecycle transitions, and reference transfers. These
//! adapters establish each contract once and expose typed operations to the
//! filesystem logic.

use core::{
    marker::PhantomData,
    mem::{MaybeUninit, align_of, offset_of, size_of},
    pin::Pin,
    ptr::{self, NonNull},
    slice,
};

use kernel::{
    alloc::KBox,
    bindings,
    cred::Credential,
    error::to_result,
    ffi,
    fs::{Kiocb, LocalFile},
    iov::{IovIterDest, IovIterSource},
    mm::virt::VmaNew,
    prelude::*,
    sync::aref::ARef,
    types::{ForeignOwnable, NotThreadSafe},
};

use crate::{
    client::{LockOwner, LockRange},
    netfs::{abi as netfs, compat as netfs_compat},
    protocol::{self, Stat},
};

use super::{DentryState, FileState, InodeState, MountState, compat};

mod target_vfs_layout {
    #![allow(
        dead_code,
        non_camel_case_types,
        non_upper_case_globals,
        unreachable_pub
    )]

    include!(concat!(
        env!("ZEROFS_KBUILD_OUTPUT"),
        "/vfs/layout_bindings.rs"
    ));
}

/// Storage for an object borrowed through a kernel callback contract.
///
/// The public adapters below retain domain-specific names and operations. This
/// type only centralizes the raw-pointer, lifetime, and `!Send`/`!Sync`
/// bookkeeping common to all of them.
struct CallbackPtr<T, Borrow> {
    raw: NonNull<T>,
    _borrow: PhantomData<Borrow>,
    _not_thread_safe: NotThreadSafe,
}

type CallbackRef<'a, T> = CallbackPtr<T, &'a T>;
type CallbackMut<'a, T> = CallbackPtr<T, &'a mut T>;

impl<T, Borrow> CallbackPtr<T, Borrow> {
    /// # Safety
    ///
    /// `raw` must satisfy the lifetime and aliasing represented by `Borrow`.
    unsafe fn from_raw(raw: *mut T) -> Option<Self> {
        Some(Self {
            raw: NonNull::new(raw)?,
            _borrow: PhantomData,
            _not_thread_safe: PhantomData,
        })
    }

    fn as_ptr(&self) -> *mut T {
        self.raw.as_ptr()
    }
}

impl<T> CallbackRef<'_, T> {
    /// Borrow a pointer already kept live by another typed owner.
    fn from_non_null(raw: NonNull<T>) -> Self {
        Self {
            raw,
            _borrow: PhantomData,
            _not_thread_safe: PhantomData,
        }
    }
}

impl<T> CallbackMut<'_, T> {
    /// # Safety
    ///
    /// `raw` must be exclusively borrowed for the resulting lifetime.
    unsafe fn from_non_null(raw: NonNull<T>) -> Self {
        Self {
            raw,
            _borrow: PhantomData,
            _not_thread_safe: PhantomData,
        }
    }

    fn as_shared(&self) -> CallbackRef<'_, T> {
        CallbackRef::from_non_null(self.raw)
    }
}

// Keep field access tied to callback adapters and Copy values.
fn callback_raw<T, Borrow>(place: &CallbackPtr<T, Borrow>) -> *mut T {
    place.as_ptr()
}

fn callback_mut_raw<T>(place: &CallbackMut<'_, T>) -> *mut T {
    place.as_ptr()
}

fn copy_field_value<T: Copy>(value: T) -> T {
    value
}

/// Read a `Copy` callback field with `READ_ONCE()` semantics.
macro_rules! read_once {
    ($place:expr, $($field:tt).+) => {{
        let raw = callback_raw(&$place);
        // SAFETY: CallbackPtr keeps the field live and correctly typed.
        unsafe { copy_field_value(ptr::addr_of!((*raw).$($field).+).read_volatile()) }
    }};
}

/// Plain-load a `Copy` callback field that needs no `READ_ONCE()` semantics.
macro_rules! read_stable {
    ($place:expr, $($field:tt).+) => {{
        let raw = callback_raw(&$place);
        // SAFETY: CallbackPtr keeps the field live and correctly typed.
        unsafe { copy_field_value(ptr::addr_of!((*raw).$($field).+).read()) }
    }};
}

/// Store a `Copy` field through an exclusive [`CallbackMut`] borrow.
macro_rules! write_field {
    ($place:expr, $($field:tt).+, $value:expr) => {{
        let raw = callback_mut_raw(&$place);
        // SAFETY: CallbackMut keeps the field live and exclusively borrowed.
        unsafe { ptr::addr_of_mut!((*raw).$($field).+).write(copy_field_value($value)) }
    }};
}

/// A live ZeroFS superblock borrowed from a VFS callback object.
pub(super) struct SuperBlockRef<'a> {
    raw: CallbackRef<'a, bindings::super_block>,
}

/// Exclusive initialization access supplied by `get_tree_nodev`.
pub(super) struct SuperBlockInitRef<'a> {
    raw: CallbackMut<'a, bindings::super_block>,
    state: Option<Pin<KBox<MountState>>>,
}

/// Exclusive teardown access supplied to `put_super`.
pub(super) struct SuperBlockReleaseRef<'a> {
    raw: CallbackMut<'a, bindings::super_block>,
}

impl<'a> SuperBlockRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain a live ZeroFS superblock for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::super_block) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackRef::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::super_block {
        self.raw.as_ptr()
    }

    pub(super) fn mount(&self) -> Result<&'a MountState> {
        let state = read_stable!(self.raw, s_fs_info).cast::<MountState>();
        if state.is_null() {
            return Err(EIO);
        }
        // SAFETY: fill_super publishes one allocation in s_fs_info and
        // put_super removes it only after VFS stops lending this superblock.
        Ok(unsafe { &*state })
    }

    pub(super) fn flags(&self) -> ffi::c_ulong {
        // s_flags may be updated by remount paths.
        read_once!(self.raw, s_flags)
    }

    pub(super) fn user_namespace_ptr(&self) -> *mut bindings::user_namespace {
        // s_user_ns is fixed when the superblock is created.
        read_stable!(self.raw, s_user_ns)
    }
}

impl<'a> SuperBlockInitRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the live, exclusively initialized superblock supplied to
    /// the current nodev fill callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::super_block) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?,
            state: None,
        })
    }

    /// Configure the VFS/BDI fields and temporarily lend the mount allocation
    /// through `s_fs_info`. Drop rolls that loan back until publish succeeds.
    pub(super) fn configure(
        &mut self,
        state: Pin<KBox<MountState>>,
        readahead_pages: usize,
    ) -> Result<()> {
        if self.state.is_some() {
            return Err(EINVAL);
        }
        let super_block = self.raw.as_ptr();
        // SAFETY: This typestate owns the unpublished superblock exclusively.
        let status = unsafe { bindings::super_setup_bdi(super_block) };
        to_result(status)?;
        let backing = unsafe { ptr::addr_of!((*super_block).s_bdi).read() };
        if backing.is_null() {
            return Err(EIO);
        }
        // SAFETY: The BDI and superblock are private to this fill operation.
        // Retain ownership in this typestate until publish() transfers it to
        // put_super.
        self.state = Some(state);
        let state = self.state.as_ref().ok_or(EIO)?;
        unsafe {
            ptr::addr_of_mut!((*backing).ra_pages).write(readahead_pages as ffi::c_ulong);
            ptr::addr_of_mut!((*backing).io_pages).write(readahead_pages as ffi::c_ulong);

            (*super_block).s_flags |= super::SB_NOSUID | super::SB_NODEV;
            (*super_block).s_magic = super::ZEROFS_MAGIC;
            (*super_block).s_blocksize = 1 << bindings::PAGE_SHIFT;
            (*super_block).s_blocksize_bits = bindings::PAGE_SHIFT as u8;
            (*super_block).s_maxbytes = bindings::loff_t::MAX;
            (*super_block).s_time_gran = 1;
            // Wire timestamps are unsigned, so let VFS clamp older dates to
            // the Unix epoch before they reach setattr.
            (*super_block).s_time_min = 0;
            (*super_block).s_op = super::SUPER_OPERATIONS.as_ptr();
            (*super_block).s_fs_info = ptr::from_ref::<MountState>(&**state)
                .cast_mut()
                .cast::<ffi::c_void>();
            bindings::set_default_d_op(super_block, super::DENTRY_OPERATIONS.as_ptr());
        }
        Ok(())
    }

    pub(super) fn as_ref(&self) -> SuperBlockRef<'_> {
        SuperBlockRef {
            raw: self.raw.as_shared(),
        }
    }

    /// Publish the root and transfer the mount allocation to `put_super`.
    pub(super) fn publish(mut self, root: NonNull<bindings::dentry>) -> Result<()> {
        let state = self.state.take().ok_or(EINVAL)?;
        // configure() installed this exact stable allocation, and root
        // publication is the final infallible fill-super transition.
        write_field!(self.raw, s_fs_info, state.into_foreign());
        write_field!(self.raw, s_root, root.as_ptr());
        Ok(())
    }
}

impl Drop for SuperBlockInitRef<'_> {
    fn drop(&mut self) {
        if self.state.is_some() {
            // Before publish, this typestate owns only the temporary s_fs_info
            // loan. The allocation itself remains with the caller.
            write_field!(self.raw, s_fs_info, ptr::null_mut());
        }
    }
}

impl<'a> SuperBlockReleaseRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the live superblock supplied exclusively to its one
    /// `put_super` callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::super_block) -> Option<Self> {
        Some(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }?,
        })
    }

    pub(super) fn take_state(self) -> Option<Pin<KBox<MountState>>> {
        let state = read_stable!(self.raw, s_fs_info);
        write_field!(self.raw, s_fs_info, ptr::null_mut());
        if state.is_null() {
            return None;
        }
        // SAFETY: put_super excludes further filesystem use, configure()
        // stored exactly this ForeignOwnable representation, and the slot is
        // cleared above so this reconstructs the sole owner.
        Some(unsafe { <Pin<KBox<MountState>> as ForeignOwnable>::from_foreign(state) })
    }
}

pub(super) struct InodeRef<'a> {
    raw: CallbackRef<'a, bindings::inode>,
}

pub(super) struct AttributeRefresh {
    pub(super) metadata: bool,
    pub(super) data: bool,
    pub(super) link_count: bool,
}

/// One owned VFS inode reference.
pub(super) struct OwnedInode {
    raw: NonNull<bindings::inode>,
    _not_thread_safe: NotThreadSafe,
}

/// Result of classifying the reference returned by `iget_locked`.
pub(super) enum IgetInode {
    Existing(OwnedInode),
    New(NewInode),
}

/// Inode state supplied to `evict_inode`.
pub(super) enum EvictionInode<'a> {
    Bad(BadEvictionInode<'a>),
    Initialized(InitializedEvictionInode<'a>),
}

/// An inode rejected before ZeroFS initialized its netfs/private tail.
pub(super) struct BadEvictionInode<'a> {
    raw: CallbackMut<'a, bindings::inode>,
}

/// A fully initialized inode under exclusive eviction ownership.
pub(super) struct InitializedEvictionInode<'a> {
    raw: CallbackMut<'a, bindings::inode>,
}

/// An owned inode reference that still carries I_NEW.
///
/// Dropping this value aborts initialization and wakes iget waiters. The only
/// successful transition installs all ZeroFS state before unlocking the inode.
pub(super) struct NewInode {
    inode: Option<OwnedInode>,
}

impl OwnedInode {
    pub(super) fn as_ref(&self) -> InodeRef<'_> {
        InodeRef {
            raw: CallbackRef::from_non_null(self.raw),
        }
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::inode {
        self.raw.as_ptr()
    }

    /// Fail initialization of an inode returned with I_NEW set.
    ///
    /// # Safety
    ///
    /// This inode must still carry I_NEW and must not have been unlocked.
    unsafe fn fail_new(self) {
        let raw = self.into_raw();
        unsafe {
            bindings::iget_failed(raw);
        }
    }

    /// Transfer this reference to a VFS operation that consumes it.
    pub(super) fn into_raw(self) -> *mut bindings::inode {
        let raw = self.raw.as_ptr();
        core::mem::forget(self);
        raw
    }
}

impl IgetInode {
    /// Classify the exact reference returned to this task by `iget_locked`.
    ///
    /// # Safety
    ///
    /// `raw` must be the direct non-error return from the current task's
    /// `iget_locked` call and must transfer that one reference here.
    pub(super) unsafe fn from_iget_locked(raw: *mut bindings::inode) -> Result<Self> {
        let inode = OwnedInode {
            raw: NonNull::new(raw).ok_or(EINVAL)?,
            _not_thread_safe: PhantomData,
        };
        if inode.as_ref().vfs_state_flags() & target_vfs_layout::ZEROFS_I_NEW != 0 {
            Ok(Self::New(NewInode { inode: Some(inode) }))
        } else {
            Ok(Self::Existing(inode))
        }
    }
}

impl<'a> EvictionInode<'a> {
    /// Classify the inode supplied to the filesystem's eviction callback.
    ///
    /// # Safety
    ///
    /// `raw` must remain exclusively owned by the current `evict_inode`
    /// callback for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::inode) -> Option<Self> {
        let raw = NonNull::new(raw)?;
        if unsafe { bindings::is_bad_inode(raw.as_ptr()) } {
            Some(Self::Bad(BadEvictionInode {
                // SAFETY: The callback exclusively owns this inode.
                raw: unsafe { CallbackMut::from_non_null(raw) },
            }))
        } else {
            Some(Self::Initialized(InitializedEvictionInode {
                // SAFETY: The callback exclusively owns this inode.
                raw: unsafe { CallbackMut::from_non_null(raw) },
            }))
        }
    }
}

impl BadEvictionInode<'_> {
    /// Finish eviction without touching the uninitialized netfs/private tail.
    pub(super) fn clear(self) {
        // SAFETY: The eviction callback exclusively owns the embedded VFS
        // inode, which is the only initialized portion on this path.
        unsafe {
            bindings::clear_inode(self.raw.as_ptr());
        }
    }
}

impl InitializedEvictionInode<'_> {
    pub(super) fn as_ref(&self) -> InodeRef<'_> {
        InodeRef {
            raw: self.raw.as_shared(),
        }
    }

    /// Drain direct I/O, netfs requests, page cache, and netfs writeback.
    pub(super) fn drain_io(&self) {
        let inode = self.raw.as_ptr();
        let context = inode.cast::<netfs::netfs_inode>();
        // SAFETY: A non-bad ZeroFS inode completed netfs initialization before
        // publication, and eviction excludes new users.
        unsafe {
            bindings::inode_dio_wait(inode);
            while bindings::atomic_read(ptr::addr_of!((*context).io_count)) != 0 {
                // The inline-only netfs wait helper has no callable symbol.
                // Sleeping one jiffy avoids spinning while normal request
                // completion wakes netfs waiters.
                bindings::schedule_timeout_uninterruptible(1);
            }
            let mapping = ptr::addr_of!((*inode).i_mapping).read();
            if !mapping.is_null() {
                bindings::truncate_inode_pages_final(mapping);
            }
            netfs::netfs_clear_inode_writeback(inode, ptr::null());
        }
    }

    pub(super) fn state(&self) -> Option<&InodeState> {
        let state = read_stable!(self.raw, i_private).cast::<InodeState>();
        // SAFETY: This initialized inode owns i_private until finish() removes
        // it, and exclusive eviction prevents concurrent teardown.
        unsafe { state.as_ref() }
    }

    /// Clear the VFS inode and recover its sole private-state allocation.
    pub(super) fn finish(self) -> Option<Pin<KBox<InodeState>>> {
        // Capture i_private before clear_inode, then clear its slot before
        // rebuilding the owner.
        let state = read_stable!(self.raw, i_private).cast::<InodeState>();
        // SAFETY: Eviction has drained all users and this value owns the final
        // teardown transition.
        unsafe {
            bindings::clear_inode(self.raw.as_ptr());
        }
        write_field!(self.raw, i_private, ptr::null_mut());
        if state.is_null() {
            None
        } else {
            // SAFETY: initialize stored this pinned-box representation, and
            // eviction owns it after clearing i_private.
            Some(unsafe {
                <Pin<KBox<InodeState>> as ForeignOwnable>::from_foreign(state.cast::<ffi::c_void>())
            })
        }
    }
}

impl Drop for OwnedInode {
    fn drop(&mut self) {
        // SAFETY: This value owns exactly one reference not transferred by
        // into_raw().
        unsafe {
            bindings::iput(self.raw.as_ptr());
        }
    }
}

impl NewInode {
    /// Populate and publish a fully initialized ZeroFS inode.
    pub(super) fn initialize(
        mut self,
        attributes: &Stat,
        state: Pin<KBox<InodeState>>,
    ) -> Result<OwnedInode> {
        // Keep this safe boundary honest even if a future caller does not run
        // get_inode's earlier protocol validation.
        super::attributes::validate_stat(attributes)?;

        let inode = match self.inode.take() {
            Some(inode) => inode,
            None => return Err(EIO),
        };
        let raw = inode.as_ptr();
        let file_type = attributes.mode & bindings::S_IFMT;

        // SAFETY: This typestate exclusively owns an inode that still carries
        // I_NEW. Validation above checked every narrowing conversion. All
        // private/netfs state is installed before unlock_new_inode publishes
        // the completed object to iget waiters.
        unsafe {
            (*raw).i_mode = attributes.mode as bindings::umode_t;
            (*raw).i_uid = compat::make_kuid((*(*raw).i_sb).s_user_ns, attributes.uid);
            (*raw).i_gid = compat::make_kgid((*(*raw).i_sb).s_user_ns, attributes.gid);
            bindings::set_nlink(raw, attributes.nlink as ffi::c_uint);
            (*raw).i_size = attributes.size as bindings::loff_t;
            (*raw).i_blocks = attributes.blocks as bindings::blkcnt_t;
            (*raw).i_blkbits = bindings::PAGE_SHIFT as u8;
            (*raw).i_atime_sec = attributes.atime_sec as bindings::time64_t;
            (*raw).i_atime_nsec = attributes.atime_nsec as u32;
            (*raw).i_mtime_sec = attributes.mtime_sec as bindings::time64_t;
            (*raw).i_mtime_nsec = attributes.mtime_nsec as u32;
            (*raw).i_ctime_sec = attributes.ctime_sec as bindings::time64_t;
            (*raw).i_ctime_nsec = attributes.ctime_nsec as u32;
            (*raw).i_generation = attributes.r#gen as u32;

            match file_type {
                bindings::S_IFDIR => {
                    (*raw).i_op = super::DIRECTORY_INODE_OPERATIONS.as_ptr();
                    (*raw).__bindgen_anon_3.i_fop = super::DIRECTORY_FILE_OPERATIONS.as_ptr();
                }
                bindings::S_IFREG => {
                    (*raw).i_op = super::FILE_INODE_OPERATIONS.as_ptr();
                    (*raw).__bindgen_anon_3.i_fop = super::FILE_FILE_OPERATIONS.as_ptr();
                    if !(*raw).i_mapping.is_null() {
                        (*(*raw).i_mapping).a_ops = super::FILE_ADDRESS_SPACE_OPERATIONS.as_ptr();
                    }
                }
                bindings::S_IFLNK => {
                    (*raw).i_op = super::SYMLINK_INODE_OPERATIONS.as_ptr();
                    (*raw).__bindgen_anon_3.i_fop = ptr::null();
                }
                bindings::S_IFCHR | bindings::S_IFBLK | bindings::S_IFIFO | bindings::S_IFSOCK => {
                    (*raw).i_op = super::FILE_INODE_OPERATIONS.as_ptr();
                    // The mount is SB_NODEV; remote device nodes remain
                    // metadata-only, but VFS still needs the canonical special
                    // inode operations and encoded device identity.
                    bindings::init_special_inode(
                        raw,
                        attributes.mode as bindings::umode_t,
                        super::attributes::wire_rdev_to_dev(attributes.rdev),
                    );
                }
                _ => {}
            }

            netfs_compat::initialize_inode(raw, super::NETFS_REQUEST_OPERATIONS.as_ptr());

            (*raw).i_private = state.into_foreign();
            bindings::unlock_new_inode(raw);
        }
        Ok(inode)
    }
}

impl Drop for NewInode {
    fn drop(&mut self) {
        if let Some(inode) = self.inode.take() {
            // SAFETY: NewInode is created only from the I_NEW branch and the
            // successful initialize transition removes the inner reference.
            unsafe {
                inode.fail_new();
            }
        }
    }
}

/// A live address-space mapping pinned by its owning inode.
pub(super) struct MappingRef<'a> {
    raw: CallbackRef<'a, bindings::address_space>,
}

impl<'a> MappingRef<'a> {
    pub(super) fn as_ptr(&self) -> *mut bindings::address_space {
        self.raw.as_ptr()
    }

    pub(super) fn reborrow(&self) -> MappingRef<'a> {
        MappingRef {
            raw: CallbackRef::from_non_null(self.raw.raw),
        }
    }

    pub(super) fn write_and_wait_all(&self) -> ffi::c_int {
        // SAFETY: The inode borrow pins this live mapping for the synchronous
        // writeback wait.
        unsafe {
            bindings::filemap_write_and_wait_range(self.raw.as_ptr(), 0, bindings::loff_t::MAX)
        }
    }
}

impl<'a> InodeRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain a live ZeroFS inode for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::inode) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackRef::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::inode {
        self.raw.as_ptr()
    }

    pub(super) fn reborrow(&self) -> InodeRef<'a> {
        InodeRef {
            raw: CallbackRef::from_non_null(self.raw.raw),
        }
    }

    pub(super) fn mode(&self) -> u32 {
        // setattr may update the mode concurrently.
        read_once!(self.raw, i_mode) as u32
    }

    pub(super) fn file_type(&self) -> u32 {
        self.mode() & bindings::S_IFMT
    }

    pub(super) fn link_count(&self) -> ffi::c_uint {
        // Link-count changes may race with getattr, so take the same kind of
        // atomic snapshot as the kernel's READ_ONCE() users.
        read_once!(self.raw, __bindgen_anon_1.i_nlink)
    }

    pub(super) fn size(&self) -> bindings::loff_t {
        // i_size changes are serialized by inode/mapping locks, but readers
        // may take a lockless snapshot just as i_size_read() does.
        read_once!(self.raw, i_size)
    }

    pub(super) fn access_time(&self) -> bindings::timespec64 {
        let inode = self.raw.as_ptr();
        // SAFETY: i_lock makes the split timestamp fields one coherent
        // snapshot while the inode borrow keeps their storage live.
        unsafe {
            bindings::spin_lock(ptr::addr_of_mut!((*inode).i_lock));
            let atime = bindings::timespec64 {
                tv_sec: (*inode).i_atime_sec,
                tv_nsec: (*inode).i_atime_nsec as ffi::c_long,
            };
            bindings::spin_unlock(ptr::addr_of_mut!((*inode).i_lock));
            atime
        }
    }

    pub(super) fn refresh_access_time_from_stat(&self, attributes: &Stat) {
        let inode = self.raw.as_ptr();
        // SAFETY: The inode borrow pins the object and i_lock serializes the
        // split timestamp fields against VFS readers.
        unsafe {
            bindings::spin_lock(ptr::addr_of_mut!((*inode).i_lock));
            (*inode).i_atime_sec = attributes.atime_sec as bindings::time64_t;
            (*inode).i_atime_nsec = attributes.atime_nsec as u32;
            bindings::spin_unlock(ptr::addr_of_mut!((*inode).i_lock));
        }
    }

    /// Clear mmap dirtiness beyond an old EOF before a later write exposes it.
    ///
    /// # Safety
    ///
    /// The caller must have held this inode's `i_rwsem` exclusively since
    /// before `i_size` grew, and `i_size` must already cover `to`.
    pub(super) unsafe fn zero_exposed_eof_tail(
        &self,
        from: bindings::loff_t,
        to: bindings::loff_t,
    ) {
        unsafe { compat::zero_exposed_eof_tail(self.raw.as_ptr(), from, to) };
    }

    pub(super) fn set_remote_size(&self, size: u64) {
        if size > bindings::loff_t::MAX as u64 {
            return;
        }
        // SAFETY: The inode borrow pins the embedded netfs context; the
        // compatibility boundary supplies netfslib's writer serialization.
        unsafe {
            netfs_compat::write_remote_size(self.raw.as_ptr(), size as bindings::loff_t);
        }
    }

    pub(super) fn extend_remote_size(&self, end: u64) {
        if end > bindings::loff_t::MAX as u64 {
            return;
        }
        // SAFETY: The inode borrow pins the embedded netfs context; the
        // compatibility boundary supplies the atomic read-modify-write.
        unsafe {
            netfs_compat::extend_remote_size(self.raw.as_ptr(), end as bindings::loff_t);
        }
    }

    /// Refresh selected shared inode fields from a validated authoritative Stat.
    ///
    /// Metadata and page-cache-derived fields have independent observation
    /// orders. Size remains separate because changing it also changes cached EOF
    /// semantics and requires the caller's inode/mapping exclusion.
    pub(super) fn refresh_attributes_from_stat(
        &self,
        attributes: &Stat,
        refresh: AttributeRefresh,
    ) {
        let inode = self.raw.as_ptr();
        // SAFETY: i_lock is the VFS synchronization point for lockless inode
        // metadata snapshots, matching the update discipline used by NFS.
        unsafe {
            bindings::spin_lock(ptr::addr_of_mut!((*inode).i_lock));
            if refresh.metadata {
                (*inode).i_mode = attributes.mode as bindings::umode_t;
                (*inode).i_uid =
                    compat::make_kuid((*(*inode).i_sb).s_user_ns, attributes.uid);
                (*inode).i_gid =
                    compat::make_kgid((*(*inode).i_sb).s_user_ns, attributes.gid);
                (*inode).i_generation = attributes.r#gen as u32;
                if refresh.link_count {
                    bindings::set_nlink(inode, attributes.nlink as ffi::c_uint);
                }
            }
            if refresh.data {
                (*inode).i_blocks = attributes.blocks as bindings::blkcnt_t;
                (*inode).i_atime_sec = attributes.atime_sec as bindings::time64_t;
                (*inode).i_atime_nsec = attributes.atime_nsec as u32;
                (*inode).i_mtime_sec = attributes.mtime_sec as bindings::time64_t;
                (*inode).i_mtime_nsec = attributes.mtime_nsec as u32;
                (*inode).i_ctime_sec = attributes.ctime_sec as bindings::time64_t;
                (*inode).i_ctime_nsec = attributes.ctime_nsec as u32;
            }
            bindings::spin_unlock(ptr::addr_of_mut!((*inode).i_lock));
        }
    }

    /// Commit set-id removals returned by a write-killpriv mutation.
    ///
    /// Netfslib invokes that setattr path while holding i_rwsem only shared.
    /// Clearing bits under i_lock is safe there and cannot replace unrelated
    /// permission bits. The caller must invalidate metadata afterward so an
    /// older clear cannot coexist with a newer cached mode observation.
    pub(super) fn apply_killpriv_mode(&self, attributes: &Stat) {
        let set_id = (bindings::S_ISUID | bindings::S_ISGID) as bindings::umode_t;
        let clear = set_id & !(attributes.mode as bindings::umode_t);
        if clear == 0 {
            return;
        }
        let inode = self.raw.as_ptr();
        // SAFETY: The inode borrow pins the object and i_lock serializes
        // lockless metadata readers plus concurrent mode publishers.
        unsafe {
            bindings::spin_lock(ptr::addr_of_mut!((*inode).i_lock));
            (*inode).i_mode &= !clear;
            bindings::spin_unlock(ptr::addr_of_mut!((*inode).i_lock));
        }
    }

    /// Refresh page-cache-sensitive size after the caller established the
    /// required inode and mapping exclusion.
    ///
    /// # Safety
    ///
    /// The caller must hold the inode/mapping locks required to serialize a
    /// regular-file size mutation against VFS and netfslib readers, and
    /// `attributes` must describe that regular file.
    pub(super) unsafe fn refresh_size_from_stat_locked(&self, attributes: &Stat) {
        let inode = self.raw.as_ptr();
        // SAFETY: The caller provides the exclusion required by
        // truncate_setsize and a validated regular-file size.
        unsafe {
            bindings::truncate_setsize(inode, attributes.size as bindings::loff_t);
        }
        self.set_remote_size(attributes.size);
    }

    /// Publish a new regular-file EOF after the mapping has been invalidated.
    ///
    /// This is the NFS-style mmap refresh case. The fault path releases its MM
    /// lock before taking the inode and mapping exclusions required here.
    ///
    /// # Safety
    ///
    /// The caller must hold the matching inode and mapping exclusions after
    /// invalidating stale cached folios.
    pub(super) unsafe fn refresh_size_after_invalidation(&self, size: u64) {
        if size > bindings::loff_t::MAX as u64 {
            return;
        }
        let inode = self.raw.as_ptr();
        // SAFETY: InodeRef keeps this ZeroFS inode live. Its allocation embeds
        // netfs_inode at offset zero, and the compatibility boundary publishes
        // both size fields with netfslib's synchronization semantics.
        unsafe {
            netfs_compat::write_local_and_remote_size(inode, size as bindings::loff_t);
        }
    }

    pub(super) fn gid(&self) -> bindings::kgid_t {
        // Ownership may be changed by setattr; take a READ_ONCE-style
        // snapshot for the create inheritance decision.
        read_once!(self.raw, i_gid)
    }

    pub(super) fn vfs_state_flags(&self) -> ffi::c_uint {
        // i_state is synchronized by VFS internals; callers use this only as
        // the same snapshot test performed by iget_locked users in C.
        const {
            assert!(
                size_of::<ffi::c_uint>() == target_vfs_layout::ZEROFS_INODE_I_STATE_SIZE as usize
            );
            assert!(
                align_of::<ffi::c_uint>() <= target_vfs_layout::ZEROFS_INODE_I_STATE_ALIGN as usize
            );
        }
        // Linux 6.18 exposes i_state as an enum-sized scalar. Linux 6.19+
        // wraps that same scalar in a one-field struct. Reading the first word
        // is the target-independent representation of inode_state_read_once().
        unsafe {
            ptr::addr_of!((*self.raw.as_ptr()).i_state)
                .cast::<ffi::c_uint>()
                .read_volatile()
        }
    }

    pub(super) fn super_block_ptr(&self) -> *mut bindings::super_block {
        // i_sb is fixed for the inode's lifetime.
        read_stable!(self.raw, i_sb)
    }

    pub(super) fn super_block(&self) -> Result<SuperBlockRef<'a>> {
        // SAFETY: The live inode pins its owning superblock.
        unsafe { SuperBlockRef::from_raw(self.super_block_ptr()) }
    }

    pub(super) fn mapping_ptr(&self) -> *mut bindings::address_space {
        // i_mapping is fixed for the inode's lifetime.
        read_stable!(self.raw, i_mapping)
    }

    pub(super) fn mapping(&self) -> Result<MappingRef<'a>> {
        Ok(MappingRef {
            raw: unsafe { CallbackRef::from_raw(self.mapping_ptr()) }.ok_or(EIO)?,
        })
    }

    pub(super) fn mount(&self) -> Result<&'a MountState> {
        self.super_block()?.mount()
    }

    pub(super) fn state(&self) -> Result<&InodeState> {
        let state = read_stable!(self.raw, i_private).cast::<InodeState>();
        if state.is_null() {
            return Err(EIO);
        }
        // SAFETY: ZeroFS installs i_private before unlocking a new inode and
        // frees it only from evict_inode after the final live reference.
        Ok(unsafe { &*state })
    }

    pub(super) fn remote_id(&self) -> Result<u64> {
        // i_ino is immutable after inode publication.
        let inode_number = read_stable!(self.raw, i_ino);
        inode_number
            .checked_sub(1)
            .map(|identifier| identifier as u64)
            .ok_or_else(super::attributes::protocol_error)
    }
}

/// A live VFS dentry borrowed from a callback.
pub(super) struct DentryRef<'a> {
    raw: CallbackRef<'a, bindings::dentry>,
}

/// Minimal dentry identity usable during either ref-walk or RCU walk.
pub(super) struct DentryIdentityRef<'a> {
    raw: CallbackRef<'a, bindings::dentry>,
}

impl<'a> DentryIdentityRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain allocated under a dentry reference or the active RCU
    /// read-side critical section for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::dentry) -> Option<Self> {
        Some(Self {
            raw: unsafe { CallbackRef::from_raw(raw) }?,
        })
    }

    pub(super) fn is_mount_root(&self) -> bool {
        // d_sb and the published s_root are immutable for this dentry's
        // lifetime and may be inspected under RCU.
        let super_block = read_stable!(self.raw, d_sb);
        !super_block.is_null()
            && unsafe { ptr::addr_of!((*super_block).s_root).read() == self.raw.as_ptr() }
    }
}

impl<'a> DentryRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain a live dentry and its attached inode, if any, must
    /// remain stabilized by the active VFS callback for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::dentry) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackRef::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::dentry {
        self.raw.as_ptr()
    }

    pub(super) fn inode(&self) -> Result<Option<InodeRef<'a>>> {
        // The constructor requires the attachment to remain stable, and a
        // non-null attachment pins its inode for the callback.
        let inode = read_once!(self.raw, d_inode);
        if inode.is_null() {
            return Ok(None);
        }
        // SAFETY: The callback's live dentry pins its attached inode.
        unsafe { InodeRef::from_raw(inode).map(Some) }
    }

    pub(super) fn state(&self) -> Option<&DentryState> {
        let state = read_stable!(self.raw, d_fsdata).cast::<DentryState>();
        // SAFETY: ZeroFS installs d_fsdata before publication and d_release
        // removes it only after the callback-held dentry reference is no
        // longer usable.
        unsafe { state.as_ref() }
    }

    pub(super) fn super_block(&self) -> Result<SuperBlockRef<'a>> {
        // d_sb is immutable for the lifetime of an instantiated dentry.
        let super_block = read_stable!(self.raw, d_sb);
        // SAFETY: The live dentry pins its owning superblock.
        unsafe { SuperBlockRef::from_raw(super_block) }
    }

    pub(super) fn has_inode(&self) -> bool {
        // The active VFS callback stabilizes the dentry attachment.
        !read_once!(self.raw, d_inode).is_null()
    }

    pub(super) fn is_parallel_lookup(&self) -> bool {
        // d_flags is concurrently inspected by namei; use READ_ONCE
        // semantics for the same snapshot test.
        let flags = read_once!(self.raw, d_flags);
        flags & bindings::dentry_flags_DCACHE_PAR_LOOKUP != 0
    }
}

/// Exclusive initialization access supplied to `d_init`.
pub(super) struct DentryInitRef<'a> {
    raw: CallbackMut<'a, bindings::dentry>,
}

impl<'a> DentryInitRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the unpublished dentry supplied exclusively to `d_init`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::dentry) -> Result<Self> {
        let raw = unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?;
        let state = read_stable!(raw, d_fsdata);
        if !state.is_null() {
            return Err(EINVAL);
        }
        Ok(Self { raw })
    }

    /// Publish the sole dentry-private allocation.
    pub(super) fn publish(self, state: Pin<KBox<DentryState>>) {
        // The constructor verified that no private allocation was already
        // installed.
        write_field!(self.raw, d_time, 0);
        write_field!(self.raw, d_fsdata, state.into_foreign());
    }
}

/// Exclusive teardown access supplied to `d_release`.
pub(super) struct DentryReleaseRef<'a> {
    raw: CallbackMut<'a, bindings::dentry>,
}

impl<'a> DentryReleaseRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the dentry supplied exclusively to its final `d_release`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::dentry) -> Option<Self> {
        Some(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }?,
        })
    }

    /// Remove and recover the dentry-private allocation, if one was installed.
    pub(super) fn take_state(self) -> Option<Pin<KBox<DentryState>>> {
        // SAFETY: d_release excludes further filesystem use. d_init stored
        // exactly this ForeignOwnable representation and the slot is cleared
        // before its sole owner is reconstructed.
        let state = read_stable!(self.raw, d_fsdata);
        write_field!(self.raw, d_fsdata, ptr::null_mut());
        if state.is_null() {
            None
        } else {
            Some(unsafe { <Pin<KBox<DentryState>> as ForeignOwnable>::from_foreign(state) })
        }
    }
}

/// A live VFS path borrowed from a callback.
pub(super) struct PathRef<'a> {
    raw: CallbackRef<'a, bindings::path>,
}

/// Exclusive cleanup slot supplied to a non-RCU `get_link` callback.
pub(super) struct DelayedCallRef<'a> {
    raw: CallbackMut<'a, bindings::delayed_call>,
}

impl<'a> DelayedCallRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the live delayed-call slot owned exclusively by the
    /// current non-RCU `get_link` callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::delayed_call) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn install_kfree(self, allocation: NonNull<ffi::c_char>) {
        // VFS invokes fn_ exactly once with arg, after it stops using the
        // returned link bytes.
        write_field!(self.raw, fn_, Some(zerofs_free_link));
        write_field!(self.raw, arg, allocation.as_ptr().cast::<ffi::c_void>());
    }
}

unsafe extern "C" fn zerofs_free_link(data: *mut ffi::c_void) {
    unsafe {
        bindings::kfree(data);
    }
}

/// Exclusive directory-emission state supplied to `iterate_shared`.
pub(super) struct DirectoryEmitContext<'a> {
    raw: CallbackMut<'a, bindings::dir_context>,
}

impl<'a> DirectoryEmitContext<'a> {
    /// # Safety
    ///
    /// `raw` must be the live directory context driven exclusively by the
    /// current `iterate_shared` callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::dir_context) -> Result<Self> {
        let raw = unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?;
        let position = read_stable!(raw, pos);
        let actor = read_stable!(raw, actor);
        if position < 0 || actor.is_none() {
            return Err(EINVAL);
        }
        Ok(Self { raw })
    }

    pub(super) fn position(&self) -> bindings::loff_t {
        // This wrapper is the exclusive driver of ctx.pos.
        read_stable!(self.raw, pos)
    }

    pub(super) fn emit(&mut self, name: &[u8], inode: u64, type_: u32) -> Result<bool> {
        if name.len() > ffi::c_int::MAX as usize {
            return Err(EOVERFLOW);
        }
        let actor = read_stable!(self.raw, actor).ok_or(EINVAL)?;
        // SAFETY: The constructor validated the actor; name remains borrowed
        // for this synchronous call and the checked length fits c_int.
        Ok(unsafe {
            actor(
                self.raw.as_ptr(),
                name.as_ptr().cast::<ffi::c_char>(),
                name.len() as ffi::c_int,
                self.position(),
                inode,
                // ZeroFS emits only the traditional DT_* file type. Linux
                // 6.19 added dt_flags_mask for optional high actor flags,
                // none of which originate in the 9P directory record.
                type_ & bindings::S_DT_MASK,
            )
        })
    }

    pub(super) fn advance(&mut self, position: u64) -> Result<()> {
        if position > bindings::loff_t::MAX as u64 {
            return Err(EOVERFLOW);
        }
        // This wrapper is the exclusive driver of ctx.pos.
        write_field!(self.raw, pos, position as bindings::loff_t);
        Ok(())
    }
}

impl<'a> PathRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain a live path for `'a`.
    pub(super) unsafe fn from_raw(raw: *const bindings::path) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackRef::from_raw(raw.cast_mut()) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn dentry(&self) -> Result<DentryRef<'a>> {
        let dentry = read_stable!(self.raw, dentry);
        // SAFETY: The callback retains the path and its referenced dentry.
        unsafe { DentryRef::from_raw(dentry) }
    }
}

/// A dentry whose name is stabilized by the active namei operation.
pub(super) struct NameDentryRef<'a> {
    dentry: DentryRef<'a>,
    name: QstrRef<'a>,
}

/// A lookup component supplied separately from a potentially renamed dentry.
pub(super) struct LookupNameRef<'a> {
    name: QstrRef<'a>,
}

/// A NUL-terminated symlink target borrowed from a VFS callback.
pub(super) struct SymlinkTargetRef<'a> {
    bytes: &'a [u8],
}

impl<'a> SymlinkTargetRef<'a> {
    /// # Safety
    ///
    /// `raw` must point to a NUL-terminated string that remains readable for
    /// `'a`.
    pub(super) unsafe fn from_raw(raw: *const ffi::c_char) -> Result<Self> {
        if raw.is_null() {
            return Err(EINVAL);
        }
        let maximum = u16::MAX as usize + 1;
        // SAFETY: The callback contract supplies a readable C string.
        let length = unsafe { bindings::strnlen(raw, maximum) } as usize;
        if length > u16::MAX as usize {
            return Err(errno!(ENAMETOOLONG));
        }
        // SAFETY: strnlen found the terminating NUL within the bound, so the
        // preceding bytes remain readable for the callback lifetime.
        let bytes = unsafe { slice::from_raw_parts(raw.cast::<u8>(), length) };
        Ok(Self { bytes })
    }

    pub(super) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

/// A qstr whose backing bytes remain stabilized by a callback contract.
struct QstrRef<'a> {
    raw: CallbackRef<'a, bindings::qstr>,
}

impl<'a> QstrRef<'a> {
    /// # Safety
    ///
    /// `raw` and its name bytes must remain readable for `'a`.
    unsafe fn from_raw(raw: *const bindings::qstr) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackRef::from_raw(raw.cast_mut()) }.ok_or(EINVAL)?,
        })
    }

    fn name(&self) -> Result<&'a [u8]> {
        // SAFETY: Construction tied the live qstr and its bytes to `'a`.
        let qstr = unsafe { self.raw.as_ptr().read() };
        // SAFETY: Reading this union member matches the target kernel's qstr
        // layout.
        let length = unsafe { qstr.__bindgen_anon_1.__bindgen_anon_1.len } as usize;
        if length == 0 || length > protocol::MAX_NAME_LEN || qstr.name.is_null() {
            return Err(errno!(ENAMETOOLONG));
        }
        // SAFETY: The constructor guarantees the name allocation for `'a`.
        let name = unsafe { slice::from_raw_parts(qstr.name, length) };
        if name.contains(&b'/') || name.contains(&b'\0') {
            return Err(errno!(ENAMETOOLONG));
        }
        Ok(name)
    }
}

impl<'a> LookupNameRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the qstr supplied to a live VFS lookup callback for `'a`.
    pub(super) unsafe fn from_raw(raw: *const bindings::qstr) -> Result<Self> {
        Ok(Self {
            name: unsafe { QstrRef::from_raw(raw)? },
        })
    }

    pub(super) fn name(&self) -> Result<&[u8]> {
        self.name.name()
    }
}

impl<'a> NameDentryRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain live and its name must be stabilized by the namei or
    /// rename locking contract for `'a`.
    pub(super) unsafe fn from_namei_raw(raw: *mut bindings::dentry) -> Result<Self> {
        // SAFETY: The stronger namei contract includes dentry liveness and
        // stabilizes its embedded qstr for the same lifetime.
        let dentry = unsafe { DentryRef::from_raw(raw)? };
        let name = unsafe { QstrRef::from_raw(ptr::addr_of!((*raw).__bindgen_anon_1.d_name))? };
        Ok(Self { dentry, name })
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::dentry {
        self.dentry.as_ptr()
    }

    pub(super) fn as_dentry(&self) -> &DentryRef<'a> {
        &self.dentry
    }

    pub(super) fn name(&self) -> Result<&[u8]> {
        self.name.name()
    }
}

/// A successfully opened ZeroFS file and its immutable private state.
pub(super) struct OpenFileRef<'a> {
    file: &'a LocalFile,
    inode: InodeRef<'a>,
    state: NonNull<FileState>,
}

/// Exclusive VMA initialization state supplied to a file `mmap` callback.
pub(super) struct MmapAreaRef<'a> {
    area: &'a VmaNew,
}

/// Validated state supplied to a mapped-file fault callback.
pub(super) struct MappedFileFault<'a> {
    raw: CallbackMut<'a, bindings::vm_fault>,
    file: OpenFileRef<'a>,
}

/// One file reference retained after a fault callback releases its VMA lock.
pub(super) struct PinnedFile {
    raw: NonNull<bindings::file>,
}

impl<'a> MmapAreaRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the live VMA initialized exclusively by the current mmap
    /// callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::vm_area_struct) -> Result<Self> {
        let raw = NonNull::new(raw).ok_or(EINVAL)?;
        // SAFETY: The mmap callback is exactly the initialization phase
        // represented by VmaNew, and keeps this VMA live for the adapter.
        Ok(Self {
            area: unsafe { VmaNew::from_raw(raw.as_ptr()) },
        })
    }

    pub(super) fn initialize_filemap(
        &mut self,
        file: &OpenFileRef<'_>,
        operations: *mut bindings::vm_operations_struct,
        private: impl ForeignOwnable,
    ) -> ffi::c_int {
        // SAFETY: Both wrappers are live for this mmap callback. Legacy mmap
        // callbacks run only after MM has failed to merge this new range, so
        // this VMA has its own private-data slot.
        let status = unsafe { bindings::generic_file_mmap(file.as_ptr(), self.area.as_ptr()) };
        if status != 0 {
            return status;
        }
        let private_slot = unsafe { ptr::addr_of_mut!((*self.area.as_ptr()).vm_private_data) };
        if !unsafe { private_slot.read() }.is_null() {
            return EBUSY.to_errno();
        }
        unsafe {
            private_slot.write(private.into_foreign());
            ptr::addr_of_mut!((*self.area.as_ptr()).vm_ops).write(operations);
        }
        0
    }
}

impl<'a> MappedFileFault<'a> {
    /// # Safety
    ///
    /// `raw` must be the live fault state supplied to a mapped-file callback;
    /// its VMA must retain the mapped ZeroFS file for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::vm_fault) -> Result<Self> {
        let raw = unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?;
        let area = read_stable!(raw, __bindgen_anon_1.vma);
        let area = NonNull::new(area).ok_or(EINVAL)?;
        let file = unsafe { ptr::addr_of!((*area.as_ptr()).vm_file).read() };
        Ok(Self {
            raw,
            file: unsafe { OpenFileRef::from_raw(file)? },
        })
    }

    pub(super) fn file(&self) -> &OpenFileRef<'a> {
        &self.file
    }

    pub(super) fn private_data(&self) -> *mut ffi::c_void {
        let area = read_stable!(self.raw, __bindgen_anon_1.vma);
        if area.is_null() {
            return ptr::null_mut();
        }
        unsafe { ptr::addr_of!((*area).vm_private_data).read() }
    }

    pub(super) fn flags(&self) -> bindings::fault_flag {
        // The MM owns this field for the callback and does not change it.
        read_stable!(self.raw, flags)
    }

    pub(super) fn run_filemap_fault(self, after_revalidation: bool) -> bindings::vm_fault_t {
        let raw = self.raw.as_ptr();
        core::mem::forget(self);
        // SAFETY: The callback retains exclusive access to the live fault
        // descriptor on entry. filemap_fault may release that protection and
        // return RETRY, so the wrapper is consumed before crossing into C.
        if after_revalidation {
            // The gate, rather than filemap, consumed the MM's first retry.
            // Let filemap treat this invocation as its own first attempt.
            unsafe { compat::filemap_fault_after_revalidation(raw) }
        } else {
            unsafe { bindings::filemap_fault(raw) }
        }
    }

    pub(super) fn run_filemap_map_pages(
        &mut self,
        start: ffi::c_ulong,
        end: ffi::c_ulong,
    ) -> bindings::vm_fault_t {
        // SAFETY: The callback retains exclusive access to the live fault
        // descriptor and supplies the map-around range.
        unsafe { bindings::filemap_map_pages(self.raw.as_ptr(), start, end) }
    }

    pub(super) fn run_netfs_page_mkwrite(
        &self,
        group: *mut netfs::netfs_group,
    ) -> bindings::vm_fault_t {
        // SAFETY: The fault state remains live for this consuming callback and
        // the selected FileState retains `group`.
        unsafe { netfs::netfs_page_mkwrite(self.raw.as_ptr(), group) }
    }

    /// Revoke this file offset from every mapping before retrying a stale
    /// write fault through the ordinary missing-page fault path.
    pub(super) fn unmap_file_page(&self) {
        let pgoff = read_stable!(self.raw, __bindgen_anon_1.pgoff);
        unsafe {
            bindings::unmap_mapping_pages(self.file.inode().mapping_ptr(), pgoff, 1, false);
        }
    }

    /// Pin the mapped file and release the callback's mmap or per-VMA lock.
    ///
    /// The consuming API prevents Rust code from touching the fault descriptor
    /// after the C compatibility helper releases the lock that kept it live.
    pub(super) fn pin_file_and_unlock(self) -> Option<PinnedFile> {
        let raw = self.raw.as_ptr();
        core::mem::forget(self);
        // SAFETY: Construction validated the live file-backed fault. The
        // caller checks the retry flags before consuming this wrapper.
        let file = unsafe { compat::pin_fault_file_and_unlock(raw) };
        NonNull::new(file).map(|raw| PinnedFile { raw })
    }
}

impl PinnedFile {
    pub(super) fn open(&self) -> Result<OpenFileRef<'_>> {
        // SAFETY: This owner retains one file reference until its Drop.
        unsafe { OpenFileRef::from_raw(self.raw.as_ptr()) }
    }
}

impl Drop for PinnedFile {
    fn drop(&mut self) {
        // SAFETY: The compatibility helper transferred exactly one get_file
        // reference to this owner.
        unsafe {
            bindings::fput(self.raw.as_ptr());
        }
    }
}

/// Exclusive access to a file before ZeroFS publishes its private state.
pub(super) struct FileOpenRef<'a> {
    file: &'a LocalFile,
}

impl<'a> FileOpenRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the live, unpublished file supplied exclusively to an
    /// `open` or `atomic_open` callback for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::file) -> Result<Self> {
        let raw = NonNull::new(raw).ok_or(EINVAL)?;
        // SAFETY: The callback keeps this unpublished file live, and no
        // fdget_pos can be active before it is installed.
        let file = unsafe { LocalFile::from_raw_file(raw.as_ptr()) };
        let private = unsafe { ptr::addr_of!((*file.as_ptr()).private_data).read() };
        if !private.is_null() {
            return Err(EINVAL);
        }
        Ok(Self { file })
    }

    pub(super) fn credential(&self) -> &Credential {
        self.file.cred()
    }

    pub(super) fn flags(&self) -> u32 {
        self.file.flags()
    }

    pub(super) fn finish_no_open(self, lookup_result: *mut bindings::dentry) -> Result<()> {
        // SAFETY: This adapter owns the unpublished file for the callback.
        to_result(unsafe { bindings::finish_no_open(self.file.as_ptr(), lookup_result) })
    }

    pub(super) fn finish_open(&self, dentry: &DentryRef<'_>) -> Result<()> {
        // SAFETY: The callback retains the file and dentry. generic_file_open
        // is the final open hook selected by this filesystem.
        to_result(unsafe {
            bindings::finish_open(
                self.file.as_ptr(),
                dentry.as_ptr(),
                Some(bindings::generic_file_open),
            )
        })
    }

    /// Transfer the initial netfs group reference into `private_data`.
    pub(super) fn publish(self, state: KBox<FileState>, created: bool) {
        // SAFETY: The constructor verified the unpublished slot was empty;
        // successful open publication is the sole writer.
        unsafe {
            ptr::addr_of_mut!((*self.file.as_ptr()).private_data).write(state.into_foreign());
            if created {
                let mode = ptr::addr_of!((*self.file.as_ptr()).f_mode).read();
                ptr::addr_of_mut!((*self.file.as_ptr()).f_mode).write(mode | super::FMODE_CREATED);
            }
        }
    }
}

/// Exclusive teardown access supplied to the final file `release`.
pub(super) struct FileReleaseRef<'a> {
    raw: CallbackMut<'a, bindings::file>,
}

impl<'a> FileReleaseRef<'a> {
    /// # Safety
    ///
    /// `raw` must be the live file supplied exclusively to its final release.
    pub(super) unsafe fn from_raw(raw: *mut bindings::file) -> Option<Self> {
        Some(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }?,
        })
    }

    /// Remove and transfer the file's initial FileState group reference.
    pub(super) fn take_state(self) -> Option<ARef<FileState>> {
        // release excludes further file operations, so this adapter has
        // exclusive teardown access to private_data.
        let state = read_stable!(self.raw, private_data);
        write_field!(self.raw, private_data, ptr::null_mut());
        NonNull::new(state.cast::<FileState>()).map(|state| {
            // SAFETY: FileOpenRef::publish stored exactly one initial ARef
            // representation in private_data, and this method cleared it.
            unsafe { ARef::from_raw(state) }
        })
    }
}

/// The target kernel's `struct file_lock_core` prefix.
///
/// `bindings::file_lock` is opaque in this target's generated bindings because
/// `<linux/filelock.h>` is not included by `bindings_helper.h`, and there is no
/// accessor API for the fields a `->lock` callback has to read. Allowlisting
/// the whole type would drag in the NFS, AFS and Ceph arms of `fl_u`; Kbuild
/// instead generates only the target's size, alignment and field offsets.
///
/// `CONFIG_RUST` depends on `!RANDSTRUCT`, so `file_lock`'s
/// `__randomize_layout` can never be in force on a kernel able to load this
/// module. The assertions below compare every accessed field with the exact
/// target headers.
#[repr(C)]
struct FileLockCoreLayout {
    // Fields this module never touches keep their kernel names behind a
    // leading underscore, so the layout stays readable without being reported
    // as unused.
    _flc_blocker: *mut FileLockCoreLayout,
    _flc_list: bindings::list_head,
    _flc_link: bindings::hlist_node,
    _flc_blocked_requests: bindings::list_head,
    _flc_blocked_member: bindings::list_head,
    _flc_owner: bindings::fl_owner_t,
    flc_flags: ffi::c_uint,
    flc_type: ffi::c_uchar,
    flc_pid: bindings::pid_t,
    _flc_link_cpu: ffi::c_int,
    _flc_wait: bindings::wait_queue_head_t,
    _flc_file: *mut bindings::file,
}

/// The prefix of `struct file_lock` this module reads and writes.
///
/// `fl_ops`, `fl_lmops` and the filesystem-private `fl_u` follow `fl_end` and
/// are never touched. No `file_lock` is ever allocated here, so the
/// declaration stops rather than restating that union.
#[repr(C)]
struct FileLockLayout {
    c: FileLockCoreLayout,
    fl_start: bindings::loff_t,
    fl_end: bindings::loff_t,
}

const _: () = {
    assert!(size_of::<FileLockLayout>() <= target_vfs_layout::ZEROFS_FILE_LOCK_SIZE as usize);
    assert!(align_of::<FileLockLayout>() <= target_vfs_layout::ZEROFS_FILE_LOCK_ALIGN as usize);
    assert!(
        size_of::<FileLockCoreLayout>() == target_vfs_layout::ZEROFS_FILE_LOCK_CORE_SIZE as usize
    );
    assert!(
        align_of::<FileLockCoreLayout>() == target_vfs_layout::ZEROFS_FILE_LOCK_CORE_ALIGN as usize
    );
    assert!(
        offset_of!(FileLockLayout, c) == target_vfs_layout::ZEROFS_FILE_LOCK_CORE_OFFSET as usize
    );
    assert!(
        offset_of!(FileLockLayout, c) + offset_of!(FileLockCoreLayout, flc_flags)
            == target_vfs_layout::ZEROFS_FILE_LOCK_FLC_FLAGS_OFFSET as usize
    );
    assert!(
        offset_of!(FileLockLayout, c) + offset_of!(FileLockCoreLayout, flc_type)
            == target_vfs_layout::ZEROFS_FILE_LOCK_FLC_TYPE_OFFSET as usize
    );
    assert!(
        offset_of!(FileLockLayout, c) + offset_of!(FileLockCoreLayout, flc_pid)
            == target_vfs_layout::ZEROFS_FILE_LOCK_FLC_PID_OFFSET as usize
    );
    assert!(
        offset_of!(FileLockLayout, fl_start)
            == target_vfs_layout::ZEROFS_FILE_LOCK_FL_START_OFFSET as usize
    );
    assert!(
        offset_of!(FileLockLayout, fl_end)
            == target_vfs_layout::ZEROFS_FILE_LOCK_FL_END_OFFSET as usize
    );
    assert!(
        size_of::<ffi::c_uint>() == target_vfs_layout::ZEROFS_FILE_LOCK_FLC_FLAGS_SIZE as usize
    );
    assert!(
        size_of::<ffi::c_uchar>() == target_vfs_layout::ZEROFS_FILE_LOCK_FLC_TYPE_SIZE as usize
    );
    assert!(
        size_of::<bindings::pid_t>() == target_vfs_layout::ZEROFS_FILE_LOCK_FLC_PID_SIZE as usize
    );
    assert!(
        size_of::<bindings::loff_t>() == target_vfs_layout::ZEROFS_FILE_LOCK_FL_START_SIZE as usize
    );
    assert!(
        size_of::<bindings::loff_t>() == target_vfs_layout::ZEROFS_FILE_LOCK_FL_END_SIZE as usize
    );
    assert!(target_vfs_layout::ZEROFS_POSIX_TEST_LOCK_SIGNATURE == 1);
    assert!(target_vfs_layout::ZEROFS_LOCKS_LOCK_INODE_WAIT_SIGNATURE == 1);
};

const FL_FLOCK: ffi::c_uint = target_vfs_layout::ZEROFS_FL_FLOCK;
const FL_CLOSE: ffi::c_uint = target_vfs_layout::ZEROFS_FL_CLOSE;
/// The `fl_end` sentinel for a range that runs to end of file.
const OFFSET_MAX: bindings::loff_t = bindings::loff_t::MAX;

// Both signatures name kernel types whose lockdep members are empty structs.
#[allow(improper_ctypes)]
unsafe extern "C" {
    /// Overwrite `fl`'s type, range and pid with the local POSIX lock that
    /// would conflict with it, or set its type to `F_UNLCK`.
    fn posix_test_lock(filp: *mut bindings::file, fl: *mut FileLockLayout);
    /// Apply `fl` to the inode's own lock state, waiting on its queue when the
    /// request carries `FL_SLEEP`. `locks_lock_file_wait` is a static inline
    /// over this exported symbol, so the inode form is called directly.
    fn locks_lock_inode_wait(inode: *mut bindings::inode, fl: *mut FileLockLayout) -> ffi::c_int;
}

/// The lock request a `->lock` or `->flock` callback operates on.
pub(super) struct FileLockRef<'a> {
    raw: CallbackMut<'a, FileLockLayout>,
}

/// The inclusive `[fl_start, fl_end]` a request named, as the kernel spells it.
///
/// A local grant may rewrite those fields, so the caller of a rollback carries
/// the range it asked for rather than reading it back.
#[derive(Clone, Copy)]
pub(super) struct RawLockRange {
    start: bindings::loff_t,
    end: bindings::loff_t,
}

impl FileLockRef<'_> {
    /// # Safety
    ///
    /// `raw` must be the live file lock the VFS lends to the current
    /// `->lock`/`->flock` callback, mutable and unaliased for the borrow.
    pub(super) unsafe fn from_raw(raw: *mut bindings::file_lock) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackMut::from_raw(raw.cast::<FileLockLayout>()) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn lock_type(&self) -> u8 {
        // The calling task is this request's only mutator.
        read_stable!(self.raw, c.flc_type)
    }

    pub(super) fn set_lock_type(&mut self, lock_type: u8) {
        write_field!(self.raw, c.flc_type, lock_type);
    }

    pub(super) fn pid(&self) -> bindings::pid_t {
        read_stable!(self.raw, c.flc_pid)
    }

    pub(super) fn set_pid(&mut self, pid: bindings::pid_t) {
        write_field!(self.raw, c.flc_pid, pid);
    }

    fn flags(&self) -> ffi::c_uint {
        read_stable!(self.raw, c.flc_flags)
    }

    pub(super) fn is_flock(&self) -> bool {
        self.flags() & FL_FLOCK != 0
    }

    /// Whether this request is the release the VFS synthesizes at close.
    pub(super) fn is_close(&self) -> bool {
        self.flags() & FL_CLOSE != 0
    }

    /// The inclusive `[fl_start, fl_end]` range as 9P `start` and `length`,
    /// where length zero means to end of file.
    ///
    /// `fl_start` is signed on the kernel side and unsigned on the wire, so a
    /// negative or inverted range is rejected rather than cast into a huge
    /// offset the server would happily lock.
    pub(super) fn range_9p(&self) -> Result<LockRange> {
        let (start, end) = (
            read_stable!(self.raw, fl_start),
            read_stable!(self.raw, fl_end),
        );
        if start < 0 || end < start {
            return Err(EINVAL);
        }
        let length = if end == OFFSET_MAX {
            0
        } else {
            end as u64 - start as u64 + 1
        };
        Ok(LockRange {
            start: start as u64,
            length,
        })
    }

    /// The owner identity this request is taken under.
    pub(super) fn owner(&self) -> LockOwner {
        LockOwner {
            proc_id: self.pid() as u32,
            flock: self.is_flock(),
        }
    }

    /// Inverse of [`FileLockRef::range_9p`], for the holder a reply reports.
    pub(super) fn set_range_9p(&mut self, start: u64, length: u64) -> Result<()> {
        if start > OFFSET_MAX as u64 {
            return Err(EINVAL);
        }
        let end = if length == 0 {
            OFFSET_MAX
        } else {
            let last = start.checked_add(length - 1).ok_or(EINVAL)?;
            if last > OFFSET_MAX as u64 {
                return Err(EINVAL);
            }
            last as bindings::loff_t
        };
        write_field!(self.raw, fl_start, start as bindings::loff_t);
        write_field!(self.raw, fl_end, end);
        Ok(())
    }

    /// Apply this request to the VFS's own lock state for `inode`.
    pub(super) fn local_apply(&mut self, inode: &InodeRef<'_>) -> Result<()> {
        // SAFETY: The callback retains both the inode and the request, and the
        // exclusive borrow means nothing else is mutating the latter.
        to_result(unsafe { locks_lock_inode_wait(inode.as_ptr(), self.raw.as_ptr()) })
    }

    /// Fill this request in from the conflicting local POSIX lock, or set its
    /// type to `F_UNLCK` when the range is free on this mount.
    pub(super) fn local_test(&mut self, file: &OpenFileRef<'_>) {
        // SAFETY: The callback retains both the file and the request, and the
        // exclusive borrow means nothing else is mutating the latter.
        unsafe { posix_test_lock(file.as_ptr(), self.raw.as_ptr()) }
    }

    /// The range this request currently names.
    pub(super) fn raw_range(&self) -> RawLockRange {
        RawLockRange {
            start: read_stable!(self.raw, fl_start),
            end: read_stable!(self.raw, fl_end),
        }
    }

    fn set_raw_range(&mut self, range: RawLockRange) {
        write_field!(self.raw, fl_start, range.start);
        write_field!(self.raw, fl_end, range.end);
    }

    /// Undo a local grant the server then refused, restoring the requested
    /// type so the caller still reports the remote error.
    ///
    /// `range` is what the caller asked for. `posix_lock_inode` merges a grant
    /// with an adjacent or overlapping same-type lock of the same owner by
    /// widening the request in place, so releasing whatever the struct names by
    /// then would take back a lock the caller held before this request and
    /// still holds, locally and on the server.
    ///
    /// Releasing the requested range is exact for a merge with an adjacent
    /// lock, which is the reachable case. An overlapping one still loses the
    /// intersection: the union has replaced both locks and nothing here knows
    /// what the earlier one covered, so its overlap cannot be reinstated. That
    /// residue is smaller than the whole union `fs/9p` releases.
    pub(super) fn revert_local(&mut self, inode: &InodeRef<'_>, range: RawLockRange) {
        let requested = self.lock_type();
        self.set_lock_type(bindings::F_UNLCK as u8);
        self.set_raw_range(range);
        // The remote error is what the caller reports either way, and a failed
        // revert can only leave this mount over-excluding its own processes.
        let _ = self.local_apply(inode);
        self.set_lock_type(requested);
    }
}

/// The normalized attribute request supplied to a filesystem `setattr`.
pub(super) struct SetattrRequest<'a> {
    raw: CallbackMut<'a, bindings::iattr>,
}

/// Exclusive output storage supplied to a VFS `getattr` callback.
pub(super) struct KstatOut<'a> {
    raw: CallbackMut<'a, bindings::kstat>,
}

/// Semantic fields written by a filesystem `statfs` callback.
pub(super) struct KstatfsValues {
    pub filesystem_type: ffi::c_long,
    pub block_size: ffi::c_long,
    pub blocks: u64,
    pub blocks_free: u64,
    pub blocks_available: u64,
    pub files: u64,
    pub files_free: u64,
    pub filesystem_id: u64,
    pub name_length: ffi::c_long,
    pub fragment_size: ffi::c_long,
    pub flags: ffi::c_long,
}

/// The target kernel's `struct kstatfs`, opaque in the canonical bindings.
#[repr(C)]
struct KstatfsLayout {
    f_type: ffi::c_long,
    f_bsize: ffi::c_long,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    f_files: u64,
    f_ffree: u64,
    f_fsid: bindings::__kernel_fsid_t,
    f_namelen: ffi::c_long,
    f_frsize: ffi::c_long,
    f_flags: ffi::c_long,
    f_spare: [ffi::c_long; 4],
}

const _: () = {
    assert!(size_of::<KstatfsLayout>() == target_vfs_layout::ZEROFS_KSTATFS_SIZE as usize);
    assert!(align_of::<KstatfsLayout>() == target_vfs_layout::ZEROFS_KSTATFS_ALIGN as usize);
    assert!(size_of::<ffi::c_long>() == target_vfs_layout::ZEROFS_KSTATFS_F_TYPE_SIZE as usize);
    assert!(size_of::<ffi::c_long>() == target_vfs_layout::ZEROFS_KSTATFS_F_BSIZE_SIZE as usize);
    assert!(size_of::<u64>() == target_vfs_layout::ZEROFS_KSTATFS_F_BLOCKS_SIZE as usize);
    assert!(size_of::<u64>() == target_vfs_layout::ZEROFS_KSTATFS_F_BFREE_SIZE as usize);
    assert!(size_of::<u64>() == target_vfs_layout::ZEROFS_KSTATFS_F_BAVAIL_SIZE as usize);
    assert!(size_of::<u64>() == target_vfs_layout::ZEROFS_KSTATFS_F_FILES_SIZE as usize);
    assert!(size_of::<u64>() == target_vfs_layout::ZEROFS_KSTATFS_F_FFREE_SIZE as usize);
    assert!(
        size_of::<bindings::__kernel_fsid_t>()
            == target_vfs_layout::ZEROFS_KSTATFS_F_FSID_SIZE as usize
    );
    assert!(size_of::<ffi::c_long>() == target_vfs_layout::ZEROFS_KSTATFS_F_NAMELEN_SIZE as usize);
    assert!(size_of::<ffi::c_long>() == target_vfs_layout::ZEROFS_KSTATFS_F_FRSIZE_SIZE as usize);
    assert!(size_of::<ffi::c_long>() == target_vfs_layout::ZEROFS_KSTATFS_F_FLAGS_SIZE as usize);
    assert!(
        size_of::<[ffi::c_long; 4]>() == target_vfs_layout::ZEROFS_KSTATFS_F_SPARE_SIZE as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_type)
            == target_vfs_layout::ZEROFS_KSTATFS_F_TYPE_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_bsize)
            == target_vfs_layout::ZEROFS_KSTATFS_F_BSIZE_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_blocks)
            == target_vfs_layout::ZEROFS_KSTATFS_F_BLOCKS_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_bfree)
            == target_vfs_layout::ZEROFS_KSTATFS_F_BFREE_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_bavail)
            == target_vfs_layout::ZEROFS_KSTATFS_F_BAVAIL_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_files)
            == target_vfs_layout::ZEROFS_KSTATFS_F_FILES_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_ffree)
            == target_vfs_layout::ZEROFS_KSTATFS_F_FFREE_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_fsid)
            == target_vfs_layout::ZEROFS_KSTATFS_F_FSID_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_namelen)
            == target_vfs_layout::ZEROFS_KSTATFS_F_NAMELEN_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_frsize)
            == target_vfs_layout::ZEROFS_KSTATFS_F_FRSIZE_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_flags)
            == target_vfs_layout::ZEROFS_KSTATFS_F_FLAGS_OFFSET as usize
    );
    assert!(
        offset_of!(KstatfsLayout, f_spare)
            == target_vfs_layout::ZEROFS_KSTATFS_F_SPARE_OFFSET as usize
    );
};

/// Exclusive output storage supplied to a VFS `statfs` callback.
pub(super) struct KstatfsOut<'a> {
    raw: CallbackMut<'a, bindings::kstatfs>,
}

impl KstatfsOut<'_> {
    /// # Safety
    ///
    /// `raw` must be the live, exclusively writable output supplied to the
    /// current `statfs` callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::kstatfs) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn write(self, values: KstatfsValues) {
        let result = KstatfsLayout {
            f_type: values.filesystem_type,
            f_bsize: values.block_size,
            f_blocks: values.blocks,
            f_bfree: values.blocks_free,
            f_bavail: values.blocks_available,
            f_files: values.files,
            f_ffree: values.files_free,
            f_fsid: bindings::__kernel_fsid_t {
                val: [
                    values.filesystem_id as u32 as ffi::c_int,
                    (values.filesystem_id >> 32) as u32 as ffi::c_int,
                ],
            },
            f_namelen: values.name_length,
            f_frsize: values.fragment_size,
            f_flags: values.flags,
            f_spare: [0; 4],
        };
        // SAFETY: The target-checked layout matches the callback output, and
        // this wrapper owns its exclusive callback borrow.
        unsafe {
            self.raw.as_ptr().cast::<KstatfsLayout>().write(result);
        }
    }
}

impl<'a> KstatOut<'a> {
    /// # Safety
    ///
    /// `raw` must be the live, exclusively writable output supplied to the
    /// current `getattr` callback.
    pub(super) unsafe fn from_raw(raw: *mut bindings::kstat) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn as_ptr(&mut self) -> *mut bindings::kstat {
        self.raw.as_ptr()
    }

    pub(super) fn as_mut(&mut self) -> &mut bindings::kstat {
        // SAFETY: The constructor establishes exclusive output access for the
        // wrapper lifetime.
        unsafe { &mut *self.raw.as_ptr() }
    }
}

impl<'a> SetattrRequest<'a> {
    /// # Safety
    ///
    /// `raw` must be the live, exclusively owned iattr for a `setattr`
    /// callback and remain valid for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::iattr) -> Result<Self> {
        Ok(Self {
            raw: unsafe { CallbackMut::from_raw(raw) }.ok_or(EINVAL)?,
        })
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::iattr {
        self.raw.as_ptr()
    }

    pub(super) fn valid(&self) -> ffi::c_uint {
        read_stable!(self.raw, ia_valid)
    }

    pub(super) fn mode(&self) -> u32 {
        read_stable!(self.raw, ia_mode) as u32
    }

    pub(super) fn vfsuid(&self) -> bindings::vfsuid_t {
        // The caller consults this union member only when ATTR_UID is set.
        read_stable!(self.raw, __bindgen_anon_1.ia_vfsuid)
    }

    pub(super) fn vfsgid(&self) -> bindings::vfsgid_t {
        // The caller consults this union member only when ATTR_GID is set.
        read_stable!(self.raw, __bindgen_anon_2.ia_vfsgid)
    }

    pub(super) fn size(&self) -> bindings::loff_t {
        read_stable!(self.raw, ia_size)
    }

    pub(super) fn atime(&self) -> bindings::timespec64 {
        read_stable!(self.raw, ia_atime)
    }

    pub(super) fn mtime(&self) -> bindings::timespec64 {
        read_stable!(self.raw, ia_mtime)
    }

    pub(super) fn file_ptr(&self) -> *mut bindings::file {
        read_stable!(self.raw, ia_file)
    }
}

impl<'a> OpenFileRef<'a> {
    /// # Safety
    ///
    /// `raw` must be a live, successfully opened ZeroFS file for `'a`.
    pub(super) unsafe fn from_raw(raw: *mut bindings::file) -> Result<Self> {
        let raw = NonNull::new(raw).ok_or(EINVAL)?;
        // SAFETY: The callback holds a live file reference. Any unlocked
        // fdget_pos operation on an unshared file belongs to this task.
        let file = unsafe { LocalFile::from_raw_file(raw.as_ptr()) };
        // SAFETY: The caller guarantees a live opened file.
        let inode = NonNull::new(unsafe { ptr::addr_of!((*file.as_ptr()).f_inode).read() })
            .ok_or(EINVAL)?;
        // SAFETY: ZeroFS publishes private_data before a successful open
        // becomes visible and clears it only in the final release callback.
        let state = NonNull::new(
            unsafe { ptr::addr_of!((*file.as_ptr()).private_data).read() }.cast::<FileState>(),
        )
        .ok_or(EBADF)?;
        Ok(Self {
            file,
            inode: unsafe { InodeRef::from_raw(inode.as_ptr())? },
            state,
        })
    }

    /// Validate the open file attached to an ATTR_FILE setattr request.
    ///
    /// # Safety
    ///
    /// `raw` must be null or a live file retained by the callback for `'a`.
    pub(super) unsafe fn from_setattr_raw(
        raw: *mut bindings::file,
        expected_inode: &InodeRef<'a>,
    ) -> Result<Self> {
        let raw = NonNull::new(raw).ok_or(EINVAL)?;
        // SAFETY: The setattr callback retains the supplied file, and any
        // unlocked fdget_pos is confined to this calling task.
        let file = unsafe { LocalFile::from_raw_file(raw.as_ptr()) };
        let inode = unsafe { ptr::addr_of!((*file.as_ptr()).f_inode).read() };
        if inode != expected_inode.as_ptr() {
            return Err(EINVAL);
        }
        let state = NonNull::new(
            unsafe { ptr::addr_of!((*file.as_ptr()).private_data).read() }.cast::<FileState>(),
        )
        .ok_or(EBADF)?;
        Ok(Self {
            file,
            inode: unsafe { InodeRef::from_raw(inode)? },
            state,
        })
    }

    pub(super) fn as_ptr(&self) -> *mut bindings::file {
        self.file.as_ptr()
    }

    pub(super) fn inode(&self) -> &InodeRef<'a> {
        &self.inode
    }

    pub(super) fn state(&self) -> &FileState {
        // SAFETY: The open file owns a group reference to this FileState.
        unsafe { self.state.as_ref() }
    }

    pub(super) fn flags(&self) -> u32 {
        self.file.flags()
    }

    pub(super) fn mode(&self) -> bindings::fmode_t {
        // f_mode is immutable after the file has been opened.
        unsafe { ptr::addr_of!((*self.file.as_ptr()).f_mode).read_volatile() }
    }

    pub(super) fn write_and_wait_range(
        &self,
        start: bindings::loff_t,
        end: bindings::loff_t,
    ) -> ffi::c_int {
        // SAFETY: This wrapper keeps the open file live for the synchronous
        // writeback wait.
        unsafe { bindings::file_write_and_wait_range(self.file.as_ptr(), start, end) }
    }

    pub(super) fn take_writeback_error(&self) -> ffi::c_int {
        // SAFETY: This operation advances only this open file's wb_err cursor.
        unsafe { bindings::file_check_and_advance_wb_err(self.file.as_ptr()) }
    }

    pub(super) fn accessed(&self) {
        // SAFETY: This adapter retains the open file for the call.
        unsafe { compat::file_accessed(self.file.as_ptr()) };
    }
}

pub(super) struct ReadCall<'a> {
    iocb: Kiocb<'a, KBox<FileState>>,
    destination: &'a mut IovIterDest<'a>,
    file: OpenFileRef<'a>,
    flags: ffi::c_int,
}

pub(super) struct PinnedUserIter {
    raw: bindings::iov_iter,
    release: bindings::iov_iter,
    dirty_bytes: usize,
}

impl PinnedUserIter {
    /// Extract and pin up to `limit` bytes from a user-backed iterator.
    ///
    /// The original iterator is copied rather than advanced. A fault after a
    /// nonempty prefix yields a shorter pinned iterator, preserving ordinary
    /// partial-I/O behavior.
    unsafe fn extract(original: *mut bindings::iov_iter, limit: usize) -> Result<Option<Self>> {
        let iter_type = unsafe { ptr::addr_of!((*original).iter_type).read() };
        if iter_type != bindings::iter_type_ITER_UBUF as u8
            && iter_type != bindings::iter_type_ITER_IOVEC as u8
        {
            return Ok(None);
        }
        if limit == 0 {
            return Err(EINVAL);
        }

        let mut scratch = MaybeUninit::<bindings::iov_iter>::uninit();
        // SAFETY: iov_iter is a C value type. The copied iterator is advanced
        // only by the extraction helper and does not own its iovec array.
        unsafe {
            ptr::copy_nonoverlapping(original, scratch.as_mut_ptr(), 1);
            // netfs_extract_user_iter sizes its bvec allocation from the
            // iterator, not from its separate length argument.
            compat::iov_iter_truncate(scratch.as_mut_ptr(), limit);
        }
        let mut extracted = MaybeUninit::<bindings::iov_iter>::uninit();
        let segments = unsafe {
            netfs::netfs_extract_user_iter(scratch.as_mut_ptr(), limit, extracted.as_mut_ptr(), 0)
        };
        if segments < 0 {
            return Err(Error::from_errno(segments as ffi::c_int));
        }
        // netfs_extract_user_iter() initializes the output bvec iterator for
        // every nonnegative return, including a zero-page extraction.
        let raw = unsafe { extracted.assume_init() };
        let pinned = Self {
            // Keep an unadvanced copy solely for releasing every page and the
            // original bvec allocation after netfslib has consumed `raw`.
            release: unsafe { ptr::read(ptr::addr_of!(raw)) },
            raw,
            dirty_bytes: 0,
        };
        if segments == 0 {
            drop(pinned);
            return Err(EFAULT);
        }
        if pinned.len() == 0 {
            // Keep the cleanup invariant even if a target kernel violates its
            // positive-result contract.
            drop(pinned);
            return Err(EIO);
        }
        Ok(Some(pinned))
    }

    fn as_mut_ptr(&mut self) -> *mut bindings::iov_iter {
        ptr::addr_of_mut!(self.raw)
    }

    fn len(&self) -> usize {
        // SAFETY: `raw` remains an initialized live iterator.
        unsafe { compat::iov_iter_count(ptr::addr_of!(self.raw)) }
    }

    fn mark_dirty(&mut self, bytes: usize) {
        self.dirty_bytes = self.dirty_bytes.max(bytes);
    }
}

impl Drop for PinnedUserIter {
    fn drop(&mut self) {
        // SAFETY: This owner is created only by one successful extraction and
        // is dropped only after netfslib has stopped using its bvec.
        unsafe {
            compat::release_pinned_iov_iter(ptr::addr_of_mut!(self.release), self.dirty_bytes);
        }
    }
}

type NetfsIterOperation =
    unsafe extern "C" fn(iocb: *mut bindings::kiocb, iter: *mut bindings::iov_iter) -> isize;

/// Run extracted user I/O through a callback-scoped copied kiocb.
///
/// Suppressing `ki_complete` makes direct I/O synchronous. Buffered reads may
/// still use IOCB_WAITQ, but that path returns EIOCBQUEUED before copying and
/// retains only the caller-owned wait entry, not this kiocb or iterator.
unsafe fn run_scoped_pinned_io(
    original: *mut bindings::kiocb,
    iterator: *mut bindings::iov_iter,
    operation: NetfsIterOperation,
) -> isize {
    let mut iocb = unsafe { ptr::read(original) };
    iocb.ki_complete = None;
    let result = unsafe { operation(ptr::addr_of_mut!(iocb), iterator) };
    unsafe {
        ptr::addr_of_mut!((*original).ki_pos).write(iocb.ki_pos);
    }
    result
}

unsafe fn run_sync_pinned_buffered_write(
    original: *mut bindings::kiocb,
    iterator: *mut bindings::iov_iter,
    group: *mut netfs::netfs_group,
) -> isize {
    let mut iocb = unsafe { ptr::read(original) };
    iocb.ki_complete = None;
    let result = unsafe {
        netfs::netfs_buffered_write_iter_locked(ptr::addr_of_mut!(iocb), iterator, group)
    };
    unsafe {
        ptr::addr_of_mut!((*original).ki_pos).write(iocb.ki_pos);
    }
    result
}

impl<'a> ReadCall<'a> {
    /// Validate the raw objects supplied to a ZeroFS `read_iter` callback.
    ///
    /// # Safety
    ///
    /// The pointers must obey the VFS iterator callback contract for `'a`.
    pub(super) unsafe fn from_raw(
        iocb: *mut bindings::kiocb,
        destination: *mut bindings::iov_iter,
    ) -> Result<Self> {
        if iocb.is_null() || destination.is_null() {
            return Err(EINVAL);
        }
        // SAFETY: The callback contract guarantees a live kiocb and file.
        let file = unsafe { OpenFileRef::from_raw((*iocb).ki_filp)? };
        // SAFETY: The caller guarantees the kiocb/private-data contract.
        let iocb = unsafe { Kiocb::<KBox<FileState>>::from_raw(iocb) };
        // SAFETY: read_iter receives an ITER_DEST iterator exclusively.
        let destination = unsafe { IovIterDest::from_raw(destination) };
        // SAFETY: The callback owns access to ki_flags for this operation.
        let flags = unsafe { (*iocb.as_raw()).ki_flags };
        Ok(Self {
            iocb,
            destination,
            file,
            flags,
        })
    }

    pub(super) fn inode(&self) -> &InodeRef<'a> {
        self.file.inode()
    }

    pub(super) fn state(&self) -> &FileState {
        self.file.state()
    }

    pub(super) fn count(&self) -> usize {
        self.destination.len()
    }

    pub(super) fn position(&self) -> i64 {
        self.iocb.ki_pos()
    }

    pub(super) fn flags(&self) -> ffi::c_int {
        self.flags
    }

    pub(super) fn accessed(&self) {
        self.file.accessed();
    }

    pub(super) fn pin_destination(&mut self, limit: usize) -> Result<Option<PinnedUserIter>> {
        // SAFETY: This adapter exclusively owns the live destination iterator.
        unsafe { PinnedUserIter::extract(self.destination.as_raw(), limit) }
    }

    fn copy_destination(&mut self) -> bindings::iov_iter {
        let mut copy = MaybeUninit::<bindings::iov_iter>::uninit();
        // SAFETY: iov_iter is a C value type. The copy borrows the same backing
        // storage and is consumed only while the original callback is live.
        unsafe {
            ptr::copy_nonoverlapping(self.destination.as_raw(), copy.as_mut_ptr(), 1);
            copy.assume_init()
        }
    }

    pub(super) fn run_netfs(&mut self, direct: bool, pinned: Option<PinnedUserIter>) -> isize {
        let Some(mut pinned) = pinned else {
            if !direct {
                // SAFETY: The adapter retains a valid kiocb and exclusive
                // non-user iterator for the duration required by its caller.
                return unsafe {
                    netfs::netfs_file_read_iter(self.iocb.as_raw(), self.destination.as_raw())
                };
            }

            // Netfslib advances a non-user direct-read iterator by the whole
            // submission before completion. Give it a callback-scoped copy so
            // a short or failed synchronous read advances the original only
            // by the bytes reported to its caller.
            let before = self.destination.len();
            let mut destination = self.copy_destination();
            let result = unsafe {
                netfs::netfs_unbuffered_read_iter(
                    self.iocb.as_raw(),
                    ptr::addr_of_mut!(destination),
                )
            };
            if result > 0 {
                self.destination
                    .advance((result as usize).min(self.destination.len()));
            } else if result == -(bindings::EIOCBQUEUED as isize) {
                let remaining = unsafe { compat::iov_iter_count(ptr::addr_of!(destination)) };
                let submitted = before.saturating_sub(remaining);
                self.destination
                    .advance(submitted.min(self.destination.len()));
            }
            return result;
        };

        let submitted = pinned.len();
        let result = unsafe {
            if direct {
                run_scoped_pinned_io(
                    self.iocb.as_raw(),
                    pinned.as_mut_ptr(),
                    netfs::netfs_unbuffered_read_iter,
                )
            } else {
                run_scoped_pinned_io(
                    self.iocb.as_raw(),
                    pinned.as_mut_ptr(),
                    netfs::netfs_file_read_iter,
                )
            }
        };
        if result > 0 {
            self.destination.advance(result as usize);
        }
        if direct && result < 0 {
            // A direct request error can mask a transferred prefix.
            // Conservatively dirty every submitted destination page.
            pinned.mark_dirty(submitted);
        } else if result > 0 {
            pinned.mark_dirty(result as usize);
        }
        result
    }
}

pub(super) struct WriteCall<'a> {
    iocb: Kiocb<'a, KBox<FileState>>,
    source: &'a mut IovIterSource<'a>,
    file: OpenFileRef<'a>,
    flags: ffi::c_int,
    asynchronous: bool,
}

impl<'a> WriteCall<'a> {
    /// Validate the raw objects supplied to a ZeroFS `write_iter` callback.
    ///
    /// # Safety
    ///
    /// The pointers must obey the VFS iterator callback contract for `'a`.
    pub(super) unsafe fn from_raw(
        iocb: *mut bindings::kiocb,
        source: *mut bindings::iov_iter,
    ) -> Result<Self> {
        if iocb.is_null() || source.is_null() {
            return Err(EINVAL);
        }
        // SAFETY: The callback contract guarantees a live kiocb and file.
        let file = unsafe { OpenFileRef::from_raw((*iocb).ki_filp)? };
        // SAFETY: The callback owns access to these operation fields.
        let (flags, asynchronous) = unsafe { ((*iocb).ki_flags, (*iocb).ki_complete.is_some()) };
        // SAFETY: The caller guarantees the kiocb/private-data contract.
        let iocb = unsafe { Kiocb::<KBox<FileState>>::from_raw(iocb) };
        // SAFETY: write_iter receives an ITER_SOURCE iterator exclusively.
        let source = unsafe { IovIterSource::from_raw(source) };
        Ok(Self {
            iocb,
            source,
            file,
            flags,
            asynchronous,
        })
    }

    pub(super) fn inode(&self) -> &InodeRef<'a> {
        self.file.inode()
    }

    pub(super) fn state(&self) -> &FileState {
        self.file.state()
    }

    pub(super) fn count(&self) -> usize {
        self.source.len()
    }

    pub(super) fn position(&self) -> i64 {
        self.iocb.ki_pos()
    }

    pub(super) fn flags(&self) -> ffi::c_int {
        self.flags
    }

    pub(super) fn is_asynchronous(&self) -> bool {
        self.asynchronous
    }

    pub(super) fn pin_source(&mut self, limit: usize) -> Result<Option<PinnedUserIter>> {
        // SAFETY: This adapter exclusively owns the live source iterator.
        unsafe { PinnedUserIter::extract(self.source.as_raw(), limit) }
    }

    fn copy_source(&mut self) -> bindings::iov_iter {
        let mut copy = MaybeUninit::<bindings::iov_iter>::uninit();
        // SAFETY: iov_iter is a C value type. The copy borrows the same backing
        // storage and is consumed only while the original callback is live.
        unsafe {
            ptr::copy_nonoverlapping(self.source.as_raw(), copy.as_mut_ptr(), 1);
            copy.assume_init()
        }
    }

    pub(super) fn generic_write_checks(
        &mut self,
        mut pinned: Option<&mut PinnedUserIter>,
    ) -> isize {
        let source = match pinned.as_mut() {
            Some(pinned) => pinned.as_mut_ptr(),
            None => self.source.as_raw(),
        };
        // SAFETY: The adapter exclusively owns the source iterator and VFS
        // permits generic_write_checks to update this kiocb.
        unsafe { bindings::generic_write_checks(self.iocb.as_raw(), source) }
    }

    pub(super) fn generic_write_check_count(&mut self) -> isize {
        let mut count = self.count() as bindings::loff_t;
        // SAFETY: The adapter owns the kiocb for this callback and count is
        // independent of the user iterator that will be pinned next.
        let status =
            unsafe { bindings::generic_write_checks_count(self.iocb.as_raw(), &mut count) };
        if status < 0 {
            status as isize
        } else {
            count as isize
        }
    }

    pub(super) fn run_netfs_unbuffered(
        &mut self,
        pinned: Option<PinnedUserIter>,
        force_synchronous: bool,
    ) -> isize {
        let Some(mut pinned) = pinned else {
            // Netfslib truncates but does not advance a non-user unbuffered
            // iterator. Give it a callback-scoped copy, then reconcile the
            // original explicitly without mistaking truncation for progress.
            let mut source = self.copy_source();
            let result = unsafe {
                if force_synchronous {
                    run_scoped_pinned_io(
                        self.iocb.as_raw(),
                        ptr::addr_of_mut!(source),
                        netfs::netfs_unbuffered_write_iter,
                    )
                } else {
                    netfs::netfs_unbuffered_write_iter(
                        self.iocb.as_raw(),
                        ptr::addr_of_mut!(source),
                    )
                }
            };
            if result > 0 {
                self.source
                    .advance((result as usize).min(self.source.len()));
            } else if result == -(bindings::EIOCBQUEUED as isize) {
                let submitted = unsafe { compat::iov_iter_count(ptr::addr_of!(source)) };
                self.source.advance(submitted.min(self.source.len()));
            }
            return result;
        };

        let result = unsafe {
            run_scoped_pinned_io(
                self.iocb.as_raw(),
                pinned.as_mut_ptr(),
                netfs::netfs_unbuffered_write_iter,
            )
        };
        if result > 0 {
            self.source.advance(result as usize);
        }
        result
    }

    pub(super) fn run_netfs_buffered(
        &mut self,
        mut pinned: Option<&mut PinnedUserIter>,
        group: *mut netfs::netfs_group,
        force_synchronous: bool,
    ) -> isize {
        let Some(pinned) = pinned.as_mut() else {
            // SAFETY: The selected FileState retains `group`; the adapter
            // retains the kiocb and non-user source iterator for this call.
            let before = self.source.len();
            let result = unsafe {
                if force_synchronous {
                    run_sync_pinned_buffered_write(self.iocb.as_raw(), self.source.as_raw(), group)
                } else {
                    netfs::netfs_buffered_write_iter_locked(
                        self.iocb.as_raw(),
                        self.source.as_raw(),
                        group,
                    )
                }
            };
            if result > 0 {
                let target = (result as usize).min(before);
                let consumed = before.saturating_sub(self.source.len());
                if consumed < target {
                    self.source.advance(target - consumed);
                }
            }
            return result;
        };
        let result = unsafe {
            run_sync_pinned_buffered_write(self.iocb.as_raw(), pinned.as_mut_ptr(), group)
        };
        if result > 0 {
            self.source.advance(result as usize);
        }
        result
    }

    pub(super) fn fsync_range(&self, start: i64, end: i64, datasync: ffi::c_int) -> ffi::c_int {
        // SAFETY: The VFS callback retains the open file through this call.
        unsafe { bindings::vfs_fsync_range(self.file.as_ptr(), start, end, datasync) }
    }
}
