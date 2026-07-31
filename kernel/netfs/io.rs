//! Typed adapters for the netfslib callbacks used by ZeroFS.
//!
//! Bindgen necessarily exposes netfslib as raw pointers.  The callback shims
//! validate those pointers once, then use these types so the filesystem logic
//! cannot accidentally use a source iterator as a destination, terminate a
//! subrequest twice, or rewind beyond bytes copied by the current callback.

use core::{
    ffi::c_void,
    marker::PhantomData,
    mem::offset_of,
    ptr::{self, NonNull},
};

use kernel::{
    bindings, ffi,
    iov::IovIterDest,
    prelude::*,
    sync::aref::{ARef, AlwaysRefCounted},
};

use super::{abi, compat};
use crate::{client::ReplyDestination, transport::PayloadIter};

/// The netfslib request origins whose behavior differs in ZeroFS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Origin {
    Readahead,
    DioRead,
    UnbufferedRead,
    Writeback,
    WritebackSingle,
    Writethrough,
    DioWrite,
    UnbufferedWrite,
    Other,
}

impl Origin {
    fn from_raw(origin: abi::netfs_io_origin) -> Self {
        match origin {
            abi::netfs_io_origin_NETFS_READAHEAD => Self::Readahead,
            abi::netfs_io_origin_NETFS_DIO_READ => Self::DioRead,
            abi::netfs_io_origin_NETFS_UNBUFFERED_READ => Self::UnbufferedRead,
            abi::netfs_io_origin_NETFS_WRITEBACK => Self::Writeback,
            abi::netfs_io_origin_NETFS_WRITEBACK_SINGLE => Self::WritebackSingle,
            abi::netfs_io_origin_NETFS_WRITETHROUGH => Self::Writethrough,
            abi::netfs_io_origin_NETFS_DIO_WRITE => Self::DioWrite,
            abi::netfs_io_origin_NETFS_UNBUFFERED_WRITE => Self::UnbufferedWrite,
            _ => Self::Other,
        }
    }

    pub(crate) fn is_direct_read(self) -> bool {
        matches!(self, Self::DioRead | Self::UnbufferedRead)
    }

    pub(crate) fn is_direct_write(self) -> bool {
        matches!(self, Self::DioWrite | Self::UnbufferedWrite)
    }

    pub(crate) fn is_writeback(self) -> bool {
        matches!(self, Self::Writeback | Self::WritebackSingle)
    }

    /// Whether netfslib holds this origin's payload folios under writeback for
    /// the whole subrequest.
    ///
    /// These are the origins whose buffer `netfs_write_folio` fills, which
    /// starts writeback on each folio before appending it and clears the mark
    /// only from the collector.
    pub(crate) fn pins_folios_under_writeback(self) -> bool {
        matches!(self, Self::Writeback | Self::Writethrough)
    }
}

/// Shared access to a request retained by netfslib.
pub(crate) struct RequestRef<'a> {
    raw: NonNull<abi::netfs_io_request>,
    _lifetime: PhantomData<&'a abi::netfs_io_request>,
}

/// The one typed owner of a filesystem request's `netfs_priv` slot.
///
/// Constructing this descriptor is unsafe; using it is safe. This keeps the
/// raw `void *` association and `ARef` ownership transitions in one audited
/// place instead of asking every request callback to choose a pointer type.
pub(crate) struct RequestPrivate<T> {
    _type: PhantomData<T>,
}

impl<T: AlwaysRefCounted> RequestPrivate<T> {
    /// Associate a Rust type with every use of one netfs request-private slot.
    ///
    /// # Safety
    ///
    /// Every writer and reader of the requests used with this descriptor must
    /// use the same `T`, and every installed pointer must come from
    /// `ARef<T>::into_raw`.
    pub(crate) const unsafe fn new() -> Self {
        Self { _type: PhantomData }
    }

    fn pointer(&self, request: &RequestRef<'_>) -> Option<NonNull<T>> {
        NonNull::new(
            // SAFETY: The request remains live for this synchronous access.
            unsafe { ptr::addr_of!((*request.raw.as_ptr()).netfs_priv).read() }.cast::<T>(),
        )
    }

    fn replace(&self, request: &mut RequestMut<'_>, value: Option<ARef<T>>) -> Option<ARef<T>> {
        let previous =
            // SAFETY: The lifecycle callback exclusively owns this slot.
            unsafe { ptr::addr_of!((*request.raw.as_ptr()).netfs_priv).read() }.cast::<T>();
        unsafe {
            ptr::addr_of_mut!((*request.raw.as_ptr()).netfs_priv).write(
                value.map_or(ptr::null_mut(), |value| {
                    ARef::into_raw(value).as_ptr().cast::<c_void>()
                }),
            );
        }
        NonNull::new(previous).map(|pointer| {
            // SAFETY: This descriptor's invariant says the old slot owns
            // exactly one ARef<T> representation.
            unsafe { ARef::from_raw(pointer) }
        })
    }

    /// Install the request's one private owner without replacing another
    /// lifecycle's value.
    pub(crate) fn install(
        &self,
        request: &mut RequestMut<'_>,
        value: ARef<T>,
    ) -> core::result::Result<(), ARef<T>> {
        // SAFETY: The lifecycle callback exclusively owns this slot.
        let empty = unsafe { ptr::addr_of!((*request.raw.as_ptr()).netfs_priv).read() }.is_null();
        if !empty {
            return Err(value);
        }
        // replace cannot return an owner after the empty check while the
        // callback holds exclusive access.
        drop(self.replace(request, Some(value)));
        Ok(())
    }

    pub(crate) fn take(&self, request: &mut RequestMut<'_>) -> Option<ARef<T>> {
        self.replace(request, None)
    }
}

impl RequestRef<'_> {
    fn new(raw: *mut abi::netfs_io_request) -> Option<Self> {
        Some(Self {
            raw: NonNull::new(raw)?,
            _lifetime: PhantomData,
        })
    }

    pub(crate) fn origin(&self) -> Origin {
        // SAFETY: Construction requires a live request retained by netfslib.
        Origin::from_raw(unsafe { ptr::addr_of!((*self.raw.as_ptr()).origin).read() })
    }

    pub(crate) fn read_size(&self) -> u32 {
        // SAFETY: Construction requires a live request retained by netfslib.
        unsafe { ptr::addr_of!((*self.raw.as_ptr()).rsize).read() }
    }

    pub(crate) fn len(&self) -> u64 {
        // SAFETY: Construction requires a live request retained by netfslib.
        unsafe { ptr::addr_of!((*self.raw.as_ptr()).len).read() }
    }

    pub(crate) fn has_iocb(&self) -> bool {
        // SAFETY: Construction requires a live request retained by netfslib.
        !unsafe { ptr::addr_of!((*self.raw.as_ptr()).iocb).read() }.is_null()
    }

    pub(crate) fn inode_ptr(&self) -> *mut bindings::inode {
        // SAFETY: Construction requires a live request retained by netfslib.
        unsafe { ptr::addr_of!((*self.raw.as_ptr()).inode).read() }
    }

    pub(crate) fn inode_size(&self) -> Option<u64> {
        let inode = self.inode_ptr();
        if inode.is_null() {
            return None;
        }
        // SAFETY: The request retains its inode until all subrequests finish.
        Some(unsafe { (*inode).i_size.max(0) as u64 })
    }

    pub(crate) fn remote_inode_size(&self) -> Option<u64> {
        let inode = self.inode_ptr();
        if inode.is_null() {
            return None;
        }
        // SAFETY: `netfs_inode.inode` is the first field, and ZeroFS allocates
        // every regular inode as that embedding.
        Some(unsafe { compat::read_remote_size(inode).max(0) } as u64)
    }
}

/// Exclusive access supplied to request lifecycle callbacks.
pub(crate) struct RequestMut<'a> {
    raw: NonNull<abi::netfs_io_request>,
    _lifetime: PhantomData<&'a mut abi::netfs_io_request>,
}

impl<'a> RequestMut<'a> {
    /// Wrap a request passed to a netfslib lifecycle callback.
    ///
    /// # Safety
    ///
    /// `raw` must remain live and grant the callback exclusive access to the
    /// filesystem-owned request fields for `'a`.
    pub(crate) unsafe fn from_raw(raw: *mut abi::netfs_io_request) -> Option<Self> {
        Some(Self {
            raw: NonNull::new(raw)?,
            _lifetime: PhantomData,
        })
    }

    pub(crate) fn as_ref(&self) -> RequestRef<'_> {
        RequestRef {
            raw: self.raw,
            _lifetime: PhantomData,
        }
    }

    pub(crate) fn origin(&self) -> Origin {
        self.as_ref().origin()
    }

    pub(crate) fn set_io_sizes(&mut self, read: u32, write: u32) {
        // SAFETY: init_request owns these filesystem-provided limits.
        unsafe {
            ptr::addr_of_mut!((*self.raw.as_ptr()).rsize).write(read);
            ptr::addr_of_mut!((*self.raw.as_ptr()).wsize).write(write);
        }
    }

    pub(crate) fn set_group(&mut self, group: *mut abi::netfs_group) {
        // SAFETY: init_request/begin_writeback own this association.
        unsafe {
            ptr::addr_of_mut!((*self.raw.as_ptr()).group).write(group);
        }
    }

    pub(crate) fn set_error(&mut self, error: Error) {
        // SAFETY: begin_writeback reports setup failure through this field.
        unsafe {
            ptr::addr_of_mut!((*self.raw.as_ptr()).error).write(error.to_errno() as ffi::c_long);
        }
    }

    pub(crate) fn set_write_stream_available(&mut self, available: bool) {
        // SAFETY: begin_writeback configures stream zero before issue_write.
        unsafe {
            ptr::addr_of_mut!((*self.raw.as_ptr()).io_streams[0].avail).write(available);
        }
    }
}

/// Raw subrequest plus the callback's exclusive borrow.
struct SubrequestHandle<'a> {
    raw: NonNull<abi::netfs_io_subrequest>,
    _lifetime: PhantomData<&'a mut abi::netfs_io_subrequest>,
}

impl<'a> SubrequestHandle<'a> {
    /// # Safety
    ///
    /// `raw` must remain live and exclusively owned by the resulting view for
    /// `'a`.
    unsafe fn from_raw(raw: *mut abi::netfs_io_subrequest) -> Option<Self> {
        Some(Self {
            raw: NonNull::new(raw)?,
            _lifetime: PhantomData,
        })
    }

    fn as_ptr(&self) -> *mut abi::netfs_io_subrequest {
        self.raw.as_ptr()
    }

    fn request(&self) -> Option<RequestRef<'_>> {
        // SAFETY: Netfslib retains the parent request for the complete
        // borrow of this live subrequest.
        RequestRef::new(unsafe { ptr::addr_of!((*self.raw.as_ptr()).rreq).read() })
    }

    fn remaining(&self) -> usize {
        // SAFETY: The issue callback may read these stable progress fields.
        let (length, transferred) = unsafe {
            (
                ptr::addr_of!((*self.raw.as_ptr()).len).read(),
                ptr::addr_of!((*self.raw.as_ptr()).transferred).read(),
            )
        };
        length.saturating_sub(transferred)
    }

    fn position(&self) -> u64 {
        // SAFETY: The issue callback may read these stable progress fields.
        let (start, transferred) = unsafe {
            (
                ptr::addr_of!((*self.raw.as_ptr()).start).read(),
                ptr::addr_of!((*self.raw.as_ptr()).transferred).read(),
            )
        };
        start.wrapping_add(transferred as u64)
    }

    fn iterator_type(&self) -> u8 {
        // SAFETY: The issue callback owns this subrequest and may inspect its
        // iterator descriptor until termination.
        unsafe { ptr::addr_of!((*self.raw.as_ptr()).io_iter.iter_type).read() }
    }

    fn iterator_is_source(&self) -> bool {
        // SAFETY: As above; data_source is immutable for one issue.
        unsafe { ptr::addr_of!((*self.raw.as_ptr()).io_iter.data_source).read() }
    }

    fn copy_to_iter(&mut self, bytes: &[u8]) -> usize {
        // SAFETY: A read issue owns a destination iterator. The exclusive
        // subrequest borrow prevents any concurrent advance.
        let iterator =
            unsafe { IovIterDest::from_raw(ptr::addr_of_mut!((*self.raw.as_ptr()).io_iter)) };
        iterator.copy_to_iter(bytes)
    }

    fn worker_safe_destination(&mut self) -> Option<ReplyDestination<'_>> {
        let iterator_type = self.iterator_type();
        let worker_safe = iterator_type == bindings::iter_type_ITER_BVEC as u8
            || iterator_type == bindings::iter_type_ITER_FOLIOQ as u8;
        if !worker_safe || self.iterator_is_source() != (bindings::ITER_DEST != 0) {
            return None;
        }
        // SAFETY: BVEC and FOLIOQ iterators are independent of the issuing
        // task. Netfslib retains their backing storage until termination, and
        // the exclusive borrow prevents another advance.
        unsafe { ReplyDestination::from_raw(ptr::addr_of_mut!((*self.raw.as_ptr()).io_iter)) }
    }

    fn add_transferred(&mut self, bytes: usize) {
        // SAFETY: The issue callback exclusively owns subrequest progress.
        unsafe {
            let transferred = ptr::addr_of!((*self.raw.as_ptr()).transferred).read();
            ptr::addr_of_mut!((*self.raw.as_ptr()).transferred)
                .write(transferred.saturating_add(bytes));
        }
    }

    fn set_flag(&mut self, bit: u32) {
        // SAFETY: The issue callback owns filesystem-provided flag updates.
        unsafe {
            bindings::__set_bit(
                bit as ffi::c_ulong,
                ptr::addr_of_mut!((*self.raw.as_ptr()).flags),
            );
        }
    }

    fn set_result(&mut self, result: Result) {
        let status = match result {
            Ok(()) => 0,
            // An errno always fits in an i16, as netfslib's own field width
            // asserts.
            Err(error) => error.to_errno() as i16,
        };
        // SAFETY: The issue callback owns the completion status.
        unsafe {
            ptr::addr_of_mut!((*self.raw.as_ptr()).error).write(status);
        }
    }

    fn payload(&self, maximum: usize) -> Option<PayloadIter<'_>> {
        if !self.iterator_is_source() {
            return None;
        }
        let splice = self.iterator_type() == bindings::iter_type_ITER_FOLIOQ as u8
            && self
                .request()
                .is_some_and(|request| request.origin().pins_folios_under_writeback());

        // SAFETY: The issue callback owns the source iterator until
        // termination, and netfslib retains the parent request, rolling
        // buffer, and every folio across that interval. Splicing is restricted
        // to FOLIOQ origins held under writeback; the skb takes another page
        // reference before the subrequest can finish.
        let payload = unsafe {
            PayloadIter::from_source(ptr::addr_of!((*self.raw.as_ptr()).io_iter), maximum, splice)
        };
        if payload.len() == 0 {
            None
        } else {
            Some(payload)
        }
    }

    fn cap_len(&mut self, maximum: usize) {
        // SAFETY: prepare_read owns the subrequest length adjustment.
        unsafe {
            let length = ptr::addr_of!((*self.raw.as_ptr()).len).read();
            ptr::addr_of_mut!((*self.raw.as_ptr()).len).write(core::cmp::min(length, maximum));
        }
    }
}

/// A worker-safe read subrequest whose issue-callback ownership was
/// transferred to its embedded work item.
#[must_use = "queue the work item so netfslib can complete the subrequest"]
pub(crate) struct ReadSubrequestWork {
    raw: NonNull<abi::netfs_io_subrequest>,
}

/// A worker-safe write subrequest whose issue-callback ownership was
/// transferred to its embedded work item.
#[must_use = "queue the work item so netfslib can complete the subrequest"]
pub(crate) struct WriteSubrequestWork {
    raw: NonNull<abi::netfs_io_subrequest>,
}

fn queue_subrequest_work(
    raw: NonNull<abi::netfs_io_subrequest>,
    worker: unsafe extern "C" fn(*mut bindings::work_struct),
) -> bool {
    // SAFETY: Transfer constructors only accept a live, worker-safe
    // subrequest whose embedded work item is initialized and unqueued.
    let work = unsafe { ptr::addr_of_mut!((*raw.as_ptr()).work) };
    unsafe {
        ptr::addr_of_mut!((*work).func).write(Some(worker));
        bindings::queue_work_on(
            bindings::wq_misc_consts_WORK_CPU_UNBOUND as ffi::c_int,
            ptr::addr_of!(bindings::system_unbound_wq).read(),
            work,
        )
    }
}

unsafe fn subrequest_from_work(
    work: *mut bindings::work_struct,
) -> Option<NonNull<abi::netfs_io_subrequest>> {
    let work = NonNull::new(work)?;
    // SAFETY: The caller guarantees this is the embedded work item of a live
    // netfs subrequest.
    let raw = unsafe {
        work.as_ptr()
            .cast::<u8>()
            .sub(offset_of!(abi::netfs_io_subrequest, work))
            .cast::<abi::netfs_io_subrequest>()
    };
    NonNull::new(raw)
}

impl ReadSubrequestWork {
    pub(crate) fn queue(self) -> bool {
        queue_subrequest_work(self.raw, read_subrequest_worker)
    }

    /// Recover the read-subrequest ownership transferred to this work item.
    ///
    /// # Safety
    ///
    /// `work` must belong to a `ReadSubrequestWork` queued exactly once.
    unsafe fn from_work(work: *mut bindings::work_struct) -> Option<ReadSubrequest<'static>> {
        let raw = unsafe { subrequest_from_work(work)? };
        unsafe { ReadSubrequest::from_raw(raw.as_ptr()) }
    }
}

impl WriteSubrequestWork {
    pub(crate) fn queue(self) -> bool {
        queue_subrequest_work(self.raw, write_subrequest_worker)
    }

    /// Recover the write-subrequest ownership transferred to this work item.
    ///
    /// # Safety
    ///
    /// `work` must belong to a `WriteSubrequestWork` queued exactly once.
    unsafe fn from_work(work: *mut bindings::work_struct) -> Option<WriteSubrequest<'static>> {
        let raw = unsafe { subrequest_from_work(work)? };
        unsafe { WriteSubrequest::from_raw(raw.as_ptr()) }
    }
}

unsafe extern "C" fn read_subrequest_worker(work: *mut bindings::work_struct) {
    // SAFETY: ReadSubrequestWork::queue installs this callback only on the
    // embedded work item whose read-subrequest ownership it consumed.
    let Some(subrequest) = (unsafe { ReadSubrequestWork::from_work(work) }) else {
        return;
    };
    crate::vfs::run_netfs_read_subrequest(subrequest);
}

unsafe extern "C" fn write_subrequest_worker(work: *mut bindings::work_struct) {
    // SAFETY: WriteSubrequestWork::queue installs this callback only on the
    // embedded work item whose write-subrequest ownership it consumed.
    let Some(subrequest) = (unsafe { WriteSubrequestWork::from_work(work) }) else {
        return;
    };
    crate::vfs::run_netfs_write_subrequest(subrequest);
}

/// Mutable access used only by `prepare_read`.
pub(crate) struct PreparedRead<'a> {
    subrequest: SubrequestHandle<'a>,
}

impl<'a> PreparedRead<'a> {
    /// # Safety
    ///
    /// `raw` must be the live subrequest supplied to `prepare_read`.
    pub(crate) unsafe fn from_raw(raw: *mut abi::netfs_io_subrequest) -> Option<Self> {
        Some(Self {
            // SAFETY: The caller transfers the callback's exclusive view.
            subrequest: unsafe { SubrequestHandle::from_raw(raw)? },
        })
    }

    pub(crate) fn request(&self) -> Option<RequestRef<'_>> {
        self.subrequest.request()
    }

    pub(crate) fn cap_len(&mut self, maximum: usize) {
        self.subrequest.cap_len(maximum);
    }
}

/// A read subrequest owned by an issue callback until termination.
pub(crate) struct ReadSubrequest<'a> {
    subrequest: SubrequestHandle<'a>,
}

impl<'a> ReadSubrequest<'a> {
    /// # Safety
    ///
    /// `raw` must be a live read subrequest issued to ZeroFS. No other code
    /// may access its filesystem-owned fields before `terminate` consumes it,
    /// and its iterator must be valid in the current execution context.
    pub(crate) unsafe fn from_raw(raw: *mut abi::netfs_io_subrequest) -> Option<Self> {
        Some(Self {
            // SAFETY: The caller transfers the callback's exclusive view.
            subrequest: unsafe { SubrequestHandle::from_raw(raw)? },
        })
    }

    pub(crate) fn request(&self) -> Option<RequestRef<'_>> {
        self.subrequest.request()
    }

    /// Lend the request-private value alongside mutable subrequest access.
    ///
    /// The higher-ranked callback cannot return either borrow, and receives
    /// only `&mut Self`, so it cannot consume and terminate the subrequest while
    /// the request-owned value is live.
    pub(crate) fn with_request_private<T: AlwaysRefCounted, R>(
        &mut self,
        private: &RequestPrivate<T>,
        f: impl for<'scope> FnOnce(&'scope T, &'scope mut Self) -> R,
    ) -> Option<R> {
        let pointer = {
            let request = self.request()?;
            private.pointer(&request)?
        };
        // SAFETY: The parent request owns this ARef representation until every
        // subrequest ends. The scoped callback cannot terminate this one or
        // allow the reference to escape.
        Some(f(unsafe { pointer.as_ref() }, self))
    }

    /// Whether netfslib materialized this iterator in thread-independent
    /// storage that may be consumed on an unbound worker.
    fn can_run_on_worker(&self) -> bool {
        // Direct user iterators are extracted into BVECs; buffered readahead
        // uses a retained FOLIOQ. Do not move UBUF/IOVEC/KVEC iterators, which
        // may refer to task-local or kmap-local state.
        let iterator_type = self.subrequest.iterator_type();
        iterator_type == bindings::iter_type_ITER_BVEC as u8
            || iterator_type == bindings::iter_type_ITER_FOLIOQ as u8
    }

    /// Transfer this worker-safe subrequest to its embedded work item.
    pub(crate) fn try_into_work(self) -> core::result::Result<ReadSubrequestWork, Self> {
        if self.can_run_on_worker() {
            Ok(ReadSubrequestWork {
                raw: self.subrequest.raw,
            })
        } else {
            Err(self)
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.subrequest.remaining()
    }

    pub(crate) fn position(&self) -> u64 {
        self.subrequest.position()
    }

    pub(crate) fn copy_to_iter(&mut self, bytes: &[u8]) -> usize {
        self.subrequest.copy_to_iter(bytes)
    }

    /// Borrow the destination iterator for one reply.
    pub(crate) fn reply_destination(&mut self) -> Option<ReplyDestination<'_>> {
        self.subrequest.worker_safe_destination()
    }

    pub(crate) fn add_transferred(&mut self, bytes: usize) {
        self.subrequest.add_transferred(bytes);
    }

    pub(crate) fn mark_progress(&mut self) {
        self.set_flag(abi::NETFS_SREQ_MADE_PROGRESS);
    }

    pub(crate) fn mark_eof(&mut self) {
        self.set_flag(abi::NETFS_SREQ_HIT_EOF);
    }

    pub(crate) fn mark_clear_tail(&mut self) {
        self.set_flag(abi::NETFS_SREQ_CLEAR_TAIL);
    }

    fn set_flag(&mut self, bit: u32) {
        self.subrequest.set_flag(bit);
    }

    pub(crate) fn set_result(&mut self, result: Result) {
        self.subrequest.set_result(result);
    }

    pub(crate) fn terminate(self) {
        // SAFETY: This type owns the one termination obligation and is
        // consumed, so safe code cannot terminate the same subrequest twice.
        unsafe {
            abi::netfs_read_subreq_terminated(self.subrequest.as_ptr());
        }
    }
}

/// A write subrequest owned by an issue callback until termination.
pub(crate) struct WriteSubrequest<'a> {
    subrequest: SubrequestHandle<'a>,
}

impl<'a> WriteSubrequest<'a> {
    /// # Safety
    ///
    /// `raw` must be a live write subrequest issued to ZeroFS. No other code
    /// may access its filesystem-owned fields before `terminate` consumes it,
    /// and its iterator must be valid in the current execution context.
    pub(crate) unsafe fn from_raw(raw: *mut abi::netfs_io_subrequest) -> Option<Self> {
        Some(Self {
            // SAFETY: The caller transfers the callback's exclusive view.
            subrequest: unsafe { SubrequestHandle::from_raw(raw)? },
        })
    }

    pub(crate) fn request(&self) -> Option<RequestRef<'_>> {
        self.subrequest.request()
    }

    /// Lend the request-private value alongside mutable subrequest access.
    ///
    /// See the read-side method: the scoped callback cannot consume this
    /// subrequest or let the borrowed request state escape.
    pub(crate) fn with_request_private<T: AlwaysRefCounted, R>(
        &mut self,
        private: &RequestPrivate<T>,
        f: impl for<'scope> FnOnce(&'scope T, &'scope mut Self) -> R,
    ) -> Option<R> {
        let pointer = {
            let request = self.request()?;
            private.pointer(&request)?
        };
        // SAFETY: The request retains this owner through the callback, whose
        // higher-ranked lifetime prevents the reference from escaping.
        Some(f(unsafe { pointer.as_ref() }, self))
    }

    /// Whether this writeback iterator is independent of the issuing task.
    fn can_run_on_worker(&self) -> bool {
        // Netfslib writeback gathers retained folios into a FOLIOQ. Direct
        // user iterators use BVEC but are intentionally issued inline.
        self.subrequest.iterator_type() == bindings::iter_type_ITER_FOLIOQ as u8
    }

    /// Transfer this worker-safe subrequest to its embedded work item.
    pub(crate) fn try_into_work(self) -> core::result::Result<WriteSubrequestWork, Self> {
        if self.can_run_on_worker() {
            Ok(WriteSubrequestWork {
                raw: self.subrequest.raw,
            })
        } else {
            Err(self)
        }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.subrequest.remaining()
    }

    pub(crate) fn position(&self) -> u64 {
        self.subrequest.position()
    }

    /// Snapshot at most `maximum` bytes of this subrequest's source iterator.
    ///
    /// The snapshot is a copy, and nothing here moves `io_iter`. Netfslib
    /// repositions it from `transferred` before every reissue
    /// (`netfs_reset_iter`), and both write collectors account from
    /// `transferred` alone, so leaving the subrequest's own iterator where it
    /// was found is what makes a short acknowledgement resume correctly.
    pub(crate) fn payload(&self, maximum: usize) -> Option<PayloadIter<'_>> {
        // MSG_SPLICE_PAGES pins the payload pages into skbs instead of copying
        // them, so they must not change until the socket is done. That holds
        // for a writeback or writethrough FOLIOQ: netfslib puts every folio
        // under writeback before it enters the rolling buffer and clears the
        // mark only after this subrequest terminates, ZeroFS attaches a
        // netfs_group to each dirtied folio so netfs_perform_write waits that
        // out, and an mmap store faults through netfs_page_mkwrite, which
        // waits too. Direct-write BVECs get no such promise: the application
        // may rewrite its own buffer mid-flight and a kernel-issued direct
        // write can supply pages that do not satisfy sendpage_ok(). Those are
        // copied.
        self.subrequest.payload(maximum)
    }

    pub(crate) fn mark_progress(&mut self) {
        self.subrequest.set_flag(abi::NETFS_SREQ_MADE_PROGRESS);
    }

    pub(crate) fn terminate(self, result: Result<usize>) {
        let transferred_or_error = match result {
            Ok(transferred) => transferred as isize,
            Err(error) => error.to_errno() as isize,
        };
        // SAFETY: This type owns the one termination obligation and is
        // consumed, so safe code cannot terminate the same subrequest twice.
        unsafe {
            abi::netfs_write_subrequest_terminated(
                self.subrequest.as_ptr().cast::<c_void>(),
                transferred_or_error,
            );
        }
    }
}
