//! Native ZeroFS VFS client.
//!
//! Speaks ZeroFS's private `9P2000.L.Z` dialect over one TCP or AF_UNIX
//! stream socket. It does not use FUSE or Linux v9fs.

#![recursion_limit = "256"]

/// A kernel errno as an [`Error`](kernel::error::Error).
///
/// `kernel::error::code`, re-exported by `kernel::prelude`, declares constants
/// for most errnos; use those directly. This covers the ones it does not
/// declare on every kernel this module builds against, such as `ESTALE`.
macro_rules! errno {
    ($name:ident) => {
        ::kernel::error::Error::from_errno(-(::kernel::bindings::$name as ::kernel::ffi::c_int))
    };
}

#[allow(dead_code)]
mod client;
mod load_parameters;
mod netfs;
#[allow(dead_code)]
mod protocol;
#[allow(dead_code)]
mod transport;
mod vfs;

use core::{
    mem::MaybeUninit,
    ptr::{self, NonNull},
};

use kernel::{
    alloc::{flags::GFP_KERNEL, KBox},
    bindings,
    cred::Credential,
    ffi,
    prelude::*,
    types::{ForeignOwnable, Opaque},
};

use crate::client::{Endpoint, RebindCredentials};

mod fs_context_abi {
    #![cfg_attr(CONFIG_RUSTC_HAS_UNNECESSARY_TRANSMUTES, allow(unnecessary_transmutes))]
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

    include!(concat!(
        env!("ZEROFS_KBUILD_OUTPUT"),
        "/fs_context_bindings.rs"
    ));
}

module! {
    type: ZeroFsModule,
    name: "zerofs",
    authors: ["Pierre Barre"],
    description: "Native Rust VFS client for ZeroFS",
    license: "GPL",
    alias: ["fs-zerofs"],
}

const FS_NAME: &[u8] = b"zerofs\0";
const CONSISTENCY_KEY: &[u8] = b"consistency\0";
const CONSISTENCY_RELAXED: &[u8] = b"relaxed\0";
const CONSISTENCY_STRICT: &[u8] = b"strict\0";
const MSIZE_KEY: &[u8] = b"msize\0";
const DEFAULT_MSIZE: u32 = protocol::MAX_MSIZE;

#[derive(Clone, Copy)]
struct MountConfig {
    consistency: vfs::Consistency,
    msize: u32,
}

impl Default for MountConfig {
    fn default() -> Self {
        Self {
            consistency: vfs::Consistency::Relaxed,
            msize: DEFAULT_MSIZE,
        }
    }
}

#[repr(u8)]
enum MountParameter {
    Consistency = 1,
    Msize = 2,
}

/// Borrowed fields used while VFS invokes a mount-context callback.
struct FsContextRef<'a>(&'a fs_context_abi::fs_context);

impl<'a> FsContextRef<'a> {
    /// # Safety
    ///
    /// `raw` must remain live with stable `net_ns`, `cred`, and `source`
    /// fields for `'a`.
    unsafe fn from_raw(raw: *mut fs_context_abi::fs_context) -> Option<Self> {
        // SAFETY: Guaranteed by the caller.
        Some(Self(unsafe { raw.as_ref()? }))
    }

    fn network_namespace(&self) -> Option<NonNull<bindings::net>> {
        NonNull::new(self.0.net_ns)
    }

    fn credential(&self) -> Option<&Credential> {
        let credential = self.0.cred;
        if credential.is_null() {
            return None;
        }
        // SAFETY: Guaranteed by the context borrow described above.
        Some(unsafe { Credential::from_ptr(credential) })
    }

    fn source(&self) -> &[u8] {
        let source = self.0.source;
        if source.is_null() {
            b"none"
        } else {
            // SAFETY: fs_context owns this NUL-terminated string for the
            // lifetime of the callback borrow.
            unsafe { CStr::from_char_ptr(source) }.to_bytes()
        }
    }

    fn mount_config(&self) -> Option<&MountConfig> {
        if self.0.fs_private.is_null() {
            return None;
        }
        // SAFETY: init_fs_context installed exactly one KBox<MountConfig>.
        // VFS keeps it live and immutable throughout this callback borrow.
        Some(unsafe { <KBox<MountConfig> as ForeignOwnable>::borrow(self.0.fs_private) })
    }
}

/// Exclusive initialization view over the opaque canonical binding.
struct FsContextInit<'a>(&'a mut fs_context_abi::fs_context);

impl<'a> FsContextInit<'a> {
    /// # Safety
    ///
    /// `raw` must be a live context supplied exclusively to a filesystem
    /// context callback for `'a`.
    unsafe fn from_raw(raw: *mut fs_context_abi::fs_context) -> Option<Self> {
        // SAFETY: Guaranteed by the caller.
        Some(Self(unsafe { raw.as_mut()? }))
    }

    /// # Safety
    ///
    /// `raw` must be the live context exclusively supplied to
    /// `init_fs_context` for `'a`.
    unsafe fn from_opaque(raw: *mut bindings::fs_context) -> Option<Self> {
        // SAFETY: Bindgen generated this target kernel's complete context
        // layout, and the caller guarantees exclusive access.
        Some(Self(unsafe {
            raw.cast::<fs_context_abi::fs_context>().as_mut()?
        }))
    }

    fn set_operations(&mut self, operations: &'static fs_context_abi::fs_context_operations) {
        self.0.ops = operations;
    }

    fn install_mount_config(&mut self, config: KBox<MountConfig>) -> Result<()> {
        if !self.0.fs_private.is_null() {
            return Err(EINVAL);
        }
        self.0.fs_private = config.into_foreign();
        Ok(())
    }

    fn mount_config_mut(&mut self) -> Option<&mut MountConfig> {
        if self.0.fs_private.is_null() {
            return None;
        }
        // SAFETY: VFS serializes mutable fs_context callbacks. This method
        // borrows the sole KBox installed by init_fs_context only for `self`.
        Some(unsafe { <KBox<MountConfig> as ForeignOwnable>::borrow_mut(self.0.fs_private) })
    }

    fn take_mount_config(&mut self) -> Option<KBox<MountConfig>> {
        // SAFETY: This exclusive wrapper owns access to the context slot.
        let config = unsafe { ptr::replace(&mut self.0.fs_private, ptr::null_mut()) };
        if config.is_null() {
            None
        } else {
            // SAFETY: install_mount_config transferred exactly one KBox into
            // this slot, and replacing it above makes this the sole recovery.
            Some(unsafe { <KBox<MountConfig> as ForeignOwnable>::from_foreign(config) })
        }
    }
}

static FS_CONTEXT_OPERATIONS: fs_context_abi::fs_context_operations =
    fs_context_abi::fs_context_operations {
        free: Some(zerofs_free_fs_context),
        dup: Some(zerofs_dup_fs_context),
        parse_param: Some(zerofs_parse_param),
        parse_monolithic: None,
        get_tree: Some(zerofs_get_tree),
        reconfigure: None,
    };

unsafe extern "C" fn zerofs_free_fs_context(fc: *mut fs_context_abi::fs_context) {
    // SAFETY: VFS invokes free once with exclusive final access to this
    // filesystem context.
    if let Some(mut context) = unsafe { FsContextInit::from_raw(fc) } {
        drop(context.take_mount_config());
    }
}

unsafe extern "C" fn zerofs_dup_fs_context(
    fc: *mut fs_context_abi::fs_context,
    source_fc: *mut fs_context_abi::fs_context,
) -> ffi::c_int {
    // SAFETY: VFS keeps the source live and immutable while supplying an
    // unpublished destination exclusively to this callback.
    let Some(source) = (unsafe { FsContextRef::from_raw(source_fc) }) else {
        return EINVAL.to_errno();
    };
    let Some(config) = source.mount_config().copied() else {
        return EINVAL.to_errno();
    };
    let config = match KBox::new(config, GFP_KERNEL) {
        Ok(config) => config,
        Err(_) => return ENOMEM.to_errno(),
    };
    let Some(mut destination) = (unsafe { FsContextInit::from_raw(fc) }) else {
        return EINVAL.to_errno();
    };
    match destination.install_mount_config(config) {
        Ok(()) => 0,
        Err(error) => error.to_errno(),
    }
}

unsafe extern "C" fn zerofs_parse_param(
    fc: *mut fs_context_abi::fs_context,
    parameter: *mut fs_context_abi::fs_parameter,
) -> ffi::c_int {
    let consistency_values = [
        fs_context_abi::constant_table {
            name: CONSISTENCY_RELAXED.as_ptr().cast(),
            value: 0,
        },
        fs_context_abi::constant_table {
            name: CONSISTENCY_STRICT.as_ptr().cast(),
            value: 1,
        },
        fs_context_abi::constant_table {
            name: ptr::null(),
            value: 0,
        },
    ];
    let parameters = [
        fs_context_abi::fs_parameter_spec {
            name: CONSISTENCY_KEY.as_ptr().cast(),
            type_: Some(fs_context_abi::fs_param_is_enum),
            opt: MountParameter::Consistency as u8,
            flags: 0,
            data: consistency_values.as_ptr().cast(),
        },
        fs_context_abi::fs_parameter_spec {
            name: MSIZE_KEY.as_ptr().cast(),
            type_: Some(fs_context_abi::fs_param_is_u32),
            opt: MountParameter::Msize as u8,
            flags: 0,
            data: ptr::null(),
        },
        fs_context_abi::fs_parameter_spec {
            name: ptr::null(),
            type_: None,
            opt: 0,
            flags: 0,
            data: ptr::null(),
        },
    ];

    // __fs_parse initializes the entire result union before looking up the
    // option, including on the unknown-option path.
    let mut parsed = MaybeUninit::<fs_context_abi::fs_parse_result>::uninit();
    // SAFETY: VFS supplies live context and parameter objects. Both descriptor
    // tables remain live for this synchronous parser call.
    let option = unsafe {
        fs_context_abi::__fs_parse(
            ptr::addr_of_mut!((*fc).log),
            parameters.as_ptr(),
            parameter,
            parsed.as_mut_ptr(),
        )
    };
    if option < 0 {
        // In particular, preserve -ENOPARAM so VFS can parse "source".
        return option;
    }
    // SAFETY: A successful __fs_parse call initialized the result.
    let parsed = unsafe { parsed.assume_init() };
    // SAFETY: Both accepted parameter descriptors write uint_32.
    let parsed_value = unsafe { *parsed.__bindgen_anon_1.uint_32.as_ref() };

    // SAFETY: VFS serializes parse_param and lends exclusive context access.
    let Some(mut context) = (unsafe { FsContextInit::from_raw(fc) }) else {
        return EINVAL.to_errno();
    };
    let Some(config) = context.mount_config_mut() else {
        return EINVAL.to_errno();
    };
    match option as u8 {
        token if token == MountParameter::Consistency as u8 => {
            config.consistency = if parsed_value == 0 {
                vfs::Consistency::Relaxed
            } else {
                vfs::Consistency::Strict
            };
            0
        }
        token if token == MountParameter::Msize as u8 => {
            if parsed_value < client::MIN_MSIZE || parsed_value > protocol::MAX_MSIZE {
                return EINVAL.to_errno();
            }
            config.msize = parsed_value;
            0
        }
        _ => EINVAL.to_errno(),
    }
}

unsafe extern "C" fn zerofs_fill_super(
    super_block: *mut bindings::super_block,
    fc: *mut fs_context_abi::fs_context,
) -> ffi::c_int {
    // SAFETY: get_tree_nodev keeps the context and these fields live
    // throughout this synchronous callback.
    let Some(context) = (unsafe { FsContextRef::from_raw(fc) }) else {
        return EINVAL.to_errno();
    };
    let (Some(network_namespace), Some(credential)) =
        (context.network_namespace(), context.credential())
    else {
        return EINVAL.to_errno();
    };
    let Some(config) = context.mount_config().copied() else {
        return EINVAL.to_errno();
    };

    let timeout_ms = load_parameters::request_timeout_ms.value();
    let grace_ms = load_parameters::reconnect_grace_ms.value();
    let requested_msize = config.msize;
    let source = context.source();
    let endpoint = if source.is_empty() || source == b"none" || source == b"tcp" {
        // Module parameters are integers only, so the parameter path expresses
        // the HA pair on a shared port. Anything else, including IPv6, belongs
        // on the mount source.
        let port = load_parameters::server_port.value();
        // ZeroFS HA is exactly two participants, so this is the pair.
        let mut targets = [([0u8; 4], 0u16); 2];
        let mut count = 0;
        for address in [
            load_parameters::server_ipv4.value(),
            load_parameters::server_ipv4_peer.value(),
        ] {
            if address != 0 {
                targets[count] = (address.to_be_bytes(), port);
                count += 1;
            }
        }
        match Endpoint::tcp_ipv4(
            network_namespace.as_ptr(),
            &targets[..count],
            timeout_ms,
            grace_ms,
            requested_msize,
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => return error.to_errno(),
        }
    } else {
        // Explicit sources may contain comma-separated TCP and AF_UNIX targets.
        match Endpoint::parse_targets(
            network_namespace.as_ptr(),
            source,
            load_parameters::server_port.value(),
            timeout_ms,
            grace_ms,
            requested_msize,
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => return error.to_errno(),
        }
    };
    let credentials = match RebindCredentials::from_credential(credential) {
        Ok(credentials) => credentials,
        Err(error) => return error.to_errno(),
    };

    // SAFETY: `get_tree_nodev()` supplies exclusive superblock initialization
    // ownership to this callback.
    unsafe { vfs::fill_super_with_endpoint(super_block, endpoint, credentials, config.consistency) }
}

unsafe extern "C" fn zerofs_get_tree(fc: *mut fs_context_abi::fs_context) -> ffi::c_int {
    // SAFETY: VFS owns `fc` for this callback and `zerofs_fill_super` follows
    // the callback contract required by `get_tree_nodev()`.
    unsafe { fs_context_abi::get_tree_nodev(fc, Some(zerofs_fill_super)) }
}

unsafe extern "C" fn zerofs_init_fs_context(fc: *mut bindings::fs_context) -> ffi::c_int {
    // SAFETY: VFS passes a valid context exclusively to this callback. The
    // generated target-header type supplies its complete layout.
    let Some(mut context) = (unsafe { FsContextInit::from_opaque(fc) }) else {
        return EINVAL.to_errno();
    };
    let config = match KBox::new(MountConfig::default(), GFP_KERNEL) {
        Ok(config) => config,
        Err(_) => return ENOMEM.to_errno(),
    };
    context.set_operations(&FS_CONTEXT_OPERATIONS);
    if let Err(error) = context.install_mount_config(config) {
        return error.to_errno();
    }

    0
}

unsafe extern "C" fn zerofs_kill_sb(super_block: *mut bindings::super_block) {
    // SAFETY: kill_sb receives exclusive shutdown ownership of this published
    // ZeroFS superblock. Mark it before kill_anon_super starts dcache eviction.
    unsafe {
        vfs::begin_shutdown(super_block);
        bindings::kill_anon_super(super_block);
    }
}

/// Stable storage registered with VFS for exactly this module lifetime.
struct RegisteredFileSystem {
    storage: KBox<Opaque<bindings::file_system_type>>,
    registered: bool,
}

// SAFETY: VFS owns all concurrent access after registration. This wrapper
// exposes no shared access to the bindgen structure and unregisters it through
// an exclusive module teardown path.
unsafe impl Send for RegisteredFileSystem {}
unsafe impl Sync for RegisteredFileSystem {}

impl RegisteredFileSystem {
    fn register(filesystem: bindings::file_system_type) -> Result<Self> {
        let storage = KBox::new(Opaque::new(filesystem), GFP_KERNEL)?;
        // SAFETY: The allocation is stable and fully initialized, and this
        // owner keeps it live until unregister_filesystem returns.
        let status = unsafe { bindings::register_filesystem(storage.get()) };
        if status < 0 {
            return Err(Error::from_errno(status));
        }
        Ok(Self {
            storage,
            registered: true,
        })
    }

    fn unregister(&mut self) {
        if !self.registered {
            return;
        }
        self.registered = false;
        // SAFETY: register() published this exact stable allocation and module
        // ownership prevents unload while VFS still references it.
        let status = unsafe { bindings::unregister_filesystem(self.storage.get()) };
        if status != 0 {
            pr_err!("failed to unregister filesystem: {}\n", status);
        } else {
            pr_info!("unregistered native ZeroFS VFS client\n");
        }
    }
}

impl Drop for RegisteredFileSystem {
    fn drop(&mut self) {
        self.unregister();
    }
}

struct ZeroFsModule {
    filesystem: RegisteredFileSystem,
}

impl kernel::Module for ZeroFsModule {
    fn init(module: &'static ThisModule) -> Result<Self> {
        vfs::initialize(module)?;

        let mut filesystem = bindings::file_system_type::default();
        filesystem.name = FS_NAME.as_ptr().cast();
        filesystem.init_fs_context = Some(zerofs_init_fs_context);
        filesystem.kill_sb = Some(zerofs_kill_sb);
        filesystem.owner = module.as_ptr();

        let filesystem = match RegisteredFileSystem::register(filesystem) {
            Ok(filesystem) => filesystem,
            Err(error) => {
                vfs::shutdown();
                return Err(error);
            }
        };

        pr_info!("registered native ZeroFS VFS client\n");
        Ok(Self { filesystem })
    }
}

impl Drop for ZeroFsModule {
    fn drop(&mut self) {
        self.filesystem.unregister();
        vfs::shutdown();
    }
}
