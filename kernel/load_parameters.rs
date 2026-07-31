//! Load-time module parameters.
//!
//! Linux 6.18 has the C module-parameter ABI but predates parameter support in
//! Rust's `module!` macro. Registration lives here so both API generations use
//! the same implementation.

use core::{cell::UnsafeCell, ptr};

use kernel::{bindings, ffi};

#[repr(transparent)]
pub(super) struct ModuleParameter<T>(UnsafeCell<T>);

// SAFETY: These parameters have permission 0, so the module loader is the only
// writer and finishes parsing them before module initialization.
unsafe impl<T: Send> Sync for ModuleParameter<T> {}

impl<T: Copy> ModuleParameter<T> {
    const fn new(default: T) -> Self {
        Self(UnsafeCell::new(default))
    }

    pub(super) fn value(&self) -> T {
        // SAFETY: Module loading publishes the parsed value before any ZeroFS
        // callback can read it, and permission 0 prevents later sysfs writes.
        unsafe { ptr::read_volatile(self.0.get()) }
    }

    const fn as_void_ptr(&self) -> *mut ffi::c_void {
        self.0.get().cast()
    }
}

#[repr(transparent)]
#[allow(dead_code)]
struct RegisteredParameter(bindings::kernel_param);

// SAFETY: The descriptor is immutable after its static initialization; the
// kernel only follows its pointers while this module remains loaded.
unsafe impl Sync for RegisteredParameter {}

const fn string_bytes<const N: usize>(value: &str) -> [u8; N] {
    let bytes = value.as_bytes();
    assert!(bytes.len() == N);
    let mut output = [0; N];
    let mut index = 0;
    while index < N {
        output[index] = bytes[index];
        index += 1;
    }
    output
}

macro_rules! module_parameter {
    (
        $name:ident: $type:ty = $default:expr,
        ops: $ops:ident,
        description: $description:literal
    ) => {
        #[allow(non_upper_case_globals)]
        pub(super) static $name: ModuleParameter<$type> = ModuleParameter::new($default);

        const _: () = {
            const PARAMETER_NAME: &kernel::str::CStr = kernel::c_str!(stringify!($name));

            #[link_section = "__param"]
            #[used(compiler)]
            static DESCRIPTOR: RegisteredParameter = RegisteredParameter(bindings::kernel_param {
                name: kernel::str::as_char_ptr_in_const_context(PARAMETER_NAME),
                mod_: super::THIS_MODULE.as_ptr(),
                // `param_ops_*` are immutable, exported kernel descriptors
                // that remain live for the module's lifetime.
                ops: core::ptr::addr_of!(bindings::$ops),
                perm: 0,
                level: -1,
                flags: 0,
                __bindgen_anon_1: bindings::kernel_param__bindgen_ty_1 {
                    arg: $name.as_void_ptr(),
                },
            });

            #[cfg(MODULE)]
            const PARAMETER_TYPE_INFO: &str =
                concat!("parmtype=", stringify!($name), ":", stringify!($type), "\0");
            #[cfg(MODULE)]
            #[link_section = ".modinfo"]
            #[used(compiler)]
            static PARAMETER_TYPE: [u8; PARAMETER_TYPE_INFO.len()] =
                string_bytes(PARAMETER_TYPE_INFO);

            #[cfg(MODULE)]
            const PARAMETER_DESCRIPTION_INFO: &str =
                concat!("parm=", stringify!($name), ":", $description, "\0");
            #[cfg(MODULE)]
            #[link_section = ".modinfo"]
            #[used(compiler)]
            static PARAMETER_DESCRIPTION: [u8; PARAMETER_DESCRIPTION_INFO.len()] =
                string_bytes(PARAMETER_DESCRIPTION_INFO);
        };
    };
}

module_parameter!(
    server_ipv4: u32 = 0x7f00_0001,
    ops: param_ops_uint,
    description: "ZeroFS IPv4 address used by none/tcp mount sources; 0 disables it"
);
module_parameter!(
    server_ipv4_peer: u32 = 0,
    ops: param_ops_uint,
    description: "The HA peer for none/tcp mount sources; 0 disables it"
);
module_parameter!(
    server_port: u16 = 5564,
    ops: param_ops_ushort,
    description: "ZeroFS TCP port used by none/tcp mount sources"
);
module_parameter!(
    request_timeout_ms: u32 = 5_000,
    ops: param_ops_uint,
    description: "Per-phase ZeroFS I/O and wait timeout in milliseconds"
);
module_parameter!(
    reconnect_grace_ms: u32 = 120_000,
    ops: param_ops_uint,
    description: "Longest a request waits for reconnect and session replay, in milliseconds; a mutation resend is bounded by the protocol retry horizon regardless"
);
