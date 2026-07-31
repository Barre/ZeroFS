#!/usr/bin/env python3

import argparse
import re
from pathlib import Path


class PatchError(RuntimeError):
    pass


def replace_once(text: str, old: str, new: str, description: str) -> str:
    count = text.count(old)
    if count != 1:
        raise PatchError(f"{description}: expected one match, found {count}")
    return text.replace(old, new, 1)


def replace_pattern(
    text: str, pattern: str, replacement: str, description: str
) -> str:
    text, count = re.subn(
        pattern, lambda _: replacement, text, count=1, flags=re.DOTALL
    )
    if count != 1:
        raise PatchError(f"{description}: expected one match, found {count}")
    return text


def patch_print(text: str) -> str:
    text = replace_once(
        text,
        "    w.pos().cast()\n}\n\n/// Format strings.",
        """    w.pos().cast()
}

#[cfg(CONFIG_PRINTK)]
pub(crate) const SELF_CONTAINED_MESSAGE_SIZE: usize = 1024;

#[cfg(CONFIG_PRINTK)]
pub(crate) fn format_message(
    args: fmt::Arguments<'_>,
    buffer: &mut [u8; SELF_CONTAINED_MESSAGE_SIZE],
) -> *const c_char {
    use fmt::Write;

    let writable = buffer.len() - 1;
    // SAFETY: The first `writable` bytes of `buffer` are valid for writes.
    let end = unsafe { buffer.as_mut_ptr().add(writable) };
    // SAFETY: `buffer` remains alive and exclusively borrowed while the formatter is used.
    let mut formatter = unsafe { RawFormatter::from_ptrs(buffer.as_mut_ptr(), end) };
    let _ = formatter.write_fmt(args);
    let written = core::cmp::min(formatter.bytes_written(), writable);
    buffer[written] = 0;
    buffer.as_ptr().cast()
}

/// Format strings.""",
        "self-contained Rust argument formatter",
    )
    text = replace_once(
        text,
        '            b"%pA\\0\\0\\0\\0\\0"',
        '            b"%s\\0\\0\\0\\0\\0\\0"',
        "continuation printk format",
    )
    text = replace_once(
        text,
        '            b"%s: %pA\\0"',
        '            b"%s: %s\\0\\0"',
        "prefixed printk format",
    )
    text = replace_pattern(
        text,
        r"pub unsafe fn call_printk\(\n"
        r".*?\n}\n\n/// Prints a message via the kernel's \[`_printk`\] for the `CONT` level\.",
        """pub unsafe fn call_printk(
    format_string: &[u8; format_strings::LENGTH],
    module_name: &[u8],
    args: fmt::Arguments<'_>,
) {
    #[cfg(CONFIG_PRINTK)]
    unsafe {
        let mut message = [0u8; SELF_CONTAINED_MESSAGE_SIZE];
        let message = format_message(args, &mut message);
        bindings::_printk(format_string.as_ptr(), module_name.as_ptr(), message);
    }
}

/// Prints a message via the kernel's [`_printk`] for the `CONT` level.""",
        "call_printk",
    )
    text = replace_pattern(
        text,
        r"pub fn call_printk_cont\(args: fmt::Arguments<'_>\) \{\n"
        r".*?\n}\n\n/// Performs formatting and forwards the string to \[`call_printk`\]\.",
        """pub fn call_printk_cont(args: fmt::Arguments<'_>) {
    #[cfg(CONFIG_PRINTK)]
    unsafe {
        let mut message = [0u8; SELF_CONTAINED_MESSAGE_SIZE];
        let message = format_message(args, &mut message);
        bindings::_printk(format_strings::CONT.as_ptr(), message);
    }
}

/// Performs formatting and forwards the string to [`call_printk`].""",
        "call_printk_cont",
    )
    return text


def patch_seq_file(text: str) -> str:
    text = text.replace("bindings, c_str, fmt,", "bindings, fmt,", 1)
    text = text.replace("fmt, str::CStrExt as _, types", "fmt, types", 1)
    text = replace_once(
        text,
        "}\n\nimpl SeqFile {",
        """}

struct SeqWriter<'a>(&'a SeqFile);

impl fmt::Write for SeqWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        // SAFETY: `value` remains valid for the duration of `seq_write`.
        let result = unsafe {
            bindings::seq_write(
                self.0.inner.get(),
                value.as_ptr().cast(),
                value.len(),
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

impl SeqFile {""",
        "SeqWriter",
    )
    text = replace_pattern(
        text,
        r"    pub fn call_printf\(&self, args: fmt::Arguments<'_>\) \{\n"
        r".*?\n    }\n}\n\n/// Write to a \[`SeqFile`\]",
        """    pub fn call_printf(&self, args: fmt::Arguments<'_>) {
        let _ = fmt::Write::write_fmt(&mut SeqWriter(self), args);
    }
}

/// Write to a [`SeqFile`]""",
        "SeqFile::call_printf",
    )
    return text


def patch_device_driver_type(text: str) -> str:
    if "bindings::driver_type" not in text:
        return text

    # The target kernel was built without CONFIG_RUST, so its device_private
    # layout has no driver_type field. Apply the upstream removal of the typed
    # drvdata accessor instead of inventing a field absent from the target ABI.
    text = replace_once(
        text,
        """use core::{
    any::TypeId,
    marker::PhantomData,
    ptr, //
};""",
        """use core::{
    marker::PhantomData,
    ptr, //
};""",
        "device TypeId import",
    )
    text = replace_once(
        text,
        """
// Assert that we can `read()` / `write()` a `TypeId` instance from / into `struct driver_type`.
static_assert!(core::mem::size_of::<bindings::driver_type>() >= core::mem::size_of::<TypeId>());
""",
        "",
        "device driver_type assertion",
    )
    text = replace_pattern(
        text,
        r"    fn set_type_id<T: 'static>\(&self\) \{\n"
        r".*?\n"
        r"    \}\n\n"
        r"    /// Store a pointer to the bound driver's private data\.",
        "    /// Store a pointer to the bound driver's private data.",
        "Device::set_type_id",
    )
    text = replace_once(
        text,
        "        self.set_type_id::<T>();\n",
        "",
        "Device::set_type_id call",
    )
    text = replace_pattern(
        text,
        r"\n    fn match_type_id<T: 'static>\(&self\) -> Result \{\n"
        r".*?\n"
        r"    pub fn drvdata<T: 'static>\(&self\) -> Result<Pin<&T>> \{\n"
        r".*?\n"
        r"    \}\n",
        "",
        "Device::drvdata",
    )
    if "driver_type" in text or "TypeId" in text:
        raise PatchError("device driver_type removal is incomplete")
    return text


def patch_device(text: str) -> str:
    text = patch_device_driver_type(text)
    text = text.replace(
        "\n#[cfg(CONFIG_PRINTK)]\nuse crate::c_str;\n",
        "\n",
        1,
    )
    text = replace_pattern(
        text,
        r"    unsafe fn printk\(&self, klevel: &\[u8\], msg: fmt::Arguments<'_>\) \{\n"
        r".*?\n    }\n\n    /// Obtain the \[`FwNode`\]",
        """    unsafe fn printk(&self, klevel: &[u8], msg: fmt::Arguments<'_>) {
        #[cfg(CONFIG_PRINTK)]
        unsafe {
            let mut message = [0u8; crate::print::SELF_CONTAINED_MESSAGE_SIZE];
            let message = crate::print::format_message(msg, &mut message);
            bindings::_dev_printk(
                klevel.as_ptr().cast::<crate::ffi::c_char>(),
                self.as_raw(),
                b"%s\\0".as_ptr().cast::<crate::ffi::c_char>(),
                message,
            )
        };
    }

    /// Obtain the [`FwNode`]""",
        "Device::printk",
    )
    return text


def patch_kunit(text: str) -> str:
    text = replace_pattern(
        text,
        r"pub fn err\(args: fmt::Arguments<'_>\) \{\n"
        r".*?\n}\n\n/// Prints a KUnit info-level message\.",
        """pub fn err(args: fmt::Arguments<'_>) {
    #[cfg(not(CONFIG_PRINTK))]
    let _ = args;

    #[cfg(CONFIG_PRINTK)]
    unsafe {
        let mut message = [0u8; crate::print::SELF_CONTAINED_MESSAGE_SIZE];
        let message = crate::print::format_message(args, &mut message);
        bindings::_printk(b"\\x013%s\\0".as_ptr().cast(), message);
    }
}

/// Prints a KUnit info-level message.""",
        "KUnit error printing",
    )
    text = replace_pattern(
        text,
        r"pub fn info\(args: fmt::Arguments<'_>\) \{\n"
        r".*?\n}\n\n/// Asserts that a boolean expression",
        """pub fn info(args: fmt::Arguments<'_>) {
    #[cfg(not(CONFIG_PRINTK))]
    let _ = args;

    #[cfg(CONFIG_PRINTK)]
    unsafe {
        let mut message = [0u8; crate::print::SELF_CONTAINED_MESSAGE_SIZE];
        let message = crate::print::format_message(args, &mut message);
        bindings::_printk(b"\\x016%s\\0".as_ptr().cast(), message);
    }
}

/// Asserts that a boolean expression""",
        "KUnit info printing",
    )
    return text


def patch_helpers(text: str) -> str:
    # pwm.h emits module metadata even when LTO removes every PWM helper.
    include = '#include "pwm.c"\n'
    if include not in text:
        return text
    return replace_once(
        text,
        include,
        "",
        "unused PWM helper",
    )


def patch_rust_makefile(text: str) -> str:
    start_marker = "bindgen_skip_c_flags :="
    end_marker = "\n\n# Derived from `scripts/Makefile.clang`."
    if text.count(start_marker) != 1:
        raise PatchError("bindgen flag filter: expected one assignment")
    start = text.index(start_marker)
    end = text.find(end_marker, start)
    if end == -1:
        raise PatchError("bindgen flag filter: assignment end not found")

    block = text[start:end]
    if "-fdump-ipa-clones" in block:
        return text
    old = "\t-fno-partial-inlining "
    if block.count(old) != 1:
        raise PatchError("bindgen flag filter: insertion point not found")
    block = block.replace(old, "\t-fdump-ipa-clones -fno-partial-inlining ", 1)
    return text[:start] + block + text[end:]


def patch_str(text: str) -> str:
    # Older trees keep this formatter crate-private, which trips dead-code
    # warnings when the selected kernel configuration omits its only caller.
    text = text.replace(
        "pub(crate) struct NullTerminatedFormatter",
        "pub struct NullTerminatedFormatter",
        1,
    )
    return text.replace(
        "    pub(crate) fn new(buffer: &'a mut [u8])",
        "    pub fn new(buffer: &'a mut [u8])",
        1,
    )


def patched_file(path: Path, patcher) -> str:
    original = path.read_text()
    if "SELF_CONTAINED_MESSAGE_SIZE" in original or "struct SeqWriter" in original:
        raise PatchError(f"{path}: source is already patched")
    patched = patcher(original)
    dependencies = (
        'b"%pA',
        'b"%s: %pA',
        'c"%pA"',
        'c_str!("%pA")',
        '%pA".as_char_ptr()',
        '%pA").as_char_ptr()',
    )
    if any(dependency in patched for dependency in dependencies):
        raise PatchError(f"{path}: a %pA format-string dependency remains")
    return patched


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Adapt copied Linux Rust support for a self-contained external "
            "module build"
        )
    )
    parser.add_argument(
        "source",
        type=Path,
        help="Linux source tree or its rust/kernel directory (modified in place)",
    )
    args = parser.parse_args()

    kernel_dir = args.source / "rust" / "kernel"
    if not kernel_dir.is_dir():
        kernel_dir = args.source

    files = (
        (kernel_dir / "print.rs", patch_print),
        (kernel_dir / "seq_file.rs", patch_seq_file),
        (kernel_dir / "device.rs", patch_device),
        (kernel_dir / "kunit.rs", patch_kunit),
        (kernel_dir / "str.rs", patch_str),
        (kernel_dir.parent / "helpers" / "helpers.c", patch_helpers),
        (kernel_dir.parent / "Makefile", patch_rust_makefile),
    )
    missing = [str(path) for path, _ in files if not path.is_file()]
    if missing:
        parser.exit(1, f"missing Rust support source: {', '.join(missing)}\n")

    try:
        patched = [(path, patched_file(path, patcher)) for path, patcher in files]
    except (OSError, PatchError) as error:
        parser.exit(1, f"runtime compatibility patch failed: {error}\n")

    for path, contents in patched:
        path.write_text(contents)


if __name__ == "__main__":
    main()
