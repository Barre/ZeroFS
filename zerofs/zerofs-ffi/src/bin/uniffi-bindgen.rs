//! The uniffi bindings generator for this crate. Build it, then generate
//! bindings from the compiled cdylib, e.g.:
//!
//! ```text
//! cargo run --bin uniffi-bindgen -- generate \
//!     --library target/debug/libzerofs_ffi.so --language python --out-dir out
//! ```
fn main() {
    uniffi::uniffi_bindgen_main()
}
