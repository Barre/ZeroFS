pub mod client;
pub mod convert;
pub mod server;

pub mod proto {
    // ObjectOperation's variants share an `Obj` prefix: the proto enum values
    // are `OBJ_*` to avoid colliding with FileOperation's `RENAME` in the same
    // package, and the lint fires on generated code we can't annotate inline.
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("zerofs.admin");
}
