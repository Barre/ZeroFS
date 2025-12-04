fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo::rerun-if-changed=proto/admin.proto");

    // SAFETY: Build scripts are single-threaded
    unsafe {
        std::env::set_var("PROTOC", protobuf_src::protoc());
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/admin.proto"], &["proto/"])?;
    Ok(())
}
