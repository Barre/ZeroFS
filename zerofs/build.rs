fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo::rerun-if-changed=proto/admin.proto");

    // Use system protoc if available, otherwise build from source via protobuf-src
    if std::env::var("PROTOC").is_err() && !has_system_protoc() {
        #[cfg(feature = "vendored-protoc")]
        {
            // SAFETY: Build scripts are single-threaded
            unsafe {
                std::env::set_var("PROTOC", protobuf_src::protoc());
            }
        }
        #[cfg(not(feature = "vendored-protoc"))]
        {
            panic!(
                "no system protoc found and the 'vendored-protoc' feature is disabled. \
                 Either install protobuf-compiler or enable the 'vendored-protoc' feature."
            );
        }
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/admin.proto"], &["proto/"])?;
    Ok(())
}

fn has_system_protoc() -> bool {
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok()
}
