//! Compile the vendored Sui v2 protos with tonic-build, using a vendored
//! `protoc` binary so no system install is required.
//!
//! The proto tree lives under `proto/` with `sui/rpc/v2/*.proto` plus the
//! transitive google/protobuf and google/rpc dependencies — vendored at a
//! pinned upstream rev by scripts/vendor-protos.sh.

use std::path::{Path, PathBuf};

fn main() -> anyhow::Result<()> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");

    // Point tonic-build at a vendored protoc instead of system PATH.
    let protoc =
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc binary present");
    std::env::set_var("PROTOC", &protoc);

    let mut protos: Vec<PathBuf> = Vec::new();
    collect_protos(&proto_root, &mut protos)?;
    if protos.is_empty() {
        anyhow::bail!(
            "no .proto files found under {} — run scripts/vendor-protos.sh first",
            proto_root.display()
        );
    }

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        // Don't try to compile google/protobuf well-known types — use prost-types instead.
        .compile_well_known_types(false)
        .compile_protos(&protos, &[&proto_root])?;

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}

fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            collect_protos(&p, out)?;
        } else if p.extension().and_then(|s| s.to_str()) == Some("proto") {
            out.push(p);
        }
    }
    Ok(())
}
