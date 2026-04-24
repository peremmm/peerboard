
fn main() {
    println!("cargo:rerun-if-changed=proto/peerboard.proto");
    prost_build::compile_protos(
        &["proto/peerboard.proto"],
        &["proto"],
    ).unwrap();
}