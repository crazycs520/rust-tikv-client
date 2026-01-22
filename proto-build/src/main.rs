// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

fn main() {
    let mut protos = glob::glob("proto/*.proto")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for file in ["executor.proto", "expression.proto", "schema.proto"] {
        protos.push(PathBuf::from("../tipb/proto").join(file));
    }

    tonic_build::configure()
        .emit_rerun_if_changed(false)
        .build_server(false)
        .include_file("mod.rs")
        .out_dir("src/generated")
        .compile(&protos, &["proto/include", "proto", "../tipb/proto"])
        .unwrap();
}
