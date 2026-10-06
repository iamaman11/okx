use std::{env, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=OKX_SOURCE_TREE");
    let tree = env::var("OKX_SOURCE_TREE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD^{tree}"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "UNAVAILABLE".to_owned());
    println!("cargo:rustc-env=OKX_SOURCE_TREE={tree}");
}
