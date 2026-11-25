#[test]
fn runtime_contract_requires_no_installation_or_usable_configuration() {
    let root = std::env::temp_dir().join(format!(
        "okx-engine-contract-{}-{}",
        std::process::id(),
        jiff::Timestamp::now().as_nanosecond()
    ));
    assert!(!root.exists());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_okx-agent"))
        .arg("runtime-contract")
        .arg("--root")
        .arg(&root)
        .args(["--key-id", "unavailable-key", "--region", "invalid-region"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    let stdout = std::str::from_utf8(&output.stdout).unwrap();
    assert_eq!(
        stdout.trim_end(),
        r#"{"schema":"okx.engine.runtime-contract/v1","runtime_owner":"engine","research_child":false}"#
    );
    assert!(!root.exists());
}
