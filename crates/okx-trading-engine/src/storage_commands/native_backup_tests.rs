use super::*;

fn root() -> PathBuf {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = std::env::temp_dir().join(format!(
        "okx-native-maintenance-{:032x}",
        u128::from_le_bytes(nonce)
    ));
    fs::create_dir(&root).unwrap();
    root
}
async fn initialized(root: &Path) {
    let request = okx_trade_execution::NewFinanceInstallation {
        installation_id: "installation_native_backup".into(),
        initialize_request_id: "initialize_backup_001".into(),
        source_tree: "a".repeat(40),
    };
    // The same metadata record written by the explicit installation command;
    // it names the actual typed request, not a financial permission marker.
    crate::accounts::initialize_installation(root, &request.installation_id).unwrap();
    fs::write(root.join("installation-operation.json"),serde_json::to_vec(&json!({
        "installation_id":request.installation_id,"operation_id":request.initialize_request_id,"release":request.source_tree
    })).unwrap()).unwrap();
    let owner = okx_trade_execution::account_worker::UnifiedFinanceHandle::initialize_new(
        root.to_path_buf(),
        request,
    )
    .await
    .unwrap();
    let mut stopped = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), stopped.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn native_whole_backup_wrapper_requires_original_metadata_catalog_and_stopped_engine() {
    let source = root();
    initialized(&source).await;
    // Created by the existing account connection owner on an ordinary used
    // installation. It is metadata, not another financial database.
    fs::create_dir(source.join("account-leases")).unwrap();
    let engine = engine_lease(&source).unwrap();
    assert!(
        backup(&source)
            .unwrap_err()
            .to_string()
            .contains("ENGINE_MUST_BE_STOPPED")
    );
    drop(engine);
    let snapshot = backup(&source).unwrap();
    assert_eq!(snapshot["scope"], "whole_installation");
    let id = snapshot["snapshot_id"].as_str().unwrap();
    assert!(snapshot_id(id));
    let original = hash(&source.join("finance/finance.sqlite")).unwrap();
    assert!(
        restore(&source, id, "restore_backup_001")
            .unwrap_err()
            .to_string()
            .contains("FINANCE_RESTORE_TARGET_EXISTS")
    );
    assert_eq!(
        hash(&source.join("finance/finance.sqlite")).unwrap(),
        original
    );
    let target = root();
    fs::create_dir(target.join("finance")).unwrap();
    fs::create_dir(target.join("finance-backups")).unwrap();
    fs::create_dir(target.join("finance-backups").join(id)).unwrap();
    for name in [
        account_catalog_proto::CATALOG_FILE,
        "installation-operation.json",
    ] {
        fs::copy(source.join(name), target.join(name)).unwrap();
    }
    for name in ["finance.sqlite", "manifest.json"] {
        fs::copy(
            source.join("finance-backups").join(id).join(name),
            target.join("finance-backups").join(id).join(name),
        )
        .unwrap();
    }
    let metadata = fs::read(target.join("installation-operation.json")).unwrap();
    let mut bad: Value = serde_json::from_slice(&metadata).unwrap();
    bad["operation_id"] = "foreign_initialize_001".into();
    fs::write(
        target.join("installation-operation.json"),
        serde_json::to_vec(&bad).unwrap(),
    )
    .unwrap();
    assert!(
        restore(&target, id, "restore_backup_001")
            .unwrap_err()
            .to_string()
            .contains("FINANCE_SNAPSHOT_SOURCE_IDENTITY_MISMATCH")
    );
    assert!(!target.join("finance/finance.sqlite").exists());
    fs::write(target.join("installation-operation.json"), metadata).unwrap();
    fs::write(target.join("unknown-financial-data"), b"preserve").unwrap();
    assert!(
        restore(&target, id, "restore_backup_001")
            .unwrap_err()
            .to_string()
            .contains("FINANCE_MAINTENANCE_UNKNOWN_ROOT_ENTRY")
    );
    assert_eq!(
        fs::read(target.join("unknown-financial-data")).unwrap(),
        b"preserve"
    );
    assert!(!target.join("finance/finance.sqlite").exists());
    // Remove only this test's known obstruction to exercise the valid route.
    fs::remove_file(target.join("unknown-financial-data")).unwrap();
    let restored = restore(&target, id, "restore_backup_001").unwrap();
    assert_eq!(restored["reconcile_required"], true);
    assert_eq!(restored["source_readiness"], "NOT_INFERRED");
    let owner = okx_trade_execution::account_worker::UnifiedFinanceHandle::open_native(
        target.clone(),
        "installation_native_backup".into(),
    )
    .await
    .unwrap();
    let mut stopped = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), stopped.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    assert!(!target.join("accounts").exists());
    fs::remove_dir_all(target).unwrap();
    fs::remove_dir_all(source).unwrap();
}
