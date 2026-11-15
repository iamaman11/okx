//! Real production writer lifecycle on isolated NEW installations.
use super::UnifiedFinanceHandle;
use crate::{NewFinanceInstallation, StoreError};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

fn fresh_root() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "okx-fresh-owner-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    root
}
fn request() -> NewFinanceInstallation {
    NewFinanceInstallation {
        installation_id: "installation_fresh".into(),
        initialize_request_id: "initialize_fresh_001".into(),
        source_tree: "a".repeat(40),
    }
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut signal = owner.termination_signal();
    drop(owner);
    if !*signal.borrow() {
        tokio::time::timeout(Duration::from_secs(2), signal.changed())
            .await
            .unwrap()
            .unwrap();
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn risk_book_contract_new_owner_exact_replay_reopen_and_no_trade_permission() {
    let root = fresh_root();
    let r = request();
    let owner = UnifiedFinanceHandle::initialize_new(root.clone(), r.clone())
        .await
        .unwrap();
    let initial = owner.installation_receipt(deadline()).await.unwrap();
    assert_eq!(initial.installation_id, r.installation_id);
    assert_eq!(initial.schema_version, 7);
    assert!(matches!(
        owner.verify_live_authority(deadline()).await,
        Err(StoreError::Invalid("FINANCE_ACCOUNT_BOOTSTRAP_REQUIRED"))
    ));
    let second = UnifiedFinanceHandle::open_native(root.clone(), r.installation_id.clone()).await;
    assert!(matches!(
        second,
        Err(StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY"))
    ));
    close(owner).await;
    let replay = UnifiedFinanceHandle::initialize_new(root.clone(), r.clone())
        .await
        .unwrap();
    assert_eq!(
        replay.installation_receipt(deadline()).await.unwrap(),
        initial
    );
    close(replay).await;
    let current = UnifiedFinanceHandle::open_native(root.clone(), r.installation_id.clone())
        .await
        .unwrap();
    assert_eq!(
        current.installation_receipt(deadline()).await.unwrap(),
        initial
    );
    close(current).await;
    let mut changed = r;
    changed.initialize_request_id = "conflicting_request_002".into();
    assert!(matches!(
        UnifiedFinanceHandle::initialize_new(root.clone(), changed).await,
        Err(StoreError::Invalid("FINANCE_INITIALIZE_CONFLICT"))
    ));
    let current = UnifiedFinanceHandle::open_native(root.clone(), "installation_fresh".into())
        .await
        .unwrap();
    assert_eq!(
        current.installation_receipt(deadline()).await.unwrap(),
        initial
    );
    let exists: bool = current
        .submit(|db| {
            Ok(db.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='finance_import_manifest')",
                [],
                |r| r.get(0),
            )?)
        })
        .await
        .unwrap();
    assert!(!exists, "new model has no import/legacy authority record");
    close(current).await;
}

#[tokio::test]
async fn risk_book_contract_new_owner_refuses_unknown_file_and_old_state_without_reset() {
    let root = fresh_root();
    std::fs::create_dir(root.join("finance")).unwrap();
    let target = root.join("finance/finance.sqlite");
    let bytes = b"unknown-original-data-do-not-replace";
    std::fs::write(&target, bytes).unwrap();
    assert!(
        UnifiedFinanceHandle::initialize_new(root.clone(), request())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(target).unwrap(), bytes);
    let old = fresh_root();
    std::fs::create_dir(old.join("accounts")).unwrap();
    std::fs::write(old.join("accounts/original.sqlite"), bytes).unwrap();
    assert!(matches!(
        UnifiedFinanceHandle::initialize_new(old.clone(), request()).await,
        Err(StoreError::Invalid("FINANCE_EXISTING_STATE_NOT_SUPPORTED"))
    ));
    assert!(!old.join("finance/finance.sqlite").exists());
    assert_eq!(
        std::fs::read(old.join("accounts/original.sqlite")).unwrap(),
        bytes
    );
}

#[tokio::test]
async fn risk_book_contract_new_owner_invalid_request_and_open_never_autocreate() {
    let root = fresh_root();
    assert!(
        UnifiedFinanceHandle::open_native(root.clone(), "installation_fresh".into())
            .await
            .is_err()
    );
    assert!(!root.join("finance").exists());
    let mut invalid = request();
    invalid.source_tree = "invalid".into();
    assert!(matches!(
        UnifiedFinanceHandle::initialize_new(root.clone(), invalid).await,
        Err(StoreError::Invalid(
            "FINANCE_INITIALIZATION_REQUEST_INVALID"
        ))
    ));
    assert!(!root.join("finance").exists());
}

#[tokio::test]
async fn risk_book_contract_new_owner_rejects_foreign_sqlite_without_checkpointing_its_wal() {
    use sha2::{Digest, Sha256};
    let root = fresh_root();
    std::fs::create_dir(root.join("finance")).unwrap();
    let path = root.join("finance/finance.sqlite");
    let foreign = rusqlite::Connection::open(&path).unwrap();
    foreign
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
        CREATE TABLE foreign_history(value TEXT); INSERT INTO foreign_history VALUES('preserve');",
        )
        .unwrap();
    // Keep the original writer alive so committed data remains in actual WAL.
    let files = [
        path.clone(),
        root.join("finance/finance.sqlite-wal"),
        root.join("finance/finance.sqlite-shm"),
    ];
    let hashes = || {
        files
            .iter()
            .map(|p| format!("{:x}", Sha256::digest(std::fs::read(p).unwrap())))
            .collect::<Vec<_>>()
    };
    let before = hashes();
    assert!(
        UnifiedFinanceHandle::initialize_new(root.clone(), request())
            .await
            .is_err()
    );
    assert_eq!(
        hashes(),
        before,
        "a rejected foreign main/WAL/SHM must remain unchanged"
    );
    assert_eq!(
        foreign
            .query_row::<String, _, _>("SELECT value FROM foreign_history", [], |r| r.get(0))
            .unwrap(),
        "preserve"
    );
}
