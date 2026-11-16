//! Whole Native SQLite/WAL snapshots from typed fresh account sources.
use super::*;
use crate::finance_snapshot::{backup_stopped_finance, restore_stopped_finance};
use crate::{AccountMeta, CapitalAssignmentCommand, NewFinanceInstallation, OpeningSnapshot};
use rust_decimal::Decimal;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

const INSTALLATION: &str = "installation_snapshot";
fn key(c: char) -> String {
    format!("okx:real:{}", c.to_string().repeat(64))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn root() -> PathBuf {
    let mut nonce = [0u8; 16];
    // Use the standard library clock and a process-local counter, not a new test dependency.
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    nonce[..8].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    nonce[8..].copy_from_slice(
        &(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64)
            .to_le_bytes(),
    );
    let path = std::env::temp_dir().join(format!(
        "okx-finance-snapshot-{}-{:032x}",
        std::process::id(),
        u128::from_le_bytes(nonce)
    ));
    std::fs::create_dir(&path).unwrap();
    path
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut done = owner.termination_signal();
    drop(owner);
    if !*done.borrow() {
        tokio::time::timeout(Duration::from_secs(3), done.changed())
            .await
            .unwrap()
            .unwrap();
    }
}
async fn fresh(path: &PathBuf) -> UnifiedFinanceHandle {
    let owner = UnifiedFinanceHandle::initialize_new(
        path.clone(),
        NewFinanceInstallation {
            installation_id: INSTALLATION.into(),
            initialize_request_id: "snapshot_initialize_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    owner
        .submit(|db| {
            db.set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                true,
            )?;
            db.pragma_update(None, "wal_autocheckpoint", 0)?;
            Ok(())
        })
        .await
        .unwrap();
    for c in ['a', 'b'] {
        let opening = OpeningSnapshot {
            opening_equity_usd: Decimal::from(100),
            source_ref: c.to_string().repeat(64),
            observed_at: 100,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USDT".into(), Decimal::from(100))],
        };
        owner
            .initialize_account(
                deadline(),
                AccountMeta {
                    account_key: key(c),
                    installation_id: INSTALLATION.into(),
                    initialized_at: 100,
                    opening_source: opening.source_ref.clone(),
                    initialize_operation_id: "snapshot_connect_001".into(),
                    release_id: "a".repeat(40),
                },
                opening,
                "c".repeat(64),
            )
            .await
            .unwrap();
    }
    owner
        .apply_campaign_policy(
            deadline(),
            NativeCampaignPolicy {
                campaign_id: format!("sha256:{}", "d".repeat(64)),
                policy_revision: 1,
                generation: 1,
                global_limit_usdt: "10".into(),
                loss_limit_usd: "10".into(),
                entry_limit_usd: "20".into(),
                accounts: ['a', 'b']
                    .into_iter()
                    .map(|c| NativeCampaignAccountPolicy {
                        account_key: key(c),
                        paused: false,
                        limit_usdt: "5".into(),
                        order_limit_usdt: "2".into(),
                        entry_limit_usd: "10".into(),
                        strategies: ["alpha", "manual"]
                            .into_iter()
                            .map(|s| NativeCampaignStrategyPolicy {
                                strategy_id: s.into(),
                                limit_usdt: "5".into(),
                            })
                            .collect(),
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
    owner
        .mutate_strategy_instance(
            deadline(),
            key('a'),
            StrategyMutationCommand {
                operation_id: "snapshot_strategy_001".into(),
                request_hash: "e".repeat(64),
                action: "create".into(),
                instance: crate::StrategyInstance {
                    instance_id: "alpha".into(),
                    definition_id: "alpha".into(),
                    definition_version: "1".into(),
                    parameters: "{}".into(),
                    enabled: false,
                    policy_revision: 1,
                    budget: Decimal::ZERO,
                    last_decision_key: None,
                },
                expected_revision: None,
                time: 101,
            },
        )
        .await
        .unwrap();
    owner
        .assign_capital(
            key('a'),
            CapitalAssignmentCommand {
                account_key: key('a'),
                operation_id: "snapshot_transfer_001".into(),
                request_hash: "f".repeat(64),
                instance_id: "alpha".into(),
                delta_usd: Decimal::from(7),
                authority_revision: 1,
                authority_receipt: "snapshot_governance_001".into(),
                time: 102,
            },
        )
        .await
        .unwrap();
    owner
}
async fn ledger(owner: &UnifiedFinanceHandle) -> Vec<(String, String, String)> {
    owner
        .submit(|db| {
            let mut q = db.prepare(
                "SELECT entry_id,amount,source_ref FROM finance_entries ORDER BY entry_id",
            )?;
            Ok(q.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn risk_book_contract_native_whole_snapshot_includes_uncheckpointed_wal_and_restores_recovery_gate()
 {
    let source = root();
    let owner = fresh(&source).await;
    let identity = owner.installation_receipt(deadline()).await.unwrap();
    let campaign = format!("sha256:{}", "d".repeat(64));
    let budget = owner.campaign_summary(campaign.clone()).await.unwrap();
    let original_ledger = ledger(&owner).await;
    assert_eq!(original_ledger.len(), 2);
    let receipt = owner
        .operation(deadline(), key('a'), "snapshot_transfer_001".into())
        .await
        .unwrap()
        .unwrap();
    let backup = source.join("snapshot");
    let accounts = vec![key('a'), key('b')];
    assert!(matches!(
        backup_stopped_finance(&source, INSTALLATION, &accounts, &backup),
        Err(StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY"))
    ));
    close(owner).await;
    let main = source.join("finance/finance.sqlite");
    let wal = source.join("finance/finance.sqlite-wal");
    assert!(
        std::fs::metadata(&wal).unwrap().len() > 32,
        "the committed money remains in WAL"
    );
    let before_main = std::fs::read(&main).unwrap();
    let before_wal = std::fs::read(&wal).unwrap();
    let manifest = backup_stopped_finance(&source, INSTALLATION, &accounts, &backup).unwrap();
    assert_eq!(
        std::fs::read(&main).unwrap(),
        before_main,
        "backup source remains read-only"
    );
    assert_eq!(std::fs::read(&wal).unwrap(), before_wal);
    assert_eq!(manifest.accounts, accounts);
    assert_eq!(manifest.campaigns, vec![campaign.clone()]);
    assert_eq!(manifest.identity.request_sha256, identity.request_sha256);
    assert!(!backup.join("finance.sqlite-wal").exists());
    assert!(!backup.join("finance.sqlite-shm").exists());
    let target = root();
    std::fs::create_dir(target.join("finance")).unwrap();
    // Published backups are immutable input, including read-only media.
    // SQLite may only open the private staging copy beside the destination.
    let archive = backup.join("finance.sqlite");
    let archive_manifest = backup.join("manifest.json");
    let original_archive = std::fs::read(&archive).unwrap();
    let original_manifest = std::fs::read(&archive_manifest).unwrap();
    let permissions =
        [&archive, &archive_manifest].map(|path| std::fs::metadata(path).unwrap().permissions());
    for (path, original) in [&archive, &archive_manifest].into_iter().zip(&permissions) {
        let mut read_only = original.clone();
        read_only.set_readonly(true);
        std::fs::set_permissions(path, read_only).unwrap();
    }
    let outcome = restore_stopped_finance(&target, INSTALLATION, &accounts, &backup);
    for (path, original) in [&archive, &archive_manifest].into_iter().zip(permissions) {
        std::fs::set_permissions(path, original).unwrap();
    }
    outcome.unwrap();
    assert_eq!(std::fs::read(&archive).unwrap(), original_archive);
    assert_eq!(std::fs::read(&archive_manifest).unwrap(), original_manifest);
    assert!(!backup.join("finance.sqlite-wal").exists());
    assert!(!backup.join("finance.sqlite-shm").exists());
    let restored = UnifiedFinanceHandle::open_native(target.clone(), INSTALLATION.into())
        .await
        .unwrap();
    assert_eq!(
        restored.installation_receipt(deadline()).await.unwrap(),
        identity
    );
    assert_eq!(restored.campaign_summary(campaign).await.unwrap(), budget);
    assert_eq!(ledger(&restored).await, original_ledger);
    assert_eq!(
        restored
            .operation(deadline(), key('a'), "snapshot_transfer_001".into())
            .await
            .unwrap(),
        Some(receipt)
    );
    for c in ['a', 'b'] {
        let audit = restored
            .recovery_snapshot(deadline(), key(c))
            .await
            .unwrap();
        assert!(audit.required);
        assert!(audit.backup_at.is_some());
        assert!(!audit.coverage_complete);
        assert!(matches!(
            restored.capital_state(key(c), 102, deadline()).await,
            Err(StoreError::Invalid(
                "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE"
            ))
        ));
    }
    close(restored).await;
    // Taking another backup while an earlier restore is unresolved must not
    // replace its missing-history cutoff with the later snapshot timestamp.
    let again = target.join("second-snapshot");
    let second = backup_stopped_finance(&target, INSTALLATION, &accounts, &again).unwrap();
    let later = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    assert!(
        later > manifest.captured_at_ms,
        "the second snapshot follows a real complete restore"
    );
    let mut edited = second;
    edited.captured_at_ms = later; // Keep the exact database SHA unchanged.
    std::fs::write(
        again.join("manifest.json"),
        serde_json::to_vec(&edited).unwrap(),
    )
    .unwrap();
    let third = root();
    std::fs::create_dir(third.join("finance")).unwrap();
    restore_stopped_finance(&third, INSTALLATION, &accounts, &again).unwrap();
    let rerecovered = UnifiedFinanceHandle::open_native(third, INSTALLATION.into())
        .await
        .unwrap();
    for c in ['a', 'b'] {
        let audit = rerecovered
            .recovery_snapshot(deadline(), key(c))
            .await
            .unwrap();
        assert!(audit.required);
        assert_eq!(
            audit.backup_at,
            Some(manifest.captured_at_ms),
            "editable manifest time cannot shorten the source audit history"
        );
    }
    close(rerecovered).await;
    assert!(matches!(
        restore_stopped_finance(&target, INSTALLATION, &accounts, &backup),
        Err(StoreError::Invalid("FINANCE_RESTORE_TARGET_EXISTS"))
    ));
}

#[tokio::test]
async fn risk_book_contract_native_snapshot_refuses_tampered_foreign_or_existing_targets() {
    let source = root();
    let owner = fresh(&source).await;
    close(owner).await;
    let accounts = vec![key('a'), key('b')];
    let backup = source.join("snapshot");
    let manifest = backup_stopped_finance(&source, INSTALLATION, &accounts, &backup).unwrap();
    let target = root();
    std::fs::create_dir(target.join("finance")).unwrap();
    assert!(restore_stopped_finance(&target, "foreign-installation", &accounts, &backup).is_err());
    assert!(!target.join("finance/finance.sqlite").exists());
    assert!(restore_stopped_finance(&target, INSTALLATION, &accounts[..1], &backup).is_err());
    assert!(!target.join("finance/finance.sqlite").exists());
    let manifest_path = backup.join("manifest.json");
    let mut bad = manifest.clone();
    bad.sha256 = "0".repeat(64);
    std::fs::write(&manifest_path, serde_json::to_vec(&bad).unwrap()).unwrap();
    assert!(restore_stopped_finance(&target, INSTALLATION, &accounts, &backup).is_err());
    assert!(!target.join("finance/finance.sqlite").exists());
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let sentinel = b"unknown-existing-original";
    let db = target.join("finance/finance.sqlite");
    std::fs::write(&db, sentinel).unwrap();
    assert!(restore_stopped_finance(&target, INSTALLATION, &accounts, &backup).is_err());
    assert_eq!(std::fs::read(&db).unwrap(), sentinel);
    let orphan = root();
    std::fs::create_dir(orphan.join("finance")).unwrap();
    std::fs::write(orphan.join("finance/finance.sqlite-wal"), sentinel).unwrap();
    assert!(restore_stopped_finance(&orphan, INSTALLATION, &accounts, &backup).is_err());
    assert_eq!(
        std::fs::read(orphan.join("finance/finance.sqlite-wal")).unwrap(),
        sentinel
    );
    assert!(!orphan.join("finance/finance.sqlite").exists());
}
