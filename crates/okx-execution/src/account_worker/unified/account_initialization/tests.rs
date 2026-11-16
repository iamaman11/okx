//! Real production single-writer bootstrap, no HTTP or trading permission.
use super::*;
use crate::NewFinanceInstallation;
use rust_decimal::Decimal;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "okx-native-account-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    path
}
async fn owner(root: &std::path::Path) -> UnifiedFinanceHandle {
    UnifiedFinanceHandle::initialize_new(
        root.to_path_buf(),
        NewFinanceInstallation {
            installation_id: "installation_native".into(),
            initialize_request_id: "initialize_native_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap()
}
fn source(uid: char) -> (AccountMeta, OpeningSnapshot) {
    let snapshot = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: uid.to_string().repeat(64),
        observed_at: 100,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    (
        AccountMeta {
            account_key: format!("okx:real:{}", uid.to_string().repeat(64)),
            installation_id: "installation_native".into(),
            initialized_at: 100,
            opening_source: snapshot.source_ref.clone(),
            initialize_operation_id: "connect_native_001".into(),
            release_id: "a".repeat(40),
        },
        snapshot,
    )
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(2), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
}
async fn counts(owner: &UnifiedFinanceHandle) -> (i64, i64, i64) {
    owner
        .submit(|db| {
            Ok((
                db.query_row("SELECT COUNT(*) FROM account_meta", [], |r| r.get(0))?,
                db.query_row("SELECT COUNT(*) FROM cash_flows", [], |r| r.get(0))?,
                db.query_row("SELECT COUNT(*) FROM operation_receipts", [], |r| r.get(0))?,
            ))
        })
        .await
        .unwrap()
}

/// Original one-owner diagnostic restore: same account scope for both
/// projections, no second SQL owner, no inferred absence on foreign scope.
#[tokio::test]
async fn risk_book_contract_native_durable_operation_diagnostics_scoped_readback() {
    let root = root();
    let owner = owner(&root).await;
    let (a, opening_a) = source('a');
    let (b, opening_b) = source('b');
    owner
        .initialize_account(deadline(), a.clone(), opening_a, "c".repeat(64))
        .await
        .unwrap();
    owner
        .initialize_account(deadline(), b.clone(), opening_b, "d".repeat(64))
        .await
        .unwrap();
    let account_a = a.account_key.clone();
    let account_b = b.account_key.clone();
    let row_account = account_a.clone();
    owner
        .submit(move |db| {
            db.execute(
                "INSERT INTO operation_receipts
                    (installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,?2,'cleanup_venue-a','protection_cleanup',?3,'ACK',100)",
                rusqlite::params!["installation_native", row_account, "f".repeat(64)],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let first = owner
        .durable_operation_diagnostics(deadline(), account_a.clone(), 128, 128)
        .await
        .unwrap();
    assert_eq!(first.0.len(), 1);
    assert_eq!(first.0[0].operation_id, "cleanup_venue-a");
    assert!(first.1.is_empty());
    let replay = owner
        .durable_operation_diagnostics(deadline(), account_a, 128, 128)
        .await
        .unwrap();
    assert_eq!(replay.0.len(), 1);
    assert!(replay.1.is_empty());
    let other = owner
        .durable_operation_diagnostics(deadline(), account_b, 128, 128)
        .await
        .unwrap();
    assert!(other.0.is_empty());
    assert!(other.1.is_empty());
    assert!(owner
        .durable_operation_diagnostics(
            deadline(),
            format!("okx:real:{}", "0".repeat(64)),
            128,
            128,
        )
        .await
        .is_err());
    close(owner).await;
    assert!(!root.join("accounts").exists(), "no per-account financial SQL");
}

#[tokio::test]
async fn risk_book_contract_native_account_initialization_scoped_replay_and_reopen() {
    let root = root();
    let owner = owner(&root).await;
    let (a, opening_a) = source('a');
    let (b, opening_b) = source('b');
    assert_eq!(
        owner
            .initialize_account(deadline(), a.clone(), opening_a.clone(), "c".repeat(64))
            .await
            .unwrap(),
        a
    );
    assert_eq!(
        owner
            .initialize_account(deadline(), b.clone(), opening_b.clone(), "d".repeat(64))
            .await
            .unwrap(),
        b
    );
    assert_eq!(counts(&owner).await, (2, 2, 6));
    let account = a.account_key.clone();
    let initialization = a.initialize_operation_id.clone();
    assert!(
        owner
            .submit(move |db| {
                let ids = [
                    initialization.clone(),
                    format!("account-connect:{initialization}"),
                ];
                for id in &ids {
                    if db
                        .execute(
                            "UPDATE operation_receipts SET result_ref='tampered'
                WHERE installation_id='installation_native' AND account_key=?1 AND operation_id=?2",
                            params![account, id],
                        )
                        .is_ok()
                    {
                        return Ok(false);
                    }
                    if db
                        .execute(
                            "DELETE FROM operation_receipts
                WHERE installation_id='installation_native' AND account_key=?1 AND operation_id=?2",
                            params![account, id],
                        )
                        .is_ok()
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .await
            .unwrap(),
        "INITIALIZE and ACCOUNT_CONNECTED receipts are immutable in SQL"
    );
    assert_eq!(counts(&owner).await, (2, 2, 6));
    owner
        .initialize_account(deadline(), a.clone(), opening_a.clone(), "c".repeat(64))
        .await
        .unwrap();
    assert_eq!(
        counts(&owner).await,
        (2, 2, 6),
        "exact reply loss replay has no second opening or receipt"
    );
    let facts = owner
        .submit(|db| {
            let mut stmt = db.prepare(
                "SELECT account_key,event_id,amount FROM cash_flows ORDER BY account_key",
            )?;
            Ok(stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
        .unwrap();
    assert_eq!(
        facts,
        vec![
            (a.account_key.clone(), "opening:USDT".into(), "100".into()),
            (b.account_key.clone(), "opening:USDT".into(), "100".into())
        ]
    );
    assert!(matches!(
        owner.verify_live_authority(deadline()).await,
        Err(StoreError::Invalid("FINANCE_CAMPAIGN_POLICY_REQUIRED"))
    ));
    let observed = crate::IngestPage {
        cursor: Some(crate::SourceCursor {
            source: "fills".into(),
            stream_partition: "SPOT".into(),
            cursor: String::new(),
            watermark: 101,
            generation: 1,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    owner
        .ingest_source_page(a.account_key.clone(), observed.clone())
        .await
        .unwrap();
    assert!(
        owner
            .ingest_source_page(format!("okx:real:{}", "f".repeat(64)), observed)
            .await
            .is_err()
    );
    assert!(matches!(
        owner.verify_live_authority(deadline()).await,
        Err(StoreError::Invalid("FINANCE_CAMPAIGN_POLICY_REQUIRED"))
    ));
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root.clone(), "installation_native".into())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .account_metadata(deadline(), a.account_key.clone())
            .await
            .unwrap(),
        Some(a.clone())
    );
    assert_eq!(
        reopened
            .account_metadata(deadline(), b.account_key.clone())
            .await
            .unwrap(),
        Some(b)
    );
    let receipt = reopened
        .operation(
            deadline(),
            a.account_key.clone(),
            format!("account-connect:{}", a.initialize_operation_id),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.kind, "ACCOUNT_CONNECTED");
    assert_eq!(receipt.request_hash, "c".repeat(64));
    assert_eq!(counts(&reopened).await, (2, 2, 6));
    assert!(
        !root.join("accounts").exists(),
        "no account-local financial database"
    );
    close(reopened).await;
}

#[tokio::test]
async fn risk_book_contract_native_account_initialization_refuses_bad_sources_atomically() {
    let root = root();
    let owner = owner(&root).await;
    let (meta, opening) = source('a');
    for kind in 0..6 {
        let mut m = meta.clone();
        let mut s = opening.clone();
        match kind {
            0 => m.installation_id = "foreign".into(),
            1 => s.positions = 1,
            2 => s.liabilities = Decimal::ONE,
            3 => s.balances[0].1 = -Decimal::ONE,
            4 => s.source_ref = "e".repeat(64),
            _ => s.balances.push(s.balances[0].clone()),
        };
        assert!(
            owner
                .initialize_account(deadline(), m, s, "c".repeat(64))
                .await
                .is_err()
        );
        assert_eq!(counts(&owner).await, (0, 0, 0));
    }
    owner
        .initialize_account(deadline(), meta.clone(), opening.clone(), "c".repeat(64))
        .await
        .unwrap();
    let before = counts(&owner).await;
    assert!(matches!(
        owner
            .initialize_account(deadline(), meta.clone(), opening.clone(), "d".repeat(64))
            .await,
        Err(StoreError::Invalid("INITIALIZE_OPERATION_CONFLICT"))
    ));
    let mut conflicting = opening;
    conflicting.balances[0].1 = Decimal::from(200);
    assert!(matches!(
        owner
            .initialize_account(deadline(), meta.clone(), conflicting, "c".repeat(64))
            .await,
        Err(StoreError::Invalid("INITIALIZE_OPERATION_CONFLICT"))
    ));
    assert_eq!(counts(&owner).await, before);
    assert_eq!(
        owner
            .account_metadata(deadline(), meta.account_key.clone())
            .await
            .unwrap(),
        Some(meta)
    );
    close(owner).await;
}
