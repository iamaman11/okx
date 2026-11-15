//! Real scoped owner: reopening does not invent recovery or clear another account.
use super::*;
use crate::account_worker::NativeRecoveryProof;
fn proof(at: i64) -> NativeRecoveryProof {
    NativeRecoveryProof {
        generation: 1,
        observed_at_ms: at,
        expected_receipts_digest: format!("{:x}", Sha256::digest(b"[]")),
        active_reservations: 0,
        occupied_loss_usd: "0".into(),
        cumulative_loss_usd: "0".into(),
        exchange_evidence: "e".repeat(64),
        financial_evidence: "f".repeat(64),
    }
}
#[tokio::test]
async fn risk_book_contract_native_storage_recovery_scoped_sources_and_reopen() {
    let (root, owner) = setup().await;
    assert!(
        !owner
            .recovery_snapshot(deadline(), key('a'))
            .await
            .unwrap()
            .required
    );
    assert!(
        !owner
            .recovery_snapshot(deadline(), key('b'))
            .await
            .unwrap()
            .required
    );
    owner
        .apply_campaign_policy(deadline(), policy())
        .await
        .unwrap();
    let at = super::super::super::opening_ingress::owner_now_ms().unwrap();
    // Models an existing restored image's durable storage gate. No live API
    // is allowed to assert or clear this fact without the original audit.
    owner
        .submit(move |db| {
            db.execute(
                "UPDATE storage_recovery SET requires_reconcile=1,backup_at=?1",
                [at - 1],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        owner
            .commit_storage_recovery(deadline(), key('a'), proof(at))
            .await
            .is_err(),
        "missing original source coverage cannot clear gate"
    );
    for (source, part) in [
        ("fills", "SPOT"),
        ("fills", "SWAP"),
        ("fills", "FUTURES"),
        ("fills", "OPTION"),
        ("bills", "account"),
        ("orders", "account"),
        ("account_snapshot", "account"),
        ("recovery_audit", "account"),
    ] {
        owner
            .ingest_source_page(
                key('a'),
                crate::IngestPage {
                    cursor: Some(crate::SourceCursor {
                        source: source.into(),
                        stream_partition: part.into(),
                        cursor: String::new(),
                        watermark: at,
                        generation: 1,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let a = owner.recovery_snapshot(deadline(), key('a')).await.unwrap();
    assert!(a.required && a.coverage_complete);
    assert!(
        !owner
            .recovery_snapshot(deadline(), key('b'))
            .await
            .unwrap()
            .coverage_complete
    );
    let mut wrong = proof(at);
    wrong.generation = 2;
    assert!(matches!(
        owner
            .commit_storage_recovery(deadline(), key('a'), wrong)
            .await,
        Err(StoreError::Invalid("STORAGE_RECOVERY_GENERATION_CHANGED"))
    ));
    let mut altered = proof(at);
    altered.expected_receipts_digest = "a".repeat(64);
    assert!(
        owner
            .commit_storage_recovery(deadline(), key('a'), altered)
            .await
            .is_err()
    );
    let mut changed_budget = proof(at);
    changed_budget.cumulative_loss_usd = "1".into();
    assert!(
        owner
            .commit_storage_recovery(deadline(), key('a'), changed_budget)
            .await
            .is_err()
    );
    assert!(
        owner
            .recovery_snapshot(deadline(), key('a'))
            .await
            .unwrap()
            .required
    );
    owner
        .commit_storage_recovery(deadline(), key('a'), proof(at))
        .await
        .unwrap();
    assert!(
        !owner
            .recovery_snapshot(deadline(), key('a'))
            .await
            .unwrap()
            .required
    );
    assert!(
        owner
            .recovery_snapshot(deadline(), key('b'))
            .await
            .unwrap()
            .required,
        "no other account gate changed"
    );
    assert!(
        owner.verify_live_authority(deadline()).await.is_err(),
        "storage audit is not financial opening or send grant"
    );
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(2), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
    let reopened = UnifiedFinanceHandle::open_native(root.clone(), "installation_native".into())
        .await
        .unwrap();
    assert!(
        !reopened
            .recovery_snapshot(deadline(), key('a'))
            .await
            .unwrap()
            .required
    );
    assert!(
        reopened
            .recovery_snapshot(deadline(), key('b'))
            .await
            .unwrap()
            .required
    );
    close(root, reopened).await;
}
