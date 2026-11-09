//! Direct owner gates used by production terminal release. These do not claim
//! to replace the full cohort/Worker census evidence in T09/T10.
use super::*;
use okx_execution::{AccountMeta, OpeningSnapshot};

#[test]
fn terminal_coverage_requires_all_frontiers_without_refreshing_source() {
    let complete = SourceCursor {
        source: "fills".into(),
        stream_partition: "SWAP".into(),
        cursor: String::new(),
        watermark: 100,
        generation: 1,
        coverage_gaps: String::new(),
    };
    assert!(source_covers_terminal(&complete, 100));
    assert!(source_covers_terminal(&complete, 99));
    assert!(!source_covers_terminal(&complete, 101));
    assert!(!source_covers_terminal(
        &SourceCursor {
            generation: 0,
            ..complete.clone()
        },
        100
    ));
    assert!(!source_covers_terminal(
        &SourceCursor {
            cursor: "next-page".into(),
            ..complete.clone()
        },
        100
    ));
    assert!(!source_covers_terminal(
        &SourceCursor {
            coverage_gaps: "missing-page".into(),
            ..complete
        },
        100
    ));
}

#[tokio::test]
async fn missing_real_sql_reservation_denies_opening_but_not_reduction() {
    let root = std::env::temp_dir().join(format!(
        "okx-terminal-gate-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let store = AccountStoreHandle::initialize_flat(
        root.join("account.db"),
        AccountMeta {
            account_key: format!("okx:real:{}", "a".repeat(64)),
            installation_id: "terminal_test".into(),
            initialized_at: 1,
            opening_source: "terminal_fixture".into(),
            initialize_operation_id: "initialize".into(),
            release_id: "a".repeat(40),
        },
        OpeningSnapshot {
            source_ref: "terminal_fixture".into(),
            observed_at: 1,
            opening_equity_usd: Decimal::from(100),
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USD".into(), Decimal::from(100))],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        terminal_receipt(&store, "missing", true).await.unwrap_err(),
        "TERMINAL_CENTRAL_RESERVATION_UNVERIFIED"
    );
    assert!(
        terminal_receipt(&store, "missing", false)
            .await
            .unwrap()
            .is_none()
    );
    // Neither read can manufacture the missing grant.
    assert!(
        store
            .call(deadline(), |s| s.central_receipt("missing"))
            .await
            .unwrap()
            .is_none()
    );
    let mut terminated = store.termination_signal();
    drop(store);
    if !*terminated.borrow() {
        tokio::time::timeout(std::time::Duration::from_secs(3), terminated.changed())
            .await
            .unwrap()
            .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn typed_error_serializes_stage_identity_and_original_reason() {
    let failure = TerminalReleaseError {
        stage: TerminalReleaseStage::Census,
        reason_code: "COHORT_FINANCIAL_SOURCE_INCOMPLETE".into(),
        account: "okx:real:account-a".into(),
        intent_id: "intent-a".into(),
        intent_revision: 7,
        reservation_key: Some("reservation-a".into()),
    };
    let diagnostic = serde_json::to_value(failure).unwrap();
    assert_eq!(diagnostic["stage"], "census");
    assert_eq!(
        diagnostic["reason_code"],
        "COHORT_FINANCIAL_SOURCE_INCOMPLETE"
    );
    assert_eq!(diagnostic["account"], "okx:real:account-a");
    assert_eq!(diagnostic["intent_revision"], 7);
    assert_eq!(diagnostic["reservation_key"], "reservation-a");
}
