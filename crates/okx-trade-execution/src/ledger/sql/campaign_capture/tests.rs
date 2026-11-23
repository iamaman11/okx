//! Namespace witnesses on the real original schema and rule implementation.
use super::*;

const INSTALLATION: &str = "campaign-installation";
fn campaign() -> String {
    format!("sha256:{}", "a".repeat(64))
}

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(include_str!("../../../../schema/finance-unified-v1.sql"))
        .unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    for account in ["account-A", "account-B"] {
        db.execute(
            "INSERT INTO account_meta VALUES(1,?1,?2,2,1,'test','init','test')",
            params![account, INSTALLATION],
        )
        .unwrap();
        db.execute(
            "INSERT INTO campaign_source_epoch VALUES(?1,?2,1,0)",
            params![INSTALLATION, account],
        )
        .unwrap();
        db.execute(
            "INSERT INTO campaign_late_head_epoch VALUES(?1,?2,1,0)",
            params![INSTALLATION, account],
        )
        .unwrap();
        for (source, partition) in [
            ("fills", "SPOT"),
            ("fills", "MARGIN"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
        ] {
            db.execute(
                "INSERT INTO source_cursors VALUES(?1,?2,?3,?4,'',100,1,'')",
                params![INSTALLATION, account, source, partition],
            )
            .unwrap();
        }
    }
    db
}

#[test]
fn scoped_campaign_baselines_and_readiness_preserve_two_accounts_same_campaign() {
    let mut db = fixture();
    let campaign = campaign();
    for account in ["account-A", "account-B"] {
        let tx = db.transaction().unwrap();
        let scope = Some((INSTALLATION, account));
        let (money, proof) =
            AccountStore::seal_captured_campaign_baseline_sql(&tx, scope, &campaign, 2, 90)
                .unwrap();
        assert_eq!(money, Decimal::ZERO);
        assert_eq!(proof.len(), 64);
        AccountStore::verify_campaign_entry_readiness_sql(&tx, scope, &campaign, 2, 90).unwrap();
        assert_eq!(
            AccountStore::seal_captured_campaign_baseline_sql(&tx, scope, &campaign, 2, 90)
                .unwrap(),
            (money, proof)
        );
        tx.commit().unwrap();
    }
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM campaign_entry_totals", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
    let tx = db.transaction().unwrap();
    AccountStore::refresh_campaign_entry_readiness_sql(
        &tx,
        Some((INSTALLATION, "account-A")),
        &campaign,
        2,
        95,
    )
    .unwrap();
    let covered: i64 = tx
        .query_row(
            "SELECT covered_through_ms FROM campaign_entry_readiness WHERE account_key='account-B'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(covered, 90, "A's refresh must not advance B's witness");
    assert!(
        AccountStore::verify_campaign_entry_readiness_sql(
            &tx,
            Some((INSTALLATION, "foreign")),
            &campaign,
            2,
            95
        )
        .is_err()
    );
    assert!(
        AccountStore::seal_captured_campaign_baseline_sql(
            &tx,
            Some(("foreign", "account-A")),
            &campaign,
            2,
            95
        )
        .is_err()
    );
    tx.commit().unwrap();
}

#[test]
fn scoped_campaign_cursor_missing_on_b_cannot_borrow_a_complete_capture() {
    let mut db = fixture();
    db.execute(
        "DELETE FROM source_cursors WHERE account_key='account-B' AND stream_partition='SWAP'",
        [],
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    let campaign = campaign();
    assert!(matches!(
        AccountStore::seal_captured_campaign_baseline_sql(
            &tx,
            Some((INSTALLATION, "account-B")),
            &campaign,
            2,
            90
        ),
        Err(StoreError::Invalid("ENTRY_BASELINE_CAPTURE_PENDING"))
    ));
    AccountStore::seal_captured_campaign_baseline_sql(
        &tx,
        Some((INSTALLATION, "account-A")),
        &campaign,
        2,
        90,
    )
    .unwrap();
    let b_receipts: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM operation_receipts WHERE account_key='account-B'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(b_receipts, 0);
    tx.commit().unwrap();
}

#[test]
fn scoped_campaign_delta_same_intent_and_revision_never_reuses_other_account_money() {
    let mut db = fixture();
    let campaign = campaign();
    let tx = db.transaction().unwrap();
    for (account, amount, digest) in [
        ("account-A", Decimal::ONE, "a".repeat(64)),
        ("account-B", Decimal::TEN, "b".repeat(64)),
    ] {
        let capture = CampaignCapture::from_scope(&tx, Some((INSTALLATION, account))).unwrap();
        let receipts = vec![("same-intent".into(), digest, amount, 1, 50)];
        capture
            .apply_captured_campaign_deltas(&campaign, 2, 90, &receipts, amount, None)
            .unwrap();
        capture
            .apply_captured_campaign_deltas(&campaign, 2, 90, &receipts, amount, None)
            .unwrap();
    }
    for (account, expected) in [("account-A", "1"), ("account-B", "10")] {
        let actual: String = tx.query_row("SELECT consumed_usd FROM campaign_entry_totals WHERE installation_id=?1 AND account_key=?2 AND campaign_id=?3", params![INSTALLATION,account,campaign], |r|r.get(0)).unwrap();
        assert_eq!(actual, expected);
    }
    let count: i64 = tx
        .query_row("SELECT COUNT(*) FROM campaign_entry_applied", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
    tx.rollback().unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM campaign_entry_totals", [], |r| r
            .get(0))
            .unwrap(),
        0
    );
}

#[test]
fn scoped_campaign_readiness_never_hides_unknown_other_account_or_stale_source() {
    let mut db = fixture();
    let campaign = campaign();
    let tx = db.transaction().unwrap();
    let a = CampaignCapture::from_scope(&tx, Some((INSTALLATION, "account-A"))).unwrap();
    let b = CampaignCapture::from_scope(&tx, Some((INSTALLATION, "account-B"))).unwrap();
    a.seal_captured_campaign_baseline(&campaign, 2, 90).unwrap();
    b.seal_captured_campaign_baseline(&campaign, 2, 90).unwrap();
    tx.execute("UPDATE source_cursors SET generation=2 WHERE account_key='account-B' AND stream_partition='SWAP'", []).unwrap();
    let before = tx.total_changes();
    a.verify_campaign_entry_readiness(&campaign, 2, 90).unwrap();
    assert!(matches!(
        b.verify_campaign_entry_readiness(&campaign, 2, 90),
        Err(StoreError::Invalid("CAMPAIGN_READINESS_STALE"))
    ));
    assert_eq!(
        tx.total_changes(),
        before,
        "verification must not manufacture a witness"
    );
    tx.rollback().unwrap();
}
