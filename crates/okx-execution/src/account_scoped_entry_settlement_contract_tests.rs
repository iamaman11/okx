//! Original raw fill/bill sources survive namespace migration without borrowing a sibling.
use super::*;

fn seed(account: &str, billed: bool, zero: bool) -> AccountStore {
    let mut s = store_with_account(account);
    let i = intent_with_plan_hash(&mut s, &"e".repeat(64));
    let q = e1_source_quote(&s, &i);
    s.bind_entry_quote(&q).unwrap();
    s.bind_entry_admission(&EntryAdmissionEvidence {
        schema: "okx.entry-admission/v2".into(),
        account_key: account.into(),
        installation_id: s.meta.installation_id.clone(),
        intent_id: i.intent_id.clone(),
        plan_hash: i.plan_hash.clone(),
        quote_digest: format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(&q).unwrap())
        ),
        admission_usd_per_usdt: Decimal::ONE,
        other_entry_costs_upper_usd: Decimal::ZERO,
        entry_upper_usd: q.total_usd,
        entry_proof_digest: "a".repeat(64),
        observed_at: q.observed_at,
    })
    .unwrap();
    acknowledged_entry_intent(&mut s);
    if !zero {
        let fee = Decimal::new(-1, 2);
        let entry = FinanceEntry {
            entry_id: "bill:one:FEE".into(),
            economic_event_key: "bill:one".into(),
            component: "FEE".into(),
            allocation_leg: "account".into(),
            amount: fee,
            currency: "USDT".into(),
            instance_id: None,
            source_ref: "fill:one".into(),
            time: 6,
            reverses_entry_id: None,
        };
        s.ingest_page(&IngestPage {
            fills: vec![Fill {
                fill_id: "one".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                trade_id: "trade-one".into(),
                exchange_order_id: "venue-order".into(),
                intent_id: Some("intent".into()),
                time: 6,
                qty: Decimal::ONE,
                price: Decimal::from(100),
                fee,
                currency: "USDT".into(),
                side: "buy".into(),
            }],
            finance: vec![entry.clone()],
            ..Default::default()
        })
        .unwrap();
        if billed {
            s.ingest_page(&IngestPage {
                bills: vec![Bill {
                    bill_id: "one".into(),
                    kind: "1:trade".into(),
                    time: 7,
                    amount: fee,
                    currency: "USDT".into(),
                    economic_event_key: "bill:one".into(),
                }],
                finance: vec![FinanceEntry {
                    source_ref: "bill:one".into(),
                    time: 7,
                    ..entry
                }],
                ..Default::default()
            })
            .unwrap();
        }
    }
    for (source, partition) in [("fills", "SWAP"), ("bills", "account")] {
        s.ingest_page(&IngestPage {
            cursor: Some(SourceCursor {
                source: source.into(),
                stream_partition: partition.into(),
                cursor: String::new(),
                watermark: 12,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        })
        .unwrap();
    }
    s.record_order_readback(
        "intent",
        "venue-order",
        if zero {
            IntentState::Cancelled
        } else {
            IntentState::Filled
        },
        if zero { Decimal::ZERO } else { Decimal::ONE },
        7,
    )
    .unwrap();
    s
}
fn imported(a: AccountStore, b: AccountStore) -> Connection {
    let ap = a.path.clone();
    let bp = b.path.clone();
    drop(a);
    drop(b);
    let a = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let b = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut db = Connection::open(path()).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&a, &b],
    )
    .unwrap();
    db
}
#[test]
fn risk_book_contract_scoped_entry_sources_reject_sibling_bill_and_bind_replay_exactly() {
    let a = seed("settlement-A", true, false);
    let b = seed("settlement-B", false, false);
    let expected_fill = a.intent_fill_cost_evidence("intent", 12).unwrap();
    let expected_bill = a.intent_fee_bill_evidence("intent", 12).unwrap();
    let mut db = imported(a, b);
    let tx = db.transaction().unwrap();
    let scope = Some(("installation", "settlement-A"));
    let actual = AccountStore::intent_fill_cost_evidence_sql(&tx, scope, "intent", 12).unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected_fill).unwrap()
    );
    assert_eq!(
        serde_json::to_value(
            AccountStore::intent_fee_bill_evidence_sql(&tx, scope, "intent", 12).unwrap()
        )
        .unwrap(),
        serde_json::to_value(expected_bill).unwrap()
    );
    assert!(
        AccountStore::intent_fee_bill_evidence_sql(
            &tx,
            Some(("installation", "settlement-B")),
            "intent",
            12
        )
        .is_err()
    );
    assert!(
        AccountStore::intent_fee_bill_evidence_sql(
            &tx,
            Some(("installation", "missing")),
            "intent",
            12
        )
        .is_err()
    );
    let partial =
        AccountStore::bind_partial_entry_settlement_sql(&tx, scope, "intent", 7, Decimal::ONE, 12)
            .unwrap();
    assert_eq!(partial.cumulative_entry_usd, Decimal::new(10001, 2));
    assert_eq!(partial.settlement_revision, 1);
    assert!(AccountStore::partial_entry_ack_pending_sql(&tx, scope, "intent").unwrap());
    assert!(
        !AccountStore::partial_entry_ack_pending_sql(
            &tx,
            Some(("installation", "settlement-B")),
            "intent"
        )
        .unwrap()
    );
    assert_eq!(
        AccountStore::bind_partial_entry_settlement_sql(&tx, scope, "intent", 7, Decimal::ONE, 12)
            .unwrap(),
        partial
    );
    assert!(
        AccountStore::verify_native_entry_ack_sql(&tx, scope, &partial, false).is_err(),
        "source binding must never invent an accepted RiskBook ACK"
    );
    let terminal =
        AccountStore::bind_terminal_entry_settlement_sql(&tx, scope, "intent", 7, Decimal::ONE, 12)
            .unwrap();
    assert_eq!(terminal.cumulative_entry_usd, partial.cumulative_entry_usd);
    assert_eq!(
        AccountStore::bind_terminal_entry_settlement_sql(&tx, scope, "intent", 7, Decimal::ONE, 12)
            .unwrap(),
        terminal
    );
    tx.commit().unwrap();
    let file = db.path().unwrap().to_owned();
    drop(db);
    let mut db = Connection::open(file).unwrap();
    let tx = db.transaction().unwrap();
    assert_eq!(
        AccountStore::bind_terminal_entry_settlement_sql(&tx, scope, "intent", 7, Decimal::ONE, 12)
            .unwrap(),
        terminal
    );
    assert!(
        AccountStore::bind_terminal_entry_settlement_sql(
            &tx,
            Some(("installation", "settlement-B")),
            "intent",
            7,
            Decimal::ONE,
            12
        )
        .is_err()
    );
    tx.rollback().unwrap();
}
#[test]
fn risk_book_contract_scoped_zero_entry_requires_own_cursor_and_cannot_borrow_other_account() {
    let a = seed("zero-A", true, true);
    let b = seed("zero-B", true, true);
    let mut db = imported(a, b);
    db.execute(
        "DELETE FROM source_cursors WHERE account_key='zero-B' AND source='bills'",
        [],
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    let proof = AccountStore::bind_zero_fill_terminal_entry_settlement_sql(
        &tx,
        Some(("installation", "zero-A")),
        "intent",
        7,
        12,
    )
    .unwrap();
    assert_eq!(proof.cumulative_entry_usd, Decimal::ZERO);
    assert_eq!(proof.venue_filled_quantity, Decimal::ZERO);
    assert!(matches!(
        AccountStore::bind_zero_fill_terminal_entry_settlement_sql(
            &tx,
            Some(("installation", "zero-B")),
            "intent",
            7,
            12
        ),
        Err(StoreError::Invalid("ZERO_ENTRY_COVERAGE_UNPROVEN"))
    ));
    tx.rollback().unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM operation_receipts WHERE kind='ENTRY_SETTLEMENT_PROVEN'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn risk_book_contract_terminal_peer_census_replays_original_raw_sources_without_writes() {
    let mut a = seed("peer-A", true, false);
    let b = seed("peer-B", false, false);
    let original = a
        .bind_terminal_entry_settlement("intent", 7, Decimal::ONE, 12)
        .unwrap();
    let mut db = imported(a, b);
    // Isolated exposure fixture: no live venue state or release is claimed.
    // The caller separately proves venue-flat; this test exercises SQL peers.
    db.execute(
        "UPDATE position_lots SET remaining_qty='0' WHERE account_key='peer-A'",
        [],
    )
    .unwrap();
    let request = || AccountCensusRequest {
        positions: vec![],
        client_order_ids: vec![],
        terminal_peer_release_intent: Some("another-intent".into()),
        limit: 100,
    };
    let tx = db.transaction().unwrap();
    let before = tx.total_changes();
    let valid =
        AccountStore::census_snapshot_sql(&tx, "installation", "peer-A", request(), 13).unwrap();
    assert_eq!(
        valid.verified_terminal_peers,
        std::collections::BTreeSet::from(["intent".into()])
    );
    assert_eq!(
        tx.total_changes(),
        before,
        "receipt replay must remain read-only"
    );
    assert_eq!(
        valid.terminal_peer_sources[0]
            .receipt
            .as_ref()
            .unwrap()
            .result_ref,
        serde_json::to_string(&original).unwrap()
    );
    let sibling =
        AccountStore::census_snapshot_sql(&tx, "installation", "peer-B", request(), 13).unwrap();
    assert!(
        sibling.verified_terminal_peers.is_empty(),
        "A's original terminal receipt cannot verify B"
    );
    tx.rollback().unwrap();
    for mutation in [
        "DELETE FROM source_cursors WHERE account_key='peer-A' AND source='bills'",
        "UPDATE source_cursors SET watermark=6 WHERE account_key='peer-A' AND source='bills'",
        "UPDATE source_cursors SET coverage_gaps='missing' WHERE account_key='peer-A' AND source='bills'",
    ] {
        let tx = db.transaction().unwrap();
        tx.execute_batch(mutation).unwrap();
        let before = tx.total_changes();
        let invalid =
            AccountStore::census_snapshot_sql(&tx, "installation", "peer-A", request(), 13)
                .unwrap();
        assert!(invalid.verified_terminal_peers.is_empty());
        assert_eq!(tx.total_changes(), before);
        tx.rollback().unwrap();
    }
}
