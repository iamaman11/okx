//! Offline primary-source namespace contract; never an activation or venue grant.
use super::*;

fn prepared_close_source(account: &str, planned: Decimal, state: IntentState) -> AccountStore {
    let mut s = store_with_account(account);
    let definition = s.ensure_manual_instance(1).unwrap();
    let intent = NewIntent {
        plan: IntentPlan {
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            operation: "close".into(),
            side: "sell".into(),
            quantity: planned,
            limit_price: Decimal::from(100),
            stop_price: None,
            strategy_instance: "manual".into(),
            planned_loss_usd: Decimal::ZERO,
            source_generation: 1,
            position_side: "long".into(),
            private_source_generation: 1,
            contract_value: Decimal::ONE,
            inverse: false,
            settlement_currency: "USDT".into(),
        },
        intent_id: "close-intent".into(),
        idempotency_key: "close-request".into(),
        request_hash: "close-body".into(),
        client_order_id: "close-client".into(),
        deadline: 100,
        plan_hash: "close-plan".into(),
        policy_revision: 1,
        risk_increase: false,
        instrument_id: "BTC-USDT-SWAP".into(),
    };
    s.prepare(
        &Decision {
            decision_id: "close-decision".into(),
            instance_id: None,
            decision_at: 2,
            input_digest: "close-input".into(),
            policy_revision: 1,
            code_version: "test".into(),
        },
        &intent,
    )
    .unwrap();
    s.reserve(
        &CentralReceipt {
            reservation_key: "local:close-intent".into(),
            account_key: account.into(),
            installation_id: "installation".into(),
            instance_id: None,
            intent_id: intent.intent_id.clone(),
            plan_hash: intent.plan_hash.clone(),
            policy_revision: 1,
            receipt_hash: "close-receipt".into(),
            requested_loss_usd: Decimal::ZERO,
            conversion_digest: "close-fx".into(),
        },
        3,
    )
    .unwrap();
    assert!(s.begin_post(&intent.intent_id, 1, 4).unwrap());
    s.record_outcome(
        &intent.intent_id,
        state,
        Some("same-order"),
        "fixture-terminal",
        5,
    )
    .unwrap();
    s.ingest_page(&IngestPage {
        cursor: Some(SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: String::new(),
            watermark: 10,
            generation: 1,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        cursor: Some(SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: String::new(),
            watermark: 10,
            generation: 1,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    })
    .unwrap();
    s
}
fn imported() -> Connection {
    imported_close_source(Decimal::ONE, IntentState::Filled)
}

fn imported_close_source(planned: Decimal, state: IntentState) -> Connection {
    let a = prepared_close_source("close-A", planned, state);
    let b = prepared_close_source("close-B", planned, state);
    let (ap, bp) = (a.path.clone(), b.path.clone());
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut db = Connection::open(path()).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    // Explicitly synthetic raw financial facts. The ORIGINAL validator must
    // match each fill's separate bill and account-leg fee, not any sibling row.
    for account in ["close-A", "close-B"] {
        db.execute("INSERT INTO fills VALUES('installation',?1,'same-fill','BTC-USDT-SWAP','same-trade','same-order','close-intent',5,'1','100','-0.01','USDT','sell')",[account]).unwrap();
        db.execute("INSERT INTO swap_fill_sources VALUES('installation',?1,'same-fill','same-bill','4','long','CLOSE')",[account]).unwrap();
    }
    for account in ["close-B"] {
        bill(&db, account);
    }
    db
}
fn bill(db: &Connection, account: &str) {
    db.execute("INSERT INTO bills VALUES('installation',?1,'same-bill','FEE',5,'-0.01','USDT','same-economic-event')",[account]).unwrap();
    db.execute("INSERT INTO bill_order_sources VALUES('installation',?1,'same-bill','BTC-USDT-SWAP','same-order','same-trade')",[account]).unwrap();
    db.execute("INSERT INTO finance_entries VALUES('installation',?1,'same-fee','same-economic-event','FEE','account','-0.01','USDT',NULL,'same-bill',5,NULL)",[account]).unwrap();
}
#[test]
fn risk_book_contract_local_terminal_close_requires_own_bill_and_atomic_ack() {
    let mut db = imported();
    {
        let tx = db.transaction().unwrap();
        let changes = tx.total_changes();
        assert!(matches!(
            AccountStore::acknowledge_local_terminal_close_sql(
                &tx,
                Some(("installation", "close-A")),
                "close-intent",
                "same-order",
                Decimal::ONE,
                5,
                11
            ),
            Err(StoreError::Invalid("LOCAL_CLOSE_FEE_BILL_UNVERIFIED"))
        ));
        assert_eq!(changes, tx.total_changes());
        assert!(
            AccountStore::local_terminal_close_evidence_sql(
                &tx,
                Some(("installation", "close-B")),
                "close-intent",
                "same-order",
                Decimal::ONE,
                5
            )
            .is_ok()
        );
        tx.rollback().unwrap();
    }
    bill(&db, "close-A");
    {
        let tx = db.transaction().unwrap();
        AccountStore::acknowledge_local_terminal_close_sql(
            &tx,
            Some(("installation", "close-A")),
            "close-intent",
            "same-order",
            Decimal::ONE,
            5,
            11,
        )
        .unwrap();
        assert_eq!(
            tx.query_row::<String, _, _>(
                "SELECT state FROM risk_receipts WHERE account_key='close-B'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
            "TERMINAL_PENDING_CENTRAL"
        );
        tx.rollback().unwrap();
    }
    assert_eq!(
        db.query_row::<String, _, _>(
            "SELECT state FROM risk_receipts WHERE account_key='close-A'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        "TERMINAL_PENDING_CENTRAL"
    );
    let tx = db.transaction().unwrap();
    for _ in 0..2 {
        AccountStore::acknowledge_local_terminal_close_sql(
            &tx,
            Some(("installation", "close-A")),
            "close-intent",
            "same-order",
            Decimal::ONE,
            5,
            11,
        )
        .unwrap();
    }
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM operation_receipts WHERE kind='LOCAL_TERMINAL_RELEASE'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    tx.commit().unwrap();
}

#[test]
fn risk_book_contract_local_partial_close_requires_terminal_positive_exact_fill_and_own_bill() {
    let mut db = imported_close_source(Decimal::from(2), IntentState::Cancelled);
    let scope = Some(("installation", "close-B"));
    let tx = db.transaction().unwrap();
    let before = tx.total_changes();
    for invalid in [Decimal::ZERO, Decimal::from(3)] {
        assert!(matches!(
            AccountStore::acknowledge_local_terminal_close_sql(
                &tx,
                scope,
                "close-intent",
                "same-order",
                invalid,
                5,
                11,
            ),
            Err(StoreError::Invalid("LOCAL_CLOSE_TERMINAL_PROOF_INVALID"))
        ));
    }
    assert!(matches!(
        AccountStore::acknowledge_local_terminal_close_sql(
            &tx,
            scope,
            "close-intent",
            "same-order",
            Decimal::new(5, 1),
            5,
            11,
        ),
        Err(StoreError::Invalid("LOCAL_CLOSE_FILL_SUM_MISMATCH"))
    ));
    assert!(matches!(
        AccountStore::acknowledge_local_terminal_close_sql(
            &tx,
            Some(("installation", "close-A")),
            "close-intent",
            "same-order",
            Decimal::ONE,
            5,
            11,
        ),
        Err(StoreError::Invalid("LOCAL_CLOSE_FEE_BILL_UNVERIFIED"))
    ));
    assert_eq!(
        tx.total_changes(),
        before,
        "negative proofs must not acknowledge local money"
    );
    for _ in 0..2 {
        AccountStore::acknowledge_local_terminal_close_sql(
            &tx,
            scope,
            "close-intent",
            "same-order",
            Decimal::ONE,
            5,
            11,
        )
        .unwrap();
    }
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM operation_receipts WHERE kind='LOCAL_TERMINAL_RELEASE'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert_eq!(
        tx.query_row::<String, _, _>(
            "SELECT state FROM risk_receipts WHERE account_key='close-A'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        "TERMINAL_PENDING_CENTRAL"
    );
    tx.commit().unwrap();

    let mut live = imported_close_source(Decimal::from(2), IntentState::Acknowledged);
    let tx = live.transaction().unwrap();
    let before = tx.total_changes();
    assert!(matches!(
        AccountStore::acknowledge_local_terminal_close_sql(
            &tx,
            scope,
            "close-intent",
            "same-order",
            Decimal::ONE,
            5,
            11,
        ),
        Err(StoreError::Invalid("LOCAL_CLOSE_TERMINAL_PROOF_INVALID"))
    ));
    assert_eq!(tx.total_changes(), before);
    tx.rollback().unwrap();
}
