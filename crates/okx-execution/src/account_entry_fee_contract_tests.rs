//! Scoped entry fee source proof using original durable account operations.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
#[test]
fn risk_book_contract_unified_entry_fee_ownership_and_late_debit() {
    let mut a = store_with_account("fee-A");
    let mut b = store_with_account("fee-B");
    for (store, order) in [(&mut a, "order-A"), (&mut b, "order-B")] {
        intent(store);
        reserve(store);
        assert!(store.begin_post("intent", 1, 4).unwrap());
        store
            .record_outcome(
                "intent",
                IntentState::Acknowledged,
                Some(order),
                "fixture-signed-ack",
                5,
            )
            .unwrap();
        // Existing original local release API, synthetic central evidence;
        // this is not a physical exchange/cutover/financial-census witness.
        store
            .acknowledge_central_release("central-key", "fixture-central-release")
            .unwrap();
    }
    let ap = a.path.clone();
    let bp = b.path.clone();
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(&ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(&bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let file = path();
    let mut target = Connection::open(&file).unwrap();
    target.pragma_update(None, "journal_mode", "WAL").unwrap();
    target.pragma_update(None, "synchronous", "FULL").unwrap();
    target.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut target,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    for (account, venue) in [("fee-A", "order-A"), ("fee-B", "order-B")] {
        let scope = Some(("installation", account));
        for (client, order) in [("stableclient", venue), ("stableclient", ""), ("", venue)] {
            let owner = AccountStore::source_order_owner_sql(&target, scope, client, order)
                .unwrap()
                .unwrap();
            assert_eq!(owner.exchange_order_id.as_deref(), Some(venue));
        }
        let other = if account == "fee-A" {
            "order-B"
        } else {
            "order-A"
        };
        assert!(
            AccountStore::source_order_owner_sql(&target, scope, "", other)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            AccountStore::source_order_owner_sql(&target, scope, "stableclient", other),
            Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"))
        ));
        assert!(matches!(
            AccountStore::source_order_owner_sql(&target, scope, "foreign-client", venue),
            Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"))
        ));
    }
    assert!(matches!(
        AccountStore::source_order_owner_sql(
            &target,
            Some(("installation", "missing-account")),
            "stableclient",
            ""
        ),
        Err(StoreError::Invalid("UNIFIED_SOURCE_ACCOUNT_NOT_FOUND"))
    ));
    let tx = target.transaction().unwrap();
    let make_page = |order: &str| IngestPage {
        bills: vec![Bill {
            bill_id: "same-fee-bill".into(),
            kind: "FEE".into(),
            time: 7,
            amount: Decimal::new(-1, 2),
            currency: "USDT".into(),
            economic_event_key: "bill:same-fee-bill".into(),
        }],
        finance: vec![FinanceEntry {
            entry_id: "same-fee-entry".into(),
            economic_event_key: "bill:same-fee-bill".into(),
            component: "FEE".into(),
            allocation_leg: "account".into(),
            amount: Decimal::new(-1, 2),
            currency: "USDT".into(),
            instance_id: Some("manual".into()),
            source_ref: "bill:same-fee-bill".into(),
            time: 7,
            reverses_entry_id: None,
        }],
        entry_order_fee_links: vec![EntryOrderFeeLink {
            bill_id: "same-fee-bill".into(),
            intent_id: "intent".into(),
            exchange_order_id: order.into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            bill_at: 7,
            fee_usdt: Decimal::new(-1, 2),
        }],
        cursor: Some(SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: "fees-complete".into(),
            watermark: 8,
            generation: 1,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    let apply = |account: &str, page: IngestPage| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::IngestSourcePage {
                account: account.into(),
                page,
            },
        )
    };
    assert!(matches!(
        apply("fee-B", make_page("order-A")),
        Err(StoreError::Invalid("ENTRY_BILL_ORDER_UNVERIFIED"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bills WHERE bill_id='same-fee-bill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    for (account, order) in [("fee-A", "order-A"), ("fee-B", "order-B")] {
        for _ in 0..2 {
            assert!(matches!(
                apply(account, make_page(order)).unwrap(),
                UnifiedFinanceWriteReceipt::SourcePageIngested
            ));
        }
        assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM operation_receipts WHERE installation_id='installation' AND account_key=?1 AND kind IN ('ENTRY_ORDER_FEE_PROVEN','ENTRY_ORDER_FEE_INDEX')",[account],|r|r.get(0)).unwrap(),2);
        assert_eq!(tx.query_row::<String,_,_>("SELECT state FROM risk_receipts WHERE installation_id='installation' AND account_key=?1 AND intent_id='intent'",[account],|r|r.get(0)).unwrap(),"LATE_SETTLEMENT_PENDING");
        assert_eq!(tx.query_row::<String,_,_>("SELECT amount FROM finance_totals WHERE installation_id='installation' AND account_key=?1 AND component='FEE' AND currency='USDT'",[account],|r|r.get(0)).unwrap(),"-0.01");
    }
    let mut early = make_page("order-A");
    early.entry_order_fee_links[0].bill_at = 3;
    assert!(matches!(
        apply("fee-A", early),
        Err(StoreError::Invalid("ENTRY_BILL_ORDER_UNVERIFIED"))
    ));
    let mut bad = make_page("order-A");
    bad.entry_order_fee_links[0].fee_usdt = Decimal::new(-2, 2);
    assert!(matches!(
        apply("fee-A", bad),
        Err(StoreError::Invalid(
            "ENTRY_BILL_AMOUNT_OR_CURRENCY_MISMATCH"
        ))
    ));
    let mut missing = make_page("order-A");
    missing.entry_order_fee_links[0].bill_id = "absent-bill".into();
    assert!(matches!(
        apply("fee-A", missing),
        Err(StoreError::Invalid("ENTRY_BILL_FINANCE_PROVENANCE_MISSING"))
    ));
    let mut stale = make_page("order-A");
    stale.cursor.as_mut().unwrap().watermark = 7;
    stale.bills[0].bill_id = "rollback-bill".into();
    stale.bills[0].economic_event_key = "bill:rollback-bill".into();
    stale.finance[0].entry_id = "rollback-fee".into();
    stale.finance[0].economic_event_key = "bill:rollback-bill".into();
    stale.finance[0].source_ref = "bill:rollback-bill".into();
    stale.entry_order_fee_links[0].bill_id = "rollback-bill".into();
    assert!(matches!(
        apply("fee-A", stale),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM operation_receipts WHERE operation_id LIKE '%rollback-bill%'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bills WHERE bill_id='rollback-bill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    tx.commit().unwrap();
    drop(target);
    let committed = Connection::open(&file).unwrap();
    for account in ["fee-A", "fee-B"] {
        assert_eq!(committed.query_row::<i64,_,_>("SELECT COUNT(*) FROM operation_receipts WHERE installation_id='installation' AND account_key=?1 AND kind='ENTRY_ORDER_FEE_PROVEN'",[account],|r|r.get(0)).unwrap(),1);
        assert_eq!(committed.query_row::<String,_,_>("SELECT amount FROM finance_totals WHERE installation_id='installation' AND account_key=?1 AND component='FEE' AND currency='USDT'",[account],|r|r.get(0)).unwrap(),"-0.01");
    }
    let reopened = Connection::open(sa.path().unwrap()).unwrap(); // Original source never mutated.
    assert_eq!(
        reopened
            .query_row::<i64, _, _>(
                "SELECT COUNT(*) FROM bills WHERE bill_id='same-fee-bill'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        0
    );
}
