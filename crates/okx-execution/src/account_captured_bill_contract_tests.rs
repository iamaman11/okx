//! Captured bill source logic uses original account preparation and shared SQL.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
fn source_bill(id: &str) -> CapturedBill {
    CapturedBill {
        bill_id: id.into(),
        client_order_id: String::new(),
        order_id: String::new(),
        instrument_id: String::new(),
        trade_id: String::new(),
        bill_type: "9".into(),
        bill_sub_type: String::new(),
        currency: "USDT".into(),
        time: 7,
        fee: None,
        pnl: None,
        balance_change: Some(Decimal::ZERO),
    }
}
fn source_page(order: &str) -> CapturedBillPage {
    let mut fee = source_bill("source-fee");
    fee.client_order_id = "stableclient".into();
    fee.order_id = order.into();
    fee.instrument_id = "BTC-USDT-SWAP".into();
    fee.bill_type = "2".into();
    fee.fee = Some(Decimal::new(-1, 2));
    fee.pnl = Some(Decimal::ZERO);
    fee.balance_change = fee.fee;
    let mut funding = source_bill("source-funding");
    funding.instrument_id = "BTC-USDT-SWAP".into();
    funding.bill_type = "8".into();
    funding.bill_sub_type = "173".into();
    funding.pnl = Some(Decimal::new(-2, 1));
    funding.fee = Some(Decimal::ZERO);
    funding.balance_change = funding.pnl;
    let mut transfer = source_bill("source-transfer");
    transfer.bill_type = "1".into();
    transfer.bill_sub_type = "11".into();
    transfer.balance_change = Some(Decimal::from(10));
    CapturedBillPage {
        cursor: SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: "captured-bills-complete".into(),
            watermark: 10,
            generation: 1,
            coverage_gaps: String::new(),
        },
        rows: vec![fee, funding, transfer],
    }
}
#[test]
fn risk_book_contract_unified_captured_bills_fee_funding_transfer() {
    let fx = UsdConversionEvidence {
        observed_at: 8,
        source_digest: "fixture-fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    let mut a = store_with_account("bill-A");
    let mut b = store_with_account("bill-B");
    for (store, order) in [(&mut a, "order-A"), (&mut b, "order-B")] {
        intent(store);
        reserve(store);
        assert!(store.begin_post("intent", 1, 4).unwrap());
        store
            .record_outcome(
                "intent",
                IntentState::Acknowledged,
                Some(order),
                "fixture-ack",
                5,
            )
            .unwrap();
    }
    for _ in 0..2 {
        a.ingest_captured_bills(&source_page("order-A"), &fx)
            .unwrap();
    }
    let ap = a.path.clone();
    let bp = b.path.clone();
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let file = path();
    let mut db = Connection::open(&file).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    let apply = |input| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::IngestCapturedBills {
                account: "bill-B".into(),
                input,
                conversion: fx.clone(),
            },
        )
    };
    for _ in 0..2 {
        assert!(matches!(
            apply(source_page("order-B")).unwrap(),
            UnifiedFinanceWriteReceipt::SourcePageIngested
        ));
    }
    for account in ["bill-A", "bill-B"] {
        assert_eq!(tx.query_row::<String,_,_>("SELECT campaign_loss FROM account_policy WHERE installation_id='installation' AND account_key=?1",[account],|r|r.get(0)).unwrap(),"0.01");
        assert_eq!(tx.query_row::<String,_,_>("SELECT amount FROM finance_totals WHERE installation_id='installation' AND account_key=?1 AND component='EXTERNAL_FLOW_USD' AND currency='USD'",[account],|r|r.get(0)).unwrap(),"10");
        assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM funding_projection_pending WHERE installation_id='installation' AND account_key=?1 AND event_id='source-funding'",[account],|r|r.get(0)).unwrap(),1);
        assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM operation_receipts WHERE installation_id='installation' AND account_key=?1 AND kind='ENTRY_ORDER_FEE_PROVEN'",[account],|r|r.get(0)).unwrap(),1);
    }
    assert!(matches!(
        apply(source_page("order-A")),
        Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"))
    ));
    let mut absent = source_page("order-B");
    absent.rows[1].pnl = None;
    assert!(matches!(
        apply(absent),
        Err(StoreError::Invalid("FUNDING_AMOUNT_SOURCE_UNAVAILABLE"))
    ));
    let mut bad_transfer = source_page("order-B");
    bad_transfer.rows[2].instrument_id = "BTC-USDT-SWAP".into();
    assert!(matches!(
        apply(bad_transfer),
        Err(StoreError::Invalid("CASH_FLOW_LINKAGE_INVALID"))
    ));
    let mut interest = source_bill("source-interest");
    interest.bill_type = "7".into();
    interest.balance_change = Some(Decimal::new(-3, 2));
    let mut extra = source_page("order-B");
    extra.rows = vec![interest.clone()];
    let mut ambiguous = extra.clone();
    ambiguous.rows[0].fee = Some(Decimal::new(-1, 2));
    assert!(matches!(
        apply(ambiguous),
        Err(StoreError::Invalid("INTEREST_BILL_AMBIGUOUS"))
    ));
    for _ in 0..2 {
        apply(extra.clone()).unwrap();
    }
    assert_eq!(
        tx.query_row::<String, _, _>(
            "SELECT campaign_loss FROM account_policy WHERE account_key='bill-B'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        "0.04"
    );
    extra.rows[0].bill_id = "rollback-interest".into();
    extra.cursor.watermark = 9;
    assert!(matches!(
        apply(extra),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bills WHERE bill_id='rollback-interest'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let mut oversized = source_page("order-B");
    oversized.rows = vec![interest; 101];
    assert!(matches!(
        apply(oversized),
        Err(StoreError::Invalid("BILL_HISTORY_PAGE_TOO_LARGE"))
    ));
    tx.commit().unwrap();
    drop(db);
    let reopened = Connection::open(file).unwrap();
    assert_eq!(
        reopened
            .query_row::<String, _, _>(
                "SELECT campaign_loss FROM account_policy WHERE account_key='bill-B'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        "0.04"
    );
    assert_eq!(
        reopened
            .query_row::<String, _, _>(
                "SELECT campaign_loss FROM account_policy WHERE account_key='bill-A'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        "0.01"
    );
}
