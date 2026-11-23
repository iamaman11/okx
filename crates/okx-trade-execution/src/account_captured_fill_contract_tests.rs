//! Parsed source ingestion uses the real owner binder, USD logic and page dispatcher.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
fn captured(order: &str, trade: &str) -> CapturedFillPage {
    CapturedFillPage {
        partition: "SWAP".into(),
        cursor: SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: "captured-complete".into(),
            watermark: 10,
            generation: 1,
            coverage_gaps: String::new(),
        },
        rows: vec![CapturedFill {
            client_order_id: "stableclient".into(),
            bill_id: format!("bill-{trade}"),
            instrument_type: "SWAP".into(),
            sub_type: "3".into(),
            position_side: "long".into(),
            fill: Fill {
                fill_id: format!("BTC-USDT-SWAP:{trade}:{order}"),
                instrument_id: "BTC-USDT-SWAP".into(),
                trade_id: trade.into(),
                exchange_order_id: order.into(),
                intent_id: None,
                time: 7,
                qty: Decimal::ONE,
                price: Decimal::from(100),
                fee: Decimal::new(-1, 2),
                currency: "USDT".into(),
                side: "buy".into(),
            },
        }],
    }
}
#[test]
fn risk_book_contract_unified_captured_fill_owner_and_usd_commit() {
    let fx = UsdConversionEvidence {
        observed_at: 8,
        source_digest: "fixture-fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    let mut a = store_with_account("captured-A");
    let mut b = store_with_account("captured-B");
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
    // Legacy production entrypoint uses the SAME binder as the unified command.
    for _ in 0..2 {
        a.ingest_captured_fills(&captured("order-A", "same-trade"), &fx)
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
    let apply = |input, conversion| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::IngestCapturedFills {
                account: "captured-B".into(),
                input,
                conversion,
            },
        )
    };
    for _ in 0..2 {
        assert!(matches!(
            apply(captured("order-B", "same-trade"), fx.clone()).unwrap(),
            UnifiedFinanceWriteReceipt::SourcePageIngested
        ));
    }
    for account in ["captured-A", "captured-B"] {
        assert_eq!(tx.query_row::<String,_,_>("SELECT intent_id FROM fills WHERE installation_id='installation' AND account_key=?1 AND trade_id='same-trade'",[account],|r|r.get(0)).unwrap(),"intent");
        assert_eq!(tx.query_row::<String,_,_>("SELECT campaign_loss FROM account_policy WHERE installation_id='installation' AND account_key=?1",[account],|r|r.get(0)).unwrap(),"0.01");
    }
    let mut oversized = captured("order-B", "oversized");
    oversized.rows = vec![oversized.rows[0].clone(); 101];
    assert!(matches!(
        apply(oversized, fx.clone()),
        Err(StoreError::Invalid("FILL_HISTORY_PAGE_TOO_LARGE"))
    ));
    let mut injected = captured("order-B", "injected");
    injected.rows[0].fill.intent_id = Some("intent".into());
    assert!(matches!(
        apply(injected, fx.clone()),
        Err(StoreError::Invalid("CAPTURE_FILL_OWNER_MUST_BE_RESOLVED"))
    ));
    assert!(matches!(
        apply(captured("order-A", "foreign"), fx.clone()),
        Err(StoreError::Invalid("SOURCE_ORDER_IDENTITY_CONFLICT"))
    ));
    let mut wrong_id = captured("order-B", "wrong-id");
    wrong_id.rows[0].fill.fill_id = "invented".into();
    assert!(matches!(
        apply(wrong_id, fx.clone()),
        Err(StoreError::Invalid("CAPTURE_FILL_IDENTITY_INVALID"))
    ));
    let mut wrong_cursor = captured("order-B", "wrong-cursor");
    wrong_cursor.cursor.stream_partition = "SPOT".into();
    assert!(matches!(
        apply(wrong_cursor, fx.clone()),
        Err(StoreError::Invalid("SOURCE_CAPTURE_CURSOR_MISMATCH"))
    ));
    let mut unlinked = captured("order-B", "unlinked");
    unlinked.rows[0].bill_id.clear();
    apply(unlinked, fx.clone()).unwrap();
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_entries WHERE source_ref='fill:BTC-USDT-SWAP:unlinked:order-B'",[],|r|r.get(0)).unwrap(),0,"SWAP without bill ID stores raw fee but cannot debit a canonical fee yet");
    let missing_fx = UsdConversionEvidence {
        usd_per_currency: Default::default(),
        ..fx
    };
    assert!(matches!(
        apply(captured("order-B", "missing-fx"), missing_fx).unwrap(),
        UnifiedFinanceWriteReceipt::SourcePageRejected {
            reason: "USD_CONVERSION_SOURCE_UNAVAILABLE"
        }
    ));
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM fills WHERE trade_id IN ('injected','foreign','wrong-id','wrong-cursor','missing-fx')",[],|r|r.get(0)).unwrap(),0);
    tx.commit().unwrap();
    drop(db);
    let reopened = Connection::open(file).unwrap();
    assert_eq!(reopened.query_row::<String,_,_>("SELECT reason FROM risk_stops WHERE account_key='captured-B' AND stop_id='usd-conversion-unavailable'",[],|r|r.get(0)).unwrap(),"USD_CONVERSION_SOURCE_UNAVAILABLE");
    assert_eq!(
        reopened
            .query_row::<String, _, _>(
                "SELECT campaign_loss FROM account_policy WHERE account_key='captured-B'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        "0.01"
    );
}
