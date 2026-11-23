//! Original USD source semantics through the unified transaction dispatcher.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
#[test]
fn risk_book_contract_unified_usd_page_replay_and_durable_stop() {
    let mut a = store_with_account("usd-A");
    let mut b = store_with_account("usd-B");
    intent(&mut a);
    intent(&mut b);
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
    let flow = CashFlow {
        event_id: "usd-flow".into(),
        kind: "DEPOSIT".into(),
        currency: "USDT".into(),
        amount: Decimal::from(8),
        time: 10,
        source_ref: "bill:deposit".into(),
    };
    let page = IngestPage {
        cash_flows: vec![flow],
        finance: vec![FinanceEntry {
            entry_id: "usd-fee".into(),
            economic_event_key: "fee-event".into(),
            component: "FEE".into(),
            allocation_leg: "account".into(),
            amount: Decimal::new(-1, 2),
            currency: "USDT".into(),
            instance_id: Some("manual".into()),
            source_ref: "bill:fee".into(),
            time: 10,
            reverses_entry_id: None,
        }],
        cursor: Some(SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: "usd-page-complete".into(),
            watermark: 10,
            generation: 1,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    let fx = UsdConversionEvidence {
        observed_at: 10,
        source_digest: "fixture-fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::new(125, 2))]),
    };
    let tx = db.transaction().unwrap();
    let apply = |account: &str, page: IngestPage, conversion: UsdConversionEvidence| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::IngestSourcePageUsd {
                account: account.into(),
                page,
                conversion,
            },
        )
    };
    let mut missing = page.clone();
    missing.cash_flows[0].currency = "BTC".into();
    missing.cash_flows[0].event_id = "blocked-flow".into();
    assert!(matches!(
        apply("usd-A", missing, fx.clone()).unwrap(),
        UnifiedFinanceWriteReceipt::SourcePageRejected {
            reason: "USD_CONVERSION_SOURCE_UNAVAILABLE"
        }
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM cash_flows WHERE event_id='blocked-flow'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM source_cursors WHERE source='bills'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let mut invalid_metadata = fx.clone();
    invalid_metadata.source_digest.clear();
    assert!(matches!(
        apply("usd-B", page.clone(), invalid_metadata),
        Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"))
    ));
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM risk_stops WHERE installation_id='installation' AND account_key='usd-B' AND stop_id='usd-conversion-unavailable'",[],|r|r.get(0)).unwrap(),0,"invalid metadata must not fabricate a conversion observation");
    for account in ["usd-A", "usd-B"] {
        for _ in 0..2 {
            assert!(matches!(
                apply(account, page.clone(), fx.clone()).unwrap(),
                UnifiedFinanceWriteReceipt::SourcePageIngested
            ));
        }
        assert_eq!(tx.query_row::<String,_,_>("SELECT amount FROM finance_totals WHERE installation_id='installation' AND account_key=?1 AND currency='USD' AND component='EXTERNAL_FLOW_USD'",[account],|r|r.get(0)).unwrap(),"10");
        assert_eq!(tx.query_row::<String,_,_>("SELECT campaign_loss FROM account_policy WHERE installation_id='installation' AND account_key=?1",[account],|r|r.get(0)).unwrap(),"0.0125");
        assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_entries WHERE installation_id='installation' AND account_key=?1 AND component='CAMPAIGN_LOSS_USD'",[account],|r|r.get(0)).unwrap(),1);
    }
    let mut negative_rate = fx.clone();
    negative_rate
        .usd_per_currency
        .insert("USDT".into(), Decimal::ZERO);
    let mut new_page = page;
    new_page.cash_flows[0].event_id = "zero-rate-flow".into();
    assert!(matches!(
        apply("usd-B", new_page, negative_rate).unwrap(),
        UnifiedFinanceWriteReceipt::SourcePageRejected {
            reason: "USD_CONVERSION_SOURCE_UNAVAILABLE"
        }
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM cash_flows WHERE event_id='zero-rate-flow'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    tx.commit().unwrap();
    drop(db);
    let reopened = Connection::open(file).unwrap();
    for account in ["usd-A", "usd-B"] {
        assert_eq!(reopened.query_row::<String,_,_>("SELECT reason FROM risk_stops WHERE installation_id='installation' AND account_key=?1 AND stop_id='usd-conversion-unavailable' AND resolution IS NULL",[account],|r|r.get(0)).unwrap(),"USD_CONVERSION_SOURCE_UNAVAILABLE");
        assert_eq!(reopened.query_row::<String,_,_>("SELECT cursor FROM source_cursors WHERE installation_id='installation' AND account_key=?1 AND source='bills'",[account],|r|r.get(0)).unwrap(),"usd-page-complete");
    }
}
