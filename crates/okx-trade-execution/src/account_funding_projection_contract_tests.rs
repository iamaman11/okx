//! Historical funding allocation: same production math, no foreign/future exposure.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
#[test]
fn risk_book_contract_unified_funding_projection_coverage_and_event_time() {
    let template = {
        let mut temp = store();
        intent(&mut temp)
    };
    let mut a = store_with_account("fund-A");
    let mut b = store_with_account("fund-B");
    a.prepare(
        &Decision {
            decision_id: "owned-decision".into(),
            instance_id: Some("manual".into()),
            decision_at: 2,
            input_digest: "fixture".into(),
            policy_revision: 1,
            code_version: "fixture".into(),
        },
        &NewIntent {
            plan: template.plan,
            intent_id: "intent".into(),
            idempotency_key: "fund-request".into(),
            request_hash: "fund-body".into(),
            client_order_id: "fund-client".into(),
            deadline: 100,
            plan_hash: "plan".into(),
            policy_revision: 1,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
        },
    )
    .unwrap();
    a.reserve(
        &CentralReceipt {
            reservation_key: "central-key".into(),
            account_key: a.meta.account_key.clone(),
            installation_id: a.meta.installation_id.clone(),
            instance_id: Some("manual".into()),
            intent_id: "intent".into(),
            plan_hash: "plan".into(),
            policy_revision: 1,
            receipt_hash: "fixture-central".into(),
            requested_loss_usd: Decimal::from(5),
            conversion_digest: "fixture-conversion".into(),
        },
        3,
    )
    .unwrap();
    assert!(a.begin_post("intent", 1, 4).unwrap());
    a.record_outcome(
        "intent",
        IntentState::Acknowledged,
        Some("fund-order"),
        "fixture-ack",
        5,
    )
    .unwrap();
    let fill = Fill {
        fill_id: "shared-fill".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        trade_id: "shared-trade".into(),
        exchange_order_id: "fund-order".into(),
        intent_id: Some("intent".into()),
        time: 7,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::ZERO,
        currency: "USDT".into(),
        side: "buy".into(),
    };
    for (source, owned) in [(&mut a, true), (&mut b, false)] {
        let mut past = fill.clone();
        if !owned {
            past.intent_id = None;
            past.qty = Decimal::from(3);
        }
        let mut fills = vec![past];
        if owned {
            let mut future = fill.clone();
            future.fill_id = "future-external".into();
            future.trade_id = "future-trade".into();
            future.intent_id = None;
            future.time = 20;
            future.qty = Decimal::from(50);
            fills.push(future);
        }
        source
            .ingest_page(&IngestPage {
                fills,
                bills: vec![Bill {
                    bill_id: "shared-funding".into(),
                    kind: "8:173".into(),
                    time: 10,
                    amount: Decimal::new(-2, 1),
                    currency: "USDT".into(),
                    economic_event_key: "bill:shared-funding".into(),
                }],
                funding_events: vec![FundingEvent {
                    event_id: "shared-funding".into(),
                    instrument_id: "BTC-USDT-SWAP".into(),
                    time: 10,
                    amount: Decimal::new(-2, 1),
                    currency: "USDT".into(),
                }],
                ..Default::default()
            })
            .unwrap();
        for (name, part) in [
            ("fills", "SPOT"),
            ("fills", "MARGIN"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
        ] {
            source
                .ingest_page(&IngestPage {
                    cursor: Some(SourceCursor {
                        source: name.into(),
                        stream_partition: part.into(),
                        cursor: String::new(),
                        watermark: 30,
                        generation: 1,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                })
                .unwrap();
        }
    }
    let fx = UsdConversionEvidence {
        observed_at: 30,
        source_digest: "fixture-fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    assert_eq!(b.attribute_pending_funding(&fx, 100).unwrap(), 1);
    assert_eq!(b.attribute_pending_funding(&fx, 100).unwrap(), 0);
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
    let apply = |account: &str, conversion| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::AttributePendingFunding {
                account: account.into(),
                conversion,
                limit: 100,
            },
        )
    };
    let mut cursor =
        AccountStore::scoped_source_cursor_sql(&tx, "installation", "fund-A", "fills", "OPTION")
            .unwrap()
            .unwrap();
    cursor.coverage_gaps = "BACKFILL_PENDING".into();
    AccountStore::upsert_source_cursor_sql(&tx, Some(("installation", "fund-A")), &cursor).unwrap();
    assert!(matches!(
        apply("fund-A", fx.clone()),
        Err(StoreError::Invalid("FUNDING_SOURCE_HISTORY_INCOMPLETE"))
    ));
    cursor.coverage_gaps.clear();
    AccountStore::upsert_source_cursor_sql(&tx, Some(("installation", "fund-A")), &cursor).unwrap();
    tx.execute_batch("SAVEPOINT mixed_generation").unwrap();
    cursor.generation = 2;
    AccountStore::upsert_source_cursor_sql(&tx, Some(("installation", "fund-A")), &cursor).unwrap();
    assert!(matches!(
        apply("fund-A", fx.clone()),
        Err(StoreError::Invalid("FUNDING_SOURCE_HISTORY_INCOMPLETE"))
    ));
    tx.execute_batch("ROLLBACK TO mixed_generation; RELEASE mixed_generation")
        .unwrap();
    tx.execute_batch("SAVEPOINT multi_currency_batch").unwrap();
    AccountStore::ingest_unified_source_page_sql(
        &tx,
        "installation",
        "fund-A",
        &IngestPage {
            bills: vec![Bill {
                bill_id: "second-funding".into(),
                kind: "8:173".into(),
                time: 11,
                amount: Decimal::new(-1, 1),
                currency: "BTC".into(),
                economic_event_key: "bill:second-funding".into(),
            }],
            funding_events: vec![FundingEvent {
                event_id: "second-funding".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                time: 11,
                amount: Decimal::new(-1, 1),
                currency: "BTC".into(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        apply("fund-A", fx.clone()).unwrap(),
        UnifiedFinanceWriteReceipt::SourcePageRejected {
            reason: "USD_CONVERSION_SOURCE_UNAVAILABLE"
        }
    ));
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_entries WHERE account_key='fund-A' AND component='FUNDING'",[],|r|r.get(0)).unwrap(),0,"later missing FX cannot commit an earlier allocation from the same command");
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM funding_projection_pending WHERE account_key='fund-A'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    tx.execute_batch("ROLLBACK TO multi_currency_batch; RELEASE multi_currency_batch")
        .unwrap();
    let missing = UsdConversionEvidence {
        usd_per_currency: Default::default(),
        ..fx.clone()
    };
    assert!(matches!(
        apply("fund-A", missing).unwrap(),
        UnifiedFinanceWriteReceipt::SourcePageRejected {
            reason: "USD_CONVERSION_SOURCE_UNAVAILABLE"
        }
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM funding_projection_pending WHERE account_key='fund-A'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    assert!(matches!(
        apply("fund-A", fx.clone()).unwrap(),
        UnifiedFinanceWriteReceipt::FundingAttributed { count: 1 }
    ));
    assert!(matches!(
        apply("fund-A", fx.clone()).unwrap(),
        UnifiedFinanceWriteReceipt::FundingAttributed { count: 0 }
    ));
    assert!(matches!(
        apply("fund-B", fx.clone()).unwrap(),
        UnifiedFinanceWriteReceipt::FundingAttributed { count: 0 }
    ));
    assert!(matches!(
        apply("foreign-account", fx),
        Err(StoreError::Invalid("UNIFIED_SOURCE_ACCOUNT_NOT_FOUND"))
    ));
    assert_eq!(tx.query_row::<String,_,_>("SELECT allocation_leg FROM finance_entries WHERE account_key='fund-A' AND component='FUNDING' AND amount='-0.2'",[],|r|r.get(0)).unwrap(),"manual");
    assert_eq!(tx.query_row::<String,_,_>("SELECT allocation_leg FROM finance_entries WHERE account_key='fund-B' AND component='FUNDING' AND amount='-0.2'",[],|r|r.get(0)).unwrap(),"unattributed");
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM risk_stops WHERE account_key='fund-A' AND stop_id='funding-attribution-incomplete'",[],|r|r.get(0)).unwrap(),0,"future unowned fill cannot affect earlier funding");
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM risk_stops WHERE account_key='fund-B' AND stop_id='funding-attribution-incomplete'",[],|r|r.get(0)).unwrap(),1);
    tx.commit().unwrap();
    drop(db);
    let reopened = Connection::open(file).unwrap();
    for account in ["fund-A", "fund-B"] {
        assert_eq!(
            reopened
                .query_row::<String, _, _>(
                    "SELECT campaign_loss FROM account_policy WHERE account_key=?1",
                    [account],
                    |r| r.get(0)
                )
                .unwrap(),
            "0.2"
        );
    }
}
