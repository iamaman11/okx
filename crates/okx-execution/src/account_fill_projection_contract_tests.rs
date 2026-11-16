//! Raw fill replay through the original FIFO projector and sole installation transaction.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
fn seed(s: &mut AccountStore, contract_value: Decimal) {
    let definition = s.ensure_manual_instance(1).unwrap();
    let q = Decimal::from(3);
    for (name, operation, side, risk_increase) in [
        ("v02-open", "open", "buy", true),
        ("v02-close", "close", "sell", false),
    ] {
        let decision = Decision {
            decision_id: format!("decision:{name}"),
            instance_id: Some("manual".into()),
            decision_at: 2,
            input_digest: format!("input:{name}"),
            policy_revision: 1,
            code_version: "fixture-v02".into(),
        };
        let request = NewIntent {
            plan: IntentPlan {
                strategy_definition_id: definition.definition_id.clone(),
                strategy_definition_version: definition.version.clone(),
                strategy_parameters_digest: definition.parameters_digest.clone(),
                operation: operation.into(),
                side: side.into(),
                position_side: "long".into(),
                quantity: q,
                limit_price: Decimal::from(if risk_increase { 101 } else { 102 }),
                stop_price: if risk_increase {
                    Some(Decimal::from(95))
                } else {
                    None
                },
                strategy_instance: "manual".into(),
                planned_loss_usd: if risk_increase {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                },
                source_generation: 1,
                private_source_generation: 1,
                contract_value,
                inverse: false,
                settlement_currency: "USDT".into(),
            },
            intent_id: name.into(),
            idempotency_key: name.into(),
            request_hash: format!("hash:{name}"),
            client_order_id: format!("client:{name}"),
            deadline: 100,
            plan_hash: format!("plan:{name}"),
            policy_revision: 1,
            risk_increase,
            instrument_id: "BTC-USDT-SWAP".into(),
        };
        s.prepare(&decision, &request).unwrap();
    }

    let first = Fill {
        fill_id: "open-1".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        trade_id: "trade-1".into(),
        exchange_order_id: "order-open".into(),
        intent_id: Some("v02-open".into()),
        time: 3,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::ZERO,
        currency: "USDT".into(),
        side: "buy".into(),
    };
    let second = Fill {
        fill_id: "open-2".into(),
        trade_id: "trade-2".into(),
        time: 4,
        qty: Decimal::from(2),
        price: Decimal::from(110),
        ..first.clone()
    };
    let close = Fill {
        fill_id: "close".into(),
        trade_id: "trade-close".into(),
        exchange_order_id: "order-close".into(),
        intent_id: Some("v02-close".into()),
        time: 5,
        qty: Decimal::from(2),
        price: Decimal::from(120),
        side: "sell".into(),
        ..first.clone()
    };
    s.ingest_page(&IngestPage {
        fills: vec![first, second, close],
        ..Default::default()
    })
    .unwrap();
}
#[test]
fn risk_book_contract_pending_fills_fifo_pnl_account_scope_and_atomic_replay() {
    let mut a = store_with_account("fifo-A");
    let mut b = store_with_account("fifo-B");
    seed(&mut a, Decimal::ONE);
    seed(&mut b, Decimal::from(2));
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
    let apply = |account: &str, limit| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::ProjectPendingFills {
                account: account.into(),
                limit,
            },
        )
    };
    assert!(apply("missing", 1000).is_err());
    assert!(matches!(
        apply("fifo-A", 0).unwrap(),
        UnifiedFinanceWriteReceipt::FillsProjected { count: 0 }
    ));
    // A later over-close must roll back earlier lots, PnL and pending ACKs.
    tx.execute_batch("SAVEPOINT bad_close").unwrap();
    AccountStore::ingest_unified_source_page_sql(
        &tx,
        "installation",
        "fifo-A",
        &IngestPage {
            fills: vec![Fill {
                fill_id: "over-close".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                trade_id: "trade-over-close".into(),
                exchange_order_id: "order-close".into(),
                intent_id: Some("v02-close".into()),
                time: 6,
                qty: Decimal::from(5),
                price: Decimal::from(120),
                fee: Decimal::ZERO,
                currency: "USDT".into(),
                side: "sell".into(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(apply("fifo-A", 1000).is_err());
    let lots: i64 = tx
        .query_row("SELECT COUNT(*) FROM position_lots", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lots, 0);
    let pnl_rows: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM finance_entries WHERE component='REALIZED_PNL'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pnl_rows, 0);
    let pending: i64 = tx
        .query_row("SELECT COUNT(*) FROM fill_projection_pending", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(pending, 7);
    tx.execute_batch("ROLLBACK TO bad_close; RELEASE bad_close")
        .unwrap();
    assert!(matches!(
        apply("fifo-A", 1000).unwrap(),
        UnifiedFinanceWriteReceipt::FillsProjected { count: 3 }
    ));
    let lots =
        AccountStore::active_lots_sql(&tx, Some(("installation", "fifo-A")), "BTC-USDT-SWAP")
            .unwrap();
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].lot_id, "lot:open-2");
    assert_eq!(lots[0].remaining_qty, Decimal::ONE);
    assert!(
        AccountStore::active_lots_sql(&tx, Some(("installation", "fifo-B")), "BTC-USDT-SWAP")
            .unwrap()
            .is_empty()
    );
    // Independent arithmetic: 1*(120-100) + 1*(120-110) = 30, despite raw close already existing.
    let pnl:String=tx.query_row("SELECT amount FROM finance_entries WHERE account_key='fifo-A' AND component='REALIZED_PNL'",[],|r|r.get(0)).unwrap();
    assert_eq!(decimal(pnl).unwrap(), Decimal::from(30));
    assert!(matches!(
        apply("fifo-A", 1000).unwrap(),
        UnifiedFinanceWriteReceipt::FillsProjected { count: 0 }
    ));
    assert!(matches!(
        apply("fifo-B", 1).unwrap(),
        UnifiedFinanceWriteReceipt::FillsProjected { count: 1 }
    ));
    assert!(matches!(
        apply("fifo-B", 1000).unwrap(),
        UnifiedFinanceWriteReceipt::FillsProjected { count: 2 }
    ));
    tx.commit().unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    let count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM finance_entries WHERE component='REALIZED_PNL' AND ((account_key='fifo-A' AND amount='30') OR (account_key='fifo-B' AND amount='60'))",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
    let pending: i64 = db
        .query_row("SELECT COUNT(*) FROM fill_projection_pending", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(pending, 0);
    // Legacy production entry also shares the exact builder and handles pending raw fills.
    let mut legacy = store();
    seed(&mut legacy, Decimal::ONE);
    assert_eq!(legacy.project_pending_fills(1000).unwrap(), 3);
    assert_eq!(
        legacy
            .finance_totals(1, 10, None)
            .unwrap()
            .get(&("USDT".into(), "REALIZED_PNL".into())),
        Some(&Decimal::from(30))
    );
    assert_eq!(legacy.project_pending_fills(1000).unwrap(), 0);
}
