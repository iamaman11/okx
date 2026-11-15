//! Direct page contract through the production command/savepoint dispatcher.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
pub(super) fn assert_contract(tx: &rusqlite::Transaction<'_>, a: &str, b: &str) {
    tx.execute_batch("SAVEPOINT page_contract").unwrap();
    let page = IngestPage {
        fills: vec![Fill {
            fill_id: "page-fill".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            trade_id: "page-trade".into(),
            exchange_order_id: "page-order".into(),
            intent_id: None,
            time: 8,
            qty: Decimal::ONE,
            price: Decimal::from(5),
            fee: -Decimal::ONE,
            currency: "USDT".into(),
            side: "buy".into(),
        }],
        swap_fill_sources: vec![SwapFillSource {
            fill_id: "page-fill".into(),
            bill_id: String::new(),
            sub_type: "3".into(),
            position_side: "long".into(),
            direction: "OPEN".into(),
        }],
        bill_order_sources: vec![BillOrderSource {
            bill_id: "page-bill".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            exchange_order_id: "page-order".into(),
            trade_id: "page-trade".into(),
        }],
        bills: vec![Bill {
            bill_id: "page-bill".into(),
            kind: "FEE".into(),
            time: 8,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:page-bill".into(),
        }],
        finance: vec![FinanceEntry {
            entry_id: "page-fee".into(),
            economic_event_key: "bill:page-bill".into(),
            component: "FEE".into(),
            allocation_leg: "account".into(),
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            instance_id: Some("manual".into()),
            source_ref: "bill:page-bill".into(),
            time: 8,
            reverses_entry_id: None,
        }],
        cursor: Some(SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: "page-complete".into(),
            watermark: 40,
            generation: 12,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    let apply = |account: &str, page: IngestPage| {
        dispatch(
            tx,
            "installation",
            UnifiedFinanceWrite::IngestSourcePage {
                account: account.into(),
                page,
            },
        )
    };
    for account in [a, b] {
        for _ in 0..2 {
            assert!(matches!(
                apply(account, page.clone()).unwrap(),
                UnifiedFinanceWriteReceipt::SourcePageIngested
            ));
        }
        assert_eq!(tx.query_row::<String,_,_>("SELECT amount FROM finance_totals WHERE installation_id='installation' AND account_key=?1 AND currency='USDT' AND component='FEE'",[account],|r|r.get(0)).unwrap(),"-1");
        assert_eq!(
            AccountStore::scoped_source_cursor_sql(tx, "installation", account, "fills", "SWAP")
                .unwrap()
                .unwrap()
                .cursor,
            "page-complete"
        );
    }
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM fills WHERE fill_id='page-fill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    let mut failing = page.clone();
    failing.fills[0].fill_id = "rolled-back-fill".into();
    failing.fills[0].trade_id = "new-trade".into();
    failing.finance[0].entry_id = "rolled-back-fee".into();
    failing.finance[0].economic_event_key = "new-event".into();
    failing.cursor.as_mut().unwrap().watermark = 39;
    assert!(matches!(
        apply(a, failing),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM fills WHERE fill_id='rolled-back-fill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM finance_entries WHERE entry_id='rolled-back-fee'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(tx.query_row::<String,_,_>("SELECT amount FROM finance_totals WHERE installation_id='installation' AND account_key=?1 AND currency='USDT' AND component='FEE'",[a],|r|r.get(0)).unwrap(),"-1");
    for account in [a, b] {
        assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_source_links WHERE installation_id='installation' AND account_key=?1 AND entry_id='page-fee' AND source_ref='fill:page-fill'",[account],|r|r.get(0)).unwrap(),1);
    }
    let mut conflict = page.clone();
    conflict.swap_fill_sources[0].bill_id = "different-bill".into();
    assert!(matches!(
        apply(a, conflict),
        Err(StoreError::Invalid("IMMUTABLE_SWAP_FILL_SOURCE_CONFLICT"))
    ));
    let mut late_fill = page.fills[0].clone();
    late_fill.fill_id = "late-fill".into();
    late_fill.exchange_order_id = "late-order".into();
    late_fill.trade_id = "late-trade".into();
    let mut label = page.swap_fill_sources[0].clone();
    label.fill_id = late_fill.fill_id.clone();
    let fill_page = IngestPage {
        fills: vec![late_fill],
        swap_fill_sources: vec![label],
        ..Default::default()
    };
    for account in [a, b] {
        apply(account, fill_page.clone()).unwrap();
    }
    let mut late_bill = page.bills[0].clone();
    late_bill.bill_id = "late-bill".into();
    late_bill.economic_event_key = "bill:late-bill".into();
    let mut late_finance = page.finance[0].clone();
    late_finance.entry_id = "late-fee".into();
    late_finance.economic_event_key = late_bill.economic_event_key.clone();
    late_finance.source_ref = late_bill.economic_event_key.clone();
    let mut origin = page.bill_order_sources[0].clone();
    origin.bill_id = late_bill.bill_id.clone();
    origin.exchange_order_id = "late-order".into();
    origin.trade_id = "late-trade".into();
    let late_page = IngestPage {
        bills: vec![late_bill],
        finance: vec![late_finance],
        bill_order_sources: vec![origin],
        ..Default::default()
    };
    for _ in 0..2 {
        apply(a, late_page.clone()).unwrap();
    }
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_source_links WHERE installation_id='installation' AND account_key=?1 AND source_ref='fill:late-fill'",[a],|r|r.get(0)).unwrap(),1);
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_source_links WHERE installation_id='installation' AND account_key=?1 AND source_ref='fill:late-fill'",[b],|r|r.get(0)).unwrap(),0,"A bill cannot bind B fill");
    let mut mismatched = late_page;
    mismatched.bills[0].amount = Decimal::from(-2);
    mismatched.finance[0].amount = Decimal::from(-2);
    apply(b, mismatched).unwrap();
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM finance_source_links WHERE installation_id='installation' AND account_key=?1 AND source_ref='fill:late-fill'",[b],|r|r.get(0)).unwrap(),0,"different fee amount cannot prove a link");
    let mut invalid = fill_page;
    invalid.fills[0].fill_id = "invalid-swap-fill".into();
    invalid.fills[0].trade_id = "invalid-trade".into();
    invalid.swap_fill_sources[0].fill_id = "invalid-swap-fill".into();
    invalid.swap_fill_sources[0].direction = "CLOSE".into();
    assert!(matches!(
        apply(a, invalid),
        Err(StoreError::Invalid("SWAP_FILL_SOURCE_INVALID"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM fills WHERE fill_id='invalid-swap-fill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let history = IngestPage {
        bills: vec![Bill {
            bill_id: "source-funding".into(),
            kind: "FUNDING".into(),
            time: 10,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:source-funding".into(),
        }],
        cash_flows: vec![CashFlow {
            event_id: "source-deposit".into(),
            kind: "DEPOSIT".into(),
            currency: "USDT".into(),
            amount: Decimal::from(7),
            time: 10,
            source_ref: "bill:deposit".into(),
        }],
        allocations: vec![Allocation {
            event_id: "source-allocation".into(),
            instance_id: "manual".into(),
            delta: Decimal::from(2),
            currency: "USDT".into(),
            time: 10,
        }],
        funding_events: vec![FundingEvent {
            event_id: "source-funding".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            time: 10,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
        }],
        cursor: Some(SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: "history-complete".into(),
            watermark: 50,
            generation: 12,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    for account in [a, b] {
        for _ in 0..2 {
            apply(account, history.clone()).unwrap();
        }
    }
    for table in ["cash_flows", "allocations", "funding_projection_pending"] {
        let event = if table == "cash_flows" {
            "source-deposit"
        } else if table == "allocations" {
            "source-allocation"
        } else {
            "source-funding"
        };
        assert_eq!(
            tx.query_row::<i64, _, _>(
                &format!("SELECT COUNT(*) FROM {table} WHERE event_id=?1"),
                [event],
                |r| r.get(0)
            )
            .unwrap(),
            2
        );
    }
    let mut missing_bill = history.clone();
    missing_bill.bills[0].bill_id = "rolled-back-funding-bill".into();
    missing_bill.funding_events[0].event_id = "missing-original-funding".into();
    assert!(
        matches!(
            apply(a, missing_bill),
            Err(StoreError::Invalid("FUNDING_SOURCE_BILL_MISSING"))
        ),
        "funding without its original bill must fail"
    );
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bills WHERE bill_id='rolled-back-funding-bill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let mut flow_conflict = history.clone();
    flow_conflict.cash_flows[0].amount = Decimal::from(8);
    assert!(matches!(
        apply(a, flow_conflict),
        Err(StoreError::Invalid("IMMUTABLE_CASH_FLOW_CONFLICT"))
    ));
    let mut allocation_conflict = history.clone();
    allocation_conflict.cash_flows[0].event_id = "rolled-back-flow".into();
    allocation_conflict.allocations[0].delta = Decimal::from(3);
    assert!(matches!(
        apply(a, allocation_conflict),
        Err(StoreError::Invalid("IMMUTABLE_ALLOCATION_CONFLICT"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM cash_flows WHERE event_id='rolled-back-flow'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let mut stale = history.clone();
    stale.cash_flows[0].event_id = "stale-flow".into();
    stale.cursor.as_mut().unwrap().watermark = 49;
    assert!(matches!(
        apply(a, stale),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM cash_flows WHERE event_id='stale-flow'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    for _ in 0..2 {
        apply(
            a,
            IngestPage {
                attributed_funding_ids: vec!["source-funding".into()],
                ..Default::default()
            },
        )
        .unwrap();
    }
    apply(a, history).unwrap();
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM funding_projection_pending WHERE installation_id='installation' AND account_key=?1 AND event_id='source-funding'",[a],|r|r.get(0)).unwrap(),0,"attributed funding must not be re-enqueued on replay");
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM funding_projection_pending WHERE installation_id='installation' AND account_key=?1 AND event_id='source-funding'",[b],|r|r.get(0)).unwrap(),1,"A acknowledgement cannot consume B pending funding");
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM operation_receipts WHERE installation_id='installation' AND account_key=?1 AND operation_id='funding:source-funding'",[a],|r|r.get(0)).unwrap(),1);
    let lot = StoredLot {
        lot_id: "projection-lot".into(),
        instance_id: None,
        intent_id: None,
        fill_id: Some("page-fill".into()),
        instrument_id: "BTC-USDT-SWAP".into(),
        side: "long".into(),
        remaining_qty: Decimal::ONE,
        cost: Decimal::from(5),
        opened_at: 8,
    };
    let nav = NavPoint {
        scope_id: "account".into(),
        time: 10,
        source_version: "projection-v1".into(),
        equity: Decimal::from(120),
        pnl: Decimal::ZERO,
        flows: Decimal::from(10),
        currency: "USD".into(),
        coverage: "COMPLETE".into(),
        mark_source: "signed-fixture-mark".into(),
    };
    let projection = IngestPage {
        lots: vec![lot],
        nav: vec![nav],
        ..Default::default()
    };
    for account in [a, b] {
        for _ in 0..2 {
            apply(account, projection.clone()).unwrap();
        }
    }
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM position_lots WHERE lot_id='projection-lot'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    for account in [a, b] {
        assert_eq!(tx.query_row::<String,_,_>("SELECT highwater FROM nav_watermarks WHERE installation_id='installation' AND account_key=?1 AND scope_id='account'",[account],|r|r.get(0)).unwrap(),"110");
    }
    let mut higher = projection.clone();
    higher.lots.clear();
    higher.nav[0].equity = Decimal::from(150);
    higher.nav[0].time = 11;
    higher.nav[0].source_version = "projection-v2".into();
    apply(a, higher).unwrap();
    assert_eq!(tx.query_row::<String,_,_>("SELECT highwater FROM nav_watermarks WHERE installation_id='installation' AND account_key=?1 AND scope_id='account'",[a],|r|r.get(0)).unwrap(),"140");
    assert_eq!(tx.query_row::<String,_,_>("SELECT highwater FROM nav_watermarks WHERE installation_id='installation' AND account_key=?1 AND scope_id='account'",[b],|r|r.get(0)).unwrap(),"110");
    let mut conflicting_nav = projection.clone();
    conflicting_nav.lots.clear();
    conflicting_nav.nav[0].equity = Decimal::from(123);
    conflicting_nav.cash_flows.push(CashFlow {
        event_id: "nav-rollback-flow".into(),
        kind: "DEPOSIT".into(),
        amount: Decimal::ONE,
        currency: "USD".into(),
        time: 12,
        source_ref: "signed-flow".into(),
    });
    assert!(matches!(
        apply(a, conflicting_nav),
        Err(StoreError::Invalid("IMMUTABLE_NAV_SOURCE_CONFLICT"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM cash_flows WHERE event_id='nav-rollback-flow'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    let mut wrong_owner = projection.clone();
    wrong_owner.nav.clear();
    wrong_owner.lots[0].side = "short".into();
    assert!(matches!(
        apply(a, wrong_owner),
        Err(StoreError::Invalid("LOT_OWNER_MISMATCH"))
    ));
    let mut invalid_lot = projection.clone();
    invalid_lot.nav.clear();
    invalid_lot.lots[0].remaining_qty = -Decimal::ONE;
    assert!(matches!(
        apply(a, invalid_lot),
        Err(StoreError::Invalid("INVALID_POSITION_LOT"))
    ));
    let mut attributed_external = projection.clone();
    attributed_external.nav.clear();
    attributed_external.lots[0].instance_id = Some("manual".into());
    assert!(matches!(
        apply(a, attributed_external),
        Err(StoreError::Invalid("EXTERNAL_FILL_MUST_BE_UNATTRIBUTED"))
    ));
    let mut unmarked = projection;
    unmarked.lots.clear();
    unmarked.nav[0].source_version = "no-mark".into();
    unmarked.nav[0].mark_source.clear();
    assert!(matches!(
        apply(a, unmarked),
        Err(StoreError::Invalid("NAV_MARK_SOURCE_REQUIRED"))
    ));
    for _ in 0..2 {
        apply(
            a,
            IngestPage {
                projected_fill_ids: vec!["page-fill".into()],
                ..Default::default()
            },
        )
        .unwrap();
    }
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM fill_projection_pending WHERE installation_id='installation' AND account_key=?1 AND fill_id='page-fill'",[a],|r|r.get(0)).unwrap(),0);
    assert_eq!(tx.query_row::<i64,_,_>("SELECT COUNT(*) FROM fill_projection_pending WHERE installation_id='installation' AND account_key=?1 AND fill_id='page-fill'",[b],|r|r.get(0)).unwrap(),1);
    let mut unsupported = page.clone();
    unsupported.entry_order_fee_links.push(EntryOrderFeeLink {
        bill_id: "page-bill".into(),
        intent_id: "not-ported".into(),
        exchange_order_id: "page-order".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        bill_at: 8,
        fee_usdt: -Decimal::ONE,
    });
    assert!(matches!(
        apply(a, unsupported),
        Err(StoreError::Invalid(
            "UNIFIED_SOURCE_PAGE_COMPONENT_NOT_PORTED"
        ))
    ));
    let mut forged = page;
    forged.cursor.as_mut().unwrap().source = "finance_provenance".into();
    assert!(matches!(
        apply(a, forged),
        Err(StoreError::Invalid("FINANCE_PROVENANCE_CURSOR_INTERNAL"))
    ));
    tx.execute_batch("ROLLBACK TO page_contract; RELEASE page_contract")
        .unwrap();
}
