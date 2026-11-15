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
    let mut unsupported = page.clone();
    unsupported.projected_fill_ids.push("not-ported".into());
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
