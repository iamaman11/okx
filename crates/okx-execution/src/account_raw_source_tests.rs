//! Original raw-source contracts run against the full imported account schema.
use super::*;
pub(super) fn assert_contract(target: &mut Connection, a_key: String, b_key: String) {
    use crate::account_worker::{
        UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
        apply_original_unified_finance_transaction as dispatch,
    };
    let tx = target.transaction().unwrap();
    let raw = Fill {
        fill_id: "same-raw-fill".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        trade_id: "same-raw-trade".into(),
        exchange_order_id: "same-raw-order".into(),
        intent_id: None,
        time: 6,
        qty: Decimal::ONE,
        price: Decimal::from(5),
        fee: Decimal::ZERO,
        currency: "USDT".into(),
        side: "buy".into(),
    };
    for account in [&a_key, &b_key] {
        for _ in 0..2 {
            assert!(matches!(
                dispatch(
                    &tx,
                    "installation",
                    UnifiedFinanceWrite::RecordSourceFills {
                        account: account.clone(),
                        fills: vec![raw.clone()]
                    }
                )
                .unwrap(),
                UnifiedFinanceWriteReceipt::SourceFillsRecorded
            ));
        }
    }
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM fills WHERE fill_id='same-raw-fill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM fill_projection_pending WHERE fill_id='same-raw-fill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    let mut conflict = raw.clone();
    conflict.fee = Decimal::ONE;
    assert!(matches!(
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::RecordSourceFills {
                account: a_key.clone(),
                fills: vec![conflict]
            }
        ),
        Err(StoreError::Invalid("IMMUTABLE_FILL_CONFLICT"))
    ));
    let mut first = raw.clone();
    first.fill_id = "rollback-fill".into();
    first.trade_id = "rollback-trade".into();
    let mut invalid = raw.clone();
    invalid.qty = Decimal::ZERO;
    assert!(matches!(
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::RecordSourceFills {
                account: a_key.clone(),
                fills: vec![first, invalid]
            }
        ),
        Err(StoreError::Invalid("INVALID_FILL"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM fills WHERE fill_id='rollback-fill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        AccountStore::source_generation_base_sql(&tx, Some(("installation", &a_key))).unwrap(),
        5,
        "raw source storage must not advance coverage"
    );
    assert!(matches!(
        dispatch(
            &tx,
            "foreign-installation",
            UnifiedFinanceWrite::RecordSourceFills {
                account: a_key.clone(),
                fills: vec![raw]
            }
        ),
        Err(StoreError::Invalid("UNIFIED_SOURCE_ACCOUNT_NOT_FOUND"))
    ));
    let bill = Bill {
        bill_id: "same-raw-bill".into(),
        kind: "FEE".into(),
        time: 7,
        amount: -Decimal::ONE,
        currency: "USDT".into(),
        economic_event_key: "same-bill-event".into(),
    };
    let link = BillOrderSource {
        bill_id: bill.bill_id.clone(),
        instrument_id: "BTC-USDT-SWAP".into(),
        exchange_order_id: "same-raw-order".into(),
        trade_id: "same-raw-trade".into(),
    };
    let write_bills = |account: &str, bills, order_sources| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::RecordSourceBills {
                account: account.into(),
                bills,
                order_sources,
            },
        )
    };
    for account in [&a_key, &b_key] {
        for _ in 0..2 {
            write_bills(account, vec![bill.clone()], vec![link.clone()]).unwrap();
        }
    }
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bills WHERE bill_id='same-raw-bill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bill_order_sources WHERE bill_id='same-raw-bill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    for field in 0..5 {
        let mut changed = bill.clone();
        match field {
            0 => changed.time += 1,
            1 => changed.amount -= Decimal::ONE,
            2 => changed.currency = "USD".into(),
            3 => changed.kind = "FUNDING".into(),
            _ => changed.economic_event_key = "different-event".into(),
        }
        assert!(matches!(
            write_bills(&a_key, vec![changed], vec![]),
            Err(StoreError::Invalid("IMMUTABLE_BILL_CONFLICT"))
        ));
    }
    let mut altered_link = link.clone();
    altered_link.trade_id = "other-trade".into();
    assert!(matches!(
        write_bills(&a_key, vec![], vec![altered_link]),
        Err(StoreError::Invalid("IMMUTABLE_BILL_ORDER_SOURCE_CONFLICT"))
    ));
    let mut orphan = link.clone();
    orphan.bill_id = "missing-bill".into();
    assert!(matches!(
        write_bills(&a_key, vec![], vec![orphan]),
        Err(StoreError::Invalid("BILL_ORDER_SOURCE_WITHOUT_BILL"))
    ));
    let mut first = bill.clone();
    first.bill_id = "rollback-bill".into();
    let mut invalid_link = link.clone();
    invalid_link.exchange_order_id = "0".into();
    assert!(matches!(
        write_bills(&a_key, vec![first], vec![invalid_link]),
        Err(StoreError::Invalid("BILL_ORDER_SOURCE_INVALID"))
    ));
    assert_eq!(
        tx.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM bills WHERE bill_id='rollback-bill'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    // A bill present only on A cannot authorize a B link.
    let mut only_a = bill.clone();
    only_a.bill_id = "only-A-bill".into();
    write_bills(&a_key, vec![only_a.clone()], vec![]).unwrap();
    let mut foreign_link = link.clone();
    foreign_link.bill_id = only_a.bill_id;
    assert!(matches!(
        write_bills(&b_key, vec![], vec![foreign_link]),
        Err(StoreError::Invalid("BILL_ORDER_SOURCE_WITHOUT_BILL"))
    ));
    assert!(matches!(
        write_bills("unknown-account", vec![], vec![]),
        Err(StoreError::Invalid("UNIFIED_SOURCE_ACCOUNT_NOT_FOUND"))
    ));
    assert_eq!(
        AccountStore::source_generation_base_sql(&tx, Some(("installation", &a_key))).unwrap(),
        5
    );
    // The real stopped import cannot be activated by a source-storage witness.
    tx.rollback().unwrap();
}
