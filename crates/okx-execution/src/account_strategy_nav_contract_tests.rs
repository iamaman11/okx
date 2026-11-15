//! Original valuation and capital contracts through one scoped SQL transaction.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
pub(super) fn seed(
    s: &mut AccountStore,
    principal: i64,
    entry: i64,
) -> StrategyValuationReconciliation {
    owned_fill(s, "alpha", "same-fill", entry, "long", 3);
    let initial = AuthorizedCapitalAssignment {
        account_key: s.meta.account_key.clone(),
        installation_id: s.meta.installation_id.clone(),
        nav_source_ref: s.meta.opening_source.clone(),
        operation_id: "same-grant".into(),
        request_hash: "fixture-grant".into(),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(principal),
        authority_revision: 1,
        authority_receipt: "fixture-governance".into(),
        time: 4,
    };
    s.commit_authorized_capital_assignment(&initial).unwrap();
    p02_completed_cursors(s, 5);
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 5,
            source_version: "verified".into(),
            equity: Decimal::from(101),
            pnl: Decimal::ONE,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "account-witness".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    // Latest totals now include future money. Historical NAV must not include it.
    s.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
        nav_source_ref: "account-witness".into(),
        operation_id: "future-grant".into(),
        request_hash: "future-grant".into(),
        delta_usd: Decimal::from(5),
        time: 8,
        ..initial
    })
    .unwrap();
    s.reconcile_strategy_valuation(&evidence(&s.meta.account_key))
        .unwrap()
}
fn evidence(account: &str) -> StrategyValuationEvidence {
    StrategyValuationEvidence {
        account_key: account.into(),
        account_nav_source: "account-witness".into(),
        as_of: 5,
        conversion: UsdConversionEvidence {
            observed_at: 5,
            source_digest: "fixture-fx".into(),
            usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
        },
        venue_positions: vec![ValuationPosition {
            instrument_id: "BTC".into(),
            signed_contracts: Decimal::ONE,
            mark_price: Decimal::from(110),
        }],
    }
}
fn cursor() -> SourceCursor {
    SourceCursor {
        source: "strategy_valuation".into(),
        stream_partition: "account".into(),
        cursor: String::new(),
        watermark: 5,
        generation: 1,
        coverage_gaps: String::new(),
    }
}
#[test]
fn risk_book_contract_scoped_strategy_nav_historical_capital_and_atomic_witnesses() {
    let mut a = store_with_account("strategy-A");
    let mut b = store_with_account("strategy-B");
    let expected_a = seed(&mut a, 20, 100);
    let expected_b = seed(&mut b, 40, 90);
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
    let apply = |account: &str, evidence, cursor| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::CommitStrategyNav {
                account: account.into(),
                evidence,
                cursor,
            },
        )
    };
    let a_scope = Some(("installation", "strategy-A"));
    let latest = AccountStore::latest_capital_assignment_time_sql(&tx, a_scope).unwrap();
    assert_eq!(latest, Some(8));
    let filtered = AccountStore::finance_attribution_filtered_sql(
        &tx,
        a_scope,
        1,
        8,
        &["alpha".into()],
        &["USD".into()],
    )
    .unwrap();
    assert_eq!(
        filtered.get(&(
            Some("alpha".into()),
            "USD".into(),
            "INTERNAL_CAPITAL_USD".into()
        )),
        Some(&Decimal::from(25))
    );
    assert_eq!(
        AccountStore::capital_state_sql(&tx, a_scope, 5, latest)
            .unwrap()
            .assigned_usd,
        Decimal::from(20)
    );
    assert_eq!(
        AccountStore::capital_state_sql(&tx, a_scope, 8, latest)
            .unwrap()
            .assigned_usd,
        Decimal::from(25)
    );
    assert!(matches!(
        AccountStore::capital_state_sql(&tx, a_scope, 5006, latest),
        Err(StoreError::Invalid(
            "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE"
        ))
    ));
    tx.execute_batch("SAVEPOINT recovering").unwrap();
    tx.execute(
        "UPDATE storage_recovery SET requires_reconcile=1 WHERE account_key='strategy-A'",
        [],
    )
    .unwrap();
    assert!(matches!(
        apply("strategy-A", evidence("strategy-A"), cursor()),
        Err(StoreError::Invalid(
            "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE"
        ))
    ));
    apply("strategy-B", evidence("strategy-B"), cursor()).unwrap();
    tx.execute_batch("ROLLBACK TO recovering; RELEASE recovering")
        .unwrap();
    assert!(apply("strategy-A", evidence("strategy-B"), cursor()).is_err());
    let mut no_mark = evidence("strategy-A");
    no_mark.venue_positions.clear();
    assert!(matches!(
        apply("strategy-A", no_mark, cursor()),
        Err(StoreError::Invalid("STRATEGY_NAV_MARK_MISSING"))
    ));
    let mut no_fx = evidence("strategy-A");
    no_fx.conversion.usd_per_currency.clear();
    assert!(matches!(
        apply("strategy-A", no_fx, cursor()),
        Err(StoreError::Invalid("STRATEGY_NAV_FX_MISSING"))
    ));
    let mut wrong_cursor = cursor();
    wrong_cursor.source = "fills".into();
    assert!(matches!(
        apply("strategy-A", evidence("strategy-A"), wrong_cursor),
        Err(StoreError::Invalid("STRATEGY_NAV_CURSOR_INVALID"))
    ));
    // Force failure at cursor publication, AFTER both NAV and receipts were written.
    tx.execute_batch("SAVEPOINT stale_cursor").unwrap();
    let mut newer = cursor();
    newer.generation = 2;
    AccountStore::upsert_source_cursor_sql(&tx, a_scope, &newer).unwrap();
    assert!(matches!(
        apply("strategy-A", evidence("strategy-A"), cursor()),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    let rows: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id='strategy:alpha'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rows, 0);
    let receipts: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM operation_receipts WHERE kind='STRATEGY_NAV_RECONCILED'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 0);
    tx.execute_batch("ROLLBACK TO stale_cursor; RELEASE stale_cursor")
        .unwrap();
    for (account, expected, capital, unrealized) in [
        ("strategy-A", expected_a, 20, Decimal::new(1, 1)),
        ("strategy-B", expected_b, 40, Decimal::new(2, 1)),
    ] {
        for _ in 0..2 {
            let UnifiedFinanceWriteReceipt::StrategyNavCommitted(actual) =
                apply(account, evidence(account), cursor()).unwrap()
            else {
                panic!("wrong receipt")
            };
            assert_eq!(actual, expected);
            assert_eq!(actual.strategies[0].capital_usd, Decimal::from(capital));
            assert_eq!(actual.strategies[0].unrealized_usd, unrealized);
            assert_eq!(
                actual.account_nav_usd,
                actual.attributed_usd + actual.unattributed_usd
            );
        }
    }
    let revision = AccountStore::strategy_valuation_revision_sql(&tx, a_scope, 5).unwrap();
    tx.execute_batch("SAVEPOINT foreign_pending").unwrap();
    AccountStore::ingest_unified_source_page_sql(
        &tx,
        "installation",
        "strategy-B",
        &IngestPage {
            fills: vec![Fill {
                fill_id: "external-pending".into(),
                instrument_id: "ETH".into(),
                trade_id: "external".into(),
                exchange_order_id: "external".into(),
                intent_id: None,
                time: 6,
                qty: Decimal::ONE,
                price: Decimal::ONE,
                fee: Decimal::ZERO,
                currency: "USDT".into(),
                side: "buy".into(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        AccountStore::strategy_valuation_revision_sql(&tx, a_scope, 5).unwrap(),
        revision
    );
    apply("strategy-A", evidence("strategy-A"), cursor()).unwrap();
    assert!(matches!(
        apply("strategy-B", evidence("strategy-B"), cursor()),
        Err(StoreError::Invalid("STRATEGY_NAV_PROJECTION_PENDING"))
    ));
    tx.execute_batch("ROLLBACK TO foreign_pending; RELEASE foreign_pending")
        .unwrap();
    // Future source changes must not invalidate the earlier as-of revision.
    let current = AccountStore::strategy_valuation_revision_sql(&tx, a_scope, i64::MAX).unwrap();
    AccountStore::ingest_unified_source_page_sql(
        &tx,
        "installation",
        "strategy-A",
        &IngestPage {
            bills: vec![Bill {
                bill_id: "future-bill".into(),
                kind: "FEE".into(),
                time: 10,
                amount: Decimal::ZERO,
                currency: "USDT".into(),
                economic_event_key: "bill:future-bill".into(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        AccountStore::strategy_valuation_revision_sql(&tx, a_scope, 5).unwrap(),
        revision
    );
    assert_ne!(
        AccountStore::strategy_valuation_revision_sql(&tx, a_scope, i64::MAX).unwrap(),
        current
    );
    tx.commit().unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    let rows: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id='strategy:alpha'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rows, 4);
    let receipts: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM operation_receipts WHERE kind='STRATEGY_NAV_RECONCILED'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(receipts, 4);
    let highwaters: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM nav_watermarks WHERE scope_id='strategy:alpha'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(highwaters, 0, "valuation cannot grant risk authority");
    for account in ["strategy-A", "strategy-B"] {
        let c = AccountStore::scoped_source_cursor_sql(
            &db,
            "installation",
            account,
            "strategy_valuation",
            "account",
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.watermark, 5);
        assert!(c.coverage_gaps.is_empty());
    }
}
