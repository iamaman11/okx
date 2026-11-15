//! NAV arithmetic and the complete account-scoped source-to-COMMIT boundary.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
fn snapshot() -> AccountNavSnapshot {
    AccountNavSnapshot {
        as_of: 10,
        generation: 2,
        source_version: "2:1".into(),
        source_ref: "fixture-snapshot".into(),
        equity_usd: Decimal::from(117),
        balances: vec![("USDT".into(), Decimal::from(117))],
        complete: true,
    }
}
#[test]
fn account_nav_arithmetic_has_no_io_and_does_not_treat_deposits_as_profit() {
    let flows = std::collections::BTreeMap::from([
        (("USDT".into(), "OPENING".into()), Decimal::from(100)),
        (("USDT".into(), "DEPOSIT".into()), Decimal::from(10)),
    ]);
    let input = snapshot();
    let page =
        build_account_nav_page(&input, &flows, Decimal::from(100), Decimal::from(10)).unwrap();
    assert_eq!(page.nav.len(), 2);
    for point in &page.nav {
        assert_eq!(point.pnl, Decimal::from(7));
        assert_eq!(point.flows, Decimal::from(10));
    }
    let mut duplicate = input.clone();
    duplicate.balances.push(duplicate.balances[0].clone());
    assert!(matches!(
        build_account_nav_page(&duplicate, &flows, Decimal::from(100), Decimal::from(10)),
        Err(StoreError::Invalid("ACCOUNT_NAV_CURRENCY_INVALID"))
    ));
    let mut overflow = input.clone();
    overflow.equity_usd = Decimal::MIN;
    assert!(matches!(
        build_account_nav_page(&overflow, &flows, Decimal::ONE, Decimal::ONE),
        Err(StoreError::Invalid("MONEY_OVERFLOW"))
    ));
    let mut partial = input;
    partial.complete = false;
    let page =
        build_account_nav_page(&partial, &flows, Decimal::from(100), Decimal::from(10)).unwrap();
    assert!(page.nav.iter().all(|p| p.coverage == "PARTIAL"));
    assert_eq!(
        page.cursor.unwrap().coverage_gaps,
        "FINANCIAL_BACKFILL_PENDING"
    );
}
#[test]
fn risk_book_contract_account_nav_uses_scoped_flows_and_durable_coverage() {
    let mut a = store_with_account("nav-A");
    let mut b = store_with_account("nav-B");
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "fixture-fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    for (store, amount) in [(&mut a, 10), (&mut b, 20)] {
        store
            .ingest_page_with_usd(
                &IngestPage {
                    cash_flows: vec![CashFlow {
                        event_id: "same-deposit".into(),
                        kind: "DEPOSIT".into(),
                        currency: "USDT".into(),
                        amount: Decimal::from(amount),
                        time: 5,
                        source_ref: "fixture-bill".into(),
                    }],
                    ..Default::default()
                },
                &fx,
            )
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
    let apply = |account: &str, snapshot| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::ProjectAccountNav {
                account: account.into(),
                snapshot,
            },
        )
    };
    assert!(apply("missing", snapshot()).is_err());
    assert!(matches!(
        apply("nav-A", snapshot()),
        Err(StoreError::Invalid("ACCOUNT_NAV_SOURCE_HISTORY_INCOMPLETE"))
    ));
    let mut partial = snapshot();
    partial.complete = false;
    assert!(matches!(
        apply("nav-A", partial).unwrap(),
        UnifiedFinanceWriteReceipt::SourcePageIngested
    ));
    let cursor = AccountStore::scoped_source_cursor_sql(
        &tx,
        "installation",
        "nav-A",
        "account_snapshot",
        "account",
    )
    .unwrap()
    .unwrap();
    assert_eq!(cursor.coverage_gaps, "FINANCIAL_BACKFILL_PENDING");
    for (source, partition) in [
        ("fills", "SPOT"),
        ("fills", "MARGIN"),
        ("fills", "SWAP"),
        ("fills", "FUTURES"),
        ("fills", "OPTION"),
        ("bills", "account"),
    ] {
        AccountStore::upsert_source_cursor_sql(
            &tx,
            Some(("installation", "nav-A")),
            &SourceCursor {
                source: source.into(),
                stream_partition: partition.into(),
                cursor: String::new(),
                watermark: 20,
                generation: 2,
                coverage_gaps: String::new(),
            },
        )
        .unwrap();
    }
    assert!(
        matches!(
            apply("nav-B", snapshot()),
            Err(StoreError::Invalid("ACCOUNT_NAV_SOURCE_HISTORY_INCOMPLETE"))
        ),
        "A coverage cannot authorize B"
    );
    let mut full = snapshot();
    full.as_of = 20;
    full.source_version = "2:2".into();
    let mut wrong = full.clone();
    wrong.generation = 3;
    assert!(matches!(
        apply("nav-A", wrong),
        Err(StoreError::Invalid("ACCOUNT_NAV_SOURCE_HISTORY_INCOMPLETE"))
    ));
    apply("nav-A", full.clone()).unwrap();
    apply("nav-A", full.clone()).unwrap();
    let mut conflict = full;
    conflict.equity_usd = Decimal::from(999);
    assert!(apply("nav-A", conflict).is_err());
    let mut other = snapshot();
    other.complete = false;
    other.equity_usd = Decimal::from(141);
    other.balances[0].1 = Decimal::from(141);
    apply("nav-B", other).unwrap();
    tx.commit().unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    for (account, expected, coverage) in [("nav-A", 7, "COMPLETE"), ("nav-B", 21, "PARTIAL")] {
        for scope in ["account", "account:USDT"] {
            let (pnl,actual):(String,String)=db.query_row("SELECT pnl,coverage FROM nav_points WHERE account_key=?1 AND scope_id=?2 AND source_version<>'opening' ORDER BY time DESC LIMIT 1",params![account,scope],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
            assert_eq!(decimal(pnl).unwrap(), Decimal::from(expected));
            assert_eq!(actual, coverage);
        }
    }
    let cursor = AccountStore::scoped_source_cursor_sql(
        &db,
        "installation",
        "nav-A",
        "account_snapshot",
        "account",
    )
    .unwrap()
    .unwrap();
    assert!(cursor.coverage_gaps.is_empty());
    assert_eq!(cursor.watermark, 20);
}
