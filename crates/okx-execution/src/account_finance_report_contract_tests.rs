//! The portfolio production read owns its SQL; foreign receipts cannot prove NAV.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, apply_original_unified_finance_transaction as dispatch,
};
fn summary() -> FinanceReportQuery {
    FinanceReportQuery {
        from: 1,
        through: 5,
        instances: vec!["alpha".into()],
        currencies: vec!["USD".into()],
        view: FinanceReportView::Summary,
    }
}
#[test]
fn risk_book_contract_portfolio_snapshot_scoped_nav_receipts_history_and_revision() {
    let mut a = store_with_account("report-A");
    let mut b = store_with_account("report-B");
    strategy_nav_contracts::seed(&mut a, 20, 100);
    strategy_nav_contracts::seed(&mut b, 40, 90);
    a.commit_reconciled_strategy_nav(&strategy_nav_contracts::evidence("report-A"))
        .unwrap();
    let original = a.finance_report(&summary()).unwrap();
    assert_eq!(original.nav[0].pnl, Some(Decimal::new(1, 1)));
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
    let report = |account: &str, q: &FinanceReportQuery| {
        AccountStore::finance_report_sql(&tx, Some(("installation", account)), q)
    };
    assert!(report("missing", &summary()).is_err());
    let mut backwards = summary();
    backwards.from = 6;
    assert!(report("report-A", &backwards).is_err());
    let apply = |account: &str| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::CommitStrategyNav {
                account: account.into(),
                evidence: strategy_nav_contracts::evidence(account),
                cursor: strategy_nav_contracts::cursor(),
            },
        )
    };
    apply("report-A").unwrap();
    let actual = report("report-A", &summary()).unwrap();
    assert_eq!(
        serde_json::to_value(&actual.nav).unwrap(),
        serde_json::to_value(&original.nav).unwrap()
    );
    assert_eq!(actual.attribution, original.attribution);
    assert_eq!(actual.initialized_at, 1);
    // A forged B NAV point plus an exact matching receipt belonging ONLY to A
    // must not be accepted by either B's capital-known or endpoint JOIN.
    tx.execute_batch("SAVEPOINT foreign_receipt").unwrap();
    let revision =
        AccountStore::strategy_valuation_revision_sql(&tx, Some(("installation", "report-B")), 5)
            .unwrap();
    let source_version = format!("strategy-reconciled-v1:revision:{revision}");
    let op = format!("strategy-nav:strategy:alpha:5:{source_version}");
    tx.execute("INSERT INTO nav_points VALUES('installation','report-B','strategy:alpha',5,?1,'40.2','0.2','40','USD','COMPLETE','account-witness')",[&source_version]).unwrap();
    tx.execute("INSERT INTO operation_receipts VALUES('installation','report-A',?1,'STRATEGY_NAV_RECONCILED',?2,'account-witness',5)",params![op,format!("revision:{revision}")]).unwrap();
    let unproven = report("report-B", &summary()).unwrap();
    assert_eq!(unproven.nav[0].current_equity, None);
    assert_eq!(unproven.nav[0].coverage, "PARTIAL");
    tx.execute_batch("ROLLBACK TO foreign_receipt; RELEASE foreign_receipt")
        .unwrap();
    apply("report-B").unwrap();
    let b = report("report-B", &summary()).unwrap();
    assert_eq!(b.nav[0].pnl, Some(Decimal::new(2, 1)));
    assert_eq!(b.nav[0].current_equity, Some(Decimal::new(402, 1)));
    for account in ["report-A", "report-B"] {
        let mut q = summary();
        q.through = 9;
        q.view = FinanceReportView::History {
            after_time: 0,
            after_id: String::new(),
            limit: 1,
        };
        let first = report(account, &q).unwrap();
        assert_eq!(first.entries.len(), 1);
        let entry = &first.entries[0];
        assert_eq!(
            entry.amount,
            Decimal::from(if account == "report-A" { 20 } else { 40 })
        );
        q.view = FinanceReportView::History {
            after_time: entry.time,
            after_id: entry.entry_id.clone(),
            limit: 1,
        };
        let next = report(account, &q).unwrap();
        assert_eq!(next.entries.len(), 1);
        assert_eq!(next.entries[0].amount, Decimal::from(5));
        assert_eq!(next.revision, first.revision);
        q.view = FinanceReportView::History {
            after_time: next.entries[0].time,
            after_id: next.entries[0].entry_id.clone(),
            limit: 1,
        };
        assert!(report(account, &q).unwrap().entries.is_empty());
    }
    let a_revision = actual.revision;
    AccountStore::ingest_unified_source_page_sql(
        &tx,
        "installation",
        "report-B",
        &IngestPage {
            nav: vec![NavPoint {
                scope_id: "account".into(),
                time: 6,
                source_version: "next".into(),
                equity: Decimal::from(102),
                pnl: Decimal::from(2),
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "new-account-witness".into(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let mut newer = summary();
    newer.through = 6;
    assert_eq!(
        report("report-B", &newer).unwrap().nav[0].coverage,
        "PARTIAL"
    );
    assert_eq!(
        report("report-A", &newer).unwrap().nav[0].coverage,
        "COMPLETE"
    );
    assert_eq!(report("report-A", &newer).unwrap().revision, a_revision);
    tx.commit().unwrap();
    drop(db);
    let mut db = Connection::open(file).unwrap();
    let read = db
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    assert_eq!(
        AccountStore::finance_report_sql(&read, Some(("installation", "report-A")), &summary())
            .unwrap()
            .nav[0]
            .pnl,
        Some(Decimal::new(1, 1))
    );
    read.commit().unwrap();
}
