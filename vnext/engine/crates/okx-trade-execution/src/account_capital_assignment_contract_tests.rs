//! Conserved capital writes on the installation owner, including a late SQL failure.
use super::*;
use crate::account_worker::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction as dispatch,
};
fn request(account: &str, delta: i64) -> AuthorizedCapitalAssignment {
    AuthorizedCapitalAssignment {
        account_key: account.into(),
        installation_id: "installation".into(),
        nav_source_ref: "account-witness".into(),
        operation_id: "same-native-assignment".into(),
        request_hash: "fixture-request".into(),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(delta),
        authority_revision: 1,
        authority_receipt: "fixture-governance".into(),
        time: 9,
    }
}
#[test]
fn risk_book_contract_scoped_capital_two_legs_receipt_and_rollback() {
    let mut a = store_with_account("capital-A");
    let mut b = store_with_account("capital-B");
    strategy_nav_contracts::seed(&mut a, 20, 100);
    strategy_nav_contracts::seed(&mut b, 40, 90);
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
    let apply = |account: &str, request| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::AssignCapital {
                account: account.into(),
                request,
            },
        )
    };
    assert!(matches!(
        apply("capital-A", request("capital-B", 3)),
        Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"))
    ));
    let mut stale = request("capital-A", 3);
    stale.authority_revision = 2;
    assert!(matches!(
        apply("capital-A", stale),
        Err(StoreError::Invalid("CAPITAL_AUTHORITY_REVISION_MISMATCH"))
    ));
    let mut backdated = request("capital-A", 3);
    backdated.time = 7;
    assert!(matches!(
        apply("capital-A", backdated),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_OUT_OF_ORDER"))
    ));
    assert!(matches!(
        apply("capital-A", request("capital-A", 999)),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"))
    ));
    assert!(matches!(
        apply("capital-A", request("capital-A", -999)),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"))
    ));
    let original =
        AccountStore::finance_rowid_sql(&tx, Some(("installation", "capital-A"))).unwrap();
    // Test-only SQL failure at the final receipt, after both economic legs and totals.
    tx.execute_batch("CREATE TEMP TRIGGER fail_capital_receipt BEFORE INSERT ON operation_receipts WHEN NEW.operation_id='same-native-assignment' AND NEW.account_key='capital-A' BEGIN SELECT RAISE(ABORT,'fixture receipt failure'); END").unwrap();
    assert!(apply("capital-A", request("capital-A", 3)).is_err());
    assert_eq!(
        AccountStore::finance_rowid_sql(&tx, Some(("installation", "capital-A"))).unwrap(),
        original
    );
    let latest =
        AccountStore::latest_capital_assignment_time_sql(&tx, Some(("installation", "capital-A")))
            .unwrap();
    assert_eq!(latest, Some(8));
    assert_eq!(
        AccountStore::capital_state_sql(&tx, Some(("installation", "capital-A")), 9, latest)
            .unwrap()
            .assigned_usd,
        Decimal::from(25)
    );
    assert!(
        AccountStore::operation_sql(
            &tx,
            Some(("installation", "capital-A")),
            "same-native-assignment"
        )
        .unwrap()
        .is_none()
    );
    tx.execute_batch("DROP TRIGGER fail_capital_receipt")
        .unwrap();
    for (account, delta, total) in [("capital-A", 3, 28), ("capital-B", -7, 38)] {
        let input = request(account, delta);
        let UnifiedFinanceWriteReceipt::CapitalAssigned(first) =
            apply(account, input.clone()).unwrap()
        else {
            panic!("wrong receipt")
        };
        assert_eq!(first.operation_id, input.operation_id);
        assert!(
            first
                .result_ref
                .contains(&format!("principal_usd:{total}|"))
        );
        let UnifiedFinanceWriteReceipt::CapitalAssigned(replay) =
            apply(account, input.clone()).unwrap()
        else {
            panic!("wrong receipt")
        };
        assert_eq!(first.result_ref, replay.result_ref);
        let mut conflict = input;
        conflict.request_hash = "changed".into();
        assert!(matches!(
            apply(account, conflict),
            Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
        ));
        let entries:i64=tx.query_row("SELECT COUNT(*) FROM finance_entries WHERE account_key=?1 AND economic_event_key='capital:same-native-assignment'",[account],|r|r.get(0)).unwrap();
        assert_eq!(entries, 2);
        assert_eq!(
            AccountStore::usd_component_total_sql(
                &tx,
                Some(("installation", account)),
                "INTERNAL_CAPITAL_USD"
            )
            .unwrap(),
            Decimal::ZERO
        );
        assert!(
            AccountStore::finance_proof_sealed_sql(&tx, Some(("installation", account))).unwrap()
        );
    }
    tx.execute_batch("SAVEPOINT typed_producer").unwrap();
    let command = CapitalAssignmentCommand {
        account_key: "capital-A".into(),
        operation_id: "typed-producer".into(),
        request_hash: "typed-hash".into(),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(2),
        authority_revision: 1,
        authority_receipt: "fixture-governance".into(),
        time: 10,
    };
    let invoke = |command| {
        dispatch(
            &tx,
            "installation",
            UnifiedFinanceWrite::AssignBoundCapital {
                account: "capital-A".into(),
                command,
            },
        )
    };
    let UnifiedFinanceWriteReceipt::CapitalAssignmentReported(first) =
        invoke(command.clone()).unwrap()
    else {
        panic!("wrong receipt")
    };
    assert_eq!(first.capital.unwrap().assigned_usd, Decimal::from(30));
    let mut stale = command.clone();
    stale.time = 6000;
    let UnifiedFinanceWriteReceipt::CapitalAssignmentReported(replay) =
        invoke(stale.clone()).unwrap()
    else {
        panic!("wrong receipt")
    };
    assert_eq!(replay.receipt, first.receipt);
    assert!(replay.capital.is_none());
    assert_eq!(replay.capital_coverage, "PARTIAL");
    stale.request_hash = "changed".into();
    assert!(matches!(
        invoke(stale),
        Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
    ));
    let mut foreign = command;
    foreign.account_key = "capital-B".into();
    assert!(matches!(
        invoke(foreign),
        Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"))
    ));
    tx.execute_batch("ROLLBACK TO typed_producer; RELEASE typed_producer")
        .unwrap();
    tx.commit().unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    for (account, expected, historical) in [("capital-A", 28, 20), ("capital-B", 38, 40)] {
        let scope = Some(("installation", account));
        let latest = AccountStore::latest_capital_assignment_time_sql(&db, scope).unwrap();
        assert_eq!(latest, Some(9));
        let state = AccountStore::capital_state_sql(&db, scope, 9, latest).unwrap();
        assert_eq!(state.assigned_usd, Decimal::from(expected));
        assert_eq!(
            AccountStore::capital_state_sql(&db, scope, 5, latest)
                .unwrap()
                .assigned_usd,
            Decimal::from(historical)
        );
        let receipt = AccountStore::operation_sql(&db, scope, "same-native-assignment")
            .unwrap()
            .unwrap();
        assert_eq!(receipt.kind, "CAPITAL_ASSIGNMENT");
        assert_eq!(receipt.committed_at, 9);
    }
}
