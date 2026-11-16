use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn request() -> NewFinanceInstallation {
    NewFinanceInstallation {
        installation_id: "installation_test".into(),
        initialize_request_id: "initialize_test_0001".into(),
        source_tree: "a".repeat(40),
    }
}
fn database() -> (std::path::PathBuf, Connection) {
    let root = std::env::temp_dir().join(format!(
        "okx-fresh-finance-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("finance.sqlite");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    (path, db)
}
#[test]
fn risk_book_contract_new_installation_creation_and_current_reopen_are_not_an_import() {
    let (path, mut db) = database();
    let first = AccountStore::initialize_finance_installation(&mut db, &request()).unwrap();
    assert_eq!(first.initialize_request_id, "initialize_test_0001");
    assert_eq!(first.schema_version, 8);
    assert!(first.created_at_ms > 0);
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='finance_import_manifest'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM account_meta", [], |r| r.get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_meta", [], |r| r.get(0))
            .unwrap(),
        0
    );
    // A legitimate new account fact changes the current database. Inspection
    // must not compare these rows to an initial empty-content digest.
    db.execute("INSERT INTO account_meta VALUES(1,'okx:real:test','installation_test',2,1,'verified-source','connect_0001','release')",[]).unwrap();
    drop(db);
    let reopened = Connection::open(&path).unwrap();
    reopened.pragma_update(None, "foreign_keys", "ON").unwrap();
    let before = reopened.total_changes();
    assert_eq!(
        AccountStore::inspect_finance_installation(&reopened, "installation_test").unwrap(),
        first
    );
    assert_eq!(reopened.total_changes(), before);
    assert_eq!(
        reopened
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM account_meta", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert!(AccountStore::inspect_finance_installation(&reopened, "another_installation").is_err());
    assert!(
        reopened
            .execute("DELETE FROM finance_installation", [])
            .is_err()
    );
    drop(reopened);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn risk_book_contract_new_installation_fee_and_late_guards_exist_before_first_receipt() {
    let (path, mut db) = database();
    let initialized = AccountStore::initialize_finance_installation(&mut db, &request()).unwrap();
    assert_eq!(initialized.schema_version, 8);
    let before = schema_digest(&db).unwrap();
    let guards: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name IN (
         'immutable_entry_fee_owner_update','immutable_entry_fee_owner_delete',
         'immutable_entry_late_update','immutable_entry_late_delete')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        guards, 4,
        "all late-money guards belong to canonical initialization"
    );
    // Schema-only fixture: exercise the immutable guards, not financial admission.
    db.execute("INSERT INTO account_meta VALUES(1,'okx:real:test','installation_test',2,1,'verified-source','connect_0001','release')",[]).unwrap();
    for kind in [
        "ENTRY_ORDER_FEE_PROVEN",
        "ENTRY_ORDER_FEE_INDEX",
        "ENTRY_LATE_SETTLEMENT_PROVEN",
        "ENTRY_LATE_SETTLED",
    ] {
        db.execute("INSERT INTO operation_receipts VALUES('installation_test','okx:real:test',?1,?1,'request','source',1)", [kind]).unwrap();
        for statement in [
            "UPDATE operation_receipts SET result_ref='changed' WHERE operation_id=?1",
            "DELETE FROM operation_receipts WHERE operation_id=?1",
        ] {
            let error = db.execute(statement, [kind]).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(if kind.starts_with("ENTRY_ORDER_FEE") {
                        "IMMUTABLE_ENTRY_FEE_OWNER"
                    } else {
                        "IMMUTABLE_LATE_ENTRY"
                    }),
                "{kind}: {error}"
            );
        }
    }
    assert_eq!(schema_digest(&db).unwrap(), before);
    assert_eq!(
        AccountStore::inspect_finance_installation(&db, "installation_test").unwrap(),
        initialized
    );
    drop(db);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
#[test]
fn risk_book_contract_new_installation_never_resets_existing_or_unknown_schema() {
    let (path, mut db) = database();
    let first = AccountStore::initialize_finance_installation(&mut db, &request()).unwrap();
    let mut conflict = request();
    conflict.initialize_request_id = "different_request_0001".into();
    assert!(matches!(
        AccountStore::initialize_finance_installation(&mut db, &conflict),
        Err(StoreError::Invalid(
            "FINANCE_INITIALIZATION_TARGET_NOT_EMPTY"
        ))
    ));
    assert_eq!(
        AccountStore::inspect_finance_installation(&db, "installation_test").unwrap(),
        first
    );
    db.execute("CREATE TABLE unknown_state(value TEXT)", [])
        .unwrap();
    assert!(matches!(
        AccountStore::inspect_finance_installation(&db, "installation_test"),
        Err(StoreError::Invalid("FINANCE_INSTALLATION_SCHEMA_MISMATCH"))
    ));
    drop(db);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    let (path, mut db) = database();
    db.execute("CREATE TABLE original_state(value TEXT)", [])
        .unwrap();
    db.execute("INSERT INTO original_state VALUES('preserved')", [])
        .unwrap();
    assert!(AccountStore::initialize_finance_installation(&mut db, &request()).is_err());
    assert_eq!(
        db.query_row::<String, _, _>("SELECT value FROM original_state", [], |r| r.get(0))
            .unwrap(),
        "preserved"
    );
    drop(db);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

#[test]
fn risk_book_contract_prior_native_version_is_rejected_without_upgrade_or_reset() {
    let (path, mut db) = database();
    AccountStore::initialize_finance_installation(&mut db, &request()).unwrap();
    db.pragma_update(None, "user_version", 7).unwrap();
    let before = db.total_changes();
    assert!(matches!(
        AccountStore::inspect_finance_installation(&db, "installation_test"),
        Err(StoreError::Invalid("FINANCE_INSTALLATION_SCHEMA_MISMATCH"))
    ));
    assert!(AccountStore::initialize_finance_installation(&mut db, &request()).is_err());
    assert_eq!(db.total_changes(), before);
    assert_eq!(
        db.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        7
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM finance_installation", [], |r| r
            .get(0))
            .unwrap(),
        1
    );
    drop(db);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
#[test]
fn risk_book_contract_new_installation_rejects_bad_request_without_schema_side_effects() {
    let (path, mut db) = database();
    let mut bad = request();
    bad.source_tree = "not-a-source-tree".into();
    assert!(AccountStore::initialize_finance_installation(&mut db, &bad).is_err());
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))
            .unwrap(),
        0
    );
    drop(db);
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
}
