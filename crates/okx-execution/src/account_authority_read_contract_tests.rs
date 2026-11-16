//! Same local receipt IDs do not make policies or reservation evidence global.
use super::*;
#[test]
fn risk_book_contract_policy_and_central_receipt_reads_are_scoped_and_exact() {
    let mut a = store_with_account("authority-A");
    let mut b = store_with_account_campaign("authority-B", "campaign-B");
    intent_with_plan_hash(&mut a, "plan-A");
    intent_with_plan_hash(&mut b, "plan-B");
    reserve(&mut a);
    reserve(&mut b);
    b.apply_authority_policy(2, true, "campaign-B", Decimal::from(15))
        .unwrap();
    let expected_a = a.account_policy().unwrap();
    let expected_b = b.account_policy().unwrap();
    let receipt_a = a.central_receipt("intent").unwrap().unwrap();
    assert_eq!(
        a.active_central_receipt("intent")
            .unwrap()
            .unwrap()
            .plan_hash,
        "plan-A"
    );
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
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    let before = tx.total_changes();
    for (account, expected, hash) in [
        ("authority-A", expected_a, "plan-A"),
        ("authority-B", expected_b, "plan-B"),
    ] {
        let scope = Some(("installation", account));
        assert_eq!(
            AccountStore::account_policy_sql(&tx, scope).unwrap(),
            expected
        );
        for view in [
            CentralReceiptView::Retained,
            CentralReceiptView::ActiveExposure,
        ] {
            let found = AccountStore::reservation_receipt_sql(&tx, scope, "intent", view)
                .unwrap()
                .unwrap();
            assert_eq!(found.account_key, account);
            assert_eq!(found.plan_hash, hash);
            assert_eq!(found.requested_loss_usd, Decimal::from(5));
            assert!(
                AccountStore::reservation_receipt_sql(&tx, scope, "missing", view)
                    .unwrap()
                    .is_none()
            );
            if account == "authority-A" {
                assert_eq!(
                    serde_json::to_value(found).unwrap(),
                    serde_json::to_value(&receipt_a).unwrap()
                );
            }
        }
    }
    assert!(AccountStore::account_policy_sql(&tx, Some(("installation", "missing"))).is_err());
    assert!(
        AccountStore::reservation_receipt_sql(
            &tx,
            Some(("foreign", "authority-A")),
            "intent",
            CentralReceiptView::Retained
        )
        .is_err()
    );
    assert_eq!(tx.total_changes(), before);
    tx.commit().unwrap();
    // Retained history must never become active exposure after RELEASED.
    db.execute("UPDATE risk_receipts SET state='RELEASED' WHERE installation_id='installation' AND account_key='authority-B'",[]).unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    assert!(
        AccountStore::reservation_receipt_sql(
            &db,
            Some(("installation", "authority-B")),
            "intent",
            CentralReceiptView::Retained
        )
        .unwrap()
        .is_some()
    );
    assert!(
        AccountStore::reservation_receipt_sql(
            &db,
            Some(("installation", "authority-B")),
            "intent",
            CentralReceiptView::ActiveExposure
        )
        .unwrap()
        .is_none()
    );
    assert!(
        AccountStore::reservation_receipt_sql(
            &db,
            Some(("installation", "authority-A")),
            "intent",
            CentralReceiptView::ActiveExposure
        )
        .unwrap()
        .is_some()
    );
}
