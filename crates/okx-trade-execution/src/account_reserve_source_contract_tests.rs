//! Native reserve sources use original quote/admission rules in both layouts.
use super::*;
fn seed(s: &mut AccountStore, proof: &str) -> AccountReserveSource {
    let intent = intent(s);
    let quote = e1_source_quote(s, &intent);
    s.bind_entry_quote(&quote).unwrap();
    assert!(matches!(
        s.reserve_source(&intent.intent_id),
        Err(StoreError::Invalid("ENTRY_ALL_IN_COST_UNPROVEN"))
    ));
    let admission = EntryAdmissionEvidence {
        schema: "okx.entry-admission/v2".into(),
        account_key: s.meta.account_key.clone(),
        installation_id: s.meta.installation_id.clone(),
        intent_id: intent.intent_id.clone(),
        plan_hash: intent.plan_hash.clone(),
        quote_digest: format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(&quote).unwrap())
        ),
        admission_usd_per_usdt: Decimal::ONE,
        other_entry_costs_upper_usd: Decimal::ZERO,
        entry_upper_usd: quote.total_usd,
        entry_proof_digest: proof.into(),
        observed_at: quote.observed_at,
    };
    s.bind_entry_admission(&admission).unwrap();
    s.reserve_source(&intent.intent_id).unwrap()
}
#[test]
fn risk_book_contract_reserve_source_quote_admission_scoped_and_revalidated() {
    let mut a = store_with_account("reserve-source-A");
    let mut b = store_with_account("reserve-source-B");
    let aa = seed(&mut a, &"a".repeat(64));
    let bb = seed(&mut b, &"b".repeat(64));
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
    let changes = tx.total_changes();
    for (account, expected) in [("reserve-source-A", &aa), ("reserve-source-B", &bb)] {
        let scope = Some(("installation", account));
        let actual = AccountStore::reserve_source_sql(&tx, scope, "intent").unwrap();
        assert_eq!(actual.quote, expected.quote);
        assert_eq!(actual.admission, expected.admission);
        assert_eq!(
            serde_json::to_value(actual.intent).unwrap(),
            serde_json::to_value(&expected.intent).unwrap()
        );
        let mut forged = expected.admission.clone();
        forged.entry_upper_usd += Decimal::ONE;
        assert!(matches!(
            AccountStore::verified_entry_admission_sql(&tx, scope, &forged),
            Err(StoreError::Invalid("ENTRY_ADMISSION_SOURCE_UNVERIFIED"))
        ));
        let mut forged = expected.quote.clone();
        forged.fee_usdt += Decimal::ONE;
        assert!(matches!(
            AccountStore::verified_stored_entry_quote_sql(&tx, scope, &forged),
            Err(StoreError::Invalid("ENTRY_QUOTE_ARITHMETIC_MISMATCH"))
        ));
    }
    assert!(
        AccountStore::reserve_source_sql(&tx, Some(("installation", "missing")), "intent").is_err()
    );
    assert!(matches!(
        AccountStore::reserve_source_sql(
            &tx,
            Some(("installation", "reserve-source-A")),
            "missing"
        ),
        Err(StoreError::Invalid("RESERVE_SOURCE_INTENT_MISSING"))
    ));
    assert!(matches!(
        AccountStore::verified_entry_admission_sql(
            &tx,
            Some(("installation", "reserve-source-B")),
            &aa.admission
        ),
        Err(StoreError::Invalid("ENTRY_ADMISSION_SOURCE_UNVERIFIED"))
    ));
    assert_eq!(
        tx.total_changes(),
        changes,
        "source reads cannot mutate money or receipts"
    );
    tx.commit().unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    assert_eq!(
        AccountStore::reserve_source_sql(&db, Some(("installation", "reserve-source-A")), "intent")
            .unwrap()
            .admission,
        aa.admission
    );
}
