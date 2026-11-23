//! Original admission rules share one scope; retries never affect a sibling account.
use super::*;

fn imported(a: AccountStore, b: AccountStore) -> Connection {
    let ap = a.path.clone();
    let bp = b.path.clone();
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut db = Connection::open(path()).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    db
}
fn admission(quote: &EntryQuoteEvidence) -> EntryAdmissionEvidence {
    EntryAdmissionEvidence {
        schema: "okx.entry-admission/v2".into(),
        account_key: quote.account_key.clone(),
        installation_id: quote.installation_id.clone(),
        intent_id: quote.intent_id.clone(),
        plan_hash: quote.plan_hash.clone(),
        quote_digest: format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(quote).unwrap())
        ),
        admission_usd_per_usdt: quote.usd_per_usdt,
        other_entry_costs_upper_usd: Decimal::ZERO,
        entry_upper_usd: quote.total_usd,
        entry_proof_digest: "a".repeat(64),
        observed_at: quote.observed_at,
    }
}
#[test]
fn risk_book_contract_admission_binding_is_scoped_exact_and_rollback_atomic() {
    let mut a = store_with_account("admission-A");
    let mut b = store_with_account("admission-B");
    let ai = intent(&mut a);
    intent(&mut b);
    let quote = e1_source_quote(&a, &ai);
    let proof = admission(&quote);
    let mut db = imported(a, b);
    let tx = db.transaction().unwrap();
    let scope = Some(("installation", "admission-A"));
    AccountStore::bind_entry_quote_sql(&tx, scope, &quote).unwrap();
    let mut invalid = proof.clone();
    invalid.entry_upper_usd += Decimal::ONE;
    assert!(AccountStore::bind_entry_admission_sql(&tx, scope, &invalid).is_err());
    tx.rollback().unwrap();
    assert!(
        AccountStore::operation_sql(&db, scope, "entry-quote:intent")
            .unwrap()
            .is_none()
    );
    let tx = db.transaction().unwrap();
    AccountStore::bind_entry_quote_sql(&tx, scope, &quote).unwrap();
    AccountStore::bind_entry_admission_sql(&tx, scope, &proof).unwrap();
    let changes = tx.total_changes();
    AccountStore::bind_entry_quote_sql(&tx, scope, &quote).unwrap();
    AccountStore::bind_entry_admission_sql(&tx, scope, &proof).unwrap();
    assert_eq!(changes, tx.total_changes());
    assert!(
        AccountStore::bind_entry_quote_sql(&tx, Some(("installation", "admission-B")), &quote)
            .is_err()
    );
    assert!(
        AccountStore::operation_sql(
            &tx,
            Some(("installation", "admission-B")),
            "entry-quote:intent"
        )
        .unwrap()
        .is_none()
    );
    tx.commit().unwrap();
    assert_eq!(
        AccountStore::reserve_source_sql(&db, scope, "intent")
            .unwrap()
            .admission,
        proof
    );
}

#[test]
fn risk_book_contract_attempt_and_rejection_are_scoped_replayable_and_never_erase_post() {
    let mut a = store_with_account("admission-A");
    let mut b = store_with_account("admission-B");
    intent(&mut a);
    intent(&mut b);
    let mut db = imported(a, b);
    let tx = db.transaction().unwrap();
    let scope = Some(("installation", "admission-A"));
    AccountStore::mark_reserve_attempt_sql(&tx, scope, "intent", "reservation-A", 3).unwrap();
    let changes = tx.total_changes();
    AccountStore::mark_reserve_attempt_sql(&tx, scope, "intent", "reservation-A", 4).unwrap();
    assert_eq!(changes, tx.total_changes());
    assert!(AccountStore::mark_reserve_attempt_sql(&tx, scope, "intent", "different", 4).is_err());
    assert!(
        AccountStore::reject_unsent_sql(&tx, scope, "intent", "budget denied", 5, true).unwrap()
    );
    assert!(
        !AccountStore::reject_unsent_sql(&tx, scope, "intent", "later reason", 6, true).unwrap()
    );
    let original = AccountStore::find_intent_sql(&tx, scope, "intent_id", "intent")
        .unwrap()
        .unwrap();
    assert_eq!(original.incident.as_deref(), Some("budget denied"));
    assert!(!AccountStore::unsent_before_reserve_sql(&tx, scope, "intent").unwrap());
    let other = Some(("installation", "admission-B"));
    assert_eq!(
        AccountStore::find_intent_sql(&tx, other, "intent_id", "intent")
            .unwrap()
            .unwrap()
            .state,
        IntentState::Prepared
    );
    tx.execute(
        "UPDATE intents SET begin_post_at=7 WHERE account_key='admission-B'",
        [],
    )
    .unwrap();
    assert!(matches!(
        AccountStore::reject_unsent_sql(&tx, other, "intent", "invalid", 8, false),
        Err(StoreError::Invalid("POST_OUTCOME_REQUIRES_RECONCILIATION"))
    ));
    tx.rollback().unwrap();
}

#[test]
fn risk_book_contract_rejected_before_reserve_has_exact_scoped_no_send_witness() {
    let mut a = store_with_account("admission-A");
    let mut b = store_with_account("admission-B");
    intent(&mut a);
    intent(&mut b);
    let mut db = imported(a, b);
    let tx = db.transaction().unwrap();
    let scope = Some(("installation", "admission-A"));
    AccountStore::reject_unsent_sql(&tx, scope, "intent", "expired", 3, false).unwrap();
    assert!(AccountStore::unsent_before_reserve_sql(&tx, scope, "intent").unwrap());
    assert!(
        !AccountStore::unsent_before_reserve_sql(
            &tx,
            Some(("installation", "admission-B")),
            "intent"
        )
        .unwrap()
    );
    assert!(
        AccountStore::unsent_before_reserve_sql(&tx, Some(("foreign", "admission-A")), "intent")
            .is_err()
    );
}

#[test]
fn risk_book_contract_manual_instance_scoped_creation_is_original_and_replay_does_not_write() {
    let mut db = Connection::open_in_memory().unwrap();
    db.execute_batch(include_str!("../schema/finance-unified-v1.sql"))
        .unwrap();
    for account in ["admission-A", "admission-B"] {
        db.execute(
            "INSERT INTO account_meta VALUES(1,?1,'installation',2,1,'test','init','test')",
            [account],
        )
        .unwrap();
        db.execute(
            "INSERT INTO account_policy VALUES('installation',?1,1,1,0,'campaign','20','0')",
            [account],
        )
        .unwrap();
    }
    let tx = db.transaction().unwrap();
    let scope = Some(("installation", "admission-A"));
    let before = tx.total_changes();
    let preview = AccountStore::plan_definition_sql(&tx, scope, "manual", false, 2).unwrap();
    assert_eq!(tx.total_changes(), before);
    assert!(AccountStore::instances_sql(&tx, scope).unwrap().is_empty());
    let definition = AccountStore::ensure_manual_instance_sql(&tx, scope, 2).unwrap();
    assert_eq!(preview.version, definition.version);
    assert_eq!(definition.parameters, "{\"kind\":\"manual\"}");
    assert_eq!(
        AccountStore::instance_sql(&tx, scope, "manual")
            .unwrap()
            .unwrap()
            .budget,
        Decimal::from(20)
    );
    let changes = tx.total_changes();
    assert_eq!(
        AccountStore::ensure_manual_instance_sql(&tx, scope, 3)
            .unwrap()
            .version,
        definition.version
    );
    assert_eq!(tx.total_changes(), changes);
    assert!(
        AccountStore::instance_sql(&tx, Some(("installation", "admission-B")), "manual")
            .unwrap()
            .is_none()
    );
    assert_eq!(AccountStore::instances_sql(&tx, scope).unwrap().len(), 1);
    assert!(
        AccountStore::instances_sql(&tx, Some(("installation", "admission-B")))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn risk_book_contract_order_readback_uses_original_scoped_transition_and_rejects_wrong_state() {
    let mut a = store_with_account("admission-A");
    let mut b = store_with_account("admission-B");
    intent(&mut a);
    reserve(&mut a);
    assert!(a.begin_post("intent", 1, 4).unwrap());
    intent(&mut b);
    reserve(&mut b);
    assert!(b.begin_post("intent", 1, 4).unwrap());
    let mut db = imported(a, b);
    let tx = db.transaction().unwrap();
    let scope = Some(("installation", "admission-A"));
    AccountStore::record_order_readback_sql(
        &tx,
        scope,
        "intent",
        "venue-A",
        IntentState::Acknowledged,
        Decimal::ZERO,
        5,
    )
    .unwrap();
    let recorded = AccountStore::find_intent_sql(&tx, scope, "intent_id", "intent")
        .unwrap()
        .unwrap();
    assert_eq!(recorded.state, IntentState::Acknowledged);
    assert_eq!(recorded.exchange_order_id.as_deref(), Some("venue-A"));
    let sibling = AccountStore::find_intent_sql(
        &tx,
        Some(("installation", "admission-B")),
        "intent_id",
        "intent",
    )
    .unwrap()
    .unwrap();
    assert_eq!(sibling.state, IntentState::Posting);
    assert!(sibling.exchange_order_id.is_none());
    assert!(matches!(
        AccountStore::record_order_readback_sql(
            &tx,
            scope,
            "intent",
            "venue-A",
            IntentState::Filled,
            Decimal::from(2),
            6
        ),
        Err(StoreError::Invalid("ORDER_READBACK_INVALID"))
    ));
    assert!(
        AccountStore::record_order_readback_sql(
            &tx,
            Some(("foreign", "admission-A")),
            "intent",
            "venue-A",
            IntentState::Filled,
            Decimal::ONE,
            6
        )
        .is_err()
    );
    tx.rollback().unwrap();
    let persisted = AccountStore::find_intent_sql(&db, scope, "intent_id", "intent")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, IntentState::Posting);
    let orders: i64 = db
        .query_row("SELECT COUNT(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    assert_eq!(orders, 0, "order and outcome must rollback together");
}
