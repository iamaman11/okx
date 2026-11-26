//! Account-scoped subject identities: a local receipt is not venue authority.
use super::*;

fn snapshot(
    store: &AccountStore,
    kind: TradeDiagnosticSubject,
    key: &str,
) -> StoreResult<TradeSubjectDiagnosticSnapshot> {
    AccountStore::trade_subject_diagnostic_sql(&store.connection, None, kind, key, 77)
}

#[test]
fn cancel_posting_is_not_an_ack_and_durable_receipt_survives_reopen() {
    let mut store = store();
    let original = intent(&mut store);
    let key = "cancel_key_123";
    assert!(
        snapshot(&store, TradeDiagnosticSubject::Cancel, key)
            .unwrap()
            .operation
            .is_none()
    );
    store.begin_operation(key, "digest", "cancel", 10).unwrap();
    let posting = snapshot(&store, TradeDiagnosticSubject::Cancel, key).unwrap();
    assert_eq!(posting.operation.as_ref().unwrap().result_ref, "POSTING");
    assert!(posting.intent.is_none());
    store
        .operation_receipt(key, "digest", "cancel", &original.intent_id, 11)
        .unwrap();
    let accepted = snapshot(&store, TradeDiagnosticSubject::Cancel, key).unwrap();
    assert_eq!(
        accepted.intent.as_ref().unwrap().intent_id,
        original.intent_id
    );
    assert_eq!(accepted.operation.as_ref().unwrap().kind, "cancel");
    let saved = store.path.clone();
    let account = store.meta.account_key.clone();
    let installation = store.meta.installation_id.clone();
    drop(store);
    let reopened = AccountStore::open_existing(&saved, &account, &installation).unwrap();
    let persisted = snapshot(&reopened, TradeDiagnosticSubject::Cancel, key).unwrap();
    assert_eq!(persisted.operation.unwrap().result_ref, original.intent_id);
}

#[test]
fn wrong_kind_and_cross_account_scope_fail_closed() {
    let mut store = store_with_account("diagnostic-A");
    intent(&mut store);
    store
        .begin_operation("cancel_key_123", "digest", "not_cancel", 10)
        .unwrap();
    assert!(matches!(
        snapshot(&store, TradeDiagnosticSubject::Cancel, "cancel_key_123"),
        Err(StoreError::Invalid("CANCEL_RECEIPT_KIND_MISMATCH"))
    ));
    assert!(
        AccountStore::trade_subject_diagnostic_sql(
            &store.connection,
            Some(("installation", "diagnostic-B")),
            TradeDiagnosticSubject::Cancel,
            "cancel_key_123",
            77,
        )
        .is_err()
    );
    assert!(snapshot(&store, TradeDiagnosticSubject::Cancel, "bad:key").is_err());
    store
        .operation_receipt("cancel_orphan_123", "hash", "cancel", "foreign-intent", 12)
        .unwrap();
    assert!(matches!(
        snapshot(&store, TradeDiagnosticSubject::Cancel, "cancel_orphan_123"),
        Err(StoreError::Invalid("CANCEL_RECEIPT_INTENT_MISSING"))
    ));
}

#[test]
fn close_requires_the_actual_close_operation_and_rejects_forged_plan() {
    let mut store = store();
    intent(&mut store);
    store
        .connection
        .execute("UPDATE intents SET idempotency_key='close_key_123'", [])
        .unwrap();
    assert!(matches!(
        snapshot(&store, TradeDiagnosticSubject::Close, "close_key_123"),
        Err(StoreError::Invalid("CLOSE_INTENT_OPERATION_MISMATCH"))
    ));
    let forged_plan = store
        .connection
        .execute(
            "UPDATE intent_plans SET operation='close' WHERE intent_id='intent'",
            [],
        );
    // The financial SQL owner makes the original plan immutable. A forged
    // close label must not bypass that guard or become diagnostic evidence.
    assert!(forged_plan.is_err());
    assert!(matches!(
        snapshot(&store, TradeDiagnosticSubject::Close, "close_key_123"),
        Err(StoreError::Invalid("CLOSE_INTENT_OPERATION_MISMATCH"))
    ));
}

#[test]
fn close_reads_only_its_own_prepared_intent_without_venue_claim() {
    let mut store = store();
    let opening = intent(&mut store);
    let decision = Decision {
        decision_id: "decision-close".into(),
        instance_id: None,
        decision_at: 3,
        input_digest: "close-inputs".into(),
        policy_revision: 1,
        code_version: "test".into(),
    };
    let request = NewIntent {
        plan: IntentPlan {
            operation: "close".into(),
            side: "sell".into(),
            ..opening.plan.clone()
        },
        intent_id: "close-intent".into(),
        idempotency_key: "close_key_123".into(),
        request_hash: "close-body".into(),
        client_order_id: "close-client".into(),
        deadline: 100,
        plan_hash: "close-plan".into(),
        policy_revision: 1,
        risk_increase: false,
        instrument_id: opening.instrument_id,
    };
    store.prepare(&decision, &request).unwrap();
    let observed = snapshot(&store, TradeDiagnosticSubject::Close, "close_key_123").unwrap();
    assert_eq!(observed.intent.unwrap().plan.operation, "close");
    assert!(observed.operation.is_none());
}

#[test]
fn forged_hash_derived_replace_cancel_is_only_unverified_candidate() {
    let mut store = store();
    intent(&mut store);
    let sha = "a".repeat(64);
    store
        .connection
        .execute(
            "UPDATE intents SET idempotency_key=?1,request_hash=?2",
            rusqlite::params!["replace_key_123", sha],
        )
        .unwrap();
    let candidate_key = format!("replace_cancel_{}", &sha[..40]);
    store
        .operation_receipt(
            &candidate_key,
            "unrelated-cancel-request",
            "cancel",
            "foreign-intent",
            12,
        )
        .unwrap();
    let before = store.connection.total_changes();
    let found = snapshot(&store, TradeDiagnosticSubject::Replace, "replace_key_123").unwrap();
    assert_eq!(store.connection.total_changes(), before);
    assert_eq!(found.intent.unwrap().plan.operation, "open");
    assert_eq!(
        found.unverified_cancel_candidate.unwrap().result_ref,
        "foreign-intent"
    );
    assert!(
        snapshot(&store, TradeDiagnosticSubject::Replace, "unknown_key_123")
            .unwrap()
            .intent
            .is_none()
    );
}
