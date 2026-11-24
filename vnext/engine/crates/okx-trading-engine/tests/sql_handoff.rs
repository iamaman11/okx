//! S0 offline vertical prototype: strictly validated command -> original
//! SQLite/WAL financial state machine -> one simulated venue request ->
//! lost ACK / restart / independent readback. This is NOT a production trading
//! handler: its synthetic central receipt is not financial risk authorization,
//! and only the native UnifiedFinanceHandle may mint CommittedVenueSend.

use okx_trade_execution::{
    AccountMeta, AccountStore, CentralReceipt, Decision, IntentPlan, IntentState, NewIntent,
    OpeningSnapshot,
};
use okx_trading_engine::{PositionTarget, ValidatedCommand, validate_wire_command};
use rust_decimal::Decimal;
use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

const WIRE: &str = r#"{"kind":"manual","account":"okx:real:verified","instrument":"BTC-USDT-SWAP","operation":"s0-command-1","target":{"kind":"long","quantity":"1.000"},"observed_at_ms":2,"expires_at_ms":100}"#;

fn temporary_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "okx-vnext-s0-typed-prepare-{}-{nonce}.sqlite",
        std::process::id()
    ))
}

fn prepared_fixture() -> (PathBuf, String, AccountStore, NewIntent) {
    let command = validate_wire_command(WIRE).expect("typed envelope");
    let ValidatedCommand::Manual(command) = command else {
        panic!("expected manual command");
    };
    let PositionTarget::Long(quantity) = command.target else {
        panic!("only this verified test target");
    };

    // This is the existing financial SQLite/WAL owner, NOT a new schema or
    // production authority. Both offline tests share the exact same fixture.
    let path = temporary_path();
    let account = command.account.as_str().to_owned();
    let meta = AccountMeta {
        initialize_operation_id: "s0-initialize".into(),
        release_id: "s0-offline".into(),
        account_key: account.clone(),
        installation_id: "s0-fixture".into(),
        initialized_at: 1,
        opening_source: "authenticated-flat".into(),
    };
    let opening = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: meta.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let mut owner = AccountStore::initialize_flat(&path, meta, &opening).expect("real WAL owner");
    owner
        .apply_authority_policy(1, false, "s0-campaign", Decimal::from(20))
        .expect("offline policy fixture");
    let definition = owner.ensure_manual_instance(1).expect("manual definition");
    let decision = Decision {
        decision_id: "s0-decision".into(),
        instance_id: None,
        decision_at: command.window.observed_at_ms(),
        input_digest: "s0-validated-payload".into(),
        policy_revision: 1,
        code_version: "s0-typed-command".into(),
    };
    let intent = NewIntent {
        plan: IntentPlan {
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            contract_value: Decimal::ONE,
            inverse: false,
            settlement_currency: "USDT".into(),
            position_side: "long".into(),
            private_source_generation: 1,
            operation: "open".into(),
            side: "buy".into(),
            quantity: quantity.amount(),
            limit_price: Decimal::from(100),
            stop_price: Some(Decimal::from(95)),
            strategy_instance: "manual".into(),
            planned_loss_usd: Decimal::from(5),
            source_generation: 1,
        },
        intent_id: "s0-intent-1".into(),
        idempotency_key: command.operation.as_str().into(),
        request_hash: "s0-body-digest".into(),
        client_order_id: "s0-offline-client".into(),
        deadline: command.window.expires_at_ms(),
        plan_hash: "a".repeat(64),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: command.instrument.as_str().into(),
    };
    let saved = owner.prepare(&decision, &intent).expect("durable prepare");
    assert_eq!(saved.state, IntentState::Prepared);
    assert!(saved.exchange_order_id.is_none());
    let repeated = owner.prepare(&decision, &intent).expect("idempotent retry");
    assert_eq!(repeated.intent_id, saved.intent_id);
    (path, account, owner, intent)
}

fn cleanup(path: &Path) {
    // Only this freshly created, uniquely named test database is disposable.
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[test]
fn typed_manual_target_reaches_original_sql_writer_and_survives_reopen_without_send() {
    let (path, account, owner, intent) = prepared_fixture();

    // Unvalidated input cannot reach the financial owner and cannot mutate
    // the existing prepared record. No risk reserve or POST is attempted.
    let forged = WIRE.replace("\"quantity\":\"1.000\"", "\"quantity\":\"-1\"");
    assert!(validate_wire_command(&forged).is_err());
    assert_eq!(
        owner
            .by_idempotency(&intent.idempotency_key)
            .expect("read back")
            .expect("prepared")
            .state,
        IntentState::Prepared
    );

    drop(owner);
    let recovered =
        AccountStore::open_existing(&path, &account, "s0-fixture").expect("real WAL reopen");
    let persisted = recovered
        .intent("s0-intent-1")
        .expect("read")
        .expect("intent");
    assert_eq!(persisted.state, IntentState::Prepared);
    assert!(persisted.exchange_order_id.is_none());
    drop(recovered);
    cleanup(&path);
}

#[derive(Default)]
struct OfflineVenue {
    posted_client_ids: Vec<String>,
}

impl OfflineVenue {
    fn post_and_lose_ack(&mut self, client_order_id: &str) {
        self.posted_client_ids.push(client_order_id.to_owned());
        // The exchange may have accepted the POST; the caller sees no ACK.
    }

    fn independent_order_readback(&self, client_order_id: &str) -> Option<&'static str> {
        self.posted_client_ids
            .iter()
            .any(|known| known == client_order_id)
            .then_some("s0-simulated-venue-order")
    }
}

#[test]
fn typed_manual_target_sqlite_lost_ack_reopen_and_no_second_simulated_post() {
    let (path, account, mut owner, intent) = prepared_fixture();
    let receipt = CentralReceipt {
        reservation_key: "s0-offline-central-reservation".into(),
        account_key: account.clone(),
        installation_id: "s0-fixture".into(),
        instance_id: None,
        intent_id: intent.intent_id.clone(),
        plan_hash: intent.plan_hash.clone(),
        policy_revision: 1,
        receipt_hash: "s0-synthetic-risk-receipt".into(),
        requested_loss_usd: Decimal::from(5),
        conversion_digest: "s0-test-conversion".into(),
    };
    // S0-only: this preconstructed receipt is NOT a RiskBook authorization.
    // S1 must replace this fixture with a real financial admission decision.
    owner
        .reserve(&receipt, 3)
        .expect("real SQLite reserve transaction");
    assert_eq!(
        owner.intent(&intent.intent_id).unwrap().unwrap().state,
        IntentState::Reserved
    );
    assert!(
        owner
            .begin_post(&intent.intent_id, 1, 4)
            .expect("durable begin-post")
    );
    assert_eq!(owner.post_fence_evidence(&intent.intent_id).unwrap().0, 4);

    // The simulated venue receives exactly one request; the ACK is lost.
    let mut venue = OfflineVenue::default();
    venue.post_and_lose_ack(&intent.client_order_id);
    owner
        .record_outcome(
            &intent.intent_id,
            IntentState::Unknown,
            None,
            "s0-lost-ack",
            5,
        )
        .expect("persist UNKNOWN, never optimistically succeed");
    drop(owner);

    let mut recovered =
        AccountStore::open_existing(&path, &account, "s0-fixture").expect("WAL restart");
    assert_eq!(
        recovered.intent(&intent.intent_id).unwrap().unwrap().state,
        IntentState::Unknown
    );
    assert!(!recovered.begin_post(&intent.intent_id, 1, 6).unwrap());
    assert_eq!(
        venue.posted_client_ids.len(),
        1,
        "no repost after lost ACK/reopen"
    );

    // The reconciliation answer comes from the simulated venue's independent
    // client-order lookup, not a guessed ACK or the mutable SQLite row.
    let venue_order = venue
        .independent_order_readback(&intent.client_order_id)
        .expect("readback resolves original client order");
    recovered
        .record_outcome(
            &intent.intent_id,
            IntentState::Acknowledged,
            Some(venue_order),
            "s0-independent-venue-readback",
            7,
        )
        .expect("reconcile original intent");
    let resolved = recovered.intent(&intent.intent_id).unwrap().unwrap();
    assert_eq!(resolved.state, IntentState::Acknowledged);
    assert_eq!(resolved.exchange_order_id.as_deref(), Some(venue_order));
    assert!(!recovered.begin_post(&intent.intent_id, 1, 8).unwrap());
    assert_eq!(venue.posted_client_ids.len(), 1);
    drop(recovered);
    cleanup(&path);
}
