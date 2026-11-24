//! S0 offline vertical witness: strictly validated command -> original durable
//! financial PREPARE. It deliberately stops BEFORE risk reserve/POST and must
//! never be described as a production trading route or a venue acceptance.

use okx_trade_execution::{
    AccountMeta, AccountStore, Decision, IntentPlan, IntentState, NewIntent, OpeningSnapshot,
};
use okx_trading_engine::{PositionTarget, ValidatedCommand, validate_wire_command};
use rust_decimal::Decimal;
use std::{path::PathBuf, time::SystemTime};

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

#[test]
fn typed_manual_target_reaches_original_sql_writer_and_survives_reopen_without_send() {
    let payload = r#"{"kind":"manual","account":"okx:real:verified","instrument":"BTC-USDT-SWAP","operation":"s0-command-1","target":{"kind":"long","quantity":"1.000"},"observed_at_ms":2,"expires_at_ms":100}"#;
    let command = validate_wire_command(payload).expect("typed envelope");
    let ValidatedCommand::Manual(command) = command else {
        panic!("expected manual command");
    };
    let PositionTarget::Long(quantity) = command.target else {
        panic!("only this verified test target");
    };

    // This is the exact existing financial SQLite/WAL owner, NOT a new schema.
    // No venue client or send capability is constructed in this witness.
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

    // Unvalidated input cannot reach the financial owner and cannot mutate
    // the existing prepared record. No risk reserve or POST is attempted.
    let forged = payload.replace("\"quantity\":\"1.000\"", "\"quantity\":\"-1\"");
    assert!(validate_wire_command(&forged).is_err());
    assert_eq!(
        owner
            .by_idempotency(command.operation.as_str())
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
    // The uniquely created test DB is temporary; never open a user DB.
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}
