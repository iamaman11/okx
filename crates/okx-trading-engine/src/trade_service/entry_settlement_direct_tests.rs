//! Native boundary coverage replacing the obsolete split-owner settlement fixture.
//!
//! Current source and money witnesses live in:
//! * native_partial_entry::risk_book_contract_native_partial_sources_missing_bill_then_terminal_status_and_replay:
//!   missing independent bill, cumulative amounts, reservation and replay.
//! * native_fee_correction::risk_book_contract_native_late_fee_rebate_and_bound_breach_use_immutable_revisions:
//!   immutable same-revision replay, late revision, source digest and one debit.
//! * native_post_release_correction::verify_post_release_correction (called by
//!   native filled-close): original terminal receipt, release and late correction.
//! * native_crash::risk_book_contract_native_os_kill_after_create_recovers_original_unknown
//!   and native_execute_safety contracts: lost venue ACK and no second send.
//!
//! The retired Worker ACK/retry/CAS choreography is not the Native owner transition.
use super::*;
use crate::accounts::native_trade_contracts::fixture as native;
use crate::native_finance_risk::NativeFinanceRisk;
use okx_ipc_contracts::accounts::Environment;
use okx_trade_execution::{Decision, IntentPlan, NewIntent};

fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}

#[test]
fn native_committed_reply_unknown_is_awaiting_evidence_at_both_boundaries() {
    let code = "UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY";
    assert_eq!(
        store_failure_kind(&okx_trade_execution::StoreError::Invalid(code)),
        SettlementFailureKind::AwaitingEvidence
    );
    assert_eq!(
        risk_failure_kind(code),
        SettlementFailureKind::AwaitingEvidence
    );
    assert_eq!(
        risk_failure_kind("NATIVE_RISK_SETTLEMENT_OWNER_UNAVAILABLE"),
        SettlementFailureKind::Unavailable
    );
    assert_eq!(
        risk_failure_kind("PREFIX_UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"),
        SettlementFailureKind::Failed
    );
}

#[tokio::test]
async fn native_entry_preconditions_reject_before_financial_mutation() {
    let fixture = native::fixture().await;
    let account = fixture.config.account_key.clone();
    let key = account.wire();
    let definition = fixture
        .owner
        .ensure_manual_instance(deadline(), key.clone())
        .await
        .unwrap();
    let at = jiff::Timestamp::now().as_millisecond();
    let revision = fixture.authority.state.revision as i64;
    let intent = fixture
        .owner
        .prepare_admission(
            deadline(),
            key.clone(),
            Decision {
                decision_id: "entry_boundary_decision".into(),
                instance_id: None,
                decision_at: at,
                input_digest: "b".repeat(64),
                policy_revision: revision,
                code_version: "fixture".into(),
            },
            NewIntent {
                intent_id: "entry_boundary_intent".into(),
                idempotency_key: "entry_boundary_command".into(),
                request_hash: "b".repeat(64),
                client_order_id: "b".repeat(32),
                deadline: at + 30_000,
                plan_hash: "c".repeat(64),
                policy_revision: revision,
                risk_increase: true,
                instrument_id: "BTC-USDT-SWAP".into(),
                plan: IntentPlan {
                    strategy_definition_id: definition.definition_id,
                    strategy_definition_version: definition.version,
                    strategy_parameters_digest: definition.parameters_digest,
                    operation: "open".into(),
                    side: "buy".into(),
                    position_side: "long".into(),
                    quantity: Decimal::ONE,
                    limit_price: Decimal::from(5),
                    stop_price: Some(Decimal::from(4)),
                    strategy_instance: "manual".into(),
                    planned_loss_usd: Decimal::ONE,
                    source_generation: 1,
                    private_source_generation: 1,
                    contract_value: Decimal::ONE,
                    inverse: false,
                    settlement_currency: "USDT".into(),
                },
            },
        )
        .await
        .unwrap();
    let store = fixture._account.execution.store.clone();
    let risk = NativeRiskClient::local(NativeFinanceRisk::from_owner(fixture.owner.clone()));
    let governance = RwLock::new(Some(fixture.authority.clone()));
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let mut demo = account.clone();
    demo.environment = Environment::Demo;
    assert_eq!(
        settle_incremental_entry(&demo, &intent, None, &store, &risk, &governance, deadline)
            .await
            .unwrap(),
        EntrySettlementOutcome::NotApplicable
    );
    let missing = settle_incremental_entry(
        &account,
        &intent,
        None,
        &store,
        &risk,
        &governance,
        deadline,
    )
    .await
    .unwrap_err();
    assert_eq!(
        missing.reason_code,
        "PARTIAL_ENTRY_ORDER_READBACK_UNAVAILABLE"
    );
    assert_eq!(missing.stage, EntrySettlementStage::VenueVerification);
    assert_eq!(missing.kind, SettlementFailureKind::Failed);
    assert_eq!(missing.intent_id, intent.intent_id);
    assert_eq!(missing.intent_revision, intent.revision);
    let alien = TradeOrderDetails {
        instrument_id: intent.instrument_id.clone(),
        order_id: "foreign-order".into(),
        client_order_id: "foreign-client".into(),
        side: "buy".into(),
        position_side: "long".into(),
        trade_mode: "cross".into(),
        order_type: "limit".into(),
        price: "5".into(),
        size: "1".into(),
        accumulated_fill_size: "0.5".into(),
        average_fill_price: "5".into(),
        state: "partially_filled".into(),
        creation_time_ms: at.to_string(),
        update_time_ms: at.to_string(),
    };
    let mismatch = settle_incremental_entry(
        &account,
        &intent,
        Some(&alien),
        &store,
        &risk,
        &governance,
        deadline,
    )
    .await
    .unwrap_err();
    assert_eq!(mismatch.reason_code, "ORDER_READBACK_IDENTITY_MISMATCH");
    assert_eq!(mismatch.stage, EntrySettlementStage::Identity);
    assert_eq!(mismatch.kind, SettlementFailureKind::Conflict);
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before
    );
    assert!(
        fixture
            .owner
            .operation(
                deadline(),
                key,
                "entry-partial-settled:entry_boundary_intent:r1".into()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    drop(store);
    drop(risk);
    fixture.finish().await;
}
