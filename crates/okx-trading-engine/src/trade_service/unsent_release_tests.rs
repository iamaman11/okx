//! Current no-send release guards on the original installation owner.
//! Economic release/replay/cold-reopen assertions live in Native
//! changed_source and reserve_recovery contracts. Remote Inspect/CAS retries
//! and a second account acknowledgement are retired with the second owner.
use super::*;
use crate::accounts::native_trade_contracts::fixture::{NativeFixture, fixture};
use okx_trade_execution::{Decision, IntentPlan, NewIntent};
use rust_decimal::Decimal;
use serde_json::json;
use std::time::Duration;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn expired() -> Instant {
    Instant::now() - Duration::from_secs(1)
}

// Typed preparation establishes identity only. It cannot create an opening,
// reservation or permission; these tests stop before financial execution.
async fn prepared() -> (NativeFixture, StoredIntent) {
    let f = fixture().await;
    let account = f.config.account_key.wire();
    let definition = f
        .owner
        .ensure_manual_instance(deadline(), account.clone())
        .await
        .unwrap();
    let intent = f
        .owner
        .prepare_admission(
            deadline(),
            account,
            Decision {
                decision_id: "native_unsent_guard_decision".into(),
                instance_id: None,
                decision_at: jiff::Timestamp::now().as_millisecond(),
                input_digest: "b".repeat(64),
                policy_revision: 1,
                code_version: "a".repeat(40),
            },
            NewIntent {
                intent_id: "native_unsent_guard_intent".into(),
                idempotency_key: "native_unsent_guard_command".into(),
                request_hash: "b".repeat(64),
                client_order_id: "nativeunsentguard001".into(),
                deadline: jiff::Timestamp::now().as_millisecond() + 30_000,
                plan_hash: "d".repeat(64),
                policy_revision: 1,
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
    (f, intent)
}

#[tokio::test]
async fn ur02_authority_failure_keeps_known_identity_and_reason() {
    let (f, intent) = prepared().await;
    let capture = f._account.recorded_capture_gate.write().await;
    let before = f.owner.campaign_summary(f.campaign.clone()).await.unwrap();
    let requests = f.feed.requests.lock().unwrap().clone();
    for foreign in [false, true] {
        let mut authority = f.authority.clone();
        authority.state.installation_id = "another-installation".into();
        let governance = RwLock::new(if foreign { Some(authority) } else { None });
        let jobs = f.owner.sql_timing().observed_jobs;
        let error = release_unsent(
            Environment::Real,
            &intent,
            UnsentReleaseDependencies {
                store: &f._account.execution.store,
                governance: &governance,
                sql_deadline: deadline,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            serde_json::to_value(&error).unwrap(),
            json!({
                "stage":"authority", "intent_id":intent.intent_id,
                "reservation_key":null,
                "reason_code":if foreign { "NATIVE_UNSENT_AUTHORITY_MISMATCH" } else { "AUTHORITY_UNAVAILABLE" },
            })
        );
        assert_eq!(
            f.owner.sql_timing().observed_jobs,
            jobs,
            "authority refusal cannot enter the financial owner"
        );
        assert_eq!(
            f.owner.campaign_summary(f.campaign.clone()).await.unwrap(),
            before
        );
        assert_eq!(*f.feed.requests.lock().unwrap(), requests);
        assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    }
    drop(capture);
    f.finish().await;
}

#[tokio::test]
async fn ur09_demo_release_is_inapplicable_before_authority_or_sql() {
    let (f, intent) = prepared().await;
    let capture = f._account.recorded_capture_gate.write().await;
    let governance = RwLock::new(None);
    let before = f.owner.campaign_summary(f.campaign.clone()).await.unwrap();
    let requests = f.feed.requests.lock().unwrap().clone();
    let jobs = f.owner.sql_timing().observed_jobs;
    assert_eq!(
        release_unsent(
            Environment::Demo,
            &intent,
            UnsentReleaseDependencies {
                store: &f._account.execution.store,
                governance: &governance,
                sql_deadline: expired,
            }
        )
        .await
        .unwrap(),
        UnsentReleaseOutcome::NotApplicable
    );
    assert_eq!(f.owner.sql_timing().observed_jobs, jobs);
    assert_eq!(
        f.owner.campaign_summary(f.campaign.clone()).await.unwrap(),
        before
    );
    assert_eq!(*f.feed.requests.lock().unwrap(), requests);
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture);
    f.finish().await;
}
