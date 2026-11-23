//! The shipping committed-claim sender and original Native outcome writer.
//! Typed local reduction setup grants no central campaign capacity; no old
//! AccountStore worker, raw SQL callback or alternate sender is involved.
use super::*;
use crate::accounts::native_trade_contracts::{
    filled_fixture::opened_market,
    fixture::{NativeFixture, fixture},
};
use okx_trade_execution::{
    CentralReceipt, CommittedVenueSend, Decision, IntentPlan, NewIntent, TradeIntentLookup,
};
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

async fn live_fixture() -> NativeFixture {
    let mut f = fixture().await;
    let (stop, task, _socket, _public) = opened_market(&mut f).await;
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    f
}

async fn reserve_reduction(f: &NativeFixture, id: &str, expired: bool) -> StoredIntent {
    let account = f.config.account_key.wire();
    let definition = f
        .owner
        .ensure_manual_instance(deadline(), account.clone())
        .await
        .unwrap();
    let revision = i64::try_from(f.authority.state.revision).unwrap();
    let at = now() - if expired { 2_000 } else { 0 };
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "close".into(),
        side: "sell".into(),
        position_side: "long".into(),
        quantity: Decimal::new(1, 1),
        limit_price: Decimal::from(1000),
        stop_price: None,
        strategy_instance: "manual".into(),
        planned_loss_usd: Decimal::ZERO,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::new(1, 2),
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let plan_hash = crate::trade_service::digest(&plan).unwrap();
    let intent = f
        .owner
        .prepare_admission(
            deadline(),
            account.clone(),
            Decision {
                decision_id: format!("decision-{id}"),
                instance_id: Some("manual".into()),
                decision_at: at,
                input_digest: "b".repeat(64),
                policy_revision: revision,
                code_version: "a".repeat(40),
            },
            NewIntent {
                intent_id: id.into(),
                idempotency_key: format!("command-{id}"),
                request_hash: "b".repeat(64),
                client_order_id: format!("client{id}"),
                deadline: at + if expired { 1_000 } else { 30_000 },
                plan_hash: plan_hash.clone(),
                policy_revision: revision,
                risk_increase: false,
                instrument_id: "BTC-USDT-SWAP".into(),
                plan,
            },
        )
        .await
        .unwrap();
    f.owner
        .reserve_local_reduction(
            deadline(),
            account.clone(),
            CentralReceipt {
                reservation_key: format!("local:{id}"),
                account_key: account.clone(),
                installation_id: f.owner.installation_id().into(),
                instance_id: Some("manual".into()),
                intent_id: id.into(),
                plan_hash: plan_hash.clone(),
                policy_revision: revision,
                receipt_hash: crate::trade_service::digest(&intent).unwrap(),
                requested_loss_usd: Decimal::ZERO,
                conversion_digest: "c".repeat(64),
            },
        )
        .await
        .unwrap();
    // Reserve changes the durable state/revision. Subsequent no-send equality
    // checks must compare the committed reservation, not the prepared image.
    f.owner
        .lookup_intent(account, TradeIntentLookup::IntentId(id.into()), deadline())
        .await
        .unwrap()
        .unwrap()
}

async fn claim_reduction(f: &NativeFixture, id: &str) -> (StoredIntent, CommittedVenueSend) {
    let intent = reserve_reduction(f, id, false).await;
    let claim = f
        .owner
        .claim_local_reduction_send(
            deadline(),
            f.config.account_key.wire(),
            id.into(),
            intent.plan_hash.clone(),
            intent.policy_revision,
        )
        .await
        .unwrap()
        .expect("first original durable Native claim");
    (intent, claim)
}

async fn assert_unsent(f: &NativeFixture, intent: &StoredIntent) {
    let restored = f
        .owner
        .lookup_intent(
            f.config.account_key.wire(),
            TradeIntentLookup::IntentId(intent.intent_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.state, IntentState::Rejected);
    assert!(restored.exchange_order_id.is_none());
    assert_eq!(restored.plan_hash, intent.plan_hash);
    assert_eq!(restored.request_hash, intent.request_hash);
    assert!(
        f.owner
            .claim_local_reduction_send(
                deadline(),
                f.config.account_key.wire(),
                restored.intent_id.clone(),
                restored.plan_hash.clone(),
                restored.policy_revision
            )
            .await
            .unwrap()
            .is_none(),
        "durable readback cannot mint a second sender"
    );
}

#[tokio::test]
async fn expired_committed_claim_never_invokes_sender_or_creates_an_unknown_outcome() {
    let f = live_fixture().await;
    let capture = f._account.recorded_capture_gate.write().await;
    let (intent, claim) = claim_reduction(&f, "nativeexpiredclaim").await;
    let budget = f.owner.campaign_summary(f.campaign.clone()).await.unwrap();
    let requests = f.feed.requests.lock().unwrap().clone();
    let calls = AtomicUsize::new(0);
    let result = post_committed_native(
        &f.owner,
        f.owner.installation_id(),
        &f.config.account_key.wire(),
        &intent,
        claim,
        Instant::now() - Duration::from_millis(1),
        || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(())
        },
    )
    .await;
    assert_eq!(result, Err("COMMAND_EXPIRED".into()));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_unsent(&f, &intent).await;
    assert_eq!(
        f.owner.campaign_summary(f.campaign.clone()).await.unwrap(),
        budget
    );
    assert_eq!(*f.feed.requests.lock().unwrap(), requests);
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture);
    f.finish().await;
}

#[tokio::test]
async fn deadline_expires_after_commit_before_gateway_send_preserves_durable_no_send() {
    let f = live_fixture().await;
    let capture = f._account.recorded_capture_gate.write().await;
    let (intent, claim) = claim_reduction(&f, "nativedeadlinesend").await;
    let budget = f.owner.campaign_summary(f.campaign.clone()).await.unwrap();
    let requests = f.feed.requests.lock().unwrap().clone();
    let posts = AtomicUsize::new(0);
    let until = Instant::now() + Duration::from_millis(20);
    let result = post_committed_native(
        &f.owner,
        f.owner.installation_id(),
        &f.config.account_key.wire(),
        &intent,
        claim,
        until,
        || async {
            tokio::time::sleep_until(until.into()).await;
            if Instant::now() >= until {
                return Err("COMMAND_EXPIRED_BEFORE_SEND".into());
            }
            posts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, String>(())
        },
    )
    .await;
    assert_eq!(result, Err("COMMAND_EXPIRED_BEFORE_SEND".into()));
    assert_eq!(posts.load(Ordering::SeqCst), 0);
    assert_unsent(&f, &intent).await;
    assert_eq!(
        f.owner.campaign_summary(f.campaign.clone()).await.unwrap(),
        budget
    );
    assert_eq!(*f.feed.requests.lock().unwrap(), requests);
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture);
    f.finish().await;
}

#[tokio::test]
async fn persisted_expired_command_cannot_acquire_a_later_send_claim() {
    let f = live_fixture().await;
    let capture = f._account.recorded_capture_gate.write().await;
    let intent = reserve_reduction(&f, "nativeexpiredcommand", true).await;
    let budget = f.owner.campaign_summary(f.campaign.clone()).await.unwrap();
    let requests = f.feed.requests.lock().unwrap().clone();
    for _ in 0..2 {
        let result = f
            .owner
            .claim_local_reduction_send(
                deadline(),
                f.config.account_key.wire(),
                intent.intent_id.clone(),
                intent.plan_hash.clone(),
                intent.policy_revision,
            )
            .await;
        assert!(matches!(
            result,
            Err(okx_trade_execution::StoreError::Invalid("COMMAND_EXPIRED"))
        ));
        let snapshot = f
            .owner
            .inspect_intent_operation(
                deadline(),
                f.config.account_key.wire(),
                intent.intent_id.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(snapshot.intent).unwrap(),
            serde_json::to_value(Some(&intent)).unwrap()
        );
        assert!(snapshot.begin_post_at_ms.is_none());
        assert_eq!(
            f.owner.campaign_summary(f.campaign.clone()).await.unwrap(),
            budget
        );
    }
    assert_eq!(*f.feed.requests.lock().unwrap(), requests);
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture);
    f.finish().await;
}
