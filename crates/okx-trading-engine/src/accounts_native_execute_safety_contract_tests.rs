//! Public Native Execute: exact ACK classification and concurrent no-send.
//! Reuses the original fresh owner, parser, source capture and public caller.
use super::filled_fixture::opened_market;
use super::*;

fn expect_create(fixture: &NativeFixture, envelope_code: &str, ack: Value) {
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    fixture.feed.expect_current_public_clock();
    // The plan, pre-reserve quote and post-HELD quote each own their reads.
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900",
                "slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: serde_json::to_vec(&json!({"code": envelope_code, "msg":"", "data": ack}))
            .unwrap(),
    });
}

fn request() -> TradeRequest {
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    request
}

async fn original_intent(fixture: &NativeFixture) -> okx_trade_execution::StoredIntent {
    fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_trade_execution::TradeIntentLookup::IdempotencyKey(
                "native_opening_required_001".into(),
            ),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .expect("public Execute must persist its original intent")
}

#[tokio::test]
async fn risk_book_contract_native_execute_bound_rejection_preserves_code_and_reserve() {
    bound_rejection("0").await;
}

#[tokio::test]
async fn risk_book_contract_native_execute_all_failed_envelope_preserves_bound_rejection() {
    bound_rejection("1").await;
}

async fn bound_rejection(envelope_code: &str) {
    let mut fixture = fixture().await;
    let (stop, task, _socket, _public) = opened_market(&mut fixture).await;
    expect_create(
        &fixture,
        envelope_code,
        json!([{"ordId":"","clOrdId":"__REQUEST_CL_ORD_ID__",
        "sCode":"51008","sMsg":"Insufficient balance"}]),
    );
    let reply = fixture
        .trade
        .trade(&fixture.config.alias, &request(), Some(&fixture.authority))
        .await
        .expect("an exact venue rejection is a known outcome");
    assert_eq!(reply["data"]["state"], "REJECTED", "{reply}");
    assert_eq!(
        reply["data"]["exchange_code"], "51008",
        "retain the order rejection code, not the envelope's successful code: {reply}"
    );
    let saved = original_intent(&fixture).await;
    assert_eq!(saved.state, okx_trade_execution::IntentState::Rejected);
    assert!(saved.exchange_order_id.is_none());
    let budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(budget.consumed_entry_usd, "0");
    assert!(
        rust_decimal::Decimal::from_str_exact(&budget.reserved_entry_usd).unwrap()
            > rust_decimal::Decimal::ZERO,
        "the Create rejection alone must not manufacture source-backed release"
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    task.await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_execute_foreign_rejection_remains_unknown() {
    let mut fixture = fixture().await;
    let (stop, task, _socket, _public) = opened_market(&mut fixture).await;
    expect_create(
        &fixture,
        "0",
        json!([{"ordId":"","clOrdId":"__REQUEST_CL_ORD_ID__",
        "sCode":"51008","sMsg":"Insufficient balance"}]),
    );
    fixture
        .market_boundary
        .foreign_create_ack
        .store(true, Ordering::SeqCst);
    let reply = fixture
        .trade
        .trade(&fixture.config.alias, &request(), Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(reply["status"], "unknown", "{reply}");
    assert_eq!(
        reply["reason_code"], "EXCHANGE_ACK_UNVERIFIED_NO_REPOST",
        "{reply}"
    );
    let saved = original_intent(&fixture).await;
    assert_eq!(saved.state, okx_trade_execution::IntentState::Unknown);
    assert!(
        !fixture
            .market_boundary
            .foreign_create_ack
            .load(Ordering::SeqCst)
    );
    assert!(saved.exchange_order_id.is_none());
    let budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(budget.consumed_entry_usd, "0");
    assert!(
        rust_decimal::Decimal::from_str_exact(&budget.reserved_entry_usd).unwrap()
            > rust_decimal::Decimal::ZERO
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    task.await.unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_execute_100_callers_one_send_policy_cas_and_payload_conflict() {
    let mut fixture = fixture().await;
    let (stop, task, _socket, _public) = opened_market(&mut fixture).await;
    expect_create(
        &fixture,
        "0",
        json!([{"ordId":"native-concurrent-001",
        "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]),
    );
    fixture.feed.arm_lost_next_create_ack();
    let gate = Arc::new(PlanningFeeGate::default());
    *fixture.market_boundary.planning_fee_gate.lock().unwrap() = Some(gate.clone());
    let start = Arc::new(tokio::sync::Barrier::new(101));
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..100 {
        let trade = fixture.trade.clone();
        let alias = fixture.config.alias.clone();
        let authority = fixture.authority.clone();
        let start = start.clone();
        calls.spawn(async move {
            start.wait().await;
            trade.trade(&alias, &request(), Some(&authority)).await
        });
    }
    start.wait().await;
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    // The winning real planner is held before prepare/reserve. Each other
    // caller must prove local/no-send; none needs another financial GET.
    tokio::time::timeout(Duration::from_secs(3), async {
        for _ in 0..99 {
            assert_eq!(
                calls.join_next().await.unwrap().unwrap().unwrap_err(),
                "ACCOUNT_MUTATION_BUSY"
            );
        }
    })
    .await
    .expect("losers must not wait for the first venue response");
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    gate.release.notify_one();
    let winner = tokio::time::timeout(Duration::from_secs(10), calls.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(winner["status"], "unknown", "{winner}");
    assert_eq!(winner["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN");
    let saved = original_intent(&fixture).await;
    assert_eq!(winner["intent_id"], saved.intent_id);
    assert_eq!(saved.state, okx_trade_execution::IntentState::Unknown);
    let held = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    // A later policy frame may pause future risk, but cannot rewrite an
    // in-flight intent's identity, request/plan hashes, revision or deadline.
    let mut newer_policy = fixture.authority.clone();
    newer_policy.state.revision += 1;
    newer_policy.state.accounts[0].policy_revision = newer_policy.state.revision;
    newer_policy.state.accounts[0].explicit_pause = true;
    fixture
        .trade
        .update_governance(&newer_policy)
        .await
        .unwrap();
    let after_policy = original_intent(&fixture).await;
    assert_eq!(
        serde_json::to_value(&saved).unwrap(),
        serde_json::to_value(&after_policy).unwrap()
    );
    assert_eq!(
        fixture
            .trade
            .update_governance(&fixture.authority)
            .await
            .unwrap_err(),
        "POLICY_REVISION_CONFLICT"
    );
    let mut conflicting_policy = newer_policy.clone();
    conflicting_policy.state.accounts[0].explicit_pause = false;
    assert_eq!(
        fixture
            .trade
            .update_governance(&conflicting_policy)
            .await
            .unwrap_err(),
        "POLICY_REVISION_CONFLICT"
    );
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(before.reserved_entry_usd, held.reserved_entry_usd);
    assert_eq!(before.reserved_loss_usd, held.reserved_loss_usd);
    assert_eq!(before.consumed_entry_usd, held.consumed_entry_usd);
    let mut altered = serde_json::to_value(request()).unwrap();
    altered["sizing"]["contracts"] = json!("0.20");
    let altered: TradeRequest = serde_json::from_value(altered).unwrap();
    assert_eq!(
        fixture
            .trade
            .trade(&fixture.config.alias, &altered, Some(&newer_policy))
            .await
            .unwrap_err(),
        "IDEMPOTENCY_CONFLICT"
    );
    let after = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert!(fixture.feed.lost_create_ack_was_consumed());
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    task.await.unwrap();
    fixture.finish().await;
}
