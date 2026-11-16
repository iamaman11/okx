//! Two public Execute callers and their original whole-cohort census share
//! one Native financial owner. The held account's HTTP delivery is the only
//! scheduling barrier; no fixture constructs a plan, reserve or money fact.
use super::filled_fixture::opened_market;
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};
use rust_decimal::Decimal;

fn request() -> TradeRequest {
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    request
}

fn fee(feed: &RecordedAccountHttp, actor: &str, at: &str) {
    feed.expect_get(RecordedGet {
        access_key: actor.into(),
        path: "/api/v5/account/trade-fee".into(),
        required_query: vec![
            ("instType".into(), "SWAP".into()),
            ("instFamily".into(), "BTC-USDT".into()),
        ],
        raw_response: envelope(json!([{"instType":"SWAP","level":"Lv1","ts":at,
            "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]}])),
    });
}

fn leverage(feed: &RecordedAccountHttp, actor: &str) {
    feed.expect_get(RecordedGet {
        access_key: actor.into(),
        path: "/api/v5/account/leverage-info".into(),
        required_query: vec![
            ("instId".into(), "BTC-USDT-SWAP".into()),
            ("mgnMode".into(), "cross".into()),
        ],
        raw_response: envelope(json!([
            {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
            {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}])),
    });
}

#[tokio::test]
async fn risk_book_contract_native_public_cohort_denied_b_does_not_block_held_a_post() {
    let mut fixture = fixture_for_accounts(&["a", "b"]).await;
    fixture.authority.state.revision = 3;
    fixture.authority.state.campaign_entry_limit_usd = Some("1.5".into());
    for account in &mut fixture.authority.state.accounts {
        account.policy_revision = 3;
    }
    assert!(fixture.authority.valid());
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();
    let (stop, market, _socket, _public) = opened_market(&mut fixture).await;
    // A pre-reserve census, B pre-reserve census and A post-HELD census each
    // complete one inventory for each independently verified account: 3 * 2.
    fixture
        .market_boundary
        .market_updates_expected
        .store(6, Ordering::SeqCst);
    let gate = Arc::new(PlanningFeeGate::default());
    *fixture
        .market_boundary
        .planning_fee_gate_actor
        .lock()
        .unwrap() = Some("fixture-observer-a".into());
    fixture
        .market_boundary
        .planning_fee_skip
        .store(2, Ordering::SeqCst);
    *fixture.market_boundary.planning_fee_gate.lock().unwrap() = Some(gate.clone());
    let at = chrono::Utc::now().timestamp_millis().to_string();
    fee(&fixture.feed, "fixture-observer-a", &at); // A plan
    leverage(&fixture.feed, "fixture-observer-a");
    leverage(&fixture.feed, "fixture-observer-a"); // A pre-reserve quote
    fee(&fixture.feed, "fixture-observer-a", &at);
    let trade = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let pending_a =
        tokio::spawn(async move { trade.trade("fixture-a", &request(), Some(&authority)).await });
    tokio::time::timeout(Duration::from_secs(8), gate.entered.notified())
        .await
        .expect("A must reach its independent post-HELD fee source");
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
    let deadline = || Instant::now() + Duration::from_secs(3);
    let a = fixture.registry.resolve("fixture-a").await.unwrap();
    let b = fixture.registry.resolve("fixture-b").await.unwrap();
    let key_a = a.configuration.account_key.wire();
    let key_b = b.configuration.account_key.wire();
    let held_a = fixture
        .owner
        .lookup_intent(
            key_a.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held_a.state, IntentState::Reserved);
    let receipt_a = fixture
        .owner
        .reservation_receipt(
            key_a.clone(),
            held_a.intent_id.clone(),
            CentralReceiptView::Retained,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt_a.account_key, key_a);
    assert_eq!(receipt_a.plan_hash, held_a.plan_hash);
    // Golden from original venue sources: 0.1 contracts * 0.01 BTC * 1000
    // = $1 entry; fee 0.0005. Loss includes $0.1 stop distance plus $0.001
    // two fee legs and $0.001 configured gap/funding reserve, totaling0.102.
    assert_eq!(receipt_a.requested_loss_usd, Decimal::new(102, 3));
    let before_b = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(before_b.reserved_entry_usd, "1.0005");
    assert_eq!(before_b.remaining_entry_usd, "0.4995");
    assert_eq!(before_b.reserved_loss_usd, "0.102");
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());

    // B receives exactly its own two assessments. It must independently read
    // BOTH accounts and observe A's durable receipt before the shared-cap decision.
    let at = chrono::Utc::now().timestamp_millis().to_string();
    fee(&fixture.feed, "fixture-observer-b", &at);
    leverage(&fixture.feed, "fixture-observer-b");
    leverage(&fixture.feed, "fixture-observer-b");
    fee(&fixture.feed, "fixture-observer-b", &at);
    let denied_b = fixture
        .trade
        .trade("fixture-b", &request(), Some(&fixture.authority))
        .await
        .expect("B public Execute returns the original shared-cap decision");
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 4);
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before_b
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());

    // A's third observation is a finite new signed source event after B's
    // decision. The gate precedes raw delivery; it never refreshes old pages.
    let b_denied = fixture
        .owner
        .lookup_intent(
            key_b.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let reads_before_diagnostic = fixture.feed.requests.lock().unwrap().clone();
    let explanation = crate::diagnostics::native_intent::read(
        &b,
        fixture.owner.installation_id(),
        &format!("intent:{}", b_denied.intent_id),
    )
    .await;
    assert_eq!(explanation["state"], "REJECTED");
    assert_eq!(
        explanation["reason_code"],
        "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED"
    );
    assert_eq!(explanation["owner"], "execution.unified_finance");
    assert_eq!(explanation["send_state"], "NOT_STARTED_IN_SQL");
    assert_eq!(
        explanation["durable_receipt"]["state"],
        "NOT_FOUND_IN_ACCOUNT_SQL"
    );
    assert_eq!(
        fixture.feed.requests.lock().unwrap().as_slice(),
        reads_before_diagnostic
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    let at = chrono::Utc::now().timestamp_millis().to_string();
    fee(&fixture.feed, "fixture-observer-a", &at);
    leverage(&fixture.feed, "fixture-observer-a");
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":at}])));
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(), path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":"BTC-USDT-SWAP","tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: envelope(json!([{"ordId":"","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"51008","sMsg":"Insufficient balance"}])),
    });
    gate.release.notify_one();
    let result_a = tokio::time::timeout(Duration::from_secs(8), pending_a)
        .await
        .unwrap()
        .unwrap()
        .expect("A public Execute continues from its original HELD receipt");

    // A known local owner denial must be durable and definitive. Otherwise
    // B's unresolved attempt poisons A's next whole-cohort admission fence.
    assert_eq!(denied_b["status"], "completed", "{denied_b}; A={result_a}");
    assert_eq!(denied_b["data"]["state"], "REJECTED", "{denied_b}");
    assert_eq!(
        denied_b["data"]["primary_reason"], "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
        "{denied_b}"
    );
    assert_eq!(denied_b["data"]["send_state"], "NOT_STARTED", "{denied_b}");
    let stored_b = fixture
        .owner
        .lookup_intent(
            key_b.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_b.state, IntentState::Rejected);
    assert!(
        fixture
            .owner
            .declined_reserve(deadline(), key_b.clone(), stored_b.intent_id.clone())
            .await
            .unwrap()
    );
    assert!(
        fixture
            .owner
            .reservation_receipt(
                key_b,
                stored_b.intent_id,
                CentralReceiptView::Retained,
                deadline()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(result_a["data"]["exchange_code"], "51008", "{result_a}");
    assert_eq!(result_a["data"]["state"], "REJECTED", "{result_a}");
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 6);
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap()[0].0,
        "fixture-executor-a"
    );
    let retained = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(retained.reserved_entry_usd, "1.0005");
    assert_eq!(retained.remaining_entry_usd, "0.4995");
    assert_eq!(retained.consumed_entry_usd, "0");
    assert_eq!(retained.reserved_loss_usd, "0.102");
    let persisted = fixture
        .owner
        .reservation_receipt(
            key_a,
            held_a.intent_id,
            CentralReceiptView::Retained,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(persisted).unwrap(),
        serde_json::to_value(receipt_a).unwrap()
    );
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    market.await.unwrap();
    drop(a);
    drop(b);
    fixture.finish().await;
}
