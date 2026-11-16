//! Positive public B execution while one A post-HELD tariff GET is blocked.
//! A's inventory remains available to the original whole-cohort census; this
//! witnesses that single HTTP boundary, not an unavailable account or venue.
use super::filled_fixture::{INSTRUMENT, opened_market};
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};
use rust_decimal::Decimal;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
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
            ("instId".into(), INSTRUMENT.into()),
            ("mgnMode".into(), "cross".into()),
        ],
        raw_response: envelope(json!([
            {"instId":INSTRUMENT,"mgnMode":"cross","posSide":"long","lever":"2"},
            {"instId":INSTRUMENT,"mgnMode":"cross","posSide":"short","lever":"2"}])),
    });
}

#[tokio::test]
async fn risk_book_contract_native_public_b_executes_and_keeps_reserve_while_a_source_blocked() {
    let mut fixture = fixture_for_accounts(&["a", "b"]).await;
    let (stop, market, _socket, _public) = opened_market(&mut fixture).await;
    // A pre-reserve plus B pre-reserve and post-HELD each read both accounts.
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
    fee(&fixture.feed, "fixture-observer-a", &at);
    leverage(&fixture.feed, "fixture-observer-a");
    leverage(&fixture.feed, "fixture-observer-a");
    fee(&fixture.feed, "fixture-observer-a", &at);
    let trade = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let pending_a =
        tokio::spawn(async move { trade.trade("fixture-a", &request(), Some(&authority)).await });
    tokio::time::timeout(Duration::from_secs(8), gate.entered.notified())
        .await
        .expect("A must block in its real third tariff GET after HELD");
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
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
    assert!(held_a.exchange_order_id.is_none());
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
    assert_eq!(receipt_a.requested_loss_usd, Decimal::new(102, 3));
    let evidence_a = fixture
        .owner
        .entry_cost_evidence(deadline(), key_a.clone(), held_a.intent_id.clone())
        .await
        .unwrap();
    assert_eq!(
        evidence_a.0.as_ref().unwrap().total_usd,
        Decimal::new(10005, 4)
    );
    let before_b = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(before_b.entry_limit_usd, "10");
    assert_eq!(before_b.reserved_entry_usd, "1.0005");
    assert_eq!(before_b.reserved_loss_usd, "0.102");
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());

    // Finite B-only tariff/leverage pages cover all three real assessments.
    // Original public snapshots still read A in each whole-cohort census.
    let at = chrono::Utc::now().timestamp_millis().to_string();
    fee(&fixture.feed, "fixture-observer-b", &at);
    leverage(&fixture.feed, "fixture-observer-b");
    leverage(&fixture.feed, "fixture-observer-b");
    fee(&fixture.feed, "fixture-observer-b", &at);
    fee(&fixture.feed, "fixture-observer-b", &at);
    leverage(&fixture.feed, "fixture-observer-b");
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":at}])));
    fixture.feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(), path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response:envelope(json!([{"ordId":"native-fairness-b-lost-ack-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    fixture.feed.arm_lost_next_create_ack();
    let started = Instant::now();
    let result_b = tokio::time::timeout(
        Duration::from_secs(10),
        fixture
            .trade
            .trade("fixture-b", &request(), Some(&fixture.authority)),
    )
    .await
    .expect("B must finish inside its production Execute lifetime while A remains held")
    .expect("B public Execute must reach its own venue send");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(result_b["status"], "unknown", "{result_b}");
    assert_eq!(
        result_b["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{result_b}"
    );
    assert!(!pending_a.is_finished(), "A gate was never released");
    assert!(fixture.feed.lost_create_ack_was_consumed());
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 6);
    {
        let posts = fixture.feed.outbound_posts.lock().unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].0, "fixture-executor-b");
        assert_eq!(posts[0].1, "/api/v5/trade/order");
    }
    let unknown_b = fixture
        .owner
        .lookup_intent(
            key_b.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result_b["intent_id"], unknown_b.intent_id);
    assert_eq!(unknown_b.state, IntentState::Unknown);
    let receipt_b = fixture
        .owner
        .reservation_receipt(
            key_b.clone(),
            unknown_b.intent_id.clone(),
            CentralReceiptView::Retained,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt_b.account_key, key_b);
    assert_eq!(receipt_b.requested_loss_usd, Decimal::new(102, 3));
    assert_ne!(receipt_b.reservation_key, receipt_a.reservation_key);
    let both = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(both.reserved_entry_usd, "2.001");
    assert_eq!(both.remaining_entry_usd, "7.999");
    assert_eq!(both.reserved_loss_usd, "0.204");
    assert_eq!(both.global_reserved_usdt, "0.204");
    assert_eq!(both.remaining_loss_usd, "9.796");
    assert_eq!(both.consumed_entry_usd, "0");
    assert_eq!(both.consumed_loss_usd, "0");
    assert_eq!(both.opening, before_b.opening);
    let unchanged_a = fixture
        .owner
        .lookup_intent(
            key_a.clone(),
            TradeIntentLookup::IntentId(held_a.intent_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(unchanged_a).unwrap(),
        serde_json::to_value(&held_a).unwrap()
    );
    let retained_a = fixture
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
    assert_eq!(
        serde_json::to_value(retained_a).unwrap(),
        serde_json::to_value(&receipt_a).unwrap()
    );
    assert_eq!(
        fixture
            .owner
            .entry_cost_evidence(deadline(), key_a.clone(), held_a.intent_id.clone())
            .await
            .unwrap(),
        evidence_a
    );
    let (risk_a, stops_a) = fixture
        .owner
        .financial_readiness(key_a.clone(), None, deadline())
        .await
        .unwrap();
    assert!(risk_a.nav_complete);
    assert_eq!(stops_a, 0);
    assert!(a.source_capture_health.read().await.reason_code.is_none());

    // Abort and await the blocked caller before public Cancel. Nothing grants
    // A a send; the same original proof releases A alone despite B's UNKNOWN.
    pending_a.abort();
    assert!(pending_a.await.unwrap_err().is_cancelled());
    *fixture.market_boundary.market.lock().unwrap() = None;
    let timer_a = a.recorded_capture_gate.write().await;
    let timer_b = b.recorded_capture_gate.write().await;
    let reads_before = fixture.feed.requests.lock().unwrap().clone();
    let cancel = TradeRequest::Cancel {
        idempotency_key: "native_fairness_cancel_a_001".into(),
        reference: held_a.intent_id.clone(),
    };
    let cancelled = fixture
        .trade
        .trade("fixture-a", &cancel, None)
        .await
        .unwrap();
    assert_eq!(
        cancelled["data"]["primary_reason"], "USER_CANCELLED_UNSENT",
        "{cancelled}"
    );
    assert_eq!(cancelled["data"]["central_release"]["state"], "verified");
    assert_eq!(cancelled["data"]["intent"]["state"], "REJECTED");
    assert_eq!(cancelled["data"]["send_state"], "NOT_STARTED");
    let released = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(released.reserved_entry_usd, "1.0005");
    assert_eq!(released.remaining_entry_usd, "8.9995");
    assert_eq!(released.reserved_loss_usd, "0.102");
    assert_eq!(released.global_reserved_usdt, "0.102");
    assert_eq!(released.remaining_loss_usd, "9.898");
    assert_eq!(released.consumed_entry_usd, "0");
    assert_eq!(released.consumed_loss_usd, "0");
    assert_eq!(released.opening, both.opening);
    let replay = fixture
        .trade
        .trade("fixture-a", &cancel, None)
        .await
        .unwrap();
    assert_eq!(replay["data"]["central_release"]["state"], "verified");
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        released
    );
    let still_b = fixture
        .owner
        .lookup_intent(
            key_b.clone(),
            TradeIntentLookup::IntentId(unknown_b.intent_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(still_b).unwrap(),
        serde_json::to_value(unknown_b).unwrap()
    );
    let retained_b = fixture
        .owner
        .reservation_receipt(
            key_b,
            receipt_b.intent_id.clone(),
            CentralReceiptView::Retained,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(retained_b).unwrap(),
        serde_json::to_value(receipt_b).unwrap()
    );
    assert_eq!(
        fixture
            .owner
            .stopped_inspect_reservation(
                fixture.campaign.clone(),
                receipt_a.reservation_key,
                held_a.plan_hash
            )
            .await
            .unwrap()
            .unwrap()
            .state,
        "RELEASED"
    );
    assert_eq!(*fixture.feed.requests.lock().unwrap(), reads_before);
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market)
        .await
        .unwrap()
        .unwrap();
    drop(timer_a);
    drop(timer_b);
    drop(a);
    drop(b);
    fixture.finish().await;
}
