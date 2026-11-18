//! A new signed tariff after HELD cannot reuse the original entry permission.
use super::filled_fixture::{INSTRUMENT, opened_market};
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};
use rust_decimal::Decimal;

#[tokio::test]
async fn risk_book_contract_native_changed_post_held_fee_releases_unsent_and_replans() {
    let mut fixture = fixture().await;
    let (stop, market, _socket, _public) = opened_market(&mut fixture).await;
    fixture
        .market_boundary
        .market_updates_expected
        .store(2, Ordering::SeqCst);
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
    let initial_at = jiff::Timestamp::now().as_millisecond().to_string();
    record_fee(&fixture.feed, &initial_at); // actual plan fee
    record_leverage(&fixture.feed); // actual plan leverage
    record_leverage(&fixture.feed); // independently signed pre-reserve leverage
    record_fee(&fixture.feed, &initial_at); // actual frozen quote fee
    let trade = fixture.trade.clone();
    let alias = fixture.config.alias.clone();
    let authority = fixture.authority.clone();
    let pending = tokio::spawn(async move {
        let mut request = execute();
        if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
            *expires_in_ms = 10_000;
        }
        trade.trade(&alias, &request, Some(&authority)).await
    });
    tokio::time::timeout(Duration::from_secs(8), gate.entered.notified())
        .await
        .expect("the third actual fee GET must occur after original HELD");
    let deadline = || Instant::now() + Duration::from_secs(3);
    let account = fixture.config.account_key.wire();
    let held = original_intent(&fixture).await;
    assert_eq!(held.state, IntentState::Reserved);
    assert!(held.exchange_order_id.is_none());
    let receipt = fixture
        .owner
        .reservation_receipt(
            account.clone(),
            held.intent_id.clone(),
            CentralReceiptView::Retained,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.account_key, account);
    assert_eq!(receipt.plan_hash, held.plan_hash);
    assert_eq!(receipt.requested_loss_usd, Decimal::new(102, 3));
    let original = fixture
        .owner
        .entry_cost_evidence(deadline(), account.clone(), held.intent_id.clone())
        .await
        .unwrap();
    let quote = original.0.as_ref().unwrap();
    let admission = original.1.as_ref().unwrap();
    assert_eq!(quote.fee_rate, Decimal::new(5, 4));
    assert_eq!(quote.total_usd, Decimal::new(10005, 4));
    assert_eq!(admission.entry_upper_usd, Decimal::new(10005, 4));
    let reserved = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(reserved.reserved_entry_usd, "1.0005");
    assert_eq!(reserved.reserved_loss_usd, "0.102");
    assert_eq!(reserved.consumed_entry_usd, "0");
    let held_risk = fixture
        .owner
        .stopped_inspect_reservation(
            fixture.campaign.clone(),
            receipt.reservation_key.clone(),
            held.plan_hash.clone(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held_risk.state, "HELD");
    assert!(matches!(
        fixture
            .owner
            .release_unsent_status(
                deadline(),
                account.clone(),
                held.intent_id.clone(),
                held.plan_hash.clone()
            )
            .await,
        Err(okx_execution::StoreError::Invalid(
            "DURABLE_UNSENT_SQL_PROOF_MISSING"
        ))
    ));
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(
        fixture.market_boundary.updates.load(Ordering::SeqCst),
        1,
        "only the pre-reserve inventory preceded this fee read"
    );

    // A new source event is delivered only after we observed the real durable
    // reservation. No timestamps, plan, NAV, policy or stored quote are edited.
    let changed_at = jiff::Timestamp::now().as_millisecond();
    assert!(changed_at >= quote.fee_exchange_at);
    fixture.feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/trade-fee".into(),
        required_query: vec![
            ("instType".into(), "SWAP".into()),
            ("instFamily".into(), "BTC-USDT".into()),
        ],
        raw_response: envelope(
            json!([{"instType":"SWAP","level":"Lv1","ts":changed_at.to_string(),
            "feeGroup":[{"groupId":"4","maker":"0.001","taker":"0.001"}]}]),
        ),
    });
    gate.release.notify_one();
    let denied = tokio::time::timeout(Duration::from_secs(8), pending)
        .await
        .unwrap()
        .unwrap()
        .expect("a changed authenticated tariff returns the original no-POST result");
    assert_eq!(denied["data"]["risk_release_state"], "RELEASED", "{denied}");
    assert_eq!(
        denied["data"]["primary_reason"], "POST_ENTRY_FEE_OR_SPEC_CHANGED",
        "{denied}"
    );
    assert_eq!(denied["data"]["intent_id"], held.intent_id, "{denied}");
    let rejected = original_intent(&fixture).await;
    assert_eq!(rejected.state, IntentState::Rejected);
    assert_eq!(rejected.plan_hash, held.plan_hash);
    assert_eq!(rejected.request_hash, held.request_hash);
    assert!(rejected.exchange_order_id.is_none());
    let reads_before_diagnostic = fixture.feed.requests.lock().unwrap().clone();
    let explanation = crate::diagnostics::native_intent::read(
        &fixture._account,
        fixture.owner.installation_id(),
        &format!("intent:{}", held.intent_id),
    )
    .await;
    assert_eq!(explanation["reason_code"], "POST_ENTRY_FEE_OR_SPEC_CHANGED");
    assert_eq!(explanation["send_state"], "NOT_STARTED_IN_SQL");
    assert_eq!(explanation["reservation_state"], "RELEASED");
    assert_eq!(explanation["unsent_proof"]["state"], "DURABLE_SQL_NO_SEND");
    assert_eq!(
        fixture.feed.requests.lock().unwrap().as_slice(),
        reads_before_diagnostic
    );
    assert_eq!(
        fixture
            .owner
            .entry_cost_evidence(deadline(), account.clone(), held.intent_id.clone())
            .await
            .unwrap(),
        original
    );
    let released = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(released.reserved_entry_usd, "0");
    assert_eq!(released.reserved_loss_usd, "0");
    assert_eq!(released.global_reserved_usdt, "0");
    assert_eq!(released.consumed_entry_usd, "0");
    assert_eq!(released.remaining_entry_usd, "10");
    assert_eq!(released.opening, reserved.opening);
    let proof = fixture
        .owner
        .unsent_release_receipt(deadline(), account.clone(), held.intent_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(proof.reason, "POST_ENTRY_FEE_OR_SPEC_CHANGED");
    assert_eq!(proof.plan_hash, held.plan_hash);
    assert_eq!(
        fixture
            .owner
            .stopped_inspect_reservation(
                fixture.campaign.clone(),
                receipt.reservation_key,
                held.plan_hash.clone()
            )
            .await
            .unwrap()
            .unwrap()
            .state,
        "RELEASED"
    );
    // Replaying the exact resolution cannot refund another unit of capacity;
    // foreign plan binding must roll back the whole rejection/release.
    assert!(
        fixture
            .owner
            .reject_and_release_unsent(
                deadline(),
                account.clone(),
                held.intent_id.clone(),
                "f".repeat(64),
                proof.reason.clone()
            )
            .await
            .is_err()
    );
    fixture
        .owner
        .reject_and_release_unsent(
            deadline(),
            account.clone(),
            held.intent_id.clone(),
            held.plan_hash.clone(),
            proof.reason,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        released
    );
    // The changed fee fails before a post-HELD leverage read and inventory.
    // Their absence is causal; there are no unused replacement HTTP pages.
    let assessments = fixture
        .feed
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter_map(|request| {
            if request.contains("/api/v5/account/trade-fee?") {
                Some("fee")
            } else if request.contains("/api/v5/account/leverage-info?") {
                Some("leverage")
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(assessments, ["fee", "leverage", "leverage", "fee", "fee"]);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 1);
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    assert_eq!(held.instrument_id, INSTRUMENT);
    // The fixture's maintenance loop was stopped after opening. Before the
    // next independent command, perform its real capture/readiness turn over
    // a new signed flat snapshot and empty incremental history. This does not
    // reuse an old freshness timestamp or alter the rejected quote.
    let capture_owner = fixture._account.clone();
    let capture_gate = capture_owner.recorded_capture_gate.write().await;
    super::filled_fixture::snapshot(&fixture.feed, "100", "0");
    super::filled_fixture::history(&fixture.feed, None);
    super::filled_fixture::capture(&fixture).await;
    let source = fixture
        .owner
        .source_cursor(account.clone(), "bills".into(), "account".into())
        .await
        .unwrap()
        .unwrap();
    fixture
        .owner
        .refresh_campaign_entry_readiness(
            deadline(),
            account.clone(),
            fixture.campaign.clone(),
            released.campaign_started_at_ms.unwrap() as i64,
            source.watermark.min(
                jiff::Timestamp::now()
                    .as_millisecond()
                    .saturating_sub(2_000),
            ),
        )
        .await
        .expect("real maintenance before independent replan");
    // A NEW command must build a NEW immutable plan at the new real tariff.
    // The rejected plan/quote remain unchanged; no old send token is reused.
    fixture
        .market_boundary
        .market_updates_expected
        .store(4, Ordering::SeqCst);
    fixture
        .market_boundary
        .publish_market_observation(deadline())
        .await;
    // Registering a captured balance made that route a finite transcript.
    // These five new signed observations have named production consumers,
    // exactly as in public Add: plan, quote, census, conversion, post-HELD.
    super::filled_fixture::snapshot(&fixture.feed, "100", "0"); // plan
    super::filled_fixture::snapshot(&fixture.feed, "100", "0"); // quote
    super::filled_fixture::snapshot(&fixture.feed, "100", "0"); // pre-reserve census
    super::filled_fixture::snapshot(&fixture.feed, "100", "0"); // conversion
    super::filled_fixture::snapshot(&fixture.feed, "100", "0"); // post-HELD
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":at}])));
    for phase in 0..3 {
        if phase == 1 {
            record_leverage(&fixture.feed);
        }
        changed_fee(&fixture.feed, &at);
        if phase != 1 {
            record_leverage(&fixture.feed);
        }
    }
    fixture.feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response:envelope(json!([{"ordId":"native-replanned-new-fee-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    fixture.feed.arm_lost_next_create_ack();
    let mut fresh = execute();
    if let TradeRequest::Execute {
        idempotency_key,
        expires_in_ms,
        ..
    } = &mut fresh
    {
        *idempotency_key = "native_new_fee_replan_001".into();
        *expires_in_ms = 10_000;
    }
    let sent = fixture
        .trade
        .trade(&fixture.config.alias, &fresh, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        sent["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{sent}"
    );
    let next_id = sent["intent_id"].as_str().unwrap().to_owned();
    assert_ne!(next_id, held.intent_id);
    let next = fixture
        .owner
        .lookup_intent(
            account.clone(),
            TradeIntentLookup::IntentId(next_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_ne!(next.plan_hash, held.plan_hash);
    assert_ne!(next.client_order_id, held.client_order_id);
    let next_quote = fixture
        .owner
        .entry_cost_evidence(deadline(), account.clone(), next_id)
        .await
        .unwrap();
    assert_eq!(next_quote.0.unwrap().fee_rate, Decimal::new(1, 3));
    let next_budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    // notional1 + fee0.001; loss0.1 + both fee legs0.002 + gap reserve0.001.
    assert_eq!(next_budget.reserved_entry_usd, "1.001");
    assert_eq!(next_budget.reserved_loss_usd, "0.103");
    assert_eq!(next_budget.remaining_entry_usd, "8.999");
    assert_eq!(next_budget.consumed_entry_usd, "0");
    assert_eq!(original_intent(&fixture).await.state, IntentState::Rejected);
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 4);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    drop(capture_gate);
    drop(capture_owner);
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

fn changed_fee(feed: &RecordedAccountHttp, at: &str) {
    feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/trade-fee".into(),
        required_query: vec![
            ("instType".into(), "SWAP".into()),
            ("instFamily".into(), "BTC-USDT".into()),
        ],
        raw_response: envelope(json!([{"instType":"SWAP","level":"Lv1","ts":at,
            "feeGroup":[{"groupId":"4","maker":"0.001","taker":"0.001"}]}])),
    });
}

async fn original_intent(fixture: &NativeFixture) -> okx_execution::StoredIntent {
    fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap()
}
