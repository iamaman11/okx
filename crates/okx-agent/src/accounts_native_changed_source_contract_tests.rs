//! A new signed tariff after HELD cannot reuse the original entry permission.
use super::filled_fixture::{INSTRUMENT, opened_market};
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};
use rust_decimal::Decimal;

#[tokio::test]
async fn risk_book_contract_native_changed_post_held_fee_retains_original_obligation_without_post()
{
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
    let initial_at = chrono::Utc::now().timestamp_millis().to_string();
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
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(
        fixture.market_boundary.updates.load(Ordering::SeqCst),
        1,
        "only the pre-reserve inventory preceded this fee read"
    );

    // A new source event is delivered only after we observed the real durable
    // reservation. No timestamps, plan, NAV, policy or stored quote are edited.
    let changed_at = chrono::Utc::now().timestamp_millis();
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
    assert_eq!(denied["status"], "unknown", "{denied}");
    assert_eq!(
        denied["reason_code"], "POST_ENTRY_FEE_OR_SPEC_CHANGED",
        "{denied}"
    );
    assert_eq!(denied["intent_id"], held.intent_id, "{denied}");
    assert_eq!(denied["retryable"], false, "{denied}");
    assert_eq!(
        serde_json::to_value(original_intent(&fixture).await).unwrap(),
        serde_json::to_value(&held).unwrap()
    );
    assert_eq!(
        fixture
            .owner
            .entry_cost_evidence(deadline(), account.clone(), held.intent_id.clone())
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        reserved
    );
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
            .unwrap(),
        held_risk
    );
    let no_proof = fixture
        .owner
        .release_unsent_status(
            deadline(),
            account.clone(),
            held.intent_id.clone(),
            held.plan_hash.clone(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        no_proof,
        okx_execution::StoreError::Invalid("DURABLE_UNSENT_SQL_PROOF_MISSING")
    ));
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        reserved
    );
    assert!(
        fixture
            .owner
            .unsent_release_receipt(deadline(), account, held.intent_id)
            .await
            .unwrap()
            .is_none()
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
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
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
