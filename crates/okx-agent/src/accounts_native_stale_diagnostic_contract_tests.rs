//! A stale authenticated tariff is rejected by the actual post-HELD producer;
//! the durable read-only explanation neither queries the venue nor retries it.
use super::filled_fixture::opened_market;
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};

#[tokio::test]
async fn risk_book_contract_native_stale_post_held_tariff_diagnostic_is_exact_and_read_only() {
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
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    record_fee(&fixture.feed, &at);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &at);
    let trade = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let alias = fixture.config.alias.clone();
    let mut pending = tokio::spawn(async move {
        let mut request = execute();
        if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
            *expires_in_ms = 10_000;
        }
        trade.trade(&alias, &request, Some(&authority)).await
    });
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::select! {
            _ = gate.entered.notified() => {},
            outcome = &mut pending => {
                let facts = fixture.owner.account_diagnostic_snapshot(Instant::now() + Duration::from_secs(3), fixture.config.account_key.wire(), None).await;
                panic!("Execute returned before post-HELD stale-source check: outcome={outcome:?}; current_account_snapshot={facts:?}");
            }
        }
    })
        .await
        .unwrap();
    let deadline = || Instant::now() + Duration::from_secs(3);
    let account = fixture.config.account_key.wire();
    let held = fixture
        .owner
        .lookup_intent(
            account.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.state, IntentState::Reserved);
    let original_receipt = fixture
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
    let original_source = fixture
        .owner
        .entry_cost_evidence(deadline(), account.clone(), held.intent_id.clone())
        .await
        .unwrap();
    let original_budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(original_budget.reserved_entry_usd, "1.0005");
    // This is the deliberately stale external fact itself, not a fabricated
    // current timestamp, changed SQL row or an elapsed-time sleep.
    let stale_exchange_at =
        (jiff::Timestamp::now().as_millisecond() - crate::config::ACCOUNT_MAX_AGE_MS - 1)
            .to_string();
    record_fee(&fixture.feed, &stale_exchange_at);
    gate.release.notify_one();
    let rejected = tokio::time::timeout(Duration::from_secs(8), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected["data"]["primary_reason"], "ENTRY_FEE_EXCHANGE_TIME_STALE",
        "{rejected}"
    );
    assert_eq!(rejected["data"]["risk_release_state"], "RELEASED");
    let account_resources = fixture._account.clone();
    let capture = account_resources.recorded_capture_gate.write().await;
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let released_budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let before = fixture
        .owner
        .lookup_intent(
            account.clone(),
            TradeIntentLookup::IntentId(held.intent_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let explanation = crate::diagnostics::native_intent::read(
        &account_resources,
        fixture.owner.installation_id(),
        &format!("intent:{}", held.intent_id),
    )
    .await;
    assert_eq!(explanation["state"], "REJECTED");
    let failure = &explanation["entry_source_failure"];
    assert_eq!(failure["state"], "STORED_FIRST_FAILURE");
    assert_eq!(failure["fact"]["reason"], "ENTRY_FEE_EXCHANGE_TIME_STALE");
    assert_eq!(failure["fact"]["source_kind"], "fee_exchange");
    assert_eq!(
        failure["fact"]["source_at_ms"],
        stale_exchange_at.parse::<i64>().unwrap()
    );
    assert!(
        failure["actual_age_ms"].as_i64().unwrap()
            > failure["required_max_age_ms"].as_i64().unwrap()
    );
    assert_eq!(failure["fact"]["plan_hash"], held.plan_hash);
    assert_eq!(failure["quality"], "HISTORICAL_NOT_CURRENT_HEALTH");
    let failure_id = crate::execution_store::OperationId::new(format!(
        "entry-source-failure:{}",
        held.intent_id
    ))
    .unwrap();
    let stored_failure = account_resources
        .execution
        .store
        .operation(deadline(), failure_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(
        account_resources
            .execution
            .store
            .operation_receipt(
                deadline(),
                failure_id.clone(),
                held.plan_hash.clone(),
                "ENTRY_SOURCE_FAILURE".into(),
                "{}".into()
            )
            .await
            .is_err()
    );
    let retained_failure = account_resources
        .execution
        .store
        .operation(deadline(), failure_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_failure.result_ref, retained_failure.result_ref);
    assert_eq!(stored_failure.committed_at, retained_failure.committed_at);
    assert_eq!(explanation["owner"], "execution.unified_finance");
    assert_eq!(explanation["stage"], "REJECTED");
    assert_eq!(explanation["reason_code"], "ENTRY_FEE_EXCHANGE_TIME_STALE");
    assert_eq!(
        explanation["reference"],
        format!("intent:{}", held.intent_id)
    );
    assert_eq!(
        explanation["recorded_incident"],
        "ENTRY_FEE_EXCHANGE_TIME_STALE"
    );
    assert_eq!(
        explanation["recorded_incident_quality"],
        "DURABLE_INTENT_INCIDENT_NOT_VENUE_CONFIRMATION"
    );
    assert_eq!(explanation["intent"]["intent_id"], held.intent_id);
    assert_eq!(explanation["intent"]["plan_hash"], held.plan_hash);
    let quote = original_source.0.as_ref().unwrap();
    assert_eq!(
        explanation["entry_source"]["state"],
        "STORED_ORIGINAL_QUOTE"
    );
    assert_eq!(
        explanation["entry_source"]["quote"],
        serde_json::to_value(quote).unwrap()
    );
    assert_eq!(
        explanation["entry_source"]["quote"]["plan_hash"],
        held.plan_hash
    );
    assert_eq!(
        explanation["entry_source"]["quote"]["fee_generation"],
        quote.fee_generation
    );
    assert_eq!(
        explanation["entry_source"]["quote"]["reference_generation"],
        quote.reference_generation
    );
    assert_eq!(
        explanation["entry_source"]["quote"]["fee_exchange_at"],
        quote.fee_exchange_at
    );
    assert_eq!(
        explanation["entry_source"]["quote"]["fee_received_at"],
        quote.fee_received_at
    );
    assert_eq!(
        explanation["entry_source"]["quote"]["conversion_received_at"],
        quote.conversion_received_at
    );
    assert_eq!(explanation["entry_source"]["failure_check"], "UNKNOWN");
    assert_eq!(
        explanation["entry_source"]["failure_source"],
        "NOT_RECORDED_IN_QUOTE"
    );
    assert_eq!(
        explanation["entry_source"]["current_validity"],
        "NOT_EVALUATED"
    );
    assert_eq!(
        explanation["durable_receipt"]["receipt_hash"],
        original_receipt.receipt_hash
    );
    assert_eq!(explanation["unsent_proof"]["state"], "DURABLE_SQL_NO_SEND");
    assert_eq!(
        explanation["unsent_proof"]["reason_code"],
        "ENTRY_FEE_EXCHANGE_TIME_STALE"
    );
    assert_eq!(explanation["send_state"], "NOT_STARTED_IN_SQL");
    assert_eq!(explanation["reservation_state"], "RELEASED");
    assert_eq!(explanation["source_as_of"]["state"], "STORED_SQL_SOURCE");
    assert_eq!(
        explanation["source_as_of"]["quality"],
        "durable_historical_source_not_current_health"
    );
    assert_eq!(
        explanation["source_as_of"]["current_validity"],
        "NOT_EVALUATED"
    );
    assert_eq!(explanation["network_calls"], 0);
    assert_eq!(explanation["send_permission"], "NOT_GRANTED");
    assert_eq!(explanation["read_only"], true);
    assert_eq!(
        explanation["missing_evidence"],
        json!([
            "FRESH_AUTHENTICATED_INSTRUMENT_TARIFF",
            "NEW_PLAN_AND_ADMISSION_REQUIRED"
        ])
    );
    assert_eq!(
        explanation["next_action"],
        "capture_fresh_tariff_then_create_new_plan_do_not_reuse_original_permission"
    );
    assert_eq!(
        fixture
            .owner
            .entry_cost_evidence(deadline(), account.clone(), held.intent_id.clone())
            .await
            .unwrap(),
        original_source
    );
    let after = fixture
        .owner
        .lookup_intent(
            account,
            TradeIntentLookup::IntentId(held.intent_id),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        released_budget
    );
    assert_eq!(released_budget.reserved_entry_usd, "0");
    assert_eq!(released_budget.reserved_loss_usd, "0");
    assert_eq!(released_budget.remaining_entry_usd, "10");
    assert_eq!(released_budget.opening, original_budget.opening);
    assert_eq!(*fixture.feed.requests.lock().unwrap(), requests);
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    drop(capture);
    drop(account_resources);
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
}
