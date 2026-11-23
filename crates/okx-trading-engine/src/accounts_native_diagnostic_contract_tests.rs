//! Current durable intent facts, through the same helper as operation routing.
use super::*;
use crate::diagnostics::native_intent::read;
use okx_trade_execution::{Decision, IntentPlan, NewIntent};
use rust_decimal::Decimal;

#[tokio::test]
async fn risk_book_contract_native_unknown_intent_diagnostic_reads_sql_without_network_or_resend() {
    let mut fixture = fixture().await;
    let (stop, task, _socket, _public) = super::filled_fixture::opened_market(&mut fixture).await;
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(), path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":"BTC-USDT-SWAP","tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response:envelope(json!([{"ordId":"native-diagnostic-unknown-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    fixture.feed.arm_lost_next_create_ack();
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let first = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(first["status"], "unknown", "{first}");
    let account = fixture._account.clone();
    let capture = account.recorded_capture_gate.write().await;
    let key = fixture.config.account_key.wire();
    let id = first["intent_id"].as_str().unwrap();
    let reference = format!("intent:{id}");
    let deadline = || Instant::now() + Duration::from_secs(2);
    let before = fixture
        .owner
        .recovery_position_owner(deadline(), key.clone(), id.into())
        .await
        .unwrap();
    let budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let sends = fixture.feed.outbound_posts.lock().unwrap().clone();
    assert!(
        matches!(
            fixture
                .owner
                .reject_and_release_unsent(
                    deadline(),
                    key.clone(),
                    id.into(),
                    before.1.as_ref().unwrap().plan_hash.clone(),
                    "POST_REPLY_LOST".into()
                )
                .await,
            Err(okx_trade_execution::StoreError::Invalid(
                "POST_OUTCOME_REQUIRES_RECONCILIATION"
            ))
        ),
        "a possibly sent order must never acquire an unsent proof"
    );
    let result = read(&account, fixture.owner.installation_id(), &reference).await;
    assert_eq!(result["state"], "UNKNOWN", "{result}");
    assert_eq!(result["stage"], "UNKNOWN");
    assert_eq!(result["reference"], reference);
    assert_eq!(result["send_state"], "POTENTIAL_SEND_COMMITTED");
    assert!(result["begin_post_at_ms"].as_i64().is_some());
    assert_eq!(
        result["next_action"],
        "request_explicit_status_for_original_identity_do_not_resend"
    );
    assert_eq!(
        result["reason_code"],
        "AUTHORITATIVE_ORDER_READBACK_REQUIRED"
    );
    assert_eq!(result["intent"]["intent_id"], id);
    assert_eq!(
        result["intent"]["plan_hash"],
        before.1.as_ref().unwrap().plan_hash
    );
    assert_eq!(result["durable_receipt"]["state"], "STORED_SQL_RECEIPT");
    assert_eq!(result["durable_receipt"]["intent_id"], id);
    assert_eq!(result["durable_receipt"]["account_key"], key);
    assert_eq!(result["durable_receipt"]["send_permission"], "NOT_GRANTED");
    assert_eq!(result["source_as_of"]["state"], "STORED_SQL_SOURCE");
    assert!(result["source_as_of"]["watermark_ms"].as_i64().unwrap() > 0);
    assert!(result["source_as_of"]["generation"].as_i64().unwrap() > 0);
    assert_eq!(result["source_as_of"]["current_validity"], "NOT_EVALUATED");
    assert_eq!(
        result["source_as_of"]["quality"],
        "durable_historical_source_not_current_health"
    );
    assert_eq!(
        result["missing_evidence"],
        json!(["AUTHORITATIVE_VENUE_ORDER_READBACK"])
    );
    assert_eq!(result["protection"]["venue_state"], "NOT_INFERRED");
    assert_eq!(result["read_only"], true);
    assert_eq!(result["network_calls"], 0);
    assert_eq!(result["send_permission"], "NOT_GRANTED");
    assert_eq!(fixture.feed.requests.lock().unwrap().as_slice(), requests);
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().as_slice(),
        sends
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(
        serde_json::to_value(
            fixture
                .owner
                .recovery_position_owner(deadline(), key, id.into())
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        budget
    );
    drop(capture);
    drop(account);
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_intent_diagnostic_is_account_scoped_and_refuses_foreign_installation()
 {
    let fixture = fixture_for_accounts(&["a", "b"]).await;
    let a = fixture.registry.resolve("fixture-a").await.unwrap();
    let b = fixture.registry.resolve("fixture-b").await.unwrap();
    let capture_a = a.recorded_capture_gate.write().await;
    let capture_b = b.recorded_capture_gate.write().await;
    let deadline = || Instant::now() + Duration::from_secs(2);
    let key = a.configuration.account_key.wire();
    let definition = fixture
        .owner
        .ensure_manual_instance(deadline(), key.clone())
        .await
        .unwrap();
    let at = jiff::Timestamp::now().as_millisecond();
    let id = "native_diagnostic_prepared_001";
    fixture
        .owner
        .prepare_admission(
            deadline(),
            key,
            Decision {
                decision_id: "diagnostic_decision_001".into(),
                instance_id: None,
                decision_at: at,
                input_digest: "diagnostic_input_001".into(),
                policy_revision: 1,
                code_version: "test".into(),
            },
            NewIntent {
                intent_id: id.into(),
                idempotency_key: "diagnostic_request_001".into(),
                request_hash: "a".repeat(64),
                client_order_id: "diagnostic_client_001".into(),
                deadline: at + 10_000,
                plan_hash: "b".repeat(64),
                policy_revision: 1,
                risk_increase: true,
                instrument_id: "BTC-USDT-SWAP".into(),
                plan: IntentPlan {
                    strategy_definition_id: definition.definition_id,
                    strategy_definition_version: definition.version,
                    strategy_parameters_digest: definition.parameters_digest,
                    contract_value: Decimal::ONE,
                    settlement_currency: "USDT".into(),
                    position_side: "long".into(),
                    operation: "open".into(),
                    side: "buy".into(),
                    quantity: Decimal::ONE,
                    limit_price: Decimal::from(100),
                    strategy_instance: "manual".into(),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    let reference = format!("intent:{id}");
    let requests = fixture.feed.requests.lock().unwrap().clone();
    let own = read(&a, fixture.owner.installation_id(), &reference).await;
    assert_eq!(own["state"], "PREPARED", "{own}");
    assert_eq!(own["account"], a.configuration.account_key.wire());
    let other = read(&b, fixture.owner.installation_id(), &reference).await;
    assert_eq!(other["state"], "NOT_FOUND", "{other}");
    assert_eq!(other["reason_code"], "INTENT_NOT_FOUND_IN_ACCOUNT_SQL");
    assert_eq!(other["stage"], "INTENT_LOOKUP");
    assert_eq!(other["reference"], reference);
    assert!(other.get("intent").is_none());
    assert_eq!(other["send_permission"], "NOT_GRANTED");
    let foreign = read(&a, "installation_foreign", &reference).await;
    assert_eq!(foreign["state"], "NOT_EVALUATED");
    assert_eq!(foreign["reason_code"], "ACCOUNT_IDENTITY_MISMATCH");
    assert_eq!(foreign["stage"], "SCOPE_CHECK");
    assert!(foreign.get("intent").is_none());
    let invalid = read(&a, fixture.owner.installation_id(), "intent:").await;
    assert_eq!(invalid["reason_code"], "INVALID_INTENT_REFERENCE");
    assert_eq!(fixture.feed.requests.lock().unwrap().as_slice(), requests);
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture_a);
    drop(capture_b);
    drop(a);
    drop(b);
    fixture.finish().await;
}

#[path = "accounts_native_diagnostic_measurement_tests.rs"]
mod measurement;
