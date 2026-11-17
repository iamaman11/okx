//! Explicit measurement of the real account-scoped SQL diagnostic read.
use super::*;

#[tokio::test]
async fn risk_book_native_diagnostic_measurement_first_and_100_warm_reads() {
    let fixture = fixture().await;
    let account = fixture._account.clone();
    let capture = account.recorded_capture_gate.write().await;
    let key = fixture.config.account_key.wire();
    let deadline = || Instant::now() + Duration::from_secs(2);
    let definition = fixture
        .owner
        .ensure_manual_instance(deadline(), key.clone())
        .await
        .unwrap();
    let at = chrono::Utc::now().timestamp_millis();
    let id = "native_diagnostic_measurement_001";
    fixture
        .owner
        .prepare_admission(
            deadline(),
            key.clone(),
            Decision {
                decision_id: "measurement_decision_001".into(),
                instance_id: None,
                decision_at: at,
                input_digest: "measurement_input_001".into(),
                policy_revision: 1,
                code_version: "test".into(),
            },
            NewIntent {
                intent_id: id.into(),
                idempotency_key: "measurement_request_001".into(),
                request_hash: "a".repeat(64),
                client_order_id: "measurement_client_001".into(),
                deadline: at + 60_000,
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
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();

    // First diagnostic read on this already-initialized owner and prepared SQL
    // fixture; this does not claim a cold OS filesystem or database cache.
    let mut samples = Vec::with_capacity(101);
    for _ in 0..101 {
        let started = Instant::now();
        let result = read(&account, fixture.owner.installation_id(), &reference).await;
        samples.push(started.elapsed().as_micros() as u64);
        assert_eq!(result["reference"], reference);
        assert_eq!(result["account"], key);
        assert_eq!(result["installation_id"], fixture.owner.installation_id());
        assert_eq!(result["intent"]["intent_id"], id);
        assert_eq!(result["intent"]["plan_hash"], "b".repeat(64));
        assert_eq!(result["state"], "PREPARED", "{result}");
        assert_eq!(result["reason_code"], "DURABLE_INTENT_STATE_OBSERVED");
        assert_eq!(result["read_only"], true);
        assert_eq!(result["network_calls"], 0);
        assert_eq!(result["send_permission"], "NOT_GRANTED");
    }
    let first_us = samples.remove(0);
    samples.sort_unstable();
    eprintln!(
        "{}",
        json!({
            "measurement": "risk_book_native_diagnostic_measurement_first_and_100_warm_reads",
            "fixture": "prepared intent; initialized owner; first diagnostic read; OS and SQL caches uncontrolled",
            "unit": "microseconds", "first_read": first_us, "warm_count": samples.len(),
            "warm_p50": samples[49], "warm_p95": samples[94], "warm_max": samples[99],
            "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism().map(|n| n.get()).ok(),
            "processor": std::env::var("PROCESSOR_IDENTIFIER").ok(),
            "debug_assertions": cfg!(debug_assertions)
        })
    );
    assert_eq!(fixture.feed.requests.lock().unwrap().as_slice(), requests);
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().as_slice(),
        posts
    );
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
    fixture.finish().await;
}
