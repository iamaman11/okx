//! Competing scoped reservations on the original fresh Native owner.
use super::*;
use rust_decimal::Decimal;
use std::str::FromStr;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

/// Owner-command contract: the typed plan/admission DTOs below are producer
/// inputs. The original owner binds them to its immutable sources and commits
/// both competing reservations. This is not public Execute/census acceptance.
#[tokio::test]
async fn risk_book_contract_native_two_accounts_concurrent_owner_reserves_share_entry_cap() {
    use okx_trade_execution::account_worker::{
        NativeReserveDecision, NativeReserveRequest, ReportedRiskCensus,
    };
    use okx_trade_execution::{
        CentralReceiptView, Decision, EntryAdmissionEvidence, EntryQuoteEvidence, IntentPlan,
        IntentState, NewIntent, TradeIntentLookup,
    };
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    let hash = |value: &Value| format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()));
    let mut fixture = fixture_for_accounts(&["a", "b"]).await;
    fixture.authority.state.revision = 3;
    fixture.authority.state.campaign_entry_limit_usd = Some("1.5".into());
    for control in &mut fixture.authority.state.accounts {
        control.policy_revision = 3;
    }
    assert!(fixture.authority.valid());
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();
    // Reuse the original maintenance opening, its real history cutoff, and
    // parsed public reference. No test installs an opening or rewrites time.
    let (stop, market, _socket, public) = super::filled_fixture::opened_market(&mut fixture).await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let opening = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert!(opening.opening.is_some());
    assert_eq!(opening.entry_limit_usd, "1.5");
    assert_eq!(opening.reserved_entry_usd, "0");
    let a = fixture.registry.resolve("fixture-a").await.unwrap();
    let b = fixture.registry.resolve("fixture-b").await.unwrap();
    let timer_a = a.recorded_capture_gate.write().await;
    let timer_b = b.recorded_capture_gate.write().await;
    let rules = public.instrument_rules("BTC-USDT-SWAP").await.unwrap();
    assert_eq!(rules.instrument.contract_value.as_deref(), Some("0.01"));
    let generation = public
        .risk_fresh_snapshot(
            "BTC-USDT-SWAP",
            jiff::Timestamp::now().as_millisecond() as u64,
            2000,
            180000,
            jiff::Timestamp::now().to_string(),
        )
        .await
        .unwrap()
        .generation;
    let mut requests = Vec::new();
    for (label, account) in [("a", &a), ("b", &b)] {
        let key = account.configuration.account_key.wire();
        let definition = fixture
            .owner
            .ensure_manual_instance(deadline(), key.clone())
            .await
            .unwrap();
        let at = jiff::Timestamp::now().as_millisecond();
        fixture.feed.expect_get(RecordedGet {
            access_key: format!("fixture-observer-{label}"),
            path: "/api/v5/account/trade-fee".into(),
            required_query: vec![
                ("instType".into(), "SWAP".into()),
                ("instFamily".into(), "BTC-USDT".into()),
            ],
            raw_response: envelope(json!([{"instType":"SWAP","level":"Lv1","ts":at.to_string(),
                "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]}])),
        });
        let snapshot = account.observer.snapshot_until(deadline()).await.unwrap();
        let fees = account
            .observer
            .fee_schedule_until(&rules, deadline())
            .await
            .unwrap();
        let usdt = snapshot
            .balance
            .details
            .iter()
            .find(|v| v.currency == "USDT")
            .unwrap();
        assert_eq!(Decimal::from_str(&usdt.equity).unwrap(), Decimal::from(100));
        assert_eq!(
            Decimal::from_str(usdt.equity_usd.as_deref().unwrap()).unwrap(),
            Decimal::from(100)
        );
        let conversion = hash(&json!((
            snapshot.account_uid_fingerprint,
            snapshot.account_generation,
            usdt
        )));
        let conversion_at = jiff::Timestamp::from_str(&snapshot.source_received_at)
            .unwrap()
            .as_millisecond();
        let observed_at = jiff::Timestamp::now().as_millisecond();
        let plan = IntentPlan {
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            quantity: Decimal::new(1, 1),
            contract_value: Decimal::new(1, 2),
            limit_price: Decimal::from(1000),
            stop_price: Some(Decimal::from(900)),
            planned_loss_usd: Decimal::new(1, 1),
            inverse: false,
            settlement_currency: "USDT".into(),
            position_side: "long".into(),
            private_source_generation: account.private_ws.status().await.connection_generation
                as i64,
            operation: "open".into(),
            side: "buy".into(),
            strategy_instance: "manual".into(),
            source_generation: generation as i64,
        };
        let plan_hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&plan).unwrap()));
        let intent_id = format!("native_cohort_reserve_{label}");
        let prepared = fixture
            .owner
            .prepare_admission(
                deadline(),
                key.clone(),
                Decision {
                    decision_id: format!("native_cohort_decision_{label}"),
                    instance_id: Some("manual".into()),
                    decision_at: observed_at,
                    input_digest: hash(&json!((&plan_hash, &conversion))),
                    policy_revision: 3,
                    code_version: crate::accounts::build_release().into(),
                },
                NewIntent {
                    plan,
                    intent_id: intent_id.clone(),
                    idempotency_key: "native_cohort_same_reserve".into(),
                    request_hash: hash(&json!((&key, &plan_hash))),
                    client_order_id: format!("nativecohort{label}"),
                    deadline: observed_at + 30_000,
                    plan_hash: plan_hash.clone(),
                    policy_revision: 3,
                    risk_increase: true,
                    instrument_id: "BTC-USDT-SWAP".into(),
                },
            )
            .await
            .unwrap();
        let quote = EntryQuoteEvidence {
            schema: "okx.entry-known-quote/v1".into(),
            account_key: key.clone(),
            installation_id: fixture.owner.installation_id().into(),
            intent_id: intent_id.clone(),
            plan_hash: plan_hash.clone(),
            policy_revision: 3,
            instrument_id: "BTC-USDT-SWAP".into(),
            quantity: prepared.plan.quantity,
            limit_price: prepared.plan.limit_price,
            contract_value: prepared.plan.contract_value,
            fee_rate: Decimal::new(5, 4),
            maker_rate: fees.maker_rate,
            taker_rate: fees.taker_rate,
            fee_level: fees.level,
            fee_exchange_at: fees.exchange_timestamp_ms.parse().unwrap(),
            usd_per_usdt: Decimal::ONE,
            notional_usdt: Decimal::ONE,
            fee_usdt: Decimal::new(5, 4),
            total_usdt: Decimal::new(10005, 4),
            total_usd: Decimal::new(10005, 4),
            fee_generation: fees.fee_generation,
            reference_generation: rules.reference_generation.clone(),
            fee_received_at: jiff::Timestamp::from_str(&fees.source_received_at)
                .unwrap()
                .as_millisecond(),
            conversion_digest: conversion.clone(),
            conversion_received_at: conversion_at,
            observed_at,
        };
        let quote_digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&quote).unwrap()));
        let entry_proof = hash(&json!((
            "owner-contract-entry-proof",
            &quote,
            &rules.reference_generation
        )));
        fixture
            .owner
            .bind_entry_evidence(
                deadline(),
                key.clone(),
                quote,
                Some(EntryAdmissionEvidence {
                    schema: "okx.entry-admission/v2".into(),
                    account_key: key.clone(),
                    installation_id: fixture.owner.installation_id().into(),
                    intent_id: intent_id.clone(),
                    plan_hash: plan_hash.clone(),
                    quote_digest,
                    admission_usd_per_usdt: Decimal::ONE,
                    other_entry_costs_upper_usd: Decimal::ZERO,
                    entry_upper_usd: Decimal::new(10005, 4),
                    entry_proof_digest: entry_proof.clone(),
                    observed_at,
                }),
            )
            .await
            .unwrap();
        let request = NativeReserveRequest {
            venue: "okx".into(),
            uid: account.configuration.account_key.verified_uid.clone(),
            run_id: okx_trade_execution::account_worker::AdmissionRunId::new(
                "continuous_engine".into(),
            )
            .unwrap(),
            intent_id: okx_trade_execution::account_worker::AdmissionIntentId::new(
                intent_id.clone(),
            )
            .unwrap(),
            strategy_id: "manual".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            plan_sha256: plan_hash,
            entry_proof_digest: entry_proof,
            conversion_digest: conversion,
            conversion_rate: "1".into(),
            loss_usdt: okx_trade_execution::risk::UsdtLoss::parse("0.1").unwrap(),
            loss_usd: okx_trade_execution::risk::UsdLoss::parse("0.1").unwrap(),
            entry_upper_usd: okx_trade_execution::risk::UsdEntryUpper::parse("1.0005").unwrap(),
            native_incarnation: hash(&json!((
                fixture.owner.installation_id(),
                std::process::id()
            )))[..32]
                .into(),
            conversion_observed_at_ms: conversion_at,
            policy_revision: 3,
        };
        let reservation_key = [
            "okx",
            "real",
            request.uid.as_str(),
            request.run_id.as_str(),
            intent_id.as_str(),
        ]
        .iter()
        .map(|v| format!("{}:{v}", v.len()))
        .collect::<String>();
        fixture
            .owner
            .mark_reserve_attempt(deadline(), key, intent_id, reservation_key)
            .await
            .unwrap();
        requests.push(request);
    }
    // Fresh original capture, not caller-declared completeness. This reported
    // census exercises SQL binding; it does not claim production census coverage.
    let (captured_a, captured_b) = tokio::join!(
        crate::source_capture::capture_account_sources_measured(
            &a.observer,
            &a.private_ws,
            &a.execution
        ),
        crate::source_capture::capture_account_sources_measured(
            &b.observer,
            &b.private_ws,
            &b.execution
        ),
    );
    for (request, snapshot) in requests
        .iter_mut()
        .zip([captured_a.unwrap().snapshot, captured_b.unwrap().snapshot])
    {
        let usdt = snapshot
            .balance
            .details
            .iter()
            .find(|v| v.currency == "USDT")
            .unwrap();
        assert_eq!(
            Decimal::from_str(&usdt.equity).unwrap(),
            Decimal::from_str(usdt.equity_usd.as_deref().unwrap()).unwrap()
        );
        // Production Reserve also obtains a fresh conversion after its quote.
        // Preserve that source observation's real receipt time and bytes.
        request.conversion_digest = hash(&json!((
            snapshot.account_uid_fingerprint,
            snapshot.account_generation,
            usdt
        )));
        request.conversion_observed_at_ms = jiff::Timestamp::from_str(&snapshot.source_received_at)
            .unwrap()
            .as_millisecond();
    }
    let mut occupied = BTreeMap::new();
    let mut losses = BTreeMap::new();
    let mut sources = Vec::new();
    for account in [&a, &b] {
        let key = account.configuration.account_key.wire();
        let (risk, stops) = fixture
            .owner
            .financial_readiness(key.clone(), None, deadline())
            .await
            .unwrap();
        assert!(risk.nav_complete);
        assert_eq!(stops, 0);
        let policy = fixture
            .owner
            .account_policy(key.clone(), deadline())
            .await
            .unwrap();
        occupied.insert(key.clone(), "0".into());
        losses.insert(key.clone(), policy.campaign_loss.to_string());
        sources.push((key, risk));
    }
    let census = ReportedRiskCensus {
        source_digest: hash(&json!((&sources, &losses))),
        generated_at_ms: okx_trade_execution::account_worker::CensusObservedAtMs::new(
            jiff::Timestamp::now().as_millisecond(),
        )
        .unwrap(),
        read_floor_ms: okx_trade_execution::account_worker::CensusReadFloorMs::new(
            sources
                .iter()
                .map(|(_, risk)| risk.source_as_of)
                .min()
                .unwrap(),
        )
        .unwrap(),
        occupied,
        cumulative_loss_usd: losses,
    };
    let (first, second) = tokio::join!(
        fixture.owner.reserve_reported(
            fixture.campaign.clone(),
            requests[0].clone(),
            census.clone()
        ),
        fixture.owner.reserve_reported(
            fixture.campaign.clone(),
            requests[1].clone(),
            census.clone()
        ),
    );
    let (winner, loser, receipt) = match (first.unwrap(), second.unwrap()) {
        (NativeReserveDecision::Held { receipt, .. }, NativeReserveDecision::Denied { reason }) => {
            assert_eq!(reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
            (0, 1, receipt)
        }
        (NativeReserveDecision::Denied { reason }, NativeReserveDecision::Held { receipt, .. }) => {
            assert_eq!(reason, "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
            (1, 0, receipt)
        }
        _ => panic!("one original Held and one shared-cap Denied required"),
    };
    let winner_key = format!("okx:real:{}", requests[winner].uid);
    let loser_key = format!("okx:real:{}", requests[loser].uid);
    assert_eq!(receipt.account_key, winner_key);
    assert_eq!(receipt.intent_id, requests[winner].intent_id.as_str());
    assert_eq!(receipt.plan_hash, requests[winner].plan_sha256);
    assert_eq!(receipt.requested_loss_usd, Decimal::new(1, 1));
    assert_eq!(
        serde_json::to_value(
            fixture
                .owner
                .reservation_receipt(
                    winner_key.clone(),
                    receipt.intent_id.clone(),
                    CentralReceiptView::Retained,
                    deadline()
                )
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(Some(receipt.clone())).unwrap()
    );
    assert!(
        fixture
            .owner
            .reservation_receipt(
                loser_key.clone(),
                requests[loser].intent_id.as_str().to_owned(),
                CentralReceiptView::Retained,
                deadline()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .owner
            .reservation_receipt(
                loser_key,
                receipt.intent_id.clone(),
                CentralReceiptView::Retained,
                deadline()
            )
            .await
            .unwrap()
            .is_none()
    );
    let retained = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(retained.reserved_entry_usd, "1.0005");
    assert_eq!(retained.remaining_entry_usd, "0.4995");
    assert_eq!(retained.consumed_entry_usd, "0");
    assert_eq!(retained.reserved_loss_usd, "0.1");
    assert_eq!(retained.global_reserved_usdt, "0.1");
    let replay = fixture
        .owner
        .reserve_reported(fixture.campaign.clone(), requests[winner].clone(), census)
        .await
        .unwrap();
    let NativeReserveDecision::Held {
        receipt: repeated, ..
    } = replay
    else {
        panic!("winner replay must retain its original Held receipt")
    };
    assert_eq!(
        serde_json::to_value(repeated).unwrap(),
        serde_json::to_value(&receipt).unwrap()
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        retained
    );
    assert_eq!(
        fixture
            .owner
            .lookup_intent(
                winner_key,
                TradeIntentLookup::IntentId(receipt.intent_id),
                deadline()
            )
            .await
            .unwrap()
            .unwrap()
            .state,
        IntentState::Reserved
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    stop.send(true).unwrap();
    market.await.unwrap();
    drop(timer_a);
    drop(timer_b);
    drop(a);
    drop(b);
    fixture.finish().await;
}
