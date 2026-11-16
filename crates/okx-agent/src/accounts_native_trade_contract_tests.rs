//! Fresh Native account → signed source capture → public trading admission.
//! These are admission/source contracts, not Execute/Status/Close acceptance.
use super::*;
use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
use crate::{
    accounts::recorded_support::{RecordedAccountHttp, RecordedWsConnector, prepared_with_socket},
    native_finance_risk::NativeFinanceRisk,
    risk_bridge::NativeRiskClient,
    trade_service::TradeService,
};
use okx_api::transport::{HttpRequest, HttpSend, HttpTransport};
use okx_protocol::{account_governance::CentralGovernanceSnapshot, trading::TradeRequest};
use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[path = "accounts_native_trade_fixture.rs"]
mod fixture;
use fixture::*;

#[path = "accounts_native_filled_close_contract_tests.rs"]
mod native_filled_close;

#[tokio::test]
async fn risk_book_contract_native_capture_and_public_execute_require_original_opening() {
    let fixture = fixture().await;
    // Public diagnosis uses the same native read-only SQL owner before any
    // financial opening. A missing source is data, never a forbidden legacy call.
    let preflight = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Preflight {},
            Some(&fixture.authority),
        )
        .await
        .expect("Native public preflight must explain current facts");
    assert!(preflight.to_string().contains("ready_for_new_risk"));
    assert!(
        fixture
            .owner
            .financial_readiness(
                "foreign-account".into(),
                None,
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .is_err()
    );
    let request = execute();
    assert!(request.valid());
    let result = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await;
    assert_eq!(result.unwrap_err(), "BASELINE_RECONCILIATION_REQUIRED");
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_maintenance_installs_original_opening_before_market_admission() {
    let fixture = fixture().await;
    let (shutdown, stopped) = watch::channel(false);
    let service = fixture.trade.clone();
    let maintenance = tokio::spawn(async move { service.run(stopped).await });
    // This predicate reads the ORIGINAL owner receipt. The fresh campaign must
    // age past its production two-second history cutoff; no timestamp/cursor
    // is manufactured to bypass that boundary. The interval only bounds SQL
    // observation frequency; it neither supplies GET pages nor drives finance.
    let mut observations = tokio::time::interval(Duration::from_millis(25));
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            observations.tick().await;
            let summary = fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap();
            if summary.opening.is_some() {
                break;
            }
        }
    })
    .await
    .expect("production maintenance must install an opening from captured Native facts");
    let request = execute();
    let result = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await;
    assert_eq!(result.unwrap_err(), "PRICE_FRESHNESS_DEADLINE");
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), maintenance)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_maintenance_reads_are_scoped_and_survive_reopen() {
    let fixture = fixture().await;
    let account_key = fixture.config.account_key.wire();
    let deadline = || Instant::now() + Duration::from_secs(3);
    // The same production plan-definition operation creates the manual
    // instance. This is not a test INSERT or a second account writer.
    fixture
        .owner
        .plan_definition(deadline(), account_key.clone(), "manual".into(), true)
        .await
        .unwrap();
    let instances = fixture
        ._account
        .execution
        .store
        .strategy_instances(deadline())
        .await
        .unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_id, "manual");
    let listed = fixture
        .trade
        .strategies(
            &fixture.config.alias,
            &okx_protocol::trading::StrategyRequest::List {},
            None,
        )
        .await
        .unwrap();
    assert_eq!(listed["data"]["instances"].as_array().unwrap().len(), 1);
    assert_eq!(listed["data"]["instances"][0]["instance_id"], "manual");
    assert!(
        fixture
            ._account
            .execution
            .store
            .reconcilable_intents_page(deadline(), 10, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .owner
            .strategy_instances(deadline(), "foreign-account".into())
            .await
            .is_err()
    );
    assert!(
        fixture
            .owner
            .reconcilable_intents_page("foreign-account".into(), 10, 0, deadline())
            .await
            .is_err()
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    fixture.registry.shutdown().await;
    let root = fixture.root.clone();
    let installation = fixture.owner.installation_id().to_owned();
    let mut closed = fixture.owner.termination_signal();
    drop(fixture);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    let owner = UnifiedFinanceHandle::open_native(root.clone(), installation)
        .await
        .unwrap();
    assert!(
        owner
            .configured_campaign(deadline())
            .await
            .unwrap()
            .is_some()
    );
    let reloaded = NativeFinanceRisk::from_owner(owner.clone())
        .resolved()
        .await
        .unwrap();
    assert!(!reloaded.campaign.is_empty());
    drop(reloaded);
    let instances = owner
        .strategy_instances(deadline(), account_key.clone())
        .await
        .unwrap();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].instance_id, "manual");
    assert!(
        owner
            .reconcilable_intents_page(account_key, 10, 0, deadline())
            .await
            .unwrap()
            .is_empty()
    );
    let mut closed = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn risk_book_contract_native_public_execute_lost_ack_and_replay_send_once() {
    let mut fixture = fixture().await;
    let (maintenance_stop, stopped) = watch::channel(false);
    let service = fixture.trade.clone();
    let maintenance = tokio::spawn(async move { service.run(stopped).await });
    let mut observations = tokio::time::interval(Duration::from_millis(25));
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            observations.tick().await;
            if fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap()
                .opening
                .is_some()
            {
                break;
            }
        }
    })
    .await
    .expect("production opening receipt before raw market publication");
    maintenance_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), maintenance)
        .await
        .unwrap()
        .unwrap();

    let (market, public, market_socket) = raw_market();
    let mut updates = public.subscribe_market_updates();
    let (market_stop, stopped) = watch::channel(false);
    let market_task = tokio::spawn(market.run(stopped));
    public.demand_instrument("BTC-USDT-SWAP").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if public
                .risk_fresh_snapshot(
                    "BTC-USDT-SWAP",
                    chrono::Utc::now().timestamp_millis() as u64,
                    2_000,
                    180_000,
                    chrono::Utc::now().to_rfc3339(),
                )
                .await
                .is_ok()
            {
                break;
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .expect("original production market parser must accept raw venue frames");
    *fixture.market_boundary.market.lock().unwrap() = Some((market_socket, public.clone()));
    fixture.trade = Arc::new(TradeService::new(
        fixture.registry.clone(),
        public,
        NativeRiskClient::local(
            NativeFinanceRisk::new(
                fixture.owner.clone(),
                fixture.owner.installation_id().into(),
                fixture.campaign.clone(),
            )
            .unwrap(),
        ),
    ));
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();
    let now = chrono::Utc::now().timestamp_millis().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    // Each observation belongs to a concrete production assessment. The
    // post-HELD assessment must perform NEW signed reads; it cannot borrow
    // either earlier phase's source. There are no spare/optional responses.
    // plan.rs: fee_schedule_until → configured_leverage_until.
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    // execute.rs: pre-reserve original entry quote.
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    // revalidate_held_entry_quote: independent post-HELD assessment.
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(), path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":"BTC-USDT-SWAP","tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response:envelope(json!([{"ordId":"native-lost-ack-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    // Two named public readbacks: same-key Execute replay, then explicit Status.
    for _ in 0..2 {
        fixture.feed.expect_get(RecordedGet {
            access_key: "fixture-executor-a".into(),
            path: "/api/v5/trade/order".into(),
            required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
            raw_response: envelope(
                json!([{"instId":"BTC-USDT-SWAP","ordId":"native-lost-ack-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long","tdMode":"cross",
                "ordType":"limit","px":"1000","sz":"0.1","accFillSz":"0","avgPx":"",
                "state":"live","cTime":now,"uTime":now}]),
            ),
        });
    }
    fixture.feed.arm_lost_next_create_ack();
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let execute_started = Instant::now();
    let first = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .unwrap_or_else(|reason| {
            panic!(
                "Native public Execute boundary: elapsed_ms={} reason={reason}",
                execute_started.elapsed().as_millis(),
            )
        });
    assert_eq!(first["status"], "unknown", "{first}");
    assert_eq!(
        first["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{first}"
    );
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
    assert!(fixture.feed.lost_create_ack_was_consumed());
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    let reference = first["intent_id"].as_str().unwrap().to_owned();
    let replay = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .expect("same-key replay must reconcile original Native intent");
    assert_eq!(replay["data"]["intent"]["intent_id"], reference, "{replay}");
    let status = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: reference.clone(),
            },
            None,
        )
        .await
        .expect("public Status observes the same original intent");
    assert_eq!(status["data"]["intent"]["intent_id"], reference, "{status}");
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        1,
        "no Create replay"
    );
    let assessment_reads = fixture
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
    assert_eq!(
        assessment_reads,
        ["fee", "leverage", "leverage", "fee", "fee", "leverage"]
    );
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "all and only named observations consumed"
    );
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

#[path = "accounts_native_strategy_contract_tests.rs"]
mod native_strategies;

#[path = "accounts_native_crash_contract_tests.rs"]
mod native_crash;

#[path = "accounts_native_partial_entry_contract_tests.rs"]
mod native_partial_entry;

#[path = "accounts_native_filled_fixture.rs"]
mod filled_fixture;

#[path = "accounts_native_execute_safety_contract_tests.rs"]
mod native_execute_safety;

#[path = "accounts_native_partial_protection_contract_tests.rs"]
mod native_partial_protection;

#[path = "accounts_native_multiaccount_contract_tests.rs"]
mod native_multiaccount;

#[path = "accounts_native_multiaccount_reserve_contract_tests.rs"]
mod native_multiaccount_reserve;

#[path = "accounts_native_fee_correction_contract_tests.rs"]
mod native_fee_correction;

#[path = "accounts_native_diagnostic_contract_tests.rs"]
mod native_diagnostics;

#[path = "accounts_native_multi_close_contract_tests.rs"]
mod native_multi_close;

#[path = "accounts_native_public_cohort_contract_tests.rs"]
mod native_public_cohort;

#[path = "accounts_native_snapshot_contract_tests.rs"]
mod native_snapshot;

#[path = "accounts_native_changed_source_contract_tests.rs"]
mod native_changed_source;

#[path = "accounts_native_reserve_recovery_contract_tests.rs"]
mod native_reserve_recovery;
