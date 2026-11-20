//! Actual Engine task composition under a separately spawned research worker.
//! This runs Engine in the test process, so it is not an Engine executable/PID proof.
#[path = "../tests/support/process_tree.rs"]
mod process_tree;
use super::*;
use crate::{
    finance_dispatch::FinanceDispatch,
    market_bootstrap::MarketBootstrapper,
    query::DispatchContext,
    research_client::{ResearchClient, ResearchClientConfig, SourceHandler},
    runtime::{EngineContext, EngineDrainOutcome, run_engine_until},
};
use okx_protocol::research_ipc::HistoryCandle;
use okx_protocol::{
    NATIVE_CHILD_CONTROL_SCHEMA_V1, NativeChildOperation, NativeChildRequest,
    analytics::*,
    research_ipc::{PublicCandleCapture, PublicSourceReply, PublicSourceRequest, ResearchScope},
};

fn heavy_research(root: std::path::PathBuf) -> ResearchClient {
    let executable = std::path::PathBuf::from(
        std::env::var_os("OKX_RESEARCH_WORKER_TEST_EXE").expect("set OKX_RESEARCH_WORKER_TEST_EXE"),
    );
    assert!(executable.is_file());
    let source: SourceHandler = Arc::new(|request| {
        Box::pin(async move {
            let PublicSourceRequest::Candles { after, .. } = request else {
                return Err("UNEXPECTED_SOURCE".into());
            };
            let cursor = after
                .as_deref()
                .ok_or("CURSOR_MISSING")?
                .parse::<u64>()
                .map_err(|e| e.to_string())?;
            let upper = (cursor / 60_000).min(60_000);
            let lower = upper.saturating_sub(100);
            let rows = (lower..upper)
                .rev()
                .map(|i| HistoryCandle {
                    open_time_ms: (i * 60_000).to_string(),
                    open: "100".into(),
                    high: "102".into(),
                    low: "99".into(),
                    close: if i % 2 == 0 { "100" } else { "101" }.into(),
                    volume: "1".into(),
                    volume_currency: "1".into(),
                    volume_quote: None,
                    confirmed: true,
                })
                .collect();
            Ok(PublicSourceReply::Candles(PublicCandleCapture {
                rows,
                raw_body: b"synthetic closed candles".to_vec(),
                acquired_at_ms: "1".into(),
            }))
        })
    });
    ResearchClient::new(
        ResearchClientConfig {
            executable,
            root,
            scope: ResearchScope::new("engine-heavy".into()).unwrap(),
        },
        source,
    )
}
fn heavy_grid(key: &str) -> ResearchRequest {
    ResearchRequest::Grid {
        idempotency_key: key.into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 60_000 * 60_000,
        strategy_id: "ema_cross_v1".into(),
        variants: (3..67)
            .map(|slow| EmaParameters { fast: 2, slow }.into())
            .collect(),
        costs: ResearchCosts {
            maker_fee_bps: 1.,
            taker_fee_bps: 2.,
            slippage_bps: 1.,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    }
}
async fn state(client: &ResearchClient, id: &str) -> serde_json::Value {
    client
        .research(ResearchRequest::Status { job_id: id.into() })
        .await
        .unwrap()
}

fn latency_proof(mut samples: Vec<f64>, name: &str) -> serde_json::Value {
    assert_eq!(samples.len(), 100, "{name}: incomplete measurement");
    let raw_ms = samples.clone();
    samples.sort_by(f64::total_cmp);
    assert!(
        samples[94] <= 100.0,
        "{name}: p95={}ms exceeds 100ms while research competes for CPU",
        samples[94]
    );
    serde_json::json!({
        "samples":100,"p50_ms":samples[49],"p95_ms":samples[94],"max_ms":samples[99],
        "raw_ms":raw_ms,
        "slo_p95_ms":100.0,
    })
}

async fn run_phase(disconnect_before_opening: bool) -> serde_json::Value {
    let mut fixture = fixture().await;
    let (market, public, _socket) = raw_market();
    let api = okx_api::OkxPublicClient::with_http_transport(
        okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
        okx_api::RateBudget::new(),
        fixture.feed.clone(),
    )
    .unwrap();
    let bootstrap = MarketBootstrapper::new(api);
    let research = heavy_research(fixture.root.join("heavy-research"));
    let first = research.research(heavy_grid("heavy-one")).await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let second = research.research(heavy_grid("heavy-two")).await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let a = state(&research, &first).await;
            let b = state(&research, &second).await;
            assert!(
                !matches!(
                    a["state"].as_str(),
                    Some("failed" | "completed" | "cancelled")
                ),
                "first job stopped before CPU boundary: {a}"
            );
            assert!(
                !matches!(
                    b["state"].as_str(),
                    Some("failed" | "completed" | "cancelled")
                ),
                "second job stopped before CPU boundary: {b}"
            );
            if a["state"] == "running"
                && b["state"] == "running"
                && (a["candidate_progress"]["completed_work_units"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0
                    || b["candidate_progress"]["completed_work_units"]
                        .as_u64()
                        .unwrap_or(0)
                        > 0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real child did not enter grid evaluation");
    fixture.trade = Arc::new(TradeService::new(
        fixture.registry.clone(),
        public.clone(),
        NativeRiskClient::local(NativeFinanceRisk::from_owner(fixture.owner.clone())),
    ));
    fixture
        .trade
        .set_live_source(okx_runtime::live_source::LiveCandleSource::new(
            bootstrap.clone(),
        ))
        .await;
    let ctx = EngineContext {
        dispatch: DispatchContext {
            finance: FinanceDispatch::ready(fixture.registry.clone(), fixture.trade.clone()),
            research: research.clone(),
            public_ws: public,
            market: bootstrap,
        },
        public: market,
        transport: None,
        mailbox: None,
        private_key: zeroize::Zeroizing::new([0; 32]),
        key_id: "research-load-engine".into(),
        poll_seconds: 1,
        native_child_control_stdio: false,
    };
    let owner = fixture.owner.clone();
    let campaign = fixture.campaign.clone();
    let installation = owner.installation_id().to_owned();
    let trade = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let child = research.clone();
    let (evidence_tx, evidence_rx) = tokio::sync::oneshot::channel();
    let until = async move {
        if disconnect_before_opening {
            child.shutdown().await;
            assert!(
                child.diagnostic_job(&first).await.is_err(),
                "child must actually disconnect"
            );
        }
        trade.update_governance(&authority).await.unwrap();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let summary = owner.campaign_summary(campaign.clone()).await.unwrap();
                if summary.opening.is_some() {
                    assert_eq!(owner.installation_id(), installation);
                    assert_eq!(summary.reserved_entry_usd, "0");
                    assert_eq!(summary.consumed_entry_usd, "0");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("actual Engine maintenance failed to install opening");
        if disconnect_before_opening {
            let restarted = state(&child, &first).await;
            assert_eq!(restarted["id"], first);
            assert_eq!(restarted["state"], "interrupted",
                "a restarted CPU job must never be represented as successful");
        } else {
            assert_eq!(
                state(&child, &first).await["state"],
                "running",
                "first job must overlap Engine progress"
            );
            assert_eq!(
                state(&child, &second).await["state"],
                "running",
                "second job must overlap Engine progress"
            );
        }

        // The Native finance owner is the real SQLite account owner, not a
        // mock financial RPC. Observe it while both research CPU jobs are
        // running, then repeat with the Worker deliberately disconnected.
        let mut native_finance_ms = Vec::with_capacity(100);
        let mut research_explain_ms = Vec::with_capacity(100);
        for _ in 0..100 {
            let began = std::time::Instant::now();
            let summary = tokio::time::timeout(
                Duration::from_secs(2), owner.campaign_summary(campaign.clone()),
            ).await.expect("Native Finance read blocked by Research").unwrap();
            native_finance_ms.push(began.elapsed().as_secs_f64() * 1000.0);
            assert!(summary.opening.is_some(), "Engine opening was lost");
            assert_eq!(summary.reserved_entry_usd, "0");
            assert_eq!(summary.consumed_entry_usd, "0");
            assert_eq!(owner.installation_id(), installation);
            if !disconnect_before_opening {
                let began = std::time::Instant::now();
                let snapshot = tokio::time::timeout(
                    Duration::from_secs(2), child.diagnostic_job(&first),
                ).await.expect("read-only Research IPC blocked by CPU").unwrap();
                research_explain_ms.push(began.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(snapshot["job"]["id"], first);
                assert!(matches!(snapshot["job"]["state"].as_str(),
                    Some("queued" | "running" | "completed")),
                    "diagnostic must be an authentic bounded job snapshot: {snapshot}");
            }
        }
        if !disconnect_before_opening {
            // Require at least one running job to overlap the entire Native
            // Finance measurement interval, not just a startup observation.
            let a = state(&child, &first).await;
            let b = state(&child, &second).await;
            assert!(a["state"] == "running" || b["state"] == "running",
                "research completed before the native finance samples: {a} {b}");
        }
        let finance = latency_proof(native_finance_ms, "native_finance_campaign_summary");
        let explain = if disconnect_before_opening {
            serde_json::Value::Null
        } else {
            latency_proof(research_explain_ms, "research_child_explain")
        };
        evidence_tx.send(serde_json::json!({
            "schema":"okx.518.t11-t12-native-concurrent-isolation/v1",
            "phase":if disconnect_before_opening {"worker_disconnected"} else {"two_heavy_grids_running"},
            "native_finance_read_only":finance,
            "research_explain":explain,
            "native_installation_identity_preserved":true,
            "reserved_entry_usd":"0","consumed_entry_usd":"0",
            "financial_owner":"native_sqlite_original_owner",
            "synthetic_public_source":true,
            "trading_requests_sent":0,
        })).expect("native proof receiver dropped");
        Some(NativeChildRequest {
            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.into(),
            request_id: "a".repeat(64),
            operation: NativeChildOperation::Shutdown {
                drain_timeout_ms: 10000,
            },
        })
    };
    let outcome = tokio::time::timeout(Duration::from_secs(40), run_engine_until(ctx, until))
        .await
        .expect("Engine STOP deadline")
        .unwrap();
    assert_eq!(outcome, EngineDrainOutcome::Drained);
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    let proof = evidence_rx.await.expect("Native isolation proof not recorded");
    research.shutdown().await;
    fixture.finish().await;
    proof
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect() {
    let measurement = process_tree::ResearchProcessTree::begin();
    let working = run_phase(false).await;
    let disconnected = run_phase(true).await;
    let evidence = measurement.finish();
    println!("RESEARCH_PROCESS_TREE={evidence}");
    println!("T11_T12_NATIVE_ISOLATION_JSON:{}", serde_json::json!({
        "schema":"okx.518.t11-t12-native-isolation-campaign/v1",
        "under_load":working,"without_worker":disconnected,
        "native_engine":"run_engine_until_real_tasks",
        "research":"separate_child_process_two_real_grids",
        "mcp_transport":"not_evaluated_in_native_test",
        "windows_process_tree":evidence,
        "exchange_trade_post":false,
    }));
}
