//! Actual Engine task composition under a separately spawned research worker.
//! This runs Engine in the test process, so it is not an Engine executable/PID proof.
#[path = "../tests/support/process_tree.rs"]
mod process_tree;
use super::*;
use crate::{
    cloudflare_transport::CloudflareTransportConfig,
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
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering as CounterOrdering};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};

#[derive(Default)]
struct SourceReadWitness {
    successful_requests: AtomicU64,
    returned_rows: AtomicU64,
}

fn heavy_research(root: std::path::PathBuf, reads: Arc<SourceReadWitness>) -> ResearchClient {
    let executable = std::path::PathBuf::from(
        std::env::var_os("OKX_RESEARCH_WORKER_TEST_EXE").expect("set OKX_RESEARCH_WORKER_TEST_EXE"),
    );
    assert!(executable.is_file());
    let source: SourceHandler = Arc::new(move |request| {
        let reads = reads.clone();
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
                .collect::<Vec<_>>();
            reads
                .returned_rows
                .fetch_add(rows.len() as u64, CounterOrdering::Relaxed);
            reads
                .successful_requests
                .fetch_add(1, CounterOrdering::Relaxed);
            Ok(PublicSourceReply::Candles(PublicCandleCapture {
                rows,
                raw_body: b"synthetic closed candles".to_vec(),
                acquired_at_ms: "1".into(),
                http_delivery: None,
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

fn boundary_grid(
    key: &str,
    instruments: usize,
    bars_per_instrument: u64,
    variants: usize,
) -> ResearchRequest {
    ResearchRequest::Grid {
        idempotency_key: key.into(),
        instruments: (0..instruments).map(|i| format!("TEST-{i}")).collect(),
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 60_000 * bars_per_instrument,
        strategy_id: "ema_cross_v1".into(),
        variants: (0..variants)
            .map(|i| {
                EmaParameters {
                    fast: 2,
                    slow: 90 + i,
                }
                .into()
            })
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

/// Pre-source wire rejection on the *same real child process* used for the
/// concurrent Grid/Finance campaign. Maximal valid plans are admission-only:
/// executing a million-bar grid here would contaminate the bounded SLO run.
async fn t10_real_worker_boundary_proof(
    research: &ResearchClient,
    reads: &SourceReadWitness,
) -> serde_json::Value {
    let before = reads.successful_requests.load(CounterOrdering::SeqCst);
    let rows_before = reads.returned_rows.load(CounterOrdering::SeqCst);
    assert_eq!(before, 0, "no source page may precede the boundary check");
    assert_eq!(rows_before, 0);
    for (name, request) in [
        (
            "one_million_bars_one_instrument_64",
            boundary_grid("t10-max-bars", 1, 1_000_000, 64),
        ),
        (
            "eight_instruments_one_million_bars_64",
            boundary_grid("t10-max-eight", 8, 125_000, 64),
        ),
    ] {
        assert!(request.valid(), "{name}: exact boundary must admit");
    }
    let mut rejected = serde_json::Map::new();
    for (name, request) in [
        (
            "one_million_plus_one_bars",
            boundary_grid("t10-over-bars", 1, 1_000_001, 64),
        ),
        (
            "nine_instruments",
            boundary_grid("t10-over-instruments", 9, 125_000, 64),
        ),
        (
            "sixty_five_variants",
            boundary_grid("t10-over-variants", 8, 125_000, 65),
        ),
    ] {
        assert!(!request.valid(), "{name}: exceeding boundary must reject");
        let error = tokio::time::timeout(Duration::from_secs(5), research.research(request))
            .await
            .expect("pre-source real Worker IPC rejection deadline")
            .expect_err("invalid request must not create a Research job");
        assert_eq!(error, "INVALID_RESEARCH_REQUEST", "{name}");
        assert_eq!(
            reads.successful_requests.load(CounterOrdering::SeqCst),
            before
        );
        assert_eq!(
            reads.returned_rows.load(CounterOrdering::SeqCst),
            rows_before
        );
        rejected.insert(name.into(), serde_json::Value::String(error));
    }
    serde_json::json!({
        "schema":"okx.518.t10-real-child-pre-source-boundaries/v1",
        "route":"real_agent_to_separate_research_worker_ipc",
        "admission_only_not_executed":["one_million_bars_one_instrument_64","eight_instruments_one_million_bars_64"],
        "rejected_before_source":rejected,
        "source_requests_before":before,
        "source_requests_after":reads.successful_requests.load(CounterOrdering::SeqCst),
        "returned_rows_before":rows_before,
        "returned_rows_after":reads.returned_rows.load(CounterOrdering::SeqCst),
        "full_scale_execution":"not_attempted",
        "side_effects":"none",
    })
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

struct McpLoadHost {
    child: Child,
    http_url: String,
    ws_url: String,
}

impl McpLoadHost {
    async fn start(installation_id: &str, governance_path: &std::path::Path) -> Self {
        let mcp = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../cloudflare/okx-mcp");
        let mut command = Command::new("node");
        command
            .arg("scripts/t10-mcp-load-host.mjs")
            .arg(installation_id)
            .arg(governance_path)
            .current_dir(mcp)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .expect("isolated MCP fixture host must start");
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(30), stdout.read_line(&mut line))
            .await
            .expect("MCP fixture host ready deadline")
            .expect("MCP fixture host stdout failed");
        let json = line
            .strip_prefix("T10_MCP_LOAD_HOST_READY:")
            .expect("MCP fixture must produce authenticated typed ready record");
        let proof: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(proof["schema"], "okx.518.t10-mcp-load-host/v1");
        assert_eq!(proof["oauth"], "test_scope_only_not_evaluated");
        assert_eq!(proof["storage"], "ephemeral_sqlite_do");
        let http_url = proof["http_url"].as_str().unwrap().to_owned();
        let ws_url = proof["ws_url"].as_str().unwrap().to_owned();
        assert!(http_url.starts_with("http://127.0.0.1:"));
        Self {
            child,
            http_url,
            ws_url,
        }
    }

    async fn close(mut self) {
        drop(self.child.stdin.take());
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait())
            .await
            .expect("MCP fixture host drain deadline")
            .expect("MCP fixture host wait");
        assert!(
            status.success(),
            "MCP fixture host must close and erase its isolated DO"
        );
    }
}

async fn mcp_request(
    client: &reqwest::Client,
    url: &str,
    action: &str,
    account: Option<&str>,
) -> serde_json::Value {
    let arguments = if action == "runtime" {
        serde_json::json!({"action":"runtime"})
    } else {
        serde_json::json!({"action":"explain","view":"finance","account":account,
            "limit":1})
    };
    let request = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
        "params":{"name":"diagnostics","arguments":arguments}});
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        client
            .post(url)
            .header("content-type", "application/json")
            .body(request.to_string())
            .send(),
    )
    .await
    .expect("MCP HTTP/DO/WSS request timeout")
    .expect("MCP localhost HTTP request failed");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let json = response.text().await.expect("MCP body read failed");
    serde_json::from_str(&json).expect("MCP JSON-RPC reply must decode")
}

async fn run_phase(disconnect_before_opening: bool, with_mcp: bool) -> serde_json::Value {
    let mut fixture = fixture().await;
    // Prepare the isolated workerd/DO before the CPU jobs begin. The whole
    // measured MCP interval must overlap real Grid compute, not fixture startup.
    let host = if with_mcp {
        let governance_path = fixture.root.join("t10-synthetic-native-governance.json");
        std::fs::write(
            &governance_path,
            serde_json::to_vec(&fixture.authority.state).unwrap(),
        )
        .unwrap();
        Some(McpLoadHost::start(fixture.owner.installation_id(), &governance_path).await)
    } else {
        None
    };
    let (market, public, _socket) = raw_market();
    let api = okx_api::OkxPublicClient::with_http_transport(
        okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
        okx_api::RateBudget::new(),
        fixture.feed.clone(),
    )
    .unwrap();
    let bootstrap = MarketBootstrapper::new(api);
    let reads = Arc::new(SourceReadWitness::default());
    let research = heavy_research(fixture.root.join("heavy-research"), reads.clone());
    let boundaries = t10_real_worker_boundary_proof(&research, &reads).await;
    // Wall clock begins after fixture start, before the first actual source
    // fetch. Cargo build and MCP host startup are outside this interval.
    let phase_started = std::time::Instant::now();
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
    let mut ctx = EngineContext {
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
    let http_url = host.as_ref().map(|host| host.http_url.clone());
    ctx.transport = host
        .as_ref()
        .map(|host| CloudflareTransportConfig::isolated_loopback_fixture(host.ws_url.clone()));
    let account = fixture.config.account_key.wire();
    let trade = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let child = research.clone();
    let source_reads = reads.clone();
    let boundary_evidence = boundaries.clone();
    let (evidence_tx, evidence_rx) = tokio::sync::oneshot::channel();
    let until = async move {
        let mcp_client = reqwest::Client::new();
        if let Some(url) = &http_url {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let result = mcp_request(&mcp_client, url, "runtime", None).await;
                    if result["result"]["structuredContent"]["data"]["transport"]["status"]
                        == "PASS"
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("genuine loopback HTTP->DO->Agent WSS must become fresh");
        }
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
            assert_eq!(
                restarted["state"], "interrupted",
                "a restarted CPU job must never be represented as successful"
            );
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
                Duration::from_secs(2),
                owner.campaign_summary(campaign.clone()),
            )
            .await
            .expect("Native Finance read blocked by Research")
            .unwrap();
            native_finance_ms.push(began.elapsed().as_secs_f64() * 1000.0);
            assert!(summary.opening.is_some(), "Engine opening was lost");
            assert_eq!(summary.reserved_entry_usd, "0");
            assert_eq!(summary.consumed_entry_usd, "0");
            assert_eq!(owner.installation_id(), installation);
            if !disconnect_before_opening {
                let began = std::time::Instant::now();
                let snapshot =
                    tokio::time::timeout(Duration::from_secs(2), child.diagnostic_job(&first))
                        .await
                        .expect("read-only Research IPC blocked by CPU")
                        .unwrap();
                research_explain_ms.push(began.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(snapshot["job"]["id"], first);
                assert!(
                    matches!(
                        snapshot["job"]["state"].as_str(),
                        Some("queued" | "running" | "completed")
                    ),
                    "diagnostic must be an authentic bounded job snapshot: {snapshot}"
                );
            }
        }
        if !disconnect_before_opening {
            // Require at least one running job to overlap the entire Native
            // Finance measurement interval, not just a startup observation.
            let a = state(&child, &first).await;
            let b = state(&child, &second).await;
            assert!(
                a["state"] == "running" || b["state"] == "running",
                "research completed before the native finance samples: {a} {b}"
            );
        }
        let finance = latency_proof(native_finance_ms, "native_finance_campaign_summary");
        let explain = if disconnect_before_opening {
            serde_json::Value::Null
        } else {
            latency_proof(research_explain_ms, "research_child_explain")
        };
        let mcp_finance = if let Some(url) = &http_url {
            let mut samples = Vec::with_capacity(100);
            for _ in 0..100 {
                let began = std::time::Instant::now();
                let reply = mcp_request(&mcp_client, url, "finance", Some(&account)).await;
                samples.push(began.elapsed().as_secs_f64() * 1000.0);
                assert!(
                    reply.get("error").is_none(),
                    "MCP schema must accept Finance read"
                );
                let data = &reply["result"]["structuredContent"]["data"];
                assert_eq!(data["schema"], "okx.finance-diagnostics/v1");
                assert_eq!(data["account"], account);
                assert_eq!(
                    data["central_budget"]["state"], "NOT_EVALUATED",
                    "MCP diagnostics must not fabricate a central financial permit"
                );
            }
            latency_proof(samples, "mcp_http_sqlite_do_wss_native_finance")
        } else {
            serde_json::Value::Null
        };
        if with_mcp && !disconnect_before_opening {
            let a = state(&child, &first).await;
            let b = state(&child, &second).await;
            assert!(
                a["state"] == "running" || b["state"] == "running",
                "real Grid must overlap entire MCP Finance measurement: {a} {b}"
            );
        }
        let successful_requests = source_reads
            .successful_requests
            .load(CounterOrdering::Relaxed);
        let returned_rows = source_reads.returned_rows.load(CounterOrdering::Relaxed);
        assert!(
            successful_requests > 0 && returned_rows > 0,
            "real Research child must have fetched typed fixture source pages"
        );
        evidence_tx.send(serde_json::json!({
            "schema":"okx.518.t11-t12-native-concurrent-isolation/v1",
            "phase":if disconnect_before_opening {"worker_disconnected"} else {"two_heavy_grids_running"},
            "native_finance_read_only":finance,
            "research_explain":explain,
            "mcp_native_finance_read_only":mcp_finance,
            "native_installation_identity_preserved":true,
            "reserved_entry_usd":"0","consumed_entry_usd":"0",
            "financial_owner":"native_sqlite_original_owner",
            "synthetic_public_source":true,
            "pre_source_boundaries":boundary_evidence,
            "source_reads":{
                "kind":"actual_research_ipc_public_candles_requests_to_synthetic_handler",
                "successful_requests":successful_requests,
                "returned_candle_rows":returned_rows,
                "venue_http_reads":0,
                "source_storage_page_reads":"not_measured",
                "observation_scope":"request_and_rows_counted_in_native_source_handler"
            },
            "phase_wall_ms_excluding_build":phase_started.elapsed().as_secs_f64() * 1000.0,
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
    let proof = evidence_rx
        .await
        .expect("Native isolation proof not recorded");
    if let Some(host) = host {
        host.close().await;
    }
    research.shutdown().await;
    fixture.finish().await;
    proof
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect() {
    let measurement = process_tree::ResearchProcessTree::begin();
    let working = run_phase(false, false).await;
    let disconnected = run_phase(true, false).await;
    let evidence = measurement.finish();
    println!("RESEARCH_PROCESS_TREE={evidence}");
    println!(
        "T11_T12_NATIVE_ISOLATION_JSON:{}",
        serde_json::json!({
            "schema":"okx.518.t11-t12-native-isolation-campaign/v1",
            "under_load":working,"without_worker":disconnected,
            "native_engine":"run_engine_until_real_tasks",
            "research":"separate_child_process_two_real_grids",
            "mcp_transport":"not_evaluated_in_native_test",
            "windows_process_tree":evidence,
            "exchange_trade_post":false,
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_http_durable_object_wss_native_finance_stays_readable_under_heavy_research() {
    let measurement = process_tree::ResearchProcessTree::begin();
    let under_load = run_phase(false, true).await;
    let worker_disconnected = run_phase(true, true).await;
    let process_tree = measurement.finish();
    println!(
        "T10_MCP_NATIVE_LOAD_JSON:{}",
        serde_json::json!({
            "schema":"okx.518.t10-mcp-do-wss-native-finance/v1",
            "under_load":under_load,"worker_disconnected":worker_disconnected,
            "mcp_transport":"real_loopback_http_miniflare_sqlite_do_websocket_native",
            "oauth":"test_scope_only_not_evaluated",
            "production_network":"not_evaluated",
            "research":"two_cpu_grids_in_separate_native_worker_child",
            "financial_owner":"existing_native_sqlite",
            "process_tree":process_tree,
            "exchange_trade_post":false,
        })
    );
}
