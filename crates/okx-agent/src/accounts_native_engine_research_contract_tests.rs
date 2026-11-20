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
async fn run_phase(disconnect_before_opening: bool) {
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
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_maintenance_and_stop_remain_independent_of_heavy_research_and_disconnect() {
    let measurement = process_tree::ResearchProcessTree::begin();
    run_phase(false).await;
    run_phase(true).await;
    let evidence = measurement.finish();
    println!("RESEARCH_PROCESS_TREE={evidence}");
}
