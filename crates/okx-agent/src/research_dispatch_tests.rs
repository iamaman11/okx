use crate::research_client::{ResearchClient, ResearchClientConfig, SourceHandler};
use okx_protocol::{
    analytics::*,
    research_ipc::{
        ClosedPrice, FundingHistoryEvent, GraphClosedBar, GraphPosition, GraphSessionId,
        GraphSessionReply, GraphSessionRequest, GraphSignalDefinition, GraphSignalIntent,
        HistoryCandle, PublicCandleCapture, PublicFundingCapture, PublicSourceReply,
        PublicSourceRequest, ResearchOpenInterest, ResearchScope, SignalPosition, SignalRequest,
    },
};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

fn fixture(source: SourceHandler) -> (PathBuf, ResearchClient) {
    let executable = PathBuf::from(
        std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
            .expect("set OKX_RESEARCH_SERVICE_TEST_EXE to the built research worker"),
    );
    assert!(executable.is_file(), "worker test executable is missing");
    // Wall-clock resolution is not a uniqueness guarantee for parallel tests.
    // Each child must own a distinct root; the production writer lock stays on.
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = std::env::temp_dir().join(format!(
        "okx-research-client-test-{}-{:032x}",
        std::process::id(),
        u128::from_le_bytes(nonce)
    ));
    std::fs::create_dir(&root).expect("research fixture requires a NEW unique root");
    let client = ResearchClient::new(
        ResearchClientConfig {
            executable,
            root: root.clone(),
            scope: ResearchScope::new("research-test".into()).unwrap(),
        },
        source,
    );
    (root, client)
}
pub(super) fn candle_source(calls: Arc<AtomicUsize>) -> SourceHandler {
    Arc::new(move |request| {
        calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let PublicSourceRequest::Candles { after, .. } = request else {
                return Err("UNEXPECTED_SOURCE".into());
            };
            let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
            let rows = (0..200u64)
                .rev()
                .filter(|i| i * 60_000 < cursor)
                .take(100)
                .map(|i| HistoryCandle {
                    open_time_ms: (i * 60_000).to_string(),
                    open: (100 + i).to_string(),
                    high: (102 + i).to_string(),
                    low: (99 + i).to_string(),
                    close: (101 + i).to_string(),
                    volume: "1".into(),
                    volume_currency: "1".into(),
                    volume_quote: None,
                    confirmed: true,
                })
                .collect();
            Ok(PublicSourceReply::Candles(PublicCandleCapture {
                raw_body: b"fixture".to_vec(),
                rows,
                acquired_at_ms: "1".into(),
            }))
        })
    })
}
fn costs() -> ResearchCosts {
    ResearchCosts {
        maker_fee_bps: 1.0,
        taker_fee_bps: 2.0,
        slippage_bps: 1.0,
        funding_mode: FundingMode::ZeroAssumption,
    }
}
pub(super) fn grid(key: &str) -> ResearchRequest {
    ResearchRequest::Grid {
        idempotency_key: key.into(),
        instruments: vec!["BTC-USDT".into(), "ETH-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 12_000_000,
        strategy_id: "ema_cross_v1".into(),
        variants: (3..67)
            .map(|slow| EmaParameters { fast: 2, slow }.into())
            .collect(),
        costs: costs(),
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    }
}
async fn terminal(client: &ResearchClient, id: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let status = client
                .research(ResearchRequest::Status { job_id: id.into() })
                .await
                .unwrap();
            if matches!(
                status["state"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("research job did not finish")
}

#[tokio::test]
async fn real_worker_live_signal_ipc_is_causal_and_has_no_source_or_finance_access() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (root, client) = fixture(candle_source(calls.clone()));
    let request = SignalRequest {
        strategy_id: "ema_cross_v1".into(),
        parameters: EmaParameters { fast: 2, slow: 4 },
        closes: [100.0, 101.0, 105.0, 120.0, 125.0]
            .into_iter()
            .enumerate()
            .map(|(i, close)| ClosedPrice {
                time_ms: (i as u64 + 1) * 60_000,
                close,
            })
            .collect(),
    };
    let result = client.evaluate_signal(request.clone()).await.unwrap();
    assert_eq!(result.position, SignalPosition::Long);
    assert!(result.matches_request(&request));
    assert_eq!(
        client.evaluate_signal(request.clone()).await.unwrap(),
        result
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut invalid = request.clone();
    invalid.closes[0].close = f64::NAN;
    assert_eq!(
        client.evaluate_signal(invalid).await.unwrap_err(),
        "INVALID_SIGNAL_REQUEST"
    );
    assert_eq!(
        client
            .evaluate_signal(SignalRequest {
                strategy_id: "not_installed".into(),
                ..request
            })
            .await
            .unwrap_err(),
        "UNSUPPORTED_STRATEGY"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    client.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn real_agent_worker_ipc_graph_stream_is_incremental_idempotent_and_read_only() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (root, client) = fixture(candle_source(calls.clone()));
    let session_id = GraphSessionId::new("research:graph:btc".into()).unwrap();
    let definition: GraphSignalDefinition = serde_json::from_value(serde_json::json!({
        "version":"okx.graph-signal/v1",
        "graph":{"version":"okx.graph/v1","steps":[
            {"id":"return","op":"returns_series","op_version":1,"inputs":{"input":"close"}},
            {"id":"positive","op":"greater_than","op_version":1,
             "inputs":{"input":"return"},"parameters":{"threshold":0.0}}
        ],"result_steps":["positive"]},
        "signal_step":"positive","target_if_true":1,"target_if_false":0,"missing":"hold"
    }))
    .unwrap();
    let restore = GraphSessionRequest::Restore {
        session_id: session_id.clone(),
        definition: Box::new(definition),
        capacity: 64,
        history: vec![
            GraphClosedBar {
                time_ms: 60_000,
                close: Some(100.),
            },
            GraphClosedBar {
                time_ms: 120_000,
                close: Some(110.),
            },
        ],
    };
    let first = client.graph_signal(restore.clone()).await.unwrap();
    assert!(first.matches_request(&restore));
    assert_eq!(first, client.graph_signal(restore).await.unwrap());
    let GraphSessionReply::Evaluated {
        graph_sha256,
        intent,
        ..
    } = first
    else {
        panic!("expected decision")
    };
    assert_eq!(intent, GraphSignalIntent::SetTarget(GraphPosition::Long));
    let next = GraphSessionRequest::Advance {
        session_id: session_id.clone(),
        graph_sha256: graph_sha256.clone(),
        expected_bars: 3,
        bar: GraphClosedBar {
            time_ms: 180_000,
            close: Some(90.),
        },
    };
    let flat = client.graph_signal(next.clone()).await.unwrap();
    assert_eq!(flat, client.graph_signal(next).await.unwrap());
    assert!(matches!(
        flat,
        GraphSessionReply::Evaluated {
            observed_bars: 3,
            intent: GraphSignalIntent::SetTarget(GraphPosition::Flat),
            ..
        }
    ));
    let gap = GraphSessionRequest::Advance {
        session_id: session_id.clone(),
        graph_sha256: graph_sha256.clone(),
        expected_bars: 4,
        bar: GraphClosedBar {
            time_ms: 240_000,
            close: None,
        },
    };
    assert!(matches!(
        client.graph_signal(gap).await.unwrap(),
        GraphSessionReply::Evaluated {
            observed_bars: 4,
            intent: GraphSignalIntent::Hold,
            ..
        }
    ));
    let old = GraphSessionRequest::Advance {
        session_id: session_id.clone(),
        graph_sha256: graph_sha256.clone(),
        expected_bars: 5,
        bar: GraphClosedBar {
            time_ms: 60_000,
            close: Some(100.),
        },
    };
    assert_eq!(
        client.graph_signal(old).await.unwrap_err(),
        "composition.stream:CLOSED_BAR_NOT_MONOTONIC"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let release = GraphSessionRequest::Release {
        session_id,
        graph_sha256,
    };
    assert!(matches!(
        client.graph_signal(release).await.unwrap(),
        GraphSessionReply::Released { .. }
    ));
    client.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn real_worker_preserves_candidate_progress_and_read_only_diagnostics() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (root, client) = fixture(candle_source(calls.clone()));
    let queued = client
        .research(grid("ipc-candidate-progress"))
        .await
        .unwrap();
    let id = queued["id"].as_str().unwrap();
    let status = terminal(&client, id).await;
    assert_eq!(status["state"], "completed", "{status}");
    assert_eq!(status["candidate_progress"]["completed_candidates"], 128);
    assert_eq!(status["candidate_progress"]["total_candidates"], 128);
    assert_eq!(status["candidate_progress"]["completed_work_units"], 386);
    assert_eq!(status["candidate_progress"]["total_work_units"], 386);
    assert_eq!(status["progress"], 100);
    let before = calls.load(Ordering::SeqCst);
    let first = client.diagnostic_job(id).await.unwrap();
    let second = client.diagnostic_job(id).await.unwrap();
    assert_eq!(first["job"]["state"], "completed");
    assert_eq!(second["job"]["state"], "completed");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        before,
        "diagnostics cannot fetch source data"
    );
    client.shutdown().await;
    assert!(
        client.diagnostic_job(id).await.is_err(),
        "diagnostics cannot start a worker"
    );
    // Reopening exercises durable progress across a real process restart.
    let reopened = terminal(&client, id).await;
    assert_eq!(reopened["candidate_progress"]["completed_candidates"], 128);
    client.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn real_worker_cancellation_fences_late_source_response() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (root, client) = fixture(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        move |_| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Err("FIXTURE_SOURCE_RELEASED".into())
            })
        }
    }));
    let queued = client.research(grid("ipc-cancel-grid")).await.unwrap();
    let id = queued["id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .unwrap();
    let cancelled = client
        .research(ResearchRequest::Cancel {
            job_id: id.into(),
            idempotency_key: "ipc-cancel-once".into(),
        })
        .await
        .unwrap();
    assert_eq!(cancelled["state"], "cancelled");
    release.notify_waiters();
    let status = terminal(&client, id).await;
    assert_eq!(status["state"], "cancelled");
    client.shutdown().await;
    let reopened = terminal(&client, id).await;
    assert_eq!(reopened["state"], "cancelled");
    client.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn real_worker_decision_replay_and_scope_are_durable_without_source_reads() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (root, client) = fixture(candle_source(calls.clone()));
    let request = ResearchRequest::Backtest {
        idempotency_key: "ipc-decision-result".into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 12_000_000,
        strategy_id: "ema_cross_v1".into(),
        parameters: EmaParameters { fast: 2, slow: 3 }.into(),
        costs: costs(),
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    };
    let mut other_request = request.clone();
    if let ResearchRequest::Backtest {
        idempotency_key,
        seed,
        ..
    } = &mut other_request
    {
        *idempotency_key = "ipc-other-decision-result".into();
        *seed = 8;
    }
    let queued = client.research(request).await.unwrap();
    let other_queued = client.research(other_request).await.unwrap();
    let other_id = other_queued["id"].as_str().unwrap().to_owned();
    assert_eq!(terminal(&client, &other_id).await["state"], "completed");
    let job_id = queued["id"].as_str().unwrap().to_owned();
    let status = terminal(&client, &job_id).await;
    assert_eq!(status["state"], "completed", "{status}");
    let result_id = status["artifact"].as_str().unwrap().to_owned();
    let request = ResearchRequest::RecordDecision {
        job_id: job_id.clone(),
        result_id,
        expected_decision_id: None,
        name: "Experiment A".into(),
        hypothesis: "Independent evidence".into(),
        status: ResearchDecisionStatus::Deferred,
        reason: "Await review".into(),
    };
    let before = calls.load(Ordering::SeqCst);
    let first = client.research(request.clone()).await.unwrap();
    let replay = client.research(request).await.unwrap();
    assert_eq!(first, replay);
    assert_eq!(first["trading_authority"], false);
    let decision_id = first["decision_id"].as_str().unwrap().to_owned();
    let foreign = client
        .research(ResearchRequest::Decision {
            job_id: other_id,
            decision_id: Some(decision_id.clone()),
        })
        .await;
    assert!(
        foreign.is_err(),
        "a decision from another job must not be disclosed"
    );
    client.shutdown().await;
    let read = client
        .research(ResearchRequest::Decision {
            job_id: job_id.clone(),
            decision_id: Some(decision_id),
        })
        .await
        .unwrap();
    assert_eq!(read, first);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        before,
        "decision metadata cannot fetch source data"
    );
    client.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// T06/T09/T11: one real Agent -> child Worker IPC campaign. Historical
/// observation timestamps may explain retrospective metrics, but cannot
/// certify an OI/funding-derived decision signal. Counterfactual money is
/// exact and currency-scoped; neither path can reach a financial writer.
#[tokio::test]
async fn t06_t09_real_worker_causality_and_economics_are_isolated() {
    let reads = Arc::new(AtomicUsize::new(0));
    let source: SourceHandler = Arc::new({
        let reads = reads.clone();
        move |request| {
            reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                match request {
                    PublicSourceRequest::Candles { after, .. } => {
                        let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
                        let rows = (0..100u64)
                            .rev()
                            .filter(|i| i * 60_000 < cursor)
                            .map(|i| HistoryCandle {
                                open_time_ms: (i * 60_000).to_string(),
                                open: (100 + i).to_string(),
                                high: (102 + i).to_string(),
                                low: (99 + i).to_string(),
                                close: (101 + i).to_string(),
                                volume: "1".into(),
                                volume_currency: "1".into(),
                                volume_quote: None,
                                confirmed: true,
                            })
                            .collect();
                        Ok(PublicSourceReply::Candles(PublicCandleCapture {
                            raw_body: b"closed-candles".to_vec(),
                            rows,
                            acquired_at_ms: "9000000".into(),
                        }))
                    }
                    PublicSourceRequest::OpenInterest { .. } => {
                        Ok(PublicSourceReply::OpenInterest(
                            (0..100u64)
                                .map(|i| ResearchOpenInterest {
                                    ts: (i * 60_000).to_string(),
                                    oi: (100 + i).to_string(),
                                    oi_currency: (100 + i).to_string(),
                                })
                                .collect(),
                        ))
                    }
                    PublicSourceRequest::Funding { after, .. } => {
                        let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
                        let rows = [60_000u64, 99 * 60_000]
                            .into_iter()
                            .filter(|time| *time < cursor)
                            .map(|time| FundingHistoryEvent {
                                funding_time_ms: time.to_string(),
                                funding_rate: "0.002".into(),
                                realized_rate: Some("0.001".into()),
                                formula_type: None,
                                method: None,
                            })
                            .collect();
                        Ok(PublicSourceReply::Funding(PublicFundingCapture {
                            raw_body: b"realized-funding".to_vec(),
                            rows,
                            acquired_at_ms: "9000000".into(),
                        }))
                    }
                    _ => Err("UNEXPECTED_SOURCE".into()),
                }
            })
        }
    });
    let (root, client) = fixture(source);
    let market_request: MarketRequest = serde_json::from_value(serde_json::json!({
        "action":"screen","instruments":["BTC-USDT-SWAP"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":6000000,"limit":1,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"price_change","op":"returns_series","input":"close"},
            {"id":"price_signal","op":"greater_than","input":"price_change","threshold":0},
            {"id":"oi_change","op":"returns_series","input":"open_interest"},
            {"id":"oi_signal","op":"greater_than","input":"oi_change","threshold":0},
            {"id":"funding_observed","op":"rolling_mean","input":"funding_rate","period":1},
            {"id":"funding_signal","op":"greater_than","input":"funding_rate","threshold":0}
        ],"result_steps":["price_signal","oi_change","oi_signal","funding_observed","funding_signal"]}
    })).unwrap();
    assert!(market_request.valid());
    let mut market = client
        .market(market_request.clone(), None, None)
        .await
        .unwrap();
    if market["rows"].is_null() {
        let job_id = market["id"].as_str().expect("queued market job ID");
        assert_eq!(terminal(&client, job_id).await["state"], "completed");
        market = client.market(market_request, None, None).await.unwrap();
    }
    let row = &market["rows"][0];
    assert_eq!(row["metrics"]["price_signal"], true);
    assert_eq!(
        row["composition_quality"]["price_signal"]["status"],
        "available_at_closed_bar"
    );
    assert!(row["metrics"]["oi_change"].is_number());
    assert_eq!(
        row["composition_quality"]["oi_change"]["status"],
        "retrospective_publication_unverified"
    );
    assert!(
        (row["metrics"]["funding_observed"].as_f64().unwrap() - 0.001).abs() < 1e-12,
        "funding uses realized_rate, never the conflicting 0.002 forecast"
    );
    assert_eq!(
        row["composition_quality"]["funding_observed"]["status"],
        "retrospective_publication_unverified"
    );
    for signal in ["oi_signal", "funding_signal"] {
        assert!(
            row["metrics"][signal].is_null(),
            "{signal} cannot be a causal signal"
        );
        assert_eq!(
            row["composition_quality"][signal]["issues"][0]["reason"],
            "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
        );
    }
    assert_eq!(
        row["source_integrity"]["quality"],
        "verified_normalized_sources"
    );
    assert_eq!(
        row["source_integrity"]["historical_publication_time_verified"],
        false
    );
    assert_eq!(
        row["source_integrity"]["metric_reexecution"]["quality"],
        "verified_reexecution"
    );
    let source_reads = reads.load(Ordering::SeqCst);
    assert!(source_reads >= 3);

    let cases = serde_json::json!([{
        "instrument":"BTC-USDT-SWAP","product":"linear_swap","side":"long",
        "quantity":"3","entry_price":"100","exit_price":"110",
        "contract_type":"linear","contract_value":"0.1",
        "contract_value_currency":"BTC","settle_currency":"USDT",
        "fees":{"currency":"USDT","amount":"0"},
        "funding":{"currency":"USDT","amount":"0"}
    },{
        "instrument":"ETH-USD","product":"spot","side":"long",
        "quantity":"1","entry_price":"20","exit_price":"22",
        "fees":{"currency":"USD","amount":"0"},
        "funding":{"currency":"USD","amount":"0"}
    }]);
    let counterfactual: ResearchRequest = serde_json::from_value(serde_json::json!({
        "action":"economics","cases":cases,"data_mode":"counterfactual"
    }))
    .unwrap();
    assert!(counterfactual.valid());
    let economy = client.research(counterfactual).await.unwrap();
    assert_eq!(economy["rows"][0]["net"]["amount_exact"], "3");
    assert_eq!(economy["rows"][0]["net"]["currency"], "USDT");
    assert_eq!(economy["rows"][1]["net"]["amount_exact"], "2");
    assert_eq!(economy["rows"][1]["net"]["currency"], "USD");
    assert!(economy["portfolio_equity"].is_null());
    assert!(economy["joint_portfolio_risk"].is_null());
    assert_eq!(
        economy["summary"]["aggregation"],
        "independent_positions_no_fx_no_joint_risk"
    );
    assert_eq!(economy["effects"]["venue_posts"], 0);
    assert_eq!(economy["effects"]["finance_writes"], 0);

    let historical: ResearchRequest = serde_json::from_value(serde_json::json!({
        "action":"economics","cases":cases,"data_mode":"historical_observed"
    }))
    .unwrap();
    let blocked = client.research(historical).await.unwrap();
    assert_eq!(blocked["status"], "blocked");
    assert_eq!(
        blocked["reason_code"],
        "HISTORICAL_RULES_AND_FEES_UNAVAILABLE"
    );
    assert_eq!(blocked["effects"]["job_created"], false);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        source_reads,
        "economic scenario must not read venues or create hidden sources"
    );
    client.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// T04/T06/T13: the real Agent -> Worker route must keep each instrument's
/// source grid and provenance separate. An OI point one millisecond off the
/// candle grid is missing, not a nearest-neighbour historical observation.
#[tokio::test]
async fn t06_real_worker_cross_instrument_oi_alignment_is_strict_and_isolated() {
    let candle_reads = Arc::new(AtomicUsize::new(0));
    let oi_reads = Arc::new(AtomicUsize::new(0));
    let source: SourceHandler = Arc::new({
        let candle_reads = candle_reads.clone();
        let oi_reads = oi_reads.clone();
        move |request| {
            let candle_reads = candle_reads.clone();
            let oi_reads = oi_reads.clone();
            Box::pin(async move {
                match request {
                    PublicSourceRequest::Candles { after, .. } => {
                        candle_reads.fetch_add(1, Ordering::SeqCst);
                        let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
                        let rows = (0..100u64)
                            .rev()
                            .filter(|index| index * 60_000 < cursor)
                            .map(|index| HistoryCandle {
                                open_time_ms: (index * 60_000).to_string(),
                                open: (100 + index).to_string(),
                                high: (102 + index).to_string(),
                                low: (99 + index).to_string(),
                                close: (101 + index).to_string(),
                                volume: "1".into(),
                                volume_currency: "1".into(),
                                volume_quote: None,
                                confirmed: true,
                            })
                            .collect();
                        Ok(PublicSourceReply::Candles(PublicCandleCapture {
                            raw_body: b"two-instrument-candles".to_vec(),
                            rows,
                            acquired_at_ms: "9000000".into(),
                        }))
                    }
                    PublicSourceRequest::OpenInterest { instrument, .. } => {
                        oi_reads.fetch_add(1, Ordering::SeqCst);
                        let shifted = instrument == "ETH-USDT-SWAP";
                        Ok(PublicSourceReply::OpenInterest(
                            (0..100u64)
                                .map(|index| ResearchOpenInterest {
                                    ts: (index * 60_000 + u64::from(shifted && index == 42))
                                        .to_string(),
                                    oi: (100 + index).to_string(),
                                    oi_currency: (100 + index).to_string(),
                                })
                                .collect(),
                        ))
                    }
                    _ => Err("UNEXPECTED_SOURCE".into()),
                }
            })
        }
    });
    let (root, client) = fixture(source);
    let request: MarketRequest = serde_json::from_value(serde_json::json!({
        "action":"screen",
        "instruments":["BTC-USDT-SWAP","ETH-USDT-SWAP"],
        "fields":["returns"],"timeframe_ms":60000,
        "start_ms":0,"end_ms":6000000,"limit":2,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"price_return","op":"returns_series","input":"close"},
            {"id":"price_signal","op":"greater_than","input":"price_return","threshold":0},
            {"id":"oi_return","op":"returns_series","input":"open_interest"},
            {"id":"oi_signal","op":"greater_than","input":"oi_return","threshold":0}
        ],"result_steps":["price_signal","oi_return","oi_signal"]}
    }))
    .unwrap();
    assert!(request.valid());
    let mut result = client.market(request.clone(), None, None).await.unwrap();
    if result["rows"].is_null() {
        let id = result["id"].as_str().expect("queued market job ID");
        assert_eq!(terminal(&client, id).await["state"], "completed");
        result = client.market(request.clone(), None, None).await.unwrap();
    }
    // A full provenance row may exceed the response byte budget. Cursor
    // pagination must replay the immutable artifact, never refetch venues.
    let source_read_count = (
        candle_reads.load(Ordering::SeqCst),
        oi_reads.load(Ordering::SeqCst),
    );
    let mut rows = Vec::new();
    loop {
        rows.extend(
            result["rows"]
                .as_array()
                .expect("market result rows")
                .iter()
                .cloned(),
        );
        let Some(cursor) = result["next_cursor"].as_str() else {
            break;
        };
        let mut next = request.clone();
        next.cursor = Some(cursor.to_owned());
        result = client.market(next, None, None).await.unwrap();
    }
    assert_eq!(
        (
            candle_reads.load(Ordering::SeqCst),
            oi_reads.load(Ordering::SeqCst)
        ),
        source_read_count,
        "cursor pages must read immutable evidence without refetching venues"
    );
    assert_eq!(rows.len(), 2, "{result}");
    let row = |instrument: &str| {
        rows.iter()
            .find(|row| row["instrument"] == instrument)
            .expect("instrument must retain its own result")
    };
    let aligned = row("BTC-USDT-SWAP");
    let shifted = row("ETH-USDT-SWAP");
    for observed in [aligned, shifted] {
        assert_eq!(observed["metrics"]["price_signal"], true);
        assert_eq!(
            observed["composition_quality"]["price_signal"]["status"],
            "available_at_closed_bar"
        );
        assert!(observed["metrics"]["oi_signal"].is_null());
        assert_eq!(
            observed["source_integrity"]["historical_publication_time_verified"],
            false
        );
        assert_eq!(
            observed["source_integrity"]["metric_reexecution"]["quality"],
            "verified_reexecution"
        );
    }
    assert!(aligned["metrics"]["oi_return"].is_number());
    assert_eq!(aligned["source_alignment"]["aligned_observations"], 100);
    assert_eq!(aligned["source_alignment"]["oi_off_grid_events"], 0);
    assert_eq!(
        aligned["composition_quality"]["oi_signal"]["issues"][0]["reason"],
        "CAUSAL_PUBLICATION_TIME_UNVERIFIED"
    );

    assert!(shifted["metrics"]["oi_return"].is_null());
    assert_eq!(shifted["source_alignment"]["aligned_observations"], 99);
    assert_eq!(shifted["source_alignment"]["oi_off_grid_events"], 1);
    assert_eq!(
        shifted["source_alignment"]["oi_first_off_grid_ms"],
        42 * 60_000 + 1
    );
    assert_eq!(
        shifted["source_alignment"]["first_unaligned_ms"],
        42 * 60_000
    );
    assert_eq!(shifted["oi_coverage"]["first_missing_ms"], 42 * 60_000);
    assert_eq!(
        shifted["composition_quality"]["oi_return"]["issues"][0]["reason"],
        "OI_RANGE_INCOMPLETE_OR_GAPPED"
    );
    assert_ne!(
        aligned["source_captures"]["open_interest"]["capture_hash"],
        shifted["source_captures"]["open_interest"]["capture_hash"]
    );
    assert_eq!(
        candle_reads.load(Ordering::SeqCst),
        2,
        "each instrument reads its candles once"
    );
    assert_eq!(
        oi_reads.load(Ordering::SeqCst),
        2,
        "each instrument reads its OI once"
    );
    client.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

/// T07/T08/T10/T13: one captured 1,440-bar market is used by the real child
/// Worker for both a causal event/unknown outcome and a train-only Grid. The
/// second instrument has the identical train prefix but a different holdout.
/// Neither an outcome label nor the changed holdout can choose a strategy.
#[tokio::test]
async fn t07_t08_real_worker_intraday_event_and_holdout_stay_separate() {
    const BARS: u64 = 1_440;
    const SPLIT: u64 = BARS * 7 / 10;
    let reads = Arc::new(AtomicUsize::new(0));
    let source: SourceHandler = Arc::new({
        let reads = reads.clone();
        move |request| {
            reads.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let PublicSourceRequest::Candles {
                    instrument, after, ..
                } = request
                else {
                    return Err("UNEXPECTED_SOURCE".into());
                };
                let altered_holdout = instrument == "ETH-USDT";
                let cursor = after.as_deref().unwrap_or("0").parse::<u64>().unwrap();
                let rows = (0..BARS)
                    .rev()
                    .filter(|index| index * 60_000 < cursor)
                    .take(100)
                    .map(|index| {
                        let mut close =
                            100.0 + index as f64 * 0.03 + ((index % 13) as f64 - 6.0) * 0.8;
                        if altered_holdout && index >= SPLIT {
                            close += 30.0 + (index % 17) as f64 * 0.2;
                        }
                        // The final closed candle is a new, known event. Its
                        // two-bar future label is *not* known or available.
                        if index == BARS - 1 {
                            close = 500.0;
                        }
                        HistoryCandle {
                            open_time_ms: (index * 60_000).to_string(),
                            open: close.to_string(),
                            high: (close + 1.0).to_string(),
                            low: (close - 1.0).to_string(),
                            close: close.to_string(),
                            volume: "1".into(),
                            volume_currency: "1".into(),
                            volume_quote: None,
                            confirmed: true,
                        }
                    })
                    .collect();
                Ok(PublicSourceReply::Candles(PublicCandleCapture {
                    raw_body: b"intraday-train-holdout".to_vec(),
                    rows,
                    acquired_at_ms: "90000000".into(),
                }))
            })
        }
    });
    let (root, client) = fixture(source);
    let market_request: MarketRequest = serde_json::from_value(serde_json::json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":BARS * 60_000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"change","op":"returns_series","input":"close"},
            {"id":"jump","op":"greater_than","input":"change","threshold":0.4},
            {"id":"event","op":"rising_edge","input":"jump"},
            {"id":"label","op":"forward_return_label","input":"close","when":"event","period":2}
        ],"result_steps":["jump","event","label"]}
    }))
    .unwrap();
    assert!(market_request.valid());
    let mut market = client
        .market(market_request.clone(), None, None)
        .await
        .unwrap();
    if market["rows"].is_null() {
        let job_id = market["id"].as_str().expect("queued market job ID");
        assert_eq!(terminal(&client, job_id).await["state"], "completed");
        market = client.market(market_request, None, None).await.unwrap();
    }
    let event = &market["rows"][0];
    assert_eq!(event["candle_coverage"]["expected_bars"], BARS);
    assert_eq!(event["candle_coverage"]["observed_unique_bars"], BARS);
    assert_eq!(event["metrics"]["jump"], true);
    assert_eq!(event["metrics"]["event"], true);
    assert_eq!(
        event["composition_quality"]["event"]["status"],
        "available_at_closed_bar"
    );
    assert!(event["metrics"]["label"].is_null());
    assert_eq!(event["units"]["label"], "outcome_ratio");
    assert!(
        event["gaps"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("OUTCOME_UNCONFIRMED:label"))
    );
    assert_eq!(
        event["source_integrity"]["metric_reexecution"]["quality"],
        "verified_reexecution"
    );

    let submit = |instrument: &str, key: &str| -> ResearchRequest {
        serde_json::from_value(serde_json::json!({
            "action":"grid","idempotency_key":key,
            "instruments":[instrument],"timeframe_ms":60000,
            "start_ms":0,"end_ms":BARS * 60_000,
            "strategy_id":"close_breakout_v1",
            "variants":[{"lookback":2},{"lookback":3},{"lookback":5}],
            "costs":{"maker_fee_bps":0,"taker_fee_bps":0,
                "slippage_bps":0,"funding_mode":"zero_assumption"},
            "data_mode":"counterfactual","seed":17
        }))
        .unwrap()
    };
    let baseline_request = submit("BTC-USDT", "ipc-intraday-oos-baseline");
    let altered_request = submit("ETH-USDT", "ipc-intraday-oos-altered");
    assert!(baseline_request.valid() && altered_request.valid());
    let baseline = client.research(baseline_request).await.unwrap();
    let altered = client.research(altered_request).await.unwrap();
    let baseline_id = baseline["id"].as_str().expect("baseline job ID");
    let altered_id = altered["id"].as_str().expect("holdout job ID");
    assert_eq!(terminal(&client, baseline_id).await["state"], "completed");
    assert_eq!(terminal(&client, altered_id).await["state"], "completed");
    let view = |job_id: &str, kind: ResearchResultView| ResearchRequest::Result {
        job_id: job_id.into(),
        cursor: None,
        limit: 50,
        view: kind,
    };
    let baseline_summary = client
        .research(view(baseline_id, ResearchResultView::Summary))
        .await
        .unwrap();
    let altered_summary = client
        .research(view(altered_id, ResearchResultView::Summary))
        .await
        .unwrap();
    let before = &baseline_summary["metadata"];
    let after = &altered_summary["metadata"];
    assert_eq!(before["grid"]["split_index"], SPLIT);
    assert_eq!(after["grid"]["split_index"], SPLIT);
    assert_eq!(before["grid"]["selected"], after["grid"]["selected"]);
    assert_eq!(
        before["grid"]["train_dataset_hash"],
        after["grid"]["train_dataset_hash"]
    );
    assert_eq!(before["grid"]["train"], after["grid"]["train"]);
    assert_ne!(
        before["dataset_hash"], after["dataset_hash"],
        "only holdout observations changed"
    );
    let baseline_variants = client
        .research(view(baseline_id, ResearchResultView::Variants))
        .await
        .unwrap();
    let altered_variants = client
        .research(view(altered_id, ResearchResultView::Variants))
        .await
        .unwrap();
    assert_eq!(baseline_variants["total_rows"], 3);
    assert_eq!(altered_variants["total_rows"], 3);
    for index in 0..3 {
        let first = &baseline_variants["rows"][index];
        let second = &altered_variants["rows"][index];
        assert_eq!(first["parameters"], second["parameters"]);
        assert_eq!(first["train_result_hash"], second["train_result_hash"]);
        assert_eq!(first["train"], second["train"]);
    }
    let exported = client
        .research(view(baseline_id, ResearchResultView::Export))
        .await
        .unwrap();
    assert_eq!(exported["side_effects"], "none");
    assert!(exported["experiment_artifact"].is_string());
    assert!(exported["economic_audit"].is_object());
    assert!(
        reads.load(Ordering::SeqCst) >= 30,
        "both 1,440-bar datasets must be acquired through real paged IPC"
    );
    client.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}
