use crate::research_client::{ResearchClient, ResearchClientConfig, SourceHandler};
use okx_protocol::{
    analytics::*,
    research_ipc::{
        HistoryCandle, PublicCandleCapture, PublicSourceReply, PublicSourceRequest, ResearchScope,
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
        std::env::var_os("OKX_RESEARCH_WORKER_TEST_EXE")
            .expect("set OKX_RESEARCH_WORKER_TEST_EXE to the built research worker"),
    );
    assert!(executable.is_file(), "worker test executable is missing");
    let root = std::env::temp_dir().join(format!(
        "okx-research-client-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
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
            .map(|slow| EmaParameters { fast: 2, slow })
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
        parameters: EmaParameters { fast: 2, slow: 3 },
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
