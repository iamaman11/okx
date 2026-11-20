use super::*;
use okx_protocol::analytics::{EmaParameters, FundingMode, ResearchCosts, ResearchDataMode};
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicUsize;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn research_warm_cancelled_explain_measurement() {
    let total = std::time::Instant::now();
    let executable = PathBuf::from(
        std::env::var_os("OKX_RESEARCH_WORKER_TEST_EXE")
            .expect("set OKX_RESEARCH_WORKER_TEST_EXE to the built Worker"),
    );
    let hash = format!("{:x}", Sha256::digest(std::fs::read(&executable).unwrap()));
    let root = std::env::temp_dir().join(format!("okx-research-measure-{}", unique()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let source: SourceHandler = Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        move |_| {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                Err("MEASUREMENT_SOURCE_RELEASED".into())
            })
        }
    });
    let client = ResearchClient::new(
        ResearchClientConfig {
            executable,
            root: root.clone(),
            scope: ResearchScope::new("research-measurement".into()).unwrap(),
        },
        source,
    );
    let cold = std::time::Instant::now();
    client.capabilities().await.expect("start real Worker");
    let cold_start_ms = cold.elapsed().as_secs_f64() * 1000.0;
    let setup = std::time::Instant::now();
    let request = ResearchRequest::Backtest {
        idempotency_key: "warm-explain-measurement".into(),
        instruments: vec!["BTC-USDT".into()],
        timeframe_ms: 60_000,
        start_ms: 0,
        end_ms: 12_000_000,
        strategy_id: "ema_cross_v1".into(),
        parameters: EmaParameters { fast: 2, slow: 3 }.into(),
        costs: ResearchCosts {
            maker_fee_bps: 1.0,
            taker_fee_bps: 2.0,
            slippage_bps: 1.0,
            funding_mode: FundingMode::ZeroAssumption,
        },
        data_mode: ResearchDataMode::Counterfactual,
        seed: 7,
    };
    let accepted = with_invocation("measure-invocation", client.research(request))
        .await
        .unwrap();
    let id = accepted["id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("source was not requested");
    let cancelled = client
        .research(ResearchRequest::Cancel {
            job_id: id.clone(),
            idempotency_key: "measure-cancel".into(),
        })
        .await
        .unwrap();
    assert_eq!(cancelled["state"], "cancelled");
    let before = calls.load(Ordering::SeqCst);
    let setup_ms = setup.elapsed().as_secs_f64() * 1000.0;
    let mut samples = Vec::with_capacity(100);
    for _ in 0..100 {
        let start = std::time::Instant::now();
        let reply = client
            .diagnostic_job(&id)
            .await
            .expect("diagnostic failed or timed out");
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(reply["job"]["id"], id);
        assert_eq!(reply["job"]["state"], "cancelled");
        assert_eq!(reply["job"]["origin_invocation"], "measure-invocation");
        assert_eq!(reply["job"]["producer"]["binary_sha256"], hash);
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        before,
        "explain requested a source"
    );
    samples.sort_by(f64::total_cmp);
    let p95 = samples[94];
    release.notify_one();
    let shutdown = std::time::Instant::now();
    client.shutdown().await;
    let shutdown_ms = shutdown.elapsed().as_secs_f64() * 1000.0;
    drop(client);
    std::fs::remove_dir_all(root).unwrap();
    println!(
        "RESEARCH_DIAGNOSTIC_MEASUREMENTS {}",
        serde_json::json!({
            "schema":"okx.512.research-diagnostic-measurement/v1", "cold_start_ms":cold_start_ms,
            "setup_ms":setup_ms, "warm_reads":100, "warm_p95_ms":p95,
            "warm_max_ms":samples[99], "shutdown_ms":shutdown_ms,
            "total_including_teardown_ms":total.elapsed().as_secs_f64()*1000.0,
            "source_calls_before":before, "source_calls_after":calls.load(Ordering::SeqCst),
            "worker_sha256":hash, "required_p95_ms":100
        })
    );
    assert!(
        p95 <= 100.0,
        "research warm explain p95 {p95}ms exceeds 100ms"
    );
}
