//! T07/T08 real-process witnesses owned by Research. The Engine no longer
//! implements a second Research client or imports its private catalog types.
//! All product requests below pass through the actual Worker IPC.

use super::research_legacy_witnesses::RealWorker;
use okx_research_protocol::{
    analytics::{MarketRequest, ResearchRequest, ResearchResultView},
    research_ipc::{
        HistoryCandle, JobId, PublicCandleCapture, PublicSourceReply, PublicSourceRequest,
        ResearchCommand, WorkerReply,
    },
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

const BARS: u64 = 1_440;
const SPLIT: u64 = BARS * 7 / 10;

fn intraday_source(request: PublicSourceRequest) -> Result<PublicSourceReply, String> {
    let PublicSourceRequest::Candles {
        instrument, after, ..
    } = request
    else {
        return Err("UNEXPECTED_SOURCE".into());
    };
    let cursor = after
        .as_deref()
        .ok_or("SOURCE_CURSOR_MISSING")?
        .parse::<u64>()
        .map_err(|_| "SOURCE_CURSOR_INVALID")?;
    let changed_holdout = instrument == "ETH-USDT";
    let rows = (0..BARS)
        .rev()
        .filter(|index| index * 60_000 < cursor)
        .take(100)
        .map(|index| {
            let mut close = 100.0 + index as f64 * 0.03 + ((index % 13) as f64 - 6.0) * 0.8;
            if changed_holdout && index >= SPLIT {
                close += 30.0 + (index % 17) as f64 * 0.2;
            }
            // The last closed bar gives a causal event, but its forward
            // outcome does not exist at the observation boundary.
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
        raw_body: b"research-real-worker-intraday-train-holdout".to_vec(),
        rows,
        acquired_at_ms: "90000000".into(),
        http_delivery: None,
    }))
}

fn grid(instrument: &str, key: &str) -> ResearchRequest {
    serde_json::from_value(json!({
        "action":"grid", "idempotency_key":key,
        "instruments":[instrument], "timeframe_ms":60000,
        "start_ms":0, "end_ms":BARS * 60_000,
        "strategy_id":"close_breakout_v1",
        "variants":[{"lookback":2},{"lookback":3},{"lookback":5}],
        "costs":{"maker_fee_bps":0,"taker_fee_bps":0,
                 "slippage_bps":0,"funding_mode":"zero_assumption"},
        "data_mode":"counterfactual", "seed":17
    }))
    .expect("typed grid")
}

fn result(job: &str, view: ResearchResultView) -> ResearchRequest {
    ResearchRequest::Result {
        job_id: job.to_owned(),
        cursor: None,
        limit: 50,
        view,
    }
}

async fn materialized_market(worker: &mut RealWorker, request: MarketRequest) -> Value {
    let initial = worker
        .market(request.clone())
        .await
        .expect("market admission");
    if initial["rows"].is_null() {
        let job = initial["id"].as_str().expect("market job ID");
        assert_eq!(worker.terminal(job).await["state"], "completed");
    }
    let materialized = worker
        .market(request)
        .await
        .expect("market materialization");
    assert!(materialized["rows"].is_array(), "{materialized}");
    materialized
}

fn market_request() -> MarketRequest {
    serde_json::from_value(json!({
        "action":"screen", "instruments":["BTC-USDT"],
        "fields":["returns"], "timeframe_ms":60000,
        "start_ms":0, "end_ms":BARS * 60_000,
        "composition":{"version":"okx.market.composition/v1","steps":[
            {"id":"change","op":"returns_series","input":"close"},
            {"id":"jump","op":"greater_than","input":"change","threshold":0.4},
            {"id":"event","op":"rising_edge","input":"jump"},
            {"id":"label","op":"forward_return_label","input":"close",
             "when":"event","period":2}
        ],"result_steps":["jump","event","label"]}
    }))
    .expect("typed intraday causal market request")
}

pub(super) async fn t07_t08_market_and_catalog_witnesses() {
    let mut worker =
        RealWorker::open("t07-t08-independent-holdout", Box::new(intraday_source)).await;

    // T07: event is known on the last CLOSED 1m bar; a two-bar forward
    // return is unavailable, so Research cannot manufacture a win label.
    let causal = market_request();
    assert!(causal.valid());
    let event_market = materialized_market(&mut worker, causal).await;
    let event = &event_market["rows"][0];
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
            .contains(&json!("OUTCOME_UNCONFIRMED:label"))
    );
    assert_eq!(
        event["source_integrity"]["metric_reexecution"]["quality"],
        "verified_reexecution"
    );

    // T08: two actual CPU Grids have identical train bars and a different
    // holdout. Selection and training hashes may not depend on the holdout.
    let baseline_request = grid("BTC-USDT", "research-real-holdout-baseline");
    let altered_request = grid("ETH-USDT", "research-real-holdout-altered");
    assert!(baseline_request.valid() && altered_request.valid());
    let first = worker.research(baseline_request.clone()).await.unwrap();
    let second = worker.research(altered_request).await.unwrap();
    let first_id = first["id"].as_str().expect("baseline grid ID").to_owned();
    let second_id = second["id"].as_str().expect("altered grid ID").to_owned();
    assert_ne!(first_id, second_id);
    assert_eq!(worker.terminal(&first_id).await["state"], "completed");
    assert_eq!(worker.terminal(&second_id).await["state"], "completed");
    let baseline = worker
        .research(result(&first_id, ResearchResultView::Summary))
        .await
        .unwrap();
    let altered = worker
        .research(result(&second_id, ResearchResultView::Summary))
        .await
        .unwrap();
    let before = &baseline["metadata"];
    let after = &altered["metadata"];
    assert_eq!(before["grid"]["split_index"], SPLIT);
    assert_eq!(after["grid"]["split_index"], SPLIT);
    for key in ["selected", "train_dataset_hash", "train"] {
        assert_eq!(
            before["grid"][key], after["grid"][key],
            "holdout influenced {key}"
        );
    }
    assert_ne!(before["dataset_hash"], after["dataset_hash"]);
    let baseline_variants = worker
        .research(result(&first_id, ResearchResultView::Variants))
        .await
        .unwrap();
    let altered_variants = worker
        .research(result(&second_id, ResearchResultView::Variants))
        .await
        .unwrap();
    assert_eq!(baseline_variants["total_rows"], 3);
    assert_eq!(altered_variants["total_rows"], 3);
    for index in 0..3 {
        let left = &baseline_variants["rows"][index];
        let right = &altered_variants["rows"][index];
        for key in ["parameters", "train_result_hash", "train"] {
            assert_eq!(
                left[key], right[key],
                "holdout changed variant {index} {key}"
            );
        }
    }
    let export = worker
        .research(result(&first_id, ResearchResultView::Export))
        .await
        .unwrap();
    assert_eq!(export["side_effects"], "none");
    assert!(export["experiment_artifact"].is_string());
    assert!(export["economic_audit"].is_object());
    assert!(
        worker.source_reads() >= 30,
        "1,440-bar datasets require real paged IPC reads"
    );

    // A second SQLite connection takes a writer lease, without writing.
    // Read-only status/diagnostics over the *actual* Worker IPC still respond
    // within 500 ms, and neither side creates database rows or fetches data.
    let catalog_path = worker.root().join("research-catalog.sqlite");
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let blocker = std::thread::spawn(move || {
        let connection = rusqlite::Connection::open(catalog_path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        let changes = connection.total_changes();
        locked_tx.send(changes).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("release read witness");
        assert_eq!(
            connection.total_changes(),
            changes,
            "locked catalog was mutated"
        );
        connection.execute_batch("ROLLBACK").unwrap();
    });
    let _changes_before = locked_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("external catalog writer lock was not acquired");
    let prior_reads = worker.source_reads();
    let began = Instant::now();
    let status = tokio::time::timeout(
        Duration::from_millis(500),
        worker.research(ResearchRequest::Status {
            job_id: first_id.clone(),
        }),
    )
    .await
    .expect("read-only Research status blocked behind SQLite writer")
    .unwrap();
    assert_eq!(status["state"], "completed");
    assert!(began.elapsed() < Duration::from_millis(500));
    let explain_start = Instant::now();
    let explained = tokio::time::timeout(
        Duration::from_millis(500),
        worker.call(ResearchCommand::Explain {
            job_id: JobId::new(first_id.clone()).unwrap(),
        }),
    )
    .await
    .expect("read-only Research diagnostic blocked behind SQLite writer")
    .unwrap();
    let WorkerReply::Product(explanation) = explained else {
        panic!("Research diagnostic returned a non-product frame");
    };
    assert!(
        explanation.is_object(),
        "diagnostic lost its bounded read-only response"
    );
    assert!(explain_start.elapsed() < Duration::from_millis(500));
    assert_eq!(worker.source_reads(), prior_reads);
    release_tx.send(()).unwrap();
    blocker.join().unwrap();

    // Three independently composed graphs have distinct stable identities.
    // Re-asking for each graph is a durable replay, not an Engine-side graph
    // or another public-source fetch. The original Worker can restart and
    // still replay the same catalogued result byte-for-byte.
    let base = json!({
        "action":"screen","instruments":["BTC-USDT"],"fields":["returns"],
        "timeframe_ms":60000,"start_ms":0,"end_ms":BARS * 60_000,"limit":1
    });
    let mut range = base.clone();
    range["composition"] = json!({"version":"okx.market.composition/v1","steps":[
        {"id":"width","op":"rolling_range","input":"close","period":3}
    ],"result_steps":["width"]});
    let mut edge = base.clone();
    edge["composition"] = json!({"version":"okx.market.composition/v1","steps":[
        {"id":"change","op":"returns_series","input":"close"},
        {"id":"avg3","op":"rolling_mean","input":"change","period":3},
        {"id":"positive","op":"greater_than","input":"avg3","threshold":0},
        {"id":"turn_on","op":"rising_edge","input":"positive"}
    ],"result_steps":["avg3","turn_on"]});
    let mut inputs = Vec::new();
    let mut fingerprints = BTreeSet::new();
    let mut jobs = BTreeSet::new();
    let mut artifacts = BTreeSet::new();
    for value in [base, range, edge] {
        let query: MarketRequest = serde_json::from_value(value).unwrap();
        assert!(query.valid());
        let completed = materialized_market(&mut worker, query.clone()).await;
        let replayed = worker.market(query.clone()).await.unwrap();
        for field in [
            "job_id",
            "market_request_fingerprint",
            "result_artifact",
            "rows",
        ] {
            assert_eq!(
                replayed[field], completed[field],
                "in-process graph replay drift: {field}"
            );
        }
        fingerprints.insert(
            completed["market_request_fingerprint"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
        jobs.insert(completed["job_id"].as_str().unwrap().to_owned());
        artifacts.insert(completed["result_artifact"].as_str().unwrap().to_owned());
        inputs.push((query, completed));
    }
    assert_eq!(
        fingerprints.len(),
        3,
        "three graphs shared a request fingerprint"
    );
    assert_eq!(jobs.len(), 3, "three graphs shared a job");
    assert_eq!(artifacts.len(), 3, "three graphs shared an artifact");
    assert!(inputs[0].1["rows"][0]["metrics"].get("width").is_none());
    assert!(inputs[1].1["rows"][0]["metrics"]["width"].is_number());
    assert!(inputs[2].1["rows"][0]["metrics"]["avg3"].is_number());
    assert!(inputs[2].1["rows"][0]["metrics"]["turn_on"].is_boolean());
    let before_restart_reads = worker.source_reads();
    worker = worker.restart().await;
    for (query, completed) in inputs {
        let replayed = worker.market(query).await.unwrap();
        for field in [
            "job_id",
            "market_request_fingerprint",
            "result_artifact",
            "rows",
        ] {
            assert_eq!(
                replayed[field], completed[field],
                "restarted Worker replay drift: {field}"
            );
        }
    }
    assert_eq!(
        worker.source_reads(),
        before_restart_reads,
        "replaying immutable graphs after restart refetched public sources"
    );
    assert_eq!(
        worker.research(baseline_request).await.unwrap()["id"],
        first_id,
        "grid semantic identity changed after restart"
    );
    worker.close().await;
}
