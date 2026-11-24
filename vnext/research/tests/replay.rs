use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn run_replay_graph(timeframe_ms: i64, bars: Value, graph: Value) -> Output {
    let request = json!({
        "timeframe_ms": timeframe_ms,
        "graph": graph,
        "bars": bars
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_okx-research-replay"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.to_string().as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn run_replay(timeframe_ms: i64, bars: Value) -> Output {
    run_replay_graph(
        timeframe_ms,
        bars,
        json!({
            "version": "okx.market.composition/v1",
            "steps": [{"id": "mean", "op": "rolling_mean", "input": "close", "period": 2}],
            "result_steps": ["mean"]
        }),
    )
}

#[test]
fn three_data_only_hypotheses_replay_identical_closed_source_with_distinct_oracles() {
    // Same immutable recorded source, same installed binary, only typed QueryPlan
    // data changes. This is a standalone CLI witness, not yet MCP/API U01.
    let bars = json!(
        (1..=6)
            .map(|index| json!({
                "timestamp_ms": index * 60_000,
                "available_at_ms": (index + 1) * 60_000,
                "close": 3.0 * index as f64,
            }))
            .collect::<Vec<_>>()
    );
    let source_bytes = serde_json::to_vec(&bars).unwrap();
    let mut observed = Vec::new();
    for (period, expected) in [(2, 16.5), (3, 15.0), (6, 10.5)] {
        let graph = json!({
            "version": "okx.market.composition/v1",
            "steps": [{"id": "mean", "op": "rolling_mean", "input": "close", "period": period}],
            "result_steps": ["mean"]
        });
        assert_eq!(serde_json::to_vec(&bars).unwrap(), source_bytes);
        let first = run_replay_graph(60_000, bars.clone(), graph.clone());
        let repeated = run_replay_graph(60_000, bars.clone(), graph);
        assert!(first.status.success(), "period={period}: {:?}", first);
        assert!(repeated.status.success(), "period={period}: {:?}", repeated);
        assert_eq!(first.stdout, repeated.stdout, "nondeterministic replay");
        let series: Value = serde_json::from_slice(&first.stdout).unwrap();
        assert_eq!(series.as_array().unwrap().len(), 6);
        assert_eq!(series[5]["timestamp_ms"], 360_000);
        assert_eq!(series[5]["available_at_ms"], 420_000);
        assert_eq!(series[5]["values"]["mean"][0], json!(expected));
        observed.push(series[5]["values"]["mean"][0].clone());
    }
    assert_ne!(observed[0], observed[1]);
    assert_ne!(observed[1], observed[2]);
    assert_ne!(observed[0], observed[2]);
}

fn replay(timestamps: [i64; 3]) -> Output {
    run_replay(
        60_000,
        json!([
            {"timestamp_ms": timestamps[0], "available_at_ms": timestamps[0] + 60_000, "close": 2.0},
            {"timestamp_ms": timestamps[1], "available_at_ms": timestamps[1] + 60_000, "close": 4.0},
            {"timestamp_ms": timestamps[2], "available_at_ms": timestamps[2] + 60_000, "close": 8.0}
        ]),
    )
}

#[test]
fn replays_closed_bars_with_a_numeric_oracle() {
    let output = replay([60_000, 120_000, 180_000]);
    assert!(output.status.success(), "{:?}", output);
    let series: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(series[0]["values"]["mean"][0], Value::Null);
    assert_eq!(series[1]["values"]["mean"][0], json!(3.0));
    assert_eq!(series[2]["values"]["mean"][0], json!(6.0));
    assert_eq!(series[2]["timestamp_ms"], 180_000);
    assert_eq!(series[2]["available_at_ms"], 240_000);
}

#[test]
fn rejects_duplicate_bars_without_publishing_partial_results() {
    let output = replay([60_000, 120_000, 120_000]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("GAP_OR_REORDER"));
}

#[test]
fn rejects_missing_and_off_grid_bars_before_publishing_any_result() {
    for timestamps in [
        [60_000, 180_000, 240_000],
        [60_000, 120_001, 180_000],
        [60_000, 180_000, 120_000],
    ] {
        let output = replay(timestamps);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn rejects_future_unavailable_or_temporally_reordered_observations() {
    let unavailable = run_replay(
        60_000,
        json!([{"timestamp_ms": 60_000, "available_at_ms": 119_999, "close": 2.0}]),
    );
    assert!(!unavailable.status.success());
    assert!(unavailable.stdout.is_empty());
    assert!(String::from_utf8_lossy(&unavailable.stderr).contains("BAR_NOT_CLOSED_OR_AVAILABLE"));

    let reordered = run_replay(
        60_000,
        json!([
            {"timestamp_ms": 60_000, "available_at_ms": 600_000, "close": 2.0},
            {"timestamp_ms": 120_000, "available_at_ms": 180_000, "close": 4.0}
        ]),
    );
    assert!(!reordered.status.success());
    assert!(reordered.stdout.is_empty());
    assert!(String::from_utf8_lossy(&reordered.stderr).contains("AVAILABILITY_REORDERED"));
}

#[test]
fn rejects_unsupported_timeframe_at_deserialization_boundary() {
    let output = run_replay(
        1,
        json!([{"timestamp_ms": 1, "available_at_ms": 2, "close": 2.0}]),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("UNSUPPORTED_TIMEFRAME"));
}

#[test]
fn missing_close_remains_unknown_through_a_real_replay() {
    let output = run_replay(
        60_000,
        json!([
            {"timestamp_ms": 60_000, "available_at_ms": 120_000, "close": 2.0},
            {"timestamp_ms": 120_000, "available_at_ms": 180_000, "close": null},
            {"timestamp_ms": 180_000, "available_at_ms": 240_000, "close": 8.0}
        ]),
    );
    assert!(output.status.success(), "{:?}", output);
    let values: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(values[1]["values"]["mean"][0], Value::Null);
    assert_eq!(values[2]["values"]["mean"][0], Value::Null);
}
