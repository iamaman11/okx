use serde_json::{Value, json};
use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn replay(timestamps: [i64; 3]) -> Output {
    let request = json!({
        "graph": {
            "version": "okx.market.composition/v1",
            "steps": [{"id": "mean", "op": "rolling_mean", "input": "close", "period": 2}],
            "result_steps": ["mean"]
        },
        "bars": [
            {"timestamp_ms": timestamps[0], "close": 2.0},
            {"timestamp_ms": timestamps[1], "close": 4.0},
            {"timestamp_ms": timestamps[2], "close": 8.0}
        ]
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

#[test]
fn replays_closed_bars_with_a_numeric_oracle() {
    let output = replay([1, 2, 3]);
    assert!(output.status.success(), "{:?}", output);
    let series: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(series[0]["values"]["mean"][0], Value::Null);
    assert_eq!(series[1]["values"]["mean"][0], json!(3.0));
    assert_eq!(series[2]["values"]["mean"][0], json!(6.0));
    assert_eq!(series[2]["timestamp_ms"], 3);
}

#[test]
fn rejects_duplicate_bars_without_publishing_partial_results() {
    let output = replay([1, 2, 2]);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
