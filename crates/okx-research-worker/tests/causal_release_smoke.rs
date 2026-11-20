//! #518: the accepted Research-only release must preserve the public
//! causal-graph wire contract without borrowing financial Engine authority.
//! The full request is decoded just as the Worker receives it over IPC.

use okx_kernel::query_plan::{CheckedComposition, Unit};
use okx_protocol::analytics::MarketRequest;
use serde_json::{Value, json};

fn causal_market_wire() -> Value {
    json!({
        "action": "screen",
        "instruments": ["BTC-USDT-SWAP"],
        "fields": ["returns"],
        "timeframe_ms": 60_000,
        "start_ms": 60_000,
        "end_ms": 480_000,
        "limit": 1,
        "composition": {
            "version": "okx.market.composition/v1",
            "steps": [
                {"id": "ret", "op": "returns_series", "input": "close"},
                {"id": "lag1", "op": "lag", "input": "ret", "period": 1},
                {"id": "condition", "op": "greater_than", "input": "lag1", "threshold": 0.0},
                {"id": "event", "op": "rising_edge", "input": "condition"},
                {"id": "label", "op": "forward_return_label", "input": "close", "period": 2, "when": "event"}
            ],
            "result_steps": ["lag1", "event", "label"]
        }
    })
}

fn compile_worker_wire(wire: Value) -> Result<CheckedComposition, String> {
    let request: MarketRequest = serde_json::from_value(wire).expect("typed market request");
    assert!(request.valid(), "public market request constraints");
    CheckedComposition::compile(*request.composition.expect("explicit composition"), 7)
}

#[test]
fn public_market_wire_keeps_causal_prefix_and_separates_future_outcome() {
    let checked = compile_worker_wire(causal_market_wire()).expect("checked graph");
    assert_eq!(checked.estimated_cells, 35);
    assert_eq!(
        checked.steps.last().expect("label step").unit,
        Unit::OutcomeRatio
    );

    // At index 4 a positive lagged return follows a negative one: an edge.
    // Appending later bars must not alter an already-observed causal feature.
    let prefix = [100.0, 102.0, 98.0, 99.0, 100.0];
    let extended = [100.0, 102.0, 98.0, 99.0, 100.0, 101.0, 110.0];
    let before = checked.evaluate_at(&prefix, 4);
    let after = checked.evaluate_at(&extended, 4);
    assert_eq!(before["lag1"], after["lag1"]);
    assert_eq!(before["event"], after["event"]);
    assert_eq!(after["event"], (Some(1.0), Unit::Event));

    // The outcome is not available on the prefix and cannot be projected at
    // the newest closed bar even when older event labels are confirmed.
    assert_eq!(before["label"], (None, Unit::OutcomeRatio));
    assert_eq!(
        checked.evaluate(&extended)["label"],
        (None, Unit::OutcomeRatio)
    );
    let (confirmed, unit) = after["label"];
    assert_eq!(unit, Unit::OutcomeRatio);
    assert!((confirmed.expect("two future bars") - (110.0 / 100.0 - 1.0)).abs() < 1e-12);
}

#[test]
fn public_market_wire_rejects_direct_and_indirect_future_leaks_before_sources() {
    let mut direct = causal_market_wire();
    direct["composition"]["steps"]
        .as_array_mut()
        .expect("steps")
        .push(json!({
            "id": "leak", "op": "greater_than", "input": "label", "threshold": 0.0
        }));
    direct["composition"]["result_steps"] = json!(["leak"]);
    assert_eq!(
        compile_worker_wire(direct).unwrap_err(),
        "composition.steps[5].input:UNIT_MISMATCH:expected=numeric_series:actual=outcome_ratio"
    );

    let mut indirect = causal_market_wire();
    indirect["composition"]["steps"]
        .as_array_mut()
        .expect("steps")
        .push(json!({
            "id": "bridge", "op": "lag", "input": "label", "period": 1
        }));
    indirect["composition"]["result_steps"] = json!(["bridge"]);
    assert_eq!(
        compile_worker_wire(indirect).unwrap_err(),
        "composition.steps[5].input:UNIT_MISMATCH:expected=causal_series:actual=outcome_ratio"
    );
}
