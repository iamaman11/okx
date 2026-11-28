//! P0-C: pure logic can be composed without a new strategy executor.
use super::*;
use crate::stream::{CausalGraphStream, ClosedBar};
use serde_json::{Value, json};

fn checked(steps: Value, results: &[&str], bars: u64) -> CheckedComposition {
    let request: CompositionRequest = serde_json::from_value(json!({
        "version": COMPOSITION_VERSION,
        "steps": steps,
        "result_steps": results,
    }))
    .unwrap();
    CheckedComposition::compile(request, bars).unwrap()
}

fn rejected(steps: Value) -> String {
    let request: CompositionRequest = serde_json::from_value(json!({
        "version": COMPOSITION_VERSION,
        "steps": steps,
        "result_steps": ["bad"],
    }))
    .unwrap();
    CheckedComposition::compile(request, 20).unwrap_err()
}

fn refusal(steps: Value, expected: &str) {
    let error = rejected(steps);
    assert!(error.contains(expected), "unexpected rejection: {error}");
}

fn with_volume(
    plan: &CheckedComposition,
    close: &[f64],
    volume: &[f64],
    at: usize,
) -> CompositionValues {
    plan.evaluate_at_with_candle_sources_diagnostics(
        close,
        CandlePriceInputs {
            volume: Some(volume),
            ..Default::default()
        },
        None,
        None,
        at,
    )
    .unwrap()
    .0
}

#[test]
fn not_and_if_else_have_strict_unknowns_without_touching_inactive_arm() {
    let plan = checked(
        json!([
            {"id":"positive","op":"greater_than","input":"close","threshold":100.0},
            {"id":"negative","op":"not","input":"positive"},
            {"id":"previous","op":"lag","input":"close","period":1},
            {"id":"choice","op":"if_else","input":"close","other":"previous","when":"positive"}
        ]),
        &["positive", "negative", "previous", "choice"],
        5,
    );
    let close = [110.0, 105.0, 90.0, f64::NAN, 95.0];
    let expected = [Some(110.0), Some(105.0), Some(105.0), None, None];
    let complements = [Some(0.0), Some(0.0), Some(1.0), None, Some(1.0)];
    for index in 0..close.len() {
        let (values, causes) = plan
            .evaluate_at_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs::default(),
                None,
                None,
                index,
            )
            .unwrap();
        assert_eq!(values["choice"], (expected[index], Unit::Price));
        assert_eq!(values["negative"], (complements[index], Unit::Boolean));
        if index == 0 {
            assert_eq!(values["previous"].0, None);
            assert!(
                !causes.contains_key("choice"),
                "unselected missing branch was evaluated"
            );
        }
        if index == 3 {
            assert_eq!(causes["choice"].origin_step, "close");
            assert_eq!(causes["choice"].origin_index, 3);
        }
        if index == 4 {
            assert_eq!(causes["choice"].origin_step, "close");
            assert_eq!(causes["choice"].origin_index, 3);
        }
    }
}

#[test]
fn boolean_rolling_min_is_exact_consecutive_confirmation_without_another_operator() {
    assert_eq!(series_operation_contract(SeriesOp::RollingMin).version, 2);
    assert_eq!(series_operation_contract(SeriesOp::Not).version, 1);
    assert_eq!(series_operation_contract(SeriesOp::IfElse).version, 1);
    assert_eq!(series_operation_contract(SeriesOp::Cooldown).version, 1);
    let plan = checked(
        json!([
            {"id":"positive","op":"greater_than","input":"close","threshold":100.0},
            {"id":"confirmed","op":"rolling_min","input":"positive","period":2},
            {"id":"event","op":"rising_edge","input":"confirmed"},
            {"id":"bounded","op":"cooldown","input":"event","period":6}
        ]),
        &["confirmed", "event", "bounded"],
        10,
    );
    // A second eligible rising event occurs five bars after the first; N=6
    // suppresses it without converting the Boolean level to an Event type.
    let close = [99.0, 101.0, 102.0, 103.0, 104.0, 99.0, 101.0, 102.0, 103.0];
    let confirmed = [
        None,
        Some(0.0),
        Some(1.0),
        Some(1.0),
        Some(1.0),
        Some(0.0),
        Some(0.0),
        Some(1.0),
        Some(1.0),
    ];
    let events = [
        None,
        None,
        Some(1.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(1.0),
        Some(0.0),
    ];
    let accepted = [
        None,
        None,
        Some(1.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
        Some(0.0),
    ];
    let mut stream = CausalGraphStream::new(&plan).unwrap();
    for index in 0..close.len() {
        let batch = plan.evaluate_at(&close[..=index], index);
        let live = stream
            .on_closed_bar(ClosedBar {
                timestamp_ms: index as i64 * 60_000,
                close: Some(close[index]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(batch, live, "batch/stream diverged at bar {index}");
        assert_eq!(batch["confirmed"], (confirmed[index], Unit::Boolean));
        assert_eq!(batch["event"], (events[index], Unit::Event));
        assert_eq!(batch["bounded"], (accepted[index], Unit::Event));
    }
    assert_eq!(
        stream.operation_count(),
        close.len() as u64 * plan.steps().len() as u64
    );
    // RollingMin is a strict complete-window operator: an unknown observation
    // stays unknown even when another observation in that window is false.
    let missing = [101.0, f64::NAN, 99.0, 102.0];
    assert_eq!(plan.evaluate_at(&missing, 2)["confirmed"].0, None);
    assert_eq!(plan.evaluate_at(&missing, 3)["confirmed"].0, Some(0.0));
}

#[test]
fn missing_event_resets_cooldown_without_synthesizing_observations() {
    let plan = checked(
        json!([
            {"id":"high","op":"greater_than","input":"close","threshold":100.0},
            {"id":"event","op":"rising_edge","input":"high"},
            {"id":"bounded","op":"cooldown","input":"event","period":10}
        ]),
        &["bounded"],
        6,
    );
    let close = [90.0, 110.0, 110.0, f64::NAN, 90.0, 110.0];
    let expected = [None, Some(1.0), Some(0.0), None, None, Some(1.0)];
    let mut stream = CausalGraphStream::new(&plan).unwrap();
    for index in 0..close.len() {
        let batch = plan.evaluate_at(&close[..=index], index);
        let live = stream
            .on_closed_bar(ClosedBar {
                timestamp_ms: index as i64 * 60_000,
                close: close[index].is_finite().then_some(close[index]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(batch, live);
        assert_eq!(batch["bounded"].0, expected[index]);
    }
}

#[test]
fn reversal_volume_regime_graph_is_composable_causal_and_streaming() {
    let plan = checked(
        json!([
            {"id":"returns","op":"returns_series","input":"close"},
            {"id":"up","op":"greater_than","input":"returns","threshold":0.0},
            {"id":"down","op":"not","input":"up"},
            {"id":"streak","op":"rolling_min","input":"up","period":2},
            {"id":"volume_ok","op":"greater_than","input":"volume","threshold":135.0},
            {"id":"regime_ok","op":"greater_than","input":"close","threshold":95.0},
            {"id":"qualified","op":"and","input":"volume_ok","other":"regime_ok"},
            {"id":"selected","op":"if_else","input":"streak","other":"down","when":"qualified"},
            {"id":"trigger","op":"rising_edge","input":"selected"},
            {"id":"signal","op":"cooldown","input":"trigger","period":5}
        ]),
        &["selected", "trigger", "signal"],
        10,
    );
    let closes = [100.0, 96.0, 92.0, 94.0, 97.0, 100.0, 99.0, 101.0, 103.0, 102.0];
    let volume = [100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0, 190.0];
    let mut streaming = CausalGraphStream::new(&plan).unwrap();
    for index in 0..closes.len() {
        let historical = with_volume(&plan, &closes, &volume, index);
        let incremental = streaming
            .on_closed_bar(ClosedBar {
                timestamp_ms: index as i64 * 60_000,
                close: Some(closes[index]),
                volume: Some(volume[index]),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            incremental, historical,
            "different logic results at index {index}"
        );
        assert_eq!(historical["selected"].1, Unit::Boolean);
        assert_eq!(historical["signal"].1, Unit::Event);
    }
    assert_eq!(with_volume(&plan, &closes, &volume, 4)["signal"].0, Some(1.0));
    assert_eq!(with_volume(&plan, &closes, &volume, 8)["signal"].0, Some(0.0));
    // Future poison on both sources is not allowed to alter past decisions.
    let mut future_prices = closes;
    let mut future_volume = volume;
    future_prices[5..].fill(1_000_000.0);
    future_volume[5..].fill(0.0);
    let baseline = with_volume(&plan, &closes, &volume, 4);
    let poisoned = with_volume(&plan, &future_prices, &future_volume, 4);
    assert_eq!(baseline, poisoned);
}

#[test]
fn typed_logic_rejects_incompatible_branches_outcomes_and_uncausal_conditions() {
    refusal(
        json!([{"id":"bad","op":"not","input":"close"}]),
        "input:UNIT_MISMATCH:expected=boolean",
    );
    refusal(
        json!([
            {"id":"condition","op":"greater_than","input":"close","threshold":1.0},
            {"id":"bad","op":"if_else","input":"close","other":"volume","when":"condition"}
        ]),
        "other:UNIT_MISMATCH",
    );
    refusal(
        json!([{"id":"bad","op":"if_else","input":"close","other":"close","when":"close"}]),
        "when:UNIT_MISMATCH:expected=boolean",
    );
    refusal(
        json!([{"id":"bad","op":"if_else","input":"close","other":"close","when":"future"}]),
        "when:UNKNOWN_OR_FORWARD_STEP:future",
    );
    refusal(
        json!([{"id":"bad","op":"cooldown","input":"close","period":3}]),
        "input:UNIT_MISMATCH:expected=event",
    );
    refusal(
        json!([
            {"id":"condition","op":"greater_than","input":"close","threshold":1.0},
            {"id":"event","op":"rising_edge","input":"condition"},
            {"id":"bad","op":"cooldown","input":"event"}
        ]),
        "period:INVALID",
    );
    refusal(
        json!([
            {"id":"condition","op":"greater_than","input":"close","threshold":100.0},
            {"id":"event","op":"rising_edge","input":"condition"},
            {"id":"label","op":"forward_return_label","input":"close","when":"event","period":1},
            {"id":"bad","op":"if_else","input":"label","other":"close","when":"condition"}
        ]),
        "outcome_ratio",
    );
}

/// Opt-in direct evidence for the new logic atoms; no noisy wall-time CI gate.
#[test]
#[ignore = "manual microbenchmark"]
fn not_if_else_cooldown_microbenchmark_reports_median_ns_per_bar() {
    use std::hint::black_box;
    use std::time::Instant;

    let plan = checked(
        json!([
            {"id":"positive","op":"greater_than","input":"close","threshold":100.0},
            {"id":"negative","op":"not","input":"positive"},
            {"id":"volume_ok","op":"greater_than","input":"volume","threshold":50.0},
            {"id":"chosen","op":"if_else","input":"positive","other":"negative","when":"volume_ok"},
            {"id":"event","op":"rising_edge","input":"chosen"},
            {"id":"signal","op":"cooldown","input":"event","period":5}
        ]),
        &["signal"],
        100_000,
    );
    for bars in [10_000_usize, 100_000] {
        let mut samples = [0_u128; 5];
        for sample in &mut samples {
            let mut runner = CausalGraphStream::new(&plan).unwrap();
            let mut checksum = 0.0;
            let started = Instant::now();
            for index in 0..bars {
                let close = black_box(if index % 19 < 9 { 101.0 } else { 99.0 });
                let volume = black_box(if index % 7 < 3 { 100.0 } else { 10.0 });
                let values = runner
                    .on_closed_bar(ClosedBar {
                        timestamp_ms: index as i64 * 60_000,
                        close: Some(close),
                        volume: Some(volume),
                        ..Default::default()
                    })
                    .unwrap();
                checksum += black_box(values["signal"].0.unwrap_or(0.0));
            }
            *sample = started.elapsed().as_nanos() / bars as u128;
            assert_eq!(runner.operation_count(), bars as u64 * plan.steps().len() as u64);
            assert!(black_box(checksum).is_finite());
        }
        samples.sort_unstable();
        println!(
            "bars={bars} not+if_else+cooldown_ns/bar={} median_of=5",
            samples[samples.len() / 2]
        );
    }
}
