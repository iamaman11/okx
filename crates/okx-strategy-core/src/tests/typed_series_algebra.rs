use super::*;
use crate::operators::EmaKernel;
use crate::stream::{CausalGraphStream, ClosedBar};
use okx_research_protocol::strategy_contract::EmaParameters;
use serde_json::json;

fn graph(steps: serde_json::Value, results: &[&str]) -> CheckedComposition {
    let request: CompositionRequest = serde_json::from_value(json!({
        "version": COMPOSITION_VERSION,
        "steps": steps,
        "result_steps": results,
    }))
    .unwrap();
    CheckedComposition::compile(request, 100).unwrap()
}

#[test]
fn bollinger_bands_are_composed_from_dimensional_price_series() {
    let plan = graph(
        json!([
            {"id":"mean","op":"rolling_mean","input":"close","period":3},
            {"id":"stddev","op":"rolling_stddev","input":"close","period":3},
            {"id":"positive","op":"scale_series","input":"stddev","factor":2.0},
            {"id":"negative","op":"scale_series","input":"stddev","factor":-2.0},
            {"id":"upper","op":"add_series","input":"mean","other":"positive"},
            {"id":"lower","op":"add_series","input":"mean","other":"negative"}
        ]),
        &["upper", "lower"],
    );
    let result = plan.evaluate(&[1.0, 2.0, 3.0]);
    let spread = 2.0 * (2.0_f64 / 3.0).sqrt();
    assert!((result["upper"].0.unwrap() - (2.0 + spread)).abs() < 1e-12);
    assert!((result["lower"].0.unwrap() - (2.0 - spread)).abs() < 1e-12);
    assert_eq!(result["upper"].1, Unit::Price);
    assert_eq!(result["lower"].1, Unit::Price);
    assert_eq!(series_operation_contract(SeriesOp::AddSeries).version, 2);
}

#[test]
fn price_plus_ratio_and_invalid_factors_fail_before_source_reads() {
    let parse = |steps| {
        let request: CompositionRequest = serde_json::from_value(json!({
            "version": COMPOSITION_VERSION,
            "steps": steps,
            "result_steps": ["bad"]
        }))
        .unwrap();
        CheckedComposition::compile(request, 100).unwrap_err()
    };
    assert!(parse(json!([
        {"id":"returns","op":"returns_series","input":"close"},
        {"id":"bad","op":"add_series","input":"close","other":"returns"}
    ])).contains("other:UNIT_MISMATCH:expected=price:actual=ratio"));
    assert!(parse(json!([
        {"id":"bad","op":"scale_series","input":"close"}
    ])).contains("factor:FINITE_REQUIRED"));
    assert!(parse(json!([
        {"id":"bad","op":"rolling_mean","input":"close","period":3,"factor":2.0}
    ])).contains("factor:UNEXPECTED"));
}

#[test]
fn independent_ema_nodes_match_registered_cross_and_stream_after_warmup() {
    let plan = graph(
        json!([
            {"id":"fast","op":"ema_series","input":"close","period":2},
            {"id":"slow","op":"ema_series","input":"close","period":3},
            {"id":"above","op":"greater_than_series","input":"fast","other":"slow"},
            {"id":"below","op":"less_than_series","input":"fast","other":"slow"}
        ]),
        &["fast", "slow", "above", "below"],
    );
    let prices = [100.0, 102.0, 101.0, 105.0, 103.0, 98.0, 99.0];
    let mut registered = EmaKernel::new(EmaParameters { fast: 2, slow: 3 }).unwrap();
    let mut streaming = CausalGraphStream::new(&plan).unwrap();
    for (index, price) in prices.into_iter().enumerate() {
        let signal = registered.push_closed(price).unwrap();
        let batch = plan.evaluate_at(&prices[..=index], index);
        let streamed = streaming.on_closed_bar(ClosedBar {
            timestamp_ms: index as i64 * 60_000,
            close: Some(price),
            ..Default::default()
        }).unwrap();
        assert_eq!(batch, streamed);
        assert_eq!(batch["above"].0, signal.map(|value| if value == 1 { 1.0 } else { 0.0 }));
        assert_eq!(batch["below"].0, signal.map(|value| if value == -1 { 1.0 } else { 0.0 }));
    }
    let before = plan.evaluate_at(&prices, 3);
    let mut changed = prices;
    changed[4..].fill(1_000_000.0);
    assert_eq!(before, plan.evaluate_at(&changed, 3));
}

#[test]
fn ema_missing_value_resets_warmup_without_forward_fill() {
    let plan = graph(
        json!([{"id":"ema","op":"ema_series","input":"close","period":3}]),
        &["ema"],
    );
    let prices = [1.0, 2.0, 3.0, f64::NAN, 5.0, 6.0, 7.0];
    let (warmup, warmup_cause) = plan.evaluate_at_with_candle_sources_diagnostics(
        &prices,
        CandlePriceInputs::default(),
        None,
        None,
        5,
    ).unwrap();
    assert_eq!(warmup["ema"].0, None);
    assert_eq!(warmup_cause["ema"].origin_step, "close");
    assert_eq!(warmup_cause["ema"].origin_index, 3);
    assert!(plan.evaluate_at(&prices, 6)["ema"].0.is_some());
}
