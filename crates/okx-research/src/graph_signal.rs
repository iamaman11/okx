//! Research-only, typed bridge from a causal composition to position intent.
//! A projection is evidence, never financial authority or an exchange order.
use okx_analysis::query_plan::{
    CausalGraphStream, CheckedComposition, CompositionValues, SeriesOp, SeriesSource, Unit,
};
pub use okx_analysis::query_plan::{GRAPH_SIGNAL_VERSION, GraphSignalSpec, MissingSignalPolicy};
use std::collections::BTreeMap;

pub struct CheckedGraphSignal {
    graph: CheckedComposition,
    signal_step: String,
    targets: [i8; 2],
    missing: MissingSignalPolicy,
    warmup_bars: usize,
}

impl CheckedGraphSignal {
    pub fn compile(spec: GraphSignalSpec, total_bars: u64) -> Result<Self, String> {
        if spec.version != GRAPH_SIGNAL_VERSION {
            return Err("graph_signal.version:UNSUPPORTED".into());
        }
        if !(-1..=1).contains(&spec.target_if_true) || !(-1..=1).contains(&spec.target_if_false) {
            return Err("graph_signal:INVALID_TARGET_POSITION".into());
        }
        // A future-return label is an outcome, not a causal decision input.
        // Reject even an unused label so a strategy cannot hide leakage in
        // its declared fingerprint or later change result_steps to activate it.
        if spec
            .graph
            .steps
            .iter()
            .any(|step| step.op == SeriesOp::ForwardReturnLabel)
        {
            return Err("graph_signal:FORWARD_LABEL_FORBIDDEN".into());
        }
        if !spec.graph.result_steps.contains(&spec.signal_step) {
            return Err("graph_signal:UNPROJECTED_SIGNAL".into());
        }
        let checked = CheckedComposition::compile(spec.graph, total_bars)?;
        let output = checked
            .steps()
            .iter()
            .find(|step| step.step.id == spec.signal_step)
            .ok_or("graph_signal:UNKNOWN_SIGNAL_STEP")?;
        if !matches!(output.unit, Unit::Boolean | Unit::Event) {
            return Err("graph_signal:SIGNAL_MUST_BE_BOOLEAN_OR_EVENT".into());
        }
        let warmup_bars = required_score_start(&checked, &spec.signal_step)?;
        Ok(CheckedGraphSignal {
            graph: checked,
            signal_step: spec.signal_step,
            targets: [spec.target_if_false, spec.target_if_true],
            missing: spec.missing,
            warmup_bars,
        })
    }
}

/// The first bar on which a signal can be acted upon at the *next open*.
/// Every dependency path contributes its own closed-bar lag; two-input steps
/// wait for the slower branch. Actual sparse/null observations can delay a
/// signal further, but can never make this earliest bound earlier.
fn required_score_start(graph: &CheckedComposition, signal_step: &str) -> Result<usize, String> {
    let mut first_known = BTreeMap::<&str, usize>::new();
    for checked in graph.steps() {
        let step = &checked.step;
        // Every admitted input is one finalised candle value. Unavailable
        // external sources are rejected by CausalGraphStream before I/O.
        let from = first_known.get(step.input.as_str()).copied().unwrap_or(0);
        let other = step
            .other
            .as_deref()
            .and_then(|input| first_known.get(input).copied())
            .unwrap_or(0);
        let delay = match step.op {
            SeriesOp::ReturnsSeries | SeriesOp::RisingEdge | SeriesOp::FallingEdge => 1,
            SeriesOp::Lag => usize::from(step.period.expect("checked lag period")),
            SeriesOp::RollingMean
            | SeriesOp::RollingMax
            | SeriesOp::RollingMin
            | SeriesOp::RollingRange
            | SeriesOp::RollingPearson
            | SeriesOp::RollingStddev
            | SeriesOp::RollingZscore => {
                usize::from(step.period.expect("checked rolling period")) - 1
            }
            _ => 0,
        };
        let start = from
            .max(other)
            .checked_add(delay)
            .ok_or("graph_signal:WARMUP_OVERFLOW")?;
        first_known.insert(&step.id, start);
    }
    first_known
        .get(signal_step)
        .ok_or("graph_signal:UNKNOWN_SIGNAL_STEP")?
        .checked_add(1)
        .ok_or_else(|| "graph_signal:WARMUP_OVERFLOW".into())
}

pub(crate) fn compiled_graph(
    spec: &GraphSignalSpec,
    total_bars: u64,
) -> Result<(CheckedGraphSignal, CausalGraphStream), String> {
    let checked = CheckedGraphSignal::compile(spec.clone(), total_bars)?;
    // Admission-time proof that this graph has no unimplemented or unavailable
    // source/operator. Source acquisition is not allowed before this check.
    let stream = CausalGraphStream::new(checked.graph())?;
    if total_bars <= checked.warmup_bars() as u64 {
        return Err("graph_signal:INSUFFICIENT_BARS_FOR_WARMUP".into());
    }
    Ok((checked, stream))
}

/// Validate the complete causal graph without reading market data or creating
/// a research job. Returns the minimum valid score_start index.
pub fn preflight_graph(spec: &GraphSignalSpec, total_bars: u64) -> Result<usize, String> {
    Ok(preflight_graph_sources(spec, total_bars)?.0)
}

/// Return the projected physical candle ports alongside graph warmup. The
/// same checked graph that drives execution determines source admission; no
/// duplicated Research-only source registry or speculative venue read exists.
pub fn preflight_graph_sources(
    spec: &GraphSignalSpec,
    total_bars: u64,
) -> Result<(usize, Vec<SeriesSource>), String> {
    let (checked, _) = compiled_graph(spec, total_bars)
        .map_err(|error| fielded_unavailable_source(spec, error))?;
    let sources = okx_analysis::query_plan::SERIES_SOURCES
        .into_iter()
        .filter(|source| checked.graph().uses_source(source.as_str()))
        .collect();
    Ok((checked.warmup_bars(), sources))
}

/// The streaming owner deliberately rejects non-candle sources. Convert its
/// source-name diagnostic into a bounded field-indexed Research preflight
/// failure, but only for a source from the single Rust contract catalog.
/// Unknown internal errors remain opaque at the public Edge boundary.
fn fielded_unavailable_source(spec: &GraphSignalSpec, error: String) -> String {
    let Some(source_name) = error.strip_prefix("composition.stream:SOURCE_NOT_SUPPORTED:") else {
        return error;
    };
    let Some(source) = SeriesSource::parse(source_name) else {
        return error;
    };
    if source.contract().graph_backtest_supported {
        return error;
    }
    for (index, step) in spec.graph.steps.iter().enumerate() {
        if step.input == source_name {
            return format!("composition.steps[{index}].input:SOURCE_UNAVAILABLE");
        }
        if step.other.as_deref() == Some(source_name) {
            return format!("composition.steps[{index}].other:SOURCE_UNAVAILABLE");
        }
    }
    error
}

impl CheckedGraphSignal {
    pub fn graph(&self) -> &CheckedComposition {
        &self.graph
    }

    pub fn warmup_bars(&self) -> usize {
        self.warmup_bars
    }

    /// Pure interpretation of a previously validated closed-bar projection.
    /// None means hold: callers must not manufacture a trade from missing data.
    /// The replay executor still owns money, next-open fills, costs and risk.
    pub fn target(&self, values: &CompositionValues) -> Result<Option<i8>, String> {
        let (signal, unit) = values
            .get(&self.signal_step)
            .ok_or("graph_signal:PROJECTION_MISSING")?;
        if !matches!(unit, Unit::Boolean | Unit::Event) {
            return Err("graph_signal:PROJECTION_UNIT_MISMATCH".into());
        }
        match signal {
            Some(0.0) => Ok(Some(self.targets[0])),
            Some(1.0) => Ok(Some(self.targets[1])),
            None => Ok(match self.missing {
                MissingSignalPolicy::Hold => None,
                MissingSignalPolicy::Flat => Some(0),
            }),
            _ => Err("graph_signal:INVALID_BOOLEAN_OBSERVATION".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_analysis::query_plan::COMPOSITION_VERSION;

    fn spec() -> GraphSignalSpec {
        GraphSignalSpec {
            version: GRAPH_SIGNAL_VERSION.into(),
            graph: serde_json::from_value(serde_json::json!({
                "version":COMPOSITION_VERSION,
                "steps":[
                    {"id":"return","op":"returns_series","input":"close"},
                    {"id":"buy","op":"greater_than","input":"return","threshold":0.0}
                ],
                "result_steps":["buy"]
            }))
            .unwrap(),
            signal_step: "buy".into(),
            target_if_true: 1,
            target_if_false: 0,
            missing: MissingSignalPolicy::Hold,
        }
    }

    #[test]
    fn causal_graph_interprets_historical_closed_bars_without_future_leakage() {
        let graph = CheckedGraphSignal::compile(spec(), 6).unwrap();
        let mut prices = vec![100.0, 110.0, 90.0, 100.0, 90.0, 120.0];
        let past = graph.graph().evaluate_at(&prices, 2);
        assert_eq!(graph.target(&past).unwrap(), Some(0));
        let latest = graph.graph().evaluate_at(&prices, 5);
        assert_eq!(graph.target(&latest).unwrap(), Some(1));
        prices[3..].fill(1_000_000.0);
        assert_eq!(past, graph.graph().evaluate_at(&prices, 2));
        assert_eq!(
            graph
                .target(&graph.graph().evaluate_at(&prices, 0))
                .unwrap(),
            None
        );
    }

    #[test]
    fn rejects_unsafe_targets_labels_and_non_boolean_results() {
        let mut target = spec();
        target.target_if_true = 2;
        assert_eq!(
            CheckedGraphSignal::compile(target, 6).err().as_deref(),
            Some("graph_signal:INVALID_TARGET_POSITION")
        );

        let mut label = spec();
        label.graph.steps.push(
            serde_json::from_value(serde_json::json!({
                "id":"lookahead","op":"forward_return_label", "input":"close",
                "when":"buy", "period":2
            }))
            .unwrap(),
        );
        assert_eq!(
            CheckedGraphSignal::compile(label, 6).err().as_deref(),
            Some("graph_signal:FORWARD_LABEL_FORBIDDEN")
        );

        let mut numeric = spec();
        numeric.signal_step = "return".into();
        numeric.graph.result_steps.push("return".into());
        assert_eq!(
            CheckedGraphSignal::compile(numeric, 6).err().as_deref(),
            Some("graph_signal:SIGNAL_MUST_BE_BOOLEAN_OR_EVENT")
        );
    }

    #[test]
    fn missing_is_not_a_trade_unless_explicit_flat_policy_is_recorded() {
        let hold = CheckedGraphSignal::compile(spec(), 3).unwrap();
        let unknown = hold.graph().evaluate_at(&[100.0, 101.0, 102.0], 0);
        assert_eq!(hold.target(&unknown).unwrap(), None);
        let mut flatten = spec();
        flatten.missing = MissingSignalPolicy::Flat;
        assert_eq!(
            CheckedGraphSignal::compile(flatten, 3)
                .unwrap()
                .target(&unknown)
                .unwrap(),
            Some(0)
        );
        let mut wrong = unknown.clone();
        wrong.insert("buy".into(), (Some(0.5), Unit::Boolean));
        assert_eq!(
            hold.target(&wrong).unwrap_err(),
            "graph_signal:INVALID_BOOLEAN_OBSERVATION"
        );
    }

    #[test]
    fn nested_signal_warmup_is_longest_dependency_path_plus_next_open() {
        let mut nested = spec();
        nested.graph = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"r", "op":"returns_series", "input":"close"},
                {"id":"smooth", "op":"rolling_mean", "input":"r", "period":3},
                {"id":"delayed", "op":"lag", "input":"smooth", "period":2},
                {"id":"positive", "op":"greater_than", "input":"delayed", "threshold":0.0},
                {"id":"entry", "op":"rising_edge", "input":"positive"}
            ],
            "result_steps":["entry"]
        }))
        .unwrap();
        nested.signal_step = "entry".into();
        // returns:1 + rolling(3):2 + lag(2):2 + rising_edge:1
        // + next_open:1 == score_start=7
        assert_eq!(preflight_graph(&nested, 8).unwrap(), 7);
        assert_eq!(
            CheckedGraphSignal::compile(nested.clone(), 8)
                .unwrap()
                .warmup_bars(),
            7
        );
        assert_eq!(
            preflight_graph(&nested, 7).unwrap_err(),
            "graph_signal:INSUFFICIENT_BARS_FOR_WARMUP"
        );
    }

    #[test]
    fn multiple_inputs_use_slower_warmup_and_1000_bars_cannot_score_1000_window() {
        let mut branch = spec();
        branch.graph = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"r", "op":"returns_series", "input":"close"},
                {"id":"early", "op":"greater_than", "input":"r", "threshold":0.0},
                {"id":"slow", "op":"rolling_mean", "input":"close", "period":5},
                {"id":"late", "op":"greater_than", "input":"slow", "threshold":0.0},
                {"id":"both", "op":"and", "input":"early", "other":"late"},
                {"id":"edge", "op":"rising_edge", "input":"both"}
            ],
            "result_steps":["edge"]
        }))
        .unwrap();
        branch.signal_step = "edge".into();
        assert_eq!(preflight_graph(&branch, 7).unwrap(), 6);

        let mut long = spec();
        long.graph = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"slow", "op":"rolling_mean", "input":"close", "period":1000},
                {"id":"signal", "op":"greater_than", "input":"slow", "threshold":0.0}
            ],
            "result_steps":["signal"]
        }))
        .unwrap();
        long.signal_step = "signal".into();
        assert_eq!(
            preflight_graph(&long, 1000).unwrap_err(),
            "graph_signal:INSUFFICIENT_BARS_FOR_WARMUP"
        );
        assert_eq!(preflight_graph(&long, 1001).unwrap(), 1000);
    }

    #[test]
    fn preflight_rejects_unavailable_source_and_accepts_incremental_dispersion() {
        let mut unavailable = spec();
        for source in okx_analysis::query_plan::SERIES_SOURCES {
            if source.contract().graph_backtest_supported || source == SeriesSource::FundingRate {
                continue;
            }
            unavailable.graph.steps[0].input = source.as_str().into();
            assert_eq!(
                preflight_graph(&unavailable, 100).unwrap_err(),
                "composition.steps[0].input:SOURCE_UNAVAILABLE",
                "{}",
                source.as_str()
            );
        }
        unavailable.graph = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"buy","op":"greater_than","input":"funding_rate","threshold":0.0}],
            "result_steps":["buy"]
        }))
        .unwrap();
        unavailable.signal_step = "buy".into();
        assert_eq!(
            preflight_graph(&unavailable, 100).unwrap_err(),
            "composition.steps[0].input:SOURCE_UNAVAILABLE"
        );
        unavailable.graph = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"return","op":"returns_series","input":"close"},
                {"id":"difference","op":"subtract_series","input":"return","other":"funding_rate"},
                {"id":"buy","op":"greater_than","input":"difference","threshold":0.0}
            ],
            "result_steps":["buy"]
        }))
        .unwrap();
        unavailable.signal_step = "buy".into();
        assert_eq!(
            preflight_graph(&unavailable, 100).unwrap_err(),
            "composition.steps[1].other:SOURCE_UNAVAILABLE"
        );
        let mut unavailable = spec();
        unavailable.graph.steps[0].input = "close".into();
        unavailable.graph.steps[0].op = SeriesOp::RollingStddev;
        unavailable.graph.steps[0].period = Some(2);
        assert_eq!(preflight_graph(&unavailable, 100).unwrap(), 2);
    }

    #[test]
    fn preflight_accepts_each_existing_causal_statistical_operator() {
        for operator in [
            SeriesOp::RollingPearson,
            SeriesOp::RollingStddev,
            SeriesOp::RollingZscore,
        ] {
            let mut graph = spec();
            // Pearson requires two ratio inputs and period >=30; the same
            // valid causal shape also exercises stddev and zscore.
            graph.graph.steps.insert(
                1,
                serde_json::from_value(serde_json::json!({
                    "id":"risk_1", "op":"rolling_mean", "input":"return", "period":30
                }))
                .unwrap(),
            );
            graph.graph.steps[1].op = operator;
            graph.graph.steps[1].other =
                (operator == SeriesOp::RollingPearson).then(|| "return".into());
            graph.graph.steps[2].input = "risk_1".into();
            assert!(preflight_graph(&graph, 100).is_ok(), "{operator:?}");
        }
    }

    #[test]
    fn preflight_binds_only_projected_closed_candle_sources() {
        let mut spec = spec();
        spec.graph = serde_json::from_value(serde_json::json!({
            "version": COMPOSITION_VERSION,
            "steps": [
                {"id":"wide", "op":"greater_than_series", "input":"high", "other":"low"},
                {"id":"busy", "op":"greater_than", "input":"volume", "threshold":0.0},
                {"id":"signal", "op":"and", "input":"wide", "other":"busy"},
                {"id":"unused", "op":"rolling_mean", "input":"close", "period":5}
            ],
            "result_steps": ["signal"]
        }))
        .unwrap();
        spec.signal_step = "signal".into();
        let (warmup, sources) = preflight_graph_sources(&spec, 100).unwrap();
        assert_eq!(warmup, 1);
        assert_eq!(sources, vec![SeriesSource::High, SeriesSource::Low, SeriesSource::Volume]);
    }
}
