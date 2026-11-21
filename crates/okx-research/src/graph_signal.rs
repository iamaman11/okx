//! Research-only, typed bridge from a causal composition to position intent.
//! A projection is evidence, never financial authority or an exchange order.
use okx_analysis::query_plan::{
    CausalGraphStream, CheckedComposition, ClosedBar, CompositionRequest, CompositionValues,
    SeriesOp, Unit,
};
use serde::{Deserialize, Serialize};

pub const GRAPH_SIGNAL_VERSION: &str = "okx.graph-signal/v1";

/// A new hypothesis is data: composing existing operations does not require a
/// new Rust StrategyKernel variant. No arbitrary expressions or code loading.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphSignalSpec {
    pub version: String,
    pub graph: CompositionRequest,
    pub signal_step: String,
    pub target_if_true: i8,
    pub target_if_false: i8,
    pub missing: MissingSignalPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingSignalPolicy {
    /// Unknown inputs produce no new order, not an implicit zero.
    Hold,
    /// An explicitly declared, auditable flattening policy.
    Flat,
}

/// A closed-bar decision is never an order. Hold differs explicitly from an
/// intentional flattening target; the replay/Engine layer owns risk approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetPosition {
    Short,
    Flat,
    Long,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphIntent {
    Hold,
    SetTarget(TargetPosition),
}

impl TargetPosition {
    fn from_checked(value: i8) -> Self {
        match value {
            -1 => Self::Short,
            0 => Self::Flat,
            1 => Self::Long,
            _ => unreachable!("compiled target range"),
        }
    }

    fn as_i8(self) -> i8 {
        match self {
            Self::Short => -1,
            Self::Flat => 0,
            Self::Long => 1,
        }
    }
}

#[derive(Clone)]
pub struct CheckedGraphSignal {
    graph: CheckedComposition,
    signal_step: String,
    targets: [i8; 2],
    missing: MissingSignalPolicy,
}

/// The same compiled, typed policy for replay and a future closed-bar live
/// adapter. It holds bounded operator state and cannot execute any order.
pub struct GraphSignalStream {
    checked: CheckedGraphSignal,
    runtime: CausalGraphStream,
}

impl GraphSignalSpec {
    pub fn compile(self, total_bars: u64) -> Result<CheckedGraphSignal, String> {
        if self.version != GRAPH_SIGNAL_VERSION {
            return Err("graph_signal.version:UNSUPPORTED".into());
        }
        if !(-1..=1).contains(&self.target_if_true) || !(-1..=1).contains(&self.target_if_false) {
            return Err("graph_signal:INVALID_TARGET_POSITION".into());
        }
        // A future-return label is an outcome, not a causal decision input.
        // Reject even an unused label so a strategy cannot hide leakage in
        // its declared fingerprint or later change result_steps to activate it.
        if self
            .graph
            .steps
            .iter()
            .any(|step| step.op == SeriesOp::ForwardReturnLabel)
        {
            return Err("graph_signal:FORWARD_LABEL_FORBIDDEN".into());
        }
        if !self.graph.result_steps.contains(&self.signal_step) {
            return Err("graph_signal:UNPROJECTED_SIGNAL".into());
        }
        let checked = CheckedComposition::compile(self.graph, total_bars)?;
        let output = checked
            .steps()
            .iter()
            .find(|step| step.step.id == self.signal_step)
            .ok_or("graph_signal:UNKNOWN_SIGNAL_STEP")?;
        if !matches!(output.unit, Unit::Boolean | Unit::Event) {
            return Err("graph_signal:SIGNAL_MUST_BE_BOOLEAN_OR_EVENT".into());
        }
        Ok(CheckedGraphSignal {
            graph: checked,
            signal_step: self.signal_step,
            targets: [self.target_if_false, self.target_if_true],
            missing: self.missing,
        })
    }
}

impl CheckedGraphSignal {
    pub fn graph(&self) -> &CheckedComposition {
        &self.graph
    }

    pub fn stream(&self) -> Result<GraphSignalStream, String> {
        Ok(GraphSignalStream {
            checked: self.clone(),
            runtime: CausalGraphStream::new(&self.graph)?,
        })
    }

    /// Recompute mathematical state over a closed prefix exactly once. The
    /// owner must independently restore its last emitted intent/order cursor.
    pub fn restore_stream(
        &self,
        bars: impl IntoIterator<Item = ClosedBar>,
    ) -> Result<GraphSignalStream, String> {
        Ok(GraphSignalStream {
            checked: self.clone(),
            runtime: CausalGraphStream::restore_from_history(&self.graph, bars)?,
        })
    }

    pub fn intent(&self, values: &CompositionValues) -> Result<GraphIntent, String> {
        let (signal, unit) = values
            .get(&self.signal_step)
            .ok_or("graph_signal:PROJECTION_MISSING")?;
        if !matches!(unit, Unit::Boolean | Unit::Event) {
            return Err("graph_signal:PROJECTION_UNIT_MISMATCH".into());
        }
        match signal {
            Some(0.0) => Ok(GraphIntent::SetTarget(TargetPosition::from_checked(
                self.targets[0],
            ))),
            Some(1.0) => Ok(GraphIntent::SetTarget(TargetPosition::from_checked(
                self.targets[1],
            ))),
            None => Ok(match self.missing {
                MissingSignalPolicy::Hold => GraphIntent::Hold,
                MissingSignalPolicy::Flat => GraphIntent::SetTarget(TargetPosition::Flat),
            }),
            _ => Err("graph_signal:INVALID_BOOLEAN_OBSERVATION".into()),
        }
    }

    /// Pure interpretation of a previously validated closed-bar projection.
    /// None means hold: callers must not manufacture a trade from missing data.
    /// The replay executor still owns money, next-open fills, costs and risk.
    pub fn target(&self, values: &CompositionValues) -> Result<Option<i8>, String> {
        Ok(match self.intent(values)? {
            GraphIntent::Hold => None,
            GraphIntent::SetTarget(position) => Some(position.as_i8()),
        })
    }
}

impl GraphSignalStream {
    pub fn on_closed_bar(&mut self, bar: ClosedBar) -> Result<GraphIntent, String> {
        self.checked.intent(&self.runtime.on_closed_bar(bar)?)
    }

    pub fn processed_bars(&self) -> u64 {
        self.runtime.processed_bars()
    }

    pub fn operation_count(&self) -> u64 {
        self.runtime.operation_count()
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
        let graph = spec().compile(6).unwrap();
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
            target.compile(6).err().as_deref(),
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
            label.compile(6).err().as_deref(),
            Some("graph_signal:FORWARD_LABEL_FORBIDDEN")
        );

        let mut numeric = spec();
        numeric.signal_step = "return".into();
        numeric.graph.result_steps.push("return".into());
        assert_eq!(
            numeric.compile(6).err().as_deref(),
            Some("graph_signal:SIGNAL_MUST_BE_BOOLEAN_OR_EVENT")
        );
    }

    #[test]
    fn missing_is_not_a_trade_unless_explicit_flat_policy_is_recorded() {
        let hold = spec().compile(3).unwrap();
        let unknown = hold.graph().evaluate_at(&[100.0, 101.0, 102.0], 0);
        assert_eq!(hold.target(&unknown).unwrap(), None);
        let mut flatten = spec();
        flatten.missing = MissingSignalPolicy::Flat;
        assert_eq!(
            flatten.compile(3).unwrap().target(&unknown).unwrap(),
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
    fn stream_and_historical_replay_emit_the_same_typed_closed_bar_intents() {
        let checked = spec().compile(200).unwrap();
        let closes = (0..100)
            .map(|index| match index % 13 {
                0 => f64::NAN,
                1 => 0.0,
                _ => 100.0 + ((index * 17) % 31) as f64,
            })
            .collect::<Vec<_>>();
        let mut stream = checked.stream().unwrap();
        let mut bars = Vec::new();
        for (index, &close) in closes.iter().enumerate() {
            let bar = ClosedBar {
                timestamp_ms: index as i64 * 60_000,
                close: Some(close),
            };
            bars.push(bar);
            let actual = stream.on_closed_bar(bar).unwrap();
            let expected = checked
                .intent(&checked.graph().evaluate_at(&closes, index))
                .unwrap();
            assert_eq!(actual, expected, "bar={index}");
        }
        assert_eq!(stream.processed_bars(), 100);
        assert_eq!(stream.operation_count(), 200);
        let mut restored = checked.restore_stream(bars).unwrap();
        assert_eq!(restored.processed_bars(), 100);
        let next = ClosedBar {
            timestamp_ms: 100 * 60_000,
            close: Some(104.0),
        };
        assert_eq!(
            restored.on_closed_bar(next).unwrap(),
            stream.on_closed_bar(next).unwrap()
        );
    }

    #[test]
    fn hold_and_explicit_flat_are_distinct_serializable_intents() {
        let values = spec().compile(2).unwrap().graph().evaluate_at(&[100.0], 0);
        let held = spec().compile(2).unwrap();
        assert_eq!(held.intent(&values).unwrap(), GraphIntent::Hold);
        let mut policy = spec();
        policy.missing = MissingSignalPolicy::Flat;
        let flat = policy.compile(2).unwrap();
        assert_eq!(
            flat.intent(&values).unwrap(),
            GraphIntent::SetTarget(TargetPosition::Flat)
        );
        assert_eq!(serde_json::to_value(GraphIntent::Hold).unwrap(), "hold");
        assert_eq!(
            serde_json::to_value(GraphIntent::SetTarget(TargetPosition::Flat)).unwrap(),
            serde_json::json!({"set_target":"flat"})
        );
    }
}
