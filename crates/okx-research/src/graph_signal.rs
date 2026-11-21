//! Research-only, typed bridge from a causal composition to position intent.
//! A projection is evidence, never financial authority or an exchange order.
use okx_analysis::query_plan::{
    CheckedComposition, CompositionRequest, CompositionValues, SeriesOp, Unit,
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

pub struct CheckedGraphSignal {
    graph: CheckedComposition,
    signal_step: String,
    targets: [i8; 2],
    missing: MissingSignalPolicy,
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
}
