//! Stable, serializable strategy requests; no operator implementation belongs here.
//! Research owns this schema and its parameter validation.
use serde::{Deserialize, Serialize};

pub const BREAKOUT_STRATEGY_ID: &str = "close_breakout_v1";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct EmaParameters {
    pub fast: usize,
    pub slow: usize,
}
impl EmaParameters {
    pub fn validate(self) -> Result<Self, String> {
        if self.fast < 1 || self.fast >= self.slow || self.slow > 1000 {
            Err("EMA requires 1 <= fast < slow <= 1000".into())
        } else {
            Ok(self)
        }
    }
}

/// Legacy typed parameters are retained for wire compatibility. New operators
/// use the open graph envelope, which does not enumerate them in the Agent.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct BreakoutParameters {
    pub lookback: usize,
}
impl BreakoutParameters {
    pub fn validate(self) -> Result<Self, String> {
        if !(2..=1000).contains(&self.lookback) {
            Err("BREAKOUT_LOOKBACK_OUT_OF_RANGE:2..1000".into())
        } else {
            Ok(self)
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(untagged)]
pub enum StrategyParameters {
    Ema(EmaParameters),
    Breakout(BreakoutParameters),
}
impl From<EmaParameters> for StrategyParameters {
    fn from(value: EmaParameters) -> Self {
        Self::Ema(value)
    }
}
impl From<BreakoutParameters> for StrategyParameters {
    fn from(value: BreakoutParameters) -> Self {
        Self::Breakout(value)
    }
}
impl StrategyParameters {
    pub fn warmup_for(self, strategy_id: &str) -> Result<usize, String> {
        match (strategy_id, self) {
            (BREAKOUT_STRATEGY_ID, Self::Breakout(p)) => Ok(p.validate()?.lookback),
            ("ema_cross_v1" | "ema_momentum_confirm_v1", Self::Ema(p)) => Ok(p.validate()?.slow),
            ("close_momentum", Self::Ema(p)) => {
                p.validate()?;
                Ok(1)
            }
            ("two_bar_momentum", Self::Ema(p)) => {
                p.validate()?;
                Ok(2)
            }
            ("no_trade", Self::Ema(p)) => {
                p.validate()?;
                Ok(0)
            }
            (
                BREAKOUT_STRATEGY_ID
                | "ema_cross_v1"
                | "ema_momentum_confirm_v1"
                | "close_momentum"
                | "two_bar_momentum"
                | "no_trade",
                _,
            ) => Err("STRATEGY_PARAMETERS_MISMATCH".into()),
            _ => Err("UNSUPPORTED_STRATEGY".into()),
        }
    }
}

/// The old closed composition wire format is frozen here. Execution and
/// compiler behavior are exclusively implemented by okx-strategy-core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionRequest {
    pub version: String,
    pub steps: Vec<SeriesStep>,
    pub result_steps: Vec<String>,
}

pub const GRAPH_SIGNAL_VERSION: &str = "okx.graph-signal/v1";

/// Research-only, versioned recipe for interpreting a causal graph as a
/// desired position. The graph compiler and executor remain in their existing
/// owners; the protocol contains data and bounded structural checks only.
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
    /// No implicit position change when the input is unavailable.
    Hold,
    /// Explicitly flatten an unknown signal; the choice is in the artifact.
    Flat,
}

impl GraphSignalSpec {
    /// Reject oversized or obviously unsafe envelopes before allocating a job.
    /// The checked graph compiler performs unit, reference, and cost analysis.
    pub fn valid_structure(&self) -> bool {
        self.version == GRAPH_SIGNAL_VERSION
            && self.graph.version == "okx.market.composition/v1"
            && (1..=32).contains(&self.graph.steps.len())
            && (1..=14).contains(&self.graph.result_steps.len())
            && self.graph.result_steps.contains(&self.signal_step)
            && valid_step_id(&self.signal_step)
            && self.graph.result_steps.iter().all(|id| valid_step_id(id))
            && self.graph.steps.iter().all(|step| {
                valid_step_id(&step.id)
                    && valid_source_ref(&step.input)
                    && step.other.as_deref().is_none_or(valid_source_ref)
                    && step.when.as_deref().is_none_or(valid_step_id)
                    && step
                        .period
                        .is_none_or(|period| (1..=1000).contains(&period))
                    && step.threshold.is_none_or(f64::is_finite)
                    && step.op != SeriesOp::ForwardReturnLabel
            })
            && (-1..=1).contains(&self.target_if_true)
            && (-1..=1).contains(&self.target_if_false)
    }
}

fn valid_step_id(value: &str) -> bool {
    (1..=32).contains(&value.len())
        && value
            .bytes()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'_')
}

fn valid_source_ref(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == b'_')
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesStep {
    pub id: String,
    pub op: SeriesOp,
    /// Source name or the ID of a previously compiled step.
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesOp {
    ReturnsSeries,
    RollingMean,
    RollingMax,
    RollingMin,
    RollingRange,
    RollingPearson,
    RollingZscore,
    RollingStddev,
    AddSeries,
    SubtractSeries,
    DivideSeries,
    And,
    Or,
    Lag,
    GreaterThan,
    GreaterThanSeries,
    LessThan,
    LessThanSeries,
    RisingEdge,
    FallingEdge,
    ForwardReturnLabel,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_wire_remains_unchanged_without_linking_math() {
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[{"id":"returns","op":"returns_series","input":"close"}],
            "result_steps":["returns"]
        }))
        .unwrap();
        assert_eq!(request.steps[0].op, SeriesOp::ReturnsSeries);
        assert_eq!(
            serde_json::to_value(request).unwrap()["steps"][0]["op"],
            "returns_series"
        );
        assert_eq!(
            StrategyParameters::Ema(EmaParameters { fast: 2, slow: 5 }).warmup_for("ema_cross_v1"),
            Ok(5)
        );
    }

    #[test]
    fn graph_signal_wire_is_strict_versioned_and_causal_before_job_admission() {
        let wire = serde_json::json!({
            "version":GRAPH_SIGNAL_VERSION,
            "graph":{
                "version":"okx.market.composition/v1",
                "steps":[
                    {"id":"returns","op":"returns_series","input":"close"},
                    {"id":"rise","op":"greater_than","input":"returns","threshold":0.0}
                ],
                "result_steps":["rise"]
            },
            "signal_step":"rise","target_if_true":1,"target_if_false":0,"missing":"hold"
        });
        let graph: GraphSignalSpec = serde_json::from_value(wire.clone()).unwrap();
        assert!(graph.valid_structure());
        assert_eq!(serde_json::to_value(&graph).unwrap(), wire);

        for (field, bad) in [
            ("version", serde_json::json!("unknown")),
            ("signal_step", serde_json::json!("missing")),
            ("target_if_true", serde_json::json!(2)),
            ("target_if_false", serde_json::json!(-2)),
        ] {
            let mut changed = wire.clone();
            changed[field] = bad;
            assert!(
                !serde_json::from_value::<GraphSignalSpec>(changed)
                    .is_ok_and(|spec| spec.valid_structure()),
                "{field}"
            );
        }
        let mut unknown = wire.clone();
        unknown["unexpected"] = serde_json::json!("forbidden");
        assert!(serde_json::from_value::<GraphSignalSpec>(unknown).is_err());

        let mut future = wire.clone();
        future["graph"]["steps"][1]["op"] = serde_json::json!("forward_return_label");
        assert!(
            !serde_json::from_value::<GraphSignalSpec>(future)
                .unwrap()
                .valid_structure()
        );

        let mut oversized = wire;
        oversized["graph"]["steps"] = serde_json::json!(vec![
            serde_json::json!({"id":"a","op":"returns_series","input":"close"});
            33
        ]);
        assert!(
            !serde_json::from_value::<GraphSignalSpec>(oversized)
                .unwrap()
                .valid_structure()
        );
    }
}
