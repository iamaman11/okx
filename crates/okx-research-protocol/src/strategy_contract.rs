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
        self.validate_structure().is_ok()
    }

    /// Bounded field paths and stable codes; no user-controlled text escapes
    /// into a public error. The same validator owns `valid_structure` and
    /// Research admission, so the two paths cannot drift.
    pub fn validate_structure(&self) -> Result<(), String> {
        if self.version != GRAPH_SIGNAL_VERSION {
            return Err("graph_signal.version:UNSUPPORTED".into());
        }
        if self.graph.version != "okx.market.composition/v1" {
            return Err("graph_signal.graph.version:UNSUPPORTED".into());
        }
        if !(1..=32).contains(&self.graph.steps.len()) {
            return Err("graph_signal.graph.steps:INVALID_COUNT".into());
        }
        if !(1..=14).contains(&self.graph.result_steps.len()) {
            return Err("graph_signal.graph.result_steps:INVALID_COUNT".into());
        }
        if !valid_step_id(&self.signal_step) {
            return Err("graph_signal.signal_step:INVALID_ID".into());
        }
        for (index, id) in self.graph.result_steps.iter().enumerate() {
            if !valid_step_id(id) {
                return Err(format!(
                    "graph_signal.graph.result_steps[{index}]:INVALID_ID"
                ));
            }
        }
        if !self.graph.result_steps.contains(&self.signal_step) {
            return Err("graph_signal.signal_step:UNPROJECTED".into());
        }
        for (index, step) in self.graph.steps.iter().enumerate() {
            if !valid_step_id(&step.id) {
                return Err(format!("graph_signal.graph.steps[{index}].id:INVALID_ID"));
            }
            if !valid_source_ref(&step.input) {
                return Err(format!(
                    "graph_signal.graph.steps[{index}].input:INVALID_REFERENCE"
                ));
            }
            if step
                .other
                .as_deref()
                .is_some_and(|other| !valid_source_ref(other))
            {
                return Err(format!(
                    "graph_signal.graph.steps[{index}].other:INVALID_REFERENCE"
                ));
            }
            if step
                .when
                .as_deref()
                .is_some_and(|when| !valid_step_id(when))
            {
                return Err(format!("graph_signal.graph.steps[{index}].when:INVALID_ID"));
            }
            if step
                .period
                .is_some_and(|period| !(1..=1000).contains(&period))
            {
                return Err(format!(
                    "graph_signal.graph.steps[{index}].period:OUT_OF_RANGE"
                ));
            }
            if step
                .threshold
                .is_some_and(|threshold| !threshold.is_finite())
            {
                return Err(format!(
                    "graph_signal.graph.steps[{index}].threshold:NONFINITE"
                ));
            }
            if step.op == SeriesOp::ForwardReturnLabel {
                return Err(format!(
                    "graph_signal.graph.steps[{index}].op:FORWARD_LABEL_FORBIDDEN"
                ));
            }
        }
        if !(-1..=1).contains(&self.target_if_true) {
            return Err("graph_signal.target_if_true:INVALID_POSITION".into());
        }
        if !(-1..=1).contains(&self.target_if_false) {
            return Err("graph_signal.target_if_false:INVALID_POSITION".into());
        }
        Ok(())
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
    /// Finite, dimensionless multiplier for `scale_series` only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor: Option<f64>,
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
    EmaSeries,
    ScaleSeries,
    AddSeries,
    SubtractSeries,
    DivideSeries,
    And,
    Or,
    Not,
    IfElse,
    Lag,
    GreaterThan,
    GreaterThanSeries,
    LessThan,
    LessThanSeries,
    RisingEdge,
    FallingEdge,
    Cooldown,
    ForwardReturnLabel,
}

impl SeriesOp {
    /// The finite protocol catalog. Both Research capabilities and the
    /// compiled operator contracts enumerate this list; there is no worker-
    /// maintained second inventory of spellings or supported operations.
    pub const ALL: [Self; 26] = [
        Self::ReturnsSeries,
        Self::RollingMean,
        Self::RollingMax,
        Self::RollingMin,
        Self::RollingRange,
        Self::RollingPearson,
        Self::RollingZscore,
        Self::RollingStddev,
        Self::EmaSeries,
        Self::ScaleSeries,
        Self::AddSeries,
        Self::SubtractSeries,
        Self::DivideSeries,
        Self::And,
        Self::Or,
        Self::Not,
        Self::IfElse,
        Self::Lag,
        Self::GreaterThan,
        Self::GreaterThanSeries,
        Self::LessThan,
        Self::LessThanSeries,
        Self::RisingEdge,
        Self::FallingEdge,
        Self::Cooldown,
        Self::ForwardReturnLabel,
    ];
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
        assert!(graph.validate_structure().is_ok());
        assert_eq!(serde_json::to_value(&graph).unwrap(), wire);

        for (field, bad, expected) in [
            (
                "version",
                serde_json::json!("unknown"),
                "graph_signal.version:UNSUPPORTED",
            ),
            (
                "signal_step",
                serde_json::json!("missing"),
                "graph_signal.signal_step:UNPROJECTED",
            ),
            (
                "target_if_true",
                serde_json::json!(2),
                "graph_signal.target_if_true:INVALID_POSITION",
            ),
            (
                "target_if_false",
                serde_json::json!(-2),
                "graph_signal.target_if_false:INVALID_POSITION",
            ),
        ] {
            let mut changed = wire.clone();
            changed[field] = bad;
            let spec: GraphSignalSpec = serde_json::from_value(changed).unwrap();
            assert!(!spec.valid_structure(), "{field}");
            assert_eq!(spec.validate_structure().unwrap_err(), expected, "{field}");
        }
        let mut unknown = wire.clone();
        unknown["unexpected"] = serde_json::json!("forbidden");
        assert!(serde_json::from_value::<GraphSignalSpec>(unknown).is_err());

        let mut future = wire.clone();
        future["graph"]["steps"][1]["op"] = serde_json::json!("forward_return_label");
        let future: GraphSignalSpec = serde_json::from_value(future).unwrap();
        assert!(!future.valid_structure());
        assert_eq!(
            future.validate_structure().unwrap_err(),
            "graph_signal.graph.steps[1].op:FORWARD_LABEL_FORBIDDEN"
        );

        let mut oversized = wire;
        oversized["graph"]["steps"] = serde_json::json!(vec![
            serde_json::json!({"id":"a","op":"returns_series","input":"close"});
            33
        ]);
        let oversized: GraphSignalSpec = serde_json::from_value(oversized).unwrap();
        assert!(!oversized.valid_structure());
        assert_eq!(
            oversized.validate_structure().unwrap_err(),
            "graph_signal.graph.steps:INVALID_COUNT"
        );
    }

    #[test]
    fn graph_signal_reports_indexed_fields_without_echoing_user_input() {
        let base = serde_json::json!({
            "version":GRAPH_SIGNAL_VERSION,
            "graph":{"version":"okx.market.composition/v1","steps":[
                {"id":"gain","op":"returns_series","input":"close"},
                {"id":"signal","op":"greater_than","input":"gain","threshold":0.0}
            ],"result_steps":["signal"]},
            "signal_step":"signal","target_if_true":1,"target_if_false":0,"missing":"hold"
        });
        for (pointer, bad, expected) in [
            (
                "/graph/version",
                serde_json::json!("future"),
                "graph_signal.graph.version:UNSUPPORTED",
            ),
            (
                "/graph/steps/0/id",
                serde_json::json!("Invalid/ID"),
                "graph_signal.graph.steps[0].id:INVALID_ID",
            ),
            (
                "/graph/steps/0/input",
                serde_json::json!("close/private/value"),
                "graph_signal.graph.steps[0].input:INVALID_REFERENCE",
            ),
            (
                "/graph/steps/1/threshold",
                serde_json::json!(0),
                "graph_signal.graph.steps[1].threshold:NONFINITE",
            ),
            (
                "/graph/result_steps/0",
                serde_json::json!("Bad ID"),
                "graph_signal.graph.result_steps[0]:INVALID_ID",
            ),
        ] {
            let mut wire = base.clone();
            *wire.pointer_mut(pointer).unwrap() = bad;
            let mut spec: GraphSignalSpec = serde_json::from_value(wire).unwrap();
            if pointer.ends_with("/threshold") {
                // JSON cannot carry NaN/Inf; exercise the Rust-typed caller.
                spec.graph.steps[1].threshold = Some(f64::INFINITY);
            }
            assert_eq!(
                spec.validate_structure().unwrap_err(),
                expected,
                "{pointer}"
            );
        }
        let mut spec: GraphSignalSpec = serde_json::from_value(base).unwrap();
        spec.graph.steps[1].period = Some(0);
        assert_eq!(
            spec.validate_structure().unwrap_err(),
            "graph_signal.graph.steps[1].period:OUT_OF_RANGE"
        );
        spec.graph.steps[1].period = None;
        spec.graph.steps[1].other = Some("bad/ref".into());
        assert_eq!(
            spec.validate_structure().unwrap_err(),
            "graph_signal.graph.steps[1].other:INVALID_REFERENCE"
        );
        spec.graph.steps[1].other = None;
        spec.graph.steps[1].when = Some("bad/when".into());
        assert_eq!(
            spec.validate_structure().unwrap_err(),
            "graph_signal.graph.steps[1].when:INVALID_ID"
        );
    }
}
