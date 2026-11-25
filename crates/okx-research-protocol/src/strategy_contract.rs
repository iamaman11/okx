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
}
