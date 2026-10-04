use serde::{Deserialize, Serialize};

use crate::{AnalysisError, decimal};

pub const BASELINE_STRATEGY_VERSION_V1: &str = "okx.strategy.baseline/2026-10-04.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaselineStrategyKind {
    NoTrade,
    CloseMomentum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyDecision {
    Hold,
    EnterLong,
    EnterShort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarDecisionInput {
    pub previous_close: String,
    pub signal_close: String,
}

pub fn evaluate_baseline_strategy(
    strategy: BaselineStrategyKind,
    input: &BarDecisionInput,
) -> Result<StrategyDecision, AnalysisError> {
    match strategy {
        BaselineStrategyKind::NoTrade => Ok(StrategyDecision::Hold),
        BaselineStrategyKind::CloseMomentum => {
            let previous = decimal("strategy_previous_close", &input.previous_close)?;
            let signal = decimal("strategy_signal_close", &input.signal_close)?;
            if previous <= rust_decimal::Decimal::ZERO {
                return Err(AnalysisError::NonPositive("strategy_previous_close"));
            }
            if signal <= rust_decimal::Decimal::ZERO {
                return Err(AnalysisError::NonPositive("strategy_signal_close"));
            }
            Ok(if signal > previous {
                StrategyDecision::EnterLong
            } else if signal < previous {
                StrategyDecision::EnterShort
            } else {
                StrategyDecision::Hold
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(previous: &str, signal: &str) -> BarDecisionInput {
        BarDecisionInput {
            previous_close: previous.to_owned(),
            signal_close: signal.to_owned(),
        }
    }

    #[test]
    fn no_trade_never_emits_a_position_decision() {
        for sample in [
            input("100", "200"),
            input("200", "100"),
            input("100", "100"),
        ] {
            assert_eq!(
                evaluate_baseline_strategy(BaselineStrategyKind::NoTrade, &sample),
                Ok(StrategyDecision::Hold)
            );
        }
    }

    #[test]
    fn close_momentum_is_deterministic_and_directional() {
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::CloseMomentum,
                &input("100", "101")
            ),
            Ok(StrategyDecision::EnterLong)
        );
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::CloseMomentum,
                &input("101", "100")
            ),
            Ok(StrategyDecision::EnterShort)
        );
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::CloseMomentum,
                &input("100", "100")
            ),
            Ok(StrategyDecision::Hold)
        );
    }

    #[test]
    fn baseline_rejects_nonpositive_prices() {
        assert!(matches!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::CloseMomentum,
                &input("0", "100")
            ),
            Err(AnalysisError::NonPositive("strategy_previous_close"))
        ));
        assert!(matches!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::CloseMomentum,
                &input("100", "-1")
            ),
            Err(AnalysisError::NonPositive("strategy_signal_close"))
        ));
    }
}
