use serde::{Deserialize, Serialize};

use crate::{AnalysisError, decimal};

pub const BASELINE_STRATEGY_VERSION_V1: &str = "okx.strategy.baseline/2026-10-04.1";
pub const TWO_BAR_MOMENTUM_STRATEGY_VERSION_V1: &str =
    "okx.strategy.two-bar-momentum/2026-10-05.1";
pub const STRATEGY_RESEARCH_METADATA_VERSION_V1: &str =
    "okx.strategy.research-metadata/2026-10-04.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BaselineStrategyKind {
    NoTrade,
    CloseMomentum,
    TwoBarMomentum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyParameterSurface {
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyResearchMetadata {
    pub version: String,
    pub signal_lookback_bars: u16,
    pub forward_outcome_bars: u16,
    pub parameter_surface: StrategyParameterSurface,
}

pub fn baseline_strategy_research_metadata(
    strategy: BaselineStrategyKind,
) -> StrategyResearchMetadata {
    let (signal_lookback_bars, forward_outcome_bars) = match strategy {
        BaselineStrategyKind::NoTrade => (0, 0),
        BaselineStrategyKind::CloseMomentum => (1, 2),
        BaselineStrategyKind::TwoBarMomentum => (2, 2),
    };
    StrategyResearchMetadata {
        version: STRATEGY_RESEARCH_METADATA_VERSION_V1.to_owned(),
        signal_lookback_bars,
        forward_outcome_bars,
        parameter_surface: StrategyParameterSurface::None,
    }
}

pub const fn baseline_strategy_version(strategy: BaselineStrategyKind) -> &'static str {
    match strategy {
        BaselineStrategyKind::NoTrade | BaselineStrategyKind::CloseMomentum => {
            BASELINE_STRATEGY_VERSION_V1
        }
        BaselineStrategyKind::TwoBarMomentum => TWO_BAR_MOMENTUM_STRATEGY_VERSION_V1,
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub antecedent_close: Option<String>,
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
        },
        BaselineStrategyKind::TwoBarMomentum => {
            let antecedent = input
                .antecedent_close
                .as_deref()
                .ok_or(AnalysisError::InvalidStrategyInput("strategy_antecedent_close"))?;
            let antecedent = decimal("strategy_antecedent_close", antecedent)?;
            let previous = decimal("strategy_previous_close", &input.previous_close)?;
            let signal = decimal("strategy_signal_close", &input.signal_close)?;
            if antecedent <= rust_decimal::Decimal::ZERO {
                return Err(AnalysisError::NonPositive("strategy_antecedent_close"));
            }
            if previous <= rust_decimal::Decimal::ZERO {
                return Err(AnalysisError::NonPositive("strategy_previous_close"));
            }
            if signal <= rust_decimal::Decimal::ZERO {
                return Err(AnalysisError::NonPositive("strategy_signal_close"));
            }
            Ok(if previous > antecedent && signal > previous {
                StrategyDecision::EnterLong
            } else if previous < antecedent && signal < previous {
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
            antecedent_close: None,
            previous_close: previous.to_owned(),
            signal_close: signal.to_owned(),
        }
    }

    fn two_bar_input(antecedent: &str, previous: &str, signal: &str) -> BarDecisionInput {
        BarDecisionInput {
            antecedent_close: Some(antecedent.to_owned()),
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
            evaluate_baseline_strategy(BaselineStrategyKind::CloseMomentum, &input("100", "101")),
            Ok(StrategyDecision::EnterLong)
        );
        assert_eq!(
            evaluate_baseline_strategy(BaselineStrategyKind::CloseMomentum, &input("101", "100")),
            Ok(StrategyDecision::EnterShort)
        );
        assert_eq!(
            evaluate_baseline_strategy(BaselineStrategyKind::CloseMomentum, &input("100", "100")),
            Ok(StrategyDecision::Hold)
        );
    }

    #[test]
    fn two_bar_momentum_requires_directional_confirmation() {
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::TwoBarMomentum,
                &two_bar_input("100", "101", "102"),
            ),
            Ok(StrategyDecision::EnterLong)
        );
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::TwoBarMomentum,
                &two_bar_input("102", "101", "100"),
            ),
            Ok(StrategyDecision::EnterShort)
        );
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::TwoBarMomentum,
                &two_bar_input("100", "101", "100.5"),
            ),
            Ok(StrategyDecision::Hold)
        );
        assert_eq!(
            evaluate_baseline_strategy(
                BaselineStrategyKind::TwoBarMomentum,
                &two_bar_input("100", "100", "101"),
            ),
            Ok(StrategyDecision::Hold)
        );
    }

    #[test]
    fn research_metadata_declares_only_real_strategy_horizons() {
        let no_trade = baseline_strategy_research_metadata(BaselineStrategyKind::NoTrade);
        assert_eq!(no_trade.signal_lookback_bars, 0);
        assert_eq!(no_trade.forward_outcome_bars, 0);
        assert_eq!(no_trade.parameter_surface, StrategyParameterSurface::None);

        let momentum = baseline_strategy_research_metadata(BaselineStrategyKind::CloseMomentum);
        assert_eq!(momentum.signal_lookback_bars, 1);
        assert_eq!(momentum.forward_outcome_bars, 2);
        assert_eq!(momentum.parameter_surface, StrategyParameterSurface::None);
        assert_eq!(momentum.version, STRATEGY_RESEARCH_METADATA_VERSION_V1);

        let confirmed = baseline_strategy_research_metadata(BaselineStrategyKind::TwoBarMomentum);
        assert_eq!(confirmed.signal_lookback_bars, 2);
        assert_eq!(confirmed.forward_outcome_bars, 2);
        assert_eq!(confirmed.parameter_surface, StrategyParameterSurface::None);
        assert_eq!(
            baseline_strategy_version(BaselineStrategyKind::TwoBarMomentum),
            TWO_BAR_MOMENTUM_STRATEGY_VERSION_V1
        );
    }

    #[test]
    fn baseline_rejects_nonpositive_prices() {
        assert!(matches!(
            evaluate_baseline_strategy(BaselineStrategyKind::CloseMomentum, &input("0", "100")),
            Err(AnalysisError::NonPositive("strategy_previous_close"))
        ));
        assert!(matches!(
            evaluate_baseline_strategy(BaselineStrategyKind::CloseMomentum, &input("100", "-1")),
            Err(AnalysisError::NonPositive("strategy_signal_close"))
        ));
    }
}
