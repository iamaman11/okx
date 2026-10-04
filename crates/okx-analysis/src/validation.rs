use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::{AnalysisError, decimal_sqrt};

pub const VALIDATION_SAMPLE_STATISTICS_SCHEMA_V1: &str =
    "okx.analysis.validation-sample-statistics/v1";
pub const VALIDATION_STATISTICS_ALGORITHM_V1: &str =
    "okx.analysis.validation-statistics/2026-10-05.1";
pub const VALIDATION_COST_STRESS_SCHEMA_V1: &str = "okx.analysis.validation-cost-stress/v1";
pub const VALIDATION_COST_STRESS_ALGORITHM_V1: &str =
    "okx.analysis.validation-cost-stress/2026-10-05.1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationSampleStatistics {
    pub schema: String,
    pub algorithm_version: String,
    pub sample_count: usize,
    pub total_net_pnl_quote: String,
    pub mean_net_pnl_quote: String,
    pub sample_stddev_net_pnl_quote: String,
    pub mean_to_sample_stddev_ratio: Option<String>,
    pub win_count: usize,
    pub loss_count: usize,
    pub zero_count: usize,
    pub gross_profit_quote: String,
    pub gross_loss_abs_quote: String,
    pub profit_factor: Option<String>,
    pub max_drawdown_quote: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationCostStressPoint {
    pub trading_cost_multiplier: String,
    pub stressed_net_pnl_quote: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationCostStress {
    pub schema: String,
    pub algorithm_version: String,
    pub points: Vec<ValidationCostStressPoint>,
    pub monotonic_nonincreasing: bool,
}

pub fn analyze_validation_pnl_samples(
    samples: &[String],
) -> Result<ValidationSampleStatistics, AnalysisError> {
    if samples.len() < 2 {
        return Err(AnalysisError::InsufficientStatisticalSamples(samples.len()));
    }
    let values = samples
        .iter()
        .map(|value| validation_decimal("validation_net_pnl_quote", value))
        .collect::<Result<Vec<_>, _>>()?;
    let count = Decimal::from(values.len() as u64);
    let total = values.iter().copied().sum::<Decimal>();
    let mean = total / count;
    let variance = values
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<Decimal>()
        / Decimal::from((values.len() - 1) as u64);
    let stddev = decimal_sqrt(variance);
    let mean_to_sample_stddev_ratio =
        (stddev > Decimal::ZERO).then(|| normalized(mean / stddev));

    let mut win_count = 0usize;
    let mut loss_count = 0usize;
    let mut zero_count = 0usize;
    let mut gross_profit = Decimal::ZERO;
    let mut gross_loss_abs = Decimal::ZERO;
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut max_drawdown = Decimal::ZERO;

    for value in values {
        if value > Decimal::ZERO {
            win_count += 1;
            gross_profit += value;
        } else if value < Decimal::ZERO {
            loss_count += 1;
            gross_loss_abs += -value;
        } else {
            zero_count += 1;
        }

        cumulative += value;
        if cumulative > peak {
            peak = cumulative;
        }
        let drawdown = peak - cumulative;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }

    let profit_factor =
        (gross_loss_abs > Decimal::ZERO).then(|| normalized(gross_profit / gross_loss_abs));

    Ok(ValidationSampleStatistics {
        schema: VALIDATION_SAMPLE_STATISTICS_SCHEMA_V1.to_owned(),
        algorithm_version: VALIDATION_STATISTICS_ALGORITHM_V1.to_owned(),
        sample_count: samples.len(),
        total_net_pnl_quote: normalized(total),
        mean_net_pnl_quote: normalized(mean),
        sample_stddev_net_pnl_quote: normalized(stddev),
        mean_to_sample_stddev_ratio,
        win_count,
        loss_count,
        zero_count,
        gross_profit_quote: normalized(gross_profit),
        gross_loss_abs_quote: normalized(gross_loss_abs),
        profit_factor,
        max_drawdown_quote: normalized(max_drawdown),
    })
}

pub fn analyze_validation_cost_stress(
    gross_pnl_quote: &str,
    trading_cost_quote: &str,
    funding_cost_quote: &str,
) -> Result<ValidationCostStress, AnalysisError> {
    let gross = validation_decimal("validation_gross_pnl_quote", gross_pnl_quote)?;
    let trading_cost = validation_decimal("validation_trading_cost_quote", trading_cost_quote)?;
    let funding_cost = validation_decimal("validation_funding_cost_quote", funding_cost_quote)?;
    if trading_cost < Decimal::ZERO {
        return Err(AnalysisError::Negative("validation_trading_cost_quote"));
    }

    let multipliers = [
        Decimal::ONE,
        Decimal::new(15, 1),
        Decimal::from(2_u32),
    ];
    let mut previous = None::<Decimal>;
    let mut monotonic_nonincreasing = true;
    let points = multipliers
        .into_iter()
        .map(|multiplier| {
            let stressed = gross - trading_cost * multiplier - funding_cost;
            if previous.is_some_and(|value| stressed > value) {
                monotonic_nonincreasing = false;
            }
            previous = Some(stressed);
            ValidationCostStressPoint {
                trading_cost_multiplier: normalized(multiplier),
                stressed_net_pnl_quote: normalized(stressed),
            }
        })
        .collect();

    Ok(ValidationCostStress {
        schema: VALIDATION_COST_STRESS_SCHEMA_V1.to_owned(),
        algorithm_version: VALIDATION_COST_STRESS_ALGORITHM_V1.to_owned(),
        points,
        monotonic_nonincreasing,
    })
}

fn validation_decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    Decimal::from_str(value).map_err(|_| AnalysisError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn normalized(value: Decimal) -> String {
    value.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_statistics_are_exact_and_drawdown_is_path_dependent() {
        let result = analyze_validation_pnl_samples(&[
            "2".to_owned(),
            "-1".to_owned(),
            "3".to_owned(),
            "-4".to_owned(),
        ])
        .expect("statistics");

        assert_eq!(result.sample_count, 4);
        assert_eq!(result.total_net_pnl_quote, "0");
        assert_eq!(result.mean_net_pnl_quote, "0");
        assert_eq!(result.win_count, 2);
        assert_eq!(result.loss_count, 2);
        assert_eq!(result.zero_count, 0);
        assert_eq!(result.gross_profit_quote, "5");
        assert_eq!(result.gross_loss_abs_quote, "5");
        assert_eq!(result.profit_factor.as_deref(), Some("1"));
        assert_eq!(result.max_drawdown_quote, "4");
        assert_eq!(result.mean_to_sample_stddev_ratio.as_deref(), Some("0"));
    }

    #[test]
    fn cost_stress_is_monotonic_when_trading_cost_is_nonnegative() {
        let result = analyze_validation_cost_stress("10", "2", "1").expect("stress");
        assert_eq!(result.points[0].stressed_net_pnl_quote, "7");
        assert_eq!(result.points[1].stressed_net_pnl_quote, "6");
        assert_eq!(result.points[2].stressed_net_pnl_quote, "5");
        assert!(result.monotonic_nonincreasing);
    }

    #[test]
    fn validation_statistics_require_two_samples() {
        assert!(matches!(
            analyze_validation_pnl_samples(&["1".to_owned()]),
            Err(AnalysisError::InsufficientStatisticalSamples(1))
        ));
    }
}
