use okx_observation::MarketHistorySnapshot;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::AnalysisError;

pub const SAMPLE_COVARIANCE_FORMULA_V1: &str = "sample-covariance/v1";

pub const PORTFOLIO_STATISTICS_SCHEMA_V1: &str = "okx.portfolio-statistics/v1";
pub const PORTFOLIO_VOLATILITY_FORMULA_V1: &str = "signed-notional-covariance-volatility/v1";
pub const HISTORICAL_STRESS_FORMULA_V1: &str = "aligned-one-bar-return-replay/v1";
pub const PARALLEL_SCENARIO_FORMULA_V1: &str = "parallel-price-move-signed-notional/v1";
pub const VOLATILITY_CONTRIBUTION_RECONCILIATION_TOLERANCE_USD: &str =
    "0.000000000000000000000001";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticalExposure {
    pub instrument_id: String,
    pub signed_notional_usd: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioStatisticsStatus {
    NotApplicable,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CovarianceCell {
    pub left_instrument: String,
    pub right_instrument: String,
    pub covariance: String,
    pub correlation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VolatilityContribution {
    pub instrument_id: String,
    pub signed_notional_usd: String,
    pub return_volatility_ratio: String,
    pub component_volatility_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistoricalStressResult {
    pub formula_version: &'static str,
    pub worst_return_endpoint_ms: String,
    pub worst_portfolio_pnl_usd: String,
    pub worst_portfolio_loss_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParallelScenarioResult {
    pub formula_version: &'static str,
    pub price_move_ratio: String,
    pub portfolio_pnl_usd: String,
    pub portfolio_loss_usd: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatisticalHistoryEvidence {
    pub instrument_id: String,
    pub reference_generation: String,
    pub history_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub requested_limit: u16,
    pub confirmed_close_count: usize,
    pub excluded_unconfirmed_count: usize,
    pub oldest_confirmed_open_time_ms: String,
    pub newest_confirmed_open_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortfolioStatisticsAnalysis {
    pub schema: &'static str,
    pub status: PortfolioStatisticsStatus,
    pub valuation_basis: &'static str,
    pub covariance_formula_version: &'static str,
    pub volatility_formula_version: &'static str,
    pub bar: Option<String>,
    pub reference_generation: Option<String>,
    pub history_evidence: Vec<StatisticalHistoryEvidence>,
    pub confirmed_aligned_close_count: usize,
    pub return_sample_count: usize,
    pub oldest_aligned_close_time_ms: Option<String>,
    pub newest_aligned_close_time_ms: Option<String>,
    pub instruments: Vec<String>,
    pub covariance: Vec<CovarianceCell>,
    pub portfolio_volatility_usd: Option<String>,
    pub volatility_contribution: Vec<VolatilityContribution>,
    pub volatility_contribution_reconciliation_residual_usd: Option<String>,
    pub volatility_contribution_reconciliation_tolerance_usd: &'static str,
    pub historical_stress: Option<HistoricalStressResult>,
    pub parallel_scenario: Option<ParallelScenarioResult>,
    pub expected_shortfall: Option<String>,
    pub expected_shortfall_status: &'static str,
}

pub fn sample_covariance_matrix(
    series: &[Vec<Decimal>],
) -> Result<Vec<Vec<Decimal>>, AnalysisError> {
    if series.is_empty() {
        return Ok(Vec::new());
    }
    let samples = series[0].len();
    if samples < 2 {
        return Err(AnalysisError::InsufficientStatisticalSamples(samples));
    }
    if series.iter().any(|values| values.len() != samples) {
        return Err(AnalysisError::StatisticalSeriesLengthMismatch);
    }

    let sample_count = Decimal::from(samples as u64);
    let denominator = Decimal::from((samples - 1) as u64);
    let means = series
        .iter()
        .map(|values| values.iter().copied().sum::<Decimal>() / sample_count)
        .collect::<Vec<_>>();

    let count = series.len();
    let mut matrix = vec![vec![Decimal::ZERO; count]; count];
    for left in 0..count {
        for right in 0..count {
            let sum = (0..samples).fold(Decimal::ZERO, |acc, index| {
                acc + (series[left][index] - means[left]) * (series[right][index] - means[right])
            });
            matrix[left][right] = sum / denominator;
        }
    }
    Ok(matrix)
}

pub fn covariance_correlation(
    covariance: Decimal,
    left_variance: Decimal,
    right_variance: Decimal,
) -> Option<Decimal> {
    let left = decimal_sqrt(left_variance.max(Decimal::ZERO));
    let right = decimal_sqrt(right_variance.max(Decimal::ZERO));
    let denominator = left * right;
    (denominator > Decimal::ZERO).then(|| covariance / denominator)
}

pub fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let two = Decimal::from(2_u32);
    let mut estimate = if value > Decimal::ONE {
        value / two
    } else {
        Decimal::ONE
    };
    for _ in 0..64 {
        let next = (estimate + value / estimate) / two;
        if next == estimate {
            break;
        }
        estimate = next;
    }
    estimate.normalize()
}

pub fn analyze_portfolio_statistics(
    exposures: &[StatisticalExposure],
    histories: &[MarketHistorySnapshot],
    parallel_scenario_move_ratio: Option<&str>,
) -> Result<PortfolioStatisticsAnalysis, AnalysisError> {
    if exposures.is_empty() {
        return Ok(PortfolioStatisticsAnalysis {
            schema: PORTFOLIO_STATISTICS_SCHEMA_V1,
            status: PortfolioStatisticsStatus::NotApplicable,
            valuation_basis: "current signed position notionalUsd multiplied by simple confirmed close-to-close returns",
            covariance_formula_version: SAMPLE_COVARIANCE_FORMULA_V1,
            volatility_formula_version: PORTFOLIO_VOLATILITY_FORMULA_V1,
            bar: histories.first().map(|history| history.bar.clone()),
            reference_generation: histories
                .first()
                .map(|history| history.reference_generation.clone()),
            history_evidence: Vec::new(),
            confirmed_aligned_close_count: 0,
            return_sample_count: 0,
            oldest_aligned_close_time_ms: None,
            newest_aligned_close_time_ms: None,
            instruments: Vec::new(),
            covariance: Vec::new(),
            portfolio_volatility_usd: None,
            volatility_contribution: Vec::new(),
            volatility_contribution_reconciliation_residual_usd: None,
            volatility_contribution_reconciliation_tolerance_usd:
                VOLATILITY_CONTRIBUTION_RECONCILIATION_TOLERANCE_USD,
            historical_stress: None,
            parallel_scenario: None,
            expected_shortfall: None,
            expected_shortfall_status: "not_computed_without_declared_tail_sample_contract",
        });
    }
    if exposures.len() > 8 {
        return Err(AnalysisError::StatisticalUniverseTooLarge(exposures.len()));
    }
    if histories.len() != exposures.len() {
        return Err(AnalysisError::StatisticalHistoryMismatch);
    }

    let bar = histories
        .first()
        .ok_or(AnalysisError::StatisticalHistoryMismatch)?
        .bar
        .clone();
    if histories.iter().any(|history| history.bar != bar) {
        return Err(AnalysisError::StatisticalBarMismatch);
    }
    let interval_ms = fixed_bar_interval_ms(&bar)?;
    let reference_generation = histories
        .first()
        .ok_or(AnalysisError::StatisticalHistoryMismatch)?
        .reference_generation
        .clone();
    if histories
        .iter()
        .any(|history| history.reference_generation != reference_generation)
    {
        return Err(AnalysisError::StatisticalReferenceMismatch);
    }

    let mut instruments = Vec::with_capacity(exposures.len());
    let mut signed_notionals = Vec::with_capacity(exposures.len());
    let mut aligned_timestamps: Option<Vec<String>> = None;
    let mut return_series = Vec::with_capacity(exposures.len());
    let mut history_evidence = Vec::with_capacity(exposures.len());

    for exposure in exposures {
        let history = histories
            .iter()
            .find(|history| history.instrument_id == exposure.instrument_id)
            .ok_or(AnalysisError::StatisticalHistoryMismatch)?;
        let confirmed = history
            .candles
            .iter()
            .filter(|candle| candle.confirmed)
            .collect::<Vec<_>>();
        if confirmed.len() < 3 {
            return Err(AnalysisError::InsufficientConfirmedStatisticalHistory {
                instrument: exposure.instrument_id.clone(),
                confirmed: confirmed.len(),
            });
        }
        let timestamps = confirmed
            .iter()
            .map(|candle| candle.open_time_ms.clone())
            .collect::<Vec<_>>();
        let parsed_timestamps = timestamps
            .iter()
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| AnalysisError::InvalidHistoryTimestamp(value.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if parsed_timestamps
            .windows(2)
            .any(|pair| pair[1].saturating_sub(pair[0]) != interval_ms)
        {
            return Err(AnalysisError::StatisticalHistoryGap(
                exposure.instrument_id.clone(),
            ));
        }
        if let Some(expected) = aligned_timestamps.as_ref() {
            if expected != &timestamps {
                return Err(AnalysisError::StatisticalHistoryNotAligned);
            }
        } else {
            aligned_timestamps = Some(timestamps.clone());
        }

        let closes = confirmed
            .iter()
            .map(|candle| crate::positive_decimal("statistical_history_close", &candle.close))
            .collect::<Result<Vec<_>, _>>()?;
        let returns = closes
            .windows(2)
            .map(|pair| pair[1] / pair[0] - Decimal::ONE)
            .collect::<Vec<_>>();

        history_evidence.push(StatisticalHistoryEvidence {
            instrument_id: exposure.instrument_id.clone(),
            reference_generation: history.reference_generation.clone(),
            history_generation: history.history_generation.clone(),
            source: history.source.clone(),
            source_received_at: history.source_received_at.clone(),
            requested_limit: history.requested_limit,
            confirmed_close_count: confirmed.len(),
            excluded_unconfirmed_count: history
                .candles
                .iter()
                .filter(|candle| !candle.confirmed)
                .count(),
            oldest_confirmed_open_time_ms: timestamps
                .first()
                .expect("confirmed history is non-empty")
                .clone(),
            newest_confirmed_open_time_ms: timestamps
                .last()
                .expect("confirmed history is non-empty")
                .clone(),
        });
        instruments.push(exposure.instrument_id.clone());
        signed_notionals.push(crate::decimal(
            "statistical_signed_notional_usd",
            &exposure.signed_notional_usd,
        )?);
        return_series.push(returns);
    }

    let timestamps = aligned_timestamps.expect("non-empty exposure set establishes timestamps");
    let covariance_matrix = sample_covariance_matrix(&return_series)?;
    let sample_count = return_series[0].len();
    let count = instruments.len();
    let return_volatility = (0..count)
        .map(|index| decimal_sqrt(covariance_matrix[index][index].max(Decimal::ZERO)))
        .collect::<Vec<_>>();

    let mut covariance = Vec::with_capacity(count * count);
    for left in 0..count {
        for right in 0..count {
            covariance.push(CovarianceCell {
                left_instrument: instruments[left].clone(),
                right_instrument: instruments[right].clone(),
                covariance: covariance_matrix[left][right].normalize().to_string(),
                correlation: covariance_correlation(
                    covariance_matrix[left][right],
                    covariance_matrix[left][left],
                    covariance_matrix[right][right],
                )
                .map(|value| value.normalize().to_string()),
            });
        }
    }

    let sigma_weight = (0..count)
        .map(|left| {
            (0..count).fold(Decimal::ZERO, |acc, right| {
                acc + covariance_matrix[left][right] * signed_notionals[right]
            })
        })
        .collect::<Vec<_>>();
    let portfolio_variance = (0..count)
        .fold(Decimal::ZERO, |acc, index| {
            acc + signed_notionals[index] * sigma_weight[index]
        })
        .max(Decimal::ZERO);
    let portfolio_volatility = decimal_sqrt(portfolio_variance);

    let volatility_contribution = (0..count)
        .map(|index| {
            let component = if portfolio_volatility > Decimal::ZERO {
                signed_notionals[index] * sigma_weight[index] / portfolio_volatility
            } else {
                Decimal::ZERO
            };
            VolatilityContribution {
                instrument_id: instruments[index].clone(),
                signed_notional_usd: signed_notionals[index].normalize().to_string(),
                return_volatility_ratio: return_volatility[index].normalize().to_string(),
                component_volatility_usd: component.normalize().to_string(),
            }
        })
        .collect::<Vec<_>>();

    let contribution_sum = volatility_contribution
        .iter()
        .map(|row| crate::decimal("component_volatility_usd", &row.component_volatility_usd))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<Decimal>();
    let contribution_residual = portfolio_volatility - contribution_sum;
    let reconciliation_tolerance = Decimal::new(1, 24);
    if contribution_residual.abs() > reconciliation_tolerance {
        return Err(AnalysisError::StatisticalVolatilityReconciliationExceeded {
            residual: contribution_residual.normalize().to_string(),
            tolerance: VOLATILITY_CONTRIBUTION_RECONCILIATION_TOLERANCE_USD.to_owned(),
        });
    }

    let mut historical_pnl = vec![Decimal::ZERO; sample_count];
    for (notional, returns) in signed_notionals.iter().zip(&return_series) {
        for (pnl, return_value) in historical_pnl.iter_mut().zip(returns) {
            *pnl += *notional * *return_value;
        }
    }
    let (worst_sample, worst_pnl) = historical_pnl
        .iter()
        .copied()
        .enumerate()
        .min_by(|(_, left), (_, right)| left.cmp(right))
        .expect("sample covariance requires non-empty samples");
    let historical_stress = HistoricalStressResult {
        formula_version: HISTORICAL_STRESS_FORMULA_V1,
        worst_return_endpoint_ms: timestamps[worst_sample + 1].clone(),
        worst_portfolio_pnl_usd: worst_pnl.normalize().to_string(),
        worst_portfolio_loss_usd: (-worst_pnl).max(Decimal::ZERO).normalize().to_string(),
    };

    let parallel_scenario = parallel_scenario_move_ratio
        .map(|value| {
            let move_ratio = crate::decimal("parallel_scenario_move_ratio", value)?;
            if move_ratio <= -Decimal::ONE {
                return Err(AnalysisError::ScenarioMoveAtOrBelowNegativeOne);
            }
            let pnl = signed_notionals.iter().copied().sum::<Decimal>() * move_ratio;
            Ok(ParallelScenarioResult {
                formula_version: PARALLEL_SCENARIO_FORMULA_V1,
                price_move_ratio: move_ratio.normalize().to_string(),
                portfolio_pnl_usd: pnl.normalize().to_string(),
                portfolio_loss_usd: (-pnl).max(Decimal::ZERO).normalize().to_string(),
            })
        })
        .transpose()?;

    Ok(PortfolioStatisticsAnalysis {
        schema: PORTFOLIO_STATISTICS_SCHEMA_V1,
        status: PortfolioStatisticsStatus::Ready,
        valuation_basis: "current signed position notionalUsd multiplied by simple confirmed close-to-close returns",
        covariance_formula_version: SAMPLE_COVARIANCE_FORMULA_V1,
        volatility_formula_version: PORTFOLIO_VOLATILITY_FORMULA_V1,
        bar: Some(bar),
        reference_generation: Some(reference_generation),
        history_evidence,
        confirmed_aligned_close_count: timestamps.len(),
        return_sample_count: sample_count,
        oldest_aligned_close_time_ms: timestamps.first().cloned(),
        newest_aligned_close_time_ms: timestamps.last().cloned(),
        instruments,
        covariance,
        portfolio_volatility_usd: Some(portfolio_volatility.normalize().to_string()),
        volatility_contribution,
        volatility_contribution_reconciliation_residual_usd: Some(
            contribution_residual.normalize().to_string(),
        ),
        volatility_contribution_reconciliation_tolerance_usd:
            VOLATILITY_CONTRIBUTION_RECONCILIATION_TOLERANCE_USD,
        historical_stress: Some(historical_stress),
        parallel_scenario,
        expected_shortfall: None,
        expected_shortfall_status: "not_computed_without_declared_tail_sample_contract",
    })
}

fn fixed_bar_interval_ms(bar: &str) -> Result<u64, AnalysisError> {
    let minutes = match bar {
        "1m" => 1,
        "3m" => 3,
        "5m" => 5,
        "15m" => 15,
        "30m" => 30,
        "1H" => 60,
        "2H" => 120,
        "4H" => 240,
        "6H" | "6Hutc" => 360,
        "12H" | "12Hutc" => 720,
        "1D" | "1Dutc" => 1_440,
        "2D" | "2Dutc" => 2_880,
        "3D" | "3Dutc" => 4_320,
        "1W" | "1Wutc" => 10_080,
        other => return Err(AnalysisError::UnsupportedStatisticalBar(other.to_owned())),
    };
    Ok(minutes * 60 * 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal")
    }

    #[test]
    fn covariance_is_symmetric_and_identical_series_have_unit_correlation() {
        let input = vec![
            vec![d("0.1"), d("-0.1"), d("0.1")],
            vec![d("0.1"), d("-0.1"), d("0.1")],
        ];
        let matrix = sample_covariance_matrix(&input).expect("matrix");
        assert_eq!(matrix[0][1], matrix[1][0]);
        assert_eq!(
            covariance_correlation(matrix[0][1], matrix[0][0], matrix[1][1])
                .expect("correlation")
                .normalize()
                .to_string(),
            "1"
        );
    }

    #[test]
    fn zero_variance_has_no_correlation() {
        assert_eq!(
            covariance_correlation(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            None
        );
    }

    #[test]
    fn mismatched_series_fail_closed() {
        assert!(matches!(
            sample_covariance_matrix(&[vec![d("1"), d("2")], vec![d("1")]]),
            Err(AnalysisError::StatisticalSeriesLengthMismatch)
        ));
    }

    fn history(instrument: &str, closes: &[&str]) -> MarketHistorySnapshot {
        MarketHistorySnapshot {
            schema: "okx.market-history/v1".to_owned(),
            instrument_id: instrument.to_owned(),
            bar: "1H".to_owned(),
            requested_limit: closes.len() as u16,
            reference_generation: "ref".to_owned(),
            source: "fixture".to_owned(),
            source_received_at: "2026-10-03T00:00:00Z".to_owned(),
            history_generation: format!("history-{instrument}"),
            all_confirmed: true,
            oldest_open_time_ms: "3600000".to_owned(),
            newest_open_time_ms: (closes.len() as u64 * 3_600_000).to_string(),
            candles: closes
                .iter()
                .enumerate()
                .map(|(index, close)| okx_observation::HistoryCandle {
                    open_time_ms: ((index as u64 + 1) * 3_600_000).to_string(),
                    open: (*close).to_owned(),
                    high: (*close).to_owned(),
                    low: (*close).to_owned(),
                    close: (*close).to_owned(),
                    volume: "1".to_owned(),
                    volume_currency: "1".to_owned(),
                    volume_quote: None,
                    confirmed: true,
                })
                .collect(),
        }
    }

    fn exposure(instrument: &str, signed_notional: &str) -> StatisticalExposure {
        StatisticalExposure {
            instrument_id: instrument.to_owned(),
            signed_notional_usd: signed_notional.to_owned(),
        }
    }

    #[test]
    fn flat_portfolio_is_explicitly_not_applicable() {
        let result = analyze_portfolio_statistics(&[], &[], Some("-0.1")).expect("analysis");
        assert_eq!(result.status, PortfolioStatisticsStatus::NotApplicable);
        assert_eq!(result.return_sample_count, 0);
        assert_eq!(result.expected_shortfall, None);
        assert!(result.historical_stress.is_none());
    }

    #[test]
    fn identical_series_have_unit_cross_correlation_and_parallel_scenario_is_signed() {
        let exposures = vec![
            exposure("BTC-USDT-SWAP", "100"),
            exposure("ETH-USDT-SWAP", "50"),
        ];
        let histories = vec![
            history("BTC-USDT-SWAP", &["100", "110", "99", "108.9"]),
            history("ETH-USDT-SWAP", &["200", "220", "198", "217.8"]),
        ];
        let result =
            analyze_portfolio_statistics(&exposures, &histories, Some("-0.1")).expect("analysis");
        let cross = result
            .covariance
            .iter()
            .find(|cell| {
                cell.left_instrument == "BTC-USDT-SWAP" && cell.right_instrument == "ETH-USDT-SWAP"
            })
            .expect("cross correlation");
        assert_eq!(cross.correlation.as_deref(), Some("1"));
        let contribution_sum = result
            .volatility_contribution
            .iter()
            .map(|row| d(&row.component_volatility_usd))
            .sum::<Decimal>();
        let portfolio_volatility = d(result
            .portfolio_volatility_usd
            .as_deref()
            .expect("portfolio volatility"));
        let residual = portfolio_volatility - contribution_sum;
        assert_eq!(
            residual.normalize().to_string(),
            result
                .volatility_contribution_reconciliation_residual_usd
                .as_deref()
                .expect("reconciliation residual")
        );
        assert!(
            residual.abs() <= Decimal::new(1, 24),
            "component contribution residual must stay within the declared tolerance"
        );
        assert_eq!(
            result.volatility_contribution_reconciliation_tolerance_usd,
            VOLATILITY_CONTRIBUTION_RECONCILIATION_TOLERANCE_USD
        );
        assert_eq!(
            result
                .parallel_scenario
                .as_ref()
                .expect("scenario")
                .portfolio_pnl_usd,
            "-15"
        );
        assert_eq!(
            result.expected_shortfall_status,
            "not_computed_without_declared_tail_sample_contract"
        );
    }

    #[test]
    fn opposite_signed_exposure_can_reduce_portfolio_volatility() {
        let histories = vec![
            history("BTC-USDT-SWAP", &["100", "110", "99", "108.9"]),
            history("ETH-USDT-SWAP", &["200", "220", "198", "217.8"]),
        ];
        let same_side = analyze_portfolio_statistics(
            &[
                exposure("BTC-USDT-SWAP", "100"),
                exposure("ETH-USDT-SWAP", "50"),
            ],
            &histories,
            None,
        )
        .expect("same side");
        let hedge = analyze_portfolio_statistics(
            &[
                exposure("BTC-USDT-SWAP", "100"),
                exposure("ETH-USDT-SWAP", "-50"),
            ],
            &histories,
            None,
        )
        .expect("hedge");
        let same = d(same_side
            .portfolio_volatility_usd
            .as_deref()
            .expect("same volatility"));
        let hedged = d(hedge
            .portfolio_volatility_usd
            .as_deref()
            .expect("hedged volatility"));
        assert!(hedged < same);
    }

    #[test]
    fn unaligned_confirmed_timestamps_fail_closed() {
        let exposures = vec![
            exposure("BTC-USDT-SWAP", "100"),
            exposure("ETH-USDT-SWAP", "50"),
        ];
        let btc = history("BTC-USDT-SWAP", &["100", "101", "102"]);
        let mut eth = history("ETH-USDT-SWAP", &["100", "101", "102"]);
        for candle in &mut eth.candles {
            let timestamp = candle.open_time_ms.parse::<u64>().expect("timestamp");
            candle.open_time_ms = (timestamp + 3_600_000).to_string();
        }
        assert!(matches!(
            analyze_portfolio_statistics(&exposures, &[btc, eth], None),
            Err(AnalysisError::StatisticalHistoryNotAligned)
        ));
    }

    #[test]
    fn internal_history_gap_fails_closed() {
        let exposures = vec![exposure("BTC-USDT-SWAP", "100")];
        let mut btc = history("BTC-USDT-SWAP", &["100", "101", "102"]);
        btc.candles[1].open_time_ms = "999".to_owned();
        assert!(matches!(
            analyze_portfolio_statistics(&exposures, &[btc], None),
            Err(AnalysisError::StatisticalHistoryGap(instrument))
                if instrument == "BTC-USDT-SWAP"
        ));
    }
}
