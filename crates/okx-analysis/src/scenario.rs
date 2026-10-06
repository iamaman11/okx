use okx_observation::{FeeScheduleSnapshot, InstrumentRulesSnapshot, MarketHistorySnapshot};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::candidate::{
    ceil_to_increment, fee_rate, floor_to_increment, gross_pnl, target_price_for_net_pnl,
    user_trading_cost,
};
use crate::{AnalysisError, LiquidityRole, PositionDirection, decimal, positive_decimal};

pub const POSITION_SCENARIO_SCHEMA_V1: &str = "okx.position-scenario/v1";
pub const HISTORY_BEHAVIOR_SCHEMA_V1: &str = "okx.history-behavior/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScenarioExitAssumption {
    Price { price: String },
    EntryMoveRatio { ratio: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionScenarioAssumptions {
    pub direction: PositionDirection,
    pub contracts: String,
    pub entry_price: String,
    pub exit: ScenarioExitAssumption,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioPriceSource {
    ExplicitPrice,
    EntryMoveRatio,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionScenarioAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub fee_generation: String,
    pub settle_currency: String,
    pub contract_value_currency: String,
    pub direction: PositionDirection,
    pub contracts: String,
    pub contract_value: String,
    pub base_quantity: String,
    pub entry_price: String,
    pub scenario_price_source: ScenarioPriceSource,
    pub requested_move_ratio: Option<String>,
    pub exit_price: String,
    pub exit_price_tick_aligned: bool,
    pub price_move_ratio: String,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
    pub entry_exchange_fee_rate: String,
    pub exit_exchange_fee_rate: String,
    pub entry_settle_notional: String,
    pub exit_settle_notional: String,
    pub entry_trading_cost_settle: String,
    pub exit_trading_cost_settle: String,
    pub gross_pnl_settle: String,
    pub net_pnl_settle: String,
    pub net_pnl_ratio_on_entry_notional: String,
    pub raw_break_even_price: String,
    pub tick_break_even_price: String,
    pub funding_included: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistoryBehaviorAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub bar: String,
    pub reference_generation: String,
    pub history_generation: String,
    pub confirmed_candle_count: usize,
    pub excluded_unconfirmed_count: usize,
    pub oldest_confirmed_open_time_ms: String,
    pub newest_confirmed_open_time_ms: String,
    pub first_close: String,
    pub last_close: String,
    pub total_close_return_ratio: String,
    pub mean_close_return_ratio: String,
    pub mean_absolute_close_return_ratio: String,
    pub max_absolute_close_return_ratio: String,
    pub max_close_drawdown_ratio: String,
    pub realized_volatility_ratio: String,
    pub first_confirmed_volume: String,
    pub last_confirmed_volume: String,
    pub volume_change_ratio: Option<String>,
    pub highest_high: String,
    pub lowest_low: String,
    pub confirmed_high_low_range_ratio: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionScenarioMechanics {
    pub settle_currency: String,
    pub contract_value_currency: String,
    pub contract_value: String,
    pub tick_size: String,
    pub entry_fee_rate: String,
    pub exit_fee_rate: String,
}

pub fn funding_user_cost_quote(
    notional_quote: &str,
    funding_rate: &str,
    direction: PositionDirection,
) -> Result<String, AnalysisError> {
    let notional = positive_decimal("funding_notional_quote", notional_quote)?;
    let rate = decimal("funding_rate", funding_rate)?;
    let user_cost = match direction {
        PositionDirection::Long => notional * rate,
        PositionDirection::Short => -(notional * rate),
    };
    Ok(user_cost.normalize().to_string())
}

pub fn analyze_position_scenario(
    rules: &InstrumentRulesSnapshot,
    fees: &FeeScheduleSnapshot,
    assumptions: &PositionScenarioAssumptions,
) -> Result<PositionScenarioAnalysis, AnalysisError> {
    let instrument = &rules.instrument;
    if instrument.instrument_id != fees.instrument_id {
        return Err(AnalysisError::InstrumentMismatch);
    }
    if rules.reference_generation != fees.reference_generation {
        return Err(AnalysisError::ReferenceGenerationMismatch);
    }
    if !fees.exact_for_instrument {
        return Err(AnalysisError::FeeScheduleNotExact);
    }
    if instrument.contract_type.as_deref() != Some("linear") {
        return Err(AnalysisError::UnsupportedContractMechanics(
            instrument
                .contract_type
                .clone()
                .unwrap_or_else(|| "<missing>".to_owned()),
        ));
    }

    let settle_currency = instrument
        .settle_currency
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(AnalysisError::MissingSettlementCurrency)?
        .to_owned();
    let contract_value_currency = instrument
        .contract_value_currency
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(AnalysisError::MissingContractValueCurrency)?
        .to_owned();
    let contract_value = positive_decimal(
        "contract_value",
        instrument.contract_value.as_deref().ok_or_else(|| {
            AnalysisError::UnsupportedContractMechanics("missing ctVal".to_owned())
        })?,
    )?;
    let tick_size = positive_decimal("tick_size", &instrument.tick_size)?;
    let entry_fee_rate = fee_rate(fees, assumptions.entry_liquidity_role)?;
    let exit_fee_rate = fee_rate(fees, assumptions.exit_liquidity_role)?;

    analyze_position_scenario_values(
        &instrument.instrument_id,
        &rules.reference_generation,
        &fees.fee_generation,
        &PositionScenarioMechanics {
            settle_currency,
            contract_value_currency,
            contract_value: contract_value.normalize().to_string(),
            tick_size: tick_size.normalize().to_string(),
            entry_fee_rate: entry_fee_rate.normalize().to_string(),
            exit_fee_rate: exit_fee_rate.normalize().to_string(),
        },
        assumptions,
    )
}

pub fn analyze_position_scenario_values(
    instrument_id: &str,
    reference_generation: &str,
    fee_generation: &str,
    mechanics: &PositionScenarioMechanics,
    assumptions: &PositionScenarioAssumptions,
) -> Result<PositionScenarioAnalysis, AnalysisError> {
    if mechanics.settle_currency.trim().is_empty() {
        return Err(AnalysisError::MissingSettlementCurrency);
    }
    if mechanics.contract_value_currency.trim().is_empty() {
        return Err(AnalysisError::MissingContractValueCurrency);
    }
    let contract_value = positive_decimal("contract_value", &mechanics.contract_value)?;
    let tick_size = positive_decimal("tick_size", &mechanics.tick_size)?;
    let entry_fee_rate = decimal("entry_fee_rate", &mechanics.entry_fee_rate)?;
    let exit_fee_rate = decimal("exit_fee_rate", &mechanics.exit_fee_rate)?;

    let contracts = positive_decimal("contracts", &assumptions.contracts)?;
    let entry_price = positive_decimal("entry_price", &assumptions.entry_price)?;

    let (price_source, requested_move_ratio, exit_price) = match &assumptions.exit {
        ScenarioExitAssumption::Price { price } => (
            ScenarioPriceSource::ExplicitPrice,
            None,
            positive_decimal("exit_price", price)?,
        ),
        ScenarioExitAssumption::EntryMoveRatio { ratio } => {
            let ratio = decimal("entry_move_ratio", ratio)?;
            if ratio <= -Decimal::ONE {
                return Err(AnalysisError::ScenarioMoveAtOrBelowNegativeOne);
            }
            let exit_price = entry_price * (Decimal::ONE + ratio);
            if exit_price <= Decimal::ZERO {
                return Err(AnalysisError::NonPositive("exit_price"));
            }
            (
                ScenarioPriceSource::EntryMoveRatio,
                Some(ratio.normalize().to_string()),
                exit_price,
            )
        }
    };

    let base_quantity = contracts * contract_value;
    let entry_notional = base_quantity * entry_price;
    let exit_notional = base_quantity * exit_price;
    let entry_cost = user_trading_cost(entry_notional, entry_fee_rate);
    let exit_cost = user_trading_cost(exit_notional, exit_fee_rate);
    let gross = gross_pnl(
        assumptions.direction,
        base_quantity,
        entry_price,
        exit_price,
    );
    let net = gross - entry_cost - exit_cost;
    let net_ratio = net / entry_notional;
    let move_ratio = exit_price / entry_price - Decimal::ONE;

    let raw_break_even = target_price_for_net_pnl(
        assumptions.direction,
        contract_value,
        entry_price,
        entry_fee_rate,
        exit_fee_rate,
        Decimal::ZERO,
    )?;
    let tick_break_even = match assumptions.direction {
        PositionDirection::Long => ceil_to_increment(raw_break_even, tick_size),
        PositionDirection::Short => floor_to_increment(raw_break_even, tick_size),
    };
    if tick_break_even <= Decimal::ZERO {
        return Err(AnalysisError::InvalidTargetPrice(instrument_id.to_owned()));
    }

    Ok(PositionScenarioAnalysis {
        schema: POSITION_SCENARIO_SCHEMA_V1.to_owned(),
        instrument_id: instrument_id.to_owned(),
        reference_generation: reference_generation.to_owned(),
        fee_generation: fee_generation.to_owned(),
        settle_currency: mechanics.settle_currency.clone(),
        contract_value_currency: mechanics.contract_value_currency.clone(),
        direction: assumptions.direction,
        contracts: contracts.normalize().to_string(),
        contract_value: contract_value.normalize().to_string(),
        base_quantity: base_quantity.normalize().to_string(),
        entry_price: entry_price.normalize().to_string(),
        scenario_price_source: price_source,
        requested_move_ratio,
        exit_price: exit_price.normalize().to_string(),
        exit_price_tick_aligned: exit_price % tick_size == Decimal::ZERO,
        price_move_ratio: move_ratio.normalize().to_string(),
        entry_liquidity_role: assumptions.entry_liquidity_role,
        exit_liquidity_role: assumptions.exit_liquidity_role,
        entry_exchange_fee_rate: entry_fee_rate.normalize().to_string(),
        exit_exchange_fee_rate: exit_fee_rate.normalize().to_string(),
        entry_settle_notional: entry_notional.normalize().to_string(),
        exit_settle_notional: exit_notional.normalize().to_string(),
        entry_trading_cost_settle: entry_cost.normalize().to_string(),
        exit_trading_cost_settle: exit_cost.normalize().to_string(),
        gross_pnl_settle: gross.normalize().to_string(),
        net_pnl_settle: net.normalize().to_string(),
        net_pnl_ratio_on_entry_notional: net_ratio.normalize().to_string(),
        raw_break_even_price: raw_break_even.normalize().to_string(),
        tick_break_even_price: tick_break_even.normalize().to_string(),
        funding_included: false,
    })
}

pub fn analyze_history_behavior(
    history: &MarketHistorySnapshot,
) -> Result<HistoryBehaviorAnalysis, AnalysisError> {
    let excluded_unconfirmed_count = history
        .candles
        .iter()
        .filter(|candle| !candle.confirmed)
        .count();
    let confirmed = history
        .candles
        .iter()
        .filter(|candle| candle.confirmed)
        .collect::<Vec<_>>();

    if confirmed.len() < 2 {
        return Err(AnalysisError::InsufficientConfirmedHistory {
            confirmed: confirmed.len(),
        });
    }

    let mut previous_timestamp = None;
    let mut closes = Vec::with_capacity(confirmed.len());
    let mut volumes = Vec::with_capacity(confirmed.len());
    let mut highest_high = None::<Decimal>;
    let mut lowest_low = None::<Decimal>;

    for candle in &confirmed {
        let timestamp = candle
            .open_time_ms
            .parse::<u64>()
            .map_err(|_| AnalysisError::InvalidHistoryTimestamp(candle.open_time_ms.clone()))?;
        if previous_timestamp.is_some_and(|previous| timestamp <= previous) {
            return Err(AnalysisError::HistoryNotChronological);
        }
        previous_timestamp = Some(timestamp);

        let open = positive_decimal("history_open", &candle.open)?;
        let high = positive_decimal("history_high", &candle.high)?;
        let low = positive_decimal("history_low", &candle.low)?;
        let close = positive_decimal("history_close", &candle.close)?;
        let volume = decimal("history_volume", &candle.volume)?;
        if volume < Decimal::ZERO {
            return Err(AnalysisError::Negative("history_volume"));
        }
        if high < open || high < close || high < low || low > open || low > close {
            return Err(AnalysisError::InvalidHistoryCandle(
                candle.open_time_ms.clone(),
            ));
        }

        highest_high = Some(highest_high.map_or(high, |current| current.max(high)));
        lowest_low = Some(lowest_low.map_or(low, |current| current.min(low)));
        closes.push(close);
        volumes.push(volume);
    }

    let first_close = closes[0];
    let last_close = *closes.last().expect("confirmed history is non-empty");
    let mut return_sum = Decimal::ZERO;
    let mut absolute_return_sum = Decimal::ZERO;
    let mut max_absolute_return = Decimal::ZERO;
    let mut squared_return_sum = Decimal::ZERO;
    let mut peak_close = first_close;
    let mut max_drawdown = Decimal::ZERO;

    for pair in closes.windows(2) {
        let value = pair[1] / pair[0] - Decimal::ONE;
        let absolute = if value < Decimal::ZERO { -value } else { value };
        return_sum += value;
        absolute_return_sum += absolute;
        max_absolute_return = max_absolute_return.max(absolute);
        squared_return_sum += value * value;

        peak_close = peak_close.max(pair[1]);
        if pair[1] < peak_close {
            let drawdown = (peak_close - pair[1]) / peak_close;
            max_drawdown = max_drawdown.max(drawdown);
        }
    }

    let return_count = Decimal::from((closes.len() - 1) as u64);
    let realized_volatility = decimal_sqrt(squared_return_sum);
    let first_volume = volumes[0];
    let last_volume = *volumes.last().expect("confirmed history is non-empty");
    let volume_change_ratio = if first_volume > Decimal::ZERO {
        Some(
            (last_volume / first_volume - Decimal::ONE)
                .normalize()
                .to_string(),
        )
    } else {
        None
    };
    let highest_high = highest_high.expect("confirmed history is non-empty");
    let lowest_low = lowest_low.expect("confirmed history is non-empty");

    Ok(HistoryBehaviorAnalysis {
        schema: HISTORY_BEHAVIOR_SCHEMA_V1.to_owned(),
        instrument_id: history.instrument_id.clone(),
        bar: history.bar.clone(),
        reference_generation: history.reference_generation.clone(),
        history_generation: history.history_generation.clone(),
        confirmed_candle_count: confirmed.len(),
        excluded_unconfirmed_count,
        oldest_confirmed_open_time_ms: confirmed
            .first()
            .expect("confirmed history is non-empty")
            .open_time_ms
            .clone(),
        newest_confirmed_open_time_ms: confirmed
            .last()
            .expect("confirmed history is non-empty")
            .open_time_ms
            .clone(),
        first_close: first_close.normalize().to_string(),
        last_close: last_close.normalize().to_string(),
        total_close_return_ratio: (last_close / first_close - Decimal::ONE)
            .normalize()
            .to_string(),
        mean_close_return_ratio: (return_sum / return_count).normalize().to_string(),
        mean_absolute_close_return_ratio: (absolute_return_sum / return_count)
            .normalize()
            .to_string(),
        max_absolute_close_return_ratio: max_absolute_return.normalize().to_string(),
        max_close_drawdown_ratio: max_drawdown.normalize().to_string(),
        realized_volatility_ratio: realized_volatility.normalize().to_string(),
        first_confirmed_volume: first_volume.normalize().to_string(),
        last_confirmed_volume: last_volume.normalize().to_string(),
        volume_change_ratio,
        highest_high: highest_high.normalize().to_string(),
        lowest_low: lowest_low.normalize().to_string(),
        confirmed_high_low_range_ratio: (highest_high / lowest_low - Decimal::ONE)
            .normalize()
            .to_string(),
    })
}

fn decimal_sqrt(value: Decimal) -> Decimal {
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
    estimate
}

#[cfg(test)]
mod tests {
    use okx_observation::{HistoryCandle, MarketHistorySnapshot};

    use super::*;

    fn mechanics() -> PositionScenarioMechanics {
        PositionScenarioMechanics {
            settle_currency: "USDT".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            contract_value: "1000".to_owned(),
            tick_size: "0.00001".to_owned(),
            entry_fee_rate: "-0.0005".to_owned(),
            exit_fee_rate: "-0.0005".to_owned(),
        }
    }

    fn position(exit: ScenarioExitAssumption) -> PositionScenarioAssumptions {
        PositionScenarioAssumptions {
            direction: PositionDirection::Long,
            contracts: "2".to_owned(),
            entry_price: "0.1".to_owned(),
            exit,
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
        }
    }

    #[test]
    fn historical_funding_cost_uses_position_direction() {
        assert_eq!(
            funding_user_cost_quote("1000", "0.0001", PositionDirection::Long)
                .expect("long funding"),
            "0.1"
        );
        assert_eq!(
            funding_user_cost_quote("1000", "0.0001", PositionDirection::Short)
                .expect("short funding"),
            "-0.1"
        );
        assert_eq!(
            funding_user_cost_quote("1000", "-0.0001", PositionDirection::Long)
                .expect("negative funding"),
            "-0.1"
        );
    }

    #[test]
    fn explicit_exit_price_reports_fee_aware_pnl_and_break_even() {
        let result = analyze_position_scenario_values(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "sha256:fee",
            &mechanics(),
            &position(ScenarioExitAssumption::Price {
                price: "0.11".to_owned(),
            }),
        )
        .expect("scenario");

        assert_eq!(result.gross_pnl_settle, "20");
        assert_eq!(result.entry_trading_cost_settle, "0.1");
        assert_eq!(result.exit_trading_cost_settle, "0.11");
        assert_eq!(result.net_pnl_settle, "19.79");
        assert_eq!(result.price_move_ratio, "0.1");
        assert!(result.exit_price_tick_aligned);
        assert!(!result.funding_included);
    }

    #[test]
    fn signed_move_ratio_can_define_theoretical_scenario_price() {
        let result = analyze_position_scenario_values(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "sha256:fee",
            &mechanics(),
            &position(ScenarioExitAssumption::EntryMoveRatio {
                ratio: "-0.075".to_owned(),
            }),
        )
        .expect("scenario");

        assert_eq!(
            result.scenario_price_source,
            ScenarioPriceSource::EntryMoveRatio
        );
        assert_eq!(result.exit_price, "0.0925");
        assert_eq!(result.requested_move_ratio.as_deref(), Some("-0.075"));
        assert!(decimal("pnl", &result.net_pnl_settle).expect("pnl") < Decimal::ZERO);
    }

    #[test]
    fn move_ratio_at_or_below_negative_one_fails_closed() {
        let error = analyze_position_scenario_values(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "sha256:fee",
            &mechanics(),
            &position(ScenarioExitAssumption::EntryMoveRatio {
                ratio: "-1".to_owned(),
            }),
        )
        .expect_err("invalid move");

        assert_eq!(error, AnalysisError::ScenarioMoveAtOrBelowNegativeOne);
    }

    fn candle(
        ts: &str,
        open: &str,
        high: &str,
        low: &str,
        close: &str,
        confirmed: bool,
    ) -> HistoryCandle {
        HistoryCandle {
            open_time_ms: ts.to_owned(),
            open: open.to_owned(),
            high: high.to_owned(),
            low: low.to_owned(),
            close: close.to_owned(),
            volume: "1".to_owned(),
            volume_currency: "1".to_owned(),
            volume_quote: Some("1".to_owned()),
            confirmed,
        }
    }

    fn history(candles: Vec<HistoryCandle>) -> MarketHistorySnapshot {
        MarketHistorySnapshot {
            schema: "okx.market-history/v1".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            requested_limit: candles.len() as u16,
            reference_generation: "sha256:reference".to_owned(),
            source: "okx_public_rest_history".to_owned(),
            source_received_at: "2026-09-27T20:00:00Z".to_owned(),
            history_generation: "sha256:history".to_owned(),
            all_confirmed: candles.iter().all(|candle| candle.confirmed),
            oldest_open_time_ms: candles.first().expect("candles").open_time_ms.clone(),
            newest_open_time_ms: candles.last().expect("candles").open_time_ms.clone(),
            candles,
        }
    }

    #[test]
    fn history_behavior_uses_confirmed_candles_only() {
        let input = history(vec![
            candle("1", "100", "102", "99", "100", true),
            candle("2", "100", "112", "99", "110", true),
            candle("3", "110", "111", "88", "90", true),
            candle("4", "90", "96", "89", "95", true),
            candle("5", "95", "120", "94", "119", false),
        ]);

        let result = analyze_history_behavior(&input).expect("history");

        assert_eq!(result.confirmed_candle_count, 4);
        assert_eq!(result.excluded_unconfirmed_count, 1);
        assert_eq!(result.first_close, "100");
        assert_eq!(result.last_close, "95");
        assert_eq!(result.total_close_return_ratio, "-0.05");
        assert_eq!(result.first_confirmed_volume, "1");
        assert_eq!(result.last_confirmed_volume, "1");
        assert_eq!(result.volume_change_ratio.as_deref(), Some("0"));
        assert!(decimal("rv", &result.realized_volatility_ratio).expect("rv") > Decimal::ZERO);
        assert_eq!(result.highest_high, "112");
        assert_eq!(result.lowest_low, "88");
        assert_eq!(
            result.max_close_drawdown_ratio,
            "0.1818181818181818181818181818"
        );
    }

    #[test]
    fn history_volume_change_and_realized_volatility_are_deterministic() {
        let mut first = candle("1", "100", "101", "99", "100", true);
        first.volume = "10".to_owned();
        let mut second = candle("2", "100", "111", "99", "110", true);
        second.volume = "15".to_owned();
        let input = history(vec![first, second]);

        let result = analyze_history_behavior(&input).expect("history");

        assert_eq!(result.realized_volatility_ratio, "0.1");
        assert_eq!(result.first_confirmed_volume, "10");
        assert_eq!(result.last_confirmed_volume, "15");
        assert_eq!(result.volume_change_ratio.as_deref(), Some("0.5"));
    }

    #[test]
    fn zero_starting_volume_has_explicit_undefined_change() {
        let mut first = candle("1", "100", "101", "99", "100", true);
        first.volume = "0".to_owned();
        let mut second = candle("2", "100", "101", "99", "100", true);
        second.volume = "1".to_owned();
        let input = history(vec![first, second]);

        let result = analyze_history_behavior(&input).expect("history");
        assert_eq!(result.volume_change_ratio, None);
    }

    #[test]
    fn history_requires_two_confirmed_candles() {
        let input = history(vec![
            candle("1", "100", "101", "99", "100", true),
            candle("2", "100", "102", "99", "101", false),
        ]);

        assert_eq!(
            analyze_history_behavior(&input),
            Err(AnalysisError::InsufficientConfirmedHistory { confirmed: 1 })
        );
    }

    #[test]
    fn invalid_ohlc_relationship_fails_closed() {
        let input = history(vec![
            candle("1", "100", "99", "98", "100", true),
            candle("2", "100", "101", "99", "100", true),
        ]);

        assert!(matches!(
            analyze_history_behavior(&input),
            Err(AnalysisError::InvalidHistoryCandle(_))
        ));
    }
}
