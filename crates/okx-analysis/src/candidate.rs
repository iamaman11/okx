use okx_observation::{FeeScheduleSnapshot, InstrumentRulesSnapshot};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{AnalysisError, LiquidityRole, PositionDirection, decimal, positive_decimal};

pub const CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1: &str = "okx.candidate-order-analysis/v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateOrderInput {
    pub direction: PositionDirection,
    pub entry_price: String,
    pub stop_price: String,
    pub max_settle_notional: String,
    pub max_stop_loss: String,
    pub target_rr: String,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateSizeConstraint {
    Notional,
    StopLoss,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidateOrderAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub fee_generation: String,
    pub settle_currency: String,
    pub direction: PositionDirection,
    pub entry_price: String,
    pub stop_price: String,
    pub target_price: String,
    pub tick_size: String,
    pub contract_value: String,
    pub lot_size: String,
    pub min_size: String,
    pub contracts: String,
    pub base_quantity: String,
    pub size_constraint: CandidateSizeConstraint,
    pub max_settle_notional: String,
    pub entry_notional_settle: String,
    pub max_stop_loss_settle: String,
    pub gross_stop_loss_settle: String,
    pub net_stop_loss_settle: String,
    pub requested_target_rr: String,
    pub achieved_target_rr: String,
    pub gross_target_profit_settle: String,
    pub net_target_profit_settle: String,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
    pub entry_exchange_fee_rate: String,
    pub exit_exchange_fee_rate: String,
    pub entry_trading_cost_settle: String,
    pub stop_exit_trading_cost_settle: String,
    pub target_exit_trading_cost_settle: String,
    pub funding_included_in_rr: bool,
}

struct CandidateMechanics {
    settle_currency: String,
    contract_value: Decimal,
    tick_size: Decimal,
    lot_size: Decimal,
    min_size: Decimal,
    entry_fee_rate: Decimal,
    exit_fee_rate: Decimal,
}

pub fn analyze_candidate_order(
    rules: &InstrumentRulesSnapshot,
    fees: &FeeScheduleSnapshot,
    input: &CandidateOrderInput,
) -> Result<CandidateOrderAnalysis, AnalysisError> {
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
        .clone()
        .filter(|value| !value.trim().is_empty())
        .ok_or(AnalysisError::MissingSettlementCurrency)?;
    let contract_value = positive_decimal(
        "contract_value",
        instrument
            .contract_value
            .as_deref()
            .ok_or_else(|| AnalysisError::UnsupportedContractMechanics("missing ctVal".to_owned()))?,
    )?;
    let tick_size = positive_decimal("tick_size", &instrument.tick_size)?;
    let lot_size = positive_decimal("lot_size", &instrument.lot_size)?;
    let min_size = positive_decimal("min_size", &instrument.min_size)?;

    let entry_fee_rate = fee_rate(fees, input.entry_liquidity_role)?;
    let exit_fee_rate = fee_rate(fees, input.exit_liquidity_role)?;
    validate_fee_rate("entry_fee_rate", entry_fee_rate)?;
    validate_fee_rate("exit_fee_rate", exit_fee_rate)?;

    let mechanics = CandidateMechanics {
        settle_currency,
        contract_value,
        tick_size,
        lot_size,
        min_size,
        entry_fee_rate,
        exit_fee_rate,
    };

    analyze_values(
        &instrument.instrument_id,
        &rules.reference_generation,
        &fees.fee_generation,
        &mechanics,
        input,
    )
}

fn analyze_values(
    instrument_id: &str,
    reference_generation: &str,
    fee_generation: &str,
    mechanics: &CandidateMechanics,
    input: &CandidateOrderInput,
) -> Result<CandidateOrderAnalysis, AnalysisError> {
    let entry_price = positive_decimal("entry_price", &input.entry_price)?;
    let stop_price = positive_decimal("stop_price", &input.stop_price)?;
    let max_notional = positive_decimal("max_settle_notional", &input.max_settle_notional)?;
    let max_stop_loss = positive_decimal("max_stop_loss", &input.max_stop_loss)?;
    let target_rr = positive_decimal("target_rr", &input.target_rr)?;

    require_tick("entry_price", entry_price, mechanics.tick_size)?;
    require_tick("stop_price", stop_price, mechanics.tick_size)?;
    require_adverse_stop(input.direction, entry_price, stop_price)?;

    let per_contract_entry_notional = mechanics.contract_value * entry_price;
    let per_contract_stop_notional = mechanics.contract_value * stop_price;
    let per_contract_entry_cost = user_fee_cost(per_contract_entry_notional, mechanics.entry_fee_rate);
    let per_contract_stop_exit_cost =
        user_fee_cost(per_contract_stop_notional, mechanics.exit_fee_rate);
    let per_contract_gross_stop_loss = match input.direction {
        PositionDirection::Long => mechanics.contract_value * (entry_price - stop_price),
        PositionDirection::Short => mechanics.contract_value * (stop_price - entry_price),
    };
    let per_contract_net_stop_loss =
        per_contract_gross_stop_loss + per_contract_entry_cost + per_contract_stop_exit_cost;
    if per_contract_net_stop_loss <= Decimal::ZERO {
        return Err(AnalysisError::NonPositiveStopLossPerContract);
    }

    let by_notional = floor_to_step(
        max_notional / per_contract_entry_notional,
        mechanics.lot_size,
    );
    let by_stop_loss = floor_to_step(
        max_stop_loss / per_contract_net_stop_loss,
        mechanics.lot_size,
    );
    let contracts = by_notional.min(by_stop_loss);
    if contracts < mechanics.min_size {
        return Err(AnalysisError::BelowMinimumOrderSize {
            computed: contracts.normalize().to_string(),
            minimum: mechanics.min_size.normalize().to_string(),
        });
    }

    let size_constraint = match by_notional.cmp(&by_stop_loss) {
        std::cmp::Ordering::Less => CandidateSizeConstraint::Notional,
        std::cmp::Ordering::Greater => CandidateSizeConstraint::StopLoss,
        std::cmp::Ordering::Equal => CandidateSizeConstraint::Both,
    };

    let base_quantity = contracts * mechanics.contract_value;
    let entry_notional = base_quantity * entry_price;
    let stop_notional = base_quantity * stop_price;
    let entry_cost = user_fee_cost(entry_notional, mechanics.entry_fee_rate);
    let stop_exit_cost = user_fee_cost(stop_notional, mechanics.exit_fee_rate);
    let gross_stop_loss = match input.direction {
        PositionDirection::Long => base_quantity * (entry_price - stop_price),
        PositionDirection::Short => base_quantity * (stop_price - entry_price),
    };
    let net_stop_loss = gross_stop_loss + entry_cost + stop_exit_cost;
    if net_stop_loss <= Decimal::ZERO {
        return Err(AnalysisError::NonPositiveStopLossPerContract);
    }

    let required_net_profit = net_stop_loss * target_rr;
    let raw_target = target_price(
        input.direction,
        entry_price,
        base_quantity,
        entry_cost,
        mechanics.exit_fee_rate,
        required_net_profit,
    )?;
    let target_price = round_target(input.direction, raw_target, mechanics.tick_size);
    if target_price <= Decimal::ZERO {
        return Err(AnalysisError::NonPositiveTargetPrice);
    }

    let target_notional = base_quantity * target_price;
    let target_exit_cost = user_fee_cost(target_notional, mechanics.exit_fee_rate);
    let gross_target_profit = match input.direction {
        PositionDirection::Long => base_quantity * (target_price - entry_price),
        PositionDirection::Short => base_quantity * (entry_price - target_price),
    };
    if gross_target_profit <= Decimal::ZERO {
        return Err(AnalysisError::NonPositiveTargetPrice);
    }
    let net_target_profit = gross_target_profit - entry_cost - target_exit_cost;
    if net_target_profit <= Decimal::ZERO {
        return Err(AnalysisError::NonPositiveTargetProfit);
    }
    let achieved_target_rr = net_target_profit / net_stop_loss;
    if achieved_target_rr < target_rr {
        return Err(AnalysisError::TargetRoundingReducedRiskReward);
    }

    Ok(CandidateOrderAnalysis {
        schema: CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1.to_owned(),
        instrument_id: instrument_id.to_owned(),
        reference_generation: reference_generation.to_owned(),
        fee_generation: fee_generation.to_owned(),
        settle_currency: mechanics.settle_currency.clone(),
        direction: input.direction,
        entry_price: entry_price.normalize().to_string(),
        stop_price: stop_price.normalize().to_string(),
        target_price: target_price.normalize().to_string(),
        tick_size: mechanics.tick_size.normalize().to_string(),
        contract_value: mechanics.contract_value.normalize().to_string(),
        lot_size: mechanics.lot_size.normalize().to_string(),
        min_size: mechanics.min_size.normalize().to_string(),
        contracts: contracts.normalize().to_string(),
        base_quantity: base_quantity.normalize().to_string(),
        size_constraint,
        max_settle_notional: max_notional.normalize().to_string(),
        entry_notional_settle: entry_notional.normalize().to_string(),
        max_stop_loss_settle: max_stop_loss.normalize().to_string(),
        gross_stop_loss_settle: gross_stop_loss.normalize().to_string(),
        net_stop_loss_settle: net_stop_loss.normalize().to_string(),
        requested_target_rr: target_rr.normalize().to_string(),
        achieved_target_rr: achieved_target_rr.normalize().to_string(),
        gross_target_profit_settle: gross_target_profit.normalize().to_string(),
        net_target_profit_settle: net_target_profit.normalize().to_string(),
        entry_liquidity_role: input.entry_liquidity_role,
        exit_liquidity_role: input.exit_liquidity_role,
        entry_exchange_fee_rate: mechanics.entry_fee_rate.normalize().to_string(),
        exit_exchange_fee_rate: mechanics.exit_fee_rate.normalize().to_string(),
        entry_trading_cost_settle: entry_cost.normalize().to_string(),
        stop_exit_trading_cost_settle: stop_exit_cost.normalize().to_string(),
        target_exit_trading_cost_settle: target_exit_cost.normalize().to_string(),
        funding_included_in_rr: false,
    })
}

fn fee_rate(
    fees: &FeeScheduleSnapshot,
    role: LiquidityRole,
) -> Result<Decimal, AnalysisError> {
    decimal(
        "fee_rate",
        match role {
            LiquidityRole::Maker => &fees.maker_rate,
            LiquidityRole::Taker => &fees.taker_rate,
        },
    )
}

fn validate_fee_rate(field: &'static str, rate: Decimal) -> Result<(), AnalysisError> {
    if rate <= -Decimal::ONE || rate >= Decimal::ONE {
        return Err(AnalysisError::InvalidFeeRate(field));
    }
    Ok(())
}

fn user_fee_cost(notional: Decimal, exchange_rate: Decimal) -> Decimal {
    -(notional * exchange_rate)
}

fn require_tick(
    field: &'static str,
    value: Decimal,
    tick_size: Decimal,
) -> Result<(), AnalysisError> {
    if value % tick_size != Decimal::ZERO {
        return Err(AnalysisError::PriceNotOnTick {
            field,
            value: value.normalize().to_string(),
            tick_size: tick_size.normalize().to_string(),
        });
    }
    Ok(())
}

fn require_adverse_stop(
    direction: PositionDirection,
    entry: Decimal,
    stop: Decimal,
) -> Result<(), AnalysisError> {
    let valid = match direction {
        PositionDirection::Long => stop < entry,
        PositionDirection::Short => stop > entry,
    };
    if valid {
        Ok(())
    } else {
        Err(AnalysisError::StopPriceNotAdverse)
    }
}

fn floor_to_step(value: Decimal, step: Decimal) -> Decimal {
    (value / step).floor() * step
}

fn round_target(
    direction: PositionDirection,
    target: Decimal,
    tick_size: Decimal,
) -> Decimal {
    match direction {
        PositionDirection::Long => (target / tick_size).ceil() * tick_size,
        PositionDirection::Short => (target / tick_size).floor() * tick_size,
    }
}

fn target_price(
    direction: PositionDirection,
    entry_price: Decimal,
    base_quantity: Decimal,
    entry_cost: Decimal,
    exit_fee_rate: Decimal,
    required_net_profit: Decimal,
) -> Result<Decimal, AnalysisError> {
    let target = match direction {
        PositionDirection::Long => {
            let denominator = base_quantity * (Decimal::ONE + exit_fee_rate);
            if denominator <= Decimal::ZERO {
                return Err(AnalysisError::InvalidFeeRate("exit_fee_rate"));
            }
            (required_net_profit + base_quantity * entry_price + entry_cost) / denominator
        }
        PositionDirection::Short => {
            let denominator = base_quantity * (Decimal::ONE - exit_fee_rate);
            if denominator <= Decimal::ZERO {
                return Err(AnalysisError::InvalidFeeRate("exit_fee_rate"));
            }
            (base_quantity * entry_price - entry_cost - required_net_profit) / denominator
        }
    };
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mechanics() -> CandidateMechanics {
        CandidateMechanics {
            settle_currency: "USDT".to_owned(),
            contract_value: Decimal::new(1000, 0),
            tick_size: Decimal::new(1, 5),
            lot_size: Decimal::new(1, 2),
            min_size: Decimal::new(1, 2),
            entry_fee_rate: Decimal::new(-5, 4),
            exit_fee_rate: Decimal::new(-5, 4),
        }
    }

    fn input(direction: PositionDirection, stop_price: &str) -> CandidateOrderInput {
        CandidateOrderInput {
            direction,
            entry_price: "0.2".to_owned(),
            stop_price: stop_price.to_owned(),
            max_settle_notional: "1000".to_owned(),
            max_stop_loss: "25".to_owned(),
            target_rr: "2".to_owned(),
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
        }
    }

    #[test]
    fn long_candidate_is_sized_by_stop_loss_and_target_rounding_preserves_rr() {
        let result = analyze_values(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "sha256:fee",
            &mechanics(),
            &input(PositionDirection::Long, "0.18"),
        )
        .expect("candidate");

        assert_eq!(result.settle_currency, "USDT");
        assert_eq!(result.contracts, "1.23");
        assert_eq!(result.size_constraint, CandidateSizeConstraint::StopLoss);
        assert_eq!(result.entry_notional_settle, "246");
        assert_eq!(result.net_stop_loss_settle, "24.8337");
        assert_eq!(result.target_price, "0.24061");
        assert_eq!(result.net_target_profit_settle, "49.67932485");
        assert!(decimal("rr", &result.achieved_target_rr).expect("rr") >= Decimal::new(2, 0));
        assert!(!result.funding_included_in_rr);
    }

    #[test]
    fn short_candidate_uses_directional_stop_and_rounds_target_down() {
        let result = analyze_values(
            "DOGE-USDT-SWAP",
            "sha256:reference",
            "sha256:fee",
            &mechanics(),
            &input(PositionDirection::Short, "0.22"),
        )
        .expect("candidate");

        assert_eq!(result.contracts, "1.23");
        assert_eq!(result.net_stop_loss_settle, "24.8583");
        assert_eq!(result.target_price, "0.1594");
        assert_eq!(result.net_target_profit_settle, "49.716969");
        assert!(decimal("rr", &result.achieved_target_rr).expect("rr") >= Decimal::new(2, 0));
    }

    #[test]
    fn input_prices_must_be_on_exchange_tick() {
        let mut candidate = input(PositionDirection::Long, "0.18");
        candidate.entry_price = "0.200001".to_owned();

        assert!(matches!(
            analyze_values(
                "DOGE-USDT-SWAP",
                "sha256:reference",
                "sha256:fee",
                &mechanics(),
                &candidate,
            ),
            Err(AnalysisError::PriceNotOnTick { .. })
        ));
    }

    #[test]
    fn stop_must_be_adverse_to_direction() {
        assert_eq!(
            analyze_values(
                "DOGE-USDT-SWAP",
                "sha256:reference",
                "sha256:fee",
                &mechanics(),
                &input(PositionDirection::Long, "0.22"),
            ),
            Err(AnalysisError::StopPriceNotAdverse)
        );
    }

    #[test]
    fn order_fails_closed_when_both_constraints_are_below_min_size() {
        let mut candidate = input(PositionDirection::Long, "0.18");
        candidate.max_settle_notional = "1".to_owned();
        candidate.max_stop_loss = "0.01".to_owned();

        assert!(matches!(
            analyze_values(
                "DOGE-USDT-SWAP",
                "sha256:reference",
                "sha256:fee",
                &mechanics(),
                &candidate,
            ),
            Err(AnalysisError::BelowMinimumOrderSize { .. })
        ));
    }
}
