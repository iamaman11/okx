use okx_observation::{FeeScheduleSnapshot, InstrumentRulesSnapshot};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{AnalysisError, LiquidityRole, PositionDirection, decimal, positive_decimal};

pub const CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1: &str = "okx.candidate-order-analysis/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidateOrderAssumptions {
    pub direction: PositionDirection,
    pub entry_price: String,
    pub stop_price: String,
    pub max_settle_notional: String,
    pub max_loss_settle: String,
    pub target_rr: String,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SizingConstraint {
    Notional,
    Risk,
    Both,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CandidateOrderAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub fee_generation: String,
    pub settle_currency: String,
    pub contract_value_currency: String,
    pub direction: PositionDirection,
    pub entry_liquidity_role: LiquidityRole,
    pub exit_liquidity_role: LiquidityRole,
    pub sizing_constraint: SizingConstraint,
    pub requested_max_settle_notional: String,
    pub requested_max_loss_settle: String,
    pub requested_target_rr: String,
    pub contracts: String,
    pub lot_size: String,
    pub min_size: String,
    pub contract_value: String,
    pub base_quantity: String,
    pub entry_price: String,
    pub stop_price: String,
    pub target_price: String,
    pub entry_settle_notional: String,
    pub entry_trading_cost_settle: String,
    pub stop_exit_settle_notional: String,
    pub stop_exit_trading_cost_settle: String,
    pub stop_gross_pnl_settle: String,
    pub stop_net_pnl_settle: String,
    pub stop_loss_settle: String,
    pub target_exit_settle_notional: String,
    pub target_exit_trading_cost_settle: String,
    pub target_gross_pnl_settle: String,
    pub target_net_pnl_settle: String,
    pub actual_target_rr: String,
    pub funding_included: bool,
}

pub fn analyze_candidate_order(
    rules: &InstrumentRulesSnapshot,
    fees: &FeeScheduleSnapshot,
    assumptions: &CandidateOrderAssumptions,
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
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(AnalysisError::MissingSettlementCurrency)?;
    let contract_value_currency = instrument
        .contract_value_currency
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or(AnalysisError::MissingContractValueCurrency)?;
    let contract_value = positive_decimal(
        "contract_value",
        instrument.contract_value.as_deref().ok_or_else(|| {
            AnalysisError::UnsupportedContractMechanics("missing ctVal".to_owned())
        })?,
    )?;
    let lot_size = positive_decimal("lot_size", &instrument.lot_size)?;
    let min_size = positive_decimal("min_size", &instrument.min_size)?;
    let tick_size = positive_decimal("tick_size", &instrument.tick_size)?;
    let entry_rate = fee_rate(fees, assumptions.entry_liquidity_role)?;
    let exit_rate = fee_rate(fees, assumptions.exit_liquidity_role)?;

    analyze_candidate_values(
        &instrument.instrument_id,
        &rules.reference_generation,
        &fees.fee_generation,
        settle_currency,
        contract_value_currency,
        contract_value,
        lot_size,
        min_size,
        tick_size,
        entry_rate,
        exit_rate,
        assumptions,
    )
}

#[allow(clippy::too_many_arguments)]
fn analyze_candidate_values(
    instrument_id: &str,
    reference_generation: &str,
    fee_generation: &str,
    settle_currency: &str,
    contract_value_currency: &str,
    contract_value: Decimal,
    lot_size: Decimal,
    min_size: Decimal,
    tick_size: Decimal,
    entry_rate: Decimal,
    exit_rate: Decimal,
    assumptions: &CandidateOrderAssumptions,
) -> Result<CandidateOrderAnalysis, AnalysisError> {
    let entry_price = positive_decimal("entry_price", &assumptions.entry_price)?;
    let stop_price = positive_decimal("stop_price", &assumptions.stop_price)?;
    require_tick_aligned("entry_price", entry_price, tick_size)?;
    require_tick_aligned("stop_price", stop_price, tick_size)?;
    require_stop_direction(
        instrument_id,
        assumptions.direction,
        entry_price,
        stop_price,
    )?;

    let max_settle_notional =
        positive_decimal("max_settle_notional", &assumptions.max_settle_notional)?;
    let max_loss_settle = positive_decimal("max_loss_settle", &assumptions.max_loss_settle)?;
    let requested_rr = positive_decimal("target_rr", &assumptions.target_rr)?;

    let entry_notional_per_contract = contract_value * entry_price;
    let stop_net_per_contract = net_pnl_per_contract(
        assumptions.direction,
        contract_value,
        entry_price,
        stop_price,
        entry_rate,
        exit_rate,
    );
    if stop_net_per_contract >= Decimal::ZERO {
        return Err(AnalysisError::StopDoesNotLose(instrument_id.to_owned()));
    }
    let stop_loss_per_contract = -stop_net_per_contract;

    let notional_cap_contracts = max_settle_notional / entry_notional_per_contract;
    let risk_cap_contracts = max_loss_settle / stop_loss_per_contract;
    let (raw_contracts, sizing_constraint) = if notional_cap_contracts < risk_cap_contracts {
        (notional_cap_contracts, SizingConstraint::Notional)
    } else if risk_cap_contracts < notional_cap_contracts {
        (risk_cap_contracts, SizingConstraint::Risk)
    } else {
        (notional_cap_contracts, SizingConstraint::Both)
    };

    let contracts = floor_to_increment(raw_contracts, lot_size);
    if contracts < min_size {
        return Err(AnalysisError::CandidateBelowMinimumSize {
            instrument_id: instrument_id.to_owned(),
            contracts: contracts.normalize().to_string(),
            min_size: min_size.normalize().to_string(),
        });
    }

    let target_net_per_contract = stop_loss_per_contract * requested_rr;
    let raw_target = target_price_for_net_pnl(
        assumptions.direction,
        contract_value,
        entry_price,
        entry_rate,
        exit_rate,
        target_net_per_contract,
    )?;
    let target_price = match assumptions.direction {
        PositionDirection::Long => ceil_to_increment(raw_target, tick_size),
        PositionDirection::Short => floor_to_increment(raw_target, tick_size),
    };
    if target_price <= Decimal::ZERO {
        return Err(AnalysisError::InvalidTargetPrice(instrument_id.to_owned()));
    }
    require_target_direction(
        instrument_id,
        assumptions.direction,
        entry_price,
        target_price,
    )?;

    let base_quantity = contracts * contract_value;
    let entry_settle_notional = base_quantity * entry_price;
    let stop_exit_settle_notional = base_quantity * stop_price;
    let target_exit_settle_notional = base_quantity * target_price;

    let entry_cost = user_trading_cost(entry_settle_notional, entry_rate);
    let stop_exit_cost = user_trading_cost(stop_exit_settle_notional, exit_rate);
    let target_exit_cost = user_trading_cost(target_exit_settle_notional, exit_rate);

    let stop_gross_pnl = gross_pnl(
        assumptions.direction,
        base_quantity,
        entry_price,
        stop_price,
    );
    let stop_net_pnl = stop_gross_pnl - entry_cost - stop_exit_cost;
    if stop_net_pnl >= Decimal::ZERO {
        return Err(AnalysisError::StopDoesNotLose(instrument_id.to_owned()));
    }
    let stop_loss = -stop_net_pnl;

    let target_gross_pnl = gross_pnl(
        assumptions.direction,
        base_quantity,
        entry_price,
        target_price,
    );
    let target_net_pnl = target_gross_pnl - entry_cost - target_exit_cost;
    if target_net_pnl <= Decimal::ZERO {
        return Err(AnalysisError::TargetDoesNotProfit(instrument_id.to_owned()));
    }
    let actual_rr = target_net_pnl / stop_loss;

    debug_assert!(entry_settle_notional <= max_settle_notional);
    debug_assert!(stop_loss <= max_loss_settle);
    debug_assert!(actual_rr >= requested_rr);

    Ok(CandidateOrderAnalysis {
        schema: CANDIDATE_ORDER_ANALYSIS_SCHEMA_V1.to_owned(),
        instrument_id: instrument_id.to_owned(),
        reference_generation: reference_generation.to_owned(),
        fee_generation: fee_generation.to_owned(),
        settle_currency: settle_currency.to_owned(),
        contract_value_currency: contract_value_currency.to_owned(),
        direction: assumptions.direction,
        entry_liquidity_role: assumptions.entry_liquidity_role,
        exit_liquidity_role: assumptions.exit_liquidity_role,
        sizing_constraint,
        requested_max_settle_notional: max_settle_notional.normalize().to_string(),
        requested_max_loss_settle: max_loss_settle.normalize().to_string(),
        requested_target_rr: requested_rr.normalize().to_string(),
        contracts: contracts.normalize().to_string(),
        lot_size: lot_size.normalize().to_string(),
        min_size: min_size.normalize().to_string(),
        contract_value: contract_value.normalize().to_string(),
        base_quantity: base_quantity.normalize().to_string(),
        entry_price: entry_price.normalize().to_string(),
        stop_price: stop_price.normalize().to_string(),
        target_price: target_price.normalize().to_string(),
        entry_settle_notional: entry_settle_notional.normalize().to_string(),
        entry_trading_cost_settle: entry_cost.normalize().to_string(),
        stop_exit_settle_notional: stop_exit_settle_notional.normalize().to_string(),
        stop_exit_trading_cost_settle: stop_exit_cost.normalize().to_string(),
        stop_gross_pnl_settle: stop_gross_pnl.normalize().to_string(),
        stop_net_pnl_settle: stop_net_pnl.normalize().to_string(),
        stop_loss_settle: stop_loss.normalize().to_string(),
        target_exit_settle_notional: target_exit_settle_notional.normalize().to_string(),
        target_exit_trading_cost_settle: target_exit_cost.normalize().to_string(),
        target_gross_pnl_settle: target_gross_pnl.normalize().to_string(),
        target_net_pnl_settle: target_net_pnl.normalize().to_string(),
        actual_target_rr: actual_rr.normalize().to_string(),
        funding_included: false,
    })
}

pub(crate) fn fee_rate(
    fees: &FeeScheduleSnapshot,
    role: LiquidityRole,
) -> Result<Decimal, AnalysisError> {
    let value = match role {
        LiquidityRole::Maker => &fees.maker_rate,
        LiquidityRole::Taker => &fees.taker_rate,
    };
    decimal("fee_rate", value)
}

pub(crate) fn user_trading_cost(notional: Decimal, exchange_rate: Decimal) -> Decimal {
    -(notional * exchange_rate)
}

pub(crate) fn gross_pnl(
    direction: PositionDirection,
    base_quantity: Decimal,
    entry_price: Decimal,
    exit_price: Decimal,
) -> Decimal {
    match direction {
        PositionDirection::Long => base_quantity * (exit_price - entry_price),
        PositionDirection::Short => base_quantity * (entry_price - exit_price),
    }
}

fn net_pnl_per_contract(
    direction: PositionDirection,
    contract_value: Decimal,
    entry_price: Decimal,
    exit_price: Decimal,
    entry_rate: Decimal,
    exit_rate: Decimal,
) -> Decimal {
    let base = contract_value;
    let gross = gross_pnl(direction, base, entry_price, exit_price);
    let entry_cost = user_trading_cost(base * entry_price, entry_rate);
    let exit_cost = user_trading_cost(base * exit_price, exit_rate);
    gross - entry_cost - exit_cost
}

pub(crate) fn target_price_for_net_pnl(
    direction: PositionDirection,
    contract_value: Decimal,
    entry_price: Decimal,
    entry_rate: Decimal,
    exit_rate: Decimal,
    target_net_per_contract: Decimal,
) -> Result<Decimal, AnalysisError> {
    let target_per_base = target_net_per_contract / contract_value;

    let target = match direction {
        PositionDirection::Long => {
            let denominator = Decimal::ONE + exit_rate;
            if denominator <= Decimal::ZERO {
                return Err(AnalysisError::InvalidFeeRate("exit_fee_rate"));
            }
            (target_per_base + entry_price * (Decimal::ONE - entry_rate)) / denominator
        }
        PositionDirection::Short => {
            let denominator = Decimal::ONE - exit_rate;
            if denominator <= Decimal::ZERO {
                return Err(AnalysisError::InvalidFeeRate("exit_fee_rate"));
            }
            (entry_price * (Decimal::ONE + entry_rate) - target_per_base) / denominator
        }
    };

    if target <= Decimal::ZERO {
        return Err(AnalysisError::InvalidTargetPrice(
            "derived target".to_owned(),
        ));
    }
    Ok(target)
}

fn require_tick_aligned(
    field: &'static str,
    value: Decimal,
    tick_size: Decimal,
) -> Result<(), AnalysisError> {
    if value % tick_size == Decimal::ZERO {
        Ok(())
    } else {
        Err(AnalysisError::PriceNotTickAligned {
            field,
            value: value.normalize().to_string(),
            tick_size: tick_size.normalize().to_string(),
        })
    }
}

fn require_stop_direction(
    instrument_id: &str,
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
        Err(AnalysisError::InvalidStopDirection(
            instrument_id.to_owned(),
        ))
    }
}

fn require_target_direction(
    instrument_id: &str,
    direction: PositionDirection,
    entry: Decimal,
    target: Decimal,
) -> Result<(), AnalysisError> {
    let valid = match direction {
        PositionDirection::Long => target > entry,
        PositionDirection::Short => target < entry,
    };
    if valid {
        Ok(())
    } else {
        Err(AnalysisError::InvalidTargetDirection(
            instrument_id.to_owned(),
        ))
    }
}

pub(crate) fn floor_to_increment(value: Decimal, increment: Decimal) -> Decimal {
    (value / increment).floor() * increment
}

pub(crate) fn ceil_to_increment(value: Decimal, increment: Decimal) -> Decimal {
    (value / increment).ceil() * increment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules_for_test() -> TestRules {
        TestRules {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            reference_generation: "sha256:reference".to_owned(),
            settle_currency: "USDT".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            contract_value: Decimal::from(1000),
            lot_size: Decimal::new(1, 2),
            min_size: Decimal::new(1, 2),
            tick_size: Decimal::new(1, 5),
        }
    }

    struct TestRules {
        instrument_id: String,
        reference_generation: String,
        settle_currency: String,
        contract_value_currency: String,
        contract_value: Decimal,
        lot_size: Decimal,
        min_size: Decimal,
        tick_size: Decimal,
    }

    fn assumptions(direction: PositionDirection, stop_price: &str) -> CandidateOrderAssumptions {
        CandidateOrderAssumptions {
            direction,
            entry_price: "0.10000".to_owned(),
            stop_price: stop_price.to_owned(),
            max_settle_notional: "1000".to_owned(),
            max_loss_settle: "50".to_owned(),
            target_rr: "2".to_owned(),
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
        }
    }

    fn analyze_test(
        rules: &TestRules,
        assumptions: &CandidateOrderAssumptions,
    ) -> Result<CandidateOrderAnalysis, AnalysisError> {
        analyze_candidate_values(
            &rules.instrument_id,
            &rules.reference_generation,
            "sha256:fees",
            &rules.settle_currency,
            &rules.contract_value_currency,
            rules.contract_value,
            rules.lot_size,
            rules.min_size,
            rules.tick_size,
            Decimal::new(-5, 4),
            Decimal::new(-5, 4),
            assumptions,
        )
    }

    #[test]
    fn long_candidate_is_risk_sized_and_fee_aware() {
        let rules = rules_for_test();
        let result = analyze_test(&rules, &assumptions(PositionDirection::Long, "0.09000"))
            .expect("candidate");

        assert_eq!(result.settle_currency, "USDT");
        assert_eq!(result.sizing_constraint, SizingConstraint::Risk);
        assert_eq!(result.contracts, "4.95");
        assert_eq!(result.entry_settle_notional, "495");
        assert!(decimal("stop_loss", &result.stop_loss_settle).expect("loss") <= Decimal::from(50));
        assert!(decimal("actual_rr", &result.actual_target_rr).expect("rr") >= Decimal::from(2));
        assert!(!result.funding_included);
    }

    #[test]
    fn short_candidate_uses_adverse_stop_and_profitable_target() {
        let rules = rules_for_test();
        let result = analyze_test(&rules, &assumptions(PositionDirection::Short, "0.11000"))
            .expect("candidate");

        assert_eq!(result.direction, PositionDirection::Short);
        assert!(
            decimal("target", &result.target_price).expect("target")
                < decimal("entry", &result.entry_price).expect("entry")
        );
        assert!(decimal("actual_rr", &result.actual_target_rr).expect("rr") >= Decimal::from(2));
    }

    #[test]
    fn candidate_never_exceeds_notional_or_loss_caps_after_lot_rounding() {
        let rules = rules_for_test();
        let mut input = assumptions(PositionDirection::Long, "0.09000");
        input.max_settle_notional = "201".to_owned();
        input.max_loss_settle = "1000".to_owned();

        let result = analyze_test(&rules, &input).expect("candidate");

        assert_eq!(result.sizing_constraint, SizingConstraint::Notional);
        assert!(
            decimal("notional", &result.entry_settle_notional).expect("notional")
                <= Decimal::from(201)
        );
        assert!(decimal("loss", &result.stop_loss_settle).expect("loss") <= Decimal::from(1000));
    }

    #[test]
    fn non_tick_aligned_stop_fails_closed() {
        let rules = rules_for_test();
        let input = assumptions(PositionDirection::Long, "0.090001");

        assert!(matches!(
            analyze_test(&rules, &input),
            Err(AnalysisError::PriceNotTickAligned {
                field: "stop_price",
                ..
            })
        ));
    }

    #[test]
    fn wrong_stop_side_fails_closed() {
        let rules = rules_for_test();
        let input = assumptions(PositionDirection::Long, "0.11000");

        assert!(matches!(
            analyze_test(&rules, &input),
            Err(AnalysisError::InvalidStopDirection(_))
        ));
    }

    #[test]
    fn tiny_risk_budget_fails_instead_of_inventing_sub_minimum_size() {
        let rules = rules_for_test();
        let mut input = assumptions(PositionDirection::Long, "0.09000");
        input.max_loss_settle = "0.01".to_owned();

        assert!(matches!(
            analyze_test(&rules, &input),
            Err(AnalysisError::CandidateBelowMinimumSize { .. })
        ));
    }

    #[test]
    fn target_rounding_never_understates_requested_rr() {
        let rules = rules_for_test();
        for (direction, stop) in [
            (PositionDirection::Long, "0.09000"),
            (PositionDirection::Short, "0.11000"),
        ] {
            let result = analyze_test(&rules, &assumptions(direction, stop)).expect("candidate");
            assert!(
                decimal("actual_rr", &result.actual_target_rr).expect("rr") >= Decimal::from(2)
            );
        }
    }
}
