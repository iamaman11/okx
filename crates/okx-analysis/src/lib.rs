mod risk;

pub use risk::{
    ACCOUNT_RISK_ANALYSIS_SCHEMA_V1, AccountRiskAnalysis, PositionRiskAnalysis,
    analyze_account_risk,
};

use std::str::FromStr;

use okx_observation::{
    FeeScheduleSnapshot, FundingRequirement, InstrumentRulesSnapshot, MarketSnapshot,
};
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

pub const COST_ANALYSIS_SCHEMA_V1: &str = "okx.cost-analysis/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionDirection {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingProjection {
    pub exchange_rate: String,
    pub user_cost_quote: String,
    pub funding_time_ms: String,
    pub next_funding_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CostAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub market_generation: String,
    pub fee_generation: String,
    pub contracts: String,
    pub contract_value: String,
    pub mark_price: String,
    pub base_quantity: String,
    pub quote_notional: String,
    pub liquidity_role: LiquidityRole,
    pub exchange_fee_rate: String,
    pub user_trading_cost_quote: String,
    pub funding: Option<FundingProjection>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AnalysisError {
    #[error("analysis input instrument mismatch")]
    InstrumentMismatch,
    #[error("analysis input reference generation mismatch")]
    ReferenceGenerationMismatch,
    #[error("fee schedule is not exact for the requested instrument")]
    FeeScheduleNotExact,
    #[error("unsupported contract mechanics '{0}'")]
    UnsupportedContractMechanics(String),
    #[error("required funding state is missing")]
    MissingFunding,
    #[error("funding semantics are unknown")]
    UnknownFundingSemantics,
    #[error("invalid decimal field '{field}': '{value}'")]
    InvalidDecimal { field: &'static str, value: String },
    #[error("decimal field '{0}' must be positive")]
    NonPositive(&'static str),
    #[error("decimal field '{0}' must not be negative")]
    Negative(&'static str),
    #[error("unsupported account mode '{0}' for derivative risk analysis")]
    UnsupportedAccountMode(String),
    #[error("unsupported position mode '{0}'")]
    UnsupportedPositionMode(String),
    #[error(
        "unsupported non-zero position type '{instrument_type}' for instrument '{instrument_id}'"
    )]
    UnsupportedPositionType {
        instrument_id: String,
        instrument_type: String,
    },
    #[error("unsupported position side '{position_side}' for instrument '{instrument_id}'")]
    UnsupportedPositionSide {
        instrument_id: String,
        position_side: String,
    },
    #[error(
        "position direction is inconsistent for instrument '{instrument_id}': side '{position_side}', position '{position}'"
    )]
    InconsistentPositionDirection {
        instrument_id: String,
        position_side: String,
        position: String,
    },
    #[error("non-zero position '{0}' is missing notionalUsd")]
    MissingPositionNotional(String),
    #[error(
        "estimated liquidation price is on the non-adverse side of mark price for instrument '{0}'"
    )]
    InconsistentLiquidationPrice(String),
}

pub fn analyze_cost(
    rules: &InstrumentRulesSnapshot,
    market: &MarketSnapshot,
    fees: &FeeScheduleSnapshot,
    contracts: &str,
    liquidity_role: LiquidityRole,
    direction: PositionDirection,
) -> Result<CostAnalysis, AnalysisError> {
    let instrument = &rules.instrument;
    if instrument.instrument_id != market.instrument_id
        || instrument.instrument_id != fees.instrument_id
    {
        return Err(AnalysisError::InstrumentMismatch);
    }
    if rules.reference_generation != market.reference_generation
        || rules.reference_generation != fees.reference_generation
    {
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

    let contracts = positive_decimal("contracts", contracts)?;
    let contract_value_text = instrument
        .contract_value
        .as_deref()
        .ok_or_else(|| AnalysisError::UnsupportedContractMechanics("missing ctVal".to_owned()))?;
    let contract_value = positive_decimal("contract_value", contract_value_text)?;
    let mark_price = positive_decimal("mark_price", &market.mark_price.price)?;

    let base_quantity = contracts * contract_value;
    let quote_notional = base_quantity * mark_price;

    let fee_rate_text = match liquidity_role {
        LiquidityRole::Maker => &fees.maker_rate,
        LiquidityRole::Taker => &fees.taker_rate,
    };
    let fee_rate = decimal("fee_rate", fee_rate_text)?;
    let user_trading_cost = -(quote_notional * fee_rate);

    let funding = match instrument.funding_requirement {
        FundingRequirement::Required => {
            let funding = market
                .funding
                .as_ref()
                .ok_or(AnalysisError::MissingFunding)?;
            let rate = decimal("funding_rate", &funding.rate)?;
            let signed = match direction {
                PositionDirection::Long => quote_notional * rate,
                PositionDirection::Short => -(quote_notional * rate),
            };
            Some(FundingProjection {
                exchange_rate: funding.rate.clone(),
                user_cost_quote: signed.normalize().to_string(),
                funding_time_ms: funding.funding_time_ms.clone(),
                next_funding_time_ms: funding.next_funding_time_ms.clone(),
            })
        }
        FundingRequirement::NotApplicable => None,
        FundingRequirement::Unknown => return Err(AnalysisError::UnknownFundingSemantics),
    };

    Ok(CostAnalysis {
        schema: COST_ANALYSIS_SCHEMA_V1.to_owned(),
        instrument_id: instrument.instrument_id.clone(),
        reference_generation: rules.reference_generation.clone(),
        market_generation: market.market_generation.clone(),
        fee_generation: fees.fee_generation.clone(),
        contracts: contracts.normalize().to_string(),
        contract_value: contract_value.normalize().to_string(),
        mark_price: mark_price.normalize().to_string(),
        base_quantity: base_quantity.normalize().to_string(),
        quote_notional: quote_notional.normalize().to_string(),
        liquidity_role,
        exchange_fee_rate: fee_rate.normalize().to_string(),
        user_trading_cost_quote: user_trading_cost.normalize().to_string(),
        funding,
    })
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    if value.is_empty() || value.len() > 64 {
        return Err(AnalysisError::InvalidDecimal {
            field,
            value: value.to_owned(),
        });
    }

    let mut dots = 0_usize;
    let mut digits = 0_usize;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' if dots == 0 => dots += 1,
            b'+' | b'-' if index == 0 => {}
            _ => {
                return Err(AnalysisError::InvalidDecimal {
                    field,
                    value: value.to_owned(),
                });
            }
        }
    }
    if digits == 0 {
        return Err(AnalysisError::InvalidDecimal {
            field,
            value: value.to_owned(),
        });
    }

    Decimal::from_str(value).map_err(|_| AnalysisError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn positive_decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    let value = decimal(field, value)?;
    if value <= Decimal::ZERO {
        return Err(AnalysisError::NonPositive(field));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn okx_negative_fee_rate_becomes_positive_user_cost() {
        let notional = Decimal::from_str("1000").expect("notional");
        let rate = Decimal::from_str("-0.001").expect("rate");
        assert_eq!((-(notional * rate)).normalize().to_string(), "1");
    }

    #[test]
    fn okx_positive_fee_rate_becomes_user_rebate() {
        let notional = Decimal::from_str("1000").expect("notional");
        let rate = Decimal::from_str("0.0008").expect("rate");
        assert_eq!((-(notional * rate)).normalize().to_string(), "-0.8");
    }

    #[test]
    fn funding_direction_sign_is_deterministic() {
        let notional = Decimal::from_str("2500").expect("notional");
        let rate = Decimal::from_str("0.0001").expect("rate");
        assert_eq!((notional * rate).normalize().to_string(), "0.25");
        assert_eq!((-(notional * rate)).normalize().to_string(), "-0.25");
    }

    #[test]
    fn decimal_parser_rejects_non_decimal_text() {
        assert!(matches!(
            decimal("test", "1e3"),
            Err(AnalysisError::InvalidDecimal { .. })
        ));
    }
}
