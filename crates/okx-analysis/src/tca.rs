use std::collections::{BTreeMap, BTreeSet};

use okx_observation::ExchangeFillIdentity;
use rust_decimal::Decimal;
use serde::Serialize;

use super::{AnalysisError, decimal, positive_decimal};

pub const EXECUTION_TCA_SCHEMA_V1: &str = "okx.execution-tca/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TcaSide {
    Buy,
    Sell,
}

impl TcaSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TcaReferencePriceBasis {
    DecisionPrice,
    ArrivalMid,
    Mark,
    LimitPrice,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TcaReference {
    pub price: String,
    pub observed_at_ms: u64,
    pub price_basis: TcaReferencePriceBasis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TcaFeeTotal {
    pub currency: String,
    pub exchange_fee_amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutionTcaAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub side: TcaSide,
    pub reference_price: String,
    pub reference_price_basis: TcaReferencePriceBasis,
    pub reference_observed_at_ms: u64,
    pub first_fill_time_ms: u64,
    pub last_fill_time_ms: u64,
    pub decision_to_first_fill_ms: u64,
    pub decision_to_last_fill_ms: u64,
    pub filled_contracts: String,
    pub maker_contracts: String,
    pub taker_contracts: String,
    pub fill_vwap: String,
    pub slippage_bps: String,
    pub gross_slippage_settle: String,
    pub fee_totals: Vec<TcaFeeTotal>,
    pub settle_fee_cost: Option<String>,
    pub net_execution_cost_settle: Option<String>,
}

pub fn analyze_execution_tca(
    instrument_id: &str,
    side: TcaSide,
    contract_value: &str,
    settle_currency: &str,
    reference: &TcaReference,
    fills: &[ExchangeFillIdentity],
) -> Result<ExecutionTcaAnalysis, AnalysisError> {
    if fills.is_empty() {
        return Err(AnalysisError::EmptyTcaFills);
    }
    if reference.observed_at_ms == 0 {
        return Err(AnalysisError::InvalidTcaReferenceTimestamp);
    }
    let reference_price = positive_decimal("tca.reference_price", &reference.price)?;
    let contract_value = positive_decimal("tca.contract_value", contract_value)?;

    let mut trade_ids = BTreeSet::new();
    let mut filled_contracts = Decimal::ZERO;
    let mut weighted_price = Decimal::ZERO;
    let mut maker_contracts = Decimal::ZERO;
    let mut taker_contracts = Decimal::ZERO;
    let mut fee_totals = BTreeMap::<String, Decimal>::new();
    let mut all_fees_in_settle = true;
    let mut first_fill_time_ms = u64::MAX;
    let mut last_fill_time_ms = 0_u64;

    for fill in fills {
        if fill.instrument_id != instrument_id {
            return Err(AnalysisError::TcaInstrumentMismatch);
        }
        if fill.side != side.as_str() {
            return Err(AnalysisError::TcaSideMismatch);
        }
        if !trade_ids.insert(fill.trade_id.clone()) {
            return Err(AnalysisError::DuplicateTcaFill(fill.trade_id.clone()));
        }
        if fill.fill_time_ms < reference.observed_at_ms {
            return Err(AnalysisError::TcaFillBeforeReference {
                fill_time_ms: fill.fill_time_ms,
                reference_time_ms: reference.observed_at_ms,
            });
        }

        let size = positive_decimal("tca.fill_size", &fill.fill_size)?;
        let price = positive_decimal("tca.fill_price", &fill.fill_price)?;
        filled_contracts += size;
        weighted_price += size * price;

        match fill.execution_type.as_deref() {
            Some("M") => maker_contracts += size,
            Some("T") => taker_contracts += size,
            Some(other) => {
                return Err(AnalysisError::UnsupportedTcaExecutionType(
                    other.to_owned(),
                ));
            }
            None => {
                return Err(AnalysisError::UnsupportedTcaExecutionType(
                    "<missing>".to_owned(),
                ));
            }
        }

        if let Some(fee_text) = fill.fee.as_deref() {
            let fee = decimal("tca.fee", fee_text)?;
            let currency = fill
                .fee_currency
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .ok_or(AnalysisError::MissingTcaFeeCurrency)?;
            *fee_totals.entry(currency.to_owned()).or_insert(Decimal::ZERO) += fee;
            if currency != settle_currency {
                all_fees_in_settle = false;
            }
        }

        first_fill_time_ms = first_fill_time_ms.min(fill.fill_time_ms);
        last_fill_time_ms = last_fill_time_ms.max(fill.fill_time_ms);
    }

    let fill_vwap = weighted_price / filled_contracts;
    let adverse_price_delta = match side {
        TcaSide::Buy => fill_vwap - reference_price,
        TcaSide::Sell => reference_price - fill_vwap,
    };
    let slippage_bps = adverse_price_delta / reference_price * Decimal::from(10_000_u32);
    let gross_slippage_settle = adverse_price_delta * filled_contracts * contract_value;

    let fee_totals = fee_totals
        .into_iter()
        .map(|(currency, amount)| TcaFeeTotal {
            currency,
            exchange_fee_amount: amount.normalize().to_string(),
        })
        .collect::<Vec<_>>();

    let settle_fee_cost = if all_fees_in_settle {
        let raw_settle_fee = fee_totals
            .iter()
            .find(|value| value.currency == settle_currency)
            .map(|value| decimal("tca.settle_fee", &value.exchange_fee_amount))
            .transpose()?
            .unwrap_or(Decimal::ZERO);
        Some((-raw_settle_fee).normalize().to_string())
    } else {
        None
    };
    let net_execution_cost_settle = settle_fee_cost
        .as_deref()
        .map(|fee| decimal("tca.settle_fee_cost", fee))
        .transpose()?
        .map(|fee| (gross_slippage_settle + fee).normalize().to_string());

    Ok(ExecutionTcaAnalysis {
        schema: EXECUTION_TCA_SCHEMA_V1.to_owned(),
        instrument_id: instrument_id.to_owned(),
        side,
        reference_price: reference_price.normalize().to_string(),
        reference_price_basis: reference.price_basis,
        reference_observed_at_ms: reference.observed_at_ms,
        first_fill_time_ms,
        last_fill_time_ms,
        decision_to_first_fill_ms: first_fill_time_ms - reference.observed_at_ms,
        decision_to_last_fill_ms: last_fill_time_ms - reference.observed_at_ms,
        filled_contracts: filled_contracts.normalize().to_string(),
        maker_contracts: maker_contracts.normalize().to_string(),
        taker_contracts: taker_contracts.normalize().to_string(),
        fill_vwap: fill_vwap.normalize().to_string(),
        slippage_bps: slippage_bps.normalize().to_string(),
        gross_slippage_settle: gross_slippage_settle.normalize().to_string(),
        fee_totals,
        settle_fee_cost,
        net_execution_cost_settle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill(
        trade_id: &str,
        price: &str,
        size: &str,
        execution_type: &str,
        fee: &str,
        fill_time_ms: u64,
    ) -> ExchangeFillIdentity {
        ExchangeFillIdentity {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            order_id: Some("ord-1".to_owned()),
            client_order_id: "managed-client".to_owned(),
            trade_id: trade_id.to_owned(),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            fill_price: price.to_owned(),
            fill_size: size.to_owned(),
            fee: Some(fee.to_owned()),
            fee_currency: Some("USDT".to_owned()),
            execution_type: Some(execution_type.to_owned()),
            fill_time_ms,
        }
    }

    #[test]
    fn tca_reconciles_vwap_liquidity_fees_slippage_and_latency() {
        let fills = vec![
            fill("trade-1", "100", "1", "M", "-0.1", 1_100),
            fill("trade-2", "101", "1", "T", "-0.1", 1_200),
        ];
        let result = analyze_execution_tca(
            "BTC-USDT-SWAP",
            TcaSide::Buy,
            "1",
            "USDT",
            &TcaReference {
                price: "100".to_owned(),
                observed_at_ms: 1_000,
                price_basis: TcaReferencePriceBasis::DecisionPrice,
            },
            &fills,
        )
        .expect("tca");

        assert_eq!(result.filled_contracts, "2");
        assert_eq!(result.maker_contracts, "1");
        assert_eq!(result.taker_contracts, "1");
        assert_eq!(result.fill_vwap, "100.5");
        assert_eq!(result.slippage_bps, "50");
        assert_eq!(result.gross_slippage_settle, "1");
        assert_eq!(result.settle_fee_cost.as_deref(), Some("0.2"));
        assert_eq!(result.net_execution_cost_settle.as_deref(), Some("1.2"));
        assert_eq!(result.decision_to_first_fill_ms, 100);
        assert_eq!(result.decision_to_last_fill_ms, 200);
    }

    #[test]
    fn non_settle_fee_keeps_observed_fee_but_withholds_net_cost() {
        let mut row = fill("trade-1", "100", "1", "T", "-0.00001", 1_100);
        row.fee_currency = Some("BTC".to_owned());
        let result = analyze_execution_tca(
            "BTC-USDT-SWAP",
            TcaSide::Buy,
            "0.01",
            "USDT",
            &TcaReference {
                price: "100".to_owned(),
                observed_at_ms: 1_000,
                price_basis: TcaReferencePriceBasis::ArrivalMid,
            },
            &[row],
        )
        .expect("tca");
        assert_eq!(result.fee_totals[0].currency, "BTC");
        assert!(result.settle_fee_cost.is_none());
        assert!(result.net_execution_cost_settle.is_none());
    }

    #[test]
    fn tca_fails_closed_on_unknown_liquidity_role_or_pre_reference_fill() {
        let mut unknown = fill("trade-1", "100", "1", "X", "-0.1", 1_100);
        assert!(matches!(
            analyze_execution_tca(
                "BTC-USDT-SWAP",
                TcaSide::Buy,
                "1",
                "USDT",
                &TcaReference {
                    price: "100".to_owned(),
                    observed_at_ms: 1_000,
                    price_basis: TcaReferencePriceBasis::DecisionPrice,
                },
                &[unknown.clone()],
            ),
            Err(AnalysisError::UnsupportedTcaExecutionType(_))
        ));

        unknown.execution_type = Some("T".to_owned());
        unknown.fill_time_ms = 999;
        assert!(matches!(
            analyze_execution_tca(
                "BTC-USDT-SWAP",
                TcaSide::Buy,
                "1",
                "USDT",
                &TcaReference {
                    price: "100".to_owned(),
                    observed_at_ms: 1_000,
                    price_basis: TcaReferencePriceBasis::DecisionPrice,
                },
                &[unknown],
            ),
            Err(AnalysisError::TcaFillBeforeReference { .. })
        ));
    }
}
