//! Exact, explicitly unit-scoped economic checks for research evidence.
//! This does not estimate inverse/option products, FX, or authorize trades.
use rust_decimal::{Decimal, prelude::ToPrimitive};
use std::{collections::BTreeMap, str::FromStr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearContract {
    pub instrument: String,
    pub base_asset: String,
    pub quote_currency: String,
    pub base_per_contract: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseQuantity {
    pub asset: String,
    pub value: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpotPair {
    pub instrument: String,
    pub base_asset: String,
    pub quote_currency: String,
}

impl SpotPair {
    pub fn from_instrument(instrument: &str) -> Result<Self, String> {
        let mut parts = instrument.split('-');
        let base = parts.next().ok_or("UNSUPPORTED_INSTRUMENT_TYPE")?;
        let quote = parts.next().ok_or("UNSUPPORTED_INSTRUMENT_TYPE")?;
        if parts.next().is_some()
            || base.is_empty()
            || quote.is_empty()
            || ![base, quote].iter().all(|s| {
                s.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
        {
            return Err("UNSUPPORTED_INSTRUMENT_TYPE".into());
        }
        Ok(Self {
            instrument: instrument.into(),
            base_asset: base.into(),
            quote_currency: quote.into(),
        })
    }

    /// Spot inventory is in base units; trade prices and fees are quote units.
    /// This oracle is for a long inventory round trip, not margin or borrow.
    pub fn pnl(
        &self,
        base_quantity: Decimal,
        entry_quote_per_base: Decimal,
        exit_quote_per_base: Decimal,
        fees: &QuoteAmount,
    ) -> Result<QuotePnl, String> {
        if base_quantity < Decimal::ZERO
            || entry_quote_per_base <= Decimal::ZERO
            || exit_quote_per_base <= Decimal::ZERO
        {
            return Err("INVALID_SPOT_POSITION".into());
        }
        if fees.currency != self.quote_currency {
            return Err("CURRENCY_MISMATCH_FX_REQUIRED".into());
        }
        if fees.value < Decimal::ZERO {
            return Err("INVALID_FEE".into());
        }
        let difference = exit_quote_per_base
            .checked_sub(entry_quote_per_base)
            .ok_or("MONEY_OVERFLOW")?;
        let gross = base_quantity
            .checked_mul(difference)
            .ok_or("MONEY_OVERFLOW")?;
        let net = gross.checked_sub(fees.value).ok_or("MONEY_OVERFLOW")?;
        Ok(QuotePnl {
            base_quantity,
            gross_quote: QuoteAmount::exact(&self.quote_currency, gross)?,
            net_quote: QuoteAmount::exact(&self.quote_currency, net)?,
        })
    }
}

impl LinearContract {
    pub fn from_reference(
        instrument: &str,
        contract_type: Option<&str>,
        contract_value: Option<&str>,
        contract_value_currency: Option<&str>,
        settle_currency: Option<&str>,
    ) -> Result<Self, String> {
        let mut parts = instrument.split('-');
        let base = parts.next().ok_or("UNSUPPORTED_INSTRUMENT_TYPE")?;
        let quote = parts.next().ok_or("UNSUPPORTED_INSTRUMENT_TYPE")?;
        if base.is_empty()
            || quote.is_empty()
            || parts.next() != Some("SWAP")
            || parts.next().is_some()
            || ![base, quote].iter().all(|s| {
                s.bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
        {
            return Err("UNSUPPORTED_INSTRUMENT_TYPE".into());
        }
        if contract_type != Some("linear") {
            return Err("UNSUPPORTED_INVERSE_CONTRACT".into());
        }
        // OKX ctVal is not intrinsically a base-asset quantity: its ctValCcy
        // is the only evidence for that conversion. Never infer the unit from
        // the product name or silently accept a quote-denominated contract.
        match contract_value_currency {
            None | Some("") => return Err("CONTRACT_UNIT_MISSING".into()),
            Some(unit) if unit != base => return Err("CONTRACT_UNIT_MISMATCH".into()),
            _ => {}
        }
        // A base-denominated ctVal does not prove the currency in which the
        // venue realizes PnL. Never net fees/funding in the ticker quote unit
        // unless settlement is explicitly attested to be the same unit.
        match settle_currency {
            None | Some("") => return Err("SETTLEMENT_UNIT_MISSING".into()),
            Some(unit) if unit != quote => return Err("SETTLEMENT_UNIT_MISMATCH".into()),
            _ => {}
        }
        let amount = Decimal::from_str(contract_value.ok_or("CONTRACT_MULTIPLIER_MISSING")?)
            .map_err(|_| "INVALID_CONTRACT_MULTIPLIER")?;
        if amount <= Decimal::ZERO {
            return Err("INVALID_CONTRACT_MULTIPLIER".into());
        }
        Ok(Self {
            instrument: instrument.into(),
            base_asset: base.into(),
            quote_currency: quote.into(),
            base_per_contract: amount,
        })
    }

    pub fn multiplier_f64(&self) -> Result<f64, String> {
        self.base_per_contract
            .to_f64()
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(|| "INVALID_CONTRACT_MULTIPLIER".into())
    }

    /// Convert *contracts* to *base asset* with checked decimal arithmetic.
    /// This is not a quote notional or an FX conversion.
    pub fn contracts_to_base(&self, contracts: Decimal) -> Result<BaseQuantity, String> {
        if contracts < Decimal::ZERO {
            return Err("INVALID_LINEAR_POSITION".into());
        }
        let value = self
            .base_per_contract
            .checked_mul(contracts)
            .ok_or("MONEY_OVERFLOW")?;
        Ok(BaseQuantity {
            asset: self.base_asset.clone(),
            value,
        })
    }

    pub fn pnl(
        &self,
        contracts: Decimal,
        entry_quote_per_base: Decimal,
        exit_quote_per_base: Decimal,
        fees: &QuoteAmount,
        funding: &QuoteAmount,
    ) -> Result<QuotePnl, String> {
        if contracts < Decimal::ZERO
            || entry_quote_per_base <= Decimal::ZERO
            || exit_quote_per_base <= Decimal::ZERO
        {
            return Err("INVALID_LINEAR_POSITION".into());
        }
        if fees.currency != self.quote_currency || funding.currency != self.quote_currency {
            return Err("CURRENCY_MISMATCH_FX_REQUIRED".into());
        }
        if fees.value < Decimal::ZERO {
            return Err("INVALID_FEE".into());
        }
        let quantity = self.contracts_to_base(contracts)?.value;
        let difference = exit_quote_per_base
            .checked_sub(entry_quote_per_base)
            .ok_or("MONEY_OVERFLOW")?;
        let gross = quantity.checked_mul(difference).ok_or("MONEY_OVERFLOW")?;
        let net = gross
            .checked_sub(fees.value)
            .and_then(|value| value.checked_sub(funding.value))
            .ok_or("MONEY_OVERFLOW")?;
        Ok(QuotePnl {
            base_quantity: quantity,
            gross_quote: QuoteAmount::exact(&self.quote_currency, gross)?,
            net_quote: QuoteAmount::exact(&self.quote_currency, net)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteAmount {
    pub currency: String,
    pub value: Decimal,
}
impl QuoteAmount {
    pub fn parse_exact(currency: &str, value: &str) -> Result<Self, String> {
        let value = Decimal::from_str(value).map_err(|_| "INVALID_EXACT_AMOUNT")?;
        Self::exact(currency, value)
    }

    pub fn exact(currency: &str, value: Decimal) -> Result<Self, String> {
        if currency.is_empty()
            || currency.len() > 16
            || !currency
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("INVALID_CURRENCY".into());
        }
        Ok(Self {
            currency: currency.into(),
            value,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotePnl {
    pub base_quantity: Decimal,
    pub gross_quote: QuoteAmount,
    pub net_quote: QuoteAmount,
}

/// Source-compatible name for the original linear-swap oracle result.
pub type LinearPnl = QuotePnl;

/// Mixing quote currencies without an attested FX rate is forbidden.
pub fn sum_quote(amounts: &[QuoteAmount]) -> Result<QuoteAmount, String> {
    let first = amounts.first().ok_or("EMPTY_PORTFOLIO")?;
    let mut sum = Decimal::ZERO;
    for value in amounts {
        if value.currency != first.currency {
            return Err("CURRENCY_MISMATCH_FX_REQUIRED".into());
        }
        sum = sum.checked_add(value.value).ok_or("MONEY_OVERFLOW")?;
    }
    QuoteAmount::exact(&first.currency, sum)
}

/// Counterfactual amounts may be grouped by their own currency without FX.
/// These are independent sums, not a cross-currency portfolio valuation or
/// a replacement for synchronized portfolio risk and account permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrencyTotal {
    pub currency: String,
    pub amount: Decimal,
    pub contributors: usize,
}

/// Deterministic order, checked fixed-point math, no implicit conversion.
pub fn group_quote_by_currency(amounts: &[QuoteAmount]) -> Result<Vec<CurrencyTotal>, String> {
    let mut totals = BTreeMap::<String, (Decimal, usize)>::new();
    for amount in amounts {
        QuoteAmount::exact(&amount.currency, amount.value)?;
        let entry = totals
            .entry(amount.currency.clone())
            .or_insert((Decimal::ZERO, 0));
        entry.0 = entry.0.checked_add(amount.value).ok_or("MONEY_OVERFLOW")?;
        entry.1 = entry.1.checked_add(1).ok_or("COUNT_OVERFLOW")?;
    }
    Ok(totals
        .into_iter()
        .map(|(currency, (amount, contributors))| CurrencyTotal {
            currency,
            amount,
            contributors,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t09_three_linear_contracts_exact_oracle() {
        let contract = LinearContract::from_reference(
            "BTC-USDT-SWAP",
            Some("linear"),
            Some("0.1"),
            Some("BTC"),
            Some("USDT"),
        )
        .unwrap();
        assert_eq!(
            contract.contracts_to_base(Decimal::from(3)).unwrap(),
            BaseQuantity {
                asset: "BTC".into(),
                value: Decimal::new(3, 1)
            }
        );
        let zero = QuoteAmount::exact("USDT", Decimal::ZERO).unwrap();
        let result = contract
            .pnl(
                Decimal::from(3),
                Decimal::from(100),
                Decimal::from(110),
                &zero,
                &zero,
            )
            .unwrap();
        assert_eq!(result.base_quantity, Decimal::new(3, 1));
        assert_eq!(result.gross_quote.value, Decimal::from(3));
        assert_eq!(result.net_quote.value, Decimal::from(3));
        assert_eq!(result.net_quote.currency, "USDT");
    }

    #[test]
    fn t09_unsupported_products_and_currency_mixing_fail_closed() {
        for kind in [Some("inverse"), Some("option"), None] {
            assert_eq!(
                LinearContract::from_reference(
                    "BTC-USD-SWAP",
                    kind,
                    Some("100"),
                    Some("USD"),
                    Some("BTC")
                )
                .unwrap_err(),
                "UNSUPPORTED_INVERSE_CONTRACT"
            );
        }
        for value in [Some("0"), Some("-1"), Some("bad")] {
            assert_eq!(
                LinearContract::from_reference(
                    "BTC-USDT-SWAP",
                    Some("linear"),
                    value,
                    Some("BTC"),
                    Some("USDT")
                )
                .unwrap_err(),
                "INVALID_CONTRACT_MULTIPLIER"
            );
        }
        for unit in [None, Some("")] {
            assert_eq!(
                LinearContract::from_reference(
                    "BTC-USDT-SWAP",
                    Some("linear"),
                    Some("0.01"),
                    unit,
                    Some("USDT")
                )
                .unwrap_err(),
                "CONTRACT_UNIT_MISSING"
            );
        }
        for unit in ["USDT", "USD", "ETH", "btc"] {
            assert_eq!(
                LinearContract::from_reference(
                    "BTC-USDT-SWAP",
                    Some("linear"),
                    Some("0.01"),
                    Some(unit),
                    Some("USDT")
                )
                .unwrap_err(),
                "CONTRACT_UNIT_MISMATCH"
            );
        }
        for unit in [None, Some("")] {
            assert_eq!(
                LinearContract::from_reference(
                    "BTC-USDT-SWAP",
                    Some("linear"),
                    Some("0.01"),
                    Some("BTC"),
                    unit
                )
                .unwrap_err(),
                "SETTLEMENT_UNIT_MISSING"
            );
        }
        for unit in ["BTC", "USD", "ETH", "usdt"] {
            assert_eq!(
                LinearContract::from_reference(
                    "BTC-USDT-SWAP",
                    Some("linear"),
                    Some("0.01"),
                    Some("BTC"),
                    Some(unit)
                )
                .unwrap_err(),
                "SETTLEMENT_UNIT_MISMATCH"
            );
        }
        assert_eq!(
            LinearContract::from_reference(
                "BTC-USDT",
                Some("linear"),
                Some("0.01"),
                Some("BTC"),
                Some("USDT")
            )
            .unwrap_err(),
            "UNSUPPORTED_INSTRUMENT_TYPE"
        );
        let btc = LinearContract::from_reference(
            "BTC-USDT-SWAP",
            Some("linear"),
            Some("100"),
            Some("BTC"),
            Some("USDT"),
        )
        .unwrap();
        assert_eq!(
            btc.contracts_to_base(Decimal::from(-1)).unwrap_err(),
            "INVALID_LINEAR_POSITION"
        );
        assert_eq!(
            btc.contracts_to_base(Decimal::MAX).unwrap_err(),
            "MONEY_OVERFLOW"
        );
        let usd = QuoteAmount::exact("USD", Decimal::ONE).unwrap();
        let usdt = QuoteAmount::exact("USDT", Decimal::ZERO).unwrap();
        assert_eq!(
            sum_quote(&[usd, usdt]).unwrap_err(),
            "CURRENCY_MISMATCH_FX_REQUIRED"
        );
    }

    #[test]
    fn t09_spot_inventory_net_fees_and_portfolio_quotes_are_exact() {
        let spot = SpotPair::from_instrument("BTC-USDT").unwrap();
        assert_eq!(spot.base_asset, "BTC");
        assert_eq!(spot.quote_currency, "USDT");
        let fee = QuoteAmount::exact("USDT", Decimal::new(5, 1)).unwrap();
        let result = spot
            .pnl(
                Decimal::new(125, 3),
                Decimal::from(40_000),
                Decimal::from(40_500),
                &fee,
            )
            .unwrap();
        assert_eq!(result.base_quantity, Decimal::new(125, 3));
        assert_eq!(result.gross_quote.value, Decimal::new(625, 1));
        assert_eq!(result.net_quote.value, Decimal::from(62));
        let swap = LinearContract::from_reference(
            "ETH-USDT-SWAP",
            Some("linear"),
            Some("0.1"),
            Some("ETH"),
            Some("USDT"),
        )
        .unwrap();
        let zero = QuoteAmount::exact("USDT", Decimal::ZERO).unwrap();
        let swap_pnl = swap
            .pnl(
                Decimal::from(2),
                Decimal::from(1_000),
                Decimal::from(1_050),
                &zero,
                &zero,
            )
            .unwrap();
        assert_eq!(swap_pnl.net_quote.value, Decimal::from(10));
        let combined = sum_quote(&[result.net_quote, swap_pnl.net_quote]).unwrap();
        assert_eq!(
            combined,
            QuoteAmount::exact("USDT", Decimal::from(72)).unwrap()
        );
        assert_eq!(
            spot.pnl(
                Decimal::ONE,
                Decimal::ONE,
                Decimal::ONE,
                &QuoteAmount::exact("USD", Decimal::ZERO).unwrap()
            )
            .unwrap_err(),
            "CURRENCY_MISMATCH_FX_REQUIRED"
        );
        assert_eq!(
            spot.pnl(-Decimal::ONE, Decimal::ONE, Decimal::ONE, &zero)
                .unwrap_err(),
            "INVALID_SPOT_POSITION"
        );
        for instrument in ["BTC-USDT-SWAP", "BTC-", "btc-USDT", "BTC--USDT"] {
            assert_eq!(
                SpotPair::from_instrument(instrument).unwrap_err(),
                "UNSUPPORTED_INSTRUMENT_TYPE"
            );
        }
    }

    #[test]
    fn t09_multi_currency_counterfactual_grouping_preserves_units_and_zero() {
        let amounts = [
            QuoteAmount::exact("USDT", Decimal::new(625, 1)).unwrap(),
            QuoteAmount::exact("USD", Decimal::from(-3)).unwrap(),
            QuoteAmount::exact("USDT", Decimal::new(95, 1)).unwrap(),
            QuoteAmount::exact("BTC", Decimal::ZERO).unwrap(),
        ];
        assert_eq!(
            group_quote_by_currency(&amounts).unwrap(),
            vec![
                CurrencyTotal {
                    currency: "BTC".into(),
                    amount: Decimal::ZERO,
                    contributors: 1
                },
                CurrencyTotal {
                    currency: "USD".into(),
                    amount: Decimal::from(-3),
                    contributors: 1
                },
                CurrencyTotal {
                    currency: "USDT".into(),
                    amount: Decimal::from(72),
                    contributors: 2
                },
            ]
        );
        assert_eq!(
            sum_quote(&amounts).unwrap_err(),
            "CURRENCY_MISMATCH_FX_REQUIRED"
        );
        assert_eq!(
            QuoteAmount::parse_exact("USDT", "62.50").unwrap(),
            amounts[0]
        );
        assert_eq!(
            QuoteAmount::parse_exact("USD", "not-a-number").unwrap_err(),
            "INVALID_EXACT_AMOUNT"
        );
        assert!(group_quote_by_currency(&[]).unwrap().is_empty());
        let overflow = [
            QuoteAmount::exact("USDT", Decimal::MAX).unwrap(),
            QuoteAmount::exact("USDT", Decimal::ONE).unwrap(),
        ];
        assert_eq!(
            group_quote_by_currency(&overflow).unwrap_err(),
            "MONEY_OVERFLOW"
        );
        assert_eq!(
            group_quote_by_currency(&[QuoteAmount {
                currency: "usd".into(),
                value: Decimal::ONE
            }])
            .unwrap_err(),
            "INVALID_CURRENCY"
        );
    }
}
