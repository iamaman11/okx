//! Exact, explicitly unit-scoped economic checks for research evidence.
//! This does not estimate inverse/option products, FX, or authorize trades.
use rust_decimal::{Decimal, prelude::ToPrimitive};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearContract {
    pub instrument: String,
    pub base_asset: String,
    pub quote_currency: String,
    pub base_per_contract: Decimal,
}

impl LinearContract {
    pub fn from_reference(
        instrument: &str,
        contract_type: Option<&str>,
        contract_value: Option<&str>,
    ) -> Result<Self, String> {
        let mut parts = instrument.split('-');
        let base = parts.next().ok_or("UNSUPPORTED_INSTRUMENT_TYPE")?;
        let quote = parts.next().ok_or("UNSUPPORTED_INSTRUMENT_TYPE")?;
        if base.is_empty()
            || quote.is_empty()
            || parts.next() != Some("SWAP")
            || parts.next().is_some()
            || ![base, quote].iter().all(|s| {
                s.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
        {
            return Err("UNSUPPORTED_INSTRUMENT_TYPE".into());
        }
        if contract_type != Some("linear") {
            return Err("UNSUPPORTED_INVERSE_CONTRACT".into());
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
        self.base_per_contract.to_f64()
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(|| "INVALID_CONTRACT_MULTIPLIER".into())
    }

    pub fn pnl(
        &self,
        contracts: Decimal,
        entry_quote_per_base: Decimal,
        exit_quote_per_base: Decimal,
        fees: &QuoteAmount,
        funding: &QuoteAmount,
    ) -> Result<LinearPnl, String> {
        if contracts < Decimal::ZERO || entry_quote_per_base <= Decimal::ZERO
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
        let quantity = self.base_per_contract.checked_mul(contracts).ok_or("MONEY_OVERFLOW")?;
        let difference = exit_quote_per_base.checked_sub(entry_quote_per_base).ok_or("MONEY_OVERFLOW")?;
        let gross = quantity.checked_mul(difference).ok_or("MONEY_OVERFLOW")?;
        let net = gross.checked_sub(fees.value).and_then(|value| value.checked_sub(funding.value))
            .ok_or("MONEY_OVERFLOW")?;
        Ok(LinearPnl {
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
    pub fn exact(currency: &str, value: Decimal) -> Result<Self, String> {
        if currency.is_empty() || currency.len() > 16
            || !currency.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err("INVALID_CURRENCY".into());
        }
        Ok(Self { currency: currency.into(), value })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinearPnl {
    pub base_quantity: Decimal,
    pub gross_quote: QuoteAmount,
    pub net_quote: QuoteAmount,
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t09_three_linear_contracts_exact_oracle() {
        let contract = LinearContract::from_reference("BTC-USD-SWAP", Some("linear"), Some("0.1")).unwrap();
        let zero = QuoteAmount::exact("USD", Decimal::ZERO).unwrap();
        let result = contract.pnl(Decimal::from(3), Decimal::from(100),
            Decimal::from(110), &zero, &zero).unwrap();
        assert_eq!(result.base_quantity, Decimal::new(3, 1));
        assert_eq!(result.gross_quote.value, Decimal::from(3));
        assert_eq!(result.net_quote.value, Decimal::from(3));
        assert_eq!(result.net_quote.currency, "USD");
    }

    #[test]
    fn t09_unsupported_products_and_currency_mixing_fail_closed() {
        for kind in [Some("inverse"), Some("option"), None] {
            assert_eq!(LinearContract::from_reference("BTC-USD-SWAP", kind, Some("100"))
                .unwrap_err(), "UNSUPPORTED_INVERSE_CONTRACT");
        }
        for value in [Some("0"), Some("-1"), Some("bad")] {
            assert_eq!(LinearContract::from_reference("BTC-USDT-SWAP", Some("linear"), value)
                .unwrap_err(), "INVALID_CONTRACT_MULTIPLIER");
        }
        let usd = QuoteAmount::exact("USD", Decimal::ONE).unwrap();
        let usdt = QuoteAmount::exact("USDT", Decimal::ZERO).unwrap();
        assert_eq!(sum_quote(&[usd, usdt]).unwrap_err(), "CURRENCY_MISMATCH_FX_REQUIRED");
    }
}
