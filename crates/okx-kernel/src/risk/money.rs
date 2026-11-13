//! Exact nonnegative campaign amounts matching the legacy 10^28-unit contract.
//! No floating-point arithmetic, clocks, host storage or external effects.
use num_bigint::BigUint;
use std::fmt;
use thiserror::Error;

const SCALE_DIGITS: u32 = 28;
const MAX_TOTAL_DIGITS: usize = 28;

fn quantum() -> BigUint {
    BigUint::from(10_u8).pow(SCALE_DIGITS)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MoneyError {
    #[error("INVALID_DECIMAL_SYNTAX")]
    InvalidSyntax,
    #[error("NUMERIC_RANGE")]
    NumericRange,
    #[error("DECIMAL_MUST_BE_POSITIVE")]
    MustBePositive,
    #[error("NEGATIVE_RISK_AMOUNT")]
    Negative,
}

/// Exact unsigned decimal expressed in legacy 10^28 atomic units.
/// The private representation cannot bypass canonical output range checks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RiskAmount(BigUint);

impl RiskAmount {
    pub fn zero() -> Self {
        Self(BigUint::from(0_u8))
    }

    pub fn parse(raw: &str) -> Result<Self, MoneyError> {
        let (whole, fraction) = match raw.split_once('.') {
            Some(parts) => parts,
            None => (raw, ""),
        };
        if whole.is_empty()
            || whole.len() > 1 && whole.starts_with('0')
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
            || raw.contains('.') && fraction.is_empty()
            || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(MoneyError::InvalidSyntax);
        }
        if whole.len() + fraction.len() > MAX_TOTAL_DIGITS {
            return Err(MoneyError::NumericRange);
        }
        let integer = BigUint::parse_bytes(whole.as_bytes(), 10)
            .ok_or(MoneyError::InvalidSyntax)?;
        let decimal = if fraction.is_empty() {
            BigUint::from(0_u8)
        } else {
            BigUint::parse_bytes(fraction.as_bytes(), 10)
                .ok_or(MoneyError::InvalidSyntax)?
                * BigUint::from(10_u8).pow(SCALE_DIGITS - fraction.len() as u32)
        };
        Ok(Self(integer * quantum() + decimal))
    }

    pub fn parse_positive(raw: &str) -> Result<Self, MoneyError> {
        let parsed = Self::parse(raw)?;
        if parsed.is_zero() {
            Err(MoneyError::MustBePositive)
        } else {
            Ok(parsed)
        }
    }

    pub fn is_zero(&self) -> bool {
        self.0 == BigUint::from(0_u8)
    }

    fn checked_from_units(units: BigUint) -> Result<Self, MoneyError> {
        let result = Self(units);
        result.canonical()?;
        Ok(result)
    }

    pub fn checked_add(&self, rhs: &Self) -> Result<Self, MoneyError> {
        Self::checked_from_units(&self.0 + &rhs.0)
    }

    pub fn checked_sub(&self, rhs: &Self) -> Result<Self, MoneyError> {
        if self < rhs {
            return Err(MoneyError::Negative);
        }
        Self::checked_from_units(&self.0 - &rhs.0)
    }

    /// Legacy Worker conversion: ceil(requested_loss_usdt * usd_per_usdt)
    /// to the 10^-28 quantum, *not* a floating-point multiplication.
    pub fn checked_mul_ceil(&self, rhs: &Self) -> Result<Self, MoneyError> {
        let unit = quantum();
        let product = &self.0 * &rhs.0;
        Self::checked_from_units((product + (&unit - BigUint::from(1_u8))) / unit)
    }

    pub fn canonical(&self) -> Result<String, MoneyError> {
        let unit = quantum();
        let whole = (&self.0 / &unit).to_str_radix(10);
        let fractional = format!("{:0>28}", (&self.0 % &unit).to_str_radix(10));
        let fractional = fractional.trim_end_matches('0');
        if whole.len() + fractional.len() > MAX_TOTAL_DIGITS {
            return Err(MoneyError::NumericRange);
        }
        Ok(if fractional.is_empty() {
            whole
        } else {
            format!("{whole}.{fractional}")
        })
    }
}

impl fmt::Display for RiskAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let canonical = self.canonical().map_err(|_| fmt::Error)?;
        f.write_str(&canonical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risk_book_contract_legacy_decimal_syntax_and_range() {
        assert_eq!(RiskAmount::parse("001"), Err(MoneyError::InvalidSyntax));
        assert_eq!(RiskAmount::parse("+1"), Err(MoneyError::InvalidSyntax));
        assert_eq!(RiskAmount::parse("-1"), Err(MoneyError::InvalidSyntax));
        assert_eq!(RiskAmount::parse(".5"), Err(MoneyError::InvalidSyntax));
        assert_eq!(RiskAmount::parse("1."), Err(MoneyError::InvalidSyntax));
        assert_eq!(RiskAmount::parse("0.0000000000000000000000000000"), Err(MoneyError::NumericRange));
        assert_eq!(RiskAmount::parse_positive("0.0"), Err(MoneyError::MustBePositive));
        assert_eq!(RiskAmount::parse("000.1"), Err(MoneyError::InvalidSyntax));
        assert_eq!(RiskAmount::parse("0.10000").unwrap().to_string(), "0.1");
        assert_eq!(RiskAmount::parse("9999999999999999999999999999").unwrap().to_string(), "9999999999999999999999999999");
        assert_eq!(RiskAmount::parse("0").unwrap().to_string(), "0");
    }

    #[test]
    fn risk_book_contract_exact_ceiling_conversion_and_checked_arithmetic() {
        let atom = RiskAmount::parse("0.000000000000000000000000001").unwrap();
        let quantum = RiskAmount::parse("0.0000000000000000000000000001").unwrap();
        assert_eq!(atom.checked_mul_ceil(&atom).unwrap(), quantum);
        assert_eq!(
            RiskAmount::parse("1.000000001").unwrap()
                .checked_mul_ceil(&RiskAmount::parse("1.001").unwrap()).unwrap().to_string(),
            "1.001000001001"
        );
        assert_eq!(
            RiskAmount::parse("9999999999999999999999999999").unwrap()
                .checked_add(&RiskAmount::parse("1").unwrap()),
            Err(MoneyError::NumericRange)
        );
        assert_eq!(
            RiskAmount::parse("0.1").unwrap()
                .checked_sub(&RiskAmount::parse("0.2").unwrap()),
            Err(MoneyError::Negative)
        );
    }
}
