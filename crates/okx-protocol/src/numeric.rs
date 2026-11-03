//! One exact ASCII wire-decimal grammar shared by public trading fields.
//! Monetary calculations remain in the existing Rust Decimal/Worker BigInt owners.
pub const MAX_DECIMAL_DIGITS: usize = 28;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalInputError {
    InvalidSyntax,
    NumericRange,
    NonPositive,
}

impl DecimalInputError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSyntax => "INVALID_DECIMAL_SYNTAX",
            Self::NumericRange => "NUMERIC_RANGE",
            Self::NonPositive => "DECIMAL_MUST_BE_POSITIVE",
        }
    }
}

/// Strict input grammar: no exponent, plus, whitespace, leading-zero integer,
/// locale separators or binary float conversion. The 28-digit bound counts
/// *every* input digit before normalizing trailing fractional zeros.
pub fn canonical_decimal(
    raw: &str,
    signed: bool,
    require_positive: bool,
) -> Result<String, DecimalInputError> {
    let (negative, unsigned) = match raw.strip_prefix('-') {
        Some(rest) if signed => (true, rest),
        Some(_) => return Err(DecimalInputError::InvalidSyntax),
        None => (false, raw),
    };
    let (whole, fraction) = match unsigned.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (unsigned, None),
    };
    if whole.is_empty()
        || whole.len() > 1 && whole.starts_with('0')
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || fraction.is_some_and(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(DecimalInputError::InvalidSyntax);
    }
    let fractional = fraction.unwrap_or("");
    if whole.len() + fractional.len() > MAX_DECIMAL_DIGITS {
        return Err(DecimalInputError::NumericRange);
    }
    let nonzero = whole
        .bytes()
        .chain(fractional.bytes())
        .any(|digit| digit != b'0');
    if require_positive && !nonzero {
        return Err(DecimalInputError::NonPositive);
    }
    if !nonzero {
        return Ok("0".to_owned());
    }
    let normalized_fraction = fractional.trim_end_matches('0');
    let mut result = String::new();
    if negative {
        result.push('-');
    }
    result.push_str(whole);
    if !normalized_fraction.is_empty() {
        result.push('.');
        result.push_str(normalized_fraction);
    }
    Ok(result)
}

pub fn positive_decimal(raw: &str) -> Result<String, DecimalInputError> {
    canonical_decimal(raw, false, true)
}

pub fn signed_decimal(raw: &str) -> Result<String, DecimalInputError> {
    canonical_decimal(raw, true, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s2_01_decimal_semantic_equivalence_and_signed_zero() {
        for input in ["5", "5.0", "5.00"] {
            assert_eq!(positive_decimal(input).unwrap(), "5");
        }
        assert_eq!(signed_decimal("-0").unwrap(), "0");
        assert_eq!(signed_decimal("0.00").unwrap(), "0");
        assert_eq!(signed_decimal("-5.0200").unwrap(), "-5.02");
        for input in ["0", "0.0", "-0"] {
            assert!(positive_decimal(input).is_err());
        }
    }

    #[test]
    fn s2_02_03_decimal_range_and_ascii_validation() {
        assert_eq!(
            positive_decimal("9999999999999999999999999999").unwrap(),
            "9999999999999999999999999999"
        );
        assert_eq!(positive_decimal("0.0000000001").unwrap(), "0.0000000001");
        assert_eq!(
            positive_decimal("99999999999999999999999999999"),
            Err(DecimalInputError::NumericRange)
        );
        for input in ["1e-10", " 5", "NaN", "01", "+5", "1.", ".5", "5,00"] {
            assert_eq!(
                positive_decimal(input),
                Err(DecimalInputError::InvalidSyntax),
                "{input}"
            );
        }
        // The raw 28-digit limit counts zeros discarded by canonicalization.
        assert_eq!(
            signed_decimal("0.0000000000000000000000000000"),
            Err(DecimalInputError::NumericRange)
        );
    }
}
