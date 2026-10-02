use std::{cmp::Ordering, str::FromStr};

use rust_decimal::Decimal;

use crate::AnalysisError;

pub const RETURN_24H_PCT_METRIC_ID: &str = "return_24h_pct";
pub const RETURN_24H_PCT_METRIC_VERSION_V1: &str = "return_24h_pct/v1";
pub const RETURN_24H_PCT_UNIT: &str = "percent";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Return24hPct {
    value: Decimal,
}

impl Return24hPct {
    pub fn value_text(&self) -> String {
        self.value.normalize().to_string()
    }
}

impl Ord for Return24hPct {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for Return24hPct {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn analyze_return_24h_pct(last: &str, open_24h: &str) -> Result<Return24hPct, AnalysisError> {
    let last = positive_decimal("last", last)?;
    let open = positive_decimal("open_24h", open_24h)?;
    let value = ((last - open) / open) * Decimal::from(100_u32);
    Ok(Return24hPct { value })
}

fn positive_decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
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
    let parsed = Decimal::from_str(value).map_err(|_| AnalysisError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })?;
    if parsed <= Decimal::ZERO {
        return Err(AnalysisError::NonPositive(field));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn return_24h_pct_is_exact_and_directional() {
        assert_eq!(
            analyze_return_24h_pct("110", "100")
                .expect("metric")
                .value_text(),
            "10"
        );
        assert_eq!(
            analyze_return_24h_pct("90", "100")
                .expect("metric")
                .value_text(),
            "-10"
        );
    }

    #[test]
    fn return_24h_pct_rejects_zero_open_and_non_decimal_input() {
        assert!(matches!(
            analyze_return_24h_pct("1", "0"),
            Err(AnalysisError::NonPositive("open_24h"))
        ));
        assert!(matches!(
            analyze_return_24h_pct("1e3", "100"),
            Err(AnalysisError::InvalidDecimal { .. })
        ));
    }

    #[test]
    fn ordering_is_numeric_not_lexical() {
        let two = analyze_return_24h_pct("102", "100").expect("two");
        let ten = analyze_return_24h_pct("110", "100").expect("ten");
        assert!(ten > two);
    }
}
