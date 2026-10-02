use rust_decimal::Decimal;
use serde::Serialize;

use crate::{AnalysisError, positive_decimal};

pub const DATED_FUTURE_BASIS_SCHEMA_V1: &str = "okx.dated-future-basis/v1";

const BASIS_POINTS: u64 = 10_000;
const MILLIS_PER_YEAR: u64 = 31_557_600_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DatedFutureBasisAnalysis {
    pub schema: String,
    pub mark_price: String,
    pub index_price: String,
    pub as_of_ms: u64,
    pub expiry_time_ms: u64,
    pub time_to_expiry_ms: u64,
    pub basis_bps: String,
    pub annualized_basis_bps: String,
}

pub fn analyze_dated_future_basis(
    mark_price: &str,
    index_price: &str,
    as_of_ms: u64,
    expiry_time_ms: &str,
) -> Result<DatedFutureBasisAnalysis, AnalysisError> {
    let mark = positive_decimal("mark_price", mark_price)?;
    let index = positive_decimal("index_price", index_price)?;
    let expiry_time_ms = expiry_time_ms
        .parse::<u64>()
        .map_err(|_| AnalysisError::InvalidExpiryTimestamp(expiry_time_ms.to_owned()))?;
    if expiry_time_ms <= as_of_ms {
        return Err(AnalysisError::ExpiryNotFuture {
            expiry_ms: expiry_time_ms,
            as_of_ms,
        });
    }

    let time_to_expiry_ms = expiry_time_ms - as_of_ms;
    let basis_bps = ((mark - index) / index) * Decimal::from(BASIS_POINTS);
    let annualized_basis_bps =
        basis_bps * Decimal::from(MILLIS_PER_YEAR) / Decimal::from(time_to_expiry_ms);

    Ok(DatedFutureBasisAnalysis {
        schema: DATED_FUTURE_BASIS_SCHEMA_V1.to_owned(),
        mark_price: mark.normalize().to_string(),
        index_price: index.normalize().to_string(),
        as_of_ms,
        expiry_time_ms,
        time_to_expiry_ms,
        basis_bps: basis_bps.normalize().to_string(),
        annualized_basis_bps: annualized_basis_bps.normalize().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_year_basis_preserves_basis_bps() {
        let analysis =
            analyze_dated_future_basis("101", "100", 1_000, &(1_000 + MILLIS_PER_YEAR).to_string())
                .expect("basis");

        assert_eq!(analysis.basis_bps, "100");
        assert_eq!(analysis.annualized_basis_bps, "100");
        assert_eq!(analysis.time_to_expiry_ms, MILLIS_PER_YEAR);
    }

    #[test]
    fn half_year_basis_annualizes_to_twice_the_basis() {
        let half_year = MILLIS_PER_YEAR / 2;
        let analysis =
            analyze_dated_future_basis("101", "100", 5_000, &(5_000 + half_year).to_string())
                .expect("basis");

        assert_eq!(analysis.basis_bps, "100");
        assert_eq!(analysis.annualized_basis_bps, "200");
    }

    #[test]
    fn expired_or_invalid_expiry_fails_closed() {
        assert!(matches!(
            analyze_dated_future_basis("101", "100", 10_000, "9999"),
            Err(AnalysisError::ExpiryNotFuture { .. })
        ));
        assert!(matches!(
            analyze_dated_future_basis("101", "100", 10_000, "not-a-time"),
            Err(AnalysisError::InvalidExpiryTimestamp(_))
        ));
    }
}
