use rust_decimal::Decimal;

use crate::AnalysisError;

pub const SAMPLE_COVARIANCE_FORMULA_V1: &str = "sample-covariance/v1";

pub fn sample_covariance_matrix(series: &[Vec<Decimal>]) -> Result<Vec<Vec<Decimal>>, AnalysisError> {
    if series.is_empty() {
        return Ok(Vec::new());
    }
    let samples = series[0].len();
    if samples < 2 {
        return Err(AnalysisError::InsufficientStatisticalSamples(samples));
    }
    if series.iter().any(|values| values.len() != samples) {
        return Err(AnalysisError::StatisticalSeriesLengthMismatch);
    }

    let sample_count = Decimal::from(samples as u64);
    let denominator = Decimal::from((samples - 1) as u64);
    let means = series
        .iter()
        .map(|values| values.iter().copied().sum::<Decimal>() / sample_count)
        .collect::<Vec<_>>();

    let count = series.len();
    let mut matrix = vec![vec![Decimal::ZERO; count]; count];
    for left in 0..count {
        for right in 0..count {
            let sum = (0..samples).fold(Decimal::ZERO, |acc, index| {
                acc + (series[left][index] - means[left])
                    * (series[right][index] - means[right])
            });
            matrix[left][right] = sum / denominator;
        }
    }
    Ok(matrix)
}

pub fn covariance_correlation(
    covariance: Decimal,
    left_variance: Decimal,
    right_variance: Decimal,
) -> Option<Decimal> {
    let left = decimal_sqrt(left_variance.max(Decimal::ZERO));
    let right = decimal_sqrt(right_variance.max(Decimal::ZERO));
    let denominator = left * right;
    (denominator > Decimal::ZERO).then(|| covariance / denominator)
}

pub fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let two = Decimal::from(2_u32);
    let mut estimate = if value > Decimal::ONE {
        value / two
    } else {
        Decimal::ONE
    };
    for _ in 0..64 {
        let next = (estimate + value / estimate) / two;
        if next == estimate {
            break;
        }
        estimate = next;
    }
    estimate.normalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal")
    }

    #[test]
    fn covariance_is_symmetric_and_identical_series_have_unit_correlation() {
        let input = vec![
            vec![d("0.1"), d("-0.1"), d("0.1")],
            vec![d("0.1"), d("-0.1"), d("0.1")],
        ];
        let matrix = sample_covariance_matrix(&input).expect("matrix");
        assert_eq!(matrix[0][1], matrix[1][0]);
        assert_eq!(
            covariance_correlation(matrix[0][1], matrix[0][0], matrix[1][1])
                .expect("correlation")
                .normalize()
                .to_string(),
            "1"
        );
    }

    #[test]
    fn zero_variance_has_no_correlation() {
        assert_eq!(
            covariance_correlation(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            None
        );
    }

    #[test]
    fn mismatched_series_fail_closed() {
        assert!(matches!(
            sample_covariance_matrix(&[vec![d("1"), d("2")], vec![d("1")]]),
            Err(AnalysisError::StatisticalSeriesLengthMismatch)
        ));
    }
}
