//! K7 canonical half-open reporting ranges and exact closed-bar coverage.
//! Pure calculations, no venue, SQL, clocks, transport or implicit fill-forward.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedRange {
    pub start_ms: u64,
    pub end_ms: u64,
    pub timeframe_ms: u64,
    pub expected_bars: u64,
}

impl ClosedRange {
    pub fn strict(start_ms: u64, end_ms: u64, timeframe_ms: u64) -> Result<Self, &'static str> {
        if timeframe_ms == 0 {
            return Err("INVALID_TIMEFRAME");
        }
        if !start_ms.is_multiple_of(timeframe_ms) || !end_ms.is_multiple_of(timeframe_ms) {
            return Err("RANGE_NOT_ALIGNED");
        }
        if end_ms <= start_ms {
            return Err("EMPTY_RANGE");
        }
        Ok(Self {
            start_ms,
            end_ms,
            timeframe_ms,
            expected_bars: (end_ms - start_ms) / timeframe_ms,
        })
    }

    /// Round *inward* to fully reported candle boundaries.
    pub fn normalize(
        requested_start_ms: u64,
        requested_end_ms: u64,
        timeframe_ms: u64,
    ) -> Result<Self, &'static str> {
        if timeframe_ms == 0 {
            return Err("INVALID_TIMEFRAME");
        }
        let rem = requested_start_ms % timeframe_ms;
        let start = if rem == 0 {
            requested_start_ms
        } else {
            requested_start_ms
                .checked_add(timeframe_ms - rem)
                .ok_or("RANGE_OVERFLOW")?
        };
        let end = (requested_end_ms / timeframe_ms) * timeframe_ms;
        Self::strict(start, end, timeframe_ms)
    }

    /// Relative ranges end at the latest fully closed boundary, not 'now'.
    pub fn relative_closed(
        now_ms: u64,
        timeframe_ms: u64,
        bars: u64,
    ) -> Result<Self, &'static str> {
        if timeframe_ms == 0 {
            return Err("INVALID_TIMEFRAME");
        }
        let end_ms = now_ms / timeframe_ms * timeframe_ms;
        let duration = bars.checked_mul(timeframe_ms).ok_or("RANGE_OVERFLOW")?;
        let start_ms = end_ms.checked_sub(duration).ok_or("RANGE_UNAVAILABLE")?;
        Self::strict(start_ms, end_ms, timeframe_ms)
    }

    /// Report missing *timestamps*, not just a misleading total bar count.
    /// Duplicate/out-of-range/off-grid source timestamps cannot heal a gap.
    pub fn coverage(&self, times: impl IntoIterator<Item = u64>) -> BarCoverage {
        let mut observed = BTreeSet::new();
        let mut duplicates = 0u64;
        let mut unexpected = 0u64;
        for time in times {
            if time < self.start_ms
                || time >= self.end_ms
                || !(time - self.start_ms).is_multiple_of(self.timeframe_ms)
            {
                unexpected += 1;
            } else if !observed.insert(time) {
                duplicates += 1;
            }
        }
        let mut missing = 0u64;
        let mut first_missing_ms = None;
        let mut missing_sample_ms = Vec::new();
        for index in 0..self.expected_bars {
            let time = self.start_ms + index * self.timeframe_ms;
            if !observed.contains(&time) {
                missing += 1;
                if first_missing_ms.is_none() {
                    first_missing_ms = Some(time);
                }
                if missing_sample_ms.len() < 16 {
                    missing_sample_ms.push(time);
                }
            }
        }
        BarCoverage {
            expected_bars: self.expected_bars,
            observed_unique_bars: observed.len() as u64,
            missing_bars: missing,
            first_missing_ms,
            missing_sample_ms,
            duplicate_bars: duplicates,
            unexpected_bars: unexpected,
            complete: missing == 0 && duplicates == 0 && unexpected == 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarCoverage {
    pub expected_bars: u64,
    pub observed_unique_bars: u64,
    pub missing_bars: u64,
    pub first_missing_ms: Option<u64>,
    pub missing_sample_ms: Vec<u64>,
    pub duplicate_bars: u64,
    pub unexpected_bars: u64,
    pub complete: bool,
}

/// A source observation with distinct event and availability times. An
/// observation cannot influence a decision made before `available_at_ms`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimedValue {
    pub time_ms: u64,
    pub available_at_ms: u64,
    pub value: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AlignmentCounts {
    pub left_total: usize,
    pub right_total: usize,
    pub left_usable: usize,
    pub right_usable: usize,
    pub left_unavailable: usize,
    pub right_unavailable: usize,
    pub left_nonfinite: usize,
    pub right_nonfinite: usize,
    pub matched: usize,
}

/// Exact inner join of ordered, unique event timestamps, with no as-of
/// forward-fill or look-ahead. The caller supplies a proven availability time
/// for each observation; unknown publication times must not be guessed here.
pub fn align_exact_at(
    left: &[TimedValue],
    right: &[TimedValue],
    decision_ms: u64,
) -> Result<(Vec<(f64, f64)>, AlignmentCounts), &'static str> {
    fn eligible(
        samples: &[TimedValue],
        decision_ms: u64,
    ) -> Result<(Vec<&TimedValue>, usize, usize), &'static str> {
        let mut available = Vec::with_capacity(samples.len());
        let (mut unavailable, mut nonfinite) = (0, 0);
        let mut previous = None;
        for sample in samples {
            if previous.is_some_and(|time| time >= sample.time_ms) {
                return Err("ALIGNMENT_DUPLICATE_OR_UNORDERED_TIME");
            }
            if sample.available_at_ms < sample.time_ms {
                return Err("ALIGNMENT_INVALID_AVAILABILITY");
            }
            previous = Some(sample.time_ms);
            if sample.time_ms >= decision_ms || sample.available_at_ms > decision_ms {
                unavailable += 1;
            } else if !sample.value.is_finite() {
                nonfinite += 1;
            } else {
                available.push(sample);
            }
        }
        Ok((available, unavailable, nonfinite))
    }
    let (a, left_unavailable, left_nonfinite) = eligible(left, decision_ms)?;
    let (b, right_unavailable, right_nonfinite) = eligible(right, decision_ms)?;
    let mut pairs = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].time_ms.cmp(&b[j].time_ms) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                pairs.push((a[i].value, b[j].value));
                i += 1;
                j += 1;
            }
        }
    }
    let matched = pairs.len();
    Ok((
        pairs,
        AlignmentCounts {
            left_total: left.len(),
            right_total: right.len(),
            left_usable: a.len(),
            right_usable: b.len(),
            left_unavailable,
            right_unavailable,
            left_nonfinite,
            right_nonfinite,
            matched,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t06_exact_alignment_respects_availability_and_rejects_ambiguous_time() {
        let left = [
            TimedValue {
                time_ms: 0,
                available_at_ms: 60,
                value: 1.0,
            },
            TimedValue {
                time_ms: 60,
                available_at_ms: 120,
                value: 2.0,
            },
        ];
        let right = [
            TimedValue {
                time_ms: 0,
                available_at_ms: 60,
                value: 2.0,
            },
            TimedValue {
                time_ms: 60,
                available_at_ms: 180,
                value: 4.0,
            },
        ];
        let (pairs, counts) = align_exact_at(&left, &right, 120).unwrap();
        assert_eq!(pairs, vec![(1.0, 2.0)]);
        assert_eq!(counts.left_usable, 2);
        assert_eq!(counts.right_usable, 1);
        assert_eq!(counts.right_unavailable, 1);
        assert_eq!(counts.matched, 1);
        assert_eq!(align_exact_at(&left, &right, 180).unwrap().0.len(), 2);
        let duplicate = [right[0], right[0]];
        assert_eq!(
            align_exact_at(&left, &duplicate, 180).unwrap_err(),
            "ALIGNMENT_DUPLICATE_OR_UNORDERED_TIME"
        );
        let impossible = [TimedValue {
            available_at_ms: 0,
            ..left[1]
        }];
        assert_eq!(
            align_exact_at(&impossible, &right, 180).unwrap_err(),
            "ALIGNMENT_INVALID_AVAILABILITY"
        );
    }

    #[test]
    fn k7_v03_relative_seven_days_hourly_has_exact_168_closed_bars() {
        let range = ClosedRange::relative_closed(1792975244680, 3_600_000, 168).unwrap();
        assert_eq!(range.start_ms, 1792368000000);
        assert_eq!(range.end_ms, 1792972800000);
        assert_eq!(range.expected_bars, 168);
        assert_eq!(range.end_ms % range.timeframe_ms, 0);
        assert!(
            range
                .coverage((0..168).map(|i| range.start_ms + i * range.timeframe_ms))
                .complete
        );
        assert_eq!(
            ClosedRange::normalize(
                range.start_ms - 1,
                range.end_ms + range.timeframe_ms - 1,
                range.timeframe_ms
            )
            .unwrap(),
            range
        );
    }

    #[test]
    fn c13_missing_internal_candle_not_healed_by_duplicate_or_unclosed() {
        let range = ClosedRange::strict(0, 168 * 3_600_000, 3_600_000).unwrap();
        let missing_at = 57 * 3_600_000;
        let mut times = (0..168)
            .filter(|i| *i != 57)
            .map(|i| i * 3_600_000)
            .collect::<Vec<_>>();
        let incomplete = range.coverage(times.iter().copied());
        assert_eq!(incomplete.observed_unique_bars, 167);
        assert_eq!(incomplete.missing_bars, 1);
        assert_eq!(incomplete.first_missing_ms, Some(missing_at));
        assert!(!incomplete.complete);
        times.push(56 * 3_600_000);
        assert_eq!(range.coverage(times.iter().copied()).missing_bars, 1);
        assert_eq!(range.coverage(times.iter().copied()).duplicate_bars, 1);
        times.push(168 * 3_600_000);
        assert_eq!(range.coverage(times).unexpected_bars, 1);
    }

    #[test]
    fn strict_and_normalize_fail_closed_on_invalid_boundaries() {
        assert_eq!(
            ClosedRange::strict(1, 3_600_000, 3_600_000),
            Err("RANGE_NOT_ALIGNED")
        );
        assert_eq!(
            ClosedRange::normalize(1, 3_600_000, 3_600_000),
            Err("EMPTY_RANGE")
        );
        assert_eq!(
            ClosedRange::relative_closed(100, 3_600_000, 168),
            Err("RANGE_UNAVAILABLE")
        );
        assert_eq!(
            ClosedRange::normalize(u64::MAX, u64::MAX, 60_000),
            Err("RANGE_OVERFLOW")
        );
    }
}
