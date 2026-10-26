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
        if start_ms % timeframe_ms != 0 || end_ms % timeframe_ms != 0 {
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
                || (time - self.start_ms) % self.timeframe_ms != 0
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn k7_v03_relative_seven_days_hourly_has_exact_168_closed_bars() {
        let range = ClosedRange::relative_closed(1792975244680, 3_600_000, 168).unwrap();
        assert_eq!(range.start_ms, 1792368000000);
        assert_eq!(range.end_ms, 1792972800000);
        assert_eq!(range.expected_bars, 168);
        assert_eq!(range.end_ms % range.timeframe_ms, 0);
        assert_eq!(
            range.coverage((0..168).map(|i| range.start_ms + i * range.timeframe_ms))
                .complete,
            true
        );
        assert_eq!(
            ClosedRange::normalize(
                range.start_ms + 1,
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
        assert_eq!(ClosedRange::strict(1, 3_600_000, 3_600_000), Err("RANGE_NOT_ALIGNED"));
        assert_eq!(ClosedRange::normalize(1, 3_600_000, 3_600_000), Err("EMPTY_RANGE"));
        assert_eq!(ClosedRange::relative_closed(100, 3_600_000, 168), Err("RANGE_UNAVAILABLE"));
        assert_eq!(ClosedRange::normalize(u64::MAX, u64::MAX, 60_000), Err("RANGE_OVERFLOW"));
    }
}
