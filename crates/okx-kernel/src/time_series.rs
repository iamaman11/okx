//! K7 canonical half-open reporting ranges and exact closed-bar coverage.
//! Pure calculations, no venue, SQL, clocks, transport or implicit fill-forward.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "StoredClosedRange")]
pub struct ClosedRange {
    pub start_ms: u64,
    pub end_ms: u64,
    pub timeframe_ms: u64,
    pub expected_bars: u64,
}

/// A persisted expected count is a claim about the grid, not an input that
/// may override its actual size. This also protects direct JSON restores of
/// plans that contain a ClosedRange.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredClosedRange {
    start_ms: u64,
    end_ms: u64,
    timeframe_ms: u64,
    expected_bars: u64,
}

impl TryFrom<StoredClosedRange> for ClosedRange {
    type Error = String;

    fn try_from(stored: StoredClosedRange) -> Result<Self, Self::Error> {
        let range = Self::strict(stored.start_ms, stored.end_ms, stored.timeframe_ms)
            .map_err(|reason| format!("range:{reason}"))?;
        if range.expected_bars != stored.expected_bars {
            return Err("range:EXPECTED_BARS_MISMATCH".into());
        }
        Ok(range)
    }
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

    /// Join a normalized event-time source to the exact candle-open grid.
    ///
    /// This is deliberately a *retrospective* join: event timestamps do not
    /// prove publication or decision-time availability. Sparse sources such as
    /// realized funding may have legitimate empty slots; the caller owns the
    /// source-specific coverage policy. No rounding, shifting or forward fill.
    pub fn align_event_values(
        &self,
        events: impl IntoIterator<Item = (u64, f64)>,
    ) -> Result<EventGrid, &'static str> {
        // Match the existing market source budget before allocating a grid.
        if self.expected_bars > 1_000_000 {
            return Err("GRID_BARS_LIMIT");
        }
        let count = usize::try_from(self.expected_bars).map_err(|_| "GRID_SIZE_OVERFLOW")?;
        let mut values = vec![None; count];
        let mut observed_events = 0usize;
        let mut off_grid_events = 0usize;
        let mut first_off_grid_ms = None;
        let mut previous_time = None;
        for (time, value) in events {
            if previous_time.is_some_and(|prior| prior >= time) {
                return Err("GRID_DUPLICATE_OR_UNORDERED_EVENT_TIME");
            }
            if !value.is_finite() {
                return Err("GRID_NONFINITE_EVENT_VALUE");
            }
            previous_time = Some(time);
            observed_events += 1;
            if let Some(offset) = time.checked_sub(self.start_ms)
                && time < self.end_ms
                && offset.is_multiple_of(self.timeframe_ms)
            {
                values[(offset / self.timeframe_ms) as usize] = Some(value);
            } else {
                off_grid_events += 1;
                first_off_grid_ms.get_or_insert(time);
            }
        }
        let aligned_events = observed_events - off_grid_events;
        let first_empty_grid_ms = values
            .iter()
            .position(Option::is_none)
            .map(|index| self.start_ms + index as u64 * self.timeframe_ms);
        Ok(EventGrid {
            values,
            observed_events,
            aligned_events,
            off_grid_events,
            first_off_grid_ms,
            first_empty_grid_ms,
        })
    }
}

/// Event-time alignment evidence. `first_empty_grid_ms` is not necessarily a
/// missing source observation: periodic funding is intentionally sparse.
#[derive(Debug, Clone, PartialEq)]
pub struct EventGrid {
    pub values: Vec<Option<f64>>,
    pub observed_events: usize,
    pub aligned_events: usize,
    pub off_grid_events: usize,
    pub first_off_grid_ms: Option<u64>,
    pub first_empty_grid_ms: Option<u64>,
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
    /// Eligible observations present only on one side of the exact join.
    pub left_unmatched: usize,
    pub right_unmatched: usize,
    pub first_unmatched_ms: Option<u64>,
    /// Earliest event excluded because its close/publication is not yet known
    /// at the caller's decision cutoff, even if both sides have that event.
    pub first_unavailable_ms: Option<u64>,
}

/// Finite values paired by the same event timestamp.
pub type AlignedValuePairs = Vec<(f64, f64)>;

/// A bounded alignment result with its explicit coverage evidence.
pub type AlignedValueResult = Result<(AlignedValuePairs, AlignmentCounts), &'static str>;

/// Exact inner join of ordered, unique event timestamps, with no as-of
/// forward-fill or look-ahead. The caller supplies a proven availability time
/// for each observation; unknown publication times must not be guessed here.
pub fn align_exact_at(
    left: &[TimedValue],
    right: &[TimedValue],
    decision_ms: u64,
) -> AlignedValueResult {
    fn eligible(
        samples: &[TimedValue],
        decision_ms: u64,
    ) -> Result<(Vec<&TimedValue>, usize, usize, Option<u64>), &'static str> {
        let mut available = Vec::with_capacity(samples.len());
        let (mut unavailable, mut nonfinite) = (0, 0);
        let mut previous = None;
        let mut first_unavailable = None;
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
                if first_unavailable.is_none() {
                    first_unavailable = Some(sample.time_ms);
                }
            } else if !sample.value.is_finite() {
                nonfinite += 1;
            } else {
                available.push(sample);
            }
        }
        Ok((available, unavailable, nonfinite, first_unavailable))
    }
    let (a, left_unavailable, left_nonfinite, first_left_unavailable) =
        eligible(left, decision_ms)?;
    let (b, right_unavailable, right_nonfinite, first_right_unavailable) =
        eligible(right, decision_ms)?;
    let mut pairs = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    let (mut left_unmatched, mut right_unmatched, mut first_unmatched_ms) = (0, 0, None);
    while i < a.len() && j < b.len() {
        match a[i].time_ms.cmp(&b[j].time_ms) {
            std::cmp::Ordering::Less => {
                left_unmatched += 1;
                first_unmatched_ms.get_or_insert(a[i].time_ms);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                right_unmatched += 1;
                first_unmatched_ms.get_or_insert(b[j].time_ms);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                pairs.push((a[i].value, b[j].value));
                i += 1;
                j += 1;
            }
        }
    }
    for sample in &a[i..] {
        left_unmatched += 1;
        first_unmatched_ms.get_or_insert(sample.time_ms);
    }
    for sample in &b[j..] {
        right_unmatched += 1;
        first_unmatched_ms =
            Some(first_unmatched_ms.map_or(sample.time_ms, |time| time.min(sample.time_ms)));
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
            left_unmatched,
            right_unmatched,
            first_unmatched_ms,
            first_unavailable_ms: first_left_unavailable
                .into_iter()
                .chain(first_right_unavailable)
                .min(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t04_t13_restored_closed_range_cannot_forge_grid_size_or_alignment() {
        let range = ClosedRange::strict(60_000, 360_000, 60_000).unwrap();
        let wire = serde_json::to_value(range).unwrap();
        let restored: ClosedRange = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(restored, range);
        assert_eq!(restored.expected_bars, 5);
        assert_eq!(
            restored.coverage([60_000, 120_000, 240_000, 300_000]).first_missing_ms,
            Some(180_000)
        );
        for (field, value) in [
            ("expected_bars", serde_json::json!(1)),
            ("timeframe_ms", serde_json::json!(0)),
            ("timeframe_ms", serde_json::json!(45_000)),
            ("end_ms", serde_json::json!(60_000)),
        ] {
            let mut invalid = wire.clone();
            invalid[field] = value;
            assert!(
                serde_json::from_value::<ClosedRange>(invalid).is_err(),
                "forged field {field} must not create a trusted range"
            );
        }
        let mut extra = wire;
        extra["capture_override"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ClosedRange>(extra).is_err());
    }

    #[test]
    fn t06_sparse_event_grid_is_exact_and_does_not_invent_publication_time() {
        let range = ClosedRange::strict(60, 360, 60).unwrap();
        let joined = range
            .align_event_values([
                (59, 9.0),
                (60, 0.0),
                (180, 0.005),
                (181, 4.0),
                (360, 7.0),
            ])
            .unwrap();
        assert_eq!(
            joined.values,
            vec![Some(0.0), None, Some(0.005), None, None]
        );
        assert_eq!(joined.observed_events, 5);
        assert_eq!(joined.aligned_events, 2);
        assert_eq!(joined.off_grid_events, 3);
        assert_eq!(joined.first_off_grid_ms, Some(59));
        assert_eq!(joined.first_empty_grid_ms, Some(120));
        // A sparse empty slot must never be substituted with the last rate.
        assert_eq!(joined.values[1], None);
        assert_eq!(
            range.align_event_values([(60, 1.0), (60, 1.0)]).unwrap_err(),
            "GRID_DUPLICATE_OR_UNORDERED_EVENT_TIME"
        );
        assert_eq!(
            range.align_event_values([(180, 1.0), (120, 1.0)]).unwrap_err(),
            "GRID_DUPLICATE_OR_UNORDERED_EVENT_TIME"
        );
        assert_eq!(
            range.align_event_values([(60, f64::NAN)]).unwrap_err(),
            "GRID_NONFINITE_EVENT_VALUE"
        );
        assert_eq!(
            range.align_event_values([(60, f64::INFINITY)]).unwrap_err(),
            "GRID_NONFINITE_EVENT_VALUE"
        );
    }

    #[test]
    fn t06_event_grid_does_not_shift_late_or_misaligned_source_observations() {
        let range = ClosedRange::strict(0, 300, 60).unwrap();
        let left = range
            .align_event_values([
                (0, 10.0),
                (60, 20.0),
                (120, 30.0),
                (180, 40.0),
                (240, 50.0),
            ])
            .unwrap();
        let right = range
            .align_event_values([(0, 100.0), (61, 200.0), (120, 300.0), (240, 500.0)])
            .unwrap();
        assert_eq!(left.aligned_events, 5);
        assert_eq!(right.aligned_events, 3);
        assert_eq!(right.first_off_grid_ms, Some(61));
        assert_eq!(right.first_empty_grid_ms, Some(60));
        let exact_pairs = left
            .values
            .iter()
            .zip(right.values.iter())
            .filter_map(|(left, right)| left.zip(*right))
            .collect::<Vec<_>>();
        assert_eq!(
            exact_pairs,
            [(10.0, 100.0), (30.0, 300.0), (50.0, 500.0)]
        );
        let oversized = ClosedRange::strict(0, 1_000_001, 1).unwrap();
        assert_eq!(
            oversized
                .align_event_values(std::iter::empty())
                .unwrap_err(),
            "GRID_BARS_LIMIT"
        );
    }

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
        assert_eq!(counts.left_unmatched, 1);
        assert_eq!(counts.right_unmatched, 0);
        assert_eq!(counts.first_unmatched_ms, Some(60));
        assert_eq!(counts.first_unavailable_ms, Some(60));
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
    fn t06_alignment_distinguishes_shifted_events_from_late_publication() {
        let left = [
            TimedValue {
                time_ms: 60,
                available_at_ms: 120,
                value: 1.0,
            },
            TimedValue {
                time_ms: 120,
                available_at_ms: 180,
                value: 2.0,
            },
            TimedValue {
                time_ms: 180,
                available_at_ms: 240,
                value: 3.0,
            },
        ];
        let right = [
            TimedValue {
                time_ms: 60,
                available_at_ms: 120,
                value: 2.0,
            },
            TimedValue {
                time_ms: 180,
                available_at_ms: 240,
                value: 6.0,
            },
            TimedValue {
                time_ms: 240,
                available_at_ms: 300,
                value: 8.0,
            },
        ];
        let (pairs, counts) = align_exact_at(&left, &right, 300).unwrap();
        assert_eq!(pairs, vec![(1.0, 2.0), (3.0, 6.0)]);
        assert_eq!(counts.left_unmatched, 1);
        assert_eq!(counts.right_unmatched, 1);
        assert_eq!(counts.first_unmatched_ms, Some(120));
        assert_eq!(counts.first_unavailable_ms, None);
        let (early, counts) = align_exact_at(&left, &right, 180).unwrap();
        assert_eq!(early, vec![(1.0, 2.0)]);
        assert_eq!(counts.first_unavailable_ms, Some(180));
        assert_eq!(counts.left_unmatched, 1);
        assert_eq!(counts.right_unmatched, 0);
    }
    #[test]
    fn t04_exact_five_bar_grid_distinguishes_missing_duplicate_and_right_boundary() {
        let range = ClosedRange::strict(60_000, 360_000, 60_000).unwrap();
        let times = [60_000, 120_000, 240_000, 300_000];
        let partial = range.coverage(times);
        assert_eq!(partial.expected_bars, 5);
        assert_eq!(partial.observed_unique_bars, 4);
        assert_eq!(partial.missing_bars, 1);
        assert_eq!(partial.first_missing_ms, Some(180_000));
        assert_eq!(partial.missing_sample_ms, vec![180_000]);
        assert!(!partial.complete);
        let duplicate = range.coverage([60_000, 120_000, 120_000, 240_000, 300_000]);
        assert_eq!(duplicate.duplicate_bars, 1);
        assert_eq!(duplicate.first_missing_ms, Some(180_000));
        let boundary = range.coverage([60_000, 120_000, 180_000, 240_000, 300_000, 360_000]);
        assert_eq!(boundary.observed_unique_bars, 5);
        assert_eq!(boundary.unexpected_bars, 1);
        assert!(!boundary.complete);
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
