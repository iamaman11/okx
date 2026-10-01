use std::time::{Duration, Instant};

use chrono::{SecondsFormat, TimeZone, Utc};
use serde::Serialize;

use crate::error::OkxError;

pub const MAX_CLOCK_ABS_OFFSET_MS: u64 = 5_000;
pub const MAX_CLOCK_RTT_MS: u64 = 2_000;
pub const MAX_CLOCK_EVIDENCE_AGE_MS: u64 = 2_000;
pub const MUTATION_REQUEST_TTL_MS: u64 = 5_000;
pub const MAX_MUTATION_REQUEST_TTL_MS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClockEvidenceSnapshot {
    pub server_time_ms: u64,
    pub local_midpoint_ms: u64,
    pub offset_ms: i64,
    pub round_trip_ms: u64,
    pub age_ms: u64,
    pub max_abs_offset_ms: u64,
    pub max_round_trip_ms: u64,
    pub max_age_ms: u64,
    pub accepted: bool,
}

#[derive(Debug, Clone)]
pub struct ClockEvidence {
    server_time_ms: u64,
    local_midpoint_ms: u64,
    offset_ms: i64,
    round_trip_ms: u64,
    observed_at: Instant,
}

impl ClockEvidence {
    pub(crate) fn from_sample(
        server_time_ms: u64,
        local_started_ms: u64,
        round_trip: Duration,
        observed_at: Instant,
    ) -> Result<Self, OkxError> {
        let round_trip_ms = u64::try_from(round_trip.as_millis())
            .map_err(|_| OkxError::Clock("clock round-trip duration overflowed u64".to_owned()))?;
        let local_midpoint_ms = local_started_ms
            .checked_add(round_trip_ms / 2)
            .ok_or_else(|| OkxError::Clock("clock midpoint overflowed u64".to_owned()))?;
        let offset = i128::from(server_time_ms) - i128::from(local_midpoint_ms);
        let offset_ms = i64::try_from(offset)
            .map_err(|_| OkxError::Clock("clock offset overflowed i64".to_owned()))?;

        Ok(Self {
            server_time_ms,
            local_midpoint_ms,
            offset_ms,
            round_trip_ms,
            observed_at,
        })
    }

    pub fn snapshot(&self) -> ClockEvidenceSnapshot {
        let age_ms = u64::try_from(self.observed_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let accepted = self.offset_ms.unsigned_abs() <= MAX_CLOCK_ABS_OFFSET_MS
            && self.round_trip_ms <= MAX_CLOCK_RTT_MS
            && age_ms <= MAX_CLOCK_EVIDENCE_AGE_MS;

        ClockEvidenceSnapshot {
            server_time_ms: self.server_time_ms,
            local_midpoint_ms: self.local_midpoint_ms,
            offset_ms: self.offset_ms,
            round_trip_ms: self.round_trip_ms,
            age_ms,
            max_abs_offset_ms: MAX_CLOCK_ABS_OFFSET_MS,
            max_round_trip_ms: MAX_CLOCK_RTT_MS,
            max_age_ms: MAX_CLOCK_EVIDENCE_AGE_MS,
            accepted,
        }
    }

    pub fn mutation_timing(&self, ttl_ms: u64) -> Result<MutationTiming, OkxError> {
        let snapshot = self.snapshot();
        if !snapshot.accepted {
            return Err(OkxError::Clock(format!(
                "unsafe mutation clock evidence: offset_ms={} round_trip_ms={} age_ms={} bounds=offset<={} rtt<={} age<={}",
                snapshot.offset_ms,
                snapshot.round_trip_ms,
                snapshot.age_ms,
                snapshot.max_abs_offset_ms,
                snapshot.max_round_trip_ms,
                snapshot.max_age_ms
            )));
        }

        let estimated_server_now_ms = self
            .server_time_ms
            .checked_add(self.round_trip_ms / 2)
            .and_then(|value| value.checked_add(snapshot.age_ms))
            .ok_or_else(|| OkxError::Clock("estimated server time overflowed u64".to_owned()))?;

        MutationTiming::from_exchange_time_ms(estimated_server_now_ms, ttl_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationTiming {
    request_time_ms: u64,
    request_timestamp: String,
    exp_time_ms: u64,
}

impl MutationTiming {
    pub fn from_exchange_time_ms(request_time_ms: u64, ttl_ms: u64) -> Result<Self, OkxError> {
        if ttl_ms == 0 || ttl_ms > MAX_MUTATION_REQUEST_TTL_MS {
            return Err(OkxError::Clock(format!(
                "mutation request TTL must be within 1..={MAX_MUTATION_REQUEST_TTL_MS} ms"
            )));
        }

        let request_timestamp = timestamp_from_unix_ms(request_time_ms)?;
        let exp_time_ms = request_time_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| OkxError::Clock("mutation deadline overflowed u64".to_owned()))?;

        Ok(Self {
            request_time_ms,
            request_timestamp,
            exp_time_ms,
        })
    }

    pub const fn request_time_ms(&self) -> u64 {
        self.request_time_ms
    }

    pub fn request_timestamp(&self) -> &str {
        &self.request_timestamp
    }

    pub const fn exp_time_ms(&self) -> u64 {
        self.exp_time_ms
    }
}

fn timestamp_from_unix_ms(value: u64) -> Result<String, OkxError> {
    let value = i64::try_from(value)
        .map_err(|_| OkxError::Clock("Unix millisecond timestamp exceeds i64".to_owned()))?;
    let timestamp = Utc
        .timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| OkxError::Clock("invalid Unix millisecond timestamp".to_owned()))?;
    Ok(timestamp.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(
        server_time_ms: u64,
        local_started_ms: u64,
        round_trip_ms: u64,
        age_ms: u64,
    ) -> ClockEvidence {
        let now = Instant::now();
        let observed_at = now
            .checked_sub(Duration::from_millis(age_ms))
            .expect("test age must fit");
        ClockEvidence::from_sample(
            server_time_ms,
            local_started_ms,
            Duration::from_millis(round_trip_ms),
            observed_at,
        )
        .expect("clock evidence")
    }

    #[test]
    fn offset_and_rtt_boundaries_are_fail_closed() {
        let positive_boundary = evidence(1_005_500, 1_000_000, 1_000, 0).snapshot();
        assert_eq!(positive_boundary.offset_ms, 5_000);
        assert!(positive_boundary.accepted);

        let positive_outside = evidence(1_005_501, 1_000_000, 1_000, 0).snapshot();
        assert_eq!(positive_outside.offset_ms, 5_001);
        assert!(!positive_outside.accepted);

        let negative_boundary = evidence(995_500, 1_000_000, 1_000, 0).snapshot();
        assert_eq!(negative_boundary.offset_ms, -5_000);
        assert!(negative_boundary.accepted);

        let negative_outside = evidence(995_499, 1_000_000, 1_000, 0).snapshot();
        assert_eq!(negative_outside.offset_ms, -5_001);
        assert!(!negative_outside.accepted);

        assert!(evidence(1_001_000, 1_000_000, 2_000, 0).snapshot().accepted);
        assert!(!evidence(1_001_000, 1_000_000, 2_001, 0).snapshot().accepted);
    }

    #[test]
    fn stale_evidence_is_rejected_without_sleeping() {
        let stale = evidence(1_000_000, 1_000_000, 0, MAX_CLOCK_EVIDENCE_AGE_MS + 1);
        assert!(!stale.snapshot().accepted);
        assert!(matches!(
            stale.mutation_timing(MUTATION_REQUEST_TTL_MS),
            Err(OkxError::Clock(_))
        ));
    }

    #[test]
    fn mutation_timing_uses_exchange_time_and_bounded_ttl() {
        let timing =
            MutationTiming::from_exchange_time_ms(1_607_428_137_715, MUTATION_REQUEST_TTL_MS)
                .expect("timing");

        assert_eq!(timing.request_time_ms(), 1_607_428_137_715);
        assert_eq!(timing.request_timestamp(), "2020-12-08T11:48:57.715Z");
        assert_eq!(timing.exp_time_ms(), 1_607_428_142_715);
        assert!(MutationTiming::from_exchange_time_ms(1, 0).is_err());
        assert!(MutationTiming::from_exchange_time_ms(1, MAX_MUTATION_REQUEST_TTL_MS + 1).is_err());
    }
}
