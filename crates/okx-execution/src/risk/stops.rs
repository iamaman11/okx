/// UTC daily rollover has no authority to clear explicit or latched blockers.
pub fn utc_day(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(86_400_000)
}
