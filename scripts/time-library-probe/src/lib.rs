//! Temporary, independent compatibility probe. Never linked into OKX production.

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Months, SecondsFormat, TimeZone as _, Utc};
    use jiff::{Span, Timestamp, tz::TimeZone as JiffTimeZone};
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    #[test]
    fn exact_unix_millisecond_and_rfc3339_boundaries() {
        let cases = [
            (-1, "1969-12-31T23:59:59.999Z"),
            (0, "1970-01-01T00:00:00.000Z"),
            (1, "1970-01-01T00:00:00.001Z"),
            (1_793_404_800_123, "2026-10-31T00:00:00.123Z"),
        ];
        for (millis, required) in cases {
            let baseline = Utc.timestamp_millis_opt(millis).single().unwrap();
            let old = baseline.to_rfc3339_opts(SecondsFormat::Millis, true);
            assert_eq!(old, required);

            let jiff = Timestamp::from_millisecond(millis).unwrap();
            assert_eq!(jiff.as_millisecond(), millis);
            let jiff_wire = format!("{jiff:.3}");
            println!("RFC3339 chrono={old} jiff={jiff_wire}");
            assert_eq!(jiff_wire, required, "jiff fixed-millisecond wire parity");

            let time = OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
                .unwrap();
            assert_eq!(time.unix_timestamp_nanos() / 1_000_000, i128::from(millis));
            let time_wire = time.format(&Rfc3339).unwrap();
            println!("RFC3339 time={time_wire}; zero-millisecond precision parity={}", time_wire == required);
        }
    }

    #[test]
    fn rfc3339_offset_parser_preserves_absolute_time() {
        for text in [
            "2026-10-30T12:00:00Z",
            "2026-10-30T14:00:00+02:00",
            "2026-10-30T07:00:00-05:00",
            "2026-10-30T12:00:00.123456789Z",
        ] {
            let old = DateTime::parse_from_rfc3339(text).unwrap().timestamp_millis();
            let new: Timestamp = text.parse().unwrap();
            let t = OffsetDateTime::parse(text, &Rfc3339).unwrap();
            assert_eq!(new.as_millisecond(), old, "jiff {text}");
            assert_eq!(t.unix_timestamp_nanos() / 1_000_000, i128::from(old), "time {text}");
        }
        for invalid in ["not-a-date", "2026-02-30T12:00:00Z", "2026-13-01T12:00:00Z"] {
            assert!(DateTime::parse_from_rfc3339(invalid).is_err());
            assert!(invalid.parse::<Timestamp>().is_err());
            assert!(OffsetDateTime::parse(invalid, &Rfc3339).is_err());
        }
    }

    #[test]
    fn three_calendar_months_match_history_archive_cutoff() {
        for date in [
            "2025-05-31T12:30:40Z",
            "2024-05-31T12:30:40Z",
            "2026-01-31T12:30:40Z",
            "2024-03-01T00:00:00Z",
        ] {
            let original = DateTime::parse_from_rfc3339(date).unwrap();
            let expected = original.checked_sub_months(Months::new(3)).unwrap().timestamp_millis();
            let timestamp: Timestamp = date.parse().unwrap();
            let actual = timestamp
                .to_zoned(JiffTimeZone::UTC)
                .checked_sub(Span::new().months(3))
                .unwrap()
                .timestamp()
                .as_millisecond();
            println!("calendar {date} chrono={expected} jiff={actual}");
            assert_eq!(actual, expected, "archive 3 calendar months: {date}");

            let fixed_days = OffsetDateTime::parse(date, &Rfc3339).unwrap()
                - time::Duration::days(90);
            println!("time 90-day shortcut differs from months={}",
                fixed_days.unix_timestamp_nanos() / 1_000_000 != i128::from(expected));
        }
    }

    #[test]
    fn windows_local_scheduler_boundary_is_available() {
        let local = jiff::Zoned::now()
            .checked_add(Span::new().seconds(10))
            .unwrap()
            .strftime("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(local.len(), 19);
        println!("jiff local task boundary={local}");
        #[cfg(windows)] {
            let baseline = (chrono::Local::now() + chrono::Duration::seconds(10))
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            println!("chrono local task boundary={baseline}");
            assert_eq!(baseline.len(), 19);
            println!("time local availability={:?}", OffsetDateTime::now_local());
        }
    }

    #[test]
    fn invalid_epoch_does_not_silently_wrap() {
        assert!(Timestamp::from_millisecond(i64::MAX).is_err());
        assert!(Utc.timestamp_millis_opt(i64::MAX).single().is_none());
        assert!(OffsetDateTime::from_unix_timestamp_nanos(i128::from(i64::MAX) * 1_000_000).is_err());
    }
}
