//! Account-local, bounded observations. Financial facts remain in AccountStore;
 //! these volatile measurements never authorize new risk or survive restart.
use std::collections::{BTreeMap, VecDeque};

const WINDOW_MS: i64 = 30 * 60 * 1000;
const CAPACITY: usize = 2048;
const MAX_TICK_GAP_MS: i64 = 5000;
const MAX_SOURCE_AGE_MS: i64 = 5000;

#[derive(Clone, Debug)]
pub struct FinanceAttempt {
    pub at_ms: i64,
    pub source_generation: u64,
    pub elapsed_ms: u64,
    pub nav_sql_owner_ms: Option<u64>,
    pub nav_sql_queue_ms: Option<u64>,
    pub snapshot_age_ms: Option<i64>,
    pub failure_code: Option<String>,
}

#[derive(Clone, Default, Debug)]
pub struct FinanceWindow {
    samples: VecDeque<FinanceAttempt>,
}

#[derive(serde::Serialize, Debug)]
pub struct LatencySummary {
    pub count: usize,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub max_ms: Option<u64>,
}

#[derive(serde::Serialize, Debug)]
pub struct FinanceWindowSummary {
    pub schema: &'static str,
    pub start_ms: i64,
    pub end_ms: i64,
    /// An empty/new or restarted process never inherits prior acceptance.
    pub state: &'static str,
    pub window_covered_by_samples: bool,
    pub samples: usize,
    pub successes: usize,
    pub failures: usize,
    pub source_age_violations: usize,
    pub tick_gaps: usize,
    pub source_generation_changes: usize,
    pub failure_reasons: BTreeMap<String, usize>,
    /// Includes *failed* capture attempts, not only successful cycles.
    pub capture_attempt_latency: LatencySummary,
    /// NAV timings exist only for completed NAV captures; failure counts are separate.
    pub successful_nav_sql_owner_latency: LatencySummary,
    pub successful_nav_sql_queue_latency: LatencySummary,
    /// At-tick evidence is not a continuous WSS packet-level attestation.
    pub wss_evidence_scope: &'static str,
    /// The backup outcome is independently proved by Windows CONTROL.
    pub backup_evidence: &'static str,
}

fn latency_summary(mut values: Vec<u64>) -> LatencySummary {
    values.sort_unstable();
    let n = values.len();
    let nearest_rank = |percentile: usize| {
        if n == 0 {
            None
        } else {
            let index = (n * percentile).div_ceil(100) - 1;
            values.get(index).copied()
        }
    };
    LatencySummary {
        count: n,
        p50_ms: nearest_rank(50),
        p95_ms: nearest_rank(95),
        max_ms: values.last().copied(),
    }
}

impl FinanceWindow {
    pub fn record(&mut self, sample: FinanceAttempt) {
        // Time regressions cannot produce a falsely complete window.
        if self.samples.back().is_some_and(|last| sample.at_ms < last.at_ms) {
            self.samples.clear();
        }
        self.samples.push_back(sample);
        if self.samples.len() > CAPACITY {
            self.samples.pop_front();
        }
    }

    pub fn summary(&self, now_ms: i64) -> FinanceWindowSummary {
        let start_ms = now_ms.saturating_sub(WINDOW_MS);
        let anchor = self.samples.iter().rev().find(|s| s.at_ms <= start_ms);
        let selected: Vec<_> = self.samples.iter()
            .filter(|s| s.at_ms > start_ms && s.at_ms <= now_ms)
            .collect();
        let mut previous = anchor;
        let mut gaps = 0;
        let mut generation_changes = 0;
        for sample in &selected {
            if let Some(prev) = previous {
                if sample.at_ms - prev.at_ms > MAX_TICK_GAP_MS {
                    gaps += 1;
                }
                if sample.source_generation != prev.source_generation {
                    generation_changes += 1;
                }
            }
            previous = Some(sample);
        }
        let last_is_fresh = previous.is_some_and(|last| {
            last.at_ms <= now_ms && now_ms - last.at_ms <= MAX_TICK_GAP_MS
        });
        let window_covered_by_samples = anchor.is_some() && last_is_fresh && !selected.is_empty();
        let mut failures = 0;
        let mut stale = 0;
        let mut failure_reasons = BTreeMap::new();
        let mut attempt_durations = Vec::with_capacity(selected.len());
        let mut nav_owner = Vec::new();
        let mut nav_queue = Vec::new();
        for sample in &selected {
            attempt_durations.push(sample.elapsed_ms);
            if let Some(code) = &sample.failure_code {
                failures += 1;
                *failure_reasons.entry(code.clone()).or_insert(0) += 1;
            } else {
                if !sample.snapshot_age_ms.is_some_and(|age| (0..=MAX_SOURCE_AGE_MS).contains(&age)) {
                    stale += 1;
                }
                if let Some(ms) = sample.nav_sql_owner_ms {
                    nav_owner.push(ms);
                }
                if let Some(ms) = sample.nav_sql_queue_ms {
                    nav_queue.push(ms);
                }
            }
        }
        let state = if !window_covered_by_samples {
            "NOT_PROVEN"
        } else if failures > 0 || stale > 0 || gaps > 0 || generation_changes > 0 {
            "FAILED"
        } else {
            // Does not assert backup success or all-packet WSS continuity.
            "OBSERVED_CLEAN"
        };
        FinanceWindowSummary {
            schema: "okx.finance-window/v1",
            start_ms,
            end_ms: now_ms,
            state,
            window_covered_by_samples,
            samples: selected.len(),
            successes: selected.len() - failures,
            failures,
            source_age_violations: stale,
            tick_gaps: gaps,
            source_generation_changes: generation_changes,
            failure_reasons,
            capture_attempt_latency: latency_summary(attempt_durations),
            successful_nav_sql_owner_latency: latency_summary(nav_owner),
            successful_nav_sql_queue_latency: latency_summary(nav_queue),
            wss_evidence_scope: "SAMPLED_AT_CAPTURE_TICKS",
            backup_evidence: "REQUIRES_WINDOWS_CONTROL_RECEIPT",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ok(at_ms: i64, elapsed_ms: u64) -> FinanceAttempt {
        FinanceAttempt {
            at_ms,
            source_generation: 4,
            elapsed_ms,
            nav_sql_owner_ms: Some(elapsed_ms / 2),
            nav_sql_queue_ms: Some(0),
            snapshot_age_ms: Some(900),
            failure_code: None,
        }
    }
    fn complete() -> FinanceWindow {
        let mut w = FinanceWindow::default();
        for t in (0..=WINDOW_MS).step_by(2000) {
            w.record(ok(t, 100));
        }
        w
    }

    #[test]
    fn exact_window_reports_nearest_rank_and_includes_healthy_samples() {
        let mut w = complete();
        w.record(ok(WINDOW_MS + 2000, 1000));
        let s = w.summary(WINDOW_MS + 2000);
        assert_eq!(s.state, "OBSERVED_CLEAN");
        assert_eq!(s.capture_attempt_latency.p95_ms, Some(100));
        assert_eq!(s.capture_attempt_latency.max_ms, Some(1000));
        assert_eq!(s.successful_nav_sql_owner_latency.max_ms, Some(500));
        assert_eq!(s.failures, 0);
        assert_eq!(s.backup_evidence, "REQUIRES_WINDOWS_CONTROL_RECEIPT");
    }

    #[test]
    fn error_stale_gap_and_wss_change_cannot_become_pass_after_recovery() {
        let mut w = FinanceWindow::default();
        for t in (0..=WINDOW_MS).step_by(2000) {
            let mut sample = ok(t, 100);
            if t == WINDOW_MS - 2000 {
                sample.elapsed_ms = 3000;
                sample.failure_code = Some("ACCOUNT_QUEUE_DEADLINE".into());
            }
            if t == WINDOW_MS {
                sample.snapshot_age_ms = Some(5001);
                sample.source_generation = 5;
            }
            w.record(sample);
        }
        assert_eq!(w.summary(WINDOW_MS).state, "FAILED");
        let s = w.summary(WINDOW_MS);
        assert_eq!(s.failures, 1);
        assert_eq!(s.source_age_violations, 1);
        assert!(s.source_generation_changes > 0);
        assert_eq!(s.capture_attempt_latency.max_ms, Some(3000));
        assert_eq!(s.failure_reasons["ACCOUNT_QUEUE_DEADLINE"], 1);

        let mut g = FinanceWindow::default();
        g.record(ok(0, 100));
        g.record(ok(WINDOW_MS - 10_000, 100));
        g.record(ok(WINDOW_MS, 100));
        assert_eq!(g.summary(WINDOW_MS).state, "FAILED");
        assert!(g.summary(WINDOW_MS).tick_gaps > 0);
    }

    #[test]
    fn short_or_restarted_window_and_clock_regression_never_prove_acceptance() {
        let mut w = FinanceWindow::default();
        w.record(ok(10_000, 100));
        assert_eq!(w.summary(WINDOW_MS).state, "NOT_PROVEN");
        assert_eq!(FinanceWindow::default().summary(WINDOW_MS).state, "NOT_PROVEN");
        w.record(ok(5_000, 100)); // clock rollback discards old observations
        assert_eq!(w.summary(WINDOW_MS).state, "NOT_PROVEN");
        assert_eq!(w.summary(WINDOW_MS + 10_000).state, "NOT_PROVEN");
    }

    #[test]
    fn bounded_memory_and_missing_latest_tick_are_not_proven() {
        let mut w = FinanceWindow::default();
        for n in 0..4000 {
            w.record(ok(n * 2000, 100));
        }
        assert_eq!(w.samples.len(), CAPACITY);
        assert_eq!(w.summary(9_000_000).state, "NOT_PROVEN");
    }

    #[test]
    fn percentile_uses_nearest_rank_not_interpolation_or_success_only_capture() {
        assert_eq!(latency_summary(vec![1, 2, 3, 4, 100]).p95_ms, Some(100));
        assert_eq!(latency_summary(vec![]).p95_ms, None);
        assert_eq!(latency_summary(vec![5]).p50_ms, Some(5));
    }
}
