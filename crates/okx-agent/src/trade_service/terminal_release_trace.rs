//! Bounded, volatile observations of the existing terminal-release owner.
//! Nothing in this projection grants risk, reconstructs an ACK, or reads SQL.
use super::terminal_release::TerminalReleaseStage;
use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{
    RwLock,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Serialize)]
struct Blocker {
    stage: TerminalReleaseStage,
    reason_code: String,
    attempt: u64,
    observed_at_ms: i64,
}

#[derive(Clone, Serialize)]
struct Row {
    intent_id: String,
    intent_revision: i64,
    attempt: u64,
    stage: TerminalReleaseStage,
    reservation_key: Option<String>,
    outcome: &'static str,
    reason_code: Option<String>,
    first_observed_blocker: Option<Blocker>,
    observed_at_ms: i64,
}

#[derive(Default)]
pub(crate) struct TerminalReleaseTrace {
    rows: RwLock<BTreeMap<String, Row>>,
    next_attempt: AtomicU64,
    dropped: AtomicU64,
    evicted: AtomicU64,
    superseded: AtomicU64,
}

impl TerminalReleaseTrace {
    const CAPACITY: usize = 128;

    pub(crate) fn reference(intent: &str) -> String {
        format!("terminal_release:{intent}")
    }

    pub(super) fn begin(&self) -> u64 {
        self.next_attempt.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe(
        &self,
        intent: &str,
        revision: i64,
        attempt: u64,
        stage: TerminalReleaseStage,
        reservation: Option<&str>,
        outcome: &'static str,
        reason: Option<&str>,
    ) {
        // Identities are internal validated keys; corrupted/oversized rows
        // must not turn this small projection into an unbounded copy.
        if intent.len() > 256 || reservation.is_some_and(|key| key.len() > 256) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(mut rows) = self.rows.try_write() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if rows.get(intent).is_some_and(|row| row.attempt > attempt) {
            self.superseded.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !rows.contains_key(intent) && rows.len() >= Self::CAPACITY {
            let oldest = rows
                .iter()
                .min_by_key(|(_, row)| row.attempt)
                .map(|(key, _)| key.clone());
            if let Some(key) = oldest {
                rows.remove(&key);
                self.evicted.fetch_add(1, Ordering::Relaxed);
            }
        }
        let now = Utc::now().timestamp_millis();
        let reason = reason.map(|s| {
            let code = s.split(':').next().unwrap_or("");
            if !code.is_empty()
                && code.len() <= 96
                && code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            {
                code.to_owned()
            } else {
                "TERMINAL_RELEASE_UNPROVEN".into()
            }
        });
        let previous = rows
            .get(intent)
            .filter(|row| row.intent_revision == revision)
            .and_then(|row| row.first_observed_blocker.clone());
        let first = previous.or_else(|| {
            reason.as_ref().map(|reason| Blocker {
                stage,
                reason_code: reason.clone(),
                attempt,
                observed_at_ms: now,
            })
        });
        rows.insert(
            intent.to_owned(),
            Row {
                intent_id: intent.to_owned(),
                intent_revision: revision,
                attempt,
                stage,
                reservation_key: reservation.map(str::to_owned),
                outcome,
                reason_code: reason,
                first_observed_blocker: first,
                observed_at_ms: now,
            },
        );
    }

    pub(crate) fn read(&self, intent: &str) -> Value {
        let health = json!({
            "scope":"this_account_terminal_release_projection",
            "capacity":Self::CAPACITY,"volatile_since_process_start":true,
            "history_complete":"UNKNOWN","durable_state":"NOT_READ",
            "initiator":"NOT_RECORDED","dropped_event_count":self.dropped.load(Ordering::Relaxed),
            "evicted_operation_count":self.evicted.load(Ordering::Relaxed),
            "superseded_event_count":self.superseded.load(Ordering::Relaxed)
        });
        let Ok(rows) = self.rows.try_read() else {
            return json!({"state":"NOT_EVALUATED","reason_code":"TERMINAL_RELEASE_TRACE_BUSY",
                "quality":"partial","trace_health":health,"next_action":"retry_read_only_diagnostic"});
        };
        let Some(row) = rows.get(intent) else {
            return json!({"state":"NOT_OBSERVED","reason_code":"VOLATILE_PROJECTION_UNAVAILABLE",
                "quality":"partial","trace_health":health,"next_action":"trade.status"});
        };
        json!({"state":"OBSERVED","snapshot":row,"trace_health":health,
            "age_ms":Utc::now().timestamp_millis().saturating_sub(row.observed_at_ms).max(0),
            "quality":"partial","next_action":match row.outcome {
                "running"=>"wait_for_owner_or_inspect_missing_source",
                "blocked"=>"obtain_required_evidence_then_trade.status",
                _=>"read_trade_status_for_authoritative_result"
            },"required_evidence":required_evidence(row.stage),
            "diagnostic_reference":Self::reference(intent)})
    }
}

fn required_evidence(stage: TerminalReleaseStage) -> &'static str {
    use TerminalReleaseStage::*;
    match stage {
        Authority => "current_governance",
        Reservation => "durable_reservation",
        Exposure => "no_remaining_exposure",
        FillCoverage => "complete_fills_covering_terminal_time",
        VenueReadback => "same_identity_terminal_order_readback",
        BillCoverage => "complete_bills_covering_terminal_time",
        Inspection => "same_identity_current_worker_observation",
        LateSettlement => "no_unacknowledged_late_settlement",
        SettlementProof => "durable_terminal_settlement_proof",
        CentralSettlement => "worker_settlement_confirmation",
        Census => "complete_current_cohort_financial_and_venue_evidence",
        Release => "worker_release_confirmation",
        LocalAcknowledgement | SqlAcknowledgement => "durable_sql_acknowledgement",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_projection_preserves_first_blocker_and_rejects_old_attempts() {
        let trace = TerminalReleaseTrace::default();
        let first = trace.begin();
        trace.observe(
            "i",
            1,
            first,
            TerminalReleaseStage::FillCoverage,
            Some("r"),
            "blocked",
            Some("FILL_COVERAGE_INCOMPLETE"),
        );
        let second = trace.begin();
        trace.observe(
            "i",
            1,
            second,
            TerminalReleaseStage::BillCoverage,
            Some("r"),
            "blocked",
            Some("FINANCIAL_COVERAGE_INCOMPLETE"),
        );
        trace.observe(
            "i",
            1,
            first,
            TerminalReleaseStage::SqlAcknowledgement,
            Some("r"),
            "returned_ok",
            None,
        );
        let view = trace.read("i");
        assert_eq!(view["snapshot"]["stage"], "bill_coverage");
        assert_eq!(
            view["snapshot"]["first_observed_blocker"]["stage"],
            "fill_coverage"
        );
        assert_eq!(view["trace_health"]["superseded_event_count"], 1);
        assert_eq!(view["trace_health"]["durable_state"], "NOT_READ");
        for n in 0..TerminalReleaseTrace::CAPACITY {
            trace.observe(
                &format!("other{n}"),
                1,
                trace.begin(),
                TerminalReleaseStage::Authority,
                None,
                "running",
                None,
            );
        }
        assert_eq!(
            trace.rows.read().unwrap().len(),
            TerminalReleaseTrace::CAPACITY
        );
        assert_eq!(trace.read("i")["state"], "NOT_OBSERVED");
        assert_eq!(
            trace.read("i")["trace_health"]["evicted_operation_count"],
            1
        );
        assert_eq!(
            TerminalReleaseTrace::default().read("i")["state"],
            "NOT_OBSERVED"
        );
    }

    #[test]
    fn busy_trace_drops_visibly_and_never_exposes_untrusted_error_text() {
        let trace = TerminalReleaseTrace::default();
        let hold = trace.rows.write().unwrap();
        trace.observe(
            "i",
            1,
            trace.begin(),
            TerminalReleaseStage::Census,
            None,
            "blocked",
            Some("secret bad text"),
        );
        assert_eq!(
            trace.read("i")["reason_code"],
            "TERMINAL_RELEASE_TRACE_BUSY"
        );
        drop(hold);
        assert_eq!(trace.read("i")["trace_health"]["dropped_event_count"], 1);
        trace.observe(
            "i",
            1,
            trace.begin(),
            TerminalReleaseStage::Census,
            None,
            "blocked",
            Some("secret bad text"),
        );
        let value = trace.read("i");
        assert_eq!(
            value["snapshot"]["reason_code"],
            "TERMINAL_RELEASE_UNPROVEN"
        );
        assert!(!value.to_string().contains("secret"));
        let newer = trace.begin();
        trace.observe(
            "i",
            2,
            newer,
            TerminalReleaseStage::Authority,
            None,
            "running",
            None,
        );
        assert!(trace.read("i")["snapshot"]["first_observed_blocker"].is_null());
    }
}
