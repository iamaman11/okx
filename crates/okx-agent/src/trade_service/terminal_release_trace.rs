//! Bounded, volatile observations of the existing terminal-release owner.
//! Nothing in this projection grants risk, reconstructs an ACK, or reads SQL.
use super::terminal_release::TerminalReleaseStage;
use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{
    RwLock,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

/// Facts already obtained by the release owner. No reader refreshes them.
#[derive(Clone, Default, Serialize)]
pub(super) struct ReleaseFacts {
    pub fills: Option<Frontier>,
    pub bills: Option<Frontier>,
    pub terminal_order_updated_at_ms: Option<i64>,
    pub remaining_exposure: Option<bool>,
    pub worker_policy_revision: Option<u64>,
    pub worker_sequence: Option<u64>,
    pub worker_released: Option<bool>,
    pub durable_receipt_state: Option<String>,
    pub durable_intent_state: Option<&'static str>,
    pub durable_policy_revision: Option<i64>,
    pub durable_release_committed_at_ms: Option<i64>,
}

#[derive(Clone, Serialize)]
pub(super) struct Frontier {
    watermark_ms: i64,
    generation: i64,
    has_next_page: bool,
    has_coverage_gaps: bool,
}
impl From<&okx_execution::SourceCursor> for Frontier {
    fn from(source: &okx_execution::SourceCursor) -> Self {
        Self {
            watermark_ms: source.watermark,
            generation: source.generation,
            has_next_page: !source.cursor.is_empty(),
            has_coverage_gaps: !source.coverage_gaps.is_empty(),
        }
    }
}

#[derive(Clone, Serialize)]
struct Invocation {
    request_id: String,
    origin: String,
}

#[derive(Clone, Serialize)]
struct Blocker {
    stage: TerminalReleaseStage,
    reason_code: String,
    attempt: u64,
    observed_at_ms: i64,
    invocation: Option<Invocation>,
    facts: ReleaseFacts,
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
    invocation: Option<Invocation>,
    facts: ReleaseFacts,
}

#[derive(Default)]
pub(crate) struct TerminalReleaseTrace {
    rows: RwLock<BTreeMap<String, Row>>,
    next_attempt: AtomicU64,
    dropped: AtomicU64,
    evicted: AtomicU64,
    superseded: AtomicU64,
    restoration: AtomicU8,
}

impl TerminalReleaseTrace {
    const CAPACITY: usize = 128;

    pub(crate) fn reference(intent: &str) -> String {
        format!("terminal_release:{intent}")
    }

    pub(crate) fn mark_restore_pending(&self) {
        self.restoration.store(1, Ordering::Release);
    }

    pub(crate) fn mark_restore_failed(&self) {
        self.restoration.store(3, Ordering::Release);
    }

    /// Seed a bounded SQL subset without inventing prior sends or callers.
    /// A live attempt always wins over a delayed startup result.
    pub(crate) fn restore_durable(
        &self,
        receipts: Vec<okx_execution::TerminalReleaseDiagnosticReceipt>,
    ) {
        let Ok(mut rows) = self.rows.try_write() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.mark_restore_failed();
            return;
        };
        for receipt in receipts.into_iter().take(Self::CAPACITY) {
            if receipt.intent_id.len() > 256 || receipt.reservation_key.len() > 256 {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if rows.contains_key(&receipt.intent_id) || rows.len() >= Self::CAPACITY {
                continue;
            }
            rows.insert(
                receipt.intent_id.clone(),
                Row {
                    intent_id: receipt.intent_id,
                    intent_revision: receipt.intent_revision,
                    attempt: 0,
                    stage: TerminalReleaseStage::DurableSnapshot,
                    reservation_key: Some(receipt.reservation_key),
                    outcome: "restored_durable",
                    reason_code: None,
                    first_observed_blocker: None,
                    observed_at_ms: Utc::now().timestamp_millis(),
                    invocation: None,
                    facts: ReleaseFacts {
                        durable_receipt_state: Some(receipt.receipt_state),
                        durable_intent_state: Some(receipt.intent_state.as_str()),
                        durable_policy_revision: Some(receipt.policy_revision),
                        durable_release_committed_at_ms: receipt.central_release_committed_at,
                        ..ReleaseFacts::default()
                    },
                },
            );
        }
        self.restoration.store(2, Ordering::Release);
    }

    pub(super) fn begin(&self) -> u64 {
        self.next_attempt.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
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
        self.observe_facts(
            intent,
            revision,
            attempt,
            stage,
            reservation,
            outcome,
            reason,
            None,
            &ReleaseFacts::default(),
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe_facts(
        &self,
        intent: &str,
        revision: i64,
        attempt: u64,
        stage: TerminalReleaseStage,
        reservation: Option<&str>,
        outcome: &'static str,
        reason: Option<&str>,
        invocation: Option<crate::diagnostics::ProtectionCleanupInvocation<'_>>,
        facts: &ReleaseFacts,
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
                invocation: invocation
                    .filter(|v| v.request_id.len() <= 128 && v.origin.len() <= 96)
                    .map(|v| Invocation {
                        request_id: v.request_id.to_owned(),
                        origin: v.origin.to_owned(),
                    }),
                facts: facts.clone(),
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
                invocation: invocation
                    .filter(|v| v.request_id.len() <= 128 && v.origin.len() <= 96)
                    .map(|v| Invocation {
                        request_id: v.request_id.to_owned(),
                        origin: v.origin.to_owned(),
                    }),
                facts: facts.clone(),
            },
        );
    }

    pub(crate) fn read(&self, intent: &str) -> Value {
        let mut health = json!({
            "scope":"this_account_terminal_release_projection",
            "capacity":Self::CAPACITY,"volatile_since_process_start":true,
            "history_complete":"UNKNOWN","durable_state":"NOT_READ",
            "initiator":"NOT_RECORDED","dropped_event_count":self.dropped.load(Ordering::Relaxed),
            "evicted_operation_count":self.evicted.load(Ordering::Relaxed),
            "superseded_event_count":self.superseded.load(Ordering::Relaxed)
            ,"restoration":match self.restoration.load(Ordering::Acquire) {
                1=>"PENDING",2=>"BOUNDED_SQL_SUBSET",3=>"FAILED",_=>"NOT_STARTED"
            }
        });
        let Ok(rows) = self.rows.try_read() else {
            return json!({"state":"NOT_EVALUATED","reason_code":"TERMINAL_RELEASE_TRACE_BUSY",
                "quality":"partial","trace_health":health,"next_action":"retry_read_only_diagnostic"});
        };
        let Some(row) = rows.get(intent) else {
            return json!({"state":"NOT_OBSERVED","reason_code":"VOLATILE_PROJECTION_UNAVAILABLE",
                "quality":"partial","trace_health":health,"next_action":"trade.status"});
        };
        health["initiator"] = json!(if row.invocation.is_some() {
            "OBSERVED_CALLER"
        } else {
            "NOT_RECORDED"
        });
        if row.outcome == "restored_durable" {
            health["durable_state"] = json!("OBSERVED_AT_STARTUP_NOT_CURRENT_READ");
        }
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
        DurableSnapshot => "current_sources_and_worker_inspection_after_restart",
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
    fn startup_sql_subset_preserves_unknown_network_history_and_never_overwrites_live_attempt() {
        let trace = TerminalReleaseTrace::default();
        trace.mark_restore_pending();
        let receipt = okx_execution::TerminalReleaseDiagnosticReceipt {
            intent_id: "restored".into(),
            intent_revision: 7,
            intent_state: okx_execution::IntentState::Cancelled,
            reservation_key: "reservation".into(),
            policy_revision: 1,
            receipt_state: "RELEASED".into(),
            central_release_committed_at: Some(100),
        };
        trace.restore_durable(vec![receipt.clone()]);
        let value = trace.read("restored");
        assert_eq!(value["snapshot"]["stage"], "durable_snapshot");
        assert_eq!(
            value["snapshot"]["facts"]["durable_intent_state"],
            "CANCELLED"
        );
        assert_eq!(value["snapshot"]["facts"]["durable_policy_revision"], 1);
        assert_eq!(
            value["snapshot"]["facts"]["durable_receipt_state"],
            "RELEASED"
        );
        assert_eq!(
            value["snapshot"]["facts"]["durable_release_committed_at_ms"],
            100
        );
        assert!(value["snapshot"]["facts"]["worker_released"].is_null());
        assert!(value["snapshot"]["invocation"].is_null());
        assert_eq!(value["trace_health"]["history_complete"], "UNKNOWN");
        assert_eq!(value["trace_health"]["restoration"], "BOUNDED_SQL_SUBSET");
        trace.observe(
            "restored",
            7,
            trace.begin(),
            TerminalReleaseStage::FillCoverage,
            Some("reservation"),
            "blocked",
            Some("FILL_COVERAGE_INCOMPLETE"),
        );
        trace.restore_durable(vec![receipt]);
        assert_eq!(trace.read("restored")["snapshot"]["stage"], "fill_coverage");
    }

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
