//! Volatile, non-authoritative trace for the protection-cleanup operation.
use chrono::Utc;
use okx_execution::ProtectionCleanupOperationReceipt;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};

const CAPACITY: usize = ProtectionOperationTrace::CAPACITY;

#[derive(Clone, serde::Serialize)]
struct Row {
    operation_id: String,
    intent_id: Option<String>,
    client_order_id: Option<String>,
    parent_invocation: Option<String>,
    parent_origin: Option<String>,
    first_observed_invocation: Option<String>,
    first_observed_origin: Option<String>,
    last_observed_invocation: Option<String>,
    last_observed_origin: Option<String>,
    stage: &'static str,
    decision: &'static str,
    durable_state: &'static str,
    adapter_attempt: &'static str,
    venue_ack: &'static str,
    inventory_received_at_ms: Option<u64>,
    position_received_at_ms: Option<i64>,
    reason_code: Option<String>,
    first_unproven_boundary: Option<&'static str>,
    first_observed_blocker: Option<ObservedBlocker>,
    missing_evidence: Option<&'static str>,
    next_action: &'static str,
    recorded_at_ms: i64,
}

#[derive(Clone, serde::Serialize)]
struct ObservedBlocker {
    reason_code: String,
    boundary: &'static str,
    missing_evidence: Option<&'static str>,
    observed_at_ms: i64,
}

pub(crate) enum ProtectionTraceEvent {
    Blocked,
    NoProtection,
    NoLiveStop,
    Inspect,
    Begin,
    AdapterInvoked,
    Ack,
    Sources {
        inventory_at_ms: u64,
        position_at_ms: i64,
    },
    Unproven(String),
    Verified,
}

/// Caller identity for the Status invocation that reached this cleanup.
/// It is diagnostic context only and never participates in operation identity.
#[derive(Clone, Copy)]
pub(crate) struct ProtectionCleanupInvocation<'a> {
    pub request_id: &'a str,
    pub origin: &'a str,
}

/// Bounded per-account projection. It never permits trading or proves SQL/venue state.
#[derive(Default)]
pub(crate) struct ProtectionOperationTrace {
    recent: std::sync::RwLock<BTreeMap<String, Row>>,
    dropped_events: AtomicU64,
    evicted_operations: AtomicU64,
    restore_state: AtomicU8,
    restore_error: AtomicU8,
    restored_operation_count: AtomicUsize,
    restore_limit_reached: AtomicBool,
    restore_skipped_invalid_count: AtomicUsize,
}

#[derive(serde::Serialize)]
struct TraceHealth {
    scope: &'static str,
    volatile_since_process_start: bool,
    history_complete: &'static str,
    no_observed_gaps_since_process_start: bool,
    dropped_event_count: u64,
    evicted_operation_count: u64,
    incomplete_reasons: TraceGaps,
    restoration: TraceRestoration,
}

#[derive(serde::Serialize)]
struct TraceGaps {
    event_drops: bool,
    operation_eviction: bool,
}

#[derive(serde::Serialize)]
struct TraceRestoration {
    state: &'static str,
    error_code: Option<&'static str>,
    restored_operation_count: usize,
    limit_reached: bool,
    skipped_invalid_receipt_count: usize,
}

impl ProtectionOperationTrace {
    pub(crate) const CAPACITY: usize = 128;

    pub(crate) fn mark_restore_pending(&self) {
        self.restore_state.store(1, Ordering::Release);
        self.restore_error.store(0, Ordering::Relaxed);
    }

    pub(crate) fn mark_restore_failed(&self, reason: &'static str) {
        self.restore_error
            .store(restore_error_code(reason), Ordering::Relaxed);
        self.restore_state.store(3, Ordering::Release);
    }

    pub(crate) fn restore_durable(
        &self,
        receipts: Vec<ProtectionCleanupOperationReceipt>,
    ) -> Result<(), &'static str> {
        let mut rows = self
            .recent
            .try_write()
            .map_err(|_| "PROTECTION_TRACE_RESTORE_BUSY")?;
        let mut limit_reached = receipts.len() >= Self::CAPACITY;
        let mut restored = 0usize;
        let mut skipped_invalid = 0usize;
        for receipt in receipts {
            if !receipt.operation_id.starts_with("cleanup_")
                || receipt.kind != "protection_cleanup"
                || !matches!(receipt.result_ref.as_str(), "POSTING" | "VERIFIED_CANCELED")
            {
                skipped_invalid += 1;
                continue;
            }
            if rows.contains_key(&receipt.operation_id) {
                continue;
            }
            if rows.len() >= Self::CAPACITY {
                limit_reached = true;
                break;
            }
            let (stage, decision, boundary) = match receipt.result_ref.as_str() {
                "POSTING" => (
                    "RESTORED_PRIOR_BEGIN",
                    "INSPECT_ONLY",
                    "POST_BEGIN_READBACK",
                ),
                "VERIFIED_CANCELED" => (
                    "RESTORED_VERIFIED_CANCELED",
                    "NOT_RECONSTRUCTED",
                    "PROCESS_RESTART_HISTORY",
                ),
                _ => unreachable!(),
            };
            rows.insert(
                receipt.operation_id.clone(),
                Row {
                    operation_id: receipt.operation_id,
                    intent_id: receipt.intent_id,
                    client_order_id: receipt.client_order_id,
                    parent_invocation: None,
                    parent_origin: None,
                    first_observed_invocation: None,
                    first_observed_origin: None,
                    last_observed_invocation: None,
                    last_observed_origin: None,
                    stage,
                    decision,
                    durable_state: if receipt.result_ref == "POSTING" {
                        "POSTING"
                    } else {
                        "VERIFIED_CANCELED"
                    },
                    adapter_attempt: "UNKNOWN_PRIOR_CALL",
                    venue_ack: "UNKNOWN_PRIOR_CALL",
                    inventory_received_at_ms: None,
                    position_received_at_ms: None,
                    reason_code: None,
                    first_unproven_boundary: Some(boundary),
                    first_observed_blocker: None,
                    missing_evidence: Some("PROCESS_LOCAL_ACK_AND_SOURCE_HISTORY"),
                    next_action: "trade.status",
                    recorded_at_ms: receipt.committed_at,
                },
            );
            restored += 1;
        }
        self.restored_operation_count
            .store(restored, Ordering::Relaxed);
        self.restore_limit_reached
            .store(limit_reached, Ordering::Relaxed);
        self.restore_skipped_invalid_count
            .store(skipped_invalid, Ordering::Relaxed);
        self.restore_state.store(2, Ordering::Release);
        Ok(())
    }

    pub(crate) fn record(
        &self,
        operation: &str,
        intent: &str,
        client: &str,
        event: ProtectionTraceEvent,
    ) {
        self.record_with_invocation(operation, intent, client, None, event);
    }

    pub(crate) fn record_with_invocation(
        &self,
        operation: &str,
        intent: &str,
        client: &str,
        invocation: Option<ProtectionCleanupInvocation<'_>>,
        event: ProtectionTraceEvent,
    ) {
        // Diagnostics must never block a trading task. A dropped event is visible
        // through the volatile/partial nature of this projection.
        let Ok(mut rows) = self.recent.try_write() else {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !rows.contains_key(operation)
            && rows.len() >= CAPACITY
            && let Some(oldest) = rows
                .iter()
                .min_by_key(|(_, row)| row.recorded_at_ms)
                .map(|(key, _)| key.clone())
        {
            rows.remove(&oldest);
            self.evicted_operations.fetch_add(1, Ordering::Relaxed);
        }
        let entry = rows.entry(operation.to_owned()).or_insert_with(|| Row {
            operation_id: operation.to_owned(),
            intent_id: Some(intent.to_owned()),
            client_order_id: Some(client.to_owned()),
            parent_invocation: None,
            parent_origin: None,
            first_observed_invocation: invocation.map(|caller| caller.request_id.to_owned()),
            first_observed_origin: invocation.map(|caller| caller.origin.to_owned()),
            last_observed_invocation: invocation.map(|caller| caller.request_id.to_owned()),
            last_observed_origin: invocation.map(|caller| caller.origin.to_owned()),
            stage: "NOT_STARTED",
            decision: "NOT_EVALUATED",
            durable_state: "NOT_EVALUATED",
            adapter_attempt: "NOT_ATTEMPTED",
            venue_ack: "NOT_OBSERVED",
            inventory_received_at_ms: None,
            position_received_at_ms: None,
            reason_code: None,
            first_unproven_boundary: Some("ADMISSION"),
            first_observed_blocker: None,
            missing_evidence: Some("SIGNED_TERMINAL_ORDER_AND_FLAT_POSITION"),
            next_action: "trade.status",
            recorded_at_ms: Utc::now().timestamp_millis(),
        });
        if let Some(caller) = invocation {
            if entry.first_observed_invocation.is_none() {
                entry.first_observed_invocation = Some(caller.request_id.to_owned());
                entry.first_observed_origin = Some(caller.origin.to_owned());
            }
            // Each event may be observed by a different Status request.
            entry.last_observed_invocation = Some(caller.request_id.to_owned());
            entry.last_observed_origin = Some(caller.origin.to_owned());
        }
        match event {
            ProtectionTraceEvent::Blocked => {
                entry.stage = "BLOCKED";
                entry.decision = "NO_SEND";
                entry.first_unproven_boundary = Some("ADMISSION");
                entry.missing_evidence = Some("TERMINAL_ORDER_AND_FLAT_POSITION");
                record_first_blocker(
                    entry,
                    "PROTECTION_CLEANUP_BLOCKED",
                    "ADMISSION",
                    Some("TERMINAL_ORDER_AND_FLAT_POSITION"),
                );
            }
            ProtectionTraceEvent::NoProtection => {
                entry.stage = "NO_ATTACHED_PROTECTION";
                entry.decision = "NO_ACTION";
                entry.first_unproven_boundary = None;
                entry.missing_evidence = None;
            }
            ProtectionTraceEvent::NoLiveStop => {
                entry.stage = "SIGNED_STOP_NOT_LIVE";
                entry.decision = "NO_POST";
                entry.first_unproven_boundary = None;
                entry.missing_evidence = None;
            }
            ProtectionTraceEvent::Inspect => {
                entry.stage = "INSPECT_PRIOR_BEGIN";
                entry.decision = "INSPECT_ONLY";
                entry.durable_state = "POSTING";
                if entry.adapter_attempt != "INVOKED_OUTBOUND_UNPROVEN" {
                    entry.adapter_attempt = "UNKNOWN_PRIOR_CALL";
                }
                if entry.venue_ack != "OBSERVED" {
                    entry.venue_ack = "UNKNOWN_PRIOR_CALL";
                }
                entry.first_unproven_boundary = Some("POST_BEGIN_READBACK");
                entry.missing_evidence = Some("SIGNED_EMPTY_ALGO_AND_FLAT_POSITION");
            }
            ProtectionTraceEvent::Begin => {
                // BEGIN is emitted only after the SQL owner confirms a new
                // durable send permit, so this caller is the operation parent.
                if entry.parent_invocation.is_none()
                    && let Some(caller) = invocation
                {
                    entry.parent_invocation = Some(caller.request_id.to_owned());
                    entry.parent_origin = Some(caller.origin.to_owned());
                }
                entry.stage = "SQL_BEGIN";
                entry.decision = "SEND_ONCE";
                entry.durable_state = "POSTING";
                entry.first_unproven_boundary = Some("VENUE_EFFECT");
                entry.missing_evidence = Some("VENUE_ACK_OR_SIGNED_READBACK");
            }
            ProtectionTraceEvent::AdapterInvoked => {
                entry.stage = "ADAPTER_INVOKED";
                entry.adapter_attempt = "INVOKED_OUTBOUND_UNPROVEN";
                entry.first_unproven_boundary = Some("VENUE_EFFECT");
            }
            ProtectionTraceEvent::Ack => {
                entry.stage = "ACK_OBSERVED";
                entry.venue_ack = "OBSERVED";
                entry.first_unproven_boundary = Some("POST_ACK_READBACK");
                entry.missing_evidence = Some("SIGNED_EMPTY_ALGO_AND_FLAT_POSITION");
            }
            ProtectionTraceEvent::Sources {
                inventory_at_ms,
                position_at_ms,
            } => {
                entry.stage = "SIGNED_SOURCES_OBSERVED";
                entry.inventory_received_at_ms = Some(inventory_at_ms);
                entry.position_received_at_ms = Some(position_at_ms);
            }
            ProtectionTraceEvent::Unproven(code) => {
                entry.stage = "OUTCOME_OR_READBACK_UNPROVEN";
                let code = code.split(':').next().unwrap_or("");
                entry.reason_code = Some(
                    if !code.is_empty()
                        && code.len() <= 96
                        && code
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                    {
                        code.to_owned()
                    } else {
                        "PROTECTION_CLEANUP_UNPROVEN".into()
                    },
                );
                if entry.venue_ack == "OBSERVED" {
                    entry.first_unproven_boundary = Some("POST_ACK_READBACK");
                    entry.missing_evidence = Some("SIGNED_EMPTY_ALGO_AND_FLAT_POSITION");
                } else {
                    entry.first_unproven_boundary = Some("VENUE_OUTCOME_OR_SIGNED_READBACK");
                    entry.missing_evidence = Some("COMPLETE_FRESH_POST_BEGIN_VENUE_PROOF");
                }
                let reason = entry
                    .reason_code
                    .as_deref()
                    .unwrap_or("PROTECTION_CLEANUP_UNPROVEN")
                    .to_owned();
                let boundary = entry.first_unproven_boundary.unwrap_or("UNKNOWN_BOUNDARY");
                let missing = entry.missing_evidence;
                record_first_blocker(entry, &reason, boundary, missing);
            }
            ProtectionTraceEvent::Verified => {
                entry.stage = "VERIFIED_CANCELED";
                entry.decision = "CONFIRMED";
                entry.durable_state = "VERIFIED_CANCELED";
                entry.reason_code = None;
                entry.first_unproven_boundary = None;
                entry.missing_evidence = None;
                entry.next_action = "inspect_separate_risk_release";
            }
        }
        entry.recorded_at_ms = Utc::now().timestamp_millis();
    }

    pub(crate) fn read(&self, reference: &str) -> Result<Value, &'static str> {
        let rows = self
            .recent
            .try_read()
            .map_err(|_| "PROTECTION_TRACE_BUSY")?;
        let health = self.health();
        Ok(rows
            .get(reference)
            .or_else(|| {
                rows.values()
                    .find(|row| {
                        row.intent_id.as_deref() == Some(reference)
                            || row.client_order_id.as_deref() == Some(reference)
                    })
            })
            .map(|entry| {
                json!({
                    "state": "OBSERVED",
                    "snapshot": entry,
                    "current_boundary": entry.first_unproven_boundary,
                    "snapshot_age_ms": Utc::now().timestamp_millis().saturating_sub(entry.recorded_at_ms),
                    "quality": "partial",
                    "trace_health": health,
                    "risk_release": "NOT_EVALUATED",
                    "trading_side_effects": false
                })
            })
            .unwrap_or_else(|| {
                json!({
                    "snapshot": Value::Null,
                    "state": "NOT_OBSERVED",
                    "quality": "partial",
                    "trace_health": health,
                    "trading_side_effects": false
                })
            }))
    }

    fn health(&self) -> TraceHealth {
        let dropped = self.dropped_events.load(Ordering::Relaxed);
        let evicted = self.evicted_operations.load(Ordering::Relaxed);
        TraceHealth {
            scope: "this_account_projection",
            volatile_since_process_start: true,
            // The projection is intentionally in-memory and starts empty on
            // every process boot. Counters can prove only that this process
            // has not observed a write drop/eviction; they cannot establish
            // complete operation history across restart or before startup.
            history_complete: "UNKNOWN",
            no_observed_gaps_since_process_start: dropped == 0 && evicted == 0,
            dropped_event_count: dropped,
            evicted_operation_count: evicted,
            incomplete_reasons: TraceGaps {
                event_drops: dropped > 0,
                operation_eviction: evicted > 0,
            },
            restoration: TraceRestoration {
                state: match self.restore_state.load(Ordering::Acquire) {
                    1 => "PENDING",
                    2 => "COMPLETE",
                    3 => "FAILED",
                    _ => "NOT_STARTED",
                },
                error_code: restore_error_name(self.restore_error.load(Ordering::Relaxed)),
                restored_operation_count: self.restored_operation_count.load(Ordering::Relaxed),
                // Exactly CAPACITY rows may still be the entire account history;
                // this means only that the bounded query reached its limit.
                limit_reached: self.restore_limit_reached.load(Ordering::Relaxed),
                skipped_invalid_receipt_count: self
                    .restore_skipped_invalid_count
                    .load(Ordering::Relaxed),
            },
        }
    }

    pub(crate) fn health_report(&self) -> Value {
        json!(self.health())
    }
}

fn restore_error_code(reason: &'static str) -> u8 {
    match reason {
        "PROTECTION_TRACE_RESTORE_BUSY" => 1,
        "PROTECTION_TRACE_RESTORE_SQL_UNAVAILABLE" => 2,
        "PROTECTION_TRACE_RESTORE_DEADLINE" => 3,
        "DIAGNOSTIC_RESTORE_CANCELLED" => 4,
        "DIAGNOSTIC_RESTORE_STORAGE_UNAVAILABLE" => 5,
        "ACCOUNT_QUEUE_DEADLINE" => 6,
        "ACCOUNT_STORAGE_DEADLINE" => 7,
        "ACCOUNT_STORAGE_BACKPRESSURE" => 8,
        _ => 255,
    }
}

fn restore_error_name(code: u8) -> Option<&'static str> {
    match code {
        1 => Some("PROTECTION_TRACE_RESTORE_BUSY"),
        2 => Some("PROTECTION_TRACE_RESTORE_SQL_UNAVAILABLE"),
        3 => Some("PROTECTION_TRACE_RESTORE_DEADLINE"),
        4 => Some("DIAGNOSTIC_RESTORE_CANCELLED"),
        5 => Some("DIAGNOSTIC_RESTORE_STORAGE_UNAVAILABLE"),
        6 => Some("ACCOUNT_QUEUE_DEADLINE"),
        7 => Some("ACCOUNT_STORAGE_DEADLINE"),
        8 => Some("ACCOUNT_STORAGE_BACKPRESSURE"),
        255 => Some("PROTECTION_TRACE_RESTORE_FAILED"),
        _ => None,
    }
}

fn record_first_blocker(
    row: &mut Row,
    reason: &str,
    boundary: &'static str,
    missing: Option<&'static str>,
) {
    if row.first_observed_blocker.is_none() {
        row.first_observed_blocker = Some(ObservedBlocker {
            reason_code: safe_code(reason, "PROTECTION_CLEANUP_UNPROVEN"),
            boundary,
            missing_evidence: missing,
            observed_at_ms: Utc::now().timestamp_millis(),
        });
    }
}

fn safe_code(message: &str, fallback: &str) -> String {
    let code = message.split(':').next().unwrap_or("");
    if !code.is_empty()
        && code.len() <= 96
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        code.to_owned()
    } else {
        fallback.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn durable_receipt(
        operation_id: &str,
        intent_id: Option<&str>,
        client_order_id: Option<&str>,
        result_ref: &str,
        committed_at: i64,
    ) -> ProtectionCleanupOperationReceipt {
        ProtectionCleanupOperationReceipt {
            operation_id: operation_id.into(),
            intent_id: intent_id.map(str::to_owned),
            client_order_id: client_order_id.map(str::to_owned),
            kind: "protection_cleanup".into(),
            request_hash: "opaque_hash_not_used_for_identity".into(),
            result_ref: result_ref.into(),
            committed_at,
        }
    }

    #[test]
    fn durable_restore_keeps_ack_and_parent_unknown_after_restart() {
        let trace = ProtectionOperationTrace::default();
        trace.mark_restore_pending();
        trace
            .restore_durable(vec![
                durable_receipt(
                    "cleanup_client-posting",
                    Some("intent-posting"),
                    Some("client-posting"),
                    "POSTING",
                    10,
                ),
                durable_receipt("cleanup_orphan", None, None, "VERIFIED_CANCELED", 20),
                durable_receipt(
                    "malformed_operation_identity",
                    Some("intent-bad"),
                    Some("client-bad"),
                    "OTHER_STATE",
                    30,
                ),
            ])
            .unwrap();

        let posting = trace.read("intent-posting").unwrap();
        assert_eq!(posting["snapshot"]["durable_state"], "POSTING");
        assert_eq!(posting["snapshot"]["adapter_attempt"], "UNKNOWN_PRIOR_CALL");
        assert_eq!(posting["snapshot"]["venue_ack"], "UNKNOWN_PRIOR_CALL");
        assert!(posting["snapshot"]["parent_invocation"].is_null());
        assert_eq!(
            posting["snapshot"]["first_unproven_boundary"],
            "POST_BEGIN_READBACK"
        );

        let orphan = trace.read("cleanup_orphan").unwrap();
        assert_eq!(orphan["snapshot"]["durable_state"], "VERIFIED_CANCELED");
        assert!(orphan["snapshot"]["intent_id"].is_null());
        assert!(orphan["snapshot"]["client_order_id"].is_null());
        assert_eq!(orphan["snapshot"]["venue_ack"], "UNKNOWN_PRIOR_CALL");
        assert!(orphan["snapshot"]["parent_invocation"].is_null());
        assert_eq!(orphan["trace_health"]["history_complete"], "UNKNOWN");
        assert_eq!(orphan["trace_health"]["restoration"]["state"], "COMPLETE");
        assert_eq!(
            orphan["trace_health"]["restoration"]["restored_operation_count"],
            2
        );
        assert_eq!(
            orphan["trace_health"]["restoration"]["skipped_invalid_receipt_count"],
            1
        );
    }

    #[test]
    fn delayed_durable_restore_never_overwrites_live_ack_or_verified_state() {
        let trace = ProtectionOperationTrace::default();
        let invocation = ProtectionCleanupInvocation {
            request_id: "req_live_0123456789",
            origin: "agent_request.trade.status",
        };
        trace.record_with_invocation(
            "cleanup_client",
            "intent",
            "client",
            Some(invocation),
            ProtectionTraceEvent::Begin,
        );
        trace.record_with_invocation(
            "cleanup_client",
            "intent",
            "client",
            Some(invocation),
            ProtectionTraceEvent::Ack,
        );
        trace.record_with_invocation(
            "cleanup_client",
            "intent",
            "client",
            Some(invocation),
            ProtectionTraceEvent::Verified,
        );
        trace
            .restore_durable(vec![durable_receipt(
                "cleanup_client",
                Some("intent"),
                Some("client"),
                "POSTING",
                1,
            )])
            .unwrap();
        let live = trace.read("cleanup_client").unwrap();
        assert_eq!(live["snapshot"]["durable_state"], "VERIFIED_CANCELED");
        assert_eq!(live["snapshot"]["venue_ack"], "OBSERVED");
        assert_eq!(live["snapshot"]["parent_invocation"], "req_live_0123456789");
        assert_eq!(live["trace_health"]["evicted_operation_count"], 0);
    }

    #[test]
    fn restore_at_projection_capacity_does_not_evict_live_rows() {
        let trace = ProtectionOperationTrace::default();
        for index in 0..ProtectionOperationTrace::CAPACITY {
            let operation = format!("live-{index}");
            trace.record(
                &operation,
                &format!("intent-{index}"),
                &format!("client-{index}"),
                ProtectionTraceEvent::Begin,
            );
        }
        trace
            .restore_durable(vec![durable_receipt(
                "cleanup_seed",
                Some("seed-intent"),
                Some("seed-client"),
                "POSTING",
                1,
            )])
            .unwrap();
        assert_eq!(trace.read("live-0").unwrap()["state"], "OBSERVED");
        assert_eq!(trace.read("cleanup_seed").unwrap()["state"], "NOT_OBSERVED");
        let health = trace.read("live-0").unwrap()["trace_health"].clone();
        assert_eq!(health["evicted_operation_count"], 0);
        assert_eq!(health["restoration"]["limit_reached"], true);
    }

    #[test]
    fn restore_failures_preserve_known_shutdown_and_queue_codes() {
        let trace = ProtectionOperationTrace::default();
        trace.mark_restore_pending();
        trace.mark_restore_failed("DIAGNOSTIC_RESTORE_CANCELLED");
        let health = trace.read("unknown").unwrap()["trace_health"].clone();
        assert_eq!(health["restoration"]["state"], "FAILED");
        assert_eq!(
            health["restoration"]["error_code"],
            "DIAGNOSTIC_RESTORE_CANCELLED"
        );

        trace.mark_restore_failed("ACCOUNT_QUEUE_DEADLINE");
        let health = trace.read("unknown").unwrap()["trace_health"].clone();
        assert_eq!(
            health["restoration"]["error_code"],
            "ACCOUNT_QUEUE_DEADLINE"
        );
        trace.mark_restore_failed("DIAGNOSTIC_RESTORE_STORAGE_UNAVAILABLE");
        let health = trace.read("unknown").unwrap()["trace_health"].clone();
        assert_eq!(
            health["restoration"]["error_code"],
            "DIAGNOSTIC_RESTORE_STORAGE_UNAVAILABLE"
        );
    }

    #[test]
    fn contention_is_counted_and_readback_marks_a_gap() {
        let trace = ProtectionOperationTrace::default();
        trace.record("op", "intent", "client", ProtectionTraceEvent::Begin);
        {
            let _held = trace.recent.write().unwrap();
            trace.record("op", "intent", "client", ProtectionTraceEvent::Ack);
        }
        let report = trace.read("op").unwrap();
        assert_eq!(report["trace_health"]["scope"], "this_account_projection");
        assert_eq!(report["trace_health"]["history_complete"], "UNKNOWN");
        assert_eq!(
            report["trace_health"]["no_observed_gaps_since_process_start"],
            false
        );
        assert_eq!(report["trace_health"]["dropped_event_count"], 1);
        assert_eq!(
            report["trace_health"]["incomplete_reasons"]["event_drops"],
            true
        );
        assert_eq!(report["snapshot"]["venue_ack"], "NOT_OBSERVED");
    }

    #[test]
    fn a_fresh_process_projection_never_claims_complete_history() {
        let trace = ProtectionOperationTrace::default();
        let report = trace.read("never-observed").unwrap();
        assert_eq!(report["state"], "NOT_OBSERVED");
        assert_eq!(report["trace_health"]["volatile_since_process_start"], true);
        assert_eq!(report["trace_health"]["history_complete"], "UNKNOWN");
        assert_eq!(
            report["trace_health"]["no_observed_gaps_since_process_start"],
            true
        );
        assert!(report["trace_health"].get("trace_complete").is_none());
    }

    #[test]
    fn eviction_is_counted_and_readback_marks_projection_incomplete() {
        let trace = ProtectionOperationTrace::default();
        {
            let mut rows = trace.recent.write().unwrap();
            for index in 0..CAPACITY {
                let id = format!("op-{index:03}");
                rows.insert(
                    id.clone(),
                    Row {
                        operation_id: id.clone(),
                        intent_id: Some(format!("intent-{id}")),
                        client_order_id: Some(format!("client-{id}")),
                        parent_invocation: None,
                        parent_origin: None,
                        first_observed_invocation: None,
                        first_observed_origin: None,
                        last_observed_invocation: None,
                        last_observed_origin: None,
                        stage: "NOT_STARTED",
                        decision: "NOT_EVALUATED",
                        durable_state: "NOT_EVALUATED",
                        adapter_attempt: "NOT_ATTEMPTED",
                        venue_ack: "NOT_OBSERVED",
                        inventory_received_at_ms: None,
                        position_received_at_ms: None,
                        reason_code: None,
                        first_unproven_boundary: None,
                        first_observed_blocker: None,
                        missing_evidence: None,
                        next_action: "trade.status",
                        recorded_at_ms: index as i64,
                    },
                );
            }
        }
        trace.record(
            "new-op",
            "new-intent",
            "new-client",
            ProtectionTraceEvent::Begin,
        );
        let report = trace.read("new-op").unwrap();
        assert_eq!(report["trace_health"]["history_complete"], "UNKNOWN");
        assert_eq!(
            report["trace_health"]["no_observed_gaps_since_process_start"],
            false
        );
        assert_eq!(report["trace_health"]["evicted_operation_count"], 1);
        assert_eq!(
            report["trace_health"]["incomplete_reasons"]["operation_eviction"],
            true
        );
        let missing = trace.read("never-seen").unwrap();
        assert_eq!(missing["state"], "NOT_OBSERVED");
        assert_eq!(missing["trace_health"]["evicted_operation_count"], 1);
        assert_eq!(missing["trace_health"]["history_complete"], "UNKNOWN");
        assert_eq!(
            missing["trace_health"]["volatile_since_process_start"],
            true
        );
    }

    #[test]
    fn later_events_do_not_overwrite_first_observed_blocker() {
        let trace = ProtectionOperationTrace::default();
        trace.record(
            "op",
            "intent",
            "client",
            ProtectionTraceEvent::Unproven("FIRST_GAP".into()),
        );
        trace.record("op", "intent", "client", ProtectionTraceEvent::Inspect);
        trace.record(
            "op",
            "intent",
            "client",
            ProtectionTraceEvent::Unproven("LATER_GAP".into()),
        );
        let report = trace.read("op").unwrap();
        assert_eq!(
            report["snapshot"]["first_observed_blocker"]["reason_code"],
            "FIRST_GAP"
        );
        assert_eq!(report["snapshot"]["reason_code"], "LATER_GAP");
        assert_eq!(
            report["snapshot"]["first_unproven_boundary"],
            "VENUE_OUTCOME_OR_SIGNED_READBACK"
        );
        assert_eq!(
            report["current_boundary"],
            "VENUE_OUTCOME_OR_SIGNED_READBACK"
        );
    }

    #[test]
    fn caller_correlation_keeps_initiator_and_latest_observer_distinct() {
        let trace = ProtectionOperationTrace::default();
        trace.record_with_invocation(
            "op",
            "intent",
            "client",
            Some(ProtectionCleanupInvocation {
                request_id: "req_first_0123456789",
                origin: "agent_request.trade.status",
            }),
            ProtectionTraceEvent::Begin,
        );
        trace.record_with_invocation(
            "op",
            "intent",
            "client",
            Some(ProtectionCleanupInvocation {
                request_id: "req_later_0123456789",
                origin: "agent_request.trade.status",
            }),
            ProtectionTraceEvent::Inspect,
        );
        let result = trace.read("op").unwrap();
        let snapshot = &result["snapshot"];
        assert_eq!(snapshot["parent_invocation"], "req_first_0123456789");
        assert_eq!(snapshot["parent_origin"], "agent_request.trade.status");
        assert_eq!(
            snapshot["first_observed_invocation"],
            "req_first_0123456789"
        );
        assert_eq!(snapshot["last_observed_invocation"], "req_later_0123456789");
        assert_eq!(
            snapshot["last_observed_origin"],
            "agent_request.trade.status"
        );
    }

    #[test]
    fn denied_observer_is_not_parent_when_later_status_commits_begin() {
        let trace = ProtectionOperationTrace::default();
        let caller = |request_id| ProtectionCleanupInvocation {
            request_id,
            origin: "agent_request.trade.status",
        };
        trace.record_with_invocation(
            "op",
            "intent",
            "client",
            Some(caller("req_denied_0123456789")),
            ProtectionTraceEvent::Blocked,
        );
        trace.record_with_invocation(
            "op",
            "intent",
            "client",
            Some(caller("req_begin_0123456789")),
            ProtectionTraceEvent::Begin,
        );
        trace.record_with_invocation(
            "op",
            "intent",
            "client",
            Some(caller("req_observe_0123456789")),
            ProtectionTraceEvent::Inspect,
        );
        let snapshot = trace.read("op").unwrap()["snapshot"].clone();
        assert_eq!(
            snapshot["first_observed_invocation"],
            "req_denied_0123456789"
        );
        assert_eq!(snapshot["parent_invocation"], "req_begin_0123456789");
        assert_eq!(
            snapshot["last_observed_invocation"],
            "req_observe_0123456789"
        );
    }
}
