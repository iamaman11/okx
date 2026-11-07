//! Cumulative entry settlement: one coordinator over existing SQL and Worker owners.
//! Terminal release remains independent in reconcile::release_terminal.
use super::{
    AccountStoreHandle, CentralGovernanceSnapshot, NativeRiskClient, StoredIntent,
    build_risk_command, err, exact,
    reconcile::{terminal_order_update_time, verified_order_readback},
};
use okx_api::TradeOrderDetails;
use okx_protocol::{
    accounts::{AccountKey, Environment},
    risk_authority::{RiskCommandAction, RiskDecisionStatus, RiskReservationState},
};
use rust_decimal::Decimal;
use std::time::Instant;
use tokio::sync::RwLock;

/// These are intentionally internal. All successful variants retain the
/// existing external "verified_or_no_change" representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EntrySettlementOutcome {
    NotApplicable,
    AlreadyConfirmed,
    LateRevisionAcknowledged,
    TerminalReplayConfirmed,
    RevisionAcknowledged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SettlementFailureKind {
    AwaitingEvidence,
    Conflict,
    Failed,
}

/// Preserve the original public reason string while exposing the violated
/// owner boundary to direct tests. Unknown errors are failures, never no-ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EntrySettlementError {
    pub(super) stage: &'static str,
    pub(super) kind: SettlementFailureKind,
    pub(super) reason_code: String,
}
impl EntrySettlementError {
    fn from_reason(reason_code: String) -> Self {
        let reason = reason_code.as_str();
        let stage = match reason {
            "PARTIAL_ENTRY_FILLS_UNCOVERED"
            | "PARTIAL_ENTRY_BILLS_UNCOVERED"
            | "PARTIAL_ENTRY_SOURCE_PAGES_INCOMPLETE"
            | "PARTIAL_ENTRY_FINANCIAL_COVERAGE_PENDING" => "source_coverage",
            "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY"
            | "RISK_TRANSPORT_LOST_DO_NOT_REPLAY"
            | "RISK_TRANSPORT_UNAVAILABLE"
            | "RISK_RESPONSE_IDENTITY_MISMATCH" => "risk_response",
            "PARTIAL_ENTRY_SETTLEMENT_CAS_BUSY"
            | "LATE_ENTRY_SETTLEMENT_CAS_BUSY"
            | "PARTIAL_ENTRY_RISK_SEQUENCE_MISSING"
            | "LATE_ENTRY_RISK_SEQUENCE_MISSING" => "risk_cas",
            "PARTIAL_ENTRY_ORDER_IDENTITY_MISMATCH"
            | "PARTIAL_ENTRY_SOURCE_IDENTITY_MISMATCH"
            | "TERMINAL_ENTRY_REPLAY_IDENTITY_MISMATCH" => "identity",
            "PARTIAL_ENTRY_ORDER_READBACK_UNAVAILABLE"
            | "PARTIAL_ENTRY_ORDER_STATE_UNVERIFIED"
            | "PARTIAL_ENTRY_ZERO_FILL_SOURCE_UNPROVEN" => "venue_verification",
            _ => "settlement",
        };
        let kind = match reason {
            "PARTIAL_ENTRY_FILLS_UNCOVERED"
            | "PARTIAL_ENTRY_BILLS_UNCOVERED"
            | "PARTIAL_ENTRY_SOURCE_PAGES_INCOMPLETE"
            | "PARTIAL_ENTRY_FINANCIAL_COVERAGE_PENDING"
            | "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY"
            | "RISK_TRANSPORT_LOST_DO_NOT_REPLAY"
            | "PARTIAL_ENTRY_SETTLEMENT_CAS_BUSY"
            | "LATE_ENTRY_SETTLEMENT_CAS_BUSY" => SettlementFailureKind::AwaitingEvidence,
            "PARTIAL_ENTRY_ORDER_IDENTITY_MISMATCH"
            | "PARTIAL_ENTRY_SOURCE_IDENTITY_MISMATCH"
            | "TERMINAL_ENTRY_REPLAY_IDENTITY_MISMATCH" => SettlementFailureKind::Conflict,
            _ => SettlementFailureKind::Failed,
        };
        Self {
            stage,
            kind,
            reason_code,
        }
    }
}

/// Explicit dependencies: verified account identity, existing intent/readback,
/// real AccountStore, RiskAuthority transport and current governance.
/// Read governance at the original point, after venue validation. Re-evaluate
/// the SQL deadline on each call as before; never pin it once at entry.
pub(super) async fn settle_incremental_entry(
    account: &AccountKey,
    i: &StoredIntent,
    order: Option<&TradeOrderDetails>,
    store: &AccountStoreHandle,
    risk: &NativeRiskClient,
    governance: &RwLock<Option<CentralGovernanceSnapshot>>,
    sql_deadline: fn() -> Instant,
) -> Result<EntrySettlementOutcome, EntrySettlementError> {
    settle_inner(account, i, order, store, risk, governance, sql_deadline)
        .await
        .map_err(EntrySettlementError::from_reason)
}

async fn settle_inner(
    account: &AccountKey,
    i: &StoredIntent,
    order: Option<&TradeOrderDetails>,
    store: &AccountStoreHandle,
    risk: &NativeRiskClient,
    governance: &RwLock<Option<CentralGovernanceSnapshot>>,
    sql_deadline: fn() -> Instant,
) -> Result<EntrySettlementOutcome, String> {
    if account.environment != okx_protocol::accounts::Environment::Real || !i.risk_increase {
        return Ok(EntrySettlementOutcome::NotApplicable);
    }
    let venue = order.ok_or("PARTIAL_ENTRY_ORDER_READBACK_UNAVAILABLE")?;
    if !matches!(
        venue.state.as_str(),
        "partially_filled" | "filled" | "canceled" | "mmp_canceled"
    ) {
        return Err("PARTIAL_ENTRY_ORDER_STATE_UNVERIFIED".into());
    }
    let filled = verified_order_readback(i, venue)?;
    if filled <= Decimal::ZERO {
        return Err("PARTIAL_ENTRY_ZERO_FILL_SOURCE_UNPROVEN".into());
    }
    let state = governance
        .read()
        .await
        .clone()
        .ok_or("AUTHORITY_UNAVAILABLE")?;
    let id = i.intent_id.clone();
    let (receipt, fill_cursor, bill_cursor) = store
        .call(sql_deadline(), move |s| {
            Ok((
                s.central_receipt(&id)?,
                s.cursor("fills", "SWAP")?,
                s.cursor("bills", "account")?,
            ))
        })
        .await
        .map_err(err)?;
    let receipt = receipt.ok_or("PARTIAL_ENTRY_RESERVATION_UNVERIFIED")?;
    if receipt.reservation_key.starts_with("local:") || receipt.plan_hash != i.plan_hash {
        return Err("PARTIAL_ENTRY_RESERVATION_INVALID".into());
    }
    if i.exchange_order_id.as_deref() != Some(venue.order_id.as_str()) {
        return Err("PARTIAL_ENTRY_ORDER_IDENTITY_MISMATCH".into());
    }
    let venue_at =
        terminal_order_update_time(Some(venue))?.ok_or("PARTIAL_ENTRY_UPDATE_TIME_MISSING")?;
    // Inspect before any native money mutation. Existing central RELEASE
    // remains terminal even when a late fee requires a new debit.
    let inspection = risk
        .request(build_risk_command(
            &state,
            format!("inspect_partial_pre_{}", i.client_order_id),
            RiskCommandAction::Inspect {
                reservation_key: receipt.reservation_key.clone(),
                plan_sha256: i.plan_hash.clone(),
            },
        ))
        .await
        .map_err(str::to_owned)?;
    let fills = fill_cursor.ok_or("PARTIAL_ENTRY_FILLS_UNCOVERED")?;
    let bills = bill_cursor.ok_or("PARTIAL_ENTRY_BILLS_UNCOVERED")?;
    if !fills.cursor.is_empty()
        || !bills.cursor.is_empty()
        || !fills.coverage_gaps.is_empty()
        || !bills.coverage_gaps.is_empty()
        || fills.generation <= 0
        || bills.generation <= 0
    {
        return Err("PARTIAL_ENTRY_SOURCE_PAGES_INCOMPLETE".into());
    }
    let through = fills.watermark.min(bills.watermark);
    if through < venue_at {
        return Err("PARTIAL_ENTRY_FINANCIAL_COVERAGE_PENDING".into());
    }
    if inspection.reservation_state == Some(RiskReservationState::Released) {
        let id = i.intent_id.clone();
        let late = store
            .call(sql_deadline(), move |s| s.entry_late_pending(&id))
            .await
            .map_err(err)?;
        if !late {
            let existing_id = format!("entry-settlement:{}", i.intent_id);
            let recorded = store
                .call(sql_deadline(), move |s| s.operation(&existing_id))
                .await
                .map_err(err)?;
            return if recorded.is_some_and(|w| w.kind == "ENTRY_SETTLEMENT_PROVEN") {
                Ok(EntrySettlementOutcome::AlreadyConfirmed)
            } else {
                Err("RELEASED_ENTRY_TERMINAL_SOURCE_MISSING".into())
            };
        }
        if !i.state.terminal() {
            return Err("LATE_ENTRY_ORDER_NOT_TERMINAL".into());
        }
        let id = i.intent_id.clone();
        let late_proof = store
            .call(sql_deadline(), move |s| {
                s.bind_late_entry_settlement(&id, venue_at, filled, through)
            })
            .await
            .map_err(err)?;
        if late_proof.schema != "okx.entry-late-settlement/v1"
            || late_proof.reservation_key != receipt.reservation_key
            || late_proof.venue_order_id != venue.order_id
        {
            return Err("LATE_ENTRY_ACCOUNT_PROOF_MISSING".into());
        }
        for _ in 0..3 {
            let checked = risk
                .request(build_risk_command(
                    &state,
                    format!("inspect_late_{}", i.client_order_id),
                    RiskCommandAction::Inspect {
                        reservation_key: receipt.reservation_key.clone(),
                        plan_sha256: i.plan_hash.clone(),
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            if checked.reservation_state != Some(RiskReservationState::Released) {
                return Err("LATE_ENTRY_RESERVATION_CHANGED".into());
            }
            let sequence = checked
                .risk_book_sequence
                .ok_or("LATE_ENTRY_RISK_SEQUENCE_MISSING")?;
            let done = risk
                .request(build_risk_command(
                    &state,
                    format!(
                        "settle_late_{}_r{}",
                        i.client_order_id, late_proof.settlement_revision
                    ),
                    RiskCommandAction::SettleEntry {
                        reservation_key: late_proof.reservation_key.clone(),
                        plan_sha256: late_proof.plan_hash.clone(),
                        expected_risk_book_sequence: sequence,
                        cumulative_entry_usd: exact(late_proof.cumulative_entry_usd),
                        settlement_revision: late_proof.settlement_revision,
                        source_digest: late_proof.source_digest.clone(),
                    },
                ))
                .await
                .map_err(str::to_owned)?;
            if done.status == RiskDecisionStatus::Updated {
                store
                    .call(sql_deadline(), move |s| {
                        s.acknowledge_late_entry_settlement(&late_proof)
                    })
                    .await
                    .map_err(err)?;
                return Ok(EntrySettlementOutcome::LateRevisionAcknowledged);
            }
            if !matches!(
                done.reason.as_deref(),
                Some(
                    "STALE_SESSION_OR_BOOK_SEQUENCE" | "ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN"
                )
            ) {
                return Err(done.reason.unwrap_or("LATE_ENTRY_SETTLEMENT_DENIED".into()));
            }
        }
        return Err("LATE_ENTRY_SETTLEMENT_CAS_BUSY".into());
    }
    // A terminal settlement may already have been bound during an earlier
    // Status which could not RELEASE because another terminal reservation
    // was still pending. Re-verify that same immutable terminal source
    // witness; never attempt a PARTIAL settlement against its receipt
    // (a different schema/revision) or silently accept changed fills/fees.
    // The normal release_terminal path below still owns Worker SETTLE_ENTRY
    // and central RELEASE; this read-only replay grants no new capacity.
    if i.state.terminal() {
        let id = i.intent_id.clone();
        let existing = store
            .call(sql_deadline(), move |s| {
                if s.operation(&format!("entry-settlement:{id}"))?.is_none() {
                    return Ok(None);
                }
                if s.entry_late_pending(&id)? {
                    return Err(okx_execution::StoreError::Invalid(
                        "TERMINAL_ENTRY_LATE_BILL_PENDING",
                    ));
                }
                s.bind_terminal_entry_settlement(&id, venue_at, filled, through)
                    .map(Some)
            })
            .await
            .map_err(err)?;
        if let Some(proof) = existing {
            if proof.plan_hash != i.plan_hash
                || proof.venue_order_id != venue.order_id
                || proof.venue_filled_quantity != filled
            {
                return Err("TERMINAL_ENTRY_REPLAY_IDENTITY_MISMATCH".into());
            }
            return Ok(EntrySettlementOutcome::TerminalReplayConfirmed);
        }
    }
    let partial_id = i.intent_id.clone();
    let proof = store
        .call(sql_deadline(), move |s| {
            s.bind_partial_entry_settlement(&partial_id, venue_at, filled, through)
        })
        .await
        .map_err(err)?;
    if proof.reservation_key != receipt.reservation_key
        || proof.plan_hash != i.plan_hash
        || proof.venue_order_id != venue.order_id
    {
        return Err("PARTIAL_ENTRY_SOURCE_IDENTITY_MISMATCH".into());
    }
    for _ in 0..3 {
        let observed = risk
            .request(build_risk_command(
                &state,
                format!("inspect_partial_{}", i.client_order_id),
                RiskCommandAction::Inspect {
                    reservation_key: receipt.reservation_key.clone(),
                    plan_sha256: i.plan_hash.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if observed.reservation_state == Some(RiskReservationState::Released) {
            return Err("LATE_ENTRY_SETTLEMENT_AFTER_RELEASE_REQUIRES_RECONCILIATION".into());
        }
        let sequence = observed
            .risk_book_sequence
            .ok_or("PARTIAL_ENTRY_RISK_SEQUENCE_MISSING")?;
        let result = risk
            .request(build_risk_command(
                &state,
                format!(
                    "settle_partial_{}_{}",
                    i.client_order_id, proof.settlement_revision
                ),
                RiskCommandAction::SettleEntry {
                    reservation_key: proof.reservation_key.clone(),
                    plan_sha256: proof.plan_hash.clone(),
                    expected_risk_book_sequence: sequence,
                    cumulative_entry_usd: exact(proof.cumulative_entry_usd),
                    settlement_revision: proof.settlement_revision,
                    source_digest: proof.source_digest.clone(),
                },
            ))
            .await
            .map_err(str::to_owned)?;
        if result.status == RiskDecisionStatus::Updated {
            let committed = proof.clone();
            store
                .call(sql_deadline(), move |s| {
                    s.acknowledge_partial_entry_settlement(&committed)
                })
                .await
                .map_err(err)?;
            return Ok(EntrySettlementOutcome::RevisionAcknowledged);
        }
        if !matches!(
            result.reason.as_deref(),
            Some("STALE_SESSION_OR_BOOK_SEQUENCE" | "ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN")
        ) {
            return Err(result
                .reason
                .unwrap_or("PARTIAL_ENTRY_SETTLEMENT_DENIED".into()));
        }
    }
    Err("PARTIAL_ENTRY_SETTLEMENT_CAS_BUSY".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r04_r05_failure_metadata_preserves_reason_codes() {
        for (reason, stage, kind) in [
            (
                "PARTIAL_ENTRY_FILLS_UNCOVERED",
                "source_coverage",
                SettlementFailureKind::AwaitingEvidence,
            ),
            (
                "PARTIAL_ENTRY_FINANCIAL_COVERAGE_PENDING",
                "source_coverage",
                SettlementFailureKind::AwaitingEvidence,
            ),
            (
                "PARTIAL_ENTRY_ORDER_IDENTITY_MISMATCH",
                "identity",
                SettlementFailureKind::Conflict,
            ),
            (
                "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY",
                "risk_response",
                SettlementFailureKind::AwaitingEvidence,
            ),
            (
                "LATE_ENTRY_SETTLEMENT_CAS_BUSY",
                "risk_cas",
                SettlementFailureKind::AwaitingEvidence,
            ),
            (
                "SQL_FAILURE_UNKNOWN",
                "settlement",
                SettlementFailureKind::Failed,
            ),
        ] {
            let result = EntrySettlementError::from_reason(reason.into());
            assert_eq!(result.stage, stage);
            assert_eq!(result.kind, kind);
            assert_eq!(result.reason_code, reason);
        }
    }
}
