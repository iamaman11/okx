//! Cumulative entry settlement through the installation's sole financial owner.
//! Terminal release remains independent in terminal_release::release_terminal.
use super::{
    CentralGovernanceSnapshot, NativeRiskClient, StoredIntent, build_risk_command, err, exact,
    reconcile::{terminal_order_update_time, verified_order_readback},
};
use crate::execution_store::ExecutionStore;
use okx_api::TradeOrderDetails;
use okx_ipc_contracts::{
    accounts::AccountKey,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum SettlementFailureKind {
    AwaitingEvidence,
    Conflict,
    Unavailable,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(super) enum EntrySettlementStage {
    SourceCoverage,
    RiskResponse,
    RiskCas,
    Identity,
    VenueVerification,
    Settlement,
}

/// The owner captures the boundary and decision when the failure occurs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EntrySettlementError {
    pub(super) stage: EntrySettlementStage,
    pub(super) kind: SettlementFailureKind,
    pub(super) intent_id: String,
    pub(super) intent_revision: i64,
    pub(super) reason_code: String,
}

fn store_failure_kind(error: &okx_trade_execution::StoreError) -> SettlementFailureKind {
    match error {
        okx_trade_execution::StoreError::Invalid(
            "UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY",
        ) => SettlementFailureKind::AwaitingEvidence,
        okx_trade_execution::StoreError::Sql(_) | okx_trade_execution::StoreError::Io(_) => {
            SettlementFailureKind::Unavailable
        }
        okx_trade_execution::StoreError::Invalid(_) => SettlementFailureKind::Failed,
    }
}

fn risk_failure_kind(reason: &str) -> SettlementFailureKind {
    match reason {
        "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY"
        | "RISK_TRANSPORT_LOST_DO_NOT_REPLAY"
        | "UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY" => {
            SettlementFailureKind::AwaitingEvidence
        }
        "NATIVE_RISK_INSPECT_SOURCE_UNAVAILABLE"
        | "NATIVE_RISK_SETTLEMENT_OWNER_UNAVAILABLE"
        | "FINANCE_CAMPAIGN_IDENTITY_UNAVAILABLE" => SettlementFailureKind::Unavailable,
        _ => SettlementFailureKind::Failed,
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
    store: &ExecutionStore,
    risk: &NativeRiskClient,
    governance: &RwLock<Option<CentralGovernanceSnapshot>>,
    sql_deadline: fn() -> Instant,
) -> Result<EntrySettlementOutcome, EntrySettlementError> {
    settle_inner(account, i, order, store, risk, governance, sql_deadline).await
}

async fn settle_inner(
    account: &AccountKey,
    i: &StoredIntent,
    order: Option<&TradeOrderDetails>,
    store: &ExecutionStore,
    risk: &NativeRiskClient,
    governance: &RwLock<Option<CentralGovernanceSnapshot>>,
    sql_deadline: fn() -> Instant,
) -> Result<EntrySettlementOutcome, EntrySettlementError> {
    use EntrySettlementStage as Stage;
    use SettlementFailureKind as Kind;
    let failure = |stage, kind, reason_code: String| EntrySettlementError {
        stage,
        kind,
        intent_id: i.intent_id.clone(),
        intent_revision: i.revision,
        reason_code,
    };
    let store_failure = |error: okx_trade_execution::StoreError| {
        let kind = store_failure_kind(&error);
        failure(Stage::Settlement, kind, err(error))
    };
    // This transport boundary exposes exact static outcomes. An unrecognized
    // outcome remains a failure and cannot be mistaken for a successful replay.
    let risk_failure = |reason: &str| {
        let kind = risk_failure_kind(reason);
        failure(Stage::RiskResponse, kind, reason.to_owned())
    };
    if account.environment != okx_ipc_contracts::accounts::Environment::Real || !i.risk_increase {
        return Ok(EntrySettlementOutcome::NotApplicable);
    }
    let venue = order.ok_or_else(|| {
        failure(
            Stage::VenueVerification,
            Kind::Failed,
            "PARTIAL_ENTRY_ORDER_READBACK_UNAVAILABLE".into(),
        )
    })?;
    if !matches!(
        venue.state.as_str(),
        "partially_filled" | "filled" | "canceled" | "mmp_canceled"
    ) {
        return Err(failure(
            Stage::VenueVerification,
            Kind::Failed,
            "PARTIAL_ENTRY_ORDER_STATE_UNVERIFIED".into(),
        ));
    }
    let filled = verified_order_readback(i, venue).map_err(|reason| {
        let kind = if reason == "ORDER_READBACK_IDENTITY_MISMATCH" {
            Kind::Conflict
        } else {
            Kind::Failed
        };
        failure(Stage::Identity, kind, reason)
    })?;
    if filled <= Decimal::ZERO {
        return Err(failure(
            Stage::VenueVerification,
            Kind::Failed,
            "PARTIAL_ENTRY_ZERO_FILL_SOURCE_UNPROVEN".into(),
        ));
    }
    let state = governance.read().await.clone().ok_or_else(|| {
        failure(
            Stage::Settlement,
            Kind::Failed,
            "AUTHORITY_UNAVAILABLE".into(),
        )
    })?;
    let id = i.intent_id.clone();
    let receipt = store
        .central_receipt(sql_deadline(), id)
        .await
        .map_err(store_failure)?;
    let fill_cursor = store
        .cursor(sql_deadline(), "fills".into(), "SWAP".into())
        .await
        .map_err(store_failure)?;
    let bill_cursor = store
        .cursor(sql_deadline(), "bills".into(), "account".into())
        .await
        .map_err(store_failure)?;
    let receipt = receipt.ok_or_else(|| {
        failure(
            Stage::Settlement,
            Kind::Failed,
            "PARTIAL_ENTRY_RESERVATION_UNVERIFIED".into(),
        )
    })?;
    if receipt.reservation_key.starts_with("local:") || receipt.plan_hash != i.plan_hash {
        return Err(failure(
            Stage::Settlement,
            Kind::Failed,
            "PARTIAL_ENTRY_RESERVATION_INVALID".into(),
        ));
    }
    if i.exchange_order_id.as_deref() != Some(venue.order_id.as_str()) {
        return Err(failure(
            Stage::Identity,
            Kind::Conflict,
            "PARTIAL_ENTRY_ORDER_IDENTITY_MISMATCH".into(),
        ));
    }
    let venue_at = terminal_order_update_time(Some(venue))
        .map_err(|reason| failure(Stage::VenueVerification, Kind::Failed, reason))?
        .ok_or_else(|| {
            failure(
                Stage::Settlement,
                Kind::Failed,
                "PARTIAL_ENTRY_UPDATE_TIME_MISSING".into(),
            )
        })?;
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
        .map_err(risk_failure)?;
    let fills = fill_cursor.ok_or_else(|| {
        failure(
            Stage::SourceCoverage,
            Kind::AwaitingEvidence,
            "PARTIAL_ENTRY_FILLS_UNCOVERED".into(),
        )
    })?;
    let bills = bill_cursor.ok_or_else(|| {
        failure(
            Stage::SourceCoverage,
            Kind::AwaitingEvidence,
            "PARTIAL_ENTRY_BILLS_UNCOVERED".into(),
        )
    })?;
    if !fills.cursor.is_empty()
        || !bills.cursor.is_empty()
        || !fills.coverage_gaps.is_empty()
        || !bills.coverage_gaps.is_empty()
        || fills.generation <= 0
        || bills.generation <= 0
    {
        return Err(failure(
            Stage::SourceCoverage,
            Kind::AwaitingEvidence,
            "PARTIAL_ENTRY_SOURCE_PAGES_INCOMPLETE".into(),
        ));
    }
    let through = fills.watermark.min(bills.watermark);
    if through < venue_at {
        return Err(failure(
            Stage::SourceCoverage,
            Kind::AwaitingEvidence,
            "PARTIAL_ENTRY_FINANCIAL_COVERAGE_PENDING".into(),
        ));
    }
    if inspection.reservation_state == Some(RiskReservationState::Released) {
        let id = i.intent_id.clone();
        let late = store
            .entry_late_pending(sql_deadline(), id)
            .await
            .map_err(store_failure)?;
        if !late {
            let existing_id = format!("entry-settlement:{}", i.intent_id);
            let recorded = store
                .operation(
                    sql_deadline(),
                    crate::execution_store::OperationId::new(existing_id).map_err(store_failure)?,
                )
                .await
                .map_err(store_failure)?;
            return if recorded.is_some_and(|w| w.kind == "ENTRY_SETTLEMENT_PROVEN") {
                Ok(EntrySettlementOutcome::AlreadyConfirmed)
            } else {
                Err(failure(
                    Stage::Settlement,
                    Kind::Failed,
                    "RELEASED_ENTRY_TERMINAL_SOURCE_MISSING".into(),
                ))
            };
        }
        if !i.state.terminal() {
            return Err(failure(
                Stage::Settlement,
                Kind::Failed,
                "LATE_ENTRY_ORDER_NOT_TERMINAL".into(),
            ));
        }
        let id = i.intent_id.clone();
        let late_proof = store
            .bind_late_entry_settlement(sql_deadline(), id, venue_at, filled, through)
            .await
            .map_err(store_failure)?;
        if late_proof.schema != "okx.entry-late-settlement/v1"
            || late_proof.reservation_key != receipt.reservation_key
            || late_proof.venue_order_id != venue.order_id
        {
            return Err(failure(
                Stage::Settlement,
                Kind::Failed,
                "LATE_ENTRY_ACCOUNT_PROOF_MISSING".into(),
            ));
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
                .map_err(risk_failure)?;
            if checked.reservation_state != Some(RiskReservationState::Released) {
                return Err(failure(
                    Stage::Settlement,
                    Kind::Failed,
                    "LATE_ENTRY_RESERVATION_CHANGED".into(),
                ));
            }
            let sequence = checked.risk_book_sequence.ok_or_else(|| {
                failure(
                    Stage::RiskCas,
                    Kind::Failed,
                    "LATE_ENTRY_RISK_SEQUENCE_MISSING".into(),
                )
            })?;
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
                .map_err(risk_failure)?;
            if done.status == RiskDecisionStatus::Updated {
                store
                    .acknowledge_late_entry_settlement(sql_deadline(), late_proof)
                    .await
                    .map_err(store_failure)?;
                return Ok(EntrySettlementOutcome::LateRevisionAcknowledged);
            }
            if !matches!(
                done.reason.as_deref(),
                Some(
                    "STALE_SESSION_OR_BOOK_SEQUENCE" | "ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN"
                )
            ) {
                return Err(failure(
                    Stage::RiskResponse,
                    Kind::Failed,
                    done.reason.unwrap_or("LATE_ENTRY_SETTLEMENT_DENIED".into()),
                ));
            }
        }
        return Err(failure(
            Stage::RiskCas,
            Kind::AwaitingEvidence,
            "LATE_ENTRY_SETTLEMENT_CAS_BUSY".into(),
        ));
    }
    // A terminal settlement may already have been bound during an earlier
    // Status which could not RELEASE because another terminal reservation
    // was still pending. Re-verify that same immutable terminal source
    // witness; never attempt a PARTIAL settlement against its receipt
    // (a different schema/revision) or silently accept changed fills/fees.
    // The normal release_terminal path owns terminal SETTLE_ENTRY and RELEASE;
    // this read-only replay grants no new capacity.
    if i.state.terminal() {
        let id = i.intent_id.clone();
        let existing = if store
            .operation(
                sql_deadline(),
                crate::execution_store::OperationId::new(format!("entry-settlement:{id}"))
                    .map_err(store_failure)?,
            )
            .await
            .map_err(store_failure)?
            .is_some()
        {
            if store
                .entry_late_pending(sql_deadline(), id.clone())
                .await
                .map_err(store_failure)?
            {
                return Err(failure(
                    Stage::Settlement,
                    Kind::Failed,
                    "TERMINAL_ENTRY_LATE_BILL_PENDING".into(),
                ));
            }
            Some(
                store
                    .bind_terminal_entry_settlement(sql_deadline(), id, venue_at, filled, through)
                    .await
                    .map_err(store_failure)?,
            )
        } else {
            None
        };
        if let Some(proof) = existing {
            if proof.plan_hash != i.plan_hash
                || proof.venue_order_id != venue.order_id
                || proof.venue_filled_quantity != filled
            {
                return Err(failure(
                    Stage::Identity,
                    Kind::Conflict,
                    "TERMINAL_ENTRY_REPLAY_IDENTITY_MISMATCH".into(),
                ));
            }
            return Ok(EntrySettlementOutcome::TerminalReplayConfirmed);
        }
    }
    let partial_id = i.intent_id.clone();
    let proof = store
        .bind_partial_entry_settlement(sql_deadline(), partial_id, venue_at, filled, through)
        .await
        .map_err(store_failure)?;
    if proof.reservation_key != receipt.reservation_key
        || proof.plan_hash != i.plan_hash
        || proof.venue_order_id != venue.order_id
    {
        return Err(failure(
            Stage::Identity,
            Kind::Conflict,
            "PARTIAL_ENTRY_SOURCE_IDENTITY_MISMATCH".into(),
        ));
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
            .map_err(risk_failure)?;
        if observed.reservation_state == Some(RiskReservationState::Released) {
            return Err(failure(
                Stage::Settlement,
                Kind::Failed,
                "LATE_ENTRY_SETTLEMENT_AFTER_RELEASE_REQUIRES_RECONCILIATION".into(),
            ));
        }
        let sequence = observed.risk_book_sequence.ok_or_else(|| {
            failure(
                Stage::RiskCas,
                Kind::Failed,
                "PARTIAL_ENTRY_RISK_SEQUENCE_MISSING".into(),
            )
        })?;
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
            .map_err(risk_failure)?;
        if result.status == RiskDecisionStatus::Updated {
            let committed = proof.clone();
            store
                .acknowledge_partial_entry_settlement(sql_deadline(), committed)
                .await
                .map_err(store_failure)?;
            return Ok(EntrySettlementOutcome::RevisionAcknowledged);
        }
        if !matches!(
            result.reason.as_deref(),
            Some("STALE_SESSION_OR_BOOK_SEQUENCE" | "ENTRY_SETTLEMENT_SOURCE_OR_STATE_UNPROVEN")
        ) {
            return Err(failure(
                Stage::RiskResponse,
                Kind::Failed,
                result
                    .reason
                    .unwrap_or("PARTIAL_ENTRY_SETTLEMENT_DENIED".into()),
            ));
        }
    }
    Err(failure(
        Stage::RiskCas,
        Kind::AwaitingEvidence,
        "PARTIAL_ENTRY_SETTLEMENT_CAS_BUSY".into(),
    ))
}

#[cfg(test)]
#[path = "entry_settlement_direct_tests.rs"]
mod direct_tests;
