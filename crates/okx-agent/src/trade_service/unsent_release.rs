//! Release only a durably rejected, never-sent intent. AccountStore owns the
//! no-send proof/ack; RiskBook owns the reservation. No venue adapter is needed.
use super::{build_risk_command, digest};
use crate::risk_bridge::NativeRiskClient;
use okx_execution::{AccountStoreHandle, StoredIntent};
use okx_protocol::{
    account_governance::CentralGovernanceSnapshot,
    accounts::Environment,
    risk_authority::{
        RiskCommandAction, RiskDecisionStatus, RiskReservationState, UnsentRiskReleaseProof,
    },
};
use serde::Serialize;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum UnsentReleaseStage {
    SqlProof,
    Authority,
    Inspect,
    Release,
    SqlAck,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(super) struct UnsentReleaseError {
    pub stage: UnsentReleaseStage,
    pub reason_code: String,
    pub intent_id: String,
    pub reservation_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UnsentReleaseOutcome {
    NotApplicable,
    LocalAcknowledged,
    CentralAcknowledged,
}

/// Explicit handles to existing owners, not another execution context. A fresh
/// SQL deadline is obtained per call, preserving the original operation bounds.
pub(super) struct UnsentReleaseDependencies<'a> {
    pub store: &'a AccountStoreHandle,
    pub risk: &'a NativeRiskClient,
    pub governance: &'a RwLock<Option<CentralGovernanceSnapshot>>,
    pub sql_deadline: fn() -> Instant,
}

pub(super) async fn release_unsent(
    environment: Environment,
    intent: &StoredIntent,
    deps: UnsentReleaseDependencies<'_>,
) -> Result<UnsentReleaseOutcome, UnsentReleaseError> {
    if environment != Environment::Real {
        return Ok(UnsentReleaseOutcome::NotApplicable);
    }
    let failure = |stage, reason_code: String, reservation_key: Option<&str>| UnsentReleaseError {
        stage,
        reason_code,
        intent_id: intent.intent_id.clone(),
        reservation_key: reservation_key.map(str::to_owned),
    };
    let id = intent.intent_id.clone();
    let proof = deps
        .store
        .call((deps.sql_deadline)(), move |s| {
            s.unsent_release_receipt(&id)
        })
        .await
        .map_err(|e| failure(UnsentReleaseStage::SqlProof, e.to_string(), None))?
        .ok_or_else(|| {
            failure(
                UnsentReleaseStage::SqlProof,
                "DURABLE_UNSENT_SQL_PROOF_MISSING".into(),
                None,
            )
        })?;
    let fail = |stage, reason: String| failure(stage, reason, Some(&proof.reservation_key));
    let state = deps.governance.read().await.clone().ok_or_else(|| {
        fail(
            UnsentReleaseStage::Authority,
            "AUTHORITY_UNAVAILABLE".into(),
        )
    })?;
    let reservation = &proof.reservation_key;
    if reservation.starts_with("local:") {
        let id = intent.intent_id.clone();
        deps.store
            .call((deps.sql_deadline)(), move |s| {
                s.acknowledge_local_unsent_release(&id)
            })
            .await
            .map_err(|e| fail(UnsentReleaseStage::SqlAck, e.to_string()))?;
        return Ok(UnsentReleaseOutcome::LocalAcknowledged);
    }
    for _ in 0..4 {
        let observed = deps
            .risk
            .request(build_risk_command(
                &state,
                format!("inspect_{}", intent.client_order_id),
                RiskCommandAction::Inspect {
                    reservation_key: reservation.clone(),
                    plan_sha256: proof.plan_hash.clone(),
                },
            ))
            .await
            .map_err(|e| fail(UnsentReleaseStage::Inspect, e.into()))?;
        let absent = observed.status == RiskDecisionStatus::Denied
            && observed.reason.as_deref() == Some("RISK_RESERVATION_NOT_FOUND");
        if observed.status != RiskDecisionStatus::Observed && !absent {
            return Err(fail(
                UnsentReleaseStage::Inspect,
                observed
                    .reason
                    .unwrap_or("CENTRAL_INSPECTION_FAILED".into()),
            ));
        }
        if observed.reservation_state == Some(RiskReservationState::Released) {
            let evidence = digest(&observed).map_err(|e| fail(UnsentReleaseStage::SqlAck, e))?;
            acknowledge(deps.store, reservation, evidence, deps.sql_deadline)
                .await
                .map_err(|e| fail(UnsentReleaseStage::SqlAck, e))?;
            return Ok(UnsentReleaseOutcome::CentralAcknowledged);
        }
        if !absent
            && !matches!(
                observed.reservation_state,
                Some(RiskReservationState::Held | RiskReservationState::Posted)
            )
        {
            return Err(fail(
                UnsentReleaseStage::Inspect,
                "UNSENT_CENTRAL_STATE_UNRESOLVED".into(),
            ));
        }
        let native_proof = UnsentRiskReleaseProof {
            reservation_key: proof.reservation_key.clone(),
            account_key: proof.account_key.clone(),
            intent_id: proof.intent_id.clone(),
            plan_hash: proof.plan_hash.clone(),
            request_hash: proof.request_hash.clone(),
            reason: proof.reason.clone(),
            record_digest: digest(&proof).map_err(|e| fail(UnsentReleaseStage::Release, e))?,
        };
        let result = deps
            .risk
            .request(build_risk_command(
                &state,
                format!("release_{}", intent.client_order_id),
                RiskCommandAction::ReleaseUnsent {
                    reservation_key: reservation.clone(),
                    plan_sha256: proof.plan_hash.clone(),
                    expected_risk_book_sequence: observed.risk_book_sequence.ok_or_else(|| {
                        fail(UnsentReleaseStage::Inspect, "RISK_SEQUENCE_MISSING".into())
                    })?,
                    proof: native_proof,
                },
            ))
            .await
            .map_err(|e| fail(UnsentReleaseStage::Release, e.into()))?;
        if result.status == RiskDecisionStatus::Updated {
            let evidence = digest(&result).map_err(|e| fail(UnsentReleaseStage::SqlAck, e))?;
            acknowledge(deps.store, reservation, evidence, deps.sql_deadline)
                .await
                .map_err(|e| fail(UnsentReleaseStage::SqlAck, e))?;
            return Ok(UnsentReleaseOutcome::CentralAcknowledged);
        }
        if result.reason.as_deref() != Some("STALE_SESSION_OR_BOOK_SEQUENCE") {
            return Err(fail(
                UnsentReleaseStage::Release,
                result
                    .reason
                    .unwrap_or("CENTRAL_UNSENT_RELEASE_DENIED".into()),
            ));
        }
    }
    Err(fail(
        UnsentReleaseStage::Release,
        "CENTRAL_UNSENT_RELEASE_CAS_BUSY".into(),
    ))
}

async fn acknowledge(
    store: &AccountStoreHandle,
    reservation: &str,
    evidence: String,
    sql_deadline: fn() -> Instant,
) -> Result<(), String> {
    let reservation = reservation.to_owned();
    store
        .call(sql_deadline(), move |s| {
            s.acknowledge_central_release(&reservation, &evidence)
        })
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
#[path = "unsent_release_tests.rs"]
mod tests;
