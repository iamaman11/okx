//! Reject/release a never-sent intent through the sole financial owner.
use crate::execution_store::ExecutionStore;
use okx_trade_execution::StoredIntent;
use okx_ipc_contracts::{account_governance::CentralGovernanceSnapshot, accounts::Environment};
use serde::Serialize;
use std::time::Instant;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum UnsentReleaseStage {
    Authority,
    Release,
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
pub(super) struct UnsentReleaseDependencies<'a> {
    pub store: &'a ExecutionStore,
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
    let failure = |stage, reason_code: String| UnsentReleaseError {
        stage,
        reason_code,
        intent_id: intent.intent_id.clone(),
        reservation_key: None,
    };
    let (owner, account) = deps.store.scope();
    let state = deps.governance.read().await.clone().ok_or_else(|| {
        failure(
            UnsentReleaseStage::Authority,
            "AUTHORITY_UNAVAILABLE".into(),
        )
    })?;
    if state.state.installation_id != owner.installation_id() {
        return Err(failure(
            UnsentReleaseStage::Authority,
            "NATIVE_UNSENT_AUTHORITY_MISMATCH".into(),
        ));
    }
    // Source, current sequence, release and receipt belong to one COMMIT.
    owner
        .release_unsent_status(
            (deps.sql_deadline)(),
            account.into(),
            intent.intent_id.clone(),
            intent.plan_hash.clone(),
        )
        .await
        .map(|local| {
            if local {
                UnsentReleaseOutcome::LocalAcknowledged
            } else {
                UnsentReleaseOutcome::CentralAcknowledged
            }
        })
        .map_err(|e| failure(UnsentReleaseStage::Release, e.to_string()))
}
#[cfg(test)]
#[path = "unsent_release_tests.rs"]
mod tests;
