//! Best-effort preservation of the actual first failed post-HELD fee check.
use crate::{accounts::AccountResources, execution_store::OperationId};
use okx_execution::{
    StoredIntent,
    account_worker::{NativeEntrySourceFailure, NativeEntrySourceKind},
};
use std::time::Instant;

pub(super) async fn record(
    account: &AccountResources,
    intent: &StoredIntent,
    fee: &okx_observation::FeeScheduleSnapshot,
    checked_at: i64,
    reason: &str,
    deadline: Instant,
) {
    let (source_kind, source_at) = match reason {
        "ENTRY_FEE_EXCHANGE_TIME_STALE" => (
            NativeEntrySourceKind::FeeExchange,
            fee.exchange_timestamp_ms.parse::<i64>().ok(),
        ),
        "ENTRY_FEE_SOURCE_STALE" => (
            NativeEntrySourceKind::FeeReceipt,
            chrono::DateTime::parse_from_rfc3339(&fee.source_received_at)
                .ok()
                .map(|v| v.timestamp_millis()),
        ),
        _ => return,
    };
    let Some(source_at) = source_at else { return };
    if source_at <= 0
        || checked_at
            .checked_sub(source_at)
            .is_none_or(|age| age <= crate::config::ACCOUNT_MAX_AGE_MS)
    {
        return;
    }
    let (owner, scope) = account.execution.store.scope();
    let fact = NativeEntrySourceFailure {
        installation_id: owner.installation_id().into(),
        account_key: scope.into(),
        intent_id: intent.intent_id.clone(),
        plan_hash: intent.plan_hash.clone(),
        phase: "POST_HELD_SOURCE_CHECK".into(),
        reason: reason.into(),
        source_kind,
        source_version: fee.fee_generation.clone(),
        reference_version: fee.reference_generation.clone(),
        source_at_ms: source_at,
        checked_at_ms: checked_at,
        max_age_ms: crate::config::ACCOUNT_MAX_AGE_MS,
    };
    let Ok(encoded) = serde_json::to_string(&fact) else {
        return;
    };
    if encoded.len() > 4096 {
        return;
    }
    let Ok(id) = OperationId::new(format!("entry-source-failure:{}", intent.intent_id)) else {
        return;
    };
    // The existing operation receipt rejects a different result. Do not change
    // the primary failure or retry a financial action if recording fails.
    match account
        .execution
        .store
        .operation(deadline, id.clone())
        .await
    {
        Ok(Some(_)) => return,
        Err(_) => {
            tracing::warn!("entry source failure inspection could not be recorded");
            return;
        }
        Ok(None) => {}
    }
    if account
        .execution
        .store
        .operation_receipt(
            deadline,
            id,
            intent.plan_hash.clone(),
            "ENTRY_SOURCE_FAILURE".into(),
            encoded,
        )
        .await
        .is_err()
    {
        tracing::warn!("entry source failure inspection could not be recorded");
    }
}
