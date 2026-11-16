//! One account-scoped durable SQL snapshot; no venue or recovery execution.
use crate::accounts::AccountResources;
use okx_execution::IntentState;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

// Read-only diagnostics must finish even when the writer cannot start the
// queued read. The financial owner's start deadline does not bound reply wait.
// Canceling this read grants no permission and cancels no financial command.
async fn bounded_snapshot<T, F>(until: Instant, read: F) -> Result<T, String>
where
    F: std::future::Future<Output = okx_execution::StoreResult<T>>,
{
    tokio::time::timeout_at(until.into(), read)
        .await
        .map_err(|_| "UNIFIED_FINANCE_DIAGNOSTIC_TIMEOUT".to_owned())?
        .map_err(|error| super::storage_failure_code(&error))
}

pub(crate) async fn read(account: &AccountResources, installation: &str, reference: &str) -> Value {
    let key = account.configuration.account_key.wire();
    let mut result = json!({
        "schema":"okx.operation-diagnostics/v1", "family":"native_intent",
        "account":key, "installation_id":installation, "reference":reference,
        "owner":"execution.unified_finance", "state":"NOT_EVALUATED", "quality":"partial",
        "read_only":true, "network_calls":0, "send_permission":"NOT_GRANTED",
        "durable_receipt":{"state":"NOT_INFERRED","reason_code":"RECEIPT_NOT_READ_IN_THIS_SNAPSHOT"},
        "causal_history":"NOT_RECONSTRUCTED",
        "next_action":"retry_read_only_diagnostic"
    });
    let Some(id) = reference.strip_prefix("intent:").filter(|id| {
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }) else {
        result["reason_code"] = "INVALID_INTENT_REFERENCE".into();
        return result;
    };
    let Some((owner, scope)) = account.execution.store.native_scope() else {
        result["reason_code"] = "NATIVE_FINANCE_OWNER_REQUIRED".into();
        return result;
    };
    if owner.installation_id() != installation || scope != key {
        result["reason_code"] = "ACCOUNT_IDENTITY_MISMATCH".into();
        return result;
    }
    let until = Instant::now() + Duration::from_secs(2);
    let snapshot = bounded_snapshot(
        until,
        owner.inspect_intent_operation(until, scope.into(), id.into()),
    )
    .await;
    let snapshot = match snapshot {
        Ok(snapshot) => snapshot,
        Err(error) => {
            result["reason_code"] = error.into();
            result["missing_evidence"] = json!(["COHERENT_ACCOUNT_SQL_SNAPSHOT"]);
            return result;
        }
    };
    result["observed_at_ms"] = snapshot.observed_at_ms.into();
    let Some(intent) = snapshot.intent else {
        result["state"] = "NOT_FOUND".into();
        result["reason_code"] = "INTENT_NOT_FOUND_IN_ACCOUNT_SQL".into();
        result["missing_evidence"] = json!(["ORIGINAL_ACCOUNT_SCOPED_INTENT"]);
        result["next_action"] = "verify_account_and_original_intent_identity".into();
        return result;
    };
    result["state"] = intent.state.as_str().into();
    result["stage"] = intent.state.as_str().into();
    result["reason_code"] = if let Some(reason) = intent
        .incident
        .as_ref()
        .filter(|_| !matches!(intent.state, IntentState::Posting | IntentState::Unknown))
    {
        reason.clone()
    } else {
        if matches!(intent.state, IntentState::Posting | IntentState::Unknown) {
            "AUTHORITATIVE_ORDER_READBACK_REQUIRED"
        } else {
            "DURABLE_INTENT_STATE_OBSERVED"
        }
        .into()
    }
    .into();
    result["send_state"] = if snapshot.begin_post_at_ms.is_some() {
        "POTENTIAL_SEND_COMMITTED"
    } else {
        "NOT_STARTED_IN_SQL"
    }
    .into();
    result["begin_post_at_ms"] = json!(snapshot.begin_post_at_ms);
    result["reservation_state"] = json!(snapshot.receipt_state);
    result["unsent_proof"] = snapshot.unsent.map_or_else(
        || json!({"state":"NOT_FOUND_IN_ACCOUNT_SQL"}),
        |proof| {
            json!({"state":"DURABLE_SQL_NO_SEND","intent_id":proof.intent_id,
            "plan_hash":proof.plan_hash,"reason_code":proof.reason})
        },
    );
    result["intent"] = json!({
        "intent_id":intent.intent_id, "client_order_id":intent.client_order_id,
        "exchange_order_id":intent.exchange_order_id, "instrument_id":intent.instrument_id,
        "plan_hash":intent.plan_hash, "request_hash":intent.request_hash,
        "policy_revision":intent.policy_revision, "revision":intent.revision,
        "deadline_ms":intent.deadline, "incident":intent.incident,
        "source_generation":intent.plan.source_generation,
        "private_source_generation":intent.plan.private_source_generation
    });
    result["durable_receipt"] = snapshot.reservation.map_or_else(
        || json!({"state":"NOT_FOUND_IN_ACCOUNT_SQL","send_permission":"NOT_GRANTED"}),
        |receipt| {
            json!({"state":"STORED_SQL_RECEIPT","send_permission":"NOT_GRANTED",
            "reservation_key":receipt.reservation_key,"account_key":receipt.account_key,
            "installation_id":receipt.installation_id,"intent_id":receipt.intent_id,
            "plan_hash":receipt.plan_hash,"policy_revision":receipt.policy_revision,
            "receipt_hash":receipt.receipt_hash,"requested_loss_usd":receipt.requested_loss_usd})
        },
    );
    result["source_as_of"] = snapshot.account_source.map_or_else(
        || json!({"state":"NOT_FOUND_IN_ACCOUNT_SQL","venue_state":"NOT_INFERRED"}),
        |source| {
            json!({"state":"STORED_SQL_SOURCE","source":source.source,
            "partition":source.stream_partition,"generation":source.generation,
            "watermark_ms":source.watermark,"cursor":source.cursor,
            "coverage_gaps":source.coverage_gaps,"current_validity":"NOT_EVALUATED"})
        },
    );
    result["protection"] = snapshot.protection.map_or_else(
        || json!({"state":"NOT_FOUND_IN_ACCOUNT_SQL","venue_state":"NOT_INFERRED"}),
        |proof| {
            json!({"state":"STORED_SQL_PROOF","venue_state":"NOT_INFERRED",
            "intent_id":proof.intent_id,"algo_id":proof.algo_id,"quantity":proof.quantity,
            "verified_at_ms":proof.verified_at,"source_digest":proof.source_digest})
        },
    );
    let stale_fee = matches!(
        intent.incident.as_deref(),
        Some("ENTRY_FEE_SOURCE_STALE" | "ENTRY_FEE_EXCHANGE_TIME_STALE")
    );
    result["missing_evidence"] = if stale_fee {
        json!([
            "FRESH_AUTHENTICATED_INSTRUMENT_TARIFF",
            "NEW_PLAN_AND_ADMISSION_REQUIRED"
        ])
    } else {
        if matches!(intent.state, IntentState::Posting | IntentState::Unknown) {
            json!(["AUTHORITATIVE_VENUE_ORDER_READBACK"])
        } else {
            json!(["CURRENT_VENUE_STATE_NOT_QUERIED"])
        }
    };
    result["next_action"] = if stale_fee {
        "capture_fresh_tariff_then_create_new_plan_do_not_reuse_original_permission"
    } else if matches!(intent.state, IntentState::Posting | IntentState::Unknown) {
        "request_explicit_status_for_original_identity_do_not_resend"
    } else {
        "inspect_original_identity_before_any_action"
    }
    .into();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn risk_book_contract_native_diagnostic_busy_read_has_bounded_unknown_result() {
        let result = bounded_snapshot::<(), _>(Instant::now(), std::future::pending()).await;
        assert!(matches!(result, Err(reason) if reason == "UNIFIED_FINANCE_DIAGNOSTIC_TIMEOUT"));
    }
}
