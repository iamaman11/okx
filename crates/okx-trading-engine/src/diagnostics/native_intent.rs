//! One account-scoped durable SQL snapshot; no venue or recovery execution.
use crate::accounts::AccountResources;
use okx_trade_execution::IntentState;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

// #512 S0 fixed read-only diagnostic budget; no financial operation is canceled.
const READ_BUDGET: Duration = Duration::from_millis(250);

// Classify only facts already recorded by the owner; this is not a replay of
// admission and cannot establish why a response was lost.
#[derive(Clone, Copy)]
enum IncidentEvidence {
    FeeSource,
    BudgetDenied,
    Other,
}

fn incident_evidence(incident: Option<&str>) -> IncidentEvidence {
    match incident {
        Some(
            "ENTRY_FEE_SOURCE_STALE"
            | "ENTRY_FEE_EXCHANGE_TIME_STALE"
            | "ENTRY_FEE_SOURCE_TIME_INVALID"
            | "ENTRY_FEE_EXCHANGE_TIME_INVALID"
            | "ENTRY_FEE_SOURCE_SCOPE_MISMATCH"
            | "ENTRY_FEE_GENERATION_INVALID",
        ) => IncidentEvidence::FeeSource,
        Some(
            "ENTRY_BUDGET_EXCEEDED"
            | "STRATEGY_BUDGET_EXCEEDED"
            | "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED"
            | "CAMPAIGN_LOSS_BUDGET_EXHAUSTED",
        ) => IncidentEvidence::BudgetDenied,
        _ => IncidentEvidence::Other,
    }
}

// Read-only diagnostics must finish even when the writer cannot start the
// queued read. The financial owner's start deadline does not bound reply wait.
// Canceling this read grants no permission and cancels no financial command.
async fn bounded_snapshot<T, F>(until: Instant, read: F) -> Result<T, String>
where
    F: std::future::Future<Output = okx_trade_execution::StoreResult<T>>,
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
        "stage":"DIAGNOSTIC_READ",
        "read_only":true, "network_calls":0, "send_permission":"NOT_GRANTED",
        "durable_receipt":{"state":"NOT_INFERRED","reason_code":"RECEIPT_NOT_READ_IN_THIS_SNAPSHOT"},
        "causal_history":"NOT_RECONSTRUCTED",
        "next_action":"retry_read_only_diagnostic"
    });
    let (owner, scope) = account.execution.store.scope();
    if owner.installation_id() != installation || scope != key {
        result["reason_code"] = "ACCOUNT_IDENTITY_MISMATCH".into();
        result["stage"] = "SCOPE_CHECK".into();
        result["missing_evidence"] = json!(["MATCHING_ACCOUNT_AND_INSTALLATION"]);
        result["next_action"] = "verify_account_and_installation_identity".into();
        return result;
    }
    let until = Instant::now() + READ_BUDGET;
    let id = if let Some(request_id) = reference.strip_prefix("request:") {
        if request_id.is_empty()
            || request_id.len() > 120
            || !request_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            result["reason_code"] = "INVALID_EDGE_REQUEST_ORIGIN".into();
            result["stage"] = "REFERENCE_VALIDATION".into();
            result["missing_evidence"] = json!(["VALID_EDGE_REQUEST_REFERENCE"]);
            result["next_action"] = "supply_original_request_reference".into();
            return result;
        }
        result["origin_request_id"] = request_id.into();
        let resolved = bounded_snapshot(
            until,
            owner.resolve_request_intent(until, scope.into(), request_id.into()),
        )
        .await;
        match resolved {
            Ok(Some(id)) => {
                result["resolved_intent_reference"] = format!("intent:{id}").into();
                id
            }
            Ok(None) => {
                result["state"] = "NOT_OBSERVED".into();
                result["stage"] = "REQUEST_ORIGIN_LOOKUP".into();
                result["reason_code"] = "REQUEST_ORIGIN_NOT_RECORDED".into();
                result["missing_evidence"] = json!(["DURABLE_REQUEST_TO_INTENT_RECEIPT"]);
                result["next_action"] = "inspect_original_idempotency_receipt_do_not_resend".into();
                return result;
            }
            Err(error) => {
                result["reason_code"] = error.into();
                result["missing_evidence"] = json!(["ACCOUNT_SCOPED_REQUEST_ORIGIN_SNAPSHOT"]);
                return result;
            }
        }
    } else if let Some(id) = reference.strip_prefix("intent:").filter(|id| {
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    }) {
        id.to_owned()
    } else {
        result["reason_code"] = "INVALID_INTENT_REFERENCE".into();
        result["stage"] = "REFERENCE_VALIDATION".into();
        result["missing_evidence"] = json!(["VALID_INTENT_REFERENCE"]);
        result["next_action"] = "supply_original_intent_reference".into();
        return result;
    };
    let snapshot = bounded_snapshot(
        until,
        owner.inspect_intent_operation(until, scope.into(), id),
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
    result["entry_source_failure"] = snapshot.entry_source_failure.as_ref().map_or_else(
        || json!({"state":"NOT_RECORDED_IN_ACCOUNT_SQL", "failure_check":"UNKNOWN"}),
        |fact| {
            json!({"state":"STORED_FIRST_FAILURE", "quality":"HISTORICAL_NOT_CURRENT_HEALTH",
            "fact":fact, "actual_age_ms":fact.checked_at_ms - fact.source_at_ms,
            "required_max_age_ms":fact.max_age_ms, "current_validity":"NOT_EVALUATED"})
        },
    );
    result["entry_source"] = snapshot.entry_quote.as_ref().map_or_else(
        || json!({"state":"NOT_RECORDED_IN_ACCOUNT_SQL", "failure_check":"UNKNOWN"}),
        |quote| json!({
            "state":"STORED_ORIGINAL_QUOTE", "quality":"HISTORICAL_NOT_CURRENT_HEALTH",
            "quote":quote,
            "quote_observation_freshness_ms":{
                "fee_exchange":quote.observed_at - quote.fee_exchange_at,
                "fee_receipt":quote.observed_at - quote.fee_received_at,
                "conversion_receipt":quote.observed_at - quote.conversion_received_at,
                "maximum":crate::config::ACCOUNT_MAX_AGE_MS
            },
            "admission":snapshot.entry_admission.as_ref(),
            "admission_quote_freshness_ms":snapshot.entry_admission.as_ref().map(|admission| json!({
                "actual":admission.observed_at - quote.observed_at, "maximum":5000
            })),
            "failure_check":"UNKNOWN", "failure_source":"NOT_RECORDED_IN_QUOTE",
            "current_validity":"NOT_EVALUATED"
        }),
    );
    result["budget_denial"] = snapshot.budget_denial.as_ref().map_or_else(
        || json!({"state":"NOT_RECORDED_IN_ACCOUNT_SQL","current_budget":"NOT_EVALUATED"}),
        |fact| {
            json!({"state":"STORED_FIRST_DENIAL","quality":"HISTORICAL_NOT_CURRENT_BUDGET",
            "current_budget":"NOT_EVALUATED","account_policy_cap":"UNKNOWN","fact":fact})
        },
    );
    let Some(intent) = snapshot.intent else {
        result["state"] = "NOT_FOUND".into();
        result["stage"] = "INTENT_LOOKUP".into();
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
    result["recorded_incident"] = json!(intent.incident);
    result["recorded_incident_quality"] = "DURABLE_INTENT_INCIDENT_NOT_VENUE_CONFIRMATION".into();
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
        || {
            json!({"state":"NOT_FOUND_IN_ACCOUNT_SQL","venue_state":"NOT_INFERRED",
            "quality":"partial","missing_evidence":["ACCOUNT_SOURCE_AS_OF"]})
        },
        |source| {
            json!({"state":"STORED_SQL_SOURCE","source":source.source,
            "partition":source.stream_partition,"generation":source.generation,
            "watermark_ms":source.watermark,"cursor":source.cursor,
            "coverage_gaps":source.coverage_gaps,"current_validity":"NOT_EVALUATED",
            "quality":"durable_historical_source_not_current_health"})
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
    let incident = incident_evidence(intent.incident.as_deref());
    let uncertain_send = matches!(intent.state, IntentState::Posting | IntentState::Unknown);
    result["missing_evidence"] = if uncertain_send {
        json!(["AUTHORITATIVE_VENUE_ORDER_READBACK"])
    } else if matches!(incident, IncidentEvidence::FeeSource) {
        json!([
            "FRESH_AUTHENTICATED_INSTRUMENT_TARIFF",
            "NEW_PLAN_AND_ADMISSION_REQUIRED"
        ])
    } else if matches!(incident, IncidentEvidence::BudgetDenied) {
        json!(["CURRENT_BUDGET_AND_NEW_ADMISSION"])
    } else {
        json!(["CURRENT_VENUE_STATE_NOT_QUERIED"])
    };
    result["next_action"] = if uncertain_send {
        "request_explicit_status_for_original_identity_do_not_resend"
    } else if matches!(incident, IncidentEvidence::FeeSource) {
        "capture_fresh_tariff_then_create_new_plan_do_not_reuse_original_permission"
    } else if matches!(incident, IncidentEvidence::BudgetDenied) {
        "inspect_current_budget_then_create_new_plan_and_admission"
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

    #[tokio::test(start_paused = true)]
    async fn logical_diagnostic_deadline_waits_until_boundary_without_real_sleep() {
        let until = Instant::now() + READ_BUDGET;
        let read = tokio::spawn(bounded_snapshot::<(), _>(until, std::future::pending()));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(!read.is_finished());
        tokio::time::advance(Duration::from_millis(150)).await;
        assert_eq!(
            read.await.unwrap(),
            Err("UNIFIED_FINANCE_DIAGNOSTIC_TIMEOUT".into())
        );
        assert_eq!(
            bounded_snapshot(until, std::future::ready(Ok(()))).await,
            Ok(())
        );
    }
}
