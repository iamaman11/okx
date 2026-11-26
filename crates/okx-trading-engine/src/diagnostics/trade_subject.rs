//! Durable *subject* diagnostics for cancel/close/replace. These operations
//! have distinct SQL identities; none is an alias for an Edge request-id.
//! A returned local receipt is not a venue readback or send authorization.
use crate::accounts::AccountResources;
use okx_trade_execution::{TradeDiagnosticSubject, TradeSubjectDiagnosticSnapshot};
use serde_json::{Value, json};
use std::time::Instant;

fn parse(reference: &str) -> Option<(TradeDiagnosticSubject, &str, &'static str)> {
    for (prefix, subject, family) in [
        ("cancel:", TradeDiagnosticSubject::Cancel, "cancel"),
        ("close:", TradeDiagnosticSubject::Close, "close"),
        ("replace:", TradeDiagnosticSubject::Replace, "replace"),
    ] {
        if let Some(key) = reference.strip_prefix(prefix) {
            return Some((subject, key, family));
        }
    }
    None
}

fn project(snapshot: TradeSubjectDiagnosticSnapshot, subject: TradeDiagnosticSubject) -> Value {
    let base = json!({"observed_at_ms":snapshot.observed_at_ms});
    let TradeSubjectDiagnosticSnapshot {
        operation,
        intent,
        unverified_cancel_candidate,
        ..
    } = snapshot;
    match subject {
        TradeDiagnosticSubject::Cancel => match operation {
            None => json!({"state":"NOT_OBSERVED","observed_stage":"CANCEL_RECEIPT_LOOKUP",
                "first_unproven_fact":"DURABLE_CANCEL_OPERATION_RECEIPT",
                "reason_code":"CANCEL_RECEIPT_NOT_RECORDED",
                "next_action":"verify_original_cancel_identity_do_not_resend",
                "history":"UNKNOWN","observation":base}),
            Some(receipt) if receipt.result_ref == "POSTING" => json!({
                "state":"OBSERVED","observed_stage":"CANCEL_STARTED_SQL_ONLY",
                "first_unproven_fact":"CANCEL_VENUE_ACK_AND_FINAL_STATE",
                "reason_code":"CANCEL_POSTING_OUTCOME_UNKNOWN",
                "operation":{"operation_id":receipt.operation_id,"kind":"cancel",
                    "committed_at_ms":receipt.committed_at},
                "next_action":"inspect_original_order_status_do_not_resend",
                "history":"UNKNOWN","observation":base}),
            Some(receipt) => {
                let Some(linked) = intent else {
                    return json!({"state":"NOT_EVALUATED",
                        "observed_stage":"CANCEL_RECEIPT_INTENT_LOOKUP",
                        "first_unproven_fact":"ACCOUNT_SCOPED_CANCEL_TARGET",
                        "reason_code":"CANCEL_RECEIPT_INTENT_MISSING",
                        "next_action":"retry_read_only_diagnostic",
                        "history":"UNKNOWN","observation":base});
                };
                // A completed local receipt can also follow an unsent
                // Prepared/Reserved rejection. It is not a venue ACK.
                json!({"state":"OBSERVED","observed_stage":"CANCEL_LOCAL_RECEIPT_RECORDED",
                    "first_unproven_fact":"CANCEL_VENUE_OR_DURABLE_UNSENT_PROOF",
                    "reason_code":"LOCAL_CANCEL_RECEIPT_NOT_VENUE_PROOF",
                    "operation":{"operation_id":receipt.operation_id,"kind":"cancel",
                        "committed_at_ms":receipt.committed_at,
                        "target_intent_id":linked.intent_id},
                    "target_intent":{"intent_id":linked.intent_id,
                        "state":linked.state.as_str(),"client_order_id":linked.client_order_id},
                    "next_action":"trade.status","history":"UNKNOWN","observation":base})
            }
        },
        TradeDiagnosticSubject::Close => match intent {
            None => json!({"state":"NOT_OBSERVED","observed_stage":"CLOSE_INTENT_LOOKUP",
                "first_unproven_fact":"DURABLE_CLOSE_INTENT",
                "reason_code":"CLOSE_INTENT_NOT_RECORDED",
                "next_action":"verify_original_close_identity_do_not_resend",
                "history":"UNKNOWN","observation":base}),
            Some(intent) => json!({"state":"OBSERVED",
                "observed_stage":intent.state.as_str(),
                "first_unproven_fact":"ORIGINAL_CLOSE_TARGET_AND_CURRENT_VENUE_STATE",
                "reason_code":"DURABLE_CLOSE_INTENT_ONLY",
                "intent":{"intent_id":intent.intent_id,"idempotency_key":intent.idempotency_key,
                    "operation":"close","state":intent.state.as_str(),
                    "client_order_id":intent.client_order_id,
                    "incident":intent.incident},
                "next_action":"trade.status","history":"UNKNOWN","observation":base}),
        },
        TradeDiagnosticSubject::Replace => match intent {
            None => json!({"state":"NOT_OBSERVED","observed_stage":"REPLACEMENT_INTENT_LOOKUP",
                "first_unproven_fact":"DURABLE_REPLACEMENT_INTENT_AND_CANCELLATION",
                "reason_code":"REPLACE_INTENT_NOT_RECORDED",
                "next_action":"inspect_original_order_status_do_not_resend",
                "history":"UNKNOWN","observation":base}),
            Some(intent) => {
                // IntentPlan.operation is necessarily "open": the replace
                // command normalizes it before persisting the new intent.
                // The original reference is not durably bound, and a forged
                // cancel with the derived key must never become proof.
                let candidate = unverified_cancel_candidate.map(|receipt| {
                    json!({
                        "operation_id":receipt.operation_id,"kind":receipt.kind,
                        "state":if receipt.result_ref == "POSTING" {"STARTED_SQL_ONLY"}
                            else {"LOCAL_RECEIPT_ONLY"},
                        "causal_link":"UNVERIFIED",
                        "committed_at_ms":receipt.committed_at
                    })
                });
                json!({"state":"NOT_EVALUATED",
                    "observed_stage":"IDEMPOTENCY_KEY_INTENT_OBSERVED",
                    "first_unproven_fact":"REPLACE_COMMAND_AND_ORIGINAL_REFERENCE_BINDING",
                    "reason_code":"REPLACE_ORIGIN_UNVERIFIED",
                    "candidate_new_intent":{"intent_id":intent.intent_id,
                        "idempotency_key":intent.idempotency_key,"state":intent.state.as_str(),
                        "client_order_id":intent.client_order_id},
                    "cancel_candidate":candidate,
                    "causal_link":"UNVERIFIED",
                    "next_action":"inspect_original_and_replacement_status_do_not_resend",
                    "history":"UNKNOWN","observation":base})
            }
        },
    }
}

pub(super) async fn read(account: &AccountResources, installation: &str, reference: &str) -> Value {
    let mut result = json!({
        "schema":"okx.operation-diagnostics/v1",
        "owner":"execution.unified_finance",
        "account":account.configuration.account_key.wire(),
        "installation_id":installation,"reference":reference,
        "reference_kind":"idempotency_key",
        "state":"NOT_EVALUATED","quality":"partial",
        "read_only":true,"network_calls":0,"send_permission":"NOT_GRANTED",
        "history":"UNKNOWN","causal_history":"NOT_RECONSTRUCTED",
        "observed_stage":"REFERENCE_VALIDATION",
        "first_unproven_fact":"VALID_TRADE_SUBJECT_REFERENCE",
        "reason_code":"INVALID_TRADE_SUBJECT_REFERENCE",
        "next_action":"supply_original_idempotency_key_do_not_resend"
    });
    let Some((subject, key, family)) = parse(reference) else {
        return result;
    };
    result["family"] = family.into();
    if !(8..=128).contains(&key.len())
        || !key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return result;
    }
    let (owner, scope) = account.execution.store.scope();
    if owner.installation_id() != installation || scope != account.configuration.account_key.wire()
    {
        result["reason_code"] = "ACCOUNT_IDENTITY_MISMATCH".into();
        result["observed_stage"] = "SCOPE_CHECK".into();
        result["first_unproven_fact"] = "MATCHING_ACCOUNT_AND_INSTALLATION".into();
        result["next_action"] = "verify_account_and_installation_identity".into();
        return result;
    }
    let until = Instant::now() + super::native_intent::READ_BUDGET;
    match super::native_intent::bounded_snapshot(
        until,
        owner.inspect_trade_subject(until, scope.to_owned(), subject, key.to_owned()),
    )
    .await
    {
        Ok(snapshot) => {
            let detail = project(snapshot, subject);
            if let Value::Object(fields) = detail {
                for (key, value) in fields {
                    result[key.as_str()] = value;
                }
            }
        }
        Err(reason) => {
            result["reason_code"] = reason.into();
            result["observed_stage"] = "ACCOUNT_SCOPED_SQL_SNAPSHOT".into();
            result["first_unproven_fact"] = "COHERENT_ACCOUNT_SQL_SNAPSHOT".into();
            result["next_action"] = "retry_read_only_diagnostic".into();
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn subjects_are_explicit_not_request_or_intent_aliases() {
        assert_eq!(parse("request:abc"), None);
        assert_eq!(parse("intent:abc"), None);
        assert_eq!(
            parse("cancel:key12345").unwrap().0,
            TradeDiagnosticSubject::Cancel
        );
        assert_eq!(
            parse("close:key12345").unwrap().0,
            TradeDiagnosticSubject::Close
        );
        assert_eq!(
            parse("replace:key12345").unwrap().0,
            TradeDiagnosticSubject::Replace
        );
    }

    #[test]
    fn missing_receipts_cannot_prove_absence_of_financial_effects() {
        for subject in [
            TradeDiagnosticSubject::Cancel,
            TradeDiagnosticSubject::Close,
            TradeDiagnosticSubject::Replace,
        ] {
            let snapshot = TradeSubjectDiagnosticSnapshot {
                observed_at_ms: 123,
                operation: None,
                intent: None,
                unverified_cancel_candidate: None,
            };
            let value = project(snapshot, subject);
            assert_eq!(value["state"], "NOT_OBSERVED");
            assert!(
                value["next_action"]
                    .as_str()
                    .unwrap()
                    .ends_with("do_not_resend")
            );
        }
    }

    #[tokio::test]
    async fn busy_owner_finance_fifo_has_a_bounded_unknown_diagnostic() {
        let result = super::super::native_intent::bounded_snapshot::<(), _>(
            Instant::now(),
            std::future::pending(),
        )
        .await;
        assert_eq!(result, Err("UNIFIED_FINANCE_DIAGNOSTIC_TIMEOUT".to_owned()));
        assert_eq!(
            super::super::native_intent::READ_BUDGET,
            Duration::from_millis(250)
        );
    }
}
