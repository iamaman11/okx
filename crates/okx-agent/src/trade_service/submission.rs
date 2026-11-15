//! The single durable claim before a possible venue send.
use super::{AccountStoreHandle, Instant, IntentState, StoredIntent, deadline, err, now};

pub(crate) async fn post_once<T, F, Fut>(
    store: &AccountStoreHandle,
    intent: &StoredIntent,
    until: Instant,
    send: F,
) -> Result<Option<T>, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    if Instant::now() >= until {
        return Err("COMMAND_EXPIRED".into());
    }
    let id = intent.intent_id.clone();
    let revision = intent.policy_revision;
    let Some(claim) = store
        .claim_legacy_send(until, id, intent.plan_hash.clone(), revision)
        .await
        .map_err(err)?
    else {
        return Ok(None);
    };
    if Instant::now() >= until {
        let id = intent.intent_id.clone();
        store
            .record_intent_outcome(
                deadline(),
                okx_execution::IntentOutcome {
                    intent_id: id,
                    state: IntentState::Rejected,
                    exchange_order_id: None,
                    evidence: "monotonic-deadline-expired-owner-confirmed-unsent".into(),
                    observed_at: now(),
                },
            )
            .await
            .map_err(err)?;
        return Err("COMMAND_EXPIRED".into());
    }
    match claim.send(send).await {
        Ok(v) => Ok(Some(v)),
        Err(reason)
            if reason == "COMMAND_EXPIRED_BEFORE_SEND"
                || reason == "POLICY_CHANGED_BEFORE_SEND" =>
        {
            let id = intent.intent_id.clone();
            let proof = reason.clone();
            store
                .record_intent_outcome(
                    deadline(),
                    okx_execution::IntentOutcome {
                        intent_id: id,
                        state: IntentState::Rejected,
                        exchange_order_id: None,
                        evidence: format!("owner-confirmed-unsent:{proof}"),
                        observed_at: now(),
                    },
                )
                .await
                .map_err(err)?;
            Err(reason)
        }
        Err(_) => {
            let id = intent.intent_id.clone();
            store
                .record_intent_outcome(
                    deadline(),
                    okx_execution::IntentOutcome {
                        intent_id: id,
                        state: IntentState::Unknown,
                        exchange_order_id: None,
                        evidence: "exchange-post-outcome-unknown".into(),
                        observed_at: now(),
                    },
                )
                .await
                .map_err(err)?;
            Ok(None)
        }
    }
}
