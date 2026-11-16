//! The single durable claim before a possible venue send.
use super::{Instant, IntentState, StoredIntent, err, now};
#[cfg(test)]
use super::{AccountStoreHandle, deadline};

#[cfg(test)]
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
    send_committed(OutcomeOwner::Legacy(store), intent, claim, until, send).await
}

enum OutcomeOwner<'a> {
    #[cfg(test)]
    Legacy(&'a AccountStoreHandle),
    Native {
        owner: &'a okx_execution::account_worker::UnifiedFinanceHandle,
        account: String,
    },
}
impl OutcomeOwner<'_> {
    async fn record(&self, outcome: okx_execution::IntentOutcome) -> Result<(), String> {
        match self {
            #[cfg(test)]
            Self::Legacy(store) => store
                .record_intent_outcome(deadline(), outcome)
                .await
                .map_err(err),
            Self::Native { owner, account } => owner
                .record_intent_outcome(account.clone(), outcome)
                .await
                .map_err(err),
        }
    }
}
/// Native already owns the durable account claim. Consuming that first-COMMIT
/// value avoids a second begin_post CAS; a read-only POSTING cannot call this.
pub(crate) async fn post_committed_native<T, F, Fut>(
    owner: &okx_execution::account_worker::UnifiedFinanceHandle,
    installation: &str,
    account: &str,
    intent: &StoredIntent,
    claim: okx_execution::CommittedVenueSend,
    until: Instant,
    send: F,
) -> Result<Option<T>, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    if owner.installation_id() != installation
        || !claim.matches(installation, account, &intent.intent_id, &intent.plan_hash)
    {
        return Err("POST_CLAIM_IDENTITY_MISMATCH".into());
    }
    send_committed(
        OutcomeOwner::Native {
            owner,
            account: account.into(),
        },
        intent,
        claim,
        until,
        send,
    )
    .await
}
async fn send_committed<T, F, Fut>(
    owner: OutcomeOwner<'_>,
    intent: &StoredIntent,
    claim: okx_execution::CommittedVenueSend,
    until: Instant,
    send: F,
) -> Result<Option<T>, String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    if Instant::now() >= until {
        let id = intent.intent_id.clone();
        owner
            .record(okx_execution::IntentOutcome {
                intent_id: id,
                state: IntentState::Rejected,
                exchange_order_id: None,
                evidence: "monotonic-deadline-expired-owner-confirmed-unsent".into(),
                observed_at: now(),
            })
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
            owner
                .record(okx_execution::IntentOutcome {
                    intent_id: id,
                    state: IntentState::Rejected,
                    exchange_order_id: None,
                    evidence: format!("owner-confirmed-unsent:{proof}"),
                    observed_at: now(),
                })
                .await
                .map_err(err)?;
            Err(reason)
        }
        Err(_) => {
            let id = intent.intent_id.clone();
            owner
                .record(okx_execution::IntentOutcome {
                    intent_id: id,
                    state: IntentState::Unknown,
                    exchange_order_id: None,
                    evidence: "exchange-post-outcome-unknown".into(),
                    observed_at: now(),
                })
                .await
                .map_err(err)?;
            Ok(None)
        }
    }
}
