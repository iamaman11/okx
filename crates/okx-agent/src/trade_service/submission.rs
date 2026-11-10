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
    if !store
        .call(until, move |s| s.begin_post(&id, revision, now()))
        .await
        .map_err(err)?
    {
        return Ok(None);
    }
    if Instant::now() >= until {
        let id = intent.intent_id.clone();
        store
            .call(deadline(), move |s| {
                s.record_outcome(
                    &id,
                    IntentState::Rejected,
                    None,
                    "monotonic-deadline-expired-owner-confirmed-unsent",
                    now(),
                )
            })
            .await
            .map_err(err)?;
        return Err("COMMAND_EXPIRED".into());
    }
    match send().await {
        Ok(v) => Ok(Some(v)),
        Err(reason)
            if reason == "COMMAND_EXPIRED_BEFORE_SEND"
                || reason == "POLICY_CHANGED_BEFORE_SEND" =>
        {
            let id = intent.intent_id.clone();
            let proof = reason.clone();
            store
                .call(deadline(), move |s| {
                    s.record_outcome(
                        &id,
                        IntentState::Rejected,
                        None,
                        &format!("owner-confirmed-unsent:{proof}"),
                        now(),
                    )
                })
                .await
                .map_err(err)?;
            Err(reason)
        }
        Err(_) => {
            let id = intent.intent_id.clone();
            store
                .call(deadline(), move |s| {
                    s.record_outcome(
                        &id,
                        IntentState::Unknown,
                        None,
                        "exchange-post-outcome-unknown",
                        now(),
                    )
                })
                .await
                .map_err(err)?;
            Ok(None)
        }
    }
}
