//! SQL side of venue submission. A recorded state is never a fresh send grant.
use crate::IntentState;
#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
use crate::{AccountStoreHandle, StoreResult};
#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
use std::time::Instant;
/// Ephemeral result of the first successful durable send claim.
/// Deliberately no Clone/Serialize/Deserialize/public constructor. Durable
/// readback, Inspect and restart cannot manufacture this value.
#[must_use = "dropping a committed claim must not be repaired by resending"]
#[derive(Debug)]
pub struct CommittedVenueSend {
    installation: String,
    account: String,
    intent: String,
    plan: String,
}
impl CommittedVenueSend {
    pub(crate) fn after_commit(
        installation: String,
        account: String,
        intent: String,
        plan: String,
    ) -> Self {
        Self {
            installation,
            account,
            intent,
            plan,
        }
    }
    pub fn matches(&self, installation: &str, account: &str, intent: &str, plan: &str) -> bool {
        self.installation == installation
            && self.account == account
            && self.intent == intent
            && self.plan == plan
    }
    /// Consuming self prevents this capability from invoking a second sender.
    pub async fn send<T, F, Fut>(self, send: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        send().await
    }
}
#[derive(Debug, Clone)]
pub struct IntentOutcome {
    pub intent_id: String,
    pub state: IntentState,
    pub exchange_order_id: Option<String>,
    pub evidence: String,
    pub observed_at: i64,
}
#[cfg(any(test, feature = "legacy-account-worker-fixtures"))]
impl AccountStoreHandle {
    /// Legacy account-only CAS. Native risk+account fencing must use its
    /// combined transaction; it must not invoke this claim a second time.
    pub async fn claim_legacy_send(
        &self,
        deadline: Instant,
        intent: String,
        expected_plan: String,
        revision: i64,
    ) -> StoreResult<Option<CommittedVenueSend>> {
        self.call(deadline, move |s| {
            // Deadline/source checks use owner execution time, not enqueue time.
            let observed_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_millis()).ok())
                .ok_or(crate::StoreError::Invalid("POST_CLAIM_CLOCK_UNAVAILABLE"))?;
            let original = s
                .intent(&intent)?
                .ok_or(crate::StoreError::Invalid("INTENT_NOT_FOUND"))?;
            if original.plan_hash != expected_plan {
                return Err(crate::StoreError::Invalid("POST_CLAIM_PLAN_MISMATCH"));
            }
            if !s.begin_post(&intent, revision, observed_at)? {
                return Ok(None);
            }
            Ok(Some(CommittedVenueSend::after_commit(
                s.meta().installation_id.clone(),
                s.meta().account_key.clone(),
                intent,
                original.plan_hash,
            )))
        })
        .await
    }
    pub async fn record_intent_outcome(
        &self,
        deadline: Instant,
        outcome: IntentOutcome,
    ) -> StoreResult<()> {
        self.call(deadline, move |s| {
            s.record_outcome(
                &outcome.intent_id,
                outcome.state,
                outcome.exchange_order_id.as_deref(),
                &outcome.evidence,
                outcome.observed_at,
            )
        })
        .await
    }
}
