//! SQL side of venue submission. A recorded state is never a fresh send grant.
use crate::{AccountStoreHandle, IntentState, StoreResult};
use std::time::Instant;
#[derive(Debug, Clone)]
pub struct IntentOutcome {
    pub intent_id: String,
    pub state: IntentState,
    pub exchange_order_id: Option<String>,
    pub evidence: String,
    pub observed_at: i64,
}
impl AccountStoreHandle {
    /// Legacy account-only CAS. Native risk+account fencing must use its
    /// combined transaction; it must not invoke this claim a second time.
    pub async fn claim_legacy_post(
        &self,
        deadline: Instant,
        intent: String,
        revision: i64,
    ) -> StoreResult<bool> {
        self.call(deadline, move |s| {
            // Deadline/source checks use owner execution time, not enqueue time.
            let observed_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_millis()).ok())
                .ok_or(crate::StoreError::Invalid("POST_CLAIM_CLOCK_UNAVAILABLE"))?;
            s.begin_post(&intent, revision, observed_at)
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
