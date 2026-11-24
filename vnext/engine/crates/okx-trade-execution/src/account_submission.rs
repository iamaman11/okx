//! SQL side of venue submission. A recorded state is never a fresh send grant.
use crate::IntentState;
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
