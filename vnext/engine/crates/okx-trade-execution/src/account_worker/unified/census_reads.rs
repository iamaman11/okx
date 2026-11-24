//! All local census observations share one installation-owner SQL snapshot.
use super::UnifiedFinanceHandle;
use crate::{AccountCensusRequest, AccountCensusSnapshot, AccountStore, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn census_snapshot(
        &self,
        account: String,
        request: AccountCensusRequest,
        deadline: Instant,
    ) -> StoreResult<AccountCensusSnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let now = super::opening_ingress::owner_now_ms()?;
            let result =
                AccountStore::census_snapshot_sql(&tx, &installation, &account, request, now)?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}
