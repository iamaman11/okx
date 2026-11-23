//! Original immutable reserve source in one scoped read snapshot.
use super::UnifiedFinanceHandle;
use crate::{AccountReserveSource, AccountStore, StoreResult};
impl UnifiedFinanceHandle {
    pub async fn reserve_source(
        &self,
        account: String,
        intent: String,
        deadline: std::time::Instant,
    ) -> StoreResult<AccountReserveSource> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let source =
                AccountStore::reserve_source_sql(&tx, Some((&installation, &account)), &intent)?;
            tx.commit()?;
            Ok(source)
        })
        .await
    }
}
