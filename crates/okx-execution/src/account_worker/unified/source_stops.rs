//! Source evidence resolves a scoped existing stop; never activates authority.
use super::UnifiedFinanceHandle;
use crate::{AccountStore, StoreError, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn resolve_source_stop(
        &self,
        deadline: Instant,
        account: String,
        reason: String,
        evidence: String,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::source_permission::require_source_account(&tx, &installation, Some(&account))?;
            AccountStore::resolve_source_stop_sql(
                &tx,
                Some((&installation, &account)),
                &reason,
                &evidence,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
}
