//! Typed account authority read models on the sole installation SQL snapshot.
use super::UnifiedFinanceHandle;
use crate::{AccountPolicySnapshot, AccountStore, CentralReceipt, CentralReceiptView, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn account_policy(
        &self,
        account: String,
        deadline: Instant,
    ) -> StoreResult<AccountPolicySnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::account_policy_sql(&tx, Some((&installation, &account)))?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn reservation_receipt(
        &self,
        account: String,
        intent: String,
        view: CentralReceiptView,
        deadline: Instant,
    ) -> StoreResult<Option<CentralReceipt>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::reservation_receipt_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                view,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}
