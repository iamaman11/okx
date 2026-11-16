//! Durable protection-operation ingress. A readback never authorizes another POST.
use super::{UnifiedFinanceHandle, require_verified_live_finance_authority};
use crate::{AccountStore, OperationReceipt, ProtectionReceipt, StoreError, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn recent_cleanup_diagnostics(
        &self,
        deadline: Instant,
        account: String,
        limit: usize,
    ) -> StoreResult<Vec<crate::ProtectionCleanupOperationReceipt>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            AccountStore::recent_cleanup_diagnostics_sql(db, Some((&installation, &account)), limit)
        })
        .await
    }
    pub async fn operation(
        &self,
        deadline: Instant,
        account: String,
        id: String,
    ) -> StoreResult<Option<OperationReceipt>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            AccountStore::operation_sql(db, Some((&installation, &account)), &id)
        })
        .await
    }
    pub async fn protection(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<Option<ProtectionReceipt>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            AccountStore::protection_sql(db, Some((&installation, &account)), &intent)
        })
        .await
    }
    pub async fn begin_operation(
        &self,
        deadline: Instant,
        account: String,
        id: String,
        hash: String,
        kind: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let result = AccountStore::begin_operation_sql(
                &tx,
                Some((&installation, &account)),
                &id,
                &hash,
                &kind,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn operation_receipt(
        &self,
        deadline: Instant,
        account: String,
        id: String,
        hash: String,
        kind: String,
        result: String,
    ) -> StoreResult<OperationReceipt> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let receipt = AccountStore::operation_receipt_sql(
                &tx,
                Some((&installation, &account)),
                &id,
                &hash,
                &kind,
                &result,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(receipt)
        })
        .await
    }
}
