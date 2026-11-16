//! Stop resizing uses original billed-reduction proof and a coherent owner snapshot.
use super::{UnifiedFinanceHandle, require_verified_live_finance_authority};
use crate::{
    AccountStore, OperationReceipt, ProtectionReceipt, StoreError, StoreResult, StoredIntent,
};
use rust_decimal::Decimal;
use std::time::Instant;

impl UnifiedFinanceHandle {
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
    )]
    pub async fn protection_resize_source(
        &self,
        deadline: Instant,
        account: String,
        reduction: String,
        order: String,
        filled: Decimal,
        venue_at: i64,
        position_qty: Decimal,
    ) -> StoreResult<(
        StoredIntent,
        ProtectionReceipt,
        String,
        Option<OperationReceipt>,
        Option<OperationReceipt>,
    )> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::protection_resize_source_sql(
                &tx,
                Some((&installation, &account)),
                &reduction,
                &order,
                filled,
                venue_at,
                position_qty,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn confirm_protection_resize(
        &self,
        deadline: Instant,
        account: String,
        new_protection: ProtectionReceipt,
        verification: String,
        hash: String,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            AccountStore::verify_protection_sql(
                &tx,
                Some((&installation, &account)),
                &new_protection,
            )?;
            AccountStore::operation_receipt_sql(
                &tx,
                Some((&installation, &account)),
                &verification,
                &hash,
                "protection_shrink_verified",
                "VERIFIED",
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
}
