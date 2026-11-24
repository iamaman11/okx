//! Existing protection state and incident rules on the single installation owner.
use super::{UnifiedFinanceHandle, require_verified_live_finance_authority};
use crate::{AccountStore, ProtectionReceipt, StoreError, StoreResult};
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn verify_protection(
        &self,
        deadline: Instant,
        account: String,
        receipt: ProtectionReceipt,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            AccountStore::verify_protection_sql(&tx, Some((&installation, &account)), &receipt)?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
    pub async fn protection_incident(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        reason: String,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            AccountStore::protection_incident_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                &reason,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
    pub async fn resolve_protection_incident_after_flat(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        digest: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let result = AccountStore::resolve_protection_incident_after_flat_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                &digest,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn effective_protected_quantity(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        filled: rust_decimal::Decimal,
    ) -> StoreResult<Option<rust_decimal::Decimal>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::effective_protected_quantity_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                filled,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}
