//! Original campaign history/readiness rules on the installation writer.
use super::{UnifiedFinanceHandle, finance_authority::require_preparation_authority};
use crate::{AccountStore, StoreError, StoreResult};
use std::time::Instant;

fn commit_progress(result: &StoreResult<impl Sized>) -> bool {
    result.is_ok()
        || matches!(
            result,
            Err(StoreError::Invalid(
                "CAMPAIGN_DELTA_BATCH_PENDING" | "CAMPAIGN_RECEIPT_BATCH_PENDING"
            ))
        )
}
impl UnifiedFinanceHandle {
    pub async fn verify_campaign_entry_readiness(
        &self,
        deadline: Instant,
        account: String,
        campaign: String,
        from: i64,
        through: i64,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::verify_campaign_entry_readiness_sql(
                &tx,
                Some((&installation, &account)),
                &campaign,
                from,
                through,
            );
            if result.is_ok() {
                tx.commit()?;
            }
            result
        })
        .await
    }
    pub async fn refresh_campaign_entry_readiness(
        &self,
        deadline: Instant,
        account: String,
        campaign: String,
        from: i64,
        through: i64,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_preparation_authority(&tx, &installation)?;
            let result = AccountStore::refresh_campaign_entry_readiness_sql(
                &tx,
                Some((&installation, &account)),
                &campaign,
                from,
                through,
            );
            // Original bounded maintenance persists progress even when a
            // complete receipt needs another turn. This is never READY.
            if commit_progress(&result) {
                tx.commit().map_err(|_| {
                    StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
                })?;
            }
            result
        })
        .await
    }
    pub async fn seal_captured_campaign_baseline(
        &self,
        deadline: Instant,
        account: String,
        campaign: String,
        from: i64,
        through: i64,
    ) -> StoreResult<(rust_decimal::Decimal, String)> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_preparation_authority(&tx, &installation)?;
            let result = AccountStore::seal_captured_campaign_baseline_sql(
                &tx,
                Some((&installation, &account)),
                &campaign,
                from,
                through,
            );
            if commit_progress(&result) {
                tx.commit().map_err(|_| {
                    StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
                })?;
            }
            result
        })
        .await
    }
}
