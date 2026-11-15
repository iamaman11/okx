//! Scoped restoration audit reads and final compare/commit on the sole writer.
use super::UnifiedFinanceHandle;
use crate::{AccountStore, RecoverySnapshot, StoreError, StoreResult};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::{str::FromStr, time::Instant};

/// Observed audit results are data. Every local monetary fact is rechecked in SQL.
pub struct NativeRecoveryProof {
    pub generation: i64,
    pub observed_at_ms: i64,
    pub expected_receipts_digest: String,
    pub active_reservations: u64,
    pub occupied_loss_usd: String,
    pub cumulative_loss_usd: String,
    pub exchange_evidence: String,
    pub financial_evidence: String,
}
impl UnifiedFinanceHandle {
    pub async fn recovery_position_owner(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<(
        Option<crate::ProtectionReceipt>,
        Option<crate::StoredIntent>,
    )> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let result = (
                AccountStore::protection_sql(&tx, scope, &intent)?,
                AccountStore::lookup_trade_intent_sql(
                    &tx,
                    scope,
                    &crate::TradeIntentLookup::IntentId(intent),
                )?,
            );
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn recovery_snapshot(
        &self,
        deadline: Instant,
        account: String,
    ) -> StoreResult<RecoverySnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::recovery_snapshot_sql(
                &tx,
                Some((&installation, &account)),
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn commit_storage_recovery(
        &self,
        deadline: Instant,
        account: String,
        proof: NativeRecoveryProof,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            super::source_permission::require_source_account(&tx, &installation, Some(&account))?;
            let now = super::opening_ingress::owner_now_ms()?;
            let invalid = || StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE");
            if proof.observed_at_ms <= 0
                || proof.observed_at_ms > now
                || now - proof.observed_at_ms > 5000
            {
                return Err(StoreError::Invalid("STORAGE_RECOVERY_SOURCE_STALE"));
            }
            let cursor = AccountStore::scoped_source_cursor_sql(
                &tx,
                &installation,
                &account,
                "account_snapshot",
                "account",
            )?;
            if cursor.is_none_or(|c| c.generation != proof.generation) {
                return Err(StoreError::Invalid("STORAGE_RECOVERY_GENERATION_CHANGED"));
            }
            let current =
                AccountStore::recovery_snapshot_sql(&tx, Some((&installation, &account)), now)?;
            let campaign =
                AccountStore::account_policy_sql(&tx, Some((&installation, &account)))?.campaign_id;
            let budget = crate::risk::read_unified_campaign_account_summary(
                &tx,
                &installation,
                &campaign,
                &account,
            )
            .map_err(StoreError::Invalid)?;
            let occupied = Decimal::from_str(&proof.occupied_loss_usd).map_err(|_| invalid())?;
            let loss = Decimal::from_str(&proof.cumulative_loss_usd).map_err(|_| invalid())?;
            let primary_occupied =
                Decimal::from_str(&budget.reserved_loss_usd).map_err(|_| invalid())?;
            let primary_loss =
                Decimal::from_str(&budget.cumulative_loss_usd).map_err(|_| invalid())?;
            let digest = format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&current.active_receipts).map_err(|_| invalid())?
                )
            );
            for receipt in &current.active_receipts {
                let reservation = crate::risk::read_unified_campaign_reservation(
                    &tx,
                    &installation,
                    &campaign,
                    &receipt.reservation_key,
                    &receipt.plan_hash,
                )
                .map_err(StoreError::Invalid)?
                .ok_or_else(invalid)?;
                if receipt.account_key != account
                    || receipt.installation_id != installation
                    || reservation.state != okx_kernel::risk::model::RiskEntryState::Open
                {
                    return Err(StoreError::Invalid(
                        "STORAGE_RECOVERY_RESERVATION_UNRESOLVED",
                    ));
                }
            }
            if !current.coverage_complete
                || current.unresolved_intents
                || budget.active_reservations != proof.active_reservations
                || primary_occupied != occupied
                || primary_loss != loss
                || proof.active_reservations != current.active_receipts.len() as u64
                || occupied
                    != current
                        .active_receipts
                        .iter()
                        .map(|r| r.requested_loss_usd)
                        .sum::<Decimal>()
                || loss > current.cumulative_loss_usd
                || digest != proof.expected_receipts_digest
            {
                return Err(invalid());
            }
            AccountStore::mark_storage_reconciled_sql(
                &tx,
                Some((&installation, &account)),
                proof.observed_at_ms,
                &proof.exchange_evidence,
                &proof.financial_evidence,
            )?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
}
