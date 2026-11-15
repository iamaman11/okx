//! Bounded committed-state queries through the same finance FIFO; never grants mutation authority.
use super::{
    UnifiedFinanceAccountPolicyReadback, UnifiedFinanceAccountReadback,
    UnifiedFinanceCampaignSummary, UnifiedFinanceHandle, UnifiedFinanceInventory,
    UnifiedFinanceReservationReadback,
};
use crate::{AccountStore, StoreError, StoreResult};
impl UnifiedFinanceHandle {
    pub async fn inventory(&self) -> StoreResult<UnifiedFinanceInventory> {
        let installation = self.installation.clone();
        self.submit(move |db| {
            let accounts = db.query_row(
                "SELECT COUNT(*) FROM account_meta WHERE installation_id=?1",
                [&installation],
                |row| row.get(0),
            )?;
            let campaigns = db.query_row(
                "SELECT COUNT(*) FROM risk_meta WHERE installation_id=?1",
                [&installation],
                |row| row.get(0),
            )?;
            let state = db.query_row(
                "SELECT state FROM finance_import_manifest
                 WHERE singleton=1 AND installation_id=?1",
                [&installation],
                |row| row.get(0),
            )?;
            Ok(UnifiedFinanceInventory {
                accounts,
                campaigns,
                state,
            })
        })
        .await
    }

    /// The existing original RiskBook Summary algorithm, executed through
    /// the installation's sole SQL owner and exact campaign identity scope.
    /// It is read-only; a Summary cannot issue a venue-send capability.
    pub async fn campaign_summary(
        &self,
        campaign: String,
    ) -> StoreResult<UnifiedFinanceCampaignSummary> {
        if campaign.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_CAMPAIGN_EMPTY"));
        }
        let installation = self.installation.clone();
        self.submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let original =
                crate::risk::read_unified_campaign_summary(&tx, &installation, &campaign)
                    .map_err(StoreError::Invalid)?;
            tx.commit()?;
            Ok(UnifiedFinanceCampaignSummary {
                campaign,
                book_sequence: original.book_sequence,
                policy_revision: original.policy_revision,
                global_reserved_usdt: original.global_reserved_usdt,
                loss_limit_usd: original.loss_limit_usd,
                consumed_loss_usd: original.consumed_loss_usd,
                reserved_loss_usd: original.reserved_loss_usd,
                remaining_loss_usd: original.remaining_loss_usd,
                remaining_entry_usd: original.remaining_entry_usd,
            })
        })
        .await
    }

    /// The original RiskBook AccountSummary from the same installation
    /// FIFO/SQLite snapshot, with full campaign+account scope verification.
    pub(crate) async fn account_summary(
        &self,
        campaign: String,
        account_key: String,
    ) -> StoreResult<crate::risk::AccountReadback> {
        if campaign.is_empty() || account_key.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_ACCOUNT_SCOPE_INVALID"));
        }
        let installation = self.installation.clone();
        self.submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let readback = crate::risk::read_unified_campaign_account_summary(
                &tx,
                &installation,
                &campaign,
                &account_key,
            )
            .map_err(StoreError::Invalid)?;
            tx.commit()?;
            Ok(readback)
        })
        .await
    }

    /// Safe external projection of the original RiskBook account SQL read.
    /// Only scoped readback data crosses crate boundaries: no SQL closure,
    /// source-attestation constructor, mutation handle or exchange capability.
    pub async fn stopped_account_summary(
        &self,
        campaign: String,
        account_key: String,
    ) -> StoreResult<UnifiedFinanceAccountReadback> {
        let original = self.account_summary(campaign, account_key).await?;
        Ok(UnifiedFinanceAccountReadback {
            account_key: original.account_key,
            policy_revision: original.policy_revision,
            book_sequence: original.book_sequence,
            active_reservations: original.active_reservations,
            reserved_loss_usdt: original.reserved_loss_usdt,
            reserved_loss_usd: original.reserved_loss_usd,
            cumulative_loss_usd: original.cumulative_loss_usd,
            latest_activity_ms: original.latest_activity_ms,
        })
    }

    /// Missing or foreign reservation is None (UNKNOWN), never an
    /// authorization to send. Original RiskBook source is queried unchanged.
    pub async fn stopped_inspect_reservation(
        &self,
        campaign: String,
        reservation_key: String,
        plan_sha256: String,
    ) -> StoreResult<Option<UnifiedFinanceReservationReadback>> {
        let found = self
            .inspect_reservation(campaign, reservation_key, plan_sha256)
            .await?;
        Ok(found.map(|original| {
            use okx_kernel::risk::model::RiskEntryState;
            let state = match original.state {
                RiskEntryState::Held => "HELD",
                RiskEntryState::Posted => "POSTED",
                RiskEntryState::Unknown => "UNKNOWN",
                RiskEntryState::Open => "OPEN",
                RiskEntryState::Released => "RELEASED",
            };
            UnifiedFinanceReservationReadback {
                reservation_key: original.reservation_key,
                plan_sha256: original.plan_sha256,
                state,
                reservation_sequence: original.reservation_sequence,
                current_book_sequence: original.current_book_sequence,
                policy_revision: original.policy_revision,
            }
        }))
    }

    /// Original scoped account policy, read via the installation SQL FIFO.
    /// Intended only to compare authenticated governance against a sealed
    /// import; no read of an account may create a mutation permit.
    pub async fn stopped_account_policy(
        &self,
        campaign: String,
        account: String,
    ) -> StoreResult<UnifiedFinanceAccountPolicyReadback> {
        if campaign.is_empty() || account.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_STOPPED_POLICY_SCOPE_INVALID"));
        }
        let installation = self.installation.clone();
        self.submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let row: (i64, i64, String, String) = tx.query_row(
                "SELECT revision,paused,campaign_id,campaign_limit
                 FROM account_policy WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
                rusqlite::params![installation, &account],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            tx.commit()?;
            if row.2 != campaign || !(0..=1).contains(&row.1) {
                return Err(StoreError::Invalid(
                    "UNIFIED_STOPPED_POLICY_IDENTITY_MISMATCH",
                ));
            }
            Ok(UnifiedFinanceAccountPolicyReadback {
                account_key: account,
                revision: u64::try_from(row.0)
                    .map_err(|_| StoreError::Invalid("UNIFIED_STOPPED_POLICY_REVISION_INVALID"))?,
                paused: row.1 == 1,
                campaign_limit_usd: row.3,
            })
        })
        .await
    }

    /// Read-only original AccountStore intent (and immutable full plan)
    /// via the installation's single SQL FIFO. Raw venue IDs are scoped by
    /// account; no second account store, mutable SQL or venue capability.
    pub(crate) async fn account_intent(
        &self,
        account: String,
        intent: String,
    ) -> StoreResult<Option<crate::StoredIntent>> {
        if account.is_empty() || intent.is_empty() {
            return Err(StoreError::Invalid("UNIFIED_FINANCE_INTENT_SCOPE_INVALID"));
        }
        let installation = self.installation.clone();
        self.submit(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let found = AccountStore::find_intent_sql(
                &tx,
                Some((&installation, &account)),
                "intent_id",
                &intent,
            )?;
            tx.commit()?;
            Ok(found)
        })
        .await
    }
}
