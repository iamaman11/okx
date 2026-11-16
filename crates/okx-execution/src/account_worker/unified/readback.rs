//! Bounded committed-state queries through the same finance FIFO; never grants mutation authority.
use super::{
    UnifiedFinanceAccountPolicyReadback, UnifiedFinanceAccountReadback,
    UnifiedFinanceCampaignSummary, UnifiedFinanceHandle, UnifiedFinanceInventory,
    UnifiedFinanceReservationReadback,
};
use crate::{AccountStore, StoreError, StoreResult};
impl UnifiedFinanceHandle {
    /// One bounded, account-scoped read of durable protection and terminal
    /// traces from the SAME finance FIFO. No secondary writer/Connection, no POST.
    pub async fn durable_operation_diagnostics(
        &self,
        deadline: std::time::Instant,
        account: String,
        cleanup_limit: usize,
        terminal_limit: usize,
    ) -> StoreResult<(
        Vec<crate::ProtectionCleanupOperationReceipt>,
        Vec<crate::TerminalReleaseDiagnosticReceipt>,
    )> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let scope = (&installation[..], &account[..]);
            let cleanup = AccountStore::recent_cleanup_diagnostics_sql(
                &tx,
                Some(scope),
                cleanup_limit,
            )?;
            let terminal = AccountStore::recent_terminal_release_diagnostics_sql(
                &tx,
                scope,
                terminal_limit,
            )?;
            tx.commit()?;
            Ok((cleanup, terminal))
        })
        .await
    }

    pub async fn finance_report(
        &self,
        account: String,
        query: crate::FinanceReportQuery,
        deadline: std::time::Instant,
    ) -> StoreResult<crate::FinanceReportSnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let snapshot =
                AccountStore::finance_report_sql(&tx, Some((&installation, &account)), &query)?;
            tx.commit()?;
            Ok(snapshot)
        })
        .await
    }

    /// Committed account-backed principal; never a reserve or venue-send grant.
    pub async fn capital_state(
        &self,
        account: String,
        now: i64,
        deadline: std::time::Instant,
    ) -> StoreResult<crate::AccountCapitalState> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let latest = AccountStore::latest_capital_assignment_time_sql(&tx, scope)?;
            let state = AccountStore::capital_state_sql(&tx, scope, now, latest)?;
            tx.commit()?;
            Ok(state)
        })
        .await
    }

    /// Scoped source identity read. The result is not an ownership assertion
    /// for a later mutation; the page/operation still validates its sources.
    pub async fn source_order_owner(
        &self,
        account: String,
        client: String,
        venue: String,
        deadline: std::time::Instant,
    ) -> StoreResult<Option<crate::StoredIntent>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let owner = AccountStore::source_order_owner_sql(
                &tx,
                Some((&installation, &account)),
                &client,
                &venue,
            )?;
            tx.commit()?;
            Ok(owner)
        })
        .await
    }

    /// Same collector frontier in one read transaction on the sole FIFO.
    pub async fn source_capture_state(
        &self,
        account: String,
        deadline: std::time::Instant,
    ) -> StoreResult<crate::AccountSourceState> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let state =
                AccountStore::source_capture_state_sql(&tx, Some((&installation, &account)))?;
            tx.commit()?;
            Ok(state)
        })
        .await
    }

    /// Resume the original observer generation from this account only.
    /// Reading a missing account is an error, never a fresh generation zero.
    pub async fn source_generation_base(&self, account: String) -> StoreResult<u64> {
        let installation = self.installation.clone();
        self.submit(move |db| {
            AccountStore::source_generation_base_sql(db, Some((&installation, &account)))
        })
        .await
    }

    /// Original durable cursor through the sole finance FIFO. No SQL or
    /// account-local writable connection escapes to the source observer.
    pub async fn source_cursor(
        &self,
        account: String,
        source: String,
        partition: String,
    ) -> StoreResult<Option<crate::SourceCursor>> {
        let installation = self.installation.clone();
        self.submit(move |db| {
            AccountStore::scoped_source_cursor_sql(db, &installation, &account, &source, &partition)
        })
        .await
    }

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
            let opening = crate::risk::read_unified_campaign_opening(&tx, &installation, &campaign)
                .map_err(|_| StoreError::Invalid("NATIVE_OPENING_SOURCE_UNAVAILABLE"))?
                .map(|source| super::UnifiedFinanceOpeningReadback {
                    source_digest: source.source_digest,
                    as_of_ms: source.as_of_ms,
                    campaign_started_at_ms: source.campaign_started_at_ms,
                });
            use rusqlite::OptionalExtension;
            let created: Option<i64> = tx
                .query_row(
                    "SELECT MIN(committed_at) FROM operation_receipts WHERE installation_id=?1
                 AND operation_id=?2 AND kind='CAMPAIGN_CREATED'",
                    rusqlite::params![installation, format!("campaign-created:{campaign}")],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let campaign_started_at_ms =
                opening
                    .as_ref()
                    .map(|o| o.campaign_started_at_ms)
                    .or(created
                        .and_then(|t| u64::try_from(t).ok())
                        .filter(|t| *t > 0));
            tx.commit()?;
            Ok(UnifiedFinanceCampaignSummary {
                campaign_started_at_ms,
                opening,
                campaign,
                book_sequence: original.book_sequence,
                policy_revision: original.policy_revision,
                global_reserved_usdt: original.global_reserved_usdt,
                loss_limit_usd: original.loss_limit_usd,
                consumed_loss_usd: original.consumed_loss_usd,
                reserved_loss_usd: original.reserved_loss_usd,
                remaining_loss_usd: original.remaining_loss_usd,
                entry_limit_usd: original.entry_limit_usd,
                consumed_entry_usd: original.consumed_entry_usd,
                reserved_entry_usd: original.reserved_entry_usd,
                entry_bound_breach_usd: original.entry_bound_breach_usd,
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
    #[cfg(test)]
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
