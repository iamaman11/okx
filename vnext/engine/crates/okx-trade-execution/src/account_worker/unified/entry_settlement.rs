//! Original source-backed entry settlement on the installation SQL owner.
use super::{UnifiedFinanceHandle, require_verified_live_finance_authority};
use crate::{
    AccountStore, EntrySettlementEvidence, IntentFeeBillEvidence, IntentFillCostEvidence,
    StoreError, StoreResult,
};
use rust_decimal::Decimal;
use std::time::Instant;

#[derive(Clone, Copy)]
enum SettlementKind {
    Partial,
    Terminal,
    Late,
}

impl UnifiedFinanceHandle {
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
    )]
    async fn bind_entry_source(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        at: i64,
        filled: Decimal,
        through: i64,
        kind: SettlementKind,
    ) -> StoreResult<EntrySettlementEvidence> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let result = match kind {
                SettlementKind::Partial => AccountStore::bind_partial_entry_settlement_sql(
                    &tx, scope, &intent, at, filled, through,
                )?,
                SettlementKind::Terminal => AccountStore::bind_terminal_entry_settlement_sql(
                    &tx, scope, &intent, at, filled, through,
                )?,
                SettlementKind::Late => AccountStore::bind_late_entry_settlement_sql(
                    &tx, scope, &intent, at, filled, through,
                )?,
            };
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(result)
        })
        .await
    }
    pub async fn bind_partial_entry_settlement(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        venue_at: i64,
        filled: Decimal,
        covered: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        self.bind_entry_source(
            deadline,
            account,
            intent,
            venue_at,
            filled,
            covered,
            SettlementKind::Partial,
        )
        .await
    }
    pub async fn bind_terminal_entry_settlement(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        venue_at: i64,
        filled: Decimal,
        covered: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        self.bind_entry_source(
            deadline,
            account,
            intent,
            venue_at,
            filled,
            covered,
            SettlementKind::Terminal,
        )
        .await
    }
    pub async fn bind_late_entry_settlement(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        venue_at: i64,
        filled: Decimal,
        covered: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        self.bind_entry_source(
            deadline,
            account,
            intent,
            venue_at,
            filled,
            covered,
            SettlementKind::Late,
        )
        .await
    }
    pub async fn bind_zero_fill_terminal_entry_settlement(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        venue_at: i64,
        covered: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        self.bind_entry_source(
            deadline,
            account,
            intent,
            venue_at,
            Decimal::ZERO,
            covered,
            SettlementKind::Terminal,
        )
        .await
    }
    pub async fn intent_fill_cost_evidence(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        through: i64,
    ) -> StoreResult<IntentFillCostEvidence> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::intent_fill_cost_evidence_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                through,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn intent_fee_bill_evidence(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        through: i64,
    ) -> StoreResult<IntentFeeBillEvidence> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::intent_fee_bill_evidence_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                through,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn partial_entry_ack_pending(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::partial_entry_ack_pending_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn entry_late_pending(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::entry_late_pending_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    /// Native SETTLE_ENTRY already commits this acknowledgement with RiskBook.
    /// Shared orchestration may verify it, but can never mint another ACK.
    pub async fn acknowledge_partial_entry_settlement(
        &self,
        deadline: Instant,
        account: String,
        accepted: EntrySettlementEvidence,
    ) -> StoreResult<()> {
        self.verify_entry_ack(deadline, account, accepted, false)
            .await
    }
    pub async fn acknowledge_late_entry_settlement(
        &self,
        deadline: Instant,
        account: String,
        accepted: EntrySettlementEvidence,
    ) -> StoreResult<()> {
        self.verify_entry_ack(deadline, account, accepted, true)
            .await
    }
    async fn verify_entry_ack(
        &self,
        deadline: Instant,
        account: String,
        accepted: EntrySettlementEvidence,
        late: bool,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::verify_native_entry_ack_sql(
                &tx,
                Some((&installation, &account)),
                &accepted,
                late,
            );
            if result.is_ok() {
                tx.commit()?;
            }
            result
        })
        .await
    }
}
