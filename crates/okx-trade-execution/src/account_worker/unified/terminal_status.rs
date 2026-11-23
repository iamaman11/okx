//! Terminal financial release uses original sources and the one financial COMMIT.
use super::{
    ReportedRiskCensus, UnifiedFinanceHandle, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction, require_verified_live_finance_authority,
};
use crate::{AccountStore, StoreError, StoreResult};
use std::time::Instant;

impl UnifiedFinanceHandle {
    pub async fn terminal_financial_snapshot(
        &self,
        deadline: Instant,
        account: String,
    ) -> StoreResult<(crate::SourceCursor, rust_decimal::Decimal)> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx, &installation, &account)?;
            let cursor = AccountStore::scoped_source_cursor_sql(
                &tx,
                &installation,
                &account,
                "bills",
                "account",
            )?
            .ok_or(StoreError::Invalid("BILLS_COVERAGE_MISSING"))?;
            let loss = AccountStore::account_policy_sql(&tx, Some((&installation, &account)))?
                .campaign_loss;
            tx.commit()?;
            Ok((cursor, loss))
        })
        .await
    }
    /// Actual signed-observer census is data, not release authority. The owner
    /// binds it to original NAV/loss/policy rows and rechecks terminal source,
    /// settled debit, lots, CAS and immutable receipts in the SAME transaction.
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
    )]
    pub async fn resolve_terminal_status(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        plan: String,
        reservation: String,
        expected_sequence: u64,
        reported: ReportedRiskCensus,
    ) -> StoreResult<u64> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let campaign =
                AccountStore::account_policy_sql(&tx, Some((&installation, &account)))?.campaign_id;
            let now = super::opening_ingress::owner_now_ms()?;
            let census = super::native_admission::bind_census(
                &tx,
                &installation,
                &campaign,
                &reported,
                now,
            )?;
            let input = crate::risk::TerminalResolveInput {
                account,
                intent,
                reservation,
                plan_sha256: plan,
                generation: census.generation,
                expected_sequence,
                now_ms: now,
                census,
            };
            let receipt = apply_original_unified_finance_transaction(
                &tx,
                &installation,
                UnifiedFinanceWrite::ResolveTerminal { campaign, input },
            )?;
            let UnifiedFinanceWriteReceipt::Risk(
                crate::risk::UnifiedRiskDecision::TerminalReleased { book_sequence, .. },
            ) = receipt
            else {
                return Err(StoreError::Invalid(
                    "NATIVE_TERMINAL_RELEASE_RECEIPT_UNEXPECTED",
                ));
            };
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(book_sequence)
        })
        .await
    }

    /// A lost reply is resolved by an already committed ORIGINAL joint receipt.
    /// This reads durable identity; it cannot create an ACK or refund capacity.
    pub async fn verify_terminal_release_ack(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        plan: String,
        reservation: String,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline),move|db| {
            let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx,&installation,&account)?;
            let campaign=AccountStore::account_policy_sql(&tx,Some((&installation,&account)))?.campaign_id;
            let observed=crate::risk::read_unified_campaign_reservation(&tx,&installation,&campaign,&reservation,&plan)
                .map_err(StoreError::Invalid)?.ok_or(StoreError::Invalid("TERMINAL_RELEASE_ACK_MISSING"))?;
            if observed.state!=okx_domain_core::risk::model::RiskEntryState::Released {return Err(StoreError::Invalid("TERMINAL_RELEASE_ACK_MISSING"));}
            let late=AccountStore::entry_late_pending_sql(&tx,Some((&installation,&account)),&intent)?;
            if late {return Err(StoreError::Invalid("LATE_ENTRY_SETTLEMENT_PENDING"));}
            let accepted:bool=tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM risk_entries e
                 JOIN intents i ON i.installation_id=e.installation_id AND i.account_key=e.account_key AND i.intent_id=e.intent_id
                 JOIN risk_receipts r ON r.installation_id=i.installation_id AND r.account_key=i.account_key AND r.intent_id=i.intent_id
                 JOIN operation_receipts o ON o.installation_id=i.installation_id AND o.account_key=i.account_key AND o.operation_id='central-release:'||e.reservation_key
                 WHERE e.installation_id=?1 AND e.account_key=?2 AND e.intent_id=?3 AND e.plan_sha256=?4 AND e.reservation_key=?5
                   AND e.state='RELEASED' AND e.release_reason='terminal-flat-census' AND e.release_request_hash=i.request_hash
                   AND r.state='RELEASED' AND r.reservation_key=e.reservation_key AND r.plan_hash=i.plan_hash
                   AND o.kind='CENTRAL_RELEASE' AND o.request_hash=e.release_request_hash
                   AND o.result_ref=e.release_proof_digest AND LENGTH(o.result_ref)=64)",
                rusqlite::params![installation,account,intent,plan,reservation],|r|r.get(0))?;
            if !accepted {return Err(StoreError::Invalid("TERMINAL_RELEASE_ACK_MISSING"));}
            tx.commit()?;Ok(())
        }).await
    }
    pub async fn acknowledge_local_terminal_close(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        order: String,
        filled: rust_decimal::Decimal,
        venue_updated_at: i64,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            AccountStore::acknowledge_local_terminal_close_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                &order,
                filled,
                venue_updated_at,
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
