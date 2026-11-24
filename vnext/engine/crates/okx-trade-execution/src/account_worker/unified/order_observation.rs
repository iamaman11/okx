//! Authenticated observer output and original early status reads on one SQL owner.
use super::*;
use crate::{
    AccountStore, EntryAdmissionEvidence, EntryQuoteEvidence, IntentState, StoreError, StoreResult,
    UnsentSqlReceipt,
};
use rust_decimal::Decimal;
use std::time::Instant;
impl UnifiedFinanceHandle {
    pub async fn record_order_readback(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        order_id: String,
        state: IntentState,
        cumulative_fill: Decimal,
    ) -> StoreResult<()> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            AccountStore::record_order_readback_sql(
                &tx,
                Some((&installation, &account)),
                &intent,
                &order_id,
                state,
                cumulative_fill,
                super::opening_ingress::owner_now_ms()?,
            )?;
            crate::risk::reconcile_outcome_unified_native(&tx, &installation, &account, &intent)
                .map_err(StoreError::Invalid)?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(())
        })
        .await
    }
    pub async fn entry_cost_evidence(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<(Option<EntryQuoteEvidence>, Option<EntryAdmissionEvidence>)> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let result = (
                AccountStore::entry_quote_sql(&tx, scope, &intent)?,
                AccountStore::entry_admission_sql(&tx, scope, &intent)?,
            );
            tx.commit()?;
            Ok(result)
        })
        .await
    }
    pub async fn declined_reserve(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline),move |db| {
            let tx=db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx,&installation,&account)?;
            let result=tx.query_row("SELECT EXISTS(SELECT 1 FROM intents i JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id AND o.installation_id=i.installation_id AND o.account_key=i.account_key JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id AND t.installation_id=i.installation_id AND t.account_key=i.account_key WHERE i.intent_id=?1 AND i.installation_id=?2 AND i.account_key=?3 AND i.state='REJECTED' AND i.begin_post_at IS NULL AND o.kind='RISK_RESERVE_DENIED' AND o.request_hash=i.request_hash AND t.kind='REJECTED_UNSENT' AND t.request_hash=i.request_hash)",rusqlite::params![intent,installation,account],|r|r.get(0))?;
            tx.commit()?;Ok(result)
        }).await
    }
    pub async fn unsent_release_receipt(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
    ) -> StoreResult<Option<UnsentSqlReceipt>> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            let result = AccountStore::unsent_release_receipt_sql(
                &tx,
                Some(&installation),
                &account,
                &intent,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}
