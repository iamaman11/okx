//! Read-only intent, receipt and source facts from one account-scoped snapshot.
use super::UnifiedFinanceHandle;
use crate::{
    AccountStore, CentralReceipt, CentralReceiptView, ProtectionReceipt, SourceCursor, StoreResult,
    StoredIntent, TradeIntentLookup,
};
use std::time::Instant;

pub struct NativeIntentDiagnosticSnapshot {
    pub observed_at_ms: i64,
    pub intent: Option<StoredIntent>,
    pub protection: Option<ProtectionReceipt>,
    pub reservation: Option<CentralReceipt>,
    pub account_source: Option<SourceCursor>,
    pub begin_post_at_ms: Option<i64>,
    pub receipt_state: Option<String>,
    pub unsent: Option<crate::UnsentSqlReceipt>,
}

impl UnifiedFinanceHandle {
    /// Stored facts only. This never reconciles, queries the venue or grants a
    /// send. Absence of a stored source is not proof of a flat venue account.
    pub async fn inspect_intent_operation(
        &self,
        deadline: Instant,
        account: String,
        intent_id: String,
    ) -> StoreResult<NativeIntentDiagnosticSnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx, &installation, &account)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let intent = AccountStore::lookup_trade_intent_sql(
                &tx,
                scope,
                &TradeIntentLookup::IntentId(intent_id.clone()),
            )?;
            use rusqlite::OptionalExtension;
            let begin_post_at_ms = tx.query_row(
                "SELECT begin_post_at FROM intents WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
                rusqlite::params![installation, account, intent_id], |r| r.get::<_, Option<i64>>(0),
            ).optional()?.flatten();
            let receipt_state = tx.query_row(
                "SELECT state FROM risk_receipts WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
                rusqlite::params![installation, account, intent_id], |r| r.get(0),
            ).optional()?;
            let result = NativeIntentDiagnosticSnapshot {
                observed_at_ms: super::opening_ingress::owner_now_ms()?,
                protection: AccountStore::protection_sql(&tx, scope, &intent_id)?,
                reservation: AccountStore::reservation_receipt_sql(
                    &tx,
                    scope,
                    &intent_id,
                    CentralReceiptView::Retained,
                )?,
                account_source: AccountStore::scoped_source_cursor_sql(
                    &tx,
                    &installation,
                    &account,
                    "account_snapshot",
                    "account",
                )?,
                intent,
                begin_post_at_ms,
                receipt_state,
                unsent: AccountStore::unsent_release_receipt_sql(
                    &tx, Some(&installation), &account, &intent_id,
                )?,
            };
            tx.commit()?;
            Ok(result)
        })
        .await
    }
}
