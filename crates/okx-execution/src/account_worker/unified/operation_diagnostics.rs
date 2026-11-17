//! Read-only intent, receipt and source facts from one account-scoped snapshot.
use super::UnifiedFinanceHandle;
use crate::{
    AccountStore, CentralReceipt, CentralReceiptView, ProtectionReceipt, SourceCursor, StoreResult,
    StoredIntent, TradeIntentLookup,
};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct NativeRiskStopDiagnostic {
    pub id: String,
    pub scope: String,
    pub reason: String,
    pub latched_at_ms: i64,
}

/// Bounded account facts; inspection never grants mutation or a venue POST.
#[derive(Debug, Clone)]
pub struct NativeAccountDiagnosticSnapshot {
    pub intent: Option<StoredIntent>,
    pub policy: crate::AccountPolicySnapshot,
    pub stop_count: u64,
    pub stops: Vec<NativeRiskStopDiagnostic>,
    pub source_coverage: Vec<(String, String, Option<SourceCursor>)>,
    pub sqlite_version: String,
    pub sqlite_source_id: String,
}

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
    /// One read-only account-scoped transaction on the existing finance FIFO.
    pub async fn account_diagnostic_snapshot(
        &self,
        deadline: Instant,
        account: String,
        reference: Option<String>,
    ) -> StoreResult<NativeAccountDiagnosticSnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx, &installation, &account)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let policy = AccountStore::account_policy_sql(&tx, scope)?;
            let intent = reference
                .map(|id| {
                    AccountStore::lookup_trade_intent_sql(
                        &tx,
                        scope,
                        &TradeIntentLookup::Reference(id),
                    )
                })
                .transpose()?
                .flatten();
            let stop_count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM risk_stops WHERE installation_id=?1 AND account_key=?2 AND resolution IS NULL",
                rusqlite::params![installation, account],
                |row| row.get(0),
            )?;
            let stop_count = u64::try_from(stop_count)
                .map_err(|_| crate::StoreError::Invalid("ACCOUNT_DIAGNOSTIC_STOP_COUNT_INVALID"))?;
            let stops = {
                let mut query = tx.prepare(
                    "SELECT stop_id,scope,reason,latched_at FROM risk_stops
                     WHERE installation_id=?1 AND account_key=?2 AND resolution IS NULL
                     ORDER BY latched_at DESC,stop_id LIMIT 10",
                )?;
                query
                    .query_map(rusqlite::params![installation, account], |row| {
                        Ok(NativeRiskStopDiagnostic {
                            id: row.get(0)?,
                            scope: row.get(1)?,
                            reason: row.get(2)?,
                            latched_at_ms: row.get(3)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut source_coverage = Vec::new();
            for (source, partition) in [
                ("account_snapshot", "account"),
                ("fills", "SPOT"),
                ("fills", "SWAP"),
                ("fills", "FUTURES"),
                ("bills", "account"),
            ] {
                let cursor = AccountStore::scoped_source_cursor_sql(
                    &tx, &installation, &account, source, partition,
                )?;
                source_coverage.push((source.to_owned(), partition.to_owned(), cursor));
            }
            let sqlite_version: String =
                tx.query_row("SELECT sqlite_version()", [], |row| row.get(0))?;
            let sqlite_source_id: String =
                tx.query_row("SELECT sqlite_source_id()", [], |row| row.get(0))?;
            tx.commit()?;
            Ok(NativeAccountDiagnosticSnapshot {
                intent, policy, stop_count, stops, source_coverage,
                sqlite_version, sqlite_source_id,
            })
        })
        .await
    }

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
