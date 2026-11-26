//! Read-only intent, receipt and source facts from one account-scoped snapshot.
use super::UnifiedFinanceHandle;
use crate::{
    AccountStore, CentralReceipt, CentralReceiptView, ProtectionReceipt, SourceCursor, StoreResult,
    StoredIntent, TradeDiagnosticSubject, TradeIntentLookup, TradeSubjectDiagnosticSnapshot,
};
use rusqlite::OptionalExtension;
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
    pub observed_at_ms: i64,
    pub financial: crate::FinancialRisk,
    pub nav_coverage: Option<String>,
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
    pub budget_denial: Option<NativeBudgetDenial>,
    pub entry_quote: Option<crate::EntryQuoteEvidence>,
    pub entry_admission: Option<crate::EntryAdmissionEvidence>,
    pub entry_source_failure: Option<NativeEntrySourceFailure>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEntrySourceFailure {
    pub installation_id: String,
    pub account_key: String,
    pub intent_id: String,
    pub plan_hash: String,
    pub phase: String,
    pub reason: String,
    pub source_kind: NativeEntrySourceKind,
    pub source_version: String,
    pub reference_version: String,
    pub source_at_ms: i64,
    pub checked_at_ms: i64,
    pub max_age_ms: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeEntrySourceKind {
    FeeExchange,
    FeeReceipt,
}

fn decode_entry_source_failure(
    receipt: &crate::OperationReceipt,
    installation: &str,
    account: &str,
    intent: &str,
    plan: &str,
) -> StoreResult<NativeEntrySourceFailure> {
    let invalid = || crate::StoreError::Invalid("ENTRY_SOURCE_FAILURE_INVALID");
    if receipt.kind != "ENTRY_SOURCE_FAILURE"
        || receipt.request_hash != plan
        || receipt.result_ref.len() > 4096
    {
        return Err(invalid());
    }
    let fact: NativeEntrySourceFailure =
        serde_json::from_str(&receipt.result_ref).map_err(|_| invalid())?;
    if fact.installation_id != installation
        || fact.account_key != account
        || fact.intent_id != intent
        || fact.plan_hash != plan
        || fact.phase != "POST_HELD_SOURCE_CHECK"
        || fact.max_age_ms != 5000
        || !matches!(
            (fact.reason.as_str(), &fact.source_kind),
            (
                "ENTRY_FEE_EXCHANGE_TIME_STALE",
                NativeEntrySourceKind::FeeExchange
            ) | ("ENTRY_FEE_SOURCE_STALE", NativeEntrySourceKind::FeeReceipt)
        )
        || fact.source_version.is_empty()
        || fact.source_version.len() > 256
        || fact.reference_version.is_empty()
        || fact.reference_version.len() > 256
        || fact.source_at_ms <= 0
        || fact.checked_at_ms > receipt.committed_at
        || fact
            .checked_at_ms
            .checked_sub(fact.source_at_ms)
            .is_none_or(|age| age <= fact.max_age_ms)
    {
        return Err(invalid());
    }
    Ok(fact)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBudgetDenial {
    pub installation_id: String,
    pub account_key: String,
    pub campaign_id: String,
    pub intent_id: String,
    pub plan_hash: String,
    pub policy_revision: u64,
    pub book_sequence: u64,
    pub reason: String,
    pub requested_entry_usd: String,
    pub requested_loss_usd: String,
    pub remaining_entry_usd: String,
    pub remaining_loss_usd: String,
    pub census_source_digest: String,
    pub census_as_of_ms: i64,
    pub census_read_floor_ms: i64,
    pub observed_at_ms: i64,
}

pub(super) fn decode_budget_denial(
    receipt: &crate::OperationReceipt,
    installation: &str,
    account: &str,
    intent: &str,
    plan: &str,
) -> StoreResult<NativeBudgetDenial> {
    let invalid = || crate::StoreError::Invalid("NATIVE_DENIAL_EVIDENCE_INVALID");
    if receipt.kind != "RESERVE_DENIED"
        || receipt.request_hash != plan
        || receipt.result_ref.len() > 4096
    {
        return Err(invalid());
    }
    let fact: NativeBudgetDenial =
        serde_json::from_str(&receipt.result_ref).map_err(|_| invalid())?;
    if fact.installation_id != installation
        || fact.account_key != account
        || fact.intent_id != intent
        || fact.plan_hash != plan
        || fact.observed_at_ms != receipt.committed_at
    {
        return Err(invalid());
    }
    Ok(fact)
}

impl UnifiedFinanceHandle {
    /// A single read-only transaction on the existing account SQL FIFO. In
    /// particular no absence of a receipt can prove an exchange mutation did
    /// not happen, and no candidate replacement relation can authorize POST.
    pub async fn inspect_trade_subject(
        &self,
        deadline: Instant,
        account: String,
        subject: TradeDiagnosticSubject,
        key: String,
    ) -> StoreResult<TradeSubjectDiagnosticSnapshot> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx, &installation, &account)?;
            let result = AccountStore::trade_subject_diagnostic_sql(
                &tx,
                Some((&installation, &account)),
                subject,
                &key,
                super::opening_ingress::owner_now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
    }

    /// Resolve only a durable PREPARE correlation committed by this account's
    /// SQL owner. A miss cannot establish that a request never had an effect.
    pub async fn resolve_request_intent(
        &self,
        deadline: Instant,
        account: String,
        request_id: String,
    ) -> StoreResult<Option<String>> {
        if !super::admission_lifecycle::valid_edge_request_reference(&request_id) {
            return Err(crate::StoreError::Invalid("INVALID_EDGE_REQUEST_ORIGIN"));
        }
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)?;
            AccountStore::verify_source_account_sql(&tx, &installation, &account)?;
            let scope = Some((installation.as_str(), account.as_str()));
            let receipt =
                AccountStore::operation_sql(&tx, scope, &format!("edge-request:{request_id}"))?;
            let result = match receipt {
                None => None,
                Some(receipt) => {
                    if receipt.kind != "EDGE_TRADE_PREPARE_ORIGIN" {
                        return Err(crate::StoreError::Invalid("EDGE_REQUEST_ORIGIN_INVALID"));
                    }
                    let intent = AccountStore::find_intent_sql(
                        &tx,
                        scope,
                        "intent_id",
                        &receipt.result_ref,
                    )?
                    .ok_or(crate::StoreError::Invalid("EDGE_REQUEST_INTENT_MISSING"))?;
                    if intent.request_hash != receipt.request_hash {
                        return Err(crate::StoreError::Invalid("EDGE_REQUEST_INTENT_MISMATCH"));
                    }
                    Some(intent.intent_id)
                }
            };
            tx.commit()?;
            Ok(result)
        })
        .await
    }
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
            let observed_at_ms = super::opening_ingress::owner_now_ms()?;
            let financial = AccountStore::financial_risk_sql(&tx, scope, observed_at_ms, None)?;
            let nav_coverage = tx.query_row(
                "SELECT coverage FROM nav_watermarks WHERE installation_id=?1 AND account_key=?2 AND scope_id='account'",
                rusqlite::params![installation, account], |row| row.get(0),
            ).optional()?;
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
                ("fills", "MARGIN"),
                ("fills", "SWAP"),
                ("fills", "FUTURES"),
                ("fills", "OPTION"),
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
                observed_at_ms, financial, nav_coverage,
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
            let budget_denial = match (&intent, AccountStore::operation_sql(
                &tx, scope, &format!("reserve-denied:{intent_id}"),
            )?) {
                (Some(original), Some(receipt)) => {
                    let fact = decode_budget_denial(
                        &receipt, &installation, &account, &intent_id, &original.plan_hash,
                    )?;
                    if i64::try_from(fact.policy_revision).ok() != Some(original.policy_revision) {
                        return Err(crate::StoreError::Invalid("NATIVE_DENIAL_EVIDENCE_INVALID"));
                    }
                    Some(fact)
                },
                (None, Some(_)) => return Err(crate::StoreError::Invalid("NATIVE_DENIAL_INTENT_MISSING")),
                (_, None) => None,
            };
            let entry_source_failure = match (&intent, AccountStore::operation_sql(&tx, scope, &format!("entry-source-failure:{intent_id}"))?) {
                (Some(original), Some(receipt)) => Some(decode_entry_source_failure(&receipt, &installation, &account, &intent_id, &original.plan_hash)?),
                (None, Some(_)) => return Err(crate::StoreError::Invalid("ENTRY_SOURCE_FAILURE_INTENT_MISSING")),
                (_, None) => None,
            };
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
                budget_denial,
                entry_quote: AccountStore::entry_quote_sql(&tx, scope, &intent_id)?,
                entry_admission: AccountStore::entry_admission_sql(&tx, scope, &intent_id)?,
                entry_source_failure,
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
