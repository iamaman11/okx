//! Exclusive runtime composition. Native never exposes a legacy account writer.
use okx_execution::account_worker::{
    AccountMaintenanceTimingV1, AccountSqlJobTiming, AccountSqlTimingV1, AccountStorageReadback,
    UnifiedFinanceHandle,
};
use okx_execution::{AccountStore, AccountStoreHandle, StoreError, StoreResult};
use std::time::Instant;

#[derive(Clone)]
pub enum ExecutionStore {
    Legacy(AccountStoreHandle),
    Native {
        owner: UnifiedFinanceHandle,
        account: String,
    },
}
impl From<AccountStoreHandle> for ExecutionStore {
    fn from(value: AccountStoreHandle) -> Self {
        Self::Legacy(value)
    }
}
impl ExecutionStore {
    pub async fn entry_late_pending(&self, deadline: Instant, id: String) -> StoreResult<bool> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| sql.entry_late_pending(&id))
                    .await
            }
            Self::Native { owner, account } => {
                owner
                    .entry_late_pending(deadline, account.clone(), id)
                    .await
            }
        }
    }
    pub async fn partial_entry_ack_pending(
        &self,
        deadline: Instant,
        id: String,
    ) -> StoreResult<bool> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| sql.partial_entry_ack_pending(&id))
                    .await
            }
            Self::Native { owner, account } => {
                owner
                    .partial_entry_ack_pending(deadline, account.clone(), id)
                    .await
            }
        }
    }
    pub async fn bind_partial_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_execution::EntrySettlementEvidence> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.bind_partial_entry_settlement(&id, at, filled, covered)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .bind_partial_entry_settlement(
                        deadline,
                        account.clone(),
                        id,
                        at,
                        filled,
                        covered,
                    )
                    .await
            }
        }
    }
    pub async fn bind_terminal_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_execution::EntrySettlementEvidence> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.bind_terminal_entry_settlement(&id, at, filled, covered)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .bind_terminal_entry_settlement(
                        deadline,
                        account.clone(),
                        id,
                        at,
                        filled,
                        covered,
                    )
                    .await
            }
        }
    }
    pub async fn bind_late_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_execution::EntrySettlementEvidence> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.bind_late_entry_settlement(&id, at, filled, covered)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .bind_late_entry_settlement(deadline, account.clone(), id, at, filled, covered)
                    .await
            }
        }
    }
    pub async fn acknowledge_partial_entry_settlement(
        &self,
        deadline: Instant,
        proof: okx_execution::EntrySettlementEvidence,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.acknowledge_partial_entry_settlement(&proof)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .acknowledge_partial_entry_settlement(deadline, account.clone(), proof)
                    .await
            }
        }
    }
    pub async fn acknowledge_late_entry_settlement(
        &self,
        deadline: Instant,
        proof: okx_execution::EntrySettlementEvidence,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.acknowledge_late_entry_settlement(&proof)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .acknowledge_late_entry_settlement(deadline, account.clone(), proof)
                    .await
            }
        }
    }
    pub async fn intent_fill_cost_evidence(
        &self,
        deadline: Instant,
        id: String,
        through: i64,
    ) -> StoreResult<okx_execution::IntentFillCostEvidence> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.intent_fill_cost_evidence(&id, through)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .intent_fill_cost_evidence(deadline, account.clone(), id, through)
                    .await
            }
        }
    }
    pub async fn intent_fee_bill_evidence(
        &self,
        deadline: Instant,
        id: String,
        through: i64,
    ) -> StoreResult<okx_execution::IntentFeeBillEvidence> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.intent_fee_bill_evidence(&id, through)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .intent_fee_bill_evidence(deadline, account.clone(), id, through)
                    .await
            }
        }
    }
    pub async fn central_receipt(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_execution::CentralReceipt>> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| sql.central_receipt(&intent))
                    .await
            }
            Self::Native { owner, account } => {
                owner
                    .reservation_receipt(
                        account.clone(),
                        intent,
                        okx_execution::CentralReceiptView::Retained,
                        deadline,
                    )
                    .await
            }
        }
    }
    pub async fn cursor(
        &self,
        deadline: Instant,
        source: String,
        partition: String,
    ) -> StoreResult<Option<okx_execution::SourceCursor>> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| sql.cursor(&source, &partition))
                    .await
            }
            Self::Native { owner, account } => {
                owner
                    .source_cursor(account.clone(), source, partition)
                    .await
            }
        }
    }
    pub async fn operation(
        &self,
        deadline: Instant,
        id: String,
    ) -> StoreResult<Option<okx_execution::OperationReceipt>> {
        match self {
            Self::Legacy(s) => s.call(deadline, move |sql| sql.operation(&id)).await,
            Self::Native { owner, account } => owner.operation(deadline, account.clone(), id).await,
        }
    }
    pub async fn begin_operation(
        &self,
        deadline: Instant,
        id: String,
        hash: String,
        kind: String,
    ) -> StoreResult<bool> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.begin_operation(&id, &hash, &kind, chrono::Utc::now().timestamp_millis())
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .begin_operation(deadline, account.clone(), id, hash, kind)
                    .await
            }
        }
    }
    pub async fn operation_receipt(
        &self,
        deadline: Instant,
        id: String,
        hash: String,
        kind: String,
        result: String,
    ) -> StoreResult<okx_execution::OperationReceipt> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.operation_receipt(
                        &id,
                        &hash,
                        &kind,
                        &result,
                        chrono::Utc::now().timestamp_millis(),
                    )
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .operation_receipt(deadline, account.clone(), id, hash, kind, result)
                    .await
            }
        }
    }
    pub async fn effective_protected_quantity(
        &self,
        deadline: Instant,
        intent: String,
        filled: rust_decimal::Decimal,
    ) -> StoreResult<Option<rust_decimal::Decimal>> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.effective_protected_quantity(&intent, filled)
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .effective_protected_quantity(deadline, account.clone(), intent, filled)
                    .await
            }
        }
    }
    pub async fn verify_protection(
        &self,
        deadline: Instant,
        receipt: okx_execution::ProtectionReceipt,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| sql.verify_protection(&receipt))
                    .await
            }
            Self::Native { owner, account } => {
                owner
                    .verify_protection(deadline, account.clone(), receipt)
                    .await
            }
        }
    }
    pub async fn protection_incident(
        &self,
        deadline: Instant,
        intent: String,
        reason: String,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.protection_incident(&intent, &reason, chrono::Utc::now().timestamp_millis())
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .protection_incident(deadline, account.clone(), intent, reason)
                    .await
            }
        }
    }
    pub async fn resolve_protection_incident_after_flat(
        &self,
        deadline: Instant,
        intent: String,
        digest: String,
    ) -> StoreResult<bool> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.resolve_protection_incident_after_flat(
                        &intent,
                        &digest,
                        chrono::Utc::now().timestamp_millis(),
                    )
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .resolve_protection_incident_after_flat(
                        deadline,
                        account.clone(),
                        intent,
                        digest,
                    )
                    .await
            }
        }
    }
    pub async fn protection(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_execution::ProtectionReceipt>> {
        match self {
            Self::Legacy(s) => s.call(deadline, move |sql| sql.protection(&intent)).await,
            Self::Native { owner, account } => {
                owner.protection(deadline, account.clone(), intent).await
            }
        }
    }
    pub fn native(owner: UnifiedFinanceHandle, account: String) -> StoreResult<Self> {
        if account.is_empty() {
            return Err(StoreError::Invalid("NATIVE_ACCOUNT_SCOPE_REQUIRED"));
        }
        Ok(Self::Native { owner, account })
    }
    pub fn legacy(&self) -> StoreResult<&AccountStoreHandle> {
        match self {
            Self::Legacy(store) => Ok(store),
            Self::Native { .. } => Err(StoreError::Invalid(
                "NATIVE_LEGACY_ACCOUNT_WRITER_FORBIDDEN",
            )),
        }
    }
    pub(crate) fn native_scope(&self) -> Option<(&UnifiedFinanceHandle, &str)> {
        match self {
            Self::Native { owner, account } => Some((owner, account)),
            Self::Legacy(_) => None,
        }
    }
    // Compatibility for unported legacy operations. This method NEVER gives
    // Native callers a Connection or AccountStore, nor opens another writer.
    pub async fn call<T, F>(&self, deadline: Instant, operation: F) -> StoreResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut AccountStore) -> StoreResult<T> + Send + 'static,
    {
        self.legacy()?.call(deadline, operation).await
    }
    pub async fn call_with_timing_observed<T, F, O>(
        &self,
        deadline: Instant,
        operation: F,
        observed: O,
    ) -> StoreResult<(T, AccountSqlJobTiming)>
    where
        T: Send + 'static,
        F: FnOnce(&mut AccountStore) -> StoreResult<T> + Send + 'static,
        O: FnOnce(AccountSqlJobTiming, bool) + Send + 'static,
    {
        self.legacy()?
            .call_with_timing_observed(deadline, operation, observed)
            .await
    }
    pub fn termination_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        match self {
            Self::Legacy(s) => s.termination_signal(),
            Self::Native { owner, .. } => owner.termination_signal(),
        }
    }
    pub fn diagnostic_storage_snapshot(&self) -> AccountStorageReadback {
        match self {
            Self::Legacy(s) => s.diagnostic_storage_snapshot(),
            Self::Native { owner, .. } => AccountStorageReadback {
                schema: "okx.account-storage-observation/v1",
                state: "NOT_EVALUATED",
                snapshot: None,
                last_attempt_ms: None,
                reason_code: Some("NATIVE_STORAGE_PROJECTION_NOT_AVAILABLE".into()),
                refresh_pending: false,
                refresh_attempts: 0,
                publication_skipped: 0,
                owner_terminated: *owner.termination_signal().borrow(),
            },
        }
    }
    pub fn maintenance_timing(&self) -> AccountMaintenanceTimingV1 {
        match self {
            Self::Legacy(s) => s.maintenance_timing(),
            Self::Native { .. } => AccountMaintenanceTimingV1 {
                schema: "okx.account-maintenance/v1",
                state: "NOT_EVALUATED",
                events: Vec::new(),
                evicted: 0,
            },
        }
    }
    pub fn sql_timing(&self) -> AccountSqlTimingV1 {
        match self {
            Self::Legacy(s) => s.sql_timing(),
            Self::Native { owner, .. } => owner.sql_timing(),
        }
    }
    pub async fn lookup_intent(
        &self,
        deadline: Instant,
        lookup: okx_execution::TradeIntentLookup,
    ) -> StoreResult<Option<okx_execution::StoredIntent>> {
        match self {
            Self::Legacy(s) => s.lookup_intent(deadline, lookup).await,
            Self::Native { owner, account } => {
                owner.lookup_intent(account.clone(), lookup, deadline).await
            }
        }
    }
    pub async fn bind_entry_evidence(
        &self,
        deadline: Instant,
        quote: okx_execution::EntryQuoteEvidence,
        admission: Option<okx_execution::EntryAdmissionEvidence>,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.bind_entry_quote(&quote)?;
                    if let Some(proof) = admission {
                        sql.bind_entry_admission(&proof)?;
                    }
                    Ok(())
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .bind_entry_evidence(deadline, account.clone(), quote, admission)
                    .await
            }
        }
    }
    pub async fn mark_reserve_attempt(
        &self,
        deadline: Instant,
        intent: String,
        key: String,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.mark_reserve_attempt(&intent, &key, chrono::Utc::now().timestamp_millis())
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .mark_reserve_attempt(deadline, account.clone(), intent, key)
                    .await
            }
        }
    }
    pub async fn reject_unsent(
        &self,
        deadline: Instant,
        intent: String,
        reason: String,
    ) -> StoreResult<bool> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| {
                    sql.reject_unsent(&intent, &reason, chrono::Utc::now().timestamp_millis())
                })
                .await
            }
            Self::Native { owner, account } => {
                owner
                    .reject_unsent(deadline, account.clone(), intent, reason)
                    .await
            }
        }
    }
    pub async fn unsent_before_reserve(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<bool> {
        match self {
            Self::Legacy(s) => {
                s.call(deadline, move |sql| sql.unsent_before_reserve(&intent))
                    .await
            }
            Self::Native { owner, account } => {
                owner
                    .unsent_before_reserve(deadline, account.clone(), intent)
                    .await
            }
        }
    }
    pub async fn reserve_source(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<okx_execution::AccountReserveSource> {
        match self {
            Self::Legacy(s) => s.reserve_source(deadline, intent).await,
            Self::Native { owner, account } => {
                owner
                    .reserve_source(account.clone(), intent, deadline)
                    .await
            }
        }
    }
    pub async fn strategy_instances(
        &self,
        deadline: Instant,
    ) -> StoreResult<Vec<okx_execution::StrategyInstance>> {
        match self {
            Self::Legacy(s) => s.call(deadline, |sql| sql.instances()).await,
            Self::Native { owner, account } => {
                owner.strategy_instances(deadline, account.clone()).await
            }
        }
    }
    pub async fn active_lots(
        &self,
        deadline: Instant,
        instrument: String,
    ) -> StoreResult<Vec<okx_execution::StoredLot>> {
        match self {
            Self::Legacy(s) => s.active_lots(deadline, instrument).await,
            Self::Native { owner, account } => {
                owner
                    .active_lots(account.clone(), instrument, deadline)
                    .await
            }
        }
    }
    pub async fn account_policy(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_execution::AccountPolicySnapshot> {
        match self {
            Self::Legacy(s) => s.account_policy(deadline).await,
            Self::Native { owner, account } => {
                owner.account_policy(account.clone(), deadline).await
            }
        }
    }
    pub async fn record_intent_outcome(
        &self,
        deadline: Instant,
        outcome: okx_execution::IntentOutcome,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => s.record_intent_outcome(deadline, outcome).await,
            Self::Native { owner, account } => {
                owner.record_intent_outcome(account.clone(), outcome).await
            }
        }
    }
    pub async fn source_capture_state(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_execution::AccountSourceState> {
        match self {
            Self::Legacy(s) => s.source_capture_state(deadline).await,
            Self::Native { owner, account } => {
                owner.source_capture_state(account.clone(), deadline).await
            }
        }
    }
    pub async fn finance_report(
        &self,
        deadline: Instant,
        query: okx_execution::FinanceReportQuery,
    ) -> StoreResult<okx_execution::FinanceReportSnapshot> {
        match self {
            Self::Legacy(s) => s.finance_report(deadline, query).await,
            Self::Native { owner, account } => {
                owner.finance_report(account.clone(), query, deadline).await
            }
        }
    }
    pub async fn assign_capital(
        &self,
        deadline: Instant,
        command: okx_execution::CapitalAssignmentCommand,
    ) -> StoreResult<okx_execution::CapitalAssignmentResult> {
        match self {
            Self::Legacy(s) => s.assign_capital(deadline, command).await,
            Self::Native { owner, account } => owner.assign_capital(account.clone(), command).await,
        }
    }
    pub async fn ingest_captured_bills(
        &self,
        deadline: Instant,
        input: okx_execution::CapturedBillPage,
        conversion: okx_execution::UsdConversionEvidence,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => s.ingest_captured_bills(deadline, input, conversion).await,
            Self::Native { owner, account } => {
                owner
                    .ingest_captured_bills(account.clone(), input, conversion)
                    .await
            }
        }
    }
    pub async fn ingest_captured_fills(
        &self,
        deadline: Instant,
        input: okx_execution::CapturedFillPage,
        conversion: okx_execution::UsdConversionEvidence,
    ) -> StoreResult<()> {
        match self {
            Self::Legacy(s) => s.ingest_captured_fills(deadline, input, conversion).await,
            Self::Native { owner, account } => {
                owner
                    .ingest_captured_fills(account.clone(), input, conversion)
                    .await
            }
        }
    }
}
