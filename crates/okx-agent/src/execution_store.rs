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
