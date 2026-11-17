//! Account-scoped access to the installation's sole financial SQL owner.
use okx_execution::account_worker::{
    AccountMaintenanceTimingV1, AccountSqlTimingV1, AccountStorageReadback, UnifiedFinanceHandle,
};
use okx_execution::{StoreError, StoreResult};
use std::time::Instant;

/// Shipping code cannot construct the old per-account financial writer.
///
/// ```compile_fail,E0599
/// use okx_agent::execution_store::ExecutionStore;
/// fn forbidden() -> ExecutionStore {
///     ExecutionStore::Legacy(todo!())
/// }
/// ```
#[derive(Clone)]
pub struct ExecutionStore {
    owner: UnifiedFinanceHandle,
    account: String,
}
impl ExecutionStore {
    pub async fn strategy_status(
        &self,
        deadline: Instant,
        instance: String,
        time: i64,
    ) -> StoreResult<serde_json::Value> {
        let Self { owner, account } = self;
        owner
            .strategy_status(deadline, account.clone(), instance, time)
            .await
    }
    pub async fn mutate_strategy_instance(
        &self,
        deadline: Instant,
        command: okx_execution::account_worker::StrategyMutationCommand,
    ) -> StoreResult<okx_execution::OperationReceipt> {
        let Self { owner, account } = self;
        owner
            .mutate_strategy_instance(deadline, account.clone(), command)
            .await
    }
    pub async fn claim_strategy_decision(
        &self,
        deadline: Instant,
        command: okx_execution::account_worker::StrategyDecisionClaimCommand,
    ) -> StoreResult<Option<String>> {
        let Self { owner, account } = self;
        owner
            .claim_strategy_decision(deadline, account.clone(), command)
            .await
    }
    pub async fn finish_strategy_no_trade(
        &self,
        deadline: Instant,
        instance: String,
        key: String,
        expected_version: String,
        time: i64,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .finish_strategy_no_trade(
                deadline,
                account.clone(),
                instance,
                key,
                expected_version,
                time,
            )
            .await
    }
    pub async fn financial_readiness(
        &self,
        deadline: Instant,
        instance: Option<String>,
    ) -> StoreResult<(okx_execution::FinancialRisk, usize)> {
        let Self { owner, account } = self;
        owner
            .financial_readiness(account.clone(), instance, deadline)
            .await
    }

    pub async fn reconcilable_intents_page(
        &self,
        deadline: Instant,
        limit: usize,
        offset: usize,
    ) -> StoreResult<Vec<okx_execution::StoredIntent>> {
        let Self { owner, account } = self;
        owner
            .reconcilable_intents_page(account.clone(), limit, offset, deadline)
            .await
    }

    pub async fn entry_late_pending(&self, deadline: Instant, id: String) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .entry_late_pending(deadline, account.clone(), id)
            .await
    }
    pub async fn partial_entry_ack_pending(
        &self,
        deadline: Instant,
        id: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .partial_entry_ack_pending(deadline, account.clone(), id)
            .await
    }
    pub async fn bind_partial_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_execution::EntrySettlementEvidence> {
        let Self { owner, account } = self;
        owner
            .bind_partial_entry_settlement(deadline, account.clone(), id, at, filled, covered)
            .await
    }
    pub async fn bind_terminal_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_execution::EntrySettlementEvidence> {
        let Self { owner, account } = self;
        owner
            .bind_terminal_entry_settlement(deadline, account.clone(), id, at, filled, covered)
            .await
    }
    pub async fn bind_late_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_execution::EntrySettlementEvidence> {
        let Self { owner, account } = self;
        owner
            .bind_late_entry_settlement(deadline, account.clone(), id, at, filled, covered)
            .await
    }
    pub async fn acknowledge_partial_entry_settlement(
        &self,
        deadline: Instant,
        proof: okx_execution::EntrySettlementEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .acknowledge_partial_entry_settlement(deadline, account.clone(), proof)
            .await
    }
    pub async fn acknowledge_late_entry_settlement(
        &self,
        deadline: Instant,
        proof: okx_execution::EntrySettlementEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .acknowledge_late_entry_settlement(deadline, account.clone(), proof)
            .await
    }
    pub async fn intent_fill_cost_evidence(
        &self,
        deadline: Instant,
        id: String,
        through: i64,
    ) -> StoreResult<okx_execution::IntentFillCostEvidence> {
        let Self { owner, account } = self;
        owner
            .intent_fill_cost_evidence(deadline, account.clone(), id, through)
            .await
    }
    pub async fn intent_fee_bill_evidence(
        &self,
        deadline: Instant,
        id: String,
        through: i64,
    ) -> StoreResult<okx_execution::IntentFeeBillEvidence> {
        let Self { owner, account } = self;
        owner
            .intent_fee_bill_evidence(deadline, account.clone(), id, through)
            .await
    }
    pub async fn central_receipt(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_execution::CentralReceipt>> {
        let Self { owner, account } = self;
        owner
            .reservation_receipt(
                account.clone(),
                intent,
                okx_execution::CentralReceiptView::Retained,
                deadline,
            )
            .await
    }
    pub async fn cursor(
        &self,
        _deadline: Instant,
        source: String,
        partition: String,
    ) -> StoreResult<Option<okx_execution::SourceCursor>> {
        let Self { owner, account } = self;
        owner
            .source_cursor(account.clone(), source, partition)
            .await
    }
    pub async fn operation(
        &self,
        deadline: Instant,
        id: String,
    ) -> StoreResult<Option<okx_execution::OperationReceipt>> {
        let Self { owner, account } = self;
        owner.operation(deadline, account.clone(), id).await
    }
    pub async fn begin_operation(
        &self,
        deadline: Instant,
        id: String,
        hash: String,
        kind: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .begin_operation(deadline, account.clone(), id, hash, kind)
            .await
    }
    pub async fn operation_receipt(
        &self,
        deadline: Instant,
        id: String,
        hash: String,
        kind: String,
        result: String,
    ) -> StoreResult<okx_execution::OperationReceipt> {
        let Self { owner, account } = self;
        owner
            .operation_receipt(deadline, account.clone(), id, hash, kind, result)
            .await
    }
    pub async fn effective_protected_quantity(
        &self,
        deadline: Instant,
        intent: String,
        filled: rust_decimal::Decimal,
    ) -> StoreResult<Option<rust_decimal::Decimal>> {
        let Self { owner, account } = self;
        owner
            .effective_protected_quantity(deadline, account.clone(), intent, filled)
            .await
    }
    pub async fn verify_protection(
        &self,
        deadline: Instant,
        receipt: okx_execution::ProtectionReceipt,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .verify_protection(deadline, account.clone(), receipt)
            .await
    }
    pub async fn protection_incident(
        &self,
        deadline: Instant,
        intent: String,
        reason: String,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .protection_incident(deadline, account.clone(), intent, reason)
            .await
    }
    pub async fn resolve_protection_incident_after_flat(
        &self,
        deadline: Instant,
        intent: String,
        digest: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .resolve_protection_incident_after_flat(deadline, account.clone(), intent, digest)
            .await
    }
    pub async fn protection(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_execution::ProtectionReceipt>> {
        let Self { owner, account } = self;
        owner.protection(deadline, account.clone(), intent).await
    }
    pub fn native(owner: UnifiedFinanceHandle, account: String) -> StoreResult<Self> {
        if account.is_empty() {
            return Err(StoreError::Invalid("NATIVE_ACCOUNT_SCOPE_REQUIRED"));
        }
        Ok(Self { owner, account })
    }

    /// Require the existing financial owner and its original account scope.
    /// A missing owner is not permission to use the retired per-account writer.
    pub(crate) fn scope(&self) -> (&UnifiedFinanceHandle, &str) {
        (&self.owner, &self.account)
    }

    /// Durable cursor seed comes from the same account-scoped finance queue.
    pub async fn source_generation_base(&self, deadline: Instant) -> StoreResult<u64> {
        let Self { owner, account } = self;
        let _ = deadline;
        owner.source_generation_base(account.clone()).await
    }

    /// Durable read-only traces for both supported diagnostic projections.
    /// This is one owner operation, not a general SQL callback.
    pub async fn durable_operation_diagnostics(
        &self,
        deadline: Instant,
        cleanup_limit: usize,
        terminal_limit: usize,
    ) -> StoreResult<(
        Vec<okx_execution::ProtectionCleanupOperationReceipt>,
        Vec<okx_execution::TerminalReleaseDiagnosticReceipt>,
    )> {
        let Self { owner, account } = self;
        owner
            .durable_operation_diagnostics(deadline, account.clone(), cleanup_limit, terminal_limit)
            .await
    }

    pub fn termination_signal(&self) -> tokio::sync::watch::Receiver<bool> {
        let Self { owner, .. } = self;
        owner.termination_signal()
    }
    pub fn diagnostic_storage_snapshot(&self) -> AccountStorageReadback {
        let Self { owner, .. } = self;
        AccountStorageReadback {
            schema: "okx.account-storage-observation/v1",
            state: "NOT_EVALUATED",
            snapshot: None,
            last_attempt_ms: None,
            reason_code: Some("NATIVE_STORAGE_PROJECTION_NOT_AVAILABLE".into()),
            refresh_pending: false,
            refresh_attempts: 0,
            publication_skipped: 0,
            owner_terminated: *owner.termination_signal().borrow(),
        }
    }
    pub fn maintenance_timing(&self) -> AccountMaintenanceTimingV1 {
        AccountMaintenanceTimingV1 {
            schema: "okx.account-maintenance/v1",
            state: "NOT_EVALUATED",
            events: Vec::new(),
            evicted: 0,
        }
    }
    pub fn sql_timing(&self) -> AccountSqlTimingV1 {
        let Self { owner, .. } = self;
        owner.sql_timing()
    }
    pub async fn lookup_intent(
        &self,
        deadline: Instant,
        lookup: okx_execution::TradeIntentLookup,
    ) -> StoreResult<Option<okx_execution::StoredIntent>> {
        let Self { owner, account } = self;
        owner.lookup_intent(account.clone(), lookup, deadline).await
    }
    pub async fn bind_entry_evidence(
        &self,
        deadline: Instant,
        quote: okx_execution::EntryQuoteEvidence,
        admission: Option<okx_execution::EntryAdmissionEvidence>,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .bind_entry_evidence(deadline, account.clone(), quote, admission)
            .await
    }
    pub async fn mark_reserve_attempt(
        &self,
        deadline: Instant,
        intent: String,
        key: String,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .mark_reserve_attempt(deadline, account.clone(), intent, key)
            .await
    }
    /// The original account-scoped order observation is committed by the sole
    /// finance owner. Callers never select an SQL backend.
    pub async fn record_order_readback(
        &self,
        deadline: Instant,
        intent: String,
        order_id: String,
        state: okx_execution::IntentState,
        filled: rust_decimal::Decimal,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .record_order_readback(deadline, account.clone(), intent, order_id, state, filled)
            .await
    }

    /// Read the original quote and admission together from one scoped owner.
    pub async fn entry_cost_evidence(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<(
        Option<okx_execution::EntryQuoteEvidence>,
        Option<okx_execution::EntryAdmissionEvidence>,
    )> {
        let Self { owner, account } = self;
        owner
            .entry_cost_evidence(deadline, account.clone(), intent)
            .await
    }

    pub async fn declined_reserve(&self, deadline: Instant, intent: String) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .declined_reserve(deadline, account.clone(), intent)
            .await
    }

    pub async fn unsent_release_receipt(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_execution::UnsentSqlReceipt>> {
        let Self { owner, account } = self;
        owner
            .unsent_release_receipt(deadline, account.clone(), intent)
            .await
    }

    pub async fn recovery_snapshot(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_execution::RecoverySnapshot> {
        let Self { owner, account } = self;
        owner.recovery_snapshot(deadline, account.clone()).await
    }

    pub async fn ingest_source_page(
        &self,
        deadline: Instant,
        page: okx_execution::IngestPage,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        let _ = deadline;
        owner.ingest_source_page(account.clone(), page).await
    }

    pub async fn recovery_position_owner(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<(
        Option<okx_execution::ProtectionReceipt>,
        Option<okx_execution::StoredIntent>,
    )> {
        let Self { owner, account } = self;
        owner
            .recovery_position_owner(deadline, account.clone(), intent)
            .await
    }

    /// Observe the original campaign source boundary through the existing
    /// account scope; never expose the owner's SQL connection to Execute.
    pub async fn verify_campaign_entry_readiness(
        &self,
        deadline: Instant,
        campaign: String,
        from: i64,
        through: i64,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .verify_campaign_entry_readiness(deadline, account.clone(), campaign, from, through)
            .await
    }

    /// Source refresh commits on the one finance writer before any opening.
    pub async fn refresh_campaign_entry_readiness(
        &self,
        deadline: Instant,
        campaign: String,
        from: i64,
        through: i64,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .refresh_campaign_entry_readiness(deadline, account.clone(), campaign, from, through)
            .await
    }

    /// The sealed historical spend and source digest are scoped facts,
    /// never a trading grant or a second copy of the financial ledger.
    pub async fn seal_captured_campaign_baseline(
        &self,
        deadline: Instant,
        campaign: String,
        from: i64,
        through: i64,
    ) -> StoreResult<(rust_decimal::Decimal, String)> {
        let Self { owner, account } = self;
        owner
            .seal_captured_campaign_baseline(deadline, account.clone(), campaign, from, through)
            .await
    }

    pub async fn reject_unsent(
        &self,
        deadline: Instant,
        intent: String,
        reason: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .reject_unsent(deadline, account.clone(), intent, reason)
            .await
    }
    pub async fn unsent_before_reserve(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .unsent_before_reserve(deadline, account.clone(), intent)
            .await
    }
    pub async fn reserve_source(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<okx_execution::AccountReserveSource> {
        let Self { owner, account } = self;
        owner
            .reserve_source(account.clone(), intent, deadline)
            .await
    }
    pub async fn strategy_instances(
        &self,
        deadline: Instant,
    ) -> StoreResult<Vec<okx_execution::StrategyInstance>> {
        let Self { owner, account } = self;
        owner.strategy_instances(deadline, account.clone()).await
    }
    pub async fn active_lots(
        &self,
        deadline: Instant,
        instrument: String,
    ) -> StoreResult<Vec<okx_execution::StoredLot>> {
        let Self { owner, account } = self;
        owner
            .active_lots(account.clone(), instrument, deadline)
            .await
    }
    pub async fn account_policy(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_execution::AccountPolicySnapshot> {
        let Self { owner, account } = self;
        owner.account_policy(account.clone(), deadline).await
    }
    pub async fn record_intent_outcome(
        &self,
        _deadline: Instant,
        outcome: okx_execution::IntentOutcome,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner.record_intent_outcome(account.clone(), outcome).await
    }
    pub async fn source_capture_state(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_execution::AccountSourceState> {
        let Self { owner, account } = self;
        owner.source_capture_state(account.clone(), deadline).await
    }
    pub async fn finance_report(
        &self,
        deadline: Instant,
        query: okx_execution::FinanceReportQuery,
    ) -> StoreResult<okx_execution::FinanceReportSnapshot> {
        let Self { owner, account } = self;
        owner.finance_report(account.clone(), query, deadline).await
    }
    pub async fn assign_capital(
        &self,
        _deadline: Instant,
        command: okx_execution::CapitalAssignmentCommand,
    ) -> StoreResult<okx_execution::CapitalAssignmentResult> {
        let Self { owner, account } = self;
        owner.assign_capital(account.clone(), command).await
    }
    pub async fn ingest_captured_bills(
        &self,
        _deadline: Instant,
        input: okx_execution::CapturedBillPage,
        conversion: okx_execution::UsdConversionEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .ingest_captured_bills(account.clone(), input, conversion)
            .await
    }
    pub async fn ingest_captured_fills(
        &self,
        _deadline: Instant,
        input: okx_execution::CapturedFillPage,
        conversion: okx_execution::UsdConversionEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .ingest_captured_fills(account.clone(), input, conversion)
            .await
    }
}
