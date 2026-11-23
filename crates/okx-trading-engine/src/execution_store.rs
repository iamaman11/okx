//! Account-scoped access to the installation's sole financial SQL owner.
use okx_trade_execution::account_worker::{
    AccountMaintenanceTimingV1, AccountSqlTimingV1, AccountStorageReadback, UnifiedFinanceHandle,
};
use okx_trade_execution::{StoreError, StoreResult};
use okx_ipc_contracts::accounts::AccountKey;
use std::time::Instant;

/// Canonical wire identity with its protocol shape checked. This does not
/// establish affiliation with the finance owner; the owner checks that.
#[derive(Clone)]
struct AccountScope {
    wire: String,
}

impl AccountScope {
    fn new(key: AccountKey) -> StoreResult<Self> {
        if !key.valid() {
            return Err(StoreError::Invalid("NATIVE_ACCOUNT_SCOPE_REQUIRED"));
        }
        Ok(Self { wire: key.wire() })
    }
}

/// The lookup variant selects the identity column; this checks only that the
/// requested wire value is present, not that an intent belongs to an account.
pub struct IntentLookup(okx_trade_execution::TradeIntentLookup);

impl IntentLookup {
    pub fn new(lookup: okx_trade_execution::TradeIntentLookup) -> StoreResult<Self> {
        use okx_trade_execution::TradeIntentLookup::*;
        let value = match &lookup {
            IntentId(value)
            | IdempotencyKey(value)
            | ClientOrderId(value)
            | ExchangeOrderId(value)
            | Reference(value) => value,
        };
        if value.is_empty() {
            return Err(StoreError::Invalid("INTENT_LOOKUP_REQUIRED"));
        }
        Ok(Self(lookup))
    }
}

#[derive(Clone)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: String) -> StoreResult<Self> {
        if value.is_empty() {
            return Err(StoreError::Invalid("OPERATION_ID_REQUIRED"));
        }
        Ok(Self(value))
    }
}

/// Shipping code cannot construct the old per-account financial writer.
///
/// ```compile_fail,E0599
/// use okx_trading_engine::execution_store::ExecutionStore;
/// fn forbidden() -> ExecutionStore {
///     ExecutionStore::Legacy(todo!())
/// }
/// ```
///
/// External callers cannot create an unchecked account-scoped store:
/// ```compile_fail,E0624
/// use okx_trading_engine::execution_store::ExecutionStore;
/// fn unchecked(owner: okx_trade_execution::account_worker::UnifiedFinanceHandle,
///              key: okx_ipc_contracts::accounts::AccountKey) {
///     let _ = ExecutionStore::native(owner, key);
/// }
/// ```
#[derive(Clone)]
pub struct ExecutionStore {
    owner: UnifiedFinanceHandle,
    account: AccountScope,
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
            .strategy_status(deadline, account.wire.clone(), instance, time)
            .await
    }
    pub async fn mutate_strategy_instance(
        &self,
        deadline: Instant,
        command: okx_trade_execution::account_worker::StrategyMutationCommand,
    ) -> StoreResult<okx_trade_execution::OperationReceipt> {
        let Self { owner, account } = self;
        owner
            .mutate_strategy_instance(deadline, account.wire.clone(), command)
            .await
    }
    pub async fn claim_strategy_decision(
        &self,
        deadline: Instant,
        command: okx_trade_execution::account_worker::StrategyDecisionClaimCommand,
    ) -> StoreResult<Option<String>> {
        let Self { owner, account } = self;
        owner
            .claim_strategy_decision(deadline, account.wire.clone(), command)
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
                account.wire.clone(),
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
    ) -> StoreResult<(okx_trade_execution::FinancialRisk, usize)> {
        let Self { owner, account } = self;
        owner
            .financial_readiness(account.wire.clone(), instance, deadline)
            .await
    }

    pub async fn reconcilable_intents_page(
        &self,
        deadline: Instant,
        limit: usize,
        offset: usize,
    ) -> StoreResult<Vec<okx_trade_execution::StoredIntent>> {
        let Self { owner, account } = self;
        owner
            .reconcilable_intents_page(account.wire.clone(), limit, offset, deadline)
            .await
    }

    pub async fn entry_late_pending(&self, deadline: Instant, id: String) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .entry_late_pending(deadline, account.wire.clone(), id)
            .await
    }
    pub async fn partial_entry_ack_pending(
        &self,
        deadline: Instant,
        id: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .partial_entry_ack_pending(deadline, account.wire.clone(), id)
            .await
    }
    pub async fn bind_partial_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_trade_execution::EntrySettlementEvidence> {
        let Self { owner, account } = self;
        owner
            .bind_partial_entry_settlement(deadline, account.wire.clone(), id, at, filled, covered)
            .await
    }
    pub async fn bind_terminal_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_trade_execution::EntrySettlementEvidence> {
        let Self { owner, account } = self;
        owner
            .bind_terminal_entry_settlement(deadline, account.wire.clone(), id, at, filled, covered)
            .await
    }
    pub async fn bind_late_entry_settlement(
        &self,
        deadline: Instant,
        id: String,
        at: i64,
        filled: rust_decimal::Decimal,
        covered: i64,
    ) -> StoreResult<okx_trade_execution::EntrySettlementEvidence> {
        let Self { owner, account } = self;
        owner
            .bind_late_entry_settlement(deadline, account.wire.clone(), id, at, filled, covered)
            .await
    }
    pub async fn acknowledge_partial_entry_settlement(
        &self,
        deadline: Instant,
        proof: okx_trade_execution::EntrySettlementEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .acknowledge_partial_entry_settlement(deadline, account.wire.clone(), proof)
            .await
    }
    pub async fn acknowledge_late_entry_settlement(
        &self,
        deadline: Instant,
        proof: okx_trade_execution::EntrySettlementEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .acknowledge_late_entry_settlement(deadline, account.wire.clone(), proof)
            .await
    }
    pub async fn intent_fill_cost_evidence(
        &self,
        deadline: Instant,
        id: String,
        through: i64,
    ) -> StoreResult<okx_trade_execution::IntentFillCostEvidence> {
        let Self { owner, account } = self;
        owner
            .intent_fill_cost_evidence(deadline, account.wire.clone(), id, through)
            .await
    }
    pub async fn intent_fee_bill_evidence(
        &self,
        deadline: Instant,
        id: String,
        through: i64,
    ) -> StoreResult<okx_trade_execution::IntentFeeBillEvidence> {
        let Self { owner, account } = self;
        owner
            .intent_fee_bill_evidence(deadline, account.wire.clone(), id, through)
            .await
    }
    pub async fn central_receipt(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_trade_execution::CentralReceipt>> {
        let Self { owner, account } = self;
        owner
            .reservation_receipt(
                account.wire.clone(),
                intent,
                okx_trade_execution::CentralReceiptView::Retained,
                deadline,
            )
            .await
    }
    pub async fn cursor(
        &self,
        _deadline: Instant,
        source: String,
        partition: String,
    ) -> StoreResult<Option<okx_trade_execution::SourceCursor>> {
        let Self { owner, account } = self;
        owner
            .source_cursor(account.wire.clone(), source, partition)
            .await
    }
    /// Look up an operation using an operation ID, not an intent lookup.
    ///
    /// ```
    /// use okx_trading_engine::execution_store::{ExecutionStore, IntentLookup, OperationId};
    /// use std::time::Instant;
    /// async fn lookup(store: &ExecutionStore, deadline: Instant, id: OperationId) {
    ///     let _ = store.operation(deadline, id).await;
    /// }
    /// ```
    ///
    /// ```compile_fail,E0308
    /// use okx_trading_engine::execution_store::{ExecutionStore, IntentLookup, OperationId};
    /// use std::time::Instant;
    /// async fn lookup(store: &ExecutionStore, deadline: Instant, id: IntentLookup) {
    ///     let _ = store.operation(deadline, id).await;
    /// }
    /// ```
    pub async fn operation(
        &self,
        deadline: Instant,
        id: OperationId,
    ) -> StoreResult<Option<okx_trade_execution::OperationReceipt>> {
        let Self { owner, account } = self;
        owner.operation(deadline, account.wire.clone(), id.0).await
    }
    pub async fn begin_operation(
        &self,
        deadline: Instant,
        id: OperationId,
        hash: String,
        kind: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .begin_operation(deadline, account.wire.clone(), id.0, hash, kind)
            .await
    }
    pub async fn operation_receipt(
        &self,
        deadline: Instant,
        id: OperationId,
        hash: String,
        kind: String,
        result: String,
    ) -> StoreResult<okx_trade_execution::OperationReceipt> {
        let Self { owner, account } = self;
        owner
            .operation_receipt(deadline, account.wire.clone(), id.0, hash, kind, result)
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
            .effective_protected_quantity(deadline, account.wire.clone(), intent, filled)
            .await
    }
    pub async fn verify_protection(
        &self,
        deadline: Instant,
        receipt: okx_trade_execution::ProtectionReceipt,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .verify_protection(deadline, account.wire.clone(), receipt)
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
            .protection_incident(deadline, account.wire.clone(), intent, reason)
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
            .resolve_protection_incident_after_flat(deadline, account.wire.clone(), intent, digest)
            .await
    }
    pub async fn protection(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_trade_execution::ProtectionReceipt>> {
        let Self { owner, account } = self;
        owner
            .protection(deadline, account.wire.clone(), intent)
            .await
    }
    pub(crate) fn native(owner: UnifiedFinanceHandle, account: AccountKey) -> StoreResult<Self> {
        Ok(Self {
            owner,
            account: AccountScope::new(account)?,
        })
    }

    /// Require the existing financial owner and its original account scope.
    /// A missing owner is not permission to use the retired per-account writer.
    pub(crate) fn scope(&self) -> (&UnifiedFinanceHandle, &str) {
        (&self.owner, &self.account.wire)
    }

    /// Durable cursor seed comes from the same account-scoped finance queue.
    pub async fn source_generation_base(&self, deadline: Instant) -> StoreResult<u64> {
        let Self { owner, account } = self;
        let _ = deadline;
        owner.source_generation_base(account.wire.clone()).await
    }

    /// Durable read-only traces for both supported diagnostic projections.
    /// This is one owner operation, not a general SQL callback.
    pub async fn durable_operation_diagnostics(
        &self,
        deadline: Instant,
        cleanup_limit: usize,
        terminal_limit: usize,
    ) -> StoreResult<(
        Vec<okx_trade_execution::ProtectionCleanupOperationReceipt>,
        Vec<okx_trade_execution::TerminalReleaseDiagnosticReceipt>,
    )> {
        let Self { owner, account } = self;
        owner
            .durable_operation_diagnostics(
                deadline,
                account.wire.clone(),
                cleanup_limit,
                terminal_limit,
            )
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
        lookup: IntentLookup,
    ) -> StoreResult<Option<okx_trade_execution::StoredIntent>> {
        let Self { owner, account } = self;
        owner
            .lookup_intent(account.wire.clone(), lookup.0, deadline)
            .await
    }
    pub async fn bind_entry_evidence(
        &self,
        deadline: Instant,
        quote: okx_trade_execution::EntryQuoteEvidence,
        admission: Option<okx_trade_execution::EntryAdmissionEvidence>,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .bind_entry_evidence(deadline, account.wire.clone(), quote, admission)
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
            .mark_reserve_attempt(deadline, account.wire.clone(), intent, key)
            .await
    }
    /// The original account-scoped order observation is committed by the sole
    /// finance owner. Callers never select an SQL backend.
    pub async fn record_order_readback(
        &self,
        deadline: Instant,
        intent: String,
        order_id: String,
        state: okx_trade_execution::IntentState,
        filled: rust_decimal::Decimal,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .record_order_readback(
                deadline,
                account.wire.clone(),
                intent,
                order_id,
                state,
                filled,
            )
            .await
    }

    /// Read the original quote and admission together from one scoped owner.
    pub async fn entry_cost_evidence(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<(
        Option<okx_trade_execution::EntryQuoteEvidence>,
        Option<okx_trade_execution::EntryAdmissionEvidence>,
    )> {
        let Self { owner, account } = self;
        owner
            .entry_cost_evidence(deadline, account.wire.clone(), intent)
            .await
    }

    pub async fn declined_reserve(&self, deadline: Instant, intent: String) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .declined_reserve(deadline, account.wire.clone(), intent)
            .await
    }

    pub async fn unsent_release_receipt(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<Option<okx_trade_execution::UnsentSqlReceipt>> {
        let Self { owner, account } = self;
        owner
            .unsent_release_receipt(deadline, account.wire.clone(), intent)
            .await
    }

    pub async fn recovery_snapshot(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_trade_execution::RecoverySnapshot> {
        let Self { owner, account } = self;
        owner
            .recovery_snapshot(deadline, account.wire.clone())
            .await
    }

    pub async fn ingest_source_page(
        &self,
        deadline: Instant,
        page: okx_trade_execution::IngestPage,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        let _ = deadline;
        owner.ingest_source_page(account.wire.clone(), page).await
    }

    pub async fn recovery_position_owner(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<(
        Option<okx_trade_execution::ProtectionReceipt>,
        Option<okx_trade_execution::StoredIntent>,
    )> {
        let Self { owner, account } = self;
        owner
            .recovery_position_owner(deadline, account.wire.clone(), intent)
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
            .verify_campaign_entry_readiness(
                deadline,
                account.wire.clone(),
                campaign,
                from,
                through,
            )
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
            .refresh_campaign_entry_readiness(
                deadline,
                account.wire.clone(),
                campaign,
                from,
                through,
            )
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
            .seal_captured_campaign_baseline(
                deadline,
                account.wire.clone(),
                campaign,
                from,
                through,
            )
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
            .reject_unsent(deadline, account.wire.clone(), intent, reason)
            .await
    }
    pub async fn unsent_before_reserve(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<bool> {
        let Self { owner, account } = self;
        owner
            .unsent_before_reserve(deadline, account.wire.clone(), intent)
            .await
    }
    pub async fn reserve_source(
        &self,
        deadline: Instant,
        intent: String,
    ) -> StoreResult<okx_trade_execution::AccountReserveSource> {
        let Self { owner, account } = self;
        owner
            .reserve_source(account.wire.clone(), intent, deadline)
            .await
    }
    pub async fn strategy_instances(
        &self,
        deadline: Instant,
    ) -> StoreResult<Vec<okx_trade_execution::StrategyInstance>> {
        let Self { owner, account } = self;
        owner
            .strategy_instances(deadline, account.wire.clone())
            .await
    }
    pub async fn active_lots(
        &self,
        deadline: Instant,
        instrument: String,
    ) -> StoreResult<Vec<okx_trade_execution::StoredLot>> {
        let Self { owner, account } = self;
        owner
            .active_lots(account.wire.clone(), instrument, deadline)
            .await
    }
    pub async fn account_policy(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_trade_execution::AccountPolicySnapshot> {
        let Self { owner, account } = self;
        owner.account_policy(account.wire.clone(), deadline).await
    }
    pub async fn record_intent_outcome(
        &self,
        _deadline: Instant,
        outcome: okx_trade_execution::IntentOutcome,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .record_intent_outcome(account.wire.clone(), outcome)
            .await
    }
    pub async fn source_capture_state(
        &self,
        deadline: Instant,
    ) -> StoreResult<okx_trade_execution::AccountSourceState> {
        let Self { owner, account } = self;
        owner
            .source_capture_state(account.wire.clone(), deadline)
            .await
    }
    pub async fn finance_report(
        &self,
        deadline: Instant,
        query: okx_trade_execution::FinanceReportQuery,
    ) -> StoreResult<okx_trade_execution::FinanceReportSnapshot> {
        let Self { owner, account } = self;
        owner
            .finance_report(account.wire.clone(), query, deadline)
            .await
    }
    pub async fn assign_capital(
        &self,
        _deadline: Instant,
        command: okx_trade_execution::CapitalAssignmentCommand,
    ) -> StoreResult<okx_trade_execution::CapitalAssignmentResult> {
        let Self { owner, account } = self;
        owner.assign_capital(account.wire.clone(), command).await
    }
    pub async fn ingest_captured_bills(
        &self,
        _deadline: Instant,
        input: okx_trade_execution::CapturedBillPage,
        conversion: okx_trade_execution::UsdConversionEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .ingest_captured_bills(account.wire.clone(), input, conversion)
            .await
    }
    pub async fn ingest_captured_fills(
        &self,
        _deadline: Instant,
        input: okx_trade_execution::CapturedFillPage,
        conversion: okx_trade_execution::UsdConversionEvidence,
    ) -> StoreResult<()> {
        let Self { owner, account } = self;
        owner
            .ingest_captured_fills(account.wire.clone(), input, conversion)
            .await
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    use okx_ipc_contracts::accounts::Environment;

    #[test]
    fn account_scope_checks_protocol_shape_and_keeps_canonical_wire() {
        let mut key = AccountKey {
            venue: "okx".into(),
            environment: Environment::Demo,
            verified_uid: "a".repeat(64),
        };
        assert_eq!(AccountScope::new(key.clone()).unwrap().wire, key.wire());
        key.verified_uid = "not-a-verified-uid".into();
        assert!(AccountScope::new(key).is_err());
    }

    #[test]
    fn identity_inputs_reject_empty_and_accept_present_values() {
        use okx_trade_execution::TradeIntentLookup;
        assert!(IntentLookup::new(TradeIntentLookup::IntentId(String::new())).is_err());
        assert!(IntentLookup::new(TradeIntentLookup::IntentId("intent-1".into())).is_ok());
        assert!(OperationId::new(String::new()).is_err());
        assert!(OperationId::new("operation-1".into()).is_ok());
    }

    // Compile-time positive control for the actual store boundary.
    #[allow(dead_code)]
    async fn typed_store_calls(store: &ExecutionStore, deadline: Instant) -> StoreResult<()> {
        let intent = IntentLookup::new(okx_trade_execution::TradeIntentLookup::IntentId(
            "intent-1".into(),
        ))?;
        let operation = OperationId::new("operation-1".into())?;
        let _ = store.lookup_intent(deadline, intent).await?;
        let _ = store.operation(deadline, operation).await?;
        Ok(())
    }
}
