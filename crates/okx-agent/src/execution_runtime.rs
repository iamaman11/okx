use std::path::Path;

use chrono::{SecondsFormat, Utc};
use okx_api::{
    AccountApi, AccountRateLimitEvidence, ClockEvidence, Credentials, MarginMode, MutationTiming,
    OkxEnvironment, OkxPublicClient, OkxRestClient, PublicDataApi, RateBudget, RateBudgetSnapshot,
    TradeApi,
};
use okx_execution::{
    AccountLedgerReconciliation, AccountLedgerReconciliationError, DurableExecutionLedger,
    ExecutionLedgerEntry, ExecutionLedgerStore, ExecutionLineageBinding, ExecutionPlan,
    ExecutionStatusEnvelope, OrderExecutor, OrderExecutorError, PositionSide, PrepareOutcome,
    SubmitDisposition,
    execution_status_with_ledger, reconcile_account_ledger,
};
use okx_observation::{
    AccountLedgerFacts, AccountSnapshot, InstrumentRulesSnapshot, VenueExecutionEvidence,
};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    AgentError, AgentResult,
    execution_preflight::{
        ExecutorCredentialPreflight, evaluate_executor_preflight_against_snapshot,
    },
};

pub const EXECUTION_PREPARED_SCHEMA_V1: &str = "okx.execution-prepared/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedDisposition {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreparedExecutionResult {
    pub schema: &'static str,
    pub disposition: PreparedDisposition,
    pub entry: ExecutionLedgerEntry,
}

pub struct ExecutionRuntime {
    environment: OkxEnvironment,
    executor_clock: OkxRestClient,
    executor_account: AccountApi,
    public_data: PublicDataApi,
    executor: Mutex<OrderExecutor<TradeApi>>,
}

impl ExecutionRuntime {
    pub fn new(
        root: &Path,
        environment: OkxEnvironment,
        executor_credentials: Credentials,
        observed_at_ms: u64,
        rate_budget: RateBudget,
    ) -> AgentResult<Self> {
        let public_data = PublicDataApi::new(OkxPublicClient::with_rate_budget(
            environment,
            rate_budget.clone(),
        )?);
        let rest = OkxRestClient::with_rate_budget(environment, executor_credentials, rate_budget)?;
        let executor_clock = rest.clone();
        let executor_account = AccountApi::new(rest.clone());
        let trade = TradeApi::new(rest);
        let ledger = DurableExecutionLedger::open(
            ExecutionLedgerStore::at(root.join("execution-ledger.json")),
            observed_at_ms,
        )?;
        Ok(Self {
            environment,
            executor_clock,
            executor_account,
            public_data,
            executor: Mutex::new(OrderExecutor::new(ledger, trade)),
        })
    }

    pub async fn reconcile_account_ledger(
        &self,
        facts: &AccountLedgerFacts,
    ) -> Result<AccountLedgerReconciliation, AccountLedgerReconciliationError> {
        let executor = self.executor.lock().await;
        reconcile_account_ledger(executor.ledger(), facts)
    }

    pub async fn preflight(
        &self,
        observer: &AccountSnapshot,
    ) -> AgentResult<ExecutorCredentialPreflight> {
        let executor = self.executor_account.config().await?;
        Ok(evaluate_executor_preflight_against_snapshot(
            self.environment,
            observer,
            &executor,
        ))
    }

    pub async fn venue_execution_evidence(
        &self,
        plan: &ExecutionPlan,
        rules: &InstrumentRulesSnapshot,
    ) -> AgentResult<VenueExecutionEvidence> {
        let margin_mode = match plan.trade_mode {
            okx_execution::TradeMode::Cross => MarginMode::Cross,
            okx_execution::TradeMode::Isolated => MarginMode::Isolated,
        };

        let public_instrument = self
            .public_data
            .instrument(rules.instrument.instrument_type, &plan.instrument_id);
        let price_limit = self.public_data.price_limit(&plan.instrument_id);
        let system_status = self.public_data.system_status("ongoing");
        let account_instrument = self
            .executor_account
            .instrument(rules.instrument.instrument_type, &plan.instrument_id);

        let (public_instrument, price_limit, system_status, account_instrument) = tokio::try_join!(
            public_instrument,
            price_limit,
            system_status,
            account_instrument
        )?;

        let max_order_size = if plan.action.is_risk_increasing() {
            Some(
                self.executor_account
                    .max_order_size(&plan.instrument_id, margin_mode, &plan.price)
                    .await?,
            )
        } else {
            None
        };

        Ok(VenueExecutionEvidence::from_okx(
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            public_instrument,
            account_instrument,
            price_limit,
            max_order_size,
            system_status,
        )?)
    }

    pub async fn clock_evidence(&self) -> Result<ClockEvidence, okx_api::OkxError> {
        self.executor_clock.clock_evidence().await
    }

    pub async fn account_rate_limit_evidence(
        &self,
    ) -> Result<AccountRateLimitEvidence, okx_api::OkxError> {
        TradeApi::new(self.executor_clock.clone())
            .account_rate_limit()
            .await
    }

    pub fn rate_budget_snapshot(&self) -> RateBudgetSnapshot {
        self.executor_clock.rate_budget().snapshot()
    }

    pub async fn prepare(
        &self,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        self.prepare_with_lineage(plan, None, observed_at_ms).await
    }

    pub async fn prepare_with_lineage(
        &self,
        plan: ExecutionPlan,
        lineage: Option<ExecutionLineageBinding>,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .prepare_with_lineage(plan, lineage, observed_at_ms)
    }

    pub async fn prepare_reverse_close(
        &self,
        plan: ExecutionPlan,
        target_position_side: PositionSide,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        self.prepare_reverse_close_with_lineage(
            plan,
            target_position_side,
            None,
            observed_at_ms,
        )
        .await
    }

    pub async fn prepare_reverse_close_with_lineage(
        &self,
        plan: ExecutionPlan,
        target_position_side: PositionSide,
        lineage: Option<ExecutionLineageBinding>,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .prepare_reverse_close_with_lineage(
                plan,
                target_position_side,
                lineage,
                observed_at_ms,
            )
    }

    pub async fn prepare_reverse_open(
        &self,
        root_intent_id: &str,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .prepare_reverse_open(root_intent_id, plan, observed_at_ms)
    }

    pub async fn abort_reverse(
        &self,
        root_intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .abort_reverse(root_intent_id, observed_at_ms)
    }

    pub async fn entry(&self, intent_id: &str) -> Option<ExecutionLedgerEntry> {
        self.executor.lock().await.ledger().get(intent_id).cloned()
    }

    pub async fn status(&self, intent_id: &str) -> AgentResult<Option<ExecutionStatusEnvelope>> {
        let executor = self.executor.lock().await;
        execution_status_with_ledger(executor.ledger(), intent_id).map_err(AgentError::from)
    }

    pub async fn live_trading_enabled(&self) -> bool {
        self.executor.lock().await.live_trading_enabled()
    }

    pub async fn submit_prepared(
        &self,
        intent_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> Result<SubmitDisposition, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .submit_prepared(intent_id, timing, observed_at_ms)
            .await
    }
}
