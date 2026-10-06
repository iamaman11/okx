use std::path::Path;

use chrono::{SecondsFormat, Utc};
use okx_api::{
    AccountApi, AccountRateLimitEvidence, ClockEvidence, Credentials, MarginMode, MutationTiming,
    OkxEnvironment, OkxPublicClient, OkxRestClient, PublicDataApi, RateBudget, RateBudgetSnapshot,
    TradeApi,
};
use okx_execution::{
    AccountLedgerReconciliation, AccountLedgerReconciliationError, DurableExecutionLedger,
    ExecutionLedgerEntry, ExecutionLedgerError, ExecutionLedgerStore, ExecutionLineageBinding,
    ExecutionPlan, ExecutionStatusEnvelope, MutationAuthority, OrderExecutor, OrderExecutorError,
    PositionSide, PrepareOutcome, ReconcileDisposition, SubmitDisposition,
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
        ExecutorCredentialPreflight, evaluate_demo_executor_preflight_against_snapshot,
        evaluate_executor_preflight_against_snapshot,
    },
};

pub const EXECUTION_PREPARED_SCHEMA_V1: &str = "okx.execution-prepared/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRuntimeMode {
    ReadOnly,
    DemoAcceptance,
}

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
    mode: ExecutionRuntimeMode,
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
        Self::new_with_mode(
            root,
            environment,
            executor_credentials,
            observed_at_ms,
            rate_budget,
            ExecutionRuntimeMode::ReadOnly,
        )
    }

    pub fn new_demo_acceptance(
        root: &Path,
        environment: OkxEnvironment,
        executor_credentials: Credentials,
        observed_at_ms: u64,
        rate_budget: RateBudget,
    ) -> AgentResult<Self> {
        if !environment.demo {
            return Err(OrderExecutorError::DemoAuthorityRequiresDemoEnvironment.into());
        }
        Self::new_with_mode(
            root,
            environment,
            executor_credentials,
            observed_at_ms,
            rate_budget,
            ExecutionRuntimeMode::DemoAcceptance,
        )
    }

    fn new_with_mode(
        root: &Path,
        environment: OkxEnvironment,
        executor_credentials: Credentials,
        observed_at_ms: u64,
        rate_budget: RateBudget,
        mode: ExecutionRuntimeMode,
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
            mode,
            executor_clock,
            executor_account,
            public_data,
            executor: Mutex::new(OrderExecutor::new(ledger, trade)),
        })
    }

    pub const fn mode(&self) -> ExecutionRuntimeMode {
        self.mode
    }

    pub const fn demo_mutation_acceptance_requested(&self) -> bool {
        matches!(self.mode, ExecutionRuntimeMode::DemoAcceptance)
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

    pub async fn demo_acceptance_preflight(
        &self,
        observer: &AccountSnapshot,
    ) -> AgentResult<ExecutorCredentialPreflight> {
        let executor = self.executor_account.config().await?;
        Ok(evaluate_demo_executor_preflight_against_snapshot(
            self.environment,
            observer,
            &executor,
        ))
    }

    pub async fn preflight_for_runtime(
        &self,
        observer: &AccountSnapshot,
    ) -> AgentResult<ExecutorCredentialPreflight> {
        match self.mode {
            ExecutionRuntimeMode::ReadOnly => self.preflight(observer).await,
            ExecutionRuntimeMode::DemoAcceptance => self.demo_acceptance_preflight(observer).await,
        }
    }

    pub async fn mutation_authority(&self) -> MutationAuthority {
        self.executor.lock().await.mutation_authority()
    }

    pub async fn submit_prepared_demo_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        intent_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> AgentResult<Option<Result<SubmitDisposition, OrderExecutorError>>> {
        if self.mode != ExecutionRuntimeMode::DemoAcceptance || !preflight.accepted {
            self.executor.lock().await.disable_mutations();
            return Ok(None);
        }

        let mut executor = self.executor.lock().await;
        executor.enable_demo_acceptance(self.environment)?;
        let result = executor
            .submit_prepared(intent_id, timing, observed_at_ms)
            .await;
        executor.disable_mutations();
        Ok(Some(result))
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
        self.prepare_reverse_close_with_lineage(plan, target_position_side, None, observed_at_ms)
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
            .prepare_reverse_close_with_lineage(plan, target_position_side, lineage, observed_at_ms)
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

    pub async fn reconcile_execution(
        &self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ReconcileDisposition, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .reconcile(intent_id, observed_at_ms)
            .await
    }

    pub async fn status(&self, intent_id: &str) -> AgentResult<Option<ExecutionStatusEnvelope>> {
        Ok(self
            .status_with_entry(intent_id)
            .await?
            .map(|(_, status)| status))
    }

    pub async fn status_with_entry(
        &self,
        intent_id: &str,
    ) -> AgentResult<Option<(ExecutionLedgerEntry, ExecutionStatusEnvelope)>> {
        let executor = self.executor.lock().await;
        let Some(entry) = executor.ledger().get(intent_id).cloned() else {
            return Ok(None);
        };
        let status =
            execution_status_with_ledger(executor.ledger(), intent_id)?.ok_or_else(|| {
                AgentError::ExecutionLedger(ExecutionLedgerError::IntentNotFound(
                    intent_id.to_owned(),
                ))
            })?;
        Ok(Some((entry, status)))
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

#[cfg(test)]
mod tests {
    use std::{
        fs, process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use okx_api::Region;

    use super::*;

    fn credentials() -> Credentials {
        Credentials::new(
            "demo-key".to_owned(),
            "demo-secret".to_owned(),
            "demo-pass".to_owned(),
        )
        .expect("credentials")
    }

    fn temp_root(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("okx-{label}-{}-{nonce}", process::id()))
    }

    #[test]
    fn demo_acceptance_runtime_rejects_production_environment_before_construction() {
        let result = ExecutionRuntime::new_demo_acceptance(
            Path::new("."),
            OkxEnvironment::new(Region::Global, false),
            credentials(),
            1,
            RateBudget::new(),
        );
        assert!(matches!(
            result,
            Err(AgentError::OrderExecutor(
                OrderExecutorError::DemoAuthorityRequiresDemoEnvironment
            ))
        ));
    }

    #[tokio::test]
    async fn demo_acceptance_runtime_starts_with_exchange_mutation_disabled() {
        let root = temp_root("demo-runtime-disabled");
        fs::create_dir_all(&root).expect("root");
        let runtime = ExecutionRuntime::new_demo_acceptance(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            1,
            RateBudget::new(),
        )
        .expect("demo runtime");

        assert_eq!(runtime.mode(), ExecutionRuntimeMode::DemoAcceptance);
        assert!(runtime.demo_mutation_acceptance_requested());
        assert_eq!(
            runtime.mutation_authority().await,
            MutationAuthority::Disabled
        );
        assert!(!runtime.live_trading_enabled().await);

        let _ = fs::remove_dir_all(root);
    }
}
