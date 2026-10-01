use std::path::Path;

use okx_api::{AccountApi, ClockEvidence, Credentials, MutationTiming, OkxEnvironment, OkxRestClient, TradeApi};
use okx_execution::{
    DurableExecutionLedger, ExecutionLedgerEntry, ExecutionLedgerStore, ExecutionPlan,
    ExecutionStatusSnapshot, OrderExecutor, OrderExecutorError, PrepareOutcome, SubmitDisposition,
    execution_status,
};
use okx_observation::AccountSnapshot;
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
    executor: Mutex<OrderExecutor<TradeApi>>,
}

impl ExecutionRuntime {
    pub fn new(
        root: &Path,
        environment: OkxEnvironment,
        executor_credentials: Credentials,
        observed_at_ms: u64,
    ) -> AgentResult<Self> {
        let rest = OkxRestClient::new(environment, executor_credentials)?;
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
            executor: Mutex::new(OrderExecutor::new(ledger, trade)),
        })
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

    pub async fn clock_evidence(&self) -> Result<ClockEvidence, okx_api::OkxError> {
        self.executor_clock.clock_evidence().await
    }

    pub async fn prepare(
        &self,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareOutcome, OrderExecutorError> {
        self.executor.lock().await.prepare(plan, observed_at_ms)
    }

    pub async fn entry(&self, intent_id: &str) -> Option<ExecutionLedgerEntry> {
        self.executor.lock().await.ledger().get(intent_id).cloned()
    }

    pub async fn status(&self, intent_id: &str) -> AgentResult<Option<ExecutionStatusSnapshot>> {
        let executor = self.executor.lock().await;
        executor
            .ledger()
            .get(intent_id)
            .map(execution_status)
            .transpose()
            .map_err(AgentError::from)
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
