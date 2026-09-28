use std::path::Path;

use okx_api::{AccountApi, Credentials, OkxEnvironment, OkxRestClient, TradeApi};
use okx_execution::{
    DurableExecutionLedger, ExecutionLedgerEntry, ExecutionLedgerStore, ExecutionPlan,
    OrderExecutor, OrderExecutorError, PrepareDisposition, SubmitDisposition,
};
use okx_observation::AccountSnapshot;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::{
    AgentResult,
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
        let executor_account = AccountApi::new(rest.clone());
        let trade = TradeApi::new(rest);
        let ledger = DurableExecutionLedger::open(
            ExecutionLedgerStore::at(root.join("execution-ledger.json")),
            observed_at_ms,
        )?;
        Ok(Self {
            environment,
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

    pub async fn prepare(
        &self,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> AgentResult<PreparedExecutionResult> {
        let mut executor = self.executor.lock().await;
        let disposition = executor.prepare(plan, observed_at_ms)?;
        let (disposition, entry) = match disposition {
            PrepareDisposition::Created(entry) => (PreparedDisposition::Created, entry),
            PrepareDisposition::Existing(entry) => (PreparedDisposition::Existing, entry),
        };
        Ok(PreparedExecutionResult {
            schema: EXECUTION_PREPARED_SCHEMA_V1,
            disposition,
            entry,
        })
    }

    pub async fn entry(&self, intent_id: &str) -> Option<ExecutionLedgerEntry> {
        self.executor.lock().await.ledger().get(intent_id).cloned()
    }

    pub async fn live_trading_enabled(&self) -> bool {
        self.executor.lock().await.live_trading_enabled()
    }

    pub async fn submit_prepared(
        &self,
        intent_id: &str,
        exp_time_ms: u64,
        observed_at_ms: u64,
    ) -> Result<SubmitDisposition, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .submit_prepared(intent_id, exp_time_ms, observed_at_ms)
            .await
    }
}
