use std::path::Path;

use chrono::{SecondsFormat, Utc};
use okx_api::{
    AccountApi, ClockEvidence, Credentials, MarginMode, MutationTiming, OkxEnvironment,
    OkxPublicClient, OkxRestClient, PublicDataApi, TradeApi,
};
use okx_execution::{
    DurableExecutionLedger, ExecutionLedgerEntry, ExecutionLedgerStore, ExecutionPlan,
    ExecutionStatusSnapshot, OrderExecutor, OrderExecutorError, PrepareOutcome, SubmitDisposition,
    execution_status,
};
use okx_observation::{AccountSnapshot, InstrumentRulesSnapshot, VenueExecutionEvidence};
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
    ) -> AgentResult<Self> {
        let public_data = PublicDataApi::new(OkxPublicClient::new(environment)?);
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
            public_data,
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

        let (public_instrument, price_limit, system_status, account_instrument) =
            tokio::try_join!(
                public_instrument,
                price_limit,
                system_status,
                account_instrument
            )?;

        let max_order_size = if plan.action == okx_execution::ExecutionAction::Open {
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
