use std::{collections::BTreeMap, path::Path, str::FromStr, time::Duration};

use chrono::{SecondsFormat, Utc};
use okx_analysis::{PortfolioRiskAnalysis, StrategyNavRiskEvidence, durable_account_stop_reason};
use okx_api::{
    AccountApi, AccountRateLimitEvidence, ClockEvidence, Credentials, MarginMode, MutationTiming,
    OkxEnvironment, OkxPublicClient, OkxRestClient, PendingProtectiveAlgoInventory, PublicDataApi,
    RateBudget, RateBudgetSnapshot, TradeApi,
};
use okx_execution::{
    AccountLedgerReconciliation, DurableExchangeEvidence, DurableExecutionLedger, ExecutionAction,
    ExecutionLedgerEntry, ExecutionLedgerError, ExecutionLedgerStore, ExecutionLineageBinding,
    ExecutionPlan, ExecutionRiskStop, ExecutionState, ExecutionStatusEnvelope,
    FencedProtectiveShrinkRequest, LeverageAttemptState, ManagedExecutionInventory,
    MutationAuthority, MutationPrepareDisposition, MutationSubmitDisposition, OrderExecutor,
    OrderExecutorError, PositionSide, PrepareOutcome, ProductionMarkEvidence,
    StrategyAllocationOpening, StrategyCapitalReservation, StrategyFinalization,
    StrategyUnverifiedClosure, SubmitDisposition, execution_status_with_ledger,
    managed_execution_inventory, reconcile_account_ledger,
};
use okx_observation::{
    AccountLedgerFacts, AccountSnapshot, InstrumentRulesSnapshot, VenueExecutionEvidence,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tokio::time::timeout;

use crate::{
    AgentError, AgentResult,
    execution_preflight::{
        ExecutorCredentialPreflight, ExecutorPreflightSnapshot,
        evaluate_demo_executor_preflight_against_snapshot,
        evaluate_executor_preflight_against_snapshot,
    },
};

pub const EXECUTION_PREPARED_SCHEMA_V1: &str = "okx.execution-prepared/v1";
pub const LEVERAGE_CHANGE_PROOF_SCHEMA_V1: &str = "okx.leverage-change-proof/v1";

/// A Hedge Cross setting is accepted only when BOTH distinct sides are
/// individually verified and have the same exact, positive leverage.
/// Account generations hash mutable snapshots and legitimately advance during a
/// financial run. Only UID is stable across refreshes. The *current* snapshot
/// generation is separately fenced against exchange ledger facts and private WS
/// in the query admission before entering this executor-owned critical section.
fn same_leverage_account(opened_uid: &str, observed_uid: &str) -> bool {
    !opened_uid.is_empty() && opened_uid == observed_uid
}

fn exact_hedge_cross_leverage(
    rows: &[okx_api::LeverageInfo],
    instrument: &str,
) -> Option<rust_decimal::Decimal> {
    if rows.len() != 2 {
        return None;
    }
    let mut long = None;
    let mut short = None;
    for row in rows {
        if row.instrument_id != instrument || row.margin_mode != "cross" {
            return None;
        }
        let value = rust_decimal::Decimal::from_str(&row.lever).ok()?;
        if value <= rust_decimal::Decimal::ZERO {
            return None;
        }
        match row.position_side.as_str() {
            "long" if long.replace(value).is_none() => {}
            "short" if short.replace(value).is_none() => {}
            _ => return None,
        }
    }
    long.filter(|value| Some(*value) == short)
}

#[derive(Debug, Clone, Serialize)]
pub struct LeverageChangeProof {
    pub schema: &'static str,
    pub mutation_id: String,
    pub instrument: String,
    pub margin_mode: &'static str,
    pub previous_leverage: String,
    pub verified_leverage: String,
    pub changed: bool,
    pub verified_both_sides: bool,
    pub account_uid_fingerprint: String,
}

/// The immutable short-lived canceled-order source window is two hours.
/// For quiet runs, refresh from the existing authenticated REST/WS owner
/// halfway through that window. This is not a timer, a new owner, or a
/// promise that an agent stopped for two hours can recover old history.
const QUIET_SOURCE_COVERAGE_REFRESH_MS: u64 = 3_600_000;

fn quiet_source_capture_due(
    run_started_ms: u64,
    latest_source_ms: Option<u64>,
    now_ms: u64,
) -> bool {
    now_ms >= run_started_ms
        && now_ms.saturating_sub(latest_source_ms.unwrap_or(run_started_ms))
            >= QUIET_SOURCE_COVERAGE_REFRESH_MS
}

/// CONTROL is the only writer of this already-existing atomic desired file.
/// The agent reads a typed, minimal view immediately before Production POST;
/// no second credentials store, policy daemon or JSON application logic.
const NATIVE_CONTROL_DESIRED: &str = r"C:\okx-control\desired.json";

#[derive(Deserialize)]
struct ProductionControlSnapshot {
    schema: String,
    agent: String,
    profile: String,
    read_only: bool,
    production_policy: Option<okx_protocol::ProductionTradingPolicy>,
    transition: Option<serde_json::Value>,
    switch_hold: Option<serde_json::Value>,
}

fn parse_native_production_policy(
    bytes: &[u8],
) -> AgentResult<okx_execution::ProductionTradingPolicy> {
    let owner: ProductionControlSnapshot = serde_json::from_slice(bytes)?;
    if owner.schema != "okx.host.desired/v3"
        || owner.agent != "RUNNING"
        || owner.profile != "production"
        || owner.read_only
        || owner.transition.is_some()
        || owner.switch_hold.is_some()
    {
        return Err(AgentError::ProductionPolicyUnavailable);
    }
    let policy = owner.production_policy.ok_or(AgentError::ProductionPolicyUnavailable)?;
    policy.validate()?;
    if !matches!(
        policy.permission,
        okx_protocol::ProductionPermission::Enabled
            | okx_protocol::ProductionPermission::Stopped
    ) {
        return Err(AgentError::ProductionPolicyUnavailable);
    }
    let permission = match policy.permission {
        okx_protocol::ProductionPermission::Enabled => okx_execution::ProductionPermission::Enabled,
        okx_protocol::ProductionPermission::Stopped => okx_execution::ProductionPermission::Stopped,
        _ => return Err(AgentError::ProductionPolicyUnavailable),
    };
    Ok(okx_execution::ProductionTradingPolicy {
        schema: policy.schema,
        revision: policy.revision,
        permission,
        account_uid_fingerprint: policy.account_uid_fingerprint,
        strategy_run_id: policy.strategy_run_id,
        operator_authority_id: policy.operator_authority_id,
        allowed_instruments: policy.allowed_instruments,
        max_total_modelled_loss_usdt: policy.max_total_modelled_loss_usdt,
    })
}

fn read_native_production_policy() -> AgentResult<okx_execution::ProductionTradingPolicy> {
    let bytes = std::fs::read(NATIVE_CONTROL_DESIRED)?;
    parse_native_production_policy(&bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionReconciliation {
    NotRequired,
    Reconciled,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRuntimeMode {
    ReadOnly,
    DemoAcceptance,
    Production,
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

/// Serializes the SINGLE mutation ingress with native profile drain.
///
/// Each mutating request holds a read guard across *all* its REST/WS checks,
/// durable changes and any exchange POST. Drain takes the exclusive write
/// guard: it cannot complete before any already admitted POST resolves.
/// Unlike a standalone freeze service this is only a lock owned by the
/// existing ExecutionRuntime, with no new process, socket or journal.
struct MutationIngress {
    accepting: RwLock<bool>,
}

impl MutationIngress {
    fn new() -> Self {
        Self {
            accepting: RwLock::new(true),
        }
    }

    async fn admit(&self) -> Option<RwLockReadGuard<'_, bool>> {
        let guard = self.accepting.read().await;
        (*guard).then_some(guard)
    }

    /// Bounded; timeout is NOT proof that an in-flight order was canceled.
    /// A timed-out drain must never lead to termination or profile change.
    async fn drain(&self) -> Result<(), tokio::time::error::Elapsed> {
        let mut guard = timeout(Duration::from_secs(15), self.accepting.write()).await?;
        *guard = false;
        Ok(())
    }
}

pub struct ExecutionRuntime {
    mutation_ingress: MutationIngress,
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

    /// Same ledger and gateway as Demo, selected only by the native CONTROL
    /// child owner and its atomic, durable policy. No ephemeral pilot.
    pub fn new_production(
        root: &Path,
        environment: OkxEnvironment,
        executor_credentials: Credentials,
        observed_at_ms: u64,
        rate_budget: RateBudget,
    ) -> AgentResult<Self> {
        if environment.demo {
            return Err(AgentError::ProductionPolicyUnavailable);
        }
        read_native_production_policy()?;
        Self::new_with_mode(
            root,
            environment,
            executor_credentials,
            observed_at_ms,
            rate_budget,
            ExecutionRuntimeMode::Production,
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
        // Market/reference and venue-public risk inputs are always real
        // Production public data; Demo is ONLY the private order/account venue.
        let public_data = PublicDataApi::new(OkxPublicClient::with_rate_budget(
            environment.production_public(),
            rate_budget.clone(),
        )?);
        let rest = OkxRestClient::with_rate_budget(environment, executor_credentials, rate_budget)?;
        let executor_clock = rest.clone();
        let executor_account = AccountApi::new(rest.clone());
        let trade = TradeApi::new(rest);
        let ledger = DurableExecutionLedger::open(
            // The existing Windows owner already supplies disjoint roots:
            // Demo C:\okx-runtime\demo and Production C:\okx-runtime.
            // Never silently strand either profile's durable history.
            ExecutionLedgerStore::at(root.join("execution-ledger.json")),
            observed_at_ms,
        )?;
        Ok(Self {
            mutation_ingress: MutationIngress::new(),
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

    /// The request holds the returned guard THROUGH the final executor
    /// response. A drain cannot slip between preflight and actual venue POST.
    /// Financial/read-only observation does not need this mutation guard.
    pub async fn admit_mutating_request(&self) -> Option<RwLockReadGuard<'_, bool>> {
        self.mutation_ingress.admit().await
    }

    /// Internal native-only primitive. This is NOT a CONTROL authorization
    /// or public agent command: the caller must still prove a fresh FLAT
    /// account and bind a trusted PID/UID/WS/ledger witness independently.
    /// No IPC caller is wired until the full single-owner handoff is accepted.
    pub async fn drain_mutation_ingress(
        &self,
        account_uid: &str,
    ) -> AgentResult<okx_execution::ProfileHandoffLedgerAssessment> {
        // One overall deadline for both the active request fence and native
        // ledger owner: an unbounded checkpoint must never hang CONTROL.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        self.mutation_ingress
            .drain()
            .await
            .map_err(|_| AgentError::SourceDrainTimeout)?;
        // Covers any native financial checkpoint outside request ingress.
        // The source must never interpret missing archive evidence as clean.
        let mut executor = tokio::time::timeout_at(deadline, self.executor.lock())
            .await
            .map_err(|_| AgentError::SourceDrainTimeout)?;
        executor.disable_mutations();
        Ok(executor
            .ledger()
            .profile_handoff_ledger_assessment(account_uid)?)
    }

    /// Single admission authority reused by every exchange POST wrapper.
    /// The operator's runtime mode and the independent Demo REST preflight
    /// must agree on the SAME UID as the entire durable managed inventory.
    fn demo_mutation_admitted(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        ledger: &DurableExecutionLedger,
    ) -> bool {
        self.environment.demo
            && self.mode == ExecutionRuntimeMode::DemoAcceptance
            && preflight.accepted
            && preflight.credential.accepted
            && !preflight.credential.production_environment
            && !preflight.credential.account_uid_fingerprint.is_empty()
            && ledger.entries().all(|entry| {
                entry.record.plan.account_uid_fingerprint
                    == preflight.credential.account_uid_fingerprint
            })
    }

    pub const fn demo_mutation_acceptance_requested(&self) -> bool {
        matches!(self.mode, ExecutionRuntimeMode::DemoAcceptance)
    }

    pub const fn production_trading_requested(&self) -> bool {
        matches!(self.mode, ExecutionRuntimeMode::Production) && !self.environment.demo
    }

    /// Return the exchange reconciliation and exact identity inventory under
    /// one ledger lock so the two views cannot observe different intent sets.
    /// This does not perform a gateway send or a mutation.
    /// Read-only admission for the in-process private-order event source.
    /// The executor ledger remains the sole owner of durable evidence.
    pub async fn strategy_financial_capture_active(&self) -> bool {
        if self.mode != ExecutionRuntimeMode::DemoAcceptance || !self.environment.demo {
            return false;
        }
        let executor = self.executor.lock().await;
        executor.ledger().capital_reservation().is_some()
            && executor.ledger().finalized_run().is_none()
            && executor.ledger().unverified_run().is_none()
    }

    /// Read-only, bounded decision for the EXISTING mailbox heartbeat.
    /// Never accepts a request-supplied cursor or treats a >2h gap as healed.
    pub async fn strategy_quiet_source_capture_due(&self, now_ms: u64) -> bool {
        if self.mode != ExecutionRuntimeMode::DemoAcceptance || !self.environment.demo {
            return false;
        }
        let executor = self.executor.lock().await;
        let ledger = executor.ledger();
        let Some(run) = ledger.capital_reservation() else {
            return false;
        };
        if ledger.finalized_run().is_some() || ledger.unverified_run().is_some() {
            return false;
        }
        quiet_source_capture_due(
            run.started_at_ms,
            ledger
                .exchange_evidence_summary()
                .and_then(|source| source.latest_source_asof_ms),
            now_ms,
        )
    }

    pub async fn reconcile_account_ledger_with_intents(
        &self,
        facts: &AccountLedgerFacts,
        coherent: bool,
        production_marks: &BTreeMap<String, ProductionMarkEvidence>,
    ) -> AgentResult<(AccountLedgerReconciliation, ManagedExecutionInventory)> {
        // Account observation remains READONLY during drain. Existing
        // event-driven evidence/NAV checkpoints may mutate the native ledger,
        // so they share the same ingress fence as mutating command dispatch.
        // A drained source returns the last durable ledger without appending.
        let capture_guard = self.mutation_ingress.admit().await;
        let mut executor = self.executor.lock().await;
        let mut reconciliation = reconcile_account_ledger(executor.ledger(), facts)?;
        let inventory = managed_execution_inventory(executor.ledger());
        // Never capture a contradictory or private-WS-incoherent snapshot.
        let mut current_source_captured = false;
        if executor.ledger().capital_reservation().is_some()
            && executor.ledger().unverified_run().is_none()
        {
            if capture_guard.is_some()
                && coherent
                && reconciliation.consistent
                && okx_execution::DurableExchangeEvidence::complete_source_checkpoint(facts)
            {
                executor.capture_exchange_evidence(facts)?;
                current_source_captured = true;
            } else {
                reconciliation.source_checkpoint_deferred = true;
            }
        }
        reconciliation.exchange_evidence = executor.ledger().exchange_evidence_summary();
        reconciliation.financial_period = if executor.ledger().unverified_run().is_some() {
            None
        } else {
            executor.ledger().financial_period_with_verified_source(
                facts,
                coherent,
                production_marks,
                current_source_captured,
            )
        };
        if current_source_captured && let Some(report) = reconciliation.financial_period.as_ref() {
            // Only a completely reconciled read may append a NAV watermark.
            // The stop, if triggered, is saved in the SAME atomic file.
            let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
            executor.observe_strategy_nav(report, facts, now)?;
        }
        reconciliation.strategy_nav_watermark = executor.ledger().strategy_nav_watermark().cloned();
        reconciliation.strategy_nav_checkpoint_count = executor.ledger().strategy_nav_point_count();
        reconciliation.strategy_risk_stop = executor.ledger().risk_stop().cloned();
        reconciliation.strategy_stop_rearm_proofs = executor.ledger().stop_rearm_proofs();
        reconciliation.unverified_run = executor.ledger().unverified_run().cloned();
        reconciliation.unverified_run_proofs = executor.ledger().archived_unverified_proofs();
        Ok((reconciliation, inventory))
    }

    /// Conservative FIRST-OPEN-only NAV admission. The full strategy NAV
    /// high-water/day-loss time series is not yet attested; after the first
    /// managed execution, further risk increases stay fail-closed.
    /// This proof is built by the one executor from immutable run and source
    /// evidence; no operator/transport-supplied NAV is accepted.
    pub async fn initial_strategy_nav_for_risk(
        &self,
        facts: &AccountLedgerFacts,
        plan: &ExecutionPlan,
        coherent: bool,
        settle_currency: Option<&str>,
        contract_type: Option<&str>,
    ) -> Option<StrategyNavRiskEvidence> {
        if self.mode != ExecutionRuntimeMode::DemoAcceptance
            || !self.environment.demo
            || !coherent
            || plan.action != ExecutionAction::Open
            || !DurableExchangeEvidence::complete_source_checkpoint(facts)
            || facts.summary.open_positions != 0
            || facts.summary.pending_orders != 0
            || !facts.usdt_margin_excludes_other_currencies()
            || settle_currency != Some("USDT")
            || contract_type != Some("linear")
        {
            return None;
        }
        let mut executor = self.executor.lock().await;
        let ledger = executor.ledger();
        let run = ledger.capital_reservation()?.clone();
        let binding = plan.risk_binding.as_ref()?;
        // Trading equity must remain equal to the source-authenticated
        // opening reserve. External cash transfers are not strategy PnL.
        let current_equity = facts.trading_usdt_equity.as_deref()?;
        if ledger.finalized_run().is_some()
            || current_equity != run.opening_account_usdt_equity.as_str()
            || ledger.risk_stop().is_some()
            || run.account_uid_fingerprint != facts.summary.authority.account_uid_fingerprint
            || run.account_uid_fingerprint != plan.account_uid_fingerprint
            || run.mandate_version != binding.mandate.version
            || run.allocated_usdt != binding.mandate.capital_base_usd
            || ledger.entries().any(|e| {
                e.created_at_ms >= run.started_at_ms
                    && (e.record.plan.intent_id != plan.intent_id
                        || e.record.plan != *plan
                        || e.record.state != ExecutionState::Prepared)
            })
            || !reconcile_account_ledger(ledger, facts).ok()?.consistent
        {
            return None;
        }
        // A captured initial source cursor is persisted atomically before
        // the policy is evaluated. Missing/raced windows never mint NAV.
        executor
            .capture_exchange_evidence_fresh_cursor(facts)
            .ok()?;
        let source = executor.ledger().exchange_evidence_summary()?;
        if source.events != 0 || !source.latest_source_complete || source.source_gap_over_two_hours
        {
            return None;
        }
        let projection = executor.ledger().financial_period(facts, coherent)?;
        // The same unsent PREPARED intent is deliberately INCOMPLETE for
        // financial finalization, but it has no exchange fills or positions.
        // Allow exactly that single reason during its independent pre-POST
        // revalidation; all other missing evidence remains a hard rejection.
        let same_unsent_prepared = executor.ledger().entries().any(|e| {
            e.created_at_ms >= run.started_at_ms
                && e.record.plan.intent_id == plan.intent_id
                && e.record.plan == *plan
                && e.record.state == ExecutionState::Prepared
        });
        let clean_projection = if same_unsent_prepared {
            projection.state == "INCOMPLETE"
                && projection.reasons == vec!["unresolved_managed_execution"]
                && projection.nav_usdt.is_none()
        } else {
            projection.state == "FLAT_RECONCILED"
                && projection.reasons.is_empty()
                && projection.nav_usdt.as_deref() == Some(run.allocated_usdt.as_str())
                && projection.unrealized_usdt.as_deref() == Some("0")
        };
        if !clean_projection
            || projection.linked_fills != 0
            || projection.linked_bills != 0
            || !projection.positions.is_empty()
            || projection.realized_usdt != "0"
            || projection.fees_usdt != "0"
            || projection.funding_usdt.is_some()
        {
            return None;
        }
        Some(StrategyNavRiskEvidence {
            run_id: run.allocation_id,
            mandate_version: run.mandate_version,
            allocation_usdt: run.allocated_usdt.clone(),
            nav_usdt: run.allocated_usdt.clone(),
            high_water_usdt: run.allocated_usdt,
            daily_realized_loss_usdt: "0".to_owned(),
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
            ExecutionRuntimeMode::ReadOnly | ExecutionRuntimeMode::Production => {
                self.preflight(observer).await
            }
            ExecutionRuntimeMode::DemoAcceptance => self.demo_acceptance_preflight(observer).await,
        }
    }

    pub async fn mutation_authority(&self) -> MutationAuthority {
        self.executor.lock().await.mutation_authority()
    }

    /// One existing native executor mutex spans read/one optional POST/readback.
    /// Never retry an uncertain POST, never use Production credentials.
    pub async fn lower_cross_leverage_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        mutation_id: &str,
        instrument: &str,
        requested: &str,
    ) -> Option<Result<LeverageChangeProof, String>> {
        let mut executor = self.executor.lock().await;
        if !self.environment.demo
            || self.mode != ExecutionRuntimeMode::DemoAcceptance
            || !preflight.accepted
            || preflight.production_environment
            || !preflight.futures_mode
            || !preflight.long_short_mode
            || !preflight.executor_trade_permission
            || preflight.account_uid_fingerprint.is_empty()
            || !instrument.ends_with("-USDT-SWAP")
            || executor.ledger().capital_reservation().is_none_or(|r| {
                !same_leverage_account(
                    &r.account_uid_fingerprint,
                    &preflight.account_uid_fingerprint,
                )
            })
            || executor.ledger().entries().any(|e| {
                executor
                    .ledger()
                    .capital_reservation()
                    .is_some_and(|r| e.created_at_ms >= r.started_at_ms)
            })
        {
            return None;
        }
        let desired = rust_decimal::Decimal::from_str(requested).ok()?;
        if desired <= rust_decimal::Decimal::ZERO {
            return None;
        }
        // A PENDING attempt was durably saved before the first POST. On any
        // later invocation, including after restart, the only allowed work
        // for that id is independent GET and a local state transition.
        let prior_attempt = executor
            .ledger()
            .leverage_change_attempt(mutation_id)
            .cloned();
        if prior_attempt.as_ref().is_some_and(|a| {
            a.account_uid_fingerprint != preflight.account_uid_fingerprint
                || a.instrument != instrument
                || rust_decimal::Decimal::from_str(&a.requested_leverage).ok() != Some(desired)
        }) {
            return Some(Err(
                "leverage mutation id is bound to a different immutable request".to_owned(),
            ));
        }
        let current_rows = match self
            .executor_account
            .leverage(instrument, MarginMode::Cross)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                return Some(Err(format!("pre-write leverage read unavailable: {error}")));
            }
        };
        let Some(current) = exact_hedge_cross_leverage(&current_rows, instrument) else {
            return Some(Err("initial long/short leverage is not exact".to_owned()));
        };
        if let Some(existing) = prior_attempt {
            if current != desired {
                return Some(Err(
                    "UNKNOWN: prior durable leverage POST may have executed; no replay is allowed"
                        .to_owned(),
                ));
            }
            if existing.state == LeverageAttemptState::Pending
                && let Err(error) = executor.verify_leverage_change(mutation_id)
            {
                return Some(Err(format!(
                    "UNKNOWN: leverage proven but receipt not persisted: {error}"
                )));
            }
            return Some(Ok(LeverageChangeProof {
                schema: LEVERAGE_CHANGE_PROOF_SCHEMA_V1,
                mutation_id: mutation_id.to_owned(),
                instrument: instrument.to_owned(),
                margin_mode: "cross",
                previous_leverage: existing.previous_leverage,
                verified_leverage: desired.to_string(),
                changed: true,
                verified_both_sides: true,
                account_uid_fingerprint: preflight.account_uid_fingerprint.clone(),
            }));
        }
        if desired > current {
            return Some(Err("leverage increases are not supported".to_owned()));
        }
        if desired != current {
            // Persist PENDING in the same crash-safe execution ledger *before*
            // the sole possible POST; no retry with same id or another id
            // for this instrument while UNKNOWN.
            let observed_at_ms = chrono::Utc::now().timestamp_millis();
            let Some(observed_at_ms) = u64::try_from(observed_at_ms).ok() else {
                return Some(Err(
                    "system clock invalid for durable leverage fence".to_owned()
                ));
            };
            if let Err(error) = executor.begin_leverage_change(
                mutation_id,
                instrument,
                requested,
                &current.to_string(),
                &preflight.account_uid_fingerprint,
                observed_at_ms,
            ) {
                return Some(Err(format!(
                    "durable pre-POST leverage fence rejected: {error}"
                )));
            }
            let _post_result = self
                .executor_account
                .set_cross_swap_leverage(instrument, requested)
                .await;
            let verification = match self
                .executor_account
                .leverage(instrument, MarginMode::Cross)
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    return Some(Err(format!(
                        "UNKNOWN: post-write leverage readback unavailable: {error}"
                    )));
                }
            };
            if exact_hedge_cross_leverage(&verification, instrument) != Some(desired) {
                return Some(Err(
                    "UNKNOWN: requested leverage not independently verified; do not replay POST"
                        .to_owned(),
                ));
            }
            if let Err(error) = executor.verify_leverage_change(mutation_id) {
                return Some(Err(format!(
                    "UNKNOWN: verified exchange setting but durable receipt failed: {error}"
                )));
            }
        }
        Some(Ok(LeverageChangeProof {
            schema: LEVERAGE_CHANGE_PROOF_SCHEMA_V1,
            mutation_id: mutation_id.to_owned(),
            instrument: instrument.to_owned(),
            margin_mode: "cross",
            previous_leverage: current.to_string(),
            verified_leverage: desired.to_string(),
            changed: current != desired,
            verified_both_sides: true,
            account_uid_fingerprint: preflight.account_uid_fingerprint.clone(),
        }))
    }

    /// Called only after the query owner has verified private account and
    /// ledger coherence. A candidate-only rejection never creates a global stop.
    pub async fn latch_account_risk_stop(
        &self,
        risk: &PortfolioRiskAnalysis,
        daily_history_complete: bool,
        observed_at_ms: u64,
    ) -> Result<Option<ExecutionRiskStop>, OrderExecutorError> {
        let Some(reason) = durable_account_stop_reason(&risk.violations, daily_history_complete)
        else {
            return Ok(None);
        };
        let mut executor = self.executor.lock().await;
        executor.stop_new_risk(reason, observed_at_ms).map(Some)
    }

    /// Storage maintenance lives under the SAME local executor mutex.
    /// This is not a strategy rearm, account change, or exchange operation.
    pub async fn migrate_finalized_storage_v14_demo(
        &self,
        run_id: &str,
        expected_manifest_sha256: &str,
        expected_finalized_checksum_sha256: &str,
        expected_stopped_at_ms: u64,
    ) -> Result<Option<okx_execution::LedgerStorageAttestation>, OrderExecutorError> {
        if !self.environment.demo || self.mode != ExecutionRuntimeMode::DemoAcceptance {
            return Ok(None);
        }
        let owner = self.executor.lock().await;
        owner
            .ledger()
            .migrate_finalized_storage_v14(
                run_id,
                expected_manifest_sha256,
                expected_finalized_checksum_sha256,
                expected_stopped_at_ms,
            )
            .map(Some)
            .map_err(Into::into)
    }

    /// Verified Demo-only administrative ledger mutation, no exchange POST.
    /// Source truth and profile are acquired by the existing execution query.
    pub async fn begin_strategy_run_demo_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        reservation: StrategyCapitalReservation,
        opening: &StrategyAllocationOpening,
        now_ms: u64,
        rearm: Option<&okx_execution::StrategyStopRearmRequest>,
    ) -> Result<Option<Result<StrategyCapitalReservation, OrderExecutorError>>, AgentError> {
        let mut executor = self.executor.lock().await;
        if !self.environment.demo
            || self.mode != ExecutionRuntimeMode::DemoAcceptance
            || !preflight.accepted
            || preflight.production_environment
            || preflight.account_uid_fingerprint != opening.account_uid_fingerprint
            || !executor
                .ledger()
                .entries()
                .all(|e| e.record.plan.account_uid_fingerprint == preflight.account_uid_fingerprint)
        {
            return Ok(None);
        }
        Ok(Some(executor.reserve_strategy_capital_with_rearm(
            reservation,
            opening,
            rearm,
            now_ms,
        )))
    }

    /// The normal Production strategy allocation uses the SAME durable
    /// executor/ledger and independent venue-opening witness as Demo.
    /// CONTROL's persistent UID/run/authority scope is read fresh.
    pub async fn begin_strategy_run_production_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        reservation: StrategyCapitalReservation,
        opening: &StrategyAllocationOpening,
        now_ms: u64,
        rearm: Option<&okx_execution::StrategyStopRearmRequest>,
    ) -> AgentResult<Option<Result<StrategyCapitalReservation, OrderExecutorError>>> {
        if !self.production_trading_requested()
            || !preflight.accepted
            || !preflight.production_environment
            || !preflight.executor_ip_bound
            || !preflight.executor_trade_permission
            || preflight.executor_withdraw_permission
        {
            return Ok(None);
        }
        let policy = read_native_production_policy()?;
        if policy.permission != okx_execution::ProductionPermission::Enabled
            || policy.account_uid_fingerprint != preflight.account_uid_fingerprint
            || policy.account_uid_fingerprint != opening.account_uid_fingerprint
            || policy.strategy_run_id != reservation.allocation_id
            || policy.operator_authority_id != reservation.operator_authority_id
        {
            return Ok(None);
        }
        let mut executor = self.executor.lock().await;
        if !executor.ledger().entries().all(|entry| {
            entry.record.plan.account_uid_fingerprint == preflight.account_uid_fingerprint
        }) {
            return Ok(None);
        }
        Ok(Some(executor.reserve_strategy_capital_with_rearm(
            reservation,
            opening,
            rearm,
            now_ms,
        )))
    }

    /// The single Demo executor captures the coherent source before the
    /// atomic FINALIZED write. Caller cannot supply the exchange witness.
    pub async fn finalize_strategy_run_demo_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        expected_run_id: &str,
        facts: &AccountLedgerFacts,
        coherent: bool,
        protection_absent: bool,
        observed_at_ms: u64,
    ) -> AgentResult<Option<Result<StrategyFinalization, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.environment.demo
            || self.mode != ExecutionRuntimeMode::DemoAcceptance
            || !preflight.accepted
            || preflight.production_environment
            || preflight.account_uid_fingerprint != facts.summary.authority.account_uid_fingerprint
            || executor.ledger().capital_reservation().is_none_or(|run| {
                run.allocation_id != expected_run_id
                    || run.account_uid_fingerprint != preflight.account_uid_fingerprint
            })
        {
            return Ok(None);
        }
        if executor.ledger().finalized_run().is_none() {
            executor.capture_exchange_evidence_fresh_cursor(facts)?;
        }
        Ok(Some(executor.finalize_strategy_run(
            facts,
            coherent,
            protection_absent,
            observed_at_ms,
        )))
    }

    /// Exact authorized Demo administrative quarantine; never an exchange
    /// order, never inferred from a private account balance.
    pub async fn quarantine_unverified_strategy_run_demo_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        expected_run_id: &str,
        facts: &AccountLedgerFacts,
        coherent: bool,
        protection_absent: bool,
        now_ms: u64,
    ) -> AgentResult<Option<Result<StrategyUnverifiedClosure, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.environment.demo
            || self.mode != ExecutionRuntimeMode::DemoAcceptance
            || !preflight.accepted
            || preflight.production_environment
            || preflight.account_uid_fingerprint != facts.summary.authority.account_uid_fingerprint
            || executor.ledger().capital_reservation().is_none_or(|run| {
                run.allocation_id != expected_run_id
                    || run.account_uid_fingerprint != preflight.account_uid_fingerprint
            })
        {
            return Ok(None);
        }
        Ok(Some(executor.quarantine_unverified_strategy_run(
            facts,
            coherent,
            protection_absent,
            now_ms,
        )))
    }

    /// Same single durable executor ledger, explicit disk read only; no
    /// exchange API/POST and no schema migration or recovery write.
    /// Exhaustive same-owner ledger state; no account exchange mutation.
    /// This does NOT freeze the executor or authorize a profile handoff.
    pub async fn profile_handoff_ledger_assessment(
        &self,
        account_uid: &str,
    ) -> Result<okx_execution::ProfileHandoffLedgerAssessment, OrderExecutorError> {
        let owner = self.executor.lock().await;
        Ok(owner
            .ledger()
            .profile_handoff_ledger_assessment(account_uid)?)
    }

    pub async fn attest_ledger_storage(
        &self,
    ) -> Result<okx_execution::LedgerStorageAttestation, OrderExecutorError> {
        let executor = self.executor.lock().await;
        Ok(executor.ledger().attest_storage()?)
    }

    pub async fn risk_stop_status(&self) -> Option<ExecutionRiskStop> {
        self.executor.lock().await.ledger().risk_stop().cloned()
    }

    /// Ordinary Production OPEN/CLOSE, not a pilot or separate executor.
    /// CONTROL is re-read on every submission; an absent/revoked policy
    /// rejects before any ledger SUBMITTING transition or exchange POST.
    /// The account and protection evidence are gathered from native
    /// authenticated REST/WS by the existing query admission owner.
    pub async fn submit_prepared_production_authorized(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        intent_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
        facts: &AccountLedgerFacts,
        protection: &PendingProtectiveAlgoInventory,
    ) -> AgentResult<Option<Result<SubmitDisposition, OrderExecutorError>>> {
        if !self.production_trading_requested()
            || !preflight.accepted
            || !preflight.credential.accepted
            || !preflight.credential.production_environment
            || !preflight.credential.executor_ip_bound
            || !preflight.credential.executor_trade_permission
            || preflight.credential.executor_withdraw_permission
            || facts.summary.authority.account_uid_fingerprint
                != preflight.credential.account_uid_fingerprint
            || !protection.complete_within_bound
        {
            return Ok(None);
        }
        let policy = read_native_production_policy()?;
        if policy.account_uid_fingerprint != preflight.credential.account_uid_fingerprint {
            return Ok(None);
        }
        // ONE executor mutex spans the final policy read, source risk
        // admission, durable SUBMITTING and exactly one exchange request.
        let mut executor = self.executor.lock().await;
        let Some(entry) = executor.ledger().get(intent_id) else {
            return Ok(None);
        };
        let plan = &entry.record.plan;
        let new_risk = plan.action.is_risk_increasing();
        let flat = facts.summary.open_positions == 0
            && facts.summary.pending_orders == 0
            && facts.authoritative_positions.iter().all(|position| {
                rust_decimal::Decimal::from_str(&position.position)
                    .is_ok_and(|v| v == rust_decimal::Decimal::ZERO)
            });
        // Existing exposure on ANY instrument or unowned conditional order
        // may occupy risk: require the entire venue FLAT for new OPEN.
        if new_risk && (!flat || protection.rows != 0) {
            return Ok(None);
        }
        let close_owned_exact = if plan.action == ExecutionAction::Close {
            let observed = facts.authoritative_positions.iter().filter(|position| {
                position.instrument_id == plan.instrument_id
                    && position.position_side == plan.position_side.as_str()
                    && rust_decimal::Decimal::from_str(&position.position)
                        .is_ok_and(|v| v > rust_decimal::Decimal::ZERO)
            }).collect::<Vec<_>>();
            observed.len() == 1
                && rust_decimal::Decimal::from_str(&observed[0].position)
                    .ok()
                    .is_some_and(|quantity| {
                        quantity > rust_decimal::Decimal::ZERO
                            && rust_decimal::Decimal::from_str(&plan.size).ok()
                                == Some(quantity)
                    })
                && executor.ledger().capital_reservation().is_some_and(|run| {
                    executor.ledger().entries().any(|owned| {
                        owned.record.plan.action == ExecutionAction::Open
                            && owned.record.strategy_run_id.as_deref()
                                == Some(run.allocation_id.as_str())
                            && owned.record.plan.instrument_id == plan.instrument_id
                            && owned.record.plan.position_side == plan.position_side
                            && owned.record.order_id.is_some()
                    })
                })
        } else {
            false
        };
        let finance_complete = if new_risk {
            executor.ledger().financial_period(facts, true).is_some_and(|period| {
                period.state == "FLAT_RECONCILED"
                    && period.nav_usdt.is_some()
                    && period.funding_usdt.is_some()
                    && period.reasons.is_empty()
            })
        } else {
            false
        };
        let witness = okx_execution::ProductionRiskWitness {
            finance_complete,
            // The executor independently scans durable parent/mutation/OCO
            // UNKNOWN states; this cannot override native state.
            unknown_mutations_reconciled: true,
            existing_modelled_loss_usdt: rust_decimal::Decimal::ZERO,
            exact_owned_reduce_only_close: close_owned_exact,
        };
        Ok(Some(
            executor
                .submit_prepared_production_authorized(
                    self.environment,
                    &policy,
                    &witness,
                    intent_id,
                    timing,
                    observed_at_ms,
                )
                .await,
        ))
    }

    pub async fn submit_prepared_demo_authorized(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        intent_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> AgentResult<Option<Result<SubmitDisposition, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.demo_mutation_admitted(preflight, executor.ledger()) {
            executor.disable_mutations();
            return Ok(None);
        }
        executor.enable_demo_acceptance(self.environment)?;
        let result = executor
            .submit_prepared(intent_id, timing, observed_at_ms)
            .await;
        executor.disable_mutations();
        Ok(Some(result))
    }

    /// Validate current owned OCO coverage under the SAME executor mutex,
    /// then perform the only possible prepared exchange submission.
    pub async fn submit_prepared_add_hedge_demo_fenced<F, Fut>(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        intent_id: &str,
        account: &AccountSnapshot,
        inventory: &PendingProtectiveAlgoInventory,
        timing: MutationTiming,
        final_private_ws_fence: F,
    ) -> AgentResult<Option<Result<SubmitDisposition, OrderExecutorError>>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let mut executor = self.executor.lock().await;
        if !self.demo_mutation_admitted(preflight, executor.ledger()) {
            executor.disable_mutations();
            return Ok(None);
        }
        executor.enable_demo_acceptance(self.environment)?;
        let observed_at_ms = timing.request_time_ms();
        let result = executor
            .submit_prepared_protected_add_hedge_fenced(
                intent_id,
                account,
                inventory,
                timing,
                observed_at_ms,
                final_private_ws_fence,
            )
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

    /// Native fenced admission of a second protected lot; no alternate
    /// executor, no independent trading authority.
    pub async fn prepare_protected_add_hedge_fenced<F, Fut>(
        &self,
        plan: ExecutionPlan,
        lineage: Option<ExecutionLineageBinding>,
        account: &AccountSnapshot,
        inventory: &PendingProtectiveAlgoInventory,
        observed_at_ms: u64,
        final_private_ws_fence: F,
    ) -> Result<PrepareOutcome, OrderExecutorError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        self.executor
            .lock()
            .await
            .prepare_protected_add_hedge_fenced(
                plan,
                lineage,
                account,
                inventory,
                observed_at_ms,
                final_private_ws_fence,
            )
            .await
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

    pub async fn abandon_prepared(
        &self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .abandon_prepared(intent_id, observed_at_ms)
    }

    pub async fn entry(&self, intent_id: &str) -> Option<ExecutionLedgerEntry> {
        self.executor.lock().await.ledger().get(intent_id).cloned()
    }

    pub async fn reconcile_once(
        &self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionReconciliation, OrderExecutorError> {
        // ExecutionStatus can still show exact durable state after draining,
        // but must not record new venue reconciliation after the source
        // manifest was frozen. This side effect is serialized with drain.
        let Some(_reconcile_guard) = self.mutation_ingress.admit().await else {
            return Ok(ExecutionReconciliation::Unavailable);
        };
        let mut executor = self.executor.lock().await;
        if executor.ledger().get(intent_id).is_none() {
            return Err(ExecutionLedgerError::IntentNotFound(intent_id.to_owned()).into());
        }
        match executor.reconcile(intent_id, observed_at_ms).await {
            Ok(okx_execution::ReconcileDisposition::Found(_)) => {
                Ok(ExecutionReconciliation::Reconciled)
            }
            Ok(okx_execution::ReconcileDisposition::Unavailable(_)) => {
                Ok(ExecutionReconciliation::Unavailable)
            }
            Err(OrderExecutorError::NotReconcilable(_)) => Ok(ExecutionReconciliation::NotRequired),
            Err(error) => Err(error),
        }
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

    pub async fn amend_revalidation_plan(
        &self,
        intent_id: &str,
        new_size: Option<String>,
        new_price: Option<String>,
    ) -> Result<ExecutionPlan, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .amend_revalidation_plan(intent_id, new_size, new_price)
    }

    pub async fn prepare_amend(
        &self,
        intent_id: &str,
        mutation_id: &str,
        new_size: Option<String>,
        new_price: Option<String>,
        observed_at_ms: u64,
    ) -> Result<MutationPrepareDisposition, OrderExecutorError> {
        self.executor.lock().await.prepare_amend(
            intent_id,
            mutation_id,
            new_size,
            new_price,
            observed_at_ms,
        )
    }

    pub async fn prepare_cancel(
        &self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<MutationPrepareDisposition, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .prepare_cancel(intent_id, mutation_id, observed_at_ms)
    }

    pub async fn submit_order_mutation_demo_authorized(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        intent_id: &str,
        mutation_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> AgentResult<Option<Result<MutationSubmitDisposition, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.demo_mutation_admitted(preflight, executor.ledger()) {
            executor.disable_mutations();
            return Ok(None);
        }
        executor.enable_demo_acceptance(self.environment)?;
        let result = executor
            .submit_order_mutation(intent_id, mutation_id, timing, observed_at_ms)
            .await;
        executor.disable_mutations();
        Ok(Some(result))
    }

    /// Existing Demo authority, one owner mutex, one fenced exchange
    /// mutation. Never enable a production mutation or trust caller proof
    /// without the executor's independent exact-ID venue validation.
    pub async fn shrink_protective_size_demo_authorized<F, Fut>(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        request: FencedProtectiveShrinkRequest<'_>,
        final_private_ws_fence: F,
    ) -> AgentResult<Option<Result<MutationSubmitDisposition, OrderExecutorError>>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let mut executor = self.executor.lock().await;
        if !self.demo_mutation_admitted(preflight, executor.ledger()) {
            executor.disable_mutations();
            return Ok(None);
        }
        executor.enable_demo_acceptance(self.environment)?;
        let result = executor
            .submit_protective_size_amend_fenced(request, final_private_ws_fence)
            .await;
        executor.disable_mutations();
        Ok(Some(result))
    }

    /// Local exchange-effect readback, NOT an amend-algos resend. Hold the
    /// one executor/ledger mutex through all venue GETs, final private-WS
    /// fence and the durable CONFIRMED transition. Production cannot mutate
    /// its ledger using a Demo-only operator confirmation.
    pub async fn confirm_protective_size_amend_demo_fenced<F, Fut>(
        &self,
        intent_id: &str,
        mutation_id: &str,
        account: &AccountSnapshot,
        inventory: &PendingProtectiveAlgoInventory,
        observed_at_ms: u64,
        final_private_ws_fence: F,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        if self.mode != ExecutionRuntimeMode::DemoAcceptance {
            return Err(OrderExecutorError::Transition(
                okx_execution::ExecutionTransitionError::LiveTradingDisabled,
            ));
        }
        self.executor
            .lock()
            .await
            .confirm_protective_size_amend_from_venue(
                intent_id,
                mutation_id,
                account,
                inventory,
                observed_at_ms,
                final_private_ws_fence,
            )
            .await
    }

    pub async fn retire_protective_size_amend_absent(
        &self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .retire_protective_size_amend_absent(intent_id, mutation_id, observed_at_ms)
    }

    pub async fn prepare_protective_cleanup(
        &self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        self.executor.lock().await.prepare_protective_cleanup(
            intent_id,
            mutation_id,
            observed_at_ms,
        )
    }

    pub async fn mark_protective_cleanup_unknown_after_restart(
        &self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        let mut executor = self.executor.lock().await;
        executor.mark_protective_cleanup_unknown_after_restart(
            intent_id,
            mutation_id,
            observed_at_ms,
        )
    }

    pub async fn confirm_protective_cleanup_absent(
        &self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, OrderExecutorError> {
        self.executor
            .lock()
            .await
            .confirm_protective_cleanup_absent(intent_id, mutation_id, observed_at_ms)
    }

    pub async fn submit_protective_cleanup_demo_authorized(
        &self,
        preflight: &ExecutorPreflightSnapshot,
        intent_id: &str,
        mutation_id: &str,
        timing: MutationTiming,
        observed_at_ms: u64,
    ) -> AgentResult<Option<Result<MutationSubmitDisposition, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.demo_mutation_admitted(preflight, executor.ledger()) {
            executor.disable_mutations();
            return Ok(None);
        }
        executor.enable_demo_acceptance(self.environment)?;
        let result = executor
            .submit_prepared_protective_cleanup(intent_id, mutation_id, timing, observed_at_ms)
            .await;
        executor.disable_mutations();
        Ok(Some(result))
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
mod mutation_ingress_tests {
    use super::MutationIngress;
    use std::sync::Arc;

    #[tokio::test]
    async fn admitted_request_must_exit_before_source_can_drain() {
        let ingress = Arc::new(MutationIngress::new());
        let existing_post = ingress.admit().await.expect("admitted");
        let another = Arc::clone(&ingress);
        let draining = tokio::spawn(async move { another.drain().await });
        tokio::task::yield_now().await;
        assert!(!draining.is_finished(), "cannot terminate active POST");
        drop(existing_post);
        draining.await.expect("task").expect("drain");
        assert!(
            ingress.admit().await.is_none(),
            "future mutating ingress denied"
        );
        // A repeated drain is idempotently denied to new mutation requests;
        // only an independently approved fresh agent can trade again.
        ingress.drain().await.expect("repeat safe");
        assert!(ingress.admit().await.is_none());
    }

    #[tokio::test]
    async fn read_only_observer_remains_independent_of_drained_mutation_gate() {
        let ingress = MutationIngress::new();
        ingress.drain().await.expect("drain");
        assert!(ingress.admit().await.is_none());
        // Observer requests bypass the guard in query::execution::dispatch.
        // No mutation authority is minted by their continued availability.
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

    fn preflight(accepted: bool) -> ExecutorPreflightSnapshot {
        let credential = ExecutorCredentialPreflight {
            schema: crate::execution_preflight::EXECUTOR_CREDENTIAL_PREFLIGHT_SCHEMA_V1,
            accepted,
            observer_read_only: true,
            observer_private_ws_converged: true,
            executor_read_permission: true,
            executor_trade_permission: true,
            executor_withdraw_permission: false,
            executor_ip_bound: true,
            account_identity_match: true,
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            futures_mode: true,
            long_short_mode: true,
            subaccount: true,
            production_environment: false,
        };
        let clock = okx_api::ClockEvidenceSnapshot {
            server_time_ms: 1_790_000_000_000,
            local_midpoint_ms: 1_790_000_000_000,
            offset_ms: 0,
            round_trip_ms: 10,
            age_ms: 0,
            max_abs_offset_ms: 5_000,
            max_round_trip_ms: 2_000,
            max_age_ms: 2_000,
            accepted: true,
        };
        let rate = AccountRateLimitEvidence {
            schema: okx_api::ACCOUNT_RATE_LIMIT_EVIDENCE_SCHEMA_V3,
            source: okx_api::AccountRateLimitSource::DemoBaseFallback,
            current_orders_per_2s: okx_api::DEFAULT_SUBACCOUNT_ORDER_LIMIT_PER_2S,
            next_orders_per_2s: None,
            fill_ratio: None,
            main_fill_ratio: None,
            updated_at_ms: None,
        };
        ExecutorPreflightSnapshot::new(credential, clock, rate, RateBudget::new().snapshot())
    }

    fn timing() -> MutationTiming {
        MutationTiming::from_exchange_time_ms(1_790_000_000_000, 5_000).expect("timing")
    }

    #[test]
    fn quiet_source_capture_admission_never_relaxes_two_hour_coverage() {
        let start = 100_000;
        let one_hour = QUIET_SOURCE_COVERAGE_REFRESH_MS;
        assert!(!quiet_source_capture_due(start, None, start));
        assert!(!quiet_source_capture_due(start, None, start + one_hour - 1));
        assert!(quiet_source_capture_due(start, None, start + one_hour));
        assert!(!quiet_source_capture_due(
            start,
            Some(start + 500),
            start + one_hour
        ));
        assert!(quiet_source_capture_due(
            start,
            Some(start + 500),
            start + one_hour + 500
        ));
        // An unavailable source owner never acquires its own timer or
        // assigns a historical zero-event proof.
        assert!(quiet_source_capture_due(
            start,
            Some(start),
            start + 9 * one_hour
        ));
        assert!(!quiet_source_capture_due(start, Some(start), start - 1));
        assert!(!quiet_source_capture_due(
            start,
            Some(start + 100_000),
            start + one_hour
        ));
    }

    #[test]
    fn existing_demo_and_production_roots_preserve_both_historical_ledger_files() {
        let base = temp_root("disjoint-profile-ledger");
        let demo_root = base.join("demo");
        fs::create_dir_all(&demo_root).unwrap();
        let production_path = base.join("execution-ledger.json");
        let demo_path = demo_root.join("execution-ledger.json");
        // A corrupt existing profile file must cause a hard failure, not an
        // implicit empty ledger from a newly named file. No file is rewritten.
        let marker = b"{intentional-corrupt-existing-ledger}";
        fs::write(&production_path, marker).unwrap();
        assert!(
            ExecutionRuntime::new(
                &base,
                OkxEnvironment::new(Region::Global, false),
                credentials(),
                100,
                RateBudget::new(),
            )
            .is_err()
        );
        let demo = ExecutionRuntime::new_demo_acceptance(
            &demo_root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            100,
            RateBudget::new(),
        )
        .expect("different accepted Demo root");
        assert_eq!(demo.mode(), ExecutionRuntimeMode::DemoAcceptance);
        assert!(!demo_path.exists());
        fs::write(&demo_path, marker).unwrap();
        assert!(
            ExecutionRuntime::new_demo_acceptance(
                &demo_root,
                OkxEnvironment::new(Region::Global, true),
                credentials(),
                101,
                RateBudget::new(),
            )
            .is_err()
        );
        assert_eq!(fs::read(&production_path).unwrap(), marker);
        assert_eq!(fs::read(&demo_path).unwrap(), marker);
        drop(demo);
        let _ = fs::remove_dir_all(base);
    }

    #[tokio::test]
    async fn shared_demo_mutation_gate_rejects_foreign_or_missing_uid_and_production_claim() {
        let root = temp_root("one-demo-gate");
        fs::create_dir_all(&root).unwrap();
        let runtime = ExecutionRuntime::new_demo_acceptance(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            1,
            RateBudget::new(),
        )
        .expect("demo runtime");
        let mut accepted = preflight(true);
        let executor = runtime.executor.lock().await;
        assert!(runtime.demo_mutation_admitted(&accepted, executor.ledger()));
        accepted.credential.account_uid_fingerprint.clear();
        assert!(!runtime.demo_mutation_admitted(&accepted, executor.ledger()));
        accepted = preflight(true);
        accepted.credential.production_environment = true;
        assert!(!runtime.demo_mutation_admitted(&accepted, executor.ledger()));
        accepted = preflight(false);
        assert!(!runtime.demo_mutation_admitted(&accepted, executor.ledger()));
        drop(executor);
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn native_runtime_drain_preserves_manifest_and_never_grants_new_mutation_authority() {
        let root = temp_root("drain-runtime-unchanged-ledger");
        fs::create_dir_all(&root).unwrap();
        let runtime = ExecutionRuntime::new_demo_acceptance(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            1,
            RateBudget::new(),
        )
        .expect("native single executor");
        // A new empty ledger has no attestable durable manifest by design.
        // Seed a real atomic on-disk STOP without an exchange POST; drain
        // must preserve exactly those original bytes and that stop reason.
        {
            let mut executor = runtime.executor.lock().await;
            executor
                .stop_new_risk("TEST_DURABLE_STOP", 2)
                .expect("sealed native ledger");
        }
        let baseline = runtime
            .profile_handoff_ledger_assessment("uid-fingerprint")
            .await
            .expect("baseline durable ledger");
        assert!(runtime.admit_mutating_request().await.is_some());
        let drained = runtime
            .drain_mutation_ingress("uid-fingerprint")
            .await
            .expect("bounded source drain");
        assert_eq!(
            drained.source_ledger_manifest_sha256,
            baseline.source_ledger_manifest_sha256
        );
        assert_eq!(drained.nonterminal_intents, 0);
        assert_eq!(drained.unresolved_order_mutations, 0);
        assert_eq!(
            drained.immutable_stop_reason.as_deref(),
            Some("TEST_DURABLE_STOP")
        );
        assert!(runtime.admit_mutating_request().await.is_none());
        assert_eq!(
            runtime.mutation_authority().await,
            MutationAuthority::Disabled
        );
        let after = runtime
            .profile_handoff_ledger_assessment("uid-fingerprint")
            .await
            .expect("read-only durable ledger");
        assert_eq!(
            after.source_ledger_manifest_sha256,
            baseline.source_ledger_manifest_sha256
        );
        drop(runtime);
        let _ = fs::remove_dir_all(root);
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

    #[tokio::test]
    async fn read_only_runtime_cannot_authorize_order_mutation_even_with_accepted_preflight() {
        let root = temp_root("read-only-mutation-disabled");
        fs::create_dir_all(&root).expect("root");
        let runtime = ExecutionRuntime::new(
            &root,
            OkxEnvironment::new(Region::Global, false),
            credentials(),
            1,
            RateBudget::new(),
        )
        .expect("read-only runtime");

        let result = runtime
            .submit_order_mutation_demo_authorized(
                &preflight(true),
                "intent_0123456789abcdef",
                "mutation_01234567",
                timing(),
                2,
            )
            .await
            .expect("fail-closed result");
        assert!(result.is_none());
        assert_eq!(
            runtime.mutation_authority().await,
            MutationAuthority::Disabled
        );

        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn strategy_opening_cannot_write_in_read_only_or_on_rejected_demo_preflight() {
        let root = temp_root("capital-opening-owner-fence");
        fs::create_dir_all(&root).expect("root");
        let authorization = format!("sha256:{}", "a".repeat(64));
        let credential = preflight(true).credential;
        let opening = StrategyAllocationOpening {
            account_uid_fingerprint: credential.account_uid_fingerprint.clone(),
            account_generation: "fresh".to_owned(),
            observed_usdt_equity: "1000".to_owned(),
            observed_at_ms: 101,
            source_history_complete: true,
            zero_positions: true,
            zero_pending_orders: true,
            zero_native_protection: true,
            operator_authority_id: authorization.clone(),
        };
        let reservation = StrategyCapitalReservation {
            schema: okx_execution::STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "strategy_owner_fence_20261013".to_owned(),
            account_uid_fingerprint: opening.account_uid_fingerprint.clone(),
            origin_schema: "okx.research.live-decision/v1".to_owned(),
            origin_version: "run-2026-10-13".to_owned(),
            mandate_version: "demo-mandate-v1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: opening.observed_usdt_equity.clone(),
            account_generation: opening.account_generation.clone(),
            operator_authority_id: authorization,
            started_at_ms: 101,
        };

        let ro = ExecutionRuntime::new(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            100,
            RateBudget::new(),
        )
        .expect("read-only owner");
        assert!(
            ro.begin_strategy_run_demo_authorized(
                &credential,
                reservation.clone(),
                &opening,
                102,
                None
            )
            .await
            .expect("read-only refused")
            .is_none()
        );
        assert!(
            ro.executor
                .lock()
                .await
                .ledger()
                .capital_reservation()
                .is_none()
        );
        drop(ro);

        let demo = ExecutionRuntime::new_demo_acceptance(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            100,
            RateBudget::new(),
        )
        .expect("Demo owner");
        let rejected = preflight(false).credential;
        assert!(
            demo.begin_strategy_run_demo_authorized(&rejected, reservation, &opening, 102, None)
                .await
                .expect("rejected preflight")
                .is_none()
        );
        assert!(
            demo.executor
                .lock()
                .await
                .ledger()
                .capital_reservation()
                .is_none()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejected_demo_preflight_does_not_submit_or_touch_ledger() {
        let root = temp_root("demo-submit-rejected-preflight");
        fs::create_dir_all(&root).expect("root");
        let runtime = ExecutionRuntime::new_demo_acceptance(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            1,
            RateBudget::new(),
        )
        .expect("demo runtime");
        let missing_intent = "intent_preflight_rejected_20261011";
        let result = runtime
            .submit_prepared_demo_authorized(&preflight(false), missing_intent, timing(), 2)
            .await
            .expect("fail-closed before any gateway send");
        assert!(result.is_none());
        assert!(runtime.entry(missing_intent).await.is_none());
        assert_eq!(
            runtime.mutation_authority().await,
            MutationAuthority::Disabled
        );
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejected_demo_preflight_cannot_authorize_order_mutation() {
        let root = temp_root("demo-mutation-rejected-preflight");
        fs::create_dir_all(&root).expect("root");
        let runtime = ExecutionRuntime::new_demo_acceptance(
            &root,
            OkxEnvironment::new(Region::Global, true),
            credentials(),
            1,
            RateBudget::new(),
        )
        .expect("demo runtime");

        let result = runtime
            .submit_order_mutation_demo_authorized(
                &preflight(false),
                "intent_0123456789abcdef",
                "mutation_01234567",
                timing(),
                2,
            )
            .await
            .expect("fail-closed result");
        assert!(result.is_none());
        assert_eq!(
            runtime.mutation_authority().await,
            MutationAuthority::Disabled
        );

        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(test)]
mod leverage_configuration_tests {
    use super::*;

    fn row(side: &str, lever: &str) -> okx_api::LeverageInfo {
        okx_api::LeverageInfo {
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            margin_mode: "cross".to_owned(),
            position_side: side.to_owned(),
            lever: lever.to_owned(),
        }
    }

    #[test]
    fn refreshed_account_generation_does_not_rebind_account_uid() {
        // A strategy reservation binds to one UID, not to a frozen account
        // snapshot generation: routine REST/WS refreshes change that hash.
        assert!(same_leverage_account("auth_uid_a", "auth_uid_a"));
        assert!(!same_leverage_account("auth_uid_a", "auth_uid_b"));
        assert!(!same_leverage_account("", ""));
    }

    #[test]
    fn hedge_leverage_requires_distinct_equal_sides() {
        let both = [row("long", "3"), row("short", "3.0")];
        assert_eq!(
            exact_hedge_cross_leverage(&both, "BTC-USDT-SWAP")
                .unwrap()
                .to_string(),
            "3"
        );
        assert!(exact_hedge_cross_leverage(&both[..1], "BTC-USDT-SWAP").is_none());
        assert!(
            exact_hedge_cross_leverage(&[row("long", "3"), row("long", "3")], "BTC-USDT-SWAP")
                .is_none()
        );
        assert!(
            exact_hedge_cross_leverage(&[row("long", "3"), row("short", "2")], "BTC-USDT-SWAP")
                .is_none()
        );
        assert!(
            exact_hedge_cross_leverage(&[row("long", "3"), row("net", "3")], "BTC-USDT-SWAP")
                .is_none()
        );
        assert!(
            exact_hedge_cross_leverage(&[row("long", "oops"), row("short", "3")], "BTC-USDT-SWAP")
                .is_none()
        );
        assert!(exact_hedge_cross_leverage(&both, "ETH-USDT-SWAP").is_none());
    }
}
