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
    StrategyFinancialPeriod, StrategyNavPoint, StrategyUnverifiedClosure, SubmitDisposition,
    execution_status_with_ledger, managed_execution_inventory, reconcile_account_ledger,
};
use okx_observation::{
    AccountLedgerFacts, AccountSnapshot, InstrumentRulesSnapshot, VenueExecutionEvidence,
};
use okx_protocol::CentralAccountPolicy;
use serde::Serialize;
use sha2::{Digest, Sha256};
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

/// A known venue REJECTED without any order ID, fill, amendment or live
/// protection is financially inert only within its exact persisted run.
/// The full authenticated financial source is STILL required to prove zero
/// order/fill/bill events; terminal status alone never authorizes new risk.
fn inert_terminal_rejection(
    record: &okx_execution::ExecutionRecord,
    uid: &str,
    run_id: &str,
) -> bool {
    record.state == ExecutionState::Rejected
        && record.plan.action == ExecutionAction::Open
        && record.plan.account_uid_fingerprint == uid
        && record.strategy_run_id.as_deref() == Some(run_id)
        && record
            .rejection_code
            .as_deref()
            .is_some_and(|v| !v.is_empty())
        && record.order_id.is_none()
        && record.exchange_state.is_none()
        && record.mutations.is_empty()
        && record.reverse.is_none()
        && record.protection.as_ref().is_some_and(|link| {
            link.status == okx_execution::ProtectiveOrderStatus::NotActivated
                && link.algo_order_id.is_none()
                && link.covered_size.is_none()
                && link.cleanup.is_none()
                && link.size_amend.is_none()
        })
}

/// A native, hash-linked NAV checkpoint is reusable across any number
/// of COMPLETED flat trading cycles in its exact strategy run. The check
/// includes every run-owned fill, fee and realized PnL. Missing funding,
/// unexplained account cash movements, unpriced positions, stale sources
/// or a different run do not produce risk authority.
fn attested_flat_nav_for_new_risk(
    run: &StrategyCapitalReservation,
    period: &StrategyFinancialPeriod,
    point: &StrategyNavPoint,
    current_usdt_equity: &str,
    account_asof_ms: u64,
) -> Option<StrategyNavRiskEvidence> {
    if period.run_id != run.allocation_id
        || period.state != "FLAT_RECONCILED"
        || !period.reasons.is_empty()
        || period.funding_usdt.as_deref() != Some("0")
        || period.unrealized_usdt.as_deref() != Some("0")
        || !period.positions.is_empty()
        || point.run_id != run.allocation_id
        || point.evidence_sha256 != period.evidence_sha256
        || point.account_as_of_ms != account_asof_ms
        || point.validate().is_err()
    {
        return None;
    }
    let nav = rust_decimal::Decimal::from_str(period.nav_usdt.as_deref()?).ok()?;
    let allocation = rust_decimal::Decimal::from_str(&run.allocated_usdt).ok()?;
    let opening_equity = rust_decimal::Decimal::from_str(&run.opening_account_usdt_equity).ok()?;
    let observed_equity = rust_decimal::Decimal::from_str(current_usdt_equity).ok()?;
    let recorded_nav = rust_decimal::Decimal::from_str(&point.nav_usdt).ok()?;
    let high_water = rust_decimal::Decimal::from_str(&point.high_water_usdt).ok()?;
    let daily_loss = rust_decimal::Decimal::from_str(&point.daily_gross_loss_usdt).ok()?;
    let realized = rust_decimal::Decimal::from_str(&period.realized_usdt).ok()?;
    let fees = rust_decimal::Decimal::from_str(&period.fees_usdt).ok()?;
    // Account cashflows remain distinct from strategy PnL. This equality
    // is checked against authenticated USDT equity, never USD conversions.
    if allocation <= rust_decimal::Decimal::ZERO
        || nav <= rust_decimal::Decimal::ZERO
        || high_water < nav
        || daily_loss < rust_decimal::Decimal::ZERO
        || nav != allocation + realized + fees
        || nav != recorded_nav
        || observed_equity != opening_equity + nav - allocation
    {
        return None;
    }
    Some(StrategyNavRiskEvidence {
        run_id: run.allocation_id.clone(),
        mandate_version: run.mandate_version.clone(),
        allocation_usdt: run.allocated_usdt.clone(),
        nav_usdt: nav.normalize().to_string(),
        high_water_usdt: high_water.normalize().to_string(),
        daily_realized_loss_usdt: daily_loss.normalize().to_string(),
    })
}

/// The final native venue witnesses are captured by one query owner before
/// the single Rust executor evaluates and submits a Production intent.
pub struct ProductionSubmitSources<'a> {
    pub facts: &'a AccountLedgerFacts,
    pub protection: &'a PendingProtectiveAlgoInventory,
}

/// One typed bundle of the central delivery grant and the independently
/// acknowledged RiskBook POST fence. Neither item alone authorizes an order.
pub struct ProductionSubmitAuthorization<'a> {
    pub central: Option<CentralAuthorizationBinding<'a>>,
    pub risk_post: Option<&'a okx_protocol::risk_authority::RiskResult>,
}

/// A source-exact terminal intent already persisted by the ONE executor.
/// It is a candidate for release, not an exchange-side FLAT assertion.
pub struct NativeTerminalRiskCandidate {
    pub reservation_key: String,
    pub plan_sha256: String,
    pub ledger: okx_protocol::risk_authority::TerminalRiskLedgerProof,
}

/// RiskBook's canonical length-delimited reservation identity. Verifying it
/// under the existing executor lock prevents reusing another intent's receipt.
fn native_risk_reservation_key(uid: &str, run_id: &str, intent_id: &str) -> String {
    ["okx", "real", uid, run_id, intent_id]
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect()
}

#[cfg(test)]
#[test]
fn native_risk_receipt_identity_matches_riskbook_key() {
    assert_eq!(
        native_risk_reservation_key(&"a".repeat(64), "run_001", "intent_001"),
        format!("3:okx4:real64:{}7:run_00110:intent_001", "a".repeat(64))
    );
    assert_ne!(
        native_risk_reservation_key(&"a".repeat(64), "run_001", "intent_001"),
        native_risk_reservation_key(&"a".repeat(64), "run_001", "intent_002")
    );
}

/// One immutable, Worker-sourced policy for exactly this authenticated
/// transport generation and one native execution request. Never persisted.
#[derive(Clone, Copy)]
pub struct CentralPolicyBinding<'a> {
    pub policy: &'a CentralAccountPolicy,
    pub session_generation: u64,
}

impl CentralPolicyBinding<'_> {
    pub fn authorizes_uid(self, uid: &str, now_ms: u64) -> bool {
        self.policy
            .authorizes_uid(uid, now_ms, self.session_generation)
    }
}
/// Typed central delivery scope; a DO risk receipt remains mandatory for POST.
#[derive(Clone, Copy)]
pub struct CentralGovernanceBinding<'a> {
    pub scope: &'a okx_protocol::account_governance::CentralGovernanceSnapshotV2,
    pub session_generation: u64,
    pub delivery_challenge_verified: bool,
}
impl CentralGovernanceBinding<'_> {
    pub fn authorizes_uid(self, uid: &str, _now_ms: u64) -> bool {
        self.scope.authorizes_challenge_bound_delivery(
            uid,
            self.session_generation,
            self.delivery_challenge_verified,
        )
    }
}
/// One central delivery binding per intent. V1 is rolling-upgrade only;
/// both forms still require a separate transactional risk POST fence.
#[derive(Clone, Copy)]
pub enum CentralAuthorizationBinding<'a> {
    Legacy(CentralPolicyBinding<'a>),
    Governance(CentralGovernanceBinding<'a>),
}
impl CentralAuthorizationBinding<'_> {
    pub fn authorizes_uid(self, uid: &str, now_ms: u64) -> bool {
        match self {
            Self::Legacy(v) => v.authorizes_uid(uid, now_ms),
            Self::Governance(v) => v.authorizes_uid(uid, now_ms),
        }
    }

    pub fn budget(self) -> Option<rust_decimal::Decimal> {
        let text = match self {
            Self::Legacy(v) => &v.policy.max_total_modelled_loss_usdt,
            Self::Governance(v) => &v.scope.state.max_total_modelled_loss_usdt,
        };
        rust_decimal::Decimal::from_str(text).ok()
    }
}

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

/// CONTROL owns process lifecycle and negative safety latches only.
/// It cannot grant trading rights, account risk, or positive budgets.
const NATIVE_CONTROL_STATE: &str = r"C:\okx-control\host-state.pb";

/// The native owner must remain available for read-only recovery, even if
/// STOP or an operator-selected READ_ONLY forbids every exchange mutation.
fn check_native_production_lifecycle_from(bytes: &[u8], for_mutation: bool) -> AgentResult<()> {
    let state = okx_protocol::host_state_proto::decode_host_state(bytes)
        .ok_or(AgentError::ProductionPolicyUnavailable)?;
    if state.desired_agent != 1
        || state.selected_environment != 1
        || for_mutation && (state.read_only || !state.explicit_trade_rearm)
        || state.transition.is_some()
        || state.switch_hold.is_some()
    {
        return Err(AgentError::ProductionPolicyUnavailable);
    }
    // Legacy positive policy never grants trading. v2 governance and the
    // atomic post fence remain necessary at the final Executor admission.
    Ok(())
}

fn require_native_production_lifecycle_from(bytes: &[u8]) -> AgentResult<()> {
    check_native_production_lifecycle_from(bytes, true)
}

fn require_native_production_owner() -> AgentResult<()> {
    let bytes = std::fs::read(NATIVE_CONTROL_STATE)?;
    check_native_production_lifecycle_from(&bytes, false)
}

fn require_native_production_lifecycle() -> AgentResult<()> {
    let bytes = std::fs::read(NATIVE_CONTROL_STATE)?;
    require_native_production_lifecycle_from(&bytes)
}

/// Temporary executor-v1 arithmetic projection from ONE verified central v2
/// governance snapshot, not a separate mutable policy or risk authority.
fn central_executor_policy(
    scope: CentralGovernanceBinding<'_>,
    run: &StrategyCapitalReservation,
    instrument: &str,
    now_ms: u64,
) -> Option<okx_execution::ProductionTradingPolicy> {
    use okx_protocol::account_governance::AccountEnvironmentV2;
    if !scope.authorizes_uid(&run.account_uid_fingerprint, now_ms)
        || run.operator_authority_id != scope.scope.state.operator_authorization.id
    {
        return None;
    }
    let account = scope.scope.state.accounts.iter().find(|a| {
        a.venue == "okx"
            && a.environment == AccountEnvironmentV2::Real
            && a.identity_verified
            && a.enabled
            && a.uid_fingerprint == run.account_uid_fingerprint
    })?;
    if !account
        .limits
        .allowed_instruments
        .iter()
        .any(|allowed| allowed == instrument)
        || !account
            .strategies
            .iter()
            .any(|strategy| strategy.enabled && strategy.id == run.mandate_version)
    {
        return None;
    }
    let global =
        rust_decimal::Decimal::from_str(&scope.scope.state.max_total_modelled_loss_usdt).ok()?;
    let ceiling =
        rust_decimal::Decimal::from_str(&scope.scope.state.operator_authorization.ceiling_usdt)
            .ok()?;
    if global <= rust_decimal::Decimal::ZERO || global > ceiling {
        return None;
    }
    Some(okx_execution::ProductionTradingPolicy {
        schema: okx_execution::PRODUCTION_TRADING_POLICY_SCHEMA_V1.to_owned(),
        revision: scope.scope.state.revision,
        permission: okx_execution::ProductionPermission::Enabled,
        account_uid_fingerprint: run.account_uid_fingerprint.clone(),
        strategy_run_id: run.allocation_id.clone(),
        operator_authority_id: run.operator_authority_id.clone(),
        allowed_instruments: account.limits.allowed_instruments.clone(),
        max_total_modelled_loss_usdt: global.normalize().to_string(),
    })
}

#[cfg(test)]
#[test]
fn lifecycle_negative_gate_does_not_depend_on_legacy_positive_policy() {
    use okx_protocol::host_state_proto::{HostStateWire, encode_host_state};
    let mut state = HostStateWire {
        version: 1,
        desired_agent: 1,
        selected_environment: 1,
        read_only: false,
        legacy_production_policy: None,
        transition: None,
        switch_hold: None,
        explicit_trade_rearm: false,
    };
    // Older journals contained a false read_only default but never carried
    // explicit post-migration mutation rearm authority.
    assert!(require_native_production_lifecycle_from(&encode_host_state(&state).unwrap()).is_err());
    state.explicit_trade_rearm = true;
    assert!(require_native_production_lifecycle_from(&encode_host_state(&state).unwrap()).is_ok());
    state.read_only = true;
    let persisted = encode_host_state(&state).unwrap();
    assert!(require_native_production_lifecycle_from(&persisted).is_err());
    // Boot and observation must survive persisted READ_ONLY; only POST is vetoed.
    assert!(check_native_production_lifecycle_from(&persisted, false).is_ok());
    state.read_only = false;
    state.selected_environment = 2;
    assert!(require_native_production_lifecycle_from(&encode_host_state(&state).unwrap()).is_err());
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
    // Held for the full lifetime, including read-only source checkpoints.
    // CONTROL offline migration acquires the same cross-process file lock.
    _writer_lease: std::fs::File,
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
        // Boot the SAME read-only observation/ledger owner under native
        // CONTROL even while new risk is explicitly OFF. Mutating paths
        // independently require !read_only immediately before venue POST.
        require_native_production_owner()?;
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
        let mut writer_lease = ExecutionLedgerStore::acquire_writer_lease(root)?;
        // Exact existing native CONTROL-selected root. A SQLite database
        // cannot be silently reused under the other environment/account;
        // check its immutable binding before opening any executor.
        let store = ExecutionLedgerStore::select_published_owner_under_writer_lease(
            root,
            &mut writer_lease,
        )?;
        if store.is_sqlite_native() {
            let bound = store
                .bound_native_account_identity()?
                .ok_or(okx_execution::ExecutionLedgerError::AccountScopeMismatch)?;
            let expected_environment = if environment.demo {
                okx_execution::LedgerEnvironment::Demo
            } else {
                okx_execution::LedgerEnvironment::Production
            };
            let expected_account = if environment.demo { "demo" } else { "real" };
            if bound.venue != okx_execution::LedgerVenue::Okx
                || bound.environment != expected_environment
                || bound.account_id != expected_account
            {
                return Err(okx_execution::ExecutionLedgerError::AccountScopeMismatch.into());
            }
        }
        let ledger = DurableExecutionLedger::open(store, observed_at_ms)?;
        Ok(Self {
            mutation_ingress: MutationIngress::new(),
            environment,
            mode,
            executor_clock,
            executor_account,
            public_data,
            executor: Mutex::new(OrderExecutor::new(ledger, trade)),
            _writer_lease: writer_lease,
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
            && ledger
                .account_scope_matches(&preflight.credential.account_uid_fingerprint)
                .unwrap_or(false)
    }

    pub const fn demo_mutation_acceptance_requested(&self) -> bool {
        matches!(self.mode, ExecutionRuntimeMode::DemoAcceptance)
    }

    pub const fn production_trading_requested(&self) -> bool {
        matches!(self.mode, ExecutionRuntimeMode::Production) && !self.environment.demo
    }

    /// Single normal financial source owner for either actual venue. This
    /// does not grant trading rights and never invents missing exchange data.
    fn financial_source_owner_active(&self) -> bool {
        matches!(
            (self.mode, self.environment.demo),
            (ExecutionRuntimeMode::DemoAcceptance, true)
                | (ExecutionRuntimeMode::Production, false)
        )
    }

    /// Return the exchange reconciliation and exact identity inventory under
    /// one ledger lock so the two views cannot observe different intent sets.
    /// This does not perform a gateway send or a mutation.
    /// Read-only admission for the in-process private-order event source.
    /// The executor ledger remains the sole owner of durable evidence.
    pub async fn strategy_financial_capture_active(&self) -> bool {
        if !self.financial_source_owner_active() {
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
        if !self.financial_source_owner_active() {
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
                .and_then(|source| source.latest_source_received_at_ms),
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
        // Initial reconciliation preceded the source capture above. Publish
        // an audit of this exact updated run, not the pre-capture snapshot.
        reconciliation.refresh_period_financial_audit();
        Ok((reconciliation, inventory))
    }

    /// Continuous, fully source-attested strategy risk admission. Reuses
    /// persisted run PnL/NAV/peak/day-loss across any number of flat
    /// OPEN -> CLOSE cycles. Never grants a synthetic first-trade NAV.
    pub async fn strategy_nav_for_new_risk(
        &self,
        facts: &AccountLedgerFacts,
        plan: &ExecutionPlan,
        coherent: bool,
        settle_currency: Option<&str>,
        contract_type: Option<&str>,
    ) -> Option<StrategyNavRiskEvidence> {
        if !matches!(
            (self.mode, self.environment.demo),
            (ExecutionRuntimeMode::DemoAcceptance, true)
                | (ExecutionRuntimeMode::Production, false)
        ) || !coherent
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
        // Financial NAV evidence is not positive trade authorization.
        if self.production_trading_requested() && require_native_production_lifecycle().is_err() {
            return None;
        }
        let binding = plan.risk_binding.as_ref()?;
        // Trading equity must remain equal to the source-authenticated
        // opening reserve. External cash transfers are not strategy PnL.
        let current_equity = facts.trading_usdt_equity.as_deref()?;
        if ledger.finalized_run().is_some()
            || ledger.risk_stop().is_some()
            || run.account_uid_fingerprint != facts.summary.authority.account_uid_fingerprint
            || run.account_uid_fingerprint != plan.account_uid_fingerprint
            || run.mandate_version != binding.mandate.version
            || run.allocated_usdt != binding.mandate.capital_base_usd
            || ledger
                .any_entry_created_since(
                    &run.account_uid_fingerprint,
                    run.started_at_ms,
                    |e| {
                        (e.record.state == ExecutionState::Rejected
                            && !inert_terminal_rejection(
                                &e.record,
                                &run.account_uid_fingerprint,
                                &run.allocation_id,
                            ))
                            || (!e.record.state.is_terminal()
                                && (e.record.plan.intent_id != plan.intent_id
                                    || e.record.plan != *plan
                                    || e.record.state != ExecutionState::Prepared))
                    },
                )
                .ok()?
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
        if source.run_id != run.allocation_id
            || !source.latest_source_complete
            || source.source_gap_over_two_hours
        {
            return None;
        }
        let projection = executor
            .ledger()
            .financial_period_for_new_risk(facts, coherent, plan)?;
        // The NAV point belongs to the current authenticated source read,
        // not the exchange's potentially stale last account-change uTime.
        let asof = okx_execution::source_receipt_ms(&facts.summary.source_received_at)?;
        // The one durable Ledger is also the NAV/high-water/day-loss writer.
        // Any breach latches STOP before the candidate can receive a permit.
        let now = u64::try_from(Utc::now().timestamp_millis()).ok()?;
        executor
            .observe_strategy_nav(&projection, facts, now)
            .ok()?;
        let ledger = executor.ledger();
        if ledger.risk_stop().is_some() {
            return None;
        }
        let point = ledger.strategy_nav_watermark()?;
        attested_flat_nav_for_new_risk(&run, &projection, point, current_equity, asof)
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
            || executor.ledger().capital_reservation().is_some_and(|r| {
                executor
                    .ledger()
                    .any_entry_created_since(
                        &r.account_uid_fingerprint,
                        r.started_at_ms,
                        |_| true,
                    )
                    .unwrap_or(true)
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
                .account_scope_matches(&preflight.account_uid_fingerprint)
                .unwrap_or(false)
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
        central: Option<CentralAuthorizationBinding<'_>>,
    ) -> AgentResult<Option<Result<StrategyCapitalReservation, OrderExecutorError>>> {
        let Some(CentralAuthorizationBinding::Governance(scope)) = central else {
            return Ok(None);
        };
        if !scope.authorizes_uid(
            &preflight.account_uid_fingerprint,
            Utc::now().timestamp_millis().max(1) as u64,
        ) {
            return Ok(None);
        }
        if !self.production_trading_requested()
            || !preflight.accepted
            || !preflight.production_environment
            || !preflight.executor_trade_permission
            || preflight.executor_withdraw_permission
        {
            return Ok(None);
        }
        require_native_production_lifecycle()?;
        if reservation.account_uid_fingerprint != preflight.account_uid_fingerprint
            || opening.account_uid_fingerprint != preflight.account_uid_fingerprint
            || reservation.operator_authority_id != scope.scope.state.operator_authorization.id
        {
            return Ok(None);
        }
        let matched = scope.scope.state.accounts.iter().any(|account| {
            account.venue == "okx"
                && account.environment
                    == okx_protocol::account_governance::AccountEnvironmentV2::Real
                && account.enabled
                && account.identity_verified
                && account.uid_fingerprint == preflight.account_uid_fingerprint
                && account
                    .strategies
                    .iter()
                    .any(|strategy| strategy.enabled && strategy.id == reservation.mandate_version)
        });
        if !matched {
            return Ok(None);
        }
        let mut executor = self.executor.lock().await;
        if !executor
            .ledger()
            .account_scope_matches(&preflight.account_uid_fingerprint)
            .unwrap_or(false)
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

    /// The same persisted run and native source must authorize financial
    /// FINALIZED/UNVERIFIED on Demo and Production. No exchange POST is
    /// possible here; this only closes the already-owned immutable ledger.
    fn strategy_financial_mutation_admitted(
        &self,
        preflight: &ExecutorCredentialPreflight,
        expected_run_id: &str,
        observed_uid: &str,
        ledger: &DurableExecutionLedger,
    ) -> bool {
        if !preflight.accepted
            || preflight.account_uid_fingerprint != observed_uid
            || !ledger.capital_reservation().is_some_and(|run| {
                run.allocation_id == expected_run_id && run.account_uid_fingerprint == observed_uid
            })
        {
            return false;
        }
        if self.demo_mutation_acceptance_requested() {
            return self.environment.demo && !preflight.production_environment;
        }
        if !self.production_trading_requested() || !preflight.production_environment {
            return false;
        }
        // Finance reconciliation may finalize a known run after central STOP,
        // but cannot create new exchange exposure or bypass CONTROL lifecycle.
        require_native_production_lifecycle().is_ok()
            && ledger.capital_reservation().is_some_and(|run| {
                run.account_uid_fingerprint == observed_uid && run.allocation_id == expected_run_id
            })
    }

    /// The single executor captures the coherent source before the
    /// atomic FINALIZED write. Caller cannot supply the exchange witness.
    pub async fn finalize_strategy_run_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        expected_run_id: &str,
        facts: &AccountLedgerFacts,
        coherent: bool,
        protection_absent: bool,
        observed_at_ms: u64,
    ) -> AgentResult<Option<Result<StrategyFinalization, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.strategy_financial_mutation_admitted(
            preflight,
            expected_run_id,
            &facts.summary.authority.account_uid_fingerprint,
            executor.ledger(),
        ) {
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

    /// Exact authorized administrative quarantine for the same native owner;
    /// never an exchange order or a fabricated historical profit.
    pub async fn quarantine_unverified_strategy_run_authorized(
        &self,
        preflight: &ExecutorCredentialPreflight,
        expected_run_id: &str,
        facts: &AccountLedgerFacts,
        coherent: bool,
        protection_absent: bool,
        now_ms: u64,
    ) -> AgentResult<Option<Result<StrategyUnverifiedClosure, OrderExecutorError>>> {
        let mut executor = self.executor.lock().await;
        if !self.strategy_financial_mutation_admitted(
            preflight,
            expected_run_id,
            &facts.summary.authority.account_uid_fingerprint,
            executor.ledger(),
        ) {
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
        sources: ProductionSubmitSources<'_>,
        grant: ProductionSubmitAuthorization<'_>,
    ) -> AgentResult<Option<Result<SubmitDisposition, OrderExecutorError>>> {
        let ProductionSubmitSources { facts, protection } = sources;
        let ProductionSubmitAuthorization {
            central: authorization,
            risk_post,
        } = grant;
        let native_uid = &preflight.credential.account_uid_fingerprint;
        let fresh_now = Utc::now().timestamp_millis().max(1) as u64;
        if !authorization.is_some_and(|p| p.authorizes_uid(native_uid, fresh_now)) {
            return Ok(None);
        }
        if !self.production_trading_requested()
            || !preflight.accepted
            || !preflight.credential.accepted
            || !preflight.credential.production_environment
            || !preflight.credential.executor_trade_permission
            || preflight.credential.executor_withdraw_permission
            || facts.summary.authority.account_uid_fingerprint
                != preflight.credential.account_uid_fingerprint
            || !protection.complete_within_bound
        {
            return Ok(None);
        }
        require_native_production_lifecycle()?;
        let Some(CentralAuthorizationBinding::Governance(scope)) = authorization else {
            return Ok(None);
        };
        let mut executor = self.executor.lock().await;
        let Some(entry) = executor.ledger().get(intent_id) else {
            return Ok(None);
        };
        let Some(run) = executor.ledger().capital_reservation() else {
            return Ok(None);
        };
        if run.account_uid_fingerprint != *native_uid
            || entry.record.strategy_run_id.as_deref() != Some(run.allocation_id.as_str())
        {
            return Ok(None);
        }
        let Some(policy) =
            central_executor_policy(scope, run, &entry.record.plan.instrument_id, fresh_now)
        else {
            return Ok(None);
        };
        let plan = &entry.record.plan;
        let new_risk = plan.action.is_risk_increasing();
        // Only an acknowledged atomic POST fence from the ONE central v2
        // RiskBook can authorize increased Real exposure. A v1 delivery grant
        // or one-account observer flag never substitutes for that receipt.
        let expected_reservation_key = executor
            .ledger()
            .capital_reservation()
            .map(|run| native_risk_reservation_key(native_uid, &run.allocation_id, intent_id));
        let post_fenced = matches!(authorization, Some(CentralAuthorizationBinding::Governance(scope))
        if risk_post.is_some_and(|proof| {
            proof.valid()
                && proof.status
                    == okx_protocol::risk_authority::RiskDecisionStatus::PostAuthorized
                && proof.policy_revision == Some(scope.scope.state.revision)
                && proof.reservation_key.as_deref() == expected_reservation_key.as_deref()
        }));
        if new_risk && !post_fenced {
            return Ok(None);
        }
        // The atomic RiskBook receipt includes all-account occupied loss and
        // outstanding reservations; only this candidate is subtracted there.
        // Never synthesize zero from a selected-account FLAT observation.
        let occupied_before_candidate = if post_fenced {
            risk_post
                .and_then(|receipt| receipt.existing_modelled_loss_usdt.as_deref())
                .and_then(|value| rust_decimal::Decimal::from_str(value).ok())
                .filter(|value| *value >= rust_decimal::Decimal::ZERO)
        } else {
            None
        };
        if new_risk && occupied_before_candidate.is_none() {
            return Ok(None);
        }
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
            let observed = facts
                .authoritative_positions
                .iter()
                .filter(|position| {
                    position.instrument_id == plan.instrument_id
                        && position.position_side == plan.position_side.as_str()
                        && rust_decimal::Decimal::from_str(&position.position)
                            .is_ok_and(|v| v > rust_decimal::Decimal::ZERO)
                })
                .collect::<Vec<_>>();
            observed.len() == 1
                && rust_decimal::Decimal::from_str(&observed[0].position)
                    .ok()
                    .is_some_and(|quantity| {
                        quantity > rust_decimal::Decimal::ZERO
                            && rust_decimal::Decimal::from_str(&plan.size).ok() == Some(quantity)
                    })
                && executor.ledger().capital_reservation().is_some_and(|run| {
                    executor
                        .ledger()
                        .any_entry_created_since(
                            &run.account_uid_fingerprint,
                            run.started_at_ms,
                            |owned| {
                                owned.record.plan.action == ExecutionAction::Open
                                    && owned.record.strategy_run_id.as_deref()
                                        == Some(run.allocation_id.as_str())
                                    && owned.record.plan.instrument_id == plan.instrument_id
                                    && owned.record.plan.position_side == plan.position_side
                                    && owned.record.order_id.is_some()
                            },
                        )
                        .unwrap_or(false)
                })
        } else {
            false
        };
        let finance_complete = if new_risk {
            let ledger = executor.ledger();
            let historical_rejections_safe = ledger.capital_reservation().is_some_and(|run| {
                !ledger
                    .any_entry_created_since(
                        &run.account_uid_fingerprint,
                        run.started_at_ms,
                        |entry| {
                            entry.record.state == ExecutionState::Rejected
                                && !inert_terminal_rejection(
                                    &entry.record,
                                    &run.account_uid_fingerprint,
                                    &run.allocation_id,
                                )
                        },
                    )
                    .unwrap_or(true)
            });
            historical_rejections_safe
                && ledger
                    .capital_reservation()
                    .zip(ledger.financial_period_for_new_risk(facts, true, plan))
                    .zip(ledger.strategy_nav_watermark())
                    .and_then(|((run, period), point)| {
                        let asof = facts
                            .summary
                            .current_account_as_of_ms
                            .as_deref()?
                            .parse::<u64>()
                            .ok()?;
                        attested_flat_nav_for_new_risk(
                            run,
                            &period,
                            point,
                            facts.trading_usdt_equity.as_deref()?,
                            asof,
                        )
                    })
                    .is_some()
        } else {
            false
        };
        let witness = okx_execution::ProductionRiskWitness {
            finance_complete,
            // The executor independently scans durable parent/mutation/OCO
            // UNKNOWN states; this cannot override native state.
            unknown_mutations_reconciled: true,
            // Occupancy comes only from the exact DO POST receipt.
            existing_modelled_loss_usdt: occupied_before_candidate,
            exact_owned_reduce_only_close: close_owned_exact,
        };
        // Final central revision/UID/expiry fence immediately before the
        // native gateway owner enters its durable SUBMITTING and single POST.
        let fresh_now = Utc::now().timestamp_millis().max(1) as u64;
        if !authorization.is_some_and(|p| p.authorizes_uid(native_uid, fresh_now)) {
            return Ok(None);
        }
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

    /// Production read-only financial maintenance. Source proof is obtained
    /// from the existing authenticated observer with a native WS cursor.
    /// This path NEVER calls the exchange mutation gateway or grants new risk.
    pub async fn close_historical_unsent_batch(
        &self,
        expected_uid: &str,
        expected_manifest_sha256: &str,
        exact_intents: &[String],
        facts: &AccountLedgerFacts,
        native_protection: &PendingProtectiveAlgoInventory,
        observed_at_ms: u64,
    ) -> AgentResult<usize> {
        if self.environment.demo
            || self.mode != ExecutionRuntimeMode::ReadOnly
            || facts.summary.authority.account_uid_fingerprint != expected_uid
            || facts.summary.open_positions != 0
            || facts.summary.pending_orders != 0
            || !facts.exchange_orders.is_empty()
            || !facts.exchange_fills.is_empty()
            || !facts.exchange_orders_history_complete
            || !facts.exchange_fills_history_complete
            || !facts.bills_history_complete
            || facts.summary.history_coverage.is_empty()
            || !facts
                .summary
                .history_coverage
                .iter()
                .all(|scope| scope.complete_within_bound)
            || !native_protection.complete_within_bound
            || native_protection.rows != 0
            || expected_manifest_sha256.len() != 64
            || !expected_manifest_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(AgentError::ProductionPolicyUnavailable);
        }
        let mut owner = self.executor.lock().await;
        let storage = owner.ledger().attest_storage()?;
        if storage.manifest_sha256 != expected_manifest_sha256 {
            return Err(AgentError::ProductionPolicyUnavailable);
        }
        let changed = owner.abandon_prepared_batch(expected_uid, exact_intents, observed_at_ms)?;
        // Successful commit must be independently readable from durable disk.
        let new_storage = owner.ledger().attest_storage()?;
        if new_storage.manifest_sha256 == storage.manifest_sha256
            || new_storage.on_disk_schema.is_empty()
        {
            return Err(AgentError::ProductionPolicyUnavailable);
        }
        Ok(changed)
    }

    /// The existing ledger, not a request field, owns the strategy identity.
    /// A PREPARED candidate may reserve risk only for its exact live run.
    pub async fn risk_run_for_prepared(
        &self,
        intent_id: &str,
    ) -> Option<StrategyCapitalReservation> {
        let owner = self.executor.lock().await;
        let entry = owner.ledger().get(intent_id)?;
        let run = owner.ledger().capital_reservation()?;
        (entry.record.state == ExecutionState::Prepared
            && entry.record.plan.action.is_risk_increasing()
            && entry.record.strategy_run_id.as_deref() == Some(run.allocation_id.as_str())
            && entry
                .record
                .plan
                .risk_binding
                .as_ref()
                .is_some_and(|binding| binding.mandate.version == run.mandate_version))
        .then(|| run.clone())
    }

    /// Reuse the ONE durable ledger owner for source/UNKNOWN attestation.
    /// Never create a second ledger or infer another UID's risk from it.
    pub async fn native_flat_risk_evidence(
        &self,
        snapshot: &AccountSnapshot,
        facts: &AccountLedgerFacts,
        algos: &okx_api::PendingDerivativeAlgoRiskInventory,
        window: &okx_runtime::PrivateConvergenceWindow,
        prepared_intent: Option<&str>,
        now_ms: u64,
    ) -> Result<crate::risk_census::AccountRiskEvidence, crate::risk_census::CensusFailure> {
        let owner = self.executor.lock().await;
        crate::risk_census::AccountRiskEvidence::from_native_flat_read(
            snapshot,
            facts,
            algos,
            window,
            owner.ledger(),
            prepared_intent,
            now_ms,
        )
    }

    /// Recoverable native release candidates from the persisted run and
    /// exact terminal intent. No independent shadow ledger or sent-order replay.
    pub async fn terminal_risk_candidates(&self) -> Vec<NativeTerminalRiskCandidate> {
        let owner = self.executor.lock().await;
        let ledger = owner.ledger();
        let Some(run) = ledger.capital_reservation() else {
            return Vec::new();
        };
        ledger
            .entries()
            .filter_map(|entry| {
                let record = &entry.record;
                if !record.plan.action.is_risk_increasing()
                    || record.plan.account_uid_fingerprint != run.account_uid_fingerprint
                    || record.strategy_run_id.as_deref() != Some(run.allocation_id.as_str())
                {
                    return None;
                }
                let state = match record.state {
                    ExecutionState::Filled => "FILLED",
                    ExecutionState::Canceled => "CANCELED",
                    ExecutionState::Rejected => "REJECTED",
                    _ => return None,
                };
                let plan_sha256 = format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&record.plan).ok()?)
                );
                let record_digest =
                    format!("{:x}", Sha256::digest(serde_json::to_vec(record).ok()?));
                Some(NativeTerminalRiskCandidate {
                    reservation_key: native_risk_reservation_key(
                        &run.account_uid_fingerprint,
                        &run.allocation_id,
                        &record.plan.intent_id,
                    ),
                    plan_sha256,
                    ledger: okx_protocol::risk_authority::TerminalRiskLedgerProof {
                        account_uid: run.account_uid_fingerprint.clone(),
                        run_id: run.allocation_id.clone(),
                        intent_id: record.plan.intent_id.clone(),
                        state: state.to_owned(),
                        record_digest,
                    },
                })
            })
            .take(32)
            .collect()
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

    #[test]
    fn native_financial_admission_works_after_many_completed_filled_cycles() {
        let run = StrategyCapitalReservation {
            schema: okx_execution::STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "strategy_run_multiple_flat_cycles_20261019".to_owned(),
            account_uid_fingerprint: "a".repeat(64),
            origin_schema: "okx.manual/v1".to_owned(),
            origin_version: "2026-10-19.3".to_owned(),
            mandate_version: "real_manual_v1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "126".to_owned(),
            account_generation: "generation".to_owned(),
            operator_authority_id: format!("sha256:{}", "b".repeat(64)),
            started_at_ms: 1_792_436_000_000,
        };
        let time = 1_792_437_000_000u64;
        let mut period = StrategyFinancialPeriod {
            schema: okx_execution::STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
            run_id: run.allocation_id.clone(),
            state: "FLAT_RECONCILED",
            evidence_sha256: "c".repeat(64),
            allocation_usdt: "100".to_owned(),
            linked_fills: 2,
            linked_bills: 2,
            realized_usdt: "-0.04".to_owned(),
            fees_usdt: "-0.02".to_owned(),
            funding_usdt: Some("0".to_owned()),
            unrealized_usdt: Some("0".to_owned()),
            nav_usdt: Some("99.94".to_owned()),
            positions: Vec::new(),
            usable_for_new_risk: false,
            reasons: Vec::new(),
        };
        let mut point = StrategyNavPoint {
            schema: "okx.strategy-nav-point/v1".to_owned(),
            run_id: run.allocation_id.clone(),
            observed_at_ms: time,
            account_as_of_ms: time,
            evidence_sha256: period.evidence_sha256.clone(),
            source_cursor_key: "order:current".to_owned(),
            source_cursor_sha256: "d".repeat(64),
            nav_usdt: "99.94".to_owned(),
            high_water_usdt: "100".to_owned(),
            daily_gross_loss_usdt: "0.06".to_owned(),
            utc_day_start_ms: time / 86_400_000 * 86_400_000,
            previous_sha256: String::new(),
            checksum_sha256: String::new(),
        };
        point.checksum_sha256 = point.hash().unwrap();
        let accepted = attested_flat_nav_for_new_risk(&run, &period, &point, "125.94", time)
            .expect("complete fill/fee and verified NAV permit next independent trade");
        assert_eq!(accepted.nav_usdt, "99.94");
        assert_eq!(accepted.high_water_usdt, "100");
        assert_eq!(accepted.daily_realized_loss_usdt, "0.06");

        // Another fully closed trade in the same run preserves prior peak
        // and accumulates net strategy losses instead of resetting to 100.
        period.linked_fills = 4;
        period.linked_bills = 4;
        period.realized_usdt = "-0.07".to_owned();
        period.fees_usdt = "-0.05".to_owned();
        period.nav_usdt = Some("99.88".to_owned());
        period.evidence_sha256 = "e".repeat(64);
        point.evidence_sha256 = period.evidence_sha256.clone();
        point.nav_usdt = "99.88".to_owned();
        point.daily_gross_loss_usdt = "0.12".to_owned();
        point.checksum_sha256 = point.hash().unwrap();
        assert_eq!(
            attested_flat_nav_for_new_risk(&run, &period, &point, "125.88", time)
                .unwrap()
                .nav_usdt,
            "99.88"
        );

        assert!(attested_flat_nav_for_new_risk(&run, &period, &point, "126", time).is_none());
        period.funding_usdt = None;
        assert!(attested_flat_nav_for_new_risk(&run, &period, &point, "125.88", time).is_none());
        period.funding_usdt = Some("0".to_owned());
        point.evidence_sha256 = "f".repeat(64);
        point.checksum_sha256 = point.hash().unwrap();
        assert!(attested_flat_nav_for_new_risk(&run, &period, &point, "125.88", time).is_none());
        point.evidence_sha256 = period.evidence_sha256.clone();
        point.checksum_sha256 = point.hash().unwrap();
        point.account_as_of_ms -= 1;
        point.checksum_sha256 = point.hash().unwrap();
        assert!(attested_flat_nav_for_new_risk(&run, &period, &point, "125.88", time).is_none());
    }

    #[test]
    fn only_exact_known_empty_rejection_can_precede_next_first_open() {
        let uid = "a".repeat(64);
        let run_id = "real_manual_run_20261019c";
        let mut prior = okx_execution::ExecutionRecord::new(ExecutionPlan {
            schema: okx_execution::EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: "real_prior_rejected_20261019".to_owned(),
            client_order_id: "okxdeadbeef1234567890".to_owned(),
            reference_generation: "ref".to_owned(),
            account_generation: "account".to_owned(),
            account_uid_fingerprint: uid.clone(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            trade_mode: okx_execution::TradeMode::Cross,
            side: okx_execution::OrderSide::Buy,
            position_side: okx_execution::PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: okx_execution::OrderType::Fok,
            size: "0.01".to_owned(),
            price: "81930.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        });
        prior.strategy_run_id = Some(run_id.to_owned());
        prior.state = ExecutionState::Rejected;
        prior.rejection_code = Some("51008".to_owned());
        prior.protection = Some(okx_execution::ProtectiveOrderLink {
            policy_version: okx_execution::PROTECTIVE_ORDER_POLICY_V1.to_owned(),
            algo_client_order_id: "prx01234567890123456789".to_owned(),
            trigger_price_basis: okx_execution::ProtectiveTriggerPriceBasis::Mark,
            status: okx_execution::ProtectiveOrderStatus::NotActivated,
            algo_order_id: None,
            covered_size: None,
            failure_code: None,
            cleanup: None,
            size_amend: None,
        });
        assert!(inert_terminal_rejection(&prior, &uid, run_id));
        assert!(!inert_terminal_rejection(&prior, &"b".repeat(64), run_id));
        assert!(!inert_terminal_rejection(&prior, &uid, "different_run"));
        prior.order_id = Some("venue-order-id".to_owned());
        assert!(!inert_terminal_rejection(&prior, &uid, run_id));
        prior.order_id = None;
        prior.protection.as_mut().unwrap().status = okx_execution::ProtectiveOrderStatus::Active;
        assert!(!inert_terminal_rejection(&prior, &uid, run_id));
        prior.protection.as_mut().unwrap().status =
            okx_execution::ProtectiveOrderStatus::NotActivated;
        prior.state = ExecutionState::UnknownSubmission;
        assert!(!inert_terminal_rejection(&prior, &uid, run_id));
        prior.state = ExecutionState::Rejected;
        prior.rejection_code = None;
        assert!(!inert_terminal_rejection(&prior, &uid, run_id));
    }

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
