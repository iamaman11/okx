use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};

use okx_observation::AccountLedgerFacts;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    ArchivedStrategyRun, DurableExchangeEvidence, EXECUTION_LINEAGE_SCHEMA_V1,
    EXECUTION_LINEAGE_SCHEMA_V2, EXECUTION_PLAN_SCHEMA_V1, ExchangeEvidenceSummary,
    ExchangeOrderState, ExecutionAction, ExecutionLineageBinding, ExecutionPlan, ExecutionRecord,
    ExecutionState, ExecutionTransitionError, MAX_ARCHIVED_STRATEGY_RUNS,
    MAX_ORDER_MUTATIONS_PER_EXECUTION, MAX_STRATEGY_NAV_POINTS, OrderMutationKind,
    OrderMutationRecord, OrderMutationResolution, OrderMutationState, PROTECTIVE_ORDER_POLICY_V1,
    PositionSide, ProductionMarkEvidence, ProtectiveCleanupState, ProtectiveOrderResolution,
    ProtectiveOrderStatus, ProtectiveSizeAmendState, ProtectiveTriggerPriceBasis,
    ReverseContinuation, ReverseExecutionLink, ReverseLeg, STRATEGY_NAV_POINT_SCHEMA_V1,
    StrategyFinalization, StrategyFinancialPeriod, StrategyNavPoint, StrategyPeriodHistory,
    StrategyUnverifiedClosure, UnverifiedStrategyProof, UnverifiedStrategyRun,
    derive_amend_request_id, derive_client_order_id, derive_protective_algo_client_id,
    derive_reverse_open_intent_id,
    model::{valid_intent_id, valid_mutation_id},
};

pub const EXECUTION_LEDGER_SCHEMA_V1: &str = "okx.execution-ledger/v1";
pub const EXECUTION_LEDGER_SCHEMA_V2: &str = "okx.execution-ledger/v2";
pub const EXECUTION_LEDGER_SCHEMA_V3: &str = "okx.execution-ledger/v3";
pub const EXECUTION_LEDGER_SCHEMA_V4: &str = "okx.execution-ledger/v4";
pub const EXECUTION_LEDGER_SCHEMA_V5: &str = "okx.execution-ledger/v5";
pub const EXECUTION_LEDGER_SCHEMA_V6: &str = "okx.execution-ledger/v6";
pub const EXECUTION_LEDGER_SCHEMA_V7: &str = "okx.execution-ledger/v7";
pub const EXECUTION_LEDGER_SCHEMA_V8: &str = "okx.execution-ledger/v8";
pub const EXECUTION_LEDGER_SCHEMA_V9: &str = "okx.execution-ledger/v9";
pub const EXECUTION_LEDGER_SCHEMA_V10: &str = "okx.execution-ledger/v10";
pub const EXECUTION_LEDGER_SCHEMA_V11: &str = "okx.execution-ledger/v11";
pub const STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1: &str = "okx.strategy-capital-reservation/v1";
pub const EXECUTION_RISK_STOP_SCHEMA_V1: &str = "okx.execution-risk-stop/v1";
pub const MAX_EXECUTION_LEDGER_RECORDS: usize = 10_000;
const DEFAULT_EXECUTION_LEDGER_PATH: &str = r"C:\okx-runtime\execution-ledger.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLedgerEntry {
    pub record: ExecutionRecord,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareDisposition {
    Created(ExecutionLedgerEntry),
    Existing(ExecutionLedgerEntry),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationPrepareDisposition {
    Created(ExecutionLedgerEntry),
    Existing(ExecutionLedgerEntry),
}

#[derive(Debug, Error)]
pub enum ExecutionLedgerError {
    #[error("execution ledger I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("execution ledger JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("execution ledger is corrupt: {0}")]
    Corrupt(&'static str),

    #[error("execution ledger timestamp must be non-zero and monotonic")]
    InvalidTimestamp,

    #[error("execution ledger already contains intent_id with a different plan")]
    IntentConflict,

    #[error("execution ledger client_order_id collision")]
    ClientOrderIdCollision,

    #[error("execution ledger contains records from a different OKX account UID")]
    AccountScopeMismatch,

    #[error(
        "another nonterminal managed execution, unresolved protection, or pending reverse owns this instrument"
    )]
    InstrumentBusy,

    #[error("reverse execution linkage does not match the requested continuation")]
    ReverseMismatch,

    #[error("reverse execution is not ready for the opposite open leg")]
    ReverseNotReady,

    #[error("reverse execution continuation was explicitly aborted")]
    ReverseAborted,

    #[error("new risk is halted by a durable execution stop")]
    NewRiskStopped,

    #[error(
        "strategy capital reservation requires independently witnessed fresh FLAT account and operator authorization"
    )]
    CapitalReservationUnattested,

    #[error("strategy capital reservation is immutable and conflicts with existing reservation")]
    CapitalReservationConflict,

    #[error("risk-increasing execution does not belong to the capital reservation")]
    CapitalReservationScopeMismatch,

    #[error("risk-stop reason must be nonempty and at most 128 bytes")]
    InvalidRiskStopReason,

    #[error("cannot FINALIZE: exact flat and native evidence conditions absent")]
    StrategyRunNotFinalizable,

    #[error(
        "unverified strategy closure requires a fresh FLAT account, no native protection, and no run-owned execution"
    )]
    StrategyRunNotQuarantinable,

    #[error("strategy period is FINALIZED; no new-risk mutation admitted")]
    StrategyRunFinalized,

    #[error("conflicting durable exchange event or source cursor identity")]
    ExchangeEvidenceConflict,

    #[error("durable exchange evidence capacity exhausted; no proven evidence is discarded")]
    ExchangeEvidenceCapacity,

    #[error("exchange REST order/fill/bill source coverage incomplete; no checkpoint persisted")]
    ExchangeSourceIncomplete,

    #[error("execution ledger capacity of {0} records is exhausted")]
    CapacityExceeded(usize),

    #[error("execution intent '{0}' is not present in the ledger")]
    IntentNotFound(String),

    #[error(transparent)]
    Transition(#[from] ExecutionTransitionError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRiskStop {
    pub schema: String,
    pub stopped_at_ms: u64,
    pub reason: String,
}

fn validate_risk_stop(value: &ExecutionRiskStop) -> Result<(), ExecutionLedgerError> {
    if value.schema != EXECUTION_RISK_STOP_SCHEMA_V1 || value.stopped_at_ms == 0 {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid durable risk-stop metadata",
        ));
    }
    if value.reason.trim().is_empty() || value.reason.len() > 128 {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid durable risk-stop reason",
        ));
    }
    Ok(())
}

/// Persisted virtual reservation, NOT a cash transfer, proof of funding, or NAV.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyCapitalReservation {
    pub schema: String,
    pub allocation_id: String,
    pub account_uid_fingerprint: String,
    pub origin_schema: String,
    pub origin_version: String,
    pub mandate_version: String,
    pub allocated_usdt: String,
    pub opening_account_usdt_equity: String,
    pub account_generation: String,
    pub operator_authority_id: String,
    pub started_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyAllocationOpening {
    pub account_uid_fingerprint: String,
    pub account_generation: String,
    pub observed_usdt_equity: String,
    pub observed_at_ms: u64,
    pub source_history_complete: bool,
    pub zero_positions: bool,
    pub zero_pending_orders: bool,
    pub zero_native_protection: bool,
    pub operator_authority_id: String,
}

fn validate_capital_reservation(
    r: &StrategyCapitalReservation,
) -> Result<(), ExecutionLedgerError> {
    let allocated = Decimal::from_str(&r.allocated_usdt).ok();
    let equity = Decimal::from_str(&r.opening_account_usdt_equity).ok();
    if r.schema != STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1
        || !valid_intent_id(&r.allocation_id)
        || r.account_uid_fingerprint.trim().is_empty()
        || r.account_generation.trim().is_empty()
        || r.origin_schema.trim().is_empty()
        || r.origin_version.trim().is_empty()
        || r.mandate_version.trim().is_empty()
        || r.started_at_ms == 0
        || r.operator_authority_id.len() != 71
        || !r.operator_authority_id.starts_with("sha256:")
        || !r.operator_authority_id[7..]
            .bytes()
            .all(|c| c.is_ascii_hexdigit())
        || allocated.is_none_or(|v| v <= Decimal::ZERO)
        || equity.is_none_or(|v| v < Decimal::ZERO)
        || allocated.zip(equity).is_none_or(|(a, e)| a > e)
    {
        return Err(ExecutionLedgerError::CapitalReservationUnattested);
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionLedgerFile {
    schema: String,
    records: Vec<ExecutionLedgerEntry>,
    #[serde(default)]
    risk_stop: Option<ExecutionRiskStop>,
    #[serde(default)]
    capital_reservation: Option<StrategyCapitalReservation>,
    #[serde(default)]
    exchange_evidence: Option<DurableExchangeEvidence>,
    #[serde(default)]
    strategy_periods: StrategyPeriodHistory,
}

type LoadedExecutionLedger = (
    BTreeMap<String, ExecutionLedgerEntry>,
    Option<ExecutionRiskStop>,
    Option<StrategyCapitalReservation>,
    Option<DurableExchangeEvidence>,
    StrategyPeriodHistory,
);

#[derive(Debug, Clone)]
pub struct ExecutionLedgerStore {
    path: PathBuf,
    max_records: usize,
}

impl ExecutionLedgerStore {
    pub fn canonical() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_EXECUTION_LEDGER_PATH),
            max_records: MAX_EXECUTION_LEDGER_RECORDS,
        }
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_records: MAX_EXECUTION_LEDGER_RECORDS,
        }
    }

    #[cfg(test)]
    fn at_with_limit(path: impl Into<PathBuf>, max_records: usize) -> Self {
        Self {
            path: path.into(),
            max_records,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn load(&self) -> Result<LoadedExecutionLedger, ExecutionLedgerError> {
        if !self.path.exists() {
            return Ok((
                BTreeMap::new(),
                None,
                None,
                None,
                StrategyPeriodHistory::default(),
            ));
        }

        let bytes = fs::read(&self.path)?;
        let file: ExecutionLedgerFile = serde_json::from_slice(&bytes)?;
        if !matches!(
            file.schema.as_str(),
            EXECUTION_LEDGER_SCHEMA_V1
                | EXECUTION_LEDGER_SCHEMA_V2
                | EXECUTION_LEDGER_SCHEMA_V3
                | EXECUTION_LEDGER_SCHEMA_V4
                | EXECUTION_LEDGER_SCHEMA_V5
                | EXECUTION_LEDGER_SCHEMA_V6
                | EXECUTION_LEDGER_SCHEMA_V7
                | EXECUTION_LEDGER_SCHEMA_V8
                | EXECUTION_LEDGER_SCHEMA_V9
                | EXECUTION_LEDGER_SCHEMA_V10
                | EXECUTION_LEDGER_SCHEMA_V11
        ) {
            return Err(ExecutionLedgerError::Corrupt("unsupported schema"));
        }
        if let Some(stop) = &file.risk_stop {
            if !matches!(
                file.schema.as_str(),
                EXECUTION_LEDGER_SCHEMA_V6
                    | EXECUTION_LEDGER_SCHEMA_V7
                    | EXECUTION_LEDGER_SCHEMA_V8
                    | EXECUTION_LEDGER_SCHEMA_V9
                    | EXECUTION_LEDGER_SCHEMA_V10
                    | EXECUTION_LEDGER_SCHEMA_V11
            ) {
                return Err(ExecutionLedgerError::Corrupt(
                    "risk stop on legacy ledger schema",
                ));
            }
            validate_risk_stop(stop)?;
        }
        if file.capital_reservation.is_some()
            && !matches!(
                file.schema.as_str(),
                EXECUTION_LEDGER_SCHEMA_V7
                    | EXECUTION_LEDGER_SCHEMA_V8
                    | EXECUTION_LEDGER_SCHEMA_V9
                    | EXECUTION_LEDGER_SCHEMA_V10
                    | EXECUTION_LEDGER_SCHEMA_V11
            )
        {
            return Err(ExecutionLedgerError::Corrupt(
                "capital reservation on legacy schema",
            ));
        }
        if let Some(reservation) = &file.capital_reservation {
            validate_capital_reservation(reservation)?;
        }
        if let Some(evidence) = &file.exchange_evidence {
            if !matches!(
                file.schema.as_str(),
                EXECUTION_LEDGER_SCHEMA_V8
                    | EXECUTION_LEDGER_SCHEMA_V9
                    | EXECUTION_LEDGER_SCHEMA_V10
                    | EXECUTION_LEDGER_SCHEMA_V11
            ) {
                return Err(ExecutionLedgerError::Corrupt(
                    "exchange evidence on legacy schema",
                ));
            }
            evidence.validate()?;
            if file.capital_reservation.as_ref().is_none_or(|r| {
                evidence.account_uid != r.account_uid_fingerprint
                    || evidence.run_id != r.allocation_id
            }) {
                return Err(ExecutionLedgerError::AccountScopeMismatch);
            }
        }
        if !matches!(
            file.schema.as_str(),
            EXECUTION_LEDGER_SCHEMA_V9 | EXECUTION_LEDGER_SCHEMA_V10 | EXECUTION_LEDGER_SCHEMA_V11
        ) && file.strategy_periods != StrategyPeriodHistory::default()
        {
            return Err(ExecutionLedgerError::Corrupt(
                "strategy closure on legacy ledger",
            ));
        }
        if !file.strategy_periods.nav_points.is_empty()
            && !matches!(
                file.schema.as_str(),
                EXECUTION_LEDGER_SCHEMA_V10 | EXECUTION_LEDGER_SCHEMA_V11
            )
        {
            return Err(ExecutionLedgerError::Corrupt(
                "NAV points must use ledger schema v10",
            ));
        }
        if (file.strategy_periods.unverified_run.is_some()
            || !file.strategy_periods.unverified_runs.is_empty())
            && file.schema != EXECUTION_LEDGER_SCHEMA_V11
        {
            return Err(ExecutionLedgerError::Corrupt(
                "unverified closure requires ledger schema v11",
            ));
        }
        file.strategy_periods.validate(
            file.capital_reservation.as_ref(),
            file.exchange_evidence.as_ref(),
        )?;
        if file.records.len() > self.max_records {
            return Err(ExecutionLedgerError::Corrupt(
                "record count exceeds capacity",
            ));
        }

        let mut by_intent = BTreeMap::new();
        let mut client_order_ids = BTreeSet::new();
        let mut ledger_uid: Option<String> = None;
        for entry in file.records {
            let uid = &entry.record.plan.account_uid_fingerprint;
            if ledger_uid.as_ref().is_some_and(|expected| expected != uid) {
                return Err(ExecutionLedgerError::AccountScopeMismatch);
            }
            ledger_uid = Some(uid.clone());
            validate_entry(&entry)?;
            validate_strategy_run_binding(
                &entry,
                file.capital_reservation.as_ref(),
                &file.strategy_periods.archived_runs,
                &file.strategy_periods.unverified_runs,
            )?;
            let intent_id = entry.record.plan.intent_id.clone();
            if by_intent.insert(intent_id, entry.clone()).is_some() {
                return Err(ExecutionLedgerError::Corrupt("duplicate intent_id"));
            }
            if !client_order_ids.insert(entry.record.plan.client_order_id.clone()) {
                return Err(ExecutionLedgerError::Corrupt("duplicate client_order_id"));
            }
        }

        Ok((
            by_intent,
            file.risk_stop,
            file.capital_reservation,
            file.exchange_evidence,
            file.strategy_periods,
        ))
    }

    fn save(
        &self,
        entries: &BTreeMap<String, ExecutionLedgerEntry>,
        risk_stop: Option<&ExecutionRiskStop>,
        capital_reservation: Option<&StrategyCapitalReservation>,
        exchange_evidence: Option<&DurableExchangeEvidence>,
        history: &StrategyPeriodHistory,
    ) -> Result<(), ExecutionLedgerError> {
        if entries.len() > self.max_records {
            return Err(ExecutionLedgerError::CapacityExceeded(self.max_records));
        }

        let mut client_order_ids = BTreeSet::new();
        let mut ledger_uid: Option<&str> = None;
        for (intent_id, entry) in entries {
            let uid = entry.record.plan.account_uid_fingerprint.as_str();
            if ledger_uid.is_some_and(|expected| expected != uid) {
                return Err(ExecutionLedgerError::AccountScopeMismatch);
            }
            ledger_uid = Some(uid);
            validate_entry(entry)?;
            validate_strategy_run_binding(
                entry,
                capital_reservation,
                &history.archived_runs,
                &history.unverified_runs,
            )?;
            if intent_id != &entry.record.plan.intent_id {
                return Err(ExecutionLedgerError::Corrupt(
                    "map key does not match intent_id",
                ));
            }
            if !client_order_ids.insert(entry.record.plan.client_order_id.clone()) {
                return Err(ExecutionLedgerError::ClientOrderIdCollision);
            }
        }

        if let Some(stop) = risk_stop {
            validate_risk_stop(stop)?;
        }

        if let Some(reservation) = capital_reservation {
            validate_capital_reservation(reservation)?;
        }

        if let Some(evidence) = exchange_evidence {
            evidence.validate()?;
            if capital_reservation.is_none_or(|r| {
                evidence.account_uid != r.account_uid_fingerprint
                    || evidence.run_id != r.allocation_id
            }) {
                return Err(ExecutionLedgerError::AccountScopeMismatch);
            }
        }
        history.validate(capital_reservation, exchange_evidence)?;
        let parent = self
            .path
            .parent()
            .ok_or(ExecutionLedgerError::Corrupt("ledger path has no parent"))?;
        fs::create_dir_all(parent)?;

        let payload = serde_json::to_vec_pretty(&ExecutionLedgerFile {
            schema: EXECUTION_LEDGER_SCHEMA_V11.to_owned(),
            records: entries.values().cloned().collect(),
            risk_stop: risk_stop.cloned(),
            capital_reservation: capital_reservation.cloned(),
            exchange_evidence: exchange_evidence.cloned(),
            strategy_periods: history.clone(),
        })?;
        if payload.len() > 128 * 1024 * 1024 {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }

        let temp = temp_path(&self.path);
        let mut file = File::create(&temp)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        drop(file);

        atomic_replace(&temp, &self.path)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct DurableExecutionLedger {
    store: ExecutionLedgerStore,
    entries: BTreeMap<String, ExecutionLedgerEntry>,
    risk_stop: Option<ExecutionRiskStop>,
    capital_reservation: Option<StrategyCapitalReservation>,
    exchange_evidence: Option<DurableExchangeEvidence>,
    strategy_periods: StrategyPeriodHistory,
}

impl DurableExecutionLedger {
    pub fn canonical(observed_at_ms: u64) -> Result<Self, ExecutionLedgerError> {
        Self::open(ExecutionLedgerStore::canonical(), observed_at_ms)
    }

    pub fn open(
        store: ExecutionLedgerStore,
        observed_at_ms: u64,
    ) -> Result<Self, ExecutionLedgerError> {
        require_timestamp(observed_at_ms)?;
        let (mut entries, risk_stop, capital_reservation, exchange_evidence, strategy_periods) =
            store.load()?;
        let mut recovered = false;

        for entry in entries.values_mut() {
            if entry.record.state == ExecutionState::Submitting {
                entry.record.mark_unknown_submission()?;
                entry.updated_at_ms = monotonic_timestamp(entry, observed_at_ms)?;
                recovered = true;
            }
            if entry.record.recover_inflight_mutation() {
                entry.updated_at_ms = monotonic_timestamp(entry, observed_at_ms)?;
                recovered = true;
            }
        }

        if recovered {
            store.save(
                &entries,
                risk_stop.as_ref(),
                capital_reservation.as_ref(),
                exchange_evidence.as_ref(),
                &strategy_periods,
            )?;
        }

        Ok(Self {
            store,
            entries,
            risk_stop,
            capital_reservation,
            exchange_evidence,
            strategy_periods,
        })
    }

    /// Persist a one-way new-risk stop in the existing atomic execution ledger.
    /// A future explicit, reviewed authorization is required for any reset.
    pub fn stop_new_risk(
        &mut self,
        reason: impl Into<String>,
        observed_at_ms: u64,
    ) -> Result<ExecutionRiskStop, ExecutionLedgerError> {
        if let Some(existing) = &self.risk_stop {
            return Ok(existing.clone());
        }
        require_timestamp(observed_at_ms)?;
        let reason = reason.into();
        if reason.trim().is_empty() || reason.len() > 128 {
            return Err(ExecutionLedgerError::InvalidRiskStopReason);
        }
        let stop = ExecutionRiskStop {
            schema: EXECUTION_RISK_STOP_SCHEMA_V1.to_owned(),
            stopped_at_ms: observed_at_ms,
            reason,
        };
        self.store.save(
            &self.entries,
            Some(&stop),
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
            &self.strategy_periods,
        )?;
        self.risk_stop = Some(stop.clone());
        Ok(stop)
    }

    /// Append exact account-sourced events to the existing atomic ledger.
    /// The caller must first independently prove source coherence.
    pub fn capture_exchange_evidence(
        &mut self,
        facts: &AccountLedgerFacts,
    ) -> Result<ExchangeEvidenceSummary, ExecutionLedgerError> {
        self.capture_exchange_evidence_with_freshness(facts, false)
    }

    /// Financial period closure always seals a freshly observed source
    /// checkpoint, even when the prior five minutes had no trade activity.
    pub fn capture_exchange_evidence_fresh_cursor(
        &mut self,
        facts: &AccountLedgerFacts,
    ) -> Result<ExchangeEvidenceSummary, ExecutionLedgerError> {
        self.capture_exchange_evidence_with_freshness(facts, true)
    }

    fn capture_exchange_evidence_with_freshness(
        &mut self,
        facts: &AccountLedgerFacts,
        require_fresh_cursor: bool,
    ) -> Result<ExchangeEvidenceSummary, ExecutionLedgerError> {
        if self.strategy_periods.finalized_run.is_some() {
            return self
                .exchange_evidence
                .as_ref()
                .map(DurableExchangeEvidence::summary)
                .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable);
        }
        let run = self
            .capital_reservation
            .as_ref()
            .ok_or(ExecutionLedgerError::CapitalReservationUnattested)?;
        let mut next = if let Some(old) = &self.exchange_evidence {
            old.clone()
        } else {
            DurableExchangeEvidence::new(run)?
        };
        next.capture(
            facts,
            &self.entries.values().cloned().collect::<Vec<_>>(),
            run,
            require_fresh_cursor,
        )?;
        if self.exchange_evidence.as_ref() != Some(&next) {
            self.store.save(
                &self.entries,
                self.risk_stop.as_ref(),
                self.capital_reservation.as_ref(),
                Some(&next),
                &self.strategy_periods,
            )?;
            self.exchange_evidence = Some(next);
        }
        Ok(self
            .exchange_evidence
            .as_ref()
            .expect("persisted")
            .summary())
    }

    /// Recompute the run from immutable source events after restart.
    /// No market-price authority is inferred from the private Demo account.
    pub fn financial_period(
        &self,
        facts: &AccountLedgerFacts,
        coherent: bool,
    ) -> Option<StrategyFinancialPeriod> {
        self.financial_period_with_production_marks(facts, coherent, &BTreeMap::new())
    }

    /// Source-attested Production marks are supplied only by the single
    /// public observation owner. Missing marks make open NAV INCOMPLETE.
    pub fn financial_period_with_production_marks(
        &self,
        facts: &AccountLedgerFacts,
        coherent: bool,
        production_marks: &BTreeMap<String, ProductionMarkEvidence>,
    ) -> Option<StrategyFinancialPeriod> {
        self.capital_reservation.as_ref().map(|run| {
            crate::assess_persisted_financial_period_with_production_marks(
                run,
                self.exchange_evidence.as_ref(),
                &self.entries.values().cloned().collect::<Vec<_>>(),
                facts,
                coherent,
                production_marks,
            )
        })
    }

    /// The caller's current REST/WS source was just checked and captured
    /// under the same ledger lock; quiet cursors may have been coalesced.
    pub fn financial_period_with_verified_source(
        &self,
        facts: &AccountLedgerFacts,
        coherent: bool,
        marks: &BTreeMap<String, ProductionMarkEvidence>,
        captured: bool,
    ) -> Option<StrategyFinancialPeriod> {
        self.capital_reservation.as_ref().map(|run| {
            crate::assess_persisted_financial_period_with_verified_source(
                run,
                self.exchange_evidence.as_ref(),
                &self.entries.values().cloned().collect::<Vec<_>>(),
                facts,
                coherent,
                marks,
                captured,
            )
        })
    }

    /// One atomic writer for NAV, high-water, source-exact daily gross loss
    /// and any irreversible breach STOP. No new risk permission is minted.
    pub fn observe_strategy_nav(
        &mut self,
        report: &StrategyFinancialPeriod,
        facts: &AccountLedgerFacts,
        now: u64,
    ) -> Result<Option<StrategyNavPoint>, ExecutionLedgerError> {
        let Some(run) = &self.capital_reservation else {
            return Ok(None);
        };
        if self.strategy_periods.finalized_run.is_some()
            || report.run_id != run.allocation_id
            || !report.reasons.is_empty()
            || !matches!(report.state, "FLAT_RECONCILED" | "ACTIVE_VALUED")
            || report.usable_for_new_risk
            || facts.summary.authority.account_uid_fingerprint != run.account_uid_fingerprint
            || !DurableExchangeEvidence::complete_source_checkpoint(facts)
            || self
                .exchange_evidence
                .as_ref()
                .is_none_or(|e| e.checksum_sha256 != report.evidence_sha256)
        {
            return Ok(None);
        }
        let Some(nav) = report
            .nav_usdt
            .as_deref()
            .and_then(|v| Decimal::from_str(v).ok())
        else {
            return Ok(None);
        };
        let as_of = facts
            .summary
            .current_account_as_of_ms
            .as_deref()
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(ExecutionLedgerError::Corrupt(
                "missing NAV account source time",
            ))?;
        if now.saturating_add(2_000) < as_of || now.abs_diff(as_of) > 30_000 {
            return Ok(None);
        }
        // A depleted or negative NAV is an auditable terminal observation:
        // persist both the value and the irreversible STOP in one file.
        let evidence = self
            .exchange_evidence
            .as_ref()
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        let (source_cursor_key, source_cursor) = evidence
            .cursors
            .iter()
            .filter(|(_, cursor)| cursor.account_as_of_ms <= as_of)
            .max_by_key(|(_, cursor)| cursor.account_as_of_ms)
            .ok_or(ExecutionLedgerError::ExchangeSourceIncomplete)?;
        if as_of - source_cursor.account_as_of_ms
            > crate::exchange_evidence::MAX_SOURCE_COVERAGE_GAP_MS
        {
            return Ok(None);
        }
        let cursor_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(source_cursor)?));
        let day = as_of / 86_400_000 * 86_400_000;
        let mut gross_loss = Decimal::ZERO;
        for event in evidence.events.values() {
            if event.strategy_run_id.as_deref() != Some(run.allocation_id.as_str())
                || event.payload["source"] != "OKX_REST_BILL"
                || event.payload["ts"]
                    .as_u64()
                    .is_none_or(|ts| ts < day || ts > as_of)
            {
                continue;
            }
            if event.payload["ccy"] != "USDT" {
                return Err(ExecutionLedgerError::Corrupt(
                    "strategy daily loss currency",
                ));
            }
            for field in ["fee", "pnl"] {
                let amount = event.payload[field]
                    .as_str()
                    .and_then(|v| Decimal::from_str(v).ok())
                    .ok_or(ExecutionLedgerError::Corrupt(
                        "invalid run-owned daily bill",
                    ))?;
                if amount < Decimal::ZERO {
                    gross_loss -= amount;
                }
            }
        }
        let prior = self.strategy_periods.nav_points.last();
        if prior.is_some_and(|p| p.observed_at_ms >= now) {
            return Ok(None);
        }
        let allocation = Decimal::from_str(&run.allocated_usdt)
            .map_err(|_| ExecutionLedgerError::CapitalReservationUnattested)?;
        let peak = prior
            .filter(|p| p.run_id == run.allocation_id)
            .and_then(|p| Decimal::from_str(&p.high_water_usdt).ok())
            .unwrap_or(allocation)
            .max(nav);
        if prior.is_some_and(|p| {
            p.run_id == run.allocation_id
                && p.nav_usdt == nav.normalize().to_string()
                && p.daily_gross_loss_usdt == gross_loss.normalize().to_string()
                && p.utc_day_start_ms == day
                && p.evidence_sha256 == report.evidence_sha256
        }) {
            return Ok(None);
        }
        if self.strategy_periods.nav_points.len() >= MAX_STRATEGY_NAV_POINTS {
            return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
        }
        let mut point = StrategyNavPoint {
            schema: STRATEGY_NAV_POINT_SCHEMA_V1.to_owned(),
            run_id: run.allocation_id.clone(),
            observed_at_ms: now,
            account_as_of_ms: as_of,
            evidence_sha256: report.evidence_sha256.clone(),
            source_cursor_key: source_cursor_key.clone(),
            source_cursor_sha256: cursor_sha256,
            nav_usdt: nav.normalize().to_string(),
            high_water_usdt: peak.normalize().to_string(),
            daily_gross_loss_usdt: gross_loss.normalize().to_string(),
            utc_day_start_ms: day,
            previous_sha256: prior.map_or("", |p| p.checksum_sha256.as_str()).to_owned(),
            checksum_sha256: String::new(),
        };
        point.checksum_sha256 = point.hash()?;
        point.validate()?;
        let mut reason = (nav <= Decimal::ZERO).then_some("STRATEGY_NAV_DEPLETED");
        // Only exact immutable run-bound plans may supply risk limits.
        for entry in self.entries.values() {
            if entry.record.strategy_run_id.as_deref() != Some(run.allocation_id.as_str())
                || !entry.record.plan.action.is_risk_increasing()
            {
                continue;
            }
            let binding = entry
                .record
                .plan
                .risk_binding
                .as_ref()
                .ok_or(ExecutionLedgerError::Corrupt("missing run risk policy"))?;
            if binding.mandate.version != run.mandate_version {
                return Err(ExecutionLedgerError::CapitalReservationScopeMismatch);
            }
            let mandate_dd = Decimal::from_str(&binding.mandate.max_drawdown_ratio)
                .map_err(|_| ExecutionLedgerError::Corrupt("invalid mandate drawdown"))?;
            let policy_dd = Decimal::from_str(&binding.policy.max_drawdown_ratio)
                .map_err(|_| ExecutionLedgerError::Corrupt("invalid policy drawdown"))?;
            let daily = Decimal::from_str(&binding.policy.max_daily_realized_loss_usd)
                .map_err(|_| ExecutionLedgerError::Corrupt("invalid policy daily loss"))?;
            if reason.is_none() {
                reason =
                    strategy_nav_stop_reason(nav, peak, gross_loss, mandate_dd, policy_dd, daily);
            }
        }
        let mut next = self.strategy_periods.clone();
        next.nav_points.push(point.clone());
        next.validate(
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
        )?;
        let new_stop = if self.risk_stop.is_none() {
            reason.map(|value| ExecutionRiskStop {
                schema: EXECUTION_RISK_STOP_SCHEMA_V1.to_owned(),
                stopped_at_ms: now,
                reason: value.to_owned(),
            })
        } else {
            None
        };
        self.store.save(
            &self.entries,
            new_stop.as_ref().or(self.risk_stop.as_ref()),
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
            &next,
        )?;
        self.strategy_periods = next;
        if let Some(stop) = new_stop {
            self.risk_stop = Some(stop);
        }
        Ok(Some(point))
    }

    pub fn strategy_nav_watermark(&self) -> Option<&StrategyNavPoint> {
        self.strategy_periods.nav_points.last().filter(|p| {
            self.capital_reservation
                .as_ref()
                .is_some_and(|r| r.allocation_id == p.run_id)
        })
    }

    pub fn strategy_nav_point_count(&self) -> usize {
        self.strategy_periods.nav_points.len()
    }

    pub fn unverified_run(&self) -> Option<&StrategyUnverifiedClosure> {
        self.strategy_periods.unverified_run.as_ref()
    }

    pub fn archived_unverified_proofs(&self) -> Vec<UnverifiedStrategyProof> {
        self.strategy_periods
            .unverified_runs
            .iter()
            .map(UnverifiedStrategyRun::proof)
            .collect()
    }

    pub fn quarantine_unverified_strategy_run(
        &mut self,
        facts: &AccountLedgerFacts,
        coherent: bool,
        native_protection_empty: bool,
        observed_at_ms: u64,
    ) -> Result<StrategyUnverifiedClosure, ExecutionLedgerError> {
        let run = self
            .capital_reservation
            .as_ref()
            .ok_or(ExecutionLedgerError::StrategyRunNotQuarantinable)?;
        let source = self
            .exchange_evidence
            .as_ref()
            .ok_or(ExecutionLedgerError::StrategyRunNotQuarantinable)?;
        if let Some(closed) = self.strategy_periods.unverified_run.as_ref() {
            closed.validate(run, source)?;
            return Ok(closed.clone());
        }
        let as_of = facts
            .summary
            .current_account_as_of_ms
            .as_deref()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or(ExecutionLedgerError::StrategyRunNotQuarantinable)?;
        if self.strategy_periods.finalized_run.is_some()
            || !coherent
            || !native_protection_empty
            || observed_at_ms < as_of
            || observed_at_ms - as_of > 30_000
            || facts.summary.open_positions != 0
            || facts.summary.pending_orders != 0
            || facts.summary.authority.account_uid_fingerprint != run.account_uid_fingerprint
            || !DurableExchangeEvidence::complete_source_checkpoint(facts)
            || !source.summary().source_gap_over_two_hours
            || !source.events.is_empty()
            || self.risk_stop.is_some()
            || self.entries.values().any(|e| {
                e.created_at_ms >= run.started_at_ms
                    || e.record.strategy_run_id.as_deref() == Some(run.allocation_id.as_str())
                    || !e.record.state.is_terminal()
            })
            || facts
                .exchange_fills
                .iter()
                .any(|f| f.fill_time_ms >= run.started_at_ms)
            || facts
                .exchange_bills
                .iter()
                .any(|b| b.timestamp_ms >= run.started_at_ms)
            || facts.exchange_orders.iter().any(|o| {
                o.update_time_ms
                    .parse::<u64>()
                    .is_ok_and(|ms| ms >= run.started_at_ms)
            })
        {
            return Err(ExecutionLedgerError::StrategyRunNotQuarantinable);
        }
        // Source remains immutable and explicitly incomplete: no NAV, PnL,
        // or financial finalization is inferred from the current FLAT state.
        let closed = StrategyUnverifiedClosure::new(run, source, observed_at_ms)?;
        let mut next = self.strategy_periods.clone();
        next.unverified_run = Some(closed.clone());
        next.validate(
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
        )?;
        self.store.save(
            &self.entries,
            self.risk_stop.as_ref(),
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
            &next,
        )?;
        self.strategy_periods = next;
        Ok(closed)
    }

    pub fn finalized_run(&self) -> Option<&StrategyFinalization> {
        self.strategy_periods.finalized_run.as_ref()
    }

    pub fn archived_runs_count(&self) -> usize {
        self.strategy_periods.archived_runs.len()
    }

    /// Exact immutable per-period NAV and checksum, without raw private
    /// exchange events. Every record is validated at ledger open/save.
    pub fn archived_strategy_proofs(&self) -> Vec<crate::ArchivedStrategyProof> {
        self.strategy_periods
            .archived_runs
            .iter()
            .map(crate::ArchivedStrategyRun::proof)
            .collect()
    }

    /// Explicit immutable close: protectiveness/WS/account coherence must
    /// already have been proved independently by the caller.
    pub fn finalize_strategy_run(
        &mut self,
        facts: &AccountLedgerFacts,
        coherent: bool,
        native_protection_empty: bool,
        observed_at_ms: u64,
    ) -> Result<StrategyFinalization, ExecutionLedgerError> {
        let as_of = facts
            .summary
            .current_account_as_of_ms
            .as_deref()
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable)?;
        if !coherent
            || !native_protection_empty
            || observed_at_ms < as_of
            || observed_at_ms - as_of > 30_000
            || facts.summary.open_positions != 0
            || facts.summary.pending_orders != 0
            || !facts.exchange_orders_history_complete
            || !facts.exchange_fills_history_complete
            || !facts.bills_history_complete
            || facts
                .summary
                .history_coverage
                .iter()
                .any(|c| !c.complete_within_bound)
        {
            return Err(ExecutionLedgerError::StrategyRunNotFinalizable);
        }
        let reservation = self
            .capital_reservation
            .as_ref()
            .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable)?;
        if facts.summary.authority.account_uid_fingerprint != reservation.account_uid_fingerprint {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        if self.strategy_periods.unverified_run.is_some() {
            return Err(ExecutionLedgerError::StrategyRunNotFinalizable);
        }
        if let Some(done) = &self.strategy_periods.finalized_run {
            return Ok(done.clone());
        }
        let evidence = self
            .exchange_evidence
            .as_ref()
            .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable)?;
        let result = self
            .financial_period(facts, coherent)
            .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable)?;
        let closed = StrategyFinalization::new(reservation, evidence, &result, observed_at_ms)?;
        let mut next = self.strategy_periods.clone();
        next.finalized_run = Some(closed.clone());
        self.store.save(
            &self.entries,
            self.risk_stop.as_ref(),
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
            &next,
        )?;
        self.strategy_periods = next;
        Ok(closed)
    }

    pub fn exchange_evidence_summary(&self) -> Option<ExchangeEvidenceSummary> {
        self.exchange_evidence
            .as_ref()
            .map(DurableExchangeEvidence::summary)
    }

    pub fn risk_stop(&self) -> Option<&ExecutionRiskStop> {
        self.risk_stop.as_ref()
    }

    pub fn capital_reservation(&self) -> Option<&StrategyCapitalReservation> {
        self.capital_reservation.as_ref()
    }

    /// Atomic, non-resettable reservation in the single execution ledger.
    /// This is only a virtual upper bound, not yet a certified strategy NAV.
    pub fn reserve_strategy_capital(
        &mut self,
        reservation: StrategyCapitalReservation,
        opening: &StrategyAllocationOpening,
        now_ms: u64,
    ) -> Result<StrategyCapitalReservation, ExecutionLedgerError> {
        validate_capital_reservation(&reservation)?;
        if let Some(existing) = &self.capital_reservation {
            if *existing == reservation {
                return Ok(existing.clone());
            }
            if self.strategy_periods.finalized_run.is_none()
                && self.strategy_periods.unverified_run.is_none()
            {
                return Err(ExecutionLedgerError::CapitalReservationConflict);
            }
        }
        if self
            .strategy_periods
            .archived_runs
            .iter()
            .any(|item| item.reservation.allocation_id == reservation.allocation_id)
            || self
                .strategy_periods
                .unverified_runs
                .iter()
                .any(|item| item.reservation.allocation_id == reservation.allocation_id)
        {
            return Err(ExecutionLedgerError::CapitalReservationConflict);
        }
        let exact = reservation.account_uid_fingerprint == opening.account_uid_fingerprint
            && reservation.account_generation == opening.account_generation
            && reservation.operator_authority_id == opening.operator_authority_id
            && reservation.started_at_ms == opening.observed_at_ms
            && Decimal::from_str(&reservation.opening_account_usdt_equity).ok()
                == Decimal::from_str(&opening.observed_usdt_equity).ok();
        if !exact
            || now_ms < opening.observed_at_ms
            || now_ms - opening.observed_at_ms > 10_000
            || !opening.source_history_complete
            || !opening.zero_positions
            || !opening.zero_pending_orders
            || !opening.zero_native_protection
            || self.risk_stop.is_some()
            || self.entries.values().any(|e| {
                !e.record.state.is_terminal() || e.created_at_ms >= reservation.started_at_ms
            })
            || self.entries.values().any(|e| {
                e.record.plan.account_uid_fingerprint != reservation.account_uid_fingerprint
            })
        {
            return Err(ExecutionLedgerError::CapitalReservationUnattested);
        }
        let mut next = self.strategy_periods.clone();
        if let Some(prior) = &self.capital_reservation {
            if next.archived_runs.len() + next.unverified_runs.len() >= MAX_ARCHIVED_STRATEGY_RUNS {
                return Err(ExecutionLedgerError::ExchangeEvidenceCapacity);
            }
            let old_source = self
                .exchange_evidence
                .clone()
                .ok_or(ExecutionLedgerError::StrategyRunNotFinalizable)?;
            if let Some(finalized) = next.finalized_run.take() {
                next.archived_runs.push(ArchivedStrategyRun {
                    reservation: prior.clone(),
                    evidence: old_source,
                    finalization: finalized,
                });
            } else if let Some(closed) = next.unverified_run.take() {
                next.unverified_runs.push(UnverifiedStrategyRun {
                    reservation: prior.clone(),
                    evidence: old_source,
                    closure: closed,
                });
            } else {
                return Err(ExecutionLedgerError::CapitalReservationConflict);
            }
        }
        self.store.save(
            &self.entries,
            self.risk_stop.as_ref(),
            Some(&reservation),
            None,
            &next,
        )?;
        self.strategy_periods = next;
        self.exchange_evidence = None;
        self.capital_reservation = Some(reservation.clone());
        Ok(reservation)
    }

    /// A saved capital boundary may only admit NEW risk from its exact
    /// origin and risk-mandate. Old unsent intents cannot cross the boundary.
    fn validate_capital_intent(
        &self,
        record: &ExecutionRecord,
        created_at_ms: u64,
    ) -> Result<(), ExecutionLedgerError> {
        let Some(r) = &self.capital_reservation else {
            return Ok(());
        };
        if !record.plan.action.is_risk_increasing() {
            return Ok(());
        }
        if self.strategy_periods.finalized_run.is_some()
            || self.strategy_periods.unverified_run.is_some()
        {
            return Err(ExecutionLedgerError::StrategyRunFinalized);
        }
        if created_at_ms <= r.started_at_ms
            || record.plan.account_uid_fingerprint != r.account_uid_fingerprint
            || record.lineage.as_ref().is_none_or(|l| {
                l.origin_schema != r.origin_schema || l.origin_version != r.origin_version
            })
            || record
                .plan
                .risk_binding
                .as_ref()
                .is_none_or(|b| b.mandate.version != r.mandate_version)
            || record.strategy_run_id.as_deref() != Some(r.allocation_id.as_str())
        {
            return Err(ExecutionLedgerError::CapitalReservationScopeMismatch);
        }
        Ok(())
    }

    pub fn require_new_risk_allowed(
        &self,
        action: ExecutionAction,
    ) -> Result<(), ExecutionLedgerError> {
        if self.strategy_periods.finalized_run.is_some() && action.is_risk_increasing() {
            return Err(ExecutionLedgerError::StrategyRunFinalized);
        }
        if self.risk_stop.is_some() && action.is_risk_increasing() {
            return Err(ExecutionLedgerError::NewRiskStopped);
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, intent_id: &str) -> Option<&ExecutionLedgerEntry> {
        self.entries.get(intent_id)
    }

    pub fn entries(&self) -> impl Iterator<Item = &ExecutionLedgerEntry> {
        self.entries.values()
    }

    pub fn prepare(
        &mut self,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        self.prepare_with_lineage(plan, None, observed_at_ms)
    }

    pub fn prepare_with_lineage(
        &mut self,
        plan: ExecutionPlan,
        lineage: Option<ExecutionLineageBinding>,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        let mut record = ExecutionRecord::new(plan);
        if let Some(lineage) = lineage {
            record.bind_lineage(lineage)?;
        }
        self.prepare_record(record, None, observed_at_ms, false)
    }

    /// Only the Rust executor can invoke this after its exact-ID venue OCO
    /// inventory + position proof and final private WS event fence.
    /// No general ledger caller gets an active-protection bypass.
    pub(crate) fn prepare_with_confirmed_active_coverage(
        &mut self,
        plan: ExecutionPlan,
        lineage: Option<ExecutionLineageBinding>,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        if !matches!(plan.action, ExecutionAction::Add | ExecutionAction::Hedge) {
            return Err(ExecutionLedgerError::InstrumentBusy);
        }
        let mut record = ExecutionRecord::new(plan);
        if let Some(lineage) = lineage {
            record.bind_lineage(lineage)?;
        }
        self.prepare_record(record, None, observed_at_ms, true)
    }

    pub fn prepare_reverse_close(
        &mut self,
        plan: ExecutionPlan,
        target_position_side: PositionSide,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        self.prepare_reverse_close_with_lineage(plan, target_position_side, None, observed_at_ms)
    }

    pub fn prepare_reverse_close_with_lineage(
        &mut self,
        plan: ExecutionPlan,
        target_position_side: PositionSide,
        lineage: Option<ExecutionLineageBinding>,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        if plan.action != ExecutionAction::Close {
            return Err(ExecutionLedgerError::ReverseMismatch);
        }
        let root_intent_id = plan.intent_id.clone();
        let open_intent_id = derive_reverse_open_intent_id(&root_intent_id);
        let mut record = ExecutionRecord::new(plan);
        if let Some(lineage) = lineage {
            record.bind_lineage(lineage)?;
        }
        record.attach_reverse(ReverseExecutionLink::close(
            root_intent_id,
            open_intent_id,
            target_position_side,
        )?)?;
        self.prepare_record(record, None, observed_at_ms, false)
    }

    pub fn prepare_reverse_open(
        &mut self,
        root_intent_id: &str,
        plan: ExecutionPlan,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        let root = self
            .entries
            .get(root_intent_id)
            .cloned()
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(root_intent_id.to_owned()))?;
        let reverse = root
            .record
            .reverse
            .as_ref()
            .ok_or(ExecutionLedgerError::ReverseMismatch)?;
        if reverse.leg != ReverseLeg::Close
            || plan.action != ExecutionAction::Open
            || plan.intent_id != reverse.open_intent_id
            || plan.instrument_id != root.record.plan.instrument_id
            || plan.account_uid_fingerprint != root.record.plan.account_uid_fingerprint
            || plan.trade_mode != root.record.plan.trade_mode
            || plan.position_side != reverse.target_position_side
        {
            return Err(ExecutionLedgerError::ReverseMismatch);
        }
        if reverse.continuation == ReverseContinuation::Aborted {
            return Err(ExecutionLedgerError::ReverseAborted);
        }
        if root.record.state != ExecutionState::Filled {
            return Err(ExecutionLedgerError::ReverseNotReady);
        }

        let mut record = ExecutionRecord::new(plan);
        if let Some(lineage) = root.record.lineage.clone() {
            record.bind_lineage(lineage)?;
        }
        record.attach_reverse(ReverseExecutionLink::open_from(reverse)?)?;
        self.prepare_record(record, Some(root_intent_id), observed_at_ms, false)
    }

    pub fn abort_reverse(
        &mut self,
        root_intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(root_intent_id, observed_at_ms, |record| {
            record.abort_reverse_continuation()
        })
    }

    /// Terminalize only an unsent PREPARED intent. No exchange gateway exists
    /// here. SUBMITTING/UNKNOWN cannot be cleared, even after a restart.
    pub fn abandon_prepared(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let entry = self
            .get(intent_id)
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;
        if entry.record.state == ExecutionState::Rejected
            && entry.record.rejection_code.as_deref()
                == Some(crate::state::LOCAL_PREPARED_ABANDONED_CODE)
        {
            return Ok(entry.clone());
        }
        self.mutate(intent_id, observed_at_ms, |record| {
            record.abandon_prepared()
        })
    }

    fn prepare_record(
        &mut self,
        mut record: ExecutionRecord,
        reverse_root_bypass: Option<&str>,
        observed_at_ms: u64,
        confirmed_coverage: bool,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        require_timestamp(observed_at_ms)?;
        validate_plan_identity(&record.plan)?;
        validate_reverse_link(&record)?;
        // Bind from the single trusted ledger owner, never from ChatGPT/plan.
        // A CLOSE has no proven lot owner yet and MUST remain unattributed.
        if record.plan.action.is_risk_increasing()
            && let Some(run) = &self.capital_reservation
        {
            record.strategy_run_id = Some(run.allocation_id.clone());
        }
        self.validate_capital_intent(&record, observed_at_ms)?;
        // One ledger is owned by exactly one authenticated exchange UID,
        // even after FLAT or when the previous managed intents are terminal.
        if self.entries.values().any(|entry| {
            entry.record.plan.account_uid_fingerprint != record.plan.account_uid_fingerprint
        }) {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }

        if let Some(existing) = self.entries.get(&record.plan.intent_id) {
            return if existing.record == record {
                Ok(PrepareDisposition::Existing(existing.clone()))
            } else {
                Err(ExecutionLedgerError::IntentConflict)
            };
        }

        if self.entries.len() >= self.store.max_records {
            return Err(ExecutionLedgerError::CapacityExceeded(
                self.store.max_records,
            ));
        }

        let busy = if confirmed_coverage {
            self.instrument_reserved_with_confirmed_coverage(
                &record.plan.instrument_id,
                &record.plan.intent_id,
                record.plan.action,
                reverse_root_bypass,
                true,
            )
        } else {
            self.instrument_reserved(
                &record.plan.instrument_id,
                &record.plan.intent_id,
                record.plan.action,
                reverse_root_bypass,
            )
        };
        if busy {
            return Err(ExecutionLedgerError::InstrumentBusy);
        }

        if self
            .entries
            .values()
            .any(|entry| entry.record.plan.client_order_id == record.plan.client_order_id)
        {
            return Err(ExecutionLedgerError::ClientOrderIdCollision);
        }

        let entry = ExecutionLedgerEntry {
            record,
            created_at_ms: observed_at_ms,
            updated_at_ms: observed_at_ms,
        };

        let intent_id = entry.record.plan.intent_id.clone();
        self.commit_new(intent_id, entry.clone())?;
        Ok(PrepareDisposition::Created(entry))
    }

    fn instrument_reserved(
        &self,
        instrument_id: &str,
        incoming_intent_id: &str,
        incoming_action: ExecutionAction,
        reverse_root_bypass: Option<&str>,
    ) -> bool {
        self.instrument_reserved_with_confirmed_coverage(
            instrument_id,
            incoming_intent_id,
            incoming_action,
            reverse_root_bypass,
            false,
        )
    }

    fn instrument_reserved_with_confirmed_coverage(
        &self,
        instrument_id: &str,
        incoming_intent_id: &str,
        incoming_action: ExecutionAction,
        reverse_root_bypass: Option<&str>,
        confirmed_coverage: bool,
    ) -> bool {
        self.entries.values().any(|entry| {
            if entry.record.plan.instrument_id != instrument_id {
                return false;
            }
            if !entry.record.state.is_terminal() {
                return true;
            }
            // The ONLY newly permitted reservation is a terminal, fully
            // filled, actively protected old lot proved under the same
            // executor lock by exact venue GET + complete OCO inventory +
            // per-Hedge-side aggregate quantities + final private WS fence.
            // UNKNOWN, failed, canceled, pending amend/cleanup and reverse
            // linkage can never borrow that permission.
            if entry.record.protection_blocks_new_managed_intent()
                && (incoming_action.is_risk_increasing()
                    || incoming_action == ExecutionAction::Reduce)
            {
                let proven_old_lot = confirmed_coverage
                    && matches!(
                        incoming_action,
                        ExecutionAction::Add | ExecutionAction::Hedge
                    )
                    && entry.record.state == ExecutionState::Filled
                    && entry.record.active_mutation().is_none()
                    && entry.record.protection.as_ref().is_some_and(|protection| {
                        protection.status == ProtectiveOrderStatus::Active
                            && protection.cleanup.is_none()
                            && protection.size_amend.as_ref().is_none_or(|amend| {
                                amend.state == ProtectiveSizeAmendState::Confirmed
                            })
                    });
                if !proven_old_lot {
                    return true;
                }
            }
            let Some(reverse) = entry.record.reverse.as_ref() else {
                return false;
            };
            reverse.leg == ReverseLeg::Close
                && reverse.continuation == ReverseContinuation::Required
                && entry.record.state == ExecutionState::Filled
                && !self.entries.contains_key(&reverse.open_intent_id)
                && !(reverse_root_bypass == Some(reverse.root_intent_id.as_str())
                    && incoming_intent_id == reverse.open_intent_id)
        })
    }

    pub fn begin_submission(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.begin_submission_with_timing(intent_id, None, observed_at_ms)
    }

    pub fn begin_submission_with_timing(
        &mut self,
        intent_id: &str,
        request_exchange_time_ms: Option<u64>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let entry = self
            .get(intent_id)
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;
        self.require_new_risk_allowed(entry.record.plan.action)?;
        self.validate_capital_intent(&entry.record, entry.created_at_ms)?;
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.begin_submission_with_timing(request_exchange_time_ms)
        })
    }

    pub fn acknowledge(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.acknowledge_with_gateway_timing(intent_id, order_id, None, observed_at_ms)
    }

    pub fn acknowledge_with_gateway_timing(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        gateway_timing_us: Option<(u64, u64)>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let order_id = order_id.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
            if let Some((in_time_us, out_time_us)) = gateway_timing_us {
                record.record_okx_gateway_timing(in_time_us, out_time_us)?;
            }
            record.acknowledge(order_id)
        })
    }

    pub fn mark_unknown_submission(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mark_unknown_submission_with_gateway_timing(intent_id, None, observed_at_ms)
    }

    pub fn mark_unknown_submission_with_gateway_timing(
        &mut self,
        intent_id: &str,
        gateway_timing_us: Option<(u64, u64)>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, move |record| {
            if let Some((in_time_us, out_time_us)) = gateway_timing_us {
                record.record_okx_gateway_timing(in_time_us, out_time_us)?;
            }
            record.mark_unknown_submission()
        })
    }

    pub fn reject_known(
        &mut self,
        intent_id: &str,
        code: impl Into<String>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.reject_known_with_gateway_timing(intent_id, code, None, observed_at_ms)
    }

    pub fn reject_known_with_gateway_timing(
        &mut self,
        intent_id: &str,
        code: impl Into<String>,
        gateway_timing_us: Option<(u64, u64)>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let code = code.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
            if let Some((in_time_us, out_time_us)) = gateway_timing_us {
                record.record_okx_gateway_timing(in_time_us, out_time_us)?;
            }
            record.reject_known(code)
        })
    }

    pub fn prepare_order_mutation(
        &mut self,
        intent_id: &str,
        mutation: OrderMutationRecord,
        observed_at_ms: u64,
    ) -> Result<MutationPrepareDisposition, ExecutionLedgerError> {
        require_timestamp(observed_at_ms)?;
        let existing = self
            .entries
            .get(intent_id)
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;
        if let Some(previous) = existing
            .record
            .mutations
            .iter()
            .find(|previous| previous.mutation_id == mutation.mutation_id)
        {
            return if previous == &mutation {
                Ok(MutationPrepareDisposition::Existing(existing.clone()))
            } else {
                Err(ExecutionTransitionError::MutationConflict(mutation.mutation_id).into())
            };
        }

        let entry = self.mutate(intent_id, observed_at_ms, move |record| {
            record.prepare_mutation(mutation).map(|_| ())
        })?;
        Ok(MutationPrepareDisposition::Created(entry))
    }

    pub fn prepare_protective_size_amend(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        new_size: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record
                .prepare_protective_size_amend(mutation_id, new_size)
                .map(|_| ())
        })
    }

    pub fn begin_protective_size_amend(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.begin_protective_size_amend(mutation_id)
        })
    }

    pub fn acknowledge_protective_size_amend(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.acknowledge_protective_size_amend(mutation_id)
        })
    }

    pub fn mark_protective_size_amend_unknown(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.mark_protective_size_amend_unknown(mutation_id)
        })
    }

    pub fn confirm_protective_size_amend(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.confirm_protective_size_amend(mutation_id)
        })
    }

    pub fn retire_protective_size_amend_absent(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.retire_protective_size_amend_absent(mutation_id)
        })
    }

    pub fn prepare_protective_cleanup(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.prepare_protective_cleanup(mutation_id).map(|_| ())
        })
    }

    pub fn begin_protective_cleanup(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.begin_protective_cleanup(mutation_id)
        })
    }

    pub fn acknowledge_protective_cleanup(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.acknowledge_protective_cleanup(mutation_id)
        })
    }

    pub fn mark_protective_cleanup_unknown(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.mark_protective_cleanup_unknown(mutation_id)
        })
    }

    pub fn confirm_protective_cleanup_absent(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.confirm_protective_cleanup_absent(mutation_id)
        })
    }

    pub fn begin_order_mutation_submission(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let entry = self
            .get(intent_id)
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;
        let mutation = entry
            .record
            .mutations
            .iter()
            .find(|item| item.mutation_id == mutation_id)
            .ok_or_else(|| {
                ExecutionLedgerError::Transition(ExecutionTransitionError::MutationNotFound(
                    mutation_id.to_owned(),
                ))
            })?;
        if mutation.kind == OrderMutationKind::Amend {
            self.require_new_risk_allowed(entry.record.plan.action)?;
            self.validate_capital_intent(&entry.record, entry.created_at_ms)?;
        }
        let mutation_id = mutation_id.to_owned();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.begin_mutation_submission(&mutation_id)
        })
    }

    pub fn acknowledge_order_mutation(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let mutation_id = mutation_id.to_owned();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.acknowledge_mutation(&mutation_id)
        })
    }

    pub fn mark_order_mutation_unknown(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let mutation_id = mutation_id.to_owned();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.mark_mutation_unknown(&mutation_id)
        })
    }

    pub fn reject_order_mutation(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        code: impl Into<String>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let mutation_id = mutation_id.to_owned();
        let code = code.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.reject_mutation(&mutation_id, code)
        })
    }

    pub fn reconcile_found(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        exchange_state: ExchangeOrderState,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.reconcile_found_with_mutation_resolution(
            intent_id,
            order_id,
            exchange_state,
            OrderMutationResolution::Pending,
            observed_at_ms,
        )
    }

    pub fn reconcile_found_with_mutation_resolution(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        exchange_state: ExchangeOrderState,
        mutation_resolution: OrderMutationResolution,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.reconcile_found_with_resolutions(
            intent_id,
            order_id,
            exchange_state,
            mutation_resolution,
            None,
            observed_at_ms,
        )
    }

    pub fn reconcile_found_with_resolutions(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        exchange_state: ExchangeOrderState,
        mutation_resolution: OrderMutationResolution,
        protection_resolution: Option<ProtectiveOrderResolution>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let order_id = order_id.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.reconcile_found_with_protection(
                order_id,
                exchange_state,
                protection_resolution,
            )?;
            record.resolve_active_mutation(mutation_resolution)
        })
    }

    fn commit_new(
        &mut self,
        intent_id: String,
        entry: ExecutionLedgerEntry,
    ) -> Result<(), ExecutionLedgerError> {
        self.entries.insert(intent_id.clone(), entry);
        if let Err(error) = self.store.save(
            &self.entries,
            self.risk_stop.as_ref(),
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
            &self.strategy_periods,
        ) {
            self.entries.remove(&intent_id);
            return Err(error);
        }
        Ok(())
    }

    fn mutate<F>(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
        mutation: F,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError>
    where
        F: FnOnce(&mut ExecutionRecord) -> Result<(), ExecutionTransitionError>,
    {
        require_timestamp(observed_at_ms)?;
        let original = self
            .entries
            .get(intent_id)
            .cloned()
            .ok_or_else(|| ExecutionLedgerError::IntentNotFound(intent_id.to_owned()))?;

        {
            let entry = self
                .entries
                .get_mut(intent_id)
                .expect("entry was checked above");
            let next_timestamp = monotonic_timestamp(entry, observed_at_ms)?;
            mutation(&mut entry.record)?;
            entry.updated_at_ms = next_timestamp;
            validate_entry(entry)?;
        }

        if let Err(error) = self.store.save(
            &self.entries,
            self.risk_stop.as_ref(),
            self.capital_reservation.as_ref(),
            self.exchange_evidence.as_ref(),
            &self.strategy_periods,
        ) {
            self.entries.insert(intent_id.to_owned(), original);
            return Err(error);
        }

        Ok(self
            .entries
            .get(intent_id)
            .expect("persisted entry exists")
            .clone())
    }
}

/// One run owns an intent only when this ledger itself recorded its opening
/// and the exact execution origin/mandate matches the opening. This check
/// rejects forged, orphaned, foreign and backdated run links on disk.
fn validate_strategy_run_binding(
    entry: &ExecutionLedgerEntry,
    active_run: Option<&StrategyCapitalReservation>,
    archived: &[ArchivedStrategyRun],
    unverified: &[UnverifiedStrategyRun],
) -> Result<(), ExecutionLedgerError> {
    let Some(run_id) = entry.record.strategy_run_id.as_deref() else {
        // Historical executions must remain readable but not retroactively
        // become attributed merely because a newer run was opened.
        return Ok(());
    };
    let run = active_run
        .filter(|r| r.allocation_id == run_id)
        .or_else(|| {
            archived
                .iter()
                .find(|v| v.reservation.allocation_id == run_id)
                .map(|v| &v.reservation)
        });
    let run = run.or_else(|| {
        unverified
            .iter()
            .find(|v| v.reservation.allocation_id == run_id)
            .map(|v| &v.reservation)
    });
    let Some(run) = run else {
        return Err(ExecutionLedgerError::CapitalReservationScopeMismatch);
    };
    if !valid_intent_id(run_id)
        || run_id != run.allocation_id
        || !entry.record.plan.action.is_risk_increasing()
        || entry.created_at_ms <= run.started_at_ms
        || entry.record.plan.account_uid_fingerprint != run.account_uid_fingerprint
        || entry.record.lineage.as_ref().is_none_or(|l| {
            l.origin_schema != run.origin_schema || l.origin_version != run.origin_version
        })
        || entry
            .record
            .plan
            .risk_binding
            .as_ref()
            .is_none_or(|b| b.mandate.version != run.mandate_version)
    {
        return Err(ExecutionLedgerError::CapitalReservationScopeMismatch);
    }
    Ok(())
}

fn validate_entry(entry: &ExecutionLedgerEntry) -> Result<(), ExecutionLedgerError> {
    validate_plan_identity(&entry.record.plan)?;
    validate_execution_lineage(&entry.record)?;
    validate_submission_timing(&entry.record)?;
    validate_protection_link(&entry.record)?;
    validate_order_mutations(&entry.record)?;
    validate_reverse_link(&entry.record)?;
    if entry.created_at_ms == 0
        || entry.updated_at_ms == 0
        || entry.updated_at_ms < entry.created_at_ms
    {
        return Err(ExecutionLedgerError::Corrupt("invalid timestamps"));
    }

    let record = &entry.record;
    let order_id_present = record
        .order_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let rejection_present = record
        .rejection_code
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());

    match record.state {
        ExecutionState::Prepared
        | ExecutionState::Submitting
        | ExecutionState::UnknownSubmission => {
            if order_id_present || record.exchange_state.is_some() || rejection_present {
                return Err(ExecutionLedgerError::Corrupt(
                    "pre-ack state contains exchange terminal metadata",
                ));
            }
        }
        ExecutionState::Acknowledged => {
            if !order_id_present || record.exchange_state.is_some() || rejection_present {
                return Err(ExecutionLedgerError::Corrupt(
                    "acknowledged state metadata is inconsistent",
                ));
            }
        }
        ExecutionState::Live => {
            validate_exchange_state(record, order_id_present, ExchangeOrderState::Live)?;
        }
        ExecutionState::PartiallyFilled => {
            validate_exchange_state(
                record,
                order_id_present,
                ExchangeOrderState::PartiallyFilled,
            )?;
        }
        ExecutionState::Filled => {
            validate_exchange_state(record, order_id_present, ExchangeOrderState::Filled)?;
        }
        ExecutionState::Canceled => {
            validate_exchange_state(record, order_id_present, ExchangeOrderState::Canceled)?;
        }
        ExecutionState::Rejected => {
            if order_id_present || record.exchange_state.is_some() || !rejection_present {
                return Err(ExecutionLedgerError::Corrupt(
                    "rejected state metadata is inconsistent",
                ));
            }
        }
    }

    Ok(())
}

fn validate_submission_timing(record: &ExecutionRecord) -> Result<(), ExecutionLedgerError> {
    let Some(timing) = record.submission_timing.as_ref() else {
        return Ok(());
    };
    if timing.request_exchange_time_ms == 0 || record.state == ExecutionState::Prepared {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid execution submission timing",
        ));
    }
    match (timing.okx_in_time_us, timing.okx_out_time_us) {
        (None, None) => Ok(()),
        (Some(in_time_us), Some(out_time_us)) if in_time_us > 0 && out_time_us >= in_time_us => {
            Ok(())
        }
        _ => Err(ExecutionLedgerError::Corrupt(
            "invalid OKX gateway timing evidence",
        )),
    }
}

fn validate_execution_lineage(record: &ExecutionRecord) -> Result<(), ExecutionLedgerError> {
    let Some(lineage) = record.lineage.as_ref() else {
        return Ok(());
    };
    let price = Decimal::from_str(&lineage.decision_reference.price)
        .ok()
        .filter(|value| *value > Decimal::ZERO);
    let lineage_schema_valid = match lineage.schema.as_str() {
        EXECUTION_LINEAGE_SCHEMA_V1 => lineage.tca_mechanics.is_none(),
        EXECUTION_LINEAGE_SCHEMA_V2 => true,
        _ => false,
    };
    let tca_mechanics_valid = lineage.tca_mechanics.as_ref().is_none_or(|mechanics| {
        mechanics.source_reference_generation == record.plan.reference_generation
            && mechanics.contract_type == "linear"
            && positive_decimal(&mechanics.contract_value).is_some()
            && !mechanics.settle_currency.trim().is_empty()
            && mechanics.settle_currency.len() <= 16
    });
    if !lineage_schema_valid
        || !tca_mechanics_valid
        || !valid_sha256_artifact_id(&lineage.origin_evidence_id)
        || lineage.origin_schema.trim().is_empty()
        || lineage.origin_schema.len() > 128
        || lineage.origin_version.trim().is_empty()
        || lineage.origin_version.len() > 128
        || lineage
            .authority_evidence_id
            .as_deref()
            .is_some_and(|value| !valid_sha256_artifact_id(value))
        || lineage.decision_reference.decision_time_ms == 0
        || lineage
            .decision_reference
            .price_policy_version
            .trim()
            .is_empty()
        || lineage.decision_reference.price_policy_version.len() > 128
        || price.is_none()
    {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid execution lineage binding",
        ));
    }

    Ok(())
}

fn validate_protection_link(record: &ExecutionRecord) -> Result<(), ExecutionLedgerError> {
    let Some(protection) = record.protection.as_ref() else {
        return Ok(());
    };
    if !record.plan.action.is_risk_increasing()
        || record.plan.open_risk.is_none()
        || protection.policy_version != PROTECTIVE_ORDER_POLICY_V1
        || protection.algo_client_order_id
            != derive_protective_algo_client_id(&record.plan.intent_id)
        || protection.trigger_price_basis != ProtectiveTriggerPriceBasis::Mark
    {
        return Err(ExecutionLedgerError::Corrupt(
            "protective linkage does not match execution plan",
        ));
    }

    match protection.status {
        ProtectiveOrderStatus::Pending | ProtectiveOrderStatus::NotActivated => {
            if protection.algo_order_id.is_some()
                || protection.covered_size.is_some()
                || protection.failure_code.is_some()
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "protective status metadata is inconsistent",
                ));
            }
        }
        ProtectiveOrderStatus::Active | ProtectiveOrderStatus::CleanedUp => {
            if protection
                .algo_order_id
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
                || protection
                    .covered_size
                    .as_deref()
                    .and_then(positive_decimal)
                    .is_none()
                || protection.failure_code.is_some()
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "active protective metadata is inconsistent",
                ));
            }
        }
        ProtectiveOrderStatus::Failed => {
            if protection
                .failure_code
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
                || protection.algo_order_id.is_some()
                || protection.covered_size.is_some()
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "failed protective metadata is inconsistent",
                ));
            }
        }
    }

    if let Some(amend) = protection.size_amend.as_ref() {
        let prior = positive_decimal(&amend.previous_size);
        let target = positive_decimal(&amend.target_size);
        if !valid_mutation_id(&amend.mutation_id)
            || amend.request_id
                != derive_amend_request_id(&record.plan.intent_id, &amend.mutation_id)
            || amend.algo_order_id.is_empty()
            || !amend.algo_order_id.bytes().all(|b| b.is_ascii_digit())
            || protection.algo_order_id.as_deref() != Some(amend.algo_order_id.as_str())
            // RETIRED_ABSENT can progress through a durable FLAT+absent
            // cleanup without ever claiming the target size was applied.
            // All non-retired resizes still require ACTIVE protection.
            || (protection.status != ProtectiveOrderStatus::Active
                && !(amend.state == ProtectiveSizeAmendState::RetiredAbsent
                    && protection.status == ProtectiveOrderStatus::CleanedUp
                    && protection.cleanup.as_ref().is_some_and(|cleanup|
                        cleanup.state == ProtectiveCleanupState::ConfirmedAbsent)))
            || !record.state.is_terminal()
            || prior.is_none()
            || target.is_none()
            || target >= prior
            || protection.covered_size.as_deref()
                != Some(match amend.state {
                    ProtectiveSizeAmendState::Confirmed => amend.target_size.as_str(),
                    _ => amend.previous_size.as_str(),
                })
            || (protection.cleanup.is_some()
                && !matches!(amend.state,
                    ProtectiveSizeAmendState::Confirmed | ProtectiveSizeAmendState::RetiredAbsent))
        {
            return Err(ExecutionLedgerError::Corrupt(
                "protective resize state does not match exact owned algo/size",
            ));
        }
    }
    if let Some(cleanup) = protection.cleanup.as_ref() {
        if !valid_mutation_id(&cleanup.mutation_id)
            || protection.algo_order_id.as_deref() != Some(cleanup.algo_order_id.as_str())
            || !record.state.is_terminal()
            || !matches!(
                (protection.status, cleanup.state),
                (
                    ProtectiveOrderStatus::Active,
                    ProtectiveCleanupState::Prepared
                ) | (
                    ProtectiveOrderStatus::Active,
                    ProtectiveCleanupState::Submitting
                ) | (
                    ProtectiveOrderStatus::Active,
                    ProtectiveCleanupState::Unknown
                ) | (
                    ProtectiveOrderStatus::Active,
                    ProtectiveCleanupState::Acknowledged
                ) | (
                    ProtectiveOrderStatus::CleanedUp,
                    ProtectiveCleanupState::ConfirmedAbsent
                )
            )
        {
            return Err(ExecutionLedgerError::Corrupt(
                "protective cleanup state does not match durable exchange ownership",
            ));
        }
    } else if protection.status == ProtectiveOrderStatus::CleanedUp {
        return Err(ExecutionLedgerError::Corrupt(
            "cleaned protective order lacks durable cleanup evidence",
        ));
    }

    if record.state == ExecutionState::Rejected
        && protection.status != ProtectiveOrderStatus::NotActivated
    {
        return Err(ExecutionLedgerError::Corrupt(
            "rejected parent has invalid protective status",
        ));
    }
    if record.state == ExecutionState::Filled
        && protection.status == ProtectiveOrderStatus::NotActivated
    {
        return Err(ExecutionLedgerError::Corrupt(
            "filled parent cannot have not-activated protection",
        ));
    }
    if matches!(
        record.state,
        ExecutionState::Live | ExecutionState::PartiallyFilled
    ) && protection.status != ProtectiveOrderStatus::Pending
    {
        return Err(ExecutionLedgerError::Corrupt(
            "nonterminal parent has resolved protective state",
        ));
    }

    Ok(())
}

fn positive_decimal(value: &str) -> Option<Decimal> {
    Decimal::from_str(value.trim())
        .ok()
        .filter(|value| *value > Decimal::ZERO)
}

fn valid_sha256_artifact_id(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn validate_reverse_link(record: &ExecutionRecord) -> Result<(), ExecutionLedgerError> {
    let Some(reverse) = record.reverse.as_ref() else {
        return Ok(());
    };
    if !valid_intent_id(&reverse.root_intent_id)
        || !valid_intent_id(&reverse.open_intent_id)
        || reverse.root_intent_id == reverse.open_intent_id
    {
        return Err(ExecutionLedgerError::Corrupt("invalid reverse linkage"));
    }
    match reverse.leg {
        ReverseLeg::Close => {
            if record.plan.intent_id != reverse.root_intent_id
                || record.plan.action != ExecutionAction::Close
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "reverse close linkage does not match plan",
                ));
            }
        }
        ReverseLeg::Open => {
            if record.plan.intent_id != reverse.open_intent_id
                || record.plan.action != ExecutionAction::Open
                || record.plan.position_side != reverse.target_position_side
                || reverse.continuation != ReverseContinuation::Required
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "reverse open linkage does not match plan",
                ));
            }
        }
    }
    Ok(())
}

fn validate_order_mutations(record: &ExecutionRecord) -> Result<(), ExecutionLedgerError> {
    if record.mutations.len() > MAX_ORDER_MUTATIONS_PER_EXECUTION {
        return Err(ExecutionLedgerError::Corrupt(
            "order mutation count exceeds capacity",
        ));
    }

    let mut ids = BTreeSet::new();
    let mut nonterminal = 0_usize;
    for mutation in &record.mutations {
        if !valid_mutation_id(&mutation.mutation_id) || !ids.insert(mutation.mutation_id.clone()) {
            return Err(ExecutionLedgerError::Corrupt(
                "invalid or duplicate order mutation id",
            ));
        }
        if !mutation.state.is_terminal() {
            nonterminal += 1;
        }
        if mutation.state == OrderMutationState::Rejected {
            if mutation
                .rejection_code
                .as_deref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(ExecutionLedgerError::Corrupt(
                    "rejected order mutation is missing rejection code",
                ));
            }
        } else if mutation.rejection_code.is_some() {
            return Err(ExecutionLedgerError::Corrupt(
                "non-rejected order mutation contains rejection code",
            ));
        }

        match mutation.kind {
            OrderMutationKind::Amend => {
                if mutation
                    .request_id
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                    || (mutation.new_size.is_none() && mutation.new_price.is_none())
                {
                    return Err(ExecutionLedgerError::Corrupt(
                        "amend mutation metadata is incomplete",
                    ));
                }
            }
            OrderMutationKind::Cancel => {
                if mutation.request_id.is_some()
                    || mutation.new_size.is_some()
                    || mutation.new_price.is_some()
                {
                    return Err(ExecutionLedgerError::Corrupt(
                        "cancel mutation contains amend metadata",
                    ));
                }
            }
        }
    }
    if nonterminal > 1 {
        return Err(ExecutionLedgerError::Corrupt(
            "multiple nonterminal order mutations",
        ));
    }
    Ok(())
}

fn validate_exchange_state(
    record: &ExecutionRecord,
    order_id_present: bool,
    expected: ExchangeOrderState,
) -> Result<(), ExecutionLedgerError> {
    if !order_id_present
        || record.exchange_state != Some(expected)
        || record.rejection_code.is_some()
    {
        Err(ExecutionLedgerError::Corrupt(
            "exchange state metadata is inconsistent",
        ))
    } else {
        Ok(())
    }
}

fn validate_plan_identity(plan: &ExecutionPlan) -> Result<(), ExecutionLedgerError> {
    if plan.schema != EXECUTION_PLAN_SCHEMA_V1
        || !valid_intent_id(&plan.intent_id)
        || plan.client_order_id != derive_client_order_id(&plan.intent_id)
        || plan.reference_generation.trim().is_empty()
        || plan.account_generation.trim().is_empty()
        || plan.account_uid_fingerprint.trim().is_empty()
        || plan.instrument_id.trim().is_empty()
    {
        return Err(ExecutionLedgerError::Corrupt(
            "invalid execution plan identity",
        ));
    }
    Ok(())
}

fn require_timestamp(value: u64) -> Result<(), ExecutionLedgerError> {
    if value == 0 {
        Err(ExecutionLedgerError::InvalidTimestamp)
    } else {
        Ok(())
    }
}

fn monotonic_timestamp(
    entry: &ExecutionLedgerEntry,
    observed_at_ms: u64,
) -> Result<u64, ExecutionLedgerError> {
    require_timestamp(observed_at_ms)?;
    if observed_at_ms < entry.updated_at_ms {
        Err(ExecutionLedgerError::InvalidTimestamp)
    } else {
        Ok(observed_at_ms)
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> Result<(), ExecutionLedgerError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let source_w = wide(source);
    let destination_w = wide(destination);
    let ok = unsafe {
        MoveFileExW(
            source_w.as_ptr(),
            destination_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };

    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> Result<(), ExecutionLedgerError> {
    fs::rename(source, destination)?;
    Ok(())
}

/// Stop classification is deterministic and cannot be used to grant risk.
fn strategy_nav_stop_reason(
    nav: Decimal,
    peak: Decimal,
    gross_loss: Decimal,
    mandate_drawdown: Decimal,
    policy_drawdown: Decimal,
    daily_limit: Decimal,
) -> Option<&'static str> {
    if peak <= Decimal::ZERO || nav <= Decimal::ZERO {
        return Some("STRATEGY_NAV_DEPLETED");
    }
    if gross_loss > daily_limit {
        return Some("STRATEGY_UTC_DAILY_GROSS_LOSS");
    }
    if (peak - nav) / peak > mandate_drawdown.min(policy_drawdown) {
        return Some("STRATEGY_MAX_DRAWDOWN");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_nav_stop_thresholds_are_conservative_and_do_not_mask_losses() {
        let d = |value| Decimal::from_str(value).unwrap();
        assert_eq!(
            strategy_nav_stop_reason(d("100"), d("100"), d("0"), d("0.2"), d("0.1"), d("10")),
            None
        );
        assert_eq!(
            strategy_nav_stop_reason(d("89.99"), d("100"), d("0"), d("0.2"), d("0.1"), d("10")),
            Some("STRATEGY_MAX_DRAWDOWN")
        );
        assert_eq!(
            strategy_nav_stop_reason(
                d("89.99"),
                d("100"),
                d("10.01"),
                d("0.2"),
                d("0.1"),
                d("10")
            ),
            Some("STRATEGY_UTC_DAILY_GROSS_LOSS")
        );
        assert_eq!(
            strategy_nav_stop_reason(d("90"), d("100"), d("10"), d("0.2"), d("0.1"), d("10")),
            None
        );
        assert_eq!(
            strategy_nav_stop_reason(d("0"), d("100"), d("0"), d("0.2"), d("0.1"), d("10")),
            Some("STRATEGY_NAV_DEPLETED")
        );
        assert_eq!(
            strategy_nav_stop_reason(
                d("100"),
                d("100"),
                d("10.000001"),
                d("0.2"),
                d("0.1"),
                d("10")
            ),
            Some("STRATEGY_UTC_DAILY_GROSS_LOSS")
        );
    }

    use crate::{
        EXECUTION_LINEAGE_SCHEMA_V1, EXECUTION_PLAN_SCHEMA_V1, ExecutionAction,
        ExecutionDecisionReference, ExecutionLineageBinding, OrderSide, OrderType, PositionSide,
        TcaReferencePriceBasis, TradeMode,
    };

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-execution-ledger-{name}-{}",
            std::process::id()
        ))
    }

    fn plan(intent_id: &str) -> ExecutionPlan {
        ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: intent_id.to_owned(),
            client_order_id: derive_client_order_id(intent_id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        }
    }

    fn lineage(seed: char) -> ExecutionLineageBinding {
        ExecutionLineageBinding {
            schema: EXECUTION_LINEAGE_SCHEMA_V1.to_owned(),
            origin_evidence_id: format!("sha256:{}", seed.to_string().repeat(64)),
            origin_schema: "okx.research.live-decision/v1".to_owned(),
            origin_version: "okx.research.live-decision/2026-10-05.1".to_owned(),
            authority_evidence_id: Some(format!("sha256:{}", "f".repeat(64))),
            decision_reference: ExecutionDecisionReference {
                decision_time_ms: 100,
                price: "0.1".to_owned(),
                price_basis: TcaReferencePriceBasis::DecisionPrice,
                price_policy_version: "decision-reference/v1".to_owned(),
            },
            tca_mechanics: None,
        }
    }

    fn protected_plan(intent_id: &str) -> ExecutionPlan {
        let mut value = plan(intent_id);
        value.open_risk = Some(crate::OpenRiskEvidence {
            fee_generation: "sha256:fee".to_owned(),
            requested_max_settle_notional: "10".to_owned(),
            requested_max_loss_settle: "1".to_owned(),
            requested_target_rr: "2".to_owned(),
            stop_price: "0.09".to_owned(),
            target_price: "0.12".to_owned(),
            entry_settle_notional: "10".to_owned(),
            stop_loss_settle: "1".to_owned(),
            actual_target_rr: "2".to_owned(),
        });
        value
    }

    fn close_plan(intent_id: &str, position_side: PositionSide) -> ExecutionPlan {
        let mut value = plan(intent_id);
        value.action = ExecutionAction::Close;
        value.position_side = position_side;
        value.side = match position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
        };
        value.open_risk = None;
        value
    }

    fn reverse_open_plan(root_intent_id: &str, position_side: PositionSide) -> ExecutionPlan {
        let intent_id = derive_reverse_open_intent_id(root_intent_id);
        let mut value = plan(&intent_id);
        value.position_side = position_side;
        value.side = match position_side {
            PositionSide::Long => OrderSide::Buy,
            PositionSide::Short => OrderSide::Sell,
        };
        value
    }

    #[test]
    fn archived_run_is_reopened_from_disk_and_tampered_final_nav_refused_without_rewrite() {
        let root = temp_root("archived-run-tamper");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let prior = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "run_archived_source_0123456789".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            origin_schema: "okx.research.live-decision/v1".to_owned(),
            origin_version: "v1".to_owned(),
            mandate_version: "mandate-v1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "1000".to_owned(),
            account_generation: "generation".to_owned(),
            operator_authority_id: format!("sha256:{}", "a".repeat(64)),
            started_at_ms: 1000,
        };
        let source = DurableExchangeEvidence::new(&prior).unwrap();
        let report = crate::StrategyFinancialPeriod {
            schema: crate::STRATEGY_FINANCIAL_PERIOD_SCHEMA_V1,
            run_id: prior.allocation_id.clone(),
            state: "FLAT_RECONCILED",
            evidence_sha256: source.checksum_sha256.clone(),
            allocation_usdt: "100".to_owned(),
            linked_fills: 0,
            linked_bills: 0,
            realized_usdt: "0".to_owned(),
            fees_usdt: "0".to_owned(),
            funding_usdt: None,
            unrealized_usdt: Some("0".to_owned()),
            nav_usdt: Some("100".to_owned()),
            positions: Vec::new(),
            usable_for_new_risk: false,
            reasons: Vec::new(),
        };
        let finalization = StrategyFinalization::new(&prior, &source, &report, 2000).unwrap();
        let mut next = prior.clone();
        next.allocation_id = "run_next_source_0123456789".to_owned();
        next.started_at_ms = 3000;
        let file = ExecutionLedgerFile {
            schema: EXECUTION_LEDGER_SCHEMA_V9.to_owned(),
            records: Vec::new(),
            risk_stop: None,
            capital_reservation: Some(next),
            exchange_evidence: None,
            strategy_periods: StrategyPeriodHistory {
                finalized_run: None,
                archived_runs: vec![ArchivedStrategyRun {
                    reservation: prior,
                    evidence: source,
                    finalization,
                }],
                nav_points: Vec::new(),
                unverified_run: None,
                unverified_runs: Vec::new(),
            },
        };
        fs::write(store.path(), serde_json::to_vec_pretty(&file).unwrap()).unwrap();
        let reopened = DurableExecutionLedger::open(store.clone(), 3200).unwrap();
        assert_eq!(reopened.archived_runs_count(), 1);
        assert!(reopened.finalized_run().is_none());
        let proofs = reopened.archived_strategy_proofs();
        let proof = &proofs[0];
        assert_eq!(proof.run_id, "run_archived_source_0123456789");
        assert_eq!(proof.nav_usdt, "100");
        assert_eq!(proof.source_events, 0);
        assert_eq!(
            proof.finalization_sha256,
            file.strategy_periods.archived_runs[0]
                .finalization
                .checksum_sha256
        );
        assert_eq!(
            proof.evidence_sha256,
            file.strategy_periods.archived_runs[0]
                .evidence
                .checksum_sha256
        );
        drop(reopened);
        let mut altered = serde_json::to_value(&file).unwrap();
        altered["strategy_periods"]["archived_runs"][0]["finalization"]["nav_usdt"] =
            serde_json::json!("1000000");
        let tampered = serde_json::to_vec_pretty(&altered).unwrap();
        fs::write(store.path(), &tampered).unwrap();
        assert!(matches!(
            DurableExecutionLedger::open(store.clone(), 3300),
            Err(ExecutionLedgerError::Corrupt(_))
        ));
        assert_eq!(fs::read(store.path()).unwrap(), tampered);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn missing_ledger_opens_empty_and_prepare_round_trips() {
        let root = temp_root("roundtrip");
        let _ = fs::remove_dir_all(&root);
        let path = root.join("ledger.json");

        let store = ExecutionLedgerStore::at(&path);
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        assert!(ledger.is_empty());

        let prepared = ledger
            .prepare(plan("intent_0123456789abcdef"), 101)
            .expect("prepare");
        assert!(matches!(prepared, PrepareDisposition::Created(_)));
        assert_eq!(ledger.len(), 1);

        let reopened = DurableExecutionLedger::open(store, 102).expect("reopen");
        let reopened_entry = reopened.get("intent_0123456789abcdef").expect("entry");
        assert_eq!(reopened_entry.record.state, ExecutionState::Prepared);
        assert!(
            reopened_entry.record.plan.risk_binding.is_none(),
            "legacy plan without serialized risk_binding must remain readable"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ledger_rejects_second_account_uid_even_after_terminal_intents_and_restart() {
        let root = temp_root("uid-scope");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).unwrap();
        let first = "intent_scope_demo_0123456789";
        ledger.prepare(plan(first), 101).unwrap();
        let mut alien = plan("intent_scope_other_0123456789");
        alien.account_uid_fingerprint = "different-exchange-uid".to_owned();
        assert!(matches!(
            ledger.prepare(alien.clone(), 102),
            Err(ExecutionLedgerError::AccountScopeMismatch)
        ));
        assert_eq!(ledger.len(), 1);
        let mut reopened = DurableExecutionLedger::open(store, 103).unwrap();
        assert!(matches!(
            reopened.prepare(alien, 104),
            Err(ExecutionLedgerError::AccountScopeMismatch)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn mixed_uid_legacy_file_is_rejected_without_mutation_or_rewrite() {
        let root = temp_root("mixed-uid-legacy");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut alien = plan("intent_mixed_other_0123456789");
        alien.account_uid_fingerprint = "second-uid".to_owned();
        let file = ExecutionLedgerFile {
            schema: EXECUTION_LEDGER_SCHEMA_V6.to_owned(),
            records: vec![
                ExecutionLedgerEntry {
                    record: ExecutionRecord::new(plan("intent_mixed_main_0123456789")),
                    created_at_ms: 100,
                    updated_at_ms: 100,
                },
                ExecutionLedgerEntry {
                    record: ExecutionRecord::new(alien),
                    created_at_ms: 100,
                    updated_at_ms: 100,
                },
            ],
            risk_stop: None,
            capital_reservation: None,
            exchange_evidence: None,
            strategy_periods: StrategyPeriodHistory::default(),
        };
        let payload = serde_json::to_vec_pretty(&file).unwrap();
        fs::write(store.path(), &payload).unwrap();
        assert!(matches!(
            DurableExecutionLedger::open(store.clone(), 101),
            Err(ExecutionLedgerError::AccountScopeMismatch)
        ));
        assert_eq!(fs::read(store.path()).unwrap(), payload);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn strategy_reservation_is_durable_scoped_and_cannot_spend_twice() {
        let root = temp_root("strategy-reservation");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).unwrap();
        let approval = format!("sha256:{}", "b".repeat(64));
        let reservation = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "strategy-demo-budget-20261013".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            origin_schema: "okx.research.live-decision/v1".to_owned(),
            origin_version: "okx.research.live-decision/2026-10-05.1".to_owned(),
            mandate_version: "bounded-demo-v1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "1000".to_owned(),
            account_generation: "fresh-generation".to_owned(),
            operator_authority_id: approval.clone(),
            started_at_ms: 1000,
        };
        let evidence = StrategyAllocationOpening {
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            account_generation: "fresh-generation".to_owned(),
            observed_usdt_equity: "1000".to_owned(),
            observed_at_ms: 1000,
            source_history_complete: true,
            zero_positions: true,
            zero_pending_orders: true,
            zero_native_protection: true,
            operator_authority_id: approval,
        };
        let mut wrong = evidence.clone();
        wrong.zero_native_protection = false;
        assert!(matches!(
            ledger.reserve_strategy_capital(reservation.clone(), &wrong, 1001),
            Err(ExecutionLedgerError::CapitalReservationUnattested)
        ));
        let mut inflated = reservation.clone();
        inflated.allocated_usdt = "1001".to_owned();
        assert!(matches!(
            ledger.reserve_strategy_capital(inflated, &evidence, 1001),
            Err(ExecutionLedgerError::CapitalReservationUnattested)
        ));
        let actual = ledger
            .reserve_strategy_capital(reservation.clone(), &evidence, 1001)
            .expect("original exclusive reserve");
        assert_eq!(actual, reservation);
        drop(ledger);
        let mut ledger = DurableExecutionLedger::open(store, 1100).unwrap();
        assert_eq!(ledger.capital_reservation(), Some(&reservation));
        assert_eq!(
            ledger
                .reserve_strategy_capital(reservation.clone(), &evidence, 1101)
                .expect("duplicate identical request has no second debit"),
            reservation
        );
        let mut another = reservation;
        another.allocation_id = "second-budget-same-account".to_owned();
        assert!(matches!(
            ledger.reserve_strategy_capital(another, &evidence, 1101),
            Err(ExecutionLedgerError::CapitalReservationConflict)
        ));
        let mut no_provenance = plan("intent_new_risk_wrong_origin_20261013");
        no_provenance.risk_binding = None;
        assert!(matches!(
            ledger.prepare(no_provenance, 1102),
            Err(ExecutionLedgerError::CapitalReservationScopeMismatch)
        ));
        let close = close_plan("intent_protective_close_20261013", PositionSide::Long);
        assert!(
            ledger.prepare(close, 1103).is_ok(),
            "safe exits are not locked"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn strategy_run_id_is_owner_derived_durable_and_rejects_tampering() {
        use okx_analysis::{
            HARD_RISK_POLICY_SCHEMA_V1, HardRiskPolicy, RiskDegradedMode, RiskMinimumQuality,
            TRADING_MANDATE_SCHEMA_V1, TradingMandate,
        };

        let root = temp_root("strategy-run-id-owner");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let authority = format!("sha256:{}", "a".repeat(64));
        let run = StrategyCapitalReservation {
            schema: STRATEGY_CAPITAL_RESERVATION_SCHEMA_V1.to_owned(),
            allocation_id: "run_owner_test_20261013".to_owned(),
            account_uid_fingerprint: "uid-fingerprint".to_owned(),
            origin_schema: "okx.research.live-decision/v1".to_owned(),
            origin_version: "okx.research.live-decision/2026-10-05.1".to_owned(),
            mandate_version: "run-owner-mandate/v1".to_owned(),
            allocated_usdt: "100".to_owned(),
            opening_account_usdt_equity: "1000".to_owned(),
            account_generation: "fresh-generation".to_owned(),
            operator_authority_id: authority.clone(),
            started_at_ms: 1000,
        };
        let opening = StrategyAllocationOpening {
            account_uid_fingerprint: run.account_uid_fingerprint.clone(),
            account_generation: run.account_generation.clone(),
            observed_usdt_equity: run.opening_account_usdt_equity.clone(),
            observed_at_ms: run.started_at_ms,
            source_history_complete: true,
            zero_positions: true,
            zero_pending_orders: true,
            zero_native_protection: true,
            operator_authority_id: authority,
        };
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        // A historical execution is NEVER attributed retroactively.
        let historical = "legacy_before_run_0123456";
        ledger.prepare(plan(historical), 101).expect("legacy");
        ledger
            .abandon_prepared(historical, 102)
            .expect("retire old prepared");
        ledger
            .reserve_strategy_capital(run.clone(), &opening, 1001)
            .expect("independently witnessed opening");
        assert!(
            ledger
                .get(historical)
                .unwrap()
                .record
                .strategy_run_id
                .is_none()
        );

        let mut owned = plan("intent_run_owner_verified_0123456");
        owned.risk_binding = Some(crate::ExecutionRiskBinding {
            mandate: TradingMandate {
                schema: TRADING_MANDATE_SCHEMA_V1.to_owned(),
                version: run.mandate_version.clone(),
                capital_base_usd: "100".to_owned(),
                decision_horizon_hours: 1,
                benchmark: None,
                allowed_instruments: vec![owned.instrument_id.clone()],
                max_drawdown_ratio: "0.2".to_owned(),
                leverage_ceiling: "3".to_owned(),
                minimum_liquidity_notional_usd: "0".to_owned(),
                max_turnover_ratio: "5".to_owned(),
            },
            policy: HardRiskPolicy {
                schema: HARD_RISK_POLICY_SCHEMA_V1.to_owned(),
                version: "run-owner-policy/v1".to_owned(),
                max_account_gross_notional_usd: "100".to_owned(),
                max_instrument_gross_notional_usd: "100".to_owned(),
                max_margin_utilization_ratio: "0.5".to_owned(),
                max_loss_per_trade_usd: "10".to_owned(),
                max_daily_realized_loss_usd: "10".to_owned(),
                max_drawdown_ratio: "0.2".to_owned(),
                max_leverage: "3".to_owned(),
                allowed_instruments: vec![owned.instrument_id.clone()],
                minimum_quality: RiskMinimumQuality::Fresh,
                degraded_mode: RiskDegradedMode::Reject,
                correlated_clusters: Vec::new(),
            },
        });
        let owned_id = owned.intent_id.clone();
        let created = ledger
            .prepare_with_lineage(owned.clone(), Some(lineage('b')), 1002)
            .expect("bind owner in ledger");
        let PrepareDisposition::Created(entry) = created else {
            panic!("first preparation must create one entry");
        };
        assert_eq!(
            entry.record.strategy_run_id.as_deref(),
            Some(run.allocation_id.as_str())
        );
        assert!(matches!(
            ledger
                .prepare_with_lineage(owned, Some(lineage('b')), 1003)
                .expect("same input is idempotent"),
            PrepareDisposition::Existing(_)
        ));
        drop(ledger);

        let mut reopened = DurableExecutionLedger::open(store.clone(), 1100).expect("restart");
        assert_eq!(
            reopened
                .get(&owned_id)
                .unwrap()
                .record
                .strategy_run_id
                .as_deref(),
            Some(run.allocation_id.as_str())
        );
        assert!(
            reopened
                .get(historical)
                .unwrap()
                .record
                .strategy_run_id
                .is_none()
        );
        reopened
            .abandon_prepared(&owned_id, 1101)
            .expect("retire unsent new-risk intent");
        // Safe CLOSE can still be prepared without claiming ownership of
        // an arbitrary existing or foreign position.
        let close_id = "intent_run_owner_safe_close_012345";
        reopened
            .prepare(close_plan(close_id, PositionSide::Long), 1102)
            .expect("exit remains allowed");
        assert!(
            reopened
                .get(close_id)
                .unwrap()
                .record
                .strategy_run_id
                .is_none()
        );
        drop(reopened);

        let intact = fs::read(store.path()).expect("persisted ledger");
        let mut forged: serde_json::Value =
            serde_json::from_slice(&intact).expect("readable ledger");
        let entries = forged["records"].as_array_mut().expect("records");
        let owner = entries
            .iter_mut()
            .find(|entry| entry["record"]["plan"]["intent_id"] == owned_id)
            .expect("owned intent");
        owner["record"]["strategy_run_id"] =
            serde_json::Value::String("different_run_0123456789".to_owned());
        let tampered = serde_json::to_vec_pretty(&forged).expect("tamper only fixture");
        fs::write(store.path(), &tampered).expect("save tampered fixture");
        assert!(matches!(
            DurableExecutionLedger::open(store.clone(), 1200),
            Err(ExecutionLedgerError::CapitalReservationScopeMismatch)
        ));
        assert_eq!(
            fs::read(store.path()).expect("failed load may not rewrite"),
            tampered
        );

        fs::write(store.path(), &intact).expect("restore fixture");
        let mut unbound: serde_json::Value = serde_json::from_slice(&intact).expect("read fixture");
        let entries = unbound["records"].as_array_mut().expect("records");
        let owner = entries
            .iter_mut()
            .find(|entry| entry["record"]["plan"]["intent_id"] == owned_id)
            .expect("owned intent");
        owner["record"]["strategy_run_id"] = serde_json::Value::Null;
        fs::write(
            store.path(),
            serde_json::to_vec_pretty(&unbound).expect("strip fixture owner"),
        )
        .expect("save legacy-like fixture");
        // Old missing links can be recovered without backfilling false PnL,
        // but the unowned risk-increasing intent cannot be submitted.
        let mut unowned = DurableExecutionLedger::open(store.clone(), 1200)
            .expect("old ledger must remain readable for exits");
        assert!(matches!(
            unowned.begin_submission(&owned_id, 1201),
            Err(ExecutionLedgerError::CapitalReservationScopeMismatch)
        ));
        assert!(
            unowned
                .require_new_risk_allowed(ExecutionAction::Close)
                .is_ok()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn execution_lineage_is_durable_idempotent_and_immutable() {
        let root = temp_root("lineage");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_lineage_012345678";
        let expected = lineage('a');

        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            let prepared = ledger
                .prepare_with_lineage(plan(intent_id), Some(expected.clone()), 101)
                .expect("prepare lineage");
            assert!(matches!(prepared, PrepareDisposition::Created(_)));
            assert_eq!(
                ledger
                    .get(intent_id)
                    .and_then(|entry| entry.record.lineage.as_ref()),
                Some(&expected)
            );
            assert!(matches!(
                ledger
                    .prepare_with_lineage(plan(intent_id), Some(expected.clone()), 102)
                    .expect("idempotent"),
                PrepareDisposition::Existing(_)
            ));
            assert!(matches!(
                ledger.prepare_with_lineage(plan(intent_id), Some(lineage('b')), 103),
                Err(ExecutionLedgerError::IntentConflict)
            ));
        }

        let reopened = DurableExecutionLedger::open(store, 200).expect("restart");
        assert_eq!(
            reopened
                .get(intent_id)
                .and_then(|entry| entry.record.lineage.as_ref()),
            Some(&expected)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn abandon_unsent_prepared_close_frees_reservation_and_preserves_parent_protection() {
        let root = temp_root("abandon-close");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let parent = "intent_abandon_parent_01234567";
        let old = "intent_abandon_close_old_01234";
        let fresh = "intent_abandon_close_new_01234";
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        ledger.prepare(protected_plan(parent), 101).expect("parent");
        ledger.begin_submission(parent, 102).expect("submit");
        ledger.acknowledge(parent, "parent123", 103).expect("ack");
        ledger
            .reconcile_found_with_resolutions(
                parent,
                "parent123",
                ExchangeOrderState::Filled,
                OrderMutationResolution::Pending,
                Some(ProtectiveOrderResolution::Active {
                    algo_order_id: "123456789012".to_owned(),
                    covered_size: "1".to_owned(),
                }),
                104,
            )
            .expect("filled with OCO");
        ledger
            .prepare(close_plan(old, PositionSide::Long), 105)
            .expect("old close");
        assert!(matches!(
            ledger.prepare(close_plan(fresh, PositionSide::Long), 106),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        let result = ledger
            .abandon_prepared(old, 107)
            .expect("strict local release");
        assert_eq!(result.record.state, ExecutionState::Rejected);
        assert_eq!(
            result.record.rejection_code.as_deref(),
            Some(crate::state::LOCAL_PREPARED_ABANDONED_CODE)
        );
        assert_eq!(
            ledger.abandon_prepared(old, 108).expect("idempotent"),
            result
        );
        drop(ledger);
        let mut restarted = DurableExecutionLedger::open(store, 200).expect("restart");
        assert_eq!(
            restarted
                .get(parent)
                .and_then(|e| e.record.protection.as_ref())
                .map(|p| p.status),
            Some(ProtectiveOrderStatus::Active)
        );
        restarted
            .prepare(close_plan(fresh, PositionSide::Long), 201)
            .expect("fresh risk-reducing close, not exchange send");
        assert!(matches!(
            restarted.prepare(close_plan(old, PositionSide::Long), 202),
            Err(ExecutionLedgerError::IntentConflict)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn abandon_fails_closed_for_submitting_and_unknown_submission() {
        let root = temp_root("abandon-unknown");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let old = "intent_abandon_unknown_0123456";
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        ledger
            .prepare(close_plan(old, PositionSide::Long), 101)
            .expect("prepare");
        ledger
            .begin_submission(old, 102)
            .expect("pre-send durable fence");
        assert!(matches!(
            ledger.abandon_prepared(old, 103),
            Err(ExecutionLedgerError::Transition(
                ExecutionTransitionError::InvalidTransition { .. }
            ))
        ));
        drop(ledger);
        let mut restarted = DurableExecutionLedger::open(store, 104).expect("restart");
        assert_eq!(
            restarted.get(old).expect("present").record.state,
            ExecutionState::UnknownSubmission
        );
        assert!(restarted.abandon_prepared(old, 105).is_err());
        assert!(matches!(
            restarted.prepare(
                close_plan("intent_after_unknown_01234567", PositionSide::Long),
                106
            ),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn duplicate_intent_reuses_exact_plan_but_rejects_conflict() {
        let root = temp_root("duplicate");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");

        let original = plan("intent_0123456789abcdef");
        ledger.prepare(original.clone(), 101).expect("first");
        assert!(matches!(
            ledger.prepare(original.clone(), 102).expect("duplicate"),
            PrepareDisposition::Existing(_)
        ));

        let mut conflict = original;
        conflict.price = "0.2".to_owned();
        assert!(matches!(
            ledger.prepare(conflict, 103),
            Err(ExecutionLedgerError::IntentConflict)
        ));
        assert_eq!(ledger.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn same_instrument_arbitration_defers_until_existing_execution_is_terminal() {
        let root = temp_root("instrument-arbitration");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");

        let first = plan("intent_first_0123456789");
        ledger.prepare(first.clone(), 101).expect("first");
        let mut second = plan("intent_second_012345678");
        second.client_order_id = derive_client_order_id(&second.intent_id);

        assert!(matches!(
            ledger.prepare(second.clone(), 102),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));

        ledger
            .begin_submission(&first.intent_id, 103)
            .expect("submit first");
        ledger
            .acknowledge(&first.intent_id, "ord-1", 104)
            .expect("ack first");
        ledger
            .reconcile_found(&first.intent_id, "ord-1", ExchangeOrderState::Filled, 105)
            .expect("first terminal");

        assert!(matches!(
            ledger.prepare(second, 106).expect("second after terminal"),
            PrepareDisposition::Created(_)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn terminal_pending_protection_reserves_across_restart_until_matching_proof() {
        let root = temp_root("protective-reservation");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_protected_01234567";

        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger
                .prepare(protected_plan(intent_id), 101)
                .expect("prepare");
            ledger.begin_submission(intent_id, 102).expect("submit");
            ledger.acknowledge(intent_id, "ord-1", 103).expect("ack");
            let terminal = ledger
                .reconcile_found_with_resolutions(
                    intent_id,
                    "ord-1",
                    ExchangeOrderState::Canceled,
                    OrderMutationResolution::Pending,
                    Some(ProtectiveOrderResolution::Pending),
                    104,
                )
                .expect("terminal pending");
            assert_eq!(terminal.record.state, ExecutionState::Canceled);
            assert!(terminal.record.protection_requires_reconciliation());
        }

        let mut restarted = DurableExecutionLedger::open(store.clone(), 200).expect("restart");
        assert!(matches!(
            restarted.prepare(plan("intent_blocked_01234567"), 201),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        assert!(!restarted.instrument_reserved(
            "DOGE-USDT-SWAP",
            "intent_close_pending_012345",
            ExecutionAction::Close,
            None,
        ));

        let resolved = restarted
            .reconcile_found_with_resolutions(
                intent_id,
                "ord-1",
                ExchangeOrderState::Canceled,
                OrderMutationResolution::Pending,
                Some(ProtectiveOrderResolution::Active {
                    algo_order_id: "algo-1".to_owned(),
                    covered_size: "0.4".to_owned(),
                }),
                202,
            )
            .expect("active protection");
        assert_eq!(
            resolved
                .record
                .protection
                .as_ref()
                .expect("protection")
                .status,
            ProtectiveOrderStatus::Active
        );
        assert!(matches!(
            restarted.prepare(plan("intent_blocked_active_0123456"), 203),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        // A partial reduce cannot safely resize the active venue OCO yet.
        assert!(restarted.instrument_reserved(
            "DOGE-USDT-SWAP",
            "intent_partial_reduce_01234567",
            ExecutionAction::Reduce,
            None
        ));
        assert!(matches!(
            restarted
                .prepare(
                    close_plan("intent_close_active_01234567", PositionSide::Long),
                    204,
                )
                .expect("close with active protection"),
            PrepareDisposition::Created(_)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn protective_resize_crash_unknown_is_durable_and_never_replayed() {
        let root = temp_root("protective-resize-unknown");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent = "intent_protection_resize_012345";
        let mutation = "resize_0123456789abcdef";
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        ledger
            .prepare(protected_plan(intent), 101)
            .expect("prepare");
        ledger.begin_submission(intent, 102).expect("send");
        ledger.acknowledge(intent, "parent123", 103).expect("ack");
        ledger
            .reconcile_found_with_resolutions(
                intent,
                "parent123",
                ExchangeOrderState::Filled,
                OrderMutationResolution::Pending,
                Some(ProtectiveOrderResolution::Active {
                    algo_order_id: "123456789012".to_owned(),
                    covered_size: "1".to_owned(),
                }),
                104,
            )
            .expect("active");
        for bad in ["0", "-1", "1", "2", "NaN", ""] {
            assert!(
                ledger
                    .prepare_protective_size_amend(intent, mutation, bad, 105)
                    .is_err()
            );
        }
        ledger
            .prepare_protective_size_amend(intent, mutation, "0.5", 105)
            .expect("persist exact target");
        ledger
            .begin_protective_size_amend(intent, mutation, 106)
            .expect("persist SUBMITTING");
        drop(ledger);

        let mut restarted = DurableExecutionLedger::open(store.clone(), 200).expect("restart");
        let amend = restarted
            .get(intent)
            .unwrap()
            .record
            .protection
            .as_ref()
            .unwrap()
            .size_amend
            .as_ref()
            .unwrap();
        assert_eq!(amend.state, ProtectiveSizeAmendState::Submitting);
        assert_eq!(amend.target_size, "0.5");
        assert!(
            restarted
                .begin_protective_size_amend(intent, mutation, 201)
                .is_err()
        );
        assert!(
            restarted
                .prepare_protective_size_amend(intent, "resize_different_01234567", "0.2", 202)
                .is_err()
        );
        assert!(matches!(
            restarted.prepare(plan("intent_resize_blocked_012345"), 203),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        restarted
            .mark_protective_size_amend_unknown(intent, mutation, 204)
            .expect("uncertain after process crash");
        drop(restarted);

        let mut recovered = DurableExecutionLedger::open(store.clone(), 300).expect("reopen");
        assert!(
            recovered
                .begin_protective_size_amend(intent, mutation, 301)
                .is_err()
        );
        assert!(
            recovered
                .acknowledge_protective_size_amend(intent, mutation, 302)
                .is_err()
        );
        let confirmed = recovered
            .confirm_protective_size_amend(intent, mutation, 303)
            .expect("only after independently attested exact venue target");
        let protected = confirmed.record.protection.as_ref().unwrap();
        assert_eq!(protected.covered_size.as_deref(), Some("0.5"));
        assert_eq!(
            protected.size_amend.as_ref().unwrap().state,
            ProtectiveSizeAmendState::Confirmed
        );
        assert!(
            recovered
                .confirm_protective_size_amend(intent, mutation, 304)
                .is_err()
        );
        assert!(matches!(
            recovered.prepare(plan("intent_resize_still_busy_012345"), 305),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        // Deliberate one-amend-per-parent policy until multi-REDUCE proven.
        assert!(
            recovered
                .prepare_protective_size_amend(intent, "resize_second_01234567", "0.25", 306)
                .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn flat_and_independently_absent_oco_retires_unknown_resize_without_replaying_post() {
        let root = temp_root("protection-flat-resize-recovery");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let parent = "intent_flat_oco_recovery_012345";
        let resize = "resize_unknown_flat_01234567";
        let cleanup = "cleanup_after_flat_01234567";
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("ledger");
        ledger
            .prepare(protected_plan(parent), 101)
            .expect("prepare");
        ledger.begin_submission(parent, 102).expect("submit");
        ledger.acknowledge(parent, "parent123", 103).expect("ack");
        ledger
            .reconcile_found_with_resolutions(
                parent,
                "parent123",
                ExchangeOrderState::Filled,
                OrderMutationResolution::Pending,
                Some(ProtectiveOrderResolution::Active {
                    algo_order_id: "123456789012".to_owned(),
                    covered_size: "1".to_owned(),
                }),
                104,
            )
            .expect("active OCO");
        ledger
            .prepare_protective_size_amend(parent, resize, "0.5", 105)
            .expect("prepared");
        ledger
            .begin_protective_size_amend(parent, resize, 106)
            .expect("SUBMITTING before crash");
        drop(ledger);

        let mut reopened = DurableExecutionLedger::open(store.clone(), 200).expect("reopen");
        assert!(
            reopened
                .begin_protective_size_amend(parent, resize, 201)
                .is_err(),
            "never replay ambiguous exchange POST"
        );
        assert!(
            reopened
                .prepare_protective_cleanup(parent, cleanup, 202)
                .is_err(),
            "UNKNOWN resize forbids normal cancellation"
        );
        // Agent may call this only after independently observed exact-UID FLAT
        // and complete zero conditional/OCO inventory under stable WS cursor.
        let retired = reopened
            .retire_protective_size_amend_absent(parent, resize, 203)
            .expect("durable local retirement");
        assert_eq!(
            retired
                .record
                .protection
                .as_ref()
                .unwrap()
                .size_amend
                .as_ref()
                .unwrap()
                .state,
            ProtectiveSizeAmendState::RetiredAbsent
        );
        assert!(
            reopened
                .begin_protective_size_amend(parent, resize, 204)
                .is_err()
        );
        reopened
            .prepare_protective_cleanup(parent, cleanup, 205)
            .expect("PREPARED without send");
        let flat = reopened
            .confirm_protective_cleanup_absent(parent, cleanup, 206)
            .expect("independent zero OCO scope");
        assert_eq!(
            flat.record.protection.as_ref().unwrap().status,
            ProtectiveOrderStatus::CleanedUp
        );
        assert!(
            reopened
                .retire_protective_size_amend_absent(parent, resize, 207)
                .is_err()
        );
        drop(reopened);
        let recovered = DurableExecutionLedger::open(store, 300).expect("reopen after terminal");
        let link = recovered
            .get(parent)
            .unwrap()
            .record
            .protection
            .as_ref()
            .unwrap();
        assert_eq!(
            link.size_amend.as_ref().unwrap().state,
            ProtectiveSizeAmendState::RetiredAbsent
        );
        assert_eq!(
            link.cleanup.as_ref().unwrap().state,
            ProtectiveCleanupState::ConfirmedAbsent
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn protective_cleanup_unknown_persists_and_blocks_replay_after_restart() {
        let root = temp_root("protective-cleanup-unknown");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent = "intent_protection_cleanup_012345";
        let mutation = "cleanup_0123456789abcdef";
        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger
                .prepare(protected_plan(intent), 101)
                .expect("prepare");
            ledger.begin_submission(intent, 102).expect("submit");
            ledger.acknowledge(intent, "parent123", 103).expect("ack");
            ledger
                .reconcile_found_with_resolutions(
                    intent,
                    "parent123",
                    ExchangeOrderState::Filled,
                    OrderMutationResolution::Pending,
                    Some(ProtectiveOrderResolution::Active {
                        algo_order_id: "123456789012".to_owned(),
                        covered_size: "1".to_owned(),
                    }),
                    104,
                )
                .expect("active protection");
            ledger
                .prepare_protective_cleanup(intent, mutation, 105)
                .expect("persist PREPARED");
            ledger
                .begin_protective_cleanup(intent, mutation, 106)
                .expect("persist SUBMITTING before HTTP");
        }
        {
            let mut restarted = DurableExecutionLedger::open(store.clone(), 200).expect("restart");
            assert!(matches!(
                restarted.begin_protective_cleanup(intent, mutation, 201),
                Err(ExecutionLedgerError::Transition(
                    ExecutionTransitionError::InvalidProtectiveCleanupTransition
                ))
            ));
            assert!(matches!(
                restarted.prepare(plan("intent_cleanup_blocked_012345"), 202),
                Err(ExecutionLedgerError::InstrumentBusy)
            ));
            restarted
                .mark_protective_cleanup_unknown(intent, mutation, 203)
                .expect("mark post-crash UNKNOWN; never replay");
        }
        let mut recovered = DurableExecutionLedger::open(store, 300).expect("restart again");
        assert!(matches!(
            recovered.begin_protective_cleanup(intent, mutation, 301),
            Err(ExecutionLedgerError::Transition(
                ExecutionTransitionError::InvalidProtectiveCleanupTransition
            ))
        ));
        let cleaned = recovered
            .confirm_protective_cleanup_absent(intent, mutation, 302)
            .expect("only independently verified absent");
        assert_eq!(
            cleaned.record.protection.as_ref().expect("link").status,
            ProtectiveOrderStatus::CleanedUp
        );
        assert!(matches!(
            recovered
                .prepare(plan("intent_cleanup_released_01234"), 303)
                .expect("flat and algo absent"),
            PrepareDisposition::Created(_)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn independently_absent_protection_prepared_cleanup_survives_restart_and_releases_new_risk() {
        let root = temp_root("protective-already-absent");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent = "intent_protection_already_absent_0123";
        let mutation = "cleanup_confirmed_absent_012345";
        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger
                .prepare(protected_plan(intent), 101)
                .expect("prepare parent");
            ledger.begin_submission(intent, 102).expect("submit parent");
            ledger
                .acknowledge(intent, "parent123", 103)
                .expect("ack parent");
            ledger
                .reconcile_found_with_resolutions(
                    intent,
                    "parent123",
                    ExchangeOrderState::Filled,
                    OrderMutationResolution::Pending,
                    Some(ProtectiveOrderResolution::Active {
                        algo_order_id: "123456789012".to_owned(),
                        covered_size: "1".to_owned(),
                    }),
                    104,
                )
                .expect("active protection");
            ledger
                .prepare_protective_cleanup(intent, mutation, 105)
                .expect("PREPARED and no exchange cancellation");
        }
        let mut restarted = DurableExecutionLedger::open(store.clone(), 200).expect("restart");
        assert!(matches!(
            restarted.prepare(plan("intent_blocked_before_confirmation"), 201),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        restarted
            .confirm_protective_cleanup_absent(intent, mutation, 202)
            .expect("independent full flat and algo-absent observation");
        assert!(matches!(
            restarted.begin_protective_cleanup(intent, mutation, 203),
            Err(ExecutionLedgerError::Transition(
                ExecutionTransitionError::InvalidProtectiveCleanupTransition
            ))
        ));
        let mut recovered = DurableExecutionLedger::open(store, 300).expect("restart again");
        let parent = recovered.get(intent).expect("durable parent");
        let protection = parent
            .record
            .protection
            .as_ref()
            .expect("durable protection");
        assert_eq!(protection.status, ProtectiveOrderStatus::CleanedUp);
        assert_eq!(
            protection.cleanup.as_ref().expect("durable proof").state,
            ProtectiveCleanupState::ConfirmedAbsent
        );
        assert!(matches!(
            recovered.prepare(plan("intent_allowed_after_confirmation"), 301),
            Ok(PrepareDisposition::Created(_))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn failed_protection_keeps_instrument_reserved() {
        let root = temp_root("protective-failed");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_protected_76543210";
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");
        ledger
            .prepare(protected_plan(intent_id), 101)
            .expect("prepare");
        ledger.begin_submission(intent_id, 102).expect("submit");
        ledger.acknowledge(intent_id, "ord-1", 103).expect("ack");
        ledger
            .reconcile_found_with_resolutions(
                intent_id,
                "ord-1",
                ExchangeOrderState::Filled,
                OrderMutationResolution::Pending,
                Some(ProtectiveOrderResolution::Failed {
                    code: "51008".to_owned(),
                }),
                104,
            )
            .expect("failed protection");

        assert!(matches!(
            ledger.prepare(plan("intent_blocked_76543210"), 105),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        assert!(matches!(
            ledger
                .prepare(
                    close_plan("intent_close_failed_76543210", PositionSide::Long),
                    106,
                )
                .expect("risk reducing close remains possible"),
            PrepareDisposition::Created(_)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nonterminal_protected_parent_still_blocks_close() {
        let root = temp_root("live-protected-parent");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_live_parent_01234567";
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");
        ledger
            .prepare(protected_plan(intent_id), 101)
            .expect("prepare protected");
        ledger.begin_submission(intent_id, 102).expect("submit");
        ledger
            .acknowledge(intent_id, "order-live", 103)
            .expect("ack");
        assert!(matches!(
            ledger.prepare(
                close_plan("intent_close_before_parent_done", PositionSide::Long),
                104,
            ),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_reserves_instrument_across_restart_until_fresh_open_is_prepared() {
        let root = temp_root("reverse-reservation");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let reverse_id = "intent_reverse_01234567";

        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            let close = close_plan(reverse_id, PositionSide::Long);
            ledger
                .prepare_reverse_close(close, PositionSide::Short, 101)
                .expect("prepare reverse close");
            ledger
                .begin_submission(reverse_id, 102)
                .expect("submit close");
            ledger
                .acknowledge(reverse_id, "close-ord", 103)
                .expect("ack close");
            ledger
                .reconcile_found(reverse_id, "close-ord", ExchangeOrderState::Filled, 104)
                .expect("close filled");

            assert!(matches!(
                ledger.prepare(plan("intent_unrelated_012345"), 105),
                Err(ExecutionLedgerError::InstrumentBusy)
            ));
        }

        let mut reopened = DurableExecutionLedger::open(store.clone(), 200).expect("restart");
        assert!(matches!(
            reopened.prepare(plan("intent_unrelated_765432"), 201),
            Err(ExecutionLedgerError::InstrumentBusy)
        ));

        let open = reverse_open_plan(reverse_id, PositionSide::Short);
        let prepared = reopened
            .prepare_reverse_open(reverse_id, open.clone(), 202)
            .expect("fresh reverse open");
        assert!(matches!(prepared, PrepareDisposition::Created(_)));
        let open_entry = reopened.get(&open.intent_id).expect("reverse open entry");
        let link = open_entry.record.reverse.as_ref().expect("reverse link");
        assert_eq!(link.root_intent_id, reverse_id);
        assert_eq!(link.leg, ReverseLeg::Open);
        assert_eq!(link.target_position_side, PositionSide::Short);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_open_is_blocked_before_close_terminal_and_after_abort() {
        let root = temp_root("reverse-gates");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");
        let reverse_id = "intent_reverse_gate_012345";

        ledger
            .prepare_reverse_close(
                close_plan(reverse_id, PositionSide::Long),
                PositionSide::Short,
                101,
            )
            .expect("prepare close");

        assert!(matches!(
            ledger.prepare_reverse_open(
                reverse_id,
                reverse_open_plan(reverse_id, PositionSide::Short),
                102,
            ),
            Err(ExecutionLedgerError::ReverseNotReady)
        ));

        ledger
            .begin_submission(reverse_id, 103)
            .expect("submit close");
        ledger
            .acknowledge(reverse_id, "close-ord", 104)
            .expect("ack close");
        ledger
            .reconcile_found(reverse_id, "close-ord", ExchangeOrderState::Filled, 105)
            .expect("close filled");
        ledger.abort_reverse(reverse_id, 106).expect("abort");

        assert!(matches!(
            ledger.prepare_reverse_open(
                reverse_id,
                reverse_open_plan(reverse_id, PositionSide::Short),
                107,
            ),
            Err(ExecutionLedgerError::ReverseAborted)
        ));
        assert!(matches!(
            ledger
                .prepare(plan("intent_after_abort_012345"), 108)
                .expect("reservation released"),
            PrepareDisposition::Created(_)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reverse_open_identity_must_match_deterministic_child_and_target_side() {
        let root = temp_root("reverse-identity");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");
        let reverse_id = "intent_reverse_identity_01";

        ledger
            .prepare_reverse_close(
                close_plan(reverse_id, PositionSide::Long),
                PositionSide::Short,
                101,
            )
            .expect("prepare close");
        ledger.begin_submission(reverse_id, 102).expect("submit");
        ledger
            .acknowledge(reverse_id, "close-ord", 103)
            .expect("ack");
        ledger
            .reconcile_found(reverse_id, "close-ord", ExchangeOrderState::Filled, 104)
            .expect("filled");

        let mut wrong_side = reverse_open_plan(reverse_id, PositionSide::Long);
        assert!(matches!(
            ledger.prepare_reverse_open(reverse_id, wrong_side.clone(), 105),
            Err(ExecutionLedgerError::ReverseMismatch)
        ));

        wrong_side.position_side = PositionSide::Short;
        wrong_side.side = OrderSide::Sell;
        wrong_side.intent_id = "intent_wrong_child_012345".to_owned();
        wrong_side.client_order_id = derive_client_order_id(&wrong_side.intent_id);
        assert!(matches!(
            ledger.prepare_reverse_open(reverse_id, wrong_side, 106),
            Err(ExecutionLedgerError::ReverseMismatch)
        ));

        let mut foreign_account = reverse_open_plan(reverse_id, PositionSide::Short);
        foreign_account.account_uid_fingerprint = "other-account-fingerprint".to_owned();
        assert!(matches!(
            ledger.prepare_reverse_open(reverse_id, foreign_account, 107),
            Err(ExecutionLedgerError::ReverseMismatch)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_v1_v2_v3_v4_ledgers_load_and_next_write_upgrades_to_v5() {
        for (index, schema) in [
            EXECUTION_LEDGER_SCHEMA_V1,
            EXECUTION_LEDGER_SCHEMA_V2,
            EXECUTION_LEDGER_SCHEMA_V3,
            EXECUTION_LEDGER_SCHEMA_V4,
        ]
        .into_iter()
        .enumerate()
        {
            let root = temp_root(&format!("schema-upgrade-{index}"));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("root");
            let path = root.join("ledger.json");
            let intent_id = format!("intent_legacy_{index}_0123456789");
            let entry = ExecutionLedgerEntry {
                record: ExecutionRecord::new(plan(&intent_id)),
                created_at_ms: 100,
                updated_at_ms: 100,
            };
            fs::write(
                &path,
                serde_json::to_vec_pretty(&ExecutionLedgerFile {
                    schema: schema.to_owned(),
                    records: vec![entry],
                    risk_stop: None,
                    capital_reservation: None,
                    exchange_evidence: None,
                    strategy_periods: StrategyPeriodHistory::default(),
                })
                .expect("legacy json"),
            )
            .expect("write legacy");

            let store = ExecutionLedgerStore::at(&path);
            let mut ledger = DurableExecutionLedger::open(store, 101).expect("load legacy");
            assert_eq!(ledger.len(), 1);
            ledger.begin_submission(&intent_id, 102).expect("write v6");

            let file: ExecutionLedgerFile =
                serde_json::from_slice(&fs::read(&path).expect("read upgraded")).expect("decode");
            assert_eq!(file.schema, EXECUTION_LEDGER_SCHEMA_V10);

            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn submission_timing_is_durable_before_send_and_survives_unknown_recovery() {
        let root = temp_root("submission-timing-recovery");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_submission_timing_01";

        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger.prepare(plan(intent_id), 101).expect("prepare");
            let submitting = ledger
                .begin_submission_with_timing(intent_id, Some(1_791_300_000_000), 102)
                .expect("submitting");
            let timing = submitting
                .record
                .submission_timing
                .as_ref()
                .expect("request timing");
            assert_eq!(timing.request_exchange_time_ms, 1_791_300_000_000);
            assert_eq!(timing.okx_in_time_us, None);
            assert_eq!(timing.okx_out_time_us, None);
        }

        let recovered = DurableExecutionLedger::open(store.clone(), 200).expect("recover");
        let entry = recovered.get(intent_id).expect("entry");
        assert_eq!(entry.record.state, ExecutionState::UnknownSubmission);
        let timing = entry.record.submission_timing.as_ref().expect("timing");
        assert_eq!(timing.request_exchange_time_ms, 1_791_300_000_000);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn gateway_timing_is_committed_atomically_with_acknowledgement() {
        let root = temp_root("gateway-timing");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_gateway_timing_0123";
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        ledger.prepare(plan(intent_id), 101).expect("prepare");
        ledger
            .begin_submission_with_timing(intent_id, Some(1_791_300_000_000), 102)
            .expect("submitting");
        let acknowledged = ledger
            .acknowledge_with_gateway_timing(
                intent_id,
                "ord-1",
                Some((1_791_300_000_100_000, 1_791_300_000_100_900)),
                103,
            )
            .expect("ack");
        let timing = acknowledged
            .record
            .submission_timing
            .as_ref()
            .expect("timing");
        assert_eq!(timing.okx_in_time_us, Some(1_791_300_000_100_000));
        assert_eq!(timing.okx_out_time_us, Some(1_791_300_000_100_900));

        let reopened = DurableExecutionLedger::open(store, 200).expect("reopen");
        assert_eq!(
            reopened
                .get(intent_id)
                .and_then(|entry| entry.record.submission_timing.as_ref())
                .and_then(|timing| timing.okx_out_time_us),
            Some(1_791_300_000_100_900)
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn risk_stop_is_durable_idempotent_and_blocks_new_risk_after_recovery() {
        let root = temp_root("risk-stop-durable");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_stop_durable_0123456789";
        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger.prepare(plan(intent_id), 101).expect("prepare");
            let stop = ledger.stop_new_risk("account-limit", 102).expect("stop");
            assert_eq!(stop.stopped_at_ms, 102);
            assert_eq!(
                ledger
                    .stop_new_risk("cannot-reset-by-repeating", 103)
                    .expect("idempotent stop"),
                stop,
            );
            assert!(matches!(
                ledger.begin_submission(intent_id, 104),
                Err(ExecutionLedgerError::NewRiskStopped)
            ));
            assert_eq!(
                ledger.get(intent_id).expect("entry").record.state,
                ExecutionState::Prepared
            );
        }
        let mut reopened = DurableExecutionLedger::open(store, 200).expect("reopen");
        assert_eq!(
            reopened.risk_stop().expect("persisted stop").reason,
            "account-limit"
        );
        assert!(matches!(
            reopened.begin_submission(intent_id, 201),
            Err(ExecutionLedgerError::NewRiskStopped)
        ));
        assert!(
            reopened
                .require_new_risk_allowed(ExecutionAction::Close)
                .is_ok()
        );
        assert!(
            reopened
                .require_new_risk_allowed(ExecutionAction::Reduce)
                .is_ok()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reducing_close_and_cancel_remain_available_after_risk_stop() {
        let root = temp_root("risk-stop-allow-reduction");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");
        let close = close_plan("intent_risk_stop_close_01", PositionSide::Long);
        ledger.prepare(close.clone(), 101).expect("prepare close");
        ledger.stop_new_risk("manual-stop", 102).expect("stop");
        let submitting = ledger
            .begin_submission(&close.intent_id, 103)
            .expect("close allowed");
        assert_eq!(submitting.record.state, ExecutionState::Submitting);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn risk_stop_cannot_be_cleared_by_corrupt_metadata_or_legacy_recovery() {
        let root = temp_root("risk-stop-metadata");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        assert!(matches!(
            ledger.stop_new_risk("  ", 101),
            Err(ExecutionLedgerError::InvalidRiskStopReason)
        ));
        ledger.stop_new_risk("manual-stop", 102).expect("durable");
        drop(ledger);
        let mut file: serde_json::Value =
            serde_json::from_slice(&fs::read(store.path()).expect("read")).expect("json");
        file["risk_stop"]["schema"] = serde_json::Value::String("wrong-schema".to_owned());
        fs::write(store.path(), serde_json::to_vec(&file).expect("encode")).expect("corrupt");
        assert!(matches!(
            DurableExecutionLedger::open(store, 200),
            Err(ExecutionLedgerError::Corrupt(
                "invalid durable risk-stop metadata"
            ))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn submitting_is_durable_before_send_and_recovers_as_unknown() {
        let root = temp_root("submitting");
        let _ = fs::remove_dir_all(&root);
        let path = root.join("ledger.json");
        let store = ExecutionLedgerStore::at(&path);

        {
            let mut ledger =
                DurableExecutionLedger::open(store.clone(), 100).expect("initial open");
            ledger
                .prepare(plan("intent_0123456789abcdef"), 101)
                .expect("prepare");
            let submitting = ledger
                .begin_submission("intent_0123456789abcdef", 102)
                .expect("persist submitting");
            assert_eq!(submitting.record.state, ExecutionState::Submitting);
        }

        let reopened = DurableExecutionLedger::open(store.clone(), 200).expect("recovery open");
        let recovered = reopened.get("intent_0123456789abcdef").expect("recovered");
        assert_eq!(recovered.record.state, ExecutionState::UnknownSubmission);
        assert_eq!(recovered.updated_at_ms, 200);
        assert!(!recovered.record.can_submit());

        let again = DurableExecutionLedger::open(store, 201).expect("second reopen");
        assert_eq!(
            again
                .get("intent_0123456789abcdef")
                .expect("persisted recovery")
                .record
                .state,
            ExecutionState::UnknownSubmission
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn inflight_order_mutation_recovers_as_unknown_without_replay_authority() {
        let root = temp_root("mutation-recovery");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at(root.join("ledger.json"));
        let intent_id = "intent_mutation_recovery_01";

        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger.prepare(plan(intent_id), 101).expect("prepare");
            ledger.begin_submission(intent_id, 102).expect("submit");
            ledger.acknowledge(intent_id, "ord-1", 103).expect("ack");
            ledger
                .reconcile_found(intent_id, "ord-1", ExchangeOrderState::Live, 104)
                .expect("live");
            ledger
                .prepare_order_mutation(
                    intent_id,
                    OrderMutationRecord::cancel("mutation_cancel_restart_01").expect("cancel"),
                    105,
                )
                .expect("prepare cancel");
            let submitting = ledger
                .begin_order_mutation_submission(intent_id, "mutation_cancel_restart_01", 106)
                .expect("persist mutation submitting");
            assert_eq!(
                submitting
                    .record
                    .active_mutation()
                    .expect("active mutation")
                    .state,
                OrderMutationState::Submitting
            );
        }

        let mut reopened = DurableExecutionLedger::open(store.clone(), 200).expect("recovery open");
        let recovered = reopened.get(intent_id).expect("entry");
        assert_eq!(recovered.record.state, ExecutionState::Live);
        assert_eq!(
            recovered
                .record
                .active_mutation()
                .expect("active mutation")
                .state,
            OrderMutationState::Unknown
        );
        assert!(matches!(
            reopened.begin_order_mutation_submission(intent_id, "mutation_cancel_restart_01", 201,),
            Err(ExecutionLedgerError::Transition(
                ExecutionTransitionError::InvalidMutationTransition { .. }
            ))
        ));

        let again = DurableExecutionLedger::open(store, 202).expect("second reopen");
        assert_eq!(
            again
                .get(intent_id)
                .expect("persisted entry")
                .record
                .active_mutation()
                .expect("mutation")
                .state,
            OrderMutationState::Unknown
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn acknowledgement_and_exchange_reconciliation_survive_restart() {
        let root = temp_root("reconcile");
        let _ = fs::remove_dir_all(&root);
        let path = root.join("ledger.json");
        let store = ExecutionLedgerStore::at(&path);

        {
            let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
            ledger
                .prepare(plan("intent_0123456789abcdef"), 101)
                .expect("prepare");
            ledger
                .begin_submission("intent_0123456789abcdef", 102)
                .expect("submit");
            ledger
                .acknowledge("intent_0123456789abcdef", "ord-1", 103)
                .expect("ack");
            let partial = ledger
                .reconcile_found(
                    "intent_0123456789abcdef",
                    "ord-1",
                    ExchangeOrderState::PartiallyFilled,
                    104,
                )
                .expect("partial");
            assert_eq!(partial.record.state, ExecutionState::PartiallyFilled);
            assert!(!partial.record.can_submit());
        }

        let mut reopened =
            DurableExecutionLedger::open(store.clone(), 200).expect("partial-fill restart");
        let partial = &reopened
            .get("intent_0123456789abcdef")
            .expect("partial entry")
            .record;
        assert_eq!(partial.state, ExecutionState::PartiallyFilled);
        assert_eq!(partial.order_id.as_deref(), Some("ord-1"));
        assert!(!partial.can_submit());

        reopened
            .reconcile_found(
                "intent_0123456789abcdef",
                "ord-1",
                ExchangeOrderState::Filled,
                201,
            )
            .expect("filled after restart");

        let terminal = DurableExecutionLedger::open(store, 300).expect("terminal reopen");
        let record = &terminal
            .get("intent_0123456789abcdef")
            .expect("terminal entry")
            .record;
        assert_eq!(record.state, ExecutionState::Filled);
        assert_eq!(record.order_id.as_deref(), Some("ord-1"));
        assert!(record.state.is_terminal());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_time_and_ledger_capacity_fail_closed() {
        let root = temp_root("capacity");
        let _ = fs::remove_dir_all(&root);
        let store = ExecutionLedgerStore::at_with_limit(root.join("ledger.json"), 1);
        let mut ledger = DurableExecutionLedger::open(store, 100).expect("open");

        assert!(matches!(
            ledger.prepare(plan("intent_0123456789abcdef"), 0),
            Err(ExecutionLedgerError::InvalidTimestamp)
        ));
        ledger
            .prepare(plan("intent_0123456789abcdef"), 101)
            .expect("first");
        assert!(matches!(
            ledger.prepare(plan("intent_fedcba9876543210"), 102),
            Err(ExecutionLedgerError::CapacityExceeded(1))
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_or_tampered_ledger_fails_closed() {
        let root = temp_root("corrupt");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let path = root.join("ledger.json");
        fs::write(&path, r#"{"schema":"wrong","records":[]}"#).expect("write corrupt");

        assert!(matches!(
            DurableExecutionLedger::open(ExecutionLedgerStore::at(&path), 100),
            Err(ExecutionLedgerError::Corrupt("unsupported schema"))
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn backward_time_does_not_mutate_persisted_state() {
        let root = temp_root("time");
        let _ = fs::remove_dir_all(&root);
        let path = root.join("ledger.json");
        let store = ExecutionLedgerStore::at(&path);
        let mut ledger = DurableExecutionLedger::open(store.clone(), 100).expect("open");
        ledger
            .prepare(plan("intent_0123456789abcdef"), 200)
            .expect("prepare");

        assert!(matches!(
            ledger.begin_submission("intent_0123456789abcdef", 199),
            Err(ExecutionLedgerError::InvalidTimestamp)
        ));
        assert_eq!(
            ledger
                .get("intent_0123456789abcdef")
                .expect("entry")
                .record
                .state,
            ExecutionState::Prepared
        );

        let reopened = DurableExecutionLedger::open(store, 201).expect("reopen");
        assert_eq!(
            reopened
                .get("intent_0123456789abcdef")
                .expect("persisted")
                .record
                .state,
            ExecutionState::Prepared
        );

        let _ = fs::remove_dir_all(root);
    }
}
