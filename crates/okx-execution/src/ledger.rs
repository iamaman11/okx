use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};

use rust_decimal::Decimal;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    EXECUTION_LINEAGE_SCHEMA_V1, EXECUTION_PLAN_SCHEMA_V1, ExchangeOrderState, ExecutionAction,
    ExecutionLineageBinding, ExecutionPlan, ExecutionRecord, ExecutionState,
    ExecutionTransitionError, MAX_ORDER_MUTATIONS_PER_EXECUTION, OrderMutationKind,
    OrderMutationRecord, OrderMutationResolution, OrderMutationState, PROTECTIVE_ORDER_POLICY_V1,
    PositionSide, ProtectiveOrderResolution, ProtectiveOrderStatus, ProtectiveTriggerPriceBasis,
    ReverseContinuation, ReverseExecutionLink, ReverseLeg, derive_client_order_id,
    derive_protective_algo_client_id, derive_reverse_open_intent_id,
    model::{valid_intent_id, valid_mutation_id},
};

pub const EXECUTION_LEDGER_SCHEMA_V1: &str = "okx.execution-ledger/v1";
pub const EXECUTION_LEDGER_SCHEMA_V2: &str = "okx.execution-ledger/v2";
pub const EXECUTION_LEDGER_SCHEMA_V3: &str = "okx.execution-ledger/v3";
pub const EXECUTION_LEDGER_SCHEMA_V4: &str = "okx.execution-ledger/v4";
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

    #[error("execution ledger capacity of {0} records is exhausted")]
    CapacityExceeded(usize),

    #[error("execution intent '{0}' is not present in the ledger")]
    IntentNotFound(String),

    #[error(transparent)]
    Transition(#[from] ExecutionTransitionError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionLedgerFile {
    schema: String,
    records: Vec<ExecutionLedgerEntry>,
}

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

    fn load(&self) -> Result<BTreeMap<String, ExecutionLedgerEntry>, ExecutionLedgerError> {
        if !self.path.exists() {
            return Ok(BTreeMap::new());
        }

        let bytes = fs::read(&self.path)?;
        let file: ExecutionLedgerFile = serde_json::from_slice(&bytes)?;
        if !matches!(
            file.schema.as_str(),
            EXECUTION_LEDGER_SCHEMA_V1
                | EXECUTION_LEDGER_SCHEMA_V2
                | EXECUTION_LEDGER_SCHEMA_V3
                | EXECUTION_LEDGER_SCHEMA_V4
        ) {
            return Err(ExecutionLedgerError::Corrupt("unsupported schema"));
        }
        if file.records.len() > self.max_records {
            return Err(ExecutionLedgerError::Corrupt(
                "record count exceeds capacity",
            ));
        }

        let mut by_intent = BTreeMap::new();
        let mut client_order_ids = BTreeSet::new();
        for entry in file.records {
            validate_entry(&entry)?;
            let intent_id = entry.record.plan.intent_id.clone();
            if by_intent.insert(intent_id, entry.clone()).is_some() {
                return Err(ExecutionLedgerError::Corrupt("duplicate intent_id"));
            }
            if !client_order_ids.insert(entry.record.plan.client_order_id.clone()) {
                return Err(ExecutionLedgerError::Corrupt("duplicate client_order_id"));
            }
        }

        Ok(by_intent)
    }

    fn save(
        &self,
        entries: &BTreeMap<String, ExecutionLedgerEntry>,
    ) -> Result<(), ExecutionLedgerError> {
        if entries.len() > self.max_records {
            return Err(ExecutionLedgerError::CapacityExceeded(self.max_records));
        }

        let mut client_order_ids = BTreeSet::new();
        for (intent_id, entry) in entries {
            validate_entry(entry)?;
            if intent_id != &entry.record.plan.intent_id {
                return Err(ExecutionLedgerError::Corrupt(
                    "map key does not match intent_id",
                ));
            }
            if !client_order_ids.insert(entry.record.plan.client_order_id.clone()) {
                return Err(ExecutionLedgerError::ClientOrderIdCollision);
            }
        }

        let parent = self
            .path
            .parent()
            .ok_or(ExecutionLedgerError::Corrupt("ledger path has no parent"))?;
        fs::create_dir_all(parent)?;

        let payload = serde_json::to_vec_pretty(&ExecutionLedgerFile {
            schema: EXECUTION_LEDGER_SCHEMA_V4.to_owned(),
            records: entries.values().cloned().collect(),
        })?;

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
        let mut entries = store.load()?;
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
            store.save(&entries)?;
        }

        Ok(Self { store, entries })
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
        self.prepare_record(record, None, observed_at_ms)
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
        self.prepare_record(record, None, observed_at_ms)
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
        self.prepare_record(record, Some(root_intent_id), observed_at_ms)
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

    fn prepare_record(
        &mut self,
        record: ExecutionRecord,
        reverse_root_bypass: Option<&str>,
        observed_at_ms: u64,
    ) -> Result<PrepareDisposition, ExecutionLedgerError> {
        require_timestamp(observed_at_ms)?;
        validate_plan_identity(&record.plan)?;
        validate_reverse_link(&record)?;

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

        if self.instrument_reserved(
            &record.plan.instrument_id,
            &record.plan.intent_id,
            reverse_root_bypass,
        ) {
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
        reverse_root_bypass: Option<&str>,
    ) -> bool {
        self.entries.values().any(|entry| {
            if entry.record.plan.instrument_id != instrument_id {
                return false;
            }
            if !entry.record.state.is_terminal() {
                return true;
            }
            if entry.record.protection_blocks_new_managed_intent() {
                return true;
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
        self.mutate(intent_id, observed_at_ms, |record| {
            record.begin_submission()
        })
    }

    pub fn acknowledge(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let order_id = order_id.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.acknowledge(order_id)
        })
    }

    pub fn mark_unknown_submission(
        &mut self,
        intent_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        self.mutate(intent_id, observed_at_ms, |record| {
            record.mark_unknown_submission()
        })
    }

    pub fn reject_known(
        &mut self,
        intent_id: &str,
        code: impl Into<String>,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let code = code.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
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

    pub fn begin_order_mutation_submission(
        &mut self,
        intent_id: &str,
        mutation_id: &str,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
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
        if let Err(error) = self.store.save(&self.entries) {
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

        if let Err(error) = self.store.save(&self.entries) {
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

fn validate_entry(entry: &ExecutionLedgerEntry) -> Result<(), ExecutionLedgerError> {
    validate_plan_identity(&entry.record.plan)?;
    validate_execution_lineage(&entry.record)?;
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

fn validate_execution_lineage(record: &ExecutionRecord) -> Result<(), ExecutionLedgerError> {
    let Some(lineage) = record.lineage.as_ref() else {
        return Ok(());
    };
    let price = Decimal::from_str(&lineage.decision_reference.price)
        .ok()
        .filter(|value| *value > Decimal::ZERO);
    if lineage.schema != EXECUTION_LINEAGE_SCHEMA_V1
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
        ProtectiveOrderStatus::Active => {
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

#[cfg(test)]
mod tests {
    use super::*;
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
            restarted
                .prepare(plan("intent_released_0123456"), 203)
                .expect("released"),
            PrepareDisposition::Created(_)
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

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_v1_v2_v3_ledgers_load_and_next_write_upgrades_to_v4() {
        for (index, schema) in [
            EXECUTION_LEDGER_SCHEMA_V1,
            EXECUTION_LEDGER_SCHEMA_V2,
            EXECUTION_LEDGER_SCHEMA_V3,
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
                })
                .expect("legacy json"),
            )
            .expect("write legacy");

            let store = ExecutionLedgerStore::at(&path);
            let mut ledger = DurableExecutionLedger::open(store, 101).expect("load legacy");
            assert_eq!(ledger.len(), 1);
            ledger.begin_submission(&intent_id, 102).expect("write v4");

            let file: ExecutionLedgerFile =
                serde_json::from_slice(&fs::read(&path).expect("read upgraded")).expect("decode");
            assert_eq!(file.schema, EXECUTION_LEDGER_SCHEMA_V4);

            let _ = fs::remove_dir_all(root);
        }
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
