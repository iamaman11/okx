use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    EXECUTION_PLAN_SCHEMA_V1, ExchangeOrderState, ExecutionPlan, ExecutionRecord, ExecutionState,
    ExecutionTransitionError, derive_client_order_id, model::valid_intent_id,
};

pub const EXECUTION_LEDGER_SCHEMA_V1: &str = "okx.execution-ledger/v1";
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
        if file.schema != EXECUTION_LEDGER_SCHEMA_V1 {
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
            schema: EXECUTION_LEDGER_SCHEMA_V1.to_owned(),
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
        require_timestamp(observed_at_ms)?;
        validate_plan_identity(&plan)?;

        if let Some(existing) = self.entries.get(&plan.intent_id) {
            return if existing.record.plan == plan {
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

        if self
            .entries
            .values()
            .any(|entry| entry.record.plan.client_order_id == plan.client_order_id)
        {
            return Err(ExecutionLedgerError::ClientOrderIdCollision);
        }

        let entry = ExecutionLedgerEntry {
            record: ExecutionRecord::new(plan),
            created_at_ms: observed_at_ms,
            updated_at_ms: observed_at_ms,
        };

        let intent_id = entry.record.plan.intent_id.clone();
        self.commit_new(intent_id, entry.clone())?;
        Ok(PrepareDisposition::Created(entry))
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

    pub fn reconcile_found(
        &mut self,
        intent_id: &str,
        order_id: impl Into<String>,
        exchange_state: ExchangeOrderState,
        observed_at_ms: u64,
    ) -> Result<ExecutionLedgerEntry, ExecutionLedgerError> {
        let order_id = order_id.into();
        self.mutate(intent_id, observed_at_ms, move |record| {
            record.reconcile_found(order_id, exchange_state)
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
        EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, OrderSide, OrderType, PositionSide, TradeMode,
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
        let reopened_entry = reopened
            .get("intent_0123456789abcdef")
            .expect("entry");
        assert_eq!(reopened_entry.record.state, ExecutionState::Prepared);
        assert!(
            reopened_entry.record.plan.risk_binding.is_none(),
            "legacy plan without serialized risk_binding must remain readable"
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
    fn acknowledgement_and_exchange_reconciliation_survive_restart() {
        let root = temp_root("reconcile");
        let _ = fs::remove_dir_all(&root);
        let path = root.join("ledger.json");
        let store = ExecutionLedgerStore::at(&path);

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
        ledger
            .reconcile_found(
                "intent_0123456789abcdef",
                "ord-1",
                ExchangeOrderState::PartiallyFilled,
                104,
            )
            .expect("partial");
        ledger
            .reconcile_found(
                "intent_0123456789abcdef",
                "ord-1",
                ExchangeOrderState::Filled,
                105,
            )
            .expect("filled");

        let reopened = DurableExecutionLedger::open(store, 200).expect("reopen");
        let record = &reopened
            .get("intent_0123456789abcdef")
            .expect("entry")
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
