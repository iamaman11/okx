//! One typed, independent account owner in the existing Rust process.
//!
//! Registration never creates a database, migrates v14, starts a service or
//! grants venue POST authority. The caller must first publish a sealed native
//! SQLite owner through CONTROL's verified offline cutover. An account handle
//! holds exactly one exclusive writer lease for its whole lifetime.
//!
//! Locks are account-local: unrelated accounts never share an execution mutex.
use super::{
    DurableExecutionLedger, ExecutionLedgerError, ExecutionLedgerStore, LedgerEnvironment,
    LedgerVenue, SqliteAccountIdentity,
};
use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// Public routing identity, derived from a verified native account binding.
/// No opaque or caller-supplied account string becomes trading authority.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccountKey {
    pub venue: LedgerVenue,
    pub environment: LedgerEnvironment,
    pub account_id: String,
    pub uid_fingerprint: String,
}

impl From<&SqliteAccountIdentity> for AccountKey {
    fn from(identity: &SqliteAccountIdentity) -> Self {
        Self {
            venue: identity.venue,
            environment: identity.environment,
            account_id: identity.account_id.clone(),
            uid_fingerprint: identity.uid_fingerprint.clone(),
        }
    }
}

/// Native ledger ownership, not an exchange adapter or alternative executor.
/// The file lease survives for as long as any handle holds this context.
#[derive(Debug)]
pub struct AccountContext {
    identity: SqliteAccountIdentity,
    root: PathBuf,
    ledger: DurableExecutionLedger,
    _writer_lease: File,
}

impl AccountContext {
    /// Open ONLY an already-published, bound SQLite account under the same
    /// cross-process lock used by CONTROL. Never stage, migrate or fall back to
    /// v14 from this API. The DB UID and venue binding must match the key.
    pub fn open_existing(
        root: &Path,
        expected: &AccountKey,
        observed_at_ms: u64,
    ) -> Result<Self, ExecutionLedgerError> {
        let mut writer_lease = ExecutionLedgerStore::acquire_writer_lease(root)?;
        let store = ExecutionLedgerStore::select_published_owner_under_writer_lease(
            root,
            &mut writer_lease,
        )?;
        if !store.is_sqlite_native() {
            return Err(ExecutionLedgerError::Corrupt(
                "account context requires published SQLite owner",
            ));
        }
        let identity = store
            .bound_native_account_identity()?
            .ok_or(ExecutionLedgerError::AccountScopeMismatch)?;
        if AccountKey::from(&identity) != *expected {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        let ledger = DurableExecutionLedger::open(store, observed_at_ms)?;
        if !ledger.account_scope_matches(&identity.uid_fingerprint)? {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        Ok(Self {
            identity,
            root: root.to_owned(),
            ledger,
            _writer_lease: writer_lease,
        })
    }

    pub fn identity(&self) -> &SqliteAccountIdentity {
        &self.identity
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn ledger(&self) -> &DurableExecutionLedger {
        &self.ledger
    }

    /// Only the account's canonical ledger may mutate its state. The caller
    /// must separately own the existing venue-level submission permission.
    pub fn ledger_mut(&mut self) -> &mut DurableExecutionLedger {
        &mut self.ledger
    }
}

/// Cloneable account-local synchronization handle. It cannot be copied into
/// a second writer: all clones share the same ledger and exclusive file lease.
pub type AccountContextHandle = Arc<Mutex<AccountContext>>;

/// One in-process account catalog, backed by independent native WAL owners.
/// Registry membership does not modify credentials, venue modes or RiskBook.
#[derive(Debug, Default)]
pub struct AccountContextRegistry {
    accounts: BTreeMap<AccountKey, RegisteredAccount>,
}

/// Registry metadata is immutable after registration. Inspecting routes and
/// roots never locks a trading account or waits for its in-flight operation.
#[derive(Debug)]
struct RegisteredAccount {
    root: PathBuf,
    handle: AccountContextHandle,
}

impl AccountContextRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an existing account exactly once. A duplicate key or second
    /// root alias is never an implicit replacement or a new writer. Different
    /// keys can be operated on concurrently via independent account handles.
    pub fn register_existing(
        &mut self,
        root: &Path,
        expected: &AccountKey,
        observed_at_ms: u64,
    ) -> Result<(), ExecutionLedgerError> {
        if self.accounts.keys().any(|owned| {
            owned.venue == expected.venue
                && owned.environment == expected.environment
                && (owned.account_id == expected.account_id
                    || owned.uid_fingerprint == expected.uid_fingerprint)
        }) || self.accounts.values().any(|owned| owned.root == root)
        {
            return Err(ExecutionLedgerError::AccountScopeMismatch);
        }
        let context = AccountContext::open_existing(root, expected, observed_at_ms)?;
        if self
            .accounts
            .insert(
                expected.clone(),
                RegisteredAccount {
                    root: root.to_owned(),
                    handle: Arc::new(Mutex::new(context)),
                },
            )
            .is_some()
        {
            return Err(ExecutionLedgerError::Corrupt(
                "account registry duplicate owner",
            ));
        }
        Ok(())
    }

    pub fn get(&self, key: &AccountKey) -> Option<AccountContextHandle> {
        self.accounts.get(key).map(|owned| Arc::clone(&owned.handle))
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EXECUTION_PLAN_SCHEMA_V1, ExecutionAction, ExecutionLedgerEntry, ExecutionPlan,
        ExecutionRecord, ExecutionState, OrderSide, OrderType, PositionSide, StrategyPeriodHistory,
        TradeMode, derive_client_order_id,
    };
    use std::{collections::BTreeMap, fs, thread};

    fn fixture(root: &Path, ordinal: u32) -> (AccountKey, String) {
        fs::create_dir_all(root).unwrap();
        let uid = format!("{ordinal:064x}");
        let account_id = format!("account-{ordinal}");
        let intent_id = format!("registered_intent_{ordinal:04}_0123456789");
        let plan = ExecutionPlan {
            schema: EXECUTION_PLAN_SCHEMA_V1.to_owned(),
            intent_id: intent_id.clone(),
            client_order_id: derive_client_order_id(&intent_id),
            reference_generation: "sha256:reference".to_owned(),
            account_generation: "sha256:account".to_owned(),
            account_uid_fingerprint: uid.clone(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            action: ExecutionAction::Open,
            order_type: OrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
            open_risk: None,
            risk_binding: None,
        };
        let mut record = ExecutionRecord::new(plan);
        record.abandon_prepared().unwrap();
        assert_eq!(record.state, ExecutionState::Rejected);
        let legacy = ExecutionLedgerStore::at(root.join("execution-ledger.json"));
        legacy
            .save(
                &BTreeMap::from([(
                    intent_id.clone(),
                    ExecutionLedgerEntry {
                        record,
                        created_at_ms: 1000,
                        updated_at_ms: 1001,
                    },
                )]),
                None,
                None,
                None,
                &StrategyPeriodHistory::default(),
                &[],
            )
            .unwrap();
        let proof = legacy.attest_storage().unwrap();
        let identity = SqliteAccountIdentity {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Demo,
            account_id,
            uid_fingerprint: uid,
            bound_at_ms: 2000,
        };
        let native = ExecutionLedgerStore::migrate_to_sqlite_offline_for_account(
            root,
            &identity.uid_fingerprint,
            &proof.manifest_sha256,
            &identity,
        )
        .expect("single owner v14 -> native cutover");
        assert!(native.is_sqlite_native());
        (AccountKey::from(&identity), intent_id)
    }

    #[test]
    fn ten_native_account_contexts_are_isolated_and_exclude_second_writers() {
        let root =
            std::env::temp_dir().join(format!("okx-account-contexts-ten-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let mut registry = AccountContextRegistry::new();
        let mut identities = Vec::new();
        for ordinal in 1..=10 {
            let account_root = root.join(format!("account-{ordinal}"));
            let (key, intent_id) = fixture(&account_root, ordinal);
            registry
                .register_existing(&account_root, &key, 3000)
                .unwrap();
            identities.push((account_root, key, intent_id));
        }
        assert_eq!(registry.len(), 10);
        let mut workers = Vec::new();
        for (root, key, intent_id) in &identities {
            let key = key.clone();
            let intent_id = intent_id.clone();
            let account_root = root.clone();
            let handle = registry.get(&key).expect("registered account");
            workers.push(thread::spawn(move || {
                let mut ctx = handle.lock().unwrap();
                assert_eq!(ctx.root(), account_root);
                assert_eq!(AccountKey::from(ctx.identity()), key);
                let prior = ctx.ledger().read_intent(&intent_id).unwrap().unwrap();
                assert_eq!(prior.record.state, ExecutionState::Rejected);
                assert_eq!(ctx.ledger().total_intents().unwrap(), 1);
                let foreign = "f".repeat(64);
                assert!(ctx.ledger().account_scope_matches(&foreign).is_err());

                // Independent WAL mutations across accounts, but never an
                // exchange POST: only locally prepare a reducing-side intent.
                let mut safe_exit = prior.record.plan;
                safe_exit.intent_id = format!("{intent_id}_exit");
                safe_exit.client_order_id = derive_client_order_id(&safe_exit.intent_id);
                safe_exit.action = ExecutionAction::Close;
                safe_exit.side = OrderSide::Sell;
                safe_exit.open_risk = None;
                safe_exit.risk_binding = None;
                ctx.ledger_mut().prepare(safe_exit.clone(), 3500).unwrap();
                assert_eq!(
                    ctx.ledger().read_intent(&safe_exit.intent_id).unwrap().unwrap().record.state,
                    ExecutionState::Prepared
                );
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let (first_root, first_key, _) = &identities[0];
        assert!(
            registry
                .register_existing(first_root, first_key, 3000)
                .is_err()
        );
        let mut impostor = first_key.clone();
        impostor.uid_fingerprint = "f".repeat(64);
        assert!(
            registry
                .register_existing(first_root, &impostor, 3000)
                .is_err()
        );
        drop(registry);
        // The lease is released only after every handle is gone, and a
        // restart revalidates each independent native owner without replay.
        let mut reopened = AccountContextRegistry::new();
        for (root, key, intent_id) in &identities {
            reopened.register_existing(root, key, 4000).unwrap();
            let handle = reopened.get(key).unwrap();
            let account = handle.lock().unwrap();
            assert_eq!(account.ledger().total_intents().unwrap(), 2);
            assert_eq!(
                account
                    .ledger()
                    .read_intent(&format!("{intent_id}_exit"))
                    .unwrap()
                    .unwrap()
                    .record
                    .state,
                ExecutionState::Prepared
            );
        }
        assert_eq!(reopened.len(), 10);
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn an_unmigrated_v14_account_cannot_enter_native_registry() {
        let root = std::env::temp_dir().join(format!(
            "okx-account-context-v14-denied-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let key = AccountKey {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Demo,
            account_id: "demo".to_owned(),
            uid_fingerprint: "a".repeat(64),
        };
        assert!(AccountContext::open_existing(&root, &key, 100).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
