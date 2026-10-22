//! Verified account resources in the existing Engine. Catalog writes are
//! immutable, atomic publications; access/limits remain exclusively in governance.
use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    execution_runtime::ExecutionRuntime,
    okx_credentials::{NamedAccountCredentials, load_native_account_credentials},
};
use chrono::Utc;
use okx_api::{AccountApi, OkxRestClient, RateBudget};
use okx_execution::{ExecutionLedgerStore, LedgerEnvironment, LedgerVenue, SqliteAccountIdentity};
use okx_protocol::accounts::{
    AccountCatalog, AccountKey, Environment, VerifiedAccountConfig, valid_reference,
};
use okx_runtime::{PrivateWsCoordinator, PrivateWsHandle};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{RwLock, Semaphore, watch},
    task::JoinHandle,
};

const CATALOG_DIR: &str = "account-catalog";
const INSTALLATION_FILE: &str = "installation.json";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Installation {
    schema: String,
    installation_id: String,
}

pub struct AccountResources {
    pub configuration: VerifiedAccountConfig,
    pub observer: AccountBootstrapper,
    pub private_ws: PrivateWsHandle,
    pub execution: Arc<ExecutionRuntime>,
    /// Bounded account-local mutation capacity. Never held by reads/analytics.
    pub mutation_capacity: Semaphore,
    source_shutdown: watch::Sender<bool>,
    verified_source_generation: AtomicU64,
    _identity_lease: File,
    source_tasks: Mutex<Vec<JoinHandle<()>>>,
}
impl Drop for AccountResources {
    fn drop(&mut self) {
        let _ = self.source_shutdown.send(true);
    }
}

pub struct AccountRuntimeRegistry {
    root: PathBuf,
    installation_id: String,
    routes: RwLock<BTreeMap<String, Arc<AccountResources>>>,
    configurations: RwLock<BTreeMap<String, VerifiedAccountConfig>>,
    failures: RwLock<BTreeMap<String, String>>,
    /// Only catalog publication is serialized. Verification/HTTP never hold it.
    catalog_write: tokio::sync::Mutex<()>,
    rate_budget: RateBudget,
}

fn invalid() -> AgentError {
    AgentError::FinancialCaptureTask("ACCOUNT_CATALOG_INVALID".into())
}
fn now_ms() -> u64 {
    Utc::now().timestamp_millis().max(1) as u64
}
fn key_hash(key: &AccountKey) -> String {
    format!("{:x}", Sha256::digest(key.wire().as_bytes()))
}

/// Create an installation once while the Engine is stopped. Normal startup
/// calls open(), which never creates a catalog or account database.
pub fn initialize_installation(root: &Path, installation_id: &str) -> AgentResult<()> {
    if !valid_reference(installation_id) {
        return Err(invalid());
    }
    fs::create_dir_all(root)?;
    let bytes = serde_json::to_vec(&Installation {
        schema: "okx.installation/v1".into(),
        installation_id: installation_id.into(),
    })?;
    publish_new(&root.join(INSTALLATION_FILE), &bytes)?;
    fs::create_dir_all(root.join(CATALOG_DIR))?;
    Ok(())
}

/// Atomically publish without replacing another account/operation. The temp
/// and target share one filesystem; no remove-then-rename gap on Windows.
fn publish_new(path: &Path, bytes: &[u8]) -> AgentResult<()> {
    let parent = path.parent().ok_or_else(invalid)?;
    fs::create_dir_all(parent)?;
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|_| invalid())?;
    let temp = parent.join(format!(
        ".publish-{}-{:x}",
        std::process::id(),
        u128::from_le_bytes(nonce)
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let outcome = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::hard_link(&temp, path)?;
        Ok(())
    })();
    drop(file);
    let _ = fs::remove_file(&temp);
    outcome?;
    #[cfg(not(windows))]
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

impl AccountRuntimeRegistry {
    pub async fn open(root: &Path, rate_budget: RateBudget) -> AgentResult<Self> {
        let installation: Installation =
            serde_json::from_slice(&fs::read(root.join(INSTALLATION_FILE))?)?;
        if installation.schema != "okx.installation/v1"
            || !valid_reference(&installation.installation_id)
        {
            return Err(invalid());
        }
        let mut configs = Vec::new();
        for entry in fs::read_dir(root.join(CATALOG_DIR))? {
            let entry = entry?;
            if entry.path().extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(entry.path())?;
            if bytes.len() > 8192 {
                return Err(invalid());
            }
            let config: VerifiedAccountConfig = serde_json::from_slice(&bytes)?;
            if entry.file_name().to_string_lossy()
                != format!("{}.json", key_hash(&config.account_key))
            {
                return Err(invalid());
            }
            configs.push(config);
            if configs.len() > 32 {
                return Err(invalid());
            }
        }
        let catalog = AccountCatalog {
            schema: AccountCatalog::SCHEMA.into(),
            installation_id: installation.installation_id.clone(),
            accounts: configs.clone(),
        };
        if !catalog.valid() {
            return Err(invalid());
        }
        let registry = Self {
            root: root.into(),
            installation_id: installation.installation_id,
            routes: RwLock::new(BTreeMap::new()),
            configurations: RwLock::new(
                configs
                    .iter()
                    .map(|c| (c.account_key.wire(), c.clone()))
                    .collect(),
            ),
            failures: RwLock::new(BTreeMap::new()),
            catalog_write: tokio::sync::Mutex::new(()),
            rate_budget,
        };
        // Build independently: one account's slow verification cannot block another.
        use futures_util::{StreamExt, stream};
        let owner = &registry;
        let opened = stream::iter(configs.into_iter().map(|config| async move {
            let result = async {
                let credentials = load_native_account_credentials(&config.credential_ref)?;
                owner
                    .open_resources(config.clone(), credentials, false)
                    .await
            };
            let result = tokio::time::timeout(std::time::Duration::from_secs(15), result).await;
            (config, result)
        }))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
        for (config, result) in opened {
            match result {
                Ok(Ok(resources)) => {
                    registry
                        .routes
                        .write()
                        .await
                        .insert(config.account_key.wire(), resources);
                }
                _ => {
                    registry.failures.write().await.insert(
                        config.account_key.wire(),
                        "ACCOUNT_CONTEXT_UNAVAILABLE".into(),
                    );
                }
            }
        }
        Ok(registry)
    }

    pub fn installation_id(&self) -> &str {
        &self.installation_id
    }
    pub async fn list(&self) -> Vec<VerifiedAccountConfig> {
        self.configurations.read().await.values().cloned().collect()
    }
    pub async fn resolve(&self, account: &str) -> Option<Arc<AccountResources>> {
        let key = self
            .configurations
            .read()
            .await
            .values()
            .find(|c| c.alias == account || c.account_key.wire() == account)
            .map(|c| c.account_key.wire())?;
        self.routes.read().await.get(&key).cloned()
    }
    pub async fn failures(&self) -> BTreeMap<String, String> {
        self.failures.read().await.clone()
    }

    pub async fn connect(
        &self,
        credential_ref: &str,
        alias: &str,
        operation_id: &str,
    ) -> AgentResult<VerifiedAccountConfig> {
        if !valid_reference(credential_ref)
            || !valid_reference(alias)
            || !(8..=128).contains(&operation_id.len())
            || !operation_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(invalid());
        }
        let configurations = self.configurations.read().await;
        if let Some(existing) = configurations
            .values()
            .find(|c| c.connect_operation_id == operation_id)
        {
            if existing.alias != alias || existing.credential_ref != credential_ref {
                return Err(AgentError::FinancialCaptureTask(
                    "IDEMPOTENCY_CONFLICT".into(),
                ));
            }
            return Ok(existing.clone());
        }
        if configurations
            .values()
            .any(|c| c.alias == alias || c.credential_ref == credential_ref)
        {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_ALREADY_CONNECTED".into(),
            ));
        }
        drop(configurations);
        let credentials = load_native_account_credentials(credential_ref)?;
        let uid = verify_credentials(&credentials, self.rate_budget.clone()).await?;
        let key = AccountKey {
            venue: "okx".into(),
            environment: if credentials.environment.demo {
                Environment::Demo
            } else {
                Environment::Real
            },
            verified_uid: uid,
        };
        if self.configurations.read().await.contains_key(&key.wire()) {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_ALREADY_CONNECTED".into(),
            ));
        }
        let configuration = VerifiedAccountConfig {
            account_key: key,
            alias: alias.into(),
            credential_ref: credential_ref.into(),
            connect_operation_id: operation_id.into(),
        };
        // Serialize initialization for this new account only, without holding the
        // catalog lock or another account's execution mutex across network I/O.
        let resources = self
            .open_resources(configuration.clone(), credentials, true)
            .await?;
        self.publish(resources).await?;
        Ok(configuration)
    }

    async fn publish(&self, resources: Arc<AccountResources>) -> AgentResult<()> {
        let configuration = &resources.configuration;
        if !configuration.valid() {
            return Err(invalid());
        }
        let _publication = self.catalog_write.lock().await;
        let mut configs = self.configurations.write().await;
        if configs.len() >= 32
            || configs.values().any(|c| {
                c.alias == configuration.alias || c.credential_ref == configuration.credential_ref
            })
            || configs.contains_key(&configuration.account_key.wire())
        {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_ALREADY_CONNECTED".into(),
            ));
        }
        let bytes = serde_json::to_vec(configuration)?;
        publish_new(
            &self
                .root
                .join(CATALOG_DIR)
                .join(format!("{}.json", key_hash(&configuration.account_key))),
            &bytes,
        )?;
        configs.insert(configuration.account_key.wire(), configuration.clone());
        self.routes
            .write()
            .await
            .insert(configuration.account_key.wire(), resources);
        Ok(())
    }

    async fn open_resources(
        &self,
        configuration: VerifiedAccountConfig,
        credentials: NamedAccountCredentials,
        initialize: bool,
    ) -> AgentResult<Arc<AccountResources>> {
        let uid = verify_credentials(&credentials, self.rate_budget.clone()).await?;
        if uid != configuration.account_key.verified_uid
            || credentials.environment.demo
                != (configuration.account_key.environment == Environment::Demo)
        {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_IDENTITY_MISMATCH".into(),
            ));
        }
        let observer = AccountBootstrapper::new(OkxRestClient::with_rate_budget(
            credentials.environment,
            credentials.observer.clone(),
            self.rate_budget.clone(),
        )?);
        let root = self
            .root
            .join("accounts")
            .join(key_hash(&configuration.account_key));
        let identity_lease = acquire_identity_lease(
            &crate::config::default_root().join("account-leases"),
            &configuration.account_key,
        )?;
        if initialize {
            let snapshot = observer.snapshot().await.map_err(|_| {
                AgentError::FinancialCaptureTask("ACCOUNT_SOURCE_UNAVAILABLE".into())
            })?;
            let algos = observer
                .pending_derivative_risk_algos()
                .await
                .map_err(|_| {
                    AgentError::FinancialCaptureTask("ACCOUNT_ALGO_SOURCE_UNAVAILABLE".into())
                })?;
            if snapshot.account_uid_fingerprint != uid
                || !snapshot.pending_orders.is_empty()
                || snapshot
                    .positions
                    .iter()
                    .any(|p| Decimal::from_str(&p.position).map_or(true, |q| q != Decimal::ZERO))
                || !algos.complete_within_bound
                || algos.rows != 0
            {
                return Err(AgentError::FinancialCaptureTask("ACCOUNT_NOT_FLAT".into()));
            }
            let identity = SqliteAccountIdentity {
                venue: LedgerVenue::Okx,
                environment: if credentials.environment.demo {
                    LedgerEnvironment::Demo
                } else {
                    LedgerEnvironment::Production
                },
                account_id: configuration.alias.clone(),
                uid_fingerprint: uid.clone(),
                bound_at_ms: now_ms(),
            };
            ExecutionLedgerStore::initialize_fresh_account(&root, &identity)?;
        }
        let execution = Arc::new(ExecutionRuntime::new_registered_account(
            &root,
            credentials.environment,
            credentials.executor,
            &uid,
            now_ms(),
            self.rate_budget.clone(),
        )?);
        let (coordinator, private_ws) = PrivateWsCoordinator::new_with_rate_budget(
            credentials.environment,
            credentials.observer,
            self.rate_budget.clone(),
        );
        let (shutdown, receiver) = watch::channel(false);
        let resources = Arc::new(AccountResources {
            configuration,
            observer,
            private_ws,
            execution,
            mutation_capacity: Semaphore::new(128),
            source_shutdown: shutdown,
            verified_source_generation: AtomicU64::new(0),
            _identity_lease: identity_lease,
            source_tasks: Mutex::new(Vec::new()),
        });
        start_sources(&resources, coordinator, receiver);
        Ok(resources)
    }

    pub async fn shutdown(&self) {
        let resources: Vec<_> = self.routes.read().await.values().cloned().collect();
        for account in &resources {
            let _ = account.source_shutdown.send(true);
        }
        for account in resources {
            let tasks = std::mem::take(
                &mut *account
                    .source_tasks
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
            );
            for mut task in tasks {
                if tokio::time::timeout(std::time::Duration::from_secs(10), &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                }
            }
        }
    }
}

async fn verify_credentials(
    credentials: &NamedAccountCredentials,
    budget: RateBudget,
) -> AgentResult<String> {
    let observer = AccountApi::new(OkxRestClient::with_rate_budget(
        credentials.environment,
        credentials.observer.clone(),
        budget.clone(),
    )?);
    let executor = AccountApi::new(OkxRestClient::with_rate_budget(
        credentials.environment,
        credentials.executor.clone(),
        budget,
    )?);
    let (observer, executor) = tokio::try_join!(observer.config(), executor.config())?;
    let result = if credentials.environment.demo {
        crate::execution_preflight::evaluate_demo_executor_preflight(
            credentials.environment,
            &observer,
            &executor,
        )
    } else {
        crate::execution_preflight::evaluate_executor_preflight(
            credentials.environment,
            &observer,
            &executor,
        )
    };
    if !result.accepted {
        return Err(AgentError::FinancialCaptureTask(
            "ACCOUNT_CREDENTIAL_IDENTITY_OR_PERMISSIONS_INVALID".into(),
        ));
    }
    Ok(result.account_uid_fingerprint)
}

fn acquire_identity_lease(root: &Path, key: &AccountKey) -> AgentResult<File> {
    fs::create_dir_all(root)?;
    let lease = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(format!("{}.lock", key_hash(key))))?;
    lease
        .try_lock()
        .map_err(|_| AgentError::FinancialCaptureTask("ACCOUNT_WRITER_ALREADY_ACTIVE".into()))?;
    Ok(lease)
}

impl AccountResources {
    pub async fn source_identity_ready(&self) -> bool {
        let status = self.private_ws.status().await;
        status.connection_generation > 0
            && status.logged_in
            && self.verified_source_generation.load(Ordering::Acquire)
                == status.connection_generation
    }
}

fn start_sources(
    resources: &Arc<AccountResources>,
    coordinator: PrivateWsCoordinator,
    mut shutdown: watch::Receiver<bool>,
) {
    let ws_shutdown = shutdown.clone();
    let ws = tokio::spawn(async move {
        if coordinator.run(ws_shutdown).await.is_err() {
            eprintln!("account private source interrupted");
        }
    });
    let weak = Arc::downgrade(resources);
    let finance = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _=shutdown.changed()=>break,_=interval.tick()=>{
             let Some(account)=weak.upgrade()else{break};
             let generation=account.private_ws.status().await.connection_generation;
             if generation==0 {continue;}
             if account.verified_source_generation.load(Ordering::Acquire)!=generation {
                let Ok(snapshot)=account.observer.snapshot().await else{continue;};
                if snapshot.account_uid_fingerprint!=account.configuration.account_key.verified_uid
                    ||!account.execution.verified_uid_matches(&snapshot.account_uid_fingerprint).await.unwrap_or(false) {
                    eprintln!("account source identity verification failed");continue;
                }
                if account.private_ws.status().await.connection_generation!=generation {continue;}
                account.verified_source_generation.store(generation,Ordering::Release);
             }
             if crate::runtime::capture_one_native_financial_source(&account.observer,&account.private_ws,&account.execution).await.is_err(){
                eprintln!("account financial source awaiting coherent readback");
             }
            }}
        }
    });
    resources
        .source_tasks
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend([ws, finance]);
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_root(label: &str) -> PathBuf {
        let mut nonce = [0u8; 8];
        getrandom::fill(&mut nonce).unwrap();
        std::env::temp_dir().join(format!("okx-398-{label}-{:x}", u64::from_le_bytes(nonce)))
    }
    fn config(uid: char, alias: &str) -> VerifiedAccountConfig {
        VerifiedAccountConfig {
            account_key: AccountKey {
                venue: "okx".into(),
                environment: Environment::Real,
                verified_uid: uid.to_string().repeat(64),
            },
            alias: alias.into(),
            credential_ref: format!("credential_{alias}"),
            connect_operation_id: format!("connect_{alias}_001"),
        }
    }
    fn registry(root: &Path) -> AccountRuntimeRegistry {
        initialize_installation(root, "installation_test").unwrap();
        AccountRuntimeRegistry {
            root: root.into(),
            installation_id: "installation_test".into(),
            routes: RwLock::new(BTreeMap::new()),
            configurations: RwLock::new(BTreeMap::new()),
            failures: RwLock::new(BTreeMap::new()),
            catalog_write: tokio::sync::Mutex::new(()),
            rate_budget: RateBudget::new(),
        }
    }
    fn resources(root: &Path, configuration: VerifiedAccountConfig) -> Arc<AccountResources> {
        let environment = okx_api::OkxEnvironment::new(okx_api::Region::Global, false);
        let root_account = root
            .join("accounts")
            .join(key_hash(&configuration.account_key));
        let identity = SqliteAccountIdentity {
            venue: LedgerVenue::Okx,
            environment: LedgerEnvironment::Production,
            account_id: configuration.alias.clone(),
            uid_fingerprint: configuration.account_key.verified_uid.clone(),
            bound_at_ms: 1,
        };
        ExecutionLedgerStore::initialize_fresh_account(&root_account, &identity).unwrap();
        let credentials =
            okx_api::Credentials::new("fixture-key".into(), "fixture-secret".into(), "fixture-pass".into()).unwrap();
        let observer = AccountBootstrapper::new(
            OkxRestClient::with_rate_budget(environment, credentials.clone(), RateBudget::new())
                .unwrap(),
        );
        let (_, private_ws) = PrivateWsCoordinator::new_with_rate_budget(
            environment,
            credentials.clone(),
            RateBudget::new(),
        );
        let execution = Arc::new(
            ExecutionRuntime::new_registered_account(
                &root_account,
                environment,
                credentials,
                &configuration.account_key.verified_uid,
                2,
                RateBudget::new(),
            )
            .unwrap(),
        );
        let identity_lease =
            acquire_identity_lease(&root.join("leases"), &configuration.account_key).unwrap();
        let (source_shutdown, _) = watch::channel(false);
        Arc::new(AccountResources {
            configuration,
            observer,
            private_ws,
            execution,
            mutation_capacity: Semaphore::new(128),
            source_shutdown,
            verified_source_generation: AtomicU64::new(0),
            _identity_lease: identity_lease,
            source_tasks: Mutex::new(Vec::new()),
        })
    }
    #[tokio::test]
    async fn two_accounts_route_to_independent_native_owners() {
        let root = test_root("routing");
        let registry = registry(&root);
        let a = resources(&root, config('a', "alpha"));
        let b = resources(&root, config('b', "bravo"));
        registry.publish(a.clone()).await.unwrap();
        registry.publish(b.clone()).await.unwrap();
        assert_eq!(registry.list().await.len(), 2);
        assert!(Arc::ptr_eq(&registry.resolve("alpha").await.unwrap(), &a));
        assert!(Arc::ptr_eq(
            &registry
                .resolve(&b.configuration.account_key.wire())
                .await
                .unwrap(),
            &b
        ));
        assert!(registry.resolve("unknown").await.is_none());
        assert_ne!(
            a.execution.native_account_key().uid_fingerprint,
            b.execution.native_account_key().uid_fingerprint
        );
        drop(registry);
        drop(a);
        drop(b);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn slow_account_a_does_not_block_b() {
        let root = test_root("independent");
        let registry = registry(&root);
        let a = resources(&root, config('a', "alpha"));
        let b = resources(&root, config('b', "bravo"));
        registry.publish(a.clone()).await.unwrap();
        registry.publish(b.clone()).await.unwrap();
        let _all_a = a.mutation_capacity.acquire_many(128).await.unwrap();
        assert!(a.mutation_capacity.try_acquire().is_err());
        assert!(b.mutation_capacity.try_acquire().is_ok());
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                registry.resolve("bravo")
            )
            .await
            .unwrap()
            .is_some()
        );
        drop(_all_a);
        drop(registry);
        drop(a);
        drop(b);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn duplicate_writer_lease_rejected_across_aliases() {
        let root = test_root("lease");
        let a = config('a', "alpha");
        let b = config('a', "bravo");
        let lease = acquire_identity_lease(&root, &a.account_key).unwrap();
        assert!(acquire_identity_lease(&root, &b.account_key).is_err());
        drop(lease);
        assert!(acquire_identity_lease(&root, &b.account_key).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn restart_missing_account_database_never_initializes() {
        let root = test_root("missing");
        let registry = registry(&root);
        let configuration = config('a', "alpha");
        let bytes = serde_json::to_vec(&configuration).unwrap();
        publish_new(
            &root
                .join(CATALOG_DIR)
                .join(format!("{}.json", key_hash(&configuration.account_key))),
            &bytes,
        )
        .unwrap();
        drop(registry);
        let reopened = AccountRuntimeRegistry::open(&root, RateBudget::new())
            .await
            .unwrap();
        assert_eq!(reopened.list().await, vec![configuration.clone()]);
        assert!(reopened.resolve("alpha").await.is_none());
        assert_eq!(
            reopened
                .connect(
                    &configuration.credential_ref,
                    "alpha",
                    &configuration.connect_operation_id
                )
                .await
                .unwrap(),
            configuration
        );
        assert!(!root.join("accounts").exists());
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn initialize_refuses_to_replace_installation() {
        let root = test_root("installation");
        initialize_installation(&root, "installation_first").unwrap();
        assert!(initialize_installation(&root, "installation_other").is_err());
        let installation: Installation =
            serde_json::from_slice(&std::fs::read(root.join(INSTALLATION_FILE)).unwrap()).unwrap();
        assert_eq!(installation.installation_id, "installation_first");
        std::fs::remove_dir_all(root).unwrap();
    }
}
