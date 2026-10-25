//! Verified account resources in the existing Engine. Catalog writes are
//! immutable, atomic publications; access/limits remain exclusively in governance.
use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    execution_runtime::ExecutionRuntime,
    okx_credentials::{NamedAccountCredentials, load_native_account_credentials},
};
use okx_api::{AccountApi, OkxRestClient, RateBudget};
use okx_execution::{AccountMeta, AccountStoreHandle, OpeningSnapshot};
use okx_protocol::{
    account_catalog_proto,
    accounts::{AccountKey, Environment, VerifiedAccountConfig, valid_reference},
};
use okx_runtime::{PrivateWsCoordinator, PrivateWsHandle};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
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

#[derive(Clone, Default, serde::Serialize)]
pub struct SourceCaptureHealth {
    pub last_attempt_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub last_failure_ms: Option<i64>,
    pub reason_code: Option<String>,
}

fn source_capture_reason(error: &AgentError) -> String {
    match error {
        AgentError::FinancialCaptureTask(code)
            if !code.is_empty()
                && code.len() <= 96
                && code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') =>
        {
            code.clone()
        }
        AgentError::Store(okx_execution::StoreError::Invalid(code))
            if !code.is_empty()
                && code.len() <= 96
                && code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') =>
        {
            (*code).into()
        }
        AgentError::Okx(_) => "EXCHANGE_SOURCE_UNAVAILABLE".into(),
        AgentError::Store(_) => "ACCOUNT_SOURCE_SQL_FAILURE".into(),
        _ => "SOURCE_CAPTURE_FAILED".into(),
    }
}

pub struct AccountResources {
    pub configuration: VerifiedAccountConfig,
    pub observer: AccountBootstrapper,
    pub private_ws: PrivateWsHandle,
    pub execution: Arc<ExecutionRuntime>,
    pub latest_snapshot: RwLock<Option<okx_observation::AccountSnapshot>>,
    pub source_capture_health: RwLock<SourceCaptureHealth>,
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
pub(crate) fn key_hash(key: &AccountKey) -> String {
    format!("{:x}", Sha256::digest(key.wire().as_bytes()))
}

/// Create an installation once while the Engine is stopped. Normal startup
/// calls open(), which never creates a catalog or account database.
pub fn initialize_installation(root: &Path, installation_id: &str) -> AgentResult<()> {
    if !okx_protocol::accounts::valid_installation(installation_id) {
        return Err(invalid());
    }
    account_catalog_proto::initialize(root, installation_id)?;
    Ok(())
}

impl AccountRuntimeRegistry {
    pub async fn open(root: &Path, rate_budget: RateBudget) -> AgentResult<Self> {
        let catalog = account_catalog_proto::promote_existing_once(root)?;
        let configs = catalog.accounts.clone();
        let registry = Self {
            root: root.into(),
            installation_id: catalog.installation_id,
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

    /// Re-open unavailable contexts without changing configuration or storage.
    /// An unreachable account never holds another account's mutation owner.
    pub async fn recover_unavailable(&self) {
        use futures_util::{StreamExt, stream};
        let unavailable = self
            .failures
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let configs = self
            .configurations
            .read()
            .await
            .values()
            .filter(|c| unavailable.contains(&c.account_key.wire()))
            .cloned()
            .collect::<Vec<_>>();
        let mut opened = stream::iter(configs.into_iter().map(|config| async move {
            let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                let credentials = load_native_account_credentials(&config.credential_ref)?;
                self.open_resources(config.clone(), credentials, false)
                    .await
            })
            .await;
            (config, result)
        }))
        .buffer_unordered(4);
        while let Some((config, result)) = opened.next().await {
            if let Ok(Ok(resources)) = result {
                self.routes
                    .write()
                    .await
                    .insert(config.account_key.wire(), resources);
                self.failures
                    .write()
                    .await
                    .remove(&config.account_key.wire());
            }
        }
    }

    pub async fn connect(
        &self,
        credential_ref: &str,
        alias: &str,
        operation_id: &str,
        request_digest: &str,
    ) -> AgentResult<VerifiedAccountConfig> {
        if !valid_reference(credential_ref)
            || request_digest.len() != 64
            || !request_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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
            if existing.alias != alias
                || existing.credential_ref != credential_ref
                || existing.configuration_digest != request_digest
            {
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
            configuration_digest: request_digest.into(),
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
        account_catalog_proto::append(&self.root, configuration)?;
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
        fs::create_dir_all(&root)?;
        let database = root.join("account.sqlite");
        let store = if initialize {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            let opening = loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(AgentError::FinancialCaptureTask(
                        "INITIAL_SNAPSHOT_UNSTABLE".into(),
                    ));
                }
                let first = observer.snapshot().await.map_err(|_| {
                    AgentError::FinancialCaptureTask("ACCOUNT_SOURCE_UNAVAILABLE".into())
                })?;
                let algos = observer
                    .pending_derivative_risk_algos()
                    .await
                    .map_err(|_| {
                        AgentError::FinancialCaptureTask("ACCOUNT_ALGO_SOURCE_UNAVAILABLE".into())
                    })?;
                let last = observer.snapshot().await.map_err(|_| {
                    AgentError::FinancialCaptureTask("ACCOUNT_SOURCE_UNAVAILABLE".into())
                })?;
                if first.account_uid_fingerprint != uid || last.account_uid_fingerprint != uid {
                    return Err(AgentError::FinancialCaptureTask(
                        "ACCOUNT_IDENTITY_MISMATCH".into(),
                    ));
                }
                if !first
                    .balance
                    .same_values_ignoring_update_times(&last.balance)
                    || first.positions != last.positions
                    || first.pending_orders != last.pending_orders
                {
                    continue;
                }
                if !algos.complete_within_bound {
                    return Err(AgentError::FinancialCaptureTask(
                        "ACCOUNT_ALGO_COVERAGE_INCOMPLETE".into(),
                    ));
                }
                let liabilities =
                    last.balance
                        .details
                        .iter()
                        .try_fold(Decimal::ZERO, |sum, d| {
                            // OKX documents liab as not applicable to Futures mode (2).
                            // Other account modes require an explicit observed debt field.
                            let value = match d.liabilities.as_deref() {
                                Some(v) => Decimal::from_str(v).map_err(|_| invalid())?,
                                None if last.account_level == "2" => Decimal::ZERO,
                                None => return Err(invalid()),
                            };
                            sum.checked_add(value).ok_or_else(invalid)
                        })?;
                let positions = last
                    .positions
                    .iter()
                    .filter(|p| Decimal::from_str(&p.position) != Ok(Decimal::ZERO))
                    .count();
                let balances = last
                    .balance
                    .details
                    .iter()
                    .map(|d| {
                        Ok((
                            d.currency.clone(),
                            Decimal::from_str(&d.equity).map_err(|_| invalid())?,
                        ))
                    })
                    .collect::<AgentResult<Vec<_>>>()?;
                let observed_at = chrono::DateTime::parse_from_rfc3339(&last.source_received_at)
                    .map_err(|_| invalid())?
                    .timestamp_millis();
                let source_ref = format!("{:x}", Sha256::digest(serde_json::to_vec(&last)?));
                break OpeningSnapshot {
                    opening_equity_usd: Decimal::from_str(&last.balance.total_equity_usd)
                        .map_err(|_| invalid())?,
                    source_ref,
                    observed_at,
                    positions,
                    orders: last.pending_orders.len(),
                    conditional_orders: algos.rows,
                    liabilities,
                    balances,
                };
            };
            let meta = AccountMeta {
                account_key: configuration.account_key.wire(),
                installation_id: self.installation_id.clone(),
                initialized_at: opening.observed_at,
                opening_source: opening.source_ref.clone(),
                initialize_operation_id: configuration.connect_operation_id.clone(),
                release_id: build_release().into(),
            };
            AccountStoreHandle::initialize_flat(database, meta, opening).await?
        } else {
            AccountStoreHandle::open_existing(
                database,
                configuration.account_key.wire(),
                self.installation_id.clone(),
            )
            .await?
        };
        store
            .call(
                std::time::Instant::now() + std::time::Duration::from_secs(2),
                |s| s.recover_posting(chrono::Utc::now().timestamp_millis()),
            )
            .await?;
        let source_generation_seed = store
            .call(
                std::time::Instant::now() + std::time::Duration::from_secs(2),
                |s| s.source_generation_base(),
            )
            .await?;
        let execution = Arc::new(ExecutionRuntime::new(
            store,
            credentials.environment,
            credentials.executor,
            uid.clone(),
            self.rate_budget.clone(),
        )?);
        let (coordinator, private_ws) = PrivateWsCoordinator::new_with_rate_budget_seed(
            credentials.environment,
            credentials.observer,
            self.rate_budget.clone(),
            source_generation_seed,
        );
        let (shutdown, receiver) = watch::channel(false);
        let resources = Arc::new(AccountResources {
            latest_snapshot: RwLock::new(None),
            source_capture_health: RwLock::new(SourceCaptureHealth::default()),
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

pub(crate) async fn verify_credentials(
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

pub(crate) fn acquire_identity_lease(root: &Path, key: &AccountKey) -> AgentResult<File> {
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

    async fn record_source_failure(&self, reason_code: &str) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut health = self.source_capture_health.write().await;
        health.last_attempt_ms = Some(now);
        health.last_failure_ms = Some(now);
        health.reason_code = Some(reason_code.to_owned());
    }

    async fn record_source_success(&self) {
        let now = chrono::Utc::now().timestamp_millis();
        let mut health = self.source_capture_health.write().await;
        health.last_attempt_ms = Some(now);
        health.last_success_ms = Some(now);
        health.reason_code = None;
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
             if generation==0 {
                 account.record_source_failure("PRIVATE_WS_NOT_READY").await;
                 continue;
             }
             if account.verified_source_generation.load(Ordering::Acquire)!=generation {
                let snapshot=match account.observer.snapshot().await {
                    Ok(snapshot)=>snapshot,
                    Err(_)=>{
                        account.record_source_failure("ACCOUNT_SOURCE_UNAVAILABLE").await;
                        continue;
                    }
                };
                if snapshot.account_uid_fingerprint!=account.configuration.account_key.verified_uid
                    ||!account.execution.verified_uid_matches(&snapshot.account_uid_fingerprint).await.unwrap_or(false) {
                    account.record_source_failure("ACCOUNT_IDENTITY_MISMATCH").await;
                    eprintln!("account source identity verification failed");continue;
                }
                if account.private_ws.status().await.connection_generation!=generation {
                    account.record_source_failure("PRIVATE_WS_GENERATION_CHANGED").await;
                    continue;
                }
                *account.latest_snapshot.write().await=Some(snapshot);
                account.verified_source_generation.store(generation,Ordering::Release);
             }
             match crate::runtime::capture_account_sources(&account.observer,&account.private_ws,&account.execution).await {
                Ok(snapshot)=>{
                    *account.latest_snapshot.write().await=Some(snapshot);
                    account.record_source_success().await;
                }
                Err(error)=>{
                    account.record_source_failure(&source_capture_reason(&error)).await;
                    eprintln!("account financial source awaiting coherent readback");
                }
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

pub fn build_release() -> &'static str {
    match option_env!("OKX_BUILD_SOURCE_TREE") {
        Some(v) => v,
        None => "UNVERIFIED_LOCAL_BUILD",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        std::env::temp_dir().join(format!(
            "okx-clean-accounts-{:032x}",
            u128::from_le_bytes(nonce)
        ))
    }
    fn key() -> AccountKey {
        AccountKey {
            venue: "okx".into(),
            environment: Environment::Real,
            verified_uid: "a".repeat(64),
        }
    }
    #[tokio::test]
    async fn initialize_without_any_legacy_files() {
        let root = root();
        initialize_installation(&root, "installation-test").unwrap();
        let registry = AccountRuntimeRegistry::open(&root, RateBudget::new())
            .await
            .unwrap();
        assert!(registry.list().await.is_empty());
        assert_eq!(registry.installation_id(), "installation-test");
        assert!(!root.join("accounts").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn legacy_stop_claims_and_epochs_are_never_read() {
        let root = root();
        initialize_installation(&root, "installation-test").unwrap();
        for name in [
            "execution-ledger.json",
            "production-policy.json",
            "strategy-epochs.json",
            "claims.json",
            "STOP",
        ] {
            std::fs::create_dir(root.join(name)).unwrap();
        }
        let registry = AccountRuntimeRegistry::open(&root, RateBudget::new())
            .await
            .unwrap();
        assert!(registry.list().await.is_empty());
        assert!(registry.failures().await.is_empty());
        assert!(!root.join("accounts").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn source_failure_codes_are_bounded_and_do_not_expose_raw_errors() {
        assert_eq!(
            source_capture_reason(&AgentError::FinancialCaptureTask(
                "PRIVATE_SOURCE_NOT_READY".into()
            )),
            "PRIVATE_SOURCE_NOT_READY"
        );
        assert_eq!(
            source_capture_reason(&AgentError::FinancialCaptureTask(
                "API secret leaked: credential".into()
            )),
            "SOURCE_CAPTURE_FAILED"
        );
        assert_eq!(
            source_capture_reason(&AgentError::Store(okx_execution::StoreError::Invalid(
                "SOURCE_CURSOR_REGRESSION"
            ))),
            "SOURCE_CURSOR_REGRESSION"
        );
    }
    #[test]
    fn duplicate_writer_lease_rejected_across_aliases() {
        let root = root();
        let account = key();
        let first = acquire_identity_lease(&root, &account).unwrap();
        assert!(acquire_identity_lease(&root, &account).is_err());
        drop(first);
        assert!(acquire_identity_lease(&root, &account).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn initialize_refuses_to_replace_installation() {
        let root = root();
        initialize_installation(&root, "installation-first").unwrap();
        assert!(initialize_installation(&root, "installation-second").is_err());
        let value = account_catalog_proto::load(&root).unwrap();
        assert_eq!(value.installation_id, "installation-first");
        std::fs::remove_dir_all(root).unwrap();
    }
}
