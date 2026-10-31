//! Verified account resources in the existing Engine. Catalog writes are
//! immutable, atomic publications; access/limits remain exclusively in governance.
use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    execution_runtime::ExecutionRuntime,
    okx_credentials::{NamedAccountCredentials, load_native_account_credentials},
};
use okx_api::{AccountApi, Credentials, OkxEnvironment, OkxRestClient, RateBudget};
use okx_execution::{AccountMeta, AccountStoreHandle, OpeningSnapshot};
use okx_protocol::{
    account_catalog_proto,
    accounts::{AccountKey, Environment, VerifiedAccountConfig, valid_reference},
};
use okx_runtime::{LiveWsConnector, PrivateWsCoordinator, PrivateWsHandle, WsConnector};
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
    time::Instant,
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
    /// Duration of the last completed capture, not a substitute for venue freshness.
    pub last_capture_duration_ms: Option<u64>,
    /// Source's ORIGINAL observation age when verified data was published.
    pub last_snapshot_age_at_publish_ms: Option<i64>,
    /// SQL cursor read and authenticated balance/positions/orders.
    pub last_observer_and_cursors_ms: Option<u64>,
    /// All five source-history fetches and durable cursor ingests.
    pub last_history_and_ingest_ms: Option<u64>,
    /// Account NAV, strategy attribution, and coverage reconciliation.
    pub last_nav_reconciliation_ms: Option<u64>,
    /// Queue wait for the same completed NAV job; excludes SQL execution.
    pub last_nav_sql_queue_wait_ms: Option<u64>,
    /// Actual execution of the same completed NAV job; excludes queue wait.
    pub last_nav_sql_owner_ms: Option<u64>,
    /// Account-local volatile 30-minute evidence; never written to SQLite.
    #[serde(skip)]
    pub(crate) finance_window: crate::finance_window::FinanceWindow,
}

fn snapshot_age_at_publish_ms(source_received_at: &str, published_at_ms: i64) -> Option<i64> {
    let observed_ms = chrono::DateTime::parse_from_rfc3339(source_received_at)
        .ok()?
        .timestamp_millis();
    published_at_ms.checked_sub(observed_ms)
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
    pub(crate) finance_trace: Arc<crate::finance_trace::FinanceTrace>,
    next_finance_cycle: AtomicU64,
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

/// One verified account bootstrap, regardless of who delivers HTTP/WS bytes.
pub(crate) struct PreparedAccountClients {
    environment: OkxEnvironment,
    observer: OkxRestClient,
    executor: OkxRestClient,
    observer_login: Credentials,
    ws_connector: Arc<dyn WsConnector>,
}

impl PreparedAccountClients {
    fn live(credentials: NamedAccountCredentials, budget: RateBudget) -> AgentResult<Self> {
        let observer = OkxRestClient::with_rate_budget(
            credentials.environment,
            credentials.observer.clone(),
            budget.clone(),
        )?;
        let executor =
            OkxRestClient::with_rate_budget(credentials.environment, credentials.executor, budget)?;
        Ok(Self {
            environment: credentials.environment,
            observer,
            executor,
            observer_login: credentials.observer,
            ws_connector: Arc::new(LiveWsConnector),
        })
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
        let catalog = account_catalog_proto::promote_existing_once(root, |catalog| {
            for account in &catalog.accounts {
                let path = root
                    .join("accounts")
                    .join(key_hash(&account.account_key))
                    .join("account.sqlite");
                let connection = rusqlite::Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(|_| std::io::Error::other("ACCOUNT_SQL_IDENTITY_UNAVAILABLE"))?;
                let (key, installation_id, version):(String,String,i64) = connection.query_row(
                    "SELECT account_key,installation_id,schema_version FROM account_meta WHERE singleton=1",
                    [],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
                ).map_err(|_|std::io::Error::other("ACCOUNT_SQL_IDENTITY_UNAVAILABLE"))?;
                if key != account.account_key.wire()
                    || installation_id != catalog.installation_id
                    || version != 2
                {
                    return Err(std::io::Error::other("ACCOUNT_SQL_IDENTITY_MISMATCH"));
                }
            }
            Ok(())
        })?;
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
        let clients = PreparedAccountClients::live(credentials, self.rate_budget.clone())?;
        let leases = crate::config::default_root().join("account-leases");
        self.open_resources_with_clients(configuration, clients, initialize, &leases)
            .await
    }

    async fn open_resources_with_clients(
        &self,
        configuration: VerifiedAccountConfig,
        clients: PreparedAccountClients,
        initialize: bool,
        lease_root: &Path,
    ) -> AgentResult<Arc<AccountResources>> {
        let uid =
            verify_client_pair(clients.environment, &clients.observer, &clients.executor).await?;
        if uid != configuration.account_key.verified_uid
            || clients.environment.demo
                != (configuration.account_key.environment == Environment::Demo)
        {
            return Err(AgentError::FinancialCaptureTask(
                "ACCOUNT_IDENTITY_MISMATCH".into(),
            ));
        }
        let observer = AccountBootstrapper::new(clients.observer);
        let root = self
            .root
            .join("accounts")
            .join(key_hash(&configuration.account_key));
        let identity_lease = acquire_identity_lease(lease_root, &configuration.account_key)?;
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
        let execution = Arc::new(ExecutionRuntime::from_client(
            store,
            clients.executor,
            clients.environment,
            uid.clone(),
        ));
        let (coordinator, private_ws) = PrivateWsCoordinator::new_with_connector_seed(
            clients.environment,
            clients.observer_login,
            self.rate_budget.clone(),
            source_generation_seed,
            clients.ws_connector,
        );
        let (shutdown, receiver) = watch::channel(false);
        let resources = Arc::new(AccountResources {
            latest_snapshot: RwLock::new(None),
            source_capture_health: RwLock::new(SourceCaptureHealth::default()),
            finance_trace: Arc::new(crate::finance_trace::FinanceTrace::default()),
            next_finance_cycle: AtomicU64::new(0),
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

    /// Test-only composition. No alternate bootstrap, bypass or installed route.
    /// All identity, permission, SQL and capture checks above still execute.
    #[cfg(test)]
    pub(crate) async fn connect_prepared(
        &self,
        configuration: VerifiedAccountConfig,
        clients: PreparedAccountClients,
    ) -> AgentResult<()> {
        let leases = self.root.join("account-leases");
        let resources = self
            .open_resources_with_clients(configuration, clients, true, &leases)
            .await?;
        self.publish(resources).await
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
    let observer = OkxRestClient::with_rate_budget(
        credentials.environment,
        credentials.observer.clone(),
        budget.clone(),
    )?;
    let executor = OkxRestClient::with_rate_budget(
        credentials.environment,
        credentials.executor.clone(),
        budget,
    )?;
    verify_client_pair(credentials.environment, &observer, &executor).await
}

async fn verify_client_pair(
    environment: OkxEnvironment,
    observer: &OkxRestClient,
    executor: &OkxRestClient,
) -> AgentResult<String> {
    let observer = AccountApi::new(observer.clone());
    let executor = AccountApi::new(executor.clone());
    let (observer, executor) = tokio::try_join!(observer.config(), executor.config())?;
    let result = if environment.demo {
        crate::execution_preflight::evaluate_demo_executor_preflight(
            environment,
            &observer,
            &executor,
        )
    } else {
        crate::execution_preflight::evaluate_executor_preflight(environment, &observer, &executor)
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

    async fn record_source_failure(
        &self,
        reason_code: &str,
        started: Instant,
        generation: u64,
        cycle_id: u64,
    ) {
        let now = chrono::Utc::now().timestamp_millis();
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.finance_trace
            .finish(cycle_id, now, elapsed_ms, Some(reason_code.to_owned()));
        let mut health = self.source_capture_health.write().await;
        health.last_attempt_ms = Some(now);
        health.last_failure_ms = Some(now);
        health.reason_code = Some(reason_code.to_owned());
        // Failed captures count in the same bounded window as successful ones.
        health
            .finance_window
            .record(crate::finance_window::FinanceAttempt {
                at_ms: now,
                source_generation: generation,
                elapsed_ms,
                nav_sql_owner_ms: None,
                nav_sql_queue_ms: None,
                snapshot_age_ms: None,
                failure_code: Some(reason_code.to_owned()),
            });
    }

    async fn record_source_success(
        &self,
        source_received_at: &str,
        capture_started: Instant,
        phases: crate::source_capture::CapturePhaseTimings,
        generation: u64,
        cycle_id: u64,
    ) {
        let now = chrono::Utc::now().timestamp_millis();
        let duration_ms = u64::try_from(capture_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let snapshot_age_ms = snapshot_age_at_publish_ms(source_received_at, now);
        self.finance_trace.finish(cycle_id, now, duration_ms, None);
        let mut health = self.source_capture_health.write().await;
        health.last_attempt_ms = Some(now);
        health.last_success_ms = Some(now);
        health.reason_code = None;
        health.last_capture_duration_ms = Some(duration_ms);
        health.last_snapshot_age_at_publish_ms = snapshot_age_ms;
        health.last_observer_and_cursors_ms = Some(phases.observer_and_cursors_ms);
        health.last_history_and_ingest_ms = Some(phases.history_and_ingest_ms);
        health.last_nav_reconciliation_ms = Some(phases.nav_reconciliation_ms);
        health.last_nav_sql_queue_wait_ms = Some(phases.nav_sql_queue_wait_ms);
        health.last_nav_sql_owner_ms = Some(phases.nav_sql_owner_ms);
        health
            .finance_window
            .record(crate::finance_window::FinanceAttempt {
                at_ms: now,
                source_generation: generation,
                elapsed_ms: duration_ms,
                nav_sql_owner_ms: Some(phases.nav_sql_owner_ms),
                nav_sql_queue_ms: Some(phases.nav_sql_queue_wait_ms),
                snapshot_age_ms,
                failure_code: None,
            });
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
             // The attempt starts at the tick, including identity recovery.
             let capture_started = Instant::now();
             let generation=account.private_ws.status().await.connection_generation;
             let cycle_id=account.next_finance_cycle.fetch_add(1,Ordering::Relaxed)+1;
             account.finance_trace.begin(cycle_id,chrono::Utc::now().timestamp_millis(),generation);
             if generation==0 {
                 account.record_source_failure("PRIVATE_WS_NOT_READY", capture_started, generation, cycle_id).await;
                 continue;
             }
             if account.verified_source_generation.load(Ordering::Acquire)!=generation {
                let snapshot=match account.observer.snapshot().await {
                    Ok(snapshot)=>snapshot,
                    Err(_)=>{
                        account.record_source_failure("ACCOUNT_SOURCE_UNAVAILABLE", capture_started, generation, cycle_id).await;
                        continue;
                    }
                };
                if snapshot.account_uid_fingerprint!=account.configuration.account_key.verified_uid
                    ||!account.execution.verified_uid_matches(&snapshot.account_uid_fingerprint).await.unwrap_or(false) {
                    account.record_source_failure("ACCOUNT_IDENTITY_MISMATCH", capture_started, generation, cycle_id).await;
                    eprintln!("account source identity verification failed");continue;
                }
                if account.private_ws.status().await.connection_generation!=generation {
                    account.record_source_failure("PRIVATE_WS_GENERATION_CHANGED", capture_started, generation, cycle_id).await;
                    continue;
                }
                *account.latest_snapshot.write().await=Some(snapshot);
                account.verified_source_generation.store(generation,Ordering::Release);
             }
             match crate::source_capture::capture_account_sources_traced(&account.observer,&account.private_ws,&account.execution,Some((Arc::clone(&account.finance_trace),cycle_id))).await {
                Ok(completed)=>{
                    let source_received_at = completed.snapshot.source_received_at.clone();
                    *account.latest_snapshot.write().await=Some(completed.snapshot);
                    account.record_source_success(
                        &source_received_at, capture_started, completed.phases, generation, cycle_id
                    ).await;
                }
                Err(error)=>{
                    account.record_source_failure(&source_capture_reason(&error), capture_started, generation, cycle_id).await;
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
pub(crate) mod recorded_support {
    use super::*;
    use okx_api::{
        HttpRequest, HttpSend, HttpStatusCode, HttpTransport, OkxError, RawHttpResponse,
    };
    use std::time::Instant;

    /// A raw external GET response, consumed once by the same production
    /// parser as a live HTTP response. Query expectations are an explicit
    /// subset: dynamic bounded capture windows are not generated by tests.
    pub(crate) struct RecordedGet {
        pub(crate) access_key: String,
        pub(crate) path: String,
        pub(crate) required_query: Vec<(String, String)>,
        pub(crate) raw_response: Vec<u8>,
    }

    /// One pre-recorded response for one exact signed executor POST. This is
    /// only a transport transcript, never an exchange/account state machine.
    pub(crate) struct RecordedPost {
        pub(crate) access_key: String,
        pub(crate) path: String,
        pub(crate) expected_body: serde_json::Value,
        pub(crate) raw_response: Vec<u8>,
    }

    #[derive(Default)]
    pub(crate) struct RecordedAccountHttp {
        pub(crate) requests: Mutex<Vec<String>>,
        pub(crate) outbound_posts: Mutex<Vec<(String, String, serde_json::Value)>>,
        posts: Mutex<Vec<RecordedPost>>,
        reads: Mutex<Vec<RecordedGet>>,
        strict_read_routes: Mutex<std::collections::BTreeSet<(String, String)>>,
    }
    impl RecordedAccountHttp {
        pub(crate) fn expect_get(&self, page: RecordedGet) {
            self.strict_read_routes
                .lock()
                .unwrap()
                .insert((page.access_key.clone(), page.path.clone()));
            self.reads.lock().unwrap().push(page);
        }

        pub(crate) fn unconsumed_reads(&self) -> usize {
            self.reads.lock().unwrap().len()
        }

        pub(crate) fn expect_post(&self, post: RecordedPost) {
            self.posts.lock().unwrap().push(post);
        }

        pub(crate) fn unconsumed_posts(&self) -> usize {
            self.posts.lock().unwrap().len()
        }
    }
    impl HttpTransport for RecordedAccountHttp {
        fn send(&self, request: HttpRequest, _: Instant) -> HttpSend<'_> {
            Box::pin(async move {
                if request.method() != "GET" && request.method() != "POST" {
                    return Err(OkxError::Response("TRANSCRIPT_METHOD_MISMATCH".into()));
                }
                let key = request
                    .headers()
                    .get("OK-ACCESS-KEY")
                    .and_then(|h| h.to_str().ok())
                    .ok_or_else(|| OkxError::Response("TRANSCRIPT_ACCOUNT_UNBOUND".into()))?;
                let (uid, permission) = match key {
                    "fixture-observer-a" => ("account-001", "read_only"),
                    "fixture-executor-a" => ("account-001", "read_only,trade"),
                    "fixture-observer-b" => ("account-002", "read_only"),
                    "fixture-executor-b" => ("account-002", "read_only,trade"),
                    _ => return Err(OkxError::Response("TRANSCRIPT_ACCOUNT_UNKNOWN".into())),
                };
                let path = request.url().path();
                if request.method() == "POST" {
                    // Count every possible outbound create, even when the
                    // transcript rejects it. Never silently replay a POST.
                    let bytes = request
                        .body()
                        .and_then(|body| body.as_bytes())
                        .ok_or_else(|| OkxError::Response("TRANSCRIPT_POST_BODY_MISSING".into()))?;
                    let observed: serde_json::Value = serde_json::from_slice(bytes)?;
                    self.outbound_posts.lock().unwrap().push((
                        key.to_owned(),
                        path.to_owned(),
                        observed.clone(),
                    ));
                    if !key.starts_with("fixture-executor-") {
                        return Err(OkxError::Response("TRANSCRIPT_POST_NOT_EXECUTOR".into()));
                    }
                    let mut expected = self.posts.lock().unwrap();
                    let index = expected
                        .iter()
                        .position(|item| item.access_key == key && item.path == path)
                        .ok_or_else(|| OkxError::Response("TRANSCRIPT_UNEXPECTED_POST".into()))?;
                    let post = &expected[index];
                    let mut body = post.expected_body.clone();
                    let client_id = observed
                        .get("clOrdId")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| OkxError::Response("TRANSCRIPT_CLIENT_ID_MISSING".into()))?;
                    if client_id.is_empty()
                        || client_id.len() > 32
                        || !client_id.bytes().all(|b| b.is_ascii_alphanumeric())
                    {
                        return Err(OkxError::Response("TRANSCRIPT_CLIENT_ID_INVALID".into()));
                    }
                    if body.get("clOrdId").and_then(serde_json::Value::as_str)
                        == Some("__REQUEST_CL_ORD_ID__")
                    {
                        body["clOrdId"] = client_id.into();
                    }
                    if body != observed {
                        return Err(OkxError::Response("TRANSCRIPT_POST_BODY_MISMATCH".into()));
                    }
                    let recorded = expected.remove(index);
                    drop(expected);
                    // Binding a dynamic client order ID to pre-recorded raw
                    // response bytes does not generate fills, fees or balances.
                    let response = String::from_utf8(recorded.raw_response)
                        .map_err(|_| OkxError::Response("TRANSCRIPT_RESPONSE_UTF8".into()))?
                        .replace("__REQUEST_CL_ORD_ID__", client_id);
                    return Ok(RawHttpResponse {
                        status: HttpStatusCode::OK,
                        headers: Default::default(),
                        body: response.into_bytes(),
                    });
                }
                // An explicitly scripted history route MUST NOT silently
                // fall back to the old empty baseline when pages run out.
                let guarded_route = (key.to_owned(), path.to_owned());
                let scripted = self
                    .strict_read_routes
                    .lock()
                    .unwrap()
                    .contains(&guarded_route);
                if scripted {
                    let mut reads = self.reads.lock().unwrap();
                    let index = reads
                        .iter()
                        .position(|page| {
                            page.access_key == key
                                && page.path == path
                                && page.required_query.iter().all(|(name, expected)| {
                                    request.url().query_pairs().any(|(k, v)| {
                                        k.as_ref() == name.as_str()
                                            && v.as_ref() == expected.as_str()
                                    })
                                })
                        })
                        .ok_or_else(|| {
                            OkxError::Response(
                                "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED".into(),
                            )
                        })?;
                    let page = reads.remove(index);
                    self.requests.lock().unwrap().push(format!(
                        "{key}:{path}?{}",
                        request.url().query().unwrap_or_default()
                    ));
                    return Ok(RawHttpResponse {
                        status: HttpStatusCode::OK,
                        headers: Default::default(),
                        body: page.raw_response,
                    });
                }
                let data = match path {
                    "/api/v5/account/config" => serde_json::json!([{
                        "uid": uid,
                        "mainUid": "main-account",
                        "acctLv": "2",
                        "posMode": "long_short_mode",
                        "type": "1",
                        "perm": permission,
                        "autoLoan": false,
                        "ip": "127.0.0.1"
                    }]),
                    "/api/v5/account/balance" => serde_json::json!([{
                        "totalEq": "100",
                        "adjEq": "100",
                        "uTime": "1793460000000",
                        "details": [{
                            "ccy": "USDT", "eq": "100",
                            "liab": "0", "availEq": "100",
                            "eqUsd": "100", "uTime": "1793460000000"
                        }]
                    }]),
                    "/api/v5/trade/fills-history" | "/api/v5/account/bills-archive" => {
                        serde_json::json!([])
                    }
                    "/api/v5/account/positions"
                    | "/api/v5/trade/orders-pending"
                    | "/api/v5/trade/orders-algo-pending" => {
                        if path == "/api/v5/trade/orders-algo-pending" {
                            let params = request.url().query_pairs().collect::<Vec<_>>();
                            if !params.iter().any(|(k, _)| k == "ordType")
                                || !params.iter().any(|(k, v)| {
                                    k == "instType" && (v == "SWAP" || v == "FUTURES")
                                })
                                || !params.iter().any(|(k, v)| k == "limit" && v == "100")
                            {
                                return Err(OkxError::Response(
                                    "TRANSCRIPT_ALGO_SCOPE_MISMATCH".into(),
                                ));
                            }
                        }
                        serde_json::json!([])
                    }
                    _ => {
                        return Err(OkxError::Response("TRANSCRIPT_UNEXPECTED_REQUEST".into()));
                    }
                };
                // A transcript has explicitly bounded reads, not an infinite
                // fake account endpoint. Concurrent polls can reorder safely.
                let request_key =
                    format!("{key}:{path}?{}", request.url().query().unwrap_or_default());
                let max_repeats = match path {
                    "/api/v5/account/config" => 10,
                    "/api/v5/account/balance"
                    | "/api/v5/account/positions"
                    | "/api/v5/trade/orders-pending" => 4,
                    "/api/v5/trade/orders-algo-pending" => 8,
                    "/api/v5/trade/fills-history" | "/api/v5/account/bills-archive" => 8,
                    _ => 0,
                };
                let mut observed = self.requests.lock().unwrap();
                if observed.iter().filter(|v| **v == request_key).count() >= max_repeats {
                    return Err(OkxError::Response("TRANSCRIPT_REPEAT_EXCEEDED".into()));
                }
                observed.push(request_key);
                drop(observed);
                let body = serde_json::to_vec(&serde_json::json!({
                    "code": "0", "msg": "", "data": data
                }))?;
                Ok(RawHttpResponse {
                    status: HttpStatusCode::OK,
                    headers: Default::default(),
                    body,
                })
            })
        }
    }

    /// Only raw recorded socket delivery; real PrivateWsCoordinator still
    /// parses, authenticates and establishes source generations.
    pub(crate) struct RecordedWsConnector {
        pub(crate) sent: Arc<Mutex<Vec<serde_json::Value>>>,
        /// Expected subscribe args and one-shot raw ACK/data frames. No
        /// snapshots or readied flags are supplied to the runtime directly.
        public_subscriptions: Arc<Mutex<Vec<(serde_json::Value, Vec<String>)>>>,
    }
    impl Default for RecordedWsConnector {
        fn default() -> Self {
            Self {
                sent: Arc::new(Mutex::new(Vec::new())),
                public_subscriptions: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }
    impl RecordedWsConnector {
        pub(crate) fn expect_public_subscription(
            &self,
            arg: serde_json::Value,
            frames: Vec<String>,
        ) {
            self.public_subscriptions.lock().unwrap().push((arg, frames));
        }

        pub(crate) fn unconsumed_public_subscriptions(&self) -> usize {
            self.public_subscriptions.lock().unwrap().len()
        }
    }
    struct RecordedWire {
        inbound: std::collections::VecDeque<tokio_tungstenite::tungstenite::Message>,
        sent: Arc<Mutex<Vec<serde_json::Value>>>,
        public_subscriptions: Option<Arc<Mutex<Vec<(serde_json::Value, Vec<String>)>>>>,
    }
    impl okx_runtime::WsWire for RecordedWire {
        fn send(
            &mut self,
            frame: tokio_tungstenite::tungstenite::Message,
        ) -> okx_runtime::WsCall<'_> {
            Box::pin(async move {
                if let tokio_tungstenite::tungstenite::Message::Text(data) = frame {
                    let outgoing: serde_json::Value =
                        serde_json::from_str(data.as_str()).unwrap_or(serde_json::Value::Null);
                    self.sent.lock().unwrap().push(outgoing.clone());
                    if let Some(script) = &self.public_subscriptions
                        && outgoing["op"] == "subscribe"
                    {
                        let Some(args) = outgoing["args"].as_array() else {
                            return Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed);
                        };
                        for arg in args {
                            let frames = {
                                let mut remaining = script.lock().unwrap();
                                let Some(index) =
                                    remaining.iter().position(|(expected, _)| expected == arg)
                                else {
                                    return Err(
                                        tokio_tungstenite::tungstenite::Error::ConnectionClosed,
                                    );
                                };
                                remaining.remove(index).1
                            };
                            self.inbound.extend(frames.into_iter().map(|raw| {
                                tokio_tungstenite::tungstenite::Message::Text(raw.into())
                            }));
                        }
                    }
                }
                Ok(())
            })
        }
        fn flush(&mut self) -> okx_runtime::WsCall<'_> {
            Box::pin(async { Ok(()) })
        }
        fn next(&mut self) -> okx_runtime::WsReceive<'_> {
            Box::pin(async move {
                if let Some(frame) = self.inbound.pop_front() {
                    Some(Ok(frame))
                } else {
                    std::future::pending().await
                }
            })
        }
        fn close(&mut self) -> okx_runtime::WsCall<'_> {
            Box::pin(async { Ok(()) })
        }
    }
    impl WsConnector for RecordedWsConnector {
        fn connect(&self, url: String) -> okx_runtime::WsConnect<'_> {
            Box::pin(async move {
                let public = url.ends_with("/public");
                let transcript: Vec<&str> = if public {
                    vec![]
                } else if url.ends_with("/business") {
                    vec![
                        r#"{"event":"login","code":"0","msg":"","connId":"recorded-business"}"#,
                        r#"{"event":"subscribe","arg":{"channel":"orders-algo","instType":"ANY"},"connId":"recorded-business"}"#,
                    ]
                } else if url.ends_with("/private") {
                    vec![
                        r#"{"event":"login","code":"0","msg":"","connId":"recorded-private"}"#,
                        r#"{"event":"subscribe","arg":{"channel":"account"},"connId":"recorded-private"}"#,
                        r#"{"event":"subscribe","arg":{"channel":"positions","instType":"ANY"},"connId":"recorded-private"}"#,
                        r#"{"event":"subscribe","arg":{"channel":"orders","instType":"ANY"},"connId":"recorded-private"}"#,
                        r#"{"arg":{"channel":"account"},"data":[{"totalEq":"100","uTime":"1793460000000"}]}"#,
                        r#"{"arg":{"channel":"positions","instType":"ANY"},"data":[]}"#,
                    ]
                } else {
                    return Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed);
                };
                let inbound = transcript
                    .into_iter()
                    .map(|raw| tokio_tungstenite::tungstenite::Message::Text(raw.into()))
                    .collect();
                Ok(Box::new(RecordedWire {
                    inbound,
                    sent: self.sent.clone(),
                    public_subscriptions: if public {
                        Some(self.public_subscriptions.clone())
                    } else {
                        None
                    },
                }) as Box<dyn okx_runtime::WsWire>)
            })
        }
    }
    pub(crate) struct OfflineSocket;
    impl WsConnector for OfflineSocket {
        fn connect(&self, _: String) -> okx_runtime::WsConnect<'_> {
            Box::pin(async { Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) })
        }
    }

    /// Exact recorded request delivery from both identity-bound API keys.
    /// Sharing this constructor prevents a separate test account owner.
    pub(crate) fn prepared(
        label: &str,
        feed: Arc<RecordedAccountHttp>,
    ) -> (VerifiedAccountConfig, PreparedAccountClients) {
        prepared_with_socket(label, feed, Arc::new(OfflineSocket))
    }

    pub(crate) fn prepared_with_socket(
        label: &str,
        feed: Arc<RecordedAccountHttp>,
        ws_connector: Arc<dyn WsConnector>,
    ) -> (VerifiedAccountConfig, PreparedAccountClients) {
        let uid = match label {
            "a" => "account-001",
            "b" => "account-002",
            _ => panic!("unexpected recorded account"),
        };
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let observed = Credentials::new(
            format!("fixture-observer-{label}"),
            "observer-secret".into(),
            "pass".into(),
        )
        .unwrap();
        let executing = Credentials::new(
            format!("fixture-executor-{label}"),
            "executor-secret".into(),
            "pass".into(),
        )
        .unwrap();
        let observer = OkxRestClient::with_http_transport(
            environment,
            observed.clone(),
            RateBudget::new(),
            feed.clone(),
        )
        .unwrap();
        let executor =
            OkxRestClient::with_http_transport(environment, executing, RateBudget::new(), feed)
                .unwrap();
        let config = VerifiedAccountConfig {
            account_key: AccountKey {
                venue: "okx".into(),
                environment: Environment::Real,
                verified_uid: okx_api::account_uid_fingerprint(uid),
            },
            alias: format!("fixture-{label}"),
            credential_ref: format!("fixture-creds-{label}"),
            connect_operation_id: format!("fixture-connect-operation-{label}"),
            configuration_digest: "a".repeat(64),
        };
        (
            config,
            PreparedAccountClients {
                environment,
                observer,
                executor,
                observer_login: observed,
                ws_connector,
            },
        )
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
    /// This exercises the same identity/permission bootstrap with an exhausted
    /// raw HTTP transcript. A failed identity proof must never create SQL or
    /// a durable account route; no real network/credential store is contacted.
    #[tokio::test]
    async fn prepared_account_rejects_unverified_identity_before_sql() {
        use okx_api::{HttpSend, HttpTransport, OkxError};
        use std::time::Instant;
        struct NoRecordedResponse;
        impl HttpTransport for NoRecordedResponse {
            fn send(&self, _: okx_api::HttpRequest, _: Instant) -> HttpSend<'_> {
                Box::pin(async { Err(OkxError::Response("HTTP_TRANSCRIPT_EXHAUSTED".into())) })
            }
        }
        let root = root();
        initialize_installation(&root, "installation-test").unwrap();
        let registry = AccountRuntimeRegistry::open(&root, RateBudget::new())
            .await
            .unwrap();
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let creds = Credentials::new(
            "fixture-key".into(),
            "fixture-secret".into(),
            "fixture-pass".into(),
        )
        .unwrap();
        let transport: Arc<dyn HttpTransport> = Arc::new(NoRecordedResponse);
        let observer = OkxRestClient::with_http_transport(
            environment,
            creds.clone(),
            RateBudget::new(),
            transport.clone(),
        )
        .unwrap();
        let executor = OkxRestClient::with_http_transport(
            environment,
            creds.clone(),
            RateBudget::new(),
            transport,
        )
        .unwrap();
        let clients = PreparedAccountClients {
            environment,
            observer,
            executor,
            observer_login: creds,
            ws_connector: Arc::new(LiveWsConnector),
        };
        let config = VerifiedAccountConfig {
            account_key: key(),
            alias: "fixture-account".into(),
            credential_ref: "fixture-credentials".into(),
            connect_operation_id: "fixture-connect-00001".into(),
            configuration_digest: "a".repeat(64),
        };
        assert!(registry.connect_prepared(config, clients).await.is_err());
        assert!(registry.list().await.is_empty());
        assert!(!root.join("accounts").exists());
        assert!(!root.join("account-leases").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    /// The production PublicWsCoordinator, not the recording, must establish
    /// an acknowledged generation, contiguous book and fresh market snapshot.
    #[tokio::test]
    async fn recorded_public_ws_frames_make_production_market_snapshot_fresh() {
        use super::recorded_support::RecordedWsConnector;
        use serde_json::{Value, json};
        let reference = okx_observation::ReferenceRegistry::from_public(
            "2026-10-31T00:00:00Z",
            vec![serde_json::from_value(json!({
                "instType": "SWAP",
                "instId": "BTC-USDT-SWAP",
                "instFamily": "BTC-USDT",
                "uly": "BTC-USDT",
                "state": "live",
                "ruleType": "normal",
                "settleCcy": "USDT",
                "tickSz": "0.1",
                "lotSz": "0.01",
                "minSz": "0.01",
                "maxLmtSz": "1000",
                "maxMktSz": "1000",
                "ctType": "linear",
                "ctVal": "0.01",
                "ctValCcy": "BTC",
                "lever": "20"
            }))
            .unwrap()],
        )
        .unwrap();
        let connector = Arc::new(RecordedWsConnector::default());
        let stamp = chrono::Utc::now().timestamp_millis();
        let timestamp = stamp.to_string();
        let future = (stamp + 8 * 60 * 60 * 1000).to_string();
        let recordings: Vec<(Value, Option<Value>, Option<&str>)> = vec![
            (json!({"channel":"instruments","instType":"SWAP"}), None, None),
            (json!({"channel":"instruments","instType":"FUTURES"}), None, None),
            (
                json!({"channel":"tickers","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "last":"1000","askPx":"1000","askSz":"10",
                    "bidPx":"999.9","bidSz":"10","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"mark-price","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "markPx":"1000","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"index-tickers","instId":"BTC-USDT"}),
                Some(json!({"instId":"BTC-USDT","idxPx":"1000","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"funding-rate","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "fundingRate":"0.0001","fundingTime":timestamp,
                    "nextFundingTime":future,"ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"open-interest","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "oi":"1000","oiCcy":"10","oiUsd":"10000","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"books","instId":"BTC-USDT-SWAP"}),
                Some(json!({"asks":[["1000","10","0","1"]],
                    "bids":[["999.9","10","0","1"]],
                    "seqId":10,"prevSeqId":-1,"ts":timestamp})),
                Some("snapshot"),
            ),
        ];
        for (arg, data, action) in recordings {
            let mut raw = vec![json!({
                "event":"subscribe","arg":arg,"connId":"recorded-public"
            }).to_string()];
            if let Some(data) = data {
                let mut message = json!({"arg":arg,"data":[data]});
                if let Some(action) = action {
                    message["action"] = action.into();
                }
                raw.push(message.to_string());
            }
            connector.expect_public_subscription(arg, raw);
        }
        let (coordinator, market) = okx_runtime::PublicWsCoordinator::new_with_connector(
            OkxEnvironment::new(okx_api::Region::Global, false),
            reference,
            RateBudget::new(),
            connector.clone(),
        );
        let (shutdown, stop) = watch::channel(false);
        let task = tokio::spawn(coordinator.run(stop));
        let mut updates = market.subscribe_market_updates();
        market.demand_instrument("BTC-USDT-SWAP").await.unwrap();
        let verified = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Ok(snapshot) = market
                    .risk_fresh_snapshot(
                        "BTC-USDT-SWAP",
                        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap(),
                        5_000,
                        5_000,
                        chrono::Utc::now().to_rfc3339(),
                    )
                    .await
                {
                    break snapshot;
                }
                updates.changed().await.expect("market coordinator stopped");
            }
        })
        .await
        .expect("recorded public WS did not produce a fresh snapshot");
        assert_eq!(verified.market.instrument_id, "BTC-USDT-SWAP");
        assert_eq!(verified.market.ticker.best_ask, "1000");
        assert_eq!(verified.order_book.seq_id, Some(10));
        assert_eq!(connector.unconsumed_public_subscriptions(), 0);
        shutdown.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("public coordinator did not stop")
            .unwrap()
            .unwrap();
    }

    /// Exercise the actual signed OKX adapter's POST through the raw
    /// transport boundary. A mismatched request cannot consume a response,
    /// and the response is not an invented execution or a fill.
    #[tokio::test]
    async fn recorded_executor_post_requires_exact_request_and_consumes_once() {
        use super::recorded_support::{RecordedAccountHttp, RecordedPost};
        use okx_api::{
            ApiOrderSide, ApiOrderType, ApiPositionSide, ApiTradeMode, Credentials, MutationTiming,
            OkxRestClient, PlaceOrderRequest, TradeApi,
        };
        let feed = Arc::new(RecordedAccountHttp::default());
        let env = OkxEnvironment::new(okx_api::Region::Global, false);
        let credentials = Credentials::new(
            "fixture-executor-a".into(),
            "executor-secret".into(),
            "pass".into(),
        )
        .unwrap();
        let client =
            OkxRestClient::with_http_transport(env, credentials, RateBudget::new(), feed.clone())
                .unwrap();
        let trade = TradeApi::new(client);
        let request = PlaceOrderRequest {
            instrument_id: "BTC-USDT-SWAP".into(),
            trade_mode: ApiTradeMode::Cross,
            client_order_id: "recordedOrder123".into(),
            side: ApiOrderSide::Buy,
            position_side: ApiPositionSide::Long,
            reduce_only: None,
            order_type: ApiOrderType::Limit,
            size: "0.30".into(),
            price: "1000".into(),
            attached_algo_orders: vec![],
        };
        let mut expected = serde_json::to_value(&request).unwrap();
        expected["clOrdId"] = "__REQUEST_CL_ORD_ID__".into();
        feed.expect_post(RecordedPost {
            access_key: "fixture-executor-a".into(),
            path: "/api/v5/trade/order".into(),
            expected_body: expected,
            raw_response: br#"{"code":"0","msg":"","data":[{"ordId":"123456","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
        });
        let timestamp = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap();
        let timing = MutationTiming::from_exchange_time_ms(timestamp, 2_000).unwrap();
        let mut wrong = request.clone();
        wrong.size = "0.40".into();
        let mismatch = trade.place_order(&wrong, &timing).await.unwrap_err();
        assert!(mismatch.to_string().contains("TRANSCRIPT_POST_BODY_MISMATCH"));
        assert_eq!(
            feed.unconsumed_posts(),
            1,
            "mismatch must not advance transcript"
        );
        let response = trade.place_order(&request, &timing).await.unwrap();
        assert_eq!(response.data.len(), 1);
        assert_eq!(response.data[0].order_id, "123456");
        assert_eq!(response.data[0].client_order_id, request.client_order_id);
        assert_eq!(feed.unconsumed_posts(), 0);
        let exhausted = trade.place_order(&request, &timing).await.unwrap_err();
        assert!(exhausted.to_string().contains("TRANSCRIPT_UNEXPECTED_POST"));
        assert_eq!(
            feed.outbound_posts.lock().unwrap().len(),
            3,
            "all possible sends, including rejected/repeated, must be observable"
        );
    }

    /// A nonempty recorded fill page enters the real OKX history decoder.
    /// Consumed external evidence cannot silently turn into a fresh empty page.
    #[tokio::test]
    async fn recorded_financial_history_page_is_raw_one_use_and_fail_closed() {
        use super::recorded_support::{RecordedAccountHttp, RecordedGet};
        use okx_api::{AccountHistoryApi, Credentials, OkxRestClient};
        let feed = Arc::new(RecordedAccountHttp::default());
        feed.expect_get(RecordedGet {
            access_key: "fixture-observer-a".into(),
            path: "/api/v5/trade/fills-history".into(),
            required_query: vec![("instType".into(), "SWAP".into())],
            raw_response: br#"{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","tradeId":"10001","ordId":"20001","clOrdId":"order123","billId":"30001","subType":"1","side":"buy","posSide":"long","fillPx":"1000","fillSz":"0.12","fee":"-0.0006","feeCcy":"USDT","ts":"1793460000000"}]}"#.to_vec(),
        });
        let client = OkxRestClient::with_http_transport(
            OkxEnvironment::new(okx_api::Region::Global, false),
            Credentials::new(
                "fixture-observer-a".into(),
                "observer-secret".into(),
                "pass".into(),
            )
            .unwrap(),
            RateBudget::new(),
            feed.clone(),
        )
        .unwrap();
        let api = AccountHistoryApi::new(client);
        let end = chrono::Utc::now().timestamp_millis();
        let rows = api
            .fills_history_page("SWAP", end - 60_000, end, None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fill_size, "0.12");
        assert_eq!(rows[0].fee, "-0.0006");
        assert_eq!(feed.unconsumed_reads(), 0);
        let exhausted = api
            .fills_history_page("SWAP", end - 60_000, end, None)
            .await
            .unwrap_err();
        assert!(
            exhausted.to_string().contains("TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED"),
            "unrecorded extra source pages must not become empty proof: {exhausted}"
        );
    }

    /// T09 bootstrap component: independent UIDs create separate real SQL
    /// owners from bounded raw HTTP records, not from a fake exchange state.
    #[tokio::test]
    async fn recorded_two_account_bootstrap_opens_independent_sql_owners() {
        use super::recorded_support::{RecordedAccountHttp, prepared};
        let temp = root();
        initialize_installation(&temp, "installation-test").unwrap();
        let registry = AccountRuntimeRegistry::open(&temp, RateBudget::new())
            .await
            .unwrap();
        let feed = Arc::new(RecordedAccountHttp::default());
        let mut stopped_sql_owners = Vec::new();

        for label in ["a", "b"] {
            let (config, clients) = prepared(label, feed.clone());
            registry
                .connect_prepared(config.clone(), clients)
                .await
                .unwrap();
            let owner = registry.resolve(&config.account_key.wire()).await.unwrap();
            assert_eq!(owner.execution.uid(), config.account_key.verified_uid);
            stopped_sql_owners.push(owner.execution.store.termination_signal());
            assert!(
                temp.join("accounts")
                    .join(key_hash(&config.account_key))
                    .join("account.sqlite")
                    .is_file()
            );
        }

        assert_eq!(registry.list().await.len(), 2);
        {
            let observed = feed.requests.lock().unwrap();
            for label in ["a", "b"] {
                let prefix = format!("fixture-observer-{label}:/api/v5/trade/orders-algo-pending?");
                assert_eq!(
                    observed
                        .iter()
                        .filter(|request| request.starts_with(&prefix))
                        .count(),
                    16,
                    "every required derivative algo scope must be read"
                );
            }
        }
        // Enter the PUBLIC production TradeService with two separately
        // bootstrapped native SQL owners. No recorded HTTP/WS input is allowed
        // to synthesize a source generation, Worker grant or venue POST.
        use crate::trade_service::TradeService;
        use okx_protocol::trading::{
            OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
        };
        let registry = Arc::new(registry);
        let budget = RateBudget::new();
        let public_environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let reference =
            okx_observation::ReferenceRegistry::from_public("2026-10-31T00:00:00Z", vec![])
                .unwrap();
        let (_public_coordinator, public_ws) =
            okx_runtime::PublicWsCoordinator::new_with_rate_budget(
                public_environment,
                reference,
                budget,
            );
        let (risk_tx, mut risk_outbound) = tokio::sync::mpsc::channel(4);
        let trade = TradeService::new(
            registry.clone(),
            public_ws,
            crate::risk_bridge::NativeRiskClient::new(risk_tx),
        );
        let request = TradeRequest::Execute {
            idempotency_key: "public_native_no_source_001".into(),
            operation: TradeOperation::Open,
            strategy_instance: "manual".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: PositionSide::Long,
            sizing: Sizing::Contracts {
                contracts: "0.10".into(),
            },
            order: OrderConstraints {
                order_type: "limit".into(),
                limit_price: Some("1000".into()),
                reference: None,
                max_entry_usd: Some("10".into()),
                max_slippage_bps: 0,
            },
            protection: Some(Protection::Price {
                stop_price: "900".into(),
            }),
            expires_in_ms: 2_000,
        };
        assert!(request.valid());
        for label in ["a", "b"] {
            let alias = format!("fixture-{label}");
            let expected_uid = okx_api::account_uid_fingerprint(&format!(
                "account-00{}",
                if label == "a" { "1" } else { "2" }
            ));
            let caps = trade
                .trade(&alias, &TradeRequest::Capabilities {}, None)
                .await
                .unwrap();
            assert_eq!(
                caps["account"],
                format!("okx:real:{expected_uid}"),
                "public trade routing must preserve the authenticated account owner"
            );
            assert_eq!(caps["executor"]["state"], "verified");
            assert_eq!(caps["ready_for_post"], false);
            let preflight = trade
                .trade(&alias, &TradeRequest::Preflight {}, None)
                .await
                .unwrap();
            assert_eq!(preflight["ready_for_new_risk"], false);
            assert_eq!(preflight["state"], "blocked");
            let rejected = trade.trade(&alias, &request, None).await.unwrap_err();
            assert_eq!(rejected, "AUTHORITY_UNAVAILABLE");
            assert!(
                risk_outbound.try_recv().is_err(),
                "an unproved public Execute must never request central risk"
            );
        }
        drop(trade);
        registry.shutdown().await;
        drop(registry);
        // On Windows, removing an open SQLite file fails with ERROR_SHARING_VIOLATION.
        // Wait for the real SQL owner to close its DB; no sleep or assumed timeout
        // is used as a substitute for a termination receipt.
        for mut stopped in stopped_sql_owners {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !*stopped.borrow_and_update() {
                    stopped
                        .changed()
                        .await
                        .expect("SQL owner exited without receipt");
                }
            })
            .await
            .expect("SQL owner did not terminate");
        }
        std::fs::remove_dir_all(temp).unwrap();
    }

    /// The original public account capture, not a fixture-set "fresh" flag.
    #[tokio::test]
    async fn recorded_ws_login_business_ack_and_six_history_pages_capture_both_sql_accounts() {
        use super::recorded_support::{
            RecordedAccountHttp, RecordedWsConnector, prepared_with_socket,
        };
        let temp = root();
        initialize_installation(&temp, "installation-test").unwrap();
        let registry = AccountRuntimeRegistry::open(&temp, RateBudget::new())
            .await
            .unwrap();
        let http = Arc::new(RecordedAccountHttp::default());
        let mut receipts = Vec::new();
        let mut sockets = Vec::new();
        for label in ["a", "b"] {
            let ws = Arc::new(RecordedWsConnector::default());
            let (config, clients) = prepared_with_socket(label, http.clone(), ws.clone());
            registry
                .connect_prepared(config.clone(), clients)
                .await
                .unwrap();
            let account = registry.resolve(&config.account_key.wire()).await.unwrap();
            receipts.push(account.execution.store.termination_signal());
            sockets.push(ws);
        }
        for (label, ws) in ["a", "b"].into_iter().zip(&sockets) {
            let account = registry.resolve(&format!("fixture-{label}")).await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(12), async {
                loop {
                    let status = account.private_ws.status().await;
                    let completed = account.source_capture_health.read().await.last_success_ms;
                    if status.logged_in
                        && status.subscriptions_complete
                        && status.algo_subscription_ready
                        && status.account_data_seen
                        && status.positions_data_seen
                        && account.source_identity_ready().await
                        && completed.is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("real source capture did not converge from recorded frames");
            {
                let sent = ws.sent.lock().unwrap();
                assert_eq!(sent.iter().filter(|v| v["op"] == "login").count(), 2);
                assert_eq!(sent.iter().filter(|v| v["op"] == "subscribe").count(), 2);
            }
            let recorded = account
                .execution
                .store
                .call(Instant::now() + std::time::Duration::from_secs(3), |s| {
                    Ok((s.cursor("fills", "SWAP")?, s.cursor("bills", "account")?))
                })
                .await
                .unwrap();
            assert!(recorded.0.is_some(), "SWAP history source not committed");
            assert!(recorded.1.is_some(), "bills history source not committed");
            assert_eq!(recorded.0.unwrap().coverage_gaps, "");
            assert_eq!(recorded.1.unwrap().coverage_gaps, "");
        }
        registry.shutdown().await;
        drop(registry);
        for mut receipt in receipts {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !*receipt.borrow_and_update() {
                    receipt
                        .changed()
                        .await
                        .expect("SQL owner exited without receipt");
                }
            })
            .await
            .expect("SQL owner not closed");
        }
        std::fs::remove_dir_all(temp).unwrap();
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
    fn published_age_retains_source_timestamp_even_past_risk_ttl() {
        let source = "2026-10-28T20:00:00.000Z";
        let observed = chrono::DateTime::parse_from_rfc3339(source)
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            snapshot_age_at_publish_ms(source, observed + 4_999),
            Some(4_999)
        );
        assert_eq!(
            snapshot_age_at_publish_ms(source, observed + 5_001),
            Some(5_001)
        );
        assert_eq!(snapshot_age_at_publish_ms(source, observed - 1), Some(-1));
        assert_eq!(snapshot_age_at_publish_ms("invalid", observed), None);
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
