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
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// One risk-increasing admission planner per account. Cancellation,
    /// protective recovery and close do NOT share this gate.
    pub new_risk_admission: Semaphore,
    /// Coalesce only concurrent idempotent Execute replays. An explicit
    /// trade.status, protection check, Close or emergency action never waits
    /// on this permit. Source/SQL/Worker financial owners stay unchanged.
    pub execute_replay_reconciliation: Semaphore,
    source_shutdown: watch::Sender<bool>,
    verified_source_generation: AtomicU64,
    _identity_lease: File,
    source_tasks: Mutex<Vec<JoinHandle<()>>>,
    // TEST-ONLY: deterministic read-scheduling barrier. Holding the write
    // lease prevents the NEXT periodic source cycle while a public
    // TradeService request is under test. It does not alter real rate
    // policy, SQL state, freshness or a live production build.
    #[cfg(test)]
    pub(crate) recorded_capture_gate: tokio::sync::RwLock<()>,
}
#[cfg(test)]
impl AccountResources {
    pub(crate) fn source_task_debug_ids(&self) -> Vec<String> {
        self.source_tasks
            .lock()
            .unwrap()
            .iter()
            .map(|task| format!("{:?}", task.id()))
            .collect()
    }
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
        let budget = self
            .rate_budget
            .for_authenticated_user(&configuration.account_key.verified_uid);
        let clients = PreparedAccountClients::live(credentials, budget)?;
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
            new_risk_admission: Semaphore::new(1),
            execute_replay_reconciliation: Semaphore::new(1),
            source_shutdown: shutdown,
            verified_source_generation: AtomicU64::new(0),
            _identity_lease: identity_lease,
            source_tasks: Mutex::new(Vec::new()),
            #[cfg(test)]
            recorded_capture_gate: tokio::sync::RwLock::new(()),
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
             // Only tests can serialize this timer-driven source cycle with
             // an independently proven admission boundary. The real source
             // capture remains unchanged and stays enabled on release.
             #[cfg(test)]
             let _recorded_capture_lease=account.recorded_capture_gate.read().await;
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
    use sha2::Digest;
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

    // A signed GET is a read of an external snapshot, not the consumption
    // of an economic event. Within an explicitly named, immutable phase
    // concurrent background capture/Preview may receive identical bytes.
    // Only a test driver can transition the phase, after its own causal
    // assertion. The production HTTP parser, freshness, quota and SQL
    // financial source owner remain unchanged.
    // Fixed test-only safety ceiling: a finite source observation is neither
    // a perpetual exchange feed nor an unlimited synthetic GET generator.
    // The clock starts at explicit phase publication, NOT on each read.
    const SIGNED_PHASE_MAX_READS: u64 = 256;
    const SIGNED_PHASE_MAX_LIFETIME: std::time::Duration = std::time::Duration::from_secs(300);

    struct ImmutableSignedGetPhase {
        phase_id: String,
        source: RecordedGet,
        deliveries: u64,
        max_deliveries: u64,
        published_at: Instant,
        ttl: std::time::Duration,
    }

    /// One pre-recorded response for one exact signed executor POST. This is
    /// only a transport transcript, never an exchange/account state machine.
    pub(crate) struct RecordedPost {
        pub(crate) access_key: String,
        pub(crate) path: String,
        pub(crate) expected_body: serde_json::Value,
        pub(crate) raw_response: Vec<u8>,
    }

    // Bind immutable external GET bytes to the order ID from a signed POST ACK.
    // No matching, fills, order creation or money is simulated in this port.
    fn recorded_order_source_matches(
        acknowledged: &[(String, String, String)],
        access_key: &str,
        client_id: &str,
        raw_get: &[u8],
    ) -> bool {
        let matched = acknowledged
            .iter()
            .filter(|(key, client, _)| key == access_key && client == client_id)
            .collect::<Vec<_>>();
        if matched.len() != 1 {
            return false;
        }
        let Ok(envelope) = serde_json::from_slice::<serde_json::Value>(raw_get) else {
            return false;
        };
        let Some(data) = envelope["data"].as_array() else {
            return false;
        };
        data.len() == 1
            && data[0]["ordId"].as_str() == Some(matched[0].2.as_str())
            && data[0]["clOrdId"].as_str() == Some("__REQUEST_CL_ORD_ID__")
    }

    // Bind history rows only to the already-observed signed POST ACK.
    fn bind_recorded_history_ids(
        acknowledgements: &[(String, String, String)],
        account: &str,
        raw: Vec<u8>,
    ) -> Result<Vec<u8>, OkxError> {
        if !raw
            .windows("__REQUEST_CL_ORD_ID__".len())
            .any(|bytes| bytes == b"__REQUEST_CL_ORD_ID__")
        {
            return Ok(raw);
        }
        let mut envelope: serde_json::Value = serde_json::from_slice(&raw)?;
        let rows = envelope["data"]
            .as_array_mut()
            .ok_or_else(|| OkxError::Response("TRANSCRIPT_HISTORY_ROWS_MISSING".into()))?;
        if rows.is_empty() {
            return Err(OkxError::Response("TRANSCRIPT_HISTORY_ROWS_MISSING".into()));
        }
        // Production financial history is read with the EXECUTOR key,
        // identical to the signing identity that received the create ACK.
        if !account.starts_with("fixture-executor-") {
            return Err(OkxError::Response(
                "TRANSCRIPT_HISTORY_EXECUTOR_IDENTITY_INVALID".into(),
            ));
        }
        for row in rows {
            if row["clOrdId"] != "__REQUEST_CL_ORD_ID__" {
                return Err(OkxError::Response(
                    "TRANSCRIPT_HISTORY_CLIENT_ID_UNBOUND".into(),
                ));
            }
            let order = row["ordId"]
                .as_str()
                .ok_or_else(|| OkxError::Response("TRANSCRIPT_HISTORY_ORDER_ID_UNBOUND".into()))?;
            let matched = acknowledgements
                .iter()
                .filter(|(key, _, id)| key == account && id == order)
                .collect::<Vec<_>>();
            if matched.len() != 1 {
                return Err(OkxError::Response(
                    "TRANSCRIPT_HISTORY_ACK_ID_MISMATCH".into(),
                ));
            }
            row["clOrdId"] = matched[0].1.clone().into();
        }
        serde_json::to_vec(&envelope).map_err(Into::into)
    }

    // Immutable external snapshots selected by an actual matching cancel ACK,
    // never by the number or order of maintenance/Status GETs.
    struct CausalAlgoScope {
        query: Vec<(String, String)>,
        before_ack: Vec<u8>,
        after_ack: Vec<u8>,
        before_reads: usize,
        after_reads: usize,
    }

    // UID B's independent signed inventory is immutable; A's cancel ACK
    // never selects or mutates this source.
    struct StaticAlgoScope {
        query: Vec<(String, String)>,
        response: Vec<u8>,
        reads: usize,
    }

    struct StaticAlgoInventory {
        account: String,
        source_received_at_ms: i64,
        scopes: Vec<StaticAlgoScope>,
    }

    struct CausalAlgoReadGate {
        entered: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    }

    struct CausalAlgoScenario {
        account: String,
        cancel_id: String,
        acked: bool,
        scopes: Vec<CausalAlgoScope>,
        held_read: Option<CausalAlgoReadGate>,
        // A started GET owns an immutable pre-ACK snapshot until delivery or
        // cancellation. Taking the one-shot gate must not mark it completed.
        inflight_held_reads: usize,
        source_received_at_ms: i64,
    }

    // The two signed API identities are different actors of the same verified
    // OKX UID. A scripted venue inventory belongs to the UID, never the key.
    fn verified_fixture_uid(actor: &str) -> Option<&'static str> {
        let label = actor
            .strip_prefix("fixture-observer-")
            .or_else(|| actor.strip_prefix("fixture-executor-"))?;
        match label {
            "a" => Some("account-001"),
            "b" => Some("account-002"),
            _ => None,
        }
    }

    // Each of 16 scopes has a predeclared budget for Status, post-ACK
    // verification, source capture and maintenance. A missing scope, extra
    // POST or exhausted bound fails closed, not as a financial denial.
    const CAUSAL_ALGO_READS_PER_SCOPE_AND_PHASE: usize = 12;
    const STATIC_ALGO_READS_PER_SCOPE: usize = 32;

    #[derive(Default)]
    pub(crate) struct RecordedAccountHttp {
        pub(crate) requests: Mutex<Vec<String>>,
        // TEST-ONLY: record each signed balance GET admission attempt before
        // fixture selection. Task IDs attribute reads to source owners versus
        // the TradeService caller without changing replay or risk behavior.
        signed_balance_reads: Mutex<Vec<(String, String, i64)>>,
        // T10-06: record the actual Tokio consumers of the finite, unsigned
        // fallback inventory. This is diagnostic test state only: never
        // supplies a venue response or changes financial source freshness.
        t10_06_pending_consumers: Mutex<Vec<(String, i64)>>,
        pub(crate) outbound_posts: Mutex<Vec<(String, String, serde_json::Value)>>,
        /// Actual signed fee GETs delivered by this test transport:
        /// (account API key, exchange ts, local delivery timestamp).
        pub(crate) delivered_fees: Mutex<Vec<(String, String, i64)>>,
        /// Strict test transcript GET denial paths only: no headers or secrets.
        pub(crate) denied_routes: Mutex<Vec<String>>,
        /// (signed account key, exact sent clOrdId, acknowledged ordId).
        acknowledged_creates: Mutex<Vec<(String, String, String)>>,
        // Test-only lost network ACK AFTER exactly one signed venue create.
        // The real HTTP request is counted and its authenticated venue
        // order identity remains available for independent GET reconciliation.
        lose_next_created_ack: AtomicBool,
        posts: Mutex<Vec<RecordedPost>>,
        reads: Mutex<Vec<RecordedGet>>,
        // Bounded extra recorded GET bytes for read-only Preview retries
        // after a proven LOCAL no-send rate defer. Required fixture pages
        // remain mandatory and are consumed first; extras are never a
        // fallback for missing routes or an unbounded exchange simulator.
        optional_reads: Mutex<Vec<RecordedGet>>,
        // Opt-in event-scoped immutable REST snapshots. Not a fallback for
        // missing one-shot history/order pages. A snapshot is never refreshed
        // by the number or timing of GETs.
        immutable_signed_phases: Mutex<Vec<ImmutableSignedGetPhase>>,
        // Digest-only signed source events for exact-phase test evidence.
        pub(crate) immutable_phase_trace: Mutex<Vec<String>>,
        // Raw public time responses, never a synthesized exchange clock.
        public_clock_reads: Mutex<Vec<Vec<u8>>>,
        // Test-only count of raw public-clock GET deliveries; the same
        // queue lock linearizes a generated external sample against its
        // exactly-once consumption by the real production OKX clock parser.
        public_clock_deliveries: AtomicU64,
        strict_read_routes: Mutex<std::collections::BTreeSet<(String, String)>>,
        causal_algos: Mutex<Option<CausalAlgoScenario>>,
        static_algos: Mutex<Option<StaticAlgoInventory>>,
        pub(crate) causal_algo_trace: Mutex<Vec<String>>,
    }
    // A cancelled/expired transport future must release its recorded I/O
    // lease as well as a normally resumed GET. This is TEST-ONLY scheduling
    // state; it is not a venue, SQL or risk transition.
    struct CausalAlgoReadInFlight<'a>(&'a RecordedAccountHttp);
    impl Drop for CausalAlgoReadInFlight<'_> {
        fn drop(&mut self) {
            let mut active = self.0.causal_algos.lock().unwrap();
            let phase = active
                .as_mut()
                .expect("TRANSCRIPT_ALGO_PHASE_RETIRED_DURING_READ");
            assert!(
                phase.inflight_held_reads > 0,
                "TRANSCRIPT_ALGO_INFLIGHT_UNDERFLOW"
            );
            phase.inflight_held_reads -= 1;
        }
    }
    impl RecordedAccountHttp {
        pub(crate) fn balance_read_probe(&self) -> serde_json::Value {
            let events = self.signed_balance_reads.lock().unwrap();
            let mut by_task = std::collections::BTreeMap::<String, usize>::new();
            for (actor, task, _) in events.iter() {
                *by_task.entry(format!("{actor}/{task}")).or_default() += 1;
            }
            serde_json::json!({
                "admitted_attempts": events.len(),
                "by_actor_and_task": by_task,
                "recent": events.iter().rev().take(6).cloned().collect::<Vec<_>>(),
            })
        }

        /// Install the FIRST snapshot of an account-owned REST route. These
        /// three routes are read-only and idempotent between source events.
        /// Fills, bills, order status, clock and algo inventories stay under
        /// their separate causal/one-shot transcript contracts.
        pub(crate) fn expect_immutable_signed_phase(&self, phase_id: &str, page: RecordedGet) {
            self.expect_immutable_signed_phase_bounded(
                phase_id,
                page,
                SIGNED_PHASE_MAX_READS,
                SIGNED_PHASE_MAX_LIFETIME,
            );
        }

        /// Declare a finite delivery budget and monotonic lifetime BEFORE
        /// publishing the source event. Only the recorded transport uses it.
        pub(crate) fn expect_immutable_signed_phase_bounded(
            &self,
            phase_id: &str,
            page: RecordedGet,
            max_deliveries: u64,
            ttl: std::time::Duration,
        ) {
            assert!(
                max_deliveries > 0 && !ttl.is_zero(),
                "TRANSCRIPT_IMMUTABLE_BUDGET_INVALID"
            );
            assert!(
                !phase_id.is_empty(),
                "TRANSCRIPT_IMMUTABLE_PHASE_ID_MISSING"
            );
            assert!(
                matches!(
                    page.path.as_str(),
                    "/api/v5/account/balance"
                        | "/api/v5/account/trade-fee"
                        | "/api/v5/account/leverage-info"
                        | "/api/v5/trade/orders-pending"
                ),
                "TRANSCRIPT_IMMUTABLE_ROUTE_NOT_ALLOWED"
            );
            let route = (page.access_key.clone(), page.path.clone());
            let mut query = page.required_query.clone();
            query.sort();
            let mut phases = self.immutable_signed_phases.lock().unwrap();
            assert!(
                !phases.iter().any(|item| {
                    let mut previous = item.source.required_query.clone();
                    previous.sort();
                    item.source.access_key == page.access_key
                        && item.source.path == page.path
                        && previous == query
                }),
                "TRANSCRIPT_IMMUTABLE_PHASE_ALREADY_REGISTERED"
            );
            assert!(
                !self
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.access_key == page.access_key && item.path == page.path),
                "TRANSCRIPT_IMMUTABLE_MIXED_WITH_QUEUED_SOURCE"
            );
            assert!(
                !self
                    .optional_reads
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.access_key == page.access_key && item.path == page.path),
                "TRANSCRIPT_IMMUTABLE_MIXED_WITH_OPTIONAL_SOURCE"
            );
            self.strict_read_routes.lock().unwrap().insert(route);
            phases.push(ImmutableSignedGetPhase {
                phase_id: phase_id.into(),
                source: page,
                deliveries: 0,
                max_deliveries,
                published_at: Instant::now(),
                ttl,
            });
            let installed = phases.last().unwrap();
            self.immutable_phase_trace.lock().unwrap().push(format!(
                "event=SIGNED_PHASE_INSTALLED phase={phase_id} actor={} route={} query={:?} sha256={:x} max_reads={max_deliveries} ttl_ms={}",
                installed.source.access_key,installed.source.path,query,
                sha2::Sha256::digest(&installed.source.raw_response),ttl.as_millis()
            ));
        }

        /// Transition ONLY the same signed account+route+query. This API
        /// does not accept a new timestamp on a GET: the new raw response
        /// must be explicitly supplied after a separately verified event.
        pub(crate) fn advance_immutable_signed_phase(
            &self,
            expected_phase: &str,
            next_phase: &str,
            page: RecordedGet,
        ) -> u64 {
            self.advance_immutable_signed_phase_bounded(
                expected_phase,
                next_phase,
                page,
                SIGNED_PHASE_MAX_READS,
                SIGNED_PHASE_MAX_LIFETIME,
            )
        }

        pub(crate) fn advance_immutable_signed_phase_bounded(
            &self,
            expected_phase: &str,
            next_phase: &str,
            page: RecordedGet,
            max_deliveries: u64,
            ttl: std::time::Duration,
        ) -> u64 {
            assert!(
                max_deliveries > 0 && !ttl.is_zero(),
                "TRANSCRIPT_IMMUTABLE_BUDGET_INVALID"
            );
            assert!(
                !expected_phase.is_empty()
                    && !next_phase.is_empty()
                    && expected_phase != next_phase,
                "TRANSCRIPT_IMMUTABLE_PHASE_TRANSITION_INVALID"
            );
            let mut query = page.required_query.clone();
            query.sort();
            let mut phases = self.immutable_signed_phases.lock().unwrap();
            let prior = phases
                .iter_mut()
                .find(|item| {
                    let mut existing = item.source.required_query.clone();
                    existing.sort();
                    item.source.access_key == page.access_key
                        && item.source.path == page.path
                        && existing == query
                })
                .expect("TRANSCRIPT_IMMUTABLE_UNREGISTERED_TRANSITION");
            assert_eq!(
                prior.phase_id, expected_phase,
                "TRANSCRIPT_IMMUTABLE_CAUSAL_PREDECESSOR_MISMATCH"
            );
            let previously_delivered = prior.deliveries;
            let old_sha256 = format!("{:x}", sha2::Sha256::digest(&prior.source.raw_response));
            prior.phase_id = next_phase.into();
            prior.source = page;
            prior.deliveries = 0;
            prior.max_deliveries = max_deliveries;
            prior.published_at = Instant::now();
            prior.ttl = ttl;
            self.immutable_phase_trace.lock().unwrap().push(format!(
                "event=SIGNED_PHASE_ADVANCED from={expected_phase} to={next_phase} actor={} route={} query={query:?} observed_reads={previously_delivered} old_sha256={old_sha256} new_sha256={:x} max_reads={max_deliveries} ttl_ms={}",
                prior.source.access_key,prior.source.path,
                sha2::Sha256::digest(&prior.source.raw_response),ttl.as_millis()
            ));
            previously_delivered
        }

        /// End a recorded phase only at a separately proven scenario event.
        /// Follow-on signed pages can then be installed without silently
        /// mixing their different economic inventories with the old bytes.
        pub(crate) fn retire_immutable_signed_phase(
            &self,
            phase_id: &str,
            access_key: &str,
            path: &str,
        ) -> u64 {
            let mut phases = self.immutable_signed_phases.lock().unwrap();
            let index = phases
                .iter()
                .position(|item| {
                    item.source.access_key == access_key
                        && item.source.path == path
                        && item.phase_id == phase_id
                })
                .expect("TRANSCRIPT_IMMUTABLE_RETIRE_PHASE_MISSING");
            let retired = phases.remove(index);
            // The strict route remains guarded while another client's order
            // phase OR finite queued readback is present on this endpoint.
            let route_still_owned = phases
                .iter()
                .any(|item| item.source.access_key == access_key && item.source.path == path)
                || self
                    .reads
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.access_key == access_key && item.path == path)
                || self
                    .optional_reads
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.access_key == access_key && item.path == path);
            if !route_still_owned {
                self.strict_read_routes
                    .lock()
                    .unwrap()
                    .remove(&(access_key.to_owned(), path.to_owned()));
            }
            self.immutable_phase_trace.lock().unwrap().push(format!(
                "event=SIGNED_PHASE_RETIRED phase={phase_id} actor={access_key} route={path} observed_reads={}",
                retired.deliveries
            ));
            retired.deliveries
        }

        pub(crate) fn immutable_signed_phase_reads(
            &self,
            access_key: &str,
            path: &str,
            phase_id: &str,
        ) -> u64 {
            self.immutable_signed_phases
                .lock()
                .unwrap()
                .iter()
                .find(|item| {
                    item.source.access_key == access_key
                        && item.source.path == path
                        && item.phase_id == phase_id
                })
                .map_or(0, |item| item.deliveries)
        }

        /// Test-only event handoff from the old finite bootstrap source to a
        /// new immutable signed snapshot. This is NOT an empty-page fallback:
        /// all retired pages must have the same exact actor/route/query and
        /// identical raw bytes. A different old source aborts the handoff.
        /// Caller holds the account's recorded_capture_gate during this event.
        pub(crate) fn supersede_queued_signed_source_at_event(
            &self,
            event_id: &str,
            phase_id: &str,
            page: RecordedGet,
        ) -> usize {
            assert!(
                !event_id.is_empty() && !phase_id.is_empty(),
                "TRANSCRIPT_SOURCE_EVENT_ID_MISSING"
            );
            assert!(
                matches!(
                    page.path.as_str(),
                    "/api/v5/account/trade-fee"
                        | "/api/v5/account/leverage-info"
                        | "/api/v5/trade/order"
                ),
                "TRANSCRIPT_SOURCE_EVENT_ROUTE_NOT_ALLOWED"
            );
            let actor = page.access_key.clone();
            let path = page.path.clone();
            let mut query = page.required_query.clone();
            query.sort();
            let mut phases = self.immutable_signed_phases.lock().unwrap();
            assert!(
                !phases.iter().any(|p| {
                    let mut existing = p.source.required_query.clone();
                    existing.sort();
                    p.source.access_key == actor && p.source.path == path && existing == query
                }),
                "TRANSCRIPT_SOURCE_EVENT_ALREADY_IMMUTABLE"
            );
            let mut required = self.reads.lock().unwrap();
            let mut optional = self.optional_reads.lock().unwrap();
            // Exact non-order query scope; order pages are initially recorded
            // with instId only, so scope them by the actual signed POST ACK's
            // (account, clOrdId, ordId) rather than by the shared endpoint.
            let acknowledged = self.acknowledged_creates.lock().unwrap();
            let client = query
                .iter()
                .find(|(name, _)| name == "clOrdId")
                .map(|(_, value)| value.as_str());
            let in_scope = |p: &RecordedGet| {
                if p.access_key != actor || p.path != path {
                    return false;
                }
                let mut old_query = p.required_query.clone();
                old_query.sort();
                if path == "/api/v5/trade/order" {
                    client.is_some_and(|id| {
                        old_query.iter().all(|pair| query.contains(pair))
                            && recorded_order_source_matches(
                                &acknowledged,
                                &actor,
                                id,
                                &p.raw_response,
                            )
                    })
                } else {
                    old_query == query
                }
            };
            let retired = required
                .iter()
                .chain(optional.iter())
                .filter(|p| in_scope(p))
                .collect::<Vec<_>>();
            assert!(
                !retired.is_empty(),
                "TRANSCRIPT_SOURCE_EVENT_OLD_SOURCE_MISSING"
            );
            assert!(
                retired.iter().all(|p| {
                    let mut old_query = p.required_query.clone();
                    old_query.sort();
                    old_query == query
                        || (path == "/api/v5/trade/order"
                            && old_query.iter().all(|pair| query.contains(pair)))
                }),
                "TRANSCRIPT_SOURCE_EVENT_OLD_QUERY_CONFLICT"
            );
            if path == "/api/v5/trade/order" {
                // Repeated signed reads of a still-LIVE order are one
                // external snapshot, NOT one new economic event per GET.
                // Only the exact already acknowledged order may enter this
                // immutable phase. Changing an order state needs a separate
                // authenticated source event, never a read-count transition.
                let client = query
                    .iter()
                    .find(|(name, _)| name == "clOrdId")
                    .map(|(_, value)| value.as_str())
                    .expect("TRANSCRIPT_ORDER_PHASE_CLIENT_ID_MISSING");
                assert!(
                    recorded_order_source_matches(
                        &acknowledged,
                        &actor,
                        client,
                        &page.raw_response,
                    ),
                    "TRANSCRIPT_ORDER_PHASE_ACK_UNBOUND"
                );
                assert!(
                    retired.iter().all(|p| recorded_order_source_matches(
                        &acknowledged,
                        &actor,
                        client,
                        &p.raw_response
                    )),
                    "TRANSCRIPT_ORDER_PHASE_OLD_ACK_UNBOUND"
                );
                assert!(
                    retired.iter().all(|p| p.raw_response == page.raw_response),
                    "TRANSCRIPT_ORDER_PHASE_UNPROVEN_STATE_CHANGE"
                );
            }
            let old_raw = &retired[0].raw_response;
            assert!(
                retired.iter().all(|p| p.raw_response == *old_raw),
                "TRANSCRIPT_SOURCE_EVENT_OLD_RAW_CONFLICT"
            );
            let old_digest = format!("{:x}", sha2::Sha256::digest(old_raw));
            let removed = retired.len();
            drop(retired);
            required.retain(|p| !in_scope(p));
            optional.retain(|p| !in_scope(p));
            self.strict_read_routes
                .lock()
                .unwrap()
                .insert((actor.clone(), path.clone()));
            phases.push(ImmutableSignedGetPhase {
                phase_id: phase_id.into(),
                source: page,
                deliveries: 0,
                max_deliveries: SIGNED_PHASE_MAX_READS,
                published_at: Instant::now(),
                ttl: SIGNED_PHASE_MAX_LIFETIME,
            });
            self.immutable_phase_trace.lock().unwrap().push(format!(
                "event=SIGNED_SOURCE_SUPERSEDED cause={event_id} phase={phase_id} actor={actor} route={path} query={query:?} old_pages={removed} old_sha256={old_digest} new_sha256={:x}",
                sha2::Sha256::digest(&phases.last().unwrap().source.raw_response)
            ));
            removed
        }

        pub(crate) fn expect_get(&self, page: RecordedGet) {
            assert!(
                !self
                    .immutable_signed_phases
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.source.access_key == page.access_key
                        && item.source.path == page.path
                        && (page.path != "/api/v5/trade/order"
                            || item.source.raw_response == page.raw_response)),
                "TRANSCRIPT_IMMUTABLE_MIXED_WITH_QUEUED_SOURCE"
            );
            self.strict_read_routes
                .lock()
                .unwrap()
                .insert((page.access_key.clone(), page.path.clone()));
            self.reads.lock().unwrap().push(page);
        }

        pub(crate) fn expect_optional_get(&self, page: RecordedGet) {
            assert!(
                !self
                    .immutable_signed_phases
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.source.access_key == page.access_key
                        && item.source.path == page.path
                        && (page.path != "/api/v5/trade/order"
                            || item.source.raw_response == page.raw_response)),
                "TRANSCRIPT_IMMUTABLE_MIXED_WITH_OPTIONAL_SOURCE"
            );
            self.strict_read_routes
                .lock()
                .unwrap()
                .insert((page.access_key.clone(), page.path.clone()));
            self.optional_reads.lock().unwrap().push(page);
        }

        /// At an explicitly verified venue transition (such as the CLOSE
        /// ACK), retire the previous finite raw source pages and register the
        /// next immutable HTTP responses. This operates ONLY on the recorded
        /// transport and cannot touch SQLite, Worker state or live trading.
        /// A missing route or a response for another account is an error.
        pub(crate) fn advance_recorded_route(
            &self,
            access_key: &str,
            path: &str,
            pages: Vec<RecordedGet>,
        ) -> usize {
            assert!(!pages.is_empty(), "TRANSCRIPT_PHASE_WITHOUT_SOURCE");
            assert!(
                !self
                    .immutable_signed_phases
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|item| item.source.access_key == access_key && item.source.path == path),
                "TRANSCRIPT_IMMUTABLE_MIXED_WITH_QUEUE_ADVANCE"
            );
            assert!(
                self.strict_read_routes
                    .lock()
                    .unwrap()
                    .contains(&(access_key.to_owned(), path.to_owned())),
                "TRANSCRIPT_PHASE_UNREGISTERED_ROUTE:{access_key}:{path}"
            );
            assert!(
                pages
                    .iter()
                    .all(|page| page.access_key == access_key && page.path == path),
                "TRANSCRIPT_PHASE_CROSS_ACCOUNT_OR_ROUTE"
            );
            let mut required = self.reads.lock().unwrap();
            let retired = required
                .iter()
                .filter(|page| page.access_key == access_key && page.path == path)
                .count();
            required.retain(|page| page.access_key != access_key || page.path != path);
            let mut optional = self.optional_reads.lock().unwrap();
            optional.retain(|page| page.access_key != access_key || page.path != path);
            required.extend(pages);
            retired
        }

        /// Explicitly finish a cancel-dependent inventory only AFTER its
        /// matching authenticated cancel ACK. Otherwise an old immutable
        /// after-ACK page would shadow the next separately recorded inventory
        /// forever, even after the new fill/SQL projection was proven. This
        /// changes only the test transport's source phase, never exchange,
        /// SQL, RiskBook or production source freshness.
        pub(crate) fn retire_acked_causal_algo_inventory(&self, account: &str) {
            let mut active = self.causal_algos.lock().unwrap();
            let phase = active
                .as_ref()
                .expect("TRANSCRIPT_ALGO_PHASE_MISSING_AT_RETIRE");
            assert_eq!(
                phase.account.as_str(),
                account,
                "TRANSCRIPT_ALGO_PHASE_CROSS_ACCOUNT_RETIRE"
            );
            assert!(
                phase.acked,
                "TRANSCRIPT_ALGO_PHASE_RETIRED_BEFORE_MATCHING_CANCEL_ACK"
            );
            assert!(
                phase.held_read.is_none() && phase.inflight_held_reads == 0,
                "TRANSCRIPT_ALGO_PHASE_RETIRED_WITH_UNRESOLVED_BARRIER"
            );
            let cancel_id = phase.cancel_id.clone();
            *active = None;
            drop(active);
            self.causal_algo_trace.lock().unwrap().push(format!(
                "event=INVENTORY_PHASE_RETIRED actor={account} cancel_id={cancel_id} evidence=CANCEL_ACK"
            ));
        }

        /// Script the two immutable signed inventories for exactly one cancel.
        /// The ACK in the real HTTP send path changes their visibility.
        /// Replacing an unacknowledged transition is forbidden.
        pub(crate) fn expect_causal_algo_inventory(
            &self,
            cancel_id: &str,
            before: Vec<RecordedGet>,
            after: Vec<RecordedGet>,
        ) {
            let path = "/api/v5/trade/orders-algo-pending";
            assert_eq!(before.len(), 16, "TRANSCRIPT_ALGO_PRE_SCOPE_COUNT");
            assert_eq!(after.len(), 16, "TRANSCRIPT_ALGO_POST_SCOPE_COUNT");
            let account = "fixture-observer-a";
            let mut scopes = Vec::with_capacity(16);
            let mut seen = std::collections::BTreeSet::new();
            for page in before {
                assert_eq!(page.access_key, account, "TRANSCRIPT_ALGO_ACCOUNT");
                assert_eq!(page.path, path, "TRANSCRIPT_ALGO_ROUTE");
                let mut query = page.required_query;
                query.sort();
                assert_eq!(query.len(), 3, "TRANSCRIPT_ALGO_QUERY_SHAPE");
                assert!(
                    seen.insert(query.clone()),
                    "TRANSCRIPT_ALGO_DUPLICATE_SCOPE"
                );
                let matching = after
                    .iter()
                    .filter(|other| {
                        let mut other_query = other.required_query.clone();
                        other_query.sort();
                        other.access_key == account && other.path == path && other_query == query
                    })
                    .collect::<Vec<_>>();
                assert_eq!(matching.len(), 1, "TRANSCRIPT_ALGO_POST_SCOPE_MISMATCH");
                scopes.push(CausalAlgoScope {
                    query,
                    before_ack: page.raw_response,
                    after_ack: matching[0].raw_response.clone(),
                    before_reads: 0,
                    after_reads: 0,
                });
            }
            let mut phase = self.causal_algos.lock().unwrap();
            assert!(
                phase.as_ref().is_none_or(|previous| {
                    previous.acked
                        && previous.held_read.is_none()
                        && previous.inflight_held_reads == 0
                }),
                "TRANSCRIPT_ALGO_UNACKNOWLEDGED_PHASE"
            );
            self.strict_read_routes
                .lock()
                .unwrap()
                .insert((account.into(), path.into()));
            self.reads
                .lock()
                .unwrap()
                .retain(|page| page.access_key != account || page.path != path);
            self.optional_reads
                .lock()
                .unwrap()
                .retain(|page| page.access_key != account || page.path != path);
            *phase = Some(CausalAlgoScenario {
                account: account.into(),
                cancel_id: cancel_id.into(),
                acked: false,
                scopes,
                held_read: None,
                inflight_held_reads: 0,
                source_received_at_ms: chrono::Utc::now().timestamp_millis(),
            });
            self.causal_algo_trace.lock().unwrap().push(format!(
                "event=INVENTORY_ARMED uid=account-001 actor={account} cancel_id={cancel_id} scopes=16"
            ));
        }

        /// A separate verified UID must supply its own exact, finite, raw
        /// signed empty census. No fallback to A and no page-count phase.
        pub(crate) fn expect_static_algo_inventory(&self, account: &str, pages: Vec<RecordedGet>) {
            const PATH: &str = "/api/v5/trade/orders-algo-pending";
            assert!(
                matches!(account, "fixture-observer-a" | "fixture-observer-b"),
                "TRANSCRIPT_STATIC_ALGO_ACCOUNT"
            );
            assert_eq!(pages.len(), 16, "TRANSCRIPT_STATIC_ALGO_SCOPE_COUNT");
            let mut seen = std::collections::BTreeSet::new();
            let mut scopes = Vec::with_capacity(16);
            for page in pages {
                assert_eq!(page.access_key, account, "TRANSCRIPT_STATIC_ALGO_ACTOR");
                assert_eq!(page.path, PATH, "TRANSCRIPT_STATIC_ALGO_ROUTE");
                let mut query = page.required_query;
                query.sort();
                assert_eq!(query.len(), 3, "TRANSCRIPT_STATIC_ALGO_QUERY_SHAPE");
                assert!(
                    seen.insert(query.clone()),
                    "TRANSCRIPT_STATIC_ALGO_DUP_SCOPE"
                );
                let source: serde_json::Value = serde_json::from_slice(&page.raw_response)
                    .expect("TRANSCRIPT_STATIC_ALGO_RAW_SOURCE");
                assert_eq!(source["code"], "0", "TRANSCRIPT_STATIC_ALGO_SOURCE_CODE");
                assert_eq!(
                    source["data"].as_array().map(Vec::len),
                    Some(0),
                    "TRANSCRIPT_STATIC_ALGO_B_NOT_FLAT"
                );
                scopes.push(StaticAlgoScope {
                    query,
                    response: page.raw_response,
                    reads: 0,
                });
            }
            self.strict_read_routes
                .lock()
                .unwrap()
                .insert((account.to_owned(), PATH.to_owned()));
            let mut installed = self.static_algos.lock().unwrap();
            if let Some(prior) = installed.as_ref() {
                // Existing T09 close/restart tests re-publish B's own
                // independent signed inventory after a source event.
                // Never allow one verified UID to replace another, or
                // permit a second publication of A's new one-shot source.
                assert_eq!(
                    prior.account, account,
                    "TRANSCRIPT_STATIC_ALGO_CROSS_ACCOUNT_REPLACEMENT"
                );
                assert_eq!(
                    account, "fixture-observer-b",
                    "TRANSCRIPT_STATIC_ALGO_PHASE_ALREADY_REGISTERED"
                );
            }
            *installed = Some(StaticAlgoInventory {
                account: account.to_owned(),
                source_received_at_ms: chrono::Utc::now().timestamp_millis(),
                scopes,
            });
            drop(installed);
            let uid = verified_fixture_uid(account).expect("verified static algo UID");
            self.causal_algo_trace.lock().unwrap().push(format!(
                "event=IMMUTABLE_ALGO_INVENTORY_ARMED uid={uid} actor={account} scopes=16"
            ));
        }

        /// Replace A's exhausted page-queue model only at the observed
        /// A order ACK + B central no-send decision boundary. All prior
        /// signed pages must be EXACTLY the same empty raw inventory for
        /// each of the sixteen scopes; neither a live stop nor a different
        /// account/query can be silently superseded. The new immutable
        /// snapshot is finite (STATIC_ALGO_READS_PER_SCOPE), not refreshed
        /// on background GET and never mutates SQL/Worker/venue state.
        pub(crate) fn publish_post_ack_flat_algo_inventory(
            &self,
            event_id: &str,
            acknowledged_order_id: &str,
            pages: Vec<RecordedGet>,
        ) -> usize {
            const ACTOR: &str = "fixture-observer-a";
            const EXECUTOR: &str = "fixture-executor-a";
            const PATH: &str = "/api/v5/trade/orders-algo-pending";
            assert!(!event_id.is_empty(), "TRANSCRIPT_ALGO_EVENT_ID_MISSING");
            assert!(
                self.acknowledged_creates
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(actor, _, order_id)| actor == EXECUTOR
                        && order_id == acknowledged_order_id),
                "TRANSCRIPT_ALGO_POST_ACK_NOT_OBSERVED"
            );
            assert_eq!(pages.len(), 16, "TRANSCRIPT_ALGO_EVENT_SCOPE_COUNT");
            let mut expected = std::collections::BTreeMap::new();
            for page in &pages {
                assert_eq!(page.access_key, ACTOR, "TRANSCRIPT_ALGO_EVENT_ACTOR");
                assert_eq!(page.path, PATH, "TRANSCRIPT_ALGO_EVENT_ROUTE");
                let mut query = page.required_query.clone();
                query.sort();
                assert_eq!(query.len(), 3, "TRANSCRIPT_ALGO_EVENT_QUERY_SHAPE");
                assert!(
                    expected.insert(query, page.raw_response.clone()).is_none(),
                    "TRANSCRIPT_ALGO_EVENT_DUPLICATE_SCOPE"
                );
            }
            let old_causal = self.causal_algos.lock().unwrap().is_some();
            if old_causal {
                // A previously installed stop transition may be retired
                // only after its own signed cancel ACK and all held GETs.
                self.retire_acked_causal_algo_inventory(ACTOR);
            }
            let mut required = self.reads.lock().unwrap();
            let mut optional = self.optional_reads.lock().unwrap();
            let retired = required
                .iter()
                .chain(optional.iter())
                .filter(|page| page.access_key == ACTOR && page.path == PATH)
                .collect::<Vec<_>>();
            assert!(
                !retired.is_empty(),
                "TRANSCRIPT_ALGO_EVENT_OLD_SOURCE_MISSING"
            );
            assert!(
                retired.iter().all(|page| {
                    let mut query = page.required_query.clone();
                    query.sort();
                    expected.get(&query) == Some(&page.raw_response)
                }),
                "TRANSCRIPT_ALGO_EVENT_SOURCE_OR_QUERY_MISMATCH"
            );
            let retired_count = retired.len();
            drop(retired);
            // Install before removing the old queue: a competing GET sees
            // either the old signed bytes or the new identical raw snapshot,
            // never an artificial empty gap between the two locks.
            self.expect_static_algo_inventory(ACTOR, pages);
            required.retain(|page| page.access_key != ACTOR || page.path != PATH);
            optional.retain(|page| page.access_key != ACTOR || page.path != PATH);
            drop(optional);
            drop(required);
            self.causal_algo_trace.lock().unwrap().push(format!(
                "event=POST_ACK_FLAT_ALGO_SOURCE actor={ACTOR} order_id={acknowledged_order_id} cause={event_id} retired_pages={retired_count} scopes=16 max_reads_per_scope={STATIC_ALGO_READS_PER_SCOPE}"
            ));
            retired_count
        }

        /// Optional deterministic I/O barrier. A GET that starts before ACK
        /// keeps its old raw response even if released after ACK.
        pub(crate) fn hold_next_causal_algo_get(
            &self,
        ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            let entered = Arc::new(tokio::sync::Notify::new());
            let resume = Arc::new(tokio::sync::Notify::new());
            let mut phase = self.causal_algos.lock().unwrap();
            let scenario = phase.as_mut().expect("TRANSCRIPT_ALGO_PHASE_REQUIRED");
            assert!(
                scenario.held_read.is_none(),
                "TRANSCRIPT_ALGO_BARRIER_ALREADY_ARMED"
            );
            scenario.held_read = Some(CausalAlgoReadGate {
                entered: entered.clone(),
                resume: resume.clone(),
            });
            (entered, resume)
        }

        pub(crate) fn unconsumed_reads(&self) -> usize {
            self.reads.lock().unwrap().len()
        }

        pub(crate) fn expect_post(&self, post: RecordedPost) {
            self.posts.lock().unwrap().push(post);
        }

        pub(crate) fn arm_lost_next_create_ack(&self) {
            assert!(
                !self.lose_next_created_ack.swap(true, Ordering::SeqCst),
                "TRANSCRIPT_ACK_LOSS_ALREADY_ARMED"
            );
        }

        pub(crate) fn lost_create_ack_was_consumed(&self) -> bool {
            !self.lose_next_created_ack.load(Ordering::SeqCst)
        }

        pub(crate) fn expect_public_clock(&self, raw_response: Vec<u8>) {
            self.public_clock_reads.lock().unwrap().push(raw_response);
        }

        /// At a proven LOCAL/NOT_STARTED admission denial, retire an unused
        /// recorded exchange-clock reply and install new raw venue bytes.
        /// This changes only the test HTTP boundary, not ClockEvidence.
        pub(crate) fn advance_recorded_public_clock(&self, raw_response: Vec<u8>) -> usize {
            let mut pending = self.public_clock_reads.lock().unwrap();
            let retired = pending.len();
            pending.clear();
            pending.push(raw_response);
            retired
        }

        pub(crate) fn unconsumed_public_clock(&self) -> usize {
            self.public_clock_reads.lock().unwrap().len()
        }

        pub(crate) fn public_clock_deliveries(&self) -> u64 {
            self.public_clock_deliveries.load(Ordering::Acquire)
        }

        /// Only a scripted external clock publisher may replace an unused
        /// raw sample before the FIRST requested clock GET. It never mints
        /// a response on demand or changes clock policy/tolerances.
        /// Once the recorded transport delivered a sample, the publisher
        /// cannot rearm that delivery or inject an unsolicited second one.
        pub(crate) fn refresh_unconsumed_public_clock(
            &self,
            expected_deliveries: u64,
            sample: Vec<u8>,
        ) -> bool {
            let mut queued = self.public_clock_reads.lock().unwrap();
            if queued.len() != 1
                || self.public_clock_deliveries.load(Ordering::Acquire) != expected_deliveries
            {
                return false;
            }
            queued[0] = sample;
            true
        }

        pub(crate) fn unconsumed_posts(&self) -> usize {
            self.posts.lock().unwrap().len()
        }
    }
    #[cfg(test)]
    mod causal_phase_transition_tests {
        use super::*;

        fn source(acked: bool) -> RecordedAccountHttp {
            let feed = RecordedAccountHttp::default();
            *feed.causal_algos.lock().unwrap() = Some(CausalAlgoScenario {
                account: "fixture-observer-a".into(),
                cancel_id: "recorded-stop-cancel-001".into(),
                acked,
                scopes: vec![],
                held_read: None,
                inflight_held_reads: 0,
                source_received_at_ms: 1,
            });
            feed
        }

        #[test]
        fn signed_cancel_ack_is_required_to_retire_a_recorded_inventory() {
            let pre_ack = source(false);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    pre_ack.retire_acked_causal_algo_inventory("fixture-observer-a");
                }))
                .is_err(),
                "cannot advance source phase before ACK"
            );

            let wrong_account = source(true);
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    wrong_account.retire_acked_causal_algo_inventory("fixture-observer-b");
                }))
                .is_err(),
                "another verified UID cannot retire A's inventory"
            );

            let acknowledged = source(true);
            acknowledged.retire_acked_causal_algo_inventory("fixture-observer-a");
            assert!(acknowledged.causal_algos.lock().unwrap().is_none());
            assert!(
                acknowledged
                    .causal_algo_trace
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|line| {
                        line.contains("INVENTORY_PHASE_RETIRED")
                            && line.contains("evidence=CANCEL_ACK")
                    })
            );
        }

        #[test]
        fn pending_late_read_barrier_blocks_inventory_retirement() {
            let held = source(true);
            held.causal_algos
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .held_read = Some(CausalAlgoReadGate {
                entered: Arc::new(tokio::sync::Notify::new()),
                resume: Arc::new(tokio::sync::Notify::new()),
            });
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    held.retire_acked_causal_algo_inventory("fixture-observer-a");
                }))
                .is_err(),
                "a held signed GET must be resumed, not discarded"
            );
        }

        fn exact_query(inst_type: &str, kind: &str) -> Vec<(String, String)> {
            vec![
                ("instType".into(), inst_type.into()),
                ("ordType".into(), kind.into()),
                ("limit".into(), "100".into()),
            ]
        }

        fn recorded_envelope(data: serde_json::Value) -> Vec<u8> {
            serde_json::to_vec(&serde_json::json!({
                "code": "0", "msg": "", "data": data
            }))
            .unwrap()
        }

        fn causal_fixture(feed: &RecordedAccountHttp) {
            let mut before = Vec::new();
            let mut after = Vec::new();
            for inst in ["SWAP", "FUTURES"] {
                for kind in [
                    "conditional",
                    "oco",
                    "trigger",
                    "move_order_stop",
                    "chase",
                    "iceberg",
                    "twap",
                    "smart_iceberg",
                ] {
                    let raw = if inst == "SWAP" && kind == "conditional" {
                        serde_json::json!([{"algoId": "900000002"}, {"algoId": "900000003"}])
                    } else {
                        serde_json::json!([])
                    };
                    let remaining = if inst == "SWAP" && kind == "conditional" {
                        serde_json::json!([{"algoId": "900000003"}])
                    } else {
                        serde_json::json!([])
                    };
                    before.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: exact_query(inst, kind),
                        raw_response: recorded_envelope(raw),
                    });
                    after.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: exact_query(inst, kind),
                        raw_response: recorded_envelope(remaining),
                    });
                }
            }
            feed.expect_causal_algo_inventory("900000002", before, after);
            feed.expect_post(RecordedPost {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/cancel-algos".into(),
                expected_body: serde_json::json!([{
                    "instId": "BTC-USDT-SWAP", "algoId": "900000002"
                }]),
                raw_response: recorded_envelope(serde_json::json!([{
                    "algoId": "900000002", "sCode": "0"
                }])),
            });
        }

        fn signed_inventory_request(actor: &str, kind: &str) -> HttpRequest {
            reqwest::Client::new()
                .get(format!(
                    "https://www.okx.com/api/v5/trade/orders-algo-pending?instType=SWAP&ordType={kind}&limit=100"
                ))
                .header("OK-ACCESS-KEY", actor)
                .build()
                .unwrap()
        }

        fn signed_cancel_request() -> HttpRequest {
            reqwest::Client::new()
                .post("https://www.okx.com/api/v5/trade/cancel-algos")
                .header("OK-ACCESS-KEY", "fixture-executor-a")
                .body(r#"[{"instId":"BTC-USDT-SWAP","algoId":"900000002"}]"#)
                .build()
                .unwrap()
        }

        fn raw_items(response: RawHttpResponse) -> serde_json::Value {
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["data"].clone()
        }

        #[tokio::test]
        async fn causal_algo_phase_keeps_old_inflight_get_until_delivery_after_cancel_ack() {
            let feed = Arc::new(RecordedAccountHttp::default());
            causal_fixture(&feed);
            let deadline = || Instant::now() + std::time::Duration::from_secs(3);

            // The first Status/maintenance observation is not allowed to
            // disappear merely because another read happens before cancel.
            let initial = feed
                .send(
                    signed_inventory_request("fixture-observer-a", "conditional"),
                    deadline(),
                )
                .await
                .unwrap();
            assert_eq!(raw_items(initial).as_array().unwrap().len(), 2);

            let (entered, resume) = feed.hold_next_causal_algo_get();
            let reading_feed = feed.clone();
            let old_read = tokio::spawn(async move {
                reading_feed
                    .send(
                        signed_inventory_request("fixture-observer-a", "conditional"),
                        Instant::now() + std::time::Duration::from_secs(3),
                    )
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
                .await
                .expect("old signed GET must reach its I/O barrier");

            let canceled = feed
                .send(signed_cancel_request(), deadline())
                .await
                .unwrap();
            assert_eq!(raw_items(canceled)[0]["algoId"], "900000002");
            {
                let state = feed.causal_algos.lock().unwrap();
                let phase = state.as_ref().unwrap();
                assert!(phase.acked);
                assert!(phase.held_read.is_none());
                assert_eq!(
                    phase.inflight_held_reads, 1,
                    "taking an I/O gate must not authorize source retirement"
                );
            }
            // After ACK, a NEW signed read sees the other still-live stop.
            let fresh = feed
                .send(
                    signed_inventory_request("fixture-observer-a", "conditional"),
                    deadline(),
                )
                .await
                .unwrap();
            assert_eq!(
                raw_items(fresh),
                serde_json::json!([{
                    "algoId": "900000003"
                }])
            );
            // The late response contains the exact immutable pre-ACK bytes.
            resume.notify_one();
            let stale = tokio::time::timeout(std::time::Duration::from_secs(2), old_read)
                .await
                .expect("old read join deadline")
                .unwrap()
                .unwrap();
            assert_eq!(raw_items(stale).as_array().unwrap().len(), 2);
            assert_eq!(
                feed.causal_algos
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .inflight_held_reads,
                0
            );
            let trace = feed.causal_algo_trace.lock().unwrap();
            let events = trace
                .iter()
                .filter(|line| {
                    line.contains("event=GET_STARTED") || line.contains("event=CANCEL_ACK")
                })
                .collect::<Vec<_>>();
            assert_eq!(events.len(), 4);
            assert!(events[0].contains("stage=BEFORE_ACK"));
            assert!(events[1].contains("stage=BEFORE_ACK"));
            assert!(events[2].contains("event=CANCEL_ACK"));
            assert!(events[3].contains("stage=AFTER_ACK"));
            drop(trace);
            feed.retire_acked_causal_algo_inventory("fixture-observer-a");
            assert!(feed.causal_algos.lock().unwrap().is_none());
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
        }

        #[tokio::test]
        async fn causal_algo_phase_rejects_foreign_cancel_ack_without_switching_a() {
            let feed = source(false);
            feed.expect_post(RecordedPost {
                access_key: "fixture-executor-b".into(),
                path: "/api/v5/trade/cancel-algos".into(),
                expected_body: serde_json::json!([{
                    "instId": "BTC-USDT-SWAP",
                    "algoId": "recorded-stop-cancel-001"
                }]),
                raw_response: recorded_envelope(serde_json::json!([{
                    "algoId": "recorded-stop-cancel-001", "sCode": "0"
                }])),
            });
            let foreign = reqwest::Client::new()
                .post("https://www.okx.com/api/v5/trade/cancel-algos")
                .header("OK-ACCESS-KEY", "fixture-executor-b")
                .body(r#"[{"instId":"BTC-USDT-SWAP","algoId":"recorded-stop-cancel-001"}]"#)
                .build()
                .unwrap();
            let denied = feed
                .send(foreign, Instant::now() + std::time::Duration::from_secs(3))
                .await
                .err()
                .expect("foreign account cannot complete A's cancel event");
            assert!(
                denied
                    .to_string()
                    .contains("TRANSCRIPT_ALGO_CANCEL_ACCOUNT_MISMATCH")
            );
            assert!(!feed.causal_algos.lock().unwrap().as_ref().unwrap().acked);
            assert_eq!(
                feed.outbound_posts.lock().unwrap().len(),
                1,
                "even rejected outbound requests remain visible"
            );
        }

        #[tokio::test]
        async fn causal_algo_phase_rejects_wrong_query_and_excess_reads_without_mutation() {
            let feed = RecordedAccountHttp::default();
            causal_fixture(&feed);
            let deadline = || Instant::now() + std::time::Duration::from_secs(3);
            let wrong = feed
                .send(
                    signed_inventory_request("fixture-observer-a", "unknown"),
                    deadline(),
                )
                .await
                .err()
                .expect("wrong query must reject");
            assert!(wrong.to_string().contains("TRANSCRIPT_ALGO_QUERY_MISMATCH"));
            let other = feed
                .send(
                    signed_inventory_request("fixture-observer-b", "conditional"),
                    deadline(),
                )
                .await
                .err()
                .expect("foreign UID must reject");
            assert!(
                other
                    .to_string()
                    .contains("TRANSCRIPT_ALGO_ACCOUNT_MISMATCH")
            );
            for _ in 0..CAUSAL_ALGO_READS_PER_SCOPE_AND_PHASE {
                let observed = feed
                    .send(
                        signed_inventory_request("fixture-observer-a", "conditional"),
                        deadline(),
                    )
                    .await
                    .unwrap();
                assert_eq!(raw_items(observed).as_array().unwrap().len(), 2);
            }
            let excess = feed
                .send(
                    signed_inventory_request("fixture-observer-a", "conditional"),
                    deadline(),
                )
                .await
                .err()
                .expect("exhausted phase must reject");
            assert!(
                excess
                    .to_string()
                    .contains("TRANSCRIPT_ALGO_PHASE_READ_BUDGET_EXCEEDED")
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
            assert!(!feed.causal_algos.lock().unwrap().as_ref().unwrap().acked);
        }
    }

    #[tokio::test]
    async fn immutable_signed_source_event_supersedes_only_coherent_old_pages() {
        let raw = |stamp: &str| {
            serde_json::to_vec(&serde_json::json!({
                "code":"0","msg":"","data":[{
                    "instType":"SWAP","level":"Lv1","ts":stamp,
                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                }]
            }))
            .unwrap()
        };
        let page = |ts: &str| RecordedGet {
            access_key: "fixture-observer-b".into(),
            path: "/api/v5/account/trade-fee".into(),
            required_query: vec![
                ("instType".into(), "SWAP".into()),
                ("instFamily".into(), "BTC-USDT".into()),
            ],
            raw_response: raw(ts),
        };
        // An inconsistent old external source is a harness failure, never
        // a financial denial or permission to discard inconvenient bytes.
        let conflicting = RecordedAccountHttp::default();
        conflicting.expect_get(page("100"));
        conflicting.expect_optional_get(page("101"));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                conflicting.supersede_queued_signed_source_at_event(
                    "observed-fee-change",
                    "fee-after",
                    page("200"),
                );
            }))
            .is_err(),
            "mixed source generations cannot be retired silently"
        );

        let feed = RecordedAccountHttp::default();
        feed.expect_get(page("100"));
        feed.expect_optional_get(page("100"));
        let retired = feed.supersede_queued_signed_source_at_event(
            "observed-fee-change",
            "fee-after",
            page("200"),
        );
        assert_eq!(retired, 2);
        assert_eq!(feed.unconsumed_reads(), 0);
        let request = || {
            reqwest::Client::new()
            .get("https://www.okx.com/api/v5/account/trade-fee?instType=SWAP&instFamily=BTC-USDT")
            .header("OK-ACCESS-KEY","fixture-observer-b")
            .build().unwrap()
        };
        for _ in 0..3 {
            let observed = feed
                .send(
                    request(),
                    Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&observed.body).unwrap()["data"][0]["ts"],
                "200"
            );
        }
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-b",
                "/api/v5/account/trade-fee",
                "fee-after"
            ),
            3
        );
        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        let trace = feed.immutable_phase_trace.lock().unwrap();
        assert_eq!(trace.len(), 1);
        assert!(
            trace[0].contains("event=SIGNED_SOURCE_SUPERSEDED")
                && trace[0].contains("old_pages=2")
                && trace[0].contains("cause=observed-fee-change")
        );
    }

    // Helpers local to these transport tests: the older causal-algo
    // module intentionally keeps its fixtures private.
    fn post_ack_exact_query(inst: &str, kind: &str) -> Vec<(String, String)> {
        vec![
            ("instType".into(), inst.into()),
            ("ordType".into(), kind.into()),
            ("limit".into(), "100".into()),
        ]
    }

    fn post_ack_raw_envelope(data: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "code": "0", "msg": "", "data": data
        }))
        .expect("signed raw algo source")
    }

    fn post_ack_signed_request(actor: &str, kind: &str) -> HttpRequest {
        reqwest::Client::new()
            .get(format!(
                "https://www.okx.com/api/v5/trade/orders-algo-pending?instType=SWAP&ordType={kind}&limit=100"
            ))
            .header("OK-ACCESS-KEY", actor)
            .build().expect("exact signed algo GET")
    }

    #[tokio::test]
    async fn post_ack_flat_algo_event_reuses_one_bounded_signed_source_per_scope() {
        fn pages() -> Vec<RecordedGet> {
            let mut result = Vec::new();
            for inst in ["SWAP", "FUTURES"] {
                for kind in [
                    "conditional",
                    "oco",
                    "trigger",
                    "move_order_stop",
                    "chase",
                    "iceberg",
                    "twap",
                    "smart_iceberg",
                ] {
                    result.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: post_ack_exact_query(inst, kind),
                        raw_response: post_ack_raw_envelope(serde_json::json!([])),
                    });
                }
            }
            result
        }
        let feed = RecordedAccountHttp::default();
        for page in pages() {
            feed.expect_get(page);
        }
        // The production HTTP transport separately records authenticated
        // order ACKs; this unit targets the transport event boundary only.
        feed.acknowledged_creates.lock().unwrap().push((
            "fixture-executor-a".into(),
            "actual-client-a".into(),
            "venue-a-ack".into(),
        ));
        assert_eq!(
            feed.publish_post_ack_flat_algo_inventory(
                "A_ACK_B_CENTRAL_DENIAL",
                "venue-a-ack",
                pages()
            ),
            16
        );
        assert_eq!(feed.unconsumed_reads(), 0);
        for _ in 0..18 {
            let signed = feed
                .send(
                    post_ack_signed_request("fixture-observer-a", "conditional"),
                    Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
                .expect("event-selected immutable A source");
            assert!(
                serde_json::from_slice::<serde_json::Value>(&signed.body)
                    .expect("raw signed inventory")["data"]
                    .as_array()
                    .expect("signed inventory data array")
                    .is_empty()
            );
        }
        let wrong = feed
            .send(
                post_ack_signed_request("fixture-observer-a", "unknown"),
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
            .err()
            .expect("unknown algo scope must fail closed");
        assert!(
            wrong
                .to_string()
                .contains("TRANSCRIPT_STATIC_ALGO_QUERY_MISMATCH")
        );
        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        let phase = feed.static_algos.lock().unwrap();
        let static_source = phase.as_ref().unwrap();
        assert_eq!(static_source.scopes.len(), 16);
        assert_eq!(
            static_source
                .scopes
                .iter()
                .find(|row| row.query == {
                    let mut q = post_ack_exact_query("SWAP", "conditional");
                    q.sort();
                    q
                })
                .unwrap()
                .reads,
            18
        );
        assert!(
            feed.causal_algo_trace
                .lock()
                .unwrap()
                .iter()
                .any(|line| line.contains("event=POST_ACK_FLAT_ALGO_SOURCE")
                    && line.contains("retired_pages=16"))
        );
    }

    #[test]
    fn post_ack_flat_algo_event_rejects_unobserved_ack_and_changed_old_stop() {
        fn empty_pages() -> Vec<RecordedGet> {
            let mut result = Vec::new();
            for inst in ["SWAP", "FUTURES"] {
                for kind in [
                    "conditional",
                    "oco",
                    "trigger",
                    "move_order_stop",
                    "chase",
                    "iceberg",
                    "twap",
                    "smart_iceberg",
                ] {
                    result.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: post_ack_exact_query(inst, kind),
                        raw_response: post_ack_raw_envelope(serde_json::json!([])),
                    });
                }
            }
            result
        }
        let missing_ack = RecordedAccountHttp::default();
        for page in empty_pages() {
            missing_ack.expect_get(page);
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                missing_ack.publish_post_ack_flat_algo_inventory(
                    "A_ACK_B_CENTRAL_DENIAL",
                    "not-acked",
                    empty_pages(),
                );
            }))
            .is_err(),
            "unconfirmed venue ACK cannot publish a new inventory"
        );
        let changed_old = RecordedAccountHttp::default();
        changed_old.acknowledged_creates.lock().unwrap().push((
            "fixture-executor-a".into(),
            "actual-client-a".into(),
            "venue-a-ack".into(),
        ));
        changed_old.expect_get(RecordedGet {
            access_key: "fixture-observer-a".into(),
            path: "/api/v5/trade/orders-algo-pending".into(),
            required_query: post_ack_exact_query("SWAP", "conditional"),
            raw_response: post_ack_raw_envelope(serde_json::json!([{
                "algoId": "still-live", "state": "live"
            }])),
        });
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                changed_old.publish_post_ack_flat_algo_inventory(
                    "A_ACK_B_CENTRAL_DENIAL",
                    "venue-a-ack",
                    empty_pages(),
                );
            }))
            .is_err(),
            "an old live stop must never be silently replaced with empty"
        );
    }

    #[tokio::test]
    async fn immutable_signed_snapshot_phases_follow_events_not_background_read_counts() {
        fn balance(account: &str, value: &str) -> RecordedGet {
            RecordedGet {
                access_key: account.into(),
                path: "/api/v5/account/balance".into(),
                required_query: vec![],
                raw_response: serde_json::to_vec(&serde_json::json!({
                    "code":"0","msg":"","data":[{"totalEq":value}]
                }))
                .unwrap(),
            }
        }
        fn read(account: &str) -> HttpRequest {
            reqwest::Client::new()
                .get("https://www.okx.com/api/v5/account/balance")
                .header("OK-ACCESS-KEY", account)
                .build()
                .unwrap()
        }
        let feed = RecordedAccountHttp::default();
        feed.expect_immutable_signed_phase("a-before-event", balance("fixture-observer-a", "100"));
        feed.expect_immutable_signed_phase("b-before-event", balance("fixture-observer-b", "250"));
        // Deliberately interleave the two independent UID sources. Any
        // number of concurrent GETs in a phase receives the SAME bytes;
        // neither count nor task scheduling changes exchange truth.
        for account in [
            "fixture-observer-a",
            "fixture-observer-b",
            "fixture-observer-a",
            "fixture-observer-a",
            "fixture-observer-b",
            "fixture-observer-a",
        ] {
            let result = feed
                .send(
                    read(account),
                    Instant::now() + std::time::Duration::from_secs(2),
                )
                .await
                .expect("signed immutable source");
            let raw: serde_json::Value = serde_json::from_slice(&result.body).unwrap();
            assert_eq!(
                raw["data"][0]["totalEq"],
                if account.ends_with("-a") {
                    "100"
                } else {
                    "250"
                }
            );
        }
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-a",
                "/api/v5/account/balance",
                "a-before-event"
            ),
            4
        );
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-b",
                "/api/v5/account/balance",
                "b-before-event"
            ),
            2
        );
        assert_eq!(
            feed.unconsumed_reads(),
            0,
            "immutable signed snapshots do not become arbitrary queued pages"
        );
        // After an independently verified source event, only A transitions;
        // B's bytes remain unchanged. No test SQL or Worker state is edited.
        let prior = feed.advance_immutable_signed_phase(
            "a-before-event",
            "a-after-event",
            balance("fixture-observer-a", "99"),
        );
        assert_eq!(prior, 4);
        let a = feed
            .send(
                read("fixture-observer-a"),
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
            .unwrap();
        let b = feed
            .send(
                read("fixture-observer-b"),
                Instant::now() + std::time::Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&a.body).unwrap()["data"][0]["totalEq"],
            "99"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&b.body).unwrap()["data"][0]["totalEq"],
            "250"
        );
        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        let trace = feed.immutable_phase_trace.lock().unwrap();
        assert_eq!(trace.len(), 3, "two sources installed and one transition");
        assert!(
            trace[2].contains("event=SIGNED_PHASE_ADVANCED")
                && trace[2].contains("observed_reads=4")
                && trace[2].contains("from=a-before-event")
                && trace[2].contains("to=a-after-event")
        );
    }

    #[tokio::test]
    async fn immutable_signed_phase_rejects_wrong_query_and_preserves_source() {
        let feed = RecordedAccountHttp::default();
        feed.expect_immutable_signed_phase(
            "fee-before",
            RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/account/trade-fee".into(),
                required_query: vec![
                    ("instType".into(), "SWAP".into()),
                    ("instFamily".into(), "BTC-USDT".into()),
                ],
                raw_response: serde_json::to_vec(&serde_json::json!({
                    "code":"0","msg":"","data":[{
                        "ts":"1793840000000","feeGroup":[{"maker":"0.0005","taker":"0.0005"}]
                    }]
                }))
                .unwrap(),
            },
        );
        let bad = reqwest::Client::new()
            .get("https://www.okx.com/api/v5/account/trade-fee?instType=SPOT&instFamily=BTC-USDT")
            .header("OK-ACCESS-KEY", "fixture-observer-a")
            .build()
            .unwrap();
        let denial = match feed
            .send(bad, Instant::now() + std::time::Duration::from_secs(2))
            .await
        {
            Ok(_) => panic!("wrong scope cannot borrow SWAP raw fees"),
            Err(denial) => denial,
        };
        assert!(
            denial
                .to_string()
                .contains("TRANSCRIPT_IMMUTABLE_QUERY_MISMATCH")
        );
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-a",
                "/api/v5/account/trade-fee",
                "fee-before"
            ),
            0
        );
        let good = reqwest::Client::new()
            .get("https://www.okx.com/api/v5/account/trade-fee?instType=SWAP&instFamily=BTC-USDT")
            .header("OK-ACCESS-KEY", "fixture-observer-a")
            .build()
            .unwrap();
        let raw = feed
            .send(good, Instant::now() + std::time::Duration::from_secs(2))
            .await
            .expect("signed correct scope");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&raw.body).unwrap()["data"][0]["ts"],
            "1793840000000"
        );
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-a",
                "/api/v5/account/trade-fee",
                "fee-before"
            ),
            1
        );
        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn immutable_signed_phase_budgets_expire_without_refresh_on_get() {
        use std::time::Duration;
        let fee = |phase: &str| RecordedGet {
            access_key: "fixture-observer-a".into(),
            path: "/api/v5/account/balance".into(),
            required_query: vec![],
            raw_response: serde_json::to_vec(&serde_json::json!({
                "code":"0","data":[{"phase":phase}]
            }))
            .unwrap(),
        };
        let read = || {
            reqwest::Client::new()
                .get("https://www.okx.com/api/v5/account/balance")
                .header("OK-ACCESS-KEY", "fixture-observer-a")
                .build()
                .unwrap()
        };
        let deadline = || Instant::now() + Duration::from_secs(2);
        let feed = RecordedAccountHttp::default();
        // A three-read event is accepted exactly three times.
        feed.expect_immutable_signed_phase_bounded(
            "before-budget",
            fee("before"),
            3,
            Duration::from_secs(3),
        );
        for _ in 0..3 {
            feed.send(read(), deadline())
                .await
                .expect("within declared budget");
        }
        let excess = feed
            .send(read(), deadline())
            .await
            .err()
            .expect("over-budget read must be denied");
        assert!(
            excess
                .to_string()
                .contains("TRANSCRIPT_IMMUTABLE_PHASE_READ_BUDGET_EXCEEDED")
        );
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-a",
                "/api/v5/account/balance",
                "before-budget"
            ),
            3
        );
        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        assert_eq!(
            feed.advance_immutable_signed_phase_bounded(
                "before-budget",
                "short-lived",
                fee("after"),
                4,
                Duration::from_millis(1)
            ),
            3
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        let expired = feed
            .send(read(), deadline())
            .await
            .err()
            .expect("expired raw source cannot be refreshed by reading it");
        assert!(
            expired
                .to_string()
                .contains("TRANSCRIPT_IMMUTABLE_PHASE_EXPIRED")
        );
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-a",
                "/api/v5/account/balance",
                "short-lived"
            ),
            0
        );
        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        // A new event explicitly republishes the source with a NEW budget.
        assert_eq!(
            feed.advance_immutable_signed_phase_bounded(
                "short-lived",
                "recovered",
                fee("fresh"),
                2,
                Duration::from_secs(3)
            ),
            0
        );
        let recovered = feed.send(read(), deadline()).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&recovered.body).unwrap()["data"][0]["phase"],
            "fresh"
        );
        let denied = feed.denied_routes.lock().unwrap();
        assert!(denied.iter().any(|x| x.contains("PHASE_EXPIRED")));
        assert!(denied.iter().any(|x| x.contains("READ_BUDGET_EXCEEDED")));
        let trace = feed.immutable_phase_trace.lock().unwrap();
        assert_eq!(trace.len(), 3);
        assert!(trace[0].contains("max_reads=3") && trace[0].contains("ttl_ms=3000"));
        assert!(trace[1].contains("max_reads=4") && trace[1].contains("ttl_ms=1"));
        assert!(trace[2].contains("max_reads=2") && trace[2].contains("ttl_ms=3000"));
    }

    #[tokio::test]
    async fn immutable_signed_pending_inventory_requires_explicit_phase_and_retirement() {
        use std::time::Duration;
        let feed = RecordedAccountHttp::default();
        let page = || RecordedGet {
            access_key: "fixture-observer-a".into(),
            path: "/api/v5/trade/orders-pending".into(),
            required_query: vec![("limit".into(), "100".into())],
            raw_response: b"{\"code\":\"0\",\"msg\":\"\",\"data\":[]}".to_vec(),
        };
        let read = |limit: &str| {
            reqwest::Client::new()
                .get(format!(
                    "https://www.okx.com/api/v5/trade/orders-pending?limit={limit}"
                ))
                .header("OK-ACCESS-KEY", "fixture-observer-a")
                .build()
                .unwrap()
        };
        let deadline = || Instant::now() + Duration::from_secs(2);
        feed.expect_immutable_signed_phase_bounded("cold-entry", page(), 2, Duration::from_secs(5));
        let wrong = feed
            .send(read("50"), deadline())
            .await
            .err()
            .expect("expected recorded transport denial");
        assert!(
            wrong
                .to_string()
                .contains("TRANSCRIPT_IMMUTABLE_QUERY_MISMATCH")
        );
        for _ in 0..2 {
            let response = feed.send(read("100"), deadline()).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response.body).unwrap()["data"],
                serde_json::json!([])
            );
        }
        assert!(
            feed.send(read("100"), deadline())
                .await
                .err()
                .expect("expected recorded transport denial")
                .to_string()
                .contains("TRANSCRIPT_IMMUTABLE_PHASE_READ_BUDGET_EXCEEDED")
        );
        assert_eq!(
            feed.advance_immutable_signed_phase_bounded(
                "cold-entry",
                "post-ack",
                page(),
                2,
                Duration::from_secs(5)
            ),
            2
        );
        feed.send(read("100"), deadline()).await.unwrap();
        assert_eq!(
            feed.retire_immutable_signed_phase(
                "post-ack",
                "fixture-observer-a",
                "/api/v5/trade/orders-pending"
            ),
            1
        );
        // The next recorded inventory is independent, not a fallback to
        // either the empty pre-ACK phase or an invented venue result.
        feed.expect_optional_get(RecordedGet {
            raw_response: b"{\"code\":\"0\",\"msg\":\"\",\"data\":[{\"ordId\":\"ack-bound-001\"}]}"
                .to_vec(),
            ..page()
        });
        let next = feed.send(read("100"), deadline()).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&next.body).unwrap()["data"][0]["ordId"],
            "ack-bound-001"
        );
        assert!(
            feed.send(read("100"), deadline())
                .await
                .err()
                .expect("expected recorded transport denial")
                .to_string()
                .contains("TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED")
        );
        assert!(feed.outbound_posts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn immutable_signed_phase_unexpected_post_is_counted_not_replayed() {
        let feed = RecordedAccountHttp::default();
        feed.expect_immutable_signed_phase(
            "no-post",
            RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/account/balance".into(),
                required_query: vec![],
                raw_response: b"{\"code\":\"0\",\"data\":[]}".to_vec(),
            },
        );
        let request = reqwest::Client::new()
            .post("https://www.okx.com/api/v5/trade/order")
            .header("OK-ACCESS-KEY", "fixture-executor-a")
            .json(&serde_json::json!({"clOrdId":"test0001"}))
            .build()
            .unwrap();
        let error = feed
            .send(request, Instant::now() + std::time::Duration::from_secs(2))
            .await
            .err()
            .expect("unmatched signed POST must fail");
        assert!(error.to_string().contains("TRANSCRIPT_UNEXPECTED_POST"));
        assert_eq!(
            feed.outbound_posts.lock().unwrap().len(),
            1,
            "even rejected outbound attempts are counted"
        );
        assert_eq!(
            feed.immutable_signed_phase_reads(
                "fixture-observer-a",
                "/api/v5/account/balance",
                "no-post"
            ),
            0
        );
    }

    impl HttpTransport for RecordedAccountHttp {
        fn send(&self, request: HttpRequest, deadline: Instant) -> HttpSend<'_> {
            Box::pin(async move {
                if request.method() != "GET" && request.method() != "POST" {
                    return Err(OkxError::Response("TRANSCRIPT_METHOD_MISMATCH".into()));
                }
                // Public time is intentionally unsigned. One explicitly recorded
                // raw response is consumed by the production clock parser.
                // A missing/extra clock read is a hard transcript failure.
                if request.method() == "GET" && request.url().path() == "/api/v5/public/time" {
                    let mut queued = self.public_clock_reads.lock().unwrap();
                    if queued.is_empty() {
                        self.denied_routes
                            .lock()
                            .unwrap()
                            .push("TRANSCRIPT_CLOCK_EXHAUSTED".into());
                        return Err(OkxError::Response("TRANSCRIPT_CLOCK_EXHAUSTED".into()));
                    }
                    let body = queued.remove(0);
                    self.public_clock_deliveries.fetch_add(1, Ordering::AcqRel);
                    drop(queued);
                    self.requests
                        .lock()
                        .unwrap()
                        .push("public:/api/v5/public/time".into());
                    return Ok(RawHttpResponse {
                        status: HttpStatusCode::OK,
                        headers: Default::default(),
                        body,
                    });
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
                if request.method() == "GET" && path == "/api/v5/account/balance" {
                    self.signed_balance_reads.lock().unwrap().push((
                        key.to_owned(),
                        format!("{:?}", tokio::task::try_id()),
                        chrono::Utc::now().timestamp_millis(),
                    ));
                }
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
                    if path != "/api/v5/trade/order"
                        && path != "/api/v5/trade/cancel-algos"
                        && path != "/api/v5/trade/cancel-order"
                    {
                        return Err(OkxError::Response(
                            "TRANSCRIPT_POST_ROUTE_UNRECOGNIZED".into(),
                        ));
                    }
                    let mut expected = self.posts.lock().unwrap();
                    let index = expected
                        .iter()
                        .position(|item| item.access_key == key && item.path == path)
                        .ok_or_else(|| OkxError::Response("TRANSCRIPT_UNEXPECTED_POST".into()))?;
                    let post = &expected[index];
                    let mut body = post.expected_body.clone();
                    let mut client_id = None;
                    if path == "/api/v5/trade/order" {
                        let id = observed
                            .get("clOrdId")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                OkxError::Response("TRANSCRIPT_CLIENT_ID_MISSING".into())
                            })?;
                        if id.is_empty()
                            || id.len() > 32
                            || !id.bytes().all(|b| b.is_ascii_alphanumeric())
                        {
                            return Err(OkxError::Response("TRANSCRIPT_CLIENT_ID_INVALID".into()));
                        }
                        if body.get("clOrdId").and_then(serde_json::Value::as_str)
                            == Some("__REQUEST_CL_ORD_ID__")
                        {
                            body["clOrdId"] = id.into();
                        }
                        if let Some(attached) = body
                            .get_mut("attachAlgoOrds")
                            .and_then(serde_json::Value::as_array_mut)
                        {
                            for algo in attached {
                                if algo
                                    .get("attachAlgoClOrdId")
                                    .and_then(serde_json::Value::as_str)
                                    == Some("__REQUEST_ATTACH_ALGO_ID__")
                                {
                                    let prefix = id.get(..31).ok_or_else(|| {
                                        OkxError::Response(
                                            "TRANSCRIPT_ATTACH_CLIENT_ID_INVALID".into(),
                                        )
                                    })?;
                                    algo["attachAlgoClOrdId"] = format!("s{prefix}").into();
                                }
                            }
                        }
                        client_id = Some(id.to_owned());
                    }
                    if body != observed {
                        return Err(OkxError::Response("TRANSCRIPT_POST_BODY_MISMATCH".into()));
                    }
                    let recorded = expected.remove(index);
                    drop(expected);
                    // Binding a dynamic client order ID to pre-recorded raw
                    // response bytes does not generate fills, fees or balances.
                    let response = String::from_utf8(recorded.raw_response)
                        .map_err(|_| OkxError::Response("TRANSCRIPT_RESPONSE_UTF8".into()))?;
                    let response = if let Some(client_id) = client_id.as_deref() {
                        response.replace("__REQUEST_CL_ORD_ID__", client_id)
                    } else {
                        response
                    };
                    if path == "/api/v5/trade/cancel-algos" {
                        let parsed: serde_json::Value = serde_json::from_str(&response)?;
                        let observed_id = observed
                            .get(0)
                            .and_then(|row| row.get("algoId"))
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| {
                                OkxError::Response("TRANSCRIPT_CANCEL_ID_MISSING".into())
                            })?;
                        if parsed["data"][0]["sCode"] != "0"
                            || parsed["data"][0]["algoId"] != observed_id
                        {
                            return Err(OkxError::Response(
                                "TRANSCRIPT_CANCEL_ACK_MISMATCH".into(),
                            ));
                        }
                        let mut phase = self.causal_algos.lock().unwrap();
                        if let Some(scenario) = phase.as_mut() {
                            // A signed B cancel must never select A's inventory
                            // merely because a venue algo ID is identical.
                            if verified_fixture_uid(key)
                                != verified_fixture_uid(scenario.account.as_str())
                            {
                                return Err(OkxError::Response(
                                    "TRANSCRIPT_ALGO_CANCEL_ACCOUNT_MISMATCH".into(),
                                ));
                            }
                            if scenario.cancel_id != observed_id || scenario.acked {
                                return Err(OkxError::Response(
                                    "TRANSCRIPT_ALGO_CANCEL_EVENT_MISMATCH".into(),
                                ));
                            }
                            scenario.acked = true;
                            self.causal_algo_trace.lock().unwrap().push(format!(
                                "event=CANCEL_ACK uid=account-001 actor={key} route={path} cancel_id={observed_id} at_ms={}",
                                chrono::Utc::now().timestamp_millis()
                            ));
                        }
                    }
                    if path == "/api/v5/trade/cancel-order" {
                        let parsed: serde_json::Value = serde_json::from_str(&response)?;
                        let submitted = observed.get("clOrdId").and_then(serde_json::Value::as_str);
                        if submitted.is_none()
                            || parsed["data"][0]["clOrdId"].as_str() != submitted
                            || parsed["data"][0]["sCode"] != "0"
                            || parsed["data"][0]["ordId"].as_str().is_none()
                        {
                            return Err(OkxError::Response(
                                "TRANSCRIPT_CANCEL_ORDER_ACK_UNBOUND".into(),
                            ));
                        }
                        // An authenticated cancel ACK is NOT terminal proof.
                    }
                    if path == "/api/v5/trade/order" {
                        let parsed: serde_json::Value = serde_json::from_str(&response)?;
                        let rows = parsed["data"].as_array().ok_or_else(|| {
                            OkxError::Response("TRANSCRIPT_CREATE_ACK_UNBOUND".into())
                        })?;
                        if rows.len() != 1 || rows[0]["clOrdId"].as_str() != client_id.as_deref() {
                            return Err(OkxError::Response("TRANSCRIPT_CREATE_ACK_UNBOUND".into()));
                        }
                        // An explicit signed venue rejection is valid raw
                        // input. Do not invent a successful order ID for it;
                        // the production PlaceAckVerdict must decide that
                        // REJECTED is different from an unbound/missing ACK.
                        if let Some(code) = rows[0]["sCode"].as_str()
                            && !code.is_empty()
                            && code != "0"
                        {
                            return Ok(RawHttpResponse {
                                status: HttpStatusCode::OK,
                                headers: Default::default(),
                                body: response.into_bytes(),
                            });
                        }
                        if rows[0]["sCode"].as_str() != Some("0") {
                            return Err(OkxError::Response("TRANSCRIPT_CREATE_ACK_UNBOUND".into()));
                        }
                        let ord_id = rows[0]["ordId"]
                            .as_str()
                            .filter(|id| !id.is_empty())
                            .ok_or_else(|| {
                                OkxError::Response("TRANSCRIPT_CREATE_ORDER_ID_MISSING".into())
                            })?;
                        self.acknowledged_creates.lock().unwrap().push((
                            key.to_owned(),
                            client_id.ok_or_else(|| {
                                OkxError::Response("TRANSCRIPT_CLIENT_ID_MISSING".into())
                            })?,
                            ord_id.to_owned(),
                        ));
                        // The venue ACK was genuine, but its delivery is
                        // lost after the authenticated create was sent. Never
                        // replay this POST to "repair" a test connection.
                        if self.lose_next_created_ack.swap(false, Ordering::SeqCst) {
                            return Err(OkxError::Response(
                                "TRANSCRIPT_POST_ACK_LOST_AFTER_SEND".into(),
                            ));
                        }
                    }
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
                // A signed GET must select exactly its own prior POST; a
                // different account's ACK, client ID or recorded order fails.
                let requested_client = if path == "/api/v5/trade/order" {
                    let ids = request
                        .url()
                        .query_pairs()
                        .filter(|(name, _)| name == "clOrdId")
                        .map(|(_, value)| value.into_owned())
                        .collect::<Vec<_>>();
                    if ids.len() != 1 || ids[0].is_empty() {
                        return Err(OkxError::Response(
                            "TRANSCRIPT_ORDER_READBACK_ID_MISMATCH".into(),
                        ));
                    }
                    Some(ids[0].clone())
                } else {
                    None
                };
                // A queued readback for a second ACKed order may coexist with
                // another order's immutable phase. Only exact query AND signed
                // ACK binding may select that queued page; a foreign ID still
                // fails closed and cannot borrow the first order's snapshot.
                let queued_matches_request = {
                    let matches_page = |page: &RecordedGet| {
                        page.access_key == key
                            && page.path == path
                            && page.required_query.iter().all(|(name, expected)| {
                                request.url().query_pairs().any(|(k, v)| {
                                    k.as_ref() == name.as_str() && v.as_ref() == expected.as_str()
                                })
                            })
                            && requested_client.as_ref().is_none_or(|client| {
                                recorded_order_source_matches(
                                    &self.acknowledged_creates.lock().unwrap(),
                                    key,
                                    client,
                                    &page.raw_response,
                                )
                            })
                    };
                    self.reads.lock().unwrap().iter().any(&matches_page)
                        || self
                            .optional_reads
                            .lock()
                            .unwrap()
                            .iter()
                            .any(&matches_page)
                };
                // Read-only authenticated snapshots are immutable WITHIN
                // a named phase, even if financial capture races Preview.
                // A different signer, route or query must fail closed.
                // No response is minted and no source time is refreshed.
                let immutable_result = {
                    let mut phases = self.immutable_signed_phases.lock().unwrap();
                    let matching = phases
                        .iter()
                        .enumerate()
                        .filter_map(|(index, item)| {
                            (item.source.access_key == key && item.source.path == path && {
                                let mut required = item.source.required_query.clone();
                                required.sort();
                                let mut sent = request
                                    .url()
                                    .query_pairs()
                                    .map(|(name, value)| (name.into_owned(), value.into_owned()))
                                    .collect::<Vec<_>>();
                                sent.sort();
                                required == sent
                            })
                            .then_some(index)
                        })
                        .collect::<Vec<_>>();
                    assert!(matching.len() <= 1, "TRANSCRIPT_IMMUTABLE_AMBIGUOUS_QUERY");
                    if let Some(index) = matching.first() {
                        let phase = &mut phases[*index];
                        if phase.published_at.elapsed() >= phase.ttl {
                            Some(Err("TRANSCRIPT_IMMUTABLE_PHASE_EXPIRED"))
                        } else if phase.deliveries >= phase.max_deliveries {
                            Some(Err("TRANSCRIPT_IMMUTABLE_PHASE_READ_BUDGET_EXCEEDED"))
                        } else if path == "/api/v5/trade/order"
                            && !requested_client.as_ref().is_some_and(|client| {
                                recorded_order_source_matches(
                                    &self.acknowledged_creates.lock().unwrap(),
                                    key,
                                    client,
                                    &phase.source.raw_response,
                                )
                            })
                        {
                            Some(Err("TRANSCRIPT_IMMUTABLE_ORDER_ACK_UNBOUND"))
                        } else {
                            phase.deliveries += 1;
                            Some(Ok(phase.source.raw_response.clone()))
                        }
                    } else if phases
                        .iter()
                        .any(|item| item.source.access_key == key && item.source.path == path)
                    {
                        if queued_matches_request {
                            None
                        } else {
                            Some(Err("TRANSCRIPT_IMMUTABLE_QUERY_MISMATCH"))
                        }
                    } else {
                        None
                    }
                };
                if let Some(result) = immutable_result {
                    let body = result.map_err(|reason| {
                        self.denied_routes
                            .lock()
                            .unwrap()
                            .push(format!("{reason}:{key}:{path}"));
                        OkxError::Response(format!("{reason}:{key}:{path}"))
                    })?;
                    let body = if path == "/api/v5/trade/order" {
                        let client = requested_client.as_deref().ok_or_else(|| {
                            OkxError::Response("TRANSCRIPT_ORDER_READBACK_ID_MISSING".into())
                        })?;
                        String::from_utf8(body)
                            .map_err(|_| {
                                OkxError::Response("TRANSCRIPT_ORDER_READBACK_UTF8".into())
                            })?
                            .replace("__REQUEST_CL_ORD_ID__", client)
                            .into_bytes()
                    } else {
                        body
                    };
                    self.requests.lock().unwrap().push(format!(
                        "{key}:{path}?{}",
                        request.url().query().unwrap_or_default()
                    ));
                    if path == "/api/v5/account/trade-fee" {
                        let raw: serde_json::Value = serde_json::from_slice(&body)?;
                        let timestamp = raw["data"][0]["ts"].as_str().ok_or_else(|| {
                            OkxError::Response("TRANSCRIPT_FEE_EXCHANGE_TS_MISSING".into())
                        })?;
                        self.delivered_fees.lock().unwrap().push((
                            key.to_owned(),
                            timestamp.into(),
                            chrono::Utc::now().timestamp_millis(),
                        ));
                    }
                    return Ok(RawHttpResponse {
                        status: HttpStatusCode::OK,
                        headers: Default::default(),
                        body,
                    });
                }
                // Unlike ordered one-shot pages, a causal inventory returns
                // the immutable phase snapshot to every authorized competing
                // GET. Its phase switches only at the signed cancel ACK.
                if path == "/api/v5/trade/orders-algo-pending" {
                    let selected = {
                        let mut phase = self.causal_algos.lock().unwrap();
                        if let Some(scenario) = phase.as_mut().filter(|scenario| {
                            verified_fixture_uid(key).is_some_and(|uid| {
                                verified_fixture_uid(scenario.account.as_str()) == Some(uid)
                            })
                        }) {
                            let mut query = request
                                .url()
                                .query_pairs()
                                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                                .collect::<Vec<_>>();
                            query.sort();
                            let scope = scenario
                                .scopes
                                .iter_mut()
                                .find(|scope| scope.query == query)
                                .ok_or_else(|| {
                                    OkxError::Response("TRANSCRIPT_ALGO_QUERY_MISMATCH".into())
                                })?;
                            let acked = scenario.acked;
                            let count = if acked {
                                &mut scope.after_reads
                            } else {
                                &mut scope.before_reads
                            };
                            if *count >= CAUSAL_ALGO_READS_PER_SCOPE_AND_PHASE {
                                return Err(OkxError::Response(
                                    "TRANSCRIPT_ALGO_PHASE_READ_BUDGET_EXCEEDED".into(),
                                ));
                            }
                            *count += 1;
                            let raw = if acked {
                                scope.after_ack.clone()
                            } else {
                                scope.before_ack.clone()
                            };
                            let stage = if acked { "AFTER_ACK" } else { "BEFORE_ACK" };
                            let digest = format!("{:x}", Sha256::digest(&raw));
                            self.causal_algo_trace.lock().unwrap().push(format!(
                                "event=GET_STARTED uid=account-001 actor={key} route={path} query={query:?} stage={stage} source_received_at_ms={} read={} digest={digest}",
                                scenario.source_received_at_ms, *count
                            ));
                            let held = scenario.held_read.take();
                            if held.is_some() {
                                scenario.inflight_held_reads += 1;
                            }
                            Some((raw, held))
                        } else {
                            // UID B never inherits UID A's cancel-dependent
                            // view. Each B GET matches a signed raw peer scope.
                            let mut peer = self.static_algos.lock().unwrap();
                            if let Some(other) = peer.as_mut().filter(|other| {
                                verified_fixture_uid(key).is_some_and(|uid| {
                                    verified_fixture_uid(other.account.as_str()) == Some(uid)
                                })
                            }) {
                                let mut query = request
                                    .url()
                                    .query_pairs()
                                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                                    .collect::<Vec<_>>();
                                query.sort();
                                let source_received_at_ms = other.source_received_at_ms;
                                let scope = other
                                    .scopes
                                    .iter_mut()
                                    .find(|scope| scope.query == query)
                                    .ok_or_else(|| {
                                        OkxError::Response(
                                            "TRANSCRIPT_STATIC_ALGO_QUERY_MISMATCH".into(),
                                        )
                                    })?;
                                if scope.reads >= STATIC_ALGO_READS_PER_SCOPE {
                                    return Err(OkxError::Response(
                                        "TRANSCRIPT_STATIC_ALGO_READ_BUDGET_EXCEEDED".into(),
                                    ));
                                }
                                scope.reads += 1;
                                let read = scope.reads;
                                let body = scope.response.clone();
                                let digest = format!("{:x}", Sha256::digest(&body));
                                let uid = verified_fixture_uid(key)
                                    .expect("static algo verified read UID");
                                self.causal_algo_trace.lock().unwrap().push(format!(
                                    "event=GET_STARTED uid={uid} actor={key} route={path} query={query:?} stage=IMMUTABLE_STATIC source_received_at_ms={source_received_at_ms} read={read} digest={digest}"
                                ));
                                Some((body, None))
                            } else {
                                if phase.is_some() && !scripted {
                                    self.causal_algo_trace.lock().unwrap().push(format!(
                                        "event=GET_DENIED actor={key} uid={uid} route={path} reason=TRANSCRIPT_ALGO_ACCOUNT_MISMATCH"
                                    ));
                                    return Err(OkxError::Response(
                                        "TRANSCRIPT_ALGO_ACCOUNT_MISMATCH".into(),
                                    ));
                                }
                                None
                            }
                        }
                    };
                    if let Some((body, held)) = selected {
                        if let Some(gate) = held {
                            let _inflight = CausalAlgoReadInFlight(self);
                            gate.entered.notify_one();
                            tokio::time::timeout_at(
                                tokio::time::Instant::from_std(deadline),
                                gate.resume.notified(),
                            )
                            .await
                            .map_err(|_| {
                                OkxError::Response("TRANSCRIPT_ALGO_HELD_READ_TIMEOUT".into())
                            })?;
                        }
                        self.requests.lock().unwrap().push(format!(
                            "{key}:{path}?{}",
                            request.url().query().unwrap_or_default()
                        ));
                        return Ok(RawHttpResponse {
                            status: HttpStatusCode::OK,
                            headers: Default::default(),
                            body,
                        });
                    }
                }
                if scripted {
                    let matches_page = |page: &RecordedGet| {
                        page.access_key == key
                            && page.path == path
                            && page.required_query.iter().all(|(name, expected)| {
                                request.url().query_pairs().any(|(k, v)| {
                                    k.as_ref() == name.as_str() && v.as_ref() == expected.as_str()
                                })
                            })
                            && requested_client.as_ref().is_none_or(|client| {
                                recorded_order_source_matches(
                                    &self.acknowledged_creates.lock().unwrap(),
                                    key,
                                    client,
                                    &page.raw_response,
                                )
                            })
                    };
                    let mut required = self.reads.lock().unwrap();
                    let page = if let Some(index) = required.iter().position(&matches_page) {
                        required.remove(index)
                    } else {
                        drop(required);
                        let mut optional = self.optional_reads.lock().unwrap();
                        let index = optional.iter().position(&matches_page).ok_or_else(|| {
                            let missing = format!(
                                "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED:{key}:{path}?{}:method={}:task={:?}:clOrdId={:?}",
                                request.url().query().unwrap_or_default(),
                                request.method(),tokio::task::try_id(),requested_client
                            );
                            self.denied_routes.lock().unwrap().push(missing.clone());
                            OkxError::Response(missing)
                        })?;
                        optional.remove(index)
                    };
                    // Order readback is an independently recorded raw GET,
                    // not a synthetic response to POST. Only its dynamic
                    // clOrdId is bound to the one actually observed executor
                    // request; no balances/fills/protection are invented.
                    let body = if path == "/api/v5/trade/order" {
                        let client_id = requested_client.as_deref().ok_or_else(|| {
                            OkxError::Response("TRANSCRIPT_ORDER_READBACK_ID_MISSING".into())
                        })?;
                        let raw = String::from_utf8(page.raw_response).map_err(|_| {
                            OkxError::Response("TRANSCRIPT_ORDER_READBACK_UTF8".into())
                        })?;
                        // Source association was checked before consumption.
                        // Only this observed clOrdId is bound to immutable bytes.
                        raw.replace("__REQUEST_CL_ORD_ID__", client_id).into_bytes()
                    } else if matches!(
                        path,
                        "/api/v5/trade/fills-history" | "/api/v5/account/bills-archive"
                    ) {
                        bind_recorded_history_ids(
                            &self.acknowledged_creates.lock().unwrap(),
                            key,
                            page.raw_response,
                        )?
                    } else {
                        page.raw_response
                    };
                    if path == "/api/v5/account/trade-fee" {
                        let raw: serde_json::Value = serde_json::from_slice(&body)?;
                        let exchange_ts = raw["data"][0]["ts"].as_str().ok_or_else(|| {
                            OkxError::Response("TRANSCRIPT_FEE_EXCHANGE_TS_MISSING".into())
                        })?;
                        self.delivered_fees.lock().unwrap().push((
                            key.to_owned(),
                            exchange_ts.to_owned(),
                            chrono::Utc::now().timestamp_millis(),
                        ));
                    }
                    #[cfg(test)]
                    if path == "/api/v5/trade/orders-algo-pending"
                        && std::env::var("OKX_T09_4_SOURCE_COHERENCE_PROBE").as_deref() == Ok("1")
                        && request
                            .url()
                            .query_pairs()
                            .any(|(k, v)| k == "ordType" && v == "conditional")
                    {
                        let parsed = serde_json::from_slice::<serde_json::Value>(&body)
                            .unwrap_or(serde_json::Value::Null);
                        let ids = parsed["data"]
                            .as_array()
                            .map(|rows| {
                                rows.iter()
                                    .filter_map(|row| row["algoId"].as_str())
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        eprintln!(
                            "T09_4_SIGNED_STOP_PAGE_V1:{}",
                            serde_json::json!({
                                "actor":key,
                                "algo_ids":ids,
                                "time_ms":chrono::Utc::now().timestamp_millis(),
                            })
                        );
                    }
                    self.requests.lock().unwrap().push(format!(
                        "{key}:{path}?{}",
                        request.url().query().unwrap_or_default()
                    ));
                    return Ok(RawHttpResponse {
                        status: HttpStatusCode::OK,
                        headers: Default::default(),
                        body,
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
                            "liab": "0", "availEq": "100", "availBal": "100",
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
                        self.denied_routes
                            .lock()
                            .unwrap()
                            .push(format!("TRANSCRIPT_UNEXPECTED_REQUEST:{key}:{path}"));
                        return Err(OkxError::Response("TRANSCRIPT_UNEXPECTED_REQUEST".into()));
                    }
                };
                // A transcript has explicitly bounded reads, not an infinite
                // fake account endpoint. Concurrent polls can reorder safely.
                let request_key =
                    format!("{key}:{path}?{}", request.url().query().unwrap_or_default());
                // The production T09 assembly runs background two-second
                // capture, multi-account census and post-HELD rereads on the
                // same recorded account. The transcript permits a finite 64
                // identical raw source responses per exact signed query.
                // There is no live fallback; unknown routes still hard-fail.
                let max_repeats = match path {
                    "/api/v5/account/config"
                    | "/api/v5/account/balance"
                    | "/api/v5/account/positions"
                    | "/api/v5/trade/orders-pending"
                    | "/api/v5/trade/orders-algo-pending"
                    | "/api/v5/trade/fills-history"
                    | "/api/v5/account/bills-archive" => {
                        // Only the explicitly opt-in long integration chain
                        // has extra finite raw observations. This is NOT an
                        // exchange rate-limit override or a live HTTP fallback.
                        if std::env::var("OKX_T09_4_MANAGED_ADD_PROBE").as_deref() == Ok("1") {
                            192
                        } else {
                            64
                        }
                    }
                    _ => 0,
                };
                let probe_pending_consumers = path == "/api/v5/trade/orders-pending"
                    && std::env::var("OKX_T10_06_CONCURRENT_PROBE").as_deref() == Ok("1");
                if probe_pending_consumers {
                    self.t10_06_pending_consumers.lock().unwrap().push((
                        format!("{:?}", tokio::task::try_id()),
                        chrono::Utc::now().timestamp_millis(),
                    ));
                }
                let mut observed = self.requests.lock().unwrap();
                let consumed = observed.iter().filter(|v| **v == request_key).count();
                if consumed >= max_repeats {
                    if probe_pending_consumers {
                        let events = self.t10_06_pending_consumers.lock().unwrap();
                        let mut by_task = std::collections::BTreeMap::<String, usize>::new();
                        for (task, _) in events.iter() {
                            *by_task.entry(task.clone()).or_default() += 1;
                        }
                        eprintln!(
                            "T10_06_PENDING_CONSUMERS_V1:{}",
                            serde_json::json!({
                                "actor": key,
                                "route": path,
                                "query": request.url().query().unwrap_or_default(),
                                "read_limit": max_repeats,
                                "accepted_reads": consumed,
                                "attempts": events.len(),
                                "first_attempt_ms": events.first().map(|(_, at)| at),
                                "last_attempt_ms": events.last().map(|(_, at)| at),
                                "reads_by_tokio_task": by_task,
                            })
                        );
                    }
                    return Err(OkxError::Response(format!(
                        "TRANSCRIPT_REPEAT_EXCEEDED:{request_key}:count={consumed}:limit={max_repeats}"
                    )));
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
    type RecordedPublicSubscriptions = Arc<Mutex<Vec<(serde_json::Value, Vec<String>)>>>;

    pub(crate) struct RecordedWsConnector {
        pub(crate) sent: Arc<Mutex<Vec<serde_json::Value>>>,
        /// Expected subscribe args and one-shot raw ACK/data frames. No
        /// snapshots or readied flags are supplied to the runtime directly.
        public_subscriptions: RecordedPublicSubscriptions,
        public_injection: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<String>>>,
        public_sender: tokio::sync::mpsc::UnboundedSender<String>,
    }
    impl Default for RecordedWsConnector {
        fn default() -> Self {
            let (public_sender, public_injection) = tokio::sync::mpsc::unbounded_channel();
            Self {
                sent: Arc::new(Mutex::new(Vec::new())),
                public_subscriptions: Arc::new(Mutex::new(Vec::new())),
                public_injection: Mutex::new(Some(public_injection)),
                public_sender,
            }
        }
    }
    impl RecordedWsConnector {
        /// Deliver another explicitly scripted RAW venue frame on the same
        /// connected socket. The production parser and generation rules remain
        /// the sole authorities for source freshness and market readiness.
        pub(crate) fn send_public_frame(&self, frame: String) {
            self.public_sender
                .send(frame)
                .expect("recorded public socket closed");
        }

        pub(crate) fn expect_public_subscription(
            &self,
            arg: serde_json::Value,
            frames: Vec<String>,
        ) {
            self.public_subscriptions
                .lock()
                .unwrap()
                .push((arg, frames));
        }

        pub(crate) fn unconsumed_public_subscriptions(&self) -> usize {
            self.public_subscriptions.lock().unwrap().len()
        }
    }
    struct RecordedWire {
        inbound: std::collections::VecDeque<tokio_tungstenite::tungstenite::Message>,
        sent: Arc<Mutex<Vec<serde_json::Value>>>,
        public_subscriptions: Option<RecordedPublicSubscriptions>,
        public_injection: Option<tokio::sync::mpsc::UnboundedReceiver<String>>,
    }
    impl okx_runtime::WsWire for RecordedWire {
        fn send(
            &mut self,
            frame: tokio_tungstenite::tungstenite::Message,
        ) -> okx_runtime::WsCall<'_> {
            Box::pin(async move {
                if let tokio_tungstenite::tungstenite::Message::Text(data) = frame {
                    // OKX application-level idle heartbeat is real production
                    // behavior (20s ping, 10s pong deadline). Script only
                    // the raw venue pong; never bypass the generation guard.
                    if data.as_str() == "ping" {
                        self.inbound
                            .push_back(tokio_tungstenite::tungstenite::Message::Text(
                                "pong".into(),
                            ));
                        return Ok(());
                    }
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
                    return Some(Ok(frame));
                }
                if let Some(injection) = self.public_injection.as_mut() {
                    return injection
                        .recv()
                        .await
                        .map(|raw| Ok(tokio_tungstenite::tungstenite::Message::Text(raw.into())));
                }
                std::future::pending().await
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
                    public_injection: if public {
                        self.public_injection.lock().unwrap().take()
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
        budget: RateBudget,
    ) -> (VerifiedAccountConfig, PreparedAccountClients) {
        prepared_with_socket(label, feed, Arc::new(OfflineSocket), budget)
    }

    pub(crate) fn prepared_with_socket(
        label: &str,
        feed: Arc<RecordedAccountHttp>,
        ws_connector: Arc<dyn WsConnector>,
        budget: RateBudget,
    ) -> (VerifiedAccountConfig, PreparedAccountClients) {
        let uid = match label {
            "a" => "account-001",
            "b" => "account-002",
            _ => panic!("unexpected recorded account"),
        };
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let budget = budget.for_authenticated_user(uid);
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
            budget.clone(),
            feed.clone(),
        )
        .unwrap();
        let executor =
            OkxRestClient::with_http_transport(environment, executing, budget, feed).unwrap();
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
    #[cfg(test)]
    mod recorded_order_binding_tests {
        use super::*;

        fn causal_algo_fixture() -> Arc<RecordedAccountHttp> {
            let feed = Arc::new(RecordedAccountHttp::default());
            let raw = |data: serde_json::Value| {
                serde_json::to_vec(&serde_json::json!({
                    "code": "0", "msg": "", "data": data
                }))
                .unwrap()
            };
            let mut before = Vec::new();
            let mut after = Vec::new();
            for inst_type in ["SWAP", "FUTURES"] {
                for kind in [
                    "conditional",
                    "oco",
                    "trigger",
                    "move_order_stop",
                    "chase",
                    "iceberg",
                    "twap",
                    "smart_iceberg",
                ] {
                    let query = vec![
                        ("instType".into(), inst_type.into()),
                        ("ordType".into(), kind.into()),
                        ("limit".into(), "100".into()),
                    ];
                    let active = inst_type == "SWAP" && kind == "conditional";
                    before.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: query.clone(),
                        raw_response: raw(if active {
                            serde_json::json!([
                                {"algoId": "900000002"},
                                {"algoId": "900000003"}
                            ])
                        } else {
                            serde_json::json!([])
                        }),
                    });
                    after.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: query,
                        raw_response: raw(if active {
                            serde_json::json!([{"algoId": "900000003"}])
                        } else {
                            serde_json::json!([])
                        }),
                    });
                }
            }
            feed.expect_causal_algo_inventory("900000002", before, after);
            feed.expect_post(RecordedPost {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/cancel-algos".into(),
                expected_body: serde_json::json!([{
                    "instId": "BTC-USDT-SWAP", "algoId": "900000002"
                }]),
                raw_response: raw(serde_json::json!([{
                    "algoId": "900000002", "sCode": "0"
                }])),
            });
            feed
        }

        fn causal_get(account: &'static str, limit: &str) -> okx_api::HttpRequest {
            let url = reqwest::Url::parse(&format!(
                "https://www.okx.com/api/v5/trade/orders-algo-pending?instType=SWAP&ordType=conditional&limit={limit}"
            ))
            .unwrap();
            let mut request = reqwest::Request::new(reqwest::Method::GET, url);
            request.headers_mut().insert(
                "OK-ACCESS-KEY",
                reqwest::header::HeaderValue::from_static(account),
            );
            request
        }

        fn causal_cancel() -> okx_api::HttpRequest {
            let url = reqwest::Url::parse("https://www.okx.com/api/v5/trade/cancel-algos").unwrap();
            let mut request = reqwest::Request::new(reqwest::Method::POST, url);
            request.headers_mut().insert(
                "OK-ACCESS-KEY",
                reqwest::header::HeaderValue::from_static("fixture-executor-a"),
            );
            *request.body_mut() = Some(reqwest::Body::from(
                r#"[{"instId":"BTC-USDT-SWAP","algoId":"900000002"}]"#,
            ));
            request
        }

        async fn deliver_causal(
            feed: &RecordedAccountHttp,
            request: okx_api::HttpRequest,
        ) -> Result<serde_json::Value, okx_api::OkxError> {
            use okx_api::HttpTransport;
            let reply = feed
                .send(request, Instant::now() + std::time::Duration::from_secs(3))
                .await?;
            serde_json::from_slice(&reply.body).map_err(Into::into)
        }

        #[tokio::test]
        async fn signed_fee_phase_advances_only_on_explicit_event_and_keeps_prior_source_time() {
            let feed = RecordedAccountHttp::default();
            let page = |stamp: &str| RecordedGet {
                access_key: "fixture-observer-b".into(),
                path: "/api/v5/account/trade-fee".into(),
                required_query: vec![
                    ("instType".into(), "SWAP".into()),
                    ("instFamily".into(), "BTC-USDT".into()),
                ],
                raw_response: serde_json::to_vec(&serde_json::json!({
                    "code":"0","msg":"","data":[{
                        "instType":"SWAP","ts":stamp,"feeGroup":[{
                            "groupId":"4","maker":"0.0005","taker":"0.0005"
                        }]
                    }]
                }))
                .unwrap(),
            };
            let get = || {
                let mut request=reqwest::Request::new(
                    reqwest::Method::GET,
                    reqwest::Url::parse(
                        "https://www.okx.com/api/v5/account/trade-fee?instType=SWAP&instFamily=BTC-USDT"
                    ).unwrap(),
                );
                request.headers_mut().insert(
                    "OK-ACCESS-KEY",
                    reqwest::header::HeaderValue::from_static("fixture-observer-b"),
                );
                request
            };
            feed.expect_immutable_signed_phase("initial", page("100"));
            for _ in 0..2 {
                let response = deliver_causal(&feed, get()).await.unwrap();
                assert_eq!(response["data"][0]["ts"], "100");
            }
            assert_eq!(
                feed.advance_immutable_signed_phase(
                    "initial",
                    "after-proven-local-nosend",
                    page("200")
                ),
                2
            );
            for _ in 0..2 {
                let response = deliver_causal(&feed, get()).await.unwrap();
                assert_eq!(response["data"][0]["ts"], "200");
            }
            assert_eq!(
                feed.immutable_signed_phase_reads(
                    "fixture-observer-b",
                    "/api/v5/account/trade-fee",
                    "after-proven-local-nosend"
                ),
                2
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
            let trace = feed.immutable_phase_trace.lock().unwrap().join("\n");
            assert!(
                trace.contains(
                    "event=SIGNED_PHASE_ADVANCED from=initial to=after-proven-local-nosend"
                )
            );
        }

        #[tokio::test]
        async fn immutable_signed_leverage_handoff_preserves_exact_source_without_extra_pages() {
            let feed = RecordedAccountHttp::default();
            let raw = serde_json::to_vec(&serde_json::json!({
                "code":"0","msg":"","data":[
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                ]
            }))
            .unwrap();
            let page = || RecordedGet {
                access_key: "fixture-observer-b".into(),
                path: "/api/v5/account/leverage-info".into(),
                required_query: vec![
                    ("instId".into(), "BTC-USDT-SWAP".into()),
                    ("mgnMode".into(), "cross".into()),
                ],
                raw_response: raw.clone(),
            };
            feed.expect_get(page());
            let retired = feed.supersede_queued_signed_source_at_event(
                "observed-prior-b-leverage",
                "unchanged-b-leverage",
                page(),
            );
            assert_eq!(retired, 1);
            let get = |mode: &str| {
                let url = reqwest::Url::parse(&format!(
                    "https://www.okx.com/api/v5/account/leverage-info?instId=BTC-USDT-SWAP&mgnMode={mode}"
                ))
                .unwrap();
                let mut request = reqwest::Request::new(reqwest::Method::GET, url);
                request.headers_mut().insert(
                    "OK-ACCESS-KEY",
                    reqwest::header::HeaderValue::from_static("fixture-observer-b"),
                );
                request
            };
            for _ in 0..3 {
                let observed = deliver_causal(&feed, get("cross")).await.unwrap();
                assert_eq!(observed["data"][0]["lever"], "2");
                assert_eq!(observed["data"][1]["lever"], "2");
            }
            let invalid = deliver_causal(&feed, get("isolated")).await.unwrap_err();
            assert!(
                invalid
                    .to_string()
                    .contains("TRANSCRIPT_IMMUTABLE_QUERY_MISMATCH")
            );
            assert_eq!(
                feed.immutable_signed_phase_reads(
                    "fixture-observer-b",
                    "/api/v5/account/leverage-info",
                    "unchanged-b-leverage"
                ),
                3,
                "a wrong query cannot consume a valid immutable source"
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        }

        #[tokio::test]
        async fn immutable_acked_order_readback_is_bound_to_one_signed_post_and_query() {
            let feed = RecordedAccountHttp::default();
            let client = "ackboundclient0001";
            let raw = serde_json::to_vec(&serde_json::json!({
                "code":"0","msg":"","data":[{
                    "instId":"BTC-USDT-SWAP","ordId":"signed-ack-001",
                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                    "state":"live","accFillSz":"0","sz":"0.3"
                }]
            }))
            .unwrap();
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                raw_response: raw.clone(),
            });
            feed.expect_post(RecordedPost{
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                expected_body:serde_json::json!({"clOrdId":client}),
                raw_response:br#"{"code":"0","data":[{"ordId":"signed-ack-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
            });
            let mut create = reqwest::Request::new(
                reqwest::Method::POST,
                reqwest::Url::parse("https://www.okx.com/api/v5/trade/order").unwrap(),
            );
            create.headers_mut().insert(
                "OK-ACCESS-KEY",
                reqwest::header::HeaderValue::from_static("fixture-executor-a"),
            );
            *create.body_mut() = Some(reqwest::Body::from(format!(r#"{{"clOrdId":"{client}"}}"#)));
            let ack = deliver_causal(&feed, create).await.unwrap();
            assert_eq!(ack["data"][0]["ordId"], "signed-ack-001");
            let retired = feed.supersede_queued_signed_source_at_event(
                "genuine-ack",
                "same-live-order",
                RecordedGet {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/order".into(),
                    required_query: vec![
                        ("instId".into(), "BTC-USDT-SWAP".into()),
                        ("clOrdId".into(), client.into()),
                    ],
                    raw_response: raw,
                },
            );
            assert_eq!(retired, 1);
            let order_get =
                |id: &str| {
                    let mut r=reqwest::Request::new(reqwest::Method::GET,
                    reqwest::Url::parse(&format!(
                        "https://www.okx.com/api/v5/trade/order?instId=BTC-USDT-SWAP&clOrdId={id}"
                    )).unwrap());
                    r.headers_mut().insert(
                        "OK-ACCESS-KEY",
                        reqwest::header::HeaderValue::from_static("fixture-executor-a"),
                    );
                    r
                };
            for _ in 0..2 {
                let observed = deliver_causal(&feed, order_get(client)).await.unwrap();
                assert_eq!(observed["data"][0]["ordId"], "signed-ack-001");
                assert_eq!(observed["data"][0]["clOrdId"], client);
                assert_eq!(observed["data"][0]["state"], "live");
            }
            let wrong = deliver_causal(&feed, order_get("unacknowledged-client"))
                .await
                .expect_err("foreign order must never use another ACK");
            assert!(
                wrong
                    .to_string()
                    .contains("TRANSCRIPT_IMMUTABLE_QUERY_MISMATCH")
            );
            assert_eq!(
                feed.immutable_signed_phase_reads(
                    "fixture-executor-a",
                    "/api/v5/trade/order",
                    "same-live-order"
                ),
                2,
                "foreign read must not consume or mutate the signed source"
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
        }

        // Reproducer: two signed orders on the SAME executor/route, with
        // different ACK-bound client IDs. Route-wide supersession used to
        // retire B's page when A changed phase.
        #[tokio::test]
        async fn immutable_acked_orders_keep_independent_sources_on_one_route() {
            let feed = RecordedAccountHttp::default();
            let client_a = "scopeclienta0001";
            let client_b = "scopeclientb0001";
            let raw = |order: &str, state: &str| {
                serde_json::to_vec(&serde_json::json!({
                    "code":"0","msg":"","data":[{
                        "instId":"BTC-USDT-SWAP","ordId":order,
                        "clOrdId":"__REQUEST_CL_ORD_ID__",
                        "state":state,"accFillSz":"0","sz":"0.3","uTime":"100"
                    }]
                }))
                .unwrap()
            };
            let page = |order: &str| RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                raw_response: raw(order, "live"),
            };
            feed.expect_get(page("signed-ack-a"));
            feed.expect_get(page("signed-ack-b"));
            for (client, order) in [(client_a, "signed-ack-a"), (client_b, "signed-ack-b")] {
                feed.expect_post(RecordedPost {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/order".into(),
                    expected_body: serde_json::json!({"clOrdId":client}),
                    raw_response: serde_json::to_vec(&serde_json::json!({
                        "code":"0","data":[{"ordId":order,"clOrdId":client,"sCode":"0"}]
                    }))
                    .unwrap(),
                });
            }
            let post = |client: &str| {
                let mut r = reqwest::Request::new(
                    reqwest::Method::POST,
                    reqwest::Url::parse("https://www.okx.com/api/v5/trade/order").unwrap(),
                );
                r.headers_mut().insert(
                    "OK-ACCESS-KEY",
                    reqwest::header::HeaderValue::from_static("fixture-executor-a"),
                );
                *r.body_mut() = Some(reqwest::Body::from(
                    serde_json::json!({"clOrdId":client}).to_string(),
                ));
                r
            };
            let get = |client: &str| {
                let mut r = reqwest::Request::new(
                    reqwest::Method::GET,
                    reqwest::Url::parse(&format!(
                        "https://www.okx.com/api/v5/trade/order?instId=BTC-USDT-SWAP&clOrdId={client}"
                    )).unwrap(),
                );
                r.headers_mut().insert(
                    "OK-ACCESS-KEY",
                    reqwest::header::HeaderValue::from_static("fixture-executor-a"),
                );
                r
            };
            assert_eq!(
                deliver_causal(&feed, post(client_a)).await.unwrap()["data"][0]["ordId"],
                "signed-ack-a"
            );
            assert_eq!(
                deliver_causal(&feed, post(client_b)).await.unwrap()["data"][0]["ordId"],
                "signed-ack-b"
            );
            let phase = |client: &str, order: &str, state: &str| RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                required_query: vec![
                    ("instId".into(), "BTC-USDT-SWAP".into()),
                    ("clOrdId".into(), client.into()),
                ],
                raw_response: raw(order, state),
            };
            assert_eq!(
                feed.supersede_queued_signed_source_at_event(
                    "ack-a",
                    "phase-a",
                    phase(client_a, "signed-ack-a", "live")
                ),
                1
            );
            // A GET delivered before the B event remains externally sourced.
            let before_b = deliver_causal(&feed, get(client_a)).await.unwrap();
            assert_eq!(before_b["data"][0]["ordId"], "signed-ack-a");
            assert_eq!(
                feed.unconsumed_reads(),
                1,
                "B must retain its own finite source"
            );
            assert_eq!(
                feed.supersede_queued_signed_source_at_event(
                    "ack-b",
                    "phase-b",
                    phase(client_b, "signed-ack-b", "live")
                ),
                1
            );
            // A response begun in the older source phase is still A's
            // authenticated bytes when observed after B's source event.
            assert_eq!(before_b["data"][0]["clOrdId"], client_a);
            assert_eq!(
                deliver_causal(&feed, get(client_a)).await.unwrap()["data"][0]["ordId"],
                "signed-ack-a"
            );
            let b_before = deliver_causal(&feed, get(client_b)).await.unwrap();
            assert_eq!(b_before["data"][0]["ordId"], "signed-ack-b");
            let b_reads = feed.immutable_signed_phase_reads(
                "fixture-executor-a",
                "/api/v5/trade/order",
                "phase-b",
            );
            feed.advance_immutable_signed_phase(
                "phase-a",
                "phase-a-new",
                phase(client_a, "signed-ack-a", "partially_filled"),
            );
            assert_eq!(
                feed.immutable_signed_phase_reads(
                    "fixture-executor-a",
                    "/api/v5/trade/order",
                    "phase-b"
                ),
                b_reads
            );
            assert_eq!(
                deliver_causal(&feed, get(client_b)).await.unwrap(),
                b_before
            );
            feed.retire_immutable_signed_phase(
                "phase-a-new",
                "fixture-executor-a",
                "/api/v5/trade/order",
            );
            assert_eq!(
                deliver_causal(&feed, get(client_b)).await.unwrap(),
                b_before
            );
            let foreign = deliver_causal(&feed, get("unacknowledgedclient"))
                .await
                .expect_err("foreign clOrdId cannot inherit B");
            assert!(
                foreign
                    .to_string()
                    .contains("TRANSCRIPT_IMMUTABLE_QUERY_MISMATCH")
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 2);
            assert_eq!(
                feed.immutable_signed_phase_reads(
                    "fixture-executor-a",
                    "/api/v5/trade/order",
                    "phase-b"
                ),
                b_reads + 2
            );
        }

        #[tokio::test]
        async fn s1_01_maintenance_read_before_cancel_ack_keeps_second_stop() {
            let feed = causal_algo_fixture();
            let before = deliver_causal(&feed, causal_get("fixture-observer-a", "100"))
                .await
                .unwrap();
            assert_eq!(before["data"].as_array().unwrap().len(), 2);
            deliver_causal(&feed, causal_cancel()).await.unwrap();
            let after = deliver_causal(&feed, causal_get("fixture-executor-a", "100"))
                .await
                .unwrap();
            assert_eq!(after["data"].as_array().unwrap().len(), 1);
            assert_eq!(after["data"][0]["algoId"], "900000003");
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
            assert_eq!(feed.unconsumed_posts(), 0);
        }

        #[tokio::test]
        async fn s1_02_pre_cancel_get_late_delivery_does_not_rewrite_inventory() {
            let feed = causal_algo_fixture();
            let (entered, resume) = feed.hold_next_causal_algo_get();
            let in_flight = {
                let feed = feed.clone();
                tokio::spawn(async move {
                    deliver_causal(&feed, causal_get("fixture-observer-a", "100")).await
                })
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
                .await
                .expect("pre-cancel read admitted at transport boundary");
            deliver_causal(&feed, causal_cancel()).await.unwrap();
            resume.notify_one();
            let old = in_flight.await.unwrap().unwrap();
            assert_eq!(old["data"].as_array().unwrap().len(), 2);
            let fresh = deliver_causal(&feed, causal_get("fixture-executor-a", "100"))
                .await
                .unwrap();
            assert_eq!(fresh["data"].as_array().unwrap().len(), 1);
            assert_eq!(fresh["data"][0]["algoId"], "900000003");
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
            let duplicate = deliver_causal(&feed, causal_cancel()).await.unwrap_err();
            assert!(duplicate.to_string().contains("TRANSCRIPT_UNEXPECTED_POST"));
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 2);
        }

        #[tokio::test]
        async fn s1_04_executor_and_observer_share_one_verified_uid_inventory() {
            let feed = causal_algo_fixture();
            let executor = deliver_causal(&feed, causal_get("fixture-executor-a", "100"))
                .await
                .unwrap();
            assert_eq!(executor["data"].as_array().unwrap().len(), 2);
            let observer = deliver_causal(&feed, causal_get("fixture-observer-a", "100"))
                .await
                .unwrap();
            assert_eq!(observer["data"].as_array().unwrap().len(), 2);
            deliver_causal(&feed, causal_cancel()).await.unwrap();
            for actor in ["fixture-observer-a", "fixture-executor-a"] {
                let fresh = deliver_causal(&feed, causal_get(actor, "100"))
                    .await
                    .unwrap();
                assert_eq!(fresh["data"].as_array().unwrap().len(), 1);
                assert_eq!(fresh["data"][0]["algoId"], "900000003");
            }
            let trace = feed.causal_algo_trace.lock().unwrap().join("\n");
            assert!(trace.contains("actor=fixture-executor-a"));
            assert!(trace.contains("actor=fixture-observer-a"));
            assert!(trace.contains("event=CANCEL_ACK"));
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
        }

        #[tokio::test]
        async fn s1_05_other_uid_has_its_own_immutable_signed_inventory() {
            let feed = causal_algo_fixture();
            let raw = serde_json::to_vec(&serde_json::json!({
                "code": "0", "msg": "", "data": []
            }))
            .unwrap();
            let mut pages = Vec::new();
            for inst_type in ["SWAP", "FUTURES"] {
                for kind in [
                    "conditional",
                    "oco",
                    "trigger",
                    "move_order_stop",
                    "chase",
                    "iceberg",
                    "twap",
                    "smart_iceberg",
                ] {
                    pages.push(RecordedGet {
                        access_key: "fixture-observer-b".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: vec![
                            ("instType".into(), inst_type.into()),
                            ("ordType".into(), kind.into()),
                            ("limit".into(), "100".into()),
                        ],
                        raw_response: raw.clone(),
                    });
                }
            }
            feed.expect_static_algo_inventory("fixture-observer-b", pages);
            for actor in ["fixture-observer-b", "fixture-executor-b"] {
                let reply = deliver_causal(&feed, causal_get(actor, "100"))
                    .await
                    .unwrap();
                assert_eq!(reply["data"].as_array().unwrap().len(), 0);
            }
            let a_before = deliver_causal(&feed, causal_get("fixture-observer-a", "100"))
                .await
                .unwrap();
            assert_eq!(a_before["data"].as_array().unwrap().len(), 2);
            deliver_causal(&feed, causal_cancel()).await.unwrap();
            let a_after = deliver_causal(&feed, causal_get("fixture-executor-a", "100"))
                .await
                .unwrap();
            assert_eq!(a_after["data"].as_array().unwrap().len(), 1);
            let b_after = deliver_causal(&feed, causal_get("fixture-observer-b", "100"))
                .await
                .unwrap();
            assert_eq!(b_after["data"].as_array().unwrap().len(), 0);
            let wrong = deliver_causal(&feed, causal_get("fixture-observer-b", "99"))
                .await
                .unwrap_err();
            assert!(
                wrong
                    .to_string()
                    .contains("TRANSCRIPT_STATIC_ALGO_QUERY_MISMATCH")
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
        }

        #[tokio::test]
        async fn s1_03_unknown_scope_fails_without_advancing_cancel_inventory() {
            let feed = causal_algo_fixture();
            let account = deliver_causal(&feed, causal_get("fixture-observer-b", "100"))
                .await
                .unwrap_err();
            assert!(
                account
                    .to_string()
                    .contains("TRANSCRIPT_ALGO_ACCOUNT_MISMATCH")
            );
            let query = deliver_causal(&feed, causal_get("fixture-observer-a", "99"))
                .await
                .unwrap_err();
            assert!(query.to_string().contains("TRANSCRIPT_ALGO_QUERY_MISMATCH"));
            let unchanged = deliver_causal(&feed, causal_get("fixture-observer-a", "100"))
                .await
                .unwrap();
            assert_eq!(unchanged["data"].as_array().unwrap().len(), 2);
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
        }

        #[test]
        fn recorded_clients_use_one_native_rate_budget_across_api_identities() {
            // A scripted transport must not gain independent admission windows
            // merely because Observer and Executor use different API keys.
            // The identical shared budget is also passed to the Registry and
            // public WS owner by the integrated Native/Worker assembly.
            let budget = RateBudget::new();
            let feed = Arc::new(RecordedAccountHttp::default());
            let (_, a) =
                prepared_with_socket("a", feed.clone(), Arc::new(OfflineSocket), budget.clone());
            let (_, b) = prepared_with_socket("b", feed, Arc::new(OfflineSocket), budget.clone());
            let plan = a
                .observer
                .rate_budget()
                .private_rest_plan("/api/v5/account/config", &[]);
            for _ in 0..5 {
                a.observer.rate_budget().admit(&plan).unwrap();
            }
            let a_plan = a
                .executor
                .rate_budget()
                .private_rest_plan("/api/v5/account/config", &[]);
            let a_denied = a.executor.rate_budget().admit(&a_plan).unwrap_err();
            assert!(!a_denied.request_sent, "same-user identities share quota");
            assert_eq!(a_denied.domain.local_max_requests, 5);
            let b_plan = b
                .observer
                .rate_budget()
                .private_rest_plan("/api/v5/account/config", &[]);
            for _ in 0..5 {
                b.observer.rate_budget().admit(&b_plan).unwrap();
            }
            let b_denied = b.executor.rate_budget().admit(&b_plan).unwrap_err();
            assert!(!b_denied.request_sent, "second account has its own bound");
            assert_ne!(a_denied.domain.scope, b_denied.domain.scope);
        }

        #[test]
        fn signed_order_readback_must_match_same_account_client_and_acknowledged_order() {
            let ack = vec![
                (
                    "fixture-executor-a".to_owned(),
                    "client_A".to_owned(),
                    "order_A".to_owned(),
                ),
                (
                    "fixture-executor-a".to_owned(),
                    "client_B".to_owned(),
                    "order_B".to_owned(),
                ),
                (
                    "fixture-executor-b".to_owned(),
                    "client_B".to_owned(),
                    "order_C".to_owned(),
                ),
            ];
            let raw = |order: &str, client: &str| {
                serde_json::to_vec(&serde_json::json!({
                    "code":"0", "msg":"", "data":[{"ordId":order,"clOrdId":client}]
                }))
                .unwrap()
            };
            assert!(recorded_order_source_matches(
                &ack,
                "fixture-executor-a",
                "client_A",
                &raw("order_A", "__REQUEST_CL_ORD_ID__")
            ));
            assert!(recorded_order_source_matches(
                &ack,
                "fixture-executor-a",
                "client_B",
                &raw("order_B", "__REQUEST_CL_ORD_ID__")
            ));
            assert!(!recorded_order_source_matches(
                &ack,
                "fixture-executor-a",
                "client_A",
                &raw("order_B", "__REQUEST_CL_ORD_ID__")
            ));
            assert!(!recorded_order_source_matches(
                &ack,
                "fixture-executor-b",
                "client_B",
                &raw("order_B", "__REQUEST_CL_ORD_ID__")
            ));
            assert!(!recorded_order_source_matches(
                &ack,
                "fixture-executor-a",
                "unknown",
                &raw("order_A", "__REQUEST_CL_ORD_ID__")
            ));
            assert!(!recorded_order_source_matches(
                &ack,
                "fixture-executor-a",
                "client_A",
                &raw("order_A", "different_client")
            ));
            let mut duplicate = ack.clone();
            duplicate.push(ack[0].clone());
            assert!(!recorded_order_source_matches(
                &duplicate,
                "fixture-executor-a",
                "client_A",
                &raw("order_A", "__REQUEST_CL_ORD_ID__")
            ));
            assert!(!recorded_order_source_matches(
                &ack,
                "fixture-executor-a",
                "client_A",
                b"{malformed"
            ));
        }

        #[test]
        fn recorded_financial_history_binds_each_order_and_rejects_other_accounts() {
            let ack = vec![
                (
                    "fixture-executor-a".to_owned(),
                    "client_A".to_owned(),
                    "order_A".to_owned(),
                ),
                (
                    "fixture-executor-a".to_owned(),
                    "client_B".to_owned(),
                    "order_B".to_owned(),
                ),
                (
                    "fixture-executor-b".to_owned(),
                    "client_C".to_owned(),
                    "order_C".to_owned(),
                ),
            ];
            let bytes = serde_json::to_vec(&serde_json::json!({
                "data":[
                    {"ordId":"order_B","clOrdId":"__REQUEST_CL_ORD_ID__"},
                    {"ordId":"order_A","clOrdId":"__REQUEST_CL_ORD_ID__"}
                ]
            }))
            .unwrap();
            let bound =
                bind_recorded_history_ids(&ack, "fixture-executor-a", bytes.clone()).unwrap();
            let parsed: serde_json::Value = serde_json::from_slice(&bound).unwrap();
            assert_eq!(parsed["data"][0]["clOrdId"], "client_B");
            assert_eq!(parsed["data"][1]["clOrdId"], "client_A");
            assert!(bind_recorded_history_ids(&ack, "fixture-executor-b", bytes.clone()).is_err());
            let mut duplicate = ack.clone();
            duplicate.push(ack[0].clone());
            assert!(bind_recorded_history_ids(&duplicate, "fixture-executor-a", bytes).is_err());
            let unknown = serde_json::to_vec(&serde_json::json!({
                "data":[{"ordId":"unknown","clOrdId":"__REQUEST_CL_ORD_ID__"}]
            }))
            .unwrap();
            assert!(bind_recorded_history_ids(&ack, "fixture-executor-a", unknown).is_err());
        }
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
            vec![
                serde_json::from_value(json!({
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
                .unwrap(),
            ],
        )
        .unwrap();
        let connector = Arc::new(RecordedWsConnector::default());
        let stamp = chrono::Utc::now().timestamp_millis();
        let timestamp = stamp.to_string();
        let future = (stamp + 8 * 60 * 60 * 1000).to_string();
        let recordings: Vec<(Value, Option<Value>, Option<&str>)> = vec![
            (
                json!({"channel":"instruments","instType":"SWAP"}),
                None,
                None,
            ),
            (
                json!({"channel":"instruments","instType":"FUTURES"}),
                None,
                None,
            ),
            (
                json!({"channel":"tickers","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "last":"1000","lastSz":"1","askPx":"1000","askSz":"10",
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
            let ack = json!({
                "event":"subscribe","arg":arg,"connId":"recorded-public"
            })
            .to_string();
            let last_ack_after_data = arg["channel"] == "books";
            let mut raw = Vec::new();
            if !last_ack_after_data {
                raw.push(ack.clone());
            }
            if let Some(data) = data {
                let mut message = json!({"arg":arg,"data":[data]});
                if let Some(action) = action {
                    message["action"] = action.into();
                }
                raw.push(message.to_string());
            }
            // The book data frame is processed BEFORE its final subscribe
            // ACK. Only the real subscription readiness wakeup can complete
            // the public market: no extra data is injected after the ACK.
            if last_ack_after_data {
                raw.push(ack);
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
        .await;
        let verified = match verified {
            Ok(snapshot) => snapshot,
            Err(_) => {
                let quality = market
                    .risk_quality_snapshot(
                        "BTC-USDT-SWAP",
                        u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap(),
                        5_000,
                        5_000,
                    )
                    .await;
                let sent = connector.sent.lock().unwrap().clone();
                let remaining = connector.unconsumed_public_subscriptions();
                panic!(
                    "PUBLIC_WS_RECORDED_SOURCE_NOT_FRESH: quality={quality:?} remaining={remaining} sent={sent:?}"
                );
            }
        };
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

    /// The production AccountBootstrapper consumes venue-provided fee, leverage
    /// and account instrument bytes through signed observer REST. No test
    /// shortcut supplies a parsed FeeScheduleSnapshot or permission flag.
    /// A consumed or wrong-account source must fail closed.
    #[tokio::test]
    async fn recorded_account_capabilities_and_fee_sources_use_real_observer_parsers() {
        use super::recorded_support::{RecordedAccountHttp, RecordedGet, prepared};
        use serde_json::json;
        let feed = Arc::new(RecordedAccountHttp::default());
        let budget = RateBudget::new();
        let (_config, clients) = prepared("a", feed.clone(), budget.clone());
        let observer = crate::account_bootstrap::AccountBootstrapper::new(clients.observer);
        let reference = okx_observation::ReferenceRegistry::from_public(
            "2026-10-31T00:00:00Z",
            vec![
                serde_json::from_value(json!({
                    "instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "instFamily":"BTC-USDT","uly":"BTC-USDT",
                    "state":"live","ruleType":"normal",
                    "settleCcy":"USDT","tickSz":"0.1",
                    "lotSz":"0.01","minSz":"0.01",
                    "maxLmtSz":"1000","maxMktSz":"1000",
                    "ctType":"linear","ctVal":"0.01","ctValCcy":"BTC",
                    "groupId":"4","lever":"20"
                }))
                .unwrap(),
            ],
        )
        .unwrap();
        let rules = reference.instrument_rules("BTC-USDT-SWAP").unwrap();
        let stamp = chrono::Utc::now().timestamp_millis().to_string();
        let envelope = |data: serde_json::Value| {
            serde_json::to_vec(&json!({"code":"0","msg":"","data":data})).unwrap()
        };
        let observer_key = "fixture-observer-a".to_owned();
        feed.expect_get(RecordedGet {
            access_key: observer_key.clone(),
            path: "/api/v5/account/instruments".into(),
            required_query: vec![("instType".into(), "SWAP".into())],
            raw_response: envelope(json!([{
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "state":"live","lever":"20"
            }])),
        });
        for _ in 0..2 {
            feed.expect_get(RecordedGet {
                access_key: observer_key.clone(),
                path: "/api/v5/account/leverage-info".into(),
                required_query: vec![
                    ("instId".into(), "BTC-USDT-SWAP".into()),
                    ("mgnMode".into(), "cross".into()),
                ],
                raw_response: envelope(json!([
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"5"},
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"5"}
                ])),
            });
            feed.expect_get(RecordedGet {
                access_key: observer_key.clone(),
                path: "/api/v5/account/trade-fee".into(),
                required_query: vec![
                    ("instType".into(), "SWAP".into()),
                    ("instFamily".into(), "BTC-USDT".into()),
                ],
                raw_response: envelope(json!([{
                    "instType":"SWAP","level":"Lv1","ts":stamp,
                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                }])),
            });
        }
        let capabilities = observer
            .trading_capabilities(&rules, okx_api::MarginMode::Cross)
            .await
            .expect("production account capability capture");
        assert!(capabilities.instrument.available_to_account);
        assert_eq!(
            capabilities.instrument.account_max_leverage.as_deref(),
            Some("20")
        );
        assert_eq!(capabilities.account.position_mode, "long_short_mode");
        assert_eq!(capabilities.account.api_key_permissions, vec!["read_only"]);
        assert_eq!(
            capabilities.account.account_uid_fingerprint,
            okx_api::account_uid_fingerprint("account-001")
        );
        assert_eq!(capabilities.configured_leverage.len(), 2);
        let rate = capabilities.fee_schedule.unwrap();
        assert_eq!(rate.maker_rate, "0.0005");
        assert_eq!(rate.taker_rate, "0.0005");
        assert!(rate.exact_for_instrument);
        assert_eq!(rate.instrument_id, "BTC-USDT-SWAP");
        let selected = observer
            .configured_leverage("BTC-USDT-SWAP", okx_api::MarginMode::Cross, "long")
            .await
            .unwrap();
        assert_eq!(selected, "5");
        let fees = observer.fee_schedule(&rules).await.unwrap();
        assert_eq!(fees.taker_rate, "0.0005");
        assert_eq!(feed.unconsumed_reads(), 0);
        // Neither cached account configuration nor a previous successful
        // normalization permits using exhausted signed financial input.
        assert!(observer.fee_schedule(&rules).await.is_err());
        assert_eq!(feed.unconsumed_reads(), 0);
        let (_other, other_clients) = prepared("b", feed.clone(), budget);
        let other = crate::account_bootstrap::AccountBootstrapper::new(other_clients.observer);
        assert!(other.fee_schedule(&rules).await.is_err());
        assert!(feed.outbound_posts.lock().unwrap().is_empty());
    }

    #[test]
    fn recorded_clock_publisher_cannot_rearm_or_multiply_source_pages() {
        use super::recorded_support::RecordedAccountHttp;
        let feed = RecordedAccountHttp::default();
        assert!(!feed.refresh_unconsumed_public_clock(0, b"unsolicited".to_vec()));
        feed.expect_public_clock(b"recorded_first".to_vec());
        assert!(feed.refresh_unconsumed_public_clock(0, b"recorded_new".to_vec()));
        assert_eq!(feed.unconsumed_public_clock(), 1);
        assert!(!feed.refresh_unconsumed_public_clock(1, b"wrong_generation".to_vec()));
        assert_eq!(feed.unconsumed_public_clock(), 1);
        feed.expect_public_clock(b"another_source".to_vec());
        assert!(!feed.refresh_unconsumed_public_clock(0, b"multiply".to_vec()));
        assert_eq!(feed.unconsumed_public_clock(), 2);
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
        assert!(
            mismatch
                .to_string()
                .contains("TRANSCRIPT_POST_BODY_MISMATCH")
        );
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
            exhausted
                .to_string()
                .contains("TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED"),
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
        let budget = RateBudget::new();
        let registry = AccountRuntimeRegistry::open(&temp, budget.clone())
            .await
            .unwrap();
        let feed = Arc::new(RecordedAccountHttp::default());
        let mut stopped_sql_owners = Vec::new();

        for label in ["a", "b"] {
            let (config, clients) = prepared(label, feed.clone(), budget.clone());
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
        let budget = RateBudget::new();
        let registry = AccountRuntimeRegistry::open(&temp, budget.clone())
            .await
            .unwrap();
        let http = Arc::new(RecordedAccountHttp::default());
        let mut receipts = Vec::new();
        let mut sockets = Vec::new();
        for label in ["a", "b"] {
            let ws = Arc::new(RecordedWsConnector::default());
            let (config, clients) =
                prepared_with_socket(label, http.clone(), ws.clone(), budget.clone());
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
