//! One bounded blocking worker per account. No SQLite calls on the async reactor.
use crate::{AccountMeta, AccountStore, OpeningSnapshot, StoreError, StoreResult};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{RecvTimeoutError, SyncSender, TrySendError, sync_channel},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
type Job = Box<dyn FnOnce(&mut AccountStore) + Send + 'static>;
#[derive(Default)]
struct SqlTimingCounters {
    jobs: AtomicU64,
    last_queue_wait_ms: AtomicU64,
    last_sql_owner_ms: AtomicU64,
    max_queue_wait_ms: AtomicU64,
    max_sql_owner_ms: AtomicU64,
}
/// Diagnostic-only timings for jobs that reached this account's SQL owner.
#[derive(Clone, Debug, serde::Serialize)]
pub struct AccountSqlTimingV1 {
    pub schema: &'static str,
    pub observed_jobs: u64,
    pub last_queue_wait_ms: u64,
    pub last_sql_owner_ms: u64,
    pub max_queue_wait_ms: u64,
    pub max_sql_owner_ms: u64,
}
struct WorkerOwner {
    sender: SyncSender<Job>,
    timings: Arc<SqlTimingCounters>,
}
#[derive(Clone)]
pub struct AccountStoreHandle {
    owner: Arc<WorkerOwner>,
}
impl std::fmt::Debug for AccountStoreHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountStoreHandle").finish_non_exhaustive()
    }
}
impl AccountStoreHandle {
    pub async fn open_existing(
        path: PathBuf,
        account_key: String,
        installation_id: String,
    ) -> StoreResult<Self> {
        Self::start(move || AccountStore::open_existing(path, &account_key, &installation_id)).await
    }
    pub async fn initialize_flat(
        path: PathBuf,
        meta: AccountMeta,
        snapshot: OpeningSnapshot,
    ) -> StoreResult<Self> {
        Self::start(move || AccountStore::initialize_or_inspect_exact(path, meta, &snapshot)).await
    }
    async fn start(
        factory: impl FnOnce() -> StoreResult<AccountStore> + Send + 'static,
    ) -> StoreResult<Self> {
        let (sender, receiver) = sync_channel::<Job>(128);
        let timings = Arc::new(SqlTimingCounters::default());
        let (ready_tx, ready_rx) = oneshot::channel();
        std::thread::Builder::new()
            .name("okx-account-sql".into())
            .spawn(move || {
                let mut store = match factory() {
                    Ok(s) => {
                        let _ = ready_tx.send(Ok(()));
                        s
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                let mut last_backup = Instant::now();
                loop {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(job) => job(&mut store),
                        Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    if let Err(e) =
                        store.maintenance(last_backup.elapsed() >= Duration::from_secs(900))
                    {
                        store.set_maintenance_error(e.to_string());
                    } else if last_backup.elapsed() >= Duration::from_secs(900) {
                        last_backup = Instant::now();
                    }
                }
            })?;
        ready_rx
            .await
            .map_err(|_| StoreError::Invalid("ACCOUNT_WORKER_STOPPED"))??;
        Ok(Self {
            owner: Arc::new(WorkerOwner { sender, timings }),
        })
    }
    pub fn sql_timing(&self) -> AccountSqlTimingV1 {
        let t = &self.owner.timings;
        AccountSqlTimingV1 {
            schema: "okx.account-sql-timing/v1",
            observed_jobs: t.jobs.load(Ordering::Relaxed),
            last_queue_wait_ms: t.last_queue_wait_ms.load(Ordering::Relaxed),
            last_sql_owner_ms: t.last_sql_owner_ms.load(Ordering::Relaxed),
            max_queue_wait_ms: t.max_queue_wait_ms.load(Ordering::Relaxed),
            max_sql_owner_ms: t.max_sql_owner_ms.load(Ordering::Relaxed),
        }
    }
    pub async fn call<T, F>(&self, deadline: Instant, operation: F) -> StoreResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut AccountStore) -> StoreResult<T> + Send + 'static,
    {
        if Instant::now() >= deadline {
            return Err(StoreError::Invalid("ACCOUNT_QUEUE_DEADLINE"));
        }
        let (tx, rx) = oneshot::channel();
        let enqueued_at = Instant::now();
        let timings = Arc::clone(&self.owner.timings);
        let job: Job = Box::new(move |store| {
            let started_at = Instant::now();
            let queue_wait = started_at.duration_since(enqueued_at);
            let result = if started_at >= deadline {
                Err(StoreError::Invalid("ACCOUNT_QUEUE_DEADLINE"))
            } else {
                store.run_before_deadline(deadline, operation)
            };
            let sql_owner_time = started_at.elapsed();
            let queue_ms = u64::try_from(queue_wait.as_millis()).unwrap_or(u64::MAX);
            let owner_ms = u64::try_from(sql_owner_time.as_millis()).unwrap_or(u64::MAX);
            timings
                .last_queue_wait_ms
                .store(queue_ms, Ordering::Relaxed);
            timings.last_sql_owner_ms.store(owner_ms, Ordering::Relaxed);
            timings
                .max_queue_wait_ms
                .fetch_max(queue_ms, Ordering::Relaxed);
            timings
                .max_sql_owner_ms
                .fetch_max(owner_ms, Ordering::Relaxed);
            timings.jobs.fetch_add(1, Ordering::Relaxed);
            // Diagnostic only: no account identity, financial facts, or SQL text.
            // The original deadline includes queue wait. Phase-named SQL
            // interruptions do not imply that the SQL statement was slow.
            let notable = queue_wait >= Duration::from_millis(250)
                || sql_owner_time >= Duration::from_millis(500)
                || result.is_err();
            let outcome = if result.is_ok() { "ok" } else { "error" };
            let _ = tx.send(result);
            if notable {
                eprintln!(
                    "ACCOUNT_SQL_TIMING_V1 queue_wait_ms={} sql_owner_ms={} total_ms={} remaining_at_start_ms={} outcome={}",
                    queue_wait.as_millis(),
                    sql_owner_time.as_millis(),
                    enqueued_at.elapsed().as_millis(),
                    deadline.saturating_duration_since(started_at).as_millis(),
                    outcome,
                );
            }
        });
        self.owner.sender.try_send(job).map_err(|e| match e {
            TrySendError::Full(_) => StoreError::Invalid("ACCOUNT_STORAGE_BACKPRESSURE"),
            TrySendError::Disconnected(_) => StoreError::Invalid("ACCOUNT_WORKER_STOPPED"),
        })?;
        // Once accepted, await the committed outcome. Dropping an HTTP request never cancels a SQL commit.
        rx.await
            .map_err(|_| StoreError::Invalid("ACCOUNT_WORKER_STOPPED"))?
    }
}
