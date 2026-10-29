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

// A WAL file can stay above the checkpoint byte threshold even after a
// successful PASSIVE checkpoint. Rechecking it after every SQL job causes
// repeated disk work and starves the same owner's read/finance queue.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15);
struct MaintenanceCadence {
    next_check: Instant,
}
impl MaintenanceCadence {
    fn new(now: Instant) -> Self {
        Self { next_check: now }
    }

    fn due(&mut self, now: Instant) -> bool {
        if now < self.next_check {
            return false;
        }
        self.next_check = now + MAINTENANCE_INTERVAL;
        true
    }
}
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
/// Timing tied to one accepted SQL-owner job, not shared last/max counters.
#[derive(Debug, Clone, Copy)]
pub struct AccountSqlJobTiming {
    pub queue_wait_ms: u64,
    pub sql_owner_ms: u64,
}
struct WorkerOwner {
    sender: SyncSender<Job>,
    timings: Arc<SqlTimingCounters>,
}

struct PendingBackup {
    handle: std::thread::JoinHandle<StoreResult<()>>,
    started: Instant,
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
                let mut pending_backup: Option<PendingBackup> = None;
                let mut maintenance_cadence = MaintenanceCadence::new(Instant::now());
                loop {
                    match receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(job) => job(&mut store),
                        Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    // The immutable recovery snapshot uses a short-lived
                    // read-only source connection. Do not join it while it
                    // runs: the owner's FIFO must stay available for NAV.
                    if pending_backup
                        .as_ref()
                        .is_some_and(|backup| backup.handle.is_finished())
                        && let Some(backup) = pending_backup.take()
                    {
                        let elapsed = backup.started.elapsed();
                        let outcome = match backup.handle.join() {
                            Ok(Ok(())) => {
                                last_backup = Instant::now();
                                store.clear_maintenance_error();
                                "ok"
                            }
                            Ok(Err(error)) => {
                                store.set_maintenance_error(error.to_string());
                                "error"
                            }
                            Err(_) => {
                                store.set_maintenance_error("BACKUP_WORKER_PANICKED".into());
                                "error"
                            }
                        };
                        eprintln!(
                            "ACCOUNT_SQL_BACKUP_V1 elapsed_ms={} outcome={}",
                            elapsed.as_millis(),
                            outcome
                        );
                    }
                    // One existing cadence/SQL writer/checkpoint owner; only
                    // the backup copy executes on a temporary read-only thread.
                    if !maintenance_cadence.due(Instant::now()) {
                        continue;
                    }
                    let started = Instant::now();
                    let succeeded = match store.maintenance() {
                        Ok(()) => true,
                        Err(e) => {
                            store.set_maintenance_error(e.to_string());
                            false
                        }
                    };
                    let elapsed = started.elapsed();
                    if elapsed >= Duration::from_millis(250) || !succeeded {
                        eprintln!(
                            "ACCOUNT_SQL_MAINTENANCE_V1 elapsed_ms={} backup_due=false outcome={}",
                            elapsed.as_millis(),
                            if succeeded { "ok" } else { "error" }
                        );
                    }
                    if pending_backup.is_none() && last_backup.elapsed() >= Duration::from_secs(900)
                    {
                        let (path, meta) = store.backup_identity();
                        match std::thread::Builder::new()
                            .name("okx-account-backup".into())
                            .spawn(move || AccountStore::periodic_backup_from_reader(path, meta))
                        {
                            Ok(handle) => {
                                pending_backup = Some(PendingBackup {
                                    handle,
                                    started: Instant::now(),
                                });
                            }
                            Err(e) => store.set_maintenance_error(e.to_string()),
                        }
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
        self.call_with_timing(deadline, operation)
            .await
            .map(|(value, _)| value)
    }

    /// Return queue and execution timing with this job's successful result.
    /// No new worker, queue, or SQL connection; failed calls retain StoreError.
    pub async fn call_with_timing<T, F>(
        &self,
        deadline: Instant,
        operation: F,
    ) -> StoreResult<(T, AccountSqlJobTiming)>
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
            let _ = tx.send(result.map(|value| {
                (
                    value,
                    AccountSqlJobTiming {
                        queue_wait_ms: queue_ms,
                        sql_owner_ms: owner_ms,
                    },
                )
            }));
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

#[cfg(test)]
mod maintenance_tests {
    use super::*;

    #[test]
    fn checkpoint_check_is_bounded_under_rapid_sql_jobs_and_idle_ticks() {
        let start = Instant::now();
        let mut cadence = MaintenanceCadence::new(start);
        // Bootstrap has one immediate maintenance check.
        assert!(cadence.due(start));
        // No repeated WAL checkpoint from SQL jobs or 1-second idle polls.
        for elapsed_ms in [1, 1000, 2000, 5000, 14_999] {
            assert!(!cadence.due(start + Duration::from_millis(elapsed_ms)));
        }
        assert!(cadence.due(start + Duration::from_secs(15)));
        assert!(!cadence.due(start + Duration::from_millis(15_001)));
        assert!(cadence.due(start + Duration::from_secs(30)));
        // A delayed owner still performs one check when it regains control.
        assert!(cadence.due(start + Duration::from_secs(950)));
        assert!(!cadence.due(start + Duration::from_secs(951)));
    }
}
