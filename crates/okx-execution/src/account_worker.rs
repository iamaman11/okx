//! One bounded blocking worker per account. No SQLite calls on the async reactor.
use crate::{AccountMeta, AccountStore, OpeningSnapshot, StoreError, StoreResult};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        mpsc::{RecvTimeoutError, SyncSender, TrySendError, sync_channel},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
type Job = Box<dyn FnOnce(&mut AccountStore) + Send + 'static>;
struct WorkerOwner {
    sender: SyncSender<Job>,
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
            owner: Arc::new(WorkerOwner { sender }),
        })
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
        let job: Job = Box::new(move |store| {
            let result = if Instant::now() >= deadline {
                Err(StoreError::Invalid("ACCOUNT_QUEUE_DEADLINE"))
            } else {
                operation(store)
            };
            let _ = tx.send(result);
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
