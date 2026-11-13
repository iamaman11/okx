//! One bounded blocking SQLite worker per campaign; Tokio never owns Connection.
//! Completion after queueing may be unknown: callers must inspect durable
//! identity, never blindly repeat external POST or assume a timed-out mutation failed.
use super::store::{
    BookReadback, HeldReceipt, ReserveInput, RiskBookStore, RiskStoreError, VerifiedCensus,
};
use std::{
    path::PathBuf,
    sync::{
        Arc, RwLock,
        mpsc::{SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::Duration,
};
use tokio::sync::{oneshot, watch};

type Job = Box<dyn FnOnce(&mut RiskBookStore, &RwLock<Option<BookReadback>>) + Send + 'static>;

struct Worker {
    sender: SyncSender<Job>,
    projection: Arc<RwLock<Option<BookReadback>>>,
    terminated: watch::Receiver<bool>,
}

/// Internal owner handle; never conveys permission to submit to the exchange.
#[derive(Clone)]
pub(super) struct RiskBookHandle {
    worker: Arc<Worker>,
}

impl RiskBookHandle {
    pub(super) async fn open_existing(
        path: PathBuf,
        installation: String,
        campaign: String,
    ) -> Result<Self, RiskStoreError> {
        let (sender, receiver) = sync_channel::<Job>(32);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (closed_tx, closed_rx) = watch::channel(false);
        let projection = Arc::new(RwLock::new(None));
        let observe = Arc::clone(&projection);
        thread::Builder::new()
            .name("okx-campaign-sql".into())
            .spawn(move || {
                let mut store = match RiskBookStore::open_existing(&path, &installation, &campaign)
                {
                    Ok(store) => store,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        let _ = closed_tx.send(true);
                        return;
                    }
                };
                let initial = store.readback();
                match initial {
                    Ok(snapshot) => {
                        if let Ok(mut guard) = observe.write() {
                            *guard = Some(snapshot);
                        }
                        let _ = ready_tx.send(Ok(()));
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        let _ = closed_tx.send(true);
                        return;
                    }
                }
                while let Ok(job) = receiver.recv() {
                    job(&mut store, &observe);
                }
                drop(store); // SQLite handles and file lease close BEFORE termination witness.
                let _ = closed_tx.send(true);
            })?;
        ready_rx
            .await
            .map_err(|_| RiskStoreError::WorkerStopped)??;
        Ok(Self {
            worker: Arc::new(Worker {
                sender,
                projection,
                terminated: closed_rx,
            }),
        })
    }

    /// Cached committed observation. Never a financial authority or live read.
    pub(super) fn diagnostic_snapshot(&self) -> Option<BookReadback> {
        self.worker
            .projection
            .read()
            .ok()
            .and_then(|snapshot| snapshot.clone())
    }

    pub(super) fn termination_signal(&self) -> watch::Receiver<bool> {
        self.worker.terminated.clone()
    }

    async fn call<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut RiskBookStore) -> Result<T, RiskStoreError> + Send + 'static,
    ) -> Result<T, RiskStoreError> {
        let (tx, rx) = oneshot::channel();
        let job: Job = Box::new(move |store, projection| {
            let result = work(store);
            // Cache only a physically committed readback, never a requested result.
            match store.readback() {
                Ok(snapshot) => {
                    if let Ok(mut current) = projection.write() {
                        *current = Some(snapshot)
                    }
                }
                Err(_) => {
                    if let Ok(mut current) = projection.write() {
                        *current = None
                    }
                }
            }
            let _ = tx.send(result);
        });
        self.worker
            .sender
            .try_send(job)
            .map_err(|error| match error {
                TrySendError::Full(_) => RiskStoreError::QueueFull,
                TrySendError::Disconnected(_) => RiskStoreError::WorkerStopped,
            })?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| RiskStoreError::OutcomeUnknown)?
            .map_err(|_| RiskStoreError::OutcomeUnknown)?
    }

    pub(super) async fn reserve(
        &self,
        input: ReserveInput,
        census: VerifiedCensus,
    ) -> Result<HeldReceipt, RiskStoreError> {
        self.call(move |store| store.reserve(&input, &census)).await
    }

    pub(super) async fn readback(&self) -> Result<BookReadback, RiskStoreError> {
        self.call(|store| store.readback()).await
    }
}
