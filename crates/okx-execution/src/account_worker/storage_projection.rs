//! One bounded derivative copy from the existing SQL owner. No reader can
//! enqueue refresh work or authorize an operation using this observation.
use super::*;
use crate::AccountStorageSnapshot;
use std::sync::atomic::AtomicBool;

const REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(1);
const REFRESH_SQL_BUDGET: Duration = Duration::from_millis(20);

#[derive(Default)]
struct Published {
    snapshot: Option<AccountStorageSnapshot>,
    last_attempt_ms: Option<i64>,
    error_code: Option<String>,
}

#[derive(Default)]
pub(super) struct StorageProjection {
    published: Mutex<Published>,
    current_changes: AtomicU64,
    refresh_attempts: AtomicU64,
    publication_skipped: AtomicU64,
    refresh_pending: AtomicBool,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct AccountStorageReadback {
    pub schema: &'static str,
    pub state: &'static str,
    pub snapshot: Option<AccountStorageSnapshot>,
    pub last_attempt_ms: Option<i64>,
    pub reason_code: Option<String>,
    pub refresh_pending: bool,
    pub refresh_attempts: u64,
    pub publication_skipped: u64,
    pub owner_terminated: bool,
}

impl StorageProjection {
    pub(super) fn readback(&self) -> AccountStorageReadback {
        let mut out = AccountStorageReadback {
            schema: "okx.account-storage-observation/v1",
            state: "NOT_EVALUATED",
            snapshot: None,
            last_attempt_ms: None,
            reason_code: Some("STORAGE_SNAPSHOT_BUSY".into()),
            refresh_pending: true,
            refresh_attempts: self.refresh_attempts.load(Ordering::Relaxed),
            publication_skipped: self.publication_skipped.load(Ordering::Relaxed),
            owner_terminated: false,
        };
        if let Ok(published) = self.published.try_lock() {
            out.snapshot = published.snapshot.clone();
            out.state = if out.snapshot.is_some() {
                "OBSERVED"
            } else {
                "NOT_EVALUATED"
            };
            out.last_attempt_ms = published.last_attempt_ms;
            out.reason_code = published.error_code.clone();
            out.refresh_pending = self.refresh_pending.load(Ordering::Acquire)
                || out.snapshot.as_ref().is_none_or(|s| {
                    s.sql_change_sequence != self.current_changes.load(Ordering::Acquire)
                })
                || out.reason_code.is_some();
            if out.snapshot.is_none() && out.reason_code.is_none() {
                out.reason_code = Some("STORAGE_SNAPSHOT_NOT_SEEDED".into());
            }
        }
        out
    }
}

#[derive(Default)]
pub(super) struct RefreshCadence {
    last_attempt: Option<Instant>,
    last_successful_sequence: Option<u64>,
    last_successful_health: Option<Option<String>>,
}
impl RefreshCadence {
    pub(super) fn observe(&mut self, store: &mut AccountStore, projection: &StorageProjection) {
        self.observe_with_budget(store, projection, REFRESH_SQL_BUDGET);
    }

    fn observe_with_budget(
        &mut self,
        store: &mut AccountStore,
        projection: &StorageProjection,
        budget: Duration,
    ) {
        let sequence = store.diagnostic_change_sequence();
        projection
            .current_changes
            .store(sequence, Ordering::Release);
        let health = store.storage_health().map(str::to_owned);
        let dirty = self.last_successful_sequence != Some(sequence)
            || self.last_successful_health.as_ref() != Some(&health);
        projection.refresh_pending.store(dirty, Ordering::Release);
        if !dirty
            || self
                .last_attempt
                .is_some_and(|at| at.elapsed() < REFRESH_MIN_INTERVAL)
        {
            return;
        }
        self.last_attempt = Some(Instant::now());
        let previous = match projection.published.try_lock() {
            Ok(published) => published.snapshot.clone(),
            Err(_) => {
                projection
                    .publication_skipped
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        projection.refresh_attempts.fetch_add(1, Ordering::Relaxed);
        let attempt_ms = wall_ms();
        let result = if budget.is_zero() {
            Err(StoreError::Invalid("ACCOUNT_STORAGE_DEADLINE"))
        } else {
            store.run_before_deadline(Instant::now() + budget, |s| {
                s.diagnostic_storage_snapshot(previous.as_ref())
            })
        };
        let Ok(mut published) = projection.published.try_lock() else {
            projection
                .publication_skipped
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        published.last_attempt_ms = Some(attempt_ms);
        match result {
            Ok(snapshot) => {
                self.last_successful_sequence = Some(sequence);
                self.last_successful_health = Some(health);
                published.snapshot = Some(snapshot);
                published.error_code = None;
                projection.refresh_pending.store(false, Ordering::Release);
            }
            Err(error) => {
                published.error_code = Some(match error {
                    StoreError::Invalid(code) => code.to_owned(),
                    _ => "ACCOUNT_STORAGE_UNAVAILABLE".into(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests;
