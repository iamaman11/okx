//! Account-local native adapters. AccountStoreHandle is the sole SQL owner;
//! this object owns no parallel ledger, access grant or financial epoch.
use crate::AgentResult;
use okx_api::{
    AccountApi, AccountHistoryApi, Credentials, OkxEnvironment, OkxRestClient, RateBudget, TradeApi,
};
use okx_execution::AccountStoreHandle;
use std::time::{Duration, Instant};

// This cache retains ONLY the authenticated executor UID proof, not a
// financial/account snapshot or permission to trade. A new source generation
// or a post-HELD mutation still obtains its own independent signed facts.
const EXECUTOR_IDENTITY_REUSE_AGE: Duration = Duration::from_secs(2);

struct ExecutorIdentityProof {
    verified_uid: String,
    generation: u64,
    received_at: Instant,
}

fn current_executor_identity(
    proof: Option<&ExecutorIdentityProof>,
    expected: &str,
    generation: u64,
    now: Instant,
) -> bool {
    proof.is_some_and(|p| {
        p.verified_uid == expected
            && p.generation == generation
            && now.saturating_duration_since(p.received_at) < EXECUTOR_IDENTITY_REUSE_AGE
    })
}

fn identity_source_deadline() -> okx_api::OkxError {
    okx_api::OkxError::Response("EXECUTOR_IDENTITY_SOURCE_DEADLINE".into())
}

pub struct ExecutionRuntime {
    pub store: AccountStoreHandle,
    pub trade: TradeApi,
    pub history: AccountHistoryApi,
    pub account: AccountApi,
    pub clock: OkxRestClient,
    pub environment: OkxEnvironment,
    // Serialize finance source windows per account before reading SQL cursors.
    // No second capture may replay an older watermark after another commits.
    pub(crate) source_capture_serial: tokio::sync::Semaphore,
    // An account-local singleflight for overlapping background/Status UID
    // checks. No reuse across verified UIDs, and never across the 2s source
    // age. Actual trade Execute still reads executor config after HELD.
    executor_identity: tokio::sync::Mutex<Option<ExecutorIdentityProof>>,
    executor_identity_refresh: tokio::sync::Semaphore,
    uid: String,
}
impl ExecutionRuntime {
    pub fn new(
        store: AccountStoreHandle,
        environment: OkxEnvironment,
        credentials: Credentials,
        uid: String,
        budget: RateBudget,
    ) -> AgentResult<Self> {
        let rest = OkxRestClient::with_rate_budget(environment, credentials, budget)?;
        Ok(Self::from_client(store, rest, environment, uid))
    }

    /// One composition path; the SQL owner and execution code remain unchanged.
    pub fn from_client(
        store: AccountStoreHandle,
        rest: OkxRestClient,
        environment: OkxEnvironment,
        uid: String,
    ) -> Self {
        Self {
            store,
            trade: TradeApi::new(rest.clone()),
            history: AccountHistoryApi::new(rest.clone()),
            account: AccountApi::new(rest.clone()),
            clock: rest,
            environment,
            source_capture_serial: tokio::sync::Semaphore::new(1),
            executor_identity: tokio::sync::Mutex::new(None),
            executor_identity_refresh: tokio::sync::Semaphore::new(1),
            uid,
        }
    }
    pub async fn verified_uid_matches_until(
        &self,
        uid: &str,
        generation: u64,
        until: Instant,
    ) -> AgentResult<bool> {
        if self.uid != uid {
            return Ok(false);
        }
        // Cache-state mutex is never held across authenticated HTTP.
        let guard = tokio::time::timeout_at(until.into(), self.executor_identity.lock())
            .await
            .map_err(|_| identity_source_deadline())?;
        if current_executor_identity(guard.as_ref(), uid, generation, Instant::now()) {
            return Ok(true);
        }
        drop(guard);
        // Account-local singleflight; the caller deadline includes queue wait.
        let _refresh = tokio::time::timeout_at(
            until.into(), self.executor_identity_refresh.acquire(),
        )
        .await
        .map_err(|_| identity_source_deadline())?
        .map_err(|_| identity_source_deadline())?;
        let guard = tokio::time::timeout_at(until.into(), self.executor_identity.lock())
            .await
            .map_err(|_| identity_source_deadline())?;
        if current_executor_identity(guard.as_ref(), uid, generation, Instant::now()) {
            return Ok(true);
        }
        drop(guard);
        let config = tokio::time::timeout_at(
            until.into(),
            self.account.config_wait(until.saturating_duration_since(Instant::now())),
        )
        .await
        .map_err(|_| identity_source_deadline())??;
        let received_at = Instant::now();
        let mut guard = tokio::time::timeout_at(until.into(), self.executor_identity.lock())
            .await
            .map_err(|_| identity_source_deadline())?;
        if okx_api::account_uid_fingerprint(&config.uid) != uid {
            *guard = None;
            return Ok(false);
        }
        *guard = Some(ExecutorIdentityProof {
            verified_uid: uid.to_owned(), generation, received_at,
        });
        Ok(true)
    }

    pub fn uid(&self) -> &str {
        &self.uid
    }
}

#[cfg(test)]
mod identity_cache_tests {
    use super::*;

    #[tokio::test]
    async fn uid_waiting_is_bounded_and_other_account_progresses() {
        let a = tokio::sync::Semaphore::new(1);
        let b = tokio::sync::Semaphore::new(1);
        let guard = a.acquire().await.unwrap();
        assert!(tokio::time::timeout_at(
            (Instant::now() + Duration::from_millis(80)).into(), a.acquire(),
        ).await.is_err());
        assert!(tokio::time::timeout_at(
            (Instant::now() + Duration::from_secs(1)).into(), b.acquire(),
        ).await.is_ok());
        drop(guard);
        assert_eq!(a.available_permits(), 1);
    }

    #[test]
    fn executor_uid_cache_cannot_cross_account_or_outlive_signed_source() {
        let now = Instant::now();
        let proof = ExecutorIdentityProof {
            verified_uid: "verified-a".into(),
            generation: 17,
            received_at: now,
        };
        assert!(current_executor_identity(
            Some(&proof),
            "verified-a",
            17,
            now
        ));
        assert!(!current_executor_identity(
            Some(&proof),
            "verified-b",
            17,
            now
        ));
        assert!(!current_executor_identity(
            Some(&proof),
            "verified-a",
            18,
            now
        ));
        assert!(!current_executor_identity(
            Some(&proof),
            "verified-a",
            17,
            now + EXECUTOR_IDENTITY_REUSE_AGE,
        ));
        assert!(!current_executor_identity(None, "verified-a", 17, now));
    }
}
