//! Research-only admission. One heavy slot and one shared memory reservation
//! are held for the entire source -> replay -> immutable publish -> SQL commit.
//! Neither permit is ever held by the trading engine or finance owner.
use std::{sync::Arc, time::Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{budget, executor};

pub(super) struct Lease {
    _heavy: OwnedSemaphorePermit,
    _memory: OwnedSemaphorePermit,
    pub(super) wait_ms: u64,
    pub(super) reserved_mib: u32,
}

pub(super) async fn acquire(
    heavy: &Arc<Semaphore>,
    memory: &Arc<Semaphore>,
    reserved_mib: u32,
    cancel: &Arc<executor::Cancel>,
) -> Result<Lease, String> {
    if reserved_mib == 0 || reserved_mib > budget::MEMORY_TOTAL_MIB {
        return Err("LIMIT_EXCEEDED:RESEARCH_MEMORY".into());
    }
    let start = Instant::now();
    let heavy = tokio::select! {
        biased;
        _ = executor::cancelled(cancel.clone()) => return Err("CANCELLED".into()),
        permit = heavy.clone().acquire_owned() => permit.map_err(|_| "ANALYTICS_QUEUE_CLOSED")?,
    };
    // Drop the heavy permit automatically if cancelled while waiting for RAM.
    let memory = tokio::select! {
        biased;
        _ = executor::cancelled(cancel.clone()) => return Err("CANCELLED".into()),
        permit = memory.clone().acquire_many_owned(reserved_mib) =>
            permit.map_err(|_| "RESEARCH_MEMORY_POOL_CLOSED")?,
    };
    if cancel.load(std::sync::atomic::Ordering::Acquire) {
        return Err("CANCELLED".into());
    }
    let elapsed = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
    Ok(Lease {
        _heavy: heavy,
        _memory: memory,
        wait_ms: elapsed,
        reserved_mib,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn t10_two_heavy_jobs_share_four_gib_and_cancelled_waiter_leaks_no_permits() {
        let heavy = Arc::new(Semaphore::new(2));
        let memory = Arc::new(Semaphore::new(budget::MEMORY_TOTAL_MIB as usize));
        let a = Arc::new(executor::Cancel::default());
        let b = Arc::new(executor::Cancel::default());
        let c = Arc::new(executor::Cancel::default());
        let first = acquire(&heavy, &memory, 3072, &a).await.unwrap();
        assert_eq!(first.reserved_mib, 3072);
        assert_eq!(memory.available_permits(), 1024);

        // B acquires the second heavy slot and must queue on memory, not run.
        let h = heavy.clone();
        let m = memory.clone();
        let token = b.clone();
        let second = tokio::spawn(async move { acquire(&h, &m, 3072, &token).await });
        timeout(Duration::from_secs(1), async {
            while heavy.available_permits() != 0 || memory.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Tokio acquire_many reserves the 1024 spare permits while queued.
        // They do not authorize execution and must be returned on cancel.
        assert_eq!(memory.available_permits(), 0);

        // C queues for heavy; cancellation of B releases its heavy slot and
        // cancellation of C must never reserve any memory.
        let h = heavy.clone();
        let m = memory.clone();
        let token = c.clone();
        let third = tokio::spawn(async move { acquire(&h, &m, 3072, &token).await });
        c.store(true, Ordering::Release);
        assert_eq!(
            timeout(Duration::from_secs(1), third)
                .await
                .unwrap()
                .unwrap()
                .err()
                .as_deref(),
            Some("CANCELLED")
        );
        b.store(true, Ordering::Release);
        assert_eq!(
            timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap()
                .err()
                .as_deref(),
            Some("CANCELLED")
        );
        assert_eq!(heavy.available_permits(), 1);
        assert_eq!(memory.available_permits(), 1024);
        drop(first);
        assert_eq!(heavy.available_permits(), 2);
        assert_eq!(
            memory.available_permits(),
            budget::MEMORY_TOTAL_MIB as usize
        );
        let fresh = acquire(
            &heavy,
            &memory,
            3072,
            &Arc::new(executor::Cancel::default()),
        )
        .await
        .unwrap();
        assert_eq!(fresh.reserved_mib, 3072);
    }

    #[tokio::test]
    async fn t10_cancel_after_both_permits_releases_exact_reservation_on_drop() {
        let heavy = Arc::new(Semaphore::new(2));
        let memory = Arc::new(Semaphore::new(budget::MEMORY_TOTAL_MIB as usize));
        let token = Arc::new(executor::Cancel::default());
        let lease = acquire(&heavy, &memory, 2048, &token).await.unwrap();
        assert_eq!(heavy.available_permits(), 1);
        assert_eq!(memory.available_permits(), 2048);
        token.store(true, Ordering::Release);
        // A running CPU task checks the token; its permits are retained until
        // the execution scope and its outstanding artifacts are dropped.
        assert_eq!(heavy.available_permits(), 1);
        assert_eq!(memory.available_permits(), 2048);
        drop(lease);
        assert_eq!(heavy.available_permits(), 2);
        assert_eq!(memory.available_permits(), 4096);
        assert_eq!(
            acquire(&heavy, &memory, 0, &token).await.err().as_deref(),
            Some("LIMIT_EXCEEDED:RESEARCH_MEMORY")
        );
        assert_eq!(
            acquire(&heavy, &memory, 4097, &token)
                .await
                .err()
                .as_deref(),
            Some("LIMIT_EXCEEDED:RESEARCH_MEMORY")
        );
    }

    #[tokio::test]
    async fn t10_two_admitted_jobs_never_allow_a_third_to_oversubscribe() {
        let heavy = Arc::new(Semaphore::new(3));
        let memory = Arc::new(Semaphore::new(budget::MEMORY_TOTAL_MIB as usize));
        let token = Arc::new(executor::Cancel::default());
        let first = acquire(&heavy, &memory, 1536, &token).await.unwrap();
        let second = acquire(&heavy, &memory, 1536, &token).await.unwrap();
        assert_eq!(memory.available_permits(), 1024);
        let (h, m, c) = (heavy.clone(), memory.clone(), token.clone());
        let mut third = tokio::spawn(async move { acquire(&h, &m, 1536, &c).await });
        assert!(
            timeout(Duration::from_millis(30), &mut third)
                .await
                .is_err()
        );
        // A pending acquire_many hides the 1024 otherwise free permits until
        // enough permits arrive; it is not a third granted resource lease.
        assert_eq!(memory.available_permits(), 0);
        drop(first);
        let third_lease = timeout(Duration::from_secs(1), third)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(third_lease.reserved_mib, 1536);
        assert_eq!(memory.available_permits(), 1024);
        drop(second);
        drop(third_lease);
        assert_eq!(
            memory.available_permits(),
            budget::MEMORY_TOTAL_MIB as usize
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t14_cancel_while_waiting_for_ram_is_durable_and_never_resurrected() {
        use super::super::jobs::{Jobs, State};
        let path = std::env::temp_dir()
            .join(format!(
                "okx-t14-resource-{}-{}",
                std::process::id(),
                jiff::Timestamp::now().as_nanosecond()
            ))
            .join("catalog.sqlite");
        let jobs = Jobs::open(path.clone()).unwrap();
        let heavy = Arc::new(Semaphore::new(2));
        let memory = Arc::new(Semaphore::new(budget::MEMORY_TOTAL_MIB as usize));
        let (running, _, run_token) = jobs.enqueue("t14-running".into(), "run".into()).unwrap();
        let (queued, _, queue_token) = jobs.enqueue("t14-queued".into(), "queue".into()).unwrap();
        let owned = acquire(&heavy, &memory, 3072, &run_token).await.unwrap();
        jobs.update(&running.id, State::Running, None, None)
            .unwrap();
        let blocked = {
            let heavy = heavy.clone();
            let memory = memory.clone();
            let token = queue_token.clone();
            tokio::spawn(async move { acquire(&heavy, &memory, 3072, &token).await })
        };
        timeout(Duration::from_secs(1), async {
            while heavy.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(jobs.begin_drain().unwrap(), 2);
        assert_eq!(
            jobs.enqueue("t14-new".into(), "new".into()).unwrap_err(),
            "RESEARCH_UPDATING"
        );
        let cancelled = jobs.cancel(&queued.id, "cancel-on-memory-wait").unwrap();
        assert_eq!(cancelled.state, State::Cancelled);
        assert!(queue_token.load(Ordering::Acquire));
        assert_eq!(
            timeout(Duration::from_secs(1), blocked)
                .await
                .unwrap()
                .unwrap()
                .err()
                .as_deref(),
            Some("CANCELLED")
        );
        assert_eq!(memory.available_permits(), 1024);
        assert_eq!(heavy.available_permits(), 1);

        // A stale callback must not make a cancelled queued job successful.
        jobs.update(&queued.id, State::Completed, Some("late".into()), None)
            .unwrap();
        assert_eq!(jobs.get(&queued.id).unwrap().state, State::Cancelled);
        jobs.update(&running.id, State::Completed, Some("proof".into()), None)
            .unwrap();
        assert_eq!(jobs.begin_drain().unwrap(), 0);
        drop(owned);
        assert_eq!(memory.available_permits(), 4096);
        assert_eq!(heavy.available_permits(), 2);
        drop(jobs);

        let reopened = Jobs::open(path).unwrap();
        assert_eq!(reopened.get(&queued.id).unwrap().state, State::Cancelled);
        assert_eq!(reopened.get(&running.id).unwrap().state, State::Completed);
        assert_eq!(
            reopened.get(&running.id).unwrap().artifact.as_deref(),
            Some("proof")
        );
        let (_, duplicate, _) = reopened
            .enqueue("t14-queued".into(), "queue".into())
            .unwrap();
        assert!(!duplicate);
        assert!(
            !reopened
                .update(&queued.id, State::Completed, Some("forged".into()), None)
                .unwrap()
        );
        assert!(reopened.get(&queued.id).unwrap().artifact.is_none());
    }
}
