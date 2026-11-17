//! Queue scheduling on the ORIGINAL fresh owner. Atomic markers witness job
//! execution only; financial COMMIT/lost-ACK witnesses live in domain tests.
use super::UnifiedFinanceHandle;
use crate::{AccountStore, NewFinanceInstallation, StoreError};
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

async fn fresh() -> (PathBuf, UnifiedFinanceHandle) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "okx-fresh-owner-queue-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let owner = UnifiedFinanceHandle::initialize_new(
        root.clone(),
        NewFinanceInstallation {
            installation_id: "installation_queue".into(),
            initialize_request_id: "initialize_queue_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    (root, owner)
}

fn enqueue<F: Future>(future: Pin<&mut F>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

async fn close(owner: UnifiedFinanceHandle) {
    let mut terminated = owner.termination_signal();
    drop(owner);
    if !*terminated.borrow() {
        tokio::time::timeout(Duration::from_secs(5), terminated.changed())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(*terminated.borrow());
}

/// Original failed-job diagnostic property, now exercised on the real
/// finance.sqlite writer. No test-only per-account SQL owner is involved.
#[tokio::test]
async fn risk_book_contract_fresh_owner_failed_job_counts_and_preserves_installation() {
    let (root, owner) = fresh().await;
    let receipt = owner.installation_receipt(deadline()).await.unwrap();
    let before = owner.sql_timing().observed_jobs;
    let result = owner
        .submit::<()>(|_| Err(StoreError::Invalid("INJECTED_UNIFIED_SQL_FAILURE")))
        .await;
    assert!(matches!(
        result,
        Err(StoreError::Invalid("INJECTED_UNIFIED_SQL_FAILURE"))
    ));
    let after = owner.sql_timing();
    assert_eq!(after.observed_jobs, before + 1);
    assert!(after.last_sql_owner_ms < 5_000);
    assert_eq!(owner.installation_receipt(deadline()).await.unwrap(), receipt);
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root, receipt.installation_id.clone())
        .await
        .unwrap();
    assert_eq!(reopened.installation_receipt(deadline()).await.unwrap(), receipt);
    close(reopened).await;
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_queue_bounded_128_fifo_and_shutdown() {
    let (root, owner) = fresh().await;
    let initial = owner.installation_receipt(deadline()).await.unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut blocker = Box::pin(owner.submit(move |_| {
        started_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_QUEUE_BARRIER_TIMEOUT"))?;
        Ok(())
    }));
    enqueue(blocker.as_mut());
    started_rx.await.unwrap();

    let observed = Arc::new(Mutex::new(Vec::new()));
    let mut queued = Vec::new();
    for index in 0..128 {
        let observed = observed.clone();
        let mut job = Box::pin(owner.submit(move |_| {
            observed.lock().unwrap().push(index);
            Ok(index)
        }));
        enqueue(job.as_mut());
        queued.push(job);
    }
    let rejected_executions = Arc::new(AtomicUsize::new(0));
    let rejected = rejected_executions.clone();
    assert!(matches!(
        owner
            .submit(move |_| {
                rejected.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await,
        Err(StoreError::Invalid("UNIFIED_FINANCE_QUEUE_FULL"))
    ));
    assert!(observed.lock().unwrap().is_empty());
    release_tx.send(()).unwrap();
    // Shutdown closes admission, then drains the accepted FIFO even though
    // every reply receiver has gone away.
    drop(blocker);
    drop(queued);
    close(owner).await;
    assert_eq!(*observed.lock().unwrap(), (0..128).collect::<Vec<_>>());
    assert_eq!(rejected_executions.load(Ordering::SeqCst), 0);
    let reopened = UnifiedFinanceHandle::open_native(root, initial.installation_id.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened.installation_receipt(deadline()).await.unwrap(),
        initial
    );
    close(reopened).await;
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_queue_deadlines_and_dropped_caller() {
    let (root, owner) = fresh().await;
    let initial = owner.installation_receipt(deadline()).await.unwrap();
    let expired_executions = Arc::new(AtomicUsize::new(0));
    let expired = expired_executions.clone();
    assert!(matches!(
        owner
            .submit_before(Some(Instant::now()), move |_| {
                expired.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await,
        Err(StoreError::Invalid("UNIFIED_FINANCE_QUEUE_DEADLINE"))
    ));

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel::<Instant>();
    let mut blocker = Box::pin(owner.submit(move |_| {
        started_tx.send(()).unwrap();
        let expires = release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_QUEUE_BARRIER_TIMEOUT"))?;
        // The dedicated SQL thread waits until the real deadline; no async
        // reactor is blocked, and every barrier has a bounded timeout.
        assert!(matches!(
            release_rx.recv_timeout(expires.saturating_duration_since(Instant::now())),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(Instant::now() >= expires);
        Ok(())
    }));
    enqueue(blocker.as_mut());
    started_rx.await.unwrap();
    let expires = Instant::now() + Duration::from_secs(1);
    let expired = expired_executions.clone();
    let mut queued_expired = Box::pin(owner.submit_before(Some(expires), move |_| {
        expired.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }));
    enqueue(queued_expired.as_mut());

    let completed = Arc::new(AtomicUsize::new(0));
    let completed_job = completed.clone();
    let identity = initial.installation_id.clone();
    let mut abandoned = Box::pin(owner.submit(move |db| {
        let receipt = AccountStore::inspect_finance_installation(db, &identity)?;
        completed_job.fetch_add(1, Ordering::SeqCst);
        Ok(receipt)
    }));
    enqueue(abandoned.as_mut());
    drop(abandoned);
    release_tx.send(expires).unwrap();
    blocker.await.unwrap();
    assert!(matches!(
        queued_expired.await,
        Err(StoreError::Invalid("UNIFIED_FINANCE_QUEUE_DEADLINE"))
    ));
    assert_eq!(
        owner.installation_receipt(deadline()).await.unwrap(),
        initial
    );
    assert_eq!(expired_executions.load(Ordering::SeqCst), 0);
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root, initial.installation_id.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened.installation_receipt(deadline()).await.unwrap(),
        initial
    );
    close(reopened).await;
}
