use super::*;
use rust_decimal::Decimal;

async fn fixture() -> (AccountStoreHandle, PathBuf) {
    let root = std::env::temp_dir().join(format!(
        "okx-storage-projection-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let store = AccountStoreHandle::initialize_flat(
        root.join("account.db"),
        AccountMeta {
            account_key: "okx:real:projection-account".into(),
            installation_id: "projection-install".into(),
            initialized_at: 1,
            opening_source: "flat-projection-fixture".into(),
            initialize_operation_id: "projection-initialize".into(),
            release_id: "fixture-release".into(),
        },
        OpeningSnapshot {
            source_ref: "flat-projection-fixture".into(),
            observed_at: 1,
            opening_equity_usd: Decimal::from(100),
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USD".into(), Decimal::from(100))],
        },
    )
    .await
    .unwrap();
    await_readback(&store, |r| r.snapshot.is_some()).await;
    (store, root)
}

async fn await_readback(
    store: &AccountStoreHandle,
    predicate: impl Fn(&AccountStorageReadback) -> bool,
) -> AccountStorageReadback {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let out = store.diagnostic_storage_snapshot();
            if predicate(&out) {
                return out;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owner must publish or expose its concrete failure")
}

async fn close(store: AccountStoreHandle, root: PathBuf) {
    let mut terminated = store.termination_signal();
    drop(store);
    if !*terminated.borrow() {
        tokio::time::timeout(Duration::from_secs(3), terminated.changed())
            .await
            .unwrap()
            .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn startup_snapshot_is_useful_and_thousand_readonly_jobs_do_not_refresh_it() {
    let (store, root) = fixture().await;
    let before = store.diagnostic_storage_snapshot();
    let snapshot = before.snapshot.as_ref().unwrap();
    assert_eq!(snapshot.account, "okx:real:projection-account");
    assert_eq!(snapshot.stop_count, 0);
    assert_eq!(snapshot.source_coverage.len(), 5);
    assert!(!snapshot.sqlite_version.is_empty());
    assert!(snapshot.database_bytes > 0);
    assert_eq!(store.sql_timing().observed_jobs, 0);
    for _ in 0..1000 {
        store
            .call(Instant::now() + Duration::from_secs(3), |_| Ok(()))
            .await
            .unwrap();
    }
    assert_eq!(store.sql_timing().observed_jobs, 1000);
    assert_eq!(
        store.diagnostic_storage_snapshot().refresh_attempts,
        before.refresh_attempts
    );
    close(store, root).await;
}

#[tokio::test]
async fn held_owner_does_not_block_snapshot_or_enqueue_any_sql_job() {
    let (store, root) = fixture().await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held = store.clone();
    let pending = tokio::spawn(async move {
        held.call(Instant::now() + Duration::from_secs(5), move |_| {
            let _ = entered_tx.send(());
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            Ok(())
        })
        .await
    });
    entered_rx.await.unwrap();
    for _ in 0..1000 {
        assert!(store.diagnostic_storage_snapshot().snapshot.is_some());
    }
    assert!(!pending.is_finished());
    assert_eq!(store.sql_timing().observed_jobs, 0);
    release_tx.send(()).unwrap();
    pending.await.unwrap().unwrap();
    store
        .call(Instant::now() + Duration::from_secs(3), |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(store.sql_timing().observed_jobs, 2);
    close(store, root).await;
}

#[tokio::test]
async fn committed_policy_updates_publish() {
    let (store, root) = fixture().await;
    store
        .call(Instant::now() + Duration::from_secs(3), |s| {
            s.apply_authority_policy(1, true, "projection-campaign", Decimal::from(10))
        })
        .await
        .unwrap();
    let updated = await_readback(&store, |r| {
        r.snapshot.as_ref().is_some_and(|s| s.policy.revision == 1)
    })
    .await;
    assert!(updated.snapshot.as_ref().unwrap().policy.explicit_pause);
    close(store, root).await;
}

#[test]
fn held_projection_lock_is_explicitly_unknown_and_does_not_wait() {
    let projection = StorageProjection::default();
    let _held = projection.published.lock().unwrap();
    let out = projection.readback();
    assert_eq!(out.state, "NOT_EVALUATED");
    assert_eq!(out.reason_code.as_deref(), Some("STORAGE_SNAPSHOT_BUSY"));
    assert!(out.snapshot.is_none());
}

#[tokio::test]
async fn refresh_budget_failure_preserves_known_facts_and_recovers() {
    let (store, root) = fixture().await;
    store
        .call(Instant::now() + Duration::from_secs(3), |sql| {
            let projection = StorageProjection::default();
            let mut cadence = RefreshCadence::default();
            cadence.observe(sql, &projection);
            let original = projection.readback().snapshot.unwrap();
            sql.latch_stop("budget-stop", "account", "TEST_STOP", 20)?;
            cadence.last_attempt = None;
            cadence.observe_with_budget(sql, &projection, Duration::ZERO);
            let failed = projection.readback();
            assert_eq!(
                failed.reason_code.as_deref(),
                Some("ACCOUNT_STORAGE_DEADLINE")
            );
            assert!(failed.refresh_pending);
            assert_eq!(failed.snapshot.as_ref().unwrap().stop_count, 0);
            assert_eq!(
                failed.snapshot.unwrap().observed_at_ms,
                original.observed_at_ms
            );
            cadence.last_attempt = None;
            cadence.observe(sql, &projection);
            let recovered = projection.readback();
            assert!(recovered.reason_code.is_none());
            assert!(!recovered.refresh_pending);
            assert_eq!(recovered.snapshot.unwrap().stop_count, 1);
            Ok(())
        })
        .await
        .unwrap();
    close(store, root).await;
}

#[tokio::test]
async fn reopened_owner_seeds_original_sql_and_bounds_stop_details() {
    let (store, root) = fixture().await;
    store
        .call(Instant::now() + Duration::from_secs(3), |sql| {
            for id in 0..15 {
                sql.latch_stop(&format!("persisted-stop-{id}"), "account", "TEST_STOP", id)?;
            }
            Ok(())
        })
        .await
        .unwrap();
    let mut terminated = store.termination_signal();
    drop(store);
    if !*terminated.borrow() {
        tokio::time::timeout(Duration::from_secs(3), terminated.changed())
            .await
            .unwrap()
            .unwrap();
    }
    let store = AccountStoreHandle::open_existing(
        root.join("account.db"),
        "okx:real:projection-account".into(),
        "projection-install".into(),
    )
    .await
    .unwrap();
    let restored = await_readback(&store, |r| r.snapshot.is_some()).await;
    let snapshot = restored.snapshot.unwrap();
    assert_eq!(snapshot.stop_count, 15);
    assert_eq!(snapshot.stops.len(), 10);
    assert_eq!(snapshot.stops[0].id, "persisted-stop-0");
    assert_eq!(store.sql_timing().observed_jobs, 0);
    close(store, root).await;
}
