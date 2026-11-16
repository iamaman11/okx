//! Bounded financial workload, not a venue-throughput or 32-account claim.
//! Uses the existing fresh-install fixture and the public capital command.
use super::{UnifiedFinanceHandle, owner_financial_reply_tests as fixture};
use rust_decimal::Decimal;
use std::time::{Duration, Instant};

const BATCHES: usize = 4;
const CONCURRENCY: usize = 16;
const OPERATION_BUDGET: Duration = Duration::from_secs(5);
const TOTAL_BUDGET: Duration = Duration::from_secs(30);
const STORAGE_BUDGET: u64 = 32 * 1024 * 1024;

async fn close(owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(OPERATION_BUDGET, stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(*stopped.borrow());
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_bounded_financial_workload_and_reopen() {
    // Limits are acceptance watchdogs for this declared debug workload; no
    // production deadline or money/source freshness rule is changed.
    let setup = Instant::now();
    let (root, owner) = fixture::fresh().await;
    let before = owner.sql_timing();
    let mut original = Vec::new();
    for batch in 0..BATCHES {
        let mut tasks = tokio::task::JoinSet::new();
        for lane in 0..CONCURRENCY {
            let handle = owner.clone();
            let mut cmd = fixture::command();
            cmd.operation_id = format!("bounded_capital_{batch}_{lane}");
            cmd.delta_usd = Decimal::new(1, 2);
            let started = Instant::now();
            tasks.spawn(async move {
                let result = tokio::time::timeout(
                    OPERATION_BUDGET,
                    handle.assign_capital(fixture::account(), cmd.clone()),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(started.elapsed() < OPERATION_BUDGET);
                (cmd, result.receipt)
            });
        }
        while let Some(done) = tasks.join_next().await {
            original.push(done.unwrap());
        }
    }
    assert_eq!(original.len(), BATCHES * CONCURRENCY);
    let state = owner
        .capital_state(fixture::account(), 102, Instant::now() + OPERATION_BUDGET)
        .await
        .unwrap();
    assert_eq!(state.assigned_usd, Decimal::new(64, 2));
    assert_eq!(state.account_nav_usd, Decimal::from(100));
    assert_eq!(state.unattributed_residual_usd, Decimal::new(9936, 2));
    let timing = owner.sql_timing();
    assert_eq!(timing.observed_jobs - before.observed_jobs, 65);
    assert!(timing.max_queue_wait_ms < OPERATION_BUDGET.as_millis() as u64);
    assert!(timing.max_sql_owner_ms < OPERATION_BUDGET.as_millis() as u64);
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root.clone(), fixture::INSTALLATION.into())
        .await
        .unwrap();
    for (cmd, receipt) in original {
        assert_eq!(
            reopened
                .assign_capital(fixture::account(), cmd)
                .await
                .unwrap()
                .receipt,
            receipt
        );
    }
    let state = reopened
        .capital_state(fixture::account(), 102, Instant::now() + OPERATION_BUDGET)
        .await
        .unwrap();
    assert_eq!(state.assigned_usd, Decimal::new(64, 2));
    let legs = reopened
        .submit(|db| {
            Ok(
                db.query_row("SELECT COUNT(*) FROM finance_entries", [], |r| {
                    r.get::<_, i64>(0)
                })?,
            )
        })
        .await
        .unwrap();
    assert_eq!(
        legs, 128,
        "64 balanced two-leg commits; replay creates no legs"
    );
    close(reopened).await;
    let bytes: u64 = std::fs::read_dir(root.join("finance"))
        .unwrap()
        .map(|item| {
            let entry = item.unwrap();
            let info = entry.file_type().unwrap();
            assert!(!info.is_symlink());
            assert!(info.is_file(), "workload finance root has no nested stores");
            entry.metadata().unwrap().len()
        })
        .sum();
    assert!(bytes <= STORAGE_BUDGET, "bounded workload storage: {bytes}");
    // Validate the exact disposable root before recursive cleanup.
    assert_eq!(
        root.parent().unwrap().canonicalize().unwrap(),
        std::env::temp_dir().canonicalize().unwrap()
    );
    assert!(
        root.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("okx-fresh-financial-reply-")
    );
    std::fs::remove_dir_all(root).unwrap();
    assert!(
        setup.elapsed() < TOTAL_BUDGET,
        "includes setup/reopen/replay/teardown"
    );
}
