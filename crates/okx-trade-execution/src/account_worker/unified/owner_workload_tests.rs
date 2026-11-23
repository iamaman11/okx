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
    let mut latency_us = Vec::new();
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
                (cmd, result.receipt, started.elapsed().as_micros() as u64)
            });
        }
        while let Some(done) = tasks.join_next().await {
            let (cmd, receipt, elapsed) = done.unwrap();
            original.push((cmd, receipt));
            latency_us.push(elapsed);
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
    if let Some(directory) = std::env::var_os("OKX_NATIVE_ACCEPTANCE_WORKLOAD_DIR") {
        use std::io::Write;
        let directory = std::path::PathBuf::from(directory);
        assert_eq!(
            directory.parent().unwrap().canonicalize().unwrap(),
            std::env::temp_dir().canonicalize().unwrap()
        );
        assert!(
            directory
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("okx-acceptance-measurements-")
        );
        assert!(
            !std::fs::symlink_metadata(&directory)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        latency_us.sort_unstable();
        let report = serde_json::json!({
            "schema":"okx.native-owner-workload/v1",
            "run_id":std::env::var("OKX_NATIVE_ACCEPTANCE_RUN_ID").unwrap(),
            "operations":64,"concurrency":16,"replays":64,"ledger_legs":legs,
            "latency_us":latency_us,"p50_us":latency_us[31],"p95_us":latency_us[60],
            "max_us":latency_us[63],"sql_jobs":timing.observed_jobs-before.observed_jobs,
            "max_queue_wait_ms":timing.max_queue_wait_ms,"max_sql_owner_ms":timing.max_sql_owner_ms,
            "storage_bytes":bytes,"setup_through_teardown_us":setup.elapsed().as_micros()
        });
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("owner-workload.json"))
            .unwrap();
        file.write_all(report.to_string().as_bytes()).unwrap();
    }
}
