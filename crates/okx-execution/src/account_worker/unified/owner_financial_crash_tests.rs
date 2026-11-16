//! Real child death on both sides of a fresh owner's financial COMMIT.
//! Reuses the typed capital fixture; this is not a venue-send crash proof.
use super::owner_financial_reply_tests::{INSTALLATION, account, assert_once, command, fresh_at};
use super::{UnifiedFinanceHandle, UnifiedFinanceWrite};
use crate::StoreError;
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn boundary(phase: &str) -> ! {
    println!("NATIVE_FINANCIAL_COMMIT_BARRIER:{phase}");
    std::io::stdout().flush().unwrap();
    // The parent OS kills this process. No graceful drop/transaction rollback.
    let (_hold, forever) = mpsc::channel::<()>();
    let _ = forever.recv();
    unreachable!()
}

async fn close(owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(5), stopped.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_os_kill_before_and_after_financial_commit() {
    if let Some(root) = std::env::var_os("OKX_FINANCIAL_COMMIT_CHILD_ROOT") {
        let root = PathBuf::from(root);
        assert!(root.is_absolute());
        assert_eq!(
            root.parent().unwrap().canonicalize().unwrap(),
            std::env::temp_dir().canonicalize().unwrap()
        );
        assert!(
            root.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("okx-financial-commit-kill-")
        );
        assert!(!root.exists());
        std::fs::create_dir(&root).unwrap();
        let owner = fresh_at(root).await;
        let phase = std::env::var("OKX_FINANCIAL_COMMIT_CHILD_PHASE").unwrap();
        owner
            .submit(move |db| -> crate::StoreResult<()> {
                let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let write = UnifiedFinanceWrite::AssignBoundCapital {
                    account: account(),
                    command: command(),
                };
                super::source_permission::require_command_permission(&tx, INSTALLATION, &write)?;
                super::apply_original_unified_finance_transaction(&tx, INSTALLATION, write)?;
                if phase == "BEFORE_COMMIT" {
                    boundary(&phase);
                }
                tx.commit().map_err(|_| {
                    StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
                })?;
                assert_eq!(phase, "AFTER_COMMIT");
                boundary(&phase);
            })
            .await
            .unwrap();
        unreachable!();
    }
    for phase in ["BEFORE_COMMIT", "AFTER_COMMIT"] {
        let root = std::env::temp_dir().join(format!(
            "okx-financial-commit-kill-{}-{}-{phase}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        assert!(!root.exists());
        let symbol = concat!(
            module_path!(),
            "::risk_book_contract_fresh_owner_os_kill_before_and_after_financial_commit"
        );
        // libtest names omit the crate prefix returned by module_path!.
        let symbol = symbol.strip_prefix("okx_execution::").unwrap();
        let mut child = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", symbol, "--nocapture"])
                .env("OKX_FINANCIAL_COMMIT_CHILD_ROOT", &root)
                .env("OKX_FINANCIAL_COMMIT_CHILD_PHASE", phase)
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line.unwrap()).is_err() {
                    break;
                }
            }
        });
        let until = Instant::now() + Duration::from_secs(15);
        let mut tail = std::collections::VecDeque::new();
        loop {
            let line = rx
                .recv_timeout(until.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|e| {
                    panic!("{phase}: child failed before durable boundary: {e}; stdout={tail:?}")
                });
            if line == format!("NATIVE_FINANCIAL_COMMIT_BARRIER:{phase}") {
                break;
            }
            if tail.len() == 16 {
                tail.pop_front();
            }
            tail.push_back(line);
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        drop(rx);
        reader.join().unwrap();
        let owner = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
            .await
            .unwrap();
        assert!(
            matches!(
                UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into()).await,
                Err(StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY"))
            ),
            "second writer must remain fenced by the real writer lease"
        );
        if phase == "BEFORE_COMMIT" {
            assert!(
                owner
                    .operation(
                        Instant::now() + Duration::from_secs(3),
                        account(),
                        command().operation_id
                    )
                    .await
                    .unwrap()
                    .is_none()
            );
            let (entries, links): (i64, i64) = owner
                .submit(|db| {
                    Ok((
                        db.query_row("SELECT COUNT(*) FROM finance_entries", [], |r| r.get(0))?,
                        db.query_row("SELECT COUNT(*) FROM finance_source_links", [], |r| {
                            r.get(0)
                        })?,
                    ))
                })
                .await
                .unwrap();
            assert_eq!(
                (entries, links),
                (0, 0),
                "uncommitted transfer must have no partial legs"
            );
        } else {
            assert_once(&owner).await;
        }
        // Same operation is either the first committed transfer or a durable
        // replay, and must always leave exactly the original two conserved legs.
        let first = owner.assign_capital(account(), command()).await.unwrap();
        assert_eq!(assert_once(&owner).await, first.receipt);
        assert_eq!(
            owner
                .assign_capital(account(), command())
                .await
                .unwrap()
                .receipt,
            first.receipt
        );
        close(owner).await;
        let reopened = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
            .await
            .unwrap();
        assert_eq!(assert_once(&reopened).await, first.receipt);
        close(reopened).await;
        // This is our directly-created isolated test root, never an installed root.
        assert_eq!(
            root.parent().unwrap().canonicalize().unwrap(),
            std::env::temp_dir().canonicalize().unwrap()
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
