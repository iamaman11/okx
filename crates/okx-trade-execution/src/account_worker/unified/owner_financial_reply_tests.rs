//! Financial reply loss on the real NEW installation writer. This witnesses
//! conserved capital transfer, not Reserve/BeginPost or a venue no-send proof.
use super::{
    NativeCampaignAccountPolicy, NativeCampaignPolicy, NativeCampaignStrategyPolicy,
    StrategyMutationCommand, UnifiedFinanceHandle, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
};
use crate::{
    AccountMeta, AccountStore, CapitalAssignmentCommand, NewFinanceInstallation, OpeningSnapshot,
    OperationReceipt, StoreError, StrategyInstance,
};
use rust_decimal::Decimal;
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(super) const INSTALLATION: &str = "installation_financial_reply";

pub(super) fn account() -> String {
    format!("okx:real:{}", "a".repeat(64))
}
pub(super) fn campaign() -> String {
    format!("sha256:{}", "c".repeat(64))
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn enqueue<F: Future>(future: Pin<&mut F>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(5), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(*stopped.borrow());
}
pub(super) async fn fresh() -> (PathBuf, UnifiedFinanceHandle) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "okx-fresh-financial-reply-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let owner = fresh_at(root.clone()).await;
    (root, owner)
}
// The caller supplies an existing isolated, empty test directory.
pub(super) async fn fresh_at(root: PathBuf) -> UnifiedFinanceHandle {
    let owner = UnifiedFinanceHandle::initialize_new(
        root.clone(),
        NewFinanceInstallation {
            installation_id: INSTALLATION.into(),
            initialize_request_id: "initialize_financial_reply_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    let opening = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: "b".repeat(64),
        observed_at: 100,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    owner
        .initialize_account(
            deadline(),
            AccountMeta {
                account_key: account(),
                installation_id: INSTALLATION.into(),
                initialized_at: 100,
                opening_source: opening.source_ref.clone(),
                initialize_operation_id: "connect_financial_reply_001".into(),
                release_id: "a".repeat(40),
            },
            opening,
            "d".repeat(64),
        )
        .await
        .unwrap();
    owner
        .apply_campaign_policy(
            deadline(),
            NativeCampaignPolicy {
                campaign_id: campaign(),
                policy_revision: 1,
                generation: 1,
                global_limit_usdt: "10".into(),
                loss_limit_usd: "10".into(),
                entry_limit_usd: "20".into(),
                accounts: vec![NativeCampaignAccountPolicy {
                    account_key: account(),
                    paused: false,
                    limit_usdt: "10".into(),
                    order_limit_usdt: "2".into(),
                    entry_limit_usd: "20".into(),
                    strategies: ["alpha", "manual"]
                        .into_iter()
                        .map(|strategy| NativeCampaignStrategyPolicy {
                            strategy_id: strategy.into(),
                            limit_usdt: "10".into(),
                        })
                        .collect(),
                }],
            },
        )
        .await
        .unwrap();
    owner
        .mutate_strategy_instance(
            deadline(),
            account(),
            StrategyMutationCommand {
                operation_id: "create_alpha_001".into(),
                request_hash: "e".repeat(64),
                action: "create".into(),
                instance: StrategyInstance {
                    instance_id: "alpha".into(),
                    definition_id: "alpha".into(),
                    definition_version: "1".into(),
                    parameters: "{}".into(),
                    enabled: false,
                    policy_revision: 1,
                    budget: Decimal::ZERO,
                    last_decision_key: None,
                },
                expected_revision: None,
                confirmed_configuration_digest: None,
                restore_revision: None,
                time: 101,
            },
        )
        .await
        .unwrap();
    assert!(
        owner
            .operation(deadline(), account(), command().operation_id)
            .await
            .unwrap()
            .is_none(),
        "bootstrap must not pre-create the financial command receipt"
    );
    let entries: i64 = owner
        .submit(|db| Ok(db.query_row("SELECT COUNT(*) FROM finance_entries", [], |r| r.get(0))?))
        .await
        .unwrap();
    assert_eq!(
        entries, 0,
        "bootstrap must not pre-book either transfer leg"
    );
    owner
}
pub(super) fn command() -> CapitalAssignmentCommand {
    CapitalAssignmentCommand {
        account_key: account(),
        operation_id: "capital_reply_001".into(),
        request_hash: "f".repeat(64),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(7),
        authority_revision: 1,
        authority_receipt: "governance_reply_001".into(),
        time: 102,
    }
}

#[tokio::test]
async fn strategy_confirmation_is_checked_in_sql_and_receipt_replays_after_reopen() {
    let (root, owner) = fresh().await;
    let stored = owner
        .strategy_instances(deadline(), account())
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.instance_id == "alpha")
        .unwrap();
    let confirmed = stored.configuration_digest(&account()).unwrap();
    let mut proposed = stored;
    proposed.policy_revision += 1;
    let mut command = StrategyMutationCommand {
        operation_id: "activate_alpha_001".into(),
        request_hash: "f".repeat(64),
        action: "start".into(),
        instance: proposed,
        expected_revision: Some(1),
        confirmed_configuration_digest: Some("0".repeat(64)),
        restore_revision: None,
        time: 102,
    };
    assert!(matches!(
        owner
            .mutate_strategy_instance(deadline(), account(), command.clone())
            .await,
        Err(StoreError::Invalid(
            "STRATEGY_CONFIGURATION_DIGEST_MISMATCH"
        ))
    ));
    assert!(
        owner
            .operation(deadline(), account(), command.operation_id.clone())
            .await
            .unwrap()
            .is_none()
    );
    command.confirmed_configuration_digest = Some(confirmed);
    let original = owner
        .mutate_strategy_instance(deadline(), account(), command.clone())
        .await
        .unwrap();
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .mutate_strategy_instance(deadline(), account(), command.clone())
            .await
            .unwrap(),
        original
    );
    command.request_hash = "1".repeat(64);
    assert!(matches!(
        reopened
            .mutate_strategy_instance(deadline(), account(), command)
            .await,
        Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
    ));
    let active: StrategyInstance = serde_json::from_str(&original.result_ref).unwrap();
    assert_eq!(active.policy_revision, 2);
    assert!(active.enabled);
    assert_eq!(original.committed_at, 102);
    close(reopened).await;
    std::fs::remove_dir_all(root).unwrap();
}

pub(super) async fn assert_once(owner: &UnifiedFinanceHandle) -> OperationReceipt {
    let receipt = owner
        .operation(deadline(), account(), command().operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.kind, "CAPITAL_ASSIGNMENT");
    assert_eq!(receipt.request_hash, command().request_hash);
    assert_eq!(receipt.committed_at, 102);
    let state = owner
        .capital_state(account(), 102, deadline())
        .await
        .unwrap();
    assert_eq!(state.account_nav_usd, Decimal::from(100));
    assert_eq!(state.assigned_usd, Decimal::from(7));
    assert_eq!(state.unattributed_residual_usd, Decimal::from(93));
    assert_eq!(state.strategy_principal_usd["alpha"], Decimal::from(7));
    let (legs, receipts, links) = owner
        .submit(|db| {
            let mut query = db.prepare(
                "SELECT entry_id,economic_event_key,component,allocation_leg,amount,currency,
                        instance_id,source_ref,time,reverses_entry_id
                 FROM finance_entries ORDER BY entry_id",
            )?;
            let legs = query
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, String>(5)?,
                        r.get::<_, Option<String>>(6)?,
                        r.get::<_, String>(7)?,
                        r.get::<_, i64>(8)?,
                        r.get::<_, Option<String>>(9)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let receipts: i64 = db.query_row(
                "SELECT COUNT(*) FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND operation_id=?3",
                rusqlite::params![INSTALLATION, account(), command().operation_id],
                |r| r.get(0),
            )?;
            let mut query = db.prepare(
                "SELECT entry_id,source_ref FROM finance_source_links ORDER BY entry_id",
            )?;
            let links = query
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            Ok((legs, receipts, links))
        })
        .await
        .unwrap();
    assert_eq!(
        legs,
        vec![
            (
                "capital:capital_reply_001:strategy:alpha".into(),
                "capital:capital_reply_001".into(),
                "INTERNAL_CAPITAL_USD".into(),
                "strategy:alpha".into(),
                "7".into(),
                "USD".into(),
                Some("alpha".into()),
                "governance_reply_001".into(),
                102,
                None
            ),
            (
                "capital:capital_reply_001:unattributed".into(),
                "capital:capital_reply_001".into(),
                "INTERNAL_CAPITAL_USD".into(),
                "unattributed".into(),
                "-7".into(),
                "USD".into(),
                None,
                "governance_reply_001".into(),
                102,
                None
            ),
        ]
    );
    assert_eq!(
        links,
        vec![
            (
                "capital:capital_reply_001:strategy:alpha".into(),
                "governance_reply_001".into()
            ),
            (
                "capital:capital_reply_001:unattributed".into(),
                "governance_reply_001".into()
            ),
        ]
    );
    assert_eq!(receipts, 1);
    let summary = owner.campaign_summary(campaign()).await.unwrap();
    assert_eq!(summary.global_reserved_usdt, "0");
    assert_eq!(summary.reserved_loss_usd, "0");
    assert_eq!(summary.consumed_loss_usd, "0");
    receipt
}

async fn replay_and_reopen(root: PathBuf, owner: UnifiedFinanceHandle, first: OperationReceipt) {
    let replay = owner.assign_capital(account(), command()).await.unwrap();
    assert_eq!(replay.receipt, first);
    assert_eq!(assert_once(&owner).await, first);
    let mut conflict = command();
    conflict.request_hash = "0".repeat(64);
    assert!(matches!(
        owner.assign_capital(account(), conflict).await,
        Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
    ));
    assert_eq!(assert_once(&owner).await, first);
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    assert_eq!(assert_once(&reopened).await, first);
    assert_eq!(
        reopened
            .assign_capital(account(), command())
            .await
            .unwrap()
            .receipt,
        first
    );
    assert_eq!(assert_once(&reopened).await, first);
    close(reopened).await;
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
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_financial_commit_lost_reply_same_identity_readback() {
    let (root, owner) = fresh().await;
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    // This private owner job uses the production permission gate, typed
    // dispatcher and COMMIT. Only the post-COMMIT reply barrier is test code.
    let mut caller = Box::pin(owner.submit(move |db| {
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let command = UnifiedFinanceWrite::AssignBoundCapital {
            account: account(),
            command: command(),
        };
        super::source_permission::require_command_permission(&tx, INSTALLATION, &command)?;
        let result = super::apply_original_unified_finance_transaction(&tx, INSTALLATION, command)?;
        tx.commit()
            .map_err(|_| StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
        let UnifiedFinanceWriteReceipt::CapitalAssignmentReported(ref assignment) = result else {
            panic!("wrong financial receipt")
        };
        // Independent witness: commit has returned; submit's reply cannot yet
        // be sent. Cancellation is causally AFTER the financial COMMIT.
        committed_tx.send(assignment.receipt.clone()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_FINANCIAL_REPLY_BARRIER_TIMEOUT"))?;
        Ok(result)
    }));
    enqueue(caller.as_mut());
    let committed = committed_rx.await.unwrap();
    drop(caller);
    release_tx.send(()).unwrap();
    assert_eq!(assert_once(&owner).await, committed);
    replay_and_reopen(root, owner, committed).await;
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_financial_cancel_after_enqueue_still_commits_once() {
    let (root, owner) = fresh().await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (unexecuted_tx, unexecuted_rx) = tokio::sync::oneshot::channel();
    let mut blocker = Box::pin(owner.submit(move |db| {
        started_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_FINANCIAL_QUEUE_BARRIER_TIMEOUT"))?;
        // This check happens after the caller was enqueued and dropped, while
        // the FIFO is still blocked. A pre-existing receipt cannot fake PASS.
        assert!(
            AccountStore::operation_sql(
                db,
                Some((INSTALLATION, &account())),
                &command().operation_id
            )?
            .is_none()
        );
        let entries: i64 =
            db.query_row("SELECT COUNT(*) FROM finance_entries", [], |r| r.get(0))?;
        assert_eq!(entries, 0);
        unexecuted_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_FINANCIAL_QUEUE_BARRIER_TIMEOUT"))?;
        Ok(())
    }));
    enqueue(blocker.as_mut());
    started_rx.await.unwrap();
    let mut caller = Box::pin(owner.assign_capital(account(), command()));
    enqueue(caller.as_mut());
    // The real typed command was accepted by the FIFO and cannot execute
    // before this drop. A dropped future cannot establish non-execution.
    drop(caller);
    release_tx.send(()).unwrap();
    unexecuted_rx.await.unwrap();
    release_tx.send(()).unwrap();
    blocker.await.unwrap();
    let committed = assert_once(&owner).await;
    replay_and_reopen(root, owner, committed).await;
}
