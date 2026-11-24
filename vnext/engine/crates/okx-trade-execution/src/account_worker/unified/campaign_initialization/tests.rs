//! Full policy bootstrap on the real single writer; no venue or financial grant.
mod recovery;
use super::*;
use crate::{AccountMeta, NewFinanceInstallation, OpeningSnapshot};
use rust_decimal::Decimal;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn key(c: char) -> String {
    format!("okx:real:{}", c.to_string().repeat(64))
}
fn policy() -> NativeCampaignPolicy {
    NativeCampaignPolicy {
        campaign_id: format!("sha256:{}", "c".repeat(64)),
        policy_revision: 1,
        generation: 1,
        global_limit_usdt: "10".into(),
        loss_limit_usd: "10".into(),
        entry_limit_usd: "20".into(),
        accounts: ['a', 'b']
            .into_iter()
            .map(|c| NativeCampaignAccountPolicy {
                account_key: key(c),
                paused: false,
                limit_usdt: "5".into(),
                order_limit_usdt: "2".into(),
                entry_limit_usd: "10".into(),
                strategies: vec![NativeCampaignStrategyPolicy {
                    strategy_id: "manual".into(),
                    limit_usdt: "5".into(),
                }],
            })
            .collect(),
    }
}
async fn setup() -> (PathBuf, UnifiedFinanceHandle) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "okx-campaign-init-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let owner = UnifiedFinanceHandle::initialize_new(
        root.clone(),
        NewFinanceInstallation {
            installation_id: "installation_native".into(),
            initialize_request_id: "initialize_native_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    for c in ['a', 'b'] {
        let opening = OpeningSnapshot {
            opening_equity_usd: Decimal::from(100),
            source_ref: c.to_string().repeat(64),
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
                    account_key: key(c),
                    installation_id: "installation_native".into(),
                    initialized_at: 100,
                    opening_source: opening.source_ref.clone(),
                    initialize_operation_id: "connect_native_001".into(),
                    release_id: "a".repeat(40),
                },
                opening,
                "d".repeat(64),
            )
            .await
            .unwrap();
    }
    (root, owner)
}
async fn snapshot(owner: &UnifiedFinanceHandle) -> (i64, i64, i64) {
    owner
        .submit(|db| {
            Ok((
                db.query_row("SELECT COUNT(*) FROM risk_meta", [], |r| r.get(0))?,
                db.query_row("SELECT COUNT(*) FROM operation_receipts", [], |r| r.get(0))?,
                db.query_row("SELECT SUM(revision) FROM account_policy", [], |r| r.get(0))?,
            ))
        })
        .await
        .unwrap()
}
async fn close(root: PathBuf, owner: UnifiedFinanceHandle) {
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(2), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn risk_book_contract_native_campaign_policy_two_accounts_replay_and_no_opening_grant() {
    let (root, owner) = setup().await;
    let p = policy();
    let first = owner
        .apply_campaign_policy(deadline(), p.clone())
        .await
        .unwrap();
    assert_eq!(
        first,
        NativeCampaignPolicyReceipt {
            policy_revision: 1,
            book_sequence: 0,
            replayed: false
        }
    );
    assert_eq!(snapshot(&owner).await, (1, 8, 2));
    let summary = owner.campaign_summary(p.campaign_id.clone()).await.unwrap();
    assert_eq!(summary.loss_limit_usd, "10");
    assert_eq!(summary.global_reserved_usdt, "0");
    assert!(
        summary.opening.is_none(),
        "policy cannot invent original reported financial opening"
    );
    assert!(summary.campaign_started_at_ms.is_some_and(|t| t > 100));
    assert!(matches!(
        owner.verify_live_authority(deadline()).await,
        Err(StoreError::Invalid("FINANCE_SOURCE_ACCEPTANCE_REQUIRED"))
    ));
    assert_execution_blocked(&owner, &p).await;
    owner
        .ingest_source_page(
            key('a'),
            crate::IngestPage {
                cursor: Some(crate::SourceCursor {
                    source: "fills".into(),
                    stream_partition: "SPOT".into(),
                    cursor: String::new(),
                    watermark: 101,
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut equivalent = p.clone();
    equivalent.loss_limit_usd = "10.00".into();
    equivalent.generation = 2; // Authenticated transport reconnected.
    assert!(
        owner
            .apply_campaign_policy(deadline(), equivalent)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(snapshot(&owner).await, (1, 8, 2));
    assert!(
        owner
            .submit(|db| {
                let generation: i64 =
                    db.query_row("SELECT generation FROM risk_meta", [], |r| r.get(0))?;
                if generation != 1 {
                    return Ok(false);
                }
                for kind in ["CAMPAIGN_CREATED", "NATIVE_CAMPAIGN_POLICY"] {
                    if db
                        .execute(
                            "UPDATE operation_receipts SET result_ref='tampered' WHERE kind=?1",
                            [kind],
                        )
                        .is_ok()
                        || db
                            .execute("DELETE FROM operation_receipts WHERE kind=?1", [kind])
                            .is_ok()
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .await
            .unwrap(),
        "reconnect preserves original generation and policy receipts are immutable"
    );
    let mut changed = p.clone();
    changed.accounts[0].paused = true;
    assert!(matches!(
        owner.apply_campaign_policy(deadline(), changed).await,
        Err(StoreError::Invalid("NATIVE_CAMPAIGN_POLICY_CONFLICT"))
    ));
    assert_eq!(snapshot(&owner).await, (1, 8, 2));
    // A new revision preserves inception and original financial rows.
    let started = summary.campaign_started_at_ms;
    let mut updated = p.clone();
    updated.policy_revision = 2;
    updated.loss_limit_usd = "9".into();
    owner
        .apply_campaign_policy(deadline(), updated)
        .await
        .unwrap();
    let summary = owner.campaign_summary(p.campaign_id).await.unwrap();
    assert_eq!(summary.campaign_started_at_ms, started);
    assert_eq!(summary.consumed_loss_usd, "0");
    assert!(summary.opening.is_none());
    assert_eq!(snapshot(&owner).await, (1, 9, 4));
    close(root, owner).await;
}

async fn assert_execution_blocked(owner: &UnifiedFinanceHandle, p: &NativeCampaignPolicy) {
    use crate::account_worker::unified::UnifiedFinanceWrite;
    use okx_engine_risk::risk::model::{PostCausalProof, RiskProposal};
    let census = crate::risk::VerifiedCensus {
        complete: true,
        generation: 1,
        source_digest: "a".repeat(64),
        generated_at_ms: 101,
        read_floor_ms: 100,
        occupied: Default::default(),
        cumulative_loss_usd: Default::default(),
    };
    let reserve = crate::risk::ReserveInput {
        venue: "okx".into(),
        uid: "a".repeat(64),
        run_id: "run_001".into(),
        intent_id: "intent_001".into(),
        strategy_id: "manual".into(),
        instrument_id: "BTC-USDT".into(),
        plan_sha256: "a".repeat(64),
        entry_proof_digest: "b".repeat(64),
        conversion_digest: "c".repeat(64),
        conversion_observed_at_ms: 101,
        conversion_rate: RiskAmount::parse_positive("1").unwrap(),
        native_incarnation: "incarnation_001".into(),
        proposal: RiskProposal {
            loss_usdt: RiskAmount::parse_positive("1").unwrap(),
            loss_usd: RiskAmount::parse_positive("1").unwrap(),
            entry_upper_usd: RiskAmount::parse_positive("1").unwrap(),
        },
        policy_revision: 1,
        now_ms: 101,
    };
    let receipt = crate::CentralReceipt {
        reservation_key: reserve.reservation_key(),
        account_key: key('a'),
        installation_id: "installation_native".into(),
        instance_id: Some("manual".into()),
        intent_id: reserve.intent_id.clone(),
        plan_hash: reserve.plan_sha256.clone(),
        policy_revision: 1,
        receipt_hash: "d".repeat(64),
        requested_loss_usd: Decimal::ONE,
        conversion_digest: reserve.conversion_digest.clone(),
    };
    let post = crate::risk::PostFenceInput {
        reservation_key: reserve.reservation_key(),
        plan_sha256: reserve.plan_sha256.clone(),
        policy_revision: 1,
        expected_sequence: 1,
        now_ms: 102,
        source_proof_digest: "e".repeat(64),
        census: census.clone(),
        causal: PostCausalProof {
            reserve_anchor: "anchor_001".into(),
            native_incarnation: "incarnation_001".into(),
            elapsed_to_proof_ms: 1,
            account_reads: vec![],
        },
    };
    for command in [
        UnifiedFinanceWrite::Reserve {
            campaign: p.campaign_id.clone(),
            input: reserve,
            census,
            receipt,
        },
        UnifiedFinanceWrite::BeginPost {
            campaign: p.campaign_id.clone(),
            input: post,
        },
    ] {
        assert!(matches!(
            owner.apply_financial_write(command).await,
            Err(StoreError::Invalid("FINANCE_SOURCE_ACCEPTANCE_REQUIRED"))
        ));
    }
}

#[tokio::test]
async fn risk_book_contract_native_campaign_policy_rejects_bad_limits_roster_and_refill_atomically()
{
    let (root, owner) = setup().await;
    let mut bad = policy();
    bad.accounts[0].order_limit_usdt = "6".into();
    assert!(owner.apply_campaign_policy(deadline(), bad).await.is_err());
    let mut missing = policy();
    missing.accounts.pop();
    assert!(matches!(
        owner.apply_campaign_policy(deadline(), missing).await,
        Err(StoreError::Invalid("NATIVE_CAMPAIGN_COHORT_MISMATCH"))
    ));
    let mut foreign = policy();
    foreign.accounts[1].account_key = key('d');
    assert!(matches!(
        owner.apply_campaign_policy(deadline(), foreign).await,
        Err(StoreError::Invalid("NATIVE_CAMPAIGN_COHORT_MISMATCH"))
    ));
    assert_eq!(snapshot(&owner).await, (0, 6, 0));
    owner
        .apply_campaign_policy(deadline(), policy())
        .await
        .unwrap();
    let mut refill = policy();
    refill.policy_revision = 2;
    refill.loss_limit_usd = "11".into();
    assert!(matches!(
        owner.apply_campaign_policy(deadline(), refill).await,
        Err(StoreError::Invalid("NATIVE_CAMPAIGN_LIMIT_INCREASE_DENIED"))
    ));
    assert_eq!(snapshot(&owner).await, (1, 8, 2));
    close(root, owner).await;
}
