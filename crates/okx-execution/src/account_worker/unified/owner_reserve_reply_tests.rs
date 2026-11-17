//! COMMIT -> reply loss on the real fresh installation writer. Source DTOs
//! are component fixtures passed through original typed owner commands; this
//! does not claim public Execute/REST/census producer coverage or a venue ACK.
use super::{
    NativePostDecision, NativePostRequest, NativeReserveDecision, NativeReserveRequest,
    ReportedOpeningCensus, ReportedRiskCensus, UnifiedFinanceHandle,
    owner_financial_reply_tests::{INSTALLATION, account, campaign, fresh},
};
use crate::{
    AccountNavSnapshot, AccountStore, CentralReceipt, CentralReceiptView, Decision,
    EntryAdmissionEvidence, EntryQuoteEvidence, IngestPage, IntentPlan, IntentState, NewIntent,
    SourceCursor, StoreError, TradeIntentLookup,
};
use okx_kernel::risk::model::{PostAccountRead, PostCausalProof};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

const INTENT: &str = "reply_intent_001";
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn now() -> i64 {
    super::opening_ingress::owner_now_ms().unwrap()
}
fn reservation() -> String {
    format!("3:okx4:real64:{}9:reply_run16:{INTENT}", "a".repeat(64))
}
fn census(at: i64) -> ReportedRiskCensus {
    ReportedRiskCensus {
        source_digest: "d".repeat(64),
        generated_at_ms: at,
        read_floor_ms: at,
        occupied: BTreeMap::from([(account(), "0".into())]),
        cumulative_loss_usd: BTreeMap::from([(account(), "0".into())]),
    }
}
fn reserve_census(request: &NativeReserveRequest) -> ReportedRiskCensus {
    let mut reported = census(now());
    // The typed conversion was captured before the later persisted NAV;
    // keep this actual fixture source floor on replay instead of claiming a
    // newer financial observation merely because the envelope is newer.
    reported.read_floor_ms = request.conversion_observed_at_ms;
    reported
}
async fn ingest_empty_history(owner: &UnifiedFinanceHandle, at: i64) {
    // Empty captured source DTOs enter through the same owner ingestion and
    // original baseline sealing used in production. No hand-written source,
    // readiness, opening, authority, HELD or POSTED SQL rows.
    for (source, partition) in [
        ("fills", "SPOT"),
        ("fills", "MARGIN"),
        ("fills", "SWAP"),
        ("fills", "FUTURES"),
        ("fills", "OPTION"),
        ("bills", "account"),
    ] {
        owner
            .ingest_source_page(
                account(),
                IngestPage {
                    cursor: Some(SourceCursor {
                        source: source.into(),
                        stream_partition: partition.into(),
                        cursor: String::new(),
                        watermark: at,
                        generation: 1,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
}
async fn nav(owner: &UnifiedFinanceHandle, at: i64) {
    // This component fixture has no venue fills or bills between phases.
    ingest_empty_history(owner, at).await;
    owner
        .project_account_nav(
            account(),
            AccountNavSnapshot {
                as_of: at,
                generation: 1,
                source_version: "reply_source_v1".into(),
                source_ref: "a".repeat(64),
                equity_usd: Decimal::from(100),
                balances: vec![("USDT".into(), Decimal::from(100))],
                complete: true,
            },
        )
        .await
        .unwrap();
}

pub(super) async fn setup() -> (PathBuf, UnifiedFinanceHandle, NativeReserveRequest) {
    setup_inner(true).await
}

pub(super) async fn setup_before_reserve_attempt()
-> (PathBuf, UnifiedFinanceHandle, NativeReserveRequest) {
    setup_inner(false).await
}

async fn setup_inner(mark_attempt: bool) -> (PathBuf, UnifiedFinanceHandle, NativeReserveRequest) {
    let (root, owner) = fresh().await;
    let started = owner
        .campaign_summary(campaign())
        .await
        .unwrap()
        .campaign_started_at_ms
        .unwrap() as i64;
    while now() <= started {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let through = now();
    ingest_empty_history(&owner, through).await;
    let (consumed, proof) = owner
        .seal_captured_campaign_baseline(deadline(), account(), campaign(), started, through)
        .await
        .unwrap();
    assert_eq!(consumed, Decimal::ZERO);
    let mut opening = crate::risk::NativeOpeningSource {
        campaign_id: campaign(),
        limit_usd: "20".into(),
        consumed_usd: consumed.to_string(),
        source_digest: String::new(),
        as_of_ms: through as u64,
        campaign_started_at_ms: started as u64,
        accounts: vec![crate::risk::NativeOpeningAccount {
            account_key: account(),
            history_from_ms: started as u64,
            history_through_ms: through as u64,
            consumed_entry_usd: consumed.to_string(),
            order_history_complete: true,
            fill_history_complete: true,
            bill_history_complete: true,
            finance_provenance_sealed: true,
            source_digest: proof,
        }],
    };
    opening.source_digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                "okx.campaign-entry-opening/v1",
                &opening.campaign_id,
                &opening.limit_usd,
                &opening.consumed_usd,
                opening.campaign_started_at_ms,
                opening.as_of_ms,
                &opening.accounts,
            ))
            .unwrap()
        )
    );
    let reported = census(through);
    owner
        .install_reported_opening(
            opening,
            ReportedOpeningCensus {
                source_digest: reported.source_digest,
                generated_at_ms: reported.generated_at_ms,
                read_floor_ms: reported.read_floor_ms,
                occupied: reported.occupied,
                cumulative_loss_usd: reported.cumulative_loss_usd,
            },
        )
        .await
        .unwrap();
    owner.verify_live_authority(deadline()).await.unwrap();

    let definition = owner
        .ensure_manual_instance(deadline(), account())
        .await
        .unwrap();
    let at = now();
    let stored = owner
        .prepare_admission(
            deadline(),
            account(),
            Decision {
                decision_id: "reply_decision_001".into(),
                instance_id: Some("manual".into()),
                decision_at: at,
                input_digest: "c".repeat(64),
                policy_revision: 1,
                code_version: "a".repeat(40),
            },
            NewIntent {
                plan: IntentPlan {
                    strategy_definition_id: definition.definition_id,
                    strategy_definition_version: definition.version,
                    strategy_parameters_digest: definition.parameters_digest,
                    contract_value: Decimal::new(1, 2),
                    inverse: false,
                    settlement_currency: "USDT".into(),
                    position_side: "long".into(),
                    private_source_generation: 1,
                    operation: "open".into(),
                    side: "buy".into(),
                    quantity: Decimal::new(1, 1),
                    limit_price: Decimal::from(1000),
                    stop_price: Some(Decimal::from(900)),
                    strategy_instance: "manual".into(),
                    planned_loss_usd: Decimal::new(1, 1),
                    source_generation: 1,
                },
                intent_id: INTENT.into(),
                idempotency_key: "reply_prepare_001".into(),
                request_hash: "f".repeat(64),
                client_order_id: "replyclient001".into(),
                deadline: at + 30_000,
                plan_hash: "e".repeat(64),
                policy_revision: 1,
                risk_increase: true,
                instrument_id: "BTC-USDT-SWAP".into(),
            },
        )
        .await
        .unwrap();
    let quote = EntryQuoteEvidence {
        schema: "okx.entry-known-quote/v1".into(),
        account_key: account(),
        installation_id: INSTALLATION.into(),
        intent_id: INTENT.into(),
        plan_hash: stored.plan_hash.clone(),
        policy_revision: 1,
        instrument_id: stored.instrument_id,
        quantity: stored.plan.quantity,
        limit_price: stored.plan.limit_price,
        contract_value: stored.plan.contract_value,
        fee_rate: Decimal::new(5, 4),
        maker_rate: "-0.0002".into(),
        taker_rate: "0.0005".into(),
        fee_level: "Lv1".into(),
        fee_exchange_at: at,
        usd_per_usdt: Decimal::ONE,
        notional_usdt: Decimal::ONE,
        fee_usdt: Decimal::new(5, 4),
        total_usdt: Decimal::new(10005, 4),
        total_usd: Decimal::new(10005, 4),
        fee_generation: format!(
            "sha256:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "BTC-USDT-SWAP",
                    "reply_reference",
                    "Lv1",
                    "-0.0002",
                    "0.0005",
                    true,
                ))
                .unwrap()
            )
        ),
        reference_generation: "reply_reference".into(),
        fee_received_at: at,
        conversion_digest: "b".repeat(64),
        conversion_received_at: at,
        observed_at: at,
    };
    let admission = EntryAdmissionEvidence {
        schema: "okx.entry-admission/v2".into(),
        account_key: account(),
        installation_id: INSTALLATION.into(),
        intent_id: INTENT.into(),
        plan_hash: stored.plan_hash.clone(),
        quote_digest: format!("{:x}", Sha256::digest(serde_json::to_vec(&quote).unwrap())),
        admission_usd_per_usdt: Decimal::ONE,
        other_entry_costs_upper_usd: Decimal::ZERO,
        entry_upper_usd: quote.total_usd,
        entry_proof_digest: "f".repeat(64),
        observed_at: at,
    };
    owner
        .bind_entry_evidence(deadline(), account(), quote, Some(admission))
        .await
        .unwrap();
    if mark_attempt {
        owner
            .mark_reserve_attempt(deadline(), account(), INTENT.into(), reservation())
            .await
            .unwrap();
    }
    nav(&owner, now()).await;
    let request = NativeReserveRequest {
        venue: "okx".into(),
        uid: "a".repeat(64),
        run_id: "reply_run".into(),
        intent_id: INTENT.into(),
        strategy_id: "manual".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_sha256: stored.plan_hash,
        entry_proof_digest: "f".repeat(64),
        conversion_digest: "b".repeat(64),
        conversion_rate: "1".into(),
        loss_usdt: "0.1".into(),
        loss_usd: "0.1".into(),
        entry_upper_usd: "1.0005".into(),
        native_incarnation: "f".repeat(32),
        conversion_observed_at_ms: at,
        policy_revision: 1,
    };
    assert!(
        owner
            .reservation_receipt(
                account(),
                INTENT.into(),
                CentralReceiptView::Retained,
                deadline()
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        owner
            .campaign_summary(campaign())
            .await
            .unwrap()
            .global_reserved_usdt,
        "0"
    );
    (root, owner, request)
}

fn held(decision: NativeReserveDecision) -> (CentralReceipt, u64) {
    let NativeReserveDecision::Held {
        receipt,
        book_sequence,
        policy_revision,
    } = decision
    else {
        panic!("source-complete reserve denied");
    };
    assert_eq!(policy_revision, 1);
    assert_eq!(receipt.reservation_key, reservation());
    assert_eq!(receipt.account_key, account());
    assert_eq!(receipt.installation_id, INSTALLATION);
    assert_eq!(receipt.intent_id, INTENT);
    assert_eq!(receipt.instance_id.as_deref(), Some("manual"));
    assert_eq!(receipt.plan_hash, "e".repeat(64));
    assert_eq!(receipt.policy_revision, 1);
    assert_eq!(receipt.requested_loss_usd, Decimal::new(1, 1));
    assert_eq!(receipt.conversion_digest, "b".repeat(64));
    assert_eq!(
        receipt.receipt_hash,
        "ec6f1918c40f423a7cf5e294ed67e8443641cb9e244e684a7f105266bc3f137d"
    );
    (receipt, book_sequence)
}

async fn assert_durable(owner: &UnifiedFinanceHandle, receipt: &CentralReceipt, posted: bool) {
    let retained = owner
        .reservation_receipt(
            account(),
            INTENT.into(),
            CentralReceiptView::Retained,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(retained).unwrap(),
        serde_json::to_value(receipt).unwrap()
    );
    let intent = owner
        .lookup_intent(
            account(),
            TradeIntentLookup::IntentId(INTENT.into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        intent.state,
        if posted {
            IntentState::Posting
        } else {
            IntentState::Reserved
        }
    );
    let summary = owner.campaign_summary(campaign()).await.unwrap();
    assert_eq!(summary.global_reserved_usdt, "0.1");
    assert_eq!(summary.reserved_loss_usd, "0.1");
    assert_eq!(summary.reserved_entry_usd, "1.0005");
    assert_eq!(summary.consumed_entry_usd, "0");
    assert_eq!(summary.consumed_loss_usd, "0");
    let expected = if posted { "POSTED" } else { "HELD" };
    assert_eq!(
        owner
            .stopped_inspect_reservation(campaign(), reservation(), receipt.plan_hash.clone())
            .await
            .unwrap()
            .unwrap()
            .state,
        expected
    );
    let (entries, receipts, begin_post) = owner.submit(|db| {
        Ok((
            db.query_row::<i64,_,_>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))?,
            db.query_row::<i64,_,_>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))?,
            db.query_row::<Option<i64>,_,_>("SELECT begin_post_at FROM intents WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3", rusqlite::params![INSTALLATION,account(),INTENT], |r| r.get(0))?,
        ))
    }).await.unwrap();
    assert_eq!((entries, receipts), (1, 1));
    assert_eq!(begin_post.is_some(), posted);
    // The same read-only snapshot feeds the public operation explanation.
    // Exercise it after the COMMIT/reply-loss boundary and again after reopen;
    // inspection cannot construct a replacement venue-send capability.
    let explanation = owner
        .inspect_intent_operation(deadline(), account(), INTENT.into())
        .await
        .unwrap();
    assert_eq!(explanation.intent.unwrap().state, intent.state);
    assert_eq!(explanation.begin_post_at_ms.is_some(), posted);
    assert_eq!(
        explanation.reservation.unwrap().receipt_hash,
        receipt.receipt_hash
    );
    assert!(explanation.unsent.is_none());
}

async fn post(owner: &UnifiedFinanceHandle, sequence: u64, held_at: Instant) -> NativePostRequest {
    // A new source observation follows the actual HELD commit. The original
    // post binder still checks the persisted NAV, policy and causal fence.
    let preceding_tick = now();
    let tick_deadline = deadline();
    while now() <= preceding_tick {
        assert!(
            Instant::now() < tick_deadline,
            "owner clock did not advance"
        );
        tokio::task::yield_now().await;
    }
    let started = held_at.elapsed().as_millis() as u64;
    let at = now();
    nav(owner, at).await;
    let completed = held_at.elapsed().as_millis() as u64;
    NativePostRequest {
        reservation_key: reservation(),
        plan_sha256: "e".repeat(64),
        source_proof_digest: "b".repeat(64),
        policy_revision: 1,
        expected_sequence: sequence,
        census: census(at),
        causal: PostCausalProof {
            reserve_anchor: reservation(),
            native_incarnation: "f".repeat(32),
            elapsed_to_proof_ms: held_at.elapsed().as_millis() as u64,
            account_reads: vec![PostAccountRead {
                account_key: account(),
                private_generation: 1,
                source_read_id: "1".repeat(64),
                content_digest: "c".repeat(64),
                started_after_held_ms: started,
                completed_after_held_ms: completed,
            }],
        },
    }
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
pub(super) async fn cleanup(root: PathBuf, owner: UnifiedFinanceHandle) {
    close(owner).await;
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
async fn assert_no_second_send(
    owner: &UnifiedFinanceHandle,
    request: NativePostRequest,
    sends: &AtomicUsize,
) {
    let mut current = request.clone();
    current.expected_sequence = owner
        .campaign_summary(campaign())
        .await
        .unwrap()
        .book_sequence;
    // Exact replay first hits the book CAS. Even learning the latest sequence
    // cannot turn the durable POSTED row into a new ephemeral send claim.
    for (retry, expected) in [
        (request, "STALE_SESSION_OR_BOOK_SEQUENCE"),
        (current, "INTENT_ALREADY_POSTED_OR_RESOLVED"),
    ] {
        match owner.begin_post_reported(campaign(), retry).await {
            Err(StoreError::Invalid(reason)) => assert_eq!(reason, expected),
            Err(error) => panic!("wrong replay failure: {error}"),
            Ok(NativePostDecision::Authorized { claim, .. }) => {
                claim
                    .send(|| async {
                        sends.fetch_add(1, Ordering::SeqCst);
                    })
                    .await;
                panic!("replayed BeginPost minted a second send capability");
            }
            Ok(NativePostDecision::Denied { reason }) => {
                panic!("expected original once-CAS rejection: {reason}")
            }
        }
    }
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_reserve_commit_lost_reply_exact_readback_once_send() {
    let (root, owner, request) = setup().await;
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let original = request.clone();
    let reported = reserve_census(&original);
    let mut caller = Box::pin(owner.submit(move |db| {
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        super::require_verified_live_finance_authority(&tx, INSTALLATION)?;
        let decision = super::native_admission::reserve_reported_sql(
            &tx,
            INSTALLATION,
            &campaign(),
            original,
            reported,
            now(),
        )?;
        tx.commit()
            .map_err(|_| StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
        let NativeReserveDecision::Held {
            ref receipt,
            book_sequence,
            ..
        } = decision
        else {
            panic!("reserve denied");
        };
        // COMMIT has returned and owner submit cannot deliver the reply yet.
        committed_tx
            .send((receipt.clone(), book_sequence, Instant::now()))
            .unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_RESERVE_REPLY_BARRIER_TIMEOUT"))?;
        Ok(decision)
    }));
    enqueue(caller.as_mut());
    let (receipt, sequence, held_at) = committed_rx.await.unwrap();
    drop(caller);
    release_tx.send(()).unwrap();
    let (golden, repeated_sequence) = held(
        owner
            .reserve_reported(campaign(), request.clone(), reserve_census(&request))
            .await
            .unwrap(),
    );
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        serde_json::to_value(&golden).unwrap()
    );
    assert_eq!(sequence, repeated_sequence);
    assert_durable(&owner, &golden, false).await;

    // Original scoped receipt replay rejects foreign account, revision and
    // receipt hash; even committing these failed checks changes no money.
    let original = golden.clone();
    owner
        .submit(move |db| {
            let tx = db.transaction()?;
            for variant in 0..3 {
                let mut foreign = original.clone();
                match variant {
                    0 => foreign.account_key = format!("okx:real:{}", "b".repeat(64)),
                    1 => foreign.policy_revision += 1,
                    _ => foreign.receipt_hash = "0".repeat(64),
                }
                assert!(
                    AccountStore::reserve_sql_transaction(
                        &tx,
                        Some((INSTALLATION, &account())),
                        &foreign,
                        now()
                    )
                    .is_err()
                );
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    assert_durable(&owner, &golden, false).await;
    close(owner).await;
    let owner = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    assert_durable(&owner, &golden, false).await;
    let reported = reserve_census(&request);
    let (reopened, replay_sequence) = held(
        owner
            .reserve_reported(campaign(), request, reported)
            .await
            .unwrap(),
    );
    assert_eq!(
        serde_json::to_value(reopened).unwrap(),
        serde_json::to_value(&golden).unwrap()
    );
    assert_eq!(replay_sequence, sequence);
    let post = post(&owner, sequence, held_at).await;
    let NativePostDecision::Authorized { claim, .. } = owner
        .begin_post_reported(campaign(), post.clone())
        .await
        .unwrap()
    else {
        panic!("first post denied");
    };
    assert!(claim.matches(INSTALLATION, &account(), INTENT, &golden.plan_hash));
    let sends = Arc::new(AtomicUsize::new(0));
    claim
        .send(|| async {
            sends.fetch_add(1, Ordering::SeqCst);
        })
        .await;
    assert_no_second_send(&owner, post, &sends).await;
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    assert_durable(&owner, &golden, true).await;
    cleanup(root, owner).await;
}

#[tokio::test]
async fn risk_book_contract_fresh_owner_begin_post_commit_lost_reply_no_second_claim_or_send() {
    let (root, owner, request) = setup().await;
    let reported = reserve_census(&request);
    let (receipt, sequence) = held(
        owner
            .reserve_reported(campaign(), request, reported)
            .await
            .unwrap(),
    );
    let post = post(&owner, sequence, Instant::now()).await;
    let original = post.clone();
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let sends = AtomicUsize::new(0);
    let mut caller = Box::pin(owner.submit(move |db| {
        // This is the complete production BeginPost operation, including
        // live authority, original sources, COMMIT and capability creation.
        let decision = super::native_admission::begin_post_reported_on_connection(
            db,
            INSTALLATION.into(),
            campaign(),
            original,
        )?;
        let NativePostDecision::Authorized {
            ref claim,
            book_sequence,
            ..
        } = decision
        else {
            panic!("first post denied");
        };
        assert!(claim.matches(INSTALLATION, &account(), INTENT, &"e".repeat(64)));
        committed_tx.send(book_sequence).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_POST_REPLY_BARRIER_TIMEOUT"))?;
        Ok(decision)
    }));
    enqueue(caller.as_mut());
    let posted_sequence = committed_rx.await.unwrap();
    assert!(posted_sequence > sequence);
    drop(caller);
    release_tx.send(()).unwrap();
    assert_durable(&owner, &receipt, true).await;
    assert_no_second_send(&owner, post.clone(), &sends).await;
    assert_eq!(sends.load(Ordering::SeqCst), 0);
    close(owner).await;
    let owner = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    assert_durable(&owner, &receipt, true).await;
    assert_no_second_send(&owner, post, &sends).await;
    assert_eq!(sends.load(Ordering::SeqCst), 0);
    assert_durable(&owner, &receipt, true).await;
    cleanup(root, owner).await;
}
