//! #500: direct production cleanup transition tests with the actual SQL owner.
//! Inputs are independent inventory/position observations, not success flags.
//! These tests exercise the exact durable decision called by TradeService.
use super::*;
use okx_api::{PendingDerivativeAlgoRiskInventory, PendingProtectiveAlgoSample};
use okx_execution::{AccountMeta, OpeningSnapshot};
use std::sync::atomic::{AtomicUsize, Ordering};

static SERIAL: AtomicUsize = AtomicUsize::new(0);
const INST: &str = "BTC-USDT-SWAP";
const ALGO: &str = "owned-protection-001";
const UID: &str = "verified-account-a";

struct Fixture {
    store: AccountStoreHandle,
    db: std::path::PathBuf,
    account_key: String,
}
async fn fixture() -> Fixture {
    let n = SERIAL.fetch_add(1, Ordering::SeqCst);
    let root =
        std::env::temp_dir().join(format!("okx-pc-500-{}-{}-{}", std::process::id(), now(), n));
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("account.db");
    let account_key = "okx:real:verified-account-a".to_owned();
    let store = AccountStoreHandle::initialize_flat(
        db.clone(),
        AccountMeta {
            account_key: account_key.clone(),
            installation_id: "installation_test".into(),
            initialized_at: now() - 10_000,
            opening_source: "opening_observation".into(),
            initialize_operation_id: format!("pc_initialize_{n}"),
            release_id: "a".repeat(40),
        },
        OpeningSnapshot {
            source_ref: "opening_observation".into(),
            observed_at: now() - 10_000,
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
    Fixture {
        store,
        db,
        account_key,
    }
}
fn inventory(
    received_at_ms: u64,
    live: Option<(&str, &str)>,
    complete: bool,
) -> PendingDerivativeAlgoRiskInventory {
    let samples = live
        .into_iter()
        .map(|(id, instrument)| PendingProtectiveAlgoSample {
            instrument_id: instrument.into(),
            algo_order_id: id.into(),
            client_order_id: "owned-protection-client".into(),
            order_type: "conditional".into(),
            state: "live".into(),
        })
        .collect::<Vec<_>>();
    PendingDerivativeAlgoRiskInventory {
        schema: "okx.pending-derivative-risk-algos/v1",
        instrument_types: "SWAP,FUTURES",
        order_types: "conditional,oco,trigger,move_order_stop,chase,iceberg,twap,smart_iceberg",
        scopes_checked: if complete { 16 } else { 15 },
        rows: samples.len(),
        complete_within_bound: complete,
        received_at_ms,
        samples,
    }
}
fn positions(qty: &str) -> Vec<(String, String)> {
    vec![(INST.into(), qty.into())]
}
async fn operation(store: &AccountStoreHandle, key: &str) -> okx_execution::OperationReceipt {
    let key = key.to_owned();
    store
        .call(deadline(), move |s| s.operation(&key))
        .await
        .unwrap()
        .unwrap()
}
async fn readback(
    store: &AccountStoreHandle,
    key: &str,
    hash: &str,
    inventory: &PendingDerivativeAlgoRiskInventory,
    uid: &str,
    source_at: i64,
    qty: &str,
) -> Result<(), String> {
    let positions = positions(qty);
    protection_cleanup_readback(
        store,
        ProtectionCleanupIdentity {
            key,
            hash,
            instrument_id: INST,
            algo_id: ALGO,
            expected_uid: UID,
        },
        ProtectionCleanupObservation {
            inventory,
            uid,
            position_received_at_ms: source_at,
            positions: &positions,
        },
    )
    .await
}

#[tokio::test]
async fn pc_01_first_send_complete_readback_and_replay_are_idempotent() {
    let f = fixture().await;
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-01", "hash-01")
            .await
            .unwrap(),
        ProtectionCleanupStep::SendOnce
    );
    let begin = operation(&f.store, "pc-01").await;
    assert_eq!(begin.result_ref, "POSTING");
    let signed = inventory((begin.committed_at + 1000) as u64, None, true);
    readback(
        &f.store,
        "pc-01",
        "hash-01",
        &signed,
        UID,
        begin.committed_at + 1000,
        "0",
    )
    .await
    .unwrap();
    assert_eq!(
        operation(&f.store, "pc-01").await.result_ref,
        "VERIFIED_CANCELED"
    );
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-01", "hash-01")
            .await
            .unwrap(),
        ProtectionCleanupStep::Confirmed
    );
}

#[tokio::test]
async fn pc_02_lost_ack_requires_signed_proof_without_second_send() {
    let f = fixture().await;
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-02", "hash-02")
            .await
            .unwrap(),
        ProtectionCleanupStep::SendOnce
    );
    let begun = operation(&f.store, "pc-02").await;
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-02", "hash-02")
            .await
            .unwrap(),
        ProtectionCleanupStep::Inspect
    );
    let still_live = inventory((begun.committed_at + 1000) as u64, Some((ALGO, INST)), true);
    assert_eq!(
        readback(
            &f.store,
            "pc-02",
            "hash-02",
            &still_live,
            UID,
            begun.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CANCEL_READBACK_PENDING"
    );
    assert_eq!(operation(&f.store, "pc-02").await.result_ref, "POSTING");
    let gone = inventory((begun.committed_at + 2000) as u64, None, true);
    readback(
        &f.store,
        "pc-02",
        "hash-02",
        &gone,
        UID,
        begun.committed_at + 2000,
        "0",
    )
    .await
    .unwrap();
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-02", "hash-02")
            .await
            .unwrap(),
        ProtectionCleanupStep::Confirmed
    );
}

#[tokio::test]
async fn pc_03_incomplete_and_stale_observations_leave_sql_posting() {
    let f = fixture().await;
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-03", "hash-03")
            .await
            .unwrap(),
        ProtectionCleanupStep::SendOnce
    );
    let begun = operation(&f.store, "pc-03").await;
    let incomplete = inventory((begun.committed_at + 1000) as u64, None, false);
    assert_eq!(
        readback(
            &f.store,
            "pc-03",
            "hash-03",
            &incomplete,
            UID,
            begun.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CANCEL_READBACK_INCOMPLETE"
    );
    let stale = inventory((begun.committed_at - 1) as u64, None, true);
    assert_eq!(
        readback(
            &f.store,
            "pc-03",
            "hash-03",
            &stale,
            UID,
            begun.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CANCEL_READBACK_STALE"
    );
    assert_eq!(operation(&f.store, "pc-03").await.result_ref, "POSTING");
}

#[tokio::test]
async fn pc_04_foreign_identity_wrong_algo_scope_and_late_exposure_fail_closed() {
    let f = fixture().await;
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-04", "hash-04")
            .await
            .unwrap(),
        ProtectionCleanupStep::SendOnce
    );
    let begun = operation(&f.store, "pc-04").await;
    let signed = inventory((begun.committed_at + 1000) as u64, None, true);
    assert_eq!(
        readback(
            &f.store,
            "pc-04",
            "hash-04",
            &signed,
            "foreign-account",
            begun.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CLEANUP_ACCOUNT_MISMATCH"
    );
    let foreign_algo = inventory(
        (begun.committed_at + 1000) as u64,
        Some((ALGO, "ETH-USDT-SWAP")),
        true,
    );
    assert_eq!(
        readback(
            &f.store,
            "pc-04",
            "hash-04",
            &foreign_algo,
            UID,
            begun.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CLEANUP_ALGO_INSTRUMENT_MISMATCH"
    );
    assert!(
        readback(
            &f.store,
            "pc-04",
            "hash-04",
            &signed,
            UID,
            begun.committed_at + 1000,
            "1"
        )
        .await
        .is_err()
    );
    assert_eq!(operation(&f.store, "pc-04").await.result_ref, "POSTING");
}

#[tokio::test]
async fn pc_05_restart_recovers_the_same_sql_begin_without_new_send() {
    let f = fixture().await;
    assert_eq!(
        protection_cleanup_step(&f.store, "pc-05", "hash-05")
            .await
            .unwrap(),
        ProtectionCleanupStep::SendOnce
    );
    let begin = operation(&f.store, "pc-05").await;
    let db = f.db.clone();
    let account_key = f.account_key.clone();
    let mut terminated = f.store.termination_signal();
    drop(f);
    // Do not race a second account owner against the first SQL writer.
    // The worker signals termination only AFTER closing SQLite and releasing
    // its exclusive writer lease; restart must wait for that real boundary.
    tokio::time::timeout(std::time::Duration::from_secs(5), terminated.changed())
        .await
        .expect("PC-05 previous SQL writer did not terminate")
        .expect("PC-05 SQL termination witness dropped");
    assert!(*terminated.borrow(), "PC-05 SQL owner still alive");
    let reopened = AccountStoreHandle::open_existing(db, account_key, "installation_test".into())
        .await
        .unwrap();
    assert_eq!(
        protection_cleanup_step(&reopened, "pc-05", "hash-05")
            .await
            .unwrap(),
        ProtectionCleanupStep::Inspect
    );
    let inventory = inventory((begin.committed_at + 1000) as u64, None, true);
    readback(
        &reopened,
        "pc-05",
        "hash-05",
        &inventory,
        UID,
        begin.committed_at + 1000,
        "0",
    )
    .await
    .unwrap();
    assert_eq!(
        operation(&reopened, "pc-05").await.result_ref,
        "VERIFIED_CANCELED"
    );
    assert_eq!(
        protection_cleanup_step(&reopened, "pc-05", "hash-05")
            .await
            .unwrap(),
        ProtectionCleanupStep::Confirmed
    );
}

#[tokio::test]
async fn pc_06_two_callers_share_one_begin_and_old_read_never_confirms() {
    let f = fixture().await;
    let (a, b) = tokio::join!(
        protection_cleanup_step(&f.store, "pc-06", "hash-06"),
        protection_cleanup_step(&f.store, "pc-06", "hash-06"),
    );
    let pair = [a.unwrap(), b.unwrap()];
    assert_eq!(
        pair.iter()
            .filter(|x| **x == ProtectionCleanupStep::SendOnce)
            .count(),
        1
    );
    assert_eq!(
        pair.iter()
            .filter(|x| **x == ProtectionCleanupStep::Inspect)
            .count(),
        1
    );
    let begin = operation(&f.store, "pc-06").await;
    let previously_started = inventory((begin.committed_at - 1) as u64, None, true);
    assert_eq!(
        readback(
            &f.store,
            "pc-06",
            "hash-06",
            &previously_started,
            UID,
            begin.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CANCEL_READBACK_STALE"
    );
    assert_eq!(operation(&f.store, "pc-06").await.result_ref, "POSTING");
    let later = inventory((begin.committed_at + 2000) as u64, None, true);
    readback(
        &f.store,
        "pc-06",
        "hash-06",
        &later,
        UID,
        begin.committed_at + 2000,
        "0",
    )
    .await
    .unwrap();
    assert_eq!(
        operation(&f.store, "pc-06").await.result_ref,
        "VERIFIED_CANCELED"
    );
}

#[tokio::test]
async fn pc_07_recorded_signed_http_cancel_ack_readback_and_sql_receipt() {
    use crate::account_bootstrap::AccountBootstrapper;
    use crate::accounts::recorded_support::{RecordedAccountHttp, RecordedGet, RecordedPost};
    use okx_api::{Credentials, OkxEnvironment, OkxRestClient, RateBudget, Region, TradeApi};
    use okx_execution::{Decision, IntentPlan, NewIntent};
    use std::sync::Arc;

    const WIRE_ALGO: &str = "900000002";
    const WIRE_UID: &str = "account-001";
    let f = fixture().await;
    let feed = Arc::new(RecordedAccountHttp::default());
    let raw = |data: serde_json::Value| {
        serde_json::to_vec(&serde_json::json!({
            "code": "0", "msg": "", "data": data
        }))
        .unwrap()
    };

    // Each signed inventory scope is recorded before the test begins.
    // Exactly one conditional stop exists before ACK, and none after it.
    let mut before = Vec::new();
    let mut after = Vec::new();
    for inst_type in ["SWAP", "FUTURES"] {
        for kind in [
            "conditional",
            "oco",
            "trigger",
            "move_order_stop",
            "chase",
            "iceberg",
            "twap",
            "smart_iceberg",
        ] {
            let query = vec![
                ("instType".to_owned(), inst_type.to_owned()),
                ("ordType".to_owned(), kind.to_owned()),
                ("limit".to_owned(), "100".to_owned()),
            ];
            let live = if inst_type == "SWAP" && kind == "conditional" {
                serde_json::json!([{
                    "instType": "SWAP", "instId": INST, "ordType": "conditional",
                    "algoId": WIRE_ALGO, "algoClOrdId": "s1234567890123456789012345678901",
                    "state": "live", "side": "sell", "posSide": "long",
                    "sz": "0.3", "tdMode": "cross",
                    "slTriggerPx": "900", "slTriggerPxType": "mark", "slOrdPx": "-1"
                }])
            } else {
                serde_json::json!([])
            };
            before.push(RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/trade/orders-algo-pending".into(),
                required_query: query.clone(),
                raw_response: raw(live),
            });
            after.push(RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/trade/orders-algo-pending".into(),
                required_query: query,
                raw_response: raw(serde_json::json!([])),
            });
        }
    }
    feed.expect_causal_algo_inventory(WIRE_ALGO, before, after);
    feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/cancel-algos".into(),
        expected_body: serde_json::json!([{
            "instId": INST, "algoId": WIRE_ALGO
        }]),
        raw_response: raw(serde_json::json!([{
            "algoId": WIRE_ALGO, "sCode": "0"
        }])),
    });

    let rate = RateBudget::new().for_authenticated_user(WIRE_UID);
    let environment = OkxEnvironment::new(Region::Global, true);
    let signer = |key: &str| {
        Credentials::new(key.to_owned(), "test-secret".into(), "test-pass".into()).unwrap()
    };
    let observer = AccountBootstrapper::new(
        OkxRestClient::with_http_transport(
            environment,
            signer("fixture-observer-a"),
            rate.clone(),
            feed.clone(),
        )
        .unwrap(),
    );
    let executor = OkxRestClient::with_http_transport(
        environment,
        signer("fixture-executor-a"),
        rate,
        feed.clone(),
    )
    .unwrap();
    let trade = TradeApi::new(executor.clone());
    let expected_uid = okx_api::account_uid_fingerprint(WIRE_UID);
    // Persist the actual intent and attached protection before cleanup.
    // A test-provided receipt cannot substitute for SQL ownership.
    let definition = f
        .store
        .call(deadline(), |s| {
            s.apply_authority_policy(1, false, "pc07", Decimal::from(20))?;
            s.ensure_manual_instance(now())
        })
        .await
        .unwrap();
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "open".into(),
        side: "buy".into(),
        position_side: "long".into(),
        quantity: Decimal::new(3, 1),
        limit_price: Decimal::from(1000),
        stop_price: Some(Decimal::from(900)),
        strategy_instance: "manual".into(),
        planned_loss_usd: Decimal::from(1),
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::ONE,
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: "pc07-decision".into(),
        instance_id: None,
        decision_at: now() - 100,
        input_digest: "a".repeat(64),
        policy_revision: 1,
        code_version: "pc07".into(),
    };
    let candidate = NewIntent {
        plan,
        intent_id: "pc07-owned-intent".into(),
        idempotency_key: "pc07-command".into(),
        request_hash: "a".repeat(64),
        client_order_id: "12345678901234567890123456789012".into(),
        deadline: now() + 30_000,
        plan_hash: "b".repeat(64),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: INST.into(),
    };
    let stored = f
        .store
        .call(deadline(), move |s| {
            let intent = s.prepare(&decision, &candidate)?;
            s.verify_protection(&okx_execution::ProtectionReceipt {
                intent_id: intent.intent_id.clone(),
                algo_id: WIRE_ALGO.into(),
                side: "sell".into(),
                quantity: Decimal::new(3, 1),
                trigger_price: Decimal::from(900),
                trigger_basis: "mark".into(),
                verified_at: now(),
                source_digest: "pc07-signed-algo-source".into(),
            })?;
            Ok(intent)
        })
        .await
        .unwrap();

    let capacity = tokio::sync::Semaphore::new(1);
    let trace = crate::diagnostics::ProtectionOperationTrace::default();
    let dependencies = CleanupDependencies {
        store: &f.store,
        observer: &observer,
        trade: &trade,
        clock: &executor,
        mutation_capacity: &capacity,
        verified_uid: &expected_uid,
        trace: &trace,
    };
    let order = CleanupOrderFacts {
        intent_id: &stored.intent_id,
        client_order_id: &stored.client_order_id,
        instrument_id: &stored.instrument_id,
        risk_increase: stored.risk_increase,
        signed_exposed: false,
        venue_order_state: "filled", // signed terminal order state supplied to public Status
    };
    let not_terminal = CleanupOrderFacts {
        venue_order_state: "live",
        ..order
    };
    cleanup_flat_protection(
        CleanupDependencies {
            store: &f.store,
            observer: &observer,
            trade: &trade,
            clock: &executor,
            mutation_capacity: &capacity,
            verified_uid: &expected_uid,
            trace: &trace,
        },
        not_terminal,
    )
    .await
    .unwrap();
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    let sql_key = format!("cleanup_{}", stored.client_order_id);
    let sql_none = f
        .store
        .call(deadline(), {
            let sql_key = sql_key.clone();
            move |s| s.operation(&sql_key)
        })
        .await
        .unwrap();
    assert!(sql_none.is_none(), "live order cannot begin cancel SQL");

    feed.expect_public_clock(raw(serde_json::json!([{
        "ts": chrono::Utc::now().timestamp_millis().to_string()
    }])));
    cleanup_flat_protection(dependencies, order).await.unwrap();
    assert_eq!(
        operation(&f.store, &sql_key).await.result_ref,
        "VERIFIED_CANCELED"
    );
    let explanation = trace.read(&sql_key).unwrap().unwrap();
    assert_eq!(
        explanation["snapshot"]["durable_state"],
        "VERIFIED_CANCELED"
    );
    assert_eq!(explanation["snapshot"]["venue_ack"], "OBSERVED");
    assert!(
        explanation["snapshot"]["inventory_received_at_ms"]
            .as_u64()
            .is_some()
    );
    assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1);
    let causal_events = feed.causal_algo_trace.lock().unwrap().clone();
    assert!(causal_events.iter().any(|event| {
        event.contains("event=CANCEL_ACK") && event.contains("cancel_id=900000002")
    }));
    cleanup_flat_protection(
        CleanupDependencies {
            store: &f.store,
            observer: &observer,
            trade: &trade,
            clock: &executor,
            mutation_capacity: &capacity,
            verified_uid: &expected_uid,
            trace: &trace,
        },
        order,
    )
    .await
    .unwrap();
    let sends = feed.outbound_posts.lock().unwrap();
    assert_eq!(sends.len(), 1, "repeated public cleanup cannot repost");
    assert_eq!(sends[0].0, "fixture-executor-a");
    assert_eq!(sends[0].1, "/api/v5/trade/cancel-algos");
    assert_eq!(sends[0].2[0]["algoId"], WIRE_ALGO);
    assert!(feed.denied_routes.lock().unwrap().is_empty());
}
