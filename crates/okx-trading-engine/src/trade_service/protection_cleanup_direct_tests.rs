//! #500: direct production cleanup transition tests with the actual SQL owner.
//! Inputs are independent inventory/position observations, not success flags.
//! These tests exercise the exact durable decision called by TradeService.
use super::*;
use crate::accounts::native_trade_contracts::{
    filled_fixture as filled,
    fixture::{self as native, NativeFixture},
};
use okx_api::{PendingDerivativeAlgoRiskInventory, PendingProtectiveAlgoSample};
const INST: &str = "BTC-USDT-SWAP";
const ALGO: &str = "owned-protection-001";

struct Fixture {
    store: ExecutionStore,
    native: NativeFixture,
}
async fn fixture() -> Fixture {
    // Even a direct durable BEGIN needs the SQL owner's original verified
    // campaign opening. Reuse the already exercised production bootstrap
    // instead of a second maintenance polling loop or an artificial source
    // refresh; pc_07 uses the same path for signed HTTP cancellation.
    let mut native = native::fixture().await;
    let (market_stop, market_task, _socket, _public) = filled::opened_market(&mut native).await;
    // No market observations are needed after bootstrap for these six direct
    // SQL contracts. Stop and join the public fixture before testing BEGIN.
    market_stop.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), market_task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        native
            .owner
            .campaign_summary(native.campaign.clone())
            .await
            .unwrap()
            .opening
            .is_some(),
        "verified campaign opening is mandatory for direct SQL cleanup"
    );
    let store = native._account.execution.store.clone();
    Fixture { store, native }
}
impl Fixture {
    async fn finish(self) {
        let Self { store, native } = self;
        drop(store);
        native.finish().await;
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
async fn operation(store: &ExecutionStore, key: &str) -> okx_trade_execution::OperationReceipt {
    store
        .operation(
            deadline(),
            crate::execution_store::OperationId::new(key.to_owned()).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
}
async fn readback(
    store: &ExecutionStore,
    key: &str,
    hash: &str,
    inventory: &PendingDerivativeAlgoRiskInventory,
    uid: &str,
    source_at: i64,
    qty: &str,
) -> Result<(), String> {
    let positions = positions(qty);
    let (_, account) = store.scope();
    let expected_uid = account.rsplit(':').next().unwrap();
    protection_cleanup_readback(
        store,
        ProtectionCleanupIdentity {
            key,
            hash,
            instrument_id: INST,
            algo_id: ALGO,
            expected_uid,
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
        &f.native.config.account_key.verified_uid,
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
    f.finish().await;
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
            &f.native.config.account_key.verified_uid,
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
        &f.native.config.account_key.verified_uid,
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
    f.finish().await;
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
            &f.native.config.account_key.verified_uid,
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
            &f.native.config.account_key.verified_uid,
            begun.committed_at + 1000,
            "0"
        )
        .await
        .unwrap_err(),
        "PROTECTION_CANCEL_READBACK_STALE"
    );
    assert_eq!(operation(&f.store, "pc-03").await.result_ref, "POSTING");
    f.finish().await;
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
            &f.native.config.account_key.verified_uid,
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
            &f.native.config.account_key.verified_uid,
            begun.committed_at + 1000,
            "1"
        )
        .await
        .is_err()
    );
    assert_eq!(operation(&f.store, "pc-04").await.result_ref, "POSTING");
    f.finish().await;
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
    let root = f.native.root.clone();
    let installation = f.native.owner.installation_id().to_owned();
    let account_key = f.native.config.account_key.clone();
    let verified_uid = f.native.config.account_key.verified_uid.clone();
    f.native.registry.shutdown().await;
    let mut terminated = f.native.owner.termination_signal();
    drop(f);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        terminated.wait_for(|v| *v),
    )
    .await
    .unwrap()
    .unwrap();
    let owner = okx_trade_execution::account_worker::UnifiedFinanceHandle::open_native(
        root.clone(),
        installation,
    )
    .await
    .unwrap();
    let reopened = ExecutionStore::native(owner, account_key).unwrap();
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
        &verified_uid,
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
    let (owner, _) = reopened.scope();
    let mut terminated = owner.termination_signal();
    drop(reopened);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        terminated.wait_for(|v| *v),
    )
    .await
    .unwrap()
    .unwrap();
    std::fs::remove_dir_all(root).unwrap();
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
            &f.native.config.account_key.verified_uid,
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
        &f.native.config.account_key.verified_uid,
        begin.committed_at + 2000,
        "0",
    )
    .await
    .unwrap();
    assert_eq!(
        operation(&f.store, "pc-06").await.result_ref,
        "VERIFIED_CANCELED"
    );
    f.finish().await;
}

#[tokio::test]
async fn pc_07_recorded_signed_http_cancel_ack_readback_and_sql_receipt() {
    use crate::account_bootstrap::AccountBootstrapper;
    use crate::accounts::recorded_support::{RecordedAccountHttp, RecordedGet, RecordedPost};
    use okx_api::{
        Credentials, HttpTransport, OkxEnvironment, OkxRestClient, RateBudget, Region, TradeApi,
    };
    use okx_ipc_contracts::trading::TradeRequest;
    use std::sync::Arc;

    const WIRE_ALGO: &str = "900000002";
    const WIRE_UID: &str = "account-001";
    let mut f = native::fixture().await;
    let (market_stop, market_task, _socket, _public) = filled::opened_market(&mut f).await;
    let account = f._account.clone();
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    f.feed
        .expect_public_clock(native::envelope(serde_json::json!([{"ts":at}])));
    native::record_fee(&f.feed, &at);
    native::record_leverage(&f.feed);
    native::record_leverage(&f.feed);
    native::record_fee(&f.feed, &at);
    native::record_fee(&f.feed, &at);
    native::record_leverage(&f.feed);
    f.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(), path: "/api/v5/trade/order".into(),
        expected_body: serde_json::json!({"instId":INST,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: native::envelope(serde_json::json!([{"ordId":filled::OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]))
    });
    f.feed.arm_lost_next_create_ack();
    let mut request = native::execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let result = f
        .trade
        .trade(&f.config.alias, &request, Some(&f.authority))
        .await
        .unwrap();
    assert_eq!(result["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN");
    let entry = result["intent_id"].as_str().unwrap().to_owned();
    let timer_capture = account.recorded_capture_gate.write().await;
    let opened_at = jiff::Timestamp::now().as_millisecond();
    filled::snapshot(&f.feed, "99.9995", "0.1");
    filled::history(
        &f.feed,
        Some((filled::OPEN_ORDER, "native-open-fill", "buy", opened_at)),
    );
    filled::capture(&f).await;
    let stored = f
        .owner
        .lookup_intent(
            f.config.account_key.wire(),
            okx_trade_execution::TradeIntentLookup::Reference(entry.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let stop_client = format!("s{}", &stored.client_order_id[..31]);
    filled::order(&f.feed, filled::OPEN_ORDER, "buy", opened_at);
    filled::protection(&f.feed, &stop_client);
    filled::snapshot(&f.feed, "99.9995", "0.1");
    filled::snapshot(&f.feed, "99.9995", "0.1");
    filled::history(&f.feed, None);
    let status = f
        .trade
        .trade(
            &f.config.alias,
            &TradeRequest::Status { reference: entry },
            None,
        )
        .await
        .unwrap();
    assert_eq!(status["data"]["intent"]["state"], "FILLED");
    assert!(
        f.owner
            .protection(
                deadline(),
                f.config.account_key.wire(),
                stored.intent_id.clone()
            )
            .await
            .unwrap()
            .is_some()
    );
    let store = account.execution.store.clone();
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
    assert_eq!(expected_uid, f.config.account_key.verified_uid);
    let capacity = tokio::sync::Semaphore::new(1);
    let trace = crate::diagnostics::ProtectionOperationTrace::default();
    let dependencies = CleanupDependencies {
        store: &store,
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
        invocation: None,
    };
    let not_terminal = CleanupOrderFacts {
        venue_order_state: "live",
        ..order
    };
    let reobserve_order = CleanupOrderFacts {
        invocation: Some(crate::diagnostics::ProtectionCleanupInvocation {
            request_id: "req_0123456789abcdef",
            origin: "agent_request.trade.status",
        }),
        ..order
    };
    cleanup_flat_protection(
        CleanupDependencies {
            store: &store,
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
    let sql_none = store
        .operation(
            deadline(),
            crate::execution_store::OperationId::new(sql_key.clone()).unwrap(),
        )
        .await
        .unwrap();
    assert!(sql_none.is_none(), "live order cannot begin cancel SQL");

    feed.expect_public_clock(raw(serde_json::json!([{
        "ts": jiff::Timestamp::now().as_millisecond().to_string()
    }])));
    // A signed GET which started before ACK must retain its old bytes
    // even when the actual transport finishes delivering them after ACK.
    // This is a fixed event barrier, not a page-count retry/sleep.
    let (entered, resume) = feed.hold_next_causal_algo_get();
    let old_feed = feed.clone();
    let delayed_get = tokio::spawn(async move {
        let request = reqwest::Client::new()
            .get("https://www.okx.com/api/v5/trade/orders-algo-pending?instType=SWAP&ordType=conditional&limit=100")
            .header("OK-ACCESS-KEY", "fixture-observer-a")
            .build()
            .unwrap();
        old_feed
            .send(request, Instant::now() + Duration::from_secs(20))
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("the signed pre-ACK GET must reach its I/O boundary");

    // Background maintenance and explicit public Status enter the SAME
    // production operation. Account mutation capacity allows only one
    // active owner; the loser cannot create a second potential venue POST.
    let (background, explicit) = tokio::join!(
        cleanup_flat_protection(dependencies, order),
        cleanup_flat_protection(
            CleanupDependencies {
                store: &store,
                observer: &observer,
                trade: &trade,
                clock: &executor,
                mutation_capacity: &capacity,
                verified_uid: &expected_uid,
                trace: &trace,
            },
            order,
        )
    );
    let results = [background, explicit];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| r
                .as_ref()
                .err()
                .is_some_and(|reason| reason == "ACCOUNT_MUTATION_BUSY"))
            .count(),
        1,
        "two cleanup callers must share the same account mutation permit"
    );
    // Only after the one real ACK, release the earlier immutable GET:
    // it still contains the live stop and cannot constitute fresh proof.
    resume.notify_one();
    let stale = tokio::time::timeout(Duration::from_secs(3), delayed_get)
        .await
        .expect("pre-ACK signed GET must complete")
        .expect("pre-ACK task must not panic")
        .expect("pre-ACK transport was registered");
    let old_bytes: serde_json::Value = serde_json::from_slice(&stale.body).unwrap();
    assert_eq!(old_bytes["data"][0]["algoId"], WIRE_ALGO);

    assert_eq!(
        operation(&store, &sql_key).await.result_ref,
        "VERIFIED_CANCELED"
    );
    let explanation = trace.read(&sql_key).unwrap();
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
            store: &store,
            observer: &observer,
            trade: &trade,
            clock: &executor,
            mutation_capacity: &capacity,
            verified_uid: &expected_uid,
            trace: &trace,
        },
        reobserve_order,
    )
    .await
    .unwrap();
    {
        let sends = feed.outbound_posts.lock().unwrap();
        assert_eq!(sends.len(), 1, "repeated public cleanup cannot repost");
        assert_eq!(sends[0].0, "fixture-executor-a");
        assert_eq!(sends[0].1, "/api/v5/trade/cancel-algos");
        assert_eq!(sends[0].2[0]["algoId"], WIRE_ALGO);
        assert!(feed.denied_routes.lock().unwrap().is_empty());
    }
    let explanation = trace.read(&sql_key).unwrap();
    assert!(explanation["snapshot"]["parent_invocation"].is_null());
    assert_eq!(
        explanation["snapshot"]["last_observed_invocation"],
        "req_0123456789abcdef"
    );
    assert_eq!(
        explanation["snapshot"]["last_observed_origin"],
        "agent_request.trade.status"
    );
    drop(timer_capture);
    drop(account);
    drop(store);
    market_stop.send(true).unwrap();
    market_task.await.unwrap();
    f.finish().await;
}
