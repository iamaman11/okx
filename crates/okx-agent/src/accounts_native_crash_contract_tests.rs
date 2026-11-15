//! A NEW Native installation killed by its parent OS process after one real
//! recorded Create, before the Native outcome commit. This is one crash point;
//! it does not claim a complete F0–F8 crash matrix.
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};

const CHILD: &str = "accounts::native_trade_contracts::risk_book_contract_native_public_execute_lost_ack_and_replay_send_once";
const MARKER: &str = "OKX_NATIVE_OS_CRASH_POST_CREATE_PRE_RESULT";

// Read-only evidence of the original posting fence, never a test SQL mutation.
fn fence(root: &Path) -> (i64, i64, String, String) {
    let db = rusqlite::Connection::open_with_flags(
        root.join("finance").join("finance.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    db.query_row(
        "SELECT (SELECT COUNT(*) FROM intents),i.begin_post_at,r.state,e.state
         FROM intents i JOIN risk_receipts r
           USING(installation_id,account_key,intent_id)
         JOIN risk_entries e USING(installation_id,account_key,intent_id)",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .unwrap()
}

fn money(summary: &okx_execution::account_worker::UnifiedFinanceCampaignSummary) -> Value {
    let mut value = serde_json::to_value(summary).unwrap();
    // Recovery advances the original sequence once; all financial values,
    // policy and original opening evidence must remain identical.
    value.as_object_mut().unwrap().remove("book_sequence");
    value
}

#[tokio::test]
async fn risk_book_contract_native_os_kill_after_create_recovers_original_unknown() {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let root = std::env::temp_dir().join(format!(
        "okx-native-os-crash-{:032x}",
        u128::from_le_bytes(nonce)
    ));
    assert!(matches!(std::fs::symlink_metadata(&root), Err(e)
        if e.kind() == std::io::ErrorKind::NotFound));
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg(CHILD)
        .arg("--nocapture")
        .env("OKX_NATIVE_CRASH_CHILD", "POST_CREATE_PRE_RESULT")
        .env("OKX_NATIVE_CRASH_ROOT", &root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .expect("Native child exited before the post-Create crash boundary");
            if line.contains(MARKER) {
                break;
            }
        }
    })
    .await
    .expect("Native child did not reach the original Create boundary");
    // This terminates the actual process, including its SQLite owner thread.
    // No future abort or graceful source/owner shutdown substitutes for death.
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(
        !status.success(),
        "the child must be killed, not finish its test"
    );

    let catalog = account_catalog_proto::load(&root).unwrap();
    assert_eq!(catalog.accounts.len(), 1);
    let account = catalog.accounts[0].account_key.wire();
    let owner = UnifiedFinanceHandle::open_native(root.clone(), catalog.installation_id.clone())
        .await
        .expect("original NEW Native SQLite must reopen after OS death");
    let deadline = || Instant::now() + Duration::from_secs(3);
    let before = owner
        .lookup_intent(
            account.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.state, IntentState::Posting);
    assert!(before.exchange_order_id.is_none());
    let receipt = owner
        .reservation_receipt(
            account.clone(),
            before.intent_id.clone(),
            CentralReceiptView::ActiveExposure,
            deadline(),
        )
        .await
        .unwrap()
        .expect("the actual Create retains its original central exposure receipt");
    assert_eq!(receipt.intent_id, before.intent_id);
    assert_eq!(receipt.plan_hash, before.plan_hash);
    assert_eq!(receipt.installation_id, catalog.installation_id);
    let campaign = owner
        .configured_campaign(deadline())
        .await
        .unwrap()
        .unwrap();
    let budget = owner.campaign_summary(campaign.clone()).await.unwrap();
    assert!(budget.opening.is_some());
    for retained in [
        &budget.global_reserved_usdt,
        &budget.reserved_loss_usd,
        &budget.reserved_entry_usd,
    ] {
        assert!(retained.parse::<Decimal>().unwrap() > Decimal::ZERO);
    }
    let original_fence = fence(&root);
    assert_eq!(original_fence.0, 1);
    assert!(original_fence.1 > 0);
    assert_eq!(
        (&*original_fence.2, &*original_fence.3),
        ("POSTED", "POSTED")
    );

    assert_eq!(
        owner
            .recover_posting(account.clone(), deadline())
            .await
            .unwrap(),
        1
    );
    let recovered = owner
        .lookup_intent(
            account.clone(),
            TradeIntentLookup::IntentId(before.intent_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.state, IntentState::Unknown);
    assert_eq!(recovered.client_order_id, before.client_order_id);
    assert_eq!(recovered.idempotency_key, before.idempotency_key);
    assert_eq!(recovered.request_hash, before.request_hash);
    assert_eq!(recovered.plan_hash, before.plan_hash);
    assert!(recovered.exchange_order_id.is_none());
    let retained = owner
        .reservation_receipt(
            account.clone(),
            before.intent_id.clone(),
            CentralReceiptView::ActiveExposure,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(retained).unwrap(),
        serde_json::to_value(receipt).unwrap()
    );
    let after = owner.campaign_summary(campaign.clone()).await.unwrap();
    assert_eq!(money(&after), money(&budget));
    assert_eq!(after.book_sequence, budget.book_sequence + 1);
    let recovered_fence = fence(&root);
    assert_eq!(
        (recovered_fence.0, recovered_fence.1),
        (original_fence.0, original_fence.1)
    );
    assert_eq!(
        (&*recovered_fence.2, &*recovered_fence.3),
        ("UNKNOWN", "UNKNOWN")
    );
    assert_eq!(owner.recover_posting(account, deadline()).await.unwrap(), 0);
    assert_eq!(
        owner.campaign_summary(campaign.clone()).await.unwrap(),
        after
    );
    assert_eq!(
        fence(&root),
        recovered_fence,
        "recovery replay cannot create another POST claim"
    );

    assert!(
        !root.join("accounts").exists(),
        "only the original financial SQLite exists"
    );
    let mut closed = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();

    // Restore the actual account route from its durable catalog. Status must
    // observe the original client ID through a new signed GET, never Create.
    let rate = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, rate.clone(), &campaign)
            .await
            .unwrap();
    let registry = Arc::new(registry);
    let feed = Arc::new(RecordedAccountHttp::default());
    let (config, clients) = prepared_with_socket(
        "a",
        feed.clone(),
        Arc::new(RecordedWsConnector::default()),
        rate.clone(),
    );
    assert_eq!(config, catalog.accounts[0]);
    let now = chrono::Utc::now().timestamp_millis().to_string();
    feed.expect_get(RecordedGet {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        required_query: vec![
            ("instId".into(), "BTC-USDT-SWAP".into()),
            ("clOrdId".into(), before.client_order_id.clone()),
        ],
        raw_response: envelope(json!([{
            "instId":"BTC-USDT-SWAP", "ordId":"native-lost-ack-001",
            "clOrdId":before.client_order_id, "side":"buy", "posSide":"long",
            "tdMode":"cross", "ordType":"limit", "px":"1000", "sz":"0.1",
            "accFillSz":"0", "avgPx":"", "state":"live", "cTime":now, "uTime":now,
        }])),
    });
    registry
        .reconnect_prepared(config.clone(), clients)
        .await
        .unwrap();
    let (_market, public) = okx_runtime::PublicWsCoordinator::new_with_connector(
        OkxEnvironment::new(okx_api::Region::Global, false),
        okx_observation::ReferenceRegistry::from_public("native-crash-readback", vec![]).unwrap(),
        rate,
        Arc::new(crate::accounts::recorded_support::OfflineSocket),
    );
    let service = TradeService::new(
        registry.clone(),
        public,
        NativeRiskClient::local(NativeFinanceRisk::from_owner(owner.clone())),
    );
    let status = service
        .trade(
            &config.alias,
            &TradeRequest::Status {
                reference: before.intent_id.clone(),
            },
            None,
        )
        .await
        .expect("public Status must read the original crashed intent");
    assert_eq!(
        status["data"]["intent"]["intent_id"], before.intent_id,
        "{status}"
    );
    assert_eq!(
        status["data"]["intent"]["client_order_id"], before.client_order_id,
        "{status}"
    );
    assert_eq!(
        status["data"]["intent"]["exchange_order_id"], "native-lost-ack-001",
        "{status}"
    );
    assert_eq!(
        feed.unconsumed_reads(),
        0,
        "the literal client-ID readback was consumed"
    );
    assert!(
        feed.outbound_posts.lock().unwrap().is_empty(),
        "Status cannot replay Create"
    );
    registry.shutdown().await;
    let mut closed = owner.termination_signal();
    drop(service);
    drop(registry);
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        root.parent().unwrap().canonicalize().unwrap(),
        std::env::temp_dir().canonicalize().unwrap()
    );
    std::fs::remove_dir_all(root).unwrap();
}
