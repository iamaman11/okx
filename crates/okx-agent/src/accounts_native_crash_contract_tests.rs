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
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut tail = std::collections::VecDeque::with_capacity(16);
        loop {
            let Some(line) = lines.next_line().await.unwrap() else {
                panic!("Native child exited before the post-Create crash boundary: status={:?}, stdout_tail={tail:?}", child.try_wait().unwrap());
            };
            if line.contains(MARKER) {
                break;
            }
            if tail.len() == 16 { tail.pop_front(); }
            tail.push_back(line);
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
    let now = jiff::Timestamp::now().as_millisecond().to_string();
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
    // A new process has no volatile operation trace. Durable diagnostics must
    // still explain the original UNKNOWN without reading the queued venue GET.
    let reconnected = registry.resolve(&config.alias).await.unwrap();
    let source_gate = reconnected.recorded_capture_gate.write().await;
    let reads_before = feed.requests.lock().unwrap().clone();
    let diagnostic = crate::diagnostics::native_intent::read(
        &reconnected,
        owner.installation_id(),
        &format!("intent:{}", before.intent_id),
    )
    .await;
    assert_eq!(diagnostic["state"], "UNKNOWN", "{diagnostic}");
    assert_eq!(diagnostic["intent"]["plan_hash"], before.plan_hash);
    assert_eq!(
        diagnostic["missing_evidence"],
        json!(["AUTHORITATIVE_VENUE_ORDER_READBACK"])
    );
    assert_eq!(feed.requests.lock().unwrap().as_slice(), reads_before);
    assert_eq!(
        feed.unconsumed_reads(),
        1,
        "read-only explanation cannot consume the future Status readback"
    );
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    drop(source_gate);
    drop(reconnected);
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

// The actual history capture commits a signed fill before its later lot
// projection. The parent kills the process while that capture remains parked.
#[tokio::test]
async fn risk_book_contract_native_os_kill_pending_fill_projects_once() {
    use super::filled_fixture::{INSTRUMENT, OPEN_ORDER, history, opened_market, snapshot};
    const FLAG: &str = "OKX_NATIVE_FILL_CRASH_CHILD";
    const READY: &str = "OKX_NATIVE_FILL_CRASH_READY=";
    if std::env::var_os(FLAG).is_some() {
        let mut fixture = fixture().await;
        let (_market_stop, _market_task, _socket, _public) = opened_market(&mut fixture).await;
        let now = jiff::Timestamp::now().as_millisecond().to_string();
        fixture
            .feed
            .expect_public_clock(envelope(json!([{"ts":now}])));
        record_fee(&fixture.feed, &now);
        record_leverage(&fixture.feed);
        record_leverage(&fixture.feed);
        record_fee(&fixture.feed, &now);
        record_fee(&fixture.feed, &now);
        record_leverage(&fixture.feed);
        fixture.feed.expect_post(RecordedPost {access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
            expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
                "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                    "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
            raw_response:envelope(json!([{"ordId":OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]))});
        fixture.feed.arm_lost_next_create_ack();
        let mut request = execute();
        if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
            *expires_in_ms = 10_000;
        }
        let result = fixture
            .trade
            .trade(&fixture.config.alias, &request, Some(&fixture.authority))
            .await
            .unwrap();
        assert_eq!(result["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN");
        assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
        let _gate = fixture._account.recorded_capture_gate.write().await;
        *fixture.market_boundary.market.lock().unwrap() = None;
        let at = jiff::Timestamp::now().as_millisecond();
        snapshot(&fixture.feed, "99.9995", "0.1");
        history(
            &fixture.feed,
            Some((OPEN_ORDER, "native-open-fill", "buy", at)),
        );
        let ready = Arc::new(tokio::sync::Notify::new());
        let capture = crate::source_capture::TEST_AFTER_HISTORY.scope(
            ready.clone(),
            crate::source_capture::capture_account_sources_measured(
                &fixture._account.observer,
                &fixture._account.private_ws,
                &fixture._account.execution,
            ),
        );
        tokio::pin!(capture);
        tokio::select! {
            result = &mut capture => panic!("capture exited before pending projection: {:?}", result.err()),
            () = ready.notified() => {},
        }
        use std::io::Write as _;
        println!("{READY}{}", fixture.root.display());
        std::io::stdout().flush().unwrap();
        std::future::pending::<()>().await;
        unreachable!();
    }
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("accounts::native_trade_contracts::native_crash::risk_book_contract_native_os_kill_pending_fill_projects_once")
        .arg("--nocapture").env(FLAG, "1").stdout(Stdio::piped()).stderr(Stdio::inherit())
        .kill_on_drop(true).spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let root = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let line = lines
                .next_line()
                .await
                .unwrap()
                .expect("child exited before pending fill boundary");
            if let Some((_, path)) = line.split_once(READY) {
                break PathBuf::from(path.trim());
            }
        }
    })
    .await
    .expect("pending fill boundary timed out");
    assert!(child.try_wait().unwrap().is_none());
    child.kill().await.unwrap();
    assert!(!child.wait().await.unwrap().success());
    let catalog = account_catalog_proto::load(&root).unwrap();
    let account = catalog.accounts[0].account_key.wire();
    let db = rusqlite::Connection::open_with_flags(
        root.join("finance/finance.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let counts = || -> (i64, i64, i64) {
        db.query_row(
        "SELECT (SELECT count(*) FROM fills), (SELECT count(*) FROM fill_projection_pending), (SELECT count(*) FROM position_lots)",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap()
    };
    assert_eq!(counts(), (1, 1, 0));
    let fee_facts = || -> Vec<(String, String, String)> {
        db.prepare("SELECT entry_id, economic_event_key, amount FROM finance_entries WHERE component='FEE' ORDER BY entry_id")
            .unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap().collect::<Result<Vec<_>, _>>().unwrap()
    };
    let before_fees = fee_facts();
    assert_eq!(before_fees.len(), 1);
    assert_eq!(
        before_fees[0].2.parse::<Decimal>().unwrap(),
        Decimal::new(-5, 4)
    );
    let owner = UnifiedFinanceHandle::open_native(root.clone(), catalog.installation_id)
        .await
        .unwrap();
    let intent = owner
        .lookup_intent(
            account.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        owner
            .project_pending_fills(account.clone(), 1000)
            .await
            .unwrap(),
        1
    );
    let lots = owner
        .active_lots(
            account.clone(),
            INSTRUMENT.into(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(lots.len(), 1);
    assert_eq!(
        lots[0].intent_id.as_deref(),
        Some(intent.intent_id.as_str())
    );
    assert_eq!(lots[0].remaining_qty, Decimal::new(1, 1));
    assert_eq!(owner.project_pending_fills(account, 1000).await.unwrap(), 0);
    assert_eq!(counts(), (1, 0, 1));
    assert_eq!(
        fee_facts(),
        before_fees,
        "projection replay must not duplicate or alter the signed fee"
    );
    drop(db);
    let mut closed = owner.termination_signal();
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
