//! Public caller cancellation after original HELD and before any venue send.
//! This is not SQL COMMIT/reply loss or process death. The cold reopen below
//! restores the same catalog and finance owner without granting another send.
use super::filled_fixture::opened_market;
use super::*;
use okx_execution::{CentralReceiptView, IntentState, TradeIntentLookup};
use rust_decimal::Decimal;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn risk_book_contract_native_public_cancel_after_held_replays_and_cold_reopens_without_send()
{
    let mut fixture = fixture().await;
    let (stop, market, _socket, _public) = opened_market(&mut fixture).await;
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        // Ordinary production maximum permits a real cold reconnect within
        // this command's original lifetime; no persisted deadline is rewritten.
        *expires_in_ms = 30_000;
    }
    assert!(request.valid());
    let gate = Arc::new(PlanningFeeGate::default());
    *fixture
        .market_boundary
        .planning_fee_gate_actor
        .lock()
        .unwrap() = Some("fixture-observer-a".into());
    fixture
        .market_boundary
        .planning_fee_skip
        .store(2, Ordering::SeqCst);
    *fixture.market_boundary.planning_fee_gate.lock().unwrap() = Some(gate.clone());
    let at = chrono::Utc::now().timestamp_millis().to_string();
    record_fee(&fixture.feed, &at); // original plan
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed); // original pre-reserve assessment
    record_fee(&fixture.feed, &at);
    let trade = fixture.trade.clone();
    let authority = fixture.authority.clone();
    let sent_request = request.clone();
    let mut caller = tokio::spawn(async move {
        trade
            .trade("fixture-a", &sent_request, Some(&authority))
            .await
    });
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::select! {
            _ = gate.entered.notified() => {},
            outcome = &mut caller => {
                let facts = fixture.owner.account_diagnostic_snapshot(deadline(), fixture.config.account_key.wire(), None).await;
                panic!("Execute returned before post-HELD fee read: outcome={outcome:?}; current_account_snapshot={facts:?}");
            }
        }
    })
        .await
        .expect("public Execute must reach its third fee read after original HELD");
    let account = fixture._account.clone();
    let key = fixture.config.account_key.wire();
    let original = fixture
        .owner
        .lookup_intent(
            key.clone(),
            TradeIntentLookup::IdempotencyKey("native_opening_required_001".into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.state, IntentState::Reserved);
    assert!(original.exchange_order_id.is_none());
    let receipt = fixture
        .owner
        .reservation_receipt(
            key.clone(),
            original.intent_id.clone(),
            CentralReceiptView::ActiveExposure,
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.intent_id, original.intent_id);
    assert_eq!(receipt.account_key, key);
    assert_eq!(receipt.plan_hash, original.plan_hash);
    assert_eq!(receipt.requested_loss_usd, Decimal::new(102, 3));
    let budget = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert!(budget.opening.is_some());
    assert_eq!(budget.reserved_entry_usd, "1.0005");
    assert_eq!(budget.remaining_entry_usd, "8.9995");
    assert_eq!(budget.reserved_loss_usd, "0.102");
    assert_eq!(budget.global_reserved_usdt, "0.102");
    assert_eq!(budget.consumed_entry_usd, "0");
    let installation = fixture
        .owner
        .installation_receipt(deadline())
        .await
        .unwrap();
    let metadata = fixture
        .owner
        .account_metadata(deadline(), key.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 1);
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());

    // The cancelled HTTP future has not consumed any third fee source. Await
    // actual task cancellation before any readback; dropping a JoinHandle alone
    // would detach the original Execute and would not prove cancellation.
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    *fixture.market_boundary.market.lock().unwrap() = None;
    let source_timer = account.recorded_capture_gate.write().await;
    let reads_before = fixture.feed.requests.lock().unwrap().clone();
    let replay = fixture
        .trade
        .trade("fixture-a", &request, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        replay["data"]["intent"]["intent_id"], original.intent_id,
        "{replay}"
    );
    assert_eq!(replay["data"]["intent"]["state"], "RESERVED", "{replay}");
    assert_eq!(
        replay["data"]["intent"]["client_order_id"],
        original.client_order_id
    );
    assert_eq!(replay["data"]["remaining_exposure"], "not_evaluated");
    assert_eq!(*fixture.feed.requests.lock().unwrap(), reads_before);
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        budget
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    market.await.unwrap();
    let root = fixture.root.clone();
    let campaign = fixture.campaign.clone();
    let authority = fixture.authority.clone();
    let catalog = account_catalog_proto::load(&root).unwrap();
    assert_eq!(catalog.accounts, vec![fixture.config.clone()]);
    let mut closed = fixture.owner.termination_signal();
    drop(source_timer);
    drop(account);
    fixture.registry.shutdown().await;
    drop(fixture);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();

    // New process-local limiter/WS transport; existing canonical SQLite and
    // catalog. Reconnection uses the production opening identity checks.
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
    assert_eq!(catalog.accounts, vec![config.clone()]);
    registry
        .reconnect_prepared(config.clone(), clients)
        .await
        .unwrap();
    let reconnected = registry.resolve(&config.alias).await.unwrap();
    let source_timer = reconnected.recorded_capture_gate.write().await;
    assert_eq!(
        owner.installation_receipt(deadline()).await.unwrap(),
        installation
    );
    assert_eq!(
        owner
            .account_metadata(deadline(), key.clone())
            .await
            .unwrap()
            .unwrap(),
        metadata
    );
    assert_eq!(
        owner.campaign_summary(campaign.clone()).await.unwrap(),
        budget
    );
    let restored = owner
        .lookup_intent(
            key.clone(),
            TradeIntentLookup::IntentId(original.intent_id.clone()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&restored).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    let active = owner
        .reconcilable_intents_page(key.clone(), 10, 0, deadline())
        .await
        .unwrap();
    assert_eq!(active.len(), 1, "reopen cannot prepare another intent");
    assert_eq!(active[0].intent_id, original.intent_id);
    assert!(
        chrono::Utc::now().timestamp_millis() < restored.deadline,
        "cold readback must occur within the ORIGINAL persisted command lifetime"
    );
    let (_market, public) = okx_runtime::PublicWsCoordinator::new_with_connector(
        OkxEnvironment::new(okx_api::Region::Global, false),
        okx_observation::ReferenceRegistry::from_public("native-held-readback", vec![]).unwrap(),
        rate,
        Arc::new(recorded_support::OfflineSocket),
    );
    let service = TradeService::new(
        registry.clone(),
        public,
        NativeRiskClient::local(NativeFinanceRisk::from_owner(owner.clone())),
    );
    service.update_governance(&authority).await.unwrap();
    let reads_before = feed.requests.lock().unwrap().clone();
    let explanation = crate::diagnostics::native_intent::read(
        &reconnected,
        owner.installation_id(),
        &format!("intent:{}", original.intent_id),
    )
    .await;
    assert_eq!(explanation["state"], "RESERVED");
    assert_eq!(explanation["send_state"], "NOT_STARTED_IN_SQL");
    assert_eq!(explanation["reservation_state"], "HELD");
    assert_eq!(
        explanation["durable_receipt"]["intent_id"],
        original.intent_id
    );
    assert_eq!(*feed.requests.lock().unwrap(), reads_before);
    let cold = service
        .trade(&config.alias, &request, Some(&authority))
        .await
        .unwrap();
    assert_eq!(
        cold["data"]["intent"]["intent_id"], original.intent_id,
        "{cold}"
    );
    assert_eq!(cold["data"]["intent"]["plan_hash"], original.plan_hash);
    assert_eq!(
        cold["data"]["intent"]["client_order_id"],
        original.client_order_id
    );
    assert_eq!(cold["data"]["intent"]["state"], "RESERVED");
    assert_eq!(cold["data"]["remaining_exposure"], "not_evaluated");
    assert_eq!(*feed.requests.lock().unwrap(), reads_before);
    assert_eq!(
        owner.campaign_summary(campaign.clone()).await.unwrap(),
        budget
    );
    let retained = owner
        .reservation_receipt(
            key.clone(),
            original.intent_id.clone(),
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
    assert_eq!(account_catalog_proto::load(&root).unwrap(), catalog);
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(feed.unconsumed_reads(), 0);
    assert_eq!(feed.unconsumed_posts(), 0);
    assert_eq!(feed.unconsumed_public_clock(), 0);
    assert!(feed.denied_routes.lock().unwrap().is_empty());
    // An explicit public Cancel can complete recovery of this proven unposted
    // intent without a clock, venue cancel or unrelated financial source read.
    let cancel = TradeRequest::Cancel {
        idempotency_key: "native_cancel_unposted_after_reopen".into(),
        reference: original.intent_id.clone(),
    };
    let cancelled = service.trade(&config.alias, &cancel, None).await.unwrap();
    assert_eq!(
        cancelled["data"]["primary_reason"], "USER_CANCELLED_UNSENT",
        "{cancelled}"
    );
    assert_eq!(
        cancelled["data"]["central_release"]["state"], "verified",
        "{cancelled}"
    );
    assert_eq!(cancelled["data"]["intent"]["state"], "REJECTED");
    assert_eq!(cancelled["data"]["send_state"], "NOT_STARTED");
    let released = owner.campaign_summary(campaign.clone()).await.unwrap();
    assert_eq!(released.reserved_entry_usd, "0");
    assert_eq!(released.reserved_loss_usd, "0");
    assert_eq!(released.global_reserved_usdt, "0");
    assert_eq!(released.consumed_entry_usd, "0");
    assert_eq!(released.remaining_entry_usd, "10");
    assert_eq!(released.opening, budget.opening);
    let replay = service.trade(&config.alias, &cancel, None).await.unwrap();
    assert_eq!(replay["data"]["central_release"]["state"], "verified");
    assert_eq!(owner.campaign_summary(campaign).await.unwrap(), released);
    assert_eq!(*feed.requests.lock().unwrap(), reads_before);
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    drop(source_timer);
    drop(reconnected);
    registry.shutdown().await;
    let mut closed = owner.termination_signal();
    drop(service);
    drop(registry);
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    assert!(!root.join("accounts").exists());
    assert!(!root.join("risk").exists());
    assert_eq!(
        root.parent().unwrap().canonicalize().unwrap(),
        std::env::temp_dir().canonicalize().unwrap()
    );
    assert!(
        root.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("okx-native-trade-")
    );
    std::fs::remove_dir_all(root).unwrap();
}
