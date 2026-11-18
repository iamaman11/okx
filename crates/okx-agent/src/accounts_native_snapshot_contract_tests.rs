//! A restored original Native database cannot grant a new venue send before
//! the original storage audit proves its missing post-snapshot source history.
use super::*;
use okx_execution::finance_snapshot::{backup_stopped_finance, restore_stopped_finance};

#[tokio::test]
async fn risk_book_contract_native_restored_snapshot_blocks_public_execute_until_storage_reconciled()
 {
    let mut original = fixture().await;
    let (market_stop, market_task, _socket, public) =
        super::filled_fixture::opened_market(&mut original).await;
    let original_identity = original
        .owner
        .installation_receipt(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let original_budget = original
        .owner
        .campaign_summary(original.campaign.clone())
        .await
        .unwrap();
    let NativeFixture {
        root: source,
        registry,
        owner,
        _account,
        config,
        feed,
        market_boundary,
        _market,
        trade,
        authority,
        campaign,
    } = original;
    registry.shutdown().await;
    let mut closed = owner.termination_signal();
    drop(_account);
    drop(trade);
    drop(registry);
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    let snapshot = source.join("snapshot");
    let accounts = vec![config.account_key.wire()];
    let manifest = backup_stopped_finance(
        &source,
        &original_identity.installation_id,
        &accounts,
        &snapshot,
    )
    .unwrap();
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    let target = std::env::temp_dir().join(format!(
        "okx-native-snapshot-restore-{:032x}",
        u128::from_le_bytes(nonce)
    ));
    std::fs::create_dir(&target).unwrap();
    std::fs::create_dir(target.join("finance")).unwrap();
    // The verified existing catalog is supplied separately; the snapshot
    // contains finance only and never invents account or credential metadata.
    std::fs::copy(
        source.join(account_catalog_proto::CATALOG_FILE),
        target.join(account_catalog_proto::CATALOG_FILE),
    )
    .unwrap();
    restore_stopped_finance(
        &target,
        &original_identity.installation_id,
        &accounts,
        &snapshot,
    )
    .unwrap();
    let rate = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&target, rate.clone(), &campaign)
            .await
            .unwrap();
    let registry = Arc::new(registry);
    assert_eq!(
        owner
            .installation_receipt(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap(),
        original_identity
    );
    assert_eq!(
        owner.campaign_summary(campaign.clone()).await.unwrap(),
        original_budget
    );
    let (again, clients) = prepared_with_socket(
        "a",
        market_boundary.clone(),
        Arc::new(RecordedWsConnector::default()),
        rate,
    );
    assert_eq!(again, config);
    registry
        .reconnect_prepared(config.clone(), clients)
        .await
        .unwrap();
    let account = registry.resolve(&config.alias).await.unwrap();
    let trade = Arc::new(TradeService::new(
        registry.clone(),
        public,
        NativeRiskClient::local(NativeFinanceRisk::from_owner(owner.clone())),
    ));
    trade.update_governance(&authority).await.unwrap();
    let restored = NativeFixture {
        root: target,
        registry,
        owner,
        _account: account,
        config,
        feed,
        market_boundary,
        _market,
        trade,
        authority,
        campaign,
    };
    super::filled_fixture::capture(&restored).await;
    let gate = restored._account.recorded_capture_gate.write().await;
    // Reconnecting/capturing changes source generations. Re-run the exact
    // maintenance owner operation against those original captured rows; the
    // old archive readiness cannot attest this new generation. This does not
    // perform the separate storage-recovery audit or clear its durable flag.
    restored
        .owner
        .refresh_campaign_entry_readiness(
            Instant::now() + Duration::from_secs(3),
            restored.config.account_key.wire(),
            restored.campaign.clone(),
            original_budget.campaign_started_at_ms.unwrap() as i64,
            jiff::Timestamp::now()
                .as_millisecond()
                .saturating_sub(2_000),
        )
        .await
        .expect("original captured campaign readiness, not storage recovery");
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    restored
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    record_fee(&restored.feed, &now);
    record_leverage(&restored.feed);
    record_leverage(&restored.feed);
    record_fee(&restored.feed, &now);
    record_fee(&restored.feed, &now);
    record_leverage(&restored.feed);
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let result = restored
        .trade
        .trade(&restored.config.alias, &request, Some(&restored.authority))
        .await;
    let reason = match result {
        Err(reason) => reason,
        Ok(value) => value["reason_code"]
            .as_str()
            .or_else(|| value["data"]["primary_reason"].as_str())
            .unwrap_or("")
            .to_owned(),
    };
    assert_eq!(
        reason, "STORAGE_RECOVERY_INCOMPLETE",
        "restored original storage gate must refuse the actual public send boundary"
    );
    assert!(restored.feed.outbound_posts.lock().unwrap().is_empty());
    let audit = restored
        .owner
        .recovery_snapshot(
            Instant::now() + Duration::from_secs(3),
            restored.config.account_key.wire(),
        )
        .await
        .unwrap();
    assert!(audit.required);
    assert_eq!(audit.backup_at, Some(manifest.captured_at_ms));
    assert!(
        !audit.coverage_complete,
        "ordinary source capture is not a restore audit"
    );
    drop(gate);
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    restored.finish().await;
    std::fs::remove_dir_all(source).unwrap();
}
