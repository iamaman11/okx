//! Native source-frontier and cold diagnostic contracts on the original owner.
use super::*;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

#[tokio::test]
async fn campaign_readiness_requires_capture_of_new_source_frontier() {
    let fixture = fixture().await;
    let account = fixture._account.clone();
    let gate = account.recorded_capture_gate.write().await;
    let key = fixture.config.account_key.wire();
    let source = fixture
        .owner
        .source_capture_state(key.clone(), deadline())
        .await
        .unwrap();
    let cursors = [
        source.cursors.spot,
        source.cursors.margin,
        source.cursors.swap,
        source.cursors.futures,
        source.cursors.options,
        source.cursors.bills,
    ];
    let covered = cursors
        .into_iter()
        .map(|cursor| {
            let cursor = cursor.expect("all six monetary source frontiers");
            assert!(
                cursor.generation > 0
                    && cursor.cursor.is_empty()
                    && cursor.coverage_gaps.is_empty()
            );
            cursor.watermark
        })
        .min()
        .unwrap();
    let from = source.initialized_at;
    assert!(covered > from);
    fixture
        .owner
        .seal_captured_campaign_baseline(
            deadline(),
            key.clone(),
            fixture.campaign.clone(),
            from,
            covered,
        )
        .await
        .unwrap();
    let through = covered + 1;
    assert!(matches!(
        fixture
            .owner
            .refresh_campaign_entry_readiness(
                deadline(),
                key.clone(),
                fixture.campaign.clone(),
                from,
                through
            )
            .await,
        Err(okx_execution::StoreError::Invalid(
            "ENTRY_BASELINE_CAPTURE_PENDING"
        ))
    ));
    // The six complete old cursors above map the former formatted diagnostic's
    // COMPLETE state; their minimum is exactly one millisecond behind the
    // requested frontier. The typed readiness result supplies the pending reason.
    assert_eq!(through - covered, 1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while jiff::Timestamp::now().as_millisecond() <= covered {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("captured frontier must not lie in the future");
    super::filled_fixture::snapshot(&fixture.feed, "100", "0");
    super::filled_fixture::history(&fixture.feed, None);
    super::filled_fixture::capture(&fixture).await;
    fixture
        .owner
        .refresh_campaign_entry_readiness(
            deadline(),
            key.clone(),
            fixture.campaign.clone(),
            from,
            through,
        )
        .await
        .unwrap();
    fixture
        .owner
        .verify_campaign_entry_readiness(deadline(), key, fixture.campaign.clone(), from, through)
        .await
        .unwrap();
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    drop(gate);
    drop(account);
    fixture.finish().await;
}

#[tokio::test]
async fn recovery_prepared_account_reopens_catalog_and_original_sql() {
    let fixture = fixture().await;
    let (shutdown, stopped) = watch::channel(false);
    let service = fixture.trade.clone();
    let maintenance = tokio::spawn(async move { service.run(stopped).await });
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap()
                .opening
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("production maintenance establishes the original campaign opening");
    shutdown.send(true).unwrap();
    maintenance.await.unwrap();
    let key = fixture.config.account_key.wire();
    let definition = fixture
        .owner
        .ensure_manual_instance(deadline(), key.clone())
        .await
        .unwrap();
    fixture
        .owner
        .begin_operation(
            deadline(),
            key.clone(),
            "cleanup_restart_diagnostic".into(),
            "restart-diagnostic-hash".into(),
            "protection_cleanup".into(),
        )
        .await
        .unwrap();
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    fixture.registry.shutdown().await;
    let root = fixture.root.clone();
    let config = fixture.config.clone();
    let campaign = fixture.campaign.clone();
    let feed = fixture.feed.clone();
    let mut closed = fixture.owner.termination_signal();
    drop(fixture);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();

    let budget = RateBudget::new();
    let (reopened, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign)
            .await
            .unwrap();
    let (mut mismatch, clients) = prepared_with_socket(
        "a",
        feed.clone(),
        Arc::new(RecordedWsConnector::default()),
        budget.clone(),
    );
    mismatch.alias = "wrong-alias".into();
    assert!(
        reopened
            .reconnect_prepared(mismatch, clients)
            .await
            .is_err()
    );
    let (_, clients) = prepared_with_socket(
        "a",
        feed.clone(),
        Arc::new(RecordedWsConnector::default()),
        budget.clone(),
    );
    reopened
        .reconnect_prepared(config.clone(), clients)
        .await
        .unwrap();
    assert_eq!(reopened.list().await, vec![config.clone()]);
    let account = reopened.resolve("fixture-a").await.unwrap();
    let instances = owner
        .strategy_instances(deadline(), key.clone())
        .await
        .unwrap();
    assert!(
        instances
            .iter()
            .any(|instance| instance.instance_id == "manual"
                && instance.definition_id == definition.definition_id)
    );
    let projection = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(report) = account
                .protection_operation_trace
                .read("cleanup_restart_diagnostic")
                && report["state"] == "OBSERVED"
            {
                break report;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup restores the durable diagnostic projection");
    assert_eq!(projection["snapshot"]["durable_state"], "POSTING");
    assert_eq!(projection["snapshot"]["venue_ack"], "UNKNOWN_PRIOR_CALL");
    assert!(projection["snapshot"]["parent_invocation"].is_null());
    assert!(projection["snapshot"]["intent_id"].is_null());
    assert_eq!(projection["trace_health"]["history_complete"], "UNKNOWN");
    let (_, clients) = prepared_with_socket(
        "a",
        feed.clone(),
        Arc::new(RecordedWsConnector::default()),
        budget,
    );
    assert!(reopened.reconnect_prepared(config, clients).await.is_err());
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    reopened.shutdown().await;
    let mut closed = owner.termination_signal();
    drop(account);
    drop(reopened);
    drop(owner);
    tokio::time::timeout(Duration::from_secs(3), closed.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
