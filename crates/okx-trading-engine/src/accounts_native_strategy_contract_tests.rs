//! Public Native strategy caller contract on the same production finance owner.
use super::*;
use okx_ipc_contracts::trading::StrategyRequest;

const INSTANCE: &str = "native-strategy-contract";
fn create() -> StrategyRequest {
    StrategyRequest::Create {
        idempotency_key: "native_strategy_create".into(),
        instance_id: INSTANCE.into(),
        definition_id: "ema_cross_v1".into(),
        version: 1,
        parameters: json!({"fast":10,"slow":20}),
        instruments: vec!["BTC-USDT-SWAP".into()],
        timeframe_ms: 60_000,
        budget_usd: "2".into(),
    }
}
async fn request(fixture: &NativeFixture, request: &StrategyRequest) -> Result<Value, String> {
    assert!(request.valid());
    fixture
        .trade
        .strategies(&fixture.config.alias, request, Some(&fixture.authority))
        .await
}
#[tokio::test]
async fn risk_book_contract_native_public_strategies_crud_claim_no_trade_share_original_owner() {
    let fixture = fixture().await;
    let account = fixture._account.clone();
    let timer_capture = account.recorded_capture_gate.write().await;
    let deadline = || Instant::now() + Duration::from_secs(3);
    let key = fixture.config.account_key.wire();
    let baseline = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert!(baseline.opening.is_none());
    assert!(
        fixture
            .owner
            .verify_live_authority(deadline())
            .await
            .is_err()
    );
    assert_eq!(
        account.execution.store.scope().0.installation_id(),
        fixture.owner.installation_id()
    );

    let created = request(&fixture, &create())
        .await
        .expect("public Native Create")["data"]["instance"]
        .clone();
    assert_eq!(created["policy_revision"], 1);
    assert_eq!(created["enabled"], false);
    assert_eq!(
        request(&fixture, &create()).await.unwrap()["data"]["instance"],
        created
    );
    let version = created["definition_version"].as_str().unwrap().to_owned();
    let listed = request(&fixture, &StrategyRequest::List {}).await.unwrap();
    assert!(
        listed["data"]["instances"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["instance_id"] == INSTANCE)
    );
    let status = StrategyRequest::Status {
        instance_id: INSTANCE.into(),
    };
    let initial_status = request(&fixture, &status)
        .await
        .expect("public Native Status");
    assert_eq!(initial_status["data"]["instance"], created);
    assert_eq!(initial_status["data"]["definition"]["version"], version);
    assert_eq!(initial_status["data"]["positions"], json!([]));
    let mut conflict = create();
    if let StrategyRequest::Create { budget_usd, .. } = &mut conflict {
        *budget_usd = "3".into();
    }
    assert_eq!(
        request(&fixture, &conflict).await.unwrap_err(),
        "IDEMPOTENCY_CONFLICT"
    );

    let start = StrategyRequest::Start {
        idempotency_key: "native_strategy_start".into(),
        instance_id: INSTANCE.into(),
        expected_revision: 1,
        configuration_digest: initial_status["data"]["configuration_digest"]
            .as_str()
            .unwrap()
            .into(),
    };
    let started = request(&fixture, &start)
        .await
        .expect("public Native Start")["data"]["instance"]
        .clone();
    assert_eq!(started["enabled"], true);
    assert_eq!(started["policy_revision"], 2);
    assert_eq!(started["definition_version"], version);
    let active_restore = StrategyRequest::RestoreConfiguration {
        idempotency_key: "native_strategy_restore_active".into(),
        instance_id: INSTANCE.into(),
        expected_revision: 2,
        source_revision: 1,
    };
    assert_eq!(
        request(&fixture, &active_restore).await.unwrap_err(),
        "STRATEGY_RESTORE_REQUIRES_STOPPED"
    );
    assert_eq!(
        request(&fixture, &start).await.unwrap()["data"]["instance"],
        started
    );

    // These are the exact typed calls used by automatic strategy_turn, after
    // the public lifecycle has bound its immutable definition in the same SQL.
    let claim = okx_trade_execution::account_worker::StrategyDecisionClaimCommand {
        instance_id: INSTANCE.into(),
        decision_key: "native-strategy-bar".into(),
        input_digest: "closed-bars-inputs".into(),
        policy_revision: i64::try_from(fixture.authority.state.revision).unwrap(),
        expected_version: version.clone(),
        activation_revision: 2,
        time: jiff::Timestamp::now().as_millisecond(),
    };
    let decision = account
        .execution
        .store
        .claim_strategy_decision(deadline(), claim.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        account
            .execution
            .store
            .claim_strategy_decision(deadline(), claim.clone())
            .await
            .unwrap(),
        Some(decision.clone())
    );
    account
        .execution
        .store
        .finish_strategy_no_trade(
            deadline(),
            INSTANCE.into(),
            claim.decision_key.clone(),
            version.clone(),
            jiff::Timestamp::now().as_millisecond(),
        )
        .await
        .unwrap();
    assert!(
        account
            .execution
            .store
            .claim_strategy_decision(deadline(), claim.clone())
            .await
            .unwrap()
            .is_none()
    );
    let terminal = fixture
        .owner
        .operation(
            deadline(),
            key.clone(),
            format!("strategy-no-trade:{decision}"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.kind, "strategy_no_trade");
    assert_eq!(terminal.request_hash, claim.input_digest);
    assert_eq!(terminal.result_ref, "NO_TRADE");
    assert_eq!(
        request(&fixture, &status).await.unwrap()["data"]["instance"]["last_decision_key"],
        claim.decision_key
    );

    let stop = StrategyRequest::Stop {
        idempotency_key: "native_strategy_stop".into(),
        instance_id: INSTANCE.into(),
    };
    let stopped =
        request(&fixture, &stop).await.expect("public Native Stop")["data"]["instance"].clone();
    assert_eq!(stopped["enabled"], false);
    assert_eq!(stopped["policy_revision"], 3);
    assert_eq!(stopped["definition_version"], version);
    assert_eq!(
        request(&fixture, &stop).await.unwrap()["data"]["instance"],
        stopped
    );
    let update = StrategyRequest::Update {
        idempotency_key: "native_strategy_update".into(),
        instance_id: INSTANCE.into(),
        expected_revision: 3,
        parameters: json!({"fast":5,"slow":30}),
        budget_usd: "3".into(),
        instruments: Some(vec!["ETH-USDT-SWAP".into()]),
        timeframe_ms: Some(300_000),
    };
    let updated = request(&fixture, &update)
        .await
        .expect("public Native Update")["data"]["instance"]
        .clone();
    assert_eq!(updated["enabled"], false);
    assert_eq!(updated["policy_revision"], 4);
    assert_ne!(updated["definition_version"], version);
    assert_eq!(
        request(&fixture, &update).await.unwrap()["data"]["instance"],
        updated
    );
    let mut stale = update.clone();
    if let StrategyRequest::Update {
        idempotency_key, ..
    } = &mut stale
    {
        *idempotency_key = "native_strategy_stale".into();
    }
    assert_eq!(
        request(&fixture, &stale).await.unwrap_err(),
        "INSTANCE_REVISION_CONFLICT"
    );
    // Replays return their original receipt even after later lifecycle changes.
    assert_eq!(
        request(&fixture, &start).await.unwrap()["data"]["instance"],
        started
    );
    let restore = StrategyRequest::RestoreConfiguration {
        idempotency_key: "native_strategy_restore".into(),
        instance_id: INSTANCE.into(),
        expected_revision: 4,
        source_revision: 1,
    };
    let restored_reply = request(&fixture, &restore).await.unwrap();
    let restored = &restored_reply["data"]["instance"];
    assert_eq!(restored["policy_revision"], 5);
    assert_eq!(restored["enabled"], false);
    for field in [
        "definition_id",
        "definition_version",
        "parameters",
        "budget",
    ] {
        assert_eq!(
            restored[field], created[field],
            "configuration field {field}"
        );
    }
    assert_eq!(restored["last_decision_key"], claim.decision_key);
    assert_eq!(
        restored_reply["data"]["configuration_digest"],
        initial_status["data"]["configuration_digest"]
    );
    assert_eq!(
        request(&fixture, &restore).await.unwrap()["data"],
        restored_reply["data"]
    );
    assert_eq!(
        restored_reply["data"]["configuration_restore_receipt"]["source_revision"],
        1
    );
    let mut stale_restore = restore.clone();
    if let StrategyRequest::RestoreConfiguration {
        idempotency_key, ..
    } = &mut stale_restore
    {
        *idempotency_key = "native_strategy_restore_stale".into();
    }
    assert_eq!(
        request(&fixture, &stale_restore).await.unwrap_err(),
        "INSTANCE_REVISION_CONFLICT"
    );

    let final_status = request(&fixture, &status).await.unwrap();
    assert_eq!(&final_status["data"]["instance"], restored);
    assert_eq!(
        final_status["data"]["definition"]["version"],
        restored["definition_version"]
    );
    let stored = fixture
        .owner
        .strategy_instances(deadline(), key)
        .await
        .unwrap()
        .into_iter()
        .find(|v| v.instance_id == INSTANCE)
        .unwrap();
    assert_eq!(
        stored.definition_version,
        restored["definition_version"].as_str().unwrap()
    );
    assert_eq!(stored.policy_revision, 5);
    let reactivation = StrategyRequest::Start {
        idempotency_key: "native_strategy_reactivate_restored".into(),
        instance_id: INSTANCE.into(),
        expected_revision: 5,
        configuration_digest: restored_reply["data"]["configuration_digest"]
            .as_str()
            .unwrap()
            .into(),
    };
    let reactivated = request(&fixture, &reactivation).await.unwrap();
    assert_eq!(reactivated["data"]["instance"]["policy_revision"], 6);
    assert_eq!(reactivated["data"]["instance"]["enabled"], true);
    assert!(
        account
            .execution
            .store
            .claim_strategy_decision(
                deadline(),
                okx_trade_execution::account_worker::StrategyDecisionClaimCommand {
                    activation_revision: 6,
                    ..claim
                },
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        baseline
    );
    assert!(
        fixture
            .owner
            .verify_live_authority(deadline())
            .await
            .is_err(),
        "strategy preparation must not manufacture a financial opening"
    );
    assert!(
        account
            .execution
            .store
            .reconcilable_intents_page(deadline(), 10, 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 0);
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}
