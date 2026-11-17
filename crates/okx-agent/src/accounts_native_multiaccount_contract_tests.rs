//! Two verified Native accounts on the original installation owner. Capital
//! preparation is tested separately from unproven shared-cap trading admission.
use super::*;
use okx_protocol::trading::{CapitalDirection, StrategyRequest};
use rust_decimal::Decimal;

const INSTANCE: &str = "native-cohort-instance";
const CREATE: &str = "native_cohort_same_create";
const ASSIGN: &str = "native_cohort_same_assignment";

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn create(budget: &str) -> StrategyRequest {
    StrategyRequest::Create {
        idempotency_key: CREATE.into(),
        instance_id: INSTANCE.into(),
        definition_id: "ema_cross_v1".into(),
        version: 1,
        parameters: json!({"fast":10,"slow":20}),
        instruments: vec!["BTC-USDT-SWAP".into()],
        timeframe_ms: 60_000,
        budget_usd: budget.into(),
    }
}

fn assign(operation: &str, amount: &str, revision: u64) -> StrategyRequest {
    StrategyRequest::CapitalAssign {
        idempotency_key: operation.into(),
        instance_id: INSTANCE.into(),
        direction: CapitalDirection::Assign,
        amount_usd: amount.into(),
        expected_authority_revision: revision,
    }
}

async fn capture(account: &AccountResources) {
    crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await
    .expect("original signed account source capture on the installation owner");
}

#[tokio::test]
async fn risk_book_contract_native_two_accounts_share_budget_and_isolate_public_capital_receipts() {
    let fixture = fixture_for_accounts(&["a", "b"]).await;
    let a = fixture.registry.resolve("fixture-a").await.unwrap();
    let b = fixture.registry.resolve("fixture-b").await.unwrap();
    let key_a = a.configuration.account_key.wire();
    let key_b = b.configuration.account_key.wire();
    assert_ne!(key_a, key_b);
    assert!(a.source_identity_ready().await);
    assert!(b.source_identity_ready().await);
    let mut expected = vec![key_a.clone(), key_b.clone()];
    expected.sort();
    assert_eq!(
        fixture
            .owner
            .connected_account_keys(deadline())
            .await
            .unwrap(),
        expected
    );
    let installation = fixture
        .owner
        .installation_receipt(deadline())
        .await
        .unwrap();
    assert_eq!(
        installation.installation_id,
        fixture.owner.installation_id()
    );
    assert_eq!(installation.schema_version, 8);
    assert_eq!(
        fixture.owner.configured_campaign(deadline()).await.unwrap(),
        Some(fixture.campaign.clone())
    );
    for account in [&a, &b] {
        let (owner, scope) = account.execution.store.scope();
        assert_eq!(owner.installation_id(), fixture.owner.installation_id());
        assert_eq!(scope, account.configuration.account_key.wire());
        let policy = owner
            .account_policy(scope.into(), deadline())
            .await
            .unwrap();
        assert_eq!(policy.campaign_id, fixture.campaign);
        assert_eq!(policy.revision as u64, fixture.authority.state.revision);
    }

    // Timers cannot consume a named mutation's observation. Explicit captures
    // below still use the real parser/owner; neither lease asserts freshness.
    let timer_a = a.recorded_capture_gate.write().await;
    let timer_b = b.recorded_capture_gate.write().await;
    let create_a = create("2");
    let create_b = create("3");
    let (created_a, created_b) = tokio::join!(
        fixture
            .trade
            .strategies("fixture-a", &create_a, Some(&fixture.authority)),
        fixture
            .trade
            .strategies("fixture-b", &create_b, Some(&fixture.authority)),
    );
    let created_a = created_a.expect("public Create A");
    let created_b = created_b.expect("same-key public Create B is account-scoped");
    assert_eq!(created_a["data"]["instance"]["instance_id"], INSTANCE);
    assert_eq!(created_b["data"]["instance"]["instance_id"], INSTANCE);
    assert_ne!(
        created_a["data"]["instance"]["budget"],
        created_b["data"]["instance"]["budget"]
    );
    let receipt_a = fixture
        .owner
        .operation(deadline(), key_a.clone(), CREATE.into())
        .await
        .unwrap()
        .unwrap();
    let receipt_b = fixture
        .owner
        .operation(deadline(), key_b.clone(), CREATE.into())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(receipt_a.request_hash, receipt_b.request_hash);
    assert_ne!(receipt_a.result_ref, receipt_b.result_ref);

    let baseline = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(baseline.loss_limit_usd, "10");
    assert_eq!(baseline.entry_limit_usd, "10");
    assert_eq!(baseline.reserved_entry_usd, "0");
    assert_eq!(baseline.global_reserved_usdt, "0");
    assert!(
        baseline.opening.is_none(),
        "preparation is not a trading opening"
    );
    let preflight = TradeRequest::Preflight {};
    let (preflight_a, preflight_b) = tokio::join!(
        fixture.trade.trade("fixture-a", &preflight, None),
        fixture.trade.trade("fixture-b", &preflight, None),
    );
    let preflight_a = preflight_a.unwrap();
    let preflight_b = preflight_b.unwrap();
    assert!(preflight_a["central_budget"].is_object());
    assert_eq!(preflight_a["central_budget"], preflight_b["central_budget"]);
    assert_eq!(preflight_a["account"], key_a);
    assert_eq!(preflight_b["account"], key_b);

    tokio::join!(capture(&a), capture(&b));
    let assign_a = assign(ASSIGN, "10", fixture.authority.state.revision);
    let assign_b = assign(ASSIGN, "20", fixture.authority.state.revision);
    // Same operation ID, different actual account-backed principal. The
    // production writer conserves each account's own two assignment legs.
    let (assigned_a, assigned_b) = tokio::join!(
        fixture
            .trade
            .strategies("fixture-a", &assign_a, Some(&fixture.authority)),
        fixture
            .trade
            .strategies("fixture-b", &assign_b, Some(&fixture.authority)),
    );
    let assigned_a = assigned_a.expect("public capital assignment A");
    let assigned_b = assigned_b.expect("public capital assignment B");
    assert_eq!(assigned_a["data"]["capital_coverage"], "COMPLETE");
    assert_eq!(assigned_b["data"]["capital_coverage"], "COMPLETE");
    let now = chrono::Utc::now().timestamp_millis();
    let capital_a = fixture
        .owner
        .capital_state(key_a.clone(), now, deadline())
        .await
        .unwrap();
    let capital_b = fixture
        .owner
        .capital_state(key_b.clone(), now, deadline())
        .await
        .unwrap();
    for (capital, principal, residual) in [(&capital_a, 10, 90), (&capital_b, 20, 80)] {
        assert_eq!(capital.account_nav_usd, Decimal::from(100));
        assert_eq!(capital.assigned_usd, Decimal::from(principal));
        assert_eq!(
            capital.strategy_principal_usd[INSTANCE],
            Decimal::from(principal)
        );
        assert_eq!(capital.unattributed_residual_usd, Decimal::from(residual));
        assert_eq!(capital.coverage, "COMPLETE");
    }
    let assigned_receipt_a = fixture
        .owner
        .operation(deadline(), key_a.clone(), ASSIGN.into())
        .await
        .unwrap()
        .unwrap();
    let assigned_receipt_b = fixture
        .owner
        .operation(deadline(), key_b.clone(), ASSIGN.into())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        assigned_receipt_a.request_hash,
        assigned_receipt_b.request_hash
    );
    assert_ne!(assigned_receipt_a.result_ref, assigned_receipt_b.result_ref);
    assert_eq!(
        fixture
            .trade
            .strategies("fixture-b", &assign_b, Some(&fixture.authority))
            .await
            .unwrap()["data"],
        assigned_b["data"],
        "exact B replay cannot add principal or select A's same-key receipt"
    );
    let conflict = assign(ASSIGN, "21", fixture.authority.state.revision);
    assert_eq!(
        fixture
            .trade
            .strategies("fixture-b", &conflict, Some(&fixture.authority))
            .await
            .unwrap_err(),
        "IDEMPOTENCY_CONFLICT"
    );

    // Shut down A's actual private source owner. The disconnected identity is
    // observed from production WS state, never written into readiness atomics.
    drop(timer_a);
    a.stop_sources().await;
    assert!(!a.source_identity_ready().await);
    assert!(b.source_identity_ready().await);
    capture(&b).await;
    let blocked_a = fixture
        .trade
        .trade("fixture-a", &TradeRequest::Preflight {}, None)
        .await
        .unwrap();
    assert_eq!(blocked_a["identity_verified"], false);
    assert!(
        blocked_a["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "WAITING_ACCOUNT_EVIDENCE")
    );
    let extra_b = assign(
        "native_cohort_b_after_a_disconnect",
        "1",
        fixture.authority.state.revision,
    );
    let after = fixture
        .trade
        .strategies("fixture-b", &extra_b, Some(&fixture.authority))
        .await
        .expect("B public capital preparation progresses while A's source is disconnected");
    assert_eq!(after["data"]["capital_coverage"], "COMPLETE");
    let capital_b = fixture
        .owner
        .capital_state(
            key_b.clone(),
            chrono::Utc::now().timestamp_millis(),
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(capital_b.assigned_usd, Decimal::from(21));
    assert_eq!(capital_b.unattributed_residual_usd, Decimal::from(79));
    assert_eq!(
        fixture
            .owner
            .operation(deadline(), key_a, ASSIGN.into())
            .await
            .unwrap(),
        Some(assigned_receipt_a)
    );
    assert!(
        fixture
            .owner
            .operation(
                deadline(),
                a.configuration.account_key.wire(),
                "native_cohort_b_after_a_disconnect".into()
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
    assert!(fixture.feed.outbound_posts.lock().unwrap().is_empty());
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert!(!fixture.root.join("accounts").exists());
    assert!(!fixture.root.join("risk").exists());
    drop(timer_b);
    drop(a);
    drop(b);
    fixture.finish().await;
}
