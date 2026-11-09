//! Direct owner gates used by production terminal release. These do not claim
//! to replace the full cohort/Worker census evidence in T09/T10.
use super::*;
use okx_execution::{AccountMeta, OpeningSnapshot};

#[test]
fn terminal_coverage_requires_all_frontiers_without_refreshing_source() {
    let complete = SourceCursor {
        source: "fills".into(),
        stream_partition: "SWAP".into(),
        cursor: String::new(),
        watermark: 100,
        generation: 1,
        coverage_gaps: String::new(),
    };
    assert!(source_covers_terminal(&complete, 100));
    assert!(source_covers_terminal(&complete, 99));
    assert!(!source_covers_terminal(&complete, 101));
    assert!(!source_covers_terminal(
        &SourceCursor {
            generation: 0,
            ..complete.clone()
        },
        100
    ));
    assert!(!source_covers_terminal(
        &SourceCursor {
            cursor: "next-page".into(),
            ..complete.clone()
        },
        100
    ));
    assert!(!source_covers_terminal(
        &SourceCursor {
            coverage_gaps: "missing-page".into(),
            ..complete
        },
        100
    ));
}

#[tokio::test]
async fn missing_real_sql_reservation_denies_opening_but_not_reduction() {
    let root = std::env::temp_dir().join(format!(
        "okx-terminal-gate-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let store = AccountStoreHandle::initialize_flat(
        root.join("account.db"),
        AccountMeta {
            account_key: format!("okx:real:{}", "a".repeat(64)),
            installation_id: "terminal_test".into(),
            initialized_at: 1,
            opening_source: "terminal_fixture".into(),
            initialize_operation_id: "initialize".into(),
            release_id: "a".repeat(40),
        },
        OpeningSnapshot {
            source_ref: "terminal_fixture".into(),
            observed_at: 1,
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
    assert_eq!(
        terminal_receipt(&store, "missing", true).await.unwrap_err(),
        "TERMINAL_CENTRAL_RESERVATION_UNVERIFIED"
    );
    assert!(
        terminal_receipt(&store, "missing", false)
            .await
            .unwrap()
            .is_none()
    );
    // Neither read can manufacture the missing grant.
    assert!(
        store
            .call(deadline(), |s| s.central_receipt("missing"))
            .await
            .unwrap()
            .is_none()
    );
    let mut terminated = store.termination_signal();
    drop(store);
    if !*terminated.borrow() {
        tokio::time::timeout(std::time::Duration::from_secs(3), terminated.changed())
            .await
            .unwrap()
            .unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn typed_error_serializes_stage_identity_and_original_reason() {
    let failure = TerminalReleaseError {
        stage: TerminalReleaseStage::Census,
        reason_code: "COHORT_FINANCIAL_SOURCE_INCOMPLETE".into(),
        account: "okx:real:account-a".into(),
        intent_id: "intent-a".into(),
        intent_revision: 7,
        reservation_key: Some("reservation-a".into()),
    };
    let diagnostic = serde_json::to_value(failure).unwrap();
    assert_eq!(diagnostic["stage"], "census");
    assert_eq!(
        diagnostic["reason_code"],
        "COHORT_FINANCIAL_SOURCE_INCOMPLETE"
    );
    assert_eq!(diagnostic["account"], "okx:real:account-a");
    assert_eq!(diagnostic["intent_revision"], 7);
    assert_eq!(diagnostic["reservation_key"], "reservation-a");
}

/// Exercise the production orchestrator, rather than just its cursor predicate.
/// A canceled zero-fill opening has no lots to manufacture or close. Its real
/// SQL reservation must remain pending when fills do not cover venue uTime.
#[tokio::test]
async fn stale_fill_coverage_blocks_release_before_worker() {
    stale_terminal_coverage_blocks_release(false).await;
}

/// The native risk channel is a scripted transport seam, not a real Worker
/// economics proof. INSPECT succeeds, but stale bills must prevent SETTLE_ENTRY
/// and RELEASE without changing the real account SQL owner on either attempt.
#[tokio::test]
async fn stale_bill_coverage_blocks_settlement_after_inspect() {
    stale_terminal_coverage_blocks_release(true).await;
}

async fn stale_terminal_coverage_blocks_release(stale_bills: bool) {
    use crate::accounts::{AccountRuntimeRegistry, initialize_installation, recorded_support};
    use crate::risk_bridge::NativeRiskClient;
    use okx_execution::{Decision, IngestPage, IntentPlan, NewIntent};
    use okx_protocol::account_governance::CentralGovernanceSnapshot;
    use okx_protocol::risk_authority::{RISK_RESULT_SCHEMA_V1, RiskResult};
    use serde_json::json;
    use std::sync::Arc;

    const ID: &str = "intent_terminal_stale_fill";
    const KEY: &str = "reservation_terminal_stale_fill";
    const ORDER: &str = "terminal_stale_fill_order";
    const INSTALLATION: &str = "terminal-stale-fill-installation";
    const TERMINAL_AT: i64 = 100;

    let root = std::env::temp_dir().join(format!(
        "okx-terminal-stale-fill-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    ));
    initialize_installation(&root, INSTALLATION).unwrap();
    let budget = okx_api::RateBudget::new();
    let accounts = Arc::new(
        AccountRuntimeRegistry::open(&root, budget.clone())
            .await
            .unwrap(),
    );
    let feed = Arc::new(recorded_support::RecordedAccountHttp::default());
    let (config, clients) = recorded_support::prepared("a", feed.clone(), budget.clone());
    accounts
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap();
    let resources = accounts.resolve(&config.account_key.wire()).await.unwrap();
    // Pause only the periodic test source consumer; the actual SQL owner and
    // release operation remain active. Bootstrap requests precede the oracle.
    let capture = resources.recorded_capture_gate.write().await;
    let account = config.account_key.wire();
    let receipt_account = account.clone();
    let intent = resources
        .execution
        .store
        .call(deadline(), move |s| {
            s.apply_authority_policy(1, false, "campaign_fixture", Decimal::from(20))?;
            let definition = s.ensure_manual_instance(1)?;
            let saved = s.prepare(
                &Decision {
                    decision_id: "decision_terminal_stale_fill".into(),
                    instance_id: None,
                    decision_at: 2,
                    input_digest: "b".repeat(64),
                    policy_revision: 1,
                    code_version: "fixture".into(),
                },
                &NewIntent {
                    intent_id: ID.into(),
                    idempotency_key: "command_terminal_stale_fill".into(),
                    request_hash: "b".repeat(64),
                    client_order_id: "c".repeat(32),
                    deadline: 1_000,
                    plan_hash: "d".repeat(64),
                    policy_revision: 1,
                    risk_increase: true,
                    instrument_id: "BTC-USDT-SWAP".into(),
                    plan: IntentPlan {
                        strategy_definition_id: definition.definition_id,
                        strategy_definition_version: definition.version,
                        strategy_parameters_digest: definition.parameters_digest,
                        operation: "open".into(),
                        side: "buy".into(),
                        position_side: "long".into(),
                        quantity: Decimal::ONE,
                        limit_price: Decimal::from(5),
                        stop_price: Some(Decimal::from(4)),
                        strategy_instance: "manual".into(),
                        planned_loss_usd: Decimal::ONE,
                        source_generation: 1,
                        private_source_generation: 1,
                        contract_value: Decimal::ONE,
                        inverse: false,
                        settlement_currency: "USDT".into(),
                    },
                },
            )?;
            s.reserve(
                &CentralReceipt {
                    reservation_key: KEY.into(),
                    account_key: receipt_account,
                    installation_id: INSTALLATION.into(),
                    instance_id: None,
                    intent_id: ID.into(),
                    plan_hash: saved.plan_hash,
                    policy_revision: 1,
                    receipt_hash: "e".repeat(64),
                    requested_loss_usd: Decimal::ONE,
                    conversion_digest: "f".repeat(64),
                },
                3,
            )?;
            assert!(s.begin_post(ID, 1, 4)?);
            s.record_order_readback(
                ID,
                ORDER,
                IntentState::Cancelled,
                Decimal::ZERO,
                TERMINAL_AT,
            )?;
            s.ingest_page(&IngestPage {
                cursor: Some(SourceCursor {
                    source: "fills".into(),
                    stream_partition: "SWAP".into(),
                    cursor: String::new(),
                    watermark: if stale_bills {
                        TERMINAL_AT
                    } else {
                        TERMINAL_AT - 1
                    },
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })?;
            if stale_bills {
                s.ingest_page(&IngestPage {
                    cursor: Some(SourceCursor {
                        source: "bills".into(),
                        stream_partition: "account".into(),
                        cursor: String::new(),
                        watermark: TERMINAL_AT - 1,
                        generation: 1,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                })?;
            }
            Ok(s.intent(ID)?.unwrap())
        })
        .await
        .unwrap();
    assert_eq!(intent.state, IntentState::Cancelled);
    let order = okx_api::TradeOrderDetails {
        instrument_id: intent.instrument_id.clone(),
        order_id: ORDER.into(),
        client_order_id: intent.client_order_id.clone(),
        side: intent.plan.side.clone(),
        position_side: intent.plan.position_side.clone(),
        trade_mode: "cross".into(),
        order_type: "limit".into(),
        price: "5".into(),
        size: "1".into(),
        accumulated_fill_size: "0".into(),
        average_fill_price: "".into(),
        state: "canceled".into(),
        creation_time_ms: "4".into(),
        update_time_ms: TERMINAL_AT.to_string(),
    };
    assert_eq!(
        verified_terminal_order_update_time(&intent, Some(&order)).unwrap(),
        TERMINAL_AT
    );
    let authority: CentralGovernanceSnapshot = serde_json::from_value(json!({
        "state":{"schema":"okx.governance/v3","revision":1,"installation_id":INSTALLATION,
            "campaign_id":format!("sha256:{}", "a".repeat(64)),"campaign_loss_limit_usd":"20",
            "max_total_modelled_loss_usdt":"5","accounts":[]},
        "issued_at_ms":1792910000000_u64,"session_generation":673
    }))
    .unwrap();
    assert!(authority.valid());
    let reference = okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap();
    let (_public_coordinator, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
        okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
        reference,
        budget.clone(),
        Arc::new(recorded_support::OfflineSocket),
    );
    let (risk_tx, mut risk_requests) = tokio::sync::mpsc::channel(4);
    let service = Arc::new(TradeService::new(
        accounts.clone(),
        public_ws.clone(),
        NativeRiskClient::new(risk_tx),
    ));
    *service.governance.write().await = Some(authority);

    let before = terminal_coverage_sql_snapshot(&resources.execution.store).await;
    assert!(before["receipt"].is_object());
    assert!(before["active_receipt"].is_object());
    assert_eq!(before["receipt"]["reservation_key"], KEY);
    assert_eq!(before["receipt"]["account_key"], account);
    assert_eq!(before["receipt"]["intent_id"], ID);
    assert_eq!(before["receipt"]["plan_hash"], intent.plan_hash);
    assert_eq!(before["active_receipt"], before["receipt"]);
    assert_eq!(before["lots"], json!([]));
    assert_eq!(before["finance"], json!([]));
    assert_eq!(
        before["fill_cursor"]["watermark"],
        if stale_bills {
            TERMINAL_AT
        } else {
            TERMINAL_AT - 1
        }
    );
    if stale_bills {
        assert_eq!(before["bill_cursor"]["watermark"], TERMINAL_AT - 1);
    }
    assert_eq!(before["settlement"], serde_json::Value::Null);
    assert_eq!(before["release"], serde_json::Value::Null);
    let requests_before = feed.requests.lock().unwrap().clone();
    let posts_before = feed.outbound_posts.lock().unwrap().clone();
    assert!(posts_before.is_empty());
    let mut inspections = 0;
    for _ in 0..2 {
        let release = service.release_terminal(
            &resources,
            &intent,
            Some(&order),
            true,
            Some(crate::diagnostics::ProtectionCleanupInvocation {
                request_id: "terminal-status-source",
                origin: "agent_request.trade.status",
            }),
        );
        let failure = if stale_bills {
            let reply = async {
                let request =
                    tokio::time::timeout(std::time::Duration::from_secs(3), risk_requests.recv())
                        .await
                        .unwrap()
                        .unwrap();
                let command = &request.command;
                assert!(command.valid());
                assert_eq!(command.installation_id, INSTALLATION);
                assert_eq!(
                    command.command_id,
                    format!("inspect_{}", intent.client_order_id)
                );
                match &command.action {
                    RiskCommandAction::Inspect {
                        reservation_key,
                        plan_sha256,
                    } => {
                        assert_eq!(reservation_key, KEY);
                        assert_eq!(plan_sha256, &intent.plan_hash);
                    }
                    _ => panic!("stale bills must not settle or release the reservation"),
                }
                inspections += 1;
                let response = RiskResult {
                    schema: RISK_RESULT_SCHEMA_V1.into(),
                    command_id: command.command_id.clone(),
                    status: RiskDecisionStatus::Observed,
                    reason: None,
                    reservation_key: Some(KEY.into()),
                    risk_book_sequence: Some(7),
                    policy_revision: Some(1),
                    existing_modelled_loss_usdt: None,
                    reservation_state: Some(RiskReservationState::Posted),
                    budget: None,
                    account_budget: None,
                };
                assert!(response.valid());
                request.return_to.send(response).unwrap();
            };
            let (result, ()) = tokio::join!(release, reply);
            result.unwrap_err()
        } else {
            release.await.unwrap_err()
        };
        assert_eq!(
            failure.stage,
            if stale_bills {
                TerminalReleaseStage::BillCoverage
            } else {
                TerminalReleaseStage::FillCoverage
            }
        );
        assert_eq!(
            failure.reason_code,
            if stale_bills {
                "FINANCIAL_COVERAGE_INCOMPLETE"
            } else {
                "FILL_COVERAGE_INCOMPLETE"
            }
        );
        assert_eq!(failure.account, account);
        assert_eq!(failure.intent_id, intent.intent_id);
        assert_eq!(failure.intent_revision, intent.revision);
        assert_eq!(failure.reservation_key.as_deref(), Some(KEY));
        assert!(matches!(
            risk_requests.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(*feed.requests.lock().unwrap(), requests_before);
        assert_eq!(*feed.outbound_posts.lock().unwrap(), posts_before);
        assert_eq!(
            terminal_coverage_sql_snapshot(&resources.execution.store).await,
            before
        );
    }
    assert_eq!(inspections, if stale_bills { 2 } else { 0 });

    // Consume the actual production trace through the public dispatcher while
    // its SQL owner and source owners are unavailable. No synthetic trace seed.
    let market = crate::market_bootstrap::MarketBootstrapper::new(
        okx_api::OkxPublicClient::with_http_transport(
            okx_api::OkxEnvironment::new(okx_api::Region::Global, false),
            budget,
            feed.clone(),
        )
        .unwrap(),
    );
    let ctx = crate::query::DispatchContext {
        accounts: accounts.clone(),
        analytics: crate::analytics::AnalyticsService::new(root.join("analytics"), market.clone())
            .unwrap(),
        public_ws,
        trade: service.clone(),
        market,
    };
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let store = resources.execution.store.clone();
    let blocked_sql = tokio::spawn(async move {
        store
            .call(deadline(), move |_| {
                entered_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap();
                Ok(())
            })
            .await
    });
    entered_rx.await.unwrap();
    let source_snapshot = resources.latest_snapshot.write().await;
    let source_health = resources.source_capture_health.write().await;
    let sql_before = resources.execution.store.sql_timing().observed_jobs;
    let denied_before = feed.denied_routes.lock().unwrap().clone();
    let operation_reference = format!("terminal_release:{ID}");
    let request = okx_protocol::AgentRequest {
        schema: okx_protocol::AGENT_REQUEST_SCHEMA_V1.into(),
        request_id: "terminal-release-diagnostic".into(),
        account: Some(account.clone()),
        operation: okx_protocol::AgentOperation::Diagnostics {
            request: okx_protocol::trading::DiagnosticsRequest {
                account: Some(account.clone()),
                reference: Some(operation_reference.clone()),
                view: Some(okx_protocol::trading::DiagnosticsView::Operation),
                cycle_id: None,
                limit: None,
            },
        },
    };
    request.validate().unwrap();
    let response = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        crate::query::dispatch(&ctx, &request),
    )
    .await
    .expect("terminal operation diagnostics must not wait for SQL or source owners")
    .result
    .unwrap();
    assert_eq!(response["family"], "terminal_release");
    assert_eq!(response["state"], "OBSERVED");
    assert_eq!(response["account"], account);
    assert_eq!(response["reference"], operation_reference);
    assert_eq!(response["read_only"], true);
    assert_eq!(response["central_budget"]["state"], "NOT_EVALUATED");
    let operation = &response["operation"];
    let snapshot = &operation["snapshot"];
    assert_eq!(snapshot["intent_id"], ID);
    assert_eq!(snapshot["intent_revision"], intent.revision);
    assert_eq!(snapshot["reservation_key"], KEY);
    assert_eq!(snapshot["attempt"], 2);
    assert_eq!(snapshot["outcome"], "blocked");
    let (stage, reason, required) = if stale_bills {
        (
            "bill_coverage",
            "FINANCIAL_COVERAGE_INCOMPLETE",
            "complete_bills_covering_terminal_time",
        )
    } else {
        (
            "fill_coverage",
            "FILL_COVERAGE_INCOMPLETE",
            "complete_fills_covering_terminal_time",
        )
    };
    assert_eq!(snapshot["stage"], stage);
    assert_eq!(snapshot["reason_code"], reason);
    assert_eq!(snapshot["first_observed_blocker"]["stage"], stage);
    assert_eq!(snapshot["first_observed_blocker"]["reason_code"], reason);
    assert_eq!(snapshot["first_observed_blocker"]["attempt"], 1);
    assert_eq!(operation["required_evidence"], required);
    assert_eq!(operation["diagnostic_reference"], operation_reference);
    assert_eq!(operation["trace_health"]["durable_state"], "NOT_READ");
    assert_eq!(operation["trace_health"]["history_complete"], "UNKNOWN");
    assert_eq!(operation["trace_health"]["initiator"], "OBSERVED_CALLER");
    assert_eq!(
        snapshot["invocation"]["request_id"],
        "terminal-status-source"
    );
    assert_eq!(
        snapshot["first_observed_blocker"]["invocation"]["request_id"],
        "terminal-status-source"
    );
    assert_eq!(
        snapshot["facts"]["fills"]["watermark_ms"],
        if stale_bills { 100 } else { 99 }
    );
    assert_eq!(snapshot["facts"]["fills"]["generation"], 1);
    assert_eq!(snapshot["facts"]["fills"]["has_coverage_gaps"], false);
    assert_eq!(snapshot["facts"]["terminal_order_updated_at_ms"], 100);
    if stale_bills {
        assert_eq!(snapshot["facts"]["bills"]["watermark_ms"], 99);
        assert_eq!(snapshot["facts"]["worker_sequence"], 7);
    } else {
        assert!(snapshot["facts"]["bills"].is_null());
        assert!(snapshot["facts"]["worker_sequence"].is_null());
    }
    assert_eq!(operation["trace_health"]["dropped_event_count"], 0);
    assert_eq!(operation["trace_health"]["evicted_operation_count"], 0);
    let mut unknown_request = request.clone();
    if let okx_protocol::AgentOperation::Diagnostics { request } = &mut unknown_request.operation {
        request.reference = Some("terminal_release:unknown-after-restart".into());
    }
    let unknown = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        crate::query::dispatch(&ctx, &unknown_request),
    )
    .await
    .unwrap()
    .result
    .unwrap();
    assert_eq!(unknown["state"], "NOT_OBSERVED");
    assert_eq!(
        unknown["operation"]["trace_health"]["history_complete"],
        "UNKNOWN"
    );
    assert!(unknown["operation"]["snapshot"].is_null());
    assert_eq!(*feed.requests.lock().unwrap(), requests_before);
    assert_eq!(*feed.outbound_posts.lock().unwrap(), posts_before);
    assert_eq!(*feed.denied_routes.lock().unwrap(), denied_before);
    assert!(matches!(
        risk_requests.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    drop(source_health);
    drop(source_snapshot);
    release_tx.send(()).unwrap();
    blocked_sql.await.unwrap().unwrap();
    resources
        .execution
        .store
        .call(deadline(), |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(
        resources.execution.store.sql_timing().observed_jobs,
        sql_before + 2
    );
    assert_eq!(
        terminal_coverage_sql_snapshot(&resources.execution.store).await,
        before
    );
    drop(ctx);

    let mut terminated = resources.execution.store.termination_signal();
    // Signal shutdown before releasing a source consumer paused at the gate.
    tokio::join!(biased; accounts.shutdown(), async { drop(capture) });
    drop(service);
    drop(resources);
    drop(accounts);
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        terminated.wait_for(|v| *v),
    )
    .await
    .unwrap()
    .unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

async fn terminal_coverage_sql_snapshot(store: &AccountStoreHandle) -> serde_json::Value {
    store
        .call(deadline(), |s| {
            Ok(serde_json::json!({
                "intent": s.intent("intent_terminal_stale_fill")?,
                "receipt": s.central_receipt("intent_terminal_stale_fill")?,
                "active_receipt": s.active_central_receipt("intent_terminal_stale_fill")?,
                "lots": s.active_lots("BTC-USDT-SWAP")?,
                "finance_revision": s.finance_revision()?,
                "finance": s.finance_totals(i64::MIN, i64::MAX, None)?.into_iter().collect::<Vec<_>>(),
                "policy": s.policy()?,
                "fill_cursor": s.cursor("fills", "SWAP")?,
                "bill_cursor": s.cursor("bills", "account")?,
                "settlement": s.operation("entry-settlement:intent_terminal_stale_fill")?,
                "release": s.operation("central-release:reservation_terminal_stale_fill")?
            }))
        })
        .await
        .unwrap()
}

#[path = "recovery_tests.rs"]
mod recovery_tests;
