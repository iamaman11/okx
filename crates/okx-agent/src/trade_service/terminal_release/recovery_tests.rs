//! Native orchestration proof over original reopened SQL. The scripted risk
//! channel is deliberately not a Worker durability or financial CAS proof.
use super::*;
use crate::accounts::{AccountRuntimeRegistry, initialize_installation, recorded_support};
use crate::risk_bridge::{NativeRiskClient, OutboundRiskCommand};
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

struct Fixture {
    root: std::path::PathBuf,
    accounts: Arc<AccountRuntimeRegistry>,
    resources: Arc<AccountResources>,
    feed: Arc<recorded_support::RecordedAccountHttp>,
    service: TradeService,
    requests: tokio::sync::mpsc::Receiver<OutboundRiskCommand>,
    intent: StoredIntent,
    order: okx_api::TradeOrderDetails,
}

async fn fixture() -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "okx-terminal-release-reopen-{}-{}",
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
            let quote = okx_execution::EntryQuoteEvidence {
                schema: "okx.entry-known-quote/v1".into(),
                account_key: s.meta().account_key.clone(),
                installation_id: INSTALLATION.into(),
                intent_id: ID.into(),
                plan_hash: saved.plan_hash.clone(),
                policy_revision: 1,
                instrument_id: saved.instrument_id.clone(),
                quantity: Decimal::ONE,
                limit_price: Decimal::from(5),
                contract_value: Decimal::ONE,
                fee_rate: Decimal::ZERO,
                maker_rate: "0".into(),
                taker_rate: "0".into(),
                fee_level: "fixture".into(),
                fee_exchange_at: 2,
                usd_per_usdt: Decimal::ONE,
                notional_usdt: Decimal::from(5),
                fee_usdt: Decimal::ZERO,
                total_usdt: Decimal::from(5),
                total_usd: Decimal::from(5),
                fee_generation: format!(
                    "sha256:{}",
                    digest(&("BTC-USDT-SWAP", "fixture", "fixture", "0", "0", true)).unwrap()
                ),
                reference_generation: "fixture".into(),
                fee_received_at: 2,
                conversion_digest: "f".repeat(64),
                conversion_received_at: 2,
                observed_at: 2,
            };
            s.bind_entry_quote(&quote)?;
            s.bind_entry_admission(&okx_execution::EntryAdmissionEvidence {
                schema: "okx.entry-admission/v2".into(),
                account_key: s.meta().account_key.clone(),
                installation_id: INSTALLATION.into(),
                intent_id: ID.into(),
                plan_hash: saved.plan_hash.clone(),
                quote_digest: digest(&quote).unwrap(),
                admission_usd_per_usdt: Decimal::ONE,
                other_entry_costs_upper_usd: Decimal::ZERO,
                entry_upper_usd: Decimal::from(5),
                entry_proof_digest: "d".repeat(64),
                observed_at: 2,
            })?;
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
                    watermark: TERMINAL_AT,
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })?;
            {
                s.ingest_page(&IngestPage {
                    cursor: Some(SourceCursor {
                        source: "bills".into(),
                        stream_partition: "account".into(),
                        cursor: String::new(),
                        watermark: TERMINAL_AT,
                        generation: 1,
                        coverage_gaps: String::new(),
                    }),
                    ..Default::default()
                })?;
            }
            s.bind_terminal_entry_settlement(ID, TERMINAL_AT, Decimal::ZERO, TERMINAL_AT)?;
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
    let persisted = snapshot(&resources.execution.store).await;
    assert!(persisted["release"].is_null());
    assert!(persisted["settlement"].is_object());
    let mut ended = resources.execution.store.termination_signal();
    tokio::join!(biased; accounts.shutdown(), async { drop(capture) });
    drop(resources);
    drop(accounts);
    tokio::time::timeout(std::time::Duration::from_secs(3), ended.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    // Normal reconnect opens the same catalog and original account.sqlite.
    let accounts = Arc::new(
        AccountRuntimeRegistry::open(&root, budget.clone())
            .await
            .unwrap(),
    );
    let (_, clients) = recorded_support::prepared("a", feed.clone(), budget.clone());
    accounts
        .reconnect_prepared(config.clone(), clients)
        .await
        .unwrap();
    let resources = accounts.resolve(&config.account_key.wire()).await.unwrap();
    let capture = resources.recorded_capture_gate.write().await;
    assert_eq!(snapshot(&resources.execution.store).await, persisted);
    let restored = resources
        .execution
        .store
        .call(deadline(), |s| s.intent(ID))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let view = resources.terminal_release_trace.read(ID);
            if view["state"] == "OBSERVED" {
                assert_eq!(view["snapshot"]["stage"], "durable_snapshot");
                assert_eq!(
                    view["snapshot"]["facts"]["durable_receipt_state"],
                    "TERMINAL_PENDING_CENTRAL"
                );
                assert!(view["snapshot"]["facts"]["worker_released"].is_null());
                assert!(view["snapshot"]["invocation"].is_null());
                assert_eq!(view["trace_health"]["history_complete"], "UNKNOWN");
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(restored).unwrap(),
        serde_json::to_value(&intent).unwrap()
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
    let (risk_tx, risk_requests) = tokio::sync::mpsc::channel(4);
    let service = TradeService::new(
        accounts.clone(),
        public_ws.clone(),
        NativeRiskClient::new(risk_tx),
    );
    *service.governance.write().await = Some(authority);

    drop(capture);
    Fixture {
        root,
        accounts,
        resources,
        feed,
        service,
        requests: risk_requests,
        intent,
        order,
    }
}

async fn snapshot(store: &AccountStoreHandle) -> serde_json::Value {
    let mut result = super::terminal_coverage_sql_snapshot(store).await;
    let money = store
        .call(deadline(), |s| {
            Ok(json!({
                "opening_equity": s.opening_equity_usd()?,
                "account_money": s.account_summary()?["money"],
                "late_pending": s.entry_late_pending(ID)?,
            }))
        })
        .await
        .unwrap();
    result["money"] = money;
    result
}

fn released(request: &OutboundRiskCommand) -> RiskResult {
    assert!(request.command.valid());
    assert_eq!(request.command.installation_id, INSTALLATION);
    assert!(matches!(&request.command.action,
        RiskCommandAction::Inspect { reservation_key, plan_sha256 }
        if reservation_key == KEY && plan_sha256 == &"d".repeat(64)));
    RiskResult {
        schema: RISK_RESULT_SCHEMA_V1.into(),
        command_id: request.command.command_id.clone(),
        status: RiskDecisionStatus::Observed,
        reason: None,
        reservation_key: Some(KEY.into()),
        risk_book_sequence: Some(7),
        policy_revision: Some(1),
        existing_modelled_loss_usdt: None,
        reservation_state: Some(RiskReservationState::Released),
        budget: None,
        account_budget: None,
    }
}

impl Fixture {
    async fn finish(self) {
        let mut ended = self.resources.execution.store.termination_signal();
        self.accounts.shutdown().await;
        drop(self.service);
        drop(self.resources);
        drop(self.accounts);
        tokio::time::timeout(std::time::Duration::from_secs(3), ended.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

#[tokio::test]
async fn terminal_release_reopen_rejects_unbound_inspection_without_sql_ack() {
    let mut f = fixture().await;
    let capture = f.resources.recorded_capture_gate.write().await;
    let before = snapshot(&f.resources.execution.store).await;
    let venue_before = f.feed.requests.lock().unwrap().clone();
    // Every malformed response must leave the original durable settlement,
    // reservation, finance and pending census status byte-for-byte unchanged.
    for case in [
        "wrong_key",
        "missing_key",
        "wrong_policy",
        "missing_policy",
        "missing_sequence",
        "wrong_command",
        "denied",
        "lost_reply",
    ] {
        let release =
            f.service
                .release_terminal(&f.resources, &f.intent, Some(&f.order), true, None);
        let reply = async {
            let request = f.requests.recv().await.unwrap();
            let mut response = released(&request);
            match case {
                "wrong_key" => response.reservation_key = Some("another-reservation".into()),
                "missing_key" => response.reservation_key = None,
                "wrong_policy" => response.policy_revision = Some(2),
                "missing_policy" => response.policy_revision = None,
                "missing_sequence" => response.risk_book_sequence = None,
                "wrong_command" => response.command_id = "another-command".into(),
                "denied" => {
                    response.status = RiskDecisionStatus::Denied;
                    response.reservation_state = None;
                    response.reason = Some("STALE_NATIVE_INSPECTION_SESSION".into());
                }
                "lost_reply" => {
                    drop(request);
                    return;
                }
                _ => unreachable!(),
            }
            request.return_to.send(response).unwrap();
        };
        let (result, ()) = tokio::join!(release, reply);
        let error = result.expect_err(case);
        assert_eq!(error.stage, TerminalReleaseStage::Inspection, "{case}");
        assert_eq!(
            snapshot(&f.resources.execution.store).await,
            before,
            "{case}"
        );
        assert_eq!(*f.feed.requests.lock().unwrap(), venue_before, "{case}");
        assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
        assert!(
            f.requests.try_recv().is_err(),
            "no second SETTLE/RELEASE: {case}"
        );
    }
    drop(capture);
    f.finish().await;
}

#[tokio::test]
async fn terminal_release_reopen_rejects_old_session_reply_before_sql_ack() {
    let mut f = fixture().await;
    let capture = f.resources.recorded_capture_gate.write().await;
    let before = snapshot(&f.resources.execution.store).await;
    let release = f
        .service
        .release_terminal(&f.resources, &f.intent, Some(&f.order), true, None);
    let reply = async {
        let request = f.requests.recv().await.unwrap();
        let mut replacement = f.service.governance.read().await.clone().unwrap();
        replacement.session_generation += 1;
        f.service.update_governance(&replacement).await.unwrap();
        let response = released(&request);
        request.return_to.send(response).unwrap();
    };
    let (result, ()) = tokio::join!(release, reply);
    let error = result.unwrap_err();
    assert_eq!(error.stage, TerminalReleaseStage::Inspection);
    assert_eq!(error.reason_code, "TERMINAL_INSPECTION_SESSION_CHANGED");
    assert_eq!(snapshot(&f.resources.execution.store).await, before);
    assert!(f.requests.try_recv().is_err());
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    drop(capture);
    f.finish().await;
}

#[tokio::test]
async fn terminal_release_reopen_inspects_once_then_acknowledges_without_mutation_replay() {
    let mut f = fixture().await;
    let capture = f.resources.recorded_capture_gate.write().await;
    // The receipt's policy is frozen. Governance rotation does not rewrite it.
    f.service
        .governance
        .write()
        .await
        .as_mut()
        .unwrap()
        .state
        .revision = 2;
    let before = snapshot(&f.resources.execution.store).await;
    let venue_before = f.feed.requests.lock().unwrap().clone();
    let release = f
        .service
        .release_terminal(&f.resources, &f.intent, Some(&f.order), true, None);
    let reply = async {
        let request = f.requests.recv().await.unwrap();
        let response = released(&request);
        assert!(response.valid());
        let hash = digest(&response).unwrap();
        request.return_to.send(response).unwrap();
        hash
    };
    let (result, response_hash) = tokio::join!(release, reply);
    result.unwrap();
    let after = snapshot(&f.resources.execution.store).await;
    assert!(after["active_receipt"].is_null());
    assert_eq!(after["release"]["result_ref"], response_hash);
    for key in [
        "intent",
        "receipt",
        "lots",
        "finance_revision",
        "finance",
        "policy",
        "fill_cursor",
        "bill_cursor",
        "settlement",
        "money",
    ] {
        assert_eq!(after[key], before[key], "{key}");
    }
    // Concurrent duplicate recovery owns no mutation command; SQL preserves
    // exactly the first acknowledgement, including its committed timestamp.
    let a = f
        .service
        .release_terminal(&f.resources, &f.intent, Some(&f.order), true, None);
    let b = f
        .service
        .release_terminal(&f.resources, &f.intent, Some(&f.order), true, None);
    let replies = async {
        for _ in 0..2 {
            let request = f.requests.recv().await.unwrap();
            let response = released(&request);
            request.return_to.send(response).unwrap();
        }
    };
    let (a, b, ()) = tokio::join!(a, b, replies);
    a.unwrap();
    b.unwrap();
    assert_eq!(snapshot(&f.resources.execution.store).await, after);
    assert!(f.requests.try_recv().is_err());
    assert_eq!(*f.feed.requests.lock().unwrap(), venue_before);
    assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    // A real independent late fee reopens the RELEASED receipt. Recovery must
    // retain its pending marker until the late SETTLE_ENTRY owner confirms it.
    f.resources
        .execution
        .store
        .call(deadline(), |s| {
            s.ingest_page(&IngestPage {
                bills: vec![okx_execution::Bill {
                    bill_id: "terminal-late-fee".into(),
                    kind: "2:trade".into(),
                    time: 101,
                    amount: Decimal::new(-1, 2),
                    currency: "USDT".into(),
                    economic_event_key: "bill:terminal-late-fee".into(),
                }],
                finance: vec![okx_execution::FinanceEntry {
                    entry_id: "bill:terminal-late-fee:FEE".into(),
                    economic_event_key: "bill:terminal-late-fee".into(),
                    component: "FEE".into(),
                    allocation_leg: "account".into(),
                    amount: Decimal::new(-1, 2),
                    currency: "USDT".into(),
                    instance_id: None,
                    source_ref: "bill:terminal-late-fee".into(),
                    time: 101,
                    reverses_entry_id: None,
                }],
                entry_order_fee_links: vec![okx_execution::EntryOrderFeeLink {
                    bill_id: "terminal-late-fee".into(),
                    intent_id: ID.into(),
                    exchange_order_id: ORDER.into(),
                    instrument_id: "BTC-USDT-SWAP".into(),
                    bill_at: 101,
                    fee_usdt: Decimal::new(-1, 2),
                }],
                ..Default::default()
            })
        })
        .await
        .unwrap();
    let late = snapshot(&f.resources.execution.store).await;
    assert_eq!(late["money"]["late_pending"], true);
    for _ in 0..2 {
        let release =
            f.service
                .release_terminal(&f.resources, &f.intent, Some(&f.order), true, None);
        let reply = async {
            let request = f.requests.recv().await.unwrap();
            let response = released(&request);
            request.return_to.send(response).unwrap();
        };
        let (result, ()) = tokio::join!(release, reply);
        let error = result.unwrap_err();
        assert_eq!(error.stage, TerminalReleaseStage::LateSettlement);
        assert_eq!(error.reason_code, "LATE_ENTRY_SETTLEMENT_PENDING");
        assert_eq!(snapshot(&f.resources.execution.store).await, late);
        assert!(f.requests.try_recv().is_err());
        assert_eq!(*f.feed.requests.lock().unwrap(), venue_before);
        assert!(f.feed.outbound_posts.lock().unwrap().is_empty());
    }
    drop(capture);
    f.finish().await;
}
