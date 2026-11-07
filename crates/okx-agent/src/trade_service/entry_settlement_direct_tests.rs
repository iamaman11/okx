//! Direct entry settlement tests: real SQL owner, programmed RiskAuthority replies.
//! The RiskAuthority seam tests orchestration, not real Worker money correctness.
use super::*;
use okx_execution::{
    AccountMeta, Bill, BillOrderSource, CentralReceipt, Decision, EntryAdmissionEvidence,
    EntryQuoteEvidence, Fill, FinanceEntry, IngestPage, IntentPlan, IntentState, NewIntent,
    OpeningSnapshot, SourceCursor,
};
use okx_protocol::risk_authority::{
    RiskCommand, RiskResult, RISK_RESULT_SCHEMA_V1,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static COUNTER: AtomicUsize = AtomicUsize::new(0);
fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

struct Fixture {
    store: AccountStoreHandle,
    account: AccountKey,
    intent: StoredIntent,
    order: TradeOrderDetails,
    governance: RwLock<Option<CentralGovernanceSnapshot>>,
}

async fn fixture(label: &str, finance: bool) -> Fixture {
    let root = std::env::temp_dir().join(format!(
        "okx-entry-settlement-{label}-{}-{}-{}",
        std::process::id(),
        now(),
        COUNTER.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let account = AccountKey {
        venue: "okx".into(),
        environment: Environment::Real,
        verified_uid: "a".repeat(64),
    };
    let account_wire = account.wire();
    let initialized_at = now() - 1_000;
    let store = AccountStoreHandle::initialize_flat(
        root.join("account.db"),
        AccountMeta {
            account_key: account_wire.clone(),
            installation_id: "installation_test".into(),
            initialized_at,
            opening_source: "opening_fixture".into(),
            initialize_operation_id: "initialize_fixture".into(),
            release_id: "a".repeat(40),
        },
        OpeningSnapshot {
            source_ref: "opening_fixture".into(),
            observed_at: initialized_at,
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
    let definition = store
        .call(deadline(), |s| s.ensure_manual_instance(now()))
        .await
        .unwrap();
    let quantity = Decimal::ONE;
    let limit_price = Decimal::from(5);
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "open".into(),
        side: "buy".into(),
        position_side: "long".into(),
        quantity,
        limit_price,
        stop_price: Some(Decimal::from(4)),
        strategy_instance: "manual".into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::ONE,
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: "decision_fixture".into(),
        instance_id: None,
        decision_at: now() - 100,
        input_digest: "b".repeat(64),
        policy_revision: 1,
        code_version: "fixture".into(),
    };
    let candidate = NewIntent {
        plan,
        intent_id: "intent_fixture".into(),
        idempotency_key: "fixture_command_01".into(),
        request_hash: "b".repeat(64),
        client_order_id: "b".repeat(32),
        deadline: now() + 30_000,
        plan_hash: "c".repeat(64),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC-USDT-SWAP".into(),
    };
    let saved = store
        .call(deadline(), move |s| {
            s.apply_authority_policy(1, false, "campaign_fixture", Decimal::from(20))?;
            s.prepare(&decision, &candidate)
        })
        .await
        .unwrap();
    let observed = now();
    let rate = Decimal::new(5, 4);
    let notional = quantity * limit_price;
    let fee = Decimal::new(25, 4);
    let total = notional + fee;
    let quote = EntryQuoteEvidence {
        schema: "okx.entry-known-quote/v1".into(),
        account_key: account_wire.clone(),
        installation_id: "installation_test".into(),
        intent_id: saved.intent_id.clone(),
        plan_hash: saved.plan_hash.clone(),
        policy_revision: saved.policy_revision,
        instrument_id: saved.instrument_id.clone(),
        quantity,
        limit_price,
        contract_value: Decimal::ONE,
        fee_rate: rate,
        maker_rate: "-0.0002".into(),
        taker_rate: "0.0005".into(),
        fee_level: "Lv1".into(),
        fee_exchange_at: observed,
        usd_per_usdt: Decimal::ONE,
        notional_usdt: notional,
        fee_usdt: fee,
        total_usdt: total,
        total_usd: total,
        fee_generation: format!("sha256:{:x}", Sha256::digest(
            serde_json::to_vec(&(
                "BTC-USDT-SWAP", "reference_fixture", "Lv1",
                "-0.0002", "0.0005", true
            )).unwrap()
        )),
        reference_generation: "reference_fixture".into(),
        fee_received_at: observed,
        conversion_digest: "b".repeat(64),
        conversion_received_at: observed,
        observed_at: observed,
    };
    let admission = EntryAdmissionEvidence {
        schema: "okx.entry-admission/v2".into(),
        account_key: quote.account_key.clone(),
        installation_id: quote.installation_id.clone(),
        intent_id: quote.intent_id.clone(),
        plan_hash: quote.plan_hash.clone(),
        quote_digest: format!("{:x}", Sha256::digest(serde_json::to_vec(&quote).unwrap())),
        admission_usd_per_usdt: Decimal::ONE,
        other_entry_costs_upper_usd: Decimal::ZERO,
        entry_upper_usd: total,
        entry_proof_digest: "a".repeat(64),
        observed_at: observed,
    };
    let receipt = CentralReceipt {
        reservation_key: "fixture_reservation".into(),
        account_key: account_wire.clone(),
        installation_id: "installation_test".into(),
        instance_id: None,
        intent_id: saved.intent_id.clone(),
        plan_hash: saved.plan_hash.clone(),
        policy_revision: 1,
        receipt_hash: "d".repeat(64),
        requested_loss_usd: Decimal::ONE,
        conversion_digest: "e".repeat(64),
    };
    store.call(deadline(), move |s| {
        s.bind_entry_quote(&quote)?;
        s.bind_entry_admission(&admission)?;
        s.reserve(&receipt, now())
    }).await.unwrap();
    let posted = super::super::execute::post_once(
        &store, &saved, deadline(), || async { Ok::<_, String>(()) }
    ).await.unwrap();
    assert_eq!(posted, Some(()));
    let id = saved.intent_id.clone();
    store.call(deadline(), move |s| {
        s.record_outcome(
            &id, IntentState::Acknowledged, Some("fixture_exchange_order"),
            "authenticated-fixture-venue-ack", now(),
        )
    }).await.unwrap();
    let at = now() + 1000;
    let filled = Decimal::new(5, 1);
    let fee = Decimal::new(-125, 5);
    if finance {
        let fill_id = format!("entry-{label}");
        let bill_id = format!("entry-bill-{label}");
        let bill_key = format!("bill:{bill_id}");
        let fill_entry = FinanceEntry {
            entry_id: format!("{bill_key}:FEE"),
            economic_event_key: bill_key.clone(),
            component: "FEE".into(),
            allocation_leg: "account".into(),
            amount: fee,
            currency: "USDT".into(),
            instance_id: None,
            source_ref: format!("fill:{fill_id}"),
            time: at,
            reverses_entry_id: None,
        };
        let id = saved.intent_id.clone();
        let source_fill_id = fill_id.clone();
        store.call(deadline(), move |s| {
            s.ingest_page(&IngestPage {
                fills: vec![Fill {
                    fill_id: source_fill_id.clone(),
                    instrument_id: "BTC-USDT-SWAP".into(),
                    trade_id: format!("venue-{source_fill_id}"),
                    exchange_order_id: "fixture_exchange_order".into(),
                    intent_id: Some(id),
                    time: at,
                    qty: filled,
                    price: Decimal::from(5),
                    fee,
                    currency: "USDT".into(),
                    side: "buy".into(),
                }],
                finance: vec![fill_entry],
                cursor: Some(SourceCursor {
                    source: "fills".into(),
                    stream_partition: "SWAP".into(),
                    cursor: String::new(),
                    watermark: at + 1,
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })
        }).await.unwrap();
        let source_bill_id = bill_id.clone();
        store.call(deadline(), move |s| {
            s.ingest_page(&IngestPage {
                bills: vec![Bill {
                    bill_id: source_bill_id.clone(),
                    kind: "2:trade".into(),
                    time: at + 1,
                    amount: fee,
                    currency: "USDT".into(),
                    economic_event_key: bill_key.clone(),
                }],
                finance: vec![FinanceEntry {
                    entry_id: format!("{bill_key}:FEE"),
                    economic_event_key: bill_key,
                    component: "FEE".into(),
                    allocation_leg: "account".into(),
                    amount: fee,
                    currency: "USDT".into(),
                    instance_id: None,
                    source_ref: format!("bill:{source_bill_id}"),
                    time: at + 1,
                    reverses_entry_id: None,
                }],
                bill_order_sources: vec![BillOrderSource {
                    bill_id: source_bill_id,
                    instrument_id: "BTC-USDT-SWAP".into(),
                    exchange_order_id: "fixture_exchange_order".into(),
                    trade_id: format!("venue-{fill_id}"),
                }],
                cursor: Some(SourceCursor {
                    source: "bills".into(),
                    stream_partition: "account".into(),
                    cursor: String::new(),
                    watermark: at + 1,
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })
        }).await.unwrap();
    }
    let target = saved.intent_id.clone();
    store.call(deadline(), move |s| s.record_order_readback(
        &target, "fixture_exchange_order", IntentState::PartiallyFilled, filled, at + 1,
    )).await.unwrap();
    let intent = store.call(deadline(), |s| s.intent("intent_fixture")).await.unwrap().unwrap();
    let order = TradeOrderDetails {
        instrument_id: intent.instrument_id.clone(),
        order_id: "fixture_exchange_order".into(),
        client_order_id: intent.client_order_id.clone(),
        side: "buy".into(),
        position_side: "long".into(),
        trade_mode: "cross".into(),
        order_type: "limit".into(),
        price: "5".into(),
        size: "1".into(),
        accumulated_fill_size: "0.5".into(),
        average_fill_price: "5".into(),
        state: "partially_filled".into(),
        creation_time_ms: at.to_string(),
        update_time_ms: (at + 1).to_string(),
    };
    let governance: CentralGovernanceSnapshot = serde_json::from_value(json!({
        "state": {
            "schema": "okx.governance/v3", "revision": 1,
            "installation_id": "installation_test",
            "campaign_id": format!("sha256:{}", "a".repeat(64)),
            "campaign_loss_limit_usd": "20",
            "max_total_modelled_loss_usdt": "5",
            "accounts": [{
                "id": "primary", "label": "Primary",
                "venue": "okx", "environment": "real",
                "uid_fingerprint": "a".repeat(64),
                "identity_verified": true, "enabled": true, "explicit_pause": false,
                "policy_revision": 1, "account_key": account_wire,
                "limits": {
                    "max_modelled_loss_usdt": "5", "max_order_loss_usdt": "2",
                    "max_notional_usdt": "100", "max_leverage": "20",
                    "max_daily_realized_loss_usdt": "8", "max_drawdown_ratio": "0.2",
                    "allowed_instruments": ["BTC-USDT-SWAP"]
                }, "strategies": []
            }]
        }, "issued_at_ms": 1792910000000_u64, "session_generation": 673_u64
    })).unwrap();
    Fixture { store, account, intent, order, governance: RwLock::new(Some(governance)) }
}

#[derive(Clone, Copy)]
enum ReplyMode {
    Accepted,
    LostSettle,
    AlwaysStale,
}

fn risk_client(mode: ReplyMode) -> (NativeRiskClient, tokio::task::JoinHandle<Vec<RiskCommand>>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::risk_bridge::OutboundRiskCommand>(16);
    let worker = tokio::spawn(async move {
        let mut commands = Vec::new();
        while let Some(outbound) = rx.recv().await {
            let cmd = outbound.command;
            let is_settle = matches!(&cmd.action, RiskCommandAction::SettleEntry { .. });
            let response = RiskResult {
                schema: RISK_RESULT_SCHEMA_V1.into(),
                command_id: cmd.command_id.clone(),
                status: if is_settle {
                    if matches!(mode, ReplyMode::AlwaysStale) {
                        RiskDecisionStatus::Denied
                    } else {
                        RiskDecisionStatus::Updated
                    }
                } else {
                    RiskDecisionStatus::Observed
                },
                reason: if is_settle && matches!(mode, ReplyMode::AlwaysStale) {
                    Some("STALE_SESSION_OR_BOOK_SEQUENCE".into())
                } else {
                    None
                },
                reservation_key: Some("fixture_reservation".into()),
                risk_book_sequence: Some(7),
                policy_revision: Some(1),
                existing_modelled_loss_usdt: None,
                reservation_state: if is_settle {
                    None
                } else {
                    Some(RiskReservationState::Held)
                },
                budget: None,
                account_budget: None,
            };
            assert!(response.valid());
            commands.push(cmd);
            if is_settle && matches!(mode, ReplyMode::LostSettle) {
                drop(outbound.return_to);
            } else {
                outbound.return_to.send(response).unwrap();
            }
        }
        commands
    });
    (NativeRiskClient::new(tx), worker)
}

async fn settle(f: &Fixture, risk: &NativeRiskClient) -> Result<EntrySettlementOutcome, EntrySettlementError> {
    settle_incremental_entry(
        &f.account, &f.intent, Some(&f.order), &f.store,
        risk, &f.governance, deadline,
    ).await
}

async fn has_ack(f: &Fixture) -> bool {
    f.store.call(deadline(), |s| {
        Ok(s.operation("entry-partial-settled:intent_fixture:r1")?.is_some())
    }).await.unwrap()
}

#[tokio::test]
async fn r01_r02_partial_settlement_and_same_revision_repeat_use_one_sql_ack() {
    let f = fixture("r01", true).await;
    let (risk, worker) = risk_client(ReplyMode::Accepted);
    assert_eq!(settle(&f, &risk).await.unwrap(), EntrySettlementOutcome::RevisionAcknowledged);
    assert!(has_ack(&f).await);
    assert_eq!(settle(&f, &risk).await.unwrap(), EntrySettlementOutcome::RevisionAcknowledged);
    drop(risk);
    let commands = worker.await.unwrap();
    assert_eq!(commands.len(), 6);
    for batch in commands.chunks_exact(3) {
        assert!(matches!(batch[0].action, RiskCommandAction::Inspect { .. }));
        assert!(matches!(batch[1].action, RiskCommandAction::Inspect { .. }));
        let RiskCommandAction::SettleEntry { settlement_revision, cumulative_entry_usd, .. } =
            &batch[2].action else { panic!("expected SETTLE_ENTRY") };
        assert_eq!(*settlement_revision, 1);
        assert_eq!(cumulative_entry_usd, "2.50125");
    }
}

#[tokio::test]
async fn r04_missing_source_cannot_ack_and_identity_mismatch_preserves_first_reason() {
    let f = fixture("r04", false).await;
    let (risk, worker) = risk_client(ReplyMode::Accepted);
    let error = settle(&f, &risk).await.unwrap_err();
    assert_eq!(error.stage, "source_coverage");
    assert_eq!(error.reason_code, "PARTIAL_ENTRY_FILLS_UNCOVERED");
    assert!(!has_ack(&f).await);
    let mut alien = f.order.clone();
    alien.client_order_id = "other-client".into();
    let identity = settle_incremental_entry(
        &f.account, &f.intent, Some(&alien), &f.store, &risk, &f.governance, deadline
    ).await.unwrap_err();
    assert_eq!(identity.reason_code, "ORDER_READBACK_IDENTITY_MISMATCH");
    assert!(!has_ack(&f).await);
    drop(risk);
    let commands = worker.await.unwrap();
    assert_eq!(commands.len(), 1, "identity denial never reaches Worker");
    assert!(matches!(commands[0].action, RiskCommandAction::Inspect { .. }));
}

#[tokio::test]
async fn r05_lost_settlement_reply_keeps_unacked_revision_and_replays_same_proof() {
    let f = fixture("r05", true).await;
    let (risk, worker) = risk_client(ReplyMode::LostSettle);
    let error = settle(&f, &risk).await.unwrap_err();
    assert_eq!(error.reason_code, "RISK_TRANSPORT_LOST_DO_NOT_REPLAY");
    assert_eq!(error.stage, "risk_response");
    assert!(!has_ack(&f).await);
    drop(risk);
    let before = worker.await.unwrap();
    assert_eq!(before.len(), 3);
    let (risk, worker) = risk_client(ReplyMode::Accepted);
    assert_eq!(settle(&f, &risk).await.unwrap(), EntrySettlementOutcome::RevisionAcknowledged);
    assert!(has_ack(&f).await);
    drop(risk);
    let after = worker.await.unwrap();
    assert_eq!(after.len(), 3);
    assert_eq!(before[2].action, after[2].action, "immutable revision replay");
}

#[tokio::test]
async fn r06_three_cas_conflicts_never_ack_unaccepted_settlement() {
    let f = fixture("r06", true).await;
    let (risk, worker) = risk_client(ReplyMode::AlwaysStale);
    let error = settle(&f, &risk).await.unwrap_err();
    assert_eq!(error.reason_code, "PARTIAL_ENTRY_SETTLEMENT_CAS_BUSY");
    assert_eq!(error.stage, "risk_cas");
    assert!(!has_ack(&f).await);
    drop(risk);
    let commands = worker.await.unwrap();
    assert_eq!(commands.len(), 7, "initial INSPECT plus three bounded CAS pairs");
    assert_eq!(commands.iter().filter(|c| matches!(&c.action, RiskCommandAction::SettleEntry { .. })).count(), 3);
}

#[tokio::test]
async fn r00_demo_and_unverified_order_block_without_sql_or_risk_mutation() {
    let f = fixture("r00", false).await;
    let (risk, worker) = risk_client(ReplyMode::Accepted);
    let mut demo = f.account.clone();
    demo.environment = Environment::Demo;
    assert_eq!(
        settle_incremental_entry(
            &demo, &f.intent, Some(&f.order), &f.store, &risk, &f.governance, deadline
        ).await.unwrap(),
        EntrySettlementOutcome::NotApplicable
    );
    let error = settle_incremental_entry(
        &f.account, &f.intent, None, &f.store, &risk, &f.governance, deadline
    ).await.unwrap_err();
    assert_eq!(error.reason_code, "PARTIAL_ENTRY_ORDER_READBACK_UNAVAILABLE");
    drop(risk);
    assert!(worker.await.unwrap().is_empty());
}
