// Direct no-send crash boundaries, not another complete T09 campaign. The parent
// kills Native after a genuine Worker commit and before delivery to its caller.
// SQL prepare/census inputs are explicit fixture inputs; no Worker reply or
// recovery receipt is fabricated. Real WSS, account SQL and public Cancel/Status
// own the cross-process recovery.
#[derive(Clone, Copy, PartialEq, Eq)]
enum T10_11UnsentPhase {
    F0,
    F1,
    F2,
    F3,
    F4,
    F8,
}
impl T10_11UnsentPhase {
    fn selected() -> Self {
        match std::env::var("OKX_T10_11_NATIVE_CRASH").unwrap().as_str() {
            "F0" => Self::F0,
            "F1" => Self::F1,
            "F2" => Self::F2,
            "F3" => Self::F3,
            "F4" => Self::F4,
            "F8" => Self::F8,
            _ => panic!("unsupported unsent crash phase"),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::F0 => "F0",
            Self::F1 => "F1",
            Self::F2 => "F2",
            Self::F3 => "F3",
            Self::F4 => "F4",
            Self::F8 => "F8",
        }
    }
    fn before_reserve(self) -> bool {
        matches!(self, Self::F0 | Self::F1)
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct T10_11PrepareInput {
    decision: okx_execution::Decision,
    intent: okx_execution::NewIntent,
}
struct T10_11UnsentProbe {
    phase: T10_11UnsentPhase,
    intent_id: String,
    reservation_key: String,
    source_floor_ms: u64,
    root: std::path::PathBuf,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    before_budget: okx_protocol::risk_authority::RiskBudgetSnapshot,
    before_sequence: u64,
    prepare_input: Option<T10_11PrepareInput>,
}
static T10_11_UNSENT_PROBE: std::sync::Mutex<Option<T10_11UnsentProbe>> =
    std::sync::Mutex::new(None);

// Read actual SQL even before any reserve attempt exists. Null means absent;
// it is never converted to a fabricated local/central receipt.
async fn t10_11_sql_snapshot(
    accounts: &Arc<AccountRuntimeRegistry>,
    id: &str,
) -> serde_json::Value {
    let owner = accounts.resolve("fixture-a").await.unwrap();
    let id = id.to_owned();
    owner
        .execution
        .store
        .call(
            std::time::Instant::now() + Duration::from_secs(3),
            move |s| {
                let intent = s.intent(&id)?;
                let attempt = s.operation(&format!("risk-reserve:{id}"))?;
                let local = s.central_receipt(&id)?;
                let key = attempt
                    .as_ref()
                    .map(|a| a.result_ref.clone())
                    .or_else(|| local.as_ref().map(|r| r.reservation_key.clone()));
                let release = match &key {
                    Some(k) => s.operation(&format!("central-release:{k}"))?,
                    None => None,
                };
                Ok(serde_json::json!({
                    "intent_id":id,"intent_present":intent.is_some(),
                    "state":intent.as_ref().map(|i|i.state.as_str()),
                    "request_hash":intent.as_ref().map(|i|&i.request_hash),
                    "plan_hash":intent.as_ref().map(|i|&i.plan_hash),
                    "deadline":intent.as_ref().map(|i|i.deadline),
                    "exchange_order_id":intent.as_ref().and_then(|i|i.exchange_order_id.as_ref()),
                    "reserve_attempt":attempt,"reservation_key":key,"central_receipt":local,
                    "terminal_receipt":s.operation(&format!("terminal:{id}"))?,
                    "central_release":release,
                }))
            },
        )
        .await
        .unwrap()
}

pub(super) async fn t10_11_hold_unsent_reply(
    result: &RiskResult,
    command: Option<&RiskCommand>,
    ctx: &DispatchContext,
    generation: u64,
) {
    let Some(command) = command else { return };
    let probe = {
        let mut slot = T10_11_UNSENT_PROBE.lock().unwrap();
        let matched = slot
            .as_ref()
            .is_some_and(|p| match (&p.phase, &command.action) {
                (T10_11UnsentPhase::F2, RiskCommandAction::Reserve { intent_id, .. }) => {
                    intent_id == &p.intent_id
                }
                (
                    T10_11UnsentPhase::F0 | T10_11UnsentPhase::F1,
                    RiskCommandAction::Inspect {
                        reservation_key, ..
                    },
                )
                | (
                    T10_11UnsentPhase::F3,
                    RiskCommandAction::Inspect {
                        reservation_key, ..
                    },
                )
                | (
                    T10_11UnsentPhase::F4,
                    RiskCommandAction::BeginPost {
                        reservation_key, ..
                    },
                )
                | (
                    T10_11UnsentPhase::F8,
                    RiskCommandAction::ReleaseUnsent {
                        reservation_key, ..
                    },
                ) => reservation_key == &p.reservation_key,
                _ => false,
            });
        if matched { slot.take() } else { None }
    };
    let Some(probe) = probe else { return };
    let sql = t10_11_sql_snapshot(&ctx.accounts, &probe.intent_id).await;
    let (status, state) = match probe.phase {
        T10_11UnsentPhase::F0 => (RiskDecisionStatus::Denied, None),
        T10_11UnsentPhase::F1 => (RiskDecisionStatus::Denied, Some("PREPARED")),
        T10_11UnsentPhase::F2 => (RiskDecisionStatus::Held, Some("PREPARED")),
        T10_11UnsentPhase::F3 => (RiskDecisionStatus::Observed, Some("RESERVED")),
        T10_11UnsentPhase::F4 => (RiskDecisionStatus::PostAuthorized, Some("RESERVED")),
        T10_11UnsentPhase::F8 => (RiskDecisionStatus::Updated, Some("REJECTED")),
    };
    assert_eq!(
        result.status, status,
        "actual Worker commit must precede kill: {:?}",
        result.reason
    );
    assert_eq!(sql["state"], serde_json::to_value(state).unwrap());
    assert_eq!(sql["intent_present"], probe.phase != T10_11UnsentPhase::F0);
    if probe.phase == T10_11UnsentPhase::F3 {
        assert_eq!(
            result.reservation_state,
            Some(okx_protocol::risk_authority::RiskReservationState::Held)
        );
    }
    assert_eq!(
        sql["central_receipt"].is_null(),
        probe.phase.before_reserve() || probe.phase == T10_11UnsentPhase::F2
    );
    assert_eq!(sql["central_release"], serde_json::Value::Null);
    if probe.phase == T10_11UnsentPhase::F8 {
        assert_eq!(sql["terminal_receipt"]["kind"], "REJECTED_UNSENT");
    }
    assert_eq!(sql["exchange_order_id"], serde_json::Value::Null);
    if probe.phase.before_reserve() {
        assert_eq!(result.reason.as_deref(), Some("RISK_RESERVATION_NOT_FOUND"));
        assert!(result.reservation_key.is_none());
        assert!(sql["reservation_key"].is_null());
        assert!(sql["reserve_attempt"].is_null());
    } else {
        assert_eq!(
            sql["reservation_key"],
            result.reservation_key.as_deref().unwrap()
        );
    }
    let posts = probe.feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts, 0);
    let handoff = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    t10_12_publish(
        &format!("{handoff}.t11-native-kill-ready"),
        &serde_json::json!({
            "schema":"okx.t10-11-native-unsent-kill-boundary/v1",
            "phase":probe.phase.name(),"pid":std::process::id(),"root":probe.root,
            "generation":generation,"sql":sql,"command":command,
            "worker_result":result,"before_budget":probe.before_budget,
            "before_sequence":probe.before_sequence,"prepare_input":probe.prepare_input,
            "candidate_reservation_key":probe.reservation_key,
            "source_floor_ms":probe.source_floor_ms,
            "venue_posts":posts,
        }),
    );
    // No timeout/Err/drop substitutes for the parent's actual OS termination.
    std::future::pending::<()>().await;
}

async fn t10_11_start_session(
    ctx: DispatchContext,
    mut risk_rx: mpsc::Receiver<OutboundRiskCommand>,
    handoff: &serde_json::Value,
) -> (
    tokio::task::JoinHandle<AgentResult<()>>,
    watch::Sender<bool>,
) {
    let socket = t10_12_connect(
        handoff["ws_url"].as_str().unwrap(),
        handoff["token"].as_str().unwrap(),
    )
    .await;
    let mut hello = test_hello();
    if let F::Hello {
        installation_id, ..
    } = &mut hello
    {
        *installation_id = "installation_test".into();
    }
    let (stop, stopped) = watch::channel(false);
    let session = tokio::spawn(async move {
        run_connected_session(socket, hello, ctx, &mut risk_rx, stopped).await
    });
    (session, stop)
}

#[tokio::test]
#[ignore = "requires the actual Worker and an OS-killing parent"]
async fn native_crash_unsent_boundary() {
    use okx_execution::{CentralReceipt, Decision, IntentPlan, NewIntent};
    use okx_protocol::risk_authority::{CausalPostProof, PostAccountReadProof};
    use rust_decimal::Decimal;
    use sha2::Digest;
    let handoff_path = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    let phase = T10_11UnsentPhase::selected();
    let (ctx, root, risk, risk_rx, feed, _) = test_dispatch_for("installation_test").await;
    std::fs::write(format!("{handoff_path}.native-ready"), b"SQL_OWNERS_OPEN").unwrap();
    let handoff = t10_12_barrier(&handoff_path).await;
    let (mut session, _stop) = t10_11_start_session(ctx.clone(), risk_rx, &handoff).await;
    let mut summary_cmd = summary("summary_t11_f2_before");
    summary_cmd.installation_id = "installation_test".into();
    let baseline = risk.request(summary_cmd).await.unwrap();
    let before = baseline.budget.clone().unwrap();
    let before_sequence = baseline.risk_book_sequence.unwrap();
    let owner = ctx.accounts.resolve("fixture-a").await.unwrap();
    let uid = owner.configuration.account_key.verified_uid.clone();
    let intent_id = "intent_t11_f2_0001";
    let key = ["okx", "real", &uid, "continuous_engine", intent_id]
        .iter()
        .map(|x| format!("{}:{}", x.len(), x))
        .collect::<String>();
    let reservation = key.clone();
    let (prepare_input, saved) = owner
        .execution
        .store
        .call(
            std::time::Instant::now() + Duration::from_secs(3),
            move |s| {
                let now = chrono::Utc::now().timestamp_millis();
                let definition = s.ensure_manual_instance(now)?;
                let input = T10_11PrepareInput {
                    decision: Decision {
                        decision_id: "decision_t11_f2".into(),
                        instance_id: None,
                        decision_at: if phase == T10_11UnsentPhase::F1 {
                            now - 2
                        } else {
                            now
                        },
                        input_digest: "b".repeat(64),
                        policy_revision: 2,
                        code_version: "fixture".into(),
                    },
                    intent: NewIntent {
                        intent_id: intent_id.into(),
                        idempotency_key: "command_t11_f2".into(),
                        request_hash: "b".repeat(64),
                        client_order_id: "c".repeat(32),
                        deadline: if phase == T10_11UnsentPhase::F1 {
                            now - 1
                        } else {
                            now + 30_000
                        },
                        plan_hash: "d".repeat(64),
                        policy_revision: 2,
                        risk_increase: true,
                        instrument_id: "BTC-USDT-SWAP".into(),
                        plan: IntentPlan {
                            strategy_definition_id: definition.definition_id,
                            strategy_definition_version: definition.version,
                            strategy_parameters_digest: definition.parameters_digest,
                            operation: "open".into(),
                            side: "buy".into(),
                            position_side: "long".into(),
                            quantity: Decimal::new(1, 1),
                            limit_price: Decimal::from(1000),
                            stop_price: Some(Decimal::from(900)),
                            strategy_instance: "manual".into(),
                            planned_loss_usd: Decimal::new(102, 3),
                            source_generation: 1,
                            private_source_generation: 1,
                            contract_value: Decimal::new(1, 2),
                            inverse: false,
                            settlement_currency: "USDT".into(),
                        },
                    },
                };
                let intent = if phase == T10_11UnsentPhase::F0 {
                    None
                } else {
                    Some(s.prepare(&input.decision, &input.intent)?)
                };
                if !phase.before_reserve() {
                    s.mark_reserve_attempt(intent_id, &reservation, now)?;
                }
                Ok((input, intent))
            },
        )
        .await
        .unwrap();
    if phase.before_reserve() {
        // Read-only Worker evidence. No RESERVE is sent by either F0 or F1.
        let inspect = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "inspect_t11_before_reserve".into(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Inspect {
                reservation_key: key.clone(),
                plan_sha256: prepare_input.intent.plan_hash.clone(),
            },
        };
        *T10_11_UNSENT_PROBE.lock().unwrap() = Some(T10_11UnsentProbe {
            phase,
            intent_id: intent_id.into(),
            reservation_key: key,
            source_floor_ms: before.source_as_of_ms,
            root,
            feed: feed.unwrap(),
            before_budget: before,
            before_sequence,
            prepare_input: Some(prepare_input),
        });
        let result = t10_12_await_recovery(&mut session, risk.request(inspect)).await;
        panic!("pre-reserve child must be OS-killed at its SQL boundary: {result:?}");
    }
    let saved = saved.unwrap();
    let peer = ctx.accounts.resolve("fixture-b").await.unwrap();
    let mut account_keys = vec![
        owner.configuration.account_key.wire(),
        peer.configuration.account_key.wire(),
    ];
    account_keys.sort();
    let at = chrono::Utc::now().timestamp_millis() as u64;
    let census = NativeRiskCensus {
        occupied: account_keys
            .iter()
            .map(|key| RiskAccountOccupancy {
                account_key: key.clone(),
                occupied_usdt: "0".into(),
                cumulative_loss_usd: "0".into(),
            })
            .collect(),
        account_keys,
        generated_at_ms: at,
        source_read_floor_ms: at,
        source_digest: "e".repeat(64),
        content_digest: "f".repeat(64),
        complete: true,
        native_account_evidence_complete: true,
        native_order_evidence_complete: true,
        native_ledger_evidence_complete: true,
    };
    let mut probe = T10_11UnsentProbe {
        phase,
        intent_id: intent_id.into(),
        reservation_key: key.clone(),
        source_floor_ms: at,
        root,
        feed: feed.unwrap(),
        before_budget: before,
        before_sequence,
        prepare_input: None,
    };
    let conversion = UsdConversion {
        usd_per_usdt: "1".into(),
        source_digest: "e".repeat(64),
        observed_at_ms: at,
    };
    let command = RiskCommand {
        schema: RISK_COMMAND_SCHEMA_V1.into(),
        command_id: "reserve_t11_f2".into(),
        installation_id: "installation_test".into(),
        action: RiskCommandAction::Reserve {
            venue: "okx".into(),
            account_uid: uid,
            run_id: "continuous_engine".into(),
            intent_id: saved.intent_id.clone(),
            plan_sha256: saved.plan_hash.clone(),
            strategy_id: "manual".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            requested_loss_usdt: "0.102".into(),
            requested_loss_usd: "0.102".into(),
            entry_upper_usd: Some("1.0005".into()),
            entry_proof_digest: Some("e".repeat(64)),
            policy_revision: 2,
            census: census.clone(),
            usd_conversion: conversion.clone(),
        },
    };
    if phase == T10_11UnsentPhase::F2 {
        *T10_11_UNSENT_PROBE.lock().unwrap() = Some(probe);
        let result = t10_12_await_recovery(&mut session, risk.request(command)).await;
        panic!("F2 child must be OS-killed before RESERVE delivery: {result:?}");
    }
    let held = t10_12_await_recovery(&mut session, risk.request(command))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held.status, RiskDecisionStatus::Held);
    let received = std::time::Instant::now();
    let receipt = CentralReceipt {
        reservation_key: key.clone(),
        account_key: owner.configuration.account_key.wire(),
        installation_id: "installation_test".into(),
        instance_id: None,
        intent_id: saved.intent_id.clone(),
        plan_hash: saved.plan_hash.clone(),
        policy_revision: 2,
        receipt_hash: format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(&held).unwrap())
        ),
        requested_loss_usd: Decimal::new(102, 3),
        conversion_digest: format!(
            "{:x}",
            sha2::Sha256::digest(serde_json::to_vec(&conversion).unwrap())
        ),
    };
    owner
        .execution
        .store
        .call(
            std::time::Instant::now() + Duration::from_secs(3),
            move |s| s.reserve(&receipt, chrono::Utc::now().timestamp_millis()),
        )
        .await
        .unwrap();
    if phase == T10_11UnsentPhase::F3 {
        // SQL has the authentic HELD receipt; no BEGIN_POST was requested.
        // INSPECT only proves the original Worker reserve still exists.
        let inspect = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "inspect_t11_f3_before_fence".into(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Inspect {
                reservation_key: key,
                plan_sha256: saved.plan_hash,
            },
        };
        *T10_11_UNSENT_PROBE.lock().unwrap() = Some(probe);
        let result = t10_12_await_recovery(&mut session, risk.request(inspect)).await;
        panic!("F3 child must be OS-killed after local RESERVED and before fence: {result:?}");
    }
    if phase == T10_11UnsentPhase::F4 {
        // Explicit boundary inputs, not a claim that the fixture performed a
        // full observer/final-quote admission. Worker validates the real CAS.
        let mut after_held = census;
        let now = chrono::Utc::now().timestamp_millis() as u64;
        after_held.generated_at_ms = now;
        after_held.source_read_floor_ms = now;
        after_held.source_digest = "a".repeat(64);
        probe.source_floor_ms = now;
        let elapsed = received.elapsed().as_millis() as u64;
        let causal = CausalPostProof {
            reserve_anchor: key.clone(),
            native_incarnation: "a".repeat(32),
            elapsed_to_proof_ms: elapsed,
            account_reads: after_held
                .account_keys
                .iter()
                .map(|account_key| PostAccountReadProof {
                    account_key: account_key.clone(),
                    private_generation: 1,
                    source_read_id: "b".repeat(64),
                    content_digest: after_held.content_digest.clone(),
                    started_after_held_ms: 0,
                    completed_after_held_ms: elapsed,
                })
                .collect(),
        };
        let fence = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "fence_t11_f4".into(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::BeginPost {
                reservation_key: key,
                plan_sha256: saved.plan_hash,
                policy_revision: 2,
                expected_risk_book_sequence: held.risk_book_sequence.unwrap(),
                source_proof_sha256: "a".repeat(64),
                census: after_held,
                causal_proof: causal,
            },
        };
        *T10_11_UNSENT_PROBE.lock().unwrap() = Some(probe);
        let result = t10_12_await_recovery(&mut session, risk.request(fence)).await;
        panic!("F4 child must be OS-killed before fence delivery: {result:?}");
    }
    *T10_11_UNSENT_PROBE.lock().unwrap() = Some(probe);
    let cancel = okx_protocol::trading::TradeRequest::Cancel {
        idempotency_key: "cancel_t11_f8".into(),
        reference: saved.intent_id,
    };
    let result = t10_12_await_recovery(
        &mut session,
        Box::pin(ctx.trade.trade("fixture-a", &cancel, None)),
    )
    .await;
    panic!("F8 child must be OS-killed before release delivery: {result:?}");
}

#[tokio::test]
#[ignore = "reopens original Native SQL after the parent proves OS termination"]
async fn native_crash_unsent_recover() {
    let handoff_path = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    let original = t10_12_barrier(&format!("{handoff_path}.t11-native-kill-ready")).await;
    let phase = T10_11UnsentPhase::selected();
    assert_eq!(original["phase"], phase.name());
    let root = std::path::PathBuf::from(original["root"].as_str().unwrap());
    assert!(
        root.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("okx-raw-native-session-")
    );
    let (ctx, reopened_root, risk, risk_rx, feed, _) =
        test_dispatch_with_root("installation_test", Some(root.clone())).await;
    assert_eq!(root, reopened_root);
    let id = original["sql"]["intent_id"].as_str().unwrap();
    let before = t10_11_sql_snapshot(&ctx.accounts, id).await;
    assert_eq!(
        before, original["sql"],
        "restart must reopen the exact original SQL facts"
    );
    let handoff = t10_12_barrier(&handoff_path).await;
    let (mut session, stop) = t10_11_start_session(ctx.clone(), risk_rx, &handoff).await;
    let mut read = summary("summary_t11_f2_reopened");
    read.installation_id = "installation_test".into();
    let worker_before = risk.request(read).await.unwrap();
    let mut old_fence_denial = None;
    if phase == T10_11UnsentPhase::F4 {
        let mut old_fence: RiskCommand =
            serde_json::from_value(original["command"].clone()).unwrap();
        old_fence.command_id = "fence_t11_replay_after_restart".into();
        let denied = risk.request(old_fence).await.unwrap();
        assert_eq!(
            denied.status,
            RiskDecisionStatus::Denied,
            "old fence must not authorize a new send"
        );
        let mut read = summary("summary_t11_after_old_fence");
        read.installation_id = "installation_test".into();
        let unchanged = risk.request(read).await.unwrap();
        assert_eq!(
            unchanged.risk_book_sequence,
            worker_before.risk_book_sequence
        );
        assert_eq!(unchanged.budget, worker_before.budget);
        old_fence_denial = Some(denied);
    }
    let owner = ctx.accounts.resolve("fixture-a").await.unwrap();
    let prepare_replay = if phase == T10_11UnsentPhase::F0 {
        let input: T10_11PrepareInput =
            serde_json::from_value(original["prepare_input"].clone()).unwrap();
        let replay = owner
            .execution
            .store
            .call(
                std::time::Instant::now() + Duration::from_secs(3),
                move |s| {
                    let first = s.prepare(&input.decision, &input.intent)?;
                    let again = s.prepare(&input.decision, &input.intent)?;
                    assert_eq!(
                        serde_json::to_value(&first).unwrap(),
                        serde_json::to_value(&again).unwrap()
                    );
                    assert_eq!(
                        s.by_idempotency(&input.intent.idempotency_key)?
                            .unwrap()
                            .intent_id,
                        first.intent_id
                    );
                    assert_eq!(
                        s.by_client_order_id(&input.intent.client_order_id)?
                            .unwrap()
                            .intent_id,
                        first.intent_id
                    );
                    Ok(serde_json::json!({"first":first,"replay":again}))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            t10_11_sql_snapshot(&ctx.accounts, id).await["state"],
            "PREPARED"
        );
        Some(replay)
    } else {
        None
    };
    let recovery = if phase == T10_11UnsentPhase::F1 {
        okx_protocol::trading::TradeRequest::Status {
            reference: id.into(),
        }
    } else {
        okx_protocol::trading::TradeRequest::Cancel {
            idempotency_key: "cancel_t11_f2".into(),
            reference: id.into(),
        }
    };
    let result = t10_12_await_recovery(
        &mut session,
        Box::pin(ctx.trade.trade("fixture-a", &recovery, None)),
    )
    .await
    .unwrap()
    .unwrap();
    let after = t10_11_sql_snapshot(&ctx.accounts, id).await;
    if phase.before_reserve() {
        assert_eq!(result["data"]["central_release"]["state"], "not_required");
        assert_eq!(
            result["data"]["central_release"]["proof"],
            "PRE_RESERVE_SQL"
        );
        assert_eq!(result["data"]["send_state"], "NOT_STARTED");
        assert!(after["reserve_attempt"].is_null());
        assert!(after["central_release"].is_null());
        assert_eq!(after["terminal_receipt"]["kind"], "REJECTED_UNSENT");
        if phase == T10_11UnsentPhase::F1 {
            assert_eq!(result["data"]["primary_reason"], "COMMAND_EXPIRED");
            assert_eq!(after["terminal_receipt"]["result_ref"], "COMMAND_EXPIRED");
        }
    } else {
        assert_eq!(result["data"]["central_release"]["state"], "verified");
        assert_eq!(after["central_release"]["kind"], "CENTRAL_RELEASE");
    }
    assert_eq!(after["state"], "REJECTED");
    assert_eq!(after["central_receipt"], original["sql"]["central_receipt"]);
    assert_eq!(after["exchange_order_id"], serde_json::Value::Null);
    let replay = ctx.trade.trade("fixture-a", &recovery, None).await.unwrap();
    assert_eq!(
        replay["data"]["central_release"],
        result["data"]["central_release"]
    );
    assert_eq!(t10_11_sql_snapshot(&ctx.accounts, id).await, after);
    let mut final_read = summary("summary_t11_f2_released");
    final_read.installation_id = "installation_test".into();
    let final_result = risk.request(final_read).await.unwrap();
    let mut expected = original["before_budget"].clone();
    expected["source_as_of_ms"] = original["source_floor_ms"].clone();
    assert_eq!(
        serde_json::to_value(&final_result.budget).unwrap(),
        expected
    );
    if phase.before_reserve() {
        assert_eq!(final_result.budget, worker_before.budget);
        assert_eq!(
            serde_json::to_value(final_result.risk_book_sequence).unwrap(),
            original["before_sequence"]
        );
        assert_eq!(
            final_result.risk_book_sequence,
            worker_before.risk_book_sequence
        );
    }
    if phase == T10_11UnsentPhase::F8 {
        assert_eq!(
            final_result.risk_book_sequence,
            worker_before.risk_book_sequence
        );
        assert_eq!(
            serde_json::to_value(final_result.risk_book_sequence).unwrap(),
            original["worker_result"]["risk_book_sequence"]
        );
        assert_eq!(
            final_result.budget, worker_before.budget,
            "INSPECT/SQL ack cannot debit or release twice"
        );
    }
    let posts = feed.unwrap().outbound_posts.lock().unwrap().len();
    assert_eq!(posts, 0);
    let account_key = owner.configuration.account_key.wire();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), session)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    ctx.accounts.shutdown().await;
    println!(
        "T10_11_NATIVE_UNSENT_RECOVERY_V1:{}",
        serde_json::json!({
            "schema":"okx.t10-11-native-unsent-recovery/v1","phase":phase.name(),
            "pid":std::process::id(),"root":root,"account_key":account_key,
            "before":before,"after":after,"worker_after":final_result,
            "worker_before":worker_before,"old_fence_denial":old_fence_denial,
            "venue_posts":posts,"original_sql_reopened":true,
            "public_recovery_replayed":true,"recovery_request":recovery,"recovery_result":result,
            "prepare_replay":prepare_replay,
        })
    );
}
