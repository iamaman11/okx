// Direct F2 boundary, not another complete T09 campaign. The parent kills this
// Native OS process after a genuine Worker HELD and before delivery to its caller.
// SQL prepare/census inputs are explicit fixture inputs; no Worker reply or
// recovery receipt is fabricated. Real WSS, account SQL and public Cancel/Status
// own the cross-process recovery.
struct T10_11F2Probe {
    root: std::path::PathBuf,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    before_budget: okx_protocol::risk_authority::RiskBudgetSnapshot,
}
static T10_11_F2_PROBE: std::sync::Mutex<Option<T10_11F2Probe>> =
    std::sync::Mutex::new(None);

pub(super) async fn t10_11_hold_reserve_reply(
    result: &RiskResult,
    command: Option<&RiskCommand>,
    ctx: &DispatchContext,
    generation: u64,
) {
    let Some(command) = command.filter(|c| c.command_id == "reserve_t11_f2") else {
        return;
    };
    let probe = T10_11_F2_PROBE.lock().unwrap().take();
    let Some(probe) = probe else { return };
    assert_eq!(result.status, RiskDecisionStatus::Held);
    let RiskCommandAction::Reserve { intent_id, .. } = &command.action else {
        panic!("F2 requires the original RESERVE")
    };
    let sql = t10_12_reserve_sql_snapshot(&ctx.accounts, intent_id).await;
    assert_eq!(sql["state"], "PREPARED");
    assert_eq!(sql["central_receipt"], serde_json::Value::Null);
    assert_eq!(sql["exchange_order_id"], serde_json::Value::Null);
    assert_eq!(sql["reservation_key"], result.reservation_key.as_deref().unwrap());
    let posts = probe.feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts, 0);
    let handoff = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    t10_12_publish(&format!("{handoff}.t11-native-kill-ready"), &json!({
        "schema":"okx.t10-11-native-f2-kill-boundary/v1",
        "phase":"F2","pid":std::process::id(),"root":probe.root,
        "generation":generation,"sql":sql,"command":command,
        "worker_held":result,"before_budget":probe.before_budget,
        "venue_posts":posts,
    }));
    // No timeout/Err/drop substitutes for the parent's actual OS termination.
    std::future::pending::<()>().await;
}

async fn t10_11_start_session(
    ctx: DispatchContext,
    mut risk_rx: mpsc::Receiver<OutboundRiskCommand>,
    handoff: &serde_json::Value,
) -> (tokio::task::JoinHandle<AgentResult<()>>, watch::Sender<bool>) {
    let socket = t10_12_connect(
        handoff["ws_url"].as_str().unwrap(), handoff["token"].as_str().unwrap(),
    ).await;
    let mut hello = test_hello();
    if let F::Hello { installation_id, .. } = &mut hello {
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
async fn native_crash_f2_boundary() {
    use okx_execution::{Decision, IntentPlan, NewIntent};
    use rust_decimal::Decimal;
    let handoff_path = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    let (ctx, root, risk, risk_rx, feed, _) = test_dispatch_for("installation_test").await;
    std::fs::write(format!("{handoff_path}.native-ready"), b"SQL_OWNERS_OPEN").unwrap();
    let handoff = t10_12_barrier(&handoff_path).await;
    let (mut session, _stop) = t10_11_start_session(ctx.clone(), risk_rx, &handoff).await;
    let mut summary_cmd = summary("summary_t11_f2_before");
    summary_cmd.installation_id = "installation_test".into();
    let before = risk.request(summary_cmd).await.unwrap().budget.unwrap();
    let owner = ctx.accounts.resolve("fixture-a").await.unwrap();
    let uid = owner.configuration.account_key.verified_uid.clone();
    let intent_id = "intent_t11_f2_0001";
    let key = ["okx", "real", &uid, "continuous_engine", intent_id]
        .iter().map(|x| format!("{}:{}", x.len(), x)).collect::<String>();
    let reservation = key.clone();
    let saved = owner.execution.store.call(std::time::Instant::now()+Duration::from_secs(3), move |s| {
        let now = chrono::Utc::now().timestamp_millis();
        let definition = s.ensure_manual_instance(now)?;
        let intent = s.prepare(&Decision {
            decision_id:"decision_t11_f2".into(),instance_id:None,decision_at:now,
            input_digest:"b".repeat(64),policy_revision:2,code_version:"fixture".into(),
        }, &NewIntent {
            intent_id:intent_id.into(),idempotency_key:"command_t11_f2".into(),
            request_hash:"b".repeat(64),client_order_id:"c".repeat(32),
            deadline:now+30_000,plan_hash:"d".repeat(64),policy_revision:2,
            risk_increase:true,instrument_id:"BTC-USDT-SWAP".into(),
            plan:IntentPlan {
                strategy_definition_id:definition.definition_id,
                strategy_definition_version:definition.version,
                strategy_parameters_digest:definition.parameters_digest,
                operation:"open".into(),side:"buy".into(),position_side:"long".into(),
                quantity:Decimal::new(1,1),limit_price:Decimal::from(1000),
                stop_price:Some(Decimal::from(900)),strategy_instance:"manual".into(),
                planned_loss_usd:Decimal::new(102,3),source_generation:1,
                private_source_generation:1,contract_value:Decimal::new(1,2),
                inverse:false,settlement_currency:"USDT".into(),
            },
        })?;
        s.mark_reserve_attempt(intent_id,&reservation,now)?;
        Ok(intent)
    }).await.unwrap();
    let peer = ctx.accounts.resolve("fixture-b").await.unwrap();
    let mut account_keys = vec![owner.configuration.account_key.wire(), peer.configuration.account_key.wire()];
    account_keys.sort();
    let at = chrono::Utc::now().timestamp_millis() as u64;
    let census = NativeRiskCensus {
        occupied:account_keys.iter().map(|key| RiskAccountOccupancy {
            account_key:key.clone(),occupied_usdt:"0".into(),cumulative_loss_usd:"0".into(),
        }).collect(),account_keys,generated_at_ms:at,source_read_floor_ms:at,
        source_digest:"e".repeat(64),content_digest:"f".repeat(64),complete:true,
        native_account_evidence_complete:true,native_order_evidence_complete:true,
        native_ledger_evidence_complete:true,
    };
    *T10_11_F2_PROBE.lock().unwrap() = Some(T10_11F2Probe {
        root,feed:feed.unwrap(),before_budget:before,
    });
    let command = RiskCommand {
        schema:RISK_COMMAND_SCHEMA_V1.into(),command_id:"reserve_t11_f2".into(),
        installation_id:"installation_test".into(),
        action:RiskCommandAction::Reserve {
            venue:"okx".into(),account_uid:uid,run_id:"continuous_engine".into(),
            intent_id:saved.intent_id,plan_sha256:saved.plan_hash,strategy_id:"manual".into(),
            instrument_id:"BTC-USDT-SWAP".into(),requested_loss_usdt:"0.102".into(),
            requested_loss_usd:"0.102".into(),entry_upper_usd:Some("1.0005".into()),
            entry_proof_digest:Some("e".repeat(64)),policy_revision:2,census,
            usd_conversion:UsdConversion {usd_per_usdt:"1".into(),source_digest:"e".repeat(64),observed_at_ms:at},
        },
    };
    let result = t10_12_await_recovery(&mut session, risk.request(command)).await;
    panic!("F2 child must be OS-killed before RESERVE delivery: {result:?}");
}

#[tokio::test]
#[ignore = "reopens original Native SQL after the parent proves OS termination"]
async fn native_crash_f2_recover() {
    let handoff_path = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    let original = t10_12_barrier(&format!("{handoff_path}.t11-native-kill-ready")).await;
    let root = std::path::PathBuf::from(original["root"].as_str().unwrap());
    assert!(root.file_name().unwrap().to_string_lossy().starts_with("okx-raw-native-session-"));
    let (ctx, reopened_root, risk, risk_rx, feed, _) =
        test_dispatch_with_root("installation_test",Some(root.clone())).await;
    assert_eq!(root,reopened_root);
    let id = original["sql"]["intent_id"].as_str().unwrap();
    let before = t10_12_reserve_sql_snapshot(&ctx.accounts,id).await;
    assert_eq!(before, original["sql"], "restart must reopen the exact original SQL facts");
    let handoff = t10_12_barrier(&handoff_path).await;
    let (mut session, stop) = t10_11_start_session(ctx.clone(),risk_rx,&handoff).await;
    let mut read = summary("summary_t11_f2_reopened");
    read.installation_id = "installation_test".into();
    risk.request(read).await.unwrap();
    let owner = ctx.accounts.resolve("fixture-a").await.unwrap();
    let cancel = okx_protocol::trading::TradeRequest::Cancel {
        idempotency_key:"cancel_t11_f2".into(),reference:id.into(),
    };
    let result = t10_12_await_recovery(&mut session,
        Box::pin(ctx.trade.trade("fixture-a",&cancel,None))).await.unwrap().unwrap();
    assert_eq!(result["data"]["central_release"]["state"],"verified");
    let after = t10_12_reserve_sql_snapshot(&ctx.accounts,id).await;
    assert_eq!(after["state"],"REJECTED");
    assert_eq!(after["central_release"]["kind"],"CENTRAL_RELEASE");
    assert_eq!(after["central_receipt"],serde_json::Value::Null);
    assert_eq!(after["exchange_order_id"],serde_json::Value::Null);
    let replay = ctx.trade.trade("fixture-a",&cancel,None).await.unwrap();
    assert_eq!(replay["data"]["central_release"]["state"],"verified");
    assert_eq!(t10_12_reserve_sql_snapshot(&ctx.accounts,id).await,after);
    let mut final_read = summary("summary_t11_f2_released");
    final_read.installation_id = "installation_test".into();
    let final_result = risk.request(final_read).await.unwrap();
    let mut expected = original["before_budget"].clone();
    expected["source_as_of_ms"] = original["command"]["action"]["census"]["source_read_floor_ms"].clone();
    assert_eq!(serde_json::to_value(&final_result.budget).unwrap(),expected);
    let posts = feed.unwrap().outbound_posts.lock().unwrap().len();
    assert_eq!(posts,0);
    let account_key = owner.configuration.account_key.wire();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5),session).await.unwrap().unwrap().unwrap();
    ctx.accounts.shutdown().await;
    println!("T10_11_NATIVE_F2_RECOVERY_V1:{}",json!({
        "schema":"okx.t10-11-native-f2-recovery/v1","phase":"F2",
        "pid":std::process::id(),"root":root,"account_key":account_key,
        "before":before,"after":after,"worker_after":final_result,
        "venue_posts":posts,"original_sql_reopened":true,
        "public_cancel_replayed":true,
    }));
}
