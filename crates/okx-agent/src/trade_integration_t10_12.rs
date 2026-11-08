// Included only in the existing Native transport integration tests. Real
// AccountRuntimeRegistry, NativeRiskClient and run_connected_session remain
// the financial and receive owners. Only message delivery/lifecycle is faulty.
static T10_12_ARM_SETTLEMENT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
// A separate Native Execute on the same SQL owner as the Worker RESERVE
// crash. It must observe PREPARED+attempt, not accept a lost HELD reply.
static T10_12_ARM_RESERVE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static T10_12_RESERVE_WAS_HELD: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static T10_12_HELD_RESERVE_REPLY: std::sync::Mutex<Option<T10_12HeldReply>> =
    std::sync::Mutex::new(None);
static T10_12_HELD_REPLY: std::sync::Mutex<Option<T10_12HeldReply>> =
    std::sync::Mutex::new(None);

#[derive(Clone)]
struct T10_12HeldReply {
    raw: Message,
    command: RiskCommand,
    result: RiskResult,
    session_id: String,
    generation: u64,
    sql_before: serde_json::Value,
}

fn t10_12_enabled() -> bool {
    std::env::var("OKX_T10_12_NATIVE_WORKER_CRASH").as_deref() == Ok("1")
}
fn t10_12_reserve_enabled() -> bool {
    std::env::var("OKX_T10_12_NATIVE_RESERVE_CRASH").as_deref() == Ok("1")
}

// Waiting for the public operation must also observe its transport owner.
// A detached supervisor panic is a failure now, not a Status timeout later.
// This helper observes lifetimes only; it cannot fabricate a risk/SQL result.
async fn t10_12_await_recovery<T>(
    session: &mut tokio::task::JoinHandle<AgentResult<()>>,
    operation: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    tokio::select! {
        biased;
        ended = session => Err(match ended {
            Ok(Ok(())) => "RECOVERY_SESSION_STOPPED_BEFORE_COMPLETION".into(),
            Ok(Err(error)) => format!("RECOVERY_SESSION_FAILED: {error}"),
            Err(error) => format!("RECOVERY_SESSION_TASK_FAILED: {error}"),
        }),
        value = operation => Ok(value),
    }
}

#[tokio::test]
async fn recovery_wait_reports_owner_failure_without_waiting_for_status_timeout() {
    let mut session = tokio::spawn(async { Err(transport("TEST_RECEIVE_BOUNDARY_FAILED")) });
    let result = tokio::time::timeout(Duration::from_secs(1), t10_12_await_recovery(
        &mut session, std::future::pending::<()>(),
    )).await.expect("owner failure must not wait for public Status");
    assert!(result.unwrap_err().contains("TEST_RECEIVE_BOUNDARY_FAILED"));
}

#[tokio::test]
async fn recovery_wait_does_not_cancel_live_session_when_operation_finishes() {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut session = tokio::spawn(async { let _ = stopped.await; Ok(()) });
    assert_eq!(t10_12_await_recovery(&mut session, async { 17 }).await.unwrap(), 17);
    assert!(!session.is_finished());
    stop.send(()).unwrap();
    session.await.unwrap().unwrap();
}

#[tokio::test]
async fn recovery_wait_reports_task_panic() {
    let mut session = tokio::spawn(async {
        panic!("TEST_RECOVERY_OWNER_PANIC");
        #[allow(unreachable_code)]
        Ok(())
    });
    let result = t10_12_await_recovery(&mut session, std::future::pending::<()>()).await;
    assert!(result.unwrap_err().contains("TEST_RECOVERY_OWNER_PANIC"));
}

async fn t10_12_barrier(path: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            match std::fs::read(path) {
                Ok(bytes) => return serde_json::from_slice(&bytes).expect("T10-12 complete barrier"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound =>
                    tokio::time::sleep(Duration::from_millis(25)).await,
                Err(error) => panic!("T10-12 barrier unavailable: {error}"),
            }
        }
    }).await.expect("T10-12 bounded lifecycle barrier")
}

fn t10_12_publish(path: &str, value: &serde_json::Value) {
    let pending = format!("{path}.pending");
    std::fs::write(&pending, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::rename(pending, path).unwrap();
}

async fn t10_12_sql_snapshot(accounts: &Arc<AccountRuntimeRegistry>) -> serde_json::Value {
    let owner = accounts.resolve("fixture-a").await.unwrap();
    owner.execution.store.call(std::time::Instant::now()+Duration::from_secs(3), |s| {
        let i=s.by_idempotency("t09_real_public_execute_0001")?
            .ok_or(okx_execution::StoreError::Invalid("T10_12_ORIGINAL_INTENT_MISSING"))?;
        let raw=s.operation(&format!("entry-partial-head:{}",i.intent_id))?
            .ok_or(okx_execution::StoreError::Invalid("T10_12_SQL_SETTLEMENT_PROOF_MISSING"))?;
        let proof:okx_execution::EntrySettlementEvidence=serde_json::from_str(&raw.result_ref)
            .map_err(|_|okx_execution::StoreError::Invalid("T10_12_SQL_PROOF_CORRUPT"))?;
        let fill=s.intent_fill_cost_evidence(&i.intent_id,proof.source_covered_through_ms)?;
        let bills=s.intent_fee_bill_evidence(&i.intent_id,proof.source_covered_through_ms)?;
        let ack=s.operation(&format!("entry-partial-settled:{}:r{}",
            i.intent_id,proof.settlement_revision))?.is_some();
        Ok(serde_json::json!({"intent_id":i.intent_id,"plan_hash":i.plan_hash,
            "request_hash":i.request_hash,"intent_revision":i.revision,
            "intent_state":i.state.as_str(),"exchange_order_id":i.exchange_order_id,
            "proof":proof,"local_settlement_ack":ack,
            "active_reservation":s.active_central_receipt(&i.intent_id)?,
            "fill_count":fill.fill_count,"bill_count":bills.linked_bill_count,
            "quantity":fill.filled_quantity.normalize().to_string(),
            "notional_usdt":fill.gross_notional_usdt.normalize().to_string(),
            "fee_usdt":bills.conservative_verified_fee_usdt.normalize().to_string(),
            "bill_digest":bills.source_digest}))
    }).await.expect("T10-12 same original Native SQL owner")
}


// This is read-only evidence from the actual Native account SQLite owner.
// The RISK_RESERVE_ATTEMPT receipt is committed BEFORE the outbound WSS
// RESERVE; neither a Worker HELD nor a lost response may fabricate a local
// central receipt or authorize a venue send.
async fn t10_12_reserve_sql_snapshot(
    accounts: &Arc<AccountRuntimeRegistry>,
    id: &str,
) -> serde_json::Value {
    let owner = accounts.resolve("fixture-a").await.unwrap();
    let id = id.to_string();
    owner
        .execution
        .store
        .call(std::time::Instant::now() + Duration::from_secs(3), move |s| {
            let intent = s.intent(&id)?.ok_or(okx_execution::StoreError::Invalid(
                "T10_12_RESERVE_SQL_INTENT_MISSING",
            ))?;
            let attempted = s
                .operation(&format!("risk-reserve:{id}"))?
                .ok_or(okx_execution::StoreError::Invalid(
                    "T10_12_RESERVE_SQL_ATTEMPT_MISSING",
                ))?;
            let terminal = s.operation(&format!("terminal:{id}"))?;
            let local = s.central_receipt(&id)?;
            let release = s.operation(&format!("central-release:{}", attempted.result_ref))?;
            Ok(serde_json::json!({
                "intent_id":intent.intent_id,
                "request_hash":intent.request_hash,
                "plan_hash":intent.plan_hash,
                "state":intent.state.as_str(),
                "exchange_order_id":intent.exchange_order_id,
                "attempt_kind":attempted.kind,
                "attempt_request_hash":attempted.request_hash,
                "reservation_key":attempted.result_ref,
                "central_receipt":local,
                "terminal_receipt":terminal,
                "central_release":release,
            }))
        })
        .await
        .expect("T10-12 must read the same original Native SQL owner")
}

pub(super) async fn t10_12_hold_settlement_reply(
    raw: &Message, result: &RiskResult, command: Option<&RiskCommand>,
    ctx: &DispatchContext, session_id: &str, generation: u64,
) -> AgentResult<()> {
    let Some(command)=command else {return Ok(())};
    // The original Native SQL owner has already written the reserve-attempt
    // identity. This hook only WITHHOLDS the authentic committed Worker HELD
    // response and kills the Worker process before any Native reserve receipt.
    if let RiskCommandAction::Reserve { intent_id, plan_sha256, .. } = &command.action
        && t10_12_reserve_enabled()
        && result.status == RiskDecisionStatus::Held
        && T10_12_ARM_RESERVE.swap(false, std::sync::atomic::Ordering::SeqCst)
    {
            let sql_before = t10_12_reserve_sql_snapshot(&ctx.accounts, intent_id).await;
            assert_eq!(sql_before["state"], "PREPARED");
            assert_eq!(sql_before["plan_hash"], plan_sha256.as_str());
            assert_eq!(sql_before["attempt_kind"], "RISK_RESERVE_ATTEMPT");
            assert_eq!(sql_before["attempt_request_hash"], sql_before["request_hash"]);
            assert_eq!(sql_before["central_receipt"], serde_json::Value::Null);
            assert_eq!(sql_before["exchange_order_id"], serde_json::Value::Null);
            assert_eq!(
                sql_before["reservation_key"],
                result.reservation_key.as_deref().expect("Worker HELD address")
            );
            *T10_12_HELD_RESERVE_REPLY.lock().unwrap() = Some(T10_12HeldReply {
                raw: raw.clone(),
                command: command.clone(),
                result: result.clone(),
                session_id: session_id.into(),
                generation,
                sql_before,
            });
            T10_12_RESERVE_WAS_HELD.store(true, std::sync::atomic::Ordering::SeqCst);
            let path = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
            t10_12_publish(
                &format!("{path}.t12-worker-kill-ready"),
                &serde_json::json!({
                    "schema":"okx.t10-12-native-held-worker-response/v1",
                    "action":"RESERVE","status":"HELD",
                    "command_id":command.command_id,
                    "reservation_key":result.reservation_key,
                    "risk_book_sequence":result.risk_book_sequence,
                    "connection_generation":generation
                }),
            );
            let killed = t10_12_barrier(&format!("{path}.t12-worker-killed")).await;
            assert_eq!(killed["os_killed"], true);
            return Err(transport("T10_12_WORKER_OS_KILLED_AFTER_RESERVE_COMMIT"));
    }
    if !matches!(command.action,RiskCommandAction::SettleEntry{..}) ||
        result.status!=RiskDecisionStatus::Updated ||
        !T10_12_ARM_SETTLEMENT.swap(false,std::sync::atomic::Ordering::SeqCst) {
        return Ok(());
    }
    let sql_before=t10_12_sql_snapshot(&ctx.accounts).await;
    assert_eq!(sql_before["local_settlement_ack"],false,
        "capture must precede original Native SQL acknowledgement");
    let proof=&sql_before["proof"];
    if let RiskCommandAction::SettleEntry {reservation_key,plan_sha256,
        cumulative_entry_usd,settlement_revision,source_digest,..}=&command.action {
        assert_eq!(reservation_key,proof["reservation_key"].as_str().unwrap());
        assert_eq!(plan_sha256,proof["plan_hash"].as_str().unwrap());
        assert_eq!(cumulative_entry_usd,"1.0005");
        assert_eq!(*settlement_revision,proof["settlement_revision"].as_u64().unwrap());
        assert_eq!(source_digest,proof["source_digest"].as_str().unwrap());
    }
    *T10_12_HELD_REPLY.lock().unwrap()=Some(T10_12HeldReply {
        raw:raw.clone(),command:command.clone(),result:result.clone(),
        session_id:session_id.into(),generation,sql_before,
    });
    let path=std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
    // No raw response or credential crosses the filesystem/log boundary.
    t10_12_publish(&format!("{path}.t12-worker-kill-ready"),&serde_json::json!({
        "schema":"okx.t10-12-native-held-worker-response/v1",
        "action":"SETTLE_ENTRY","status":"UPDATED",
        "command_id":command.command_id,"reservation_key":result.reservation_key,
        "risk_book_sequence":result.risk_book_sequence,
        "connection_generation":generation,
    }));
    let killed=t10_12_barrier(&format!("{path}.t12-worker-killed")).await;
    assert_eq!(killed["os_killed"],true);
    Err(transport("T10_12_WORKER_OS_KILLED_AFTER_SETTLEMENT_COMMIT"))
}

// Delays the ORIGINAL immutable Worker response across the real OS restart.
// The next connection uses an actual socket/HelloAck, not a fabricated session.
struct T10_12OldReply<S> {
    socket:S,
    old:Option<Message>,
    after_command_id:String,
    after_outbound:bool,
}
impl<S> T10_12OldReply<S> {
    fn after_command(socket:S, old:Message, command_id:&str) -> Self {
        Self {socket, old:Some(old), after_command_id:command_id.into(), after_outbound:false}
    }
}
impl<S> Stream for T10_12OldReply<S>
where S:Stream<Item=Result<Message,tokio_tungstenite::tungstenite::Error>>+Unpin {
    type Item=Result<Message,tokio_tungstenite::tungstenite::Error>;
    fn poll_next(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<Option<Self::Item>> {
        if self.after_outbound && let Some(old)=self.old.take(){
            return Poll::Ready(Some(Ok(old)));
        }
        Pin::new(&mut self.socket).poll_next(cx)
    }
}
impl<S> Sink<Message> for T10_12OldReply<S>
where S:Sink<Message,Error=tokio_tungstenite::tungstenite::Error>+Unpin {
    type Error=tokio_tungstenite::tungstenite::Error;
    fn poll_ready(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<Result<(),Self::Error>> {
        Pin::new(&mut self.socket).poll_ready(cx)
    }
    fn start_send(mut self:Pin<&mut Self>,message:Message)->Result<(),Self::Error> {
        let named = matches!(frame(message.clone()),Ok(F::RiskControl{command,..})
            if command.command_id == self.after_command_id);
        Pin::new(&mut self.socket).start_send(message)?;
        // Maintenance and public Status share this socket. They cannot arm
        // the fault intended for the probe's own pending response waiter.
        self.after_outbound |= named;
        Ok(())
    }
    fn poll_flush(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<Result<(),Self::Error>> {
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_close(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<Result<(),Self::Error>> {
        Pin::new(&mut self.socket).poll_close(cx)
    }
}

async fn t10_12_connect(url:&str,token:&str) ->
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    assert!(url.starts_with("ws://127.0.0.1:"));
    let mut request=url.into_client_request().unwrap();
    request.headers_mut().insert(AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap());
    tokio::time::timeout(Duration::from_secs(10),connect_async(request))
        .await.unwrap().unwrap().0
}

#[tokio::test]
async fn recovery_old_reply_waits_for_its_command_not_background_traffic() {
    let (ctx, root, client, mut risk_rx) = test_dispatch().await;
    let (input_tx, input_rx) = mpsc::unbounded_channel();
    let (output_tx, mut output_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let session_id = "session_recovery_boundary".to_owned();
    let probe = summary("risk_named_recovery_probe");
    let old = wire(F::RiskControlResult {
        schema:SCHEMA.into(), session_id:session_id.clone(), connection_generation:11,
        result:denied("risk_original_prior_generation"),
    });
    let socket = T10_12OldReply::after_command(
        RawFrames {inbound:input_rx, outbound:output_tx}, old, &probe.command_id,
    );
    let session = tokio::spawn(async move {
        run_connected_session(socket,test_hello(),ctx,&mut risk_rx,shutdown_rx).await
    });
    let exercise = async {
        assert!(matches!(parse(output_rx.recv().await.unwrap()), F::Hello{..}));
        input_tx.send(Ok(wire(F::HelloAck {
            schema:SCHEMA.into(), session_id:session_id.clone(), connection_generation:12,
        }))).unwrap();
        // Force the competing request before the probe, without sleeps.
        let background = tokio::spawn({let client=client.clone(); async move {
            client.request(summary("risk_background_before_probe")).await
        }});
        let observed = loop {
            if let F::RiskControl{command,..}=parse(output_rx.recv().await.unwrap()) {
                break command;
            }
        };
        assert_eq!(observed.command_id,"risk_background_before_probe");
        input_tx.send(Ok(wire(F::RiskControlResult {
            schema:SCHEMA.into(), session_id, connection_generation:12,
            result:denied(&observed.command_id),
        }))).unwrap();
        // With the previous any-outbound trigger this fails: the background
        // command receives the disconnect and the probe never owned it.
        assert_eq!(background.await.unwrap().unwrap().command_id,observed.command_id);
        let unresolved = client.request(probe).await;
        assert_eq!(unresolved.unwrap_err(),"RISK_TRANSPORT_LOST_DO_NOT_REPLAY");
        let error = session.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("SESSION_GENERATION_MISMATCH"));
    };
    tokio::time::timeout(Duration::from_secs(3),exercise).await
        .expect("direct receive-boundary test must finish without a campaign");
    drop(client);
    drop(input_tx);
    drop(output_rx);
    drop(shutdown_tx);
    std::fs::remove_dir_all(root).unwrap();
}

async fn t10_12_supervise<S>(socket:S,hello:F,ctx:DispatchContext,
    risk_rx:&mut mpsc::Receiver<OutboundRiskCommand>,shutdown:watch::Receiver<bool>,
    risk:NativeRiskClient,authentication: (String,String)) -> AgentResult<()>
where S:Stream<Item=Result<Message,tokio_tungstenite::tungstenite::Error>>+
    Sink<Message,Error=tokio_tungstenite::tungstenite::Error>+Unpin+Send {
    let (token,handoff)=authentication;
    let first=run_connected_session(socket,hello.clone(),ctx.clone(),risk_rx,shutdown.clone()).await;
    assert!(first.unwrap_err().to_string().contains(
        "T10_12_WORKER_OS_KILLED_AFTER_SETTLEMENT_COMMIT"));
    let held=T10_12_HELD_REPLY.lock().unwrap().take().expect("authentic held settlement reply");
    let restarted=t10_12_barrier(&format!("{handoff}.t12-worker-restarted")).await;
    let url=restarted["ws_url"].as_str().unwrap();
    let socket=t10_12_connect(url,&token).await;
    let mut probe=summary("risk_t10_12_new_generation_pending");
    probe.installation_id="installation_test".into();
    let old=T10_12OldReply::after_command(socket,held.raw.clone(),&probe.command_id);
    let (rejected,pending)=tokio::join!(
        run_connected_session(old,hello.clone(),ctx.clone(),risk_rx,shutdown.clone()),
        risk.request(probe));
    assert!(rejected.unwrap_err().to_string().contains("SESSION_GENERATION_MISMATCH"),
        "the unchanged production receive path must reject authentic old bytes");
    assert_eq!(pending.unwrap_err(),"RISK_TRANSPORT_LOST_DO_NOT_REPLAY");
    let sql_after_rejection=t10_12_sql_snapshot(&ctx.accounts).await;
    assert_eq!(sql_after_rejection,held.sql_before,
        "authentic old reply cannot acknowledge or mutate original account SQL");

    let socket=t10_12_connect(url,&token).await;
    let active=run_connected_session(socket,hello,ctx.clone(),risk_rx,shutdown);
    tokio::pin!(active);
    let recover=async {
        let mut before=summary("risk_t10_12_recovered_before_replay");
        before.installation_id="installation_test".into();
        let before=risk.request(before).await.unwrap();
        assert_eq!(before.risk_book_sequence,held.result.risk_book_sequence);
        let entry=before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(entry.consumed_usd,"1.0005");
        let RiskCommandAction::SettleEntry {reservation_key,plan_sha256,..}=&held.command.action
            else {panic!("original SETTLE_ENTRY command required")};
        let inspect=risk.request(RiskCommand {schema:RISK_COMMAND_SCHEMA_V1.into(),
            command_id:"inspect_t10_12_original_after_worker_crash".into(),
            installation_id:"installation_test".into(),
            action:RiskCommandAction::Inspect{reservation_key:reservation_key.clone(),
                plan_sha256:plan_sha256.clone()}}).await.unwrap();
        assert_eq!(inspect.status,RiskDecisionStatus::Observed);
        let replay=risk.request(held.command.clone()).await.unwrap();
        assert_eq!(replay.status,RiskDecisionStatus::Updated);
        assert_eq!(replay.risk_book_sequence,before.risk_book_sequence);
        let mut after=summary("risk_t10_12_recovered_after_replay");
        after.installation_id="installation_test".into();
        let after=risk.request(after).await.unwrap();
        assert_eq!(after.risk_book_sequence,before.risk_book_sequence);
        assert_eq!(serde_json::to_value(&after.budget).unwrap(),
            serde_json::to_value(&before.budget).unwrap());
        let sql=t10_12_sql_snapshot(&ctx.accounts).await;
        assert_eq!(sql,held.sql_before,"Worker replay alone cannot synthesize a local SQL ACK");
        let witness=serde_json::json!({
            "schema":"okx.t10-12-native-same-sql-recovery/v1",
            "old_reply_rejected":true,"reason":"SESSION_GENERATION_MISMATCH",
            "original_session_id":held.session_id,"original_generation":held.generation,
            "original_command_id":held.command.command_id,
            "original_reservation_key":reservation_key,
            "sql_before":held.sql_before,"sql_after_old_reply":sql_after_rejection,
            "sql_after_worker_replay":sql,"worker_before":before.budget,
            "worker_after":after.budget,"worker_sequence":after.risk_book_sequence,
            "exact_settlement_replay_status":"UPDATED"});
        t10_12_publish(&format!("{handoff}.t12-native-recovered"),&witness);
        println!("T10_12_NATIVE_SAME_SQL_RECOVERY_V1:{witness}");
    };
    tokio::select! {
        result=&mut active => panic!("T10-12 recovered transport exited too early: {result:?}"),
        ()=recover => {}
    }
    active.await
}

async fn t10_12_reserve_supervise<S>(
    socket: S,
    hello: F,
    ctx: DispatchContext,
    risk_rx: &mut mpsc::Receiver<OutboundRiskCommand>,
    shutdown: watch::Receiver<bool>,
    risk: NativeRiskClient,
    authentication: (String, String),
) -> AgentResult<()>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin
        + Send,
{
    let (token, handoff) = authentication;
    let first = run_connected_session(socket, hello.clone(), ctx.clone(), risk_rx, shutdown.clone()).await;
    assert!(first.unwrap_err().to_string().contains(
        "T10_12_WORKER_OS_KILLED_AFTER_RESERVE_COMMIT"
    ));
    let held = T10_12_HELD_RESERVE_REPLY.lock().unwrap().take()
        .expect("authentic committed HELD reply withheld before Native receipt");
    let intent_id = held.sql_before["intent_id"].as_str().unwrap().to_owned();
    // Keep the Worker OFFLINE until the original Native Execute has recorded
    // a durable no-send rejection. Otherwise a newly reconnected INSPECT
    // could let a still-running initial Execute progress to POST.
    let rejected = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let sql = t10_12_reserve_sql_snapshot(&ctx.accounts, &intent_id).await;
            if sql["state"] == "REJECTED" {
                break sql;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("Native must durably record no-send before Worker reconnect");
    assert_eq!(rejected["central_receipt"], serde_json::Value::Null);
    assert_eq!(rejected["exchange_order_id"], serde_json::Value::Null);
    assert_eq!(rejected["reservation_key"], held.sql_before["reservation_key"]);
    assert_eq!(rejected["terminal_receipt"]["kind"], "REJECTED_UNSENT");
    t10_12_publish(&format!("{handoff}.t12-native-no-send"), &serde_json::json!({
        "intent_id":intent_id,"original_reservation_key":rejected["reservation_key"],
        "native_sql_no_send":true,
    }));
    eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:NO_SEND_RECORDED");
    let restarted = t10_12_barrier(&format!("{handoff}.t12-worker-restarted")).await;
    eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:WORKER_RESTART_REPORTED");
    let url = restarted["ws_url"].as_str().unwrap();
    let socket = t10_12_connect(url, &token).await;
    let mut probe = summary("risk_t10_12_reserve_old_reply");
    probe.installation_id = "installation_test".into();
    let old = T10_12OldReply::after_command(socket,held.raw.clone(),&probe.command_id);
    eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:STALE_REPLY_PROBE_STARTED");
    let (rejected_frame, pending) = tokio::join!(
        run_connected_session(old, hello.clone(), ctx.clone(), risk_rx, shutdown.clone()),
        risk.request(probe)
    );
    assert!(rejected_frame.unwrap_err().to_string().contains("SESSION_GENERATION_MISMATCH"),
        "old Worker generation must not authorize an SQL reserve receipt");
    assert_eq!(pending.unwrap_err(), "RISK_TRANSPORT_LOST_DO_NOT_REPLAY");
    eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:STALE_REPLY_REJECTED");
    let after_old = t10_12_reserve_sql_snapshot(&ctx.accounts, &intent_id).await;
    assert_eq!(after_old["central_receipt"], serde_json::Value::Null);
    assert_eq!(after_old["exchange_order_id"], serde_json::Value::Null);
    assert_eq!(after_old["attempt_request_hash"], held.sql_before["request_hash"]);

    let socket = t10_12_connect(url, &token).await;
    eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:RECOVERY_SOCKET_CONNECTED");
    let active = run_connected_session(socket, hello, ctx.clone(), risk_rx, shutdown);
    tokio::pin!(active);
    let recover = async {
        let mut before_cmd = summary("risk_t10_12_reserve_restart_before");
        before_cmd.installation_id = "installation_test".into();
        let before = risk.request(before_cmd).await.unwrap();
        eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:RECOVERY_SUMMARY_RECEIVED");
        let reservation_key = held.result.reservation_key.as_ref().unwrap().clone();
        let RiskCommandAction::Reserve { plan_sha256, .. } = &held.command.action
            else { panic!("original RESERVE required") };
        let inspect = risk.request(RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id:"inspect_t10_12_reserve_original_after_os_kill".into(),
            installation_id:"installation_test".into(),
            action:RiskCommandAction::Inspect {
                reservation_key:reservation_key.clone(),
                plan_sha256:plan_sha256.clone(),
            },
        }).await.unwrap();
        eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:ORIGINAL_RESERVATION_INSPECTED");
        assert_eq!(inspect.status, RiskDecisionStatus::Observed);
        assert!(matches!(inspect.reservation_state,
            Some(RiskReservationState::Held | RiskReservationState::Released)),
            "Worker must preserve the original risk address, even if a no-send release raced");
        let mut after_cmd = summary("risk_t10_12_reserve_restart_after");
        after_cmd.installation_id = "installation_test".into();
        let after = risk.request(after_cmd).await.unwrap();
        assert_eq!(serde_json::to_value(&before.budget).unwrap(),
            serde_json::to_value(&after.budget).unwrap(),
            "read-only INSPECT cannot create a second grant or consume entry spend");
        let sql = t10_12_reserve_sql_snapshot(&ctx.accounts, &intent_id).await;
        assert_eq!(sql["state"], "REJECTED");
        assert_eq!(sql["central_receipt"], serde_json::Value::Null);
        assert_eq!(sql["exchange_order_id"], serde_json::Value::Null);
        let witness = serde_json::json!({
            "schema":"okx.t10-12-native-reserve-same-sql/v1",
            "original_command_id":held.command.command_id,
            "original_session_id":held.session_id,
            "original_generation":held.generation,
            "original_reservation_key":reservation_key,
            "original_sql":held.sql_before,
            "sql_after_no_send":rejected,
            "sql_after_old_reply":after_old,
            "sql_after_worker_inspect":sql,
            "worker_before_inspect":before.budget,
            "worker_after_inspect":after.budget,
            "old_reply_rejected":true,
            "inspected_state":format!("{:?}",inspect.reservation_state),
            "no_second_grant":true,
        });
        t10_12_publish(&format!("{handoff}.t12-native-recovered"), &witness);
        eprintln!("T10_12_RESERVE_RECOVERY_STAGE_V1:RECOVERY_WITNESS_PUBLISHED");
        println!("T10_12_NATIVE_RESERVE_WORKER_RECOVERY_V1:{witness}");
    };
    tokio::select! {
        result=&mut active => panic!("T10-12 reserve recovery session exited prematurely: {result:?}"),
        ()=recover => {}
    }
    active.await
}
