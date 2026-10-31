//! One authenticated connection multiplexes bounded independent native work.
use crate::{
    AgentError, AgentResult,
    query::{DispatchContext, dispatch_with_governance, response},
    risk_bridge::OutboundRiskCommand,
};
use futures_util::{
    FutureExt, Sink, SinkExt, Stream, StreamExt, future::BoxFuture, stream::FuturesUnordered,
};
use okx_protocol::{
    AgentOperation, DIRECT_TRANSPORT_FRAME_SCHEMA_V2 as SCHEMA, DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES,
    DirectRuntimeProfile, DirectTransportFrame as F,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    io::Read,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{AUTHORIZATION, HeaderValue},
    },
};
use zeroize::Zeroizing;

pub struct CloudflareTransportConfig {
    ws_url: String,
    runtime_id: String,
    bearer_token: Zeroizing<String>,
}
impl CloudflareTransportConfig {
    pub fn new(
        ws_url: String,
        runtime_id: String,
        bearer_token: Zeroizing<String>,
        profile: DirectRuntimeProfile,
    ) -> AgentResult<Self> {
        if !ws_url.starts_with("wss://") || ws_url.contains('@') || ws_url.contains('#') {
            return Err(AgentError::InvalidCloudflareWsUrl);
        }
        if runtime_id.is_empty()
            || runtime_id.len() > 64
            || !runtime_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(AgentError::InvalidCloudflareRuntimeId);
        }
        if profile != DirectRuntimeProfile::Engine {
            return Err(AgentError::InvalidCloudflareRuntimeId);
        }
        Ok(Self {
            ws_url,
            runtime_id,
            bearer_token,
        })
    }
    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }
}
/// Re-hash the running executable on every authenticated session. This is a
/// *claim*, never independent release authorization; Worker must compare it
/// to fresh, owner-authored native CONTROL evidence.
fn current_binary_sha256() -> AgentResult<String> {
    let executable = std::env::current_exe().map_err(|_| transport("EXE_IDENTITY_UNAVAILABLE"))?;
    let mut file =
        std::fs::File::open(executable).map_err(|_| transport("EXE_HASH_UNAVAILABLE"))?;
    let mut digest = Sha256::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut chunk)
            .map_err(|_| transport("EXE_HASH_UNAVAILABLE"))?;
        if count == 0 {
            break;
        }
        digest.update(&chunk[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

/// Existing 64-flight budget reserves its final slot for bounded diagnostics.
fn flight_admitted(inflight: usize, finance_view: bool) -> bool {
    inflight < if finance_view { 64 } else { 63 }
}

fn nonce() -> u128 {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("system random unavailable");
    u128::from_le_bytes(bytes)
}
fn transport(message: &str) -> AgentError {
    AgentError::CloudflareTransport(message.into())
}
fn frame(message: Message) -> AgentResult<F> {
    let Message::Text(text) = message else {
        return Err(transport("NON_TEXT_FRAME"));
    };
    if text.len() > DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES {
        return Err(transport("OVERSIZED_FRAME"));
    }
    let f: F = serde_json::from_str(&text)?;
    f.validate_incoming_envelope()?;
    Ok(f)
}
async fn send<S>(sink: &mut S, f: F) -> AgentResult<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let data = serde_json::to_string(&f)?;
    if data.len() > DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES {
        return Err(transport("OVERSIZED_RESPONSE"));
    }
    sink.send(Message::Text(data.into()))
        .await
        .map_err(|_| transport("WS_WRITE_FAILED"))
}
pub async fn run_cloudflare_transport(
    config: &CloudflareTransportConfig,
    ctx: DispatchContext,
    mut risk: mpsc::Receiver<OutboundRiskCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> AgentResult<()> {
    let mut retry = 0usize;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let result = run_session(config, ctx.clone(), &mut risk, shutdown.clone()).await;
        if *shutdown.borrow() {
            return Ok(());
        }
        if result.is_err() {
            eprintln!(
                "direct transport disconnected; pending exchange outcomes require exact readback"
            );
        }
        let seconds = [1, 5, 15, 30, 60][retry.min(4)];
        retry = (retry + 1).min(4);
        tokio::select! {_=shutdown.changed()=>return Ok(()),_=tokio::time::sleep(Duration::from_secs(seconds))=>{}}
    }
}
async fn run_session(
    config: &CloudflareTransportConfig,
    ctx: DispatchContext,
    risk: &mut mpsc::Receiver<OutboundRiskCommand>,
    shutdown: watch::Receiver<bool>,
) -> AgentResult<()> {
    let mut request = config
        .ws_url
        .clone()
        .into_client_request()
        .map_err(|_| transport("INVALID_WS_URL"))?;
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", config.bearer_token.as_str()))
            .map_err(|_| transport("INVALID_TOKEN"))?,
    );
    let (socket, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(request))
        .await
        .map_err(|_| transport("WS_CONNECT_TIMEOUT"))?
        .map_err(|_| transport("WS_CONNECT_FAILED"))?;
    let connection_id = format!("native_{:032x}", nonce());
    let hello = F::Hello {
        schema: SCHEMA.into(),
        runtime_id: config.runtime_id.clone(),
        connection_id,
        runtime_profile: DirectRuntimeProfile::Engine,
        installation_id: ctx.accounts.installation_id().into(),
        contract: okx_protocol::accounts::PRODUCT_CONTRACT.into(),
        agent_pid: std::process::id(),
        agent_binary_sha256: current_binary_sha256()?,
        agent_incarnation: std::env::var("OKX_CONTROL_AGENT_INCAR")
            .map_err(|_| transport("CONTROL_INCARNATION_UNAVAILABLE"))?,
    };
    run_connected_session(socket, hello, ctx, risk, shutdown).await
}

/// This is the same production session loop for live WSS and raw-frame tests.
/// Only delivery can be substituted: identity, session generation, risk replies,
/// authorization, dispatch, and all financial ownership remain unchanged.
async fn run_connected_session<S>(
    socket: S,
    hello: F,
    ctx: DispatchContext,
    risk: &mut mpsc::Receiver<OutboundRiskCommand>,
    mut shutdown: watch::Receiver<bool>,
) -> AgentResult<()>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin
        + Send,
{
    let (mut sink, mut source) = socket.split();
    send(&mut sink, hello).await?;
    let hello = tokio::time::timeout(Duration::from_secs(5), source.next())
        .await
        .map_err(|_| transport("HELLO_TIMEOUT"))?
        .ok_or_else(|| transport("WS_CLOSED"))?
        .map_err(|_| transport("WS_READ_FAILED"))?;
    let F::HelloAck {
        session_id,
        connection_generation,
        ..
    } = frame(hello)?
    else {
        return Err(transport("HELLO_ACK_REQUIRED"));
    };
    let mut pending: HashMap<String, oneshot::Sender<okx_protocol::risk_authority::RiskResult>> =
        HashMap::new();
    let mut challenges: HashMap<String, Instant> = HashMap::new();
    let mut active = HashSet::new();
    let mut flights: FuturesUnordered<BoxFuture<'static, (String, okx_protocol::AgentResponse)>> =
        FuturesUnordered::new();
    let read_capacity = Arc::new(tokio::sync::Semaphore::new(7));
    let finance_capacity = Arc::new(tokio::sync::Semaphore::new(1));
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_seen = Instant::now();
    loop {
        tokio::select! {
        _=shutdown.changed()=>{
        // Work already accepted by an account SQL owner remains awaited; graceful shutdown is bounded.
        let deadline=tokio::time::sleep(Duration::from_secs(10));tokio::pin!(deadline);
        while !flights.is_empty(){tokio::select!{_= &mut deadline=>break,_=flights.next()=>{}}}return Ok(());
        },
        _=heartbeat.tick()=>{if last_seen.elapsed()>Duration::from_secs(35){return Err(transport("HEARTBEAT_TIMEOUT"));}send(&mut sink,F::Ping{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,nonce:format!("native_ping_{:032x}",nonce())}).await?;},
        Some(outbound)=risk.recv()=>{pending.retain(|_,s|!s.is_closed());if pending.len()>=32{drop(outbound);continue;}let id=outbound.command.command_id.clone();if pending.contains_key(&id){drop(outbound);continue;}pending.insert(id,outbound.return_to);send(&mut sink,F::RiskControl{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,command:outbound.command}).await?;},
        Some((id,result))=flights.next(),if !flights.is_empty()=>{active.remove(&id);send(&mut sink,F::Response{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,response:result}).await?;},
        message=source.next()=>{
        let f=frame(message.ok_or_else(||transport("WS_CLOSED"))?.map_err(|_|transport("WS_READ_FAILED"))?)?;
        let(s,g)=match &f{F::Ping{session_id,connection_generation,..}|F::Pong{session_id,connection_generation,..}|F::Request{session_id,connection_generation,..}|F::RiskControlResult{session_id,connection_generation,..}|F::PolicySnapshot{session_id,connection_generation,..}=>(session_id,*connection_generation),_=>return Err(transport("UNEXPECTED_FRAME"))};
        if s!=&session_id||g!=connection_generation{return Err(transport("SESSION_GENERATION_MISMATCH"));}last_seen=Instant::now();
        match f{
        F::Ping{nonce,..}=>{challenges.retain(|_,t|t.elapsed()<Duration::from_secs(15));if challenges.len()<64{challenges.insert(nonce.clone(),Instant::now());}send(&mut sink,F::Pong{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,nonce}).await?;},
        F::Pong{..}=>{},
        F::PolicySnapshot{snapshot,..}=>{ctx.trade.update_governance(&snapshot).await.map_err(|_|transport("INVALID_POLICY_SNAPSHOT"))?;},
        F::RiskControlResult{result,..}=>{pending.retain(|_,sender|!sender.is_closed());if let Some(sender)=pending.remove(&result.command_id){let _=sender.send(result);}},
        F::Request{request,operator_governance,..}=>{
        let id=request.request_id.clone();send(&mut sink,F::DeliveryAck{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,request_id:id.clone()}).await?;
        if active.contains(&id){continue;}
        let finance_view = matches!(&request.operation, AgentOperation::Diagnostics{request:diagnostic} if diagnostic.view==Some(okx_protocol::trading::DiagnosticsView::Finance));
        if !flight_admitted(flights.len(), finance_view) {send(&mut sink,F::Response{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,response:response(&id,Err("NATIVE_QUEUE_FULL".into()))}).await?;continue;}
        let governance=operator_governance.filter(|g|g.valid()&&g.session_generation==connection_generation&&g.state.installation_id==ctx.accounts.installation_id()&&g.delivery_challenge.as_ref().and_then(|key|challenges.remove(key)).is_some_and(|t|t.elapsed()<Duration::from_secs(15)));
        let context=ctx.clone();let reads=read_capacity.clone();let fast_reads=finance_capacity.clone();active.insert(id.clone());
        flights.push(async move{
            let _permit=if finance_view {
                match fast_reads.try_acquire_owned() {
                    Ok(permit)=>Some(permit),
                    Err(_)=>return (id,response(&request.request_id,Err("NATIVE_DIAGNOSTICS_BUSY".into()))),
                }
            } else if request.operation.direct_transport_read_only() {
                reads.acquire_owned().await.ok()
            } else {None};
            let result=dispatch_with_governance(&context,&request,governance.as_ref()).await;
            (id,result)
        }.boxed());
        },_=>unreachable!()}
        }
        }
    }
}

#[cfg(test)]
mod reserved_finance_admission_tests {
    use super::*;

    #[test]
    fn ordinary_flights_cannot_fill_diagnostic_slot() {
        assert!(flight_admitted(62, false));
        assert!(!flight_admitted(63, false));
        assert!(flight_admitted(63, true));
        assert!(!flight_admitted(64, true));
    }

    #[test]
    fn finance_read_admission_is_independent_and_fails_fast() {
        let normal = Arc::new(tokio::sync::Semaphore::new(7));
        let finance = Arc::new(tokio::sync::Semaphore::new(1));
        let ordinary: Vec<_> = (0..7)
            .map(|_| Arc::clone(&normal).try_acquire_owned().unwrap())
            .collect();
        assert!(Arc::clone(&normal).try_acquire_owned().is_err());
        let diagnostic = Arc::clone(&finance).try_acquire_owned().unwrap();
        assert!(Arc::clone(&finance).try_acquire_owned().is_err());
        drop(diagnostic);
        assert!(Arc::clone(&finance).try_acquire_owned().is_ok());
        drop(ordinary);
    }
}

#[cfg(test)]
mod raw_native_session_tests {
    use super::*;
    use crate::{
        accounts::{AccountRuntimeRegistry, initialize_installation},
        analytics::AnalyticsService,
        market_bootstrap::MarketBootstrapper,
        risk_bridge::NativeRiskClient,
        trade_service::TradeService,
    };
    use futures_util::{Sink, Stream};
    use okx_api::{OkxEnvironment, OkxPublicClient, RateBudget, Region};
    use okx_observation::ReferenceRegistry;
    use okx_protocol::risk_authority::{
        CausalPostProof, NativeRiskCensus, PostAccountReadProof, RISK_COMMAND_SCHEMA_V1,
        RISK_RESULT_SCHEMA_V1, RiskAccountOccupancy, RiskCommand, RiskCommandAction,
        RiskDecisionStatus, RiskResult, UsdConversion,
    };
    use okx_runtime::PublicWsCoordinator;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::sync::mpsc;

    /// A recorded frame delivery, not a venue, financial engine or dispatcher.
    struct RawFrames {
        inbound: mpsc::UnboundedReceiver<Result<Message, tokio_tungstenite::tungstenite::Error>>,
        outbound: mpsc::UnboundedSender<Message>,
    }
    impl Stream for RawFrames {
        type Item = Result<Message, tokio_tungstenite::tungstenite::Error>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.inbound.poll_recv(cx)
        }
    }
    impl Sink<Message> for RawFrames {
        type Error = tokio_tungstenite::tungstenite::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, frame: Message) -> Result<(), Self::Error> {
            self.outbound
                .send(frame)
                .map_err(|_| tokio_tungstenite::tungstenite::Error::ConnectionClosed)
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }
    fn wire(message: F) -> Message {
        Message::Text(serde_json::to_string(&message).unwrap().into())
    }
    fn parse(frame: Message) -> F {
        serde_json::from_str(frame.to_text().unwrap()).unwrap()
    }
    fn test_hello() -> F {
        F::Hello {
            schema: SCHEMA.into(),
            runtime_id: "windows-primary".into(),
            connection_id: "native_0123456789abcdef0123456789abcdef".into(),
            installation_id: "installation-test".into(),
            contract: okx_protocol::accounts::PRODUCT_CONTRACT.into(),
            runtime_profile: DirectRuntimeProfile::Engine,
            agent_pid: 100,
            agent_binary_sha256: "a".repeat(64),
            agent_incarnation: format!("ctl_{}", "b".repeat(32)),
        }
    }
    async fn test_dispatch_for(
        installation: &str,
    ) -> (
        DispatchContext,
        std::path::PathBuf,
        NativeRiskClient,
        mpsc::Receiver<OutboundRiskCommand>,
    ) {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).unwrap();
        let root = std::env::temp_dir().join(format!(
            "okx-raw-native-session-{:032x}",
            u128::from_le_bytes(bytes)
        ));
        initialize_installation(&root, installation).unwrap();
        let budget = RateBudget::new();
        let accounts = Arc::new(
            AccountRuntimeRegistry::open(&root, budget.clone())
                .await
                .unwrap(),
        );
        if std::env::var("OKX_NATIVE_WORKER_INTEGRATION")
            .ok()
            .as_deref()
            == Some("1")
            && installation == "installation_test"
        {
            let feed = Arc::new(crate::accounts::recorded_support::RecordedAccountHttp::default());
            for label in ["a", "b"] {
                // The real private coordinator receives only recorded socket
                // frames. It must establish login, subscription generations and
                // independently reconcile the six financial history scopes
                // through the production SQL owner.
                let socket = Arc::new(
                    crate::accounts::recorded_support::RecordedWsConnector::default(),
                );
                let (config, clients) =
                    crate::accounts::recorded_support::prepared_with_socket(
                        label, feed.clone(), socket,
                    );
                accounts.connect_prepared(config, clients).await.unwrap();
            }
        }
        let env = OkxEnvironment::new(Region::Global, false);
        let market = MarketBootstrapper::new(
            OkxPublicClient::with_rate_budget(env, budget.clone()).unwrap(),
        );
        let reference = ReferenceRegistry::from_public("2026-10-31T00:00:00Z", vec![]).unwrap();
        let (_public, public_ws) =
            PublicWsCoordinator::new_with_rate_budget(env, reference, budget);
        let analytics = AnalyticsService::new(root.join("analytics"), market.clone()).unwrap();
        let (risk_tx, risk_rx) = mpsc::channel(8);
        let risk = NativeRiskClient::new(risk_tx);
        let trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            risk.clone(),
        ));
        trade.set_analytics(analytics.clone()).await;
        let ctx = DispatchContext {
            accounts,
            analytics,
            public_ws,
            trade,
            market,
        };
        (ctx, root, risk, risk_rx)
    }
    async fn test_dispatch() -> (
        DispatchContext,
        std::path::PathBuf,
        NativeRiskClient,
        mpsc::Receiver<OutboundRiskCommand>,
    ) {
        test_dispatch_for("installation-test").await
    }

    fn summary(id: &str) -> RiskCommand {
        RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: id.into(),
            installation_id: "installation-test".into(),
            action: RiskCommandAction::Summary {},
        }
    }
    fn denied(id: &str) -> RiskResult {
        RiskResult {
            schema: RISK_RESULT_SCHEMA_V1.into(),
            command_id: id.into(),
            status: RiskDecisionStatus::Denied,
            reason: Some("CENTRAL_REAL_AUTHORIZATION_INACTIVE".into()),
            reservation_key: None,
            risk_book_sequence: None,
            policy_revision: None,
            existing_modelled_loss_usdt: None,
            reservation_state: None,
            budget: None,
            account_budget: None,
        }
    }

    #[tokio::test]
    async fn actual_native_session_binds_risk_reply_to_generation_and_id() {
        let (ctx, root, client, mut risk_rx) = test_dispatch().await;
        let (input_tx, input_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();
        let socket = RawFrames {
            inbound: input_rx,
            outbound: output_tx,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            run_connected_session(socket, test_hello(), ctx, &mut risk_rx, shutdown_rx).await
        });
        let first = tokio::time::timeout(Duration::from_secs(3), output_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(parse(first), F::Hello { .. }));
        let session_id = "session_test_12345678".to_owned();
        let generation = 9;
        input_tx
            .send(Ok(wire(F::HelloAck {
                schema: SCHEMA.into(),
                session_id: session_id.clone(),
                connection_generation: generation,
            })))
            .unwrap();
        let command = summary("risk_wire_12345");
        let request = tokio::spawn({
            let client = client.clone();
            async move { client.request(command).await }
        });
        let seen = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match parse(output_rx.recv().await.expect("outbound closed")) {
                    F::RiskControl { command, .. } => break command,
                    F::Ping { .. } => continue,
                    other => panic!("unexpected native outbound: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(seen.command_id, "risk_wire_12345");
        input_tx
            .send(Ok(wire(F::RiskControlResult {
                schema: SCHEMA.into(),
                session_id,
                connection_generation: generation,
                result: denied(&seen.command_id),
            })))
            .unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(answer.status, RiskDecisionStatus::Denied);
        assert_eq!(answer.command_id, seen.command_id);
        shutdown_tx.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), session)
                .await
                .unwrap()
                .unwrap()
                .is_ok()
        );
        drop(client);
        drop(input_tx);
        drop(output_rx);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn stale_native_risk_reply_cannot_grant_a_different_generation() {
        let (ctx, root, client, mut risk_rx) = test_dispatch().await;
        let (input_tx, input_rx) = mpsc::unbounded_channel();
        let (output_tx, mut output_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            run_connected_session(
                RawFrames {
                    inbound: input_rx,
                    outbound: output_tx,
                },
                test_hello(),
                ctx,
                &mut risk_rx,
                shutdown_rx,
            )
            .await
        });
        assert!(matches!(
            parse(output_rx.recv().await.unwrap()),
            F::Hello { .. }
        ));
        let session_id = "session_test_87654321".to_owned();
        input_tx
            .send(Ok(wire(F::HelloAck {
                schema: SCHEMA.into(),
                session_id: session_id.clone(),
                connection_generation: 12,
            })))
            .unwrap();
        let command = summary("risk_wire_99999");
        let request = tokio::spawn({
            let client = client.clone();
            async move { client.request(command).await }
        });
        let observed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let F::RiskControl { command, .. } = parse(output_rx.recv().await.unwrap()) {
                    break command.command_id;
                }
            }
        })
        .await
        .unwrap();
        input_tx
            .send(Ok(wire(F::RiskControlResult {
                schema: SCHEMA.into(),
                session_id,
                connection_generation: 11,
                result: denied(&observed),
            })))
            .unwrap();
        let failed = tokio::time::timeout(Duration::from_secs(3), session)
            .await
            .unwrap()
            .unwrap();
        assert!(
            failed
                .unwrap_err()
                .to_string()
                .contains("SESSION_GENERATION_MISMATCH")
        );
        let unresolved = tokio::time::timeout(Duration::from_secs(3), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unresolved.unwrap_err(), "RISK_TRANSPORT_LOST_DO_NOT_REPLAY");
        drop(client);
        drop(input_tx);
        drop(shutdown_tx);
        drop(output_rx);
        std::fs::remove_dir_all(root).unwrap();
    }
    /// Opt-in only from the pinned Linux integration job. This socket talks
    /// directly to the real workerd RuntimeSession and its persisted SQLite DO;
    /// there is no mock Worker/RiskBook, fixture exchange or alternate executor.
    #[tokio::test]
    async fn native_to_workerd_risk_summary_over_actual_websocket() {
        let Ok(url) = std::env::var("OKX_INTEGRATION_WORKER_WS_URL") else {
            return;
        };
        assert!(
            url.starts_with("ws://127.0.0.1:"),
            "integration socket must be a local ephemeral workerd endpoint"
        );
        let token = std::env::var("OKX_INTEGRATION_WORKER_TOKEN").unwrap();
        let reservation_key = std::env::var("OKX_INTEGRATION_RESERVATION").unwrap();
        let (ctx, root, risk, mut risk_rx) = test_dispatch_for("installation_test").await;
        let trade = ctx.trade.clone();
        let accounts = ctx.accounts.clone();
        let stopped = ["fixture-a", "fixture-b"].iter().map(|alias| async {
            accounts
                .resolve(alias)
                .await
                .unwrap()
                .execution
                .store
                .termination_signal()
        });
        let mut stopped = futures_util::future::join_all(stopped).await;
        // T09.0 prerequisite: on the SAME assembly that will talk to the
        // production Worker RiskBook, both accounts must have obtained
        // identity-bound private WS generations and completed real six-source
        // ingestion. No source generation, cursor or portfolio fact is
        // inserted by the test driver.
        for alias in ["fixture-a", "fixture-b"] {
            let resource = accounts.resolve(alias).await.unwrap();
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    let status = resource.private_ws.status().await;
                    let captured = resource.source_capture_health.read().await.last_success_ms;
                    if status.logged_in
                        && status.subscriptions_complete
                        && status.algo_subscription_ready
                        && status.account_data_seen
                        && status.positions_data_seen
                        && resource.source_identity_ready().await
                        && captured.is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("T09.0 private generation and source capture were not established");
            let cursors = resource
                .execution
                .store
                .call(std::time::Instant::now() + Duration::from_secs(3), |s| {
                    Ok((s.cursor("fills", "SWAP")?, s.cursor("bills", "account")?))
                })
                .await
                .unwrap();
            assert_eq!(
                cursors.0.expect("missing SWAP fill cursor").coverage_gaps, "",
                "the production financial capture must have no SWAP history gaps"
            );
            assert_eq!(
                cursors.1.expect("missing bill cursor").coverage_gaps, "",
                "the production financial capture must have no bill history gaps"
            );
        }
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let (socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(request))
            .await
            .unwrap()
            .unwrap();
        let mut hello = test_hello();
        if let F::Hello {
            installation_id, ..
        } = &mut hello
        {
            *installation_id = "installation_test".into();
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let session = tokio::spawn(async move {
            run_connected_session(socket, hello, ctx, &mut risk_rx, shutdown_rx).await
        });
        let mut command = summary("risk_native_real_workerd_001");
        command.installation_id = "installation_test".into();
        let result = risk.request(command).await.unwrap();
        assert_eq!(result.status, RiskDecisionStatus::Observed);
        let budget = result.budget.as_ref().unwrap();
        assert_eq!(budget.campaign_limit_usd, "10");
        assert_eq!(budget.reserved_loss_usd, "1");
        let entry = budget.entry_budget.as_ref().unwrap();
        assert_eq!(entry.reserved_usd, "2");
        assert_eq!(entry.consumed_usd, "0");
        // Enter the real public TradeService on both authenticated SQL owners.
        // Missing private WS source facts cannot become a venue POST, even with
        // a valid Worker grant in the SAME authenticated process/session.
        use okx_protocol::trading::{
            OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
        };
        let attempted = TradeRequest::Execute {
            idempotency_key: "public_native_worker_unproven_001".into(),
            operation: TradeOperation::Open,
            strategy_instance: "manual".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: PositionSide::Long,
            sizing: Sizing::Contracts {
                contracts: "0.10".into(),
            },
            order: OrderConstraints {
                order_type: "limit".into(),
                limit_price: Some("1000".into()),
                reference: None,
                max_entry_usd: Some("10".into()),
                max_slippage_bps: 0,
            },
            protection: Some(Protection::Price {
                stop_price: "900".into(),
            }),
            expires_in_ms: 2_000,
        };
        assert!(attempted.valid());
        for alias in ["fixture-a", "fixture-b"] {
            let caps = trade
                .trade(alias, &TradeRequest::Capabilities {}, None)
                .await
                .unwrap();
            let uid = if alias == "fixture-a" {
                okx_api::account_uid_fingerprint("account-001")
            } else {
                okx_api::account_uid_fingerprint("account-002")
            };
            assert_eq!(caps["account"], format!("okx:real:{uid}"));
            assert_eq!(caps["executor"]["state"], "verified");
            assert_eq!(caps["ready_for_post"], false);
            let preflight = trade
                .trade(alias, &TradeRequest::Preflight {}, None)
                .await
                .unwrap();
            assert_eq!(preflight["ready_for_new_risk"], false);
            let failed = trade.trade(alias, &attempted, None).await;
            assert!(
                failed.is_err(),
                "unverified source must not POST: {failed:?}"
            );
        }
        let mut before = summary("risk_public_trade_did_not_post_001");
        before.installation_id = "installation_test".into();
        let untouched = risk.request(before).await.unwrap();
        assert_eq!(untouched.budget.as_ref().unwrap().reserved_loss_usd, "1");
        assert_eq!(
            untouched
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            "2"
        );
        let inspect = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "risk_native_real_workerd_inspect_001".into(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Inspect {
                reservation_key: reservation_key.clone(),
                plan_sha256: "b".repeat(64),
            },
        };
        let inspected = risk.request(inspect).await.unwrap();
        assert_eq!(inspected.status, RiskDecisionStatus::Observed);
        assert_eq!(
            inspected.reservation_key.as_deref(),
            Some(reservation_key.as_str())
        );
        assert!(inspected.reservation_state.is_some());
        // The second intent is sent by actual Rust Native through the same
        // authenticated WSS path. Evidence values are explicitly scripted
        // boundary inputs: no venue claims and no exchange POST in this test.
        let uid = okx_api::account_uid_fingerprint("account-001");
        let account_key = format!("okx:real:{uid}");
        let timestamp = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        };
        let captured_at = timestamp() - 300;
        let census = NativeRiskCensus {
            account_keys: vec![account_key.clone()],
            occupied: vec![RiskAccountOccupancy {
                account_key: account_key.clone(),
                occupied_usdt: "0".into(),
                cumulative_loss_usd: "0".into(),
            }],
            generated_at_ms: captured_at,
            source_read_floor_ms: captured_at,
            source_digest: "c".repeat(64),
            content_digest: "a".repeat(64),
            complete: true,
            native_account_evidence_complete: true,
            native_order_evidence_complete: true,
            native_ledger_evidence_complete: true,
        };
        let reserve = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: "risk_native_reserve_workerd_001".into(),
            installation_id: "installation_test".into(),
            action: RiskCommandAction::Reserve {
                venue: "okx".into(),
                account_uid: uid,
                run_id: "run_native_workerd_001".into(),
                intent_id: "intent_native_workerd_001".into(),
                strategy_id: "manual".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                plan_sha256: "d".repeat(64),
                requested_loss_usdt: "1".into(),
                requested_loss_usd: "1".into(),
                entry_upper_usd: Some("2".into()),
                entry_proof_digest: Some("e".repeat(64)),
                usd_conversion: UsdConversion {
                    usd_per_usdt: "1".into(),
                    source_digest: "e".repeat(64),
                    observed_at_ms: captured_at,
                },
                policy_revision: 1,
                census: census.clone(),
            },
        };
        let granted = risk
            .reserve_then_fence(reserve, |held| {
                let key = held.reservation_key.clone().unwrap();
                let sequence = held.risk_book_sequence.unwrap();
                let mut fresh = census;
                fresh.generated_at_ms = timestamp();
                fresh.source_read_floor_ms = fresh.generated_at_ms;
                fresh.source_digest = "f".repeat(64);
                async move {
                    Ok(RiskCommand {
                        schema: RISK_COMMAND_SCHEMA_V1.into(),
                        command_id: "risk_native_post_workerd_001".into(),
                        installation_id: "installation_test".into(),
                        action: RiskCommandAction::BeginPost {
                            reservation_key: key.clone(),
                            plan_sha256: "d".repeat(64),
                            policy_revision: 1,
                            expected_risk_book_sequence: sequence,
                            source_proof_sha256: "f".repeat(64),
                            census: fresh,
                            causal_proof: CausalPostProof {
                                reserve_anchor: key,
                                native_incarnation: "a".repeat(32),
                                elapsed_to_proof_ms: 30,
                                account_reads: vec![PostAccountReadProof {
                                    account_key,
                                    private_generation: 7,
                                    source_read_id: "b".repeat(64),
                                    content_digest: "a".repeat(64),
                                    started_after_held_ms: 5,
                                    completed_after_held_ms: 20,
                                }],
                            },
                        },
                    })
                }
            })
            .await
            .unwrap();
        assert_eq!(granted.status, RiskDecisionStatus::PostAuthorized);
        assert_eq!(granted.existing_modelled_loss_usdt.as_deref(), Some("1"));
        let mut final_read = summary("risk_native_after_second_fence_001");
        final_read.installation_id = "installation_test".into();
        let checked = risk.request(final_read).await.unwrap();
        assert_eq!(checked.budget.as_ref().unwrap().reserved_loss_usd, "2");
        assert_eq!(
            checked
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            "4"
        );
        shutdown_tx.send(true).unwrap();
        let terminated = tokio::time::timeout(Duration::from_secs(10), session)
            .await
            .unwrap()
            .unwrap();
        assert!(
            terminated.is_ok(),
            "native session should terminate cleanly: {terminated:?}"
        );
        drop(trade);
        accounts.shutdown().await;
        drop(accounts);
        drop(risk);
        for receipt in &mut stopped {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !*receipt.borrow_and_update() {
                    receipt
                        .changed()
                        .await
                        .expect("SQL owner shutdown receipt missing");
                }
            })
            .await
            .expect("SQL owner still active");
        }
        std::fs::remove_dir_all(root).unwrap();
        println!("native_to_persisted_workerd_riskbook PASS: no venue POST");
    }
}
