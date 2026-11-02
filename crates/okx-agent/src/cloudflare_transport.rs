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
    use okx_api::{
        OkxEnvironment, OkxPublicClient, RateBudget, RateOperationClass, RateThrottleSource, Region,
    };
    use okx_observation::ReferenceRegistry;
    use okx_protocol::risk_authority::{
        CausalPostProof, NativeRiskCensus, PostAccountReadProof, RISK_COMMAND_SCHEMA_V1,
        RISK_RESULT_SCHEMA_V1, RiskAccountOccupancy, RiskCommand, RiskCommandAction,
        RiskDecisionStatus, RiskResult, UsdConversion,
    };
    use okx_runtime::PublicWsCoordinator;
    use rust_decimal::Decimal;
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
        Option<Arc<crate::accounts::recorded_support::RecordedAccountHttp>>,
        RateBudget,
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
        let mut recorded_http = None;
        if std::env::var("OKX_NATIVE_WORKER_INTEGRATION")
            .ok()
            .as_deref()
            == Some("1")
            && installation == "installation_test"
        {
            let feed = Arc::new(crate::accounts::recorded_support::RecordedAccountHttp::default());
            recorded_http = Some(feed.clone());
            for label in ["a", "b"] {
                // The real private coordinator receives only recorded socket
                // frames. It must establish login, subscription generations and
                // independently reconcile the six financial history scopes
                // through the production SQL owner.
                let socket =
                    Arc::new(crate::accounts::recorded_support::RecordedWsConnector::default());
                let (config, clients) = crate::accounts::recorded_support::prepared_with_socket(
                    label,
                    feed.clone(),
                    socket,
                    budget.clone(),
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
            PublicWsCoordinator::new_with_rate_budget(env, reference, budget.clone());
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
        (ctx, root, risk, risk_rx, recorded_http, budget)
    }
    async fn test_dispatch() -> (
        DispatchContext,
        std::path::PathBuf,
        NativeRiskClient,
        mpsc::Receiver<OutboundRiskCommand>,
    ) {
        let (ctx, root, risk, risk_rx, _, _) = test_dispatch_for("installation-test").await;
        (ctx, root, risk, risk_rx)
    }

    fn summary(id: &str) -> RiskCommand {
        RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.into(),
            command_id: id.into(),
            installation_id: "installation-test".into(),
            action: RiskCommandAction::Summary {},
        }
    }
    include!("trade_integration_t09_3.rs");
    include!("trade_integration_t09_4.rs");

    /// Retry permission is test-only and limited to an explicitly read-only
    /// Preview that failed at the *local* request-admission boundary. The
    /// production RateBudget supplies the exact no-send defer: no fixed sleep,
    /// disabled limiter, exchange retry, or inferred POST safety.
    fn recorded_preview_local_defer(
        reason: &str,
        budget: &RateBudget,
        remaining: Duration,
    ) -> Option<Duration> {
        if !reason.starts_with("OKX rate/backpressure defer: ") {
            return None;
        }
        let throttle = budget.snapshot().last_throttle?;
        if throttle.source != RateThrottleSource::LocalBudget
            || throttle.operation != RateOperationClass::PrivateRestRead
            || throttle.request_sent
            || !throttle.retryable
            || throttle.local_defer_ms == 0
        {
            return None;
        }
        let delay = Duration::from_millis(throttle.local_defer_ms);
        (delay <= remaining).then_some(delay)
    }

    // An explicitly recorded *new* all-scopes inventory after T09.1 has
    // verified cancellation of its attached protection. Keep the finite
    // transcript builder synchronous, outside the large Native async test
    // future; it must not synthesize state in AccountStore or RiskBook.
    fn recorded_flat_derivative_inventory_after_close(
        feed: &crate::accounts::recorded_support::RecordedAccountHttp,
    ) -> (usize, usize) {
        use crate::accounts::recorded_support::RecordedGet;
        // The completed T09.1 terminal source proved a flat position.
        // A later admission still requires NEW signed source pages, not
        // silent defaults after the prior transcript has been consumed.
        let raw_empty = serde_json::to_vec(&serde_json::json!({"code":"0","msg":"","data":[]}))
            .expect("T09.2 signed raw empty source");
        let retired_positions = feed.advance_recorded_route(
            "fixture-observer-a",
            "/api/v5/account/positions",
            (0..48)
                .map(|_| RecordedGet {
                    access_key: "fixture-observer-a".into(),
                    path: "/api/v5/account/positions".into(),
                    required_query: vec![],
                    raw_response: raw_empty.clone(),
                })
                .collect(),
        );
        let kinds = [
            "conditional",
            "oco",
            "trigger",
            "move_order_stop",
            "chase",
            "iceberg",
            "twap",
            "smart_iceberg",
        ];
        let mut pages = Vec::with_capacity(5 * 2 * kinds.len());
        for _ in 0..5 {
            for inst_type in ["SWAP", "FUTURES"] {
                for kind in kinds {
                    pages.push(RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-algo-pending".into(),
                        required_query: vec![
                            ("instType".into(), inst_type.into()),
                            ("ordType".into(), kind.into()),
                            ("limit".into(), "100".into()),
                        ],
                        raw_response: raw_empty.clone(),
                    });
                }
            }
        }
        let retired_algos = feed.advance_recorded_route(
            "fixture-observer-a",
            "/api/v5/trade/orders-algo-pending",
            pages,
        );
        (retired_positions, retired_algos)
    }

    #[test]
    fn recorded_preview_retry_requires_local_no_send_private_get() {
        let budget = RateBudget::new().for_authenticated_user("account-001");
        let plan = budget.private_rest_plan("/api/v5/account/config", &[]);
        for _ in 0..5 {
            budget.admit(&plan).unwrap();
        }
        let denied = budget.admit(&plan).unwrap_err();
        assert!(!denied.request_sent);
        assert_eq!(denied.source, RateThrottleSource::LocalBudget);
        assert!(
            recorded_preview_local_defer(
                "OKX rate/backpressure defer: RateThrottleEvidence { ... }",
                &budget,
                Duration::from_secs(3),
            )
            .is_some()
        );
        assert!(
            recorded_preview_local_defer("VENUE_ACK_MISSING", &budget, Duration::from_secs(3),)
                .is_none()
        );
        assert!(
            recorded_preview_local_defer(
                "OKX rate/backpressure defer: RateThrottleEvidence { ... }",
                &budget,
                Duration::ZERO,
            )
            .is_none()
        );
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
        let Ok(handoff_path) = std::env::var("OKX_INTEGRATION_HANDOFF_PATH") else {
            return;
        };
        // This real two-account SQL bootstrap MUST precede the Worker
        // campaign's beginning. A later account cannot certify an earlier
        // history; the production owner correctly refuses such a baseline.
        let (mut ctx, root, risk, mut risk_rx, recorded_http, rate_budget) =
            test_dispatch_for("installation_test").await;
        let accounts = ctx.accounts.clone();
        std::fs::write(format!("{handoff_path}.native-ready"), b"SQL_OWNERS_OPEN")
            .expect("test bootstrap marker must be written after SQL initialization");
        // The Node driver creates the actual workerd/SQLite campaign only
        // after observing the marker, then supplies its local WSS address.
        // This file is a transport barrier, never a source of trading facts.
        let handoff = tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                match std::fs::read(&handoff_path) {
                    Ok(bytes) => break bytes,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(error) => panic!("integration handoff unavailable: {error}"),
                }
            }
        })
        .await
        .expect("workerd bootstrap handoff deadline");
        let handoff: serde_json::Value = serde_json::from_slice(&handoff).unwrap();
        let url = handoff["ws_url"].as_str().unwrap().to_owned();
        assert!(
            url.starts_with("ws://127.0.0.1:"),
            "integration socket must be a local ephemeral workerd endpoint"
        );
        let token = handoff["token"].as_str().unwrap().to_owned();
        let reservation_key = handoff["reservation_key"].as_str().unwrap_or("").to_owned();
        let add_mode = std::env::var("OKX_T09_4_MANAGED_ADD_PROBE").as_deref() == Ok("1");

        // The same real public WS coordinator consumes recorded raw market
        // frames. These are immutable venue observations, not a fabricated
        // MarketSnapshot or a separate pricing/decision implementation.
        use crate::accounts::recorded_support::{RecordedGet, RecordedWsConnector};
        use serde_json::json;
        let feed = recorded_http.expect("the integration run requires recorded HTTP");
        let connector = Arc::new(RecordedWsConnector::default());
        let reference = ReferenceRegistry::from_public(
            chrono::Utc::now().to_rfc3339(),
            vec![
                serde_json::from_value(json!({
                    "instType":"SWAP","instId":"BTC-USDT-SWAP",
                    "instFamily":"BTC-USDT","uly":"BTC-USDT","state":"live",
                    "ruleType":"normal","settleCcy":"USDT","tickSz":"0.1",
                    "lotSz":"0.01","minSz":"0.01","maxLmtSz":"1000",
                    "maxMktSz":"1000","ctType":"linear","ctVal":"0.01",
                    "ctValCcy":"BTC","groupId":"4","lever":"20"
                }))
                .unwrap(),
            ],
        )
        .unwrap();
        let stamp = chrono::Utc::now().timestamp_millis();
        let timestamp = stamp.to_string();
        let funding_time = (stamp + 8 * 60 * 60 * 1000).to_string();
        let recorded_public = vec![
            (
                json!({"channel":"instruments","instType":"SWAP"}),
                None,
                None,
            ),
            (
                json!({"channel":"instruments","instType":"FUTURES"}),
                None,
                None,
            ),
            (
                json!({"channel":"tickers","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                 "last":"1000","lastSz":"1","askPx":"1000","askSz":"10",
                 "bidPx":"999.9","bidSz":"10","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"mark-price","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                 "markPx":"1000","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"index-tickers","instId":"BTC-USDT"}),
                Some(json!({"instId":"BTC-USDT","idxPx":"1000","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"funding-rate","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                 "fundingRate":"0.0001","fundingTime":timestamp,
                 "nextFundingTime":funding_time,"ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"open-interest","instId":"BTC-USDT-SWAP"}),
                Some(json!({"instType":"SWAP","instId":"BTC-USDT-SWAP",
                 "oi":"1000","oiCcy":"10","oiUsd":"10000","ts":timestamp})),
                None,
            ),
            (
                json!({"channel":"books","instId":"BTC-USDT-SWAP"}),
                Some(json!({"asks":[["1000","10","0","1"]],
                 "bids":[["999.9","10","0","1"]],
                 "seqId":10,"prevSeqId":-1,"ts":timestamp})),
                Some("snapshot"),
            ),
        ];
        for (arg, data, action) in recorded_public.clone() {
            let ack = json!({"event":"subscribe","arg":arg,"connId":"recorded-public"}).to_string();
            let books = arg["channel"] == "books";
            let mut transcript = Vec::new();
            if !books {
                transcript.push(ack.clone());
            }
            if let Some(data) = data {
                let mut frame = json!({"arg":arg,"data":[data]});
                if let Some(action) = action {
                    frame["action"] = action.into();
                }
                transcript.push(frame.to_string());
            }
            if books {
                transcript.push(ack);
            }
            connector.expect_public_subscription(arg, transcript);
        }
        let (coordinator, public_ws) = PublicWsCoordinator::new_with_connector(
            OkxEnvironment::new(Region::Global, false),
            reference,
            RateBudget::new(),
            connector.clone(),
        );
        let (public_shutdown, public_receiver) = watch::channel(false);
        let public_task = tokio::spawn(coordinator.run(public_receiver));
        let public_trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            risk.clone(),
        ));
        public_trade.set_analytics(ctx.analytics.clone()).await;
        ctx.trade = public_trade.clone();
        ctx.public_ws = public_ws.clone();
        let mut market_updates = public_ws.subscribe_market_updates();
        public_ws.demand_instrument("BTC-USDT-SWAP").await.unwrap();
        let market = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(market) = public_ws
                    .risk_fresh_snapshot(
                        "BTC-USDT-SWAP",
                        chrono::Utc::now().timestamp_millis() as u64,
                        5000,
                        5000,
                        chrono::Utc::now().to_rfc3339(),
                    )
                    .await
                {
                    break market;
                }
                market_updates
                    .changed()
                    .await
                    .expect("public source coordinator exited");
            }
        })
        .await
        .expect("recorded market frames were not processed by production WS");
        assert_eq!(market.market.ticker.best_ask, "1000");
        assert_eq!(connector.unconsumed_public_subscriptions(), 0);
        // Every fee/leverage response passes through the existing signed
        // observer API parser. A second account cannot borrow A's tariff.
        let envelope = |data: serde_json::Value| {
            serde_json::to_vec(&json!({"code":"0","msg":"","data":data})).unwrap()
        };
        for label in ["a", "b"] {
            let access_key = format!("fixture-observer-{label}");
            // Two mandatory reads per account prove both fee/leveraged plan
            // paths. Four finite additional raw replies permit a read-only
            // Preview to resume after a rate defer without a fixture server,
            // a live fallback, or weakening required-page completion.
            for attempt in 0..6 {
                let page = RecordedGet {
                    access_key: access_key.clone(),
                    path: "/api/v5/account/trade-fee".into(),
                    required_query: vec![
                        ("instType".into(), "SWAP".into()),
                        ("instFamily".into(), "BTC-USDT".into()),
                    ],
                    raw_response: envelope(json!([{
                        "instType":"SWAP","level":"Lv1","ts":stamp.to_string(),
                        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                    }])),
                };
                if attempt < 2 {
                    feed.expect_get(page);
                } else {
                    feed.expect_optional_get(page);
                }
            }
            for attempt in 0..6 {
                let page = RecordedGet {
                    access_key: access_key.clone(),
                    path: "/api/v5/account/leverage-info".into(),
                    required_query: vec![
                        ("instId".into(), "BTC-USDT-SWAP".into()),
                        ("mgnMode".into(), "cross".into()),
                    ],
                    raw_response: envelope(json!([
                        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                    ])),
                };
                if attempt < 2 {
                    feed.expect_get(page);
                } else {
                    feed.expect_optional_get(page);
                }
            }
        }
        let trade = public_trade;
        // Replay a newly timestamped RAW public frame only after a bounded
        // local no-send defer. The production WS parser and freshness checks
        // remain the only authority; no direct market-state setter is used.
        let refresh_recorded_market = || {
            let refreshed_at = chrono::Utc::now().timestamp_millis();
            for (arg, observed, action) in recorded_public.iter().cloned() {
                if arg["channel"] == "instruments" && arg["instType"] == "SWAP" {
                    connector.send_public_frame(
                        json!({
                            "arg":arg,
                            "data":[{
                                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                "instFamily":"BTC-USDT","uly":"BTC-USDT","state":"live",
                                "ruleType":"normal","settleCcy":"USDT","tickSz":"0.1",
                                "lotSz":"0.01","minSz":"0.01","maxLmtSz":"1000",
                                "maxMktSz":"1000","ctType":"linear","ctVal":"0.01",
                                "ctValCcy":"BTC","groupId":"4","lever":"20"
                            }]
                        })
                        .to_string(),
                    );
                } else if let Some(mut value) = observed {
                    value["ts"] = refreshed_at.to_string().into();
                    if value.get("fundingTime").is_some() {
                        value["fundingTime"] = refreshed_at.to_string().into();
                        value["nextFundingTime"] =
                            (refreshed_at + 8 * 60 * 60 * 1000).to_string().into();
                    }
                    if action == Some("snapshot") {
                        value["seqId"] = 11.into();
                    }
                    let mut frame = json!({"arg":arg,"data":[value]});
                    if let Some(action) = action {
                        frame["action"] = action.into();
                    }
                    connector.send_public_frame(frame.to_string());
                }
            }
            refreshed_at
        };

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
        let mut t09_bootstrap_accounts = Vec::new();
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
                    Ok((
                        s.cursor("fills", "SWAP")?,
                        s.cursor("bills", "account")?,
                        s.active_intents(1)?,
                    ))
                })
                .await
                .unwrap();
            assert_eq!(
                cursors
                    .0
                    .as_ref()
                    .expect("missing SWAP fill cursor")
                    .coverage_gaps,
                "",
                "the production financial capture must have no SWAP history gaps"
            );
            assert_eq!(
                cursors
                    .1
                    .as_ref()
                    .expect("missing bill cursor")
                    .coverage_gaps,
                "",
                "the production financial capture must have no bill history gaps"
            );
            assert!(
                cursors.2.is_empty(),
                "T09.0 bootstrap must contain no preseeded active intent"
            );
            let source_capture_ms = resource
                .source_capture_health
                .read()
                .await
                .last_success_ms
                .expect("T09.0 must have captured source metadata");
            t09_bootstrap_accounts.push(json!({
                "alias":alias,
                "verified_uid":resource.configuration.account_key.verified_uid,
                "private_generation":resource.private_ws.status().await.connection_generation,
                "source_capture_ms":source_capture_ms,
                "fill_cursor_watermark":cursors.0.unwrap().watermark,
                "bill_cursor_watermark":cursors.1.unwrap().watermark,
                "active_intent_count":cursors.2.len(),
            }));
        }
        let mut t09_databases = std::fs::read_dir(root.join("accounts"))
            .unwrap()
            .map(|entry| {
                let file = entry.unwrap().path().join("account.sqlite");
                assert!(
                    file.is_file(),
                    "T09.0 requires physically distinct SQLite account files"
                );
                file.display().to_string()
            })
            .collect::<Vec<_>>();
        t09_databases.sort();
        assert_eq!(
            t09_databases.len(),
            2,
            "one independent SQLite file per account"
        );
        assert_ne!(
            t09_bootstrap_accounts[0]["verified_uid"], t09_bootstrap_accounts[1]["verified_uid"],
            "T09.0 requires two independently verified account UIDs"
        );
        println!(
            "T09_0_NATIVE_SQL_EVIDENCE_V1:{}",
            json!({
                "accounts":t09_bootstrap_accounts,"sqlite_files":t09_databases,
            })
        );
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
        assert_eq!(budget.reserved_loss_usd, if add_mode { "0" } else { "1" });
        let entry = budget.entry_budget.as_ref().unwrap();
        assert_eq!(entry.reserved_usd, if add_mode { "0" } else { "2" });
        assert_eq!(entry.consumed_usd, "0");
        // The Worker publishes its real PolicySnapshot over the same WSS.
        // Its revision must be durably applied by the owning SQL worker
        // before the account may claim central authority.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let capabilities = trade
                    .trade(
                        "fixture-a",
                        &okx_protocol::trading::TradeRequest::Capabilities {},
                        None,
                    )
                    .await
                    .unwrap();
                if capabilities["native_sql_policy"]["matches_governance"] == true {
                    assert_eq!(capabilities["governance"]["enabled"], true);
                    assert_eq!(capabilities["native_sql_policy"]["revision"], 2);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Worker PolicySnapshot was not committed by native account SQL");
        // Both SQL owners must have independently applied the SAME Worker
        // revision before any shared-risk reservation is attempted.
        let caps_b = trade
            .trade(
                "fixture-b",
                &okx_protocol::trading::TradeRequest::Capabilities {},
                None,
            )
            .await
            .unwrap();
        assert_eq!(caps_b["native_sql_policy"]["matches_governance"], true);
        assert_eq!(caps_b["native_sql_policy"]["revision"], 2);
        // Positive public Preview: the production instrument parser,
        // market coordinator, account bootstrap, fee calculator, SQL policy
        // and final value projection all run against the same live owners.
        // Preview must not create a reserve or send any venue POST.
        use okx_protocol::trading::{
            OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
        };
        let preview = TradeRequest::Preview {
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
        };
        assert!(preview.valid());
        for alias in ["fixture-a", "fixture-b"] {
            // Two background financial owners and capability reads share the
            // genuine five-per-two-second OKX config budget per UID. A LOCAL
            // no-send defer may happen before Preview reaches any venue API.
            // Preview has no POST/reservation; use the *typed* RateBudget
            // evidence to retry it, with a fixed overall test deadline.
            let until = std::time::Instant::now() + Duration::from_secs(15);
            let output = loop {
                let answer =
                    tokio::time::timeout_at(until.into(), trade.trade(alias, &preview, None))
                        .await
                        .expect("T09 preview bounded rate-admission deadline");
                match answer {
                    Ok(value) => break value,
                    Err(reason) => {
                        let remaining = until.saturating_duration_since(std::time::Instant::now());
                        let delay = recorded_preview_local_defer(&reason, &rate_budget, remaining)
                            .unwrap_or_else(|| {
                                panic!("T09 preview non-retryable failure: {reason}")
                            });
                        tokio::time::timeout_at(until.into(), tokio::time::sleep(delay))
                            .await
                            .expect("T09 local no-send rate deferral exceeded preview budget");
                        // New raw market observation is necessary after a defer:
                        // reusing a time-expired quote would be a false PASS.
                        refresh_recorded_market();
                    }
                }
            };
            assert_eq!(output["status"], "completed");
            assert_eq!(output["data"]["entry_costs"]["entry_total_usd"], "1.0005");
            assert_eq!(
                output["data"]["entry_costs"]["all_in_entry_cost_proven"],
                true
            );
            assert_eq!(
                output["data"]["entry_costs"]["entry_budget_upper_reporting"],
                "1.0005"
            );
            assert_eq!(output["data"]["admission"], "preview_only");
            assert_eq!(output["data"]["side_effects"], "none");
        }
        assert!(
            feed.outbound_posts.lock().unwrap().is_empty(),
            "public preview must never invoke the production venue POST transport"
        );
        assert_eq!(
            feed.unconsumed_reads(),
            0,
            "every scripted fee/leverage source must have been consumed"
        );
        let mut before = summary("risk_public_trade_did_not_post_001");
        before.installation_id = "installation_test".into();
        let untouched = risk.request(before).await.unwrap();
        assert_eq!(
            untouched.budget.as_ref().unwrap().reserved_loss_usd,
            if add_mode { "0" } else { "1" }
        );
        assert_eq!(
            untouched
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            if add_mode { "0" } else { "2" }
        );
        if !add_mode {
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
        }
        // The second intent is sent by actual Rust Native through the same
        // authenticated WSS path. Evidence values are explicitly scripted
        // boundary inputs: no venue claims and no exchange POST in this test.
        let checked = if add_mode {
            let mut cmd = summary("risk_t09_4_clean_native");
            cmd.installation_id = "installation_test".into();
            risk.request(cmd).await.unwrap()
        } else {
            let uid = okx_api::account_uid_fingerprint("account-001");
            let account_key = format!("okx:real:{uid}");
            let account_key_b = format!(
                "okx:real:{}",
                okx_api::account_uid_fingerprint("account-002")
            );
            let mut account_keys = vec![account_key.clone(), account_key_b];
            account_keys.sort();
            assert_ne!(account_keys[0], account_keys[1]);
            let timestamp = || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            };
            let captured_at = timestamp() - 300;
            let census = NativeRiskCensus {
                account_keys: account_keys.clone(),
                occupied: account_keys
                    .iter()
                    .map(|key| RiskAccountOccupancy {
                        account_key: key.clone(),
                        occupied_usdt: "0".into(),
                        cumulative_loss_usd: "0".into(),
                    })
                    .collect(),
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
                    policy_revision: 2,
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
                                policy_revision: 2,
                                expected_risk_book_sequence: sequence,
                                source_proof_sha256: "f".repeat(64),
                                census: fresh,
                                causal_proof: CausalPostProof {
                                    reserve_anchor: key,
                                    native_incarnation: "a".repeat(32),
                                    elapsed_to_proof_ms: 30,
                                    account_reads: account_keys
                                        .iter()
                                        .map(|key| PostAccountReadProof {
                                            account_key: key.clone(),
                                            private_generation: 7,
                                            source_read_id: "b".repeat(64),
                                            content_digest: "a".repeat(64),
                                            started_after_held_ms: 5,
                                            completed_after_held_ms: 20,
                                        })
                                        .collect(),
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
            checked
        };
        // T09.1 production admission probe. Unlike the preceding component
        // risk commands this executes the PUBLIC TradeService command, using
        // exactly one pre-recorded raw OKX POST and actual Native/Worker owners.
        // It must never be counted as full T09 until fill/close/restart are
        // independently reconciled in the same assembly.
        // Start the actual Engine maintenance owner; admission must not
        // race an as-yet-unreconciled campaign history. Read each account's
        // independent SQL owner until the persisted readiness proof exists.
        // Neither the test nor this readback writes a synthetic receipt.
        let (maintenance_shutdown, maintenance_rx) = watch::channel(false);
        let maintenance_trade = trade.clone();
        let maintenance_task = tokio::spawn(async move {
            maintenance_trade.run(maintenance_rx).await;
        });
        let native_origin = i64::try_from(
            checked
                .budget
                .as_ref()
                .unwrap()
                .campaign_origin
                .as_ref()
                .unwrap()
                .started_at_ms,
        )
        .unwrap();
        let campaign_id = checked
            .budget
            .as_ref()
            .unwrap()
            .entry_budget
            .as_ref()
            .unwrap()
            .campaign_id
            .clone();
        let mut last_readiness_error = String::from("NOT_CHECKED");
        let readiness = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let through = chrono::Utc::now().timestamp_millis() - 2_000;
                let mut both_ready = true;
                for alias in ["fixture-a", "fixture-b"] {
                    let owner = accounts.resolve(alias).await.unwrap();
                    let campaign = campaign_id.clone();
                    match owner
                        .execution
                        .store
                        .call(
                            std::time::Instant::now() + Duration::from_secs(2),
                            move |s| {
                                s.verify_campaign_entry_readiness(&campaign, native_origin, through)
                            },
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(e) => {
                            last_readiness_error = format!("{alias}: {e}");
                            both_ready = false;
                            break;
                        }
                    }
                }
                if both_ready {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(
            readiness.is_ok(),
            "production maintenance must reconcile BOTH native SQL campaign frontiers: {last_readiness_error}"
        );
        use crate::accounts::recorded_support::RecordedPost;
        // SQL baseline recovery may take several capture slots. Refresh the
        // market strictly by replaying a NEW, timestamped set of raw public WS
        // inputs into the existing connected production coordinator. There is
        // no direct market-state setter and no freshness-TTL exemption.
        let refreshed_at = refresh_recorded_market();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let rules = public_ws.instrument_rules("BTC-USDT-SWAP").await.unwrap();
                let reference_ms = chrono::DateTime::parse_from_rfc3339(&rules.source_received_at)
                    .unwrap()
                    .timestamp_millis();
                let fresh = public_ws
                    .risk_fresh_snapshot(
                        "BTC-USDT-SWAP",
                        chrono::Utc::now().timestamp_millis() as u64,
                        5_000,
                        5_000,
                        chrono::Utc::now().to_rfc3339(),
                    )
                    .await
                    .is_ok();
                if reference_ms >= refreshed_at && fresh {
                    break;
                }
                market_updates
                    .changed()
                    .await
                    .expect("public coordinator exited");
            }
        })
        .await
        .expect("refreshed recorded WS sources were not accepted by production parser");
        let fresh_fee_at = chrono::Utc::now().timestamp_millis().to_string();
        for _ in 0..3 {
            feed.expect_get(RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/account/trade-fee".into(),
                required_query: vec![
                    ("instType".into(), "SWAP".into()),
                    ("instFamily".into(), "BTC-USDT".into()),
                ],
                raw_response: envelope(json!([{
                    "instType":"SWAP","level":"Lv1","ts":fresh_fee_at,
                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                }])),
            });
            feed.expect_get(RecordedGet {
                access_key: "fixture-observer-a".into(),
                path: "/api/v5/account/leverage-info".into(),
                required_query: vec![
                    ("instId".into(), "BTC-USDT-SWAP".into()),
                    ("mgnMode".into(), "cross".into()),
                ],
                raw_response: envelope(json!([
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                ])),
            });
        }
        let fresh_clock_at = chrono::Utc::now().timestamp_millis().to_string();
        feed.expect_public_clock(envelope(json!([{"ts":fresh_clock_at}])));
        feed.expect_post(RecordedPost {
            access_key: "fixture-executor-a".into(),
            path: "/api/v5/trade/order".into(),
            expected_body: json!({
                "instId":"BTC-USDT-SWAP","tdMode":"cross",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","ordType":"limit",
                "sz":"0.1","px":"1000",
                "attachAlgoOrds":[{
                    "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                    "tpTriggerPxType":"mark",
                    "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
                }]
            }),
            raw_response: br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t09-opening-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
        });
        // A successful create ACK is not a complete lifecycle. Production
        // status independently queries the order by the exact submitted
        // client ID. Record BOTH GETs (first Execute and idempotent replay).
        // "live" with zero fills is an actual distinct test source, not
        // a fabricated fill or settlement.
        let raw_order = envelope(json!([{
            "instId":"BTC-USDT-SWAP",
            "ordId":"recorded-t09-opening-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","tdMode":"cross",
            "ordType":"limit","px":"1000","sz":"0.1",
            "accFillSz":"0","avgPx":"",
            "state":"live",
            "cTime":fresh_clock_at,"uTime":fresh_clock_at
        }]));
        for _ in 0..2 {
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                raw_response: raw_order.clone(),
            });
        }
        let execute = TradeRequest::Execute {
            idempotency_key: "t09_real_public_execute_0001".into(),
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
            expires_in_ms: 30_000,
        };
        assert!(execute.valid());
        // The recorded venue has a live public feed, not a single frozen tick.
        // The production PublicWsCoordinator receives ONLY raw transcript
        // frames. No market-state setter or source-freshness bypass is used.
        // A bounded test-only publisher continues the recorded stream during
        // rate-budget admission and after-HELD census.
        let (feed_shutdown, mut feed_stop) = watch::channel(false);
        let feed_connector = connector.clone();
        let feed_rows = recorded_public.clone();
        let public_feed = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(750));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut book_seq = 12_i64;
            loop {
                tokio::select! {
                    changed = feed_stop.changed() => {
                        if changed.is_err() || *feed_stop.borrow() {
                            break;
                        }
                    }
                    _ = ticks.tick() => {
                        let observed_at = chrono::Utc::now().timestamp_millis();
                        for (arg, original, action) in &feed_rows {
                            if arg["channel"] == "instruments" && arg["instType"] == "SWAP" {
                                feed_connector.send_public_frame(json!({
                                    "arg": arg,
                                    "data":[{
                                        "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                        "instFamily":"BTC-USDT","uly":"BTC-USDT",
                                        "state":"live","ruleType":"normal",
                                        "settleCcy":"USDT","tickSz":"0.1",
                                        "lotSz":"0.01","minSz":"0.01",
                                        "maxLmtSz":"1000","maxMktSz":"1000",
                                        "ctType":"linear","ctVal":"0.01",
                                        "ctValCcy":"BTC","groupId":"4","lever":"20"
                                    }]
                                }).to_string());
                            } else if let Some(mut value) = original.clone() {
                                value["ts"] = observed_at.to_string().into();
                                if value.get("fundingTime").is_some() {
                                    value["fundingTime"] = observed_at.to_string().into();
                                    value["nextFundingTime"] =
                                        (observed_at + 8 * 60 * 60 * 1000).to_string().into();
                                }
                                if *action == Some("snapshot") {
                                    value["seqId"] = book_seq.into();
                                    book_seq += 1;
                                }
                                let mut frame = json!({"arg":arg,"data":[value]});
                                if let Some(action) = action {
                                    frame["action"] = (*action).into();
                                }
                                feed_connector.send_public_frame(frame.to_string());
                            }
                        }
                    }
                }
            }
        });
        // An admission can be deferred by the SHARED production RateBudget
        // while maintenance consumes the same authenticated private-GET
        // domain. Retry is permitted here ONLY after a proven LOCAL,
        // NOT_STARTED, RELEASED intent with zero venue POST attempts. Every
        // subsequent request is a new explicit intent; an UNKNOWN/potentially
        // sent mutation must NEVER be retried.
        let mut execute = execute;
        let opened = Box::pin(async {
            let admission_deadline = std::time::Instant::now() + Duration::from_secs(16);
            for attempt in 0..4 {
                let admission = trade
                    .trade("fixture-a", &execute, None)
                    .await
                    .expect("T09.1 public Execute must return its typed admission");
                if admission["status"] == "partial" {
                    return admission;
                }
                assert_eq!(
                    admission["status"], "completed",
                    "T09.1 cannot retry UNKNOWN or partial execution: {admission}"
                );
                assert_eq!(
                    admission["data"]["state"], "REJECTED",
                    "only definitive local rejection may permit a new intent: {admission}"
                );
                assert_eq!(
                    admission["data"]["rejection_origin"], "LOCAL",
                    "venue-side or unverified rejection cannot authorize retry: {admission}"
                );
                assert_eq!(
                    admission["data"]["send_state"], "NOT_STARTED",
                    "potentially sent POST must remain UNKNOWN/no-resubmit: {admission}"
                );
                assert_eq!(
                    admission["data"]["risk_release_state"], "RELEASED",
                    "risk must be released after definitive local rejection: {admission}"
                );
                assert_eq!(
                    feed.outbound_posts.lock().unwrap().len(),
                    0,
                    "local rate denial must occur before ANY venue create attempt"
                );
                let reason = admission["data"]["primary_reason"]
                    .as_str()
                    .expect("T09.1 local denial must provide a reason");
                let local_reason = reason.strip_prefix("OKX API error: ").unwrap_or(reason);
                let remaining =
                    admission_deadline.saturating_duration_since(std::time::Instant::now());
                let delay = recorded_preview_local_defer(local_reason, &rate_budget, remaining)
                    .unwrap_or_else(|| {
                        panic!("T09.1 non-private-GET/nonlocal denial cannot retry: {admission}; rate={:?}",
                            rate_budget.snapshot().last_throttle)
                    });
                assert!(attempt < 3, "T09.1 bounded local no-send recovery exhausted");
                let mut audit = summary(&format!("risk_t09_1_local_no_send_{attempt}"));
                audit.installation_id = "installation_test".into();
                let after_denial = risk.request(audit).await.unwrap();
                assert_eq!(
                    after_denial.budget.as_ref().unwrap()
                        .entry_budget.as_ref().unwrap().reserved_usd,
                    if add_mode {"0"} else {"4"},
                    "a denied unsent opening must not retain central entry budget"
                );
                println!(
                    "T09_1_LOCAL_NO_SEND_RATE_RECOVERY_V1:{}",
                    json!({
                        "attempt":attempt,
                        "reason":reason,
                        "delay_ms":delay.as_millis(),
                        "outbound_posts":0,
                        "reserved_entry_usd":if add_mode {"0"} else {"4"},
                    })
                );
                tokio::time::timeout_at(
                    admission_deadline.into(),
                    tokio::time::sleep(delay),
                ).await.expect("T09.1 bounded local private GET defer expired");
                refresh_recorded_market();
                // Refresh signed external fee/leverage responses as RAW GETs;
                // previous attempts' pages cannot be promoted to new venue time.
                let retry_fee_at = chrono::Utc::now().timestamp_millis().to_string();
                feed.advance_recorded_route(
                    "fixture-observer-a",
                    "/api/v5/account/trade-fee",
                    (0..3).map(|_| RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/account/trade-fee".into(),
                        required_query: vec![
                            ("instType".into(), "SWAP".into()),
                            ("instFamily".into(), "BTC-USDT".into()),
                        ],
                        raw_response: envelope(json!([{
                            "instType":"SWAP","level":"Lv1","ts":retry_fee_at,
                            "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                        }])),
                    }).collect(),
                );
                feed.advance_recorded_route(
                    "fixture-observer-a",
                    "/api/v5/account/leverage-info",
                    (0..3).map(|_| RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/account/leverage-info".into(),
                        required_query: vec![
                            ("instId".into(), "BTC-USDT-SWAP".into()),
                            ("mgnMode".into(), "cross".into()),
                        ],
                        raw_response: envelope(json!([
                            {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                            {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                        ])),
                    }).collect(),
                );
                let retry_clock = chrono::Utc::now().timestamp_millis().to_string();
                feed.advance_recorded_public_clock(envelope(json!([{"ts":retry_clock}])));
                if let TradeRequest::Execute { idempotency_key, .. } = &mut execute {
                    *idempotency_key = format!("t09_real_public_execute_0001_rate_{attempt}");
                }
            }
            panic!("T09.1 exhausted definitive local no-send admission attempts");
        }).await;
        // Venue ACK and identity-bound order readback prove the create,
        // but no fill/fee/bill has been published. The production Engine
        // must report financial incompleteness instead of fabricated money.
        assert_eq!(
            opened["status"], "partial",
            "T09.1 public ACK without settlement must remain partial: {opened}"
        );
        assert_eq!(opened["data"]["intent"]["state"], "ACKNOWLEDGED");
        assert_eq!(
            opened["data"]["intent"]["exchange_order_id"],
            "recorded-t09-opening-001"
        );
        assert_eq!(
            opened["data"]["entry_costs"]["entry_budget_upper_reporting"],
            "1.0005"
        );
        assert_eq!(opened["data"]["financial_complete"], false);
        assert_eq!(opened["warnings"], json!(["FINANCIAL_BACKFILL_PENDING"]));
        assert_eq!(
            feed.unconsumed_posts(),
            0,
            "T09.1 must consume the one signed create response; actual public TradeService outcome: {opened}; outbound POST attempts: {:?}",
            feed.outbound_posts.lock().unwrap()
        );
        assert_eq!(
            feed.unconsumed_public_clock(),
            0,
            "production exchange-clock evidence must come from the raw response"
        );
        {
            // The synchronous transcript lock cannot live across a Rust
            // await. The scope also proves assertions read one snapshot.
            let sends = feed.outbound_posts.lock().unwrap();
            assert_eq!(
                sends.len(),
                1,
                "at most one possible create POST per intent"
            );
            assert_eq!(sends[0].0, "fixture-executor-a");
            assert_eq!(sends[0].1, "/api/v5/trade/order");
        }
        let mut before_repeat = summary("risk_before_t09_5_idempotent_replay");
        before_repeat.installation_id = "installation_test".into();
        let before_repeat = risk.request(before_repeat).await.unwrap();
        assert_eq!(
            before_repeat
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            if add_mode { "1.0005" } else { "5.0005" }
        );
        let repeated = trade
            .trade("fixture-a", &execute, None)
            .await
            .expect("T09.1 identical Execute must be idempotent");
        // An unchanged idempotency key must never send another POST. When
        // the read-only order GET is locally deferred, UNKNOWN with the exact
        // no-resubmit reason is the safe production answer; it is not an
        // alternative proof of a fill, ACK or financial settlement.
        let replay_readback_unknown = repeated["status"] == "unknown";
        let opened_intent_id = opened["data"]["intent"]["intent_id"]
            .as_str()
            .expect("public intent ID")
            .to_string();
        if replay_readback_unknown {
            assert_eq!(
                repeated["reason_code"],
                "ORDER_READBACK_INCOMPLETE_NO_RESUBMIT"
            );
            assert_eq!(repeated["retryable"], false);
            assert_eq!(repeated["intent_id"], opened_intent_id);
        } else {
            assert_eq!(
                repeated["status"], "partial",
                "unexpected idempotent replay outcome: {repeated}"
            );
            assert_eq!(repeated["data"]["intent"]["state"], "ACKNOWLEDGED");
            assert_eq!(
                repeated["data"]["intent"]["exchange_order_id"],
                "recorded-t09-opening-001"
            );
            assert_eq!(repeated["data"]["intent"]["intent_id"], opened_intent_id);
        }
        assert_eq!(
            feed.outbound_posts.lock().unwrap().len(),
            1,
            "identical Execute must not post to OKX twice"
        );
        // A read-only replay may return UNKNOWN even after transport has
        // consumed the raw GET: a deadline or decode/readback failure does
        // not turn the observation into a verified outcome. Only an
        // *unattempted* read leaves exactly one queued transcript reply.
        // Both cases must remain no-resubmit and subsequently reconcile.
        let replay_remaining = feed.unconsumed_reads();
        assert!(
            replay_remaining == 0 || (replay_readback_unknown && replay_remaining == 1),
            "T09_5_REPLAY_TRANSCRIPT_INVALID: status={}, remaining={replay_remaining}",
            repeated["status"]
        );
        let mut after_public = summary("risk_after_public_trade_post_001");
        after_public.installation_id = "installation_test".into();
        let after = risk.request(after_public).await.unwrap();
        assert_eq!(after.status, RiskDecisionStatus::Observed);
        let idempotent_intent_id = opened_intent_id;
        let saved_intent_id = idempotent_intent_id.clone();
        let saved = accounts
            .resolve("fixture-a")
            .await
            .unwrap()
            .execution
            .store
            .call(
                std::time::Instant::now() + Duration::from_secs(3),
                move |s| s.intent(&saved_intent_id),
            )
            .await
            .unwrap()
            .expect("idempotent public intent must persist in native SQL");
        assert_eq!(saved.intent_id, idempotent_intent_id);
        assert_eq!(saved.state, okx_execution::IntentState::Acknowledged);
        assert_eq!(
            before_repeat
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            after
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            "replaying a public idempotency key must not reserve more central entry capacity"
        );
        println!(
            "T09_5_NATIVE_IDEMPOTENCY_EVIDENCE_V1:{}",
            json!({
                "account_uid": accounts.resolve("fixture-a").await.unwrap().configuration.account_key.verified_uid,
                "opened_intent_id":idempotent_intent_id,
                "replayed_intent_id":if replay_readback_unknown {
                    repeated["intent_id"].clone()
                } else {
                    repeated["data"]["intent"]["intent_id"].clone()
                },
                "replay_readback_unknown_no_resubmit":replay_readback_unknown,
                "sql_intent_id":saved.intent_id,
                "sql_revision":saved.revision,
                "sql_state":saved.state.as_str(),
                "created_posts":feed.outbound_posts.lock().unwrap().len(),
                "reserved_entry_usd_before_replay": before_repeat.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
                "reserved_entry_usd_after_replay": after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
            })
        );
        assert_eq!(
            after
                .budget
                .as_ref()
                .unwrap()
                .entry_budget
                .as_ref()
                .unwrap()
                .reserved_usd,
            if add_mode { "1.0005" } else { "5.0005" },
            "RiskBook holds the public entry without duplicate reservations"
        );
        // The Close probe needs a fresh raw public price after financial
        // reconciliation. Baseline T09.0/T09.5 still stops the publisher here.
        let status_probe = std::env::var("OKX_T09_1_STATUS_PROBE").as_deref() == Ok("1");
        if !status_probe {
            feed_shutdown.send(true).unwrap();
        }
        maintenance_shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), maintenance_task)
            .await
            .expect("maintenance owner did not terminate")
            .unwrap();
        // One recorded real-format fill and one linked fee bill through the
        // EXISTING account source cycle, never a direct test SQL ingest.
        // This probes T09.1 financial sources; Close/release remain unaccepted.
        let financial_time = chrono::Utc::now().timestamp_millis().to_string();
        let raw_fill = envelope(json!([{
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "tradeId":"t09-entry-trade-001","ordId":"recorded-t09-opening-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","billId":"t09-entry-bill-001",
            "subType":"3","side":"buy","posSide":"long",
            "fillPx":"1000","fillSz":"0.1","fillPnl":"0",
            "fee":"-0.0005","feeCcy":"USDT",
            "ts":financial_time,"fillTime":financial_time
        }]));
        let raw_bill = envelope(json!([{
            "billId":"t09-entry-bill-001","instType":"SWAP",
            "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-opening-001",
            "tradeId":"t09-entry-trade-001","clOrdId":"__REQUEST_CL_ORD_ID__",
            "ccy":"USDT","type":"2","subType":"3",
            "balChg":"-0.0005","fee":"-0.0005","pnl":"0","ts":financial_time
        }]));
        // Same immutable page can be read more than once by the 2-second
        // owner; SQL event IDs must still make money exactly-once.
        for _ in 0..12 {
            // A strict signed history route covers ALL five instType
            // partitions. The other four have their own explicit empty
            // recordings; no source may fall back to an invented zero page.
            for partition in ["SPOT", "MARGIN", "FUTURES", "OPTION"] {
                feed.expect_get(RecordedGet {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/fills-history".into(),
                    required_query: vec![("instType".into(), partition.into())],
                    raw_response: envelope(json!([])),
                });
            }
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/fills-history".into(),
                required_query: vec![("instType".into(), "SWAP".into())],
                raw_response: raw_fill.clone(),
            });
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/account/bills-archive".into(),
                required_query: vec![],
                raw_response: raw_bill.clone(),
            });
        }
        let owner = accounts.resolve("fixture-a").await.unwrap();
        let id = idempotent_intent_id.clone();
        let captured = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                let intent = id.clone();
                let readback = owner
                    .execution
                    .store
                    .call(
                        std::time::Instant::now() + Duration::from_secs(3),
                        move |s| {
                            let fills = s.cursor("fills", "SWAP")?.ok_or(
                                okx_execution::StoreError::Invalid("T09_FILL_CURSOR_MISSING"),
                            )?;
                            let bills = s.cursor("bills", "account")?.ok_or(
                                okx_execution::StoreError::Invalid("T09_BILL_CURSOR_MISSING"),
                            )?;
                            Ok((
                                s.intent_fill_cost_evidence(&intent, fills.watermark),
                                s.intent_fee_bill_evidence(&intent, bills.watermark),
                            ))
                        },
                    )
                    .await;
                if let Ok((Ok(fill), Ok(bill))) = readback {
                    break (fill, bill);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        let captured = match captured {
            Ok(captured) => captured,
            Err(_) => {
                // Read-only diagnostics from the SAME live Native SQL owner.
                // Do not manufacture coverage, ingest test rows or release risk.
                let health = owner.source_capture_health.read().await;
                let reason = health.reason_code.clone();
                let last_success = health.last_success_ms;
                let last_failure = health.last_failure_ms;
                drop(health);
                let intent = idempotent_intent_id.clone();
                let cursors = owner.execution.store
                    .call(std::time::Instant::now() + Duration::from_secs(3), move |s| {
                        let fills = s.cursor("fills", "SWAP")?;
                        let bills = s.cursor("bills", "account")?;
                        let fill_watermark = fills.as_ref().map_or(0, |c| c.watermark);
                        let bill_watermark = bills.as_ref().map_or(0, |c| c.watermark);
                        Ok(json!({
                            "fills_cursor": fills,
                            "bills_cursor": bills,
                            "fills":format!("{:?}", s.intent_fill_cost_evidence(&intent,fill_watermark)),
                            "fee_bills":format!("{:?}", s.intent_fee_bill_evidence(&intent,bill_watermark))
                        }))
                    }).await;
                // Read-only independent SQLite diagnostic: source refs and
                // matching economic rows only, never direct account mutation.
                let financial_links = std::fs::read_dir(root.join("accounts"))
                    .unwrap()
                    .filter_map(|entry| {
                        let path = entry.ok()?.path().join("account.sqlite");
                        let sql = rusqlite::Connection::open_with_flags(
                            path,
                            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                        ).ok()?;
                        let mut stmt = sql.prepare(
                            "SELECT l.source_ref,e.entry_id,e.economic_event_key,e.component,e.allocation_leg,e.amount,e.currency
                             FROM finance_source_links l
                             JOIN finance_entries e ON e.entry_id=l.entry_id
                             WHERE l.source_ref LIKE '%t09-entry-trade-001%'
                                OR e.economic_event_key='bill:t09-entry-bill-001'
                             ORDER BY l.source_ref,e.entry_id LIMIT 24",
                        ).ok()?;
                        let entries = stmt.query_map([], |r| {
                            Ok((
                                r.get::<_,String>(0)?,
                                r.get::<_,String>(1)?,
                                r.get::<_,String>(2)?,
                                r.get::<_,String>(3)?,
                                r.get::<_,String>(4)?,
                                r.get::<_,String>(5)?,
                                r.get::<_,String>(6)?,
                            ))
                        }).ok()?.filter_map(Result::ok).collect::<Vec<_>>();
                        Some(entries)
                    })
                    .collect::<Vec<_>>();
                let observed = feed.requests.lock().unwrap();
                let recent = observed.iter().rev().take(24).cloned().collect::<Vec<_>>();
                panic!(
                    "T09_FILL_BILL_CAPTURE_NOT_PROVEN reason={reason:?} last_success={last_success:?} last_failure={last_failure:?} sql={cursors:?} finance_source_links={financial_links:?} queued_recorded_pages={} observed_requests={recent:?}",
                    feed.unconsumed_reads()
                );
            }
        };
        // The readback's account runtime is a separate Arc cloned from the
        // registry. Drop it explicitly so the SQL owner can shut down after
        // the session and registry owners are released.
        drop(owner);
        assert_eq!(captured.0.fill_count, 1);
        assert_eq!(captured.0.filled_quantity, Decimal::new(1, 1));
        assert_eq!(captured.0.gross_notional_usdt, Decimal::ONE);
        assert_eq!(captured.1.linked_bill_count, 1);
        assert_eq!(
            captured.1.conservative_verified_fee_usdt,
            Decimal::new(5, 4)
        );
        println!(
            "T09_1_NATIVE_FILL_BILL_EVIDENCE_V1:{}",
            json!({
                "intent_id":idempotent_intent_id,
                "fills":captured.0.fill_count,"bills":captured.1.linked_bill_count,
                "notional_usdt":captured.0.gross_notional_usdt.to_string(),
                "fee_usdt":captured.1.conservative_verified_fee_usdt.to_string(),
                "source_digest":captured.1.source_digest,
                "state":"FILL_AND_BILL_VERIFIED_CLOSE_RELEASE_PENDING"
            })
        );
        // Optional independent T09.1 continuation. This uses the very same
        // live TradeService, signed Executor GET, account SQL owner and central
        // workerd RiskBook, but never changes the accepted T09.0/T09.5 run.
        let mut managed_add_proof: Option<serde_json::Value> = None;
        if status_probe {
            let after_fill_order = envelope(json!([{
                "instId":"BTC-USDT-SWAP",
                "ordId":"recorded-t09-opening-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","tdMode":"cross",
                "ordType":"limit","px":"1000","sz":"0.1",
                "accFillSz":"0.1","avgPx":"1000",
                "state":"filled",
                "cTime":financial_time,"uTime":financial_time
            }]));
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                raw_response: after_fill_order,
            });
            let position = envelope(json!([{
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "posSide":"long","pos":"0.1","mgnMode":"cross",
                "avgPx":"1000","markPx":"1000","uTime":financial_time
            }]));
            for _ in 0..16 {
                feed.expect_get(RecordedGet {
                    access_key: "fixture-observer-a".into(),
                    path: "/api/v5/account/positions".into(),
                    required_query: vec![],
                    raw_response: position.clone(),
                });
            }
            let client = saved
                .client_order_id
                .get(..31)
                .expect("native client ID is at least 31 bytes");
            let stop_client = format!("s{client}");
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order-algo".into(),
                required_query: vec![("algoClOrdId".into(), stop_client.clone())],
                raw_response: envelope(json!([{
                    "instId":"BTC-USDT-SWAP",
                    "algoId":"900000001",
                    "algoClOrdId":stop_client,
                    "state":"live","sz":"0.1","side":"sell",
                    "posSide":"long","tdMode":"cross","ordType":"conditional",
                    "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
                }])),
            });
            let actual = trade
                .trade(
                    "fixture-a",
                    &okx_protocol::trading::TradeRequest::Status {
                        reference: idempotent_intent_id.clone(),
                    },
                    None,
                )
                .await
                .expect("production trade.status must return its real evidence");
            assert_eq!(
                feed.outbound_posts.lock().unwrap().len(),
                1,
                "trade.status after a fill must never replay an exchange create"
            );
            let mut after_status = summary("risk_t09_1_after_public_status_001");
            after_status.installation_id = "installation_test".into();
            let observed = risk.request(after_status).await.unwrap();
            println!(
                "T09_1_NATIVE_POST_FILL_STATUS_V1:{}",
                json!({
                    "intent_id":idempotent_intent_id,
                    "state":actual["data"]["intent"]["state"],
                    "status":actual["status"],
                    "entry_fill_evidence":actual["data"]["entry_fill_evidence"],
                    "incremental_entry_settlement":actual["data"]["incremental_entry_settlement"],
                    "protection":actual["data"]["protection"],
                    "central_release":actual["data"]["central_release"],
                    "financial_complete":actual["data"]["financial_complete"],
                    "worker_budget":observed.budget,
                    "risk_book_sequence":observed.risk_book_sequence,
                    "created_posts":feed.outbound_posts.lock().unwrap().len()
                })
            );
            // T09.1 next boundary: a user-facing Close is routed through the
            // same TradeService/SQL owner. The only synthetic input is the
            // independently recorded signed OKX create ACK and order GET.
            // It may not claim a flat position until close fills and the
            // protection inventory are independently captured.
            use crate::accounts::recorded_support::RecordedPost;
            let close_time = chrono::Utc::now().timestamp_millis().to_string();
            feed.expect_public_clock(envelope(json!([{"ts":close_time}])));
            feed.expect_post(RecordedPost {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                expected_body: json!({
                    "instId":"BTC-USDT-SWAP","tdMode":"cross",
                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                    "side":"sell","posSide":"long","ordType":"ioc",
                    "sz":"0.1","px":"999"
                }),
                raw_response: br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t09-close-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
            });
            feed.expect_get(RecordedGet {
                access_key: "fixture-executor-a".into(),
                path: "/api/v5/trade/order".into(),
                required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                raw_response: envelope(json!([{
                    "instId":"BTC-USDT-SWAP",
                    "ordId":"recorded-t09-close-001",
                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                    "side":"sell","posSide":"long","tdMode":"cross",
                    "ordType":"ioc","px":"999.0","sz":"0.1",
                    "accFillSz":"0","avgPx":"","state":"live",
                    "cTime":close_time,"uTime":close_time
                }])),
            });
            let close_result = trade
                .trade(
                    "fixture-a",
                    &okx_protocol::trading::TradeRequest::Close {
                        idempotency_key: "t09_real_public_close_0001".into(),
                        reference: idempotent_intent_id.clone(),
                        expires_in_ms: 30_000,
                    },
                    None,
                )
                .await;
            if let Err(reason) = &close_result {
                // The Node runner must show this exact public production
                // denial instead of truncating a nested cargo panic stack.
                let resource = accounts.resolve("fixture-a").await.unwrap();
                let health = resource.source_capture_health.read().await;
                let source_reason = health.reason_code.clone();
                let source_success = health.last_success_ms;
                drop(health);
                println!(
                    "T09_1_NATIVE_CLOSE_FAILURE_V1:{}",
                    json!({
                        "reason_code":reason,
                        "source_reason_code":source_reason,
                        "last_source_success_ms":source_success,
                        "outbound_posts":feed.outbound_posts.lock().unwrap().len()
                    })
                );
            }
            let close = close_result.expect("T09.1 Close must use production TradeService");
            let attempted_posts = feed.outbound_posts.lock().unwrap().len();
            let close_ack = &close["data"]["intent"]["exchange_order_id"];
            if attempted_posts != 2 || close_ack != "recorded-t09-close-001" {
                // An HTTP attempt counts even when the transport rejects the
                // exact body. Record the observed wire payload and the public
                // result, never infer an exchange ACK from POST count alone.
                let outbound = feed.outbound_posts.lock().unwrap();
                println!(
                    "T09_1_NATIVE_CLOSE_FAILURE_V1:{}",
                    json!({
                        "reason_code":format!(
                            "CLOSE_ACK_UNPROVEN:status={}:reason={}:intent_state={}:send_state={}:ack={}",
                            close["status"],close["reason_code"],
                            close["data"]["intent"]["state"],close["data"]["send_state"],close_ack
                        ),
                        "outbound_posts":attempted_posts,
                        "source_reason_code":null,
                        "unconsumed_recorded_posts":feed.unconsumed_posts(),
                        "last_outbound_post":outbound.last(),
                        "close_result":close
                    })
                );
            }
            assert_eq!(
                attempted_posts, 2,
                "open + close must issue exactly one venue create each"
            );
            assert_eq!(close_ack, "recorded-t09-close-001");
            assert_ne!(close["data"]["intent"]["intent_id"], idempotent_intent_id);
            println!(
                "T09_1_NATIVE_CLOSE_ACK_V1:{}",
                json!({
                    "opening_intent_id":idempotent_intent_id,
                    "close_intent_id":close["data"]["intent"]["intent_id"],
                    "close_state":close["data"]["intent"]["state"],
                    "close_order_id":close["data"]["intent"]["exchange_order_id"],
                    "created_posts":feed.outbound_posts.lock().unwrap().len(),
                    "financial_complete":close["data"]["financial_complete"],
                    "status":close["status"]
                })
            );
            Box::pin(async {
                // Explicitly transition only the RECORDED venue transport from
                // the still-open position to the authenticated terminal close.
                // The Native source-capture SQL owner itself is unchanged.
                // An external CLOSE ACK alone was insufficient in earlier T09.1.
                let closing_at = chrono::Utc::now().timestamp_millis().to_string();
                let raw_close_fill = envelope(json!([
                    {
                        "instType":"SWAP","instId":"BTC-USDT-SWAP",
                        "tradeId":"t09-entry-trade-001",
                        "ordId":"recorded-t09-opening-001",
                        "clOrdId":"__REQUEST_CL_ORD_ID__",
                        "billId":"t09-entry-bill-001",
                        "subType":"3","side":"buy","posSide":"long",
                        "fillPx":"1000","fillSz":"0.1","fillPnl":"0",
                        "fee":"-0.0005","feeCcy":"USDT",
                        "ts":financial_time,"fillTime":financial_time
                    },
                    {
                        "instType":"SWAP","instId":"BTC-USDT-SWAP",
                        "tradeId":"t09-close-trade-001",
                        "ordId":"recorded-t09-close-001",
                        "clOrdId":"__REQUEST_CL_ORD_ID__",
                        "billId":"t09-close-bill-001",
                        "subType":"5","side":"sell","posSide":"long",
                        "fillPx":"999","fillSz":"0.1","fillPnl":"-0.001",
                        "fee":"-0.0005","feeCcy":"USDT",
                        "ts":closing_at,"fillTime":closing_at
                    }
                ]));
                let raw_close_bill = envelope(json!([
                    {
                        "billId":"t09-entry-bill-001","instType":"SWAP",
                        "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-opening-001",
                        "tradeId":"t09-entry-trade-001","clOrdId":"__REQUEST_CL_ORD_ID__",
                        "ccy":"USDT","type":"2","subType":"3",
                        "balChg":"-0.0005","fee":"-0.0005","pnl":"0","ts":financial_time
                    },
                    {
                        "billId":"t09-close-bill-001","instType":"SWAP",
                        "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-close-001",
                        "tradeId":"t09-close-trade-001","clOrdId":"__REQUEST_CL_ORD_ID__",
                        "ccy":"USDT","type":"2","subType":"5",
                        "balChg":"-0.0015","fee":"-0.0005","pnl":"-0.001",
                        "ts":closing_at
                    }
                ]));
                // All six history partitions remain strictly scripted; old
                // entry-only repetitions are retired at the acknowledged venue
                // transition so the first terminal source read sees BOTH trades.
                // Independent signed POST ACKs bind the dynamic client IDs.
                let mut final_fills = Vec::new();
                for _ in 0..12 {
                    for partition in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"] {
                        final_fills.push(RecordedGet {
                            access_key: "fixture-executor-a".into(),
                            path: "/api/v5/trade/fills-history".into(),
                            required_query: vec![("instType".into(), partition.into())],
                            raw_response: if partition == "SWAP" {
                                raw_close_fill.clone()
                            } else {
                                envelope(json!([]))
                            },
                        });
                    }
                }
                let retired_fill_pages = feed.advance_recorded_route(
                    "fixture-executor-a",
                    "/api/v5/trade/fills-history",
                    final_fills,
                );
                let retired_bill_pages = feed.advance_recorded_route(
                    "fixture-executor-a",
                    "/api/v5/account/bills-archive",
                    (0..12)
                        .map(|_| RecordedGet {
                            access_key: "fixture-executor-a".into(),
                            path: "/api/v5/account/bills-archive".into(),
                            required_query: vec![],
                            raw_response: raw_close_bill.clone(),
                        })
                        .collect(),
                );
                let retired_position_pages = feed.advance_recorded_route(
                    "fixture-observer-a",
                    "/api/v5/account/positions",
                    (0..24)
                        .map(|_| RecordedGet {
                            access_key: "fixture-observer-a".into(),
                            path: "/api/v5/account/positions".into(),
                            required_query: vec![],
                            raw_response: envelope(json!([])),
                        })
                        .collect(),
                );
                println!(
                    "T09_1_RECORDED_TERMINAL_SOURCE_PHASE_V1:{}",
                    json!({
                        "retired_entry_only_fill_pages":retired_fill_pages,
                        "retired_entry_only_bill_pages":retired_bill_pages,
                        "retired_open_position_pages":retired_position_pages,
                        "close_order_id":"recorded-t09-close-001",
                        "independent_close_fill_id":"t09-close-trade-001",
                        "independent_close_bill_id":"t09-close-bill-001"
                    })
                );
                // The filled close and the separately completed opening order are
                // both independently fetched from their actual signed clOrdId.
                let mut terminal_orders = Vec::new();
                for (order_id, side, time, px) in [
                    ("recorded-t09-close-001", "sell", closing_at.as_str(), "999"),
                    (
                        "recorded-t09-opening-001",
                        "buy",
                        financial_time.as_str(),
                        "1000",
                    ),
                ] {
                    terminal_orders.push(RecordedGet {
                        access_key: "fixture-executor-a".into(),
                        path: "/api/v5/trade/order".into(),
                        required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                        raw_response: envelope(json!([{
                            "instId":"BTC-USDT-SWAP","ordId":order_id,
                            "clOrdId":"__REQUEST_CL_ORD_ID__",
                            "side":side,"posSide":"long","tdMode":"cross",
                            "ordType":if side == "sell" {"ioc"}else{"limit"},
                            "px":px,"sz":"0.1","accFillSz":"0.1","avgPx":px,
                            "state":"filled","cTime":time,"uTime":time
                        }])),
                    });
                }
                // A previously rate-deferred live-order GET may remain
                // unconsumed. The confirmed CLOSE ACK is the phase boundary:
                // retire those old recorded bytes before terminal readbacks.
                feed.advance_recorded_route(
                    "fixture-executor-a",
                    "/api/v5/trade/order",
                    terminal_orders,
                );
                // The original attached stop remains LIVE after close, so the
                // production flat-protection owner must cancel that exact algoId.
                // Subsequent source inventories are explicitly empty, only after
                // a signed cancel ACK; 16 derivative scopes per query remain real.
                let derivative_kinds = [
                    "conditional",
                    "oco",
                    "trigger",
                    "move_order_stop",
                    "chase",
                    "iceberg",
                    "twap",
                    "smart_iceberg",
                ];
                for turn in 0..5 {
                    for inst_type in ["SWAP", "FUTURES"] {
                        for kind in derivative_kinds {
                            let live = turn == 0 && inst_type == "SWAP" && kind == "conditional";
                            feed.expect_get(RecordedGet {
                                access_key: "fixture-observer-a".into(),
                                path: "/api/v5/trade/orders-algo-pending".into(),
                                required_query: vec![
                                    ("instType".into(), inst_type.into()),
                                    ("ordType".into(), kind.into()),
                                    ("limit".into(), "100".into()),
                                ],
                                raw_response: if live {
                                    envelope(json!([{
                                        "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                        "ordType":"conditional","state":"live",
                                        "algoId":"900000001",
                                        "algoClOrdId":format!("s{}", &saved.client_order_id[..31])
                                    }]))
                                } else {
                                    envelope(json!([]))
                                },
                            });
                        }
                    }
                }
                feed.expect_public_clock(envelope(json!([{"ts":closing_at}])));
                feed.expect_post(RecordedPost {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/cancel-algos".into(),
                    expected_body: json!([{
                        "instId":"BTC-USDT-SWAP","algoId":"900000001"
                    }]),
                    raw_response:
                        br#"{"code":"0","msg":"","data":[{"algoId":"900000001","sCode":"0"}]}"#
                            .to_vec(),
                });
                let resource = accounts.resolve("fixture-a").await.unwrap();
                // AccountResources already owns one two-second financial source
                // capture loop. Starting another capture here can read the same
                // old cursor and race that owner to a later SQL upsert, causing
                // SOURCE_CURSOR_REGRESSION. Observe the existing production
                // source owner instead: its authenticated snapshot plus BOTH
                // independently committed, monotonic fills/bills cursors.
                // Nothing here writes a cursor, infers an empty source page,
                // increases TTL or creates an additional financial owner.
                let closed_ms: i64 = closing_at
                    .parse()
                    .expect("T09.1 terminal close exchange timestamp");
                let mut last_capture_witness = json!(null);
                let terminal_capture = tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        let health = resource.source_capture_health.read().await.clone();
                        let snapshot = resource.latest_snapshot.read().await.clone();
                        let (fills, bills) = resource
                            .execution
                            .store
                            .call(std::time::Instant::now() + Duration::from_secs(3), |s| {
                                Ok((s.cursor("fills", "SWAP")?, s.cursor("bills", "account")?))
                            })
                            .await
                            .expect("T09.1 read-only production finance owner readback");
                        let source_ms = snapshot.as_ref().and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(&s.source_received_at)
                                .ok()
                                .map(|t| t.timestamp_millis())
                        });
                        let fill_ready = fills.as_ref().is_some_and(|c| {
                            c.generation > 0 && c.cursor.is_empty()
                                && c.coverage_gaps.is_empty() && c.watermark >= closed_ms
                        });
                        let bill_ready = bills.as_ref().is_some_and(|c| {
                            c.generation > 0 && c.cursor.is_empty()
                                && c.coverage_gaps.is_empty() && c.watermark >= closed_ms
                        });
                        let source_ready = health.last_success_ms.is_some_and(|t| t >= closed_ms)
                            && source_ms.is_some_and(|t| t >= closed_ms);
                        last_capture_witness = json!({
                            "capture_success_ms": health.last_success_ms,
                            "capture_failure": health.reason_code,
                            "snapshot_received_ms": source_ms,
                            "source_ready": source_ready,
                            "fill_ready": fill_ready,
                            "bill_ready": bill_ready,
                            "fills": fills,
                            "bills": bills
                        });
                        if source_ready && fill_ready && bill_ready {
                            let snapshot = snapshot
                                .expect("T09.1 proven production source snapshot");
                            assert!(
                                snapshot.positions.is_empty(),
                                "T09.1 signed post-close positions GET must prove flat"
                            );
                            break snapshot;
                        }
                        // Condition-driven read-only polling; the sole account
                        // finance owner continues on its existing cadence.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "T09_1_TERMINAL_SOURCE_OWNER_NOT_COHERENT:{last_capture_witness}"
                    )
                });
                println!(
                    "T09_1_TERMINAL_CAPTURE_ADMISSION_V1:{}",
                    json!({
                        "owner":"existing_account_finance_cycle",
                        "read_only_witness":last_capture_witness,
                        "total_bound_ms":15000
                    })
                );
                assert!(
                    terminal_capture.positions.is_empty(),
                    "final position must come from the authenticated financial owner"
                );
                let closing_intent_id = close["data"]["intent"]["intent_id"]
                    .as_str()
                    .expect("close must have its own intent")
                    .to_owned();
                let close_receipt_id = format!("local-terminal-release:{closing_intent_id}");
                let close_receipt_probe = close_receipt_id.clone();
                let premature_local_release = resource
                    .execution
                    .store
                    .call(std::time::Instant::now() + Duration::from_secs(3), move |s| {
                        Ok(s.operation(&close_receipt_probe)?.is_some())
                    })
                    .await
                    .expect("T09.1 must inspect the durable local close receipt");
                assert!(
                    !premature_local_release,
                    "Close ACK/captured pages alone cannot release a local terminal marker"
                );
                let closing_intent_read = closing_intent_id.clone();
                let opening_intent_read = idempotent_intent_id.clone();
                let ledger_after_close = resource
                    .execution
                    .store
                    .call(
                        std::time::Instant::now() + Duration::from_secs(3),
                        move |s| {
                            let close_intent = s.intent(&closing_intent_read)?.ok_or(
                                okx_execution::StoreError::Invalid("T09_CLOSE_INTENT_MISSING"),
                            )?;
                            let open_intent = s.intent(&opening_intent_read)?.ok_or(
                                okx_execution::StoreError::Invalid("T09_OPEN_INTENT_MISSING"),
                            )?;
                            let lots = s.active_lots("BTC-USDT-SWAP")?;
                            Ok(json!({
                                "close_intent":close_intent.intent_id,
                                "opening_intent":open_intent.intent_id,
                                "remaining_lots":lots.iter()
                                    .filter(|lot| lot.remaining_qty > Decimal::ZERO)
                                    .count(),
                                "fill_cursor":s.cursor("fills","SWAP")?,
                                "bill_cursor":s.cursor("bills","account")?
                            }))
                        },
                    )
                    .await
                    .expect("separate persisted SQL owner must confirm close fill projection");
                assert_eq!(
                    ledger_after_close["remaining_lots"], 0,
                    "Native SQL must close attributed exposure using the venue close fill"
                );
                let close_verified = trade
                    .trade(
                        "fixture-a",
                        &TradeRequest::Status {
                            reference: closing_intent_id.clone(),
                        },
                        None,
                    )
                    .await
                    .expect("signed terminal close readback must reach public trade.status");
                // Closing terminal readback must discharge its own local SQL
                // marker. Otherwise opening RELEASE is permanently blocked
                // by an unrelated local: terminal receipt in cohort census.
                let close_pending_probe = closing_intent_id.clone();
                let local_receipt_probe = close_receipt_id.clone();
                let (close_still_pending, local_receipt_written) = resource
                    .execution
                    .store
                    .call(std::time::Instant::now() + Duration::from_secs(3), move |s| {
                        let (_, pending) = s.risk_census_intents(100)?;
                        Ok((
                            pending.contains(&close_pending_probe),
                            s.operation(&local_receipt_probe)?.is_some(),
                        ))
                    })
                    .await
                    .expect("T09.1 must read the actual SQL owner after close status");
                assert!(
                    local_receipt_written && !close_still_pending,
                    "T09.1_CLOSE_LOCAL_TERMINAL_UNRESOLVED: receipt={local_receipt_written}, pending={close_still_pending}, close_status={close_verified}"
                );
                let opening_verified = trade
                    .trade(
                        "fixture-a",
                        &TradeRequest::Status {
                            reference: idempotent_intent_id.clone(),
                        },
                        None,
                    )
                    .await
                    .expect("opening status must release central risk from real terminal proof");
                assert_eq!(
                    opening_verified["data"]["central_release"]["state"],
                    "verified",
                    "T09_1_CENTRAL_RELEASE_PENDING:{}",
                    json!({
                        "release":opening_verified["data"]["central_release"],
                        "financial_error":opening_verified["data"]["financial_error"],
                        "incremental_entry_settlement":
                            opening_verified["data"]["incremental_entry_settlement"],
                        "entry_fill_evidence_state":
                            opening_verified["data"]["entry_fill_evidence"]["state"],
                        "quality":opening_verified["quality"],
                    })
                );
                let mut final_summary = summary("risk_t09_1_terminal_close_release");
                final_summary.installation_id = "installation_test".into();
                let worker_terminal = risk.request(final_summary).await.unwrap();
                println!(
                    "T09_1_NATIVE_TERMINAL_CLOSE_V1:{}",
                    json!({
                        "opening_intent_id":idempotent_intent_id,
                        "close_intent_id":closing_intent_id,
                        "close_status":close_verified,
                        "opening_status":opening_verified,
                        "ledger":ledger_after_close,
                        "created_posts":feed.outbound_posts.lock().unwrap().len(),
                        "worker_budget":worker_terminal.budget,
                        "worker_risk_book_sequence":worker_terminal.risk_book_sequence
                    })
                );
            })
            .await;
            // T09.2 is a distinct public order after the fully accepted T09.1
            // terminal RELEASE. Only explicit, recorded signed OKX I/O is
            // substituted. Both SQL and Worker remain production owners.
            if std::env::var("OKX_T09_2_ADMISSION_PROBE").as_deref() == Ok("1") {
                Box::pin(async {
                    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
                    // The first financial owner was stopped before the terminal
                    // T09.1 source phase. The next public entry requires its own
                    // fresh source frontier from the SAME production maintenance
                    // lifecycle, not an ignored readiness error or extra Engine.
                    let (second_shutdown, second_rx) = watch::channel(false);
                    let second_trade = trade.clone();
                    let second_maintenance = tokio::spawn(async move {
                        second_trade.run(second_rx).await;
                    });
                    let mut last_readiness = String::from("NOT_OBSERVED");
                    tokio::time::timeout(Duration::from_secs(15), async {
                        loop {
                            let through = chrono::Utc::now().timestamp_millis() - 2_000;
                            let mut all_ready = true;
                            for alias in ["fixture-a", "fixture-b"] {
                                let owner = accounts.resolve(alias).await.unwrap();
                                let campaign = campaign_id.clone();
                                match owner
                                    .execution
                                    .store
                                    .call(
                                        std::time::Instant::now() + Duration::from_secs(2),
                                        move |s| {
                                            s.verify_campaign_entry_readiness(
                                                &campaign,
                                                native_origin,
                                                through,
                                            )
                                        },
                                    )
                                    .await
                                {
                                    Ok(()) => {}
                                    Err(error) => {
                                        last_readiness = format!("{alias}: {error}");
                                        all_ready = false;
                                        break;
                                    }
                                }
                            }
                            if all_ready {
                                break;
                            }
                            // Only bounded read-only campaign-source polling.
                            // The venue POST remains one-shot after proof.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    })
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "T09.2 production campaign source frontier not refreshed: {last_readiness}"
                        )
                    });
                    let mut before_cmd = summary("risk_t09_2_before_preview");
                    before_cmd.installation_id = "installation_test".into();
                    let before = risk.request(before_cmd).await.unwrap();
                    let prior_entry = before
                        .budget
                        .as_ref()
                        .unwrap()
                        .entry_budget
                        .as_ref()
                        .unwrap();
                    assert_eq!(prior_entry.consumed_usd, "1.0005");
                    assert_eq!(prior_entry.reserved_usd, if add_mode {"0"} else {"4"});
                    refresh_recorded_market();
                    let fee_clock = chrono::Utc::now().timestamp_millis().to_string();
                    for _ in 0..4 {
                        feed.expect_get(RecordedGet {
                            access_key: "fixture-observer-a".into(),
                            path: "/api/v5/account/trade-fee".into(),
                            required_query: vec![
                                ("instType".into(), "SWAP".into()),
                                ("instFamily".into(), "BTC-USDT".into()),
                            ],
                            raw_response: envelope(json!([{
                                "instType":"SWAP","level":"Lv1","ts":fee_clock,
                                "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                            }])),
                        });
                        feed.expect_get(RecordedGet {
                            access_key: "fixture-observer-a".into(),
                            path: "/api/v5/account/leverage-info".into(),
                            required_query: vec![
                                ("instId".into(), "BTC-USDT-SWAP".into()),
                                ("mgnMode".into(), "cross".into()),
                            ],
                            raw_response: envelope(json!([
                                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                            ])),
                        });
                    }
                    let second_preview = TradeRequest::Preview {
                        operation: TradeOperation::Open,
                        strategy_instance: "manual".into(),
                        instrument: "BTC-USDT-SWAP".into(),
                        side: PositionSide::Long,
                        sizing: Sizing::Contracts {
                            contracts: "0.30".into(),
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
                    };
                    let preview_until = std::time::Instant::now() + Duration::from_secs(12);
                    let preview_out = loop {
                        let response = tokio::time::timeout_at(
                            preview_until.into(),
                            trade.trade("fixture-a", &second_preview, None),
                        )
                        .await
                        .expect("T09.2 bounded preview admission");
                        match response {
                            Ok(value) => break value,
                            Err(reason) => {
                                let remaining =
                                    preview_until.saturating_duration_since(std::time::Instant::now());
                                let delay =
                                    recorded_preview_local_defer(&reason, &rate_budget, remaining)
                                        .unwrap_or_else(|| {
                                            panic!("T09.2 non-local Preview error: {reason}")
                                        });
                                tokio::time::timeout_at(
                                    preview_until.into(),
                                    tokio::time::sleep(delay),
                                )
                                .await
                                .expect("T09.2 local Preview defer expired");
                                refresh_recorded_market();
                            }
                        }
                    };
                    assert_eq!(preview_out["status"], "completed");
                    assert_eq!(
                        preview_out["data"]["entry_costs"]["entry_total_usd"],
                        "3.0015"
                    );
                    assert_eq!(preview_out["data"]["admission"], "preview_only");
                    assert_eq!(preview_out["data"]["side_effects"], "none");
                    assert_eq!(
                        feed.outbound_posts.lock().unwrap().len(),
                        3,
                        "T09.2 Preview may never send a new venue create"
                    );
                    let mut before_execute = summary("risk_t09_2_after_preview_no_reserve");
                    before_execute.installation_id = "installation_test".into();
                    let unchanged = risk.request(before_execute).await.unwrap();
                    assert_eq!(
                        unchanged
                            .budget
                            .as_ref()
                            .unwrap()
                            .entry_budget
                            .as_ref()
                            .unwrap()
                            .reserved_usd,
                        if add_mode {"0"} else {"4"},
                        "T09.2 preview must not reserve entry capacity"
                    );
                    // A current HTTP receipt cannot refresh an old exchange fee ts.
                    // Retire prior Preview bytes ONLY at the recorded transport
                    // boundary and prove a full public Execute fails closed.
                    refresh_recorded_market();
                    let stale_fee_at = (chrono::Utc::now().timestamp_millis() - 60_000).to_string();
                    let retired_preview_fees = feed.advance_recorded_route(
                        "fixture-observer-a",
                        "/api/v5/account/trade-fee",
                        vec![RecordedGet {
                            access_key: "fixture-observer-a".into(),
                            path: "/api/v5/account/trade-fee".into(),
                            required_query: vec![
                                ("instType".into(), "SWAP".into()),
                                ("instFamily".into(), "BTC-USDT".into()),
                            ],
                            raw_response: envelope(json!([{
                                "instType":"SWAP","level":"Lv1","ts":stale_fee_at,
                                "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                            }])),
                        }],
                    );
                    let stale_request = TradeRequest::Execute {
                        idempotency_key: "t09_stale_exchange_fee_denied_0001".into(),
                        operation: TradeOperation::Open,
                        strategy_instance: "manual".into(),
                        instrument: "BTC-USDT-SWAP".into(),
                        side: PositionSide::Long,
                        sizing: Sizing::Contracts {
                            contracts: "0.30".into(),
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
                        expires_in_ms: 30_000,
                    };
                    let stale_reason = trade
                        .trade("fixture-a", &stale_request, None)
                        .await
                        .expect_err("stale signed fee must fail before Native admission/POST");
                    assert_eq!(
                        stale_reason, "ENTRY_FEE_EXCHANGE_TIME_STALE",
                        "production exchange-time source gate must reject the exact stale fee"
                    );
                    assert_eq!(
                        feed.outbound_posts.lock().unwrap().len(),
                        3,
                        "stale exchange fee must result in ZERO additional signed POSTs"
                    );
                    let stale_delivery = feed
                        .delivered_fees
                        .lock()
                        .unwrap()
                        .last()
                        .cloned()
                        .expect("stale execution must consume an actual recorded fee GET");
                    assert_eq!(stale_delivery.0, "fixture-observer-a");
                    assert_eq!(stale_delivery.1, stale_fee_at);
                    println!(
                        "T09_2_STALE_FEE_DENIAL_V1:{}",
                        json!({
                            "exchange_ts":stale_delivery.1,
                            "http_delivered_at_ms":stale_delivery.2,
                            "retired_preview_fee_pages":retired_preview_fees,
                            "reason":stale_reason,
                            "public_execute_outcome":"ERR_BEFORE_POST",
                            "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
                        })
                    );
                    let mut after_stale_cmd = summary("risk_t09_2_after_stale_no_send");
                    after_stale_cmd.installation_id = "installation_test".into();
                    let after_stale = risk.request(after_stale_cmd).await.unwrap();
                    assert_eq!(
                        after_stale
                            .budget
                            .as_ref()
                            .unwrap()
                            .entry_budget
                            .as_ref()
                            .unwrap()
                            .reserved_usd,
                        if add_mode {"0"} else {"4"},
                        "stale rejection must not retain a new central entry reservation"
                    );

                    // A NEW authentic-shaped signed GET transcript immediately
                    // precedes positive Execute; the production freshness gate,
                    // fee generation and reference generation remain unchanged.
                    refresh_recorded_market();
                    let execute_fee_at = chrono::Utc::now().timestamp_millis().to_string();
                    let retired_stale_fees = feed.advance_recorded_route(
                        "fixture-observer-a",
                        "/api/v5/account/trade-fee",
                        (0..4)
                            .map(|_| RecordedGet {
                                access_key: "fixture-observer-a".into(),
                                path: "/api/v5/account/trade-fee".into(),
                                required_query: vec![
                                    ("instType".into(), "SWAP".into()),
                                    ("instFamily".into(), "BTC-USDT".into()),
                                ],
                                raw_response: envelope(json!([{
                                    "instType":"SWAP","level":"Lv1","ts":execute_fee_at,
                                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                                }])),
                            })
                            .collect(),
                    );
                    assert_eq!(
                        retired_stale_fees, 0,
                        "negative Execute must consume the lone stale signed fee reply"
                    );
                    // Preview and the explicit negative Execute may already
                    // consume all leverage-info pages. Bind the NEW signed
                    // leverage observation to the positive admission phase.
                    feed.advance_recorded_route(
                        "fixture-observer-a",
                        "/api/v5/account/leverage-info",
                        (0..4).map(|_| RecordedGet {
                            access_key: "fixture-observer-a".into(),
                            path: "/api/v5/account/leverage-info".into(),
                            required_query: vec![
                                ("instId".into(), "BTC-USDT-SWAP".into()),
                                ("mgnMode".into(), "cross".into()),
                            ],
                            raw_response: envelope(json!([
                                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                            ])),
                        }).collect(),
                    );
                    // Only a complete fresh recorded external inventory may
                    // prove that the previously cancelled attached stop is gone.
                    eprintln!("T09_2_PHASE_V1:BEFORE_ALGO_INVENTORY");
                    let (retired_terminal_position_pages, retired_terminal_algo_pages) =
                        recorded_flat_derivative_inventory_after_close(&feed);
                    eprintln!("T09_2_PHASE_V1:AFTER_ALGO_INVENTORY");
                    println!(
                        "T09_2_ALGO_INVENTORY_TRANSCRIPT_V1:{}",
                        json!({
                            "terminal_pages_retired":retired_terminal_algo_pages,
                            "terminal_position_pages_retired":retired_terminal_position_pages,
                            "new_position_read_pages":48,
                            "fresh_signed_scopes":16,
                            "max_reads_per_scope":5,
                            "prior_close_release":"verified"
                        })
                    );
                    let new_clock = chrono::Utc::now().timestamp_millis().to_string();
                    feed.advance_recorded_public_clock(envelope(json!([{"ts":new_clock}])));
                    feed.expect_post(RecordedPost {
                        access_key: "fixture-executor-a".into(),
                        path: "/api/v5/trade/order".into(),
                        expected_body: json!({
                            "instId":"BTC-USDT-SWAP","tdMode":"cross",
                            "clOrdId":"__REQUEST_CL_ORD_ID__",
                            "side":"buy","posSide":"long","ordType":"limit",
                            "sz":"0.3","px":"1000",
                            "attachAlgoOrds":[{
                                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                                "tpTriggerPxType":"mark",
                                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
                            }]
                        }),
                        raw_response: br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t09-second-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
                    });
                    feed.advance_recorded_route(
                        "fixture-executor-a",
                        "/api/v5/trade/order",
                        (0..2)
                            .map(|_| RecordedGet {
                                access_key: "fixture-executor-a".into(),
                                path: "/api/v5/trade/order".into(),
                                required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                                raw_response: envelope(json!([{
                                    "instId":"BTC-USDT-SWAP",
                                    "ordId":"recorded-t09-second-001",
                                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                                    "side":"buy","posSide":"long","tdMode":"cross",
                                    "ordType":"limit","px":"1000","sz":"0.3",
                                    "accFillSz":"0","avgPx":"","state":"live",
                                    "cTime":new_clock,"uTime":new_clock
                                }])),
                            })
                            .collect(),
                    );
                    let second = TradeRequest::Execute {
                        idempotency_key: "t09_real_public_execute_0002".into(),
                        operation: TradeOperation::Open,
                        strategy_instance: "manual".into(),
                        instrument: "BTC-USDT-SWAP".into(),
                        side: PositionSide::Long,
                        sizing: Sizing::Contracts {
                            contracts: "0.30".into(),
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
                        expires_in_ms: 30_000,
                    };
                    eprintln!("T09_2_PHASE_V1:BEFORE_POSITIVE_EXECUTE");
                    // The production Execute future must not be polled inside
                    // the large T09.2 integration task's call stack. Schedule
                    // it independently on the SAME Tokio runtime and retain
                    // the unchanged TradeService, SQL and Worker owners.
                    let execute_trade = trade.clone();
                    let execute_task = tokio::spawn(async move {
                        eprintln!("T09_2_PHASE_V1:EXECUTE_TASK_STARTED");
                        let result = execute_trade.trade("fixture-a", &second, None).await;
                        eprintln!("T09_2_PHASE_V1:EXECUTE_TASK_RETURNED");
                        result
                    });
                    eprintln!("T09_2_PHASE_V1:EXECUTE_TASK_SPAWNED");
                    let accepted = execute_task
                        .await
                        .expect("T09.2 production Execute Tokio task must complete")
                        .expect("T09.2 public TradeService must admit second 0.30 order");
                    eprintln!("T09_2_PHASE_V1:AFTER_POSITIVE_EXECUTE");
                    if accepted["status"] != "partial" {
                        eprintln!(
                            "T09_2_SIGNED_GET_DENIAL_V1:{}",
                            json!({
                                "outcome":accepted,
                                "denied_routes":feed.denied_routes.lock().unwrap().clone(),
                                "successful_gets":feed.requests.lock().unwrap().iter().rev().take(16).cloned().collect::<Vec<_>>(),
                                "fee_source_timestamps":feed.delivered_fees.lock().unwrap().iter().rev().take(6).cloned().collect::<Vec<_>>(),
                                "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
                                "unconsumed_required_gets":feed.unconsumed_reads(),
                                "unconsumed_public_clock":feed.unconsumed_public_clock(),
                            })
                        );
                    }
                    assert_eq!(
                        accepted["status"], "partial",
                        "T09.2 ACK-only is not financial settlement: {accepted}"
                    );
                    assert_eq!(accepted["data"]["intent"]["state"], "ACKNOWLEDGED");
                    assert_eq!(
                        accepted["data"]["intent"]["exchange_order_id"],
                        "recorded-t09-second-001"
                    );
                    assert_eq!(accepted["data"]["financial_complete"], false);
                    let positive_fee_deliveries = feed.delivered_fees.lock().unwrap().clone();
                    assert!(
                        positive_fee_deliveries
                            .iter()
                            .any(|(key, ts, _)| key == "fixture-observer-a" && ts == &execute_fee_at),
                        "positive Execute must consume its newly recorded fee GET: {positive_fee_deliveries:?}"
                    );
                    println!(
                        "T09_2_FEE_TRANSCRIPT_V1:{}",
                        json!({
                            "positive_exchange_ts":execute_fee_at,
                            "last_fee_deliveries":positive_fee_deliveries.iter().rev().take(5).collect::<Vec<_>>(),
                        })
                    );
                    assert_eq!(
                        feed.outbound_posts.lock().unwrap().len(),
                        4,
                        "T09.2 must add exactly ONE create after T09.1 open/close/cancel"
                    );
                    assert_eq!(
                        feed.unconsumed_posts(),
                        0,
                        "T09.2 must consume the single exact signed create response"
                    );
                    let new_intent_id = accepted["data"]["intent"]["intent_id"]
                        .as_str()
                        .expect("T09.2 Native intent id")
                        .to_string();
                    let sql_id = new_intent_id.clone();
                    let resource = accounts.resolve("fixture-a").await.unwrap();
                    let (persisted, receipt, (post_fence_ms, post_fence_state)) = resource
                        .execution
                        .store
                        .call(
                            std::time::Instant::now() + Duration::from_secs(3),
                            move |s| {
                                Ok((
                                    s.intent(&sql_id)?
                                        .ok_or(okx_execution::StoreError::Invalid(
                                            "T09_2_SQL_INTENT_MISSING",
                                        ))?,
                                    s.central_receipt(&sql_id)?.ok_or(
                                        okx_execution::StoreError::Invalid("T09_2_SQL_RESERVE_MISSING"),
                                    )?,
                                    s.post_fence_evidence(&sql_id)?,
                                ))
                            },
                        )
                        .await
                        .expect(
                            "T09.2 SQL, POST fence and central reserve must be independently durable",
                        );
                    eprintln!("T09_2_PHASE_V1:AFTER_SQL_READBACK");
                    assert!(post_fence_ms > 0);
                    // begin_post_at remains durable; authenticated ACK
                    // advances the same SQL receipt from POSTED to OPEN.
                    assert_eq!(post_fence_state, "OPEN");
                    assert_eq!(persisted.state, okx_execution::IntentState::Acknowledged);
                    assert_eq!(
                        persisted.exchange_order_id.as_deref(),
                        Some("recorded-t09-second-001")
                    );
                    assert_eq!(persisted.plan.quantity, Decimal::new(3, 1));
                    assert_eq!(receipt.plan_hash, persisted.plan_hash);
                    assert!(
                        !receipt.reservation_key.starts_with("local:"),
                        "new opening risk must own a real central RiskBook reservation"
                    );
                    let mut after_cmd = summary("risk_t09_2_after_real_create");
                    after_cmd.installation_id = "installation_test".into();
                    let after = risk.request(after_cmd).await.unwrap();
                    let b = after
                        .budget
                        .as_ref()
                        .unwrap()
                        .entry_budget
                        .as_ref()
                        .unwrap();
                    eprintln!("T09_2_PHASE_V1:AFTER_WORKER_READBACK");
                    assert_eq!(b.consumed_usd, "1.0005");
                    assert_eq!(b.reserved_usd, if add_mode {"3.0015"} else {"7.0015"});
                    println!(
                        "T09_2_NATIVE_ADMISSION_V1:{}",
                        json!({
                            "intent_id":persisted.intent_id,
                            "account_uid":resource.configuration.account_key.verified_uid,
                            "client_order_id":persisted.client_order_id,
                            "state":persisted.state.as_str(),
                            "sql_revision":persisted.revision,
                            "sql_begin_post_at_ms":post_fence_ms,
                            "sql_post_reservation_state":post_fence_state,
                            "sql_exchange_order_id":persisted.exchange_order_id,
                            "central_reservation_key":receipt.reservation_key,
                            "central_receipt_hash":receipt.receipt_hash,
                            "plan_hash":persisted.plan_hash,
                            "quote_entry_usd":preview_out["data"]["entry_costs"]["entry_total_usd"],
                            "create_posts_total":feed.outbound_posts.lock().unwrap().len(),
                            "preview_reserved_entry_usd":unchanged.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
                            "after_worker_budget":after.budget,
                            "after_risk_book_sequence":after.risk_book_sequence,
                            "post_ack_financial_complete":accepted["data"]["financial_complete"]
                        })
                    );
                    let mut captured_signed_times=None;
                    if std::env::var("OKX_T09_3_PARTIAL_PROBE").as_deref() == Ok("1") {
                        let (first, ts) = tokio::spawn(t09_3_phase(
                            accounts.clone(),trade.clone(),feed.clone(),
                            new_intent_id.clone(),persisted.client_order_id.clone(),None,
                        )).await.expect("T09.3 partial native task");
                        let (terminal, _) = tokio::spawn(t09_3_phase(
                            accounts.clone(),trade.clone(),feed.clone(),
                            new_intent_id.clone(),persisted.client_order_id.clone(),Some(ts),
                        )).await.expect("T09.3 terminal native task");
                        captured_signed_times=Some((first["raw_history_event_at_ms"].as_i64().unwrap(),terminal["raw_history_event_at_ms"].as_i64().unwrap()));
                        let mut summary_cmd=summary("risk_t09_3_after_terminal");
                        summary_cmd.installation_id="installation_test".into();
                        let after=risk.request(summary_cmd).await.unwrap();
                        println!("T09_3_NATIVE_PARTIAL_TERMINAL_V1:{}",
                            serde_json::json!({
                                "intent_id":new_intent_id,
                                "phases":[first,terminal],
                                "worker_budget":after.budget,
                                "risk_book_sequence":after.risk_book_sequence,
                                "outbound_posts":feed.outbound_posts.lock().unwrap().len()
                            })
                        );
                    }
                    if add_mode {
                        let (frontier_tx,frontier_rx)=tokio::sync::oneshot::channel();
                        let (market_tx,market_rx)=tokio::sync::oneshot::channel();
                        let (first_event_ms,last_event_ms)=captured_signed_times.expect("T09.4 requires T09.3 signed event times");
                        let task = tokio::spawn(t09_4_managed_add(T09_4Context {
                            accounts: accounts.clone(),
                            trade: trade.clone(),
                            risk: risk.clone(),
                            feed: feed.clone(),
                            prior_client_id: persisted.client_order_id.clone(),
                            campaign_id: campaign_id.clone(),
                            native_origin,
                            first_fill_at_ms: first_event_ms,
                            terminal_fill_at_ms: last_event_ms,
                            need_new_market: frontier_tx,
                            fresh_market_ready: market_rx,
                        }));
                        frontier_rx.await.expect("T09.4 financial readiness handoff failed");
                        refresh_recorded_market();
                        market_tx.send(()).expect("T09.4 signed market reply handoff failed");
                        let add=task.await.expect("T09.4 managed Add native task");
                        println!("T09_4_NATIVE_ADD_BOUND_V1:{add}");
                        managed_add_proof = Some(add);
                    }
                    second_shutdown.send(true).unwrap();
                    tokio::time::timeout(Duration::from_secs(10), second_maintenance)
                        .await
                        .expect("T09.2 financial maintenance did not terminate")
                        .unwrap();
                })
                .await;
            }
            feed_shutdown.send(true).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), public_feed)
            .await
            .expect("recorded public publisher did not terminate")
            .unwrap();
        println!("t09_public_trade_execute_open_ack_and_idempotency PASS: posts=1");
        shutdown_tx.send(true).unwrap();
        let terminated = tokio::time::timeout(Duration::from_secs(10), session)
            .await
            .unwrap()
            .unwrap();
        assert!(
            terminated.is_ok(),
            "native session should terminate cleanly: {terminated:?}"
        );
        public_shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), public_task)
            .await
            .expect("public WS coordinator did not stop")
            .unwrap()
            .unwrap();
        let restart_account = if status_probe {
            Some(
                accounts
                    .resolve("fixture-a")
                    .await
                    .unwrap()
                    .configuration
                    .account_key
                    .clone(),
            )
        } else {
            None
        };
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
        if let Some(account) = restart_account {
            // Reopen the SAME verified Native AccountStore through its normal
            // SQLite owner constructor after the old worker has terminated.
            // No test-only SQL mutation, copy or projection is permitted.
            let key = account.wire();
            let path = root
                .join("accounts")
                .join(crate::accounts::key_hash(&account))
                .join("account.sqlite");
            let restored = okx_execution::AccountStoreHandle::open_existing(
                path,
                key,
                "installation_test".into(),
            )
            .await
            .expect("T09.1 must restart the persisted native SQL owner");
            let readback = restored
                .call(std::time::Instant::now() + Duration::from_secs(5), move |s| {
                    let opening = s.by_idempotency("t09_real_public_execute_0001")?.ok_or(
                        okx_execution::StoreError::Invalid("T09_RESTART_OPEN_MISSING"),
                    )?;
                    let close = s.by_idempotency("t09_real_public_close_0001")?.ok_or(
                        okx_execution::StoreError::Invalid("T09_RESTART_CLOSE_MISSING"),
                    )?;
                    let active_central = s.active_central_receipt(&opening.intent_id)?;
                    let lots = s.active_lots("BTC-USDT-SWAP")?;
                    let second = s.by_idempotency("t09_real_public_execute_0002")?;
                    let second_central = match second.as_ref() {
                        Some(i) => s.active_central_receipt(&i.intent_id)?.is_some(),
                        None => false,
                    };
                    // A complete T09.4 proof must survive reopening this
                    // exact production AccountStore, not a test-owned ledger.
                    let managed_add = if add_mode {
                        let add=s.by_idempotency("t09_real_public_add_0001")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_RESTART_ADD_MISSING"))?;
                        let protection=s.protection(&add.intent_id)?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_RESTART_PROTECTION_MISSING"))?;
                        let fills=s.cursor("fills","SWAP")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_RESTART_FILLS_CURSOR_MISSING"))?;
                        let bills=s.cursor("bills","account")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_RESTART_BILLS_CURSOR_MISSING"))?;
                        let fill=s.intent_fill_cost_evidence(&add.intent_id,fills.watermark)?;
                        let billed=s.intent_fee_bill_evidence(&add.intent_id,bills.watermark)?;
                        let own_lots:Vec<_>=lots.iter()
                            .filter(|l|l.intent_id.as_deref()==Some(add.intent_id.as_str())
                                && l.remaining_qty>Decimal::ZERO)
                            .collect();
                        let own_qty=own_lots.iter().fold(Decimal::ZERO,|q,l|q+l.remaining_qty);
                        let central=s.active_central_receipt(&add.intent_id)?;
                        Some(json!({
                            "intent_id":add.intent_id,"state":add.state.as_str(),
                            "venue_order_id":add.exchange_order_id,
                            "sql_revision":add.revision,
                            "active_central_receipt":central.is_some(),
                            "lot_count":own_lots.len(),
                            "lot_quantity":own_qty.normalize().to_string(),
                            "protection":protection,
                            "protected_quantity":protection.quantity.normalize().to_string(),
                            "filled_quantity":fill.filled_quantity.normalize().to_string(),
                            "gross_notional_usdt":fill.gross_notional_usdt.normalize().to_string(),
                            "linked_bill_count":billed.linked_bill_count,
                            "linked_bill_fee_usdt":billed.conservative_verified_fee_usdt.normalize().to_string(),
                            "linked_bill_source_digest":billed.source_digest,
                            "entry_total_usdt":(fill.gross_notional_usdt+billed.conservative_verified_fee_usdt).normalize().to_string()
                        }))
                    } else {None};
                    Ok(json!({
                        "opening_intent_id":opening.intent_id,
                        "opening_state":opening.state.as_str(),
                        "close_intent_id":close.intent_id,
                        "close_state":close.state.as_str(),
                        "active_central_receipt":active_central.is_some(),
                        "remaining_lot_count":lots.iter()
                            .filter(|lot| lot.remaining_qty > Decimal::ZERO)
                            .count(),
                        "second_intent_state":second.as_ref().map(|i|i.state.as_str()),
                        "second_active_central_receipt":second_central,
                        "managed_add":managed_add,
                        "remaining_lot_quantity":lots.iter().fold(Decimal::ZERO,|q,lot|q+lot.remaining_qty).normalize().to_string(),
                        "fills_cursor":s.cursor("fills", "SWAP")?,
                        "bills_cursor":s.cursor("bills", "account")?
                    }))
                })
                .await
                .expect("native restarted SQL owner must read its durable state");
            println!("T09_1_NATIVE_SQL_RESTART_V1:{readback}");
            assert_eq!(readback["opening_state"], "FILLED");
            assert_eq!(readback["close_state"], "FILLED");
            if add_mode {
                let add = &readback["managed_add"];
                assert_eq!(
                    readback["remaining_lot_count"], 3,
                    "T09.4 must persist 3 independent real-fill lots"
                );
                assert_eq!(readback["remaining_lot_quantity"], "0.5");
                assert_eq!(add["state"], "FILLED");
                assert_eq!(add["venue_order_id"], "recorded-t09-add-001");
                assert_eq!(
                    add["active_central_receipt"], true,
                    "live managed risk must remain held"
                );
                assert_eq!(add["lot_count"], 1);
                assert_eq!(add["lot_quantity"], "0.2");
                assert_eq!(add["protected_quantity"], "0.2");
                assert_eq!(add["filled_quantity"], "0.2");
                assert_eq!(add["gross_notional_usdt"], "2");
                assert_eq!(add["linked_bill_count"], 1);
                assert_eq!(add["linked_bill_fee_usdt"], "0.001");
                assert_eq!(add["entry_total_usdt"], "2.001");
                assert_eq!(readback["second_intent_state"], "FILLED");
                assert_eq!(readback["second_active_central_receipt"], true);
            } else if std::env::var("OKX_T09_3_PARTIAL_PROBE").as_deref() == Ok("1") {
                assert_eq!(readback["remaining_lot_count"], 2);
                assert_eq!(readback["remaining_lot_quantity"], "0.3");
                assert_eq!(readback["second_intent_state"], "FILLED");
                assert_eq!(readback["second_active_central_receipt"], true);
            } else {
                assert_eq!(readback["remaining_lot_count"], 0);
            }
            assert_eq!(readback["active_central_receipt"], false);
            if add_mode {
                let add = managed_add_proof
                    .as_ref()
                    .expect("T09.4 settled Add must precede restart");
                let settled = &add["settlement"];
                let budget = &settled["worker_budget"];
                assert_eq!(budget["entry_budget"]["consumed_usd"], "6.003");
                assert_eq!(budget["entry_budget"]["reserved_usd"], "0");
                assert_eq!(readback["managed_add"]["intent_id"], add["intent_id"]);
                // Each source witness binds its OWN immutable cursor
                // frontier. Exact source IDs, venue order, filled amount,
                // bill count and fee agree; changing the coverage frontier
                // legitimately changes a source witness digest.
                assert_eq!(
                    readback["managed_add"]["linked_bill_source_digest"]
                        .as_str()
                        .map(str::len),
                    Some(64)
                );
                assert_eq!(
                    settled["filled"]["linked_bill_source_digest"]
                        .as_str()
                        .map(str::len),
                    Some(64)
                );
                assert_eq!(settled["filled"]["fee_usdt"], "0.001");
                assert_eq!(settled["filled"]["quantity"], "0.2");
                println!(
                    "T09_4_NATIVE_MANAGED_ADD_V1:{}",
                    json!({
                        "operation":"add",
                        "instrument":"BTC-USDT-SWAP",
                        "intent_id":add["intent_id"],
                        "account_uid":add["account_uid"],
                        "central_receipt_hash":add["central_receipt_hash"],
                        "venue_order_id":add["venue_order_id"],
                        "sql_begin_post_at_ms":add["sql_begin_post_at_ms"],
                        "sql_revision":readback["managed_add"]["sql_revision"],
                        "risk_book_sequence":settled["risk_book_sequence"],
                        "added_quantity":readback["managed_add"]["lot_quantity"],
                        "add_entry_usd":readback["managed_add"]["entry_total_usdt"],
                        "linked_bill_fee_usdt":readback["managed_add"]["linked_bill_fee_usdt"],
                        "linked_bill_source_digest":readback["managed_add"]["linked_bill_source_digest"],
                        "cumulative_spent_usd":budget["entry_budget"]["consumed_usd"],
                        "worker_budget":settled["worker_budget"],
                        "outbound_posts":5,
                        "sql_restart_managed_exposure":readback["remaining_lot_quantity"],
                        "protection":readback["managed_add"]["protection"]
                    })
                );
            }
            let mut finished = restored.termination_signal();
            drop(restored);
            tokio::time::timeout(Duration::from_secs(5), async {
                while !*finished.borrow_and_update() {
                    finished.changed().await.expect("restarted SQL owner died");
                }
            })
            .await
            .expect("restarted SQL owner did not close");
        }
        std::fs::remove_dir_all(root).unwrap();
        println!("native_to_persisted_workerd_riskbook PASS: no venue POST");
    }
}
