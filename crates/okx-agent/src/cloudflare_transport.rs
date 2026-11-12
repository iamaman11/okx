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

// Only the public Native/Worker integration test may arm a single fault
// at the real Worker response boundary. Production builds have no such knob.
#[cfg(test)]
static T10_04_CORRUPT_NEXT_HELD_COMMAND: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static T10_04_FOREIGN_COMMAND_INJECTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// A test-only post-HELD handoff: Worker commits the authentic reservation,
// Native has NOT yet received that reply, and the scripted signed source
// changes before the ordinary production revalidation may continue.
#[cfg(test)]
static T10_15_ARM_AFTER_HELD: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static T10_15_WORKER_HELD_COMMITTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static T10_15_SIGNED_SOURCE_CHANGED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// T10-03: Worker commits a real BeginPost CAS but its signed authorization
// reply is lost at the Native receive boundary. Never forge POST authority.
#[cfg(test)]
static T10_03_DROP_NEXT_POST_AUTH: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static T10_03_WORKER_POST_COMMITTED_REPLY_LOST: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// The unit test observes the actual dispatcher join point. This is not
// a production scheduling knob or a second command/control path.
#[cfg(test)]
static RISK_IDENTICAL_INFLIGHT_COMMAND_JOINED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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
    // A command ID is a deterministic identity, not a unique waiter ID.
    // Same-ID + same-payload callers share the single actual Worker request
    // and each receives its bound result. Same ID with different contents
    // is a fatal protocol conflict; it can never inherit the first grant.
    let mut pending: HashMap<
        String,
        (
            okx_protocol::risk_authority::RiskCommand,
            Vec<oneshot::Sender<okx_protocol::risk_authority::RiskResult>>,
        ),
    > = HashMap::new();
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
        Some(outbound)=risk.recv(),if pending.len()<32=>{
            // Never take a queued command only to drop its one-shot sender
            // when the in-flight window is full. Backpressure remains on
            // the bounded risk channel; an unsent command receives a local
            // admission error, not a false "Worker reply lost" diagnosis.
            //
            // Keep known command IDs until their actual Worker reply, even
            // after the requesting task times out or is cancelled. A late
            // authentic reply is not an unsolicited foreign authorization.
            let id=outbound.command.command_id.clone();
            if let Some((first,waiters))=pending.get_mut(&id) {
                if *first!=outbound.command {
                    return Err(transport("RISK_COMMAND_ID_PAYLOAD_CONFLICT"));
                }
                // Bound total requesters for a single deterministic command;
                // never silently drop a sender awaiting a Worker decision.
                if waiters.len()>=128 {
                    return Err(transport("RISK_IDENTICAL_WAITERS_LIMIT"));
                }
                waiters.push(outbound.return_to);
                #[cfg(test)]
                RISK_IDENTICAL_INFLIGHT_COMMAND_JOINED.fetch_add(
                    1,std::sync::atomic::Ordering::SeqCst
                );
                continue;
            }
            let command=outbound.command;
            pending.insert(id, (command.clone(),vec![outbound.return_to]));
            send(&mut sink,F::RiskControl{
                schema:SCHEMA.into(),session_id:session_id.clone(),
                connection_generation,command
            }).await?;
        },
        Some((id,result))=flights.next(),if !flights.is_empty()=>{active.remove(&id);send(&mut sink,F::Response{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,response:result}).await?;},
        message=source.next()=>{
        let incoming=message.ok_or_else(||transport("WS_CLOSED"))?.map_err(|_|transport("WS_READ_FAILED"))?;
        #[cfg(test)]
        let original_bytes=incoming.clone();
        let f=frame(incoming)?;
        let(s,g)=match &f{F::Ping{session_id,connection_generation,..}|F::Pong{session_id,connection_generation,..}|F::Request{session_id,connection_generation,..}|F::RiskControlResult{session_id,connection_generation,..}|F::PolicySnapshot{session_id,connection_generation,..}=>(session_id,*connection_generation),_=>return Err(transport("UNEXPECTED_FRAME"))};
        if s!=&session_id||g!=connection_generation{return Err(transport("SESSION_GENERATION_MISMATCH"));}last_seen=Instant::now();
        match f{
        F::Ping{nonce,..}=>{challenges.retain(|_,t|t.elapsed()<Duration::from_secs(15));if challenges.len()<64{challenges.insert(nonce.clone(),Instant::now());}send(&mut sink,F::Pong{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,nonce}).await?;},
        F::Pong{..}=>{},
        F::PolicySnapshot{snapshot,..}=>{ctx.trade.update_governance(&snapshot).await.map_err(|_|transport("INVALID_POLICY_SNAPSHOT"))?;},
        F::RiskControlResult{result,..}=>{
            #[cfg(test)]
            raw_native_session_tests::t10_11_hold_unsent_reply(
                &result,pending.get(&result.command_id).map(|(cmd,_)|cmd),&ctx,
                connection_generation
            ).await;
            #[cfg(test)]
            let mut result=result;
            #[cfg(test)]
            raw_native_session_tests::t10_12_hold_settlement_reply(
                &original_bytes,&result,pending.get(&result.command_id).map(|(cmd,_)|cmd),
                &ctx,&session_id,connection_generation
            ).await?;
            #[cfg(test)]
            if result.status==okx_protocol::risk_authority::RiskDecisionStatus::PostAuthorized
                && T10_03_DROP_NEXT_POST_AUTH.swap(
                    false,std::sync::atomic::Ordering::SeqCst)
            {
                // The real Worker receipt already committed and crossed WSS.
                // Only this Native test delivery is discarded. The awaiting
                // risk requester expires UNKNOWN, then TradeService INSPECTs
                // the original reserve and MUST NOT send a venue order.
                T10_03_WORKER_POST_COMMITTED_REPLY_LOST.store(
                    true,std::sync::atomic::Ordering::SeqCst);
                continue;
            }
            // This is a one-shot corruption of the delivery only: the actual
            // Worker committed its real HELD receipt and sent correct bytes.
            // The test verifies Native rejects a foreign ID after that commit.
            #[cfg(test)]
            if result.status==okx_protocol::risk_authority::RiskDecisionStatus::Held
                && T10_04_CORRUPT_NEXT_HELD_COMMAND.swap(
                    false,std::sync::atomic::Ordering::SeqCst
                )
            {
                result.command_id="risk_t10_04_foreign_unbound_0001".into();
                T10_04_FOREIGN_COMMAND_INJECTED.store(
                    true,std::sync::atomic::Ordering::SeqCst
                );
            }
            // An authentic, committed HELD is the causal boundary for the
            // optional source-change test. No test ever creates or mutates
            // the Worker response. The production post-HELD freshness fence
            // runs unchanged when the signed source phase is published.
            #[cfg(test)]
            if result.status==okx_protocol::risk_authority::RiskDecisionStatus::Held
                && T10_15_ARM_AFTER_HELD.swap(
                    false,std::sync::atomic::Ordering::SeqCst
                )
            {
                T10_15_WORKER_HELD_COMMITTED.store(
                    true,std::sync::atomic::Ordering::SeqCst
                );
                tokio::time::timeout(Duration::from_secs(4),async {
                    while !T10_15_SIGNED_SOURCE_CHANGED.load(
                        std::sync::atomic::Ordering::SeqCst
                    ) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }).await.map_err(|_|transport("T10_15_SIGNED_SOURCE_HANDOFF_MISSING"))?;
            }
            // An unsolicited, stale or foreign command ID must never be
            // mistaken for the active reservation's authorization. Reject
            // the frame instead of silently dropping it and waiting for a
            // timeout while the caller still believes a grant is pending.
            // Match the exact command that was actually sent. If Native
            // timed out, sending to its closed receiver simply discards the
            // authentic late reply; a genuinely foreign ID still tears down
            // this WS generation (T10-04).
            let (_,waiters)=pending.remove(&result.command_id)
                .ok_or_else(||transport("RISK_RESULT_COMMAND_UNBOUND"))?;
            // All recipients sent exactly these same command bytes and
            // must verify their own command ID in NativeRiskClient::request.
            // Late/cancelled receivers safely discard only their own copy.
            for sender in waiters {let _=sender.send(result.clone());}
        },
        F::Request{request,operator_governance,..}=>{
        let id=request.request_id.clone();send(&mut sink,F::DeliveryAck{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,request_id:id.clone()}).await?;
        if active.contains(&id){continue;}
        let finance_view = matches!(&request.operation, AgentOperation::Diagnostics{request:diagnostic} if diagnostic.view.is_some());
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
        RiskDecisionStatus, RiskReservationState, RiskResult, UsdConversion,
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
        test_dispatch_with_root(installation, None).await
    }

    async fn test_dispatch_with_root(
        installation: &str,
        existing: Option<std::path::PathBuf>,
    ) -> (
        DispatchContext,
        std::path::PathBuf,
        NativeRiskClient,
        mpsc::Receiver<OutboundRiskCommand>,
        Option<Arc<crate::accounts::recorded_support::RecordedAccountHttp>>,
        RateBudget,
    ) {
        let reopen = existing.is_some();
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).unwrap();
        let root = existing.unwrap_or_else(|| {
            std::env::temp_dir().join(format!(
                "okx-raw-native-session-{:032x}",
                u128::from_le_bytes(bytes)
            ))
        });
        if !reopen {
            initialize_installation(&root, installation).unwrap();
        } else {
            assert!(
                root.join("accounts").is_dir(),
                "original crash SQL root required"
            );
        }
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
                if reopen {
                    accounts.reconnect_prepared(config, clients).await.unwrap();
                } else {
                    accounts.connect_prepared(config, clients).await.unwrap();
                }
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
    // A separate OS process, launched after the parent drops both SQL owners,
    // proves the persisted account files are independently reopenable. The
    // child has no exchange transport, Worker client or venue POST capability.
    #[tokio::test]
    async fn t09_7_native_process_restore_existing_sql() {
        if std::env::var("OKX_T09_7_REOPEN_CHILD").as_deref() != Ok("1") {
            return;
        }
        let path = std::path::PathBuf::from(
            std::env::var_os("OKX_T09_7_ACCOUNT_A_SQL").expect("T09.7 A SQL file"),
        );
        let key = std::env::var("OKX_T09_7_ACCOUNT_A_KEY").expect("T09.7 A UID");
        let other_path = std::path::PathBuf::from(
            std::env::var_os("OKX_T09_7_ACCOUNT_B_SQL").expect("T09.7 B SQL file"),
        );
        let other_key = std::env::var("OKX_T09_7_ACCOUNT_B_KEY").expect("T09.7 B UID");
        assert_ne!(path, other_path, "T09.7 two separate SQL owners");
        assert_ne!(key, other_key, "T09.7 two distinct UIDs");
        let owner =
            okx_execution::AccountStoreHandle::open_existing(path, key, "installation_test".into())
                .await
                .expect("T09.7 child process must reopen A SQL");
        let account_a = owner.call(
            std::time::Instant::now()+Duration::from_secs(5),
            |s| {
                let lots=s.active_lots("BTC-USDT-SWAP")?;
                let second=s.by_idempotency("t09_real_public_execute_0002")?
                    .ok_or(okx_execution::StoreError::Invalid("T09_7_SECOND_ENTRY_MISSING"))?;
                let add=s.by_idempotency("t09_real_public_add_0001")?
                    .ok_or(okx_execution::StoreError::Invalid("T09_7_ADD_MISSING"))?;
                let close_030=s.by_idempotency("t09_real_public_close_030_0001")?
                    .ok_or(okx_execution::StoreError::Invalid("T09_7_CLOSE_030_MISSING"))?;
                let close_020=s.by_idempotency("t09_real_public_close_020_0001")?
                    .ok_or(okx_execution::StoreError::Invalid("T09_7_CLOSE_020_MISSING"))?;
                for close in [&close_030,&close_020] {
                    if close.state!=okx_execution::IntentState::Filled {
                        return Err(okx_execution::StoreError::Invalid("T09_7_CLOSE_NOT_FILLED"));
                    }
                }
                let held_second=s.active_central_receipt(&second.intent_id)?.is_some();
                let held_add=s.active_central_receipt(&add.intent_id)?.is_some();
                let finance=s.finance_totals(0,i64::MAX,None)?;
                let fee=finance.get(&("USDT".into(),"FEE".into()))
                    .copied().unwrap_or(Decimal::ZERO);
                let pnl=finance.get(&("USDT".into(),"REALIZED_PNL".into()))
                    .copied().unwrap_or(Decimal::ZERO);
                let opening=s.opening_equity_usd()?;
                let equity=opening+fee+pnl;
                Ok(serde_json::json!({
                    "remaining_lots":lots.iter().filter(|lot|lot.remaining_qty>Decimal::ZERO).count(),
                    "active_reserve_second":held_second,
                    "active_reserve_add":held_add,
                    "close_030_state":close_030.state.as_str(),
                    "close_020_state":close_020.state.as_str(),
                    "account_opening_equity_usd":opening.normalize().to_string(),
                    "total_fees_usdt":fee.normalize().to_string(),
                    "realized_pnl_usdt":pnl.normalize().to_string(),
                    "equity_a_usd":equity.normalize().to_string(),
                    "finance_revision":s.finance_revision()?,
                }))
            }
        ).await.expect("T09.7 child independent A finance readback");
        assert_eq!(account_a["remaining_lots"], 0);
        assert_eq!(account_a["active_reserve_second"], false);
        assert_eq!(account_a["active_reserve_add"], false);
        assert_eq!(account_a["account_opening_equity_usd"], "100");
        assert_eq!(account_a["total_fees_usdt"], "-0.006");
        assert_eq!(account_a["realized_pnl_usdt"], "0");
        assert_eq!(account_a["equity_a_usd"], "99.994");
        let other = okx_execution::AccountStoreHandle::open_existing(
            other_path,
            other_key,
            "installation_test".into(),
        )
        .await
        .expect("T09.7 child process must reopen B SQL");
        let account_b = other
            .call(std::time::Instant::now() + Duration::from_secs(5), |s| {
                let lots = s.active_lots("BTC-USDT-SWAP")?;
                let finance = s.finance_totals(0, i64::MAX, None)?;
                let opening = s.opening_equity_usd()?;
                Ok(serde_json::json!({
                    "remaining_lots":lots.len(),
                    "finance_totals":finance.len(),
                    "equity_b_usd":opening.normalize().to_string(),
                    "finance_revision":s.finance_revision()?,
                }))
            })
            .await
            .expect("T09.7 child independent B readback");
        assert_eq!(account_b["remaining_lots"], 0);
        assert_eq!(account_b["finance_totals"], 0);
        assert_eq!(account_b["equity_b_usd"], "100");
        println!(
            "T09_7_NATIVE_PROCESS_RESTART_V1:{}",
            serde_json::json!({
                "new_os_process":true,
                "account_a":account_a,
                "account_b":account_b,
                "new_exchange_posts":0,
            })
        );
        drop(owner);
        drop(other);
    }
    include!("trade_integration_t09_3.rs");
    include!("trade_integration_t09_4.rs");
    include!("trade_integration_t09_6.rs");
    include!("trade_integration_t10_14.rs");
    include!("trade_integration_t10_02.rs");
    include!("trade_integration_t10_04.rs");
    include!("trade_integration_t09_spec.rs");
    include!("trade_integration_t10_05.rs");
    include!("trade_integration_t10_15.rs");
    include!("trade_integration_t10_03.rs");
    include!("trade_integration_t10_01.rs");
    include!("trade_integration_t10_13.rs");
    include!("trade_integration_t10_08.rs");
    include!("trade_integration_t10_09.rs");
    include!("trade_integration_t10_10.rs");
    include!("trade_integration_t10_12.rs");
    include!("trade_integration_t10_11.rs");
    include!("trade_integration_t10_07.rs");

    /// Retry permission is test-only and limited to an explicitly read-only
    /// Preview that failed at the *local* request-admission boundary. The
    /// production RateBudget supplies the exact no-send defer: no fixed sleep,
    /// disabled limiter, exchange retry, or inferred POST safety.
    fn recorded_preview_local_defer(
        reason: &str,
        budget: &RateBudget,
        remaining: Duration,
    ) -> Option<Duration> {
        // Account-facing TradeService may wrap the same typed local no-send
        // refusal as "OKX API error: ...". Strip only this exact envelope;
        // the shared RateBudget witness still decides whether waiting is safe.
        let reason = reason
            .strip_prefix("OKX API error: ")
            .or_else(|| reason.strip_prefix("OKX private API error: "))
            .unwrap_or(reason);
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
            recorded_preview_local_defer(
                "OKX API error: OKX rate/backpressure defer: RateThrottleEvidence { ... }",
                &budget,
                Duration::from_secs(3),
            )
            .is_some(),
            "TradeService wrapper must preserve only typed LOCAL/NO-SEND read retry"
        );
        assert!(
            recorded_preview_local_defer(
                "OKX API error: VENUE_ACK_MISSING",
                &budget,
                Duration::from_secs(3),
            )
            .is_none()
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
    #[tokio::test]
    async fn foreign_risk_reply_command_never_authorizes_pending_reserve() {
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
        let session_id = "session_test_unbound_command".to_owned();
        let generation = 21;
        input_tx
            .send(Ok(wire(F::HelloAck {
                schema: SCHEMA.into(),
                session_id: session_id.clone(),
                connection_generation: generation,
            })))
            .unwrap();

        let operation = tokio::spawn({
            let client = client.clone();
            async move { client.request(summary("risk_wire_expected_command")).await }
        });
        let observed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match parse(output_rx.recv().await.expect("risk outbound closed")) {
                    F::RiskControl { command, .. } => break command.command_id,
                    F::Ping { .. } => continue,
                    other => panic!("unexpected native outbound: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observed, "risk_wire_expected_command");
        input_tx
            .send(Ok(wire(F::RiskControlResult {
                schema: SCHEMA.into(),
                session_id,
                connection_generation: generation,
                result: denied("risk_wire_foreign_command"),
            })))
            .unwrap();

        let stopped = tokio::time::timeout(Duration::from_secs(3), session)
            .await
            .unwrap()
            .unwrap();
        assert!(
            stopped
                .unwrap_err()
                .to_string()
                .contains("RISK_RESULT_COMMAND_UNBOUND")
        );
        let pending = tokio::time::timeout(Duration::from_secs(3), operation)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pending.unwrap_err(),
            "RISK_TRANSPORT_LOST_DO_NOT_REPLAY",
            "an unbound reply cannot become an authorization or a second grant"
        );
        drop(client);
        drop(input_tx);
        drop(shutdown_tx);
        drop(output_rx);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn late_bound_risk_reply_after_cancelled_native_wait_does_not_close_session() {
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
        let session_id = "session_test_late_bound".to_owned();
        let generation = 23;
        input_tx
            .send(Ok(wire(F::HelloAck {
                schema: SCHEMA.into(),
                session_id: session_id.clone(),
                connection_generation: generation,
            })))
            .unwrap();

        let first = tokio::spawn({
            let client = client.clone();
            async move { client.request(summary("risk_wire_late_known")).await }
        });
        let first_id = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match parse(output_rx.recv().await.expect("first risk outbound closed")) {
                    F::RiskControl { command, .. } => break command.command_id,
                    F::Ping { .. } => continue,
                    other => panic!("unexpected outbound: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(first_id, "risk_wire_late_known");
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        // The Worker response is authentic and ID-bound, but the original
        // Native waiter has been cancelled. Do not deliver its authority
        // to another request; equally, do not break the whole WS session.
        input_tx
            .send(Ok(wire(F::RiskControlResult {
                schema: SCHEMA.into(),
                session_id: session_id.clone(),
                connection_generation: generation,
                result: denied(&first_id),
            })))
            .unwrap();

        let next = tokio::spawn({
            let client = client.clone();
            async move { client.request(summary("risk_wire_after_late")).await }
        });
        let next_id = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match parse(output_rx.recv().await.expect("second risk outbound closed")) {
                    F::RiskControl { command, .. } => break command.command_id,
                    F::Ping { .. } => continue,
                    other => panic!("unexpected outbound after known late reply: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(next_id, "risk_wire_after_late");
        input_tx
            .send(Ok(wire(F::RiskControlResult {
                schema: SCHEMA.into(),
                session_id,
                connection_generation: generation,
                result: denied(&next_id),
            })))
            .unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(3), next)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(answer.command_id, next_id);
        assert_eq!(answer.status, RiskDecisionStatus::Denied);
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
    async fn identical_inflight_risk_commands_get_one_worker_call_and_two_bound_replies() {
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
        let session_id = "session_test_same_risk_command".to_owned();
        let generation = 24;
        input_tx
            .send(Ok(wire(F::HelloAck {
                schema: SCHEMA.into(),
                session_id: session_id.clone(),
                connection_generation: generation,
            })))
            .unwrap();

        let first = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .request(summary("risk_wire_same_deterministic"))
                    .await
            }
        });
        let first_id = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match parse(output_rx.recv().await.expect("outbound closed")) {
                    F::RiskControl { command, .. } => break command.command_id,
                    F::Ping { .. } => continue,
                    other => panic!("unexpected outbound: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(first_id, "risk_wire_same_deterministic");

        let prior =
            RISK_IDENTICAL_INFLIGHT_COMMAND_JOINED.load(std::sync::atomic::Ordering::SeqCst);
        let second = tokio::spawn({
            let client = client.clone();
            async move {
                client
                    .request(summary("risk_wire_same_deterministic"))
                    .await
            }
        });
        // Wait for the ACTUAL single-owner join, not for an arbitrary sleep.
        tokio::time::timeout(Duration::from_secs(3), async {
            while RISK_IDENTICAL_INFLIGHT_COMMAND_JOINED.load(std::sync::atomic::Ordering::SeqCst)
                == prior
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same deterministic Worker request must join its first caller");
        while let Ok(message) = output_rx.try_recv() {
            assert!(
                !matches!(parse(message), F::RiskControl { .. }),
                "identical concurrent command must not send a second Worker mutation"
            );
        }
        input_tx
            .send(Ok(wire(F::RiskControlResult {
                schema: SCHEMA.into(),
                session_id,
                connection_generation: generation,
                result: denied(&first_id),
            })))
            .unwrap();
        for request in [first, second] {
            let observed = tokio::time::timeout(Duration::from_secs(3), request)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(observed.command_id, first_id);
            assert_eq!(observed.status, RiskDecisionStatus::Denied);
        }
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
    async fn same_risk_command_id_with_different_payload_never_inherits_worker_result() {
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
        input_tx
            .send(Ok(wire(F::HelloAck {
                schema: SCHEMA.into(),
                session_id: "session_test_risk_payload_conflict".into(),
                connection_generation: 25,
            })))
            .unwrap();
        let first = tokio::spawn({
            let client = client.clone();
            async move { client.request(summary("risk_wire_payload_conflict")).await }
        });
        let id = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match parse(output_rx.recv().await.expect("outbound closed")) {
                    F::RiskControl { command, .. } => break command.command_id,
                    F::Ping { .. } => continue,
                    other => panic!("unexpected outbound: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(id, "risk_wire_payload_conflict");
        let mut incompatible = summary("risk_wire_payload_conflict");
        incompatible.installation_id = "different_test_installation".into();
        assert!(incompatible.valid());
        let second = tokio::spawn({
            let client = client.clone();
            async move { client.request(incompatible).await }
        });
        let terminated = tokio::time::timeout(Duration::from_secs(3), session)
            .await
            .unwrap()
            .unwrap();
        assert!(
            terminated
                .unwrap_err()
                .to_string()
                .contains("RISK_COMMAND_ID_PAYLOAD_CONFLICT")
        );
        for caller in [first, second] {
            let outcome = tokio::time::timeout(Duration::from_secs(3), caller)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                outcome.unwrap_err(),
                "RISK_TRANSPORT_LOST_DO_NOT_REPLAY",
                "same command ID with different payload cannot receive a grant"
            );
        }
        drop(client);
        drop(shutdown_tx);
        drop(input_tx);
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
        let mut owned_accounts = Vec::new();
        for alias in ["fixture-a", "fixture-b"] {
            let account = accounts
                .resolve(alias)
                .await
                .expect("bootstrapped SQL owner");
            owned_accounts.push(serde_json::json!({
                "account_key": account.configuration.account_key.wire(),
                "sql_path": root.join("accounts")
                    .join(crate::accounts::key_hash(&account.configuration.account_key))
                    .join("account.sqlite"),
            }));
        }
        // Ownership evidence only. Existing consumers still use this file as
        // a readiness barrier; it contains no handoff token or trading facts.
        std::fs::write(
            format!("{handoff_path}.native-ready"),
            serde_json::to_vec(&serde_json::json!({
                "schema": "okx.native-owner-ready/v1",
                "native_pid": std::process::id(),
                "native_root": root,
                "accounts": owned_accounts,
            }))
            .unwrap(),
        )
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
        let close_mode = std::env::var("OKX_T09_6_CLOSE_PROBE").as_deref() == Ok("1");
        let add_mode =
            close_mode || std::env::var("OKX_T09_4_MANAGED_ADD_PROBE").as_deref() == Ok("1");
        let t10_seven_one_campaign = std::env::var("OKX_T10_07_ONE_CAMPAIGN").as_deref() == Ok("1");
        let clean_mode = add_mode || t10_seven_one_campaign;

        // The same real public WS coordinator consumes recorded raw market
        // frames. These are immutable venue observations, not a fabricated
        // MarketSnapshot or a separate pricing/decision implementation.
        use crate::accounts::recorded_support::{RecordedGet, RecordedWsConnector};
        use serde_json::json;
        let feed = recorded_http.expect("the integration run requires recorded HTTP");
        let connector = Arc::new(RecordedWsConnector::default());
        let spec_mode = std::env::var("OKX_T09_SPEC_SECOND_LINEAR_PROBE").as_deref() == Ok("1");
        let mut supported_specs = vec![
            serde_json::from_value(json!({
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "instFamily":"BTC-USDT","uly":"BTC-USDT","state":"live",
                "ruleType":"normal","settleCcy":"USDT","tickSz":"0.1",
                "lotSz":"0.01","minSz":"0.01","maxLmtSz":"1000",
                "maxMktSz":"1000","ctType":"linear","ctVal":"0.01",
                "ctValCcy":"BTC","groupId":"4","lever":"20"
            }))
            .unwrap(),
        ];
        if spec_mode {
            // Second exchange-provided valid linear spec, different symbol,
            // multiplier, lot, minimum and tick. This is test-only recorded
            // public metadata; it cannot grant policy permission by itself.
            supported_specs.push(
                serde_json::from_value(json!({
                    "instType":"SWAP","instId":"ETH-USDT-SWAP",
                    "instFamily":"ETH-USDT","uly":"ETH-USDT","state":"live",
                    "ruleType":"normal","settleCcy":"USDT","tickSz":"0.5",
                    "lotSz":"0.05","minSz":"0.05","maxLmtSz":"1000",
                    "maxMktSz":"1000","ctType":"linear","ctVal":"0.02",
                    "ctValCcy":"ETH","groupId":"4","lever":"20"
                }))
                .unwrap(),
            );
        }
        let reference =
            ReferenceRegistry::from_public(chrono::Utc::now().to_rfc3339(), supported_specs)
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
        let recovery_risk = risk.clone();
        let recovery_handoff = handoff_path.clone();
        let mut session = tokio::spawn(async move {
            let result = if t10_12_reserve_enabled() {
                t10_12_reserve_supervise(
                    socket,
                    hello,
                    ctx,
                    &mut risk_rx,
                    shutdown_rx,
                    recovery_risk,
                    (token.to_string(), recovery_handoff),
                )
                .await
            } else if t10_12_enabled() {
                t10_12_supervise(
                    socket,
                    hello,
                    ctx,
                    &mut risk_rx,
                    shutdown_rx,
                    recovery_risk,
                    (token.to_string(), recovery_handoff),
                )
                .await
            } else {
                run_connected_session(socket, hello, ctx, &mut risk_rx, shutdown_rx).await
            };
            if let Err(ref reason) = result {
                // Record the *real* Native WSS first failure. A later
                // RISK_TRANSPORT_LOST is only the requester's secondary
                // symptom; do not guess that this session broke due to a
                // full queue, a foreign frame, or a missing reply.
                eprintln!("T10_06_NATIVE_WSS_SESSION_EXIT_V1:{reason}");
            }
            result
        });
        let mut command = summary("risk_native_real_workerd_001");
        command.installation_id = "installation_test".into();
        let result = risk.request(command).await.unwrap();
        assert_eq!(result.status, RiskDecisionStatus::Observed);
        let budget = result.budget.as_ref().unwrap();
        assert_eq!(budget.campaign_limit_usd, "10");
        assert_eq!(budget.reserved_loss_usd, if clean_mode { "0" } else { "1" });
        let entry = budget.entry_budget.as_ref().unwrap();
        assert_eq!(entry.reserved_usd, if clean_mode { "0" } else { "2" });
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
            if clean_mode { "0" } else { "1" }
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
            if clean_mode { "0" } else { "2" }
        );
        if !clean_mode {
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
        let checked = if clean_mode {
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
        let fresh_clock_at = chrono::Utc::now().timestamp_millis().to_string();
        feed.expect_public_clock(envelope(json!([{"ts":fresh_clock_at}])));
        // The canonical T09.1 tariff refresh is published below at the
        // first Execute boundary (fresh_fee_at). Do not stage a second
        // mandatory tariff page set here: an unused recorded response is
        // not an authenticated GET and must remain visibly unconsumed.
        if t10_seven_one_campaign {
            // Continuous RAW public-WS source only while this one Native
            // campaign runs. The same production coordinator must parse
            // fresh quote, book, funding and reference data throughout
            // admission. A single fresh snapshot before Execute is NOT
            // enough under the production two-second quote age bound.
            // There is no synthetic MarketSnapshot or TTL bypass.
            let (t10_feed_shutdown, mut t10_feed_stop) = watch::channel(false);
            let t10_feed_connector = connector.clone();
            let t10_feed_rows = recorded_public.clone();
            let t10_market_task = tokio::spawn(async move {
                let mut ticks = tokio::time::interval(Duration::from_millis(750));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut book_seq = 12_i64;
                loop {
                    tokio::select! {
                        changed=t10_feed_stop.changed()=>{
                            if changed.is_err()||*t10_feed_stop.borrow(){break;}
                        },
                        _=ticks.tick()=>{
                            let observed_at=chrono::Utc::now().timestamp_millis();
                            for (arg,original,action) in &t10_feed_rows {
                                if arg["channel"]=="instruments"&&arg["instType"]=="SWAP" {
                                    t10_feed_connector.send_public_frame(json!({
                                        "arg":arg,"data":[{
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
                                } else if let Some(mut value)=original.clone() {
                                    value["ts"]=observed_at.to_string().into();
                                    if value.get("fundingTime").is_some(){
                                        value["fundingTime"]=observed_at.to_string().into();
                                        value["nextFundingTime"]=
                                            (observed_at+8*60*60*1000).to_string().into();
                                    }
                                    if *action==Some("snapshot"){
                                        value["seqId"]=book_seq.into();
                                        book_seq+=1;
                                    }
                                    let mut frame=json!({"arg":arg,"data":[value]});
                                    if let Some(action)=action{
                                        frame["action"]=(*action).into();
                                    }
                                    t10_feed_connector.send_public_frame(frame.to_string());
                                }
                            }
                        }
                    }
                }
            });
            let witness = t10_07_native_one_campaign(
                accounts.clone(),
                trade.clone(),
                risk.clone(),
                public_ws.clone(),
                feed.clone(),
                rate_budget.clone(),
                &refresh_recorded_market,
            )
            .await;
            println!("T10_07_NATIVE_ONE_CAMPAIGN_V1:{witness}");
            let _ = t10_feed_shutdown.send(true);
            tokio::time::timeout(Duration::from_secs(5), t10_market_task)
                .await
                .expect("T10-07 live market publisher teardown deadline")
                .expect("T10-07 live market publisher task");
            let _ = maintenance_shutdown.send(true);
            let _ = public_shutdown.send(true);
            let _ = shutdown_tx.send(true);
            tokio::time::timeout(Duration::from_secs(10), maintenance_task)
                .await
                .expect("T10-07 maintenance shutdown deadline")
                .expect("T10-07 maintenance task exit");
            accounts.shutdown().await;
            let _ = tokio::time::timeout(Duration::from_secs(10), public_task)
                .await
                .expect("T10-07 public WS shutdown deadline");
            let _ = tokio::time::timeout(Duration::from_secs(10), session)
                .await
                .expect("T10-07 Native risk session shutdown deadline");
            println!("native_to_persisted_workerd_riskbook PASS: T10-07 one Native campaign");
            return;
        }
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
        // Test-only *external source stream*: public /time is sampled as
        // immutable raw JSON at 750ms scripted observation ticks, not
        // fabricated when the production GET happens. Every sample remains
        // subject to the unchanged production clock skew/RTT/age gate.
        // The producer stops as soon as ONE genuine GET consumes the sample,
        // and cannot rearm it (atomic queue+delivery fence in RecordedAccountHttp).
        // This models a changing exchange clock without a test HTTP server.
        let clock_before_first_execute = feed.public_clock_deliveries();
        let clock_feed = feed.clone();
        let first_clock_publisher = tokio::spawn(async move {
            let mut ticks = tokio::time::interval(Duration::from_millis(750));
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut published = 0usize;
            for _ in 0..32 {
                ticks.tick().await;
                let raw = serde_json::to_vec(&json!({
                    "code": "0",
                    "msg": "",
                    "data": [{"ts": chrono::Utc::now().timestamp_millis().to_string()}]
                }))
                .unwrap();
                if !clock_feed.refresh_unconsumed_public_clock(clock_before_first_execute, raw) {
                    break;
                }
                published += 1;
            }
            published
        });
        // The campaign SQL witness is short-lived (3 s relative to the
        // source cutoff). The earlier readiness handoff precedes several
        // public and private observations and cannot be reused at Execute.
        // This check is READ-ONLY against both existing SQL owners. Only
        // production maintenance can refresh their persisted source frontier;
        // an absent/stale receipt still fails closed under the real policy.
        let mut latest_readiness = String::from("NOT_OBSERVED");
        tokio::time::timeout(Duration::from_secs(9), async {
            loop {
                let through = chrono::Utc::now().timestamp_millis() - 2_000;
                let mut all_recent = true;
                for alias in ["fixture-a", "fixture-b"] {
                    let account = accounts.resolve(alias).await.unwrap();
                    let campaign = campaign_id.clone();
                    match account.execution.store.call(
                        std::time::Instant::now() + Duration::from_secs(2),
                        move |s| s.verify_campaign_entry_readiness(
                            &campaign, native_origin, through
                        ),
                    ).await {
                        Ok(()) => {}
                        Err(reason) => {
                            all_recent = false;
                            latest_readiness = format!("{alias}:{reason}");
                            break;
                        }
                    }
                }
                if all_recent {
                    eprintln!(
                        "T09_1_PRE_EXECUTE_READINESS_V1:actors=2 through_ms={through} state=SOURCE_VERIFIED"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or_else(|_| panic!(
            "T09_1_PRE_EXECUTE_CAMPAIGN_READINESS_NOT_FRESH:{latest_readiness}"
        ));

        // Record the first signed fee/leverage pages only AFTER the two
        // independent SQL campaign frontiers are fresh. Previously their
        // exchange ts was captured before the bounded readiness wait; when
        // maintenance needed several seconds, the otherwise valid fee page
        // expired under the unchanged 5s production source-age gate before
        // public Execute could consume it. These are still finite, immutable
        // raw responses; a late/invalid page must fail closed.
        // Leverage does not have a signed exchange-time expiry. Publish
        // the initial fee pages at the FIRST public Execute event instead,
        // after all read-only readiness and preview work. This preserves
        // the original three finite signed pages and unchanged 5s gate.
        for _ in 0..3 {
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
        // An admission can be deferred by the SHARED production RateBudget
        // while maintenance consumes the same authenticated private-GET
        // domain. Retry is permitted here ONLY after a proven LOCAL,
        // NOT_STARTED, RELEASED intent with zero venue POST attempts. Every
        // subsequent request is a new explicit intent; an UNKNOWN/potentially
        // sent mutation must NEVER be retried.
        let mut execute = execute;
        if t10_12_reserve_enabled() {
            T10_12_ARM_RESERVE.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        let opened = Box::pin(async {
            let admission_deadline = std::time::Instant::now() + Duration::from_secs(16);
            for attempt in 0..4 {
                if attempt == 0 {
                    // The Native Execute is now actually ready to consume
                    // this independently signed exchange observation.
                    // Do not refresh bytes on GET, replay a consumed source,
                    // or create additional pages because the runner is slow.
                    let execute_fee_at =
                        if std::env::var("OKX_T09_FIRST_FEE_EXPIRED_PROBE").as_deref() == Ok("1") {
                            // Pre-recorded OLD exchange timestamp. Freshness is
                            // checked by production, not by test wall-clock delay.
                            "1793460000000".to_owned()
                        } else {
                            chrono::Utc::now().timestamp_millis().to_string()
                        };
                    // First public Execute is a distinct signed source event.
                    // Retire old pre-Execute/Preview observations rather than
                    // append fresh pages BEHIND still-unconsumed stale bytes.
                    // The production 5s fee gate stays unchanged.
                    let _old_fee_pages = feed.advance_recorded_route(
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
                                "instType":"SWAP","level":"Lv1","ts":execute_fee_at,
                                "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                            }])),
                        }).collect(),
                    );
                 }
                let admission: Result<serde_json::Value, String> = if attempt == 0
                    && std::env::var("OKX_T10_06_CONCURRENT_PROBE").as_deref() == Ok("1")
                {
                    // Unlike the warm replay test below, NONE of these 100
                    // calls has an already durable intent at the barrier.
                    // Only one existing Native owner may win BEGIN_POST. All
                    // other calls must be the same owner/key or fail locally,
                    // never create a second reservation or exchange POST.
                    for _ in 0..100 {
                        feed.expect_optional_get(RecordedGet {
                            access_key:"fixture-executor-a".into(),
                            path:"/api/v5/trade/order".into(),
                            required_query:vec![
                                ("instId".into(),"BTC-USDT-SWAP".into())
                            ],
                            raw_response:raw_order.clone(),
                        });
                    }
                    // One recorded raw empty pending-order inventory remains
                    // immutable while 100 public Execute entrances compete.
                    // The prior generic 64-GET fallback is not a valid
                    // bound for 100 planned consumers. Declare the 100
                    // entrants plus at most two account-owner reads for each
                    // two-second finance interval during the 16-second
                    // admission deadline, before starting the burst.
                    // No venue order/fee/risk data is calculated here.
                    let pending_source = || RecordedGet {
                        access_key: "fixture-observer-a".into(),
                        path: "/api/v5/trade/orders-pending".into(),
                        required_query: vec![("limit".into(), "100".into())],
                        raw_response: envelope(json!([])),
                    };
                    let pending_read_budget: u64 = 100 + 2 * (16 / 2 + 1);
                    assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0,
                        "T10-06 cold source must be registered before any create POST");
                    feed.expect_immutable_signed_phase_bounded(
                        "t10-06-cold-entrance", pending_source(),
                        pending_read_budget, Duration::from_secs(20),
                    );
                    let started = std::time::Instant::now();
                    let gate = Arc::new(tokio::sync::Barrier::new(101));
                    let mut tasks = tokio::task::JoinSet::new();
                    for _ in 0..100 {
                        let public = trade.clone();
                        let request = execute.clone();
                        let wait = gate.clone();
                        tasks.spawn(async move {
                            wait.wait().await;
                            public.trade("fixture-a", &request, None).await
                        });
                    }
                    gate.wait().await;
                    let mut signed = Vec::new();
                    let mut busy = 0usize;
                    let mut read_deferred = 0usize;
                    while let Some(result) = tokio::time::timeout_at(
                        admission_deadline.into(), tasks.join_next()
                    ).await.expect("T10-06 cold concurrent join deadline") {
                        match result.expect("T10-06 cold public task panicked") {
                            Ok(v) if v["status"] == "partial" || v["status"] == "unknown" => {
                                signed.push(v);
                            }
                            Err(reason) if reason == "ACCOUNT_MUTATION_BUSY" => busy += 1,
                            Err(reason) if recorded_preview_local_defer(
                                &reason, &rate_budget,
                                admission_deadline.saturating_duration_since(
                                    std::time::Instant::now()
                                )
                            ).is_some() => read_deferred += 1,
                            other => panic!("T10_06_UNSAFE_COLD_CONCURRENT_OUTCOME:{other:?}"),
                        }
                    }
                    assert_eq!(signed.len() + busy + read_deferred, 100,
                        "T10-06 all 100 cold calls must be accounted for");
                    let acknowledged = signed.iter()
                        .find(|v| v["status"] == "partial"
                            && v["data"]["intent"]["state"] == "ACKNOWLEDGED")
                        .expect("T10-06 one cold public call must get actual venue ACK")
                        .clone();
                    let original = acknowledged["data"]["intent"]["intent_id"]
                        .as_str().expect("T10-06 original durable intent");
                    for result in &signed {
                        let observed = result["data"]["intent"]["intent_id"]
                            .as_str().or_else(|| result["intent_id"].as_str());
                        assert_eq!(observed, Some(original),
                            "100 concurrent same-key requests must never create a second intent");
                        if result["status"] == "unknown" {
                            assert_eq!(result["retryable"],false,
                                "T10-06 UNKNOWN must be no-resubmit");
                        }
                    }
                    assert_eq!(feed.outbound_posts.lock().unwrap().len(), 1,
                        "T10-06 cold concurrency may create only ONE venue POST");
                    // One real ACK is the causal boundary between the cold
                    // incoming burst and subsequent same-key SQL replays.
                    // Republish the SAME recorded raw inventory only at that
                    // observed event; do not create a second economic order.
                    let cold_source_reads = feed.advance_immutable_signed_phase_bounded(
                        "t10-06-cold-entrance",
                        "t10-06-post-ack-replay",
                        pending_source(),
                        pending_read_budget,
                        Duration::from_secs(300),
                    );
                    assert!(cold_source_reads > 0 && cold_source_reads <= pending_read_budget,
                        "T10-06 cold source must be consumed within its declared budget");
                    println!("T10_06_NATIVE_COLD_CONCURRENT_V1:{}",json!({
                        "requested":100,
                        "same_durable_intent_readbacks":signed.len(),
                        "local_mutation_busy_no_send":busy,
                        "local_private_read_defer_no_send":read_deferred,
                        "original_intent_id":original,
                        "outbound_create_posts":1,
                        "elapsed_ms":started.elapsed().as_millis()
                    }));
                    Ok(acknowledged)
                } else {
                    trade.trade("fixture-a", &execute, None).await
                };
                let (reason, delay) = match admission {
                    Ok(admission) => {
                        if t10_12_reserve_enabled()
                            && T10_12_RESERVE_WAS_HELD.load(std::sync::atomic::Ordering::SeqCst)
                        {
                            // Do not convert a lost Worker HELD response into another
                            // Execute or a possibly sent exchange order. The same
                            // original Native SQL rejection is reconciled below.
                            return admission;
                        }
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
                            .expect("T09.1 local denial must provide a reason")
                            .to_owned();
                        let local_reason = reason
                            .strip_prefix("OKX API error: ")
                            .unwrap_or(reason.as_str());
                        let remaining =
                            admission_deadline.saturating_duration_since(std::time::Instant::now());
                        let delay = if local_reason == "ENTRY_FEE_EXCHANGE_TIME_STALE" {
                            // The signed fee source has expired while another owner
                            // consumed this account's rate budget. The guards above
                            // prove NOT_STARTED, RELEASED and zero venue POSTs.
                            // Publish the next independently timed signed fee event
                            // below; never reuse or treat the stale fee as current.
                            Duration::ZERO
                        } else {
                            recorded_preview_local_defer(local_reason, &rate_budget, remaining)
                                .unwrap_or_else(|| {
                                    panic!("T09.1 non-private-GET/nonlocal denial cannot retry: {admission}; rate={:?}",
                                        rate_budget.snapshot().last_throttle)
                                })
                        };
                        (reason, delay)
                    }
                    Err(reason) if reason == "ENTRY_FEE_EXCHANGE_TIME_STALE" => {
                        // An expired tariff may be retried with a fresh recorded
                        // source ONLY if the original SQL owner has no intent.
                        // Later-stage expiry after PREPARE is NO-RESUBMIT.
                        assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0,
                            "pre-plan expired fee cannot send venue POST");
                        let key = match &execute {
                            TradeRequest::Execute { idempotency_key, .. } => idempotency_key.clone(),
                            _ => panic!("T09.1 Execute identity required"),
                        };
                        let owner = accounts.resolve("fixture-a").await
                            .expect("T09.1 original SQL owner required");
                        let persisted = owner.execution.store.call(
                            std::time::Instant::now() + Duration::from_secs(2),
                            move |sql| sql.by_idempotency(&key),
                        ).await.expect("T09.1 exact SQL identity readback required");
                        assert!(persisted.is_none(),
                            "expired fee after SQL PREPARE must not create a new intent");
                        if attempt == 0 {
                            println!(
                                "T09_1_PRE_INTENT_STALE_FEE_PROOF_V1:{}",
                                json!({
                                    "reason": reason,
                                    "durable_intent_found": false,
                                    "outbound_posts": 0,
                                })
                            );
                        }
                        (reason, Duration::ZERO)
                    }
                    Err(reason) => panic!(
                        "T09.1 unexpected Native error cannot authorize retry: {reason}"
                    ),
                };
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
        if t10_12_reserve_enabled() {
            assert_eq!(
                opened["status"], "completed",
                "lost HELD must resolve to a durable local no-send result: {opened}"
            );
            assert_eq!(opened["data"]["state"], "REJECTED");
            assert_eq!(opened["data"]["rejection_origin"], "LOCAL");
            assert_eq!(opened["data"]["send_state"], "NOT_STARTED");
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
            let intent_id = opened["data"]["intent_id"]
                .as_str()
                .expect("original Native SQL intent identity")
                .to_string();
            let original = t10_12_reserve_sql_snapshot(&accounts, &intent_id).await;
            assert_eq!(original["state"], "REJECTED");
            assert_eq!(original["attempt_kind"], "RISK_RESERVE_ATTEMPT");
            assert_eq!(original["central_receipt"], serde_json::Value::Null);
            assert_eq!(original["exchange_order_id"], serde_json::Value::Null);
            assert_eq!(original["terminal_receipt"]["kind"], "REJECTED_UNSENT");
            // A durably REJECTED_UNSENT Native intent is safe to inspect
            // while the Worker reconnects. Its first public Status can
            // legitimately report PENDING/transport lost; waiting for the
            // supervisor's completed recovery BEFORE the first Status caused
            // a lifecycle barrier deadlock. Drive recovery only through
            // the ORIGINAL read/reconcile operation, never Execute/RESERVE
            // or a second exchange POST. Keep the bound below the child and
            // case timeouts; unexpected reasons fail at the first boundary.
            // Keep the large nested TradeService Status/reconcile future
            // off the default Windows Rust test thread's 2 MiB stack. The
            // same production call, finite timeout and assertions remain.
            // Only this opt-in T10-12 crash-test future is boxed.
            let status = t10_12_await_recovery(&mut session, Box::pin(tokio::time::timeout(Duration::from_secs(55), async {
                loop {
                    let observed = trade
                        .trade(
                            "fixture-a",
                            &okx_protocol::trading::TradeRequest::Status {
                                reference: intent_id.clone(),
                            },
                            None,
                        )
                        .await
                        .expect("public Status must reconcile original lost RESERVE");
                    assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
                    if observed["data"]["central_release"]["state"] == "verified" {
                        break observed;
                    }
                    assert_eq!(
                        observed["data"]["central_release"]["state"],
                        "pending",
                        "unexpected nonterminal reserve recovery: {observed}"
                    );
                    assert!(
                        matches!(
                            observed["data"]["central_release"]["reason_code"].as_str(),
                            Some(
                                "RISK_TRANSPORT_LOST_DO_NOT_REPLAY"
                                    | "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY"
                                    | "RISK_TRANSPORT_UNAVAILABLE"
                            )
                        ),
                        "only a proven transport interruption may keep original no-send pending: {observed}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })))
            .await
            .expect("T10-12 recovery owner failed before original public Status completed")
            .expect("T10-12 original public Status never reconciled after Worker reconnect");
            // A successful Status alone is not proof of the OS-kill, old
            // generation rejection or persisted Worker restart. Require
            // that independent same-SQL supervisor witness as well.
            let handoff = std::env::var("OKX_INTEGRATION_HANDOFF_PATH").unwrap();
            let ready = tokio::time::timeout(
                Duration::from_secs(12),
                t10_12_barrier(&format!("{handoff}.t12-native-recovered")),
            )
            .await
            .expect("T10-12 authenticated Worker recovery witness missing after Status");
            assert_eq!(ready["schema"], "okx.t10-12-native-reserve-same-sql/v1");
            assert_eq!(ready["old_reply_rejected"], true);
            assert_eq!(ready["original_sql"]["intent_id"], intent_id);
            assert_eq!(
                ready["original_reservation_key"],
                original["reservation_key"]
            );
            assert_eq!(ready["sql_after_worker_inspect"]["state"], "REJECTED");
            assert_eq!(
                ready["sql_after_worker_inspect"]["central_receipt"],
                serde_json::Value::Null
            );
            assert_eq!(
                ready["sql_after_worker_inspect"]["exchange_order_id"],
                serde_json::Value::Null
            );
            assert_eq!(feed.outbound_posts.lock().unwrap().len(), 0);
            let after = t10_12_reserve_sql_snapshot(&accounts, &intent_id).await;
            assert_eq!(after["state"], "REJECTED");
            assert_eq!(after["central_receipt"], serde_json::Value::Null);
            assert_eq!(after["exchange_order_id"], serde_json::Value::Null);
            assert_eq!(after["central_release"]["kind"], "CENTRAL_RELEASE");
            assert_eq!(after["reservation_key"], original["reservation_key"]);
            let key = original["reservation_key"].as_str().unwrap().to_string();
            let plan = original["plan_hash"].as_str().unwrap().to_string();
            let inspect = risk
                .request(RiskCommand {
                    schema: RISK_COMMAND_SCHEMA_V1.into(),
                    command_id: "inspect_t10_12_native_reserve_after_public_status".into(),
                    installation_id: "installation_test".into(),
                    action: RiskCommandAction::Inspect {
                        reservation_key: key,
                        plan_sha256: plan,
                    },
                })
                .await
                .unwrap();
            assert_eq!(inspect.status, RiskDecisionStatus::Observed);
            assert_eq!(
                inspect.reservation_state,
                Some(RiskReservationState::Released)
            );
            let mut risk_after = summary("risk_t10_12_reserve_final_no_leak");
            risk_after.installation_id = "installation_test".into();
            let final_budget = risk.request(risk_after).await.unwrap().budget.unwrap();
            assert_eq!(
                final_budget.entry_budget.as_ref().unwrap().consumed_usd,
                "0",
                "lost RESERVE may not spend entry money"
            );
            // The two unrelated reservations already present in `checked`
            // must survive. Only this original unsent intent may be released.
            // RESERVE installs its own census floor (not a monotonic clock).
            // Restore every monetary field and require that exact source time
            // to survive the Worker crash and read-only/no-send recovery.
            let mut expected_budget = checked.budget.clone().unwrap();
            let reserve_source = ready["original_reserve_source_as_of_ms"]
                .as_u64()
                .expect("original authenticated RESERVE census floor");
            assert!(reserve_source > 0);
            expected_budget.source_as_of_ms = Some(reserve_source);
            assert_eq!(
                final_budget, expected_budget,
                "no-send recovery must restore the pre-Execute budget without affecting other reservations"
            );
            let verified = serde_json::json!({
                "schema":"okx.t10-12-native-reserve-sql-release/v1",
                "intent_id":intent_id,
                "old_reply_rejected":ready["old_reply_rejected"],
                "before_status":original,
                "after_status":after,
                "public_status":status["status"],
                "public_central_release":status["data"]["central_release"],
                "worker_reservation_state":"RELEASED",
                "worker_budget":final_budget,
                "worker_budget_before_execute":checked.budget,
                "outbound_create_posts":0,
                "no_second_grant":true,
            });
            println!("T10_12_NATIVE_RESERVE_SAME_SQL_RECOVERY_V1:{verified}");
            feed_shutdown.send(true).unwrap();
            maintenance_shutdown.send(true).unwrap();
            public_shutdown.send(true).unwrap();
            shutdown_tx.send(true).unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(10), maintenance_task)
                .await
                .expect("reserve crash maintenance owner must stop");
            let _ = tokio::time::timeout(Duration::from_secs(10), public_feed)
                .await
                .expect("reserve crash recorded market publisher must stop");
            let _ = tokio::time::timeout(Duration::from_secs(10), session)
                .await
                .expect("reserve crash recovered WSS session must stop");
            return;
        }
        let clock_samples = tokio::time::timeout(Duration::from_secs(2), first_clock_publisher)
            .await
            .expect("first external clock publisher did not terminate")
            .expect("first external clock publisher task failed");
        assert!(
            feed.public_clock_deliveries() > clock_before_first_execute,
            "production clock evidence must consume a one-use raw clock GET"
        );
        eprintln!(
            "T09_1_RECORDED_CLOCK_SOURCE_V1:samples={clock_samples} delivered={}",
            feed.public_clock_deliveries() - clock_before_first_execute
        );
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
        // S2-04 Native/public-path proof: a differently spelled decimal
        // request is the SAME semantic intent. No second reserve or POST.
        // Do not update the original persisted plan or its deadline.
        let mut semantic_replay = execute.clone();
        if let TradeRequest::Execute {
            sizing: Sizing::Contracts { contracts },
            order,
            protection: Some(Protection::Price { stop_price }),
            ..
        } = &mut semantic_replay
        {
            *contracts = "0.1".into();
            order.limit_price = Some("1000.00".into());
            order.max_entry_usd = Some("10.00".into());
            *stop_price = "900.00".into();
        } else {
            panic!("S2-04 requires the original contracts/price/stop request");
        }
        assert!(semantic_replay.valid());
        let repeated = trade
            .trade("fixture-a", &semantic_replay, None)
            .await
            .expect("S2-04 equivalent decimal Execute must reuse the original intent");
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
            "semantically equivalent Execute must not post to OKX twice"
        );
        // Same authenticated account/action/key with a meaningful quantity
        // or side change is an IDEMPOTENCY_CONFLICT before any possible POST.
        let mut amount_conflict = semantic_replay.clone();
        if let TradeRequest::Execute {
            sizing: Sizing::Contracts { contracts },
            ..
        } = &mut amount_conflict
        {
            *contracts = "0.11".into();
        } else {
            panic!("S2-04 contracts variant missing");
        }
        let amount_conflict_reason = trade
            .trade("fixture-a", &amount_conflict, None)
            .await
            .expect_err("S2-04 amount mismatch must not replay");
        assert_eq!(amount_conflict_reason, "IDEMPOTENCY_CONFLICT");
        let mut side_conflict = semantic_replay.clone();
        if let TradeRequest::Execute { side, .. } = &mut side_conflict {
            *side = PositionSide::Short;
        } else {
            panic!("S2-04 side variant missing");
        }
        let side_conflict_reason = trade
            .trade("fixture-a", &side_conflict, None)
            .await
            .expect_err("S2-04 side mismatch must not replay");
        assert_eq!(side_conflict_reason, "IDEMPOTENCY_CONFLICT");
        let mut reference_conflict = semantic_replay.clone();
        if let TradeRequest::Execute { order, .. } = &mut reference_conflict {
            order.reference = Some("other_saved_intent".into());
        } else {
            panic!("S2-04 target variant missing");
        }
        assert!(reference_conflict.valid());
        let reference_conflict_reason = trade
            .trade("fixture-a", &reference_conflict, None)
            .await
            .expect_err("S2-04 target reference change cannot reuse an intent");
        assert_eq!(reference_conflict_reason, "IDEMPOTENCY_CONFLICT");
        let mut basis_conflict = semantic_replay.clone();
        if let TradeRequest::Execute { protection, .. } = &mut basis_conflict {
            *protection = Some(Protection::Distance {
                stop_distance_bps: 1000,
            });
        } else {
            panic!("S2-04 stop basis variant missing");
        }
        assert!(basis_conflict.valid());
        let basis_conflict_reason = trade
            .trade("fixture-a", &basis_conflict, None)
            .await
            .expect_err("S2-04 protection basis change cannot reuse an intent");
        assert_eq!(basis_conflict_reason, "IDEMPOTENCY_CONFLICT");
        let mut deadline_conflict = semantic_replay.clone();
        if let TradeRequest::Execute { expires_in_ms, .. } = &mut deadline_conflict {
            *expires_in_ms = 20000;
        } else {
            panic!("S2-04 explicit deadline variant missing");
        }
        assert!(deadline_conflict.valid());
        let deadline_conflict_reason = trade
            .trade("fixture-a", &deadline_conflict, None)
            .await
            .expect_err("S2-04 explicit expiry change cannot extend an existing intent");
        assert_eq!(deadline_conflict_reason, "IDEMPOTENCY_CONFLICT");
        assert_eq!(
            feed.outbound_posts.lock().unwrap().len(),
            1,
            "S2-04 equivalent replay and five conflicts may not create extra venue POSTs"
        );
        // T10-06: 100 real simultaneous PUBLIC TradeService requests with
        // exactly the same persisted owner/account/idempotency/payload.
        // Competing requests may be locally deferred by the production
        // admission/read budget, but must never create another intent or
        // transmit another venue POST. A definitive independent admission
        // cannot borrow the first worker reservation.
        if std::env::var("OKX_T10_06_CONCURRENT_PROBE").as_deref() == Ok("1") {
            // The script asks for exactly 100 idempotent public reads. Record
            // one immutable authenticated order GET per caller, NOT a new
            // create/fill, and do not let unbounded default pages mask a
            // missing source. Production ratelimits still apply normally.
            for _ in 0..100 {
                feed.expect_optional_get(RecordedGet {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/order".into(),
                    required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                    raw_response: raw_order.clone(),
                });
            }
            let mut concurrent = tokio::task::JoinSet::new();
            let started = std::time::Instant::now();
            for _ in 0..100 {
                let public = trade.clone();
                let same_request = semantic_replay.clone();
                concurrent
                    .spawn(async move { public.trade("fixture-a", &same_request, None).await });
            }
            let mut identical = 0_u32;
            let mut local_read_denied = 0_u32;
            let mut errors = Vec::new();
            while let Some(task) = concurrent.join_next().await {
                match task.expect("T10-06 public task must finish") {
                    Ok(v) => {
                        let id = v["data"]["intent"]["intent_id"]
                            .as_str()
                            .or_else(|| v["intent_id"].as_str());
                        if id == Some(opened_intent_id.as_str()) {
                            if v["status"] == "unknown" {
                                assert_eq!(
                                    v["retryable"], false,
                                    "T10-06 unknown must never authorize a resend"
                                );
                                assert_eq!(v["next_action"], "trade.status");
                                assert!(
                                    matches!(
                                        v["reason_code"].as_str(),
                                        Some(
                                            "ACCOUNT_MUTATION_BUSY_STATUS_NOT_EVALUATED_NO_RESUBMIT"
                                        ) | Some("ORDER_READBACK_INCOMPLETE_NO_RESUBMIT")
                                            | Some("EXCHANGE_ORDER_STATE_UNKNOWN")
                                    ),
                                    "T10-06 unknown must identify a real read/owner blocker: {v}"
                                );
                            }
                            identical += 1;
                        } else {
                            errors.push(format!("T10_06_WRONG_INTENT:{v}"));
                        }
                    }
                    Err(reason) => {
                        let last = rate_budget.snapshot().last_throttle;
                        if reason.contains("rate/backpressure defer")
                            && last.as_ref().is_some_and(|r| !r.request_sent)
                        {
                            local_read_denied += 1;
                        } else {
                            errors.push(format!("T10_06_NONLOCAL_FAILURE:{reason}"));
                        }
                    }
                }
            }
            assert!(
                errors.is_empty(),
                "T10-06 unsafe concurrent results: {errors:?}"
            );
            assert_eq!(
                identical + local_read_denied,
                100,
                "each T10-06 request must be accounted for"
            );
            assert_eq!(
                feed.outbound_posts.lock().unwrap().len(),
                1,
                "100 concurrent same-key calls must not emit another POST"
            );
            let saved_id = opened_intent_id.clone();
            let semantic_key = match &semantic_replay {
                TradeRequest::Execute {
                    idempotency_key, ..
                } => idempotency_key.clone(),
                _ => unreachable!("T10-06 requires original public Execute key"),
            };
            let owner = accounts.resolve("fixture-a").await.unwrap();
            let (by_id, by_key) = owner
                .execution
                .store
                .call(
                    std::time::Instant::now() + Duration::from_secs(3),
                    move |s| Ok((s.intent(&saved_id)?, s.by_idempotency(&semantic_key)?)),
                )
                .await
                .expect("T10-06 same account SQL owner");
            // Do not mistake a previously rejected LOCAL/NOT_STARTED key
            // for the real ACKed order after a legal explicit-key retry.
            let persisted = by_id.expect("T10-06 original persisted intent");
            let keyed = by_key.expect("T10-06 semantic key persisted intent");
            assert_eq!(
                keyed.intent_id, persisted.intent_id,
                "100 same-key public requests must bind to original intent"
            );
            assert_eq!(persisted.intent_id, opened_intent_id);
            assert_eq!(persisted.state, okx_execution::IntentState::Acknowledged);
            println!(
                "T10_06_NATIVE_CONCURRENT_V1:{}",
                json!({
                    "requested":100,
                    "same_intent_returns":identical,
                    "local_no_send_read_denials":local_read_denied,
                    "nonlocal_failures":errors.len(),
                    "original_intent_id":opened_intent_id,
                    "persisted_intent_id":persisted.intent_id,
                    "sql_revision":persisted.revision,
                    "created_posts":feed.outbound_posts.lock().unwrap().len(),
                    "elapsed_ms":started.elapsed().as_millis(),
                    "conflict_reason":amount_conflict_reason,
                })
            );
        }
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
                "sql_request_hash":saved.request_hash,
                "sql_plan_hash":saved.plan_hash,
                "sql_deadline_ms":saved.deadline,
                "sql_client_order_id":saved.client_order_id,
                "sql_policy_revision":saved.policy_revision,
                "created_posts":feed.outbound_posts.lock().unwrap().len(),
                "semantic_decimal_equivalent_replay":true,
                "semantic_amount_conflict_reason":amount_conflict_reason,
                "semantic_side_conflict_reason":side_conflict_reason,
                "semantic_reference_conflict_reason":reference_conflict_reason,
                "semantic_basis_conflict_reason":basis_conflict_reason,
                "semantic_deadline_conflict_reason":deadline_conflict_reason,
                "reserved_entry_usd_before_replay": before_repeat.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
                "reserved_entry_usd_after_replay": after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
            })
        );
        // S2-07 account identity uses the independently verified SQL
        // owner, never the mutable UI alias or the request display account.
        // A different account may reuse the SAME public idempotency token
        // without sharing the A request hash or seeing its SQL reservation.
        use sha2::{Digest, Sha256};
        let account_a = accounts.resolve("fixture-a").await.unwrap();
        let account_b = accounts.resolve("fixture-b").await.unwrap();
        let scope_a = account_a.configuration.account_key.wire();
        let scope_b = account_b.configuration.account_key.wire();
        assert_ne!(scope_a, scope_b);
        let hash_a = format!(
            "{:x}",
            Sha256::digest(
                execute
                    .canonical_semantic_json(&scope_a)
                    .unwrap()
                    .as_bytes()
            )
        );
        let hash_b = format!(
            "{:x}",
            Sha256::digest(
                execute
                    .canonical_semantic_json(&scope_b)
                    .unwrap()
                    .as_bytes()
            )
        );
        assert_eq!(hash_a, saved.request_hash);
        assert_ne!(
            hash_a, hash_b,
            "S2-07 verified accounts must have separate identities"
        );
        let other_key_absent = account_b
            .execution
            .store
            .call(std::time::Instant::now() + Duration::from_secs(3), |sql| {
                Ok(sql
                    .by_idempotency("t09_real_public_execute_0001")?
                    .is_none())
            })
            .await
            .expect("S2-07 independent account B Native SQL");
        assert!(
            other_key_absent,
            "S2-07 account B must not inherit A's intent"
        );
        println!(
            "S2_07_NATIVE_ACCOUNT_SCOPE_V1:{}",
            json!({
                "same_client_key":true,
                "distinct_account_scopes":true,
                "owner_semantic_sha256":hash_a,
                "other_semantic_sha256":hash_b,
                "foreign_sql_intent_absent":other_key_absent,
                "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
            })
        );
        // These read-only probes must not outlive their phase: keeping a
        // cloned AccountResources Arc alive prevents the normal SQL-owner
        // shutdown receipt and causes a false S1-05 teardown timeout.
        drop(account_a);
        drop(account_b);
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
        let mut first_close_at: Option<String> = None;
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
            // T10-12 explicitly invokes two public Status calls on the
            // same original intent: before Worker OS-kill and after the
            // authenticated reconnect. Each performs its own signed order
            // GET; one recording cannot cover both. Replay only the EXACT
            // immutable recorded venue bytes, never a made-up order or new
            // economic event. All other cases still get one order page.
            let recorded_status_reads = if t10_12_enabled() { 2 } else { 1 };
            for _ in 0..recorded_status_reads {
                feed.expect_get(RecordedGet {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/order".into(),
                    required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                    raw_response: after_fill_order.clone(),
                });
            }
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
            let signed_stop_readback = envelope(json!([{
                "instId":"BTC-USDT-SWAP",
                "algoId":"900000001",
                "algoClOrdId":stop_client,
                "state":"live","sz":"0.1","side":"sell",
                "posSide":"long","tdMode":"cross","ordType":"conditional",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
            }]));
            // Just as the order, the attached stop is independently read on
            // EACH public Status. The recovery Status must not treat an
            // exhausted test transcript as a verified protection incident.
            // Both reads still run through the original signed Executor GET
            // and production identity/coverage checks. A differing actual
            // second reply remains free to fail closed; the test records an
            // unchanged venue state, not a cached or assumed protection.
            let recorded_protection_reads = if t10_12_enabled() { 2 } else { 1 };
            for _ in 0..recorded_protection_reads {
                feed.expect_get(RecordedGet {
                    access_key: "fixture-executor-a".into(),
                    path: "/api/v5/trade/order-algo".into(),
                    required_query: vec![("algoClOrdId".into(), stop_client.clone())],
                    raw_response: signed_stop_readback.clone(),
                });
            }
            if t10_12_enabled() {
                T10_12_ARM_SETTLEMENT.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let mut actual = trade
                .trade(
                    "fixture-a",
                    &okx_protocol::trading::TradeRequest::Status {
                        reference: idempotent_intent_id.clone(),
                    },
                    None,
                )
                .await
                .expect("production trade.status must return its real evidence");
            if t10_12_enabled() {
                let recovered =
                    t10_12_barrier(&format!("{handoff_path}.t12-native-recovered")).await;
                assert_eq!(recovered["old_reply_rejected"], true);
                actual = trade
                    .trade(
                        "fixture-a",
                        &okx_protocol::trading::TradeRequest::Status {
                            reference: idempotent_intent_id.clone(),
                        },
                        None,
                    )
                    .await
                    .expect("T10-12 same original Status recovery");
                let after = t10_12_sql_snapshot(&accounts).await;
                // Read-only failure witness on the ORIGINAL account/SQL
                // owner. A missing local ACK could mean source capture never
                // completed, a Worker CAS response was denied, or an existing
                // terminal receipt skipped an unacknowledged partial revision.
                // Do not alter the status decision or seed any test source.
                let terminal_receipt = accounts.resolve("fixture-a").await.unwrap();
                let key = idempotent_intent_id.clone();
                let terminal_receipt = terminal_receipt
                    .execution
                    .store
                    .call(
                        std::time::Instant::now() + Duration::from_secs(3),
                        move |s| s.operation(&format!("entry-settlement:{key}")),
                    )
                    .await
                    .expect("T10-12 terminal receipt read-only diagnostic");
                assert_eq!(
                    after["local_settlement_ack"],
                    true,
                    "production Status must durably acknowledge original settlement; status={}; incremental={}; release={}; financial_error={}; financial_complete={}; terminal_receipt={}; sql_ack={}",
                    actual["status"],
                    actual["data"]["incremental_entry_settlement"],
                    actual["data"]["central_release"],
                    actual["data"]["financial_error"],
                    actual["data"]["financial_complete"],
                    terminal_receipt.is_some(),
                    after["local_settlement_ack"]
                );
                assert_eq!(after["proof"], recovered["sql_before"]["proof"]);
                assert_eq!(after["fill_count"], recovered["sql_before"]["fill_count"]);
                assert_eq!(after["bill_count"], recovered["sql_before"]["bill_count"]);
                println!("T10_12_NATIVE_LOCAL_RECOVERY_V1:{after}");
            }
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
            if std::env::var("OKX_T10_13_STOP_PROBE").as_deref() == Ok("1") {
                let stop = t10_13_stop_and_source_blocker(
                    &handoff_path,
                    accounts.clone(),
                    trade.clone(),
                    risk.clone(),
                    feed.clone(),
                    idempotent_intent_id.clone(),
                )
                .await;
                println!("T10_13_NATIVE_STOP_AFTER_FILL_V1:{stop}");
            }
            // A terminal fill/bill and a public Status observation have
            // now occurred. Re-read the SAME immutable signed financial
            // history at this causal frontier: the previous finite POST-fill
            // pages may have been consumed by independent maintenance
            // before public Close is admitted. Never turn an exhausted SPOT
            // source into a fabricated empty/campaign-ready state.
            let mut close_history_pages = Vec::new();
            for _ in 0..12 {
                for partition in ["SPOT", "MARGIN", "FUTURES", "OPTION", "SWAP"] {
                    close_history_pages.push(RecordedGet {
                        access_key: "fixture-executor-a".into(),
                        path: "/api/v5/trade/fills-history".into(),
                        required_query: vec![("instType".into(), partition.into())],
                        raw_response: if partition == "SWAP" {
                            raw_fill.clone()
                        } else {
                            envelope(json!([]))
                        },
                    });
                }
            }
            let close_fills_retired = feed.advance_recorded_route(
                "fixture-executor-a",
                "/api/v5/trade/fills-history",
                close_history_pages,
            );
            let close_bills_retired = feed.advance_recorded_route(
                "fixture-executor-a",
                "/api/v5/account/bills-archive",
                (0..12)
                    .map(|_| RecordedGet {
                        access_key: "fixture-executor-a".into(),
                        path: "/api/v5/account/bills-archive".into(),
                        required_query: vec![],
                        raw_response: raw_bill.clone(),
                    })
                    .collect(),
            );
            eprintln!(
                "T09_1_POST_FILL_SOURCE_FRONTIER_V1:retired_fills={} retired_bills={} recorded_pages_per_partition=12",
                close_fills_retired, close_bills_retired
            );
            // T09.1 next boundary: a user-facing Close is routed through the
            // same TradeService/SQL owner. The only synthetic input is the
            // independently recorded signed OKX create ACK and order GET.
            // It may not claim a flat position until close fills and the
            // protection inventory are independently captured.
            use crate::accounts::recorded_support::RecordedPost;
            let close_time = chrono::Utc::now().timestamp_millis().to_string();
            // A later mutation must not consume the old entry-clock
            // observation left in a shared recorded clock queue.
            // Retire it only AFTER the authenticated post-fill source
            // frontier above; do not change production clock limits.
            feed.advance_recorded_public_clock(envelope(json!([{"ts":close_time}])));
            // T09.8 executes this SAME T09.1 Close after an extended
            // source/SQL reconciliation. A clock timestamp frozen when
            // the fixture is registered may be >5s old by the first real
            // /public/time GET. Publish fresh immutable external raw
            // samples at bounded ticks ONLY before the first GET consumes
            // one. The recorded transport's atomic delivery fence ensures
            // we cannot rearm an already consumed sample, and production
            // clock skew, RTT, age and mutation gates remain unchanged.
            assert_eq!(
                feed.unconsumed_public_clock(),
                1,
                "T09.1 Close must have exactly one raw exchange-clock source"
            );
            let close_clock_before = feed.public_clock_deliveries();
            let close_clock_feed = feed.clone();
            let close_clock_publisher = tokio::spawn(async move {
                let mut ticks = tokio::time::interval(Duration::from_millis(200));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut published = 0usize;
                for _ in 0..140 {
                    ticks.tick().await;
                    let raw = serde_json::to_vec(&json!({
                        "code":"0","msg":"",
                        "data":[{"ts":chrono::Utc::now().timestamp_millis().to_string()}]
                    }))
                    .expect("T09.1 recorded external clock bytes");
                    if !close_clock_feed.refresh_unconsumed_public_clock(close_clock_before, raw) {
                        break;
                    }
                    published += 1;
                }
                published
            });
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
            if close_result.is_ok() {
                let samples = tokio::time::timeout(Duration::from_secs(2), close_clock_publisher)
                    .await
                    .expect("T09.1 close clock producer did not terminate")
                    .expect("T09.1 close clock producer panicked");
                assert_eq!(
                    feed.public_clock_deliveries(),
                    close_clock_before + 1,
                    "T09.1 Close must consume one real recorded public-time GET"
                );
                eprintln!(
                    "T09_1_CLOSE_CLOCK_SOURCE_V1:samples={samples} before={} after={}",
                    close_clock_before,
                    feed.public_clock_deliveries()
                );
            } else {
                // An early source/risk rejection must not leave a fixture
                // publisher running or mask the original production reason.
                close_clock_publisher.abort();
            }
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
            assert_eq!(
                close["data"]["intent"]["state"], "ACKNOWLEDGED",
                "T09_1_CLOSE_ACK_NOT_DURABLY_ADMITTED: {close}"
            );
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
                first_close_at = Some(closing_at.clone());
                // For T09.7 the venue fills the old IOC at 1000 (price improvement).
                // T09.1 alone retains its accepted 999 venue fixture.
                let accepted_close_price = if close_mode { "1000" } else { "999" };
                let accepted_close_pnl = if close_mode { "0" } else { "-0.001" };
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
                        "fillPx":accepted_close_price,"fillSz":"0.1","fillPnl":accepted_close_pnl,
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
                        "balChg":if close_mode { "-0.0005" } else { "-0.0015" },"fee":"-0.0005","pnl":accepted_close_pnl,
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
                            "px":px,"sz":"0.1","accFillSz":"0.1","avgPx":if side == "sell" { accepted_close_price } else { px },
                            "state":"filled","cTime":time,"uTime":time
                        }])),
                    });
                }
                // The two filled order observations have exact authenticated
                // POST ACK identities. Publish each as one immutable signed
                // phase: maintenance and Status must not compete for one page.
                let close_client = accounts.resolve("fixture-a").await.unwrap()
                    .execution.store.call(
                        std::time::Instant::now() + Duration::from_secs(3),
                        |sql| {
                            Ok(sql.by_idempotency("t09_real_public_close_0001")?
                                .ok_or(okx_execution::StoreError::Invalid(
                                    "T09_CLOSE_SQL_ORDER_MISSING"
                                ))?.client_order_id)
                        }
                    ).await.expect("verified close must have durable client order identity");
                assert_ne!(close_client, saved.client_order_id,
                    "close and opening cannot share an exchange client ID");
                let phased_orders = terminal_orders.iter()
                    .zip([close_client, saved.client_order_id.clone()])
                    .map(|(recorded, client)| RecordedGet {
                        access_key: recorded.access_key.clone(),
                        path: recorded.path.clone(),
                        required_query: vec![
                            ("instId".into(), "BTC-USDT-SWAP".into()),
                            ("clOrdId".into(), client),
                        ],
                        raw_response: recorded.raw_response.clone(),
                    }).collect::<Vec<_>>();
                feed.advance_recorded_route(
                    "fixture-executor-a",
                    "/api/v5/trade/order",
                    terminal_orders,
                );
                for (index, page) in phased_orders.into_iter().enumerate() {
                    let promoted = feed.supersede_queued_signed_source_at_event(
                        "t09-1-verified-terminal-order",
                        if index == 0 {
                            "t09-1-filled-close-order"
                        } else {
                            "t09-1-filled-opening-order"
                        },
                        page,
                    );
                    assert!(promoted > 0,
                        "an immutable filled order requires an ACK-bound recorded predecessor");
                }
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
                // The cancel-algo mutation clock must be sampled close to the
                // actual protection reconciliation, not before a potentially
                // 15-second source-capture observation. Queue it at the public
                // Status boundary below; preserve the production 5s clock gate.
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
                let mut sql_verified_capture_ms: Option<i64> = None;
                let terminal_capture = tokio::time::timeout(Duration::from_secs(15), async {
                    loop {
                        let health = resource.source_capture_health.read().await.clone();
                        let snapshot = resource.latest_snapshot.read().await.clone();
                        let source_ms = snapshot.as_ref().and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(&s.source_received_at)
                                .ok()
                                .map(|t| t.timestamp_millis())
                        });
                        let source_ready = health.last_success_ms.is_some_and(|t| t >= closed_ms)
                            && source_ms.is_some_and(|t| t >= closed_ms);
                        let signed_flat = snapshot.as_ref()
                            .is_some_and(|s|s.positions.is_empty());
                        // Every completed capture commits its SQL pages
                        // BEFORE source_capture_health reports success.
                        // Reading SQL early or repeatedly for the SAME cycle
                        // just queues behind financial ingest and can starve
                        // the actual account owner. Await a newly completed
                        // signed-flat cycle, then read its cursor proof once.
                        if !source_ready || !signed_flat
                            || sql_verified_capture_ms == health.last_success_ms
                        {
                            last_capture_witness = json!({
                                "signed_flat":signed_flat,
                                "capture_success_ms":health.last_success_ms,
                                "capture_failure":health.reason_code,
                                "snapshot_received_ms":source_ms,
                                "source_ready":source_ready,
                                "cursor_stage":"WAIT_FINANCE_CYCLE"
                            });
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        sql_verified_capture_ms = health.last_success_ms;
                        let (fills, bills) = resource
                            .execution
                            .store
                            .call(std::time::Instant::now() + Duration::from_secs(3), |s| {
                                Ok((s.cursor("fills", "SWAP")?, s.cursor("bills", "account")?))
                            })
                            .await
                            .expect("T09.1 read-only production finance owner readback");
                        let fill_ready = fills.as_ref().is_some_and(|c| {
                            c.generation > 0 && c.cursor.is_empty()
                                && c.coverage_gaps.is_empty() && c.watermark >= closed_ms
                        });
                        let bill_ready = bills.as_ref().is_some_and(|c| {
                            c.generation > 0 && c.cursor.is_empty()
                                && c.coverage_gaps.is_empty() && c.watermark >= closed_ms
                        });
                        last_capture_witness = json!({
                            "signed_flat":signed_flat,
                            "capture_success_ms": health.last_success_ms,
                            "capture_failure": health.reason_code,
                            "snapshot_received_ms": source_ms,
                            "source_ready": source_ready,
                            "fill_ready": fill_ready,
                            "bill_ready": bill_ready,
                            "fills": fills,
                            "bills": bills
                        });
                        // A newer source/cursor clock can coexist with an
                        // in-flight earlier signed nonflat GET. The same
                        // authenticated snapshot must prove FLAT before the
                        // final capture is accepted; retain the 15s bound.
                        if source_ready && fill_ready && bill_ready && signed_flat {
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
                assert_eq!(
                    close_verified["data"]["intent"]["state"], "FILLED",
                    "T09_1_CLOSE_TERMINAL_READBACK_NOT_FILLED: {close_verified}"
                );
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
                // Previous Windows T09.1 intermittently reused the recorded
                // pre-capture clock at opening Status (offset ~-5.1s).
                // The signed raw /public/time reply now represents the time
                // of the pending production protection cancellation. No
                // clock tolerance, SQL state, or risk authority is changed.
                let terminal_cancel_clock = chrono::Utc::now().timestamp_millis().to_string();
                feed.expect_public_clock(envelope(json!([{"ts":terminal_cancel_clock}])));
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
                    // Capture the new authenticated-shaped fee source at the
                    // last deterministic pre-Execute boundary, AFTER prior
                    // census and source preparation. Production fee TTL is
                    // unchanged; any truly stale source must still fail closed.
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
                    eprintln!("T09_2_PHASE_V1:BEFORE_POSITIVE_EXECUTE");
                    // The production Execute future must not be polled inside
                    // the large T09.2 integration task's call stack. Schedule
                    // it independently on the SAME Tokio runtime and retain
                    // the unchanged TradeService, SQL and Worker owners.
                    let execute_trade = trade.clone();
                    let execute_accounts = accounts.clone();
                    let execute_risk = risk.clone();
                    let execute_feed = feed.clone();
                    let execute_budget = rate_budget.clone();
                    let execute_task = tokio::spawn(async move {
                        eprintln!("T09_2_PHASE_V1:EXECUTE_TASK_STARTED");
                        let until = std::time::Instant::now() + Duration::from_secs(10);
                        for attempt in 0..24 {
                            // Explicit NEW exchange-clock observation at the
                            // public Execute boundary. A clock sampled before
                            // the preceding finance/census I/O can be older
                            // than production's unchanged 5 s skew budget.
                            // Never refresh a source merely because a GET
                            // occurred; retries here prove SQL intent absence.
                            let clock_at = chrono::Utc::now().timestamp_millis();
                            execute_feed.advance_recorded_public_clock(
                                serde_json::to_vec(&json!({
                                    "code":"0","msg":"",
                                    "data":[{"ts":clock_at.to_string()}]
                                })).expect("T09.2 fresh clock event raw envelope")
                            );
                            let result = execute_trade.trade("fixture-a", &second, None).await;
                            match result {
                                Ok(result) => {
                                    eprintln!("T09_2_PHASE_V1:EXECUTE_TASK_RETURNED");
                                    return Ok(result);
                                }
                                Err(reason) => {
                                    // A real shared PrivateRestUser RateBudget
                                    // may defer account/config before an intent
                                    // exists. This is NOT permission to replay
                                    // an ambiguous/sent order. Re-enter only
                                    // after proving no SQL intent, no new POST,
                                    // and no new shared monetary reservation.
                                    let remaining = until.saturating_duration_since(
                                        std::time::Instant::now()
                                    );
                                    let Some(delay) = recorded_preview_local_defer(
                                        &reason,
                                        &execute_budget,
                                        remaining,
                                    ) else {
                                        return Err(reason);
                                    };
                                    let owner = execute_accounts
                                        .resolve("fixture-a")
                                        .await
                                        .expect("local defer account owner");
                                    let absent = owner
                                        .execution
                                        .store
                                        .call(
                                            std::time::Instant::now() + Duration::from_secs(3),
                                            |s| Ok(s.by_idempotency(
                                                "t09_real_public_execute_0002"
                                            )?.is_none()),
                                        )
                                        .await
                                        .expect("local defer exact SQL absence");
                                    assert!(absent, "cannot retry after a durable intent exists");
                                    let posts = execute_feed.outbound_posts.lock().unwrap().len();
                                    assert_eq!(
                                        posts, 3,
                                        "rate-deferred execute cannot have a possible venue POST"
                                    );
                                    let mut check = summary(&format!(
                                        "risk_t09_2_local_get_defer_{attempt}"
                                    ));
                                    check.installation_id = "installation_test".into();
                                    let current = execute_risk
                                        .request(check)
                                        .await
                                        .expect("local defer central readback");
                                    let b = current.budget.as_ref().expect("central budget");
                                    let entry = b.entry_budget.as_ref().expect("entry budget");
                                    assert_eq!(
                                        entry.reserved_usd,
                                        if add_mode { "0" } else { "4" },
                                        "local read defer must not hold new entry risk"
                                    );
                                    assert_eq!(
                                        entry.consumed_usd,
                                        if add_mode { "0" } else { "1.0005" },
                                        "local read defer cannot change campaign spent"
                                    );
                                    assert_eq!(
                                        b.reserved_loss_usd,
                                        if add_mode { "0" } else { "2" },
                                        "local read defer cannot mint modeled loss"
                                    );
                                    let wait = delay.max(Duration::from_millis(250));
                                    if attempt == 23 || wait > remaining {
                                        return Err(format!(
                                            "T09_2_LOCAL_PRIVATE_GET_DEFER_DEADLINE:{reason}"
                                        ));
                                    }
                                    eprintln!(
                                        "T09_2_LOCAL_PRIVATE_GET_DEFER_V1:attempt={attempt} delay_ms={} no_intent={absent} posts={posts} entry_reserved={} entry_spent={}",
                                        wait.as_millis(),
                                        entry.reserved_usd,
                                        entry.consumed_usd
                                    );
                                    tokio::time::sleep(wait).await;
                                }
                            }
                        }
                        Err("T09_2_LOCAL_PRIVATE_GET_DEFER_EXHAUSTED".into())
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

                    // The authenticated A create ACK, durable SQL and Worker
                    // readback establish T10-05's next recorded source event.
                    // Both handoffs require the exact old signed bytes: no
                    // route-wide replacement, new fill, or invented inventory.
                    if std::env::var("OKX_T10_05_LOST_ACK_PROBE").as_deref() == Ok("1")
                        || std::env::var("OKX_T10_05_EXPLICIT_REJECT_PROBE").as_deref() == Ok("1")
                    {
                        let _source_lease = resource.recorded_capture_gate.write().await;
                        let retired_order = feed.supersede_queued_signed_source_at_event(
                            "t10-05-a-verified-create-ack",
                            "t10-05-a-live-order",
                            RecordedGet {
                                access_key: "fixture-executor-a".into(),
                                path: "/api/v5/trade/order".into(),
                                required_query: vec![
                                    ("instId".into(), "BTC-USDT-SWAP".into()),
                                    ("clOrdId".into(), persisted.client_order_id.clone()),
                                ],
                                raw_response: envelope(json!([{
                                    "instId": "BTC-USDT-SWAP",
                                    "ordId": "recorded-t09-second-001",
                                    "clOrdId": "__REQUEST_CL_ORD_ID__",
                                    "side": "buy", "posSide": "long", "tdMode": "cross",
                                    "ordType": "limit", "px": "1000", "sz": "0.3",
                                    "accFillSz": "0", "avgPx": "", "state": "live",
                                    "cTime": new_clock, "uTime": new_clock
                                }])),
                            },
                        );
                        assert!(retired_order > 0, "T10-05 A order predecessor absent");
                        let mut scopes = Vec::with_capacity(16);
                        for inst_type in ["SWAP", "FUTURES"] {
                            for kind in [
                                "conditional", "oco", "trigger", "move_order_stop",
                                "chase", "iceberg", "twap", "smart_iceberg",
                            ] {
                                scopes.push(RecordedGet {
                                    access_key: "fixture-observer-a".into(),
                                    path: "/api/v5/trade/orders-algo-pending".into(),
                                    required_query: vec![
                                        ("instType".into(), inst_type.into()),
                                        ("ordType".into(), kind.into()),
                                        ("limit".into(), "100".into()),
                                    ],
                                    raw_response: envelope(json!([])),
                                });
                            }
                        }
                        let retired_algos = feed.publish_post_ack_flat_algo_inventory(
                            "T10_05_A_VERIFIED_CREATE_ACK",
                            "recorded-t09-second-001",
                            scopes,
                        );
                        assert!(retired_algos > 0, "T10-05 A algo predecessor absent");
                    }

                    // Slice B negative regression through the *real* public API:
                    // account B must not resolve, inspect or cancel A's durable
                    // intent, even if the caller knows its exact reference.
                    // The venue boundary and central economic state must remain
                    // unchanged. This is only one prerequisite of T09-MA, not
                    // an acceptance of its multi-strategy/concurrency contract.
                    let other = accounts.resolve("fixture-b").await.unwrap();
                    assert_ne!(
                        other.configuration.account_key.verified_uid,
                        resource.configuration.account_key.verified_uid,
                        "two aliases must not silently share a verified UID"
                    );
                    // T09-MA partial Native proof: actual independent SQL
                    // owners durably register two distinct strategy instances.
                    // They stay disabled in this probe: registering an
                    // instance must never silently authorize automated live
                    // trading, reserve central risk or mint venue POST.
                    let sql_a = resource.execution.store.clone();
                    let sql_b = other.execution.store.clone();
                    let (a_instances, b_instances) = tokio::join!(
                        sql_a.call(std::time::Instant::now() + Duration::from_secs(5), |s| {
                            let instance = okx_execution::StrategyInstance {
                                instance_id: "native_alpha".into(),
                                definition_id: "ema_cross_v1".into(),
                                definition_version: "1".into(),
                                parameters: r#"{"parameters":{"fast":2,"slow":3},"instruments":["BTC-USDT-SWAP"],"timeframe_ms":60000}"#.into(),
                                enabled: false,
                                policy_revision: 1,
                                budget: Decimal::ONE,
                                last_decision_key: None,
                            };
                            s.mutate_instance(
                                "native_ma_register_a_0001",
                                "native_ma_registration_hash_a_0001",
                                "create",
                                &instance,
                                None,
                                chrono::Utc::now().timestamp_millis(),
                            )?;
                            s.instances()
                        }),
                        sql_b.call(std::time::Instant::now() + Duration::from_secs(5), |s| {
                            let instance = okx_execution::StrategyInstance {
                                instance_id: "native_beta".into(),
                                definition_id: "ema_cross_v1".into(),
                                definition_version: "1".into(),
                                parameters: r#"{"parameters":{"fast":3,"slow":5},"instruments":["BTC-USDT-SWAP"],"timeframe_ms":60000}"#.into(),
                                enabled: false,
                                policy_revision: 1,
                                budget: Decimal::ONE,
                                last_decision_key: None,
                            };
                            s.mutate_instance(
                                "native_ma_register_b_0001",
                                "native_ma_registration_hash_b_0001",
                                "create",
                                &instance,
                                None,
                                chrono::Utc::now().timestamp_millis(),
                            )?;
                            s.instances()
                        })
                    );
                    let a_instances = a_instances.expect("A strategy definition committed to A SQL");
                    let b_instances = b_instances.expect("B strategy definition committed to B SQL");
                    assert!(a_instances.iter().any(|i|
                        i.instance_id == "native_alpha" && !i.enabled));
                    assert!(b_instances.iter().any(|i|
                        i.instance_id == "native_beta" && !i.enabled));
                    assert!(!a_instances.iter().any(|i| i.instance_id == "native_beta"),
                        "strategy B must never appear in SQL account A");
                    assert!(!b_instances.iter().any(|i| i.instance_id == "native_alpha"),
                        "strategy A must never appear in SQL account B");

                    // Concurrent read-only public TradeService calls target
                    // their own account SQL, while sharing ONE central
                    // RiskBook. Preflight is explicitly non-authorizing.
                    let (native_a, native_b) = tokio::join!(
                        trade.trade("fixture-a", &TradeRequest::Preflight {}, None),
                        trade.trade("fixture-b", &TradeRequest::Preflight {}, None),
                    );
                    let native_a = native_a.expect("A public preflight must be serviced");
                    let native_b = native_b.expect("B public preflight must be serviced");
                    assert_eq!(
                        native_a["account"],
                        resource.configuration.account_key.wire(),
                        "A preflight must not be rerouted by B account selection"
                    );
                    assert_eq!(
                        native_b["account"],
                        other.configuration.account_key.wire(),
                        "B preflight must own its distinct SQL and verified UID"
                    );
                    assert_eq!(native_a["ready_for_new_risk"], false);
                    assert_eq!(native_b["ready_for_new_risk"], false);
                    assert_eq!(
                        feed.outbound_posts.lock().unwrap().len(),
                        4,
                        "non-authorizing two-account preflights cannot emit exchange POST"
                    );
                    println!(
                        "T09_MA_NATIVE_SQL_STRATEGY_PREFLIGHT_V1:{}",
                        json!({
                            "account_a_uid":resource.configuration.account_key.verified_uid,
                            "account_b_uid":other.configuration.account_key.verified_uid,
                            "a_strategy_registered":true,
                            "b_strategy_registered":true,
                            "a_other_strategy_absent":true,
                            "b_other_strategy_absent":true,
                            "a_strategy_enabled":false,
                            "b_strategy_enabled":false,
                            "a_public_account":native_a["account"],
                            "b_public_account":native_b["account"],
                            "a_new_risk_authorized":native_a["ready_for_new_risk"],
                            "b_new_risk_authorized":native_b["ready_for_new_risk"],
                            "native_outbound_posts":feed.outbound_posts.lock().unwrap().len(),
                        })
                    );
                    let foreign_id = new_intent_id.clone();
                    let before_foreign = other
                        .execution
                        .store
                        .call(std::time::Instant::now() + Duration::from_secs(3), move |s| {
                            Ok((
                                s.intent(&foreign_id)?.is_none(),
                                s.by_idempotency("t09_real_public_execute_0002")?.is_none(),
                            ))
                        })
                        .await
                        .expect("independent B SQL owner must answer");
                    assert_eq!(before_foreign, (true, true), "A obligation leaked into B SQL");
                    let sends_before_foreign = feed.outbound_posts.lock().unwrap().len();
                    let foreign_status = trade
                        .trade(
                            "fixture-b",
                            &TradeRequest::Status {
                                reference: new_intent_id.clone(),
                            },
                            None,
                        )
                        .await;
                    assert_eq!(foreign_status.as_ref().err().map(String::as_str),
                        Some("INTENT_NOT_FOUND"), "B cannot read A's intent by identifier");
                    let foreign_cancel = trade
                        .trade(
                            "fixture-b",
                            &TradeRequest::Cancel {
                                idempotency_key: "t09_cross_uid_cancel_denied_0001".into(),
                                reference: new_intent_id.clone(),
                            },
                            None,
                        )
                        .await;
                    assert_eq!(foreign_cancel.as_ref().err().map(String::as_str),
                        Some("INTENT_NOT_FOUND"), "B cannot cancel A's order");
                    let foreign_id = new_intent_id.clone();
                    let after_foreign = other
                        .execution
                        .store
                        .call(std::time::Instant::now() + Duration::from_secs(3), move |s| {
                            Ok((
                                s.intent(&foreign_id)?.is_none(),
                                s.by_idempotency("t09_real_public_execute_0002")?.is_none(),
                            ))
                        })
                        .await
                        .expect("B SQL readback after denied cross-account requests");
                    assert_eq!(after_foreign, (true, true), "B must remain financially isolated");
                    let sends_after_foreign = feed.outbound_posts.lock().unwrap().len();
                    assert_eq!(sends_after_foreign, sends_before_foreign,
                        "cross-account Status/Cancel must not send an exchange POST");
                    let mut foreign_budget_cmd = summary("risk_t09_2_after_cross_uid_denial");
                    foreign_budget_cmd.installation_id = "installation_test".into();
                    let foreign_budget = risk.request(foreign_budget_cmd).await.unwrap();
                    let original_budget = after.budget.as_ref().unwrap();
                    let cross_budget = foreign_budget.budget.as_ref().unwrap();
                    assert_eq!(
                        cross_budget.entry_budget.as_ref().unwrap().consumed_usd,
                        original_budget.entry_budget.as_ref().unwrap().consumed_usd,
                        "foreign request cannot debit shared entry campaign"
                    );
                    assert_eq!(
                        cross_budget.entry_budget.as_ref().unwrap().reserved_usd,
                        original_budget.entry_budget.as_ref().unwrap().reserved_usd,
                        "foreign request cannot change shared entry reservations"
                    );
                    assert_eq!(cross_budget.reserved_loss_usd, original_budget.reserved_loss_usd,
                        "foreign request cannot change central loss reservation");
                    println!(
                        "T09_MA_CROSS_UID_DENIAL_V1:{}",
                        json!({
                            "owner_uid":resource.configuration.account_key.verified_uid,
                            "foreign_uid":other.configuration.account_key.verified_uid,
                            "owner_intent_id":new_intent_id,
                            "foreign_sql_intent_absent":after_foreign.0,
                            "foreign_sql_idempotency_absent":after_foreign.1,
                            "status_reason":foreign_status.err(),
                            "cancel_reason":foreign_cancel.err(),
                            "outbound_posts_before":sends_before_foreign,
                            "outbound_posts_after":sends_after_foreign,
                            "worker_budget":cross_budget,
                            "risk_book_sequence":foreign_budget.risk_book_sequence,
                        })
                    );

                    // B1 / #449 T09-MA + T10-07 prerequisite: a SECOND
                    // verified UID reaches the public Native TradeService on
                    // the same live, persisted Worker campaign as A's
                    // acknowledged, but not yet financially settled entry.
                    // It requests > remaining shared entry headroom and must
                    // be refused centrally without any B venue POST. This
                    // isolated probe is NOT the full B two-positive-execute,
                    // three-by-four, or RELEASE acceptance.
                    if std::env::var("OKX_B1_NATIVE_CAP_PROBE").as_deref() == Ok("1") {
                        let mut cap_before_cmd = summary("risk_b1_before_b_public_execute");
                        cap_before_cmd.installation_id = "installation_test".into();
                        let cap_before = risk.request(cap_before_cmd).await.unwrap();
                        let before_money = cap_before.budget.as_ref().unwrap();
                        let before_entry = before_money.entry_budget.as_ref().unwrap();
                        assert_eq!(before_entry.consumed_usd, "1.0005");
                        assert_eq!(before_entry.reserved_usd, "7.0015");
                        assert_eq!(before_entry.remaining_usd, "1.998");
                        let posts_before = feed.outbound_posts.lock().unwrap().len();
                        assert_eq!(posts_before, 4);

                        // T09.1's authenticated terminal fill/bill history
                        // remains immutable while A's T09.2 entry is LIVE and
                        // unfilled. B1 has two explicitly distinct causal
                        // windows: B negative source-bound admission and B
                        // positive source-bound admission after its refusal.
                        // A may be captured by six 2-second maintenance
                        // ticks during each 12-second window, plus at most
                        // two direct admission/Status scans. Each scan reads
                        // the five exact instType partitions and one bill
                        // archive. There is NO default empty-page fallback,
                        // extra GET, financial rewrite or production TTL bypass.
                        // T09.1's signed terminal history is now one immutable
                        // event-scoped source for concurrent finance readers.
                        // Re-reading A must not republish fresh pages, timestamps,
                        // financial facts or alter a shared GET queue.
                        let observe_a_finance = |phase: &str| {
                            eprintln!(
                                "B1_A_FINANCIAL_SOURCE_REUSE_V1:phase={phase} source=SIGNED_TERMINAL_HISTORY"
                            );
                        };
                        observe_a_finance("A_PENDING_ACK_BEFORE_B_DENIAL");

                        // B1 causal source boundary: A's signed venue ACK
                        // was persisted before B may request a new central
                        // risk decision. Register a finite post-ACK account
                        // balance observation for the real background owners.
                        // The earlier bootstrap balance must not become an
                        // accidental global GET-count admission gate. This
                        // does not change a production TTL, risk budget, SQL
                        // record, or authorize any venue POST.
                        // The exact A second-entry create ACK and its one
                        // original signed GET are already proven above.
                        // During independent B admission the same production
                        // reconciliation owner can re-read this A order.
                        // Make only that existing A order source available
                        // at the post-ACK B1 phase, not a guessed venue order,
                        // fabricated fill, new POST, or global replay increase.
                        // A's authenticated second create ACK and first signed
                        // live-order read already exist. Additional maintenance
                        // and public Status consumers see the SAME snapshot
                        // until an explicit later venue order-state event.
                        // No new order, fill, timestamp or risk state is minted.
                        let prior_a_order_pages=feed.supersede_queued_signed_source_at_event(
                            "b1-a-second-create-ack","b1-a-second-live-order",
                            RecordedGet {
                                access_key:"fixture-executor-a".into(),
                                path:"/api/v5/trade/order".into(),
                                required_query:vec![
                                    ("instId".into(),"BTC-USDT-SWAP".into()),
                                    ("clOrdId".into(),persisted.client_order_id.clone()),
                                ],
                                raw_response:envelope(json!([{
                                    "instId":"BTC-USDT-SWAP",
                                    "ordId":"recorded-t09-second-001",
                                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                                    "side":"buy","posSide":"long",
                                    "tdMode":"cross","ordType":"limit",
                                    "px":"1000","sz":"0.3",
                                    "accFillSz":"0","avgPx":"",
                                    "state":"live","cTime":new_clock,
                                    "uTime":new_clock
                                }])),
                            }
                        );
                        assert!(prior_a_order_pages>0,
                            "B1 requires the authenticated A order predecessor");
                        // Post-ACK campaign census may reread the signed A
                        // pending-order inventory while B tries admission.
                        // Reflect exactly the already ACK-bound A second order
                        // (ordId, clOrdId, size, state from its original order
                        // GET), not an invented empty inventory or a new send.
                        // Finite phase-scoped pages avoid relying on the
                        // unrelated bootstrap raw 64-repeat ceiling.
                        // The previously observed T10-06 replay is over;
                        // this separate POST ACK permits a NEW pending-order
                        // inventory, never mixed with the prior empty bytes.
                        if std::env::var("OKX_T10_06_CONCURRENT_PROBE").as_deref() == Ok("1") {
                            feed.retire_immutable_signed_phase(
                                "t10-06-post-ack-replay", "fixture-observer-a",
                                "/api/v5/trade/orders-pending",
                            );
                        }
                        for _ in 0..24 {
                            feed.expect_optional_get(RecordedGet {
                                access_key: "fixture-observer-a".into(),
                                path: "/api/v5/trade/orders-pending".into(),
                                required_query: vec![("limit".into(), "100".into())],
                                raw_response: envelope(json!([{
                                    "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                    "ordId":"recorded-t09-second-001",
                                    "clOrdId":persisted.client_order_id.clone(),
                                    "side":"buy","posSide":"long","tdMode":"cross",
                                    "ordType":"limit","px":"1000","sz":"0.3",
                                    "accFillSz":"0","avgPx":"","state":"live",
                                    "cTime":new_clock.clone(),"uTime":new_clock.clone()
                                }])),
                            });
                        }
                        let b1_balance_at = chrono::Utc::now().timestamp_millis().to_string();
                        for (label, equity) in [("a", "100"), ("b", "100")] {
                            for _ in 0..24 {
                                feed.expect_optional_get(RecordedGet {
                                    access_key: format!("fixture-observer-{label}"),
                                    path: "/api/v5/account/balance".into(),
                                    required_query: vec![],
                                    raw_response: envelope(json!([{
                                        "totalEq": equity, "adjEq": equity,
                                        "uTime": b1_balance_at,
                                        "details": [{
                                            "ccy": "USDT", "eq": equity,
                                            "liab": "0", "availEq": equity,
                                            "availBal": equity, "eqUsd": equity,
                                            "uTime": b1_balance_at
                                        }]
                                    }])),
                                });
                            }
                        }
                        refresh_recorded_market();
                        let b_fee_ts = chrono::Utc::now().timestamp_millis().to_string();
                        feed.advance_recorded_route(
                            "fixture-observer-b",
                            "/api/v5/account/trade-fee",
                            (0..4).map(|_| RecordedGet {
                                access_key:"fixture-observer-b".into(),
                                path:"/api/v5/account/trade-fee".into(),
                                required_query:vec![
                                    ("instType".into(),"SWAP".into()),
                                    ("instFamily".into(),"BTC-USDT".into()),
                                ],
                                raw_response:envelope(json!([{
                                    "instType":"SWAP","level":"Lv1","ts":b_fee_ts,
                                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                                }])),
                            }).collect(),
                        );
                        feed.advance_recorded_route(
                            "fixture-observer-b",
                            "/api/v5/account/leverage-info",
                            (0..4).map(|_| RecordedGet {
                                access_key:"fixture-observer-b".into(),
                                path:"/api/v5/account/leverage-info".into(),
                                required_query:vec![
                                    ("instId".into(),"BTC-USDT-SWAP".into()),
                                    ("mgnMode".into(),"cross".into()),
                                ],
                                raw_response:envelope(json!([
                                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                                ])),
                            }).collect(),
                        );
                        let b_clock = chrono::Utc::now().timestamp_millis().to_string();
                        feed.advance_recorded_public_clock(envelope(json!([{"ts":b_clock}])));
                        // Deliberately reuse A's idempotency key: identity is
                        // (AccountKey,action,key), never the key alone.
                        let b_request = TradeRequest::Execute {
                            idempotency_key:"t09_real_public_execute_0002".into(),
                            operation:TradeOperation::Open,
                            strategy_instance:"manual".into(),
                            instrument:"BTC-USDT-SWAP".into(),
                            side:PositionSide::Long,
                            sizing:Sizing::Contracts{contracts:"0.30".into()},
                            order:OrderConstraints {
                                order_type:"limit".into(),
                                limit_price:Some("1000".into()),
                                reference:None,
                                max_entry_usd:Some("10".into()),
                                max_slippage_bps:0,
                            },
                            protection:Some(Protection::Price{
                                stop_price:"900".into()
                            }),
                            expires_in_ms:30_000,
                        };
                        assert!(b_request.valid());
                        let b_trade = trade.clone();
                        let b_accounts = accounts.clone();
                        let b_feed = feed.clone();
                        let b_budget = rate_budget.clone();
                        // Preserve causal no-send proof before a local-only
                        // rate-budget retry; never retry after an SQL intent
                        // exists or a possible venue POST has been observed.
                        let b_task = tokio::spawn(async move {
                            let until = std::time::Instant::now() + Duration::from_secs(12);
                            for attempt in 0..12 {
                                match b_trade.trade("fixture-b",&b_request,None).await {
                                    Ok(result)=>return Ok(result),
                                    Err(reason)=>{
                                        let remaining=until.saturating_duration_since(
                                            std::time::Instant::now());
                                        let Some(delay)=recorded_preview_local_defer(
                                            &reason,&b_budget,remaining
                                        ) else {return Err(reason)};
                                        let b_owner=b_accounts.resolve("fixture-b").await
                                            .expect("B1 independent B SQL owner");
                                        let prior=b_owner.execution.store.call(
                                            std::time::Instant::now()+Duration::from_secs(3),
                                            |s| s.by_idempotency(
                                                "t09_real_public_execute_0002"
                                            )
                                        ).await.expect("B1 durable SQL check before any retry");
                                        if let Some(intent)=prior {
                                            // Never run another Execute after PREPARE, even if
                                            // a private REST read failed locally. This is a
                                            // causal signal that the original Execute must
                                            // resolve the source inside its durable lifetime.
                                            return Err(format!(
                                                "B1_DURABLE_AFTER_LOCAL_READ_DEFER:state={}:revision={}:reason={reason}",
                                                intent.state.as_str(),intent.revision
                                            ));
                                        }
                                        assert_eq!(b_feed.outbound_posts.lock().unwrap().len(),4,
                                            "B1 local read defer cannot transmit venue POST");
                                        if attempt==11 || delay>remaining {
                                            return Err(format!(
                                                "B1_LOCAL_SOURCE_DEFER_DEADLINE:{reason}"
                                            ));
                                        }
                                        tokio::time::sleep(delay.max(Duration::from_millis(200))).await;
                                    }
                                }
                            }
                            Err("B1_LOCAL_SOURCE_DEFER_EXHAUSTED".into())
                        });
                        let denied=b_task.await.expect("B1 B-side production task must join")
                            .expect("B1 requires a structured central risk refusal");
                        assert_eq!(denied["data"]["state"],"REJECTED",
                            "B1 cannot claim an authorized second account order");
                        assert_eq!(denied["data"]["rejection_origin"],"CENTRAL");
                        assert_eq!(denied["data"]["send_state"],"NOT_STARTED");
                        assert_eq!(denied["data"]["primary_reason"],
                            "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
                        let delivered_b=feed.delivered_fees.lock().unwrap().clone();
                        assert!(delivered_b.iter().any(|(key,ts,_)|
                            key=="fixture-observer-b" && ts==&b_fee_ts
                        ),"B1 refusal needs B's own newly signed tariff");
                        let posts_after=feed.outbound_posts.lock().unwrap().len();
                        assert_eq!(posts_after,posts_before,"B1 cannot reach venue POST");
                        assert_eq!(feed.unconsumed_posts(),0);
                        let b_account=accounts.resolve("fixture-b").await.unwrap();
                        assert_ne!(b_account.configuration.account_key.verified_uid,
                            resource.configuration.account_key.verified_uid);
                        let (b_state,b_held,b_quote,b_upper,b_intent)=b_account.execution.store.call(
                            std::time::Instant::now()+Duration::from_secs(3),
                            |s| {
                                let intent=s.by_idempotency("t09_real_public_execute_0002")?
                                    .ok_or(okx_execution::StoreError::Invalid(
                                        "B1_B_ACCOUNT_SQL_INTENT_MISSING"))?;
                                let held=s.active_central_receipt(&intent.intent_id)?.is_some();
                                let quote=s.entry_quote(&intent.intent_id)?
                                    .ok_or(okx_execution::StoreError::Invalid(
                                        "B1_B_ACCOUNT_QUOTE_MISSING"))?;
                                let cost=s.entry_admission(&intent.intent_id)?
                                    .ok_or(okx_execution::StoreError::Invalid(
                                        "B1_B_ACCOUNT_COST_PROOF_MISSING"))?;
                                Ok((intent.state.as_str().to_owned(),held,
                                    quote.total_usd.normalize().to_string(),
                                    cost.entry_upper_usd.normalize().to_string(),
                                    intent.intent_id))
                            }
                        ).await.expect("B1 authentic B SQL readback");
                        assert_eq!(b_state,"REJECTED");
                        assert!(!b_held,"B1 B cannot retain unauthorized reserve");
                        assert_eq!(b_quote,"3.0015");
                        assert_eq!(b_upper,"3.0015");
                        let a_id=persisted.intent_id.clone();
                        assert_ne!(b_intent,a_id,"two SQL owners must not share an intent");
                        let mut cap_after_cmd=summary("risk_b1_after_b_public_execute");
                        cap_after_cmd.installation_id="installation_test".into();
                        let cap_after=risk.request(cap_after_cmd).await.unwrap();
                        let after_money=cap_after.budget.as_ref().unwrap();
                        let after_entry=after_money.entry_budget.as_ref().unwrap();
                        assert_eq!(after_entry.consumed_usd,before_entry.consumed_usd);
                        assert_eq!(after_entry.reserved_usd,before_entry.reserved_usd);
                        assert_eq!(after_entry.remaining_usd,before_entry.remaining_usd);
                        assert_eq!(after_money.reserved_loss_usd,before_money.reserved_loss_usd);
                        println!("B1_NATIVE_SHARED_CAP_DENIAL_V1:{}",json!({
                            "account_a_uid":resource.configuration.account_key.verified_uid,
                            "account_b_uid":b_account.configuration.account_key.verified_uid,
                            "same_idempotency_key":true,
                            "a_intent_id":a_id,
                            "b_intent_id":b_intent,
                            "b_sql_state":b_state,
                            "b_sql_reserve_held":b_held,
                            "b_sql_quote_upper_usd":b_quote,
                            "b_sql_admission_upper_usd":b_upper,
                            "b_signed_fee_ts":b_fee_ts,
                            "denial_reason":denied["data"]["primary_reason"],
                            "denial_origin":denied["data"]["rejection_origin"],
                            "send_state":denied["data"]["send_state"],
                            "outbound_posts_before":posts_before,
                            "outbound_posts_after":posts_after,
                            "before_budget":before_money,
                            "after_budget":after_money,
                            "before_sequence":cap_before.risk_book_sequence,
                            "after_sequence":cap_after.risk_book_sequence
                        }));
                        if std::env::var("OKX_B1_SECOND_POSITIVE_PROBE").as_deref()
                            == Ok("1")
                        {
                            // After proving B's larger request cannot borrow A's
                            // budget, send a NEW, smaller, source-priced public
                            // B Execute on its own SQL owner. It has a distinct
                            // key from B's rejected same-key probe. A and B
                            // share ONE persisted campaign, not two $10 caps.
                            // B1 finite source phase, established AFTER
                            // A's independent pending create ACK and B's
                            // central no-send denial. A's prior terminal
                            // reconciliation proved no live standalone algo;
                            // its newer A order is still unfilled. Do not
                            // invent fills, cancel ACKs or protective stops.
                            //
                            // A's original protection was cancelled and
                            // terminally settled before its new order ACK.
                            // B's independent central DENIED response now
                            // establishes the next signed empty-A inventory
                            // event. One raw snapshot per exact query is
                            // immutable across competing maintenance/Status
                            // reads. No GET-count transition or repeated
                            // fabricated empty pages can make an order safe.
                            let b1_algo_kinds = [
                                "conditional", "oco", "trigger", "move_order_stop",
                                "chase", "iceberg", "twap", "smart_iceberg",
                            ];
                            let b1_empty_algo_source = envelope(json!([]));
                            let mut b1_a_algo_pages =
                                Vec::with_capacity(2 * b1_algo_kinds.len());
                            for inst_type in ["SWAP", "FUTURES"] {
                                for kind in b1_algo_kinds {
                                    b1_a_algo_pages.push(RecordedGet {
                                        access_key: "fixture-observer-a".into(),
                                        path: "/api/v5/trade/orders-algo-pending".into(),
                                        required_query: vec![
                                            ("instType".into(), inst_type.into()),
                                            ("ordType".into(), kind.into()),
                                            ("limit".into(), "100".into()),
                                        ],
                                        raw_response: b1_empty_algo_source.clone(),
                                    });
                                }
                            }
                            let retired_b1_a_algo_pages =
                                feed.publish_post_ack_flat_algo_inventory(
                                    "A_ACK_AND_B_CENTRAL_NO_SEND",
                                    "recorded-t09-second-001",
                                    b1_a_algo_pages,
                                );
                            println!("B1_A_ALGO_PHASE_V1:{}", json!({
                                "boundary": "A_PENDING_ACK_AND_B_CENTRAL_NO_SEND",
                                "scopes": 16,
                                "immutable_source_per_exact_query": true,
                                "max_reads_per_scope": 32,
                                "retired_prior_source_pages": retired_b1_a_algo_pages,
                                "outbound_posts": feed.outbound_posts.lock().unwrap().len(),
                            }));
                            // This is a NEW admitted B source cycle, not
                            // a license for indefinite A history reads.
                            // The same immutable A finance history remains
                            // authoritative; only observation slots change.
                            observe_a_finance("B_POSITIVE_AFTER_CENTRAL_NO_SEND");
                            refresh_recorded_market();
                            let b_positive_ts = chrono::Utc::now().timestamp_millis().to_string();
                            feed.advance_recorded_route(
                                "fixture-observer-b",
                                "/api/v5/account/trade-fee",
                                (0..4).map(|_| RecordedGet {
                                    access_key: "fixture-observer-b".into(),
                                    path: "/api/v5/account/trade-fee".into(),
                                    required_query: vec![
                                        ("instType".into(), "SWAP".into()),
                                        ("instFamily".into(), "BTC-USDT".into()),
                                    ],
                                    raw_response: envelope(json!([{
                                        "instType": "SWAP", "level": "Lv1", "ts": b_positive_ts,
                                        "feeGroup": [{"groupId": "4", "maker": "0.0005", "taker": "0.0005"}]
                                    }])),
                                }).collect(),
                            );
                            feed.advance_recorded_route(
                                "fixture-observer-b",
                                "/api/v5/account/leverage-info",
                                (0..4).map(|_| RecordedGet {
                                    access_key: "fixture-observer-b".into(),
                                    path: "/api/v5/account/leverage-info".into(),
                                    required_query: vec![
                                        ("instId".into(), "BTC-USDT-SWAP".into()),
                                        ("mgnMode".into(), "cross".into()),
                                    ],
                                    raw_response: envelope(json!([
                                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "long", "lever": "2"},
                                        {"instId": "BTC-USDT-SWAP", "mgnMode": "cross", "posSide": "short", "lever": "2"}
                                    ])),
                                }).collect(),
                            );
                            let b_positive_clock = chrono::Utc::now().timestamp_millis().to_string();
                            feed.advance_recorded_public_clock(
                                envelope(json!([{"ts": b_positive_clock}]))
                            );
                            feed.expect_post(RecordedPost {
                                access_key: "fixture-executor-b".into(),
                                path: "/api/v5/trade/order".into(),
                                expected_body: json!({
                                    "instId": "BTC-USDT-SWAP", "tdMode": "cross",
                                    "clOrdId": "__REQUEST_CL_ORD_ID__",
                                    "side": "buy", "posSide": "long", "ordType": "limit",
                                    "sz": "0.1", "px": "1000",
                                    "attachAlgoOrds": [{
                                        "attachAlgoClOrdId": "__REQUEST_ATTACH_ALGO_ID__",
                                        "tpTriggerPxType": "mark", "slTriggerPx": "900",
                                        "slTriggerPxType": "mark", "slOrdPx": "-1"
                                    }]
                                }),
                                raw_response: br#"{"code":"0","msg":"","data":[{"ordId":"recorded-b1-b-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
                            });
                            // First B executor order read on this
                            // account: register bounded, optional raw venue
                            // GETs rather than advancing an unregistered
                            // phase. ACK comes only from the exact POST below.
                            for _ in 0..2 {
                                feed.expect_optional_get(RecordedGet {
                                    access_key: "fixture-executor-b".into(),
                                    path: "/api/v5/trade/order".into(),
                                    required_query: vec![("instId".into(), "BTC-USDT-SWAP".into())],
                                    raw_response: envelope(json!([{
                                        "instId": "BTC-USDT-SWAP", "ordId": "recorded-b1-b-001",
                                        "clOrdId": "__REQUEST_CL_ORD_ID__",
                                        "side": "buy", "posSide": "long",
                                        "tdMode": "cross", "ordType": "limit", "px": "1000",
                                        "sz": "0.1", "accFillSz": "0", "avgPx": "",
                                        "state": "live", "cTime": b_positive_clock,
                                        "uTime": b_positive_clock
                                    }])),
                                });
                            }
                            let b_positive_request = TradeRequest::Execute {
                                idempotency_key: "b1_b_positive_after_shared_denial_001".into(),
                                operation: TradeOperation::Open,
                                strategy_instance: "manual".into(),
                                instrument: "BTC-USDT-SWAP".into(),
                                side: PositionSide::Long,
                                sizing: Sizing::Contracts { contracts: "0.10".into() },
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
                            assert!(b_positive_request.valid());
                            // Snapshot the transport trace before B's second positive
                            // Execute. No new pages or changed risk policy.
                            let b_positive_read_cursor = feed.requests.lock().unwrap().len();
                            let ma_probe = std::env::var("OKX_T09_MA_CONCURRENT_PROBE")
                                .as_deref() == Ok("1");
                            // A independent read and B NEW Execute enter the same
                            // public TradeService at a causally shared barrier.
                            // They are two distinct authenticated Native SQL
                            // owners with one persisted Worker RiskBook. An A
                            // query must never be routed to B's new intent.
                            let ma_barrier = ma_probe.then(|| Arc::new(
                                tokio::sync::Barrier::new(2)
                            ));
                            let ma_status_task = ma_barrier.as_ref().map(|barrier| {
                                let gate = barrier.clone();
                                let public_a = trade.clone();
                                let a_intent = new_intent_id.clone();
                                tokio::spawn(async move {
                                    gate.wait().await;
                                    public_a.trade(
                                        "fixture-a",
                                        &TradeRequest::Status { reference: a_intent },
                                        None,
                                    ).await
                                })
                            });
                            let b_gate = ma_barrier.clone();
                            let b_public = trade.clone();
                            let b_source = feed.clone();
                            let b_accounts_positive = accounts.clone();
                            let b_rate = rate_budget.clone();
                            let b_positive_task = tokio::spawn(async move {
                                if let Some(barrier) = b_gate { barrier.wait().await; }
                                let until = std::time::Instant::now() + Duration::from_secs(12);
                                let mut b_positive_request = b_positive_request;
                                for attempt in 0..12 {
                                    let key = match &b_positive_request {
                                        TradeRequest::Execute { idempotency_key, .. } =>
                                            idempotency_key.clone(),
                                        _ => unreachable!("B1 only sends a valid Execute"),
                                    };
                                    match b_public.trade("fixture-b", &b_positive_request, None).await {
                                        Ok(outcome) => {
                                            let primary = outcome["data"]["primary_reason"]
                                                .as_str().unwrap_or("");
                                            let remaining = until.saturating_duration_since(
                                                std::time::Instant::now()
                                            );
                                            let definitive_local_unsent =
                                                outcome["status"] == "completed"
                                                && outcome["data"]["state"] == "REJECTED"
                                                && outcome["data"]["rejection_origin"] == "LOCAL"
                                                && outcome["data"]["send_state"] == "NOT_STARTED"
                                                && matches!(
                                                    outcome["data"]["risk_release_state"].as_str(),
                                                    Some("RELEASED" | "NOT_RESERVED")
                                                );
                                            let delay = definitive_local_unsent.then(|| {
                                                recorded_preview_local_defer(
                                                    primary, &b_rate, remaining
                                                )
                                            }).flatten();
                                            let Some(delay) = delay else {
                                                return Ok((outcome, key));
                                            };
                                            // Reissuing a new explicit key is legal ONLY
                                            // after this original key is durably REJECTED,
                                            // its central receipt is no longer active, and
                                            // no venue POST has been attempted. Never retry
                                            // UNKNOWN, possibly sent, or unreleased risk.
                                            let owner = b_accounts_positive.resolve("fixture-b")
                                                .await.expect("B1 rate-rejected B owner");
                                            let sql_key = key.clone();
                                            let (state, still_held, before_reserve) = owner.execution.store.call(
                                                std::time::Instant::now() + Duration::from_secs(3),
                                                move |s| {
                                                    let intent = s.by_idempotency(&sql_key)?
                                                        .ok_or(okx_execution::StoreError::Invalid(
                                                            "B1_RATE_REJECTED_SQL_MISSING"
                                                        ))?;
                                                    Ok((intent.state.as_str().to_owned(),
                                                        s.active_central_receipt(
                                                            &intent.intent_id
                                                        )?.is_some(),
                                                        s.unsent_before_reserve(&intent.intent_id)?))
                                                }
                                            ).await.expect("B1 durable local no-send release proof");
                                            assert_eq!(state, "REJECTED");
                                            assert!(!still_held, "B1 cannot retry a held B risk");
                                            if outcome["data"]["risk_release_state"] == "NOT_RESERVED" {
                                                assert!(before_reserve,
                                                    "B1 must prove no Worker RESERVE attempt before retry");
                                            }
                                            assert_eq!(
                                                b_source.outbound_posts.lock().unwrap().len(), 4,
                                                "B1 local defer cannot replay a venue POST"
                                            );
                                            if attempt == 11 || delay > remaining {
                                                return Ok((outcome, key));
                                            }
                                            eprintln!(
                                                "B1_B_LOCAL_NO_SEND_RATE_RELEASED_V1:attempt={attempt} delay_ms={} sql_state={state} held={still_held} venue_posts=4",
                                                delay.as_millis()
                                            );
                                            tokio::time::sleep(
                                                delay.max(Duration::from_millis(200))
                                            ).await;
                                            // Fresh real-API-shaped recorded responses at
                                            // this proven local no-send retry boundary.
                                            // Rates, leverage and policy stay unchanged.
                                            let observed_at =
                                                chrono::Utc::now().timestamp_millis().to_string();
                                            b_source.advance_recorded_route(
                                                "fixture-observer-b",
                                                "/api/v5/account/trade-fee",
                                                (0..4).map(|_| RecordedGet {
                                                    access_key:"fixture-observer-b".into(),
                                                    path:"/api/v5/account/trade-fee".into(),
                                                    required_query:vec![
                                                        ("instType".into(),"SWAP".into()),
                                                        ("instFamily".into(),"BTC-USDT".into()),
                                                    ],
                                                    raw_response:envelope(json!([{
                                                        "instType":"SWAP","level":"Lv1",
                                                        "ts":observed_at,
                                                        "feeGroup":[{
                                                            "groupId":"4","maker":"0.0005",
                                                            "taker":"0.0005"
                                                        }]
                                                    }])),
                                                }).collect(),
                                            );
                                            b_source.advance_recorded_route(
                                                "fixture-observer-b",
                                                "/api/v5/account/leverage-info",
                                                (0..4).map(|_| RecordedGet {
                                                    access_key:"fixture-observer-b".into(),
                                                    path:"/api/v5/account/leverage-info".into(),
                                                    required_query:vec![
                                                        ("instId".into(),"BTC-USDT-SWAP".into()),
                                                        ("mgnMode".into(),"cross".into()),
                                                    ],
                                                    raw_response:envelope(json!([
                                                        {"instId":"BTC-USDT-SWAP","mgnMode":"cross",
                                                         "posSide":"long","lever":"2"},
                                                        {"instId":"BTC-USDT-SWAP","mgnMode":"cross",
                                                         "posSide":"short","lever":"2"}
                                                    ])),
                                                }).collect(),
                                            );
                                            b_source.advance_recorded_public_clock(
                                                envelope(json!([{"ts":observed_at}]))
                                            );
                                            if let TradeRequest::Execute {
                                                idempotency_key, ..
                                            } = &mut b_positive_request {
                                                *idempotency_key = format!(
                                                    "b1_b_positive_after_shared_denial_001_rate_{attempt}"
                                                );
                                            }
                                        }
                                        Err(reason) => {
                                            let remaining = until.saturating_duration_since(
                                                std::time::Instant::now()
                                            );
                                            let Some(delay) = recorded_preview_local_defer(
                                                &reason, &b_rate, remaining
                                            ) else { return Err(reason) };
                                            let owner = b_accounts_positive.resolve("fixture-b")
                                                .await.expect("B1 B positive SQL owner");
                                            let sql_key = key.clone();
                                            let original = owner.execution.store.call(
                                                std::time::Instant::now() + Duration::from_secs(3),
                                                move |s| s.by_idempotency(&sql_key)
                                            ).await.expect("B1 positive SQL must be read before retry");
                                            if let Some(intent)=original {
                                                return Err(format!(
                                                    "B1_DURABLE_POSITIVE_AFTER_LOCAL_READ_DEFER:state={}:revision={}:reason={reason}",
                                                    intent.state.as_str(),intent.revision
                                                ));
                                            }
                                            assert_eq!(
                                                b_source.outbound_posts.lock().unwrap().len(), 4,
                                                "local defer cannot replay a B venue POST"
                                            );
                                            if attempt == 11 || delay > remaining {
                                                return Err(format!(
                                                    "B1_POSITIVE_PRIVATE_READ_DEADLINE:{reason}"
                                                ));
                                            }
                                            tokio::time::sleep(
                                                delay.max(Duration::from_millis(200))
                                            ).await;
                                        }
                                    }
                                }
                                Err("B1_POSITIVE_PRIVATE_READ_EXHAUSTED".into())
                            });
                            let b_positive_result = b_positive_task.await
                                .expect("B1 Native B positive task join");
                            let ma_a_status = if let Some(task) = ma_status_task {
                                Some(task.await.expect("T09-MA A Status task must join"))
                            } else { None };
                            if let Err(ref reason) = b_positive_result {
                                let requests = feed.requests.lock().unwrap();
                                let a_algo_prefix =
                                    "fixture-observer-a:/api/v5/trade/orders-algo-pending?";
                                let mut scopes = std::collections::BTreeMap::<String, usize>::new();
                                let mut total = 0_usize;
                                let mut phase_reads = 0_usize;
                                for (index, route) in requests.iter().enumerate() {
                                    if !route.starts_with(a_algo_prefix) {
                                        continue;
                                    }
                                    total += 1;
                                    if index >= b_positive_read_cursor {
                                        phase_reads += 1;
                                    }
                                    let inst_type = if route.contains("instType=SWAP") {
                                        "SWAP"
                                    } else if route.contains("instType=FUTURES") {
                                        "FUTURES"
                                    } else {
                                        "UNKNOWN"
                                    };
                                    let kind = [
                                        "conditional", "oco", "trigger", "move_order_stop",
                                        "chase", "iceberg", "twap", "smart_iceberg",
                                    ].into_iter().find(|kind| {
                                        route.contains(&format!("ordType={kind}"))
                                    }).unwrap_or("UNKNOWN");
                                    *scopes.entry(format!("{inst_type}:{kind}")).or_default() += 1;
                                }
                                let reason_code = if reason.contains(
                                    "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED"
                                ) {
                                    "TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED"
                                } else if reason.contains("TRANSCRIPT_REPEAT_EXCEEDED") {
                                    "TRANSCRIPT_REPEAT_EXCEEDED"
                                } else if reason.contains("rate/backpressure defer") {
                                    "LOCAL_RATE_DEFER"
                                } else {
                                    "OTHER_B1_EXECUTE_FAILURE"
                                };
                                println!("B1_POSITIVE_TRANSPORT_FAILURE_V1:{}", json!({
                                    "phase": "B_POSITIVE_AFTER_A_ACK",
                                    "reason_code": reason_code,
                                    "source_actor": "fixture-observer-a",
                                    "source_route": "/api/v5/trade/orders-algo-pending",
                                    "a_algo_success_reads_total": total,
                                    "a_algo_success_reads_since_positive": phase_reads,
                                    "a_algo_scopes": scopes,
                                    "outbound_posts": feed.outbound_posts.lock().unwrap().len(),
                                }));
                            }
                            let (b_positive, b_positive_accepted_key) = b_positive_result
                                .expect("B1 real B venue create must be admitted");
                            if b_positive["status"] != "partial" {
                                // First actual production denial is authoritative.
                                // Only redact-safe typed fields are emitted;
                                // no signed HTTP bytes, keys or bearer tokens.
                                println!("B1_POSITIVE_DIAGNOSTIC_V1:{}", json!({
                                    "status": b_positive["status"],
                                    "state": b_positive["data"]["state"],
                                    "primary_reason": b_positive["data"]["primary_reason"],
                                    "rejection_origin": b_positive["data"]["rejection_origin"],
                                    "send_state": b_positive["data"]["send_state"],
                                    "as_of_ms": b_positive["as_of_ms"],
                                    "outbound_posts": feed.outbound_posts.lock().unwrap().len(),
                                    "denied_routes": feed.denied_routes.lock().unwrap()
                                        .iter().rev().take(6).cloned().collect::<Vec<_>>(),
                                    "recorded_balance_phase_ms": b1_balance_at,
                                }));
                            }
                            assert_eq!(b_positive["status"], "partial");
                            assert_eq!(b_positive["data"]["intent"]["state"], "ACKNOWLEDGED");
                            assert_eq!(b_positive["data"]["intent"]["exchange_order_id"],
                                "recorded-b1-b-001");
                            assert_eq!(b_positive["data"]["financial_complete"], false);
                            let posts_b = feed.outbound_posts.lock().unwrap().len();
                            assert_eq!(posts_b, 5, "exactly ONE independent B create POST");
                            assert_eq!(feed.unconsumed_posts(), 0);
                            let (state_b, held_b, upper_b, order_b, intent_b, b_strategy) =
                                b_account.execution.store.call(
                                    std::time::Instant::now() + Duration::from_secs(3),
                                    move |s| {
                                        let intent = s.by_idempotency(
                                            &b_positive_accepted_key
                                        )?.ok_or(okx_execution::StoreError::Invalid(
                                            "B1_POSITIVE_SQL_INTENT_MISSING"
                                        ))?;
                                        let held = s.active_central_receipt(&intent.intent_id)?.is_some();
                                        let quote = s.entry_quote(&intent.intent_id)?
                                            .ok_or(okx_execution::StoreError::Invalid(
                                                "B1_POSITIVE_SQL_ENTRY_QUOTE_MISSING"
                                            ))?;
                                        Ok((intent.state.as_str().to_owned(), held,
                                            quote.total_usd.normalize().to_string(),
                                            intent.exchange_order_id.clone(),
                                            intent.intent_id,
                                            intent.plan.strategy_instance))
                                    }
                                ).await.expect("B1 persisted B positive SQL");
                            assert_eq!(state_b, "ACKNOWLEDGED");
                            assert!(held_b, "B1 B position must retain its true central risk");
                            assert_eq!(upper_b, "1.0005");
                            assert_eq!(order_b.as_deref(), Some("recorded-b1-b-001"));
                            assert_ne!(intent_b, persisted.intent_id);
                            let mut b_after_cmd = summary("risk_b1_after_positive_b_account");
                            b_after_cmd.installation_id = "installation_test".into();
                            let b_after = risk.request(b_after_cmd).await.unwrap();
                            let b_after_budget = b_after.budget.as_ref().unwrap();
                            let b_after_entry = b_after_budget.entry_budget.as_ref().unwrap();
                            assert_eq!(b_after_entry.consumed_usd, "1.0005");
                            assert_eq!(b_after_entry.reserved_usd, "8.002");
                            assert_eq!(b_after_entry.remaining_usd, "0.9975");
                            if let Some(observed) = ma_a_status {
                                let source_a = observed.expect(
                                    "T09-MA A must service its concurrent read"
                                );
                                assert_eq!(source_a["data"]["intent"]["intent_id"],
                                    new_intent_id, "A Status cannot see B's intent");
                                assert_eq!(source_a["data"]["intent"]["state"],
                                    "ACKNOWLEDGED", "A order must remain owned and unsettled");
                                assert_eq!(feed.outbound_posts.lock().unwrap().len(), 5,
                                    "A Status must not create a second venue POST");
                                println!("T09_MA_CONCURRENT_PUBLIC_V1:{}", json!({
                                    "schema":"okx.t09-ma-public-native-concurrency/v1",
                                    "a_intent_id":new_intent_id,
                                    "b_intent_id":intent_b,
                                    "a_account_uid":resource.configuration.account_key.verified_uid,
                                    "b_account_uid":b_account.configuration.account_key.verified_uid,
                                    "a_public_status":source_a["status"],
                                    "a_sql_state":source_a["data"]["intent"]["state"],
                                    "b_sql_state":state_b,
                                    "native_public_barrier_participants":2,
                                    "same_instrument":"BTC-USDT-SWAP",
                                    "a_strategy_instance":persisted.plan.strategy_instance,
                                    "b_strategy_instance":b_strategy,
                                    "a_scoped_strategy":format!("{}:{}", resource.configuration.account_key.wire(), persisted.plan.strategy_instance),
                                    "b_scoped_strategy":format!("{}:{}", b_account.configuration.account_key.wire(), b_strategy),
                                    "outbound_posts_after":5,
                                    "worker_shared_cap_usd":b_after_budget.campaign_limit_usd,
                                    "worker_reserved_usd":b_after_entry.reserved_usd,
                                    "worker_consumed_usd":b_after_entry.consumed_usd,
                                    "worker_sequence":b_after.risk_book_sequence,
                                }));
                            }
                            println!("B1_NATIVE_SECOND_ACCOUNT_ACCEPTED_V1:{}", json!({
                                "account_a_uid": resource.configuration.account_key.verified_uid,
                                "account_b_uid": b_account.configuration.account_key.verified_uid,
                                "b_intent_id": intent_b,
                                "b_sql_state": state_b,
                                "b_sql_reserve_held": held_b,
                                "b_entry_quote_usd": upper_b,
                                "b_exchange_order_id": order_b,
                                "outbound_posts_before": posts_after,
                                "outbound_posts_after": posts_b,
                                "before_budget": after_money,
                                "after_budget": b_after_budget,
                                "risk_book_sequence": b_after.risk_book_sequence,
                            }));
                            if std::env::var("OKX_T10_14_NATIVE_PROBE").as_deref() == Ok("1") {
                                let client_b = b_positive["data"]["intent"]["client_order_id"]
                                    .as_str().expect("T10-14 B client ID").to_owned();
                                let isolation = t10_14_b_public_cancel_while_a_sql_held(
                                    accounts.clone(), trade.clone(), risk.clone(), feed.clone(),
                                    intent_b.clone(), client_b.clone(),
                                ).await;
                                println!("T10_14_NATIVE_ISOLATION_V1:{isolation}");
                                if std::env::var("OKX_T10_09_LATE_FILL_PROBE").as_deref()==Ok("1") {
                                    let late = t10_09_b_signed_late_fill_after_cancel(
                                        accounts.clone(), trade.clone(), risk.clone(), feed.clone(),
                                        intent_b.clone(), client_b,
                                    ).await;
                                    println!("T10_09_NATIVE_LATE_B_FILL_V1:{late}");
                                    refresh_recorded_market();
                                    let reduce=t10_09_b_public_partial_reduce(
                                        accounts.clone(),trade.clone(),feed.clone(),
                                        intent_b.clone(),
                                    ).await;
                                    println!("T10_09_NATIVE_PARTIAL_REDUCE_V1:{reduce}");
                                    let reduction=t10_09_b_signed_partial_reduce_finance(
                                        accounts.clone(),trade.clone(),feed.clone(),
                                        &reduce,
                                        late["signed_late_at_ms"].as_i64().unwrap(),
                                    ).await;
                                    println!("T10_09_NATIVE_REDUCE_FINANCE_V1:{reduction}");
                                    let terminal=t10_09_b_terminal_after_partial(
                                        (accounts.clone(),trade.clone(),risk.clone(),
                                         rate_budget.clone()),feed.clone(),intent_b.clone(),
                                        late["signed_late_at_ms"].as_i64().unwrap(),
                                        reduction["signed_reduce_at_ms"].as_i64().unwrap(),
                                        new_clock.parse::<i64>()
                                            .expect("T10-09 original signed A create time"),
                                        &observe_a_finance,
                                    ).await;
                                    println!("T10_09_NATIVE_TERMINAL_V1:{terminal}");
                                }
                            }
                        }
                    }
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
                    if std::env::var("OKX_T10_01_PARTIAL_SOURCE_RECOVERY_PROBE").as_deref()==Ok("1") {
                        refresh_recorded_market();
                        let recovery=t10_01_partial_balance_denial_recovery(
                            accounts.clone(),trade.clone(),risk.clone(),
                            feed.clone(),rate_budget.clone(),&refresh_recorded_market
                        ).await;
                        println!("T10_01_NATIVE_SOURCE_RECOVERY_V1:{recovery}");
                    }
                    if std::env::var("OKX_T10_03_POST_AUTH_REPLY_LOST_PROBE").as_deref()==Ok("1") {
                        refresh_recorded_market();
                        let lost=t10_03_worker_committed_post_reply_lost(
                            accounts.clone(),trade.clone(),risk.clone(),feed.clone()
                        ).await;
                        println!("T10_03_NATIVE_POST_AUTH_REPLY_LOST_V1:{lost}");
                    }
                    if std::env::var("OKX_T10_15_POST_HELD_SOURCE_PROBE").as_deref()==Ok("1") {
                        refresh_recorded_market();
                        let switched=t10_15_fee_source_after_worker_held(
                            accounts.clone(),trade.clone(),risk.clone(),
                            feed.clone(),rate_budget.clone(),
                            &refresh_recorded_market
                        ).await;
                        println!("T10_15_NATIVE_CAUSAL_SOURCE_V1:{switched}");
                    }
                    if std::env::var("OKX_T10_05_LOST_ACK_PROBE").as_deref()==Ok("1")
                        || std::env::var("OKX_T10_05_EXPLICIT_REJECT_PROBE").as_deref()==Ok("1") {
                        refresh_recorded_market();
                        let mode=std::env::var("OKX_T10_05_LOST_ACK_PROBE").as_deref()==Ok("1");
                        let witness=t10_05_create_outcome(
                            accounts.clone(),trade.clone(),risk.clone(),feed.clone(),rate_budget.clone(),mode
                        ).await;
                        println!("T10_05_NATIVE_POST_OUTCOME_V1:{witness}");
                    }
                    if spec_mode {
                        let proved=t09_spec_second_linear_product(
                            accounts.clone(),trade.clone(),risk.clone(),
                            public_ws.clone(),connector.clone(),feed.clone(),
                            rate_budget.clone()
                        ).await;
                        println!("T09_SPEC_NATIVE_SECOND_LINEAR_V1:{proved}");
                    }
                    if std::env::var("OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE").as_deref()==Ok("1") {
                        // Financially clean A order+Worker readback already
                        // exist. Trigger only the next independent B RESERVE,
                        // never the T09 baseline authorization.
                        refresh_recorded_market();
                        let refused=t10_04_foreign_worker_result(
                            accounts.clone(),trade.clone(),risk.clone(),feed.clone()
                        ).await;
                        println!("T10_04_NATIVE_FOREIGN_WORKER_REPLY_V1:{refused}");
                    }
                    let mut captured_signed_times=None;
                    if std::env::var("OKX_T09_3_PARTIAL_PROBE").as_deref() == Ok("1") {
                        let (first, ts) = tokio::spawn(t09_3_phase(
                            accounts.clone(),trade.clone(),feed.clone(),risk.clone(),
                            new_intent_id.clone(),persisted.client_order_id.clone(),None,
                        )).await.expect("T09.3 partial native task");
                        let t10_08_failure = if std::env::var(
                            "OKX_T10_08_PROTECTION_FAILURE_PROBE"
                        ).as_deref() == Ok("1") {
                            let failed=t10_08_failed_stop_before_late_fill(
                                accounts.clone(),trade.clone(),risk.clone(),feed.clone(),
                                new_intent_id.clone(),persisted.client_order_id.clone(),
                                first.clone(),
                            ).await;
                            println!("T10_08_NATIVE_PROTECTION_FAILURE_V1:{failed}");
                            Some(failed)
                        } else {None};
                        let (terminal, _) = tokio::spawn(t09_3_phase(
                            accounts.clone(),trade.clone(),feed.clone(),risk.clone(),
                            new_intent_id.clone(),persisted.client_order_id.clone(),Some(ts),
                        )).await.expect("T09.3 terminal native task");
                        captured_signed_times=Some((first["raw_history_event_at_ms"].as_i64().unwrap(),terminal["raw_history_event_at_ms"].as_i64().unwrap()));
                        let t10_08_recovery = if t10_08_failure.is_some() {
                            let recovered=t10_08_recover_signed_flat_after_late_fill(
                                (accounts.clone(),trade.clone(),risk.clone(),feed.clone()),
                                rate_budget.clone(),new_intent_id.clone(),
                                persisted.client_order_id.clone(),
                                first["raw_history_event_at_ms"].as_i64().unwrap(),
                                &checked.budget.as_ref().unwrap()
                                    .entry_budget.as_ref().unwrap().reserved_usd,
                                &terminal
                            ).await;
                            println!("T10_08_NATIVE_FLAT_RECOVERY_V1:{recovered}");
                            Some(recovered)
                        } else {None};
                        // The terminal signed fill+bill may reach SQL before
                        // a fresh account-wide financial capture can commit
                        // its central SETTLE_ENTRY. Earlier checks incorrectly
                        // allowed Native to exit with the *partial* Worker
                        // cost still reserved. Continue the same PUBLIC Status
                        // reconciliation, with no test-owned financial writes,
                        // until the persisted Worker proves the terminal debit.
                        let mut last_status = terminal["entry_fill_evidence_state"].clone();
                        let mut final_settlement_attempts = 0u32;
                        let after = loop {
                            let mut summary_cmd = summary(&format!(
                                "risk_t09_3_terminal_money_{final_settlement_attempts}"
                            ));
                            summary_cmd.installation_id = "installation_test".into();
                            let observed = risk.request(summary_cmd).await.unwrap();
                            let entry = observed.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
                            if entry.consumed_usd == "4.002"
                                && entry.reserved_usd == if add_mode { "0" } else { "4" }
                            {
                                break observed;
                            }
                            assert!(
                                final_settlement_attempts < 3,
                                "T09.3 terminal SQL fills/bills not settled by actual public Status: spent={} held={} status={last_status}",
                                entry.consumed_usd,
                                entry.reserved_usd
                            );
                            assert_eq!(
                                feed.outbound_posts.lock().unwrap().len(),
                                if t10_08_recovery.is_some() { 6 } else { 4 },
                                "T09.3/T10-08 monetary catch-up must never resubmit a venue create"
                            );
                            final_settlement_attempts += 1;
                            tokio::time::sleep(Duration::from_millis(250)).await;
                            let settled = trade
                                .trade(
                                    "fixture-a",
                                    &TradeRequest::Status {
                                        reference: new_intent_id.clone(),
                                    },
                                    None,
                                )
                                .await
                                .expect("terminal settlement requires the same real TradeService Status");
                            last_status = json!({
                                "status": settled["status"],
                                "incremental": settled["data"]["incremental_entry_settlement"],
                                "fill": settled["data"]["entry_fill_evidence"]["state"],
                                "financial_complete": settled["data"]["financial_complete"],
                            });
                        };
                        println!(
                            "T09_3_TERMINAL_SETTLEMENT_RECONCILED_V1:{}",
                            json!({
                                "extra_status_checks":final_settlement_attempts,
                                "worker_risk_book_sequence":after.risk_book_sequence,
                                "consumed_usd":after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().consumed_usd,
                                "reserved_usd":after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
                                "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
                            })
                        );
                        if std::env::var("OKX_T10_10_FINANCE_REPLAY_PROBE").as_deref()==Ok("1") {
                            let repeated=t10_10_signed_duplicate_ingestion(
                                accounts.clone(),trade.clone(),risk.clone(),feed.clone(),
                                new_intent_id.clone(),idempotent_intent_id.clone(),financial_time.clone(),
                            ).await;
                            println!("T10_10_NATIVE_SIGNED_REPLAY_V1:{repeated}");
                        }
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
                            rate_budget: rate_budget.clone(),
                            feed: feed.clone(),
                            prior_client_id: persisted.client_order_id.clone(),
                            campaign_id: campaign_id.clone(),
                            native_origin,
                            first_fill_at_ms: first_event_ms,
                            terminal_fill_at_ms: last_event_ms,
                            need_new_market: frontier_tx,
                            fresh_market_ready: market_rx,
                        }));
                        if frontier_rx.await.is_err() {
                            // A closed readiness channel means the child
                            // terminated BEFORE asking for a market update.
                            // Join it to preserve the original panic/cause;
                            // reporting RecvError alone masks the first failure.
                            let child = task.await;
                            panic!("T09_4_CHILD_BEFORE_MARKET_HANDOFF:{child:?}");
                        }
                        refresh_recorded_market();
                        market_tx.send(()).expect("T09.4 signed market reply handoff failed");
                        let add=task.await.expect("T09.4 managed Add native task");
                        println!("T09_4_NATIVE_ADD_BOUND_V1:{add}");
                        if std::env::var("OKX_T10_02_EXTERNAL_EXPOSURE_PROBE").as_deref()==Ok("1") {
                            // The positive managed-Add settlement can wait for
                            // finance readbacks. Publish an explicit fresh raw
                            // public market frame before the negative Execute;
                            // source expiry must not hide the true foreign lot.
                            refresh_recorded_market();
                            let denied=t10_02_unowned_exposure_denied(
                                accounts.clone(),trade.clone(),risk.clone(),feed.clone(),
                                add["intent_id"].as_str().expect("T10-02 managed ID").to_owned()
                            ).await;
                            println!("T10_02_NATIVE_UNOWNED_EXPOSURE_V1:{denied}");
                        }
                        if close_mode {
                            let add_id=add["intent_id"].as_str().expect("T09.6 add intent id").to_owned();
                            let mut closes=Vec::new();
                            for (opening_intent_id,operation_key,quantity,order_id,post_count) in [
                                (new_intent_id.clone(),"t09_real_public_close_030_0001","0.3","recorded-t09-close-030",6),
                                (add_id,"t09_real_public_close_020_0001","0.2","recorded-t09-close-020",7),
                            ] {
                                refresh_recorded_market();
                                closes.push(t09_6_close_owned_lot(T09_6CloseContext {
                                    accounts:accounts.clone(),
                                    trade:trade.clone(),rate_budget:rate_budget.clone(),
                                    feed:feed.clone(),opening_intent_id,
                                    operation_key:operation_key.into(),quantity,
                                    venue_order_id:order_id,expected_post_count:post_count,
                                }).await);
                            }
                            // Capture the owned SQL identity and arm the exact finite
                            // signed cancel source before the first terminal-flat
                            // observation can wake a concurrent production Status.
                            let owner=accounts.resolve("fixture-a").await.unwrap();
                            let second_sql_id=new_intent_id.clone();
                            let add_sql_id=add["intent_id"].as_str()
                                .expect("T09.6 Add intent for cancel source").to_owned();
                            let (second_client_id,add_client_id)=owner.execution.store.call(
                                std::time::Instant::now()+Duration::from_secs(3),
                                move |s| {
                                    let second=s.intent(&second_sql_id)?
                                        .ok_or(okx_execution::StoreError::Invalid("T09_6_SECOND_INTENT_MISSING"))?;
                                    let add=s.intent(&add_sql_id)?
                                        .ok_or(okx_execution::StoreError::Invalid("T09_6_ADD_INTENT_MISSING"))?;
                                    Ok((second.client_order_id,add.client_order_id))
                                }
                            ).await.expect("T09.6 persisted protection owners");
                            let first_key=format!("cleanup_{second_client_id}");
                            let second_key=format!("cleanup_{add_client_id}");
                            let prior=owner.execution.store.call(
                                std::time::Instant::now()+Duration::from_secs(3),
                                move |s| Ok((s.operation(&first_key)?,s.operation(&second_key)?))
                            ).await.expect("T09.6 source phase SQL boundary");
                            let prior_posts=feed.outbound_posts.lock().unwrap().iter()
                                .filter(|(_,route,body)| route=="/api/v5/trade/cancel-algos"
                                    && body.get(0).and_then(|row|row.get("algoId"))
                                        .and_then(serde_json::Value::as_str)
                                        .is_some_and(|id|id=="900000002"||id=="900000003"))
                                .count();
                            assert!(
                                prior.0.is_none() && prior.1.is_none() && prior_posts==0,
                                "T09_6_CANCEL_BEFORE_SIGNED_SOURCE:SQL={prior:?}:posts={prior_posts}"
                            );
                            t09_6_cleanup_recorded_transport(
                                &feed,"900000002",true,&second_client_id,&add_client_id
                            );
                            let close_frontier=t09_6_publish_terminal_sources(T09_6TerminalSources {
                                feed:feed.clone(),
                                first_open_at_ms:financial_time.clone(),
                                first_close_at_ms:first_close_at.clone()
                                    .expect("T09.6 T09.1 terminal close timestamp"),
                                second_first_at_ms:first_event_ms,
                                second_last_at_ms:last_event_ms,
                                add_fill_at_ms:add["settlement"]["add_fill_at_ms"].as_i64()
                                    .expect("T09.6 immutable Add fill timestamp"),
                                closes:closes.clone(),
                            });
                            let mut witness=serde_json::json!(null);
                            tokio::time::timeout(Duration::from_secs(20),async {
                                loop {
                                    let (fills,bills)=owner.execution.store.call(
                                        std::time::Instant::now()+Duration::from_secs(3),
                                        |s|Ok((s.cursor("fills","SWAP")?,s.cursor("bills","account")?))
                                    ).await.expect("T09.6 source cursors from native SQL");
                                    let health=owner.source_capture_health.read().await.clone();
                                    let snapshot=owner.latest_snapshot.read().await.clone();
                                    let ready_f=fills.as_ref().is_some_and(|c|
                                        c.watermark>=close_frontier && c.coverage_gaps.is_empty());
                                    let ready_b=bills.as_ref().is_some_and(|c|
                                        c.watermark>=close_frontier && c.coverage_gaps.is_empty());
                                    let flat=snapshot.as_ref().is_some_and(|s|s.positions.is_empty()
                                        && chrono::DateTime::parse_from_rfc3339(&s.source_received_at)
                                            .ok().is_some_and(|t|t.timestamp_millis()>=close_frontier));
                                    witness=serde_json::json!({
                                        "fills":fills,"bills":bills,"flat":flat,
                                        "capture_reason":health.reason_code,
                                        "capture_success_ms":health.last_success_ms,
                                    });
                                    if ready_f && ready_b && flat { break }
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                }
                            }).await.unwrap_or_else(|_|panic!(
                                "T09_6_SIGNED_TERMINAL_CAPTURE_UNPROVEN:{witness}"
                            ));
                            println!("T09_6_NATIVE_FINANCIAL_SOURCES_V1:{witness}");
                            let terminal=t09_6_source_backed_close_release(
                                T09_6FinalizeContext {
                                    accounts: &accounts,
                                    trade: &trade,
                                    risk: &risk,
                                    rate_budget: &rate_budget,
                                    feed: &feed,
                                    second_entry_id: &new_intent_id,
                                    add_entry_id: add["intent_id"].as_str()
                                        .expect("T09.6 Add source"),
                                    second_client_id: &second_client_id,
                                    add_client_id: &add_client_id,
                                    closes: &closes,
                                },
                            ).await;
                            println!("T09_6_NATIVE_TERMINAL_V1:{terminal}");
                            // T09.6/T09.7 must produce independently checkable close
                            // and restart receipts even when T09.8 is still
                            // blocked. The T09.8 run must exercise the SAME
                            // production TradeService with the complete
                            // recorded financial cohort and fail if the
                            // budget denial is not source-proven.
                            if std::env::var("OKX_T09_8_DENIAL_PROBE")
                                .is_ok_and(|value| value == "1")
                            {
                                // The same signed public market is refreshed by
                                // the recorded production boundary, never a test
                                // exchange or financial shortcut.
                                refresh_recorded_market();
                                let denial=t09_8_excess_entry_denied(
                                    &accounts,&trade,&risk,&feed
                                ).await;
                                println!("T09_8_NATIVE_CAMPAIGN_DENIAL_V1:{denial}");
                            }

                            println!("T09_6_NATIVE_CLOSE_ACK_V1:{}",serde_json::json!({
                                "closes":closes,"created_posts":feed.outbound_posts.lock().unwrap().len(),
                                "terminal_proven":false
                            }));
                        }
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
        if std::env::var("OKX_S2_05_POLICY_CAS_PROBE").as_deref() == Ok("1") {
            assert!(
                !status_probe,
                "S2-05 policy rotation is a standalone T09.5 probe"
            );
            let posts_before = feed.outbound_posts.lock().unwrap().len();
            assert_eq!(
                posts_before, 1,
                "S2-05 begins after one real venue create ACK"
            );
            std::fs::write(
                format!("{handoff_path}.s2-policy-cas-ready"),
                b"AFTER_SIGNED_ACK_AND_SEMANTIC_REPLAY",
            )
            .expect("S2-05 causal Worker policy-CAS barrier");
            // The REAL RuntimeSession commits an account pause via its
            // governance HTTP CAS and pushes PolicySnapshot over existing WSS.
            // Only the normal production policy handler can update account SQL.
            let paused = tokio::time::timeout(Duration::from_secs(18), async {
                loop {
                    let caps = trade
                        .trade("fixture-a", &TradeRequest::Capabilities {}, None)
                        .await
                        .expect("S2-05 native capabilities readback");
                    if caps["governance"]["revision"] == json!(3)
                        && caps["governance"]["explicit_pause"] == true
                        && caps["native_sql_policy"]["revision"] == json!(3)
                        && caps["native_sql_policy"]["matches_governance"] == true
                    {
                        break caps;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("S2-05 actual Worker policy CAS must reach both Native SQL owners");
            let resumed = trade
                .trade("fixture-a", &execute, None)
                .await
                .expect("S2-05 old accepted key must survive an actual policy pause");
            let replay_id = if resumed["status"] == "unknown" {
                resumed["intent_id"].as_str()
            } else {
                resumed["data"]["intent"]["intent_id"].as_str()
            };
            assert_eq!(
                replay_id,
                Some(saved.intent_id.as_str()),
                "S2-05 old semantic request must retain original identity after policy CAS"
            );
            let mut new_key = execute.clone();
            if let TradeRequest::Execute {
                idempotency_key, ..
            } = &mut new_key
            {
                *idempotency_key = "s2_05_new_policy_key_denied_0001".into();
            } else {
                panic!("S2-05 requires the original Execute shape");
            }
            let new_reason = trade
                .trade("fixture-a", &new_key, None)
                .await
                .expect_err("S2-05 new key must apply the current pause");
            assert_eq!(
                new_reason, "ACCOUNT_NEW_RISK_PAUSED",
                "S2-05 new key must be denied by current policy rather than stale authority"
            );
            let persisted = accounts
                .resolve("fixture-a")
                .await
                .unwrap()
                .execution
                .store
                .call(std::time::Instant::now() + Duration::from_secs(3), |sql| {
                    sql.by_idempotency("t09_real_public_execute_0001")
                })
                .await
                .expect("S2-05 original Native SQL owner must answer")
                .expect("S2-05 original SQL intent cannot be lost");
            assert_eq!(persisted.request_hash, saved.request_hash);
            assert_eq!(persisted.plan_hash, saved.plan_hash);
            assert_eq!(persisted.deadline, saved.deadline);
            assert_eq!(persisted.client_order_id, saved.client_order_id);
            assert_eq!(persisted.policy_revision, saved.policy_revision);
            let posts_after = feed.outbound_posts.lock().unwrap().len();
            assert_eq!(
                posts_after, posts_before,
                "S2-05 policy CAS must not trigger a new POST"
            );
            println!(
                "S2_05_NATIVE_POLICY_REPLAY_V1:{}",
                json!({
                    "source":"runtime_session_governance_cas_over_wss",
                    "original_intent_id":saved.intent_id,
                    "replayed_intent_id":replay_id,
                    "original_policy_revision":saved.policy_revision,
                    "new_governance_revision":paused["governance"]["revision"],
                    "native_sql_revision":paused["native_sql_policy"]["revision"],
                    "native_sql_matches_governance":paused["native_sql_policy"]["matches_governance"],
                    "original_request_hash":saved.request_hash,
                    "persisted_request_hash":persisted.request_hash,
                    "original_deadline_ms":saved.deadline,
                    "persisted_deadline_ms":persisted.deadline,
                    "new_key_reason":new_reason,
                    "posts_before":posts_before,
                    "posts_after":posts_after,
                })
            );
        }
        println!("t09_public_trade_execute_open_ack_and_idempotency PASS: posts=1");
        // A foreign Worker command closes the transport BEFORE normal
        // shutdown; the receiver may already be gone. That is the fault
        // under test, not a failure to send a shutdown notification.
        // Ordinary cohorts still REQUIRE a live receiver and clean join.
        if std::env::var("OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE").as_deref() == Ok("1") {
            let _ = shutdown_tx.send(true);
        } else {
            shutdown_tx
                .send(true)
                .expect("ordinary Native WSS shutdown receiver");
        }
        let terminated = tokio::time::timeout(Duration::from_secs(10), session)
            .await
            .unwrap()
            .unwrap();
        if std::env::var("OKX_T10_04_FOREIGN_WORKER_REPLY_PROBE").as_deref() == Ok("1") {
            let transport_failure = terminated
                .expect_err("T10-04 wrong Worker command must close the Native WSS session");
            assert!(
                transport_failure
                    .to_string()
                    .contains("RISK_RESULT_COMMAND_UNBOUND"),
                "T10-04 must preserve first binding failure: {transport_failure}"
            );
        } else {
            assert!(
                terminated.is_ok(),
                "native session should terminate cleanly: {terminated:?}"
            );
        }
        public_shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), public_task)
            .await
            .expect("public WS coordinator did not stop")
            .unwrap()
            .unwrap();
        let restart_b_account = if close_mode {
            Some(
                accounts
                    .resolve("fixture-b")
                    .await
                    .unwrap()
                    .configuration
                    .account_key
                    .clone(),
            )
        } else {
            None
        };
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
        println!(
            "NATIVE_OWNERS_CLOSED_V1:{}",
            serde_json::json!({
                "schema": "okx.native-owners-closed/v1",
                "native_pid": std::process::id(),
                "native_wss_joined": true,
                "public_ws_joined": true,
                "account_sources_shutdown_complete": true,
                "sql_owners_terminated": stopped.len(),
            })
        );
        if let Some(account) = restart_account {
            // Reopen the SAME verified Native AccountStore through its normal
            // SQLite owner constructor after the old worker has terminated.
            // No test-only SQL mutation, copy or projection is permitted.
            let key = account.wire();
            let child_a_key = key.clone();
            let path = root
                .join("accounts")
                .join(crate::accounts::key_hash(&account))
                .join("account.sqlite");
            let child_a_path = path.clone();
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
                    // T10-08 follows the same T09.3 entry, but unlike the
                    // standalone partial-fill case it later CLOSED all own
                    // lots and durably resolved its protection incident.
                    // Verify these facts AFTER stopping the SQL worker and
                    // reopening the original on-disk SQLite store.
                    let t10_08_recovery = if std::env::var(
                        "OKX_T10_08_PROTECTION_FAILURE_PROBE"
                    ).as_deref() == Ok("1") {
                        let original = second.as_ref().ok_or(
                            okx_execution::StoreError::Invalid(
                                "T10_08_RESTART_ORIGINAL_INTENT_MISSING"
                            )
                        )?;
                        let close_key=format!("safety_close_{}",&original.client_order_id);
                        let safety_close=s.by_idempotency(&close_key)?.ok_or(
                            okx_execution::StoreError::Invalid(
                                "T10_08_RESTART_CLOSE_INTENT_MISSING"
                            )
                        )?;
                        let receipt=s.operation(&format!(
                            "protection-recovered:{}",original.intent_id
                        ))?.ok_or(okx_execution::StoreError::Invalid(
                            "T10_08_RESTART_RECOVERY_RECEIPT_MISSING"
                        ))?;
                        let still_stopped=s.stops()?.iter().any(|(id,_,_,_)|
                            id==&format!("protection:{}",original.intent_id)
                        );
                        Some(json!({
                            "schema":"okx.t10-08-native-sql-restart-recovery/v1",
                            "original_intent_id":original.intent_id,
                            "original_state":original.state.as_str(),
                            "original_revision":original.revision,
                            "original_incident":original.incident,
                            "original_central_held":second_central,
                            "close_intent_id":safety_close.intent_id,
                            "close_state":safety_close.state.as_str(),
                            "close_revision":safety_close.revision,
                            "close_quantity":safety_close.plan.quantity.normalize().to_string(),
                            "close_risk_increase":safety_close.risk_increase,
                            "risk_stop_latched":still_stopped,
                            "recovery_receipt_kind":receipt.kind,
                            "recovery_source_digest":receipt.result_ref,
                        }))
                    } else {None};
                    Ok(json!({
                        "opening_intent_id":opening.intent_id,
                        "opening_state":opening.state.as_str(),
                        "opening_request_hash":opening.request_hash,
                        "opening_plan_hash":opening.plan_hash,
                        "opening_deadline_ms":opening.deadline,
                        "opening_client_order_id":opening.client_order_id,
                        "opening_policy_revision":opening.policy_revision,
                        "close_intent_id":close.intent_id,
                        "close_state":close.state.as_str(),
                        "active_central_receipt":active_central.is_some(),
                        "remaining_lot_count":lots.iter()
                            .filter(|lot| lot.remaining_qty > Decimal::ZERO)
                            .count(),
                        "second_intent_state":second.as_ref().map(|i|i.state.as_str()),
                        "second_active_central_receipt":second_central,
                        "managed_add":managed_add,
                        "t10_08_recovery":t10_08_recovery,
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
                    readback["remaining_lot_count"],
                    if close_mode { 0 } else { 3 },
                    "T09.4/T09.6 must persist exact closed vs live lots"
                );
                assert_eq!(
                    readback["remaining_lot_quantity"],
                    if close_mode { "0" } else { "0.5" }
                );
                assert_eq!(add["state"], "FILLED");
                assert_eq!(add["venue_order_id"], "recorded-t09-add-001");
                assert_eq!(
                    add["active_central_receipt"], !close_mode,
                    "live managed risk must remain held"
                );
                assert_eq!(add["lot_count"], if close_mode { 0 } else { 1 });
                assert_eq!(add["lot_quantity"], if close_mode { "0" } else { "0.2" });
                assert_eq!(add["protected_quantity"], "0.2");
                assert_eq!(add["filled_quantity"], "0.2");
                assert_eq!(add["gross_notional_usdt"], "2");
                assert_eq!(add["linked_bill_count"], 1);
                assert_eq!(add["linked_bill_fee_usdt"], "0.001");
                assert_eq!(add["entry_total_usdt"], "2.001");
                assert_eq!(readback["second_intent_state"], "FILLED");
                assert_eq!(readback["second_active_central_receipt"], !close_mode);
            } else if std::env::var("OKX_T10_08_PROTECTION_FAILURE_PROBE").as_deref() == Ok("1") {
                // This is not the standalone T09.3 exposure! Its two lots
                // were actually reduced to zero by the one public Close,
                // and the exact original central grant was released.
                assert_eq!(
                    readback["remaining_lot_count"], 0,
                    "T10-08 restarted SQL must have no unprotected lots"
                );
                assert_eq!(readback["remaining_lot_quantity"], "0");
                assert_eq!(readback["second_intent_state"], "FILLED");
                assert_eq!(readback["second_active_central_receipt"], false);
                let proof = &readback["t10_08_recovery"];
                assert_eq!(proof["schema"], "okx.t10-08-native-sql-restart-recovery/v1");
                assert_eq!(proof["original_state"], "FILLED");
                assert_eq!(proof["original_incident"], serde_json::Value::Null);
                assert_eq!(proof["original_central_held"], false);
                assert_eq!(proof["close_state"], "FILLED");
                assert_eq!(proof["close_quantity"], "0.3");
                assert_eq!(proof["close_risk_increase"], false);
                assert_eq!(proof["risk_stop_latched"], false);
                assert_eq!(
                    proof["recovery_receipt_kind"],
                    "PROTECTION_INCIDENT_FLAT_RESOLVED"
                );
                assert_eq!(
                    proof["recovery_source_digest"].as_str().map(str::len),
                    Some(64)
                );
                println!("T10_08_NATIVE_RESTART_RECOVERY_V1:{proof}");
            } else if std::env::var("OKX_T09_3_PARTIAL_PROBE").as_deref() == Ok("1") {
                assert_eq!(readback["remaining_lot_count"], 2);
                assert_eq!(readback["remaining_lot_quantity"], "0.3");
                assert_eq!(readback["second_intent_state"], "FILLED");
                assert_eq!(readback["second_active_central_receipt"], !close_mode);
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
                        "added_quantity":settled["filled"]["quantity"],
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
            if close_mode {
                // The child is a real process with no exchange transport.
                // Compare the parent recorded HTTP boundary before and after
                // its lifetime; never claim zero repost from a literal alone.
                let posts_before_child = feed.outbound_posts.lock().unwrap().len();
                assert_eq!(posts_before_child, 9, "T09.7 terminal transport baseline");
                let other = restart_b_account.as_ref().expect("T09.7 B account UID");
                let child_b_path = root
                    .join("accounts")
                    .join(crate::accounts::key_hash(other))
                    .join("account.sqlite");
                let output=tokio::time::timeout(Duration::from_secs(20),
                    tokio::process::Command::new(std::env::current_exe()
                        .expect("T09.7 own Rust test executable"))
                        .arg("--exact")
                        .arg("cloudflare_transport::raw_native_session_tests::t09_7_native_process_restore_existing_sql")
                        .arg("--nocapture")
                        .env("OKX_T09_7_REOPEN_CHILD","1")
                        .env("OKX_T09_7_ACCOUNT_A_SQL",child_a_path)
                        .env("OKX_T09_7_ACCOUNT_A_KEY",child_a_key)
                        .env("OKX_T09_7_ACCOUNT_B_SQL",child_b_path)
                        .env("OKX_T09_7_ACCOUNT_B_KEY",other.wire())
                        .output()
                ).await.expect("T09.7 independent Native process timed out")
                 .expect("T09.7 failed to launch Native process");
                let stdout = String::from_utf8(output.stdout).expect("T09.7 child output encoding");
                assert!(
                    output.status.success(),
                    "T09.7 Native process crash/reopen failed: {}",
                    stdout.chars().take(1000).collect::<String>()
                );
                let receipt = stdout
                    .lines()
                    .find_map(|line| {
                        line.split_once("T09_7_NATIVE_PROCESS_RESTART_V1:")
                            .map(|(_, json)| json.to_owned())
                    })
                    .expect("T09.7 real child process SQL receipt missing");
                let proof: serde_json::Value =
                    serde_json::from_str(&receipt).expect("T09.7 child Native receipt invalid");
                assert_eq!(proof["new_os_process"], true);
                assert_eq!(proof["new_exchange_posts"], 0);
                let posts_after_child = feed.outbound_posts.lock().unwrap().len();
                assert_eq!(
                    posts_after_child, posts_before_child,
                    "T09.7 independent native restart must not repost a venue operation"
                );
                let mut proof = proof;
                proof["recorded_transport_posts_before"] = json!(posts_before_child);
                proof["recorded_transport_posts_after"] = json!(posts_after_child);
                proof["new_exchange_posts"] = json!(posts_after_child - posts_before_child);
                println!("T09_7_NATIVE_PROCESS_RESTART_V1:{proof}");
            }
        }
        std::fs::remove_dir_all(root).unwrap();
        println!("native_to_persisted_workerd_riskbook PASS: no venue POST");
    }
}
