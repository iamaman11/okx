//! One authenticated connection multiplexes bounded independent native work.
use crate::{
    AgentError, AgentResult,
    query::{DispatchContext, dispatch_with_governance, response},
    risk_bridge::OutboundRiskCommand,
};
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
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
    mut shutdown: watch::Receiver<bool>,
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
    let (mut sink, mut source) = socket.split();
    let connection_id = format!("native_{:032x}", nonce());
    send(
        &mut sink,
        F::Hello {
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
        },
    )
    .await?;
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
        if flights.len() >= if finance_view {64} else {63} {send(&mut sink,F::Response{schema:SCHEMA.into(),session_id:session_id.clone(),connection_generation,response:response(&id,Err("NATIVE_QUEUE_FULL".into()))}).await?;continue;}
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
