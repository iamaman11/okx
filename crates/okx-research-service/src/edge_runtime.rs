//! Independently authenticated, Research-owned Edge transport. This module has
//! no account store, exchange credentials, execution trait or Engine client.
//! Every source reachable through `AnalyticsService` is a bounded public GET.
use crate::{
    analytics::AnalyticsService,
    ipc::portfolio_scenario_wire,
    producer::Producer,
    source_api::PublicSourceClient,
};
use futures_util::{SinkExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use okx_ipc_contracts::{
    AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest, AgentResponse,
    AgentResponseStatus, DIRECT_TRANSPORT_FRAME_SCHEMA_V2 as SCHEMA,
    DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES as FRAME_LIMIT, DataQuality, DirectRuntimeProfile,
    DirectTransportFrame as Frame, research_ipc::ResearchScope,
    trading::DiagnosticsView,
};
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{AUTHORIZATION, HeaderValue},
    },
};
use zeroize::Zeroizing;

const MAX_RESEARCH_IN_FLIGHT: usize = 6;
const REQUEST_DEADLINE: Duration = Duration::from_secs(80);

/// Inputs are supplied by the native process supervisor, never via CLI args,
/// argv, request payloads or public log messages.
pub struct EdgeRuntimeConfig {
    url: String,
    runtime_id: String,
    host_scope: String,
    incarnation: String,
    token: Zeroizing<String>,
}

impl EdgeRuntimeConfig {
    pub fn from_environment() -> Result<Self, String> {
        fn required(name: &str) -> Result<String, String> {
            std::env::var(name).map_err(|_| format!("{name}_UNAVAILABLE"))
        }
        let token = Zeroizing::new(required("OKX_RESEARCH_BEARER_TOKEN")?);
        Self::new(
            required("OKX_RESEARCH_EDGE_WSS_URL")?,
            required("OKX_RESEARCH_RUNTIME_ID")?,
            required("OKX_RESEARCH_HOST_SCOPE")?,
            required("OKX_CONTROL_AGENT_INCAR")?,
            token,
        )
    }

    pub fn new(
        url: String,
        runtime_id: String,
        host_scope: String,
        incarnation: String,
        token: Zeroizing<String>,
    ) -> Result<Self, String> {
        if !url.starts_with("wss://")
            || url.contains('@')
            || url.contains('#')
            || url.len() > 2048
            || url.bytes().any(|b| b.is_ascii_control())
        {
            return Err("INVALID_RESEARCH_EDGE_WSS_URL".into());
        }
        if runtime_id.is_empty()
            || runtime_id.len() > 64
            || !runtime_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("INVALID_RESEARCH_RUNTIME_ID".into());
        }
        if host_scope.len() != 64
            || !host_scope
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("INVALID_RESEARCH_HOST_SCOPE".into());
        }
        if incarnation.len() != 36
            || !incarnation.starts_with("ctl_")
            || !incarnation[4..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("INVALID_RESEARCH_INCARNATION".into());
        }
        if token.is_empty()
            || token.len() > 1024
            || token.bytes().any(|b| !b.is_ascii_graphic())
        {
            return Err("INVALID_RESEARCH_BEARER".into());
        }
        Ok(Self {
            url,
            runtime_id,
            host_scope,
            incarnation,
            token,
        })
    }

    fn hello(&self, producer: &Producer) -> Frame {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).expect("RESEARCH_RANDOM_UNAVAILABLE");
        Frame::HelloResearch {
            schema: SCHEMA.into(),
            runtime_id: self.runtime_id.clone(),
            connection_id: format!("research_{:032x}", u128::from_le_bytes(random)),
            host_scope: self.host_scope.clone(),
            contract: okx_ipc_contracts::accounts::PRODUCT_CONTRACT.into(),
            runtime_profile: DirectRuntimeProfile::Research,
            agent_pid: std::process::id(),
            agent_binary_sha256: producer.binary_sha256().into(),
            agent_incarnation: self.incarnation.clone(),
        }
    }
}

/// Run without an Agent/Engine process, account credentials, or financial DB.
/// The research catalog is locked to the separately provided scope and root.
pub async fn run(root: PathBuf, scope: ResearchScope) -> Result<(), String> {
    let config = EdgeRuntimeConfig::from_environment()?;
    std::fs::create_dir_all(&root).map_err(|_| "RESEARCH_ROOT_CREATE_FAILED")?;
    let source = PublicSourceClient::new(Arc::new(
        crate::independent_source::IndependentOkxPublicSource::new()?,
    ));
    let producer = Producer::current()?;
    let service = AnalyticsService::new_scoped_with_producer(root, source, &scope, producer.clone())?;
    let mut retries = 0usize;
    loop {
        if let Err(code) = session(&config, &producer, &service).await {
            // Error codes contain no secret, request body, URL or market data.
            tracing::warn!(reason = code, "research-only WSS disconnected");
        }
        let seconds = [1, 3, 10, 30, 60][retries.min(4)];
        retries = (retries + 1).min(4);
        tokio::time::sleep(Duration::from_secs(seconds)).await;
    }
}

fn parse_frame(message: Message) -> Result<Option<Frame>, String> {
    match message {
        Message::Text(json) => {
            if json.len() > FRAME_LIMIT {
                return Err("RESEARCH_EDGE_FRAME_OVERSIZED".into());
            }
            let frame: Frame = serde_json::from_str(&json)
                .map_err(|_| "INVALID_RESEARCH_EDGE_FRAME")?;
            frame.validate_incoming_envelope().map_err(|_| "INVALID_RESEARCH_EDGE_ENVELOPE")?;
            Ok(Some(frame))
        }
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        _ => Err("RESEARCH_EDGE_NON_TEXT_FRAME".into()),
    }
}

fn research_session_binding(frame: &Frame, session: &str, generation: u64) -> Result<(), String> {
    let (received_session, received_generation) = match frame {
        Frame::Ping { session_id, connection_generation, .. }
        | Frame::Pong { session_id, connection_generation, .. }
        | Frame::Request { session_id, connection_generation, .. } =>
            (session_id, *connection_generation),
        _ => return Err("RESEARCH_EDGE_FRAME_FORBIDDEN".into()),
    };
    if received_session == session && received_generation == generation {
        Ok(())
    } else {
        Err("RESEARCH_EDGE_GENERATION_MISMATCH".into())
    }
}

async fn send<S>(socket: &mut S, frame: Frame) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let json = serde_json::to_string(&frame).map_err(|_| "RESEARCH_EDGE_ENCODE_FAILED")?;
    if json.len() > FRAME_LIMIT {
        return Err("RESEARCH_EDGE_RESPONSE_OVERSIZED".into());
    }
    socket.send(Message::Text(json.into())).await.map_err(|_| "RESEARCH_EDGE_WRITE_FAILED".into())
}

async fn session(
    config: &EdgeRuntimeConfig,
    producer: &Producer,
    service: &AnalyticsService,
) -> Result<(), String> {
    let mut request = config.url.clone().into_client_request()
        .map_err(|_| "INVALID_RESEARCH_EDGE_WSS_URL")?;
    let header = Zeroizing::new(format!("Bearer {}", config.token.as_str()));
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&header).map_err(|_| "INVALID_RESEARCH_BEARER")?,
    );
    let (socket, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(request))
        .await.map_err(|_| "RESEARCH_EDGE_CONNECT_TIMEOUT")?
        .map_err(|_| "RESEARCH_EDGE_CONNECT_FAILED")?;
    let (mut sink, mut source) = socket.split();
    let hello = config.hello(producer);
    hello.validate().map_err(|_| "RESEARCH_HELLO_INVALID")?;
    send(&mut sink, hello).await?;
    let ack = tokio::time::timeout(Duration::from_secs(5), source.next())
        .await.map_err(|_| "RESEARCH_HELLO_TIMEOUT")?
        .ok_or("RESEARCH_HELLO_CLOSED")?
        .map_err(|_| "RESEARCH_EDGE_READ_FAILED")?;
    let Some(Frame::HelloAck { session_id, connection_generation, .. }) = parse_frame(ack)?
    else { return Err("RESEARCH_HELLO_ACK_REQUIRED".into()); };

    let mut requests: FuturesUnordered<BoxFuture<'static, (String, AgentResponse)>> =
        FuturesUnordered::new();
    let mut pending = HashSet::<String>::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_seen = Instant::now();
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > Duration::from_secs(40) {
                    return Err("RESEARCH_EDGE_HEARTBEAT_TIMEOUT".into());
                }
                let mut random = [0u8;16];
                getrandom::fill(&mut random).map_err(|_| "RESEARCH_RANDOM_UNAVAILABLE")?;
                send(&mut sink, Frame::Ping {
                    schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                    nonce: format!("research_ping_{:032x}", u128::from_le_bytes(random)),
                }).await?;
            },
            Some((id, result)) = requests.next(), if !requests.is_empty() => {
                pending.remove(&id);
                let response = Frame::Response {
                    schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                    response: result,
                };
                if serde_json::to_vec(&response).map_err(|_| "RESEARCH_EDGE_ENCODE_FAILED")?.len() > FRAME_LIMIT {
                    send(&mut sink, Frame::Response {
                        schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                        response: response_for(&id, Err("RESEARCH_RESULT_TOO_LARGE".into())),
                    }).await?;
                } else {
                    send(&mut sink, response).await?;
                }
            },
            next = source.next() => {
                let message = next.ok_or("RESEARCH_EDGE_CLOSED")?
                    .map_err(|_| "RESEARCH_EDGE_READ_FAILED")?;
                let Some(frame) = parse_frame(message)? else {continue};
                research_session_binding(&frame, &session_id, connection_generation)?;
                last_seen = Instant::now();
                match frame {
                    Frame::Ping { nonce, .. } => {
                        send(&mut sink, Frame::Pong {
                            schema: SCHEMA.into(), session_id: session_id.clone(),
                            connection_generation, nonce,
                        }).await?;
                    },
                    Frame::Pong { .. } => {},
                    Frame::Request { request, operator_governance, .. } => {
                        let id = request.request_id.clone();
                        // An acknowledged request can be terminally rejected; it never
                        // receives a financial owner, even on a malformed operation.
                        send(&mut sink, Frame::DeliveryAck {
                            schema: SCHEMA.into(), session_id: session_id.clone(),
                            connection_generation, request_id: id.clone(),
                        }).await?;
                        if pending.contains(&id) {
                            continue;
                        }
                        if operator_governance.is_some() || !research_only_permits(&request) {
                            send(&mut sink, Frame::Response {
                                schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                                response: rejected(&id, "RESEARCH_OPERATION_FORBIDDEN"),
                            }).await?;
                        } else if request.validate().is_err() {
                            send(&mut sink, Frame::Response {
                                schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                                response: rejected(&id, "INVALID_RESEARCH_REQUEST"),
                            }).await?;
                        } else if requests.len() >= MAX_RESEARCH_IN_FLIGHT {
                            send(&mut sink, Frame::Response {
                                schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                                response: rejected(&id, "RESEARCH_QUEUE_FULL"),
                            }).await?;
                        } else {
                            pending.insert(id.clone());
                            let executor = service.clone();
                            requests.push(Box::pin(async move {
                                let result = tokio::time::timeout(
                                    REQUEST_DEADLINE, dispatch(&executor, request.operation)
                                ).await.unwrap_or_else(|_| Err("RESEARCH_REQUEST_TIMEOUT".into()));
                                (id.clone(), response_for(&id, result))
                            }));
                        }
                    },
                    _ => unreachable!("strict incoming research frame allowlist"),
                }
            },
        }
    }
}

/// This is the only remote operation gateway. Adding a new `AgentOperation`
/// variant cannot implicitly give it access to Research or finance.
pub fn research_only_permits(request: &AgentRequest) -> bool {
    request.account.is_none() && match &request.operation {
        AgentOperation::Market { .. }
        | AgentOperation::MarketCapabilities
        | AgentOperation::Research { .. }
        | AgentOperation::PortfolioScenario { .. }
        | AgentOperation::FindInstruments { .. }
        | AgentOperation::MarketOverview { .. }
        | AgentOperation::MarketMicrostructure { .. } => true,
        AgentOperation::Diagnostics { request } =>
            request.account.is_none() && request.view == Some(DiagnosticsView::Research),
        _ => false,
    }
}

async fn dispatch(service: &AnalyticsService, operation: AgentOperation) -> Result<serde_json::Value, String> {
    match operation {
        AgentOperation::Market { request } => {
            if request.universe.is_some() {
                return Err("RESEARCH_MARKET_EXPLICIT_INSTRUMENTS_REQUIRED".into());
            }
            service.market(request, None, None).await
        },
        AgentOperation::MarketCapabilities => Ok(service.market_capabilities()),
        AgentOperation::Research { request } => service.research(request).await,
        AgentOperation::PortfolioScenario { request } => portfolio_scenario_wire(&request),
        AgentOperation::Diagnostics { request } if request.view == Some(DiagnosticsView::Research)
            && request.account.is_none() => Ok(service.diagnostic_job(
                request.reference.as_deref().ok_or("RESEARCH_DIAGNOSTICS_REFERENCE_REQUIRED")?
            )),
        // These originally used Engine's *WebSocket cache* and global
        // reference registry. Do not quietly claim equivalent independent
        // public evidence until their Research-owned source is implemented.
        AgentOperation::MarketMicrostructure { .. }
        | AgentOperation::FindInstruments { .. }
        | AgentOperation::MarketOverview { .. } =>
            Err("RESEARCH_PUBLIC_LIVE_VIEW_UNAVAILABLE".into()),
        _ => Err("RESEARCH_OPERATION_FORBIDDEN".into()),
    }
}

fn response_for(id: &str, result: Result<serde_json::Value, String>) -> AgentResponse {
    let quality = match &result {
        Ok(value) if value.get("quality").and_then(serde_json::Value::as_str)
            .is_some_and(|v| matches!(v,"partial"|"missing"|"stale"|"degraded")) => DataQuality::Degraded,
        Ok(_) => DataQuality::Fresh,
        Err(_) => DataQuality::NotReady,
    };
    let (status, result, failure) = match result {
        Ok(value) => (AgentResponseStatus::Completed, Some(value), None),
        Err(code) => {
            let source_deferred = okx_ipc_contracts::research_ipc::is_retryable_source_defer(&code);
            let retryable = source_deferred || code == "RESEARCH_QUEUE_FULL"
                || code == "RESEARCH_REQUEST_TIMEOUT";
            let safe_code = if source_deferred {
                "MARKET_SOURCE_RETRYABLE".into()
            } else if code.len() <= 96
                && code.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                { code } else { "RESEARCH_REQUEST_FAILED".into() };
            (AgentResponseStatus::Failed, None, Some(AgentFailure {
                retryable,
                message: "Research request unavailable".into(), code: safe_code,
            }))
        },
    };
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.into(), request_id: id.into(), status,
        generated_at: jiff::Timestamp::now().to_string(), quality,
        result_schema: Some(okx_ipc_contracts::accounts::PRODUCT_CONTRACT.into()),
        result, failure, warnings: vec![],
    }
}

fn rejected(id: &str, code: &str) -> AgentResponse {
    let mut response = response_for(id, Err(code.into()));
    response.status = AgentResponseStatus::Rejected;
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(operation: serde_json::Value) -> AgentRequest {
        serde_json::from_value(json!({
            "schema":"okx.agent.request/v1", "request_id":"research_request_0123456789",
            "operation":operation,
        })).unwrap()
    }

    #[test]
    fn profile_has_distinct_research_hello_and_no_finance_identity() {
        let cfg = EdgeRuntimeConfig::new(
            "wss://edge.example/runtime".into(), "windows-primary".into(), "a".repeat(64),
            format!("ctl_{}", "b".repeat(32)), Zeroizing::new("research-secret".into()),
        ).unwrap();
        let producer = Producer::new("c".repeat(64), None, None).unwrap();
        let hello = cfg.hello(&producer);
        assert!(hello.validate().is_ok());
        let value = serde_json::to_value(&hello).unwrap();
        assert_eq!(value["type"], "hello_research");
        assert_eq!(value["runtime_profile"], "research");
        assert_eq!(value["schema"], SCHEMA);
        assert!(value.get("installation_id").is_none());
        assert!(value.get("token").is_none());
        assert!(value.get("account").is_none());
        assert!(EdgeRuntimeConfig::new("ws://edge.example/runtime".into(), "windows-primary".into(),
            "a".repeat(64), format!("ctl_{}", "b".repeat(32)), Zeroizing::new("secret".into())).is_err());
    }

    #[test]
    fn native_owner_never_accepts_financial_or_nested_account_requests() {
        for operation in [
            json!({"type":"trade","request":{"action":"capabilities"}}),
            json!({"type":"account_summary"}),
            json!({"type":"mailbox_telemetry"}),
            json!({"type":"strategies","request":{"action":"list"}}),
        ] {
            assert!(!research_only_permits(&request(operation)));
        }
        assert!(research_only_permits(&request(json!({"type":"market_capabilities"}))));
        assert!(research_only_permits(&request(json!({"type":"research","request":{"action":"capabilities"}}))));
        let mut with_account = request(json!({"type":"market_capabilities"}));
        with_account.account = Some("okx:real:account".into());
        assert!(!research_only_permits(&with_account));
        let diagnostics = request(json!({"type":"diagnostics","request":{
            "reference":"job_123","view":"research","account":null,"cycle_id":null,"limit":null
        }}));
        assert!(research_only_permits(&diagnostics));
        let mut nested = diagnostics.clone();
        if let AgentOperation::Diagnostics { request } = &mut nested.operation {
            request.account = Some("account".into());
        }
        assert!(!research_only_permits(&nested));
    }

    #[test]
    fn inbound_financial_frames_are_never_accepted() {
        let session = "session_abcdefghijklmnop";
        let rejected = Frame::Response {
            schema: SCHEMA.into(), session_id: session.into(), connection_generation:1,
            response: rejected("research_request_0123456789", "RESEARCH_OPERATION_FORBIDDEN"),
        };
        assert_eq!(research_session_binding(&rejected,session,1).unwrap_err(),
            "RESEARCH_EDGE_FRAME_FORBIDDEN");
        let ping=Frame::Ping {
            schema:SCHEMA.into(), session_id:session.into(), connection_generation:1,
            nonce:"ping_abcdefghijklmnop".into(),
        };
        assert!(research_session_binding(&ping,session,1).is_ok());
        assert_eq!(research_session_binding(&ping,session,2).unwrap_err(),
            "RESEARCH_EDGE_GENERATION_MISMATCH");
        let frame = json!({
            "type":"risk_control", "schema":SCHEMA,
            "session_id":"session_abcdefghijklmnop", "connection_generation":1,
            "command":{"action":"stop_new_real_risk"}
        });
        // No permissive dynamic deserialization or fallback into `dispatch`.
        // An incomplete financial frame is invalid at protocol ingress.
        assert!(serde_json::from_value::<Frame>(frame).is_err());
        assert_eq!(
            response_for("research_request_0123456789",Err("RESEARCH_OPERATION_FORBIDDEN".into())).
                failure.as_ref().unwrap().code,
            "RESEARCH_OPERATION_FORBIDDEN",
        );
    }
}
