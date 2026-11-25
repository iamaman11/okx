//! Independently authenticated, Research-owned Edge transport. This module has
//! no account store, exchange credentials, execution trait or Engine client.
//! Market views use bounded public WS and HTTP observations; analytic jobs use public GETs.
use crate::{
    analytics::AnalyticsService, independent_source::IndependentOkxPublicSource,
    ipc::portfolio_scenario_wire, producer::Producer, public_catalog, public_microstructure,
    public_overview, public_ws, source_api::PublicSourceClient,
};
use futures_util::{SinkExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use okx_research_protocol::{
    edge::{
        AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentResponse, AgentResponseStatus, DataQuality,
        FRAME_LIMIT, IncomingFrame, OutgoingFrame, PRODUCT_CONTRACT, ResearchCommand, SCHEMA,
    },
    research_ipc::ResearchScope,
};
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async,
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
            required("OKX_RESEARCH_INCAR")?,
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
        if token.is_empty() || token.len() > 1024 || token.bytes().any(|b| !b.is_ascii_graphic()) {
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

    fn hello(&self, producer: &Producer) -> OutgoingFrame {
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).expect("RESEARCH_RANDOM_UNAVAILABLE");
        OutgoingFrame::HelloResearch {
            schema: SCHEMA.into(),
            runtime_id: self.runtime_id.clone(),
            connection_id: format!("research_{:032x}", u128::from_le_bytes(random)),
            host_scope: self.host_scope.clone(),
            contract: PRODUCT_CONTRACT.into(),
            runtime_profile: "research",
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
    let source = Arc::new(IndependentOkxPublicSource::new()?);
    let producer = Producer::current()?;
    let service = AnalyticsService::new_scoped_with_producer(
        root,
        PublicSourceClient::new(source.clone()),
        &scope,
        producer.clone(),
    )?;
    let mut retries = 0usize;
    loop {
        if let Err(code) = session(&config, &producer, &service, &source).await {
            // Error codes contain no secret, request body, URL or market data.
            tracing::warn!(reason = code, "research-only WSS disconnected");
        }
        let seconds = [1, 3, 10, 30, 60][retries.min(4)];
        retries = (retries + 1).min(4);
        tokio::time::sleep(Duration::from_secs(seconds)).await;
    }
}

fn parse_frame(message: Message) -> Result<Option<IncomingFrame>, String> {
    match message {
        Message::Text(json) => {
            if json.len() > FRAME_LIMIT {
                return Err("RESEARCH_EDGE_FRAME_OVERSIZED".into());
            }
            let frame: IncomingFrame =
                serde_json::from_str(&json).map_err(|_| "INVALID_RESEARCH_EDGE_FRAME")?;
            frame
                .validate_incoming_envelope()
                .map_err(|_| "INVALID_RESEARCH_EDGE_ENVELOPE")?;
            Ok(Some(frame))
        }
        Message::Ping(_) | Message::Pong(_) => Ok(None),
        _ => Err("RESEARCH_EDGE_NON_TEXT_FRAME".into()),
    }
}

fn research_session_binding(
    frame: &IncomingFrame,
    session: &str,
    generation: u64,
) -> Result<(), String> {
    let (received_session, received_generation) = match frame {
        IncomingFrame::Ping {
            session_id,
            connection_generation,
            ..
        }
        | IncomingFrame::Pong {
            session_id,
            connection_generation,
            ..
        }
        | IncomingFrame::Request {
            session_id,
            connection_generation,
            ..
        } => (session_id, *connection_generation),
        _ => return Err("RESEARCH_EDGE_FRAME_FORBIDDEN".into()),
    };
    if received_session == session && received_generation == generation {
        Ok(())
    } else {
        Err("RESEARCH_EDGE_GENERATION_MISMATCH".into())
    }
}

async fn send<S>(socket: &mut S, frame: OutgoingFrame) -> Result<(), String>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let json = serde_json::to_string(&frame).map_err(|_| "RESEARCH_EDGE_ENCODE_FAILED")?;
    if json.len() > FRAME_LIMIT {
        return Err("RESEARCH_EDGE_RESPONSE_OVERSIZED".into());
    }
    socket
        .send(Message::Text(json.into()))
        .await
        .map_err(|_| "RESEARCH_EDGE_WRITE_FAILED".into())
}

async fn session(
    config: &EdgeRuntimeConfig,
    producer: &Producer,
    service: &AnalyticsService,
    public_data: &Arc<IndependentOkxPublicSource>,
) -> Result<(), String> {
    let mut request = config
        .url
        .clone()
        .into_client_request()
        .map_err(|_| "INVALID_RESEARCH_EDGE_WSS_URL")?;
    let header = Zeroizing::new(format!("Bearer {}", config.token.as_str()));
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&header).map_err(|_| "INVALID_RESEARCH_BEARER")?,
    );
    let (socket, _) = tokio::time::timeout(Duration::from_secs(15), connect_async(request))
        .await
        .map_err(|_| "RESEARCH_EDGE_CONNECT_TIMEOUT")?
        .map_err(|_| "RESEARCH_EDGE_CONNECT_FAILED")?;
    run_session(socket, config.hello(producer), service, public_data).await
}

async fn run_session<S: AsyncRead + AsyncWrite + Unpin>(
    socket: WebSocketStream<S>,
    hello: OutgoingFrame,
    service: &AnalyticsService,
    public_data: &Arc<IndependentOkxPublicSource>,
) -> Result<(), String> {
    let (mut sink, mut source) = socket.split();
    send(&mut sink, hello).await?;
    let ack = tokio::time::timeout(Duration::from_secs(5), source.next())
        .await
        .map_err(|_| "RESEARCH_HELLO_TIMEOUT")?
        .ok_or("RESEARCH_HELLO_CLOSED")?
        .map_err(|_| "RESEARCH_EDGE_READ_FAILED")?;
    let Some(IncomingFrame::HelloAck {
        session_id,
        connection_generation,
        ..
    }) = parse_frame(ack)?
    else {
        return Err("RESEARCH_HELLO_ACK_REQUIRED".into());
    };

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
                send(&mut sink, OutgoingFrame::Ping {
                    schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                    nonce: format!("research_ping_{:032x}", u128::from_le_bytes(random)),
                }).await?;
            },
            Some((id, result)) = requests.next(), if !requests.is_empty() => {
                pending.remove(&id);
                let response = OutgoingFrame::Response {
                    schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                    response: result,
                };
                if serde_json::to_vec(&response).map_err(|_| "RESEARCH_EDGE_ENCODE_FAILED")?.len() > FRAME_LIMIT {
                    send(&mut sink, OutgoingFrame::Response {
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
                    IncomingFrame::Ping { nonce, .. } => {
                        send(&mut sink, OutgoingFrame::Pong {
                            schema: SCHEMA.into(), session_id: session_id.clone(),
                            connection_generation, nonce,
                        }).await?;
                    },
                    IncomingFrame::Pong { .. } => {},
                    IncomingFrame::Request { request, operator_governance, .. } => {
                        let id = request.request_id.clone();
                        // An acknowledged request can be terminally rejected; it never
                        // receives a financial owner, even on a malformed operation.
                        send(&mut sink, OutgoingFrame::DeliveryAck {
                            schema: SCHEMA.into(), session_id: session_id.clone(),
                            connection_generation, request_id: id.clone(),
                        }).await?;
                        if pending.contains(&id) {
                            continue;
                        }
                        let command = if operator_governance.is_some() {
                            Err("RESEARCH_OPERATION_FORBIDDEN")
                        } else {
                            ResearchCommand::try_from(*request)
                        };
                        let command = match command {
                            Ok(command) => command,
                            Err(reason) => {
                                send(&mut sink, OutgoingFrame::Response {
                                    schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                                    response: rejected(&id, reason),
                                }).await?;
                                continue;
                            }
                        };
                        if requests.len() >= MAX_RESEARCH_IN_FLIGHT {
                            send(&mut sink, OutgoingFrame::Response {
                                schema: SCHEMA.into(), session_id: session_id.clone(), connection_generation,
                                response: rejected(&id, "RESEARCH_QUEUE_FULL"),
                            }).await?;
                        } else {
                            pending.insert(id.clone());
                            let executor = service.clone();
                            let public_data = public_data.clone();
                            requests.push(Box::pin(async move {
                                let result = tokio::time::timeout(
                                    REQUEST_DEADLINE, dispatch(&executor, &public_data, command)
                                ).await.unwrap_or_else(|_| Err("RESEARCH_REQUEST_TIMEOUT".into()));
                                (id.clone(), response_for(&id, result))
                            }));
                        }
                    },
                    IncomingFrame::HelloAck { .. } => unreachable!("session binding rejects late hello ACK"),
                }
            },
        }
    }
}

async fn dispatch(
    service: &AnalyticsService,
    public_data: &IndependentOkxPublicSource,
    command: ResearchCommand,
) -> Result<serde_json::Value, String> {
    match command {
        ResearchCommand::Market(request) => service.market_standalone(request, public_data).await,
        ResearchCommand::MarketCapabilities => Ok(service.market_capabilities()),
        ResearchCommand::Research(request) => service.research(request).await,
        ResearchCommand::PortfolioScenario(request) => portfolio_scenario_wire(&request),
        ResearchCommand::ExplainJob(reference) => {
            let id = reference
                .as_deref()
                .ok_or("RESEARCH_DIAGNOSTICS_REFERENCE_REQUIRED")?;
            // The old deployment projection described a child observed by
            // Engine. It cannot be reused for an unsupervised Research process.
            if id == "deployment" {
                return Err("RESEARCH_DEPLOYMENT_NOT_SUPERVISED".into());
            }
            if id.len() != 64
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err("INVALID_JOB_REFERENCE".into());
            }
            Ok(service.diagnostic_job(id))
        }
        ResearchCommand::FindInstruments {
            asset,
            settle_currency,
            instrument_type,
        } => {
            public_catalog::find_instruments(
                public_data,
                &asset,
                settle_currency.as_deref(),
                instrument_type,
            )
            .await
        }
        ResearchCommand::MarketOverview { instrument } => {
            match public_ws::capture(public_data, &instrument).await {
                Ok(snapshot) => Ok(public_overview::websocket_overview(&snapshot)),
                Err(reason) => {
                    let mut result =
                        public_overview::market_overview(public_data, &instrument).await?;
                    result["public_ws_quality"]["reason_code"] = serde_json::json!(reason);
                    Ok(result)
                }
            }
        }
        ResearchCommand::MarketMicrostructure {
            instrument,
            size_contracts,
        } => {
            let size = size_contracts.as_deref();
            public_microstructure::validate_request(&instrument, size)?;
            match public_ws::capture(public_data, &instrument).await {
                Ok(snapshot) => public_microstructure::websocket_microstructure(&snapshot, size),
                Err(reason) => {
                    let mut result = public_microstructure::market_microstructure(
                        public_data,
                        &instrument,
                        size,
                    )
                    .await?;
                    result["public_ws_quality"] = serde_json::json!({"reason_code":reason});
                    Ok(result)
                }
            }
        }
    }
}

fn response_for(id: &str, result: Result<serde_json::Value, String>) -> AgentResponse {
    let quality = match &result {
        Ok(value)
            if value
                .get("quality")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|v| matches!(v, "partial" | "missing" | "stale" | "degraded")) =>
        {
            DataQuality::Degraded
        }
        Ok(_) => DataQuality::Fresh,
        Err(_) => DataQuality::NotReady,
    };
    let (status, result, failure) = match result {
        Ok(value) => (AgentResponseStatus::Completed, Some(value), None),
        Err(code) => {
            let source_deferred =
                okx_research_protocol::research_ipc::is_retryable_source_defer(&code);
            let retryable = source_deferred
                || code == "RESEARCH_QUEUE_FULL"
                || code == "RESEARCH_REQUEST_TIMEOUT";
            let safe_code = if source_deferred {
                "MARKET_SOURCE_RETRYABLE".into()
            } else if code == "GLOBAL_RANK_UNIVERSE_LIMIT:512"
                || (code.len() <= 96
                    && code
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
            {
                code
            } else {
                "RESEARCH_REQUEST_FAILED".into()
            };
            (
                AgentResponseStatus::Failed,
                None,
                Some(AgentFailure {
                    retryable,
                    message: "Research request unavailable".into(),
                    code: safe_code,
                }),
            )
        }
    };
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.into(),
        request_id: id.into(),
        status,
        generated_at: jiff::Timestamp::now().to_string(),
        quality,
        result_schema: Some(PRODUCT_CONTRACT.into()),
        result,
        failure,
        warnings: vec![],
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

    fn request(
        operation: serde_json::Value,
    ) -> okx_research_protocol::edge::ResearchRequestEnvelope {
        serde_json::from_value(json!({
            "schema":"okx.agent.request/v1", "request_id":"research_request_0123456789",
            "operation":operation,
        }))
        .unwrap()
    }

    #[test]
    fn profile_has_distinct_research_hello_and_no_finance_identity() {
        let cfg = EdgeRuntimeConfig::new(
            "wss://edge.example/runtime".into(),
            "windows-primary".into(),
            "a".repeat(64),
            format!("ctl_{}", "b".repeat(32)),
            Zeroizing::new("research-secret".into()),
        )
        .unwrap();
        let producer = Producer::new("c".repeat(64), None, None).unwrap();
        let hello = cfg.hello(&producer);
        let value = serde_json::to_value(&hello).unwrap();
        assert_eq!(value["type"], "hello_research");
        assert_eq!(value["runtime_profile"], "research");
        assert_eq!(value["schema"], SCHEMA);
        assert!(value.get("installation_id").is_none());
        assert!(value.get("token").is_none());
        assert!(value.get("account").is_none());
        assert!(
            EdgeRuntimeConfig::new(
                "ws://edge.example/runtime".into(),
                "windows-primary".into(),
                "a".repeat(64),
                format!("ctl_{}", "b".repeat(32)),
                Zeroizing::new("secret".into())
            )
            .is_err()
        );
    }

    #[test]
    fn native_owner_never_accepts_financial_or_nested_account_requests() {
        for operation in [
            json!({"type":"trade","request":{"action":"capabilities"}}),
            json!({"type":"account_summary"}),
            json!({"type":"mailbox_telemetry"}),
            json!({"type":"strategies","request":{"action":"list"}}),
        ] {
            assert!(matches!(
                ResearchCommand::try_from(request(operation)),
                Err("RESEARCH_OPERATION_FORBIDDEN")
            ));
        }
        assert!(matches!(
            ResearchCommand::try_from(request(json!({"type":"market_capabilities"}))),
            Ok(ResearchCommand::MarketCapabilities)
        ));
        assert!(matches!(
            ResearchCommand::try_from(request(
                json!({"type":"research","request":{"action":"capabilities"}})
            )),
            Ok(ResearchCommand::Research(_))
        ));
        let mut with_account = request(json!({"type":"market_capabilities"}));
        with_account.account = Some("okx:real:account".into());
        assert!(matches!(
            ResearchCommand::try_from(with_account),
            Err("RESEARCH_OPERATION_FORBIDDEN")
        ));
        let diagnostics = |account| {
            request(json!({"type":"diagnostics","request":{
                "reference":"job_123","view":"research","account":account,"cycle_id":null,"limit":null
            }}))
        };
        assert!(matches!(
            ResearchCommand::try_from(diagnostics(None::<&str>)),
            Ok(ResearchCommand::ExplainJob(Some(id))) if id == "job_123"
        ));
        assert!(matches!(
            ResearchCommand::try_from(diagnostics(Some("account"))),
            Err("RESEARCH_OPERATION_FORBIDDEN")
        ));
        let mut malformed = request(json!({"type":"market_capabilities"}));
        malformed.schema = "unsupported".into();
        assert!(matches!(
            ResearchCommand::try_from(malformed),
            Err("INVALID_RESEARCH_REQUEST")
        ));
    }

    #[test]
    fn inbound_financial_frames_are_never_accepted() {
        let session = "session_abcdefghijklmnop";
        let rejected = OutgoingFrame::Response {
            schema: SCHEMA.into(),
            session_id: session.into(),
            connection_generation: 1,
            response: rejected(
                "research_request_0123456789",
                "RESEARCH_OPERATION_FORBIDDEN",
            ),
        };
        assert_eq!(
            parse_frame(Message::Text(
                serde_json::to_string(&rejected).unwrap().into()
            ))
            .unwrap_err(),
            "INVALID_RESEARCH_EDGE_FRAME"
        );
        let ping = IncomingFrame::Ping {
            schema: SCHEMA.into(),
            session_id: session.into(),
            connection_generation: 1,
            nonce: "ping_abcdefghijklmnop".into(),
        };
        assert!(research_session_binding(&ping, session, 1).is_ok());
        assert_eq!(
            research_session_binding(&ping, session, 2).unwrap_err(),
            "RESEARCH_EDGE_GENERATION_MISMATCH"
        );
        let frame = json!({
            "type":"risk_control", "schema":SCHEMA,
            "session_id":"session_abcdefghijklmnop", "connection_generation":1,
            "command":{"action":"stop_new_real_risk"}
        });
        // No permissive dynamic deserialization or fallback into `dispatch`.
        // An incomplete financial frame is invalid at protocol ingress.
        assert!(serde_json::from_value::<IncomingFrame>(frame).is_err());
        assert_eq!(
            response_for(
                "research_request_0123456789",
                Err("RESEARCH_OPERATION_FORBIDDEN".into())
            )
            .failure
            .as_ref()
            .unwrap()
            .code,
            "RESEARCH_OPERATION_FORBIDDEN",
        );
    }

    #[tokio::test]
    async fn edge_session_rejects_trade_and_then_completes_research_on_same_socket() {
        use tokio_tungstenite::tungstenite::protocol::Role;

        let dir = tempfile::tempdir().unwrap();
        let producer = Producer::new("c".repeat(64), None, None).unwrap();
        let config = EdgeRuntimeConfig::new(
            "wss://research.invalid/direct".into(),
            "windows-research".into(),
            "a".repeat(64),
            format!("ctl_{}", "b".repeat(32)),
            Zeroizing::new("test-bearer".into()),
        )
        .unwrap();
        let scope = ResearchScope::new("test-research-scope".to_owned()).unwrap();
        let public = Arc::new(IndependentOkxPublicSource::new().unwrap());
        let service = AnalyticsService::new_scoped_with_producer(
            dir.path().to_path_buf(),
            PublicSourceClient::new(public.clone()),
            &scope,
            producer.clone(),
        )
        .unwrap();
        let (research_io, edge_io) = tokio::io::duplex(16 * 1024);
        let research = WebSocketStream::from_raw_socket(research_io, Role::Client, None).await;
        let mut edge = WebSocketStream::from_raw_socket(edge_io, Role::Server, None).await;
        let fake_edge = async {
            async fn receive(
                edge: &mut WebSocketStream<tokio::io::DuplexStream>,
            ) -> serde_json::Value {
                loop {
                    let frame: serde_json::Value = serde_json::from_str(
                        edge.next().await.unwrap().unwrap().to_text().unwrap(),
                    )
                    .unwrap();
                    if frame["type"] != "ping" {
                        return frame;
                    }
                }
            }

            let hello = receive(&mut edge).await;
            assert_eq!(hello["type"], "hello_research");
            assert_eq!(hello["schema"], SCHEMA);
            assert_eq!(hello["runtime_profile"], "research");
            assert_eq!(hello["host_scope"], "a".repeat(64));
            assert!(hello.get("installation_id").is_none());
            let session = "session_0123456789";
            let hello_ack = json!({
                "type":"hello_ack", "schema":SCHEMA, "session_id":session, "connection_generation":2
            });
            edge.send(Message::Text(hello_ack.to_string().into()))
                .await
                .unwrap();

            for (id, operation, governance, status) in [
                (
                    "edge_trade_request_01",
                    json!({"type":"trade","request":{"action":"capabilities"}}),
                    serde_json::Value::Null,
                    "rejected",
                ),
                (
                    "edge_governance_request",
                    json!({"type":"research","request":{"action":"capabilities"}}),
                    json!({"session_generation":2}),
                    "rejected",
                ),
                (
                    "edge_research_request_01",
                    json!({"type":"research","request":{"action":"capabilities"}}),
                    serde_json::Value::Null,
                    "completed",
                ),
            ] {
                let request = json!({
                    "type":"request", "schema":SCHEMA, "session_id":session, "connection_generation":2,
                    "request":{
                        "schema":"okx.agent.request/v1",
                        "request_id":id,
                        "operation":operation,
                    },
                    "operator_governance":governance,
                });
                edge.send(Message::Text(request.to_string().into()))
                    .await
                    .unwrap();
                let ack = receive(&mut edge).await;
                assert_eq!(
                    ack,
                    json!({
                        "type":"delivery_ack", "schema":SCHEMA, "session_id":session,
                        "connection_generation":2, "request_id":id,
                    })
                );
                let response = receive(&mut edge).await;
                assert_eq!(response["type"], "response");
                assert_eq!(response["schema"], SCHEMA);
                assert_eq!(response["session_id"], session);
                assert_eq!(response["connection_generation"], 2);
                assert_eq!(response["response"]["schema"], AGENT_RESPONSE_SCHEMA_V1);
                assert_eq!(response["response"]["request_id"], id);
                assert_eq!(response["response"]["status"], status);
                if status == "rejected" {
                    assert_eq!(
                        response["response"]["failure"]["code"],
                        "RESEARCH_OPERATION_FORBIDDEN"
                    );
                    assert!(response["response"]["result"].is_null());
                } else {
                    assert!(response["response"]["failure"].is_null());
                    assert!(response["response"]["result"].is_object());
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                () = fake_edge => {},
                result = run_session(research, config.hello(&producer), &service, &public) => {
                    panic!("Research session ended unexpectedly: {result:?}");
                }
            }
        })
        .await
        .expect("Research frame round trip timed out");
    }
}

#[cfg(test)]
#[path = "edge_market_tests.rs"]
mod market_tests;
