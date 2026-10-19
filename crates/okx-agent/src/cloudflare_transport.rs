use std::{collections::HashMap, path::Path, time::Duration};

use chrono::{SecondsFormat, Utc};
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use okx_protocol::account_governance::{AuthorizationStatus, CentralGovernanceSnapshotV2};
use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentResponse, AgentResponseStatus,
    CentralAccountPolicy, DIRECT_TRANSPORT_FRAME_SCHEMA_V1, DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES,
    DataQuality, DirectRuntimeProfile, DirectTransportFrame,
};
use okx_runtime::{PrivateWsHandle, PublicWsHandle};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, MissedTickBehavior, interval, sleep},
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

use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    execution_runtime::{ExecutionRuntime, ExecutionRuntimeMode},
    market_bootstrap::MarketBootstrapper,
    query::{ObservationQueryContext, dispatch},
    research_session::ResearchSessionHandle,
    risk_bridge::{NativeRiskClient, OutboundRiskCommand},
};

const RECONNECT_BACKOFF_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
const HEARTBEAT_INTERVAL_SECONDS: u64 = 15;
const HEARTBEAT_DEADLINE_SECONDS: u64 = 5;
const HEARTBEAT_IDLE_RESET_SECONDS: u64 = 24 * 60 * 60;
const MAX_INFLIGHT_READ_QUERIES: usize = 8;
const MAX_PENDING_DELIVERY_CHALLENGES: usize = 64;
const DELIVERY_CHALLENGE_MAX_AGE: Duration = Duration::from_secs(15);
const DIRECT_TRANSPORT_MUTATION_REJECTED: &str = "DIRECT_TRANSPORT_MUTATION_REJECTED";
const DIRECT_TRANSPORT_BUSY: &str = "DIRECT_TRANSPORT_BUSY";
const DIRECT_TRANSPORT_QUERY_FAILED: &str = "DIRECT_TRANSPORT_QUERY_FAILED";
const DIRECT_TRANSPORT_INPUT_REJECTED: &str = "DIRECT_TRANSPORT_INPUT_REJECTED";
const DIRECT_TRANSPORT_MUTATION_OUTCOME_UNKNOWN: &str =
    "EXECUTION_OUTCOME_UNKNOWN_RECONCILE_EXACT_INTENT";

fn direct_operation_allowed(
    profile: DirectRuntimeProfile,
    runtime_mode: Option<ExecutionRuntimeMode>,
    operation: &AgentOperation,
) -> bool {
    operation.direct_transport_read_only()
        || (profile == DirectRuntimeProfile::Production
            && runtime_mode == Some(ExecutionRuntimeMode::ReadOnly)
            && matches!(operation, AgentOperation::ReconcileUnsentPrepared { .. }))
        || (profile == DirectRuntimeProfile::DemoAcceptance
            && runtime_mode == Some(ExecutionRuntimeMode::DemoAcceptance)
            && operation.direct_transport_demo_execution())
}

/// Consume the exact ping/pong challenge delivered by the authenticated Worker.
/// Only a native monotonic clock measures freshness; Cloudflare's Date.now()
/// advances on I/O and cannot be used as a cross-runtime admission clock.
fn consume_fresh_delivery_challenge(
    pending: &mut HashMap<String, Instant>,
    nonce: Option<&str>,
) -> bool {
    nonce
        .and_then(|nonce| pending.remove(nonce))
        .is_some_and(|seen_at| seen_at.elapsed() <= DELIVERY_CHALLENGE_MAX_AGE)
}

/// Delivery admission only. Native Rust will independently check UID,
/// Trade credential, financial continuity and shared risk before any POST.
fn central_production_delivery_admission(
    profile: DirectRuntimeProfile,
    mode: Option<ExecutionRuntimeMode>,
    operation: &AgentOperation,
    policy: Option<&CentralAccountPolicy>,
    governance: Option<&CentralGovernanceSnapshotV2>,
    generation: u64,
    now_ms: u64,
    governance_challenge_fresh: bool,
) -> Result<(), &'static str> {
    if profile != DirectRuntimeProfile::Production {
        return Err("PRODUCTION_PROFILE_REQUIRED");
    }
    if mode != Some(ExecutionRuntimeMode::Production) {
        return Err("NATIVE_PRODUCTION_EXECUTOR_NOT_ACTIVE");
    }
    if !matches!(
        operation,
        AgentOperation::BeginStrategyRun { .. }
            | AgentOperation::PrepareExecution { .. }
            | AgentOperation::SubmitPreparedExecution { .. }
            | AgentOperation::FinalizeStrategyRun { .. }
            | AgentOperation::QuarantineUnverifiedStrategyRun { .. }
    ) {
        return Err("PRODUCTION_OPERATION_UNSUPPORTED");
    }
    // One source of admission truth, with a stable, nonsecret reason on
    // rejection. Positive trade rights remain with native Rust/RiskBook.
    if let Some(scope) = governance {
        if !scope.valid() {
            return Err("CENTRAL_GOVERNANCE_V2_INVALID");
        }
        if scope.session_generation != generation {
            return Err("CENTRAL_GOVERNANCE_V2_GENERATION_MISMATCH");
        }
        // Challenge was issued for THIS request by the authenticated Worker
        // after a successful ping/pong. A nonce is consumed only once on the
        // native WebSocket, and its lifetime uses Instant (not wall clocks).
        if !governance_challenge_fresh {
            return Err("CENTRAL_GOVERNANCE_V2_CHALLENGE_NOT_FRESH");
        }
        if !scope.state.trading_requested
            || scope.state.operator_authorization.status != AuthorizationStatus::Active
        {
            return Err("CENTRAL_GOVERNANCE_V2_STOPPED");
        }
        return Ok(());
    }
    // Historical v1 delivery schema is retained for older sessions only;
    // it still cannot replace the native v2 RiskBook permit for a Real POST.
    if let Some(scope) = policy {
        if !scope.valid() {
            return Err("CENTRAL_POLICY_V1_INVALID");
        }
        if !scope.trading_requested || scope.revision == 0 {
            return Err("CENTRAL_POLICY_V1_STOPPED");
        }
        if scope.session_generation != generation {
            return Err("CENTRAL_POLICY_V1_GENERATION_MISMATCH");
        }
        if scope.issued_at_ms > now_ms
            || now_ms.saturating_sub(scope.issued_at_ms)
                > okx_protocol::CENTRAL_ACCOUNT_POLICY_MAX_AGE_MS
        {
            return Err("CENTRAL_POLICY_V1_NOT_FRESH");
        }
        return Ok(());
    }
    Err("CENTRAL_PRODUCTION_SCOPE_NOT_DELIVERED")
}

pub struct CloudflareTransportConfig {
    ws_url: String,
    runtime_id: String,
    runtime_profile: DirectRuntimeProfile,
    bearer_token: Zeroizing<String>,
}

impl CloudflareTransportConfig {
    pub fn new(
        ws_url: String,
        runtime_id: String,
        bearer_token: Zeroizing<String>,
        runtime_profile: DirectRuntimeProfile,
    ) -> AgentResult<Self> {
        validate_ws_url(&ws_url)?;
        validate_runtime_id(&runtime_id)?;
        Ok(Self {
            ws_url,
            runtime_id,
            runtime_profile,
            bearer_token,
        })
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }
}

#[derive(Clone, Copy)]
pub struct CloudflareQueryRuntimeContext<'a> {
    pub public_ws: &'a PublicWsHandle,
    pub market: &'a MarketBootstrapper,
    pub account: Option<&'a AccountBootstrapper>,
    pub private_ws: Option<&'a PrivateWsHandle>,
    pub execution: Option<&'a ExecutionRuntime>,
    pub research_root: &'a Path,
    pub research_session: &'a ResearchSessionHandle,
}

pub async fn run_cloudflare_transport(
    config: &CloudflareTransportConfig,
    context: CloudflareQueryRuntimeContext<'_>,
    mut shutdown: watch::Receiver<bool>,
) -> AgentResult<()> {
    let mut retry_index = 0usize;

    loop {
        if *shutdown.borrow() {
            return Ok(());
        }

        let session = run_session(config, context, shutdown.clone());
        tokio::pin!(session);

        let session_result = tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => return Ok(()),
                    Ok(()) => continue,
                    Err(_) => return Ok(()),
                }
            }
            result = &mut session => result,
        };

        match session_result {
            Ok(()) => {
                retry_index = 0;
            }
            Err(error) => {
                eprintln!("cloudflare transport session unavailable: {error}");
            }
        }

        let delay = RECONNECT_BACKOFF_SECONDS[retry_index.min(RECONNECT_BACKOFF_SECONDS.len() - 1)];
        retry_index = retry_index.saturating_add(1);
        eprintln!("cloudflare transport reconnect in {delay}s");

        tokio::select! {
            _ = sleep(Duration::from_secs(delay)) => {}
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => return Ok(()),
                    Ok(()) => {}
                    Err(_) => return Ok(()),
                }
            }
        }
    }
}

async fn run_session(
    config: &CloudflareTransportConfig,
    context: CloudflareQueryRuntimeContext<'_>,
    mut shutdown: watch::Receiver<bool>,
) -> AgentResult<()> {
    let mut request = config
        .ws_url
        .as_str()
        .into_client_request()
        .map_err(|error| AgentError::CloudflareTransport(error.to_string()))?;
    let auth = HeaderValue::from_str(&format!("Bearer {}", config.bearer_token.as_str()))
        .map_err(|_| AgentError::InvalidCloudflareToken)?;
    request.headers_mut().insert(AUTHORIZATION, auth);

    let (stream, _) = connect_async(request)
        .await
        .map_err(|error| AgentError::CloudflareTransport(error.to_string()))?;
    let (mut sink, mut source) = stream.split();

    let connection_id = random_token("conn_")?;
    send_frame(
        &mut sink,
        &DirectTransportFrame::Hello {
            schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
            runtime_id: config.runtime_id.clone(),
            connection_id,
            runtime_profile: Some(config.runtime_profile),
        },
    )
    .await?;

    let hello = tokio::select! {
        changed = shutdown.changed() => {
            match changed {
                Ok(()) if *shutdown.borrow() => return Ok(()),
                Ok(()) => return Err(AgentError::CloudflareTransport("shutdown state changed during handshake".to_owned())),
                Err(_) => return Ok(()),
            }
        }
        message = source.next() => {
            next_text_frame(message)?
        }
    };

    let (session_id, connection_generation) = match hello {
        DirectTransportFrame::HelloAck {
            session_id,
            connection_generation,
            ..
        } => (session_id, connection_generation),
        _ => {
            return Err(AgentError::CloudflareTransport(
                "expected hello_ack".to_owned(),
            ));
        }
    };

    eprintln!(
        "cloudflare transport connected generation={} endpoint={}",
        connection_generation,
        config.ws_url()
    );

    let mut heartbeat = interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECONDS));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut heartbeat_nonce: Option<String> = None;
    let heartbeat_deadline = sleep(Duration::from_secs(HEARTBEAT_IDLE_RESET_SECONDS));
    tokio::pin!(heartbeat_deadline);
    let mut in_flight: FuturesUnordered<BoxFuture<'_, (String, bool, AgentResult<AgentResponse>)>> =
        FuturesUnordered::new();
    // No two exchange-affecting commands may be outstanding on the same
    // transport generation. Read/status requests remain independently bounded.
    let mut active_mutation: Option<String> = None;
    // Request-scoped Worker ping challenges. No scheduler or second owner.
    let mut pending_delivery_challenges: HashMap<String, Instant> = HashMap::new();
    // One existing transport and exactly one outbound mutation owner.
    let (risk_tx, mut risk_rx) = mpsc::channel::<OutboundRiskCommand>(4);
    let risk_client = NativeRiskClient::new(risk_tx);
    let mut pending_risk: HashMap<
        String,
        oneshot::Sender<okx_protocol::risk_authority::RiskResult>,
    > = HashMap::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                match changed {
                    Ok(()) if *shutdown.borrow() => {
                        let _ = sink.send(Message::Close(None)).await;
                        return Ok(());
                    }
                    Ok(()) => {}
                    Err(_) => return Ok(()),
                }
            }
            _ = heartbeat.tick() => {
                if heartbeat_nonce.is_some() {
                    return Err(AgentError::CloudflareTransport(
                        "cloudflare heartbeat already pending".to_owned(),
                    ));
                }
                let nonce = random_token("heartbeat_")?;
                send_frame(
                    &mut sink,
                    &DirectTransportFrame::Ping {
                        schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                        session_id: session_id.clone(),
                        connection_generation,
                        nonce: nonce.clone(),
                    },
                )
                .await?;
                heartbeat_nonce = Some(nonce);
                heartbeat_deadline
                    .as_mut()
                    .reset(Instant::now() + Duration::from_secs(HEARTBEAT_DEADLINE_SECONDS));
            }
            _ = &mut heartbeat_deadline => {
                if heartbeat_nonce.is_some() {
                    return Err(AgentError::CloudflareTransport(
                        "cloudflare heartbeat pong deadline exceeded".to_owned(),
                    ));
                }
                heartbeat_deadline
                    .as_mut()
                    .reset(Instant::now() + Duration::from_secs(HEARTBEAT_IDLE_RESET_SECONDS));
            }
            outbound_risk = risk_rx.recv() => {
                if let Some(outbound) = outbound_risk {
                    if active_mutation.is_none() || pending_risk.len() >= 4 {
                        drop(outbound.return_to);
                        continue;
                    }
                    let id = outbound.command.command_id.clone();
                    if pending_risk.contains_key(&id) {
                        drop(outbound.return_to);
                        continue;
                    }
                    pending_risk.insert(id, outbound.return_to);
                    send_frame(
                        &mut sink,
                        &DirectTransportFrame::RiskControl {
                            schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                            session_id: session_id.clone(),
                            connection_generation,
                            command: outbound.command,
                        },
                    ).await?;
                }
            }
            completed = in_flight.next(), if !in_flight.is_empty() => {
                let Some((request_id, mutating, result)) = completed else {
                    continue;
                };
                if active_mutation.as_deref() == Some(request_id.as_str()) {
                    active_mutation = None;
                }
                let response = completed_direct_response(&request_id, mutating, result);
                response.validate()?;
                send_frame(
                    &mut sink,
                    &DirectTransportFrame::Response {
                        schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                        session_id: session_id.clone(),
                        connection_generation,
                        response,
                    },
                )
                .await?;
            }
            message = source.next() => {
                let frame = next_text_frame(message)?;
                match frame {
                    DirectTransportFrame::Ping {
                        session_id: frame_session,
                        connection_generation: frame_generation,
                        nonce,
                        ..
                    } => {
                        require_session(
                            &session_id,
                            connection_generation,
                            &frame_session,
                            frame_generation,
                        )?;
                        pending_delivery_challenges.retain(|_, seen_at| {
                            seen_at.elapsed() <= DELIVERY_CHALLENGE_MAX_AGE
                        });
                        if nonce.starts_with("ping_")
                            && pending_delivery_challenges.len() < MAX_PENDING_DELIVERY_CHALLENGES
                        {
                            pending_delivery_challenges.insert(nonce.clone(), Instant::now());
                        }
                        send_frame(
                            &mut sink,
                            &DirectTransportFrame::Pong {
                                schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                                session_id: session_id.clone(),
                                connection_generation,
                                nonce,
                            },
                        )
                        .await?;
                    }
                    DirectTransportFrame::Pong {
                        session_id: frame_session,
                        connection_generation: frame_generation,
                        nonce,
                        ..
                    } => {
                        require_session(
                            &session_id,
                            connection_generation,
                            &frame_session,
                            frame_generation,
                        )?;
                        match heartbeat_nonce.as_deref() {
                            Some(expected) if expected == nonce => {
                                heartbeat_nonce = None;
                                heartbeat_deadline
                                    .as_mut()
                                    .reset(Instant::now() + Duration::from_secs(HEARTBEAT_IDLE_RESET_SECONDS));
                            }
                            _ => {
                                return Err(AgentError::CloudflareTransport(
                                    "unexpected cloudflare heartbeat pong".to_owned(),
                                ));
                            }
                        }
                    }
                    DirectTransportFrame::Request {
                        session_id: frame_session,
                        connection_generation: frame_generation,
                        request,
                        operator_policy,
                        operator_governance,
                        ..
                    } => {
                        require_session(
                            &session_id,
                            connection_generation,
                            &frame_session,
                            frame_generation,
                        )?;
                        // Envelope and request ID were validated by next_text_frame;
                        // send ACK before validating the typed operation so that bad
                        // user input cannot disconnect a healthy trading session.
                        send_frame(
                            &mut sink,
                            &DirectTransportFrame::DeliveryAck {
                                schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                                session_id: session_id.clone(),
                                connection_generation,
                                request_id: request.request_id.clone(),
                            },
                        )
                        .await?;

                        if let Err(error) = request.validate() {
                            eprintln!("direct transport rejected malformed operation: {error}");
                            let response = direct_rejection(
                                &request.request_id,
                                DIRECT_TRANSPORT_INPUT_REJECTED,
                                "native typed request failed validation before any trade action",
                                false,
                            );
                            send_frame(
                                &mut sink,
                                &DirectTransportFrame::Response {
                                    schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                                    session_id: session_id.clone(),
                                    connection_generation,
                                    response,
                                },
                            )
                            .await?;
                            continue;
                        }

                        // Explicit user-authorized Demo acceptance reuses the same
                        // immutable Rust prepare/submit/mutate executor and policy.
                        // Production is denied even if Worker/OAuth sends a write
                        // through this otherwise authenticated read transport.
                        let mutating = !request.operation.direct_transport_read_only();
                        let governance_challenge_fresh = if mutating {
                            consume_fresh_delivery_challenge(
                                &mut pending_delivery_challenges,
                                operator_governance
                                    .as_ref()
                                    .and_then(|scope| scope.delivery_challenge.as_deref()),
                            )
                        } else {
                            false
                        };
                        let delivery = central_production_delivery_admission(
                            config.runtime_profile,
                            context.execution.map(ExecutionRuntime::mode),
                            &request.operation,
                            operator_policy.as_ref(),
                            operator_governance.as_ref(),
                            frame_generation,
                            Utc::now().timestamp_millis().max(1) as u64,
                            governance_challenge_fresh,
                        );
                        let allowed = direct_operation_allowed(
                            config.runtime_profile,
                            context.execution.map(ExecutionRuntime::mode),
                            &request.operation,
                        ) || delivery.is_ok();
                        if !allowed {
                            // A specific typed native reason avoids repeated
                            // permission/configuration changes on an unknown gate.
                            let reason = if config.runtime_profile == DirectRuntimeProfile::DemoAcceptance {
                                "DEMO_MUTATION_ADMISSION_NOT_ACTIVE"
                            } else {
                                delivery.err().unwrap_or("NATIVE_MUTATION_ADMISSION_DENIED")
                            };
                            eprintln!("direct mutation delivery denied: {reason}");
                            let response = direct_rejection(
                                &request.request_id,
                                DIRECT_TRANSPORT_MUTATION_REJECTED,
                                reason,
                                false,
                            );
                            send_frame(
                                &mut sink,
                                &DirectTransportFrame::Response {
                                    schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                                    session_id: session_id.clone(),
                                    connection_generation,
                                    response,
                                },
                            )
                            .await?;
                            continue;
                        }

                        if in_flight.len() >= MAX_INFLIGHT_READ_QUERIES
                            || (mutating && active_mutation.is_some())
                        {
                            let response = direct_rejection(
                                &request.request_id,
                                DIRECT_TRANSPORT_BUSY,
                                "direct transport read concurrency bound reached",
                                true,
                            );
                            send_frame(
                                &mut sink,
                                &DirectTransportFrame::Response {
                                    schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
                                    session_id: session_id.clone(),
                                    connection_generation,
                                    response,
                                },
                            )
                            .await?;
                            continue;
                        }

                        let query_context =
                            ObservationQueryContext::live_with_execution_research_and_session(
                                context.public_ws,
                                context.market,
                                None,
                                context.account,
                                context.private_ws,
                                context.execution,
                                Some(context.research_root),
                                Some(context.research_session),
                            );
                        let generated_at =
                            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
                        let response_request_id = request.request_id.clone();
                        if mutating {
                            active_mutation = Some(response_request_id.clone());
                        }
                        let risk_client = risk_client.clone();
                        in_flight.push(
                            async move {
                                let query_context = query_context
                                    .with_operator_policy(operator_policy.as_ref(), frame_generation)
                                    .with_operator_governance(operator_governance.as_ref())
                                    .with_risk_client(Some(&risk_client));
                                let result =
                                    dispatch(&request, query_context, &generated_at).await;
                                (response_request_id, mutating, result)
                            }
                            .boxed(),
                        );
                    }
                    DirectTransportFrame::RiskControlResult {
                        session_id: frame_session,
                        connection_generation: frame_generation,
                        result,
                        ..
                    } => {
                        require_session(
                            &session_id,
                            connection_generation,
                            &frame_session,
                            frame_generation,
                        )?;
                        let Some(tx) = pending_risk.remove(&result.command_id) else {
                            return Err(AgentError::CloudflareTransport(
                                "unsolicited risk authority result".to_owned(),
                            ));
                        };
                        let _ = tx.send(result);
                    }
                    DirectTransportFrame::Hello { .. }
                    | DirectTransportFrame::HelloAck { .. }
                    | DirectTransportFrame::DeliveryAck { .. }
                    | DirectTransportFrame::Response { .. }
                    | DirectTransportFrame::RiskControl { .. } => {
                        return Err(AgentError::CloudflareTransport(
                            "unexpected direct transport frame from server".to_owned(),
                        ));
                    }
                }
            }
        }
    }
}

fn completed_direct_response(
    request_id: &str,
    mutating: bool,
    result: AgentResult<AgentResponse>,
) -> AgentResponse {
    match result {
        Ok(response) => response,
        Err(error) => {
            eprintln!(
                "direct transport request {request_id} failed (mutation={mutating}): {error}"
            );
            AgentResponse {
                schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                request_id: request_id.to_owned(),
                status: AgentResponseStatus::Failed,
                generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                quality: DataQuality::NotReady,
                result_schema: None,
                result: None,
                failure: Some(AgentFailure {
                    code: if mutating {
                        DIRECT_TRANSPORT_MUTATION_OUTCOME_UNKNOWN
                    } else {
                        DIRECT_TRANSPORT_QUERY_FAILED
                    }
                    .to_owned(),
                    message: if mutating {
                        "trade result is unconfirmed; reconcile the exact intent, never replay POST"
                    } else {
                        "direct transport read query failed"
                    }
                    .to_owned(),
                    retryable: !mutating,
                }),
                warnings: Vec::new(),
            }
        }
    }
}

fn direct_rejection(request_id: &str, code: &str, message: &str, retryable: bool) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request_id.to_owned(),
        status: AgentResponseStatus::Rejected,
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: code.to_owned(),
            message: message.to_owned(),
            retryable,
        }),
        warnings: Vec::new(),
    }
}

async fn send_frame<S>(sink: &mut S, frame: &DirectTransportFrame) -> AgentResult<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    frame.validate()?;
    let payload = serde_json::to_string(frame)?;
    if payload.len() > DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES {
        return Err(AgentError::CloudflareTransport(
            "direct transport payload exceeds bound".to_owned(),
        ));
    }
    sink.send(Message::Text(payload.into()))
        .await
        .map_err(|error| AgentError::CloudflareTransport(error.to_string()))
}

fn next_text_frame(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> AgentResult<DirectTransportFrame> {
    let message = message
        .ok_or_else(|| AgentError::CloudflareTransport("cloudflare WebSocket closed".to_owned()))?;
    let message = message.map_err(|error| AgentError::CloudflareTransport(error.to_string()))?;

    let text = match message {
        Message::Text(text) => text,
        Message::Close(_) => {
            return Err(AgentError::CloudflareTransport(
                "cloudflare WebSocket closed".to_owned(),
            ));
        }
        Message::Ping(_) | Message::Pong(_) => {
            return Err(AgentError::CloudflareTransport(
                "unexpected WebSocket control frame".to_owned(),
            ));
        }
        Message::Binary(_) | Message::Frame(_) => {
            return Err(AgentError::CloudflareTransport(
                "binary direct transport frames are not supported".to_owned(),
            ));
        }
    };

    if text.len() > DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES {
        return Err(AgentError::CloudflareTransport(
            "direct transport payload exceeds bound".to_owned(),
        ));
    }
    let frame: DirectTransportFrame = serde_json::from_str(text.as_str())?;
    frame.validate_incoming_envelope()?;
    Ok(frame)
}

fn require_session(
    expected_session: &str,
    expected_generation: u64,
    actual_session: &str,
    actual_generation: u64,
) -> AgentResult<()> {
    if actual_session == expected_session && actual_generation == expected_generation {
        Ok(())
    } else {
        Err(AgentError::CloudflareTransport(
            "stale or mismatched direct transport generation".to_owned(),
        ))
    }
}

fn validate_ws_url(value: &str) -> AgentResult<()> {
    if value.len() <= 512
        && value.starts_with("wss://")
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        Ok(())
    } else {
        Err(AgentError::InvalidCloudflareWsUrl)
    }
}

fn validate_runtime_id(value: &str) -> AgentResult<()> {
    if (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        Ok(())
    } else {
        Err(AgentError::InvalidCloudflareRuntimeId)
    }
}

fn random_token(prefix: &str) -> AgentResult<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| AgentError::Random(error.to_string()))?;
    let mut result = String::with_capacity(prefix.len() + bytes.len() * 2);
    result.push_str(prefix);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}")
            .map_err(|error| AgentError::CloudflareTransport(error.to_string()))?;
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloudflare_endpoint_is_wss_only_and_bounded() {
        assert!(validate_ws_url("wss://okx.example.test/runtime").is_ok());
        assert!(matches!(
            validate_ws_url("https://okx.example.test/runtime"),
            Err(AgentError::InvalidCloudflareWsUrl)
        ));
        assert!(matches!(
            validate_ws_url("wss://bad host/runtime"),
            Err(AgentError::InvalidCloudflareWsUrl)
        ));
    }

    #[test]
    fn main_transport_demonstrates_strict_profile_admission() {
        let submit = AgentOperation::SubmitPreparedExecution {
            intent_id: "intent_demo_stage4c_01234567".to_owned(),
        };
        let read = AgentOperation::AccountSummary;
        assert!(direct_operation_allowed(
            DirectRuntimeProfile::DemoAcceptance,
            Some(ExecutionRuntimeMode::DemoAcceptance),
            &submit,
        ));
        for (profile, mode) in [
            (
                DirectRuntimeProfile::Production,
                Some(ExecutionRuntimeMode::ReadOnly),
            ),
            (
                DirectRuntimeProfile::DemoAcceptance,
                Some(ExecutionRuntimeMode::ReadOnly),
            ),
            (
                DirectRuntimeProfile::Production,
                Some(ExecutionRuntimeMode::DemoAcceptance),
            ),
            (DirectRuntimeProfile::DemoAcceptance, None),
        ] {
            assert!(!direct_operation_allowed(profile, mode, &submit));
            assert!(direct_operation_allowed(profile, mode, &read));
        }
    }

    #[test]
    fn central_policy_gates_production_delivery_without_granting_trade() {
        let submit = AgentOperation::SubmitPreparedExecution {
            intent_id: "intent_central_policy_012345".to_owned(),
        };
        let p = CentralAccountPolicy {
            schema: okx_protocol::CENTRAL_ACCOUNT_POLICY_SCHEMA_V1.to_owned(),
            revision: 3,
            trading_requested: true,
            allowed_account_uids: vec!["a".repeat(64)],
            max_total_modelled_loss_usdt: "10".to_owned(),
            issued_at_ms: 1_000,
            session_generation: 7,
        };
        let admit = |policy: Option<&CentralAccountPolicy>, profile, mode, generation, now| {
            central_production_delivery_admission(
                profile, mode, &submit, policy, None, generation, now, false,
            )
            .is_ok()
        };
        assert!(admit(
            Some(&p),
            DirectRuntimeProfile::Production,
            Some(ExecutionRuntimeMode::Production),
            7,
            1_001
        ));
        assert!(!admit(
            None,
            DirectRuntimeProfile::Production,
            Some(ExecutionRuntimeMode::Production),
            7,
            1_001
        ));
        assert!(!admit(
            Some(&p),
            DirectRuntimeProfile::Production,
            Some(ExecutionRuntimeMode::ReadOnly),
            7,
            1_001
        ));
        assert!(!admit(
            Some(&p),
            DirectRuntimeProfile::Production,
            Some(ExecutionRuntimeMode::Production),
            8,
            1_001
        ));
        assert!(!admit(
            Some(&p),
            DirectRuntimeProfile::Production,
            Some(ExecutionRuntimeMode::Production),
            7,
            16_001
        ));
        let mut revoked = p;
        revoked.trading_requested = false;
        assert!(!admit(
            Some(&revoked),
            DirectRuntimeProfile::Production,
            Some(ExecutionRuntimeMode::Production),
            7,
            1_001
        ));
    }

    #[test]
    fn production_governance_v2_delivery_reports_exact_blocker() {
        use okx_protocol::account_governance::{
            AccountControlV2, AccountEnvironmentV2, AccountGovernanceV2, AccountLimitsV2,
            GOVERNANCE_SCHEMA_V2, OperatorAuthorizationV2, StrategyLimitsV2,
        };
        let submit = AgentOperation::SubmitPreparedExecution {
            intent_id: "intent_governance_delivery_20261019".to_owned(),
        };
        let mut scope = CentralGovernanceSnapshotV2 {
            state: AccountGovernanceV2 {
                schema: GOVERNANCE_SCHEMA_V2.to_owned(),
                revision: 3,
                trading_requested: true,
                operator_authorization: OperatorAuthorizationV2 {
                    id: format!("sha256:{}", "a".repeat(64)),
                    status: AuthorizationStatus::Active,
                    ceiling_usdt: "10".to_owned(),
                },
                max_total_modelled_loss_usdt: "10".to_owned(),
                accounts: vec![AccountControlV2 {
                    id: "okx_real_1beb3c684155".to_owned(),
                    label: "OKX Real".to_owned(),
                    venue: "okx".to_owned(),
                    environment: AccountEnvironmentV2::Real,
                    uid_fingerprint: "b".repeat(64),
                    identity_verified: true,
                    enabled: true,
                    limits: AccountLimitsV2 {
                        max_modelled_loss_usdt: "10".to_owned(),
                        max_order_loss_usdt: "10".to_owned(),
                        max_notional_usdt: Some("200".to_owned()),
                        max_leverage: Some("3".to_owned()),
                        max_daily_realized_loss_usdt: Some("10".to_owned()),
                        max_drawdown_ratio: Some("0.1".to_owned()),
                        allowed_instruments: vec!["DOGE-USDT-SWAP".to_owned()],
                    },
                    strategies: vec![StrategyLimitsV2 {
                        id: "real_manual_v1".to_owned(),
                        enabled: true,
                        allocated_capital_usdt: "100".to_owned(),
                        max_modelled_loss_usdt: "10".to_owned(),
                        max_daily_realized_loss_usdt: Some("10".to_owned()),
                        max_drawdown_ratio: Some("0.1".to_owned()),
                    }],
                }],
            },
            issued_at_ms: 1_000,
            session_generation: 514,
            delivery_challenge: Some("ping_valid_governance_probe".to_owned()),
        };
        assert!(scope.valid());
        let admitted = |value: Option<&CentralGovernanceSnapshotV2>, generation, now| {
            central_production_delivery_admission(
                DirectRuntimeProfile::Production,
                Some(ExecutionRuntimeMode::Production),
                &submit,
                None,
                value,
                generation,
                now,
                true,
            )
        };
        assert_eq!(admitted(Some(&scope), 514, 1_001), Ok(()));
        assert_eq!(
            admitted(None, 514, 1_001),
            Err("CENTRAL_PRODUCTION_SCOPE_NOT_DELIVERED")
        );
        assert_eq!(
            admitted(Some(&scope), 515, 1_001),
            Err("CENTRAL_GOVERNANCE_V2_GENERATION_MISMATCH")
        );
        // No cross-machine clock agreement is required when the exact
        // request-scoped challenge passed the native monotonic freshness gate.
        assert_eq!(admitted(Some(&scope), 514, 16_001), Ok(()));
        assert_eq!(
            central_production_delivery_admission(
                DirectRuntimeProfile::Production,
                Some(ExecutionRuntimeMode::Production),
                &submit,
                None,
                Some(&scope),
                514,
                1_001,
                false,
            ),
            Err("CENTRAL_GOVERNANCE_V2_CHALLENGE_NOT_FRESH")
        );
        scope.state.trading_requested = false;
        assert_eq!(
            admitted(Some(&scope), 514, 1_001),
            Err("CENTRAL_GOVERNANCE_V2_STOPPED")
        );
        assert_eq!(
            central_production_delivery_admission(
                DirectRuntimeProfile::Production,
                Some(ExecutionRuntimeMode::ReadOnly),
                &submit,
                None,
                Some(&scope),
                514,
                1_001,
                true,
            ),
            Err("NATIVE_PRODUCTION_EXECUTOR_NOT_ACTIVE")
        );
    }

    #[test]
    fn delivery_challenge_is_single_use_bounded_and_monotonic() {
        let mut pending = HashMap::new();
        let nonce = "ping_verified_once_012345";
        pending.insert(nonce.to_owned(), Instant::now());
        assert!(!consume_fresh_delivery_challenge(
            &mut pending,
            Some("ping_different")
        ));
        assert!(consume_fresh_delivery_challenge(&mut pending, Some(nonce)));
        assert!(!consume_fresh_delivery_challenge(&mut pending, Some(nonce)));
        assert!(!consume_fresh_delivery_challenge(&mut pending, None));
        pending.insert(
            nonce.to_owned(),
            Instant::now() - DELIVERY_CHALLENGE_MAX_AGE - Duration::from_millis(1),
        );
        assert!(!consume_fresh_delivery_challenge(&mut pending, Some(nonce)));
    }

    #[test]
    fn session_generation_must_match_exactly() {
        assert!(require_session("session_0123456789", 4, "session_0123456789", 4).is_ok());
        assert!(require_session("session_0123456789", 4, "session_0123456789", 3).is_err());
        assert!(require_session("session_0123456789", 4, "session_other_012345", 4).is_err());
    }

    #[test]
    fn post_mutation_internal_error_requires_exact_intent_readback_without_retry() {
        let response = completed_direct_response(
            "req_direct_submit_unsafe_0001",
            true,
            Err(AgentError::CloudflareTransport(
                "private exchange detail".to_owned(),
            )),
        );
        assert_eq!(response.status, AgentResponseStatus::Failed);
        let failure = response.failure.expect("typed uncertain mutation");
        assert_eq!(failure.code, DIRECT_TRANSPORT_MUTATION_OUTCOME_UNKNOWN);
        assert!(!failure.retryable);
        assert!(failure.message.contains("exact intent"));
        assert!(!failure.message.contains("private exchange detail"));
    }

    #[test]
    fn request_local_read_error_becomes_typed_failure() {
        let response = completed_direct_response(
            "req_direct_query_failure_0001",
            false,
            Err(AgentError::CloudflareTransport(
                "internal detail must stay local".to_owned(),
            )),
        );

        assert_eq!(response.status, AgentResponseStatus::Failed);
        let failure = response.failure.expect("typed failure");
        assert_eq!(failure.code, DIRECT_TRANSPORT_QUERY_FAILED);
        assert_eq!(failure.message, "direct transport read query failed");
        assert!(failure.retryable);
        assert!(!failure.message.contains("internal detail"));
    }
}
