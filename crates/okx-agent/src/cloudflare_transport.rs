use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use okx_protocol::{
    AGENT_RESPONSE_SCHEMA_V1, DIRECT_TRANSPORT_FRAME_SCHEMA_V1,
    DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES, AgentFailure, AgentResponse, AgentResponseStatus,
    DataQuality, DirectTransportFrame,
};
use okx_runtime::{PrivateWsHandle, PublicWsHandle};
use tokio::{
    sync::watch,
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
    execution_runtime::ExecutionRuntime,
    market_bootstrap::MarketBootstrapper,
    query::{ObservationQueryContext, dispatch},
};

const RECONNECT_BACKOFF_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
const HEARTBEAT_INTERVAL_SECONDS: u64 = 15;
const HEARTBEAT_DEADLINE_SECONDS: u64 = 5;
const HEARTBEAT_IDLE_RESET_SECONDS: u64 = 24 * 60 * 60;
const MAX_INFLIGHT_READ_QUERIES: usize = 8;
const DIRECT_TRANSPORT_MUTATION_REJECTED: &str = "DIRECT_TRANSPORT_MUTATION_REJECTED";
const DIRECT_TRANSPORT_BUSY: &str = "DIRECT_TRANSPORT_BUSY";

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
    ) -> AgentResult<Self> {
        validate_ws_url(&ws_url)?;
        validate_runtime_id(&runtime_id)?;
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

#[derive(Clone, Copy)]
pub struct CloudflareQueryRuntimeContext<'a> {
    pub public_ws: &'a PublicWsHandle,
    pub market: &'a MarketBootstrapper,
    pub account: Option<&'a AccountBootstrapper>,
    pub private_ws: Option<&'a PrivateWsHandle>,
    pub execution: Option<&'a ExecutionRuntime>,
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
    let mut in_flight: FuturesUnordered<BoxFuture<'_, (String, AgentResult<AgentResponse>)>> =
        FuturesUnordered::new();

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
            completed = in_flight.next(), if !in_flight.is_empty() => {
                let Some((_request_id, result)) = completed else {
                    continue;
                };
                let response = result?;
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
                        ..
                    } => {
                        require_session(
                            &session_id,
                            connection_generation,
                            &frame_session,
                            frame_generation,
                        )?;
                        request.validate()?;

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

                        if !request.operation.direct_transport_read_only() {
                            let response = direct_rejection(
                                &request.request_id,
                                DIRECT_TRANSPORT_MUTATION_REJECTED,
                                "mutation-capable operations are not accepted on the direct ChatGPT transport",
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

                        if in_flight.len() >= MAX_INFLIGHT_READ_QUERIES {
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

                        let query_context = ObservationQueryContext::live_with_execution(
                            context.public_ws,
                            context.market,
                            None,
                            context.account,
                            context.private_ws,
                            context.execution,
                        );
                        let generated_at =
                            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
                        let response_request_id = request.request_id.clone();
                        in_flight.push(
                            async move {
                                let result =
                                    dispatch(&request, query_context, &generated_at).await;
                                (response_request_id, result)
                            }
                            .boxed(),
                        );
                    }
                    DirectTransportFrame::Hello { .. }
                    | DirectTransportFrame::HelloAck { .. }
                    | DirectTransportFrame::DeliveryAck { .. }
                    | DirectTransportFrame::Response { .. } => {
                        return Err(AgentError::CloudflareTransport(
                            "unexpected direct transport frame from server".to_owned(),
                        ));
                    }
                }
            }
        }
    }
}

fn direct_rejection(
    request_id: &str,
    code: &str,
    message: &str,
    retryable: bool,
) -> AgentResponse {
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
    frame.validate()?;
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
    fn session_generation_must_match_exactly() {
        assert!(require_session("session_0123456789", 4, "session_0123456789", 4).is_ok());
        assert!(require_session("session_0123456789", 4, "session_0123456789", 3).is_err());
        assert!(require_session("session_0123456789", 4, "session_other_012345", 4).is_err());
    }
}
