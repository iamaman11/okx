//! Independent, optional CONTROL → Edge read-only Research process attestor.
//!
//! The attestor is not a second supervisor and never starts, stops or reconciles
//! Research. Its bounded owner queue is consumed by the main CONTROL loop.
//! The bearer has an independent Windows Credential Manager target and is never
//! read from the GitHub, Research or Engine credentials.

use std::{
    collections::HashSet,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use okx_host_protocol::ResearchRuntimeStatus;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
        protocol::{Message, WebSocketConfig},
    },
};
use zeroize::{Zeroize, Zeroizing};

use crate::{HostControlResult, auth::load_native_attestor_token, lifecycle_executor::HostExecutor};

const FRAME_SCHEMA: &str = "okx.control-attestor.frame/v1";
const ATTESTOR_URL: &str = "wss://okx-cloudflare-mcp.okx-794.workers.dev/attestor";
const MAX_FRAME_BYTES: usize = 8192;
const MAX_CHALLENGES_PER_SESSION: usize = 128;
const MAX_CHALLENGE_FUTURE_MS: u64 = 3000;
const STATUS_TIMEOUT: Duration = Duration::from_millis(850);
const ACK_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL_RETRY_SECONDS: u64 = 2;
const MAX_RETRY_SECONDS: u64 = 60;
const STABLE_SESSION_BACKOFF_RESET: Duration = Duration::from_secs(30);

pub struct ObservationRequest {
    reply: oneshot::Sender<Option<ResearchRuntimeStatus>>,
}

/// Runs on CONTROL's existing owner loop. This readback may observe a child
/// exiting, but it does not mutate desired state or trigger lifecycle work.
pub fn answer_observation(executor: &mut HostExecutor, request: ObservationRequest) {
    let observed = executor
        .observe_research_read_only()
        .ok()
        .and_then(|value| serde_json::from_value::<ResearchRuntimeStatus>(value).ok())
        .filter(|status| status.validate().is_ok());
    let _ = request.reply.send(observed);
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum OutgoingFrame {
    #[serde(rename = "hello_control")]
    HelloControl {
        schema: &'static str,
        connection_id: String,
    },
    #[serde(rename = "research_observation")]
    ResearchObservation {
        schema: &'static str,
        request_id: String,
        nonce: String,
        session_id: String,
        connection_generation: u64,
        research_session_id: String,
        research_connection_generation: u64,
        observed_at_ms: u64,
        status: Box<ResearchRuntimeStatus>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum IncomingFrame {
    #[serde(rename = "hello_ack")]
    HelloAck {
        schema: String,
        session_id: String,
        connection_generation: u64,
    },
    #[serde(rename = "observe_research")]
    ObserveResearch {
        schema: String,
        request_id: String,
        nonce: String,
        session_id: String,
        connection_generation: u64,
        research_session_id: String,
        research_connection_generation: u64,
        deadline_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Challenge {
    request_id: String,
    nonce: String,
    session_id: String,
    connection_generation: u64,
    research_session_id: String,
    research_connection_generation: u64,
    deadline_ms: u64,
}

impl Challenge {
    fn validate(&self, session_id: &str, generation: u64, now_ms: u64) -> Result<(), &'static str> {
        if !bounded_identifier(&self.request_id, "att_", 128)
            || !bounded_identifier(&self.nonce, "nonce_", 128)
            || !bounded_identifier(&self.research_session_id, "session_", 128)
            || self.session_id != session_id
            || self.connection_generation != generation
            || self.research_connection_generation == 0
            || self.deadline_ms <= now_ms
            || self.deadline_ms.saturating_sub(now_ms) > MAX_CHALLENGE_FUTURE_MS
        {
            return Err("ATTESTOR_CHALLENGE_INVALID");
        }
        Ok(())
    }
}

fn bounded_identifier(value: &str, prefix: &str, max_len: usize) -> bool {
    value.starts_with(prefix)
        && value.len() >= prefix.len() + 16
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn unix_millis() -> Result<u64, &'static str> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "ATTESTOR_CLOCK_INVALID")?
        .as_millis()
        .try_into()
        .map_err(|_| "ATTESTOR_CLOCK_INVALID")
}

fn parse_frame(frame: Message) -> Result<IncomingFrame, &'static str> {
    let Message::Text(payload) = frame else {
        return Err("ATTESTOR_NON_TEXT_FRAME");
    };
    if payload.len() > MAX_FRAME_BYTES {
        return Err("ATTESTOR_OVERSIZED_FRAME");
    }
    serde_json::from_str::<IncomingFrame>(&payload).map_err(|_| "ATTESTOR_INVALID_FRAME")
}

async fn send_frame<S>(sink: &mut S, frame: OutgoingFrame) -> Result<(), &'static str>
where
    S: Sink<Message> + Unpin,
{
    let payload = serde_json::to_string(&frame).map_err(|_| "ATTESTOR_ENCODE_FAILED")?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err("ATTESTOR_OVERSIZED_RESPONSE");
    }
    timeout(
        Duration::from_secs(2),
        sink.send(Message::Text(payload.into())),
    )
    .await
    .map_err(|_| "ATTESTOR_SEND_TIMEOUT")?
    .map_err(|_| "ATTESTOR_SEND_FAILED")
}

fn connection_id() -> Result<String, &'static str> {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|_| "ATTESTOR_RANDOM_FAILED")?;
    let id = nonce
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    nonce.zeroize();
    Ok(id)
}

// Keep the initial hello/ack boundary independently testable without a
// network connection. Production always supplies ACK_TIMEOUT (5 seconds).
async fn read_hello_ack<S, E>(
    reader: &mut S,
    deadline: Duration,
) -> Result<(String, u64), &'static str>
where
    S: Stream<Item = Result<Message, E>> + Unpin,
{
    let first = timeout(deadline, reader.next())
        .await
        .map_err(|_| "ATTESTOR_ACK_TIMEOUT")?
        .ok_or("ATTESTOR_ACK_CLOSED")?
        .map_err(|_| "ATTESTOR_ACK_READ_FAILED")?;
    let IncomingFrame::HelloAck {
        schema,
        session_id,
        connection_generation,
    } = parse_frame(first)?
    else {
        return Err("ATTESTOR_ACK_REQUIRED");
    };
    if schema != FRAME_SCHEMA
        || !bounded_identifier(&session_id, "control_", 128)
        || connection_generation == 0
    {
        return Err("ATTESTOR_ACK_INVALID");
    }
    Ok((session_id, connection_generation))
}

async fn run_session(
    token: &str,
    requests: &mpsc::Sender<ObservationRequest>,
) -> Result<(), &'static str> {
    let mut request = ATTESTOR_URL
        .into_client_request()
        .map_err(|_| "ATTESTOR_INVALID_URL")?;
    let header = Zeroizing::new(format!("Bearer {token}"));
    let mut authorization =
        HeaderValue::from_str(&header).map_err(|_| "ATTESTOR_INVALID_BEARER")?;
    authorization.set_sensitive(true);
    request.headers_mut().insert(AUTHORIZATION, authorization);
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME_BYTES))
        .max_frame_size(Some(MAX_FRAME_BYTES));
    let (socket, _) = timeout(
        Duration::from_secs(8),
        connect_async_with_config(request, Some(config), false),
    )
    .await
    .map_err(|_| "ATTESTOR_CONNECT_TIMEOUT")?
    .map_err(|_| "ATTESTOR_CONNECT_FAILED")?;
    let (mut sender, mut reader) = socket.split();
    send_frame(
        &mut sender,
        OutgoingFrame::HelloControl {
            schema: FRAME_SCHEMA,
            connection_id: connection_id()?,
        },
    )
    .await?;

    let (session_id, connection_generation) = read_hello_ack(&mut reader, ACK_TIMEOUT).await?;

    let mut nonces = HashSet::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                // A live socket can stay open after Edge fingerprint rotation.
                // Recheck the local credential on the existing heartbeat so
                // its next reconnect uses the replacement without restarting
                // CONTROL (or either native child process).
                ensure_current_credential(token, load_native_attestor_token())?;
                timeout(Duration::from_secs(2), sender.send(Message::Ping(Vec::new().into())))
                    .await.map_err(|_| "ATTESTOR_PING_TIMEOUT")?
                    .map_err(|_| "ATTESTOR_PING_FAILED")?;
            }
            next = reader.next() => {
                let message = next.ok_or("ATTESTOR_SOCKET_CLOSED")?
                    .map_err(|_| "ATTESTOR_SOCKET_READ_FAILED")?;
                if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                    continue;
                }
                let IncomingFrame::ObserveResearch {
                    schema, request_id, nonce, session_id: echoed_session,
                    connection_generation: echoed_generation,
                    research_session_id, research_connection_generation, deadline_ms,
                } = parse_frame(message)? else {
                    return Err("ATTESTOR_CHALLENGE_REQUIRED");
                };
                if schema != FRAME_SCHEMA { return Err("ATTESTOR_SCHEMA_INVALID"); }
                let challenge = Challenge {
                    request_id, nonce, session_id: echoed_session,
                    connection_generation: echoed_generation,
                    research_session_id, research_connection_generation, deadline_ms,
                };
                challenge.validate(&session_id, connection_generation, unix_millis()?)?;
                if nonces.len() >= MAX_CHALLENGES_PER_SESSION
                    || !nonces.insert(challenge.nonce.clone()) {
                    return Err("ATTESTOR_REPLAY_OR_LIMIT");
                }
                let (reply, result) = oneshot::channel();
                requests.try_send(ObservationRequest { reply })
                    .map_err(|_| "ATTESTOR_OWNER_QUEUE_UNAVAILABLE")?;
                let status = timeout(STATUS_TIMEOUT, result)
                    .await
                    .map_err(|_| "ATTESTOR_OWNER_TIMEOUT")?
                    .map_err(|_| "ATTESTOR_OWNER_CLOSED")?
                    .ok_or("ATTESTOR_STATUS_UNAVAILABLE")?;
                let observed_at_ms = unix_millis()?;
                if observed_at_ms >= challenge.deadline_ms {
                    return Err("ATTESTOR_CHALLENGE_EXPIRED");
                }
                send_frame(&mut sender, OutgoingFrame::ResearchObservation {
                    schema: FRAME_SCHEMA,
                    request_id: challenge.request_id,
                    nonce: challenge.nonce,
                    session_id: challenge.session_id,
                    connection_generation: challenge.connection_generation,
                    research_session_id: challenge.research_session_id,
                    research_connection_generation: challenge.research_connection_generation,
                    observed_at_ms,
                    status: Box::new(status),
                }).await?;
            }
        }
    }
}

// A long-lived session must not inherit an hour-old failure backoff after a
// normal reconnect. Quick failures retain the same bounded exponential delay.
fn retry_after_session(current: u64, elapsed: Duration) -> (u64, u64) {
    let wait = if elapsed >= STABLE_SESSION_BACKOFF_RESET {
        INITIAL_RETRY_SECONDS
    } else {
        current
    };
    (wait, wait.saturating_mul(2).min(MAX_RETRY_SECONDS))
}

/// The running CONTROL owns the native Credential Manager entry. A rotated or
/// revoked key must end this attestor-only socket, not restart CONTROL or its
/// Engine/Research children. Errors are fixed codes: never log bearer bytes.
fn ensure_current_credential(
    active: &str,
    loaded: HostControlResult<Option<Zeroizing<String>>>,
) -> Result<(), &'static str> {
    match loaded {
        Ok(Some(current)) if current.as_str() == active => Ok(()),
        Ok(Some(_)) => Err("ATTESTOR_CREDENTIAL_ROTATED"),
        Ok(None) => Err("ATTESTOR_CREDENTIAL_REVOKED"),
        Err(_) => Err("ATTESTOR_CREDENTIAL_UNAVAILABLE"),
    }
}

/// Best-effort by construction: a missing or disconnected attestor never
/// changes CONTROL lifecycle, GitHub owner, or existing Research transport.
pub async fn run(token: Zeroizing<String>, requests: mpsc::Sender<ObservationRequest>) {
    let mut retry_seconds = INITIAL_RETRY_SECONDS;
    let mut token = Some(token);
    loop {
        if requests.is_closed() {
            return;
        }
        let started = Instant::now();
        let outcome = match token.as_deref() {
            Some(token) => tokio::select! {
                outcome = run_session(token, &requests) => outcome,
                _ = requests.closed() => return,
            },
            None => Err("ATTESTOR_CREDENTIAL_REVOKED"),
        };
        if let Err(code) = outcome {
            eprintln!("CONTROL attestor unavailable: {code}; read-only evidence remains partial");
        }
        let (wait, next) = retry_after_session(retry_seconds, started.elapsed());
        retry_seconds = next;
        tokio::select! {
            _ = sleep(Duration::from_secs(wait)) => {},
            _ = requests.closed() => return,
        }
        // Re-read after every disconnected session. Keeping only the initial
        // bearer would make a replaced Credential Manager key unusable until
        // a CONTROL restart, which can also disturb its owned child processes.
        token = match load_native_attestor_token() {
            Ok(current) => current,
            Err(_) => {
                eprintln!("CONTROL attestor credential unavailable; read-only evidence remains partial");
                None
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attestor_credential_rotation_is_fail_closed_and_never_echoes_secrets() {
        let original = "a".repeat(64);
        let replacement = "b".repeat(64);
        assert_eq!(
            ensure_current_credential(&original, Ok(Some(Zeroizing::new(original.clone())))),
            Ok(())
        );
        assert_eq!(
            ensure_current_credential(&original, Ok(Some(Zeroizing::new(replacement.clone())))),
            Err("ATTESTOR_CREDENTIAL_ROTATED")
        );
        assert_eq!(
            ensure_current_credential(&original, Ok(None)),
            Err("ATTESTOR_CREDENTIAL_REVOKED")
        );
        let error = ensure_current_credential(
            &original,
            Err(crate::HostControlError::SecretStore(replacement.clone())),
        )
        .unwrap_err();
        assert_eq!(error, "ATTESTOR_CREDENTIAL_UNAVAILABLE");
        assert!(!error.contains(&original));
        assert!(!error.contains(&replacement));
    }

    #[test]
    fn attestor_reconnect_backoff_recovers_after_stable_session() {
        let mut current = INITIAL_RETRY_SECONDS;
        for expected in [2, 4, 8, 16, 32, 60, 60] {
            let (wait, next) = retry_after_session(current, Duration::from_secs(1));
            assert_eq!(wait, expected);
            current = next;
        }
        assert_eq!(current, MAX_RETRY_SECONDS);
        let (wait, next) = retry_after_session(current, STABLE_SESSION_BACKOFF_RESET);
        assert_eq!(wait, INITIAL_RETRY_SECONDS);
        assert_eq!(next, 4);
        assert_eq!(
            retry_after_session(
                current,
                STABLE_SESSION_BACKOFF_RESET - Duration::from_nanos(1)
            ),
            (MAX_RETRY_SECONDS, MAX_RETRY_SECONDS)
        );
        assert_eq!(retry_after_session(next, Duration::ZERO), (4, 8));
    }

    #[tokio::test]
    async fn dropped_control_owner_stops_attestor_without_reconnecting() {
        let (requests, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(
            timeout(
                Duration::from_millis(100),
                // Invalid HTTP header prevents any accidental outbound connection
                // if owner-closure handling ever regresses.
                run(Zeroizing::new("invalid\nheader".to_owned()), requests)
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn missing_initial_hello_ack_times_out_before_attestation() {
        // Pending means the authenticated socket never sends its first ACK;
        // no network, credential, CONTROL process, or Research state is used.
        let mut silent = futures_util::stream::pending::<Result<Message, ()>>();
        assert_eq!(ACK_TIMEOUT, Duration::from_secs(5));
        assert_eq!(
            read_hello_ack(&mut silent, Duration::from_millis(20)).await,
            Err("ATTESTOR_ACK_TIMEOUT")
        );
        let mut closed = futures_util::stream::empty::<Result<Message, ()>>();
        assert_eq!(
            read_hello_ack(&mut closed, Duration::from_millis(20)).await,
            Err("ATTESTOR_ACK_CLOSED")
        );
    }

    #[tokio::test]
    async fn initial_hello_ack_rejects_foreign_and_malformed_frames() {
        let valid = serde_json::json!({
            "type": "hello_ack",
            "schema": FRAME_SCHEMA,
            "session_id": "control_0123456789abcdef",
            "connection_generation": 7,
        });
        let mut reader =
            futures_util::stream::iter([Ok::<_, ()>(Message::Text(valid.to_string().into()))]);
        assert_eq!(
            read_hello_ack(&mut reader, Duration::from_millis(20)).await,
            Ok(("control_0123456789abcdef".into(), 7))
        );

        let mut broken = futures_util::stream::iter([Err::<Message, ()>(())]);
        assert_eq!(
            read_hello_ack(&mut broken, Duration::from_millis(20)).await,
            Err("ATTESTOR_ACK_READ_FAILED")
        );

        let challenge = valid_challenge(1_800_000_000_000);
        let wrong_first_type = serde_json::json!({
            "type": "observe_research",
            "schema": FRAME_SCHEMA,
            "request_id": challenge.request_id,
            "nonce": challenge.nonce,
            "session_id": challenge.session_id,
            "connection_generation": challenge.connection_generation,
            "research_session_id": challenge.research_session_id,
            "research_connection_generation": challenge.research_connection_generation,
            "deadline_ms": challenge.deadline_ms,
        });
        let mut wrong_schema = valid.clone();
        wrong_schema["schema"] = "foreign-schema".into();
        let mut zero_generation = valid.clone();
        zero_generation["connection_generation"] = 0.into();
        let mut invalid_session = valid.clone();
        invalid_session["session_id"] = "control_short".into();
        let mut injected_owner = valid.clone();
        injected_owner["owner"] = "CONTROL".into();
        let mut unknown_type = valid.clone();
        unknown_type["type"] = "start_research".into();
        for (frame, expected) in [
            (wrong_first_type, "ATTESTOR_ACK_REQUIRED"),
            (wrong_schema, "ATTESTOR_ACK_INVALID"),
            (zero_generation, "ATTESTOR_ACK_INVALID"),
            (invalid_session, "ATTESTOR_ACK_INVALID"),
            (injected_owner, "ATTESTOR_INVALID_FRAME"),
            (unknown_type, "ATTESTOR_INVALID_FRAME"),
        ] {
            let mut reader =
                futures_util::stream::iter([Ok::<_, ()>(Message::Text(frame.to_string().into()))]);
            assert_eq!(
                read_hello_ack(&mut reader, Duration::from_millis(20)).await,
                Err(expected)
            );
        }
        for (frame, expected) in [
            (Message::Binary(vec![0].into()), "ATTESTOR_NON_TEXT_FRAME"),
            (
                Message::Text("x".repeat(MAX_FRAME_BYTES + 1).into()),
                "ATTESTOR_OVERSIZED_FRAME",
            ),
        ] {
            let mut reader = futures_util::stream::iter([Ok::<_, ()>(frame)]);
            assert_eq!(
                read_hello_ack(&mut reader, Duration::from_millis(20)).await,
                Err(expected)
            );
        }
    }

    fn valid_challenge(now_ms: u64) -> Challenge {
        Challenge {
            request_id: "att_0123456789abcdef".into(),
            nonce: "nonce_0123456789abcdef".into(),
            session_id: "session_0123456789abcdef".into(),
            connection_generation: 7,
            research_session_id: "session_1234567890abcdef".into(),
            research_connection_generation: 9,
            deadline_ms: now_ms + 2500,
        }
    }

    #[test]
    fn challenge_is_nonce_generation_and_deadline_bound() {
        let now = 1_800_000_000_000;
        let correct = valid_challenge(now);
        assert!(correct.validate(&correct.session_id, 7, now).is_ok());
        assert!(correct.validate(&correct.session_id, 8, now).is_err());
        assert!(correct.validate("session_foreign00000000", 7, now).is_err());
        let mut stale = correct.clone();
        stale.deadline_ms = now;
        assert!(stale.validate(&stale.session_id, 7, now).is_err());
        let mut far_future = correct.clone();
        far_future.deadline_ms = now + MAX_CHALLENGE_FUTURE_MS + 1;
        assert!(far_future.validate(&far_future.session_id, 7, now).is_err());
        let mut no_research_generation = correct.clone();
        no_research_generation.research_connection_generation = 0;
        assert!(
            no_research_generation
                .validate(&no_research_generation.session_id, 7, now)
                .is_err()
        );
    }

    #[test]
    fn challenge_parser_rejects_extra_keys_and_mutating_commands() {
        let mut frame = serde_json::json!({
            "schema": FRAME_SCHEMA,
            "type": "observe_research",
            "request_id": "att_0123456789abcdef",
            "nonce": "nonce_0123456789abcdef",
            "session_id": "session_0123456789abcdef",
            "connection_generation": 7,
            "research_session_id": "session_1234567890abcdef",
            "research_connection_generation": 9,
            "deadline_ms": 1800000002500_u64,
        });
        assert!(matches!(
            parse_frame(Message::Text(frame.to_string().into())),
            Ok(IncomingFrame::ObserveResearch { .. })
        ));
        frame["trade"] = serde_json::json!(true);
        assert!(parse_frame(Message::Text(frame.to_string().into())).is_err());
        frame["type"] = serde_json::json!("start_research");
        assert!(parse_frame(Message::Text(frame.to_string().into())).is_err());
    }

    #[test]
    fn frame_parser_limits_payload_before_deserialization() {
        assert!(parse_frame(Message::Text("a".repeat(MAX_FRAME_BYTES + 1).into())).is_err());
        assert!(parse_frame(Message::Binary(vec![1, 2, 3].into())).is_err());
    }
}
