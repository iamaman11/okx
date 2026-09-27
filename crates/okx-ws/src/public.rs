use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use okx_api::OkxEnvironment;
use serde_json::Value;
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, interval, sleep},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as TungsteniteError, Message},
};

use crate::protocol::{InboundMessage, Subscription, WsArg, parse_text, subscribe_payload};

pub const RECONNECT_BACKOFF_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
const HEARTBEAT_CHECK_SECONDS: u64 = 1;
const IDLE_BEFORE_PING_SECONDS: u64 = 20;
const PONG_TIMEOUT_SECONDS: u64 = 10;
const SERVICE_UPGRADE_NOTICE_CODE: &str = "64008";

#[derive(Debug, Clone, PartialEq)]
pub enum PublicWsEvent {
    Connected {
        generation: u64,
    },
    ConnectFailed {
        retry_in_ms: u64,
        error: String,
    },
    Subscribed {
        generation: u64,
        arg: WsArg,
        connection_id: Option<String>,
    },
    Data {
        generation: u64,
        arg: WsArg,
        action: Option<String>,
        data: Vec<Value>,
    },
    Notice {
        generation: u64,
        code: Option<String>,
        message: Option<String>,
        connection_id: Option<String>,
    },
    ServerError {
        generation: u64,
        code: Option<String>,
        message: Option<String>,
        arg: Option<WsArg>,
    },
    Other {
        generation: u64,
        value: Value,
    },
    Disconnected {
        generation: u64,
        reason: String,
    },
}

#[derive(Debug, Error)]
pub enum PublicWsError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] TungsteniteError),

    #[error("websocket JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("public websocket event receiver was dropped")]
    EventReceiverClosed,

    #[error("OKX public websocket heartbeat timed out")]
    HeartbeatTimeout,

    #[error("OKX public websocket sent an unexpected binary frame")]
    UnexpectedBinaryFrame,
}

pub struct PublicWsRuntime {
    environment: OkxEnvironment,
    subscriptions: Vec<Subscription>,
    events: mpsc::Sender<PublicWsEvent>,
}

impl PublicWsRuntime {
    pub fn new(
        environment: OkxEnvironment,
        subscriptions: Vec<Subscription>,
        events: mpsc::Sender<PublicWsEvent>,
    ) -> Self {
        Self {
            environment,
            subscriptions,
            events,
        }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), PublicWsError> {
        let mut generation = 0_u64;
        let mut reconnect_attempt = 0_usize;

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }

            match connect_async(self.environment.public_ws_url()).await {
                Ok((mut socket, _response)) => {
                    generation = generation.saturating_add(1);
                    self.emit(PublicWsEvent::Connected { generation }).await?;

                    let payload = subscribe_payload(&self.subscriptions)?;
                    socket.send(Message::Text(payload.into())).await?;

                    let outcome = self
                        .run_generation(generation, &mut socket, &mut shutdown)
                        .await;

                    match outcome {
                        Ok(SessionOutcome::Shutdown) => return Ok(()),
                        Ok(SessionOutcome::Reconnect { saw_data, reason }) => {
                            self.emit(PublicWsEvent::Disconnected { generation, reason })
                                .await?;
                            if saw_data {
                                reconnect_attempt = 0;
                            } else {
                                reconnect_attempt = reconnect_attempt.saturating_add(1);
                            }
                        }
                        Err(error) => {
                            self.emit(PublicWsEvent::Disconnected {
                                generation,
                                reason: error.to_string(),
                            })
                            .await?;
                            reconnect_attempt = reconnect_attempt.saturating_add(1);
                        }
                    }
                }
                Err(error) => {
                    let delay = reconnect_delay(reconnect_attempt);
                    self.emit(PublicWsEvent::ConnectFailed {
                        retry_in_ms: delay.as_millis() as u64,
                        error: error.to_string(),
                    })
                    .await?;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if wait_or_shutdown(delay, &mut shutdown).await {
                        return Ok(());
                    }
                    continue;
                }
            }

            let delay = reconnect_delay(reconnect_attempt);
            if wait_or_shutdown(delay, &mut shutdown).await {
                return Ok(());
            }
        }
    }

    async fn run_generation<S>(
        &self,
        generation: u64,
        socket: &mut tokio_tungstenite::WebSocketStream<S>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<SessionOutcome, PublicWsError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut heartbeat = interval(Duration::from_secs(HEARTBEAT_CHECK_SECONDS));
        let mut last_received = Instant::now();
        let mut awaiting_pong_since: Option<Instant> = None;
        let mut saw_data = false;

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = socket.close(None).await;
                        return Ok(SessionOutcome::Shutdown);
                    }
                }
                maybe_message = socket.next() => {
                    let Some(message) = maybe_message else {
                        return Ok(SessionOutcome::Reconnect {
                            saw_data,
                            reason: "websocket stream ended".to_owned(),
                        });
                    };
                    let message = message?;
                    last_received = Instant::now();

                    match message {
                        Message::Text(text) => {
                            match parse_text(text.as_str())? {
                                InboundMessage::Pong => {
                                    awaiting_pong_since = None;
                                }
                                InboundMessage::Subscribed { arg, connection_id } => {
                                    self.emit(PublicWsEvent::Subscribed {
                                        generation,
                                        arg,
                                        connection_id,
                                    }).await?;
                                }
                                InboundMessage::Data { arg, action, data } => {
                                    saw_data = true;
                                    self.emit(PublicWsEvent::Data {
                                        generation,
                                        arg,
                                        action,
                                        data,
                                    }).await?;
                                }
                                InboundMessage::Notice {
                                    code,
                                    message,
                                    connection_id,
                                } => {
                                    let reconnect_for_upgrade =
                                        code.as_deref() == Some(SERVICE_UPGRADE_NOTICE_CODE);
                                    self.emit(PublicWsEvent::Notice {
                                        generation,
                                        code,
                                        message,
                                        connection_id,
                                    }).await?;
                                    if reconnect_for_upgrade {
                                        return Ok(SessionOutcome::Reconnect {
                                            saw_data,
                                            reason: "OKX service-upgrade notice".to_owned(),
                                        });
                                    }
                                }
                                InboundMessage::Error { code, message, arg } => {
                                    self.emit(PublicWsEvent::ServerError {
                                        generation,
                                        code,
                                        message,
                                        arg,
                                    }).await?;
                                }
                                InboundMessage::Other(value) => {
                                    self.emit(PublicWsEvent::Other {
                                        generation,
                                        value,
                                    }).await?;
                                }
                            }
                        }
                        Message::Ping(_) => {
                            socket.flush().await?;
                        }
                        Message::Pong(_) => {}
                        Message::Close(frame) => {
                            return Ok(SessionOutcome::Reconnect {
                                saw_data,
                                reason: frame
                                    .map(|frame| format!("server close: {}", frame.reason))
                                    .unwrap_or_else(|| "server close".to_owned()),
                            });
                        }
                        Message::Binary(_) => return Err(PublicWsError::UnexpectedBinaryFrame),
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    if let Some(sent_at) = awaiting_pong_since {
                        if sent_at.elapsed() >= Duration::from_secs(PONG_TIMEOUT_SECONDS) {
                            return Err(PublicWsError::HeartbeatTimeout);
                        }
                    } else if last_received.elapsed() >= Duration::from_secs(IDLE_BEFORE_PING_SECONDS) {
                        socket.send(Message::Text("ping".into())).await?;
                        awaiting_pong_since = Some(Instant::now());
                    }
                }
            }
        }
    }

    async fn emit(&self, event: PublicWsEvent) -> Result<(), PublicWsError> {
        self.events
            .send(event)
            .await
            .map_err(|_| PublicWsError::EventReceiverClosed)
    }
}

enum SessionOutcome {
    Shutdown,
    Reconnect { saw_data: bool, reason: String },
}

pub fn reconnect_delay(attempt: usize) -> Duration {
    let index = attempt.min(RECONNECT_BACKOFF_SECONDS.len() - 1);
    Duration::from_secs(RECONNECT_BACKOFF_SECONDS[index])
}

async fn wait_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_is_bounded() {
        assert_eq!(reconnect_delay(0), Duration::from_secs(1));
        assert_eq!(reconnect_delay(1), Duration::from_secs(5));
        assert_eq!(reconnect_delay(2), Duration::from_secs(15));
        assert_eq!(reconnect_delay(3), Duration::from_secs(30));
        assert_eq!(reconnect_delay(4), Duration::from_secs(60));
        assert_eq!(reconnect_delay(99), Duration::from_secs(60));
    }
}
