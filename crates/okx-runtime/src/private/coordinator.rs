use std::{sync::Arc, time::Duration};

use chrono::Utc;
use okx_api::{BalanceSnapshot, Credentials, OkxEnvironment, PendingOrder, Position};
use okx_ws::{PrivateInboundMessage, PrivateWsConnection, PrivateWsError};
use tokio::{
    sync::{RwLock, watch},
    time::{Instant, interval, sleep_until},
};

use super::{
    PrivateRuntimeError,
    state::{
        PrivateConvergenceCursor, PrivateConvergenceError, PrivateConvergenceWindow,
        PrivateRuntimeState, PrivateWsEvent, PrivateWsStatus, baseline_private_subscriptions,
    },
};

pub const PRIVATE_RECONNECT_BACKOFF_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
const HEARTBEAT_TICK_SECONDS: u64 = 1;
const IDLE_BEFORE_PING_SECONDS: u64 = 20;
const PONG_TIMEOUT_SECONDS: u64 = 10;
const SERVICE_UPGRADE_NOTICE_CODE: &str = "64008";

pub struct PrivateWsHandle {
    state: Arc<RwLock<PrivateRuntimeState>>,
}

pub struct PrivateWsCoordinator {
    environment: OkxEnvironment,
    credentials: Credentials,
    state: Arc<RwLock<PrivateRuntimeState>>,
    generation: u64,
}

#[derive(Debug)]
enum GenerationOutcome {
    Shutdown,
    Reconnect { saw_data: bool, reason: String },
}

impl PrivateWsHandle {
    pub async fn status(&self) -> PrivateWsStatus {
        self.state.read().await.status()
    }

    pub fn state(&self) -> Arc<RwLock<PrivateRuntimeState>> {
        Arc::clone(&self.state)
    }

    pub async fn convergence_cursor(
        &self,
    ) -> Result<PrivateConvergenceCursor, PrivateConvergenceError> {
        self.state.read().await.convergence_cursor()
    }

    pub async fn convergence_window(
        &self,
        cursor: PrivateConvergenceCursor,
    ) -> Result<PrivateConvergenceWindow, PrivateConvergenceError> {
        self.state.read().await.convergence_window(cursor)
    }
}

impl PrivateWsCoordinator {
    pub fn new(environment: OkxEnvironment, credentials: Credentials) -> (Self, PrivateWsHandle) {
        let state = Arc::new(RwLock::new(PrivateRuntimeState::new()));
        (
            Self {
                environment,
                credentials,
                state: Arc::clone(&state),
                generation: 0,
            },
            PrivateWsHandle { state },
        )
    }

    pub async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PrivateRuntimeError> {
        let mut reconnect_attempt = 0_usize;

        loop {
            if *shutdown.borrow() {
                self.state
                    .write()
                    .await
                    .set_disconnected(false, "runtime shutdown");
                return Ok(());
            }

            self.state.write().await.set_connecting();
            let outcome = match PrivateWsConnection::connect(self.environment).await {
                Ok(mut connection) => {
                    self.generation = self.generation.saturating_add(1);
                    self.state.write().await.begin_generation(self.generation);

                    match self.run_generation(&mut connection, &mut shutdown).await {
                        Ok(outcome) => outcome,
                        Err(error) => GenerationOutcome::Reconnect {
                            saw_data: false,
                            reason: error.to_string(),
                        },
                    }
                }
                Err(error) => GenerationOutcome::Reconnect {
                    saw_data: false,
                    reason: error.to_string(),
                },
            };

            match outcome {
                GenerationOutcome::Shutdown => {
                    self.state
                        .write()
                        .await
                        .set_disconnected(false, "runtime shutdown");
                    return Ok(());
                }
                GenerationOutcome::Reconnect { saw_data, reason } => {
                    self.state.write().await.set_disconnected(true, reason);
                    if saw_data {
                        reconnect_attempt = 0;
                    }
                }
            }

            let delay = private_reconnect_delay(reconnect_attempt);
            if backoff_until(Instant::now() + delay, &mut shutdown).await {
                self.state
                    .write()
                    .await
                    .set_disconnected(false, "runtime shutdown");
                return Ok(());
            }
            reconnect_attempt = reconnect_attempt.saturating_add(1);
        }
    }

    async fn run_generation(
        &mut self,
        connection: &mut PrivateWsConnection,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<GenerationOutcome, PrivateRuntimeError> {
        let timestamp = Utc::now().timestamp().max(0).to_string();
        let login = self.credentials.websocket_login_material(&timestamp)?;
        connection.login(&login).await?;

        let mut heartbeat = interval(Duration::from_secs(HEARTBEAT_TICK_SECONDS));
        let mut last_inbound = Instant::now();
        let mut awaiting_pong_since: Option<Instant> = None;
        let mut subscriptions_sent = false;
        let mut saw_data = false;

        loop {
            tokio::select! {
                message = connection.next() => {
                    last_inbound = Instant::now();
                    match message {
                        Err(error) => {
                            return Ok(GenerationOutcome::Reconnect {
                                saw_data,
                                reason: error.to_string(),
                            });
                        }
                        Ok(None) => {
                            return Ok(GenerationOutcome::Reconnect {
                                saw_data,
                                reason: "OKX private websocket closed".to_owned(),
                            });
                        }
                        Ok(Some(message)) => match message {
                            PrivateInboundMessage::Pong => {
                                awaiting_pong_since = None;
                            }
                            PrivateInboundMessage::Login {
                                code,
                                message,
                                connection_id,
                            } => {
                                if code != "0" {
                                    return Ok(GenerationOutcome::Reconnect {
                                        saw_data,
                                        reason: format!(
                                            "OKX private websocket login failed code={code} message={message}"
                                        ),
                                    });
                                }
                                self.state
                                    .write()
                                    .await
                                    .acknowledge_login(connection_id);

                                if !subscriptions_sent {
                                    let subscriptions = baseline_private_subscriptions();
                                    connection.subscribe(&subscriptions).await?;
                                    subscriptions_sent = true;
                                }
                            }
                            PrivateInboundMessage::Subscribed {
                                arg,
                                connection_id,
                            } => {
                                if !self.state.read().await.logged_in {
                                    return Ok(GenerationOutcome::Reconnect {
                                        saw_data,
                                        reason: "private subscription acknowledged before login".to_owned(),
                                    });
                                }
                                self.state
                                    .write()
                                    .await
                                    .acknowledge_subscription(&arg, connection_id);
                            }
                            PrivateInboundMessage::Unsubscribed { .. } => {
                                return Ok(GenerationOutcome::Reconnect {
                                    saw_data,
                                    reason: "required private subscription was removed".to_owned(),
                                });
                            }
                            PrivateInboundMessage::Error {
                                code,
                                message,
                                arg: _,
                                connection_id: _,
                            } => {
                                return Ok(GenerationOutcome::Reconnect {
                                    saw_data,
                                    reason: format!(
                                        "OKX private websocket error code={} message={}",
                                        code.as_deref().unwrap_or("<none>"),
                                        message.as_deref().unwrap_or("<none>")
                                    ),
                                });
                            }
                            PrivateInboundMessage::Notice {
                                code,
                                message,
                                connection_id: _,
                            } => {
                                if code.as_deref() == Some(SERVICE_UPGRADE_NOTICE_CODE) {
                                    return Ok(GenerationOutcome::Reconnect {
                                        saw_data,
                                        reason: format!(
                                            "OKX service-upgrade notice: {}",
                                            message.as_deref().unwrap_or("<none>")
                                        ),
                                    });
                                }
                            }
                            PrivateInboundMessage::Data { arg, data } => {
                                if !self.state.read().await.logged_in {
                                    return Ok(GenerationOutcome::Reconnect {
                                        saw_data,
                                        reason: "private data arrived before login".to_owned(),
                                    });
                                }

                                let event = match arg.channel {
                                    okx_ws::PrivateChannel::Account => {
                                        let updates = data
                                            .into_iter()
                                            .map(serde_json::from_value::<BalanceSnapshot>)
                                            .collect::<Result<Vec<_>, _>>()?;
                                        PrivateWsEvent::Account(updates)
                                    }
                                    okx_ws::PrivateChannel::Positions => {
                                        let updates = data
                                            .into_iter()
                                            .map(serde_json::from_value::<Position>)
                                            .collect::<Result<Vec<_>, _>>()?;
                                        PrivateWsEvent::Positions(updates)
                                    }
                                    okx_ws::PrivateChannel::Orders => {
                                        let updates = data
                                            .into_iter()
                                            .map(serde_json::from_value::<PendingOrder>)
                                            .collect::<Result<Vec<_>, _>>()?;
                                        PrivateWsEvent::Orders(updates)
                                    }
                                };

                                saw_data = true;
                                self.state
                                    .write()
                                    .await
                                    .observe_event(event, now_ms());
                            }
                            PrivateInboundMessage::Other(_) => {}
                        },
                    }
                }
                _ = heartbeat.tick() => {
                    if let Some(sent_at) = awaiting_pong_since {
                        if sent_at.elapsed() >= Duration::from_secs(PONG_TIMEOUT_SECONDS) {
                            return Ok(GenerationOutcome::Reconnect {
                                saw_data,
                                reason: "OKX private application heartbeat timed out".to_owned(),
                            });
                        }
                    } else if last_inbound.elapsed() >= Duration::from_secs(IDLE_BEFORE_PING_SECONDS) {
                        connection.send_application_ping().await?;
                        awaiting_pong_since = Some(Instant::now());
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = connection.close().await;
                        return Ok(GenerationOutcome::Shutdown);
                    }
                }
            }
        }
    }
}

pub fn private_reconnect_delay(attempt: usize) -> Duration {
    let index = attempt.min(PRIVATE_RECONNECT_BACKOFF_SECONDS.len() - 1);
    Duration::from_secs(PRIVATE_RECONNECT_BACKOFF_SECONDS[index])
}

async fn backoff_until(deadline: Instant, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep_until(deadline) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

fn now_ms() -> u64 {
    Utc::now().timestamp_millis().max(0) as u64
}

impl From<PrivateWsError> for PrivateRuntimeError {
    fn from(error: PrivateWsError) -> Self {
        Self::WebSocket(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_reconnect_backoff_is_bounded() {
        assert_eq!(private_reconnect_delay(0), Duration::from_secs(1));
        assert_eq!(private_reconnect_delay(1), Duration::from_secs(5));
        assert_eq!(private_reconnect_delay(2), Duration::from_secs(15));
        assert_eq!(private_reconnect_delay(3), Duration::from_secs(30));
        assert_eq!(private_reconnect_delay(4), Duration::from_secs(60));
        assert_eq!(private_reconnect_delay(99), Duration::from_secs(60));
    }
}
