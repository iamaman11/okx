use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{SecondsFormat, Utc};
use okx_api::{
    InstrumentType, OkxEnvironment, PublicFundingRate, PublicIndexTicker, PublicInstrument,
    PublicMarkPrice, PublicOpenInterest, PublicTicker,
};
use okx_observation::{
    BookLevelUpdate, FundingRequirement, InstrumentRulesSnapshot, LiveMarketSnapshot,
    MarketReadiness, MarketReadinessReport, MarketStreamError, MarketStreamState, OrderBookMessage,
    ReferenceError, ReferenceRegistry,
};
use okx_ws::{
    InboundMessage, PublicChannel, PublicWsConnection, PublicWsError, Subscription, WsArg,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    sync::{RwLock, mpsc, watch},
    time::{Instant, interval, sleep_until},
};

pub const RECONNECT_BACKOFF_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
pub const PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2: &str = "okx.snapshot-quality/v2";
const HEARTBEAT_TICK_SECONDS: u64 = 1;
const IDLE_BEFORE_PING_SECONDS: u64 = 20;
const PONG_TIMEOUT_SECONDS: u64 = 10;
const SERVICE_UPGRADE_NOTICE_CODE: &str = "64008";
const COMMAND_CAPACITY: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PublicConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicQualitySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub quality: MarketReadiness,
    pub reference_generation: String,
    pub reference_source_received_at: String,
    pub market_mode: String,
    pub persistent_ws_connected: bool,
    pub connection_state: PublicConnectionState,
    pub connection_generation: u64,
    pub connection_id_fingerprint: Option<String>,
    pub acknowledged_subscriptions: usize,
    pub sequence_continuity_proven: bool,
    pub order_book_seq_id: Option<i64>,
    pub order_book_exchange_timestamp_ms: Option<String>,
    pub oldest_required_receive_ms: Option<u64>,
    pub reason: String,
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub struct PublicRuntimeState {
    reference: ReferenceRegistry,
    markets: BTreeMap<String, MarketStreamState>,
    connection_state: PublicConnectionState,
    generation: u64,
    connection_id: Option<String>,
    acknowledged_subscriptions: BTreeSet<Subscription>,
    last_error: Option<String>,
}

#[derive(Clone)]
pub struct PublicWsHandle {
    commands: mpsc::Sender<CoordinatorCommand>,
    state: Arc<RwLock<PublicRuntimeState>>,
}

pub struct PublicWsCoordinator {
    environment: OkxEnvironment,
    state: Arc<RwLock<PublicRuntimeState>>,
    commands: mpsc::Receiver<CoordinatorCommand>,
    demands: BTreeSet<String>,
    generation: u64,
}

#[derive(Debug)]
enum CoordinatorCommand {
    DemandInstrument(String),
}

#[derive(Debug)]
enum GenerationOutcome {
    Shutdown,
    Reconnect { saw_data: bool, reason: String },
}

#[derive(Debug)]
enum LoopAction {
    Message(Result<Option<InboundMessage>, PublicWsError>),
    Command(Option<CoordinatorCommand>),
    HeartbeatTick,
    Shutdown,
}

#[derive(Debug, Error)]
pub enum PublicRuntimeError {
    #[error("public websocket transport error: {0}")]
    WebSocket(#[from] PublicWsError),

    #[error("public websocket JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("reference data error: {0}")]
    Reference(#[from] ReferenceError),

    #[error("streaming market state error: {0}")]
    Stream(#[from] MarketStreamError),

    #[error("public runtime command channel is closed")]
    CommandChannelClosed,

    #[error("public runtime command queue is full")]
    CommandQueueFull,

    #[error("system clock is before Unix epoch")]
    ClockBeforeEpoch,

    #[error("instrument '{0}' is not present in reference data")]
    InstrumentNotFound(String),

    #[error("instrument '{0}' has no underlying/index id in reference data")]
    MissingUnderlying(String),

    #[error("instrument '{instrument_id}' has unknown funding semantics")]
    UnknownFundingSemantics { instrument_id: String },

    #[error("books message action is missing or unsupported: {0:?}")]
    UnsupportedBookAction(Option<String>),

    #[error("websocket data for channel {channel:?} is missing instId")]
    MissingChannelInstrument { channel: PublicChannel },

    #[error("live market state for instrument '{0}' is not initialized")]
    MarketStateNotInitialized(String),

    #[error("current live state for instrument '{0}' is not FRESH")]
    MarketNotFresh(String),
}

impl PublicRuntimeState {
    pub fn new(reference: ReferenceRegistry) -> Self {
        Self {
            reference,
            markets: BTreeMap::new(),
            connection_state: PublicConnectionState::Disconnected,
            generation: 0,
            connection_id: None,
            acknowledged_subscriptions: BTreeSet::new(),
            last_error: None,
        }
    }

    pub fn connection_state(&self) -> PublicConnectionState {
        self.connection_state
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn connection_id(&self) -> Option<&str> {
        self.connection_id.as_deref()
    }

    pub fn reference(&self) -> &ReferenceRegistry {
        &self.reference
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn acknowledged_subscription_count(&self) -> usize {
        self.acknowledged_subscriptions.len()
    }

    fn begin_generation(&mut self, generation: u64, demands: &BTreeSet<String>) {
        self.connection_state = PublicConnectionState::Connected;
        self.generation = generation;
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
        self.last_error = None;

        let reference_generation = self.reference.generation().as_str().to_owned();
        for instrument_id in demands {
            if self.reference.get(instrument_id).is_some() {
                match self.markets.get_mut(instrument_id) {
                    Some(state) => state.reset_generation(generation, reference_generation.clone()),
                    None => {
                        self.markets.insert(
                            instrument_id.clone(),
                            MarketStreamState::new(
                                instrument_id.clone(),
                                generation,
                                reference_generation.clone(),
                            ),
                        );
                    }
                }
            }
        }
    }

    fn set_disconnected(&mut self, reconnecting: bool, reason: impl Into<String>) {
        self.connection_state = if reconnecting {
            PublicConnectionState::Reconnecting
        } else {
            PublicConnectionState::Disconnected
        };
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
        self.last_error = Some(reason.into());
    }

    fn set_connecting(&mut self) {
        self.connection_state = PublicConnectionState::Connecting;
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
    }

    fn acknowledge(&mut self, arg: &WsArg, connection_id: Option<String>) {
        self.acknowledged_subscriptions
            .insert(subscription_from_arg(arg));
        if connection_id.is_some() {
            self.connection_id = connection_id;
        }
    }

    fn unacknowledge(&mut self, arg: &WsArg) {
        self.acknowledged_subscriptions
            .remove(&subscription_from_arg(arg));
    }

    fn subscriptions_complete(&self, instrument_id: &str) -> bool {
        let Ok(required) = required_subscriptions(&self.reference, instrument_id) else {
            return false;
        };
        baseline_subscriptions()
            .into_iter()
            .chain(required)
            .all(|subscription| self.acknowledged_subscriptions.contains(&subscription))
    }

    pub fn readiness(
        &self,
        instrument_id: &str,
        now_ms: u64,
        max_age_ms: u64,
        rest_fallback_available: bool,
    ) -> Result<MarketReadinessReport, PublicRuntimeError> {
        let market = self.markets.get(instrument_id).ok_or_else(|| {
            PublicRuntimeError::MarketStateNotInitialized(instrument_id.to_owned())
        })?;
        Ok(market.readiness(
            &self.reference,
            now_ms,
            max_age_ms,
            self.connection_state == PublicConnectionState::Connected,
            self.subscriptions_complete(instrument_id),
            rest_fallback_available,
        ))
    }

    pub fn quality_snapshot(
        &self,
        instrument_id: &str,
        now_ms: u64,
        max_age_ms: u64,
        rest_fallback_available: bool,
    ) -> Result<PublicQualitySnapshot, PublicRuntimeError> {
        let market = self.markets.get(instrument_id).ok_or_else(|| {
            PublicRuntimeError::MarketStateNotInitialized(instrument_id.to_owned())
        })?;
        let readiness =
            self.readiness(instrument_id, now_ms, max_age_ms, rest_fallback_available)?;
        Ok(PublicQualitySnapshot {
            schema: PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2.to_owned(),
            instrument_id: instrument_id.to_owned(),
            quality: readiness.quality,
            reference_generation: self.reference.generation().as_str().to_owned(),
            reference_source_received_at: self.reference.source_received_at().to_owned(),
            market_mode: "websocket".to_owned(),
            persistent_ws_connected: self.connection_state == PublicConnectionState::Connected,
            connection_state: self.connection_state,
            connection_generation: self.generation,
            connection_id_fingerprint: self.connection_id.as_deref().map(connection_fingerprint),
            acknowledged_subscriptions: self.acknowledged_subscriptions.len(),
            sequence_continuity_proven: readiness.sequence_continuity_proven,
            order_book_seq_id: market.order_book_seq_id(),
            order_book_exchange_timestamp_ms: market
                .order_book_exchange_timestamp_ms()
                .map(ToOwned::to_owned),
            oldest_required_receive_ms: readiness.oldest_required_receive_ms,
            reason: readiness.reason,
            last_error: self.last_error.clone(),
        })
    }

    pub fn fresh_snapshot(
        &self,
        instrument_id: &str,
        now_ms: u64,
        max_age_ms: u64,
        source_received_at: impl Into<String>,
    ) -> Result<LiveMarketSnapshot, PublicRuntimeError> {
        let readiness = self.readiness(instrument_id, now_ms, max_age_ms, false)?;
        if readiness.quality != MarketReadiness::Fresh {
            return Err(PublicRuntimeError::MarketNotFresh(instrument_id.to_owned()));
        }
        self.markets
            .get(instrument_id)
            .ok_or_else(|| PublicRuntimeError::MarketStateNotInitialized(instrument_id.to_owned()))?
            .publish(&self.reference, source_received_at)
            .map_err(Into::into)
    }
}

impl PublicWsHandle {
    pub async fn demand_instrument(
        &self,
        instrument_id: impl Into<String>,
    ) -> Result<(), PublicRuntimeError> {
        let instrument_id = instrument_id.into();
        if self.state.read().await.markets.contains_key(&instrument_id) {
            return Ok(());
        }

        self.commands
            .try_send(CoordinatorCommand::DemandInstrument(instrument_id.clone()))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Closed(_) => PublicRuntimeError::CommandChannelClosed,
                mpsc::error::TrySendError::Full(_) => PublicRuntimeError::CommandQueueFull,
            })?;

        let mut state = self.state.write().await;
        if state.reference.get(&instrument_id).is_some()
            && !state.markets.contains_key(&instrument_id)
        {
            let generation = state.generation;
            let reference_generation = state.reference.generation().as_str().to_owned();
            state.markets.insert(
                instrument_id.clone(),
                MarketStreamState::new(instrument_id, generation, reference_generation),
            );
        }
        Ok(())
    }

    pub async fn reference_snapshot(&self) -> ReferenceRegistry {
        self.state.read().await.reference.clone()
    }

    pub async fn instrument_rules(&self, instrument_id: &str) -> Option<InstrumentRulesSnapshot> {
        self.state
            .read()
            .await
            .reference
            .instrument_rules(instrument_id)
    }

    pub async fn quality_snapshot(
        &self,
        instrument_id: &str,
        now_ms: u64,
        max_age_ms: u64,
        rest_fallback_available: bool,
    ) -> Result<PublicQualitySnapshot, PublicRuntimeError> {
        self.state.read().await.quality_snapshot(
            instrument_id,
            now_ms,
            max_age_ms,
            rest_fallback_available,
        )
    }

    pub async fn fresh_snapshot(
        &self,
        instrument_id: &str,
        now_ms: u64,
        max_age_ms: u64,
        source_received_at: impl Into<String>,
    ) -> Result<LiveMarketSnapshot, PublicRuntimeError> {
        self.state.read().await.fresh_snapshot(
            instrument_id,
            now_ms,
            max_age_ms,
            source_received_at,
        )
    }

    pub fn state(&self) -> Arc<RwLock<PublicRuntimeState>> {
        Arc::clone(&self.state)
    }
}

impl PublicWsCoordinator {
    pub fn new(
        environment: OkxEnvironment,
        reference: ReferenceRegistry,
    ) -> (Self, PublicWsHandle) {
        let state = Arc::new(RwLock::new(PublicRuntimeState::new(reference)));
        let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
        (
            Self {
                environment,
                state: Arc::clone(&state),
                commands: commands_rx,
                demands: BTreeSet::new(),
                generation: 0,
            },
            PublicWsHandle {
                commands: commands_tx,
                state,
            },
        )
    }

    pub async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PublicRuntimeError> {
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
            let outcome = match PublicWsConnection::connect(self.environment).await {
                Ok(mut connection) => {
                    self.generation = self.generation.saturating_add(1);
                    self.state
                        .write()
                        .await
                        .begin_generation(self.generation, &self.demands);

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

            let delay = reconnect_delay(reconnect_attempt);
            if self
                .backoff_until(Instant::now() + delay, &mut shutdown)
                .await?
            {
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
        connection: &mut PublicWsConnection,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<GenerationOutcome, PublicRuntimeError> {
        let mut pending = BTreeSet::new();
        let mut failed = BTreeSet::new();
        let mut heartbeat = interval(Duration::from_secs(HEARTBEAT_TICK_SECONDS));
        let mut last_inbound = Instant::now();
        let mut awaiting_pong_since: Option<Instant> = None;
        let mut saw_data = false;
        let mut commands_open = true;

        self.reconcile_subscriptions(connection, &mut pending, &failed)
            .await?;

        loop {
            let action = tokio::select! {
                message = connection.next() => LoopAction::Message(message),
                command = self.commands.recv(), if commands_open => LoopAction::Command(command),
                _ = heartbeat.tick() => LoopAction::HeartbeatTick,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        LoopAction::Shutdown
                    } else {
                        continue;
                    }
                }
            };

            match action {
                LoopAction::Shutdown => {
                    let _ = connection.close().await;
                    return Ok(GenerationOutcome::Shutdown);
                }
                LoopAction::Command(Some(command)) => {
                    self.apply_command(command).await;
                    self.reconcile_subscriptions(connection, &mut pending, &failed)
                        .await?;
                }
                LoopAction::Command(None) => {
                    commands_open = false;
                }
                LoopAction::HeartbeatTick => {
                    if let Some(sent_at) = awaiting_pong_since {
                        if sent_at.elapsed() >= Duration::from_secs(PONG_TIMEOUT_SECONDS) {
                            return Ok(GenerationOutcome::Reconnect {
                                saw_data,
                                reason: "OKX application heartbeat timed out".to_owned(),
                            });
                        }
                    } else if last_inbound.elapsed()
                        >= Duration::from_secs(IDLE_BEFORE_PING_SECONDS)
                    {
                        connection.send_application_ping().await?;
                        awaiting_pong_since = Some(Instant::now());
                    }
                }
                LoopAction::Message(Err(error)) => {
                    return Ok(GenerationOutcome::Reconnect {
                        saw_data,
                        reason: error.to_string(),
                    });
                }
                LoopAction::Message(Ok(None)) => {
                    return Ok(GenerationOutcome::Reconnect {
                        saw_data,
                        reason: "OKX public websocket closed".to_owned(),
                    });
                }
                LoopAction::Message(Ok(Some(message))) => {
                    last_inbound = Instant::now();
                    match message {
                        InboundMessage::Pong => {
                            awaiting_pong_since = None;
                        }
                        InboundMessage::Subscribed { arg, connection_id } => {
                            let subscription = subscription_from_arg(&arg);
                            pending.remove(&subscription);
                            failed.remove(&subscription);
                            self.state.write().await.acknowledge(&arg, connection_id);
                        }
                        InboundMessage::Unsubscribed {
                            arg,
                            connection_id: _,
                        } => {
                            let subscription = subscription_from_arg(&arg);
                            pending.remove(&subscription);
                            self.state.write().await.unacknowledge(&arg);
                            self.reconcile_subscriptions(connection, &mut pending, &failed)
                                .await?;
                        }
                        InboundMessage::Error { code, message, arg } => {
                            if let Some(arg) = arg {
                                let subscription = subscription_from_arg(&arg);
                                pending.remove(&subscription);
                                failed.insert(subscription);
                            }
                            self.state.write().await.last_error = Some(format!(
                                "OKX websocket error code={} message={}",
                                code.as_deref().unwrap_or("<none>"),
                                message.as_deref().unwrap_or("<none>")
                            ));
                        }
                        InboundMessage::Notice {
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
                        InboundMessage::Data { arg, action, data } => {
                            saw_data = true;
                            match self.apply_data(self.generation, arg, action, data).await {
                                Ok(true) => {
                                    return Ok(GenerationOutcome::Reconnect {
                                        saw_data,
                                        reason: "reference data changed; rebuilding one coherent generation"
                                            .to_owned(),
                                    });
                                }
                                Ok(false) => {}
                                Err(error) => {
                                    return Ok(GenerationOutcome::Reconnect {
                                        saw_data,
                                        reason: error.to_string(),
                                    });
                                }
                            }
                        }
                        InboundMessage::Other(_) => {}
                    }
                }
            }
        }
    }

    async fn reconcile_subscriptions(
        &self,
        connection: &mut PublicWsConnection,
        pending: &mut BTreeSet<Subscription>,
        failed: &BTreeSet<Subscription>,
    ) -> Result<(), PublicRuntimeError> {
        let desired = {
            let state = self.state.read().await;
            desired_subscriptions(&state.reference, &self.demands)
        };
        let observed = self.state.read().await.acknowledged_subscriptions.clone();

        let additions: Vec<_> = desired
            .difference(&observed)
            .filter(|subscription| !pending.contains(*subscription))
            .filter(|subscription| !failed.contains(*subscription))
            .cloned()
            .collect();

        if !additions.is_empty() {
            connection.subscribe(&additions).await?;
            pending.extend(additions);
        }
        Ok(())
    }

    async fn apply_command(&mut self, command: CoordinatorCommand) {
        match command {
            CoordinatorCommand::DemandInstrument(instrument_id) => {
                self.demands.insert(instrument_id.clone());
                let mut state = self.state.write().await;

                let generation = state.generation;
                let reference_generation = state.reference.generation().as_str().to_owned();
                let instrument = state.reference.get(&instrument_id);
                if let Some(instrument) = instrument {
                    if instrument.funding_requirement == FundingRequirement::Unknown {
                        state.last_error = Some(format!(
                            "instrument '{instrument_id}' has unknown funding semantics"
                        ));
                    }
                    if !state.markets.contains_key(&instrument_id) {
                        state.markets.insert(
                            instrument_id.clone(),
                            MarketStreamState::new(instrument_id, generation, reference_generation),
                        );
                    }
                }
            }
        }
    }

    async fn apply_data(
        &mut self,
        generation: u64,
        arg: WsArg,
        action: Option<String>,
        data: Vec<serde_json::Value>,
    ) -> Result<bool, PublicRuntimeError> {
        let received_at_ms = now_ms()?;
        let received_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let mut state = self.state.write().await;

        match arg.channel {
            PublicChannel::Instruments => {
                let updates: Vec<PublicInstrument> = data
                    .into_iter()
                    .map(serde_json::from_value)
                    .collect::<Result<_, _>>()?;
                Ok(state.reference.apply_public_updates(received_at, updates)?)
            }
            PublicChannel::Tickers => {
                for value in data {
                    let update: PublicTicker = serde_json::from_value(value)?;
                    if let Some(market) = state.markets.get_mut(&update.instrument_id) {
                        market.apply_ticker(generation, received_at_ms, update)?;
                    }
                }
                Ok(false)
            }
            PublicChannel::MarkPrice => {
                for value in data {
                    let update: PublicMarkPrice = serde_json::from_value(value)?;
                    if let Some(market) = state.markets.get_mut(&update.instrument_id) {
                        market.apply_mark_price(generation, received_at_ms, update)?;
                    }
                }
                Ok(false)
            }
            PublicChannel::IndexTickers => {
                for value in data {
                    let update: PublicIndexTicker = serde_json::from_value(value)?;
                    let targets: Vec<_> = state
                        .markets
                        .keys()
                        .filter(|instrument_id| {
                            state
                                .reference
                                .get(instrument_id)
                                .and_then(|instrument| instrument.underlying.as_deref())
                                == Some(update.instrument_id.as_str())
                        })
                        .cloned()
                        .collect();
                    for instrument_id in targets {
                        if let Some(market) = state.markets.get_mut(&instrument_id) {
                            market.apply_index_ticker(
                                generation,
                                received_at_ms,
                                update.clone(),
                            )?;
                        }
                    }
                }
                Ok(false)
            }
            PublicChannel::FundingRate => {
                for value in data {
                    let update: PublicFundingRate = serde_json::from_value(value)?;
                    if let Some(market) = state.markets.get_mut(&update.instrument_id) {
                        market.apply_funding_rate(generation, received_at_ms, update)?;
                    }
                }
                Ok(false)
            }
            PublicChannel::OpenInterest => {
                for value in data {
                    let update: PublicOpenInterest = serde_json::from_value(value)?;
                    if let Some(market) = state.markets.get_mut(&update.instrument_id) {
                        market.apply_open_interest(generation, received_at_ms, update)?;
                    }
                }
                Ok(false)
            }
            PublicChannel::Books => {
                let instrument_id =
                    arg.instrument_id
                        .ok_or(PublicRuntimeError::MissingChannelInstrument {
                            channel: PublicChannel::Books,
                        })?;
                let market = state.markets.get_mut(&instrument_id).ok_or_else(|| {
                    PublicRuntimeError::MarketStateNotInitialized(instrument_id.clone())
                })?;
                for value in data {
                    let message = decode_book(serde_json::from_value(value)?);
                    match action.as_deref() {
                        Some("snapshot") => {
                            market.apply_book_snapshot(generation, received_at_ms, message)?
                        }
                        Some("update") => {
                            market.apply_book_update(generation, received_at_ms, message)?
                        }
                        other => {
                            return Err(PublicRuntimeError::UnsupportedBookAction(
                                other.map(ToOwned::to_owned),
                            ));
                        }
                    }
                }
                Ok(false)
            }
        }
    }

    async fn backoff_until(
        &mut self,
        deadline: Instant,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<bool, PublicRuntimeError> {
        let mut commands_open = true;
        loop {
            tokio::select! {
                _ = sleep_until(deadline) => return Ok(false),
                command = self.commands.recv(), if commands_open => {
                    match command {
                        Some(command) => self.apply_command(command).await,
                        None => commands_open = false,
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(true);
                    }
                }
            }
        }
    }
}

pub fn reconnect_delay(attempt: usize) -> Duration {
    let index = attempt.min(RECONNECT_BACKOFF_SECONDS.len() - 1);
    Duration::from_secs(RECONNECT_BACKOFF_SECONDS[index])
}

fn baseline_subscriptions() -> BTreeSet<Subscription> {
    [
        Subscription::instrument_type(PublicChannel::Instruments, InstrumentType::Swap.to_string()),
        Subscription::instrument_type(
            PublicChannel::Instruments,
            InstrumentType::Futures.to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

fn required_subscriptions(
    reference: &ReferenceRegistry,
    instrument_id: &str,
) -> Result<BTreeSet<Subscription>, PublicRuntimeError> {
    let instrument = reference
        .get(instrument_id)
        .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.to_owned()))?;
    let underlying = instrument
        .underlying
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| PublicRuntimeError::MissingUnderlying(instrument_id.to_owned()))?;

    let mut subscriptions = BTreeSet::from([
        Subscription::instrument(PublicChannel::Tickers, instrument_id),
        Subscription::instrument(PublicChannel::MarkPrice, instrument_id),
        Subscription::instrument(PublicChannel::IndexTickers, underlying),
        Subscription::instrument(PublicChannel::OpenInterest, instrument_id),
        Subscription::instrument(PublicChannel::Books, instrument_id),
    ]);

    match instrument.funding_requirement {
        FundingRequirement::Required => {
            subscriptions.insert(Subscription::instrument(
                PublicChannel::FundingRate,
                instrument_id,
            ));
        }
        FundingRequirement::NotApplicable => {}
        FundingRequirement::Unknown => {
            return Err(PublicRuntimeError::UnknownFundingSemantics {
                instrument_id: instrument_id.to_owned(),
            });
        }
    }
    Ok(subscriptions)
}

fn desired_subscriptions(
    reference: &ReferenceRegistry,
    demands: &BTreeSet<String>,
) -> BTreeSet<Subscription> {
    let mut desired = baseline_subscriptions();
    for instrument_id in demands {
        if let Ok(required) = required_subscriptions(reference, instrument_id) {
            desired.extend(required);
        }
    }
    desired
}

fn subscription_from_arg(arg: &WsArg) -> Subscription {
    Subscription {
        channel: arg.channel,
        instrument_type: arg.instrument_type.clone(),
        instrument_family: arg.instrument_family.clone(),
        instrument_id: arg.instrument_id.clone(),
    }
}

fn connection_fingerprint(connection_id: &str) -> String {
    let digest = Sha256::digest(connection_id.as_bytes());
    format!("{digest:x}")
}

#[derive(Debug, Deserialize)]
struct RawBookData {
    asks: Vec<[String; 4]>,
    bids: Vec<[String; 4]>,
    ts: String,
    #[serde(rename = "seqId")]
    seq_id: i64,
    #[serde(rename = "prevSeqId")]
    prev_seq_id: i64,
}

fn decode_book(raw: RawBookData) -> OrderBookMessage {
    OrderBookMessage {
        asks: raw.asks.into_iter().map(decode_level).collect(),
        bids: raw.bids.into_iter().map(decode_level).collect(),
        exchange_timestamp_ms: raw.ts,
        seq_id: raw.seq_id,
        prev_seq_id: raw.prev_seq_id,
    }
}

fn decode_level(row: [String; 4]) -> BookLevelUpdate {
    let [price, size, _deprecated, order_count] = row;
    BookLevelUpdate {
        price,
        size,
        order_count: Some(order_count),
    }
}

fn now_ms() -> Result<u64, PublicRuntimeError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PublicRuntimeError::ClockBeforeEpoch)?
        .as_millis();
    Ok(millis.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;

    fn instrument(id: &str, instrument_type: &str, rule_type: &str) -> PublicInstrument {
        PublicInstrument {
            instrument_type: instrument_type.to_owned(),
            instrument_id: id.to_owned(),
            instrument_family: "DOGE-USDT".to_owned(),
            underlying: "DOGE-USDT".to_owned(),
            state: "live".to_owned(),
            rule_type: rule_type.to_owned(),
            base_currency: String::new(),
            quote_currency: String::new(),
            settle_currency: "USDT".to_owned(),
            tick_size: "0.00001".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: "1000000".to_owned(),
            max_market_size: "100000".to_owned(),
            max_limit_amount: String::new(),
            max_market_amount: String::new(),
            contract_type: "linear".to_owned(),
            contract_value: "1000".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            fee_group_id: "4".to_owned(),
            lever: "50".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
        }
    }

    fn reference(instrument: PublicInstrument) -> ReferenceRegistry {
        ReferenceRegistry::from_public("2026-09-27T00:00:00.000Z", vec![instrument])
            .expect("reference")
    }

    #[test]
    fn connection_fingerprint_is_stable_and_does_not_expose_conn_id() {
        let first = connection_fingerprint("conn-secret-value");
        let second = connection_fingerprint("conn-secret-value");
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(!first.contains("conn-secret-value"));
    }

    #[tokio::test]
    async fn demand_registration_is_local_idempotent_and_network_independent() {
        let reference = reference(instrument("DOGE-USDT-SWAP", "SWAP", "normal"));
        let (_coordinator, handle) = PublicWsCoordinator::new(
            OkxEnvironment::new(okx_api::Region::Global, false),
            reference,
        );

        handle
            .demand_instrument("DOGE-USDT-SWAP")
            .await
            .expect("first demand");
        handle
            .demand_instrument("DOGE-USDT-SWAP")
            .await
            .expect("repeat demand");

        let state = handle.state.read().await;
        assert!(state.markets.contains_key("DOGE-USDT-SWAP"));
        assert_eq!(state.markets.len(), 1);
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_starts_at_one_second() {
        assert_eq!(reconnect_delay(0), Duration::from_secs(1));
        assert_eq!(reconnect_delay(1), Duration::from_secs(5));
        assert_eq!(reconnect_delay(2), Duration::from_secs(15));
        assert_eq!(reconnect_delay(3), Duration::from_secs(30));
        assert_eq!(reconnect_delay(4), Duration::from_secs(60));
        assert_eq!(reconnect_delay(99), Duration::from_secs(60));
    }

    #[test]
    fn baseline_and_demand_subscriptions_are_reference_driven() {
        let reference = reference(instrument("DOGE-USDT-SWAP", "SWAP", "normal"));
        let required = required_subscriptions(&reference, "DOGE-USDT-SWAP").expect("subscriptions");

        assert!(required.contains(&Subscription::instrument(
            PublicChannel::FundingRate,
            "DOGE-USDT-SWAP"
        )));
        assert!(required.contains(&Subscription::instrument(
            PublicChannel::IndexTickers,
            "DOGE-USDT"
        )));
        assert!(required.contains(&Subscription::instrument(
            PublicChannel::Books,
            "DOGE-USDT-SWAP"
        )));
        assert_eq!(baseline_subscriptions().len(), 2);
    }

    #[test]
    fn ordinary_future_does_not_subscribe_funding_but_xperp_does() {
        let ordinary = reference(instrument("DOGE-USDT-261225", "FUTURES", "normal"));
        let ordinary_required =
            required_subscriptions(&ordinary, "DOGE-USDT-261225").expect("ordinary");
        assert!(
            !ordinary_required
                .iter()
                .any(|subscription| subscription.channel == PublicChannel::FundingRate)
        );

        let xperp = reference(instrument("DOGE-USDT-XPERP", "FUTURES", "xperp"));
        let xperp_required = required_subscriptions(&xperp, "DOGE-USDT-XPERP").expect("xperp");
        assert!(
            xperp_required
                .iter()
                .any(|subscription| subscription.channel == PublicChannel::FundingRate)
        );
    }

    #[test]
    fn unknown_funding_semantics_fail_closed() {
        let reference = reference(instrument("DOGE-USDT-FUTURE", "FUTURES", "future_rule"));
        assert!(matches!(
            required_subscriptions(&reference, "DOGE-USDT-FUTURE"),
            Err(PublicRuntimeError::UnknownFundingSemantics { .. })
        ));
    }

    #[test]
    fn standard_books_array_maps_price_size_and_order_count_only() {
        let raw: RawBookData = serde_json::from_str(
            r#"{
                "asks":[["0.12346","10","0","3"]],
                "bids":[["0.12345","12","0","4"]],
                "ts":"1790467200127",
                "seqId":10,
                "prevSeqId":-1,
                "checksum":0
            }"#,
        )
        .expect("book");
        let book = decode_book(raw);

        assert_eq!(book.asks[0].price, "0.12346");
        assert_eq!(book.asks[0].size, "10");
        assert_eq!(book.asks[0].order_count.as_deref(), Some("3"));
        assert_eq!(book.bids[0].order_count.as_deref(), Some("4"));
        assert_eq!(book.prev_seq_id, -1);
    }
}
