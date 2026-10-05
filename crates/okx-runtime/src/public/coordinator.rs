use std::{
    collections::{BTreeSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use okx_api::{
    OkxEnvironment, PublicFundingRate, PublicIndexTicker, PublicInstrument, PublicMarkPrice,
    PublicOpenInterest, PublicTicker, RateBudget,
};
use okx_observation::{
    FundingRequirement, InstrumentRulesSnapshot, LiveMarketSnapshot, MarketStreamState,
    ReferenceRegistry,
};
use okx_ws::{
    InboundMessage, PublicChannel, PublicWsConnection, PublicWsError, Subscription, WsArg,
};
use tokio::{
    sync::{RwLock, mpsc, watch},
    time::{Instant, interval, sleep_until},
};

use super::{
    PublicRuntimeError,
    decode::{decode_book, now_ms},
    state::{PublicQualitySnapshot, PublicRuntimeState},
    subscriptions::{desired_subscriptions, subscription_from_arg},
};

pub const RECONNECT_BACKOFF_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
const HEARTBEAT_TICK_SECONDS: u64 = 1;
const IDLE_BEFORE_PING_SECONDS: u64 = 20;
const PONG_TIMEOUT_SECONDS: u64 = 10;
const SERVICE_UPGRADE_NOTICE_CODE: &str = "64008";
const COMMAND_CAPACITY: usize = 128;
pub const MAX_ACTIVE_MARKETS: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PublicMarketWakeup {
    pub sequence: u64,
    pub generation: u64,
    pub received_at_ms: u64,
}

#[derive(Clone)]
pub struct PublicWsHandle {
    commands: mpsc::Sender<CoordinatorCommand>,
    state: Arc<RwLock<PublicRuntimeState>>,
    market_updates: watch::Sender<PublicMarketWakeup>,
}

pub struct PublicWsCoordinator {
    environment: OkxEnvironment,
    state: Arc<RwLock<PublicRuntimeState>>,
    commands: mpsc::Receiver<CoordinatorCommand>,
    market_updates: watch::Sender<PublicMarketWakeup>,
    market_update_sequence: u64,
    demands: BTreeSet<String>,
    demand_recency: VecDeque<String>,
    generation: u64,
    rate_budget: RateBudget,
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

impl PublicWsHandle {
    pub async fn demand_instrument(
        &self,
        instrument_id: impl Into<String>,
    ) -> Result<(), PublicRuntimeError> {
        let instrument_id = instrument_id.into();

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

    pub fn subscribe_market_updates(&self) -> watch::Receiver<PublicMarketWakeup> {
        self.market_updates.subscribe()
    }
}

impl PublicWsCoordinator {
    pub fn new(
        environment: OkxEnvironment,
        reference: ReferenceRegistry,
    ) -> (Self, PublicWsHandle) {
        Self::new_with_rate_budget(environment, reference, RateBudget::new())
    }

    pub fn new_with_rate_budget(
        environment: OkxEnvironment,
        reference: ReferenceRegistry,
        rate_budget: RateBudget,
    ) -> (Self, PublicWsHandle) {
        let state = Arc::new(RwLock::new(PublicRuntimeState::new(reference)));
        let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (market_updates, _market_updates_rx) =
            watch::channel(PublicMarketWakeup::default());
        (
            Self {
                environment,
                state: Arc::clone(&state),
                commands: commands_rx,
                market_updates: market_updates.clone(),
                market_update_sequence: 0,
                demands: BTreeSet::new(),
                demand_recency: VecDeque::new(),
                generation: 0,
                rate_budget,
            },
            PublicWsHandle {
                commands: commands_tx,
                state,
                market_updates,
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
            let connection_scope =
                format!("public-generation-{}", self.generation.saturating_add(1));
            let outcome = match PublicWsConnection::connect_with_rate_budget(
                self.environment,
                self.rate_budget.clone(),
                connection_scope,
            )
            .await
            {
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

        let removals: Vec<_> = observed
            .difference(&desired)
            .filter(|subscription| !pending.contains(*subscription))
            .cloned()
            .collect();

        if !removals.is_empty() {
            connection.unsubscribe(&removals).await?;
            pending.extend(removals);
        }

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
                let evicted = touch_demand(
                    &mut self.demands,
                    &mut self.demand_recency,
                    instrument_id.clone(),
                );
                let mut state = self.state.write().await;

                if let Some(evicted) = evicted {
                    state.remove_market(&evicted);
                }

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
        let is_market_update = !matches!(&arg.channel, PublicChannel::Instruments);
        let mut state = self.state.write().await;

        let reference_changed = match arg.channel {
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
        }?;
        drop(state);

        if is_market_update {
            self.market_update_sequence = self.market_update_sequence.saturating_add(1);
            self.market_updates.send_replace(PublicMarketWakeup {
                sequence: self.market_update_sequence,
                generation,
                received_at_ms,
            });
        }

        Ok(reference_changed)
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

fn touch_demand(
    demands: &mut BTreeSet<String>,
    recency: &mut VecDeque<String>,
    instrument_id: String,
) -> Option<String> {
    if let Some(position) = recency.iter().position(|value| value == &instrument_id) {
        recency.remove(position);
    }
    demands.insert(instrument_id.clone());
    recency.push_back(instrument_id);

    if recency.len() <= MAX_ACTIVE_MARKETS {
        return None;
    }

    let evicted = recency
        .pop_front()
        .expect("working set exceeds bound only with an eviction candidate");
    demands.remove(&evicted);
    Some(evicted)
}

pub fn reconnect_delay(attempt: usize) -> Duration {
    let index = attempt.min(RECONNECT_BACKOFF_SECONDS.len() - 1);
    Duration::from_secs(RECONNECT_BACKOFF_SECONDS[index])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demand_working_set_is_bounded_and_lru() {
        let mut demands = BTreeSet::new();
        let mut recency = VecDeque::new();

        for index in 0..MAX_ACTIVE_MARKETS {
            assert_eq!(
                touch_demand(
                    &mut demands,
                    &mut recency,
                    format!("ASSET{index:02}-USDT-SWAP"),
                ),
                None
            );
        }
        assert_eq!(demands.len(), MAX_ACTIVE_MARKETS);

        assert_eq!(
            touch_demand(&mut demands, &mut recency, "ASSET00-USDT-SWAP".to_owned(),),
            None
        );

        let evicted = touch_demand(&mut demands, &mut recency, "NEW-USDT-SWAP".to_owned());
        assert_eq!(evicted.as_deref(), Some("ASSET01-USDT-SWAP"));
        assert_eq!(demands.len(), MAX_ACTIVE_MARKETS);
        assert!(demands.contains("ASSET00-USDT-SWAP"));
        assert!(demands.contains("NEW-USDT-SWAP"));
        assert!(!demands.contains("ASSET01-USDT-SWAP"));
    }
}
