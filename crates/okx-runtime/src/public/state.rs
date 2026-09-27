use std::collections::{BTreeMap, BTreeSet};

use okx_observation::{
    InstrumentRulesSnapshot, InstrumentSearchSnapshot, LiveMarketSnapshot, MarketReadiness,
    MarketReadinessReport, MarketSnapshot, MarketStreamState, ReferenceRegistry,
};
use okx_ws::{Subscription, WsArg};
use serde::Serialize;

use super::{
    PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2, PublicRuntimeError,
    subscriptions::{
        baseline_subscriptions, connection_fingerprint, required_subscriptions,
        subscription_from_arg,
    },
};

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

#[derive(Debug, Clone)]
pub struct PublicMarketOverviewView {
    pub rules: InstrumentRulesSnapshot,
    pub quality: PublicQualitySnapshot,
    pub live_market: Option<MarketSnapshot>,
    pub reference: ReferenceRegistry,
}

#[derive(Debug)]
pub struct PublicRuntimeState {
    pub(super) reference: ReferenceRegistry,
    pub(super) markets: BTreeMap<String, MarketStreamState>,
    pub(super) connection_state: PublicConnectionState,
    pub(super) generation: u64,
    pub(super) connection_id: Option<String>,
    pub(super) acknowledged_subscriptions: BTreeSet<Subscription>,
    pub(super) last_error: Option<String>,
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

    pub(super) fn begin_generation(&mut self, generation: u64, demands: &BTreeSet<String>) {
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

    pub(super) fn set_disconnected(&mut self, reconnecting: bool, reason: impl Into<String>) {
        self.connection_state = if reconnecting {
            PublicConnectionState::Reconnecting
        } else {
            PublicConnectionState::Disconnected
        };
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
        self.last_error = Some(reason.into());
    }

    pub(super) fn set_connecting(&mut self) {
        self.connection_state = PublicConnectionState::Connecting;
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
    }

    pub(super) fn acknowledge(&mut self, arg: &WsArg, connection_id: Option<String>) {
        self.acknowledged_subscriptions
            .insert(subscription_from_arg(arg));
        if connection_id.is_some() {
            self.connection_id = connection_id;
        }
    }

    pub(super) fn unacknowledge(&mut self, arg: &WsArg) {
        self.acknowledged_subscriptions
            .remove(&subscription_from_arg(arg));
    }

    pub(super) fn subscriptions_complete(&self, instrument_id: &str) -> bool {
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

    pub fn find_instruments(
        &self,
        asset: &str,
        quote: Option<&str>,
        limit: usize,
    ) -> InstrumentSearchSnapshot {
        self.reference.find_instruments(asset, quote, limit)
    }

    pub fn market_overview_view(
        &self,
        instrument_id: &str,
        now_ms: u64,
        max_age_ms: u64,
        source_received_at: impl Into<String>,
    ) -> Result<PublicMarketOverviewView, PublicRuntimeError> {
        let rules = self
            .reference
            .instrument_rules(instrument_id)
            .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.to_owned()))?;
        let quality = self.quality_snapshot(instrument_id, now_ms, max_age_ms, true)?;
        let live_market = if quality.quality == MarketReadiness::Fresh {
            Some(
                self.fresh_snapshot(
                    instrument_id,
                    now_ms,
                    max_age_ms,
                    source_received_at,
                )?
                .market,
            )
        } else {
            None
        };

        Ok(PublicMarketOverviewView {
            rules,
            quality,
            live_market,
            reference: self.reference.clone(),
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
