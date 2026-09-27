use std::collections::BTreeSet;

use okx_ws::{PrivateChannel, PrivateSubscription, PrivateWsArg};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub const PRIVATE_WS_STATUS_SCHEMA_V1: &str = "okx.private-ws-status/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrivateConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrivateWsStatus {
    pub schema: String,
    pub connection_state: PrivateConnectionState,
    pub connection_generation: u64,
    pub logged_in: bool,
    pub connection_id_fingerprint: Option<String>,
    pub acknowledged_subscriptions: usize,
    pub subscriptions_complete: bool,
    pub account_data_seen: bool,
    pub positions_data_seen: bool,
    pub orders_updates_seen: bool,
    pub last_inbound_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub struct PrivateRuntimeState {
    pub(super) connection_state: PrivateConnectionState,
    pub(super) generation: u64,
    pub(super) logged_in: bool,
    pub(super) connection_id: Option<String>,
    pub(super) acknowledged_subscriptions: BTreeSet<PrivateSubscription>,
    pub(super) account_data_seen: bool,
    pub(super) positions_data_seen: bool,
    pub(super) orders_updates_seen: bool,
    pub(super) last_inbound_ms: Option<u64>,
    pub(super) last_error: Option<String>,
}

impl PrivateRuntimeState {
    pub fn new() -> Self {
        Self {
            connection_state: PrivateConnectionState::Disconnected,
            generation: 0,
            logged_in: false,
            connection_id: None,
            acknowledged_subscriptions: BTreeSet::new(),
            account_data_seen: false,
            positions_data_seen: false,
            orders_updates_seen: false,
            last_inbound_ms: None,
            last_error: None,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn connection_state(&self) -> PrivateConnectionState {
        self.connection_state
    }

    pub fn status(&self) -> PrivateWsStatus {
        PrivateWsStatus {
            schema: PRIVATE_WS_STATUS_SCHEMA_V1.to_owned(),
            connection_state: self.connection_state,
            connection_generation: self.generation,
            logged_in: self.logged_in,
            connection_id_fingerprint: self
                .connection_id
                .as_deref()
                .map(connection_fingerprint),
            acknowledged_subscriptions: self.acknowledged_subscriptions.len(),
            subscriptions_complete: baseline_private_subscriptions()
                .iter()
                .all(|subscription| self.acknowledged_subscriptions.contains(subscription)),
            account_data_seen: self.account_data_seen,
            positions_data_seen: self.positions_data_seen,
            orders_updates_seen: self.orders_updates_seen,
            last_inbound_ms: self.last_inbound_ms,
            last_error: self.last_error.clone(),
        }
    }

    pub(super) fn set_connecting(&mut self) {
        self.connection_state = PrivateConnectionState::Connecting;
        self.logged_in = false;
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
    }

    pub(super) fn begin_generation(&mut self, generation: u64) {
        self.connection_state = PrivateConnectionState::Connected;
        self.generation = generation;
        self.logged_in = false;
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
        self.account_data_seen = false;
        self.positions_data_seen = false;
        self.orders_updates_seen = false;
        self.last_inbound_ms = None;
        self.last_error = None;
    }

    pub(super) fn set_disconnected(&mut self, reconnecting: bool, reason: impl Into<String>) {
        self.connection_state = if reconnecting {
            PrivateConnectionState::Reconnecting
        } else {
            PrivateConnectionState::Disconnected
        };
        self.logged_in = false;
        self.connection_id = None;
        self.acknowledged_subscriptions.clear();
        self.last_error = Some(reason.into());
    }

    pub(super) fn acknowledge_login(&mut self, connection_id: Option<String>) {
        self.logged_in = true;
        if connection_id.is_some() {
            self.connection_id = connection_id;
        }
    }

    pub(super) fn acknowledge_subscription(
        &mut self,
        arg: &PrivateWsArg,
        connection_id: Option<String>,
    ) {
        self.acknowledged_subscriptions
            .insert(subscription_from_arg(arg));
        if connection_id.is_some() {
            self.connection_id = connection_id;
        }
    }

    pub(super) fn observe_data(&mut self, channel: PrivateChannel, received_at_ms: u64) {
        self.last_inbound_ms = Some(received_at_ms);
        match channel {
            PrivateChannel::Account => self.account_data_seen = true,
            PrivateChannel::Positions => self.positions_data_seen = true,
            PrivateChannel::Orders => self.orders_updates_seen = true,
        }
    }
}

impl Default for PrivateRuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

pub fn baseline_private_subscriptions() -> [PrivateSubscription; 3] {
    [
        PrivateSubscription::account(),
        PrivateSubscription::positions_any(),
        PrivateSubscription::orders_any(),
    ]
}

pub(super) fn subscription_from_arg(arg: &PrivateWsArg) -> PrivateSubscription {
    PrivateSubscription {
        channel: arg.channel,
        ccy: arg.ccy.clone(),
        instrument_type: arg.instrument_type.clone(),
        instrument_family: arg.instrument_family.clone(),
        instrument_id: arg.instrument_id.clone(),
    }
}

pub fn connection_fingerprint(connection_id: &str) -> String {
    format!("{:x}", Sha256::digest(connection_id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_reset_revokes_all_old_private_evidence() {
        let mut state = PrivateRuntimeState::new();
        state.begin_generation(1);
        state.acknowledge_login(Some("conn-secret".to_owned()));
        for subscription in baseline_private_subscriptions() {
            let arg = PrivateWsArg {
                channel: subscription.channel,
                ccy: subscription.ccy,
                instrument_type: subscription.instrument_type,
                instrument_family: subscription.instrument_family,
                instrument_id: subscription.instrument_id,
            };
            state.acknowledge_subscription(&arg, None);
        }
        state.observe_data(PrivateChannel::Account, 10);
        state.observe_data(PrivateChannel::Positions, 11);

        let first = state.status();
        assert!(first.logged_in);
        assert!(first.subscriptions_complete);
        assert!(first.account_data_seen);
        assert!(first.positions_data_seen);
        assert!(!first
            .connection_id_fingerprint
            .as_deref()
            .unwrap_or_default()
            .contains("conn-secret"));

        state.begin_generation(2);
        let second = state.status();
        assert_eq!(second.connection_generation, 2);
        assert!(!second.logged_in);
        assert_eq!(second.acknowledged_subscriptions, 0);
        assert!(!second.account_data_seen);
        assert!(!second.positions_data_seen);
        assert!(!second.orders_updates_seen);
    }
}
