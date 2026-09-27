use std::collections::{BTreeSet, VecDeque};

use okx_observation::AccountWsEvent;
use okx_ws::{PrivateSubscription, PrivateWsArg};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const PRIVATE_WS_STATUS_SCHEMA_V1: &str = "okx.private-ws-status/v1";
pub const PRIVATE_EVENT_JOURNAL_CAPACITY: usize = 4096;

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

pub type PrivateWsEvent = AccountWsEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrivateConvergenceCursor {
    pub generation: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone)]
pub struct PrivateConvergenceWindow {
    pub generation: u64,
    pub status: PrivateWsStatus,
    pub events: Vec<PrivateWsEvent>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PrivateConvergenceError {
    #[error("private websocket is not ready for convergence")]
    NotReady,

    #[error("private websocket generation changed during REST bootstrap")]
    GenerationChanged,

    #[error("private websocket event journal no longer contains the convergence cursor")]
    JournalGap,
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
    event_sequence: u64,
    event_journal: VecDeque<(u64, PrivateWsEvent)>,
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
            event_sequence: 0,
            event_journal: VecDeque::new(),
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
            connection_id_fingerprint: self.connection_id.as_deref().map(connection_fingerprint),
            acknowledged_subscriptions: self.acknowledged_subscriptions.len(),
            subscriptions_complete: self.subscriptions_complete(),
            account_data_seen: self.account_data_seen,
            positions_data_seen: self.positions_data_seen,
            orders_updates_seen: self.orders_updates_seen,
            last_inbound_ms: self.last_inbound_ms,
            last_error: self.last_error.clone(),
        }
    }

    pub fn convergence_cursor(&self) -> Result<PrivateConvergenceCursor, PrivateConvergenceError> {
        if !self.convergence_ready() {
            return Err(PrivateConvergenceError::NotReady);
        }
        Ok(PrivateConvergenceCursor {
            generation: self.generation,
            sequence: self.event_sequence,
        })
    }

    pub fn convergence_window(
        &self,
        cursor: PrivateConvergenceCursor,
    ) -> Result<PrivateConvergenceWindow, PrivateConvergenceError> {
        if cursor.generation != self.generation {
            return Err(PrivateConvergenceError::GenerationChanged);
        }
        if !self.convergence_ready() {
            return Err(PrivateConvergenceError::NotReady);
        }

        if let Some((oldest_sequence, _)) = self.event_journal.front()
            && cursor.sequence.saturating_add(1) < *oldest_sequence
        {
            return Err(PrivateConvergenceError::JournalGap);
        }

        let events = self
            .event_journal
            .iter()
            .filter(|(sequence, _)| *sequence > cursor.sequence)
            .map(|(_, event)| event.clone())
            .collect();

        Ok(PrivateConvergenceWindow {
            generation: self.generation,
            status: self.status(),
            events,
        })
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
        self.event_sequence = 0;
        self.event_journal.clear();
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

    pub(super) fn observe_event(&mut self, event: PrivateWsEvent, received_at_ms: u64) {
        self.last_inbound_ms = Some(received_at_ms);
        match &event {
            PrivateWsEvent::Account(_) => self.account_data_seen = true,
            PrivateWsEvent::Positions(_) => self.positions_data_seen = true,
            PrivateWsEvent::Orders(_) => self.orders_updates_seen = true,
        }

        self.event_sequence = self.event_sequence.saturating_add(1);
        self.event_journal.push_back((self.event_sequence, event));
        while self.event_journal.len() > PRIVATE_EVENT_JOURNAL_CAPACITY {
            self.event_journal.pop_front();
        }
    }

    fn subscriptions_complete(&self) -> bool {
        baseline_private_subscriptions()
            .iter()
            .all(|subscription| self.acknowledged_subscriptions.contains(subscription))
    }

    fn convergence_ready(&self) -> bool {
        self.connection_state == PrivateConnectionState::Connected
            && self.logged_in
            && self.subscriptions_complete()
            && self.account_data_seen
            && self.positions_data_seen
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
        let mut state = ready_state();
        let first = state.status();
        assert!(first.logged_in);
        assert!(first.subscriptions_complete);
        assert!(first.account_data_seen);
        assert!(first.positions_data_seen);
        assert!(
            !first
                .connection_id_fingerprint
                .as_deref()
                .unwrap_or_default()
                .contains("conn-secret")
        );
        assert!(state.convergence_cursor().is_ok());

        state.begin_generation(2);
        let second = state.status();
        assert_eq!(second.connection_generation, 2);
        assert!(!second.logged_in);
        assert_eq!(second.acknowledged_subscriptions, 0);
        assert!(!second.account_data_seen);
        assert!(!second.positions_data_seen);
        assert!(!second.orders_updates_seen);
        assert_eq!(
            state.convergence_cursor(),
            Err(PrivateConvergenceError::NotReady)
        );
    }

    #[test]
    fn convergence_window_is_generation_bound() {
        let mut state = ready_state();
        let cursor = state.convergence_cursor().expect("cursor");
        state.observe_event(PrivateWsEvent::Orders(Vec::new()), 12);
        let window = state.convergence_window(cursor).expect("window");
        assert_eq!(window.generation, 1);
        assert_eq!(window.events.len(), 1);

        state.begin_generation(2);
        assert_eq!(
            state.convergence_window(cursor).expect_err("old cursor"),
            PrivateConvergenceError::GenerationChanged
        );
    }

    #[test]
    fn journal_gap_fails_closed() {
        let mut state = ready_state();
        let cursor = state.convergence_cursor().expect("cursor");
        for index in 0..=PRIVATE_EVENT_JOURNAL_CAPACITY {
            state.observe_event(PrivateWsEvent::Orders(Vec::new()), 100 + index as u64);
        }
        assert_eq!(
            state.convergence_window(cursor).expect_err("journal gap"),
            PrivateConvergenceError::JournalGap
        );
    }

    fn ready_state() -> PrivateRuntimeState {
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
        state.observe_event(PrivateWsEvent::Account(Vec::new()), 10);
        state.observe_event(PrivateWsEvent::Positions(Vec::new()), 11);
        state
    }
}
