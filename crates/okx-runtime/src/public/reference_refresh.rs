//! On-demand full derivative reference reads. REST never owns live WS state.
use std::str::FromStr;
use std::{sync::atomic::Ordering, time::Instant};

use jiff::Timestamp;
use okx_api::{InstrumentType, PublicDataApi};
use okx_observation::{InstrumentRulesSnapshot, ReferenceRegistry};
use tokio::sync::oneshot;

use super::{
    PublicRuntimeError,
    coordinator::{CoordinatorCommand, PublicWsHandle},
};

// Same source validity required by the supported entry assessment. Refreshing
// acquires real source evidence; it never extends the validity of old bytes.
const REFERENCE_MAX_AGE_MS: i64 = 5_000;

fn fresh(source: &str, now: i64) -> bool {
    jiff::Timestamp::from_str(source).is_ok_and(|at| {
        let at = at.as_millisecond();
        at > 0 && at <= now && now - at <= REFERENCE_MAX_AGE_MS
    })
}

impl PublicWsHandle {
    /// Refresh stale full SWAP/FUTURES evidence through the existing public
    /// REST parser. No network runs under the coordinator state lock; its
    /// queued CAS is the sole publication point and rejects intervening WS
    /// updates, even if their normalized content or timestamp is unchanged.
    pub async fn ensure_fresh_instrument_rules(
        &self,
        instrument_id: &str,
        deadline: Instant,
    ) -> Result<InstrumentRulesSnapshot, PublicRuntimeError> {
        if deadline <= Instant::now() {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_DEADLINE",
            ));
        }
        let _refresh = tokio::time::timeout_at(deadline.into(), self.reference_refresh_lock.lock())
            .await
            .map_err(|_| PublicRuntimeError::ReferenceRefresh("REFERENCE_REFRESH_DEADLINE"))?;
        let (expected_generation, expected_revision, initial) = {
            let state = self.state.read().await;
            let rules = state.reference.instrument_rules(instrument_id);
            if state.reference_unavailable.is_none() {
                let rules = rules
                    .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.into()))?;
                if fresh(&rules.source_received_at, Timestamp::now().as_millisecond()) {
                    return Ok(rules);
                }
            }
            (
                state.reference.generation().as_str().to_owned(),
                self.reference_revision.load(Ordering::SeqCst),
                state.reference_unavailable.is_some(),
            )
        };
        let refreshed = self
            .refresh_reference(
                Some(instrument_id),
                deadline,
                expected_generation.clone(),
                expected_revision,
            )
            .await;
        match refreshed {
            Ok(()) => {}
            Err(PublicRuntimeError::ReferenceRefresh("REFERENCE_REFRESH_CONTENT_CHANGED"))
                if initial => {}
            Err(error) => return Err(error),
        }
        let state = self.state.read().await;
        if (initial
            && (state.reference_unavailable.is_some()
                || self.reference_revision.load(Ordering::SeqCst)
                    != expected_revision.saturating_add(1)))
            || (!initial && state.reference.generation().as_str() != expected_generation)
        {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_CONTENT_CHANGED",
            ));
        }
        let rules = state
            .reference
            .instrument_rules(instrument_id)
            .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.into()))?;
        if !fresh(&rules.source_received_at, Timestamp::now().as_millisecond()) {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_SOURCE_STALE",
            ));
        }
        Ok(rules)
    }
    /// Acquire the initial full reference on a request that needs it. Requests
    /// independent of reference data never wait for this source.
    pub async fn ensure_reference_available(
        &self,
        deadline: Instant,
    ) -> Result<(), PublicRuntimeError> {
        if deadline <= Instant::now() {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_DEADLINE",
            ));
        }
        let _refresh = tokio::time::timeout_at(deadline.into(), self.reference_refresh_lock.lock())
            .await
            .map_err(|_| PublicRuntimeError::ReferenceRefresh("REFERENCE_REFRESH_DEADLINE"))?;
        let (generation, revision) = {
            let state = self.state.read().await;
            if state.reference_unavailable.is_none() {
                return Ok(());
            }
            (
                state.reference.generation().as_str().to_owned(),
                self.reference_revision.load(Ordering::SeqCst),
            )
        };
        match self
            .refresh_reference(None, deadline, generation, revision)
            .await
        {
            Ok(())
            | Err(PublicRuntimeError::ReferenceRefresh("REFERENCE_REFRESH_CONTENT_CHANGED")) => {
                self.reference_available().await
            }
            Err(error) => Err(error),
        }
    }

    async fn refresh_reference(
        &self,
        instrument_id: Option<&str>,
        deadline: Instant,
        expected_generation: String,
        expected_revision: u64,
    ) -> Result<(), PublicRuntimeError> {
        let client = self
            .reference_client
            .as_ref()
            .map_err(|error| PublicRuntimeError::ReferenceSource(error.clone()))?;
        let api = PublicDataApi::new(client.clone());
        // Earliest request start is conservative for BOTH complete partitions.
        // A single-symbol update cannot renew a registry-global source time.
        let source_floor = format!("{:.3}", Timestamp::now());
        let reference = tokio::time::timeout_at(deadline.into(), async {
            let swap = api
                .instruments(InstrumentType::Swap, None)
                .await
                .map_err(|error| PublicRuntimeError::ReferenceSource(error.to_string()))?;
            let futures = api
                .instruments(InstrumentType::Futures, None)
                .await
                .map_err(|error| PublicRuntimeError::ReferenceSource(error.to_string()))?;
            if swap.iter().any(|row| row.instrument_type != "SWAP")
                || futures.iter().any(|row| row.instrument_type != "FUTURES")
            {
                return Err(PublicRuntimeError::ReferenceRefresh(
                    "REFERENCE_REFRESH_PARTITION_MISMATCH",
                ));
            }
            ReferenceRegistry::from_public(
                source_floor.clone(),
                swap.into_iter().chain(futures).collect(),
            )
            .map_err(PublicRuntimeError::from)
        })
        .await
        .map_err(|_| PublicRuntimeError::ReferenceRefresh("REFERENCE_REFRESH_DEADLINE"))??;
        if reference.is_empty() {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_EMPTY",
            ));
        }
        if !fresh(&source_floor, Timestamp::now().as_millisecond()) {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_SOURCE_STALE",
            ));
        }
        if let Some(id) = instrument_id
            && reference.get(id).is_none()
        {
            return Err(PublicRuntimeError::InstrumentNotFound(id.into()));
        }
        let (reply, receive) = oneshot::channel();
        self.commands
            .try_send(CoordinatorCommand::RefreshReference {
                expected_generation: expected_generation.clone(),
                expected_revision,
                reference,
                deadline,
                reply,
            })
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    PublicRuntimeError::CommandChannelClosed
                }
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    PublicRuntimeError::CommandQueueFull
                }
            })?;
        tokio::time::timeout_at(deadline.into(), receive)
            .await
            .map_err(|_| PublicRuntimeError::ReferenceRefresh("REFERENCE_REFRESH_DEADLINE"))?
            .map_err(|_| PublicRuntimeError::CommandChannelClosed)?
            .map_err(PublicRuntimeError::ReferenceRefresh)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "reference_refresh_tests.rs"]
mod tests;
