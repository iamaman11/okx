//! On-demand full derivative reference reads. REST never owns live WS state.
use std::{sync::atomic::Ordering, time::Instant};

use chrono::{SecondsFormat, Utc};
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
    chrono::DateTime::parse_from_rfc3339(source).is_ok_and(|at| {
        let at = at.timestamp_millis();
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
        let (expected_generation, expected_revision) = {
            let state = self.state.read().await;
            let rules = state
                .reference
                .instrument_rules(instrument_id)
                .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.into()))?;
            if fresh(&rules.source_received_at, Utc::now().timestamp_millis()) {
                return Ok(rules);
            }
            (
                rules.reference_generation,
                self.reference_revision.load(Ordering::SeqCst),
            )
        };
        let client = self
            .reference_client
            .as_ref()
            .map_err(|error| PublicRuntimeError::ReferenceSource(error.clone()))?;
        let api = PublicDataApi::new(client.clone());
        // Earliest request start is conservative for BOTH complete partitions.
        // A single-symbol update cannot renew a registry-global source time.
        let source_floor = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
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
        if !fresh(&source_floor, Utc::now().timestamp_millis()) {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_SOURCE_STALE",
            ));
        }
        if reference.get(instrument_id).is_none() {
            return Err(PublicRuntimeError::InstrumentNotFound(instrument_id.into()));
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
        let state = self.state.read().await;
        if state.reference.generation().as_str() != expected_generation {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_CONTENT_CHANGED",
            ));
        }
        let rules = state
            .reference
            .instrument_rules(instrument_id)
            .ok_or_else(|| PublicRuntimeError::InstrumentNotFound(instrument_id.into()))?;
        if !fresh(&rules.source_received_at, Utc::now().timestamp_millis()) {
            return Err(PublicRuntimeError::ReferenceRefresh(
                "REFERENCE_REFRESH_SOURCE_STALE",
            ));
        }
        Ok(rules)
    }
}

#[cfg(test)]
#[path = "reference_refresh_tests.rs"]
mod tests;
