use super::{AnalyticsService, RunnerGuard, jobs::SignalOutboxState};
use okx_ipc_contracts::research_ipc::{
    SignalPosition, SignalSourceDigest, StrategySignalInput, StrategySignalProposal,
};
use sha2::{Digest, Sha256};

const PROPOSAL_LIFETIME_MS: u64 = 30_000;

fn now_ms() -> u64 {
    jiff::Timestamp::now().as_millisecond().max(0) as u64
}

impl AnalyticsService {
    /// Return a committed proposal, or stage bounded computation for a later
    /// poll. There is no live exchange or financial state in this worker.
    pub async fn stage_strategy_signal(
        &self,
        input: StrategySignalInput,
    ) -> Result<Option<StrategySignalProposal>, String> {
        let admitted = now_ms();
        if !input.valid_at(admitted) {
            return Err("INVALID_STRATEGY_SIGNAL_INPUT".into());
        }
        let last = input
            .prices
            .closes
            .last()
            .expect("validated prices")
            .time_ms;
        let quotes: Vec<(u64, f64)> = input
            .prices
            .closes
            .iter()
            .map(|bar| (bar.time_ms, bar.close))
            .collect();
        let wire = serde_json::to_vec(&(last, quotes)).map_err(|e| e.to_string())?;
        let digest = SignalSourceDigest::new(format!("{:x}", Sha256::digest(wire)))?;
        let key = okx_research::canonical_sha256(&(
            &input.instance_id,
            &input.definition_id,
            &input.definition_version,
            &input.instrument,
            input.timeframe_ms,
            input.prices.parameters,
            &digest,
        ))
        .map_err(|e| e.to_string())?;
        let expires = admitted
            .saturating_add(PROPOSAL_LIFETIME_MS)
            .min(last.saturating_add(input.timeframe_ms.saturating_mul(2)));
        if expires <= admitted {
            return Ok(None);
        }
        let permit = self
            .signal_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| "RESEARCH_SIGNAL_BUSY")?;
        let runner = RunnerGuard::new(self.runners.clone());
        let service = self.clone();
        // Keep admission and scheduling in the same owned task even if the
        // IPC request is cancelled while SQLite is committing.
        tokio::spawn(async move {
            let jobs = service.jobs.clone();
            let slot_key = key.clone();
            let state = tokio::task::spawn_blocking(move || {
                jobs.stage_strategy_signal(&slot_key, admitted, expires)
            })
            .await
            .map_err(|_| "RESEARCH_SIGNAL_ADMISSION_FAILED")??;
            match state {
                SignalOutboxState::Pending => Ok(None),
                SignalOutboxState::Ready(value) => Ok(value.filter(|v| v.valid_at(now_ms()))),
                SignalOutboxState::Start => {
                    tokio::spawn(async move {
                        let _permit = permit;
                        let _runner = runner;
                        let proposal = match service.evaluate_signal(input.prices).await {
                            Ok(decision) if decision.position != SignalPosition::Warmup => {
                                let produced = now_ms();
                                let proposal = StrategySignalProposal {
                                    schema: StrategySignalProposal::SCHEMA.into(),
                                    instance_id: input.instance_id,
                                    definition_id: input.definition_id,
                                    definition_version: input.definition_version,
                                    instrument: input.instrument,
                                    timeframe_ms: input.timeframe_ms,
                                    last_closed_ms: last,
                                    available_at_ms: input.available_at_ms,
                                    produced_at_ms: produced,
                                    expires_at_ms: expires,
                                    source_digest: digest,
                                    position: decision.position,
                                };
                                proposal.valid_at(produced).then_some(proposal)
                            }
                            Ok(_) => None,
                            Err(reason) => {
                                tracing::warn!(%reason, "strategy signal evaluation rejected");
                                None
                            }
                        };
                        match tokio::task::spawn_blocking(move || {
                            service
                                .jobs
                                .publish_strategy_signal(&key, admitted, proposal.as_ref())
                        })
                        .await
                        {
                            Ok(Ok(())) => {}
                            _ => tracing::warn!("strategy signal publication failed"),
                        }
                    });
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|_| "RESEARCH_SIGNAL_ADMISSION_FAILED")?
    }
}
