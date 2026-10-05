use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use okx_analysis::{candidate_risk_context_from_account, baseline_strategy_research_metadata};
use okx_observation::MarketReadiness;
use okx_protocol::DataQuality;
use okx_research::{
    BUILD_SOURCE_TREE, ExperimentSpec, LIVE_RESEARCH_SESSION_CHECKPOINT_SCHEMA_V1,
    LiveResearchDisposition, LiveResearchSessionCheckpoint, LiveResearchSessionConfig,
    LiveResearchSessionMode, LiveResearchSessionStatus, PaperVirtualPosition,
    RESEARCH_FUNDING_SCHEMA_V1, ResearchArtifactStore, ResearchFundingEvent,
    evaluate_live_research_decision, load_live_research_session_config,
    paper_realized_pnl_after_trade, settle_paper_virtual_position,
};
use okx_runtime::{PrivateWsHandle, PublicMarketWakeup, PublicWsHandle};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    account_bootstrap::AccountBootstrapper,
    market_bootstrap::MarketBootstrapper,
    query::{ObservationQueryContext, PUBLIC_MARKET_MAX_AGE_MS, assemble_account_snapshot},
};

const COMMAND_CAPACITY: usize = 8;
const POINTER_SCHEMA_V1: &str = "okx.research.live-session-pointer/v1";
pub const RESEARCH_SESSION_STATUS_SCHEMA_V1: &str = "okx.research.live-session-status/v1";
const ONE_HOUR_MS: u64 = 3_600_000;
const FUNDING_HISTORY_LIMIT: u16 = 20;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResearchSessionStatus {
    pub schema: &'static str,
    pub state: &'static str,
    pub session_id: Option<String>,
    pub mode: Option<LiveResearchSessionMode>,
    pub instrument_id: Option<String>,
    pub strategy: Option<okx_analysis::BaselineStrategyKind>,
    pub strategy_version: Option<String>,
    pub config_artifact_id: Option<String>,
    pub checkpoint_artifact_id: Option<String>,
    pub last_evaluated_entry_open_time_ms: Option<String>,
    pub latest_decision_artifact_id: Option<String>,
    pub latest_paper_trade_artifact_id: Option<String>,
    pub last_blocker: Option<String>,
    pub decision_count: u64,
    pub blocked_count: u64,
    pub would_submit_count: u64,
    pub paper_trade_count: u64,
    pub paper_realized_net_pnl_quote: String,
    pub paper_position_open: bool,
    pub source_tree: &'static str,
    pub exchange_mutation_authority: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchSessionFailure {
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl ResearchSessionFailure {
    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: "RESEARCH_SESSION_UNAVAILABLE",
            message: message.into(),
            retryable: true,
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: "RESEARCH_SESSION_CONFLICT",
            message: message.into(),
            retryable: false,
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "RESEARCH_SESSION_INVALID",
            message: message.into(),
            retryable: false,
        }
    }
}

#[derive(Clone)]
pub struct ResearchSessionHandle {
    commands: mpsc::Sender<ResearchSessionCommand>,
}

impl ResearchSessionHandle {
    pub async fn start(
        &self,
        promotion_transition_artifact_id: String,
        experiment_spec_artifact_id: String,
    ) -> Result<ResearchSessionStatus, ResearchSessionFailure> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(ResearchSessionCommand::Start {
                promotion_transition_artifact_id,
                experiment_spec_artifact_id,
                reply,
            })
            .await
            .map_err(|_| ResearchSessionFailure::unavailable("research session owner is closed"))?;
        receive
            .await
            .map_err(|_| ResearchSessionFailure::unavailable("research session owner stopped"))?
    }

    pub async fn inspect(&self) -> Result<ResearchSessionStatus, ResearchSessionFailure> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(ResearchSessionCommand::Inspect { reply })
            .await
            .map_err(|_| ResearchSessionFailure::unavailable("research session owner is closed"))?;
        receive
            .await
            .map_err(|_| ResearchSessionFailure::unavailable("research session owner stopped"))?
    }

    pub async fn stop(
        &self,
        session_id: String,
    ) -> Result<ResearchSessionStatus, ResearchSessionFailure> {
        let (reply, receive) = oneshot::channel();
        self.commands
            .send(ResearchSessionCommand::Stop { session_id, reply })
            .await
            .map_err(|_| ResearchSessionFailure::unavailable("research session owner is closed"))?;
        receive
            .await
            .map_err(|_| ResearchSessionFailure::unavailable("research session owner stopped"))?
    }
}

enum ResearchSessionCommand {
    Start {
        promotion_transition_artifact_id: String,
        experiment_spec_artifact_id: String,
        reply: oneshot::Sender<Result<ResearchSessionStatus, ResearchSessionFailure>>,
    },
    Inspect {
        reply: oneshot::Sender<Result<ResearchSessionStatus, ResearchSessionFailure>>,
    },
    Stop {
        session_id: String,
        reply: oneshot::Sender<Result<ResearchSessionStatus, ResearchSessionFailure>>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ResearchSessionRuntimeError {
    #[error("research session pointer I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("research session pointer JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("research session artifact error: {0}")]
    Research(#[from] okx_research::ResearchError),
    #[error("research session persisted state is inconsistent")]
    PersistedState,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchSessionPointer {
    schema: String,
    checkpoint_artifact_id: String,
}

struct ActiveResearchSession {
    config: LiveResearchSessionConfig,
    config_artifact_id: String,
    spec: ExperimentSpec,
    checkpoint: LiveResearchSessionCheckpoint,
    checkpoint_artifact_id: String,
}

pub struct ResearchSessionRuntime {
    root: PathBuf,
    store: ResearchArtifactStore,
    public_ws: PublicWsHandle,
    market: MarketBootstrapper,
    account: Option<AccountBootstrapper>,
    private_ws: Option<PrivateWsHandle>,
    commands: mpsc::Receiver<ResearchSessionCommand>,
    wakeups: watch::Receiver<PublicMarketWakeup>,
    active: Option<ActiveResearchSession>,
    last_checkpoint: Option<(LiveResearchSessionCheckpoint, String)>,
    last_config: Option<LiveResearchSessionConfig>,
}

impl ResearchSessionRuntime {
    pub fn new(
        root: PathBuf,
        public_ws: PublicWsHandle,
        market: MarketBootstrapper,
        account: Option<AccountBootstrapper>,
        private_ws: Option<PrivateWsHandle>,
    ) -> (Self, ResearchSessionHandle) {
        let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
        let wakeups = public_ws.subscribe_market_updates();
        let store = ResearchArtifactStore::at(root.join("research"));
        (
            Self {
                root,
                store,
                public_ws,
                market,
                account,
                private_ws,
                commands: commands_rx,
                wakeups,
                active: None,
                last_checkpoint: None,
                last_config: None,
            },
            ResearchSessionHandle {
                commands: commands_tx,
            },
        )
    }

    pub async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ResearchSessionRuntimeError> {
        self.restore().await?;

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        return Ok(());
                    };
                    self.handle_command(command).await?;
                }
                changed = self.wakeups.changed() => {
                    if changed.is_err() {
                        return Err(ResearchSessionRuntimeError::PersistedState);
                    }
                    let wakeup = *self.wakeups.borrow_and_update();
                    self.handle_market_wakeup(wakeup).await?;
                }
            }
        }
    }

    async fn restore(&mut self) -> Result<(), ResearchSessionRuntimeError> {
        let Some(pointer) = self.read_pointer()? else {
            return Ok(());
        };
        let checkpoint: LiveResearchSessionCheckpoint =
            self.store.read_evidence_json(&pointer.checkpoint_artifact_id)?;
        checkpoint.validate()?;
        let config: LiveResearchSessionConfig =
            self.store.read_evidence_json(&checkpoint.config_artifact_id)?;
        config.validate()?;
        let (rebuilt, spec) = load_live_research_session_config(
            &self.store,
            &config.promotion_transition_artifact_id,
            &config.experiment_spec_artifact_id,
        )?;
        if rebuilt != config || checkpoint.session_id != config.session_id {
            return Err(ResearchSessionRuntimeError::PersistedState);
        }

        self.last_checkpoint = Some((checkpoint.clone(), pointer.checkpoint_artifact_id.clone()));
        self.last_config = Some(config.clone());
        if checkpoint.status == LiveResearchSessionStatus::Active {
            self.public_ws
                .demand_instrument(config.instrument_id.clone())
                .await
                .map_err(|_| ResearchSessionRuntimeError::PersistedState)?;
            self.active = Some(ActiveResearchSession {
                config,
                config_artifact_id: checkpoint.config_artifact_id.clone(),
                spec,
                checkpoint,
                checkpoint_artifact_id: pointer.checkpoint_artifact_id,
            });
        }
        Ok(())
    }

    async fn handle_command(
        &mut self,
        command: ResearchSessionCommand,
    ) -> Result<(), ResearchSessionRuntimeError> {
        match command {
            ResearchSessionCommand::Start {
                promotion_transition_artifact_id,
                experiment_spec_artifact_id,
                reply,
            } => {
                let result = self
                    .start_session(
                        promotion_transition_artifact_id,
                        experiment_spec_artifact_id,
                    )
                    .await;
                let _ = reply.send(result);
            }
            ResearchSessionCommand::Inspect { reply } => {
                let _ = reply.send(Ok(self.status()));
            }
            ResearchSessionCommand::Stop { session_id, reply } => {
                let result = self.stop_session(&session_id);
                let _ = reply.send(result);
            }
        }
        Ok(())
    }

    async fn start_session(
        &mut self,
        promotion_transition_artifact_id: String,
        experiment_spec_artifact_id: String,
    ) -> Result<ResearchSessionStatus, ResearchSessionFailure> {
        let (config, spec) = load_live_research_session_config(
            &self.store,
            &promotion_transition_artifact_id,
            &experiment_spec_artifact_id,
        )
        .map_err(|error| ResearchSessionFailure::invalid(error.to_string()))?;

        if let Some(active) = self.active.as_ref() {
            if active.config.session_id == config.session_id {
                return Ok(self.status());
            }
            return Err(ResearchSessionFailure::conflict(format!(
                "research session '{}' is already active",
                active.config.session_id
            )));
        }

        self.public_ws
            .demand_instrument(config.instrument_id.clone())
            .await
            .map_err(|error| ResearchSessionFailure::unavailable(error.to_string()))?;
        let (config_artifact_id, _) = self
            .store
            .publish_evidence(&config)
            .map_err(|error| ResearchSessionFailure::invalid(error.to_string()))?;
        let checkpoint = LiveResearchSessionCheckpoint::build(
            config.session_id.clone(),
            config_artifact_id.clone(),
            LiveResearchSessionStatus::Active,
            None,
            None,
            None,
            None,
            None,
            0,
            0,
            0,
            0,
            "0",
            None,
        )
        .map_err(|error| ResearchSessionFailure::invalid(error.to_string()))?;
        let (checkpoint_artifact_id, _) = self
            .store
            .publish_evidence(&checkpoint)
            .map_err(|error| ResearchSessionFailure::unavailable(error.to_string()))?;
        self.write_pointer(&checkpoint_artifact_id)
            .map_err(|error| ResearchSessionFailure::unavailable(error.to_string()))?;

        self.last_checkpoint = Some((checkpoint.clone(), checkpoint_artifact_id.clone()));
        self.last_config = Some(config.clone());
        self.active = Some(ActiveResearchSession {
            config,
            config_artifact_id,
            spec,
            checkpoint,
            checkpoint_artifact_id,
        });
        Ok(self.status())
    }

    fn stop_session(
        &mut self,
        session_id: &str,
    ) -> Result<ResearchSessionStatus, ResearchSessionFailure> {
        let Some(mut active) = self.active.take() else {
            if self
                .last_checkpoint
                .as_ref()
                .is_some_and(|(checkpoint, _)| {
                    checkpoint.status == LiveResearchSessionStatus::Stopped
                        && checkpoint.session_id == session_id
                })
            {
                return Ok(self.status());
            }
            return Err(ResearchSessionFailure::invalid(
                "requested research session is not active",
            ));
        };
        if active.config.session_id != session_id {
            self.active = Some(active);
            return Err(ResearchSessionFailure::conflict(
                "stop request targets a different research session",
            ));
        }

        let stopped = LiveResearchSessionCheckpoint::build(
            active.checkpoint.session_id.clone(),
            active.config_artifact_id.clone(),
            LiveResearchSessionStatus::Stopped,
            Some(active.checkpoint_artifact_id.clone()),
            active.checkpoint.last_evaluated_entry_open_time_ms.clone(),
            active.checkpoint.latest_decision_artifact_id.clone(),
            active.checkpoint.latest_paper_trade_artifact_id.clone(),
            active.checkpoint.last_blocker.clone(),
            active.checkpoint.decision_count,
            active.checkpoint.blocked_count,
            active.checkpoint.would_submit_count,
            active.checkpoint.paper_trade_count,
            active.checkpoint.paper_realized_net_pnl_quote.clone(),
            active.checkpoint.paper_open_position.clone(),
        )
        .map_err(|error| ResearchSessionFailure::invalid(error.to_string()))?;
        let (artifact_id, _) = self
            .store
            .publish_evidence(&stopped)
            .map_err(|error| ResearchSessionFailure::unavailable(error.to_string()))?;
        self.write_pointer(&artifact_id)
            .map_err(|error| ResearchSessionFailure::unavailable(error.to_string()))?;
        self.last_config = Some(active.config);
        self.last_checkpoint = Some((stopped, artifact_id));
        Ok(self.status())
    }

    async fn handle_market_wakeup(
        &mut self,
        wakeup: PublicMarketWakeup,
    ) -> Result<(), ResearchSessionRuntimeError> {
        if wakeup.sequence == 0 {
            return Ok(());
        }
        let Some(active) = self.active.as_ref() else {
            return Ok(());
        };
        let bucket_open_ms = (wakeup.received_at_ms / ONE_HOUR_MS) * ONE_HOUR_MS;
        if active
            .checkpoint
            .last_evaluated_entry_open_time_ms
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|last| last >= bucket_open_ms)
        {
            return Ok(());
        }

        let quality = match self
            .public_ws
            .quality_snapshot(
                &active.config.instrument_id,
                wakeup.received_at_ms,
                PUBLIC_MARKET_MAX_AGE_MS,
                false,
            )
            .await
        {
            Ok(value) => value,
            Err(_) => return Ok(()),
        };
        if quality.quality != MarketReadiness::Fresh || !quality.sequence_continuity_proven {
            return Ok(());
        }

        let reference = self.public_ws.reference_snapshot().await;
        if reference.generation().as_str() != quality.reference_generation {
            return Ok(());
        }

        let paper_funding = if active.config.mode == LiveResearchSessionMode::Paper
            && active.checkpoint.paper_open_position.is_some()
        {
            match self
                .market
                .funding_history(
                    &reference,
                    &active.config.instrument_id,
                    FUNDING_HISTORY_LIMIT,
                )
                .await
            {
                Ok(snapshot) => Some(
                    snapshot
                        .events
                        .into_iter()
                        .map(|event| ResearchFundingEvent {
                            schema: RESEARCH_FUNDING_SCHEMA_V1.to_owned(),
                            available_time_ms: event.funding_time_ms.clone(),
                            funding_time_ms: event.funding_time_ms,
                            funding_rate: event.funding_rate,
                            realized_rate: event.realized_rate,
                            formula_type: event.formula_type,
                            method: event.method,
                        })
                        .collect::<Vec<_>>(),
                ),
                Err(_) => return Ok(()),
            }
        } else {
            None
        };

        let metadata = baseline_strategy_research_metadata(active.spec.strategy);
        let history_limit = u16::from(metadata.signal_lookback_bars).saturating_add(2);
        let history = match self
            .market
            .history(
                &reference,
                &active.config.instrument_id,
                "1H",
                history_limit,
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                return self
                    .persist_blocked_bucket(
                        bucket_open_ms,
                        format!("MARKET_HISTORY_UNAVAILABLE:{error}"),
                    )
                    .map(|_| ());
            }
        };
        let current = history.candles.last();
        if current
            .and_then(|candle| candle.open_time_ms.parse::<u64>().ok())
            != Some(bucket_open_ms)
        {
            return self
                .persist_blocked_bucket(
                    bucket_open_ms,
                    "CURRENT_ENTRY_BUCKET_UNAVAILABLE".to_owned(),
                )
                .map(|_| ());
        }

        self.evaluate_bucket(bucket_open_ms, history, paper_funding)
            .await
    }

    async fn evaluate_bucket(
        &mut self,
        bucket_open_ms: u64,
        history: okx_observation::MarketHistorySnapshot,
        paper_funding: Option<Vec<ResearchFundingEvent>>,
    ) -> Result<(), ResearchSessionRuntimeError> {
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };

        let mut latest_paper_trade_artifact_id =
            active.checkpoint.latest_paper_trade_artifact_id.clone();
        let mut paper_trade_count = active.checkpoint.paper_trade_count;
        let mut paper_realized_net_pnl_quote =
            active.checkpoint.paper_realized_net_pnl_quote.clone();
        let mut paper_open_position = active.checkpoint.paper_open_position.clone();

        if active.config.mode == LiveResearchSessionMode::Paper {
            if let Some(position) = paper_open_position.take() {
                let Some(exit_candle) = history.candles.last() else {
                    self.active = Some(active);
                    return Ok(());
                };
                let funding = paper_funding.as_deref().unwrap_or(&[]);
                match settle_paper_virtual_position(
                    &active.config.session_id,
                    &active.spec,
                    &position,
                    &exit_candle.open_time_ms,
                    &exit_candle.open,
                    funding,
                ) {
                    Ok(trade) => {
                        paper_realized_net_pnl_quote =
                            paper_realized_pnl_after_trade(
                                &paper_realized_net_pnl_quote,
                                &trade,
                            )?;
                        paper_trade_count = paper_trade_count.saturating_add(1);
                        let (artifact_id, _) = self.store.publish_evidence(&trade)?;
                        latest_paper_trade_artifact_id = Some(artifact_id);
                    }
                    Err(error) => {
                        active.checkpoint.paper_open_position = Some(position);
                        self.active = Some(active);
                        self.persist_terminal_block(
                            bucket_open_ms,
                            format!("PAPER_SETTLEMENT_FAILED:{error}"),
                        )?;
                        return Ok(());
                    }
                }
            }
        }

        active.checkpoint.latest_paper_trade_artifact_id =
            latest_paper_trade_artifact_id.clone();
        active.checkpoint.paper_trade_count = paper_trade_count;
        active.checkpoint.paper_realized_net_pnl_quote =
            paper_realized_net_pnl_quote.clone();
        active.checkpoint.paper_open_position = paper_open_position.clone();

        let Some(account) = self.account.as_ref() else {
            self.active = Some(active);
            return self
                .persist_blocked_bucket(
                    bucket_open_ms,
                    "ACCOUNT_OBSERVER_UNAVAILABLE".to_owned(),
                )
                .map(|_| ());
        };
        let assembled = match assemble_account_snapshot(ObservationQueryContext::live_with_private(
            &self.public_ws,
            &self.market,
            None,
            Some(account),
            self.private_ws.as_ref(),
        ))
        .await
        {
            Ok(value) => value,
            Err(_) => {
                self.active = Some(active);
                return self
                    .persist_blocked_bucket(
                        bucket_open_ms,
                        "ACCOUNT_SNAPSHOT_UNAVAILABLE".to_owned(),
                    )
                    .map(|_| ());
            }
        };
        let ledger = match account.ledger_facts(&assembled.snapshot).await {
            Ok(value) => value,
            Err(_) => {
                self.active = Some(active);
                return self
                    .persist_blocked_bucket(
                        bucket_open_ms,
                        "ACCOUNT_LEDGER_UNAVAILABLE".to_owned(),
                    )
                    .map(|_| ());
            }
        };
        let risk_evidence = match candidate_risk_context_from_account(
            &assembled.snapshot,
            &ledger.summary,
            &active.spec.mandate,
            &active.spec.instrument_id,
            assembled.quality == DataQuality::Fresh,
        ) {
            Ok(value) => value,
            Err(error) => {
                self.active = Some(active);
                return self
                    .persist_blocked_bucket(
                        bucket_open_ms,
                        format!("RISK_CONTEXT_INVALID:{error}"),
                    )
                    .map(|_| ());
            }
        };
        let mut risk_context = risk_evidence.context;
        if !risk_evidence.unsupported_daily_loss_currencies.is_empty() {
            risk_context.account_is_fresh = false;
        }

        let decision = match evaluate_live_research_decision(
            &active.spec,
            &history,
            &risk_context,
        ) {
            Ok(value) => value,
            Err(error) => {
                self.active = Some(active);
                return self
                    .persist_blocked_bucket(
                        bucket_open_ms,
                        format!("LIVE_DECISION_BLOCKED:{error}"),
                    )
                    .map(|_| ());
            }
        };
        if decision.exchange_mutation_authority {
            return Err(ResearchSessionRuntimeError::PersistedState);
        }
        let (decision_artifact_id, _) = self.store.publish_evidence(&decision)?;

        if active.config.mode == LiveResearchSessionMode::Paper
            && decision.disposition == LiveResearchDisposition::WouldSubmit
        {
            let Some(direction) = decision.direction else {
                return Err(ResearchSessionRuntimeError::PersistedState);
            };
            paper_open_position = Some(PaperVirtualPosition {
                decision_evidence_artifact_id: decision_artifact_id.clone(),
                direction,
                entry_time_ms: decision.entry_open_time_ms.clone(),
                entry_price: decision.entry_price.clone(),
            });
        }

        let decision_count = active.checkpoint.decision_count.saturating_add(1);
        let would_submit_count = active
            .checkpoint
            .would_submit_count
            .saturating_add(if decision.disposition == LiveResearchDisposition::WouldSubmit {
                1
            } else {
                0
            });
        let next = LiveResearchSessionCheckpoint::build(
            active.checkpoint.session_id.clone(),
            active.config_artifact_id.clone(),
            LiveResearchSessionStatus::Active,
            Some(active.checkpoint_artifact_id.clone()),
            Some(bucket_open_ms.to_string()),
            Some(decision_artifact_id),
            latest_paper_trade_artifact_id,
            None,
            decision_count,
            active.checkpoint.blocked_count,
            would_submit_count,
            paper_trade_count,
            paper_realized_net_pnl_quote,
            paper_open_position,
        )?;
        let (artifact_id, _) = self.store.publish_evidence(&next)?;
        self.write_pointer(&artifact_id)?;

        active.checkpoint = next.clone();
        active.checkpoint_artifact_id = artifact_id.clone();
        self.last_checkpoint = Some((next, artifact_id));
        self.active = Some(active);
        Ok(())
    }

    fn persist_blocked_bucket(
        &mut self,
        bucket_open_ms: u64,
        blocker: String,
    ) -> Result<ResearchSessionStatus, ResearchSessionRuntimeError> {
        let Some(mut active) = self.active.take() else {
            return Ok(self.status());
        };
        let next = LiveResearchSessionCheckpoint::build(
            active.checkpoint.session_id.clone(),
            active.config_artifact_id.clone(),
            LiveResearchSessionStatus::Active,
            Some(active.checkpoint_artifact_id.clone()),
            Some(bucket_open_ms.to_string()),
            active.checkpoint.latest_decision_artifact_id.clone(),
            active.checkpoint.latest_paper_trade_artifact_id.clone(),
            Some(blocker),
            active.checkpoint.decision_count,
            active.checkpoint.blocked_count.saturating_add(1),
            active.checkpoint.would_submit_count,
            active.checkpoint.paper_trade_count,
            active.checkpoint.paper_realized_net_pnl_quote.clone(),
            active.checkpoint.paper_open_position.clone(),
        )?;
        let (artifact_id, _) = self.store.publish_evidence(&next)?;
        self.write_pointer(&artifact_id)?;
        active.checkpoint = next.clone();
        active.checkpoint_artifact_id = artifact_id.clone();
        self.last_checkpoint = Some((next, artifact_id));
        self.active = Some(active);
        Ok(self.status())
    }

    fn persist_terminal_block(
        &mut self,
        bucket_open_ms: u64,
        blocker: String,
    ) -> Result<(), ResearchSessionRuntimeError> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let next = LiveResearchSessionCheckpoint::build(
            active.checkpoint.session_id.clone(),
            active.config_artifact_id,
            LiveResearchSessionStatus::Stopped,
            Some(active.checkpoint_artifact_id),
            Some(bucket_open_ms.to_string()),
            active.checkpoint.latest_decision_artifact_id,
            active.checkpoint.latest_paper_trade_artifact_id,
            Some(blocker),
            active.checkpoint.decision_count,
            active.checkpoint.blocked_count.saturating_add(1),
            active.checkpoint.would_submit_count,
            active.checkpoint.paper_trade_count,
            active.checkpoint.paper_realized_net_pnl_quote,
            active.checkpoint.paper_open_position,
        )?;
        let (artifact_id, _) = self.store.publish_evidence(&next)?;
        self.write_pointer(&artifact_id)?;
        self.last_checkpoint = Some((next, artifact_id));
        Ok(())
    }

    fn status(&self) -> ResearchSessionStatus {
        let active = self.active.as_ref();
        let checkpoint = active
            .map(|value| (&value.checkpoint, value.checkpoint_artifact_id.as_str()))
            .or_else(|| {
                self.last_checkpoint
                    .as_ref()
                    .map(|(checkpoint, artifact_id)| (checkpoint, artifact_id.as_str()))
            });
        let config = active
            .map(|value| &value.config)
            .or(self.last_config.as_ref());

        ResearchSessionStatus {
            schema: RESEARCH_SESSION_STATUS_SCHEMA_V1,
            state: match checkpoint.map(|(value, _)| value.status) {
                Some(LiveResearchSessionStatus::Active) => "ACTIVE",
                Some(LiveResearchSessionStatus::Stopped) => "STOPPED",
                None => "IDLE",
            },
            session_id: checkpoint.map(|(value, _)| value.session_id.clone()),
            mode: config.map(|value| value.mode),
            instrument_id: config.map(|value| value.instrument_id.clone()),
            strategy: config.map(|value| value.strategy),
            strategy_version: config.map(|value| value.strategy_version.clone()),
            config_artifact_id: checkpoint.map(|(value, _)| value.config_artifact_id.clone()),
            checkpoint_artifact_id: checkpoint.map(|(_, artifact_id)| artifact_id.to_owned()),
            last_evaluated_entry_open_time_ms: checkpoint
                .and_then(|(value, _)| value.last_evaluated_entry_open_time_ms.clone()),
            latest_decision_artifact_id: checkpoint
                .and_then(|(value, _)| value.latest_decision_artifact_id.clone()),
            latest_paper_trade_artifact_id: checkpoint
                .and_then(|(value, _)| value.latest_paper_trade_artifact_id.clone()),
            last_blocker: checkpoint.and_then(|(value, _)| value.last_blocker.clone()),
            decision_count: checkpoint.map_or(0, |(value, _)| value.decision_count),
            blocked_count: checkpoint.map_or(0, |(value, _)| value.blocked_count),
            would_submit_count: checkpoint.map_or(0, |(value, _)| value.would_submit_count),
            paper_trade_count: checkpoint.map_or(0, |(value, _)| value.paper_trade_count),
            paper_realized_net_pnl_quote: checkpoint
                .map_or_else(|| "0".to_owned(), |(value, _)| {
                    value.paper_realized_net_pnl_quote.clone()
                }),
            paper_position_open: checkpoint
                .is_some_and(|(value, _)| value.paper_open_position.is_some()),
            source_tree: BUILD_SOURCE_TREE,
            exchange_mutation_authority: false,
        }
    }

    fn pointer_path(&self) -> PathBuf {
        self.root.join("research").join("live-session-pointer.json")
    }

    fn read_pointer(&self) -> Result<Option<ResearchSessionPointer>, ResearchSessionRuntimeError> {
        let path = self.pointer_path();
        match fs::read(path) {
            Ok(bytes) => {
                let pointer: ResearchSessionPointer = serde_json::from_slice(&bytes)?;
                if pointer.schema != POINTER_SCHEMA_V1
                    || !pointer.checkpoint_artifact_id.starts_with("sha256:")
                {
                    return Err(ResearchSessionRuntimeError::PersistedState);
                }
                Ok(Some(pointer))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write_pointer(&self, checkpoint_artifact_id: &str) -> Result<(), ResearchSessionRuntimeError> {
        let path = self.pointer_path();
        let parent = path.parent().ok_or(ResearchSessionRuntimeError::PersistedState)?;
        fs::create_dir_all(parent)?;
        let pointer = ResearchSessionPointer {
            schema: POINTER_SCHEMA_V1.to_owned(),
            checkpoint_artifact_id: checkpoint_artifact_id.to_owned(),
        };
        let bytes = serde_json::to_vec(&pointer)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }
}

