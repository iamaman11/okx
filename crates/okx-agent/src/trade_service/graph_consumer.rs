//! Engine-side bounded Research outbox consumer and explicit-policy admission.
//!
//! Broadcast is a wake-up hint only. Every accepted event comes from the
//! Research SQLite journal after the last ORIGINAL finance SQL checkpoint.
//! Never fabricate Hold, accept a missing sequence, or skip an actionable bar.
use super::graph_target::GraphTargetBinding;
use super::{AccountResources, GRAPH_BINDINGS_BATCH, TradeService};
use crate::research_client::{GraphSignalEvent, ResearchClient};
use okx_execution::position_target::TargetPlan;
use okx_protocol::research_ipc::{
    GraphFeedFaultReason, GraphPublicationRequest, GraphSignalPublication,
};
use okx_protocol::trading::{GraphExecutionPolicy, GraphStrategyConfigurationV1, TradeRequest};
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::sync::broadcast;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GraphConsumeOutcome {
    Idle {
        accepted: u16,
        confirmed_sequence: u64,
        has_more: bool,
    },
    /// The cursor is NOT acknowledged. Engine must prepare the financial
    /// action or wait for settlement and retry the same event first.
    Blocked {
        publication: GraphSignalPublication,
        plan: TargetPlan,
    },
    /// A bounded task has been given one exact immutable request. This is not
    /// a venue acknowledgement or evidence that an order was posted.
    OpenAdmissionScheduled { sequence: u64 },
    FeedUnavailable(GraphFeedFaultReason),
    ResearchUnavailable,
    UnrelatedNotifications,
}

/// A lightweight, explicitly activated Engine subscriber. Creating it does
/// not open orders, subscribe to market reads or start a Tokio task.
pub(crate) struct EngineGraphConsumer {
    account: String,
    binding: GraphTargetBinding,
    notices: broadcast::Receiver<GraphSignalEvent>,
    needs_replay: bool,
}

impl EngineGraphConsumer {
    pub(crate) fn new(
        research: &ResearchClient,
        account: String,
        binding: GraphTargetBinding,
    ) -> Result<Self, String> {
        // `account` selects the Engine registry alias; `binding.account_key`
        // independently pins the original finance owner identity.
        if !binding.valid() || account.trim().is_empty() {
            return Err("GRAPH_CONSUMER_BINDING_INVALID".into());
        }
        Ok(Self {
            account,
            binding,
            notices: research.subscribe_graph_signals(),
            needs_replay: true,
        })
    }

    /// Read at most one bounded journal page. The most recently ACKed
    /// finance cursor is always the initial authority, never a volatile
    /// broadcast event, local counter, or Research process memory.
    pub(crate) async fn replay_one_page(
        &self,
        engine: &TradeService,
        research: &ResearchClient,
    ) -> Result<GraphConsumeOutcome, String> {
        let confirmed = engine
            .graph_confirmed_cursor(&self.account, &self.binding)
            .await?
            .ok_or("GRAPH_FINANCE_BINDING_MISSING")?;
        let request = GraphPublicationRequest {
            session_id: self.binding.session_id.clone(),
            graph_sha256: self.binding.graph_sha256.clone(),
            after_sequence: confirmed.last_sequence(),
            limit: GraphPublicationRequest::MAX_PAGE,
        };
        let page = research.replay_graph_publications(request.clone()).await?;
        if !page.matches_request(&request) {
            return Err("GRAPH_CONSUMER_JOURNAL_PAGE_INVALID".into());
        }
        let resources = engine.resources(&self.account).await?;
        if resources.configuration.account_key.wire() != self.binding.account_key {
            return Err("GRAPH_CONSUMER_ACCOUNT_MISMATCH".into());
        }
        let mut last = confirmed;
        let mut accepted = 0_u16;
        for publication in page.publications {
            let preview = engine
                .preview_graph_publication(&self.account, &self.binding, &last, &publication)
                .await?
                .ok_or("GRAPH_CONSUMER_JOURNAL_DUPLICATE")?;
            if preview.plan != TargetPlan::NoChange {
                return Ok(GraphConsumeOutcome::Blocked {
                    publication,
                    plan: preview.plan,
                });
            }
            let _ack = resources
                .execution
                .store
                .acknowledge_graph_idle(
                    super::deadline(),
                    self.binding.storage_binding(),
                    GraphTargetBinding::storage_cursor(&preview.next_cursor),
                    super::now(),
                )
                .await
                .map_err(super::err)?;
            last = preview.next_cursor;
            accepted += 1;
        }
        Ok(GraphConsumeOutcome::Idle {
            accepted,
            confirmed_sequence: last.last_sequence(),
            has_more: page.has_more,
        })
    }

    /// Reconcile the journal on startup, then on every matching notification.
    /// Lost broadcast frames trigger the same durable replay path, never a
    /// guessed state. Unrelated sessions are ignored in a bounded batch.
    pub(crate) async fn next(
        &mut self,
        engine: &TradeService,
        research: &ResearchClient,
    ) -> Result<GraphConsumeOutcome, String> {
        if self.needs_replay {
            let result = self.replay_one_page(engine, research).await?;
            self.needs_replay = matches!(
                result,
                GraphConsumeOutcome::Blocked { .. }
                    | GraphConsumeOutcome::Idle { has_more: true, .. }
            );
            return Ok(result);
        }
        // Wait for ONE wake-up; drain the remaining already-queued frames
        // without waiting for another unrelated session to publish. The
        // per-turn bound must limit *work*, not require 64 notifications.
        let mut next = self.notices.recv().await;
        for index in 0..GraphPublicationRequest::MAX_PAGE {
            match next {
                Ok(GraphSignalEvent::Published(publication))
                    if publication.session_id == self.binding.session_id
                        && publication.graph_sha256 == self.binding.graph_sha256 =>
                {
                    self.needs_replay = true;
                    return self.next_replay(engine, research).await;
                }
                Ok(GraphSignalEvent::FeedUnavailable(fault))
                    if fault.session_id == self.binding.session_id
                        && fault.graph_sha256 == self.binding.graph_sha256 =>
                {
                    self.needs_replay = true;
                    return Ok(GraphConsumeOutcome::FeedUnavailable(fault.reason));
                }
                Ok(GraphSignalEvent::ServiceUnavailable)
                | Err(broadcast::error::RecvError::Closed) => {
                    self.needs_replay = true;
                    return Ok(GraphConsumeOutcome::ResearchUnavailable);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    self.needs_replay = true;
                    return self.next_replay(engine, research).await;
                }
                Ok(_) => {}
            }
            if index + 1 == GraphPublicationRequest::MAX_PAGE {
                break;
            }
            next = match self.notices.try_recv() {
                Ok(notification) => Ok(notification),
                Err(broadcast::error::TryRecvError::Empty) => {
                    return Ok(GraphConsumeOutcome::UnrelatedNotifications);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    Err(broadcast::error::RecvError::Closed)
                }
                Err(broadcast::error::TryRecvError::Lagged(missed)) => {
                    Err(broadcast::error::RecvError::Lagged(missed))
                }
            };
        }
        Ok(GraphConsumeOutcome::UnrelatedNotifications)
    }

    async fn next_replay(
        &mut self,
        engine: &TradeService,
        research: &ResearchClient,
    ) -> Result<GraphConsumeOutcome, String> {
        let result = self.replay_one_page(engine, research).await?;
        self.needs_replay = matches!(
            result,
            GraphConsumeOutcome::Blocked { .. } | GraphConsumeOutcome::Idle { has_more: true, .. }
        );
        Ok(result)
    }
}

impl TradeService {
    /// Execute one explicitly armed strategy through the ORIGINAL finance,
    /// RiskBook and venue owner. Observational Start never reaches this path.
    /// The account-local permit coalesces repeated maintenance turns while
    /// SQL identity and the original TradeService protect crash/retry cases.
    async fn schedule_graph_open(
        self: &Arc<Self>,
        resources: &Arc<AccountResources>,
        binding: &GraphTargetBinding,
        publication: &GraphSignalPublication,
    ) -> Result<bool, String> {
        let instance = resources
            .execution
            .store
            .strategy_instances(super::deadline())
            .await
            .map_err(super::err)?
            .into_iter()
            .find(|v| v.instance_id == binding.instance_id)
            .ok_or("GRAPH_OPEN_STRATEGY_MISSING")?;
        let configuration: GraphStrategyConfigurationV1 =
            serde_json::from_str(&instance.parameters)
                .map_err(|_| "GRAPH_OPEN_CONFIGURATION_INVALID")?;
        if !configuration.valid()
            || !configuration.instruments.contains(&binding.instrument_id)
            || configuration.timeframe_ms != binding.timeframe_ms
        {
            return Err("GRAPH_OPEN_CONFIGURATION_MISMATCH".into());
        }
        if matches!(configuration.execution, GraphExecutionPolicy::Observe) {
            return Ok(false);
        }
        let command = binding
            .open_request(&instance, publication, &configuration.execution)?
            .ok_or("GRAPH_OPEN_POLICY_UNARMED")?;
        let TradeRequest::Execute { idempotency_key, .. } = &command else {
            return Err("GRAPH_OPEN_EXECUTE_REQUIRED".into());
        };
        let permit = match resources.graph_order_dispatch.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return Ok(false),
        };
        let account_key = resources.configuration.account_key.wire();
        if !self.recovery_schedule.lock().await.due(
            &account_key,
            idempotency_key,
            Instant::now(),
        ) {
            return Ok(false);
        }
        let trade = Arc::clone(self);
        let account = resources.configuration.alias.clone();
        let binding = binding.clone();
        let publication = publication.clone();
        let key = idempotency_key.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match trade
                .execute_graph_open(&account, &binding, &publication, &command)
                .await
            {
                Ok(_) => {
                    eprintln!(
                        "OKX_GRAPH_ENGINE_V1 instance={} instrument={} sequence={} state=ADMISSION_RETURNED decision={}",
                        binding.instance_id, binding.instrument_id, publication.sequence, key
                    );
                }
                Err(reason) => {
                    // Original finance SQL and status/recovery remain the
                    // authority when an error occurs after PREPARED.
                    let code = if reason.len() <= 96 && reason.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') {
                        reason.as_str()
                    } else {
                        "GRAPH_OPEN_ADMISSION_FAILURE"
                    };
                    eprintln!(
                        "OKX_GRAPH_ENGINE_V1 instance={} instrument={} sequence={} state=ADMISSION_UNAVAILABLE reason={code}",
                        binding.instance_id, binding.instrument_id, publication.sequence
                    );
                }
            }
        });
        Ok(true)
    }

    /// One bounded journal maintenance turn. Bindings are always
    /// rediscovered from original finance SQL, including after Engine restart.
    /// It may persist idle ACKs. Only an explicitly armed Open can enqueue an
    /// original TradeService admission; Close and settlement stay blocked.
    pub(super) async fn graph_research_turn(
        self: &Arc<Self>,
        resources: &Arc<AccountResources>,
    ) -> Result<Vec<GraphConsumeOutcome>, String> {
        let account = resources.configuration.account_key.wire();
        let offset = self.recovery_schedule.lock().await.graph_page(&account);
        let page = resources
            .execution
            .store
            .graph_bindings_page(super::deadline(), GRAPH_BINDINGS_BATCH, offset)
            .await
            .map_err(super::err)?;
        self.recovery_schedule
            .lock()
            .await
            .advance_graph_page(&account, offset, page.scanned);
        if page.active.is_empty() {
            return Ok(Vec::new());
        }
        let research = self
            .signal_research
            .read()
            .await
            .clone()
            .ok_or("GRAPH_RESEARCH_NOT_READY")?;
        let mut outcomes = Vec::with_capacity(page.active.len());
        for stored in page.active {
            let binding = GraphTargetBinding::from_storage(stored)?;
            let consumer = EngineGraphConsumer::new(
                &research,
                resources.configuration.alias.clone(),
                binding.clone(),
            )?;
            let mut outcome = tokio::time::timeout(
                Duration::from_millis(180),
                consumer.replay_one_page(self, &research),
            )
            .await
            .map_err(|_| "GRAPH_RESEARCH_REPLAY_TIMEOUT")??;
            let open_candidate = match &outcome {
                GraphConsumeOutcome::Blocked {
                    publication,
                    plan: TargetPlan::Open(_),
                } => Some(publication.clone()),
                _ => None,
            };
            if let Some(publication) = open_candidate {
                match self.schedule_graph_open(resources, &binding, &publication).await {
                    Ok(true) => {
                        outcome = GraphConsumeOutcome::OpenAdmissionScheduled {
                            sequence: publication.sequence,
                        };
                    }
                    Ok(false) => {}
                    Err(reason) => {
                        let code = if reason.len() <= 96 && reason.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') {
                            reason.as_str()
                        } else {
                            "GRAPH_OPEN_POLICY_FAILURE"
                        };
                        eprintln!(
                            "OKX_GRAPH_ENGINE_V1 account={} instance={} state=OPEN_NOT_SCHEDULED reason={code}",
                            account, binding.instance_id
                        );
                    }
                }
            }
            if let GraphConsumeOutcome::Blocked {
                ref publication,
                ref plan,
            } = outcome
            {
                let reason = match plan {
                    TargetPlan::Open(_) => "OPEN_AWAITING_EXECUTION",
                    TargetPlan::CloseExisting(_) => "CLOSE_AWAITING_EXECUTION",
                    TargetPlan::AwaitSettlement => "AWAITING_SETTLEMENT",
                    TargetPlan::NoChange => "PLAN_INCONSISTENT",
                };
                eprintln!(
                    "OKX_GRAPH_ENGINE_V1 account={} instance={} instrument={} sequence={} state=BLOCKED reason={}",
                    account,
                    binding.instance_id,
                    binding.instrument_id,
                    publication.sequence,
                    reason
                );
            }
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }
}
