//! Read-only, restartable graph sessions. The Worker owns ephemeral mathematical
//! state; the Engine owns the durable decision/order cursor and risk authority.
use super::{executor::CpuExecutor, graph_outbox::GraphOutbox, graph_resolver};
use okx_analysis::query_plan::ClosedBar;
use okx_protocol::research_ipc::{
    GraphClosedBar, GraphMissingPolicy, GraphPosition, GraphPublicationPage,
    GraphPublicationRequest, GraphSessionId, GraphSessionReply, GraphSessionRequest,
    GraphSignalDefinition, GraphSignalIntent, GraphSignalPublication,
};
use okx_research::graph_signal::{
    GraphIntent, GraphSignalSpec, GraphSignalStream, MissingSignalPolicy, TargetPosition,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::Ordering},
};

const MAX_SESSIONS: usize = 32;

#[derive(Clone)]
pub(super) struct GraphSessions {
    slots: Arc<Mutex<BTreeMap<GraphSessionId, Arc<Mutex<GraphSlot>>>>>,
    outbox: GraphOutbox,
}

struct GraphSlot {
    digest: String,
    history_digest: String,
    capacity: u64,
    restored_interval_ms: Option<u64>,
    runner: GraphSignalStream,
    last_bar: GraphClosedBar,
    last_intent: GraphSignalIntent,
    needs_restore: bool,
    feed_active: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FeedCursor {
    pub last_closed_ms: u64,
    pub observed_bars: u64,
}

#[cfg(test)]
impl Default for GraphSessions {
    fn default() -> Self {
        Self {
            slots: Default::default(),
            outbox: GraphOutbox::memory(),
        }
    }
}

fn convert(bar: GraphClosedBar) -> ClosedBar {
    ClosedBar {
        timestamp_ms: i64::try_from(bar.time_ms).expect("validated timestamp"),
        close: bar.close,
    }
}

fn verified_history_interval(history: &[GraphClosedBar]) -> Option<u64> {
    let period = history.get(1)?.time_ms.checked_sub(history[0].time_ms)?;
    if period == 0
        || history.iter().any(|bar| bar.time_ms % period != 0)
        || history
            .windows(2)
            .any(|pair| pair[1].time_ms - pair[0].time_ms != period)
    {
        return None;
    }
    Some(period)
}

fn intent(value: GraphIntent) -> GraphSignalIntent {
    match value {
        GraphIntent::Hold => GraphSignalIntent::Hold,
        GraphIntent::SetTarget(target) => GraphSignalIntent::SetTarget(match target {
            TargetPosition::Flat => GraphPosition::Flat,
            TargetPosition::Long => GraphPosition::Long,
            TargetPosition::Short => GraphPosition::Short,
        }),
    }
}

fn reply(session_id: GraphSessionId, slot: &GraphSlot) -> GraphSessionReply {
    GraphSessionReply::Evaluated {
        schema: GraphSessionReply::SCHEMA.into(),
        session_id,
        graph_sha256: slot.digest.clone(),
        last_closed_ms: slot.last_bar.time_ms,
        observed_bars: slot.runner.processed_bars(),
        intent: slot.last_intent,
    }
}

impl GraphSessions {
    pub(super) fn open(path: PathBuf) -> Result<Self, String> {
        Ok(Self {
            slots: Default::default(),
            outbox: GraphOutbox::open(path)?,
        })
    }

    /// Reject a new moving-window seed if any signal for this identity has
    /// reached the durable journal. A restart needs exact-prefix recovery.
    pub(super) async fn ensure_fresh_bootstrap(
        &self,
        cpu: &CpuExecutor,
        session_id: GraphSessionId,
    ) -> Result<(), String> {
        let outbox = self.outbox.clone();
        cpu.run(Default::default(), move |_| {
            if outbox.has_session_publications(&session_id)? {
                return Err("GRAPH_BOOTSTRAP_PUBLISHED_SESSION_REQUIRES_RECOVERY".into());
            }
            Ok(())
        })
        .await
    }

    /// Query a committed signal page without activating or restoring a runner.
    pub(super) async fn replay(
        &self,
        cpu: &CpuExecutor,
        request: GraphPublicationRequest,
    ) -> Result<GraphPublicationPage, String> {
        if !request.valid() {
            return Err("GRAPH_PUBLICATION_REQUEST_INVALID".into());
        }
        let outbox = self.outbox.clone();
        cpu.run(Default::default(), move |cancel| {
            if cancel.load(Ordering::Acquire) {
                return Err("CANCELLED".into());
            }
            outbox.page(&request)
        })
        .await
    }

    /// Claim a restored graph for exactly one Research-owned public feed.
    /// A concurrent Agent-driven Advance must never race this owner.
    pub(super) async fn claim_feed(
        &self,
        cpu: &CpuExecutor,
        session_id: GraphSessionId,
        graph_sha256: String,
        timeframe_ms: u64,
    ) -> Result<FeedCursor, String> {
        let slots = self.slots.clone();
        cpu.run(Default::default(), move |_| {
            let active = slots
                .lock()
                .map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?
                .get(&session_id)
                .cloned()
                .ok_or("GRAPH_SESSION_RESTORE_REQUIRED")?;
            let mut active = active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
            if active.digest != graph_sha256 {
                return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
            }
            if active.needs_restore {
                return Err("GRAPH_SESSION_RESTORE_REQUIRED".into());
            }
            if active.feed_active {
                return Err("GRAPH_FEED_ALREADY_ACTIVE".into());
            }
            if timeframe_ms == 0 || active.restored_interval_ms != Some(timeframe_ms) {
                return Err("GRAPH_FEED_TIMEFRAME_MISMATCH".into());
            }
            if active.runner.processed_bars() >= active.capacity {
                return Err("GRAPH_FEED_CAPACITY_EXHAUSTED".into());
            }
            active.feed_active = true;
            Ok(FeedCursor {
                last_closed_ms: active.last_bar.time_ms,
                observed_bars: active.runner.processed_bars(),
            })
        })
        .await
    }

    pub(super) async fn feed_cursor(
        &self,
        cpu: &CpuExecutor,
        session_id: GraphSessionId,
        graph_sha256: String,
    ) -> Result<FeedCursor, String> {
        let slots = self.slots.clone();
        cpu.run(Default::default(), move |_| {
            let active = slots
                .lock()
                .map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?
                .get(&session_id)
                .cloned()
                .ok_or("GRAPH_SESSION_RESTORE_REQUIRED")?;
            let active = active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
            if active.digest != graph_sha256 {
                return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
            }
            if active.needs_restore || !active.feed_active {
                return Err("GRAPH_FEED_NOT_ACTIVE".into());
            }
            Ok(FeedCursor {
                last_closed_ms: active.last_bar.time_ms,
                observed_bars: active.runner.processed_bars(),
            })
        })
        .await
    }

    pub(super) async fn release_feed(
        &self,
        cpu: &CpuExecutor,
        session_id: GraphSessionId,
        graph_sha256: String,
        poison: bool,
    ) -> Result<(), String> {
        let slots = self.slots.clone();
        cpu.run(Default::default(), move |_| {
            let active = slots
                .lock()
                .map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?
                .get(&session_id)
                .cloned()
                .ok_or("GRAPH_SESSION_RESTORE_REQUIRED")?;
            let mut active = active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
            if active.digest != graph_sha256 {
                return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
            }
            active.feed_active = false;
            if poison {
                active.needs_restore = true;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn advance_from_feed(
        &self,
        cpu: &CpuExecutor,
        request: GraphSessionRequest,
    ) -> Result<(GraphSessionReply, bool), String> {
        self.execute_transition_owned(cpu, request, true).await
    }

    #[cfg(test)]
    pub(super) async fn execute(
        &self,
        cpu: &CpuExecutor,
        request: GraphSessionRequest,
    ) -> Result<GraphSessionReply, String> {
        self.execute_transition(cpu, request)
            .await
            .map(|(reply, _)| reply)
    }

    /// The bool means a new closed-bar transition, never Restore or retry.
    /// Only new transitions may produce an unsolicited publication.
    pub(super) async fn execute_transition(
        &self,
        cpu: &CpuExecutor,
        request: GraphSessionRequest,
    ) -> Result<(GraphSessionReply, bool), String> {
        self.execute_transition_owned(cpu, request, false).await
    }

    async fn execute_transition_owned(
        &self,
        cpu: &CpuExecutor,
        request: GraphSessionRequest,
        feed_owner: bool,
    ) -> Result<(GraphSessionReply, bool), String> {
        if !request.valid() {
            return Err("GRAPH_SESSION_REQUEST_INVALID".into());
        }
        let slots = self.slots.clone();
        let outbox = self.outbox.clone();
        cpu.run(Default::default(), move |cancel| {
            if cancel.load(Ordering::Acquire) {
                return Err("CANCELLED".into());
            }
            match request {
                GraphSessionRequest::Bootstrap { .. } => {
                    Err("GRAPH_BOOTSTRAP_PUBLIC_SOURCE_REQUIRED".into())
                }
                GraphSessionRequest::Restore {
                    session_id,
                    definition,
                    capacity,
                    history,
                } => {
                    let digest = format!(
                        "{:x}",
                        Sha256::digest(serde_json::to_vec(&definition).map_err(|e| e.to_string())?)
                    );
                    let history_digest = format!(
                        "{:x}",
                        Sha256::digest(serde_json::to_vec(&history).map_err(|e| e.to_string())?)
                    );
                    let mut runner = compile(&definition, capacity)?.stream()?;
                    let mut latest = GraphSignalIntent::Hold;
                    for &bar in &history {
                        if cancel.load(Ordering::Acquire) {
                            return Err("CANCELLED".into());
                        }
                        latest = intent(runner.on_closed_bar(convert(bar))?);
                    }
                    let new = GraphSlot {
                        digest,
                        history_digest,
                        capacity,
                        restored_interval_ms: verified_history_interval(&history),
                        runner,
                        last_bar: *history.last().expect("validated nonempty"),
                        last_intent: latest,
                        needs_restore: false,
                        feed_active: false,
                    };
                    outbox.verify_restored(
                        &GraphSignalPublication::from_decision(&reply(session_id.clone(), &new))
                            .expect("evaluated graph reply"),
                    )?;
                    let mut registry = slots
                        .lock()
                        .map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?;
                    if let Some(existing) = registry.get(&session_id) {
                        let active = existing.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
                        if active.digest == new.digest
                            && active.history_digest == new.history_digest
                            && active.capacity == new.capacity
                            && active.runner.processed_bars() == new.runner.processed_bars()
                            && active.last_bar == new.last_bar
                            && active.last_intent == new.last_intent
                            && !active.needs_restore
                        {
                            return Ok((reply(session_id, &active), false));
                        }
                        return Err("GRAPH_SESSION_ALREADY_ACTIVE".into());
                    }
                    if registry.len() >= MAX_SESSIONS {
                        return Err("GRAPH_SESSION_LIMIT".into());
                    }
                    let result = reply(session_id.clone(), &new);
                    registry.insert(session_id, Arc::new(Mutex::new(new)));
                    Ok((result, false))
                }
                GraphSessionRequest::Advance {
                    session_id,
                    graph_sha256,
                    expected_bars,
                    bar,
                } => {
                    let active = {
                        let registry = slots
                            .lock()
                            .map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?;
                        registry
                            .get(&session_id)
                            .cloned()
                            .ok_or("GRAPH_SESSION_RESTORE_REQUIRED")?
                    };
                    let mut active = active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
                    if active.digest != graph_sha256 {
                        return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
                    }
                    if active.needs_restore {
                        return Err("GRAPH_SESSION_RESTORE_REQUIRED".into());
                    }
                    if active.feed_active != feed_owner {
                        return Err(if active.feed_active {
                            "GRAPH_FEED_OWNS_SESSION"
                        } else {
                            "GRAPH_FEED_NOT_ACTIVE"
                        }
                        .into());
                    }
                    let seen = active.runner.processed_bars();
                    if expected_bars == seen && bar == active.last_bar {
                        // Retried response after timeout: no second state update.
                        return Ok((reply(session_id, &active), false));
                    }
                    if expected_bars != seen + 1 {
                        return Err("GRAPH_SESSION_SEQUENCE_MISMATCH".into());
                    }
                    // Validation in CausalGraphStream happens before any state
                    // mutation, including duplicate/out-of-order and capacity.
                    let outcome = active.runner.on_closed_bar(convert(bar))?;
                    active.last_bar = bar;
                    active.last_intent = intent(outcome);
                    let decision = reply(session_id, &active);
                    let publication = GraphSignalPublication::from_decision(&decision)
                        .expect("evaluated graph reply");
                    match outbox.record(&publication) {
                        Ok(newly_committed) => Ok((decision, newly_committed)),
                        Err(error) => {
                            // The math runner moved, but no event was accepted.
                            // Fail closed until Release + Restore or restart.
                            active.needs_restore = true;
                            Err(error)
                        }
                    }
                }
                GraphSessionRequest::Release {
                    session_id,
                    graph_sha256,
                } => {
                    let mut registry = slots
                        .lock()
                        .map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?;
                    if let Some(active) = registry.get(&session_id) {
                        let active = active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
                        if active.digest != graph_sha256 {
                            return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
                        }
                        if active.feed_active {
                            return Err("GRAPH_FEED_OWNS_SESSION".into());
                        }
                    }
                    registry.remove(&session_id);
                    Ok((GraphSessionReply::Released { session_id }, false))
                }
            }
        })
        .await
    }
}

fn compile(
    definition: &GraphSignalDefinition,
    capacity: u64,
) -> Result<okx_research::graph_signal::CheckedGraphSignal, String> {
    GraphSignalSpec {
        version: definition.version.clone(),
        graph: graph_resolver::resolve(&definition.graph)?,
        signal_step: definition.signal_step.clone(),
        target_if_true: definition.target_if_true,
        target_if_false: definition.target_if_false,
        missing: match definition.missing {
            GraphMissingPolicy::Hold => MissingSignalPolicy::Hold,
            GraphMissingPolicy::Flat => MissingSignalPolicy::Flat,
        },
    }
    .compile(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn definition() -> GraphSignalDefinition {
        serde_json::from_value(json!({
            "version":"okx.graph-signal/v1",
            "graph": {
                "schema":"okx.graph/v1",
                "steps":[
                    {"id":"return","op":{"id":"returns_series","version":1},"inputs":[{"source":"close"}]},
                    {"id":"positive","op":{"id":"greater_than","version":1},
                     "inputs":[{"step":"return"}],"params":{"threshold":0.0}}
                ],
                "outputs":["positive"]
            },
            "signal_step":"positive",
            "target_if_true":1,
            "target_if_false":0,
            "missing":"hold"
        }))
        .unwrap()
    }

    fn bar(time: u64, close: Option<f64>) -> GraphClosedBar {
        GraphClosedBar {
            time_ms: time,
            close,
        }
    }

    #[test]
    fn autonomous_feed_requires_contiguous_aligned_restored_prefix() {
        assert_eq!(
            verified_history_interval(&[
                bar(60_000, Some(100.)),
                bar(120_000, Some(110.)),
                bar(180_000, None),
            ]),
            Some(60_000)
        );
        assert_eq!(
            verified_history_interval(&[
                bar(60_000, Some(100.)),
                bar(120_000, Some(110.)),
                bar(240_000, None),
            ]),
            None
        );
        assert_eq!(verified_history_interval(&[bar(60_000, Some(100.))]), None);
    }

    #[tokio::test]
    async fn causal_graph_session_replays_once_advances_and_rejects_stale_reordered_updates() {
        let registry = GraphSessions::default();
        let cpu = CpuExecutor::default();
        let id = GraphSessionId::new("account:instance:BTC".into()).unwrap();
        registry
            .ensure_fresh_bootstrap(&cpu, id.clone())
            .await
            .unwrap();
        let restore = GraphSessionRequest::Restore {
            session_id: id.clone(),
            definition: Box::new(definition()),
            capacity: 100,
            history: vec![bar(10, Some(100.)), bar(20, Some(110.))],
        };
        let (response, publish_restore) = registry
            .execute_transition(&cpu, restore.clone())
            .await
            .unwrap();
        assert!(
            !publish_restore,
            "restoring history cannot publish old trades"
        );
        assert!(response.matches_request(&restore));
        assert_eq!(
            response,
            registry.execute(&cpu, restore.clone()).await.unwrap()
        );
        let GraphSessionReply::Evaluated {
            graph_sha256: digest,
            intent,
            ..
        } = response
        else {
            panic!("typed decision")
        };
        assert_eq!(intent, GraphSignalIntent::SetTarget(GraphPosition::Long));
        let next = GraphSessionRequest::Advance {
            session_id: id.clone(),
            graph_sha256: digest.clone(),
            expected_bars: 3,
            bar: bar(30, Some(90.)),
        };
        let (result, publish_next) = registry
            .execute_transition(&cpu, next.clone())
            .await
            .unwrap();
        assert!(publish_next, "only a newly closed bar can publish");
        assert_eq!(
            registry
                .ensure_fresh_bootstrap(&cpu, id.clone())
                .await
                .unwrap_err(),
            "GRAPH_BOOTSTRAP_PUBLISHED_SESSION_REQUIRES_RECOVERY"
        );
        assert!(result.matches_request(&next));
        let (retried, publish_retry) = registry
            .execute_transition(&cpu, next.clone())
            .await
            .unwrap();
        assert!(!publish_retry, "retry must never re-emit an old intent");
        assert_eq!(result, retried);
        assert!(matches!(
            result,
            GraphSessionReply::Evaluated {
                intent: GraphSignalIntent::SetTarget(GraphPosition::Flat),
                observed_bars: 3,
                ..
            }
        ));
        assert_eq!(
            registry.execute(&cpu, restore).await.unwrap_err(),
            "GRAPH_SESSION_ALREADY_ACTIVE"
        );

        let stale = GraphSessionRequest::Advance {
            session_id: id.clone(),
            graph_sha256: digest.clone(),
            expected_bars: 3,
            bar: bar(31, Some(95.)),
        };
        assert_eq!(
            registry.execute(&cpu, stale).await.unwrap_err(),
            "GRAPH_SESSION_SEQUENCE_MISMATCH"
        );
        let out_of_order = GraphSessionRequest::Advance {
            session_id: id.clone(),
            graph_sha256: digest.clone(),
            expected_bars: 4,
            bar: bar(29, Some(95.)),
        };
        assert_eq!(
            registry.execute(&cpu, out_of_order).await.unwrap_err(),
            "composition.stream:CLOSED_BAR_NOT_MONOTONIC"
        );
        let correct = GraphSessionRequest::Advance {
            session_id: id.clone(),
            graph_sha256: digest.clone(),
            expected_bars: 4,
            bar: bar(40, None),
        };
        assert!(matches!(
            registry.execute(&cpu, correct).await.unwrap(),
            GraphSessionReply::Evaluated {
                intent: GraphSignalIntent::Hold,
                observed_bars: 4,
                ..
            }
        ));

        let wrong_hash = GraphSessionRequest::Release {
            session_id: id.clone(),
            graph_sha256: "f".repeat(64),
        };
        assert_eq!(
            registry.execute(&cpu, wrong_hash).await.unwrap_err(),
            "GRAPH_SESSION_DEFINITION_MISMATCH"
        );
        let release = GraphSessionRequest::Release {
            session_id: id.clone(),
            graph_sha256: digest,
        };
        let removed = registry.execute(&cpu, release.clone()).await.unwrap();
        assert!(removed.matches_request(&release));
        assert_eq!(removed, registry.execute(&cpu, release).await.unwrap());
    }

    #[tokio::test]
    async fn worker_restart_restores_math_without_any_persistent_trade_effect() {
        let cpu = CpuExecutor::default();
        let id = GraphSessionId::new("restart".into()).unwrap();
        let restore = GraphSessionRequest::Restore {
            session_id: id.clone(),
            definition: Box::new(definition()),
            capacity: 100,
            history: vec![bar(10, Some(100.)), bar(20, Some(110.)), bar(30, Some(90.))],
        };
        let first = GraphSessions::default()
            .execute(&cpu, restore.clone())
            .await
            .unwrap();
        let after_restart = GraphSessions::default()
            .execute(&cpu, restore)
            .await
            .unwrap();
        assert_eq!(first, after_restart);
        let GraphSessionReply::Evaluated { graph_sha256, .. } = after_restart else {
            panic!("evaluated")
        };
        let next = GraphSessionRequest::Advance {
            session_id: id,
            graph_sha256,
            expected_bars: 4,
            bar: bar(40, Some(99.)),
        };
        assert!(matches!(
            GraphSessions::default()
                .execute(&cpu, next)
                .await
                .unwrap_err()
                .as_str(),
            "GRAPH_SESSION_RESTORE_REQUIRED"
        ));
    }
}
