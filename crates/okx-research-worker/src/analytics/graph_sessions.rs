//! Read-only, restartable graph sessions. The Worker owns ephemeral mathematical
//! state; the Engine owns the durable decision/order cursor and risk authority.
use super::{executor::CpuExecutor, graph_resolver};
use okx_analysis::query_plan::ClosedBar;
use okx_protocol::research_ipc::{
    GraphClosedBar, GraphMissingPolicy, GraphPosition, GraphSessionId, GraphSessionReply,
    GraphSessionRequest, GraphSignalDefinition, GraphSignalIntent,
};
use okx_research::graph_signal::{
    GraphIntent, GraphSignalSpec, GraphSignalStream, MissingSignalPolicy, TargetPosition,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, atomic::Ordering},
};

const MAX_SESSIONS: usize = 32;

#[derive(Clone, Default)]
pub(super) struct GraphSessions {
    slots: Arc<Mutex<BTreeMap<GraphSessionId, Arc<Mutex<GraphSlot>>>>>,
}

struct GraphSlot {
    digest: String,
    history_digest: String,
    capacity: u64,
    runner: GraphSignalStream,
    last_bar: GraphClosedBar,
    last_intent: GraphSignalIntent,
}

fn convert(bar: GraphClosedBar) -> ClosedBar {
    ClosedBar {
        timestamp_ms: i64::try_from(bar.time_ms).expect("validated timestamp"),
        close: bar.close,
    }
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
    pub(super) async fn execute(
        &self,
        cpu: &CpuExecutor,
        request: GraphSessionRequest,
    ) -> Result<GraphSessionReply, String> {
        if !request.valid() {
            return Err("GRAPH_SESSION_REQUEST_INVALID".into());
        }
        let slots = self.slots.clone();
        cpu.run(Default::default(), move |cancel| {
            if cancel.load(Ordering::Acquire) {
                return Err("CANCELLED".into());
            }
            match request {
                GraphSessionRequest::Restore {
                    session_id,
                    definition,
                    capacity,
                    history,
                } => {
                    let digest = format!("{:x}", Sha256::digest(
                        serde_json::to_vec(&definition).map_err(|e| e.to_string())?
                    ));
                    let history_digest = format!("{:x}", Sha256::digest(
                        serde_json::to_vec(&history).map_err(|e| e.to_string())?
                    ));
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
                        runner,
                        last_bar: *history.last().expect("validated nonempty"),
                        last_intent: latest,
                    };
                    let mut registry = slots.lock().map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?;
                    if let Some(existing) = registry.get(&session_id) {
                        let active = existing.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
                        if active.digest == new.digest
                            && active.history_digest == new.history_digest
                            && active.capacity == new.capacity
                            && active.runner.processed_bars() == new.runner.processed_bars()
                            && active.last_bar == new.last_bar
                            && active.last_intent == new.last_intent
                        {
                            return Ok(reply(session_id, &active));
                        }
                        return Err("GRAPH_SESSION_ALREADY_ACTIVE".into());
                    }
                    if registry.len() >= MAX_SESSIONS {
                        return Err("GRAPH_SESSION_LIMIT".into());
                    }
                    let result = reply(session_id.clone(), &new);
                    registry.insert(session_id, Arc::new(Mutex::new(new)));
                    Ok(result)
                }
                GraphSessionRequest::Advance {
                    session_id,
                    graph_sha256,
                    expected_bars,
                    bar,
                } => {
                    let active = {
                        let registry = slots.lock().map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?;
                        registry.get(&session_id).cloned().ok_or("GRAPH_SESSION_RESTORE_REQUIRED")?
                    };
                    let mut active = active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?;
                    if active.digest != graph_sha256 {
                        return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
                    }
                    let seen = active.runner.processed_bars();
                    if expected_bars == seen && bar == active.last_bar {
                        // Retried response after timeout: no second state update.
                        return Ok(reply(session_id, &active));
                    }
                    if expected_bars != seen + 1 {
                        return Err("GRAPH_SESSION_SEQUENCE_MISMATCH".into());
                    }
                    // Validation in CausalGraphStream happens before any state
                    // mutation, including duplicate/out-of-order and capacity.
                    let outcome = active.runner.on_closed_bar(convert(bar))?;
                    active.last_bar = bar;
                    active.last_intent = intent(outcome);
                    Ok(reply(session_id, &active))
                }
                GraphSessionRequest::Release { session_id, graph_sha256 } => {
                    let mut registry = slots.lock().map_err(|_| "GRAPH_SESSION_REGISTRY_POISONED")?;
                    if let Some(active) = registry.get(&session_id) {
                        if active.lock().map_err(|_| "GRAPH_SESSION_POISONED")?.digest != graph_sha256 {
                            return Err("GRAPH_SESSION_DEFINITION_MISMATCH".into());
                        }
                    }
                    registry.remove(&session_id);
                    Ok(GraphSessionReply::Released { session_id })
                }
            }
        }).await
    }
}

fn compile(definition: &GraphSignalDefinition, capacity: u64) -> Result<okx_research::graph_signal::CheckedGraphSignal, String> {
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
                "version":"okx.graph/v1",
                "steps":[
                    {"id":"return","op":"returns_series","op_version":1,"inputs":{"input":"close"}},
                    {"id":"positive","op":"greater_than","op_version":1,
                     "inputs":{"input":"return"},"parameters":{"threshold":0.0}}
                ],
                "result_steps":["positive"]
            },
            "signal_step":"positive",
            "target_if_true":1,
            "target_if_false":0,
            "missing":"hold"
        })).unwrap()
    }

    fn bar(time: u64, close: Option<f64>) -> GraphClosedBar {
        GraphClosedBar { time_ms: time, close }
    }

    #[tokio::test]
    async fn causal_graph_session_replays_once_advances_and_rejects_stale_reordered_updates() {
        let registry = GraphSessions::default();
        let cpu = CpuExecutor::default();
        let id = GraphSessionId::new("account:instance:BTC".into()).unwrap();
        let restore = GraphSessionRequest::Restore {
            session_id: id.clone(),
            definition: Box::new(definition()),
            capacity: 100,
            history: vec![bar(10, Some(100.)), bar(20, Some(110.))],
        };
        let response = registry.execute(&cpu, restore.clone()).await.unwrap();
        assert!(response.matches_request(&restore));
        assert_eq!(response, registry.execute(&cpu, restore.clone()).await.unwrap());
        let GraphSessionReply::Evaluated { graph_sha256: digest, intent, .. } = response else {
            panic!("typed decision")
        };
        assert_eq!(intent, GraphSignalIntent::SetTarget(GraphPosition::Long));
        let next = GraphSessionRequest::Advance {
            session_id: id.clone(), graph_sha256: digest.clone(),
            expected_bars: 3, bar: bar(30, Some(90.)),
        };
        let result = registry.execute(&cpu, next.clone()).await.unwrap();
        assert!(result.matches_request(&next));
        assert_eq!(result, registry.execute(&cpu, next.clone()).await.unwrap());
        assert!(matches!(result, GraphSessionReply::Evaluated { intent: GraphSignalIntent::SetTarget(GraphPosition::Flat), observed_bars: 3, .. }));
        assert_eq!(registry.execute(&cpu, restore).await.unwrap_err(), "GRAPH_SESSION_ALREADY_ACTIVE");

        let stale = GraphSessionRequest::Advance { session_id: id.clone(), graph_sha256: digest.clone(), expected_bars: 3, bar: bar(31, Some(95.)) };
        assert_eq!(registry.execute(&cpu, stale).await.unwrap_err(), "GRAPH_SESSION_SEQUENCE_MISMATCH");
        let out_of_order = GraphSessionRequest::Advance { session_id: id.clone(), graph_sha256: digest.clone(), expected_bars: 4, bar: bar(29, Some(95.)) };
        assert_eq!(registry.execute(&cpu, out_of_order).await.unwrap_err(), "composition.stream:CLOSED_BAR_NOT_MONOTONIC");
        let correct = GraphSessionRequest::Advance { session_id: id.clone(), graph_sha256: digest.clone(), expected_bars: 4, bar: bar(40, None) };
        assert!(matches!(registry.execute(&cpu, correct).await.unwrap(), GraphSessionReply::Evaluated { intent: GraphSignalIntent::Hold, observed_bars: 4, .. }));

        let wrong_hash = GraphSessionRequest::Release { session_id: id.clone(), graph_sha256: "f".repeat(64) };
        assert_eq!(registry.execute(&cpu, wrong_hash).await.unwrap_err(), "GRAPH_SESSION_DEFINITION_MISMATCH");
        let release = GraphSessionRequest::Release { session_id: id.clone(), graph_sha256: digest };
        let removed = registry.execute(&cpu, release.clone()).await.unwrap();
        assert!(removed.matches_request(&release));
        assert_eq!(removed, registry.execute(&cpu, release).await.unwrap());
    }

    #[tokio::test]
    async fn worker_restart_restores_math_without_any_persistent_trade_effect() {
        let cpu = CpuExecutor::default();
        let id = GraphSessionId::new("restart".into()).unwrap();
        let restore = GraphSessionRequest::Restore { session_id: id.clone(),
            definition: Box::new(definition()), capacity: 100,
            history: vec![bar(10, Some(100.)), bar(20, Some(110.)), bar(30, Some(90.))] };
        let first = GraphSessions::default().execute(&cpu, restore.clone()).await.unwrap();
        let after_restart = GraphSessions::default().execute(&cpu, restore).await.unwrap();
        assert_eq!(first, after_restart);
        let GraphSessionReply::Evaluated { graph_sha256, .. } = after_restart else { panic!("evaluated") };
        let next = GraphSessionRequest::Advance { session_id: id, graph_sha256, expected_bars: 4, bar: bar(40, Some(99.)) };
        assert!(matches!(GraphSessions::default().execute(&cpu, next).await.unwrap_err().as_str(), "GRAPH_SESSION_RESTORE_REQUIRED"));
    }
}
