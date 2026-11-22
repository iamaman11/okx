//! Strategy start -> Research public feed, with a finance-owned activation.
//! This module NEVER places an order. Engine's journal consumer remains an
//! independent risk/position authority and still blocks actionable targets.
use super::{AccountResources, TradeService, digest};
use crate::research_client::ResearchClient;
use okx_execution::StrategyInstance;
use okx_protocol::{
    research_ipc::{
        GraphFeedReply, GraphFeedRequest, GraphPublicationCursor, GraphSessionId,
        GraphSessionRequest,
    },
    trading::GraphStrategyConfigurationV1,
};
use std::{sync::Arc, time::Instant};

use super::graph_target::GraphTargetBinding;

const HISTORY_BARS: u16 = 64;
const SESSION_CAPACITY: u64 = 100_000;
const FEED_POLL_MS: u64 = 5_000;

/// Stable per-(account, instance, revision, instrument) identity. The graph
/// digest is a hash of versioned DATA, never interpretation of its operators.
pub(super) fn activation_binding(
    account_key: &str,
    instance: &StrategyInstance,
    configuration: &GraphStrategyConfigurationV1,
    instrument: &str,
) -> Result<GraphTargetBinding, String> {
    if !instance.enabled
        || instance.definition_id != GraphStrategyConfigurationV1::DEFINITION_ID
        || !configuration.valid()
        || !configuration.instruments.iter().any(|v| v == instrument)
    {
        return Err("GRAPH_ACTIVATION_STRATEGY_INVALID".into());
    }
    let session_id = GraphSessionId::new(format!(
        "engine:{}",
        digest(&(
            account_key,
            &instance.instance_id,
            &instance.definition_version,
            instance.policy_revision,
            instrument,
        ))?
    ))
    .map_err(str::to_owned)?;
    let binding = GraphTargetBinding {
        account_key: account_key.to_owned(),
        instance_id: instance.instance_id.clone(),
        definition_version: instance.definition_version.clone(),
        policy_revision: instance.policy_revision,
        session_id,
        graph_sha256: digest(&configuration.signal)?,
        instrument_id: instrument.to_owned(),
        timeframe_ms: configuration.timeframe_ms,
    };
    if !binding.valid() {
        return Err("GRAPH_ACTIVATION_BINDING_INVALID".into());
    }
    Ok(binding)
}

/// Best-effort shutdown after the original finance owner commits Stop or an
/// Update to a new revision. Old publications are already financially stale;
/// this frees Research CPU/session capacity without touching positions.
pub(super) async fn retire_graph_instance(
    research: &ResearchClient,
    account_key: &str,
    instance: &StrategyInstance,
) -> Result<(), String> {
    if !instance.enabled || instance.definition_id != GraphStrategyConfigurationV1::DEFINITION_ID {
        return Ok(());
    }
    let configuration: GraphStrategyConfigurationV1 = serde_json::from_str(&instance.parameters)
        .map_err(|_| "GRAPH_RETIRE_CONFIGURATION_CORRUPT")?;
    for instrument in &configuration.instruments {
        let binding = activation_binding(account_key, instance, &configuration, instrument)?;
        research
            .graph_feed(GraphFeedRequest::Stop {
                session_id: binding.session_id.clone(),
                graph_sha256: binding.graph_sha256.clone(),
            })
            .await?;
        research
            .graph_signal(GraphSessionRequest::Release {
                session_id: binding.session_id,
                graph_sha256: binding.graph_sha256,
            })
            .await?;
    }
    Ok(())
}

impl TradeService {
    async fn activate_graph_instrument(
        &self,
        resources: &Arc<AccountResources>,
        research: &ResearchClient,
        instance: &StrategyInstance,
        configuration: &GraphStrategyConfigurationV1,
        instrument: &str,
    ) -> Result<GraphFeedReply, String> {
        let binding = activation_binding(
            &resources.configuration.account_key.wire(),
            instance,
            configuration,
            instrument,
        )?;
        let account = &resources.configuration.alias;
        if self
            .graph_confirmed_cursor(account, &binding)
            .await?
            .is_none()
        {
            let restored = research
                .graph_signal(GraphSessionRequest::Bootstrap {
                    session_id: binding.session_id.clone(),
                    definition: Box::new(configuration.signal.clone()),
                    capacity: SESSION_CAPACITY,
                    instrument: instrument.to_owned(),
                    timeframe_ms: configuration.timeframe_ms,
                    history_bars: HISTORY_BARS,
                })
                .await?;
            let cursor = GraphPublicationCursor::from_restored(&restored).map_err(str::to_owned)?;
            if !cursor.matches_binding(&binding.session_id, &binding.graph_sha256) {
                return Err("GRAPH_ACTIVATION_RESEARCH_HASH_MISMATCH".into());
            }
            // Finance re-reads the current enabled strategy/policy revision
            // in the original SQL transaction; a concurrent Stop/Update wins.
            if let Err(error) = self.bind_graph_strategy(account, &binding, &cursor).await {
                let _ = research
                    .graph_signal(GraphSessionRequest::Release {
                        session_id: binding.session_id,
                        graph_sha256: binding.graph_sha256,
                    })
                    .await;
                return Err(error);
            }
        }
        // Durable finance binding exists BEFORE the first autonomous poll.
        // The feed never receives account credentials or permission to trade.
        research
            .graph_feed(GraphFeedRequest::Start {
                session_id: binding.session_id,
                graph_sha256: binding.graph_sha256,
                instrument: instrument.to_owned(),
                timeframe_ms: configuration.timeframe_ms,
                poll_every_ms: FEED_POLL_MS,
            })
            .await
    }

    /// One graph instrument per existing maintenance turn, rotating across
    /// account instances. Retries use the existing bounded recovery schedule;
    /// no second permanent task, actor, financial store or order loop exists.
    pub(super) async fn graph_activation_turn(
        &self,
        resources: &Arc<AccountResources>,
        instances: &[StrategyInstance],
    ) -> Result<(), String> {
        let mut candidates = Vec::new();
        let mut count = 0usize;
        for instance in instances
            .iter()
            .filter(|i| i.enabled && i.definition_id == GraphStrategyConfigurationV1::DEFINITION_ID)
        {
            let configuration: GraphStrategyConfigurationV1 =
                serde_json::from_str(&instance.parameters)
                    .map_err(|_| "GRAPH_ACTIVATION_CONFIGURATION_CORRUPT")?;
            if !configuration.valid() {
                return Err("GRAPH_ACTIVATION_CONFIGURATION_INVALID".into());
            }
            count += configuration.instruments.len();
            candidates.push((instance, configuration));
        }
        if count == 0 {
            return Ok(());
        }
        let account = resources.configuration.account_key.wire();
        let mut offset = self
            .recovery_schedule
            .lock()
            .await
            .next_graph_activation(&account, count);
        for (instance, configuration) in candidates {
            if offset >= configuration.instruments.len() {
                offset -= configuration.instruments.len();
                continue;
            }
            let instrument = &configuration.instruments[offset];
            let binding = activation_binding(&account, instance, &configuration, instrument)?;
            let due = self.recovery_schedule.lock().await.due(
                &account,
                &format!("graph-observe:{}", binding.session_id.as_str()),
                Instant::now(),
            );
            if !due {
                return Ok(());
            }
            let research = self
                .signal_research
                .read()
                .await
                .clone()
                .ok_or("GRAPH_RESEARCH_NOT_READY")?;
            let result = self
                .activate_graph_instrument(
                    resources,
                    &research,
                    instance,
                    &configuration,
                    instrument,
                )
                .await?;
            if !matches!(result, GraphFeedReply::Started { .. }) {
                return Err("GRAPH_ACTIVATION_FEED_NOT_STARTED".into());
            }
            return Ok(());
        }
        Err("GRAPH_ACTIVATION_INDEX_INVALID".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use serde_json::json;

    fn graph_configuration() -> GraphStrategyConfigurationV1 {
        GraphStrategyConfigurationV1 {
            signal: serde_json::from_value(json!({
                "version":"okx.graph-signal/v1",
                "graph":{"schema":"okx.graph/v1","steps":[
                    {"id":"return","op":{"id":"returns_series","version":1},"inputs":[{"source":"close"}]},
                    {"id":"positive","op":{"id":"greater_than","version":1},
                     "inputs":[{"step":"return"}],"params":{"threshold":0.0}}
                ],"outputs":["positive"]},
                "signal_step":"positive","target_if_true":1,"target_if_false":0,"missing":"hold"
            })).unwrap(),
            instruments: vec!["BTC-USDT-SWAP".into(), "ETH-USDT-SWAP".into()],
            timeframe_ms: 60_000,
        }
    }

    #[test]
    fn identity_is_stable_per_account_revision_and_instrument() {
        let configuration = graph_configuration();
        let mut instance = StrategyInstance {
            instance_id: "graph-a".into(),
            definition_id: GraphStrategyConfigurationV1::DEFINITION_ID.into(),
            definition_version: "1".into(),
            parameters: serde_json::to_string(&configuration).unwrap(),
            enabled: true,
            policy_revision: 2,
            budget: Decimal::ONE,
            last_decision_key: None,
        };
        let btc =
            activation_binding("account-a", &instance, &configuration, "BTC-USDT-SWAP").unwrap();
        assert_eq!(
            btc,
            activation_binding("account-a", &instance, &configuration, "BTC-USDT-SWAP").unwrap()
        );
        let eth =
            activation_binding("account-a", &instance, &configuration, "ETH-USDT-SWAP").unwrap();
        assert_ne!(btc.session_id, eth.session_id);
        assert_eq!(btc.graph_sha256, eth.graph_sha256);
        assert_ne!(
            btc.session_id,
            activation_binding("account-b", &instance, &configuration, "BTC-USDT-SWAP")
                .unwrap()
                .session_id
        );
        assert_eq!(btc.graph_sha256, digest(&configuration.signal).unwrap());
        instance.policy_revision += 1;
        assert_ne!(
            btc.session_id,
            activation_binding("account-a", &instance, &configuration, "BTC-USDT-SWAP")
                .unwrap()
                .session_id
        );
        instance.enabled = false;
        assert_eq!(
            activation_binding("account-a", &instance, &configuration, "BTC-USDT-SWAP")
                .unwrap_err(),
            "GRAPH_ACTIVATION_STRATEGY_INVALID"
        );
    }
}
