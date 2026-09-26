use serde::Serialize;

use crate::{
    AgentResult,
    config::{AGENT_RUNTIME_SCHEMA_V1, AgentConfig},
    identity::AgentIdentity,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeState {
    ReadyIdle,
    ShuttingDown,
}

#[derive(Debug, Serialize)]
pub struct RuntimeEvent<'a> {
    pub schema: &'static str,
    pub state: RuntimeState,
    pub root: &'a std::path::Path,
    pub key_id: &'a str,
    pub public_key: &'a str,
}

pub async fn run_until_shutdown(
    config: &AgentConfig,
    identity: &AgentIdentity,
) -> AgentResult<()> {
    emit(RuntimeState::ReadyIdle, config, identity)?;
    tokio::signal::ctrl_c().await?;
    emit(RuntimeState::ShuttingDown, config, identity)?;
    Ok(())
}

fn emit(
    state: RuntimeState,
    config: &AgentConfig,
    identity: &AgentIdentity,
) -> AgentResult<()> {
    let event = RuntimeEvent {
        schema: AGENT_RUNTIME_SCHEMA_V1,
        state,
        root: &config.root,
        key_id: &identity.key_id,
        public_key: &identity.public_key,
    };
    println!("{}", serde_json::to_string(&event)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_event_contains_only_public_identity() {
        let config =
            AgentConfig::new(std::path::PathBuf::from("root"), "agent-key-1".to_owned());
        let identity = AgentIdentity {
            schema: "okx.agent.identity/v1",
            key_id: "agent-key-1".to_owned(),
            public_key: "public".to_owned(),
        };
        let event = RuntimeEvent {
            schema: AGENT_RUNTIME_SCHEMA_V1,
            state: RuntimeState::ReadyIdle,
            root: &config.root,
            key_id: &identity.key_id,
            public_key: &identity.public_key,
        };
        let json = serde_json::to_string(&event).expect("serialize");

        assert!(json.contains("READY_IDLE"));
        assert!(!json.contains("private_key"));
    }
}
