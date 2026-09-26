use std::path::PathBuf;

use serde::Serialize;

pub const DEFAULT_AGENT_KEY_ID: &str = "agent-key-1";
pub const AGENT_RUNTIME_SCHEMA_V1: &str = "okx.agent.runtime/v1";
pub const AGENT_IDENTITY_SCHEMA_V1: &str = "okx.agent.identity/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentConfig {
    pub root: PathBuf,
    pub key_id: String,
}

impl AgentConfig {
    pub fn new(root: PathBuf, key_id: String) -> Self {
        Self { root, key_id }
    }
}

pub fn default_root() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(r"C:\okx")
    } else {
        PathBuf::from(".okx-agent")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_config_contains_no_secret_material() {
        let config = AgentConfig::new(PathBuf::from("test-root"), "agent-key-1".to_owned());
        let json = serde_json::to_string(&config).expect("serialize");

        assert!(json.contains("agent-key-1"));
        assert!(!json.contains("private_key"));
        assert!(!json.contains("secret"));
    }
}
