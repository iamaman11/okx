use std::path::PathBuf;

use serde::Serialize;

pub const DEFAULT_AGENT_KEY_ID: &str = "agent-key-1";
pub const AGENT_RUNTIME_SCHEMA_V1: &str = "okx.agent.runtime/v1";
pub const AGENT_IDENTITY_SCHEMA_V1: &str = "okx.agent.identity/v1";
pub const QUOTE_MAX_AGE_MS: u64 = 2_000;
pub const ACCOUNT_MAX_AGE_MS: i64 = 5_000;

pub fn evidence_fresh(received: i64, now: i64, max_age_ms: i64) -> bool {
    received > 0 && now >= received && now - received <= max_age_ms
}

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
        PathBuf::from(r"C:\okx-runtime")
    } else {
        PathBuf::from(".okx-agent")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_freshness_boundaries_reject_future_and_missing_receipts() {
        assert!(evidence_fresh(5_000, 10_000, ACCOUNT_MAX_AGE_MS));
        assert!(!evidence_fresh(4_999, 10_000, ACCOUNT_MAX_AGE_MS));
        assert!(!evidence_fresh(10_001, 10_000, ACCOUNT_MAX_AGE_MS));
        assert!(!evidence_fresh(0, 10_000, ACCOUNT_MAX_AGE_MS));
        assert!(evidence_fresh(8_000, 10_000, QUOTE_MAX_AGE_MS as i64));
        assert!(!evidence_fresh(7_999, 10_000, QUOTE_MAX_AGE_MS as i64));
    }

    #[test]
    fn windows_default_root_is_runtime_not_mutable_workspace() {
        if cfg!(windows) {
            assert_eq!(default_root(), PathBuf::from(r"C:\okx-runtime"));
        }
    }

    #[test]
    fn explicit_config_contains_no_secret_material() {
        let config = AgentConfig::new(PathBuf::from("test-root"), "agent-key-1".to_owned());
        let json = serde_json::to_string(&config).expect("serialize");

        assert!(json.contains("agent-key-1"));
        assert!(!json.contains("private_key"));
        assert!(!json.contains("secret"));
    }
}
