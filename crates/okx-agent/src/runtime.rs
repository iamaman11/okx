use std::time::Duration;

use serde::Serialize;
use tokio::time::{MissedTickBehavior, interval};

use crate::{
    AgentError, AgentResult,
    config::{AGENT_RUNTIME_SCHEMA_V1, AgentConfig},
    github_mailbox::GitHubMailboxClient,
    identity::AgentIdentity,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeState {
    ReadyIdle,
    ReadyMailbox,
    ShuttingDown,
}

#[derive(Debug, Serialize)]
pub struct RuntimeEvent<'a> {
    pub schema: &'static str,
    pub state: RuntimeState,
    pub root: &'a std::path::Path,
    pub key_id: &'a str,
    pub public_key: &'a str,
    pub mailbox_issue: Option<u64>,
}

pub async fn run_until_shutdown(config: &AgentConfig, identity: &AgentIdentity) -> AgentResult<()> {
    emit(RuntimeState::ReadyIdle, config, identity, None)?;
    tokio::signal::ctrl_c().await?;
    emit(RuntimeState::ShuttingDown, config, identity, None)?;
    Ok(())
}

pub async fn run_mailbox_until_shutdown(
    config: &AgentConfig,
    identity: &AgentIdentity,
    mailbox: &GitHubMailboxClient,
    mailbox_issue: u64,
    poll_seconds: u64,
    agent_private_key: &[u8; 32],
) -> AgentResult<()> {
    if !(1..=60).contains(&poll_seconds) {
        return Err(AgentError::InvalidPollInterval);
    }

    mailbox.verify_repository_identity().await?;
    mailbox.ensure_identity_published(identity).await?;
    emit(
        RuntimeState::ReadyMailbox,
        config,
        identity,
        Some(mailbox_issue),
    )?;

    let mut ticker = interval(Duration::from_secs(poll_seconds));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                break;
            }
            _ = ticker.tick() => {
                match mailbox.process_pending(&config.key_id, agent_private_key).await {
                    Ok(processed) if processed > 0 => {
                        eprintln!("mailbox processed {processed} terminal request(s)");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!("mailbox poll failed: {error}");
                    }
                }
            }
        }
    }

    emit(
        RuntimeState::ShuttingDown,
        config,
        identity,
        Some(mailbox_issue),
    )?;
    Ok(())
}

fn emit(
    state: RuntimeState,
    config: &AgentConfig,
    identity: &AgentIdentity,
    mailbox_issue: Option<u64>,
) -> AgentResult<()> {
    let event = RuntimeEvent {
        schema: AGENT_RUNTIME_SCHEMA_V1,
        state,
        root: &config.root,
        key_id: &identity.key_id,
        public_key: &identity.public_key,
        mailbox_issue,
    };
    println!("{}", serde_json::to_string(&event)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_event_contains_only_public_identity() {
        let config = AgentConfig::new(std::path::PathBuf::from("root"), "agent-key-1".to_owned());
        let identity = AgentIdentity {
            schema: "okx.agent.identity/v1",
            key_id: "agent-key-1".to_owned(),
            public_key: "public".to_owned(),
        };
        let event = RuntimeEvent {
            schema: AGENT_RUNTIME_SCHEMA_V1,
            state: RuntimeState::ReadyMailbox,
            root: &config.root,
            key_id: &identity.key_id,
            public_key: &identity.public_key,
            mailbox_issue: Some(10),
        };
        let json = serde_json::to_string(&event).expect("serialize");

        assert!(json.contains("READY_MAILBOX"));
        assert!(json.contains("\"mailbox_issue\":10"));
        assert!(!json.contains("private_key"));
        assert!(!json.contains("token"));
    }
}
