use std::time::Duration;

use okx_runtime::{PublicWsCoordinator, PublicWsHandle};
use serde::Serialize;
use tokio::{
    sync::watch,
    time::{MissedTickBehavior, interval},
};

use crate::{
    AgentError, AgentResult,
    config::{AGENT_RUNTIME_SCHEMA_V1, AgentConfig},
    github_mailbox::GitHubMailboxClient,
    identity::AgentIdentity,
    market_bootstrap::MarketBootstrapper,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeState {
    ReadyIdle,
    DegradedMailbox,
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

pub struct MailboxRuntimeContext<'a> {
    pub config: &'a AgentConfig,
    pub identity: &'a AgentIdentity,
    pub mailbox: &'a GitHubMailboxClient,
    pub mailbox_issue: u64,
    pub agent_private_key: &'a [u8; 32],
    pub public_ws: &'a PublicWsHandle,
    pub market: &'a MarketBootstrapper,
}

pub async fn run_mailbox_until_shutdown(
    context: MailboxRuntimeContext<'_>,
    public_ws_coordinator: PublicWsCoordinator,
    poll_seconds: u64,
) -> AgentResult<()> {
    let MailboxRuntimeContext {
        config,
        identity,
        mailbox,
        mailbox_issue,
        agent_private_key,
        public_ws,
        market,
    } = context;
    if !(1..=60).contains(&poll_seconds) {
        return Err(AgentError::InvalidPollInterval);
    }

    let (public_shutdown_tx, public_shutdown_rx) = watch::channel(false);
    let mut public_runtime_task = tokio::spawn(public_ws_coordinator.run(public_shutdown_rx));

    let mut github_verified = false;
    let mut identity_published = false;
    let mut ready_emitted = false;

    emit(
        RuntimeState::DegradedMailbox,
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
            runtime_result = &mut public_runtime_task => {
                emit(
                    RuntimeState::ShuttingDown,
                    config,
                    identity,
                    Some(mailbox_issue),
                )?;
                return match runtime_result {
                    Ok(Ok(())) => Err(AgentError::PublicRuntimeTask(
                        "public WebSocket coordinator exited before agent shutdown".to_owned(),
                    )),
                    Ok(Err(error)) => Err(error.into()),
                    Err(error) => Err(AgentError::PublicRuntimeTask(error.to_string())),
                };
            }
            _ = ticker.tick() => {
                if !github_verified {
                    match mailbox.verify_repository_identity().await {
                        Ok(()) => {
                            github_verified = true;
                            eprintln!("mailbox repository identity verified");
                        }
                        Err(error) => {
                            eprintln!("mailbox identity verification unavailable: {error}");
                            continue;
                        }
                    }
                }

                if !identity_published {
                    match mailbox.ensure_identity_published(identity).await {
                        Ok(()) => {
                            identity_published = true;
                        }
                        Err(error) => {
                            eprintln!("mailbox identity publication unavailable: {error}");
                            github_verified = false;
                            continue;
                        }
                    }
                }

                if !ready_emitted {
                    emit(
                        RuntimeState::ReadyMailbox,
                        config,
                        identity,
                        Some(mailbox_issue),
                    )?;
                    ready_emitted = true;
                }

                match mailbox
                    .process_pending(&config.key_id, agent_private_key, public_ws, market)
                    .await
                {
                    Ok(processed) if processed > 0 => {
                        eprintln!("mailbox processed {processed} terminal request(s)");
                    }
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!("mailbox poll failed: {error}");
                        github_verified = false;
                        identity_published = false;
                        ready_emitted = false;
                    }
                }
            }
        }
    }

    let _ = public_shutdown_tx.send(true);
    match public_runtime_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(error.into()),
        Err(error) => return Err(AgentError::PublicRuntimeTask(error.to_string())),
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
