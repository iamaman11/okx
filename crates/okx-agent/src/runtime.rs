use std::time::Duration;

use okx_github::GitHubBackoff;
use okx_runtime::{PrivateWsCoordinator, PrivateWsHandle, PublicWsCoordinator, PublicWsHandle};
use serde::Serialize;
use tokio::{
    sync::watch,
    time::{MissedTickBehavior, interval},
};

use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    cloudflare_transport::{
        CloudflareQueryRuntimeContext, CloudflareTransportConfig, run_cloudflare_transport,
    },
    config::{AGENT_RUNTIME_SCHEMA_V1, AgentConfig},
    execution_runtime::ExecutionRuntime,
    github_mailbox::{GitHubMailboxClient, MailboxQueryRuntimeContext},
    identity::AgentIdentity,
    market_bootstrap::MarketBootstrapper,
    research_session::ResearchSessionRuntime,
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
    pub account: Option<&'a AccountBootstrapper>,
    pub private_ws: Option<&'a PrivateWsHandle>,
    pub execution: Option<&'a ExecutionRuntime>,
    pub cloudflare: Option<&'a CloudflareTransportConfig>,
}

pub async fn run_mailbox_until_shutdown(
    context: MailboxRuntimeContext<'_>,
    public_ws_coordinator: PublicWsCoordinator,
    private_ws_coordinator: Option<PrivateWsCoordinator>,
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
        account,
        private_ws,
        execution,
        cloudflare,
    } = context;
    if !(1..=60).contains(&poll_seconds) {
        return Err(AgentError::InvalidPollInterval);
    }

    let (runtime_shutdown_tx, public_shutdown_rx) = watch::channel(false);
    let mut public_runtime_task = tokio::spawn(public_ws_coordinator.run(public_shutdown_rx));

    let (research_session_runtime, research_session) = ResearchSessionRuntime::new(
        config.root.clone(),
        public_ws.clone(),
        market.clone(),
        account.cloned(),
        private_ws.cloned(),
    );
    let research_shutdown_rx = runtime_shutdown_tx.subscribe();
    let mut research_session_task =
        tokio::spawn(research_session_runtime.run(research_shutdown_rx));
    let mut private_runtime_task = private_ws_coordinator.map(|coordinator| {
        let private_shutdown_rx = runtime_shutdown_tx.subscribe();
        tokio::spawn(async move {
            let result = coordinator.run(private_shutdown_rx).await;
            if let Err(error) = &result {
                eprintln!("private WebSocket coordinator exited: {error}");
            }
            result
        })
    });

    let cloudflare_shutdown_rx = runtime_shutdown_tx.subscribe();
    let cloudflare_context = CloudflareQueryRuntimeContext {
        public_ws,
        market,
        account,
        private_ws,
        execution,
        research_root: &config.root,
        research_session: &research_session,
    };
    let mut cloudflare_runtime = Box::pin(async move {
        match cloudflare {
            Some(config) => {
                run_cloudflare_transport(config, cloudflare_context, cloudflare_shutdown_rx).await
            }
            None => std::future::pending::<AgentResult<()>>().await,
        }
    });

    let mut github_verified = false;
    let mut identity_published = false;
    let mut ready_emitted = false;
    let mut github_backoff = GitHubBackoff::default();

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
            cloudflare_result = &mut cloudflare_runtime => {
                emit(
                    RuntimeState::ShuttingDown,
                    config,
                    identity,
                    Some(mailbox_issue),
                )?;
                return match cloudflare_result {
                    Ok(()) => Err(AgentError::CloudflareTransport(
                        "Cloudflare transport exited before agent shutdown".to_owned(),
                    )),
                    Err(error) => Err(error),
                };
            }
            research_result = &mut research_session_task => {
                emit(
                    RuntimeState::ShuttingDown,
                    config,
                    identity,
                    Some(mailbox_issue),
                )?;
                return match research_result {
                    Ok(Ok(())) => Err(AgentError::ResearchSessionTask(
                        "research session owner exited before agent shutdown".to_owned(),
                    )),
                    Ok(Err(error)) => Err(AgentError::ResearchSessionTask(error.to_string())),
                    Err(error) => Err(AgentError::ResearchSessionTask(error.to_string())),
                };
            }
            _ = ticker.tick() => {
                if !github_backoff.ready() {
                    continue;
                }

                if !github_verified {
                    match mailbox.verify_repository_identity().await {
                        Ok(()) => {
                            github_backoff.on_success();
                            github_verified = true;
                            eprintln!("mailbox repository identity verified");
                        }
                        Err(error) => {
                            if let AgentError::Github(github_error) = &error {
                                let delay = github_backoff.on_error(github_error);
                                eprintln!(
                                    "mailbox identity verification unavailable: {error}; class={:?}; retry_in_ms={}",
                                    github_backoff.last_class(),
                                    delay.as_millis()
                                );
                            } else {
                                eprintln!("mailbox identity verification unavailable: {error}");
                            }
                            continue;
                        }
                    }
                }

                if !identity_published {
                    match mailbox.ensure_identity_published(identity).await {
                        Ok(()) => {
                            github_backoff.on_success();
                            identity_published = true;
                        }
                        Err(error) => {
                            if let AgentError::Github(github_error) = &error {
                                let delay = github_backoff.on_error(github_error);
                                eprintln!(
                                    "mailbox identity publication unavailable: {error}; class={:?}; retry_in_ms={}",
                                    github_backoff.last_class(),
                                    delay.as_millis()
                                );
                            } else {
                                eprintln!("mailbox identity publication unavailable: {error}");
                            }
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
                    .process_pending(MailboxQueryRuntimeContext {
                        expected_key_id: &config.key_id,
                        agent_private_key,
                        public_ws,
                        market,
                        account,
                        private_ws,
                        execution,
                        research_root: &config.root,
                        research_session: &research_session,
                    })
                    .await
                {
                    Ok(processed) if processed > 0 => {
                        github_backoff.on_success();
                        eprintln!("mailbox processed {processed} terminal request(s)");
                    }
                    Ok(_) => {
                        github_backoff.on_success();
                    }
                    Err(error) => {
                        if let AgentError::Github(github_error) = &error {
                            let delay = github_backoff.on_error(github_error);
                            eprintln!(
                                "mailbox poll failed: {error}; class={:?}; retry_in_ms={}",
                                github_backoff.last_class(),
                                delay.as_millis()
                            );
                        } else {
                            eprintln!("mailbox poll failed: {error}");
                        }
                        github_verified = false;
                        identity_published = false;
                        ready_emitted = false;
                    }
                }
            }
        }
    }

    let _ = runtime_shutdown_tx.send(true);
    match public_runtime_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(error.into()),
        Err(error) => return Err(AgentError::PublicRuntimeTask(error.to_string())),
    }

    if let Some(task) = private_runtime_task.take() {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!("private WebSocket shutdown error: {error}"),
            Err(error) => eprintln!("private WebSocket task join error: {error}"),
        }
    }

    match research_session_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(AgentError::ResearchSessionTask(error.to_string())),
        Err(error) => return Err(AgentError::ResearchSessionTask(error.to_string())),
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
