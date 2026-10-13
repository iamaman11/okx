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
    // Integrated event-driven native financial checkpoint. This future
    // shares the same single runtime and single ledger mutex, not a second
    // agent/service, scheduler or polling task.
    let mut private_financial_capture = Box::pin(async {
        match (account, private_ws, execution) {
            (Some(account), Some(private_ws), Some(execution)) => {
                capture_native_finance_on_order_events(account, private_ws, execution).await
            }
            _ => std::future::pending::<Result<(), String>>().await,
        }
    });
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
            result = &mut private_financial_capture => {
                // Unavailable capture is an observable fail-closed runtime
                // condition, never an alternate REST submit or a quiet PASS.
                return Err(AgentError::PublicRuntimeTask(format!(
                    "native financial source capture owner exited: {result:?}"
                )));
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

/// A private order update, not a timer or ChatGPT query, wakes this
/// observer. It obtains a fresh REST baseline and complete exact native
/// orders/fills/bills, then checks both private WS convergence fences before
/// persisting under the already existing one execution-ledger mutex.
/// 
/// Failed or incomplete attempts are diagnostic only: no fabricated cursor,
/// no fake fill/bill, and no acknowledgement of source completeness.
async fn capture_native_finance_on_order_events(
    account: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
) -> Result<(), String> {
    let mut signals = private_ws.subscribe_financial_order_events();
    loop {
        signals.changed().await.map_err(|error| error.to_string())?;
        if !execution.strategy_financial_capture_active().await {
            continue;
        }
        if let Err(reason) =
            capture_one_native_financial_source(account, private_ws, execution).await
        {
            // Do not terminate the private stream on a transient REST/WS race;
            // subsequent authenticated order events can retry, while source
            // gap and incomplete period remain independently visible.
            eprintln!("native financial evidence NOT_CAPTURED: {reason}");
        }
    }
}

async fn capture_one_native_financial_source(
    account: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
) -> Result<(), String> {
    let cursor = private_ws.convergence_cursor().await
        .map_err(|error| format!("initial private WS not ready: {error}"))?;
    let rest = account.snapshot().await
        .map_err(|error| format!("independent REST account: {error}"))?;
    let first = private_ws.convergence_window(cursor).await
        .map_err(|error| format!("initial WS source changed: {error}"))?;
    if !first.events.is_empty() {
        return Err("private order/account events during REST bootstrap".into());
    }
    let fingerprint = first.status.connection_id_fingerprint.as_deref()
        .ok_or("private WS connection fingerprint unavailable")?;
    let received = first.status.last_inbound_ms
        .ok_or("private WS last inbound time missing")?;
    let snapshot = rest.converge_private_ws(
        first.generation, fingerprint, received, &first.events,
    ).map_err(|error| format!("REST+WS convergence: {error}"))?;
    let history_cursor = private_ws.convergence_cursor().await
        .map_err(|error| format!("private history cursor: {error}"))?;
    let facts = account.ledger_facts(&snapshot).await
        .map_err(|error| format!("native order/fill/bill readback: {error}"))?;
    let window = private_ws.convergence_window(history_cursor).await
        .map_err(|error| format!("private history read interrupted: {error}"))?;
    if !window.events.is_empty()
        || window.generation != first.generation
        || facts.summary.account_generation != snapshot.account_generation
        || facts.summary.authority.account_uid_fingerprint
            != snapshot.authority.account_uid_fingerprint
        || !okx_execution::DurableExchangeEvidence::complete_source_checkpoint(&facts)
    {
        return Err("native financial history/account/WS proof incomplete".into());
    }
    let (assessment, _) = execution
        .reconcile_account_ledger_with_intents(&facts, true)
        .await
        .map_err(|error| format!("durable native financial capture: {error}"))?;
    if !assessment.consistent {
        return Err("native financial reconciliation inconsistent".into());
    }
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
