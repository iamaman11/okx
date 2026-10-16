use std::{
    io::{BufRead, Read},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;

use okx_github::GitHubBackoff;
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponseStatus,
    NATIVE_CHILD_CONTROL_MAX_BYTES, NATIVE_CHILD_CONTROL_PREFIX, NATIVE_CHILD_CONTROL_SCHEMA_V1,
    NATIVE_SOURCE_CANDIDATE_SCHEMA_V1, NativeChildOperation, NativeChildReply, NativeChildRequest,
    NativeDrainedSourceCandidate, NativeReadOnlyTargetProof,
};
use okx_runtime::{PrivateWsCoordinator, PrivateWsHandle, PublicWsCoordinator, PublicWsHandle};
use serde::Serialize;
use tokio::{
    sync::{mpsc, watch},
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
    query::ObservationQueryContext,
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

// Reuse the existing heartbeat; never spin a new timer or retry task.
// A failed venue proof can be retried only at a bounded cadence.
const MIN_QUIET_SOURCE_RETRY_MS: u64 = 300_000;

fn quiet_source_retry_allowed(last_attempt_ms: Option<u64>, now_ms: u64) -> bool {
    last_attempt_ms.is_none_or(|last| now_ms >= last && now_ms - last >= MIN_QUIET_SOURCE_RETRY_MS)
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
    pub execution: Option<&'a Arc<ExecutionRuntime>>,
    pub cloudflare: Option<&'a CloudflareTransportConfig>,
    pub native_child_control_stdio: bool,
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
        execution: execution_arc,
        cloudflare,
        native_child_control_stdio,
    } = context;
    let execution = execution_arc.map(Arc::as_ref);
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
                capture_native_finance_on_private_events(account, private_ws, execution).await
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
    let mut last_quiet_source_attempt_ms = None;

    emit(
        RuntimeState::DegradedMailbox,
        config,
        identity,
        Some(mailbox_issue),
    )?;

    // Exactly one inbound local line per process; no socket, no daemon and
    // no general shell RPC. A reader thread is blocked in inherited stdin,
    // then forwards one bounded typed command to the existing Tokio owner.
    let (native_tx, mut native_rx) = mpsc::unbounded_channel::<NativeChildRequest>();
    if native_child_control_stdio {
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            let mut bounded = stdin
                .lock()
                .take((NATIVE_CHILD_CONTROL_MAX_BYTES + 1) as u64);
            let mut frame = Vec::new();
            if bounded.read_until(b'\n', &mut frame).is_ok()
                && frame.len() <= NATIVE_CHILD_CONTROL_MAX_BYTES
                && frame.last() == Some(&b'\n')
                && let Ok(request) = serde_json::from_slice::<NativeChildRequest>(&frame)
                && request.valid()
            {
                let _ = native_tx.send(request);
            }
        });
    }
    let mut native_request_consumed = false;
    let (drain_tx, mut drain_rx) = mpsc::unbounded_channel();

    let mut ticker = interval(Duration::from_secs(poll_seconds));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            Some(request) = native_rx.recv(), if native_child_control_stdio && !native_request_consumed => {
                native_request_consumed = true;
                if matches!(request.operation, NativeChildOperation::VerifyReadOnly) {
                    // One strictly read-only native observation from target.
                    let verify = async {
                        let observer = account.ok_or("no observer")?;
                        let private = private_ws.ok_or("no private WS")?;
                        // A genuine Production observer may have ONLY a
                        // read-only OKX credential, with NO executor credential.
                        // Absence of an executor means mutations cannot be
                        // submitted. If it exists, it MUST be native ReadOnly
                        // with exchange mutation authority disabled.
                        if let Some(executor) = execution {
                            if executor.mode() != crate::execution_runtime::ExecutionRuntimeMode::ReadOnly {
                                return Err("execution mode is not ReadOnly");
                            }
                            if executor.mutation_authority().await != okx_execution::MutationAuthority::Disabled {
                                return Err("native mutation authority enabled");
                            }
                        }
                        let before = private.convergence_cursor().await.map_err(|_| "WS unavailable")?;
                        let (snapshot, algos) = tokio::try_join!(
                            async { observer.snapshot().await.map_err(|_| "account REST unavailable") },
                            async { observer.pending_protective_algos().await.map_err(|_| "protective inventory unavailable") },
                        )?;
                        let after = private.convergence_cursor().await.map_err(|_| "WS convergence lost")?;
                        if before != after || after.generation == 0 {
                            return Err("WS changed during REST receipt");
                        }
                        let parsed = chrono::DateTime::parse_from_rfc3339(&snapshot.source_received_at)
                            .map_err(|_| "invalid source receipt")?;
                        let at = u64::try_from(parsed.timestamp_millis())
                            .map_err(|_| "source receipt out of range")?;
                        let proof = NativeReadOnlyTargetProof {
                            schema: "okx.native-readonly-target/v1".to_owned(),
                            account_uid_fingerprint: snapshot.account_uid_fingerprint,
                            snapshot_received_ms: at,
                            private_ws_generation: after.generation,
                            positions: snapshot.positions.len() as u64,
                            orders: snapshot.pending_orders.len() as u64,
                            native_oco: algos.rows as u64,
                            native_oco_complete: algos.complete_within_bound,
                            read_only_credentials: true, // strict_read_only_permissions enforced by AccountBootstrapper
                            executor_mutations_disabled: true, // confirmed in single executor above
                        };
                        let now = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
                        if !proof.valid_for(&request.expected_uid_fingerprint, now) {
                            return Err("target ReadOnly/UID/source not proven");
                        }
                        Ok(proof)
                    };
                    let response = tokio::time::timeout(Duration::from_secs(8), verify).await;
                    let reply = match response {
                        Ok(Ok(proof)) => NativeChildReply {
                            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.to_owned(),
                            request_id: request.request_id,
                            disposition: "VERIFIED_READ_ONLY".to_owned(),
                            ledger_only: None,
                            source_candidate: None,
                            target_read_only: Some(proof),
                            error: None,
                        },
                        _ => NativeChildReply {
                            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.to_owned(),
                            request_id: request.request_id,
                            disposition: "BLOCKED".to_owned(),
                            ledger_only: None,
                            source_candidate: None,
                            target_read_only: None,
                            error: Some("destination ReadOnly verification denied".to_owned()),
                        },
                    };
                    eprintln!("{NATIVE_CHILD_CONTROL_PREFIX}{}", serde_json::to_string(&reply)?);
                    continue;
                }
                // Do not await drain inside this select loop: an in-flight
                // Cloudflare request must continue polling until it releases
                // the very same mutation ingress lock.
                if let Some(owner) = execution_arc {
                    let owner = Arc::clone(owner);
                    let tx = drain_tx.clone();
                    tokio::spawn(async move {
                        let result = match request.operation {
                            NativeChildOperation::Drain => owner
                                .drain_mutation_ingress(&request.expected_uid_fingerprint)
                                .await
                                .map_err(|error| error.to_string()),
                            NativeChildOperation::VerifyReadOnly => Err("handled directly".to_owned()),
                        };
                        let _ = tx.send((request, result));
                    });
                } else {
                    // Strict observer-only native source: there is no mutation
                    // executor to drain. This proof cannot invent a ledger hash
                    // or authorize itself without CONTROL's durable ReadOnly bit.
                    let evidence = tokio::time::timeout(
                        Duration::from_secs(8),
                        verify_ledgerless_read_only_source(
                            &config.root,
                            account,
                            private_ws,
                            &request.expected_uid_fingerprint,
                        ),
                    ).await;
                    let reply = match evidence {
                        Ok(Ok(source)) => NativeChildReply {
                            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.to_owned(),
                            request_id: request.request_id,
                            disposition: "DRAINED_SOURCE_CANDIDATE".to_owned(),
                            ledger_only: None,
                            source_candidate: Some(source),
                            target_read_only: None,
                            error: None,
                        },
                        _ => NativeChildReply {
                            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.to_owned(),
                            request_id: request.request_id,
                            disposition: "BLOCKED".to_owned(),
                            ledger_only: None,
                            source_candidate: None,
                            target_read_only: None,
                            error: Some("observer-only source/ledger absence not proven".to_owned()),
                        },
                    };
                    eprintln!("{NATIVE_CHILD_CONTROL_PREFIX}{}", serde_json::to_string(&reply)?);
                }
            }
            Some((request, drained)) = drain_rx.recv() => {
                // Reuse the same native source query used by primary MCP.
                // It cannot change the ledger manifest after drain; it must
                // prove fresh REST receipt, coherent WS, current flat source,
                // all OCO/conditional scopes, UID and immutable source hash.
                let source = if drained.is_ok() {
                    let probe = AgentRequest {
                        schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
                        request_id: request.request_id.clone(),
                        operation: AgentOperation::ProfileHandoffReadiness,
                    };
                    let context = ObservationQueryContext::live_with_execution(
                        public_ws, market, None, account, private_ws, execution,
                    );
                    match tokio::time::timeout(
                        Duration::from_secs(8),
                        crate::query::dispatch(
                            &probe,
                            context,
                            &Utc::now().to_rfc3339(),
                        ),
                    ).await {
                        Ok(Ok(reply)) if reply.status == AgentResponseStatus::Completed => reply.result,
                        _ => None,
                    }
                } else {
                    None
                };
                let ledger = drained.ok();
                let source_valid = source.as_ref().is_some_and(|value| {
                    value.pointer("/read_only_source_assessment/preconditions_pass")
                        == Some(&serde_json::json!(true))
                        && value.pointer("/current_source_quality")
                            == Some(&serde_json::json!("FRESH"))
                        && value.pointer("/source_account_uid_fingerprint")
                            == Some(&serde_json::json!(&request.expected_uid_fingerprint))
                        && value.pointer("/native_ledger/source_ledger_manifest_sha256")
                            == ledger.as_ref().map(|record| {
                                serde_json::json!(&record.source_ledger_manifest_sha256)
                            }).as_ref()
                });
                // This is a source observation candidate, not a switch permit.
                // CONTROL still must bind the owner PID, binary hashes, launch
                // lease, exact WS generation, one-use state and target UID.
                let candidate = source.as_ref().and_then(|value| {
                    serde_json::from_value::<NativeDrainedSourceCandidate>(serde_json::json!({
                        "schema": NATIVE_SOURCE_CANDIDATE_SCHEMA_V1,
                        "account_uid_fingerprint": value["source_account_uid_fingerprint"],
                        "snapshot_received_ms": value["source_snapshot_received_ms"],
                        "source_age_ms": value["source_age_ms"],
                        "private_ws_generation": value["source_private_ws_generation"],
                        "ledger_manifest_sha256": value["native_ledger"]["source_ledger_manifest_sha256"],
                        "historical_canceled_identity_exception": value["read_only_source_assessment"]["historical_canceled_identity_exception"],
                        "positions": value["source"]["positions"],
                        "orders": value["source"]["orders"],
                        "native_oco": value["source"]["native_oco"],
                    })).ok()
                });
                let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
                let admitted = source_valid && candidate.as_ref().is_some_and(|proof| {
                    ledger.as_ref().is_some_and(|record| {
                        proof.valid_for(
                            &request.expected_uid_fingerprint,
                            &record.source_ledger_manifest_sha256,
                            now_ms,
                        )
                    })
                });
                // Native source proof is NOT PID/lease/release handoff authority.
                let reply = if admitted {
                    NativeChildReply {
                        schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.to_owned(),
                        request_id: request.request_id,
                        disposition: "DRAINED_SOURCE_CANDIDATE".to_owned(),
                        ledger_only: ledger.and_then(|value| serde_json::to_value(value).ok()),
                        source_candidate: candidate,
                        target_read_only: None,
                        error: None,
                    }
                } else {
                    NativeChildReply {
                        schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.to_owned(),
                        request_id: request.request_id,
                        disposition: "BLOCKED".to_owned(),
                        ledger_only: None,
                        source_candidate: None,
                        target_read_only: None,
                        error: Some("drain or current REST/WS/OCO/UID/ledger proof not admitted".to_owned()),
                    }
                };
                eprintln!("{NATIVE_CHILD_CONTROL_PREFIX}{}", serde_json::to_string(&reply)?);
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                break;
            }
            result = &mut private_financial_capture => {
                // Unavailable capture is an observable fail-closed runtime
                // condition, never an alternate REST submit or a quiet PASS.
                return Err(AgentError::FinancialCaptureTask(format!(
                    "event-driven financial source capture exited: {result:?}"
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
                // Same single native event loop; no second timer, worker,
                // scheduler or exchange-POST path. Native private WS events
                // remain the immediate capture trigger. The pre-existing
                // mailbox heartbeat covers truly QUIET source intervals,
                // which WS events alone cannot prove stayed event-free.
                // The 1h source deadline is strictly below the 2h venue
                // window for short-lived canceled/unfilled order evidence.
                let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
                if quiet_source_retry_allowed(last_quiet_source_attempt_ms, now_ms)
                    && let (Some(account), Some(private_ws), Some(execution)) =
                        (account, private_ws, execution)
                    && execution.strategy_quiet_source_capture_due(now_ms).await
                {
                    last_quiet_source_attempt_ms = Some(now_ms);
                    if let Err(reason) =
                        capture_one_native_financial_source(account, private_ws, execution).await
                    {
                        // No false cursor, no eventual replays, no fabricated
                        // zero-event proof. Missing >2h past remains INCOMPLETE.
                        eprintln!("native quiet financial source NOT_CAPTURED: {reason}");
                    }
                }

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

/// A real nonempty private financial event, not a timer or ChatGPT query, wakes this
/// The absence of an execution ledger is itself a fail-closed disk proof.
/// Any matching file or archive forbids observer-only account handoff.
fn no_native_ledger_at(root: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    for entry in entries {
        let Ok(entry) = entry else { return false };
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("execution-ledger")
        {
            return false;
        }
    }
    true
}

async fn verify_ledgerless_read_only_source(
    root: &std::path::Path,
    account: Option<&AccountBootstrapper>,
    private_ws: Option<&PrivateWsHandle>,
    expected_uid: &str,
) -> Result<NativeDrainedSourceCandidate, &'static str> {
    let observer = account.ok_or("observer unavailable")?;
    let private = private_ws.ok_or("private WS unavailable")?;
    if !no_native_ledger_at(root) {
        return Err("native ledger exists or unreadable");
    }
    let before = private
        .convergence_cursor()
        .await
        .map_err(|_| "private WS unavailable")?;
    let snapshot = observer
        .snapshot()
        .await
        .map_err(|_| "observer REST unavailable")?;
    let (algos, facts) = tokio::try_join!(
        async {
            observer
                .pending_protective_algos()
                .await
                .map_err(|_| "OCO unavailable")
        },
        async {
            observer
                .ledger_facts(&snapshot)
                .await
                .map_err(|_| "history unavailable")
        },
    )?;
    let after = private
        .convergence_cursor()
        .await
        .map_err(|_| "private WS unavailable")?;
    let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default();
    let received_ms = chrono::DateTime::parse_from_rfc3339(&snapshot.source_received_at)
        .ok()
        .and_then(|at| u64::try_from(at.timestamp_millis()).ok())
        .ok_or("invalid REST receipt")?;
    if before != after
        || after.generation == 0
        || snapshot.account_uid_fingerprint != expected_uid
        || !snapshot.positions.is_empty()
        || !snapshot.pending_orders.is_empty()
        || facts.summary.open_positions != 0
        || facts.summary.pending_orders != 0
        || algos.rows != 0
        || !algos.complete_within_bound
        || facts.summary.history_coverage.is_empty()
        || facts
            .summary
            .history_coverage
            .iter()
            .any(|row| !row.complete_within_bound)
        || !facts.exchange_orders_history_complete
        || !facts.exchange_fills_history_complete
        || !facts.bills_history_complete
        || now_ms < received_ms
        || now_ms - received_ms > 10_000
        || !no_native_ledger_at(root)
    {
        return Err("observer-only source not FLAT, FRESH or complete");
    }
    let candidate = NativeDrainedSourceCandidate {
        schema: NATIVE_SOURCE_CANDIDATE_SCHEMA_V1.to_owned(),
        account_uid_fingerprint: snapshot.account_uid_fingerprint,
        snapshot_received_ms: received_ms,
        source_age_ms: now_ms - received_ms,
        private_ws_generation: after.generation,
        ledger_manifest_sha256: String::new(),
        historical_canceled_identity_exception: false,
        positions: 0,
        orders: 0,
        native_oco: 0,
    };
    if !candidate.valid_for(expected_uid, "", now_ms) {
        return Err("invalid observer-only receipt");
    }
    Ok(candidate)
}

/// observer. It obtains a fresh REST baseline and complete exact native
/// orders/fills/bills, then checks both private WS convergence fences before
/// persisting under the already existing one execution-ledger mutex.
///
/// Failed or incomplete attempts are diagnostic only: no fabricated cursor,
/// no fake fill/bill, and no acknowledgement of source completeness.
async fn capture_native_finance_on_private_events(
    account: &AccountBootstrapper,
    private_ws: &PrivateWsHandle,
    execution: &ExecutionRuntime,
) -> Result<(), String> {
    let mut signals = private_ws.subscribe_financial_source_events();
    // Restart proof: if a prior Demo run already exists, attempt one native
    // source check immediately. Do not require a new order or invent a
    // scheduler just to re-attach the persisted period after process crash.
    if execution.strategy_financial_capture_active().await
        && let Err(reason) =
            capture_one_native_financial_source(account, private_ws, execution).await
    {
        eprintln!("native financial evidence startup NOT_CAPTURED: {reason}");
    }
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
    let cursor = private_ws
        .convergence_cursor()
        .await
        .map_err(|error| format!("initial private WS not ready: {error}"))?;
    let rest = account
        .snapshot()
        .await
        .map_err(|error| format!("independent REST account: {error}"))?;
    let first = private_ws
        .convergence_window(cursor)
        .await
        .map_err(|error| format!("initial WS source changed: {error}"))?;
    if !first.events.is_empty() {
        return Err("private order/account events during REST bootstrap".into());
    }
    let fingerprint = first
        .status
        .connection_id_fingerprint
        .as_deref()
        .ok_or("private WS connection fingerprint unavailable")?;
    let received = first
        .status
        .last_inbound_ms
        .ok_or("private WS last inbound time missing")?;
    let snapshot = rest
        .converge_private_ws(first.generation, fingerprint, received, &first.events)
        .map_err(|error| format!("REST+WS convergence: {error}"))?;
    let history_cursor = private_ws
        .convergence_cursor()
        .await
        .map_err(|error| format!("private history cursor: {error}"))?;
    let facts = account
        .ledger_facts(&snapshot)
        .await
        .map_err(|error| format!("native order/fill/bill readback: {error}"))?;
    let window = private_ws
        .convergence_window(history_cursor)
        .await
        .map_err(|error| format!("private history read interrupted: {error}"))?;
    if !window.events.is_empty()
        || window.generation != first.generation
        || facts.summary.account_generation != snapshot.account_generation
        || !okx_execution::DurableExchangeEvidence::complete_source_checkpoint(&facts)
    {
        return Err("native financial history/account/WS proof incomplete".into());
    }
    let (assessment, _) = execution
        .reconcile_account_ledger_with_intents(&facts, true, &std::collections::BTreeMap::new())
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
    fn quiet_financial_source_retry_is_bounded_in_existing_heartbeat() {
        let t = 1_790_000_000_000_u64;
        assert!(quiet_source_retry_allowed(None, t));
        assert!(!quiet_source_retry_allowed(Some(t), t));
        assert!(!quiet_source_retry_allowed(Some(t), t + 299_999));
        assert!(quiet_source_retry_allowed(Some(t), t + 300_000));
        assert!(!quiet_source_retry_allowed(Some(t + 1), t));
    }

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
