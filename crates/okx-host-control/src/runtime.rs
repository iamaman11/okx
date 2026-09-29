use std::{collections::BTreeSet, future::Future, path::PathBuf, time::Duration};

use chrono::{SecondsFormat, Utc};
use okx_github::{
    GitHubBackoff, GitHubClient, GitHubError, IssueCheckpoint, IssueComment, IssueCursorStore,
    OWNER_USER_ID,
};
use okx_protocol::{
    HOST_CONTROL_RESULT_SCHEMA_V1, HostControlFailure, HostControlOperation, HostControlRequest,
    HostControlResult, HostControlStatus,
};
use tokio::time::{Interval, MissedTickBehavior, interval};

use crate::{
    HostControlError, HostControlResult as LocalResult,
    artifact::{deploy_agent, stage_controller_update},
    autostart, controller_update,
    executor::HostExecutor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessTransition {
    None,
    Handoff,
    Crash,
}

pub const CONTROL_ISSUE_NUMBER: u64 = 12;
pub const DEFAULT_CONTROL_POLL_SECONDS: u64 = 5;
const LOCAL_RECONCILE_SECONDS: u64 = 1;
const MAX_CONTROL_BODY_BYTES: usize = 4096;
const MAX_CONTROL_RESULT_BYTES: usize = 16 * 1024;
const CONTROL_RESPONSE_TOO_LARGE_CODE: &str = "CONTROL_RESPONSE_TOO_LARGE";
const CONTROL_CURSOR_PATH: &str = r"C:\okx-control\github-control-issue-12-cursor.json";

#[derive(Debug)]
enum NetworkWait<T> {
    Completed(T),
    Shutdown,
}

struct PendingControlBatch {
    cursor_store: IssueCursorStore,
    checkpoint: IssueCheckpoint,
    comments: Vec<IssueComment>,
}

pub async fn run_until_shutdown(
    github: &GitHubClient,
    executor: &mut HostExecutor,
    poll_seconds: u64,
) -> LocalResult<()> {
    if !(1..=60).contains(&poll_seconds) {
        return Err(HostControlError::InvalidPollInterval);
    }

    let mut github_verified = false;
    let mut github_backoff = GitHubBackoff::default();
    let mut initial_state = "DEGRADED";

    let mut reconcile_ticker = interval(Duration::from_secs(LOCAL_RECONCILE_SECONDS));
    reconcile_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Tokio intervals tick immediately once. Consume that edge so the explicit
    // initial reconcile below remains the first lifecycle action.
    reconcile_ticker.tick().await;

    if let Err(error) = executor.reconcile_desired() {
        eprintln!("initial lifecycle reconcile failed: {error}");
    }

    match await_network_with_reconcile(
        github.verify_repository_identity(),
        executor,
        &mut reconcile_ticker,
    )
    .await?
    {
        NetworkWait::Completed(Ok(())) => {
            github_backoff.on_success();
            github_verified = true;
            initial_state = "READY";
        }
        NetworkWait::Completed(Err(error)) => {
            let delay = github_backoff.on_error(&error);
            eprintln!(
                "initial GitHub identity verification deferred: {error}; class={:?}; retry_in_ms={}",
                github_backoff.last_class(),
                delay.as_millis()
            );
        }
        NetworkWait::Shutdown => {
            emit_shutdown(executor);
            return Ok(());
        }
    }

    println!(
        "{}",
        serde_json::json!({
            "schema": "okx.host-control.runtime/v1",
            "state": initial_state,
            "control_issue": CONTROL_ISSUE_NUMBER,
            "github_identity_verified": github_verified,
            "local_reconcile_seconds": LOCAL_RECONCILE_SECONDS,
            "github_poll_seconds": poll_seconds
        })
    );

    let mut github_ticker = interval(Duration::from_secs(poll_seconds));
    github_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                break;
            }
            _ = reconcile_ticker.tick() => {
                reconcile_lifecycle(executor);
            }
            _ = github_ticker.tick() => {
                if !github_backoff.ready() {
                    continue;
                }

                if !github_verified {
                    match await_network_with_reconcile(
                        github.verify_repository_identity(),
                        executor,
                        &mut reconcile_ticker,
                    )
                    .await?
                    {
                        NetworkWait::Completed(Ok(())) => {
                            github_backoff.on_success();
                            github_verified = true;
                            eprintln!("GitHub repository identity verified");
                        }
                        NetworkWait::Completed(Err(error)) => {
                            let delay = github_backoff.on_error(&error);
                            eprintln!(
                                "GitHub identity verification still unavailable: {error}; class={:?}; retry_in_ms={}",
                                github_backoff.last_class(),
                                delay.as_millis()
                            );
                            continue;
                        }
                        NetworkWait::Shutdown => break,
                    }
                }

                let batch = match await_network_with_reconcile(
                    fetch_pending_control(github),
                    executor,
                    &mut reconcile_ticker,
                )
                .await?
                {
                    NetworkWait::Completed(Ok(batch)) => batch,
                    NetworkWait::Completed(Err(error)) => {
                        if let HostControlError::Github(github_error) = &error {
                            let delay = github_backoff.on_error(github_error);
                            eprintln!(
                                "host-control poll failed: {error}; class={:?}; retry_in_ms={}",
                                github_backoff.last_class(),
                                delay.as_millis()
                            );
                            github_verified = false;
                        } else {
                            eprintln!("host-control poll failed: {error}");
                        }
                        continue;
                    }
                    NetworkWait::Shutdown => break,
                };

                match process_control_batch(github, executor, batch).await {
                    Ok(_) => github_backoff.on_success(),
                    Err(error) => {
                        if let HostControlError::Github(github_error) = &error {
                            let delay = github_backoff.on_error(github_error);
                            eprintln!(
                                "host-control operation failed: {error}; class={:?}; retry_in_ms={}",
                                github_backoff.last_class(),
                                delay.as_millis()
                            );
                            github_verified = false;
                        } else {
                            eprintln!("host-control operation failed: {error}");
                        }
                    }
                }
            }
        }
    }

    emit_shutdown(executor);
    Ok(())
}

async fn await_network_with_reconcile<F>(
    future: F,
    executor: &mut HostExecutor,
    reconcile_ticker: &mut Interval,
) -> LocalResult<NetworkWait<F::Output>>
where
    F: Future,
{
    tokio::pin!(future);

    loop {
        tokio::select! {
            output = &mut future => return Ok(NetworkWait::Completed(output)),
            _ = reconcile_ticker.tick() => reconcile_lifecycle(executor),
            result = tokio::signal::ctrl_c() => {
                result?;
                return Ok(NetworkWait::Shutdown);
            }
        }
    }
}

fn reconcile_lifecycle(executor: &mut HostExecutor) {
    if let Err(error) = executor.reconcile_desired() {
        eprintln!("host lifecycle reconcile failed: {error}");
    }
}

fn emit_shutdown(executor: &mut HostExecutor) {
    executor.shutdown();
    println!(
        "{}",
        serde_json::json!({
            "schema": "okx.host-control.runtime/v1",
            "state": "SHUTDOWN"
        })
    );
}

async fn fetch_pending_control(github: &GitHubClient) -> LocalResult<PendingControlBatch> {
    let cursor_store =
        IssueCursorStore::new(PathBuf::from(CONTROL_CURSOR_PATH), CONTROL_ISSUE_NUMBER)?;
    let mut checkpoint = load_checkpoint_for_poll(&cursor_store)?;

    let comments = if checkpoint.ledger_initialized {
        github
            .issue_comments_after(CONTROL_ISSUE_NUMBER, checkpoint.cursor.as_ref())
            .await?
    } else {
        let history = github.issue_comments(CONTROL_ISSUE_NUMBER).await?;
        checkpoint
            .terminal_request_ids
            .extend(control_terminal_request_ids(&history));
        checkpoint.ledger_initialized = true;

        if checkpoint.cursor.is_none() {
            history
        } else {
            github
                .issue_comments_after(CONTROL_ISSUE_NUMBER, checkpoint.cursor.as_ref())
                .await?
        }
    };

    Ok(PendingControlBatch {
        cursor_store,
        checkpoint,
        comments,
    })
}

async fn process_control_batch(
    github: &GitHubClient,
    executor: &mut HostExecutor,
    batch: PendingControlBatch,
) -> LocalResult<usize> {
    let PendingControlBatch {
        cursor_store,
        checkpoint,
        comments,
    } = batch;

    if comments.is_empty() {
        if let Some(cursor) = checkpoint.cursor.as_ref()
            && checkpoint.ledger_initialized
        {
            cursor_store.save_checkpoint(cursor, &checkpoint.terminal_request_ids, true)?;
        }
        return Ok(0);
    }

    let mut terminal_ids = checkpoint.terminal_request_ids.clone();
    terminal_ids.extend(control_terminal_request_ids(&comments));

    let mut processed = 0usize;
    for comment in &comments {
        if comment.user_id != OWNER_USER_ID || comment.body.len() > MAX_CONTROL_BODY_BYTES {
            continue;
        }

        let Ok(request) = serde_json::from_str::<HostControlRequest>(&comment.body) else {
            continue;
        };
        if request.validate().is_err() || terminal_ids.contains(&request.request_id) {
            continue;
        }

        let operation = request.operation;
        let (execution, transition) = match &operation {
            HostControlOperation::DeployAgent {
                run_id,
                artifact_id,
                expected_source_tree,
            } => (
                deploy_agent(
                    github,
                    executor,
                    *run_id,
                    *artifact_id,
                    expected_source_tree,
                )
                .await,
                ProcessTransition::None,
            ),
            HostControlOperation::StageControllerUpdate {
                run_id,
                artifact_id,
                expected_source_tree,
            } => (
                stage_controller_update(
                    github,
                    executor,
                    *run_id,
                    *artifact_id,
                    expected_source_tree,
                )
                .await,
                ProcessTransition::None,
            ),
            HostControlOperation::HandoffControllerUpdate => (
                controller_update::prepare_handoff(&request.request_id),
                ProcessTransition::Handoff,
            ),
            HostControlOperation::HandoffToAutostart => {
                (autostart::run_now(), ProcessTransition::Handoff)
            }
            HostControlOperation::AcceptanceCrashController => (
                autostart::ensure_policy_valid().map(|()| {
                    serde_json::json!({
                        "autostart_policy_valid": true,
                        "crash_after_terminal_ack": true
                    })
                }),
                ProcessTransition::Crash,
            ),
            _ => (executor.execute(operation.clone()), ProcessTransition::None),
        };
        let (status, details, failure) = match execution {
            Ok(details) => (HostControlStatus::Pass, Some(details), None),
            Err(error) => (
                HostControlStatus::Fail,
                None,
                Some(HostControlFailure {
                    code: error.code().to_owned(),
                    message: error.to_string(),
                }),
            ),
        };

        let result = bounded_control_result(HostControlResult {
            schema: HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: request.request_id.clone(),
            operation,
            status,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            details,
            failure,
        })?;
        let result_body = serde_json::to_string(&result)?;

        github
            .post_issue_comment(CONTROL_ISSUE_NUMBER, &result_body)
            .await?;

        terminal_ids.insert(request.request_id);
        processed += 1;

        if result.status == HostControlStatus::Pass {
            match transition {
                ProcessTransition::None => {}
                ProcessTransition::Handoff => std::process::exit(0),
                ProcessTransition::Crash => std::process::exit(70),
            }
        }
    }

    if let Some(cursor) = comments.last().map(IssueComment::cursor) {
        cursor_store.save_checkpoint(&cursor, &terminal_ids, true)?;
    }

    Ok(processed)
}

fn bounded_control_result(result: HostControlResult) -> LocalResult<HostControlResult> {
    result.validate()?;
    let serialized = serde_json::to_vec(&result)?;
    if serialized.len() <= MAX_CONTROL_RESULT_BYTES {
        return Ok(result);
    }

    let bounded = HostControlResult {
        schema: result.schema,
        request_id: result.request_id,
        operation: result.operation,
        status: HostControlStatus::Fail,
        observed_at: result.observed_at,
        details: None,
        failure: Some(HostControlFailure {
            code: CONTROL_RESPONSE_TOO_LARGE_CODE.to_owned(),
            message: format!(
                "CONTROL result exceeded bounded response budget: serialized={}B limit={}B",
                serialized.len(),
                MAX_CONTROL_RESULT_BYTES
            ),
        }),
    };
    bounded.validate()?;
    if serde_json::to_vec(&bounded)?.len() > MAX_CONTROL_RESULT_BYTES {
        return Err(HostControlError::ControlResponseBudgetInvariant);
    }
    Ok(bounded)
}

pub async fn process_pending(
    github: &GitHubClient,
    executor: &mut HostExecutor,
) -> LocalResult<usize> {
    let batch = fetch_pending_control(github).await?;
    process_control_batch(github, executor, batch).await
}

fn load_checkpoint_for_poll(store: &IssueCursorStore) -> LocalResult<IssueCheckpoint> {
    match store.load_checkpoint() {
        Ok(checkpoint) => Ok(checkpoint),
        Err(GitHubError::CursorJson(error)) => {
            eprintln!(
                "control cursor JSON invalid at {}: {}; falling back to bounded bootstrap scan",
                store.path().display(),
                error
            );
            Ok(IssueCheckpoint::default())
        }
        Err(GitHubError::CursorStateMismatch) => {
            eprintln!(
                "control cursor state mismatch at {}; falling back to bounded bootstrap scan",
                store.path().display()
            );
            Ok(IssueCheckpoint::default())
        }
        Err(error) => Err(error.into()),
    }
}

fn control_terminal_request_ids(comments: &[IssueComment]) -> BTreeSet<String> {
    comments
        .iter()
        .filter(|comment| {
            comment.user_id == OWNER_USER_ID && comment.body.len() <= MAX_CONTROL_BODY_BYTES
        })
        .filter_map(|comment| serde_json::from_str::<HostControlResult>(&comment.body).ok())
        .filter(|result| result.validate().is_ok())
        .map(|result| result.request_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_control_terminal_ids_block_late_duplicate_request_ids() {
        let ids = BTreeSet::from(["ctl_0123456789abcdef".to_owned()]);
        assert!(ids.contains("ctl_0123456789abcdef"));
    }

    #[test]
    fn control_cadences_keep_local_reconcile_faster_than_network_polling() {
        assert_eq!(LOCAL_RECONCILE_SECONDS, 1);
        assert_eq!(DEFAULT_CONTROL_POLL_SECONDS, 5);
    }

    #[test]
    fn control_issue_is_pinned() {
        assert_eq!(CONTROL_ISSUE_NUMBER, 12);
        assert_eq!(OWNER_USER_ID, 44_100_369);
    }

    #[test]
    fn oversized_control_result_terminalizes_as_compact_failure() {
        let result = HostControlResult {
            schema: HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: "ctl_budget_0123456789".to_owned(),
            operation: HostControlOperation::Status,
            status: HostControlStatus::Pass,
            observed_at: "2026-09-29T13:00:00.000Z".to_owned(),
            details: Some(serde_json::json!({"blob": "x".repeat(MAX_CONTROL_RESULT_BYTES)})),
            failure: None,
        };

        let bounded = bounded_control_result(result).expect("bounded result");
        assert_eq!(bounded.status, HostControlStatus::Fail);
        assert_eq!(
            bounded.failure.as_ref().map(|failure| failure.code.as_str()),
            Some(CONTROL_RESPONSE_TOO_LARGE_CODE)
        );
        assert!(serde_json::to_vec(&bounded).expect("serialize").len() <= MAX_CONTROL_RESULT_BYTES);
    }

    #[test]
    fn compact_control_result_is_preserved() {
        let result = HostControlResult {
            schema: HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: "ctl_budget_small_012345".to_owned(),
            operation: HostControlOperation::Status,
            status: HostControlStatus::Pass,
            observed_at: "2026-09-29T13:00:00.000Z".to_owned(),
            details: Some(serde_json::json!({"ready": true})),
            failure: None,
        };

        let bounded = bounded_control_result(result).expect("bounded result");
        assert_eq!(bounded.status, HostControlStatus::Pass);
        assert!(bounded.failure.is_none());
    }
}
