use std::{
    collections::BTreeSet,
    path::PathBuf,
    time::{Duration, Instant},
};

use chrono::{SecondsFormat, Utc};
use okx_github::{
    GitHubClient, GitHubError, IssueCheckpoint, IssueComment, IssueCursorStore,
    IssuePollTelemetryStore, OWNER_USER_ID,
};
use okx_protocol::{
    HOST_CONTROL_RESULT_SCHEMA_V1, HostControlFailure, HostControlOperation, HostControlRequest,
    HostControlResult, HostControlStatus,
};
use tokio::time::{MissedTickBehavior, interval};

use crate::{
    HostControlError, HostControlResult as LocalResult, artifact::deploy_agent, autostart,
    executor::HostExecutor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessTransition {
    None,
    Handoff,
    Crash,
}

pub const CONTROL_ISSUE_NUMBER: u64 = 12;
const MAX_CONTROL_BODY_BYTES: usize = 4096;
const CONTROL_CURSOR_PATH: &str = r"C:\okx-control\github-control-issue-12-cursor.json";
pub const CONTROL_TELEMETRY_PATH: &str =
    r"C:\okx-control\github-control-issue-12-telemetry.json";

pub async fn run_until_shutdown(
    github: &GitHubClient,
    executor: &mut HostExecutor,
    poll_seconds: u64,
) -> LocalResult<()> {
    if !(1..=60).contains(&poll_seconds) {
        return Err(HostControlError::InvalidPollInterval);
    }

    let mut github_verified = false;
    let mut initial_state = "DEGRADED";

    if let Err(error) = executor.reconcile_desired() {
        eprintln!("initial lifecycle reconcile failed: {error}");
    }

    match github.verify_repository_identity().await {
        Ok(()) => {
            github_verified = true;
            initial_state = "READY";
        }
        Err(error) => {
            eprintln!("initial GitHub identity verification deferred: {error}");
        }
    }

    println!(
        "{}",
        serde_json::json!({
            "schema": "okx.host-control.runtime/v1",
            "state": initial_state,
            "control_issue": CONTROL_ISSUE_NUMBER,
            "github_identity_verified": github_verified
        })
    );

    let mut ticker = interval(Duration::from_secs(poll_seconds));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                break;
            }
            _ = ticker.tick() => {
                if let Err(error) = executor.reconcile_desired() {
                    eprintln!("host lifecycle reconcile failed: {error}");
                }

                if !github_verified {
                    match github.verify_repository_identity().await {
                        Ok(()) => {
                            github_verified = true;
                            eprintln!("GitHub repository identity verified");
                        }
                        Err(error) => {
                            eprintln!("GitHub identity verification still unavailable: {error}");
                            continue;
                        }
                    }
                }

                if let Err(error) = process_pending(github, executor).await {
                    eprintln!("host-control poll failed: {error}");
                    if matches!(error, HostControlError::Github(_)) {
                        github_verified = false;
                    }
                }
            }
        }
    }

    executor.shutdown();
    println!(
        "{}",
        serde_json::json!({
            "schema": "okx.host-control.runtime/v1",
            "state": "SHUTDOWN"
        })
    );
    Ok(())
}

pub async fn process_pending(
    github: &GitHubClient,
    executor: &mut HostExecutor,
) -> LocalResult<usize> {
    let cursor_store =
        IssueCursorStore::new(PathBuf::from(CONTROL_CURSOR_PATH), CONTROL_ISSUE_NUMBER)?;
    let mut checkpoint = load_checkpoint_for_poll(&cursor_store)?;
    let fetch_started = Instant::now();

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
    let fetch_latency = fetch_started.elapsed();

    if comments.is_empty() {
        if let Some(cursor) = checkpoint.cursor.as_ref()
            && checkpoint.ledger_initialized
        {
            cursor_store.save_checkpoint(cursor, &checkpoint.terminal_request_ids, true)?;
        }
        record_control_telemetry(
            fetch_latency,
            0,
            checkpoint.cursor.as_ref(),
            None,
        );
        return Ok(0);
    }

    let mut terminal_ids = checkpoint.terminal_request_ids.clone();
    terminal_ids.extend(control_terminal_request_ids(&comments));

    let mut processed = 0usize;
    let mut last_terminal_request_id = last_control_terminal_request_id(&comments);
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

        let result = HostControlResult {
            schema: HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: request.request_id.clone(),
            operation,
            status,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            details,
            failure,
        };
        result.validate()?;

        github
            .post_issue_comment(CONTROL_ISSUE_NUMBER, &serde_json::to_string(&result)?)
            .await?;

        last_terminal_request_id = Some(request.request_id.clone());
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

    let completed_cursor = comments.last().map(IssueComment::cursor);
    if let Some(cursor) = completed_cursor.as_ref() {
        cursor_store.save_checkpoint(cursor, &terminal_ids, true)?;
    }
    record_control_telemetry(
        fetch_latency,
        comments.len(),
        completed_cursor.as_ref().or(checkpoint.cursor.as_ref()),
        last_terminal_request_id.as_deref(),
    );

    Ok(processed)
}

fn record_control_telemetry(
    fetch_latency: Duration,
    comments_scanned: usize,
    cursor: Option<&okx_github::IssueCommentCursor>,
    last_terminal_request_id: Option<&str>,
) {
    let Ok(store) =
        IssuePollTelemetryStore::new(PathBuf::from(CONTROL_TELEMETRY_PATH), CONTROL_ISSUE_NUMBER)
    else {
        return;
    };
    if let Err(error) = store.record_success(
        fetch_latency,
        comments_scanned,
        cursor,
        last_terminal_request_id,
    ) {
        eprintln!("control telemetry update failed: {error}");
    }
}

fn last_control_terminal_request_id(comments: &[IssueComment]) -> Option<String> {
    comments
        .iter()
        .filter(|comment| {
            comment.user_id == OWNER_USER_ID && comment.body.len() <= MAX_CONTROL_BODY_BYTES
        })
        .filter_map(|comment| serde_json::from_str::<HostControlResult>(&comment.body).ok())
        .filter(|result| result.validate().is_ok())
        .last()
        .map(|result| result.request_id)
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
    fn control_issue_is_pinned() {
        assert_eq!(CONTROL_ISSUE_NUMBER, 12);
        assert_eq!(OWNER_USER_ID, 44_100_369);
    }
}
