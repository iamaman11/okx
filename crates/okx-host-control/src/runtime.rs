use std::{collections::HashSet, path::PathBuf, time::Duration};

use chrono::{SecondsFormat, Utc};
use okx_github::{
    GitHubClient, GitHubError, IssueComment, IssueCommentCursor, IssueCursorStore, OWNER_USER_ID,
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
const CONTROL_CURSOR_PATH: &str = r"C:\okx-control\github-control-issue-12-cursor.json";
const MAX_CONTROL_BODY_BYTES: usize = 4096;

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
    let cursor_store = control_cursor_store()?;
    let cursor = load_cursor_for_poll(&cursor_store)?;
    let comments = github
        .issue_comments_after(CONTROL_ISSUE_NUMBER, cursor.as_ref())
        .await?;
    if comments.is_empty() {
        return Ok(0);
    }

    let mut terminal_request_ids = terminal_request_ids(&comments);
    let mut processed = 0usize;

    for comment in &comments {
        if comment.user_id != OWNER_USER_ID || comment.body.len() > MAX_CONTROL_BODY_BYTES {
            continue;
        }

        let Ok(request) = serde_json::from_str::<HostControlRequest>(&comment.body) else {
            continue;
        };
        if request.validate().is_err() || terminal_request_ids.contains(&request.request_id) {
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

        terminal_request_ids.insert(request.request_id);
        processed += 1;

        if result.status == HostControlStatus::Pass {
            match transition {
                ProcessTransition::None => {}
                ProcessTransition::Handoff => std::process::exit(0),
                ProcessTransition::Crash => std::process::exit(70),
            }
        }
    }

    if let Some(cursor) = completed_batch_cursor(&comments) {
        cursor_store.save(&cursor)?;
    }

    Ok(processed)
}

fn control_cursor_store() -> LocalResult<IssueCursorStore> {
    Ok(IssueCursorStore::new(
        PathBuf::from(CONTROL_CURSOR_PATH),
        CONTROL_ISSUE_NUMBER,
    )?)
}

fn load_cursor_for_poll(
    cursor_store: &IssueCursorStore,
) -> LocalResult<Option<IssueCommentCursor>> {
    match cursor_store.load() {
        Ok(cursor) => Ok(cursor),
        Err(GitHubError::CursorJson(error)) => {
            eprintln!(
                "control cursor JSON invalid at {}: {}; falling back to bounded bootstrap scan",
                cursor_store.path().display(),
                error
            );
            Ok(None)
        }
        Err(GitHubError::CursorStateMismatch) => {
            eprintln!(
                "control cursor state mismatch at {}; falling back to bounded bootstrap scan",
                cursor_store.path().display()
            );
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

fn terminal_request_ids(comments: &[IssueComment]) -> HashSet<String> {
    comments
        .iter()
        .filter(|comment| comment.user_id == OWNER_USER_ID)
        .filter(|comment| comment.body.len() <= MAX_CONTROL_BODY_BYTES)
        .filter_map(|comment| serde_json::from_str::<HostControlResult>(&comment.body).ok())
        .filter(|result| result.validate().is_ok())
        .map(|result| result.request_id)
        .collect()
}

fn completed_batch_cursor(comments: &[IssueComment]) -> Option<IssueCommentCursor> {
    comments.last().map(IssueComment::cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_issue_is_pinned() {
        assert_eq!(CONTROL_ISSUE_NUMBER, 12);
        assert_eq!(OWNER_USER_ID, 44_100_369);
        assert_eq!(
            CONTROL_CURSOR_PATH,
            r"C:\okx-control\github-control-issue-12-cursor.json"
        );
    }

    #[test]
    fn replay_batch_with_terminal_ack_is_idempotent() {
        let request = serde_json::json!({
            "schema": "okx.windows.control/v1",
            "request_id": "ctl-1",
            "operation": { "type": "transport_status" }
        })
        .to_string();
        let terminal = serde_json::json!({
            "schema": "okx.windows.control.result/v1",
            "request_id": "ctl-1",
            "operation": { "type": "transport_status" },
            "status": "pass",
            "observed_at": "2026-09-27T12:00:01.000Z",
            "details": {},
            "failure": null
        })
        .to_string();

        let comments = vec![
            IssueComment {
                id: 10,
                body: request,
                user_id: OWNER_USER_ID,
                created_at: "2026-09-27T12:00:00Z".to_owned(),
            },
            IssueComment {
                id: 11,
                body: terminal,
                user_id: OWNER_USER_ID,
                created_at: "2026-09-27T12:00:01Z".to_owned(),
            },
        ];

        assert!(terminal_request_ids(&comments).contains("ctl-1"));
        assert_eq!(
            completed_batch_cursor(&comments),
            Some(IssueCommentCursor {
                created_at: "2026-09-27T12:00:01Z".to_owned(),
                id: 11,
            })
        );
    }
}
