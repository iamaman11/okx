use std::{collections::HashSet, time::Duration};

use chrono::{SecondsFormat, Utc};
use okx_github::{GitHubClient, OWNER_USER_ID};
use okx_protocol::{
    HOST_CONTROL_RESULT_SCHEMA_V1, HostControlFailure, HostControlRequest, HostControlResult,
    HostControlStatus,
};
use tokio::time::{MissedTickBehavior, interval};

use crate::{HostControlError, HostControlResult as LocalResult, executor::HostExecutor};

pub const CONTROL_ISSUE_NUMBER: u64 = 12;
const MAX_CONTROL_BODY_BYTES: usize = 4096;

pub async fn run_until_shutdown(
    github: &GitHubClient,
    executor: &mut HostExecutor,
    poll_seconds: u64,
) -> LocalResult<()> {
    if !(1..=60).contains(&poll_seconds) {
        return Err(HostControlError::InvalidPollInterval);
    }

    github.verify_repository_identity().await?;
    println!(
        "{}",
        serde_json::json!({
            "schema": "okx.host-control.runtime/v1",
            "state": "READY",
            "control_issue": CONTROL_ISSUE_NUMBER
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
                if let Err(error) = process_pending(github, executor).await {
                    eprintln!("host-control poll failed: {error}");
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
    let mut comments = github.issue_comments(CONTROL_ISSUE_NUMBER).await?;
    comments.sort_by_key(|comment| comment.id);

    let mut terminal_request_ids = HashSet::new();
    for comment in &comments {
        if comment.user_id != OWNER_USER_ID || comment.body.len() > MAX_CONTROL_BODY_BYTES {
            continue;
        }

        if let Ok(result) = serde_json::from_str::<HostControlResult>(&comment.body)
            && result.validate().is_ok()
        {
            terminal_request_ids.insert(result.request_id);
        }
    }

    let mut processed = 0usize;
    for comment in comments {
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
        let execution = executor.execute(operation);
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
    }

    Ok(processed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_issue_is_pinned() {
        assert_eq!(CONTROL_ISSUE_NUMBER, 12);
        assert_eq!(OWNER_USER_ID, 44_100_369);
    }
}
