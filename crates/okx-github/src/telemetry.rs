use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::{GitHubError, IssueCommentCursor, REPOSITORY_ID};

pub const ISSUE_POLL_TELEMETRY_SCHEMA_V1: &str = "okx.github.issue-poll-telemetry/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueResponseSizeTelemetry {
    pub plaintext_bytes: u64,
    pub plaintext_budget_bytes: u64,
    pub predicted_comment_bytes: u64,
    pub budget_exceeded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIssuePollTelemetry {
    schema: String,
    repository_id: u64,
    issue_number: u64,
    polls_completed: u64,
    comments_scanned_total: u64,
    last_comments_scanned: u64,
    last_poll_completed_at: String,
    last_fetch_latency_ms: u64,
    #[serde(default)]
    last_request_latency_ms: Option<u64>,
    #[serde(default)]
    last_response_size: Option<IssueResponseSizeTelemetry>,
    cursor: Option<IssueCommentCursor>,
    last_terminal_request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IssuePollTelemetryStatus {
    pub schema: String,
    pub repository_id: u64,
    pub issue_number: u64,
    pub polls_completed: u64,
    pub comments_scanned_total: u64,
    pub last_comments_scanned: u64,
    pub last_poll_completed_at: String,
    pub poll_age_ms: u64,
    pub last_fetch_latency_ms: u64,
    pub last_request_latency_ms: Option<u64>,
    pub last_response_size: Option<IssueResponseSizeTelemetry>,
    pub cursor: Option<IssueCommentCursor>,
    pub last_terminal_request_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IssuePollTelemetryStore {
    path: PathBuf,
    issue_number: u64,
}

impl IssuePollTelemetryStore {
    pub fn new(path: impl Into<PathBuf>, issue_number: u64) -> Result<Self, GitHubError> {
        if issue_number == 0 {
            return Err(GitHubError::InvalidIssueNumber);
        }
        Ok(Self {
            path: path.into(),
            issue_number,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn record_success(
        &self,
        fetch_latency: Duration,
        comments_scanned: usize,
        cursor: Option<&IssueCommentCursor>,
        last_terminal_request_id: Option<&str>,
        last_request_latency_ms: Option<u64>,
        last_response_size: Option<IssueResponseSizeTelemetry>,
    ) -> Result<(), GitHubError> {
        let previous = self.load_persisted()?;
        let polls_completed = previous
            .as_ref()
            .map_or(1, |value| value.polls_completed.saturating_add(1));
        let comments_scanned_total = previous.as_ref().map_or(comments_scanned as u64, |value| {
            value
                .comments_scanned_total
                .saturating_add(comments_scanned as u64)
        });
        let last_terminal_request_id = last_terminal_request_id.map(str::to_owned).or_else(|| {
            previous
                .as_ref()
                .and_then(|value| value.last_terminal_request_id.clone())
        });
        let last_request_latency_ms = last_request_latency_ms.or_else(|| {
            previous
                .as_ref()
                .and_then(|value| value.last_request_latency_ms)
        });
        let last_response_size = last_response_size.or_else(|| {
            previous
                .as_ref()
                .and_then(|value| value.last_response_size)
        });

        let telemetry = PersistedIssuePollTelemetry {
            schema: ISSUE_POLL_TELEMETRY_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            issue_number: self.issue_number,
            polls_completed,
            comments_scanned_total,
            last_comments_scanned: comments_scanned as u64,
            last_poll_completed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            last_fetch_latency_ms: fetch_latency.as_millis().min(u64::MAX as u128) as u64,
            last_request_latency_ms,
            last_response_size,
            cursor: cursor.cloned(),
            last_terminal_request_id,
        };
        self.write_atomic(&telemetry)
    }

    pub fn status(&self) -> Result<Option<IssuePollTelemetryStatus>, GitHubError> {
        let Some(value) = self.load_persisted()? else {
            return Ok(None);
        };

        let completed_at = DateTime::parse_from_rfc3339(&value.last_poll_completed_at)
            .map_err(GitHubError::TelemetryTimestamp)?;
        let age = Utc::now()
            .signed_duration_since(completed_at)
            .num_milliseconds()
            .max(0) as u64;

        Ok(Some(IssuePollTelemetryStatus {
            schema: value.schema,
            repository_id: value.repository_id,
            issue_number: value.issue_number,
            polls_completed: value.polls_completed,
            comments_scanned_total: value.comments_scanned_total,
            last_comments_scanned: value.last_comments_scanned,
            last_poll_completed_at: value.last_poll_completed_at,
            poll_age_ms: age,
            last_fetch_latency_ms: value.last_fetch_latency_ms,
            last_request_latency_ms: value.last_request_latency_ms,
            last_response_size: value.last_response_size,
            cursor: value.cursor,
            last_terminal_request_id: value.last_terminal_request_id,
        }))
    }

    fn load_persisted(&self) -> Result<Option<PersistedIssuePollTelemetry>, GitHubError> {
        if !self.path.exists() {
            return Ok(None);
        }

        let bytes =
            fs::read(&self.path).map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        let value: PersistedIssuePollTelemetry =
            serde_json::from_slice(&bytes).map_err(GitHubError::TelemetryJson)?;
        if value.schema != ISSUE_POLL_TELEMETRY_SCHEMA_V1
            || value.repository_id != REPOSITORY_ID
            || value.issue_number != self.issue_number
        {
            return Err(GitHubError::TelemetryStateMismatch);
        }
        Ok(Some(value))
    }

    fn write_atomic(&self, value: &PersistedIssuePollTelemetry) -> Result<(), GitHubError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        }

        let bytes = serde_json::to_vec(value).map_err(GitHubError::TelemetryJson)?;
        let tmp = self.path.with_extension("tmp");
        let mut file =
            File::create(&tmp).map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        file.write_all(&bytes)
            .map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        file.sync_all()
            .map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        if self.path.exists() {
            fs::remove_file(&self.path)
                .map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        }
        fs::rename(&tmp, &self.path)
            .map_err(|error| GitHubError::TelemetryIo(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "okx-github-telemetry-{name}-{}.json",
            std::process::id()
        ))
    }

    #[test]
    fn telemetry_is_bounded_and_accumulates_counts() {
        let path = temp_path("counts");
        let _ = fs::remove_file(&path);
        let store = IssuePollTelemetryStore::new(&path, 10).expect("store");
        let cursor = IssueCommentCursor {
            created_at: "2026-09-27T15:00:00Z".to_owned(),
            id: 42,
        };

        store
            .record_success(
                Duration::from_millis(25),
                3,
                Some(&cursor),
                Some("req_0123456789abcdef"),
                Some(1234),
                Some(IssueResponseSizeTelemetry {
                    plaintext_bytes: 10_535,
                    plaintext_budget_bytes: 12_288,
                    predicted_comment_bytes: 14_312,
                    budget_exceeded: false,
                }),
            )
            .expect("first");
        store
            .record_success(
                Duration::from_millis(11),
                0,
                Some(&cursor),
                None,
                None,
                None,
            )
            .expect("second");

        let status = store.status().expect("status").expect("present");
        assert_eq!(status.polls_completed, 2);
        assert_eq!(status.comments_scanned_total, 3);
        assert_eq!(status.last_comments_scanned, 0);
        assert_eq!(status.last_fetch_latency_ms, 11);
        assert_eq!(status.last_request_latency_ms, Some(1234));
        assert_eq!(
            status.last_response_size,
            Some(IssueResponseSizeTelemetry {
                plaintext_bytes: 10_535,
                plaintext_budget_bytes: 12_288,
                predicted_comment_bytes: 14_312,
                budget_exceeded: false,
            })
        );
        assert_eq!(status.cursor, Some(cursor));
        assert_eq!(
            status.last_terminal_request_id.as_deref(),
            Some("req_0123456789abcdef")
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn telemetry_rejects_wrong_issue_identity() {
        let path = temp_path("identity");
        let _ = fs::remove_file(&path);
        let store = IssuePollTelemetryStore::new(&path, 10).expect("store");
        store
            .record_success(Duration::from_millis(1), 0, None, None, None, None)
            .expect("record");

        let other = IssuePollTelemetryStore::new(&path, 12).expect("other");
        assert!(matches!(
            other.status(),
            Err(GitHubError::TelemetryStateMismatch)
        ));

        let _ = fs::remove_file(path);
    }
}
