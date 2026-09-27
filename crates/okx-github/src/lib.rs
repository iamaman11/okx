pub mod telemetry;

pub use telemetry::{
    ISSUE_POLL_TELEMETRY_SCHEMA_V1, IssuePollTelemetryStatus, IssuePollTelemetryStore,
};

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat};
use reqwest::{Client, RequestBuilder, Response, header::HeaderMap};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

pub const GITHUB_API_BASE: &str = "https://api.github.com";
pub const REPOSITORY: &str = "iamaman11/okx";
pub const REPOSITORY_ID: u64 = 1_388_071_566;
pub const OWNER_USER_ID: u64 = 44_100_369;

const COMMENTS_PER_PAGE: u32 = 100;
const MAX_COMMENT_PAGES: u32 = 10;
pub const MAX_COMMENT_BODY_BYTES: usize = 64 * 1024;
pub const ISSUE_CURSOR_SCHEMA_V1: &str = "okx.github.issue-cursor/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GitHubFailureClass {
    Authentication,
    PrimaryRateLimit,
    SecondaryRateLimit,
    PermissionOrResource,
    TransientServer,
    Network,
    UnexpectedResponse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubResponseError {
    pub class: GitHubFailureClass,
    pub status: u16,
    pub rate_limit_limit: Option<u64>,
    pub rate_limit_remaining: Option<u64>,
    pub rate_limit_reset: Option<u64>,
    pub retry_after_seconds: Option<u64>,
    pub request_id: Option<String>,
}

impl std::fmt::Display for GitHubResponseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "class={:?} status={} remaining={:?} reset={:?} retry_after={:?} request_id={:?}",
            self.class,
            self.status,
            self.rate_limit_remaining,
            self.rate_limit_reset,
            self.retry_after_seconds,
            self.request_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct GitHubBackoff {
    failure_count: u32,
    next_retry_at: Option<Instant>,
    last_class: Option<GitHubFailureClass>,
}

impl Default for GitHubBackoff {
    fn default() -> Self {
        Self {
            failure_count: 0,
            next_retry_at: None,
            last_class: None,
        }
    }
}

impl GitHubBackoff {
    pub fn ready(&self) -> bool {
        self.next_retry_at
            .is_none_or(|deadline| Instant::now() >= deadline)
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.next_retry_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .filter(|duration| !duration.is_zero())
    }

    pub const fn last_class(&self) -> Option<GitHubFailureClass> {
        self.last_class
    }

    pub fn on_success(&mut self) {
        self.failure_count = 0;
        self.next_retry_at = None;
        self.last_class = None;
    }

    pub fn on_error(&mut self, error: &GitHubError) -> Duration {
        let class = error.failure_class();
        let delay = retry_delay_for(error, self.failure_count, unix_now_seconds());
        self.failure_count = self.failure_count.saturating_add(1);
        self.next_retry_at = Some(Instant::now() + delay);
        self.last_class = Some(class);
        delay
    }
}

#[derive(Debug, Error)]
pub enum GitHubError {
    #[error("GitHub HTTP transport error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("GitHub response rejected: {0}")]
    Response(GitHubResponseError),

    #[error("GitHub repository identity mismatch")]
    RepositoryIdentityMismatch,

    #[error("GitHub issue number must be non-zero")]
    InvalidIssueNumber,

    #[error("GitHub issue history exceeded the bounded scan limit")]
    HistoryLimitExceeded,

    #[error("GitHub issue comment exceeds the bounded payload limit")]
    CommentTooLarge,

    #[error("GitHub workflow run does not satisfy the trusted artifact policy")]
    UntrustedWorkflowRun,

    #[error("GitHub workflow artifact does not satisfy the trusted artifact policy")]
    UntrustedArtifact,

    #[error("GitHub issue cursor JSON is invalid: {0}")]
    CursorJson(serde_json::Error),

    #[error("GitHub issue cursor does not match the pinned repository/issue")]
    CursorStateMismatch,

    #[error("GitHub issue cursor I/O error: {0}")]
    CursorIo(#[from] std::io::Error),

    #[error("GitHub issue poll telemetry JSON is invalid: {0}")]
    TelemetryJson(serde_json::Error),

    #[error("GitHub issue poll telemetry does not match the pinned repository/issue")]
    TelemetryStateMismatch,

    #[error("GitHub issue poll telemetry timestamp is invalid: {0}")]
    TelemetryTimestamp(chrono::ParseError),

    #[error("GitHub issue poll telemetry I/O error: {0}")]
    TelemetryIo(String),
}

impl GitHubError {
    pub const fn failure_class(&self) -> GitHubFailureClass {
        match self {
            Self::Http(_) => GitHubFailureClass::Network,
            Self::Response(error) => error.class,
            Self::RepositoryIdentityMismatch => GitHubFailureClass::UnexpectedResponse,
            Self::InvalidIssueNumber
            | Self::HistoryLimitExceeded
            | Self::CommentTooLarge
            | Self::UntrustedWorkflowRun
            | Self::UntrustedArtifact
            | Self::CursorJson(_)
            | Self::CursorStateMismatch
            | Self::CursorIo(_)
            | Self::TelemetryJson(_)
            | Self::TelemetryStateMismatch
            | Self::TelemetryTimestamp(_)
            | Self::TelemetryIo(_) => GitHubFailureClass::UnexpectedResponse,
        }
    }

    pub const fn response_error(&self) -> Option<&GitHubResponseError> {
        match self {
            Self::Response(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowRun {
    pub id: u64,
    pub name: String,
    pub event: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub head_sha: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowArtifact {
    pub id: u64,
    pub name: String,
    pub expired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IssueCommentCursor {
    pub created_at: String,
    pub id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueComment {
    pub id: u64,
    pub body: String,
    pub user_id: u64,
    pub created_at: String,
}

impl IssueComment {
    pub fn cursor(&self) -> IssueCommentCursor {
        IssueCommentCursor {
            created_at: self.created_at.clone(),
            id: self.id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IssueCheckpoint {
    pub cursor: Option<IssueCommentCursor>,
    pub terminal_request_ids: BTreeSet<String>,
    pub ledger_initialized: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIssueCursor {
    schema: String,
    repository_id: u64,
    issue_number: u64,
    cursor: IssueCommentCursor,
    #[serde(default)]
    terminal_request_ids: BTreeSet<String>,
    #[serde(default)]
    ledger_initialized: bool,
}

#[derive(Debug, Clone)]
pub struct IssueCursorStore {
    path: PathBuf,
    issue_number: u64,
}

impl IssueCursorStore {
    pub fn new(path: impl Into<PathBuf>, issue_number: u64) -> Result<Self, GitHubError> {
        validate_issue_number(issue_number)?;
        Ok(Self {
            path: path.into(),
            issue_number,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<IssueCommentCursor>, GitHubError> {
        Ok(self.load_checkpoint()?.cursor)
    }

    pub fn load_checkpoint(&self) -> Result<IssueCheckpoint, GitHubError> {
        if !self.path.exists() {
            return Ok(IssueCheckpoint::default());
        }

        let bytes = fs::read(&self.path)?;
        let persisted: PersistedIssueCursor =
            serde_json::from_slice(&bytes).map_err(GitHubError::CursorJson)?;

        if persisted.schema != ISSUE_CURSOR_SCHEMA_V1
            || persisted.repository_id != REPOSITORY_ID
            || persisted.issue_number != self.issue_number
        {
            return Err(GitHubError::CursorStateMismatch);
        }

        Ok(IssueCheckpoint {
            cursor: Some(persisted.cursor),
            terminal_request_ids: persisted.terminal_request_ids,
            ledger_initialized: persisted.ledger_initialized,
        })
    }

    pub fn save(&self, cursor: &IssueCommentCursor) -> Result<(), GitHubError> {
        let checkpoint = self.load_checkpoint()?;
        self.save_checkpoint(
            cursor,
            &checkpoint.terminal_request_ids,
            checkpoint.ledger_initialized,
        )
    }

    pub fn save_checkpoint(
        &self,
        cursor: &IssueCommentCursor,
        terminal_request_ids: &BTreeSet<String>,
        ledger_initialized: bool,
    ) -> Result<(), GitHubError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let persisted = PersistedIssueCursor {
            schema: ISSUE_CURSOR_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            issue_number: self.issue_number,
            cursor: cursor.clone(),
            terminal_request_ids: terminal_request_ids.clone(),
            ledger_initialized,
        };
        let bytes = serde_json::to_vec(&persisted).map_err(GitHubError::CursorJson)?;

        let tmp = self.path.with_extension("tmp");
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;

        if self.path.exists() {
            fs::remove_file(&self.path)?;
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

pub struct GitHubClient {
    http: Client,
    token: Zeroizing<String>,
}

impl GitHubClient {
    pub fn new(token: Zeroizing<String>, user_agent: &str) -> Result<Self, GitHubError> {
        let http = Client::builder().user_agent(user_agent).build()?;
        Ok(Self { http, token })
    }

    async fn send_checked(&self, request: RequestBuilder) -> Result<Response, GitHubError> {
        let response = request.send().await?;
        if response.status().is_success() {
            return Ok(response);
        }
        Err(GitHubError::Response(classify_response_error(&response)))
    }

    pub async fn verify_repository_identity(&self) -> Result<(), GitHubError> {
        let url = format!("{GITHUB_API_BASE}/repos/{REPOSITORY}");
        let repository: RepositoryIdentity = self
            .send_checked(
                self.http
                    .get(url)
                    .bearer_auth(self.token.as_str())
                    .header("Accept", "application/vnd.github+json"),
            )
            .await?
            .json()
            .await?;

        if repository.id != REPOSITORY_ID
            || repository.owner.id != OWNER_USER_ID
            || repository.full_name != REPOSITORY
        {
            return Err(GitHubError::RepositoryIdentityMismatch);
        }

        Ok(())
    }

    pub async fn issue_comments(
        &self,
        issue_number: u64,
    ) -> Result<Vec<IssueComment>, GitHubError> {
        self.issue_comments_after(issue_number, None).await
    }

    pub async fn recent_issue_comments(
        &self,
        issue_number: u64,
    ) -> Result<Vec<IssueComment>, GitHubError> {
        let comments = self.issue_comments(issue_number).await?;
        let keep_from = comments.len().saturating_sub(COMMENTS_PER_PAGE as usize);
        Ok(comments[keep_from..].to_vec())
    }

    pub async fn issue_comments_after(
        &self,
        issue_number: u64,
        cursor: Option<&IssueCommentCursor>,
    ) -> Result<Vec<IssueComment>, GitHubError> {
        validate_issue_number(issue_number)?;

        let since = cursor.map(cursor_overlap_since).transpose()?;
        let mut fresh = Vec::new();

        for page in 1..=MAX_COMMENT_PAGES {
            let url =
                format!("{GITHUB_API_BASE}/repos/{REPOSITORY}/issues/{issue_number}/comments");
            let mut query = vec![
                ("per_page", COMMENTS_PER_PAGE.to_string()),
                ("page", page.to_string()),
            ];
            if let Some(since) = since.as_ref() {
                query.push(("since", since.clone()));
            }

            let page_comments: Vec<RawIssueComment> = self
                .send_checked(
                    self.http
                        .get(url)
                        .query(&query)
                        .bearer_auth(self.token.as_str())
                        .header("Accept", "application/vnd.github+json"),
                )
                .await?
                .json()
                .await?;

            let count = page_comments.len();
            fresh.extend(
                page_comments
                    .into_iter()
                    .map(RawIssueComment::into_issue_comment)
                    .filter(|comment| cursor.is_none_or(|current| comment.cursor() > *current)),
            );

            if count < COMMENTS_PER_PAGE as usize {
                fresh.sort_by_key(IssueComment::cursor);
                return Ok(fresh);
            }
        }

        Err(GitHubError::HistoryLimitExceeded)
    }

    pub async fn workflow_run(&self, run_id: u64) -> Result<WorkflowRun, GitHubError> {
        let url = format!("{GITHUB_API_BASE}/repos/{REPOSITORY}/actions/runs/{run_id}");
        let run: RawWorkflowRun = self
            .send_checked(
                self.http
                    .get(url)
                    .bearer_auth(self.token.as_str())
                    .header("Accept", "application/vnd.github+json"),
            )
            .await?
            .json()
            .await?;

        if run.id != run_id {
            return Err(GitHubError::UntrustedWorkflowRun);
        }

        Ok(WorkflowRun {
            id: run.id,
            name: run.name,
            event: run.event,
            status: run.status,
            conclusion: run.conclusion,
            head_sha: run.head_sha,
        })
    }

    pub async fn workflow_artifact(
        &self,
        run_id: u64,
        artifact_id: u64,
    ) -> Result<WorkflowArtifact, GitHubError> {
        let url = format!(
            "{GITHUB_API_BASE}/repos/{REPOSITORY}/actions/runs/{run_id}/artifacts?per_page=100"
        );
        let response: RawArtifactsResponse = self
            .send_checked(
                self.http
                    .get(url)
                    .bearer_auth(self.token.as_str())
                    .header("Accept", "application/vnd.github+json"),
            )
            .await?
            .json()
            .await?;

        let artifact = response
            .artifacts
            .into_iter()
            .find(|artifact| artifact.id == artifact_id)
            .ok_or(GitHubError::UntrustedArtifact)?;

        Ok(WorkflowArtifact {
            id: artifact.id,
            name: artifact.name,
            expired: artifact.expired,
        })
    }

    pub async fn download_artifact_zip(&self, artifact_id: u64) -> Result<Vec<u8>, GitHubError> {
        let url =
            format!("{GITHUB_API_BASE}/repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip");
        let bytes = self
            .send_checked(
                self.http
                    .get(url)
                    .bearer_auth(self.token.as_str())
                    .header("Accept", "application/vnd.github+json"),
            )
            .await?
            .bytes()
            .await?;

        Ok(bytes.to_vec())
    }

    pub async fn post_issue_comment(
        &self,
        issue_number: u64,
        body: &str,
    ) -> Result<(), GitHubError> {
        validate_issue_number(issue_number)?;

        if body.len() > MAX_COMMENT_BODY_BYTES {
            return Err(GitHubError::CommentTooLarge);
        }

        let url = format!("{GITHUB_API_BASE}/repos/{REPOSITORY}/issues/{issue_number}/comments");
        self.send_checked(
            self.http
                .post(url)
                .bearer_auth(self.token.as_str())
                .header("Accept", "application/vnd.github+json")
                .json(&serde_json::json!({ "body": body })),
        )
        .await?;

        Ok(())
    }
}

fn classify_response_error(response: &Response) -> GitHubResponseError {
    let status = response.status().as_u16();
    let headers = response.headers();
    let rate_limit_limit = header_u64(headers, "x-ratelimit-limit");
    let rate_limit_remaining = header_u64(headers, "x-ratelimit-remaining");
    let rate_limit_reset = header_u64(headers, "x-ratelimit-reset");
    let retry_after_seconds = header_u64(headers, "retry-after");
    let request_id = header_text(headers, "x-github-request-id");

    let class = classify_status(status, rate_limit_remaining, retry_after_seconds);

    GitHubResponseError {
        class,
        status,
        rate_limit_limit,
        rate_limit_remaining,
        rate_limit_reset,
        retry_after_seconds,
        request_id,
    }
}

fn classify_status(
    status: u16,
    rate_limit_remaining: Option<u64>,
    retry_after_seconds: Option<u64>,
) -> GitHubFailureClass {
    match status {
        401 => GitHubFailureClass::Authentication,
        403 | 429 if retry_after_seconds.is_some() => GitHubFailureClass::SecondaryRateLimit,
        403 | 429 if rate_limit_remaining == Some(0) => GitHubFailureClass::PrimaryRateLimit,
        429 => GitHubFailureClass::SecondaryRateLimit,
        403 | 404 => GitHubFailureClass::PermissionOrResource,
        500..=599 => GitHubFailureClass::TransientServer,
        400..=499 => GitHubFailureClass::PermissionOrResource,
        _ => GitHubFailureClass::UnexpectedResponse,
    }
}

fn header_u64(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.trim().parse().ok()
}

fn header_text(headers: &HeaderMap, name: &'static str) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?.trim();
    (!value.is_empty()).then(|| value.chars().take(128).collect())
}

fn retry_delay_for(error: &GitHubError, attempt: u32, now_epoch_seconds: u64) -> Duration {
    const TRANSIENT: [u64; 5] = [1, 5, 15, 30, 60];
    const SLOW: [u64; 4] = [60, 120, 300, 300];

    let response = error.response_error();
    if let Some(retry_after) = response.and_then(|value| value.retry_after_seconds) {
        return Duration::from_secs(retry_after.max(1));
    }

    match error.failure_class() {
        GitHubFailureClass::PrimaryRateLimit => {
            let reset = response.and_then(|value| value.rate_limit_reset);
            Duration::from_secs(
                reset
                    .map(|value| value.saturating_sub(now_epoch_seconds).max(1))
                    .unwrap_or(60),
            )
        }
        GitHubFailureClass::SecondaryRateLimit
        | GitHubFailureClass::Authentication
        | GitHubFailureClass::PermissionOrResource
        | GitHubFailureClass::UnexpectedResponse => {
            Duration::from_secs(SLOW[attempt.min((SLOW.len() - 1) as u32) as usize])
        }
        GitHubFailureClass::TransientServer | GitHubFailureClass::Network => {
            Duration::from_secs(TRANSIENT[attempt.min((TRANSIENT.len() - 1) as u32) as usize])
        }
    }
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn cursor_overlap_since(cursor: &IssueCommentCursor) -> Result<String, GitHubError> {
    let timestamp = DateTime::parse_from_rfc3339(&cursor.created_at)
        .map_err(|_| GitHubError::CursorStateMismatch)?
        - ChronoDuration::seconds(1);
    Ok(timestamp.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn validate_issue_number(issue_number: u64) -> Result<(), GitHubError> {
    if issue_number == 0 {
        Err(GitHubError::InvalidIssueNumber)
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct RepositoryIdentity {
    id: u64,
    full_name: String,
    owner: RepositoryOwner,
}

#[derive(Debug, Deserialize)]
struct RepositoryOwner {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct RawWorkflowRun {
    id: u64,
    name: String,
    event: String,
    status: String,
    conclusion: Option<String>,
    head_sha: String,
}

#[derive(Debug, Deserialize)]
struct RawArtifactsResponse {
    artifacts: Vec<RawWorkflowArtifact>,
}

#[derive(Debug, Deserialize)]
struct RawWorkflowArtifact {
    id: u64,
    name: String,
    expired: bool,
}

#[derive(Debug, Deserialize)]
struct RawIssueComment {
    id: u64,
    #[serde(default)]
    body: String,
    user: CommentUser,
    created_at: String,
}

impl RawIssueComment {
    fn into_issue_comment(self) -> IssueComment {
        IssueComment {
            id: self.id,
            body: self.body,
            user_id: self.user.id,
            created_at: self.created_at,
        }
    }
}

#[derive(Debug, Deserialize)]
struct CommentUser {
    id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response_error(
        status: u16,
        remaining: Option<u64>,
        reset: Option<u64>,
        retry_after: Option<u64>,
    ) -> GitHubError {
        GitHubError::Response(GitHubResponseError {
            class: classify_status(status, remaining, retry_after),
            status,
            rate_limit_limit: Some(5_000),
            rate_limit_remaining: remaining,
            rate_limit_reset: reset,
            retry_after_seconds: retry_after,
            request_id: Some("REQ_TEST".to_owned()),
        })
    }

    #[test]
    fn classifies_auth_permission_and_rate_limit_responses() {
        assert_eq!(
            response_error(401, Some(100), None, None).failure_class(),
            GitHubFailureClass::Authentication
        );
        assert_eq!(
            response_error(403, Some(0), Some(2_000), None).failure_class(),
            GitHubFailureClass::PrimaryRateLimit
        );
        assert_eq!(
            response_error(403, Some(100), None, Some(90)).failure_class(),
            GitHubFailureClass::SecondaryRateLimit
        );
        assert_eq!(
            response_error(429, Some(100), None, None).failure_class(),
            GitHubFailureClass::SecondaryRateLimit
        );
        assert_eq!(
            response_error(403, Some(100), None, None).failure_class(),
            GitHubFailureClass::PermissionOrResource
        );
    }

    #[test]
    fn retry_policy_honors_retry_after_and_primary_reset() {
        let secondary = response_error(429, Some(100), None, Some(73));
        assert_eq!(
            retry_delay_for(&secondary, 0, 1_000),
            Duration::from_secs(73)
        );

        let primary = response_error(403, Some(0), Some(1_120), None);
        assert_eq!(
            retry_delay_for(&primary, 0, 1_000),
            Duration::from_secs(120)
        );
    }

    #[test]
    fn retry_policy_uses_bounded_slow_and_transient_backoff() {
        let permission = response_error(403, Some(100), None, None);
        assert_eq!(
            retry_delay_for(&permission, 0, 1_000),
            Duration::from_secs(60)
        );
        assert_eq!(
            retry_delay_for(&permission, 99, 1_000),
            Duration::from_secs(300)
        );

        let server = response_error(503, Some(100), None, None);
        assert_eq!(retry_delay_for(&server, 0, 1_000), Duration::from_secs(1));
        assert_eq!(retry_delay_for(&server, 99, 1_000), Duration::from_secs(60));
    }

    #[test]
    fn backoff_success_resets_failure_state() {
        let mut backoff = GitHubBackoff::default();
        let error = response_error(401, Some(100), None, None);
        assert_eq!(backoff.on_error(&error), Duration::from_secs(60));
        assert_eq!(
            backoff.last_class(),
            Some(GitHubFailureClass::Authentication)
        );
        assert!(!backoff.ready());

        backoff.on_success();
        assert!(backoff.ready());
        assert_eq!(backoff.remaining(), None);
        assert_eq!(backoff.last_class(), None);
    }

    #[test]
    fn repository_identity_constants_are_pinned() {
        assert_eq!(REPOSITORY, "iamaman11/okx");
        assert_eq!(REPOSITORY_ID, 1_388_071_566);
        assert_eq!(OWNER_USER_ID, 44_100_369);
    }

    #[test]
    fn issue_number_zero_fails_closed() {
        assert!(matches!(
            validate_issue_number(0),
            Err(GitHubError::InvalidIssueNumber)
        ));
    }

    #[test]
    fn cursor_overlap_rewinds_one_second_for_timestamp_ties() {
        let cursor = IssueCommentCursor {
            created_at: "2026-09-27T12:00:00Z".to_owned(),
            id: 10,
        };
        assert_eq!(
            cursor_overlap_since(&cursor).expect("overlap"),
            "2026-09-27T11:59:59Z"
        );
    }

    #[test]
    fn comment_cursor_orders_by_creation_then_id() {
        let older = IssueCommentCursor {
            created_at: "2026-09-27T12:00:00Z".to_owned(),
            id: 10,
        };
        let newer_same_second = IssueCommentCursor {
            created_at: "2026-09-27T12:00:00Z".to_owned(),
            id: 11,
        };
        let newest = IssueCommentCursor {
            created_at: "2026-09-27T12:00:01Z".to_owned(),
            id: 1,
        };

        assert!(older < newer_same_second);
        assert!(newer_same_second < newest);
    }

    #[test]
    fn cursor_store_round_trips_and_rejects_wrong_issue() {
        let root =
            std::env::temp_dir().join(format!("okx-github-cursor-{}-{}", std::process::id(), 47));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("temp root");

        let path = root.join("cursor.json");
        let store = IssueCursorStore::new(&path, 10).expect("store");
        let cursor = IssueCommentCursor {
            created_at: "2026-09-27T12:00:00Z".to_owned(),
            id: 123,
        };
        let terminal_ids = BTreeSet::from([
            "req_0123456789abcdef".to_owned(),
            "req_fedcba9876543210".to_owned(),
        ]);
        store
            .save_checkpoint(&cursor, &terminal_ids, true)
            .expect("save");
        let checkpoint = store.load_checkpoint().expect("load checkpoint");
        assert_eq!(checkpoint.cursor, Some(cursor));
        assert_eq!(checkpoint.terminal_request_ids, terminal_ids);
        assert!(checkpoint.ledger_initialized);

        let wrong = IssueCursorStore::new(&path, 12).expect("wrong store");
        assert!(matches!(
            wrong.load(),
            Err(GitHubError::CursorStateMismatch)
        ));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_cursor_without_ledger_migrates_as_uninitialized() {
        let root =
            std::env::temp_dir().join(format!("okx-github-legacy-cursor-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("temp root");
        let path = root.join("cursor.json");
        fs::write(
            &path,
            serde_json::json!({
                "schema": ISSUE_CURSOR_SCHEMA_V1,
                "repository_id": REPOSITORY_ID,
                "issue_number": 10,
                "cursor": {
                    "created_at": "2026-09-27T12:00:00Z",
                    "id": 123
                }
            })
            .to_string(),
        )
        .expect("legacy write");

        let store = IssueCursorStore::new(&path, 10).expect("store");
        let checkpoint = store.load_checkpoint().expect("legacy load");
        assert_eq!(
            checkpoint.cursor,
            Some(IssueCommentCursor {
                created_at: "2026-09-27T12:00:00Z".to_owned(),
                id: 123,
            })
        );
        assert!(checkpoint.terminal_request_ids.is_empty());
        assert!(!checkpoint.ledger_initialized);

        let _ = fs::remove_dir_all(root);
    }
}
