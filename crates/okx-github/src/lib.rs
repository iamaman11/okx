use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use reqwest::Client;
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

#[derive(Debug, Error)]
pub enum GitHubError {
    #[error("GitHub HTTP error: {0}")]
    Http(#[from] reqwest::Error),

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

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedIssueCursor {
    schema: String,
    repository_id: u64,
    issue_number: u64,
    cursor: IssueCommentCursor,
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
        if !self.path.exists() {
            return Ok(None);
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

        Ok(Some(persisted.cursor))
    }

    pub fn save(&self, cursor: &IssueCommentCursor) -> Result<(), GitHubError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let persisted = PersistedIssueCursor {
            schema: ISSUE_CURSOR_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            issue_number: self.issue_number,
            cursor: cursor.clone(),
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

    pub async fn verify_repository_identity(&self) -> Result<(), GitHubError> {
        let url = format!("{GITHUB_API_BASE}/repos/{REPOSITORY}");
        let repository: RepositoryIdentity = self
            .http
            .get(url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
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
        validate_issue_number(issue_number)?;

        let url = format!(
            "{GITHUB_API_BASE}/repos/{REPOSITORY}/issues/{issue_number}/comments?per_page={COMMENTS_PER_PAGE}&page=1&sort=created&direction=desc"
        );
        let comments: Vec<RawIssueComment> = self
            .http
            .get(url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(comments
            .into_iter()
            .map(RawIssueComment::into_issue_comment)
            .collect())
    }

    pub async fn issue_comments_after(
        &self,
        issue_number: u64,
        cursor: Option<&IssueCommentCursor>,
    ) -> Result<Vec<IssueComment>, GitHubError> {
        validate_issue_number(issue_number)?;

        let mut fresh = Vec::new();
        for page in 1..=MAX_COMMENT_PAGES {
            let url = format!(
                "{GITHUB_API_BASE}/repos/{REPOSITORY}/issues/{issue_number}/comments?per_page={COMMENTS_PER_PAGE}&page={page}&sort=created&direction=desc"
            );
            let page_comments: Vec<RawIssueComment> = self
                .http
                .get(url)
                .bearer_auth(self.token.as_str())
                .header("Accept", "application/vnd.github+json")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;

            let count = page_comments.len();
            let mut reached_cursor = false;
            for raw in page_comments {
                let comment = raw.into_issue_comment();
                if cursor.is_none_or(|current| comment.cursor() > *current) {
                    fresh.push(comment);
                } else {
                    reached_cursor = true;
                    break;
                }
            }

            if reached_cursor || count < COMMENTS_PER_PAGE as usize {
                fresh.sort_by_key(IssueComment::cursor);
                return Ok(fresh);
            }
        }

        Err(GitHubError::HistoryLimitExceeded)
    }

    pub async fn workflow_run(&self, run_id: u64) -> Result<WorkflowRun, GitHubError> {
        let url = format!("{GITHUB_API_BASE}/repos/{REPOSITORY}/actions/runs/{run_id}");
        let run: RawWorkflowRun = self
            .http
            .get(url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
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
            .http
            .get(url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
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
            .http
            .get(url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .send()
            .await?
            .error_for_status()?
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
        self.http
            .post(url)
            .bearer_auth(self.token.as_str())
            .header("Accept", "application/vnd.github+json")
            .json(&serde_json::json!({ "body": body }))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
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
        let root = std::env::temp_dir().join(format!(
            "okx-github-cursor-{}-{}",
            std::process::id(),
            47
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("temp root");

        let path = root.join("cursor.json");
        let store = IssueCursorStore::new(&path, 10).expect("store");
        let cursor = IssueCommentCursor {
            created_at: "2026-09-27T12:00:00Z".to_owned(),
            id: 123,
        };
        store.save(&cursor).expect("save");
        assert_eq!(store.load().expect("load"), Some(cursor));

        let wrong = IssueCursorStore::new(&path, 12).expect("wrong store");
        assert!(matches!(
            wrong.load(),
            Err(GitHubError::CursorStateMismatch)
        ));

        let _ = fs::remove_dir_all(root);
    }
}
