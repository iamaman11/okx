use reqwest::Client;
use serde::Deserialize;
use thiserror::Error;
use zeroize::Zeroizing;

pub const GITHUB_API_BASE: &str = "https://api.github.com";
pub const REPOSITORY: &str = "iamaman11/okx";
pub const REPOSITORY_ID: u64 = 1_388_071_566;
pub const OWNER_USER_ID: u64 = 44_100_369;

const COMMENTS_PER_PAGE: u32 = 100;
const MAX_COMMENT_PAGES: u32 = 10;
pub const MAX_COMMENT_BODY_BYTES: usize = 64 * 1024;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub struct IssueComment {
    pub id: u64,
    pub body: String,
    pub user_id: u64,
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
        validate_issue_number(issue_number)?;

        let mut all = Vec::new();
        for page in 1..=MAX_COMMENT_PAGES {
            let url = format!(
                "{GITHUB_API_BASE}/repos/{REPOSITORY}/issues/{issue_number}/comments?per_page={COMMENTS_PER_PAGE}&page={page}"
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
            all.extend(page_comments.into_iter().map(|comment| IssueComment {
                id: comment.id,
                body: comment.body,
                user_id: comment.user.id,
            }));

            if count < COMMENTS_PER_PAGE as usize {
                return Ok(all);
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
        let url = format!(
            "{GITHUB_API_BASE}/repos/{REPOSITORY}/actions/artifacts/{artifact_id}/zip"
        );
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

struct RawIssueComment {
    id: u64,
    #[serde(default)]
    body: String,
    user: CommentUser,
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
}
