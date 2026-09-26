use std::collections::HashSet;

use okx_protocol::{MailboxDirection, MailboxEnvelope};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    AgentError, AgentResult,
    identity::AgentIdentity,
    once::process_once_now,
};

pub const GITHUB_REPOSITORY_ID: u64 = 1_388_071_566;
pub const GITHUB_OWNER_USER_ID: u64 = 44_100_369;
pub const GITHUB_MAILBOX_IDENTITY_SCHEMA_V1: &str = "okx.github-mailbox.identity/v1";

const GITHUB_API_BASE: &str = "https://api.github.com";
const GITHUB_REPOSITORY: &str = "iamaman11/okx";
const MAX_COMMENT_PAGES: u32 = 10;
const COMMENTS_PER_PAGE: u32 = 100;
const MAX_MAILBOX_BODY_BYTES: usize = 64 * 1024;

pub struct GitHubMailboxClient {
    http: reqwest::Client,
    token: Zeroizing<String>,
    issue_number: u64,
}

impl GitHubMailboxClient {
    pub fn new(issue_number: u64, token: Zeroizing<String>) -> AgentResult<Self> {
        if issue_number == 0 {
            return Err(AgentError::InvalidMailboxIssue);
        }

        let http = reqwest::Client::builder()
            .user_agent("iamaman11-okx-agent/0.1")
            .build()?;

        Ok(Self {
            http,
            token,
            issue_number,
        })
    }

    pub async fn verify_repository_identity(&self) -> AgentResult<()> {
        let url = format!("{GITHUB_API_BASE}/repos/{GITHUB_REPOSITORY}");
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

        if repository.id != GITHUB_REPOSITORY_ID
            || repository.owner.id != GITHUB_OWNER_USER_ID
            || repository.full_name != GITHUB_REPOSITORY
        {
            return Err(AgentError::GithubRepositoryIdentityMismatch);
        }

        Ok(())
    }

    pub async fn ensure_identity_published(&self, identity: &AgentIdentity) -> AgentResult<()> {
        let expected = PublishedIdentity {
            schema: GITHUB_MAILBOX_IDENTITY_SCHEMA_V1.to_owned(),
            repository_id: GITHUB_REPOSITORY_ID,
            owner_user_id: GITHUB_OWNER_USER_ID,
            issue_number: self.issue_number,
            key_id: identity.key_id.clone(),
            public_key: identity.public_key.clone(),
        };

        let comments = self.comments().await?;
        let already_published = comments.iter().any(|comment| {
            comment.user.id == GITHUB_OWNER_USER_ID
                && serde_json::from_str::<PublishedIdentity>(&comment.body)
                    .is_ok_and(|published| published == expected)
        });

        if !already_published {
            self.post_comment(&serde_json::to_string(&expected)?).await?;
        }

        Ok(())
    }

    pub async fn process_pending(
        &self,
        expected_key_id: &str,
        agent_private_key: &[u8; 32],
    ) -> AgentResult<usize> {
        let mut comments = self.comments().await?;
        comments.sort_by_key(|comment| comment.id);

        let mut terminal_request_ids = HashSet::new();
        for comment in &comments {
            if comment.user.id != GITHUB_OWNER_USER_ID || comment.body.len() > MAX_MAILBOX_BODY_BYTES
            {
                continue;
            }
            if let Ok(envelope) = serde_json::from_str::<MailboxEnvelope>(&comment.body)
                && envelope.direction == MailboxDirection::AgentToClient
            {
                terminal_request_ids.insert(envelope.request_id);
            }
        }

        let mut processed = 0usize;
        for comment in comments {
            if comment.user.id != GITHUB_OWNER_USER_ID || comment.body.len() > MAX_MAILBOX_BODY_BYTES
            {
                continue;
            }

            let Ok(envelope) = serde_json::from_str::<MailboxEnvelope>(&comment.body) else {
                continue;
            };
            if envelope.direction != MailboxDirection::ClientToAgent
                || terminal_request_ids.contains(&envelope.request_id)
            {
                continue;
            }

            match process_once_now(&envelope, expected_key_id, agent_private_key) {
                Ok(response) => {
                    self.post_comment(&serde_json::to_string(&response)?).await?;
                    terminal_request_ids.insert(envelope.request_id);
                    processed += 1;
                }
                Err(error) => {
                    eprintln!(
                        "mailbox request {} rejected before terminal response: {}",
                        envelope.request_id, error
                    );
                }
            }
        }

        Ok(processed)
    }

    async fn comments(&self) -> AgentResult<Vec<IssueComment>> {
        let mut all = Vec::new();

        for page in 1..=MAX_COMMENT_PAGES {
            let url = format!(
                "{GITHUB_API_BASE}/repos/{GITHUB_REPOSITORY}/issues/{}/comments?per_page={COMMENTS_PER_PAGE}&page={page}",
                self.issue_number
            );
            let page_comments: Vec<IssueComment> = self
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
            all.extend(page_comments);
            if count < COMMENTS_PER_PAGE as usize {
                return Ok(all);
            }
        }

        Err(AgentError::MailboxHistoryLimitExceeded)
    }

    async fn post_comment(&self, body: &str) -> AgentResult<()> {
        if body.len() > MAX_MAILBOX_BODY_BYTES {
            return Err(AgentError::MailboxPayloadTooLarge(body.len()));
        }

        let url = format!(
            "{GITHUB_API_BASE}/repos/{GITHUB_REPOSITORY}/issues/{}/comments",
            self.issue_number
        );
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
struct IssueComment {
    id: u64,
    #[serde(default)]
    body: String,
    user: CommentUser,
}

#[derive(Debug, Deserialize)]
struct CommentUser {
    id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedIdentity {
    pub schema: String,
    pub repository_id: u64,
    pub owner_user_id: u64,
    pub issue_number: u64,
    pub key_id: String,
    pub public_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_identity_contains_public_material_only() {
        let identity = PublishedIdentity {
            schema: GITHUB_MAILBOX_IDENTITY_SCHEMA_V1.to_owned(),
            repository_id: GITHUB_REPOSITORY_ID,
            owner_user_id: GITHUB_OWNER_USER_ID,
            issue_number: 10,
            key_id: "agent-key-1".to_owned(),
            public_key: "public-key".to_owned(),
        };

        let json = serde_json::to_string(&identity).expect("serialize");
        assert!(json.contains("public-key"));
        assert!(!json.contains("private"));
        assert!(!json.contains("token"));
    }
}
