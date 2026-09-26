use std::collections::HashSet;

use okx_github::{GitHubClient, OWNER_USER_ID, REPOSITORY_ID};
use okx_protocol::{MailboxDirection, MailboxEnvelope};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{AgentError, AgentResult, identity::AgentIdentity, once::process_once_now};

pub const GITHUB_MAILBOX_IDENTITY_SCHEMA_V1: &str = "okx.github-mailbox.identity/v1";

pub struct GitHubMailboxClient {
    github: GitHubClient,
    issue_number: u64,
}

impl GitHubMailboxClient {
    pub fn new(issue_number: u64, token: Zeroizing<String>) -> AgentResult<Self> {
        if issue_number == 0 {
            return Err(AgentError::InvalidMailboxIssue);
        }

        Ok(Self {
            github: GitHubClient::new(token, "iamaman11-okx-agent/0.1")?,
            issue_number,
        })
    }

    pub async fn verify_repository_identity(&self) -> AgentResult<()> {
        self.github.verify_repository_identity().await?;
        Ok(())
    }

    pub async fn ensure_identity_published(&self, identity: &AgentIdentity) -> AgentResult<()> {
        let expected = PublishedIdentity {
            schema: GITHUB_MAILBOX_IDENTITY_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            owner_user_id: OWNER_USER_ID,
            issue_number: self.issue_number,
            key_id: identity.key_id.clone(),
            public_key: identity.public_key.clone(),
        };

        let comments = self.github.issue_comments(self.issue_number).await?;
        let already_published = comments.iter().any(|comment| {
            comment.user_id == OWNER_USER_ID
                && serde_json::from_str::<PublishedIdentity>(&comment.body)
                    .is_ok_and(|published| published == expected)
        });

        if !already_published {
            self.github
                .post_issue_comment(self.issue_number, &serde_json::to_string(&expected)?)
                .await?;
        }

        Ok(())
    }

    pub async fn process_pending(
        &self,
        expected_key_id: &str,
        agent_private_key: &[u8; 32],
    ) -> AgentResult<usize> {
        let mut comments = self.github.issue_comments(self.issue_number).await?;
        comments.sort_by_key(|comment| comment.id);

        let mut terminal_request_ids = HashSet::new();
        for comment in &comments {
            if comment.user_id != OWNER_USER_ID {
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
            if comment.user_id != OWNER_USER_ID {
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
                    self.github
                        .post_issue_comment(self.issue_number, &serde_json::to_string(&response)?)
                        .await?;
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
            repository_id: REPOSITORY_ID,
            owner_user_id: OWNER_USER_ID,
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
