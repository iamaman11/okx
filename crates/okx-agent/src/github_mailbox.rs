use std::{collections::HashSet, path::Path};

use okx_github::{
    GitHubClient, GitHubError, IssueComment, IssueCommentCursor, IssueCursorStore, OWNER_USER_ID,
    REPOSITORY_ID,
};
use okx_protocol::{MailboxDirection, MailboxEnvelope};
use okx_runtime::PublicWsHandle;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    AgentError, AgentResult,
    identity::AgentIdentity,
    market_bootstrap::MarketBootstrapper,
    once::{ObservationQueryContext, process_once_now},
};

pub const GITHUB_MAILBOX_IDENTITY_SCHEMA_V1: &str = "okx.github-mailbox.identity/v1";

pub struct GitHubMailboxClient {
    github: GitHubClient,
    issue_number: u64,
    cursor_store: IssueCursorStore,
}

impl GitHubMailboxClient {
    pub fn new(
        issue_number: u64,
        token: Zeroizing<String>,
        state_root: &Path,
    ) -> AgentResult<Self> {
        if issue_number == 0 {
            return Err(AgentError::InvalidMailboxIssue);
        }

        let cursor_path = state_root
            .join("github-mailbox")
            .join(format!("issue-{issue_number}-cursor.json"));

        Ok(Self {
            github: GitHubClient::new(token, "iamaman11-okx-agent/0.1")?,
            issue_number,
            cursor_store: IssueCursorStore::new(cursor_path, issue_number)?,
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

        let comments = self.github.recent_issue_comments(self.issue_number).await?;
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
        public_ws: &PublicWsHandle,
        market: &MarketBootstrapper,
    ) -> AgentResult<usize> {
        let cursor = self.load_cursor_for_poll()?;
        let comments = self
            .github
            .issue_comments_after(self.issue_number, cursor.as_ref())
            .await?;
        if comments.is_empty() {
            return Ok(0);
        }

        let mut terminal_request_ids = terminal_request_ids(&comments);
        let mut processed = 0usize;
        let mut batch_complete = true;

        for comment in &comments {
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

            match process_once_now(
                &envelope,
                expected_key_id,
                agent_private_key,
                ObservationQueryContext::live(public_ws, market),
            )
            .await
            {
                Ok(response) => {
                    self.github
                        .post_issue_comment(self.issue_number, &serde_json::to_string(&response)?)
                        .await?;
                    terminal_request_ids.insert(envelope.request_id);
                    processed += 1;
                }
                Err(error) => {
                    batch_complete = false;
                    eprintln!(
                        "mailbox request {} rejected before terminal response: {}",
                        envelope.request_id, error
                    );
                }
            }
        }

        if let Some(cursor) = completed_batch_cursor(&comments, batch_complete) {
            self.cursor_store.save(&cursor)?;
        }

        Ok(processed)
    }

    fn load_cursor_for_poll(&self) -> AgentResult<Option<IssueCommentCursor>> {
        match self.cursor_store.load() {
            Ok(cursor) => Ok(cursor),
            Err(GitHubError::CursorJson(error)) => {
                eprintln!(
                    "mailbox cursor JSON invalid at {}: {}; falling back to bounded bootstrap scan",
                    self.cursor_store.path().display(),
                    error
                );
                Ok(None)
            }
            Err(GitHubError::CursorStateMismatch) => {
                eprintln!(
                    "mailbox cursor state mismatch at {}; falling back to bounded bootstrap scan",
                    self.cursor_store.path().display()
                );
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn terminal_request_ids(comments: &[IssueComment]) -> HashSet<String> {
    comments
        .iter()
        .filter(|comment| comment.user_id == OWNER_USER_ID)
        .filter_map(|comment| serde_json::from_str::<MailboxEnvelope>(&comment.body).ok())
        .filter(|envelope| envelope.direction == MailboxDirection::AgentToClient)
        .map(|envelope| envelope.request_id)
        .collect()
}

fn completed_batch_cursor(
    comments: &[IssueComment],
    batch_complete: bool,
) -> Option<IssueCommentCursor> {
    batch_complete
        .then(|| comments.last().map(IssueComment::cursor))
        .flatten()
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
    fn terminal_response_in_same_replayed_batch_prevents_duplicate_request() {
        let request_body = serde_json::json!({
            "schema": "okx.mailbox.envelope/v1",
            "request_id": "req-1",
            "direction": "client_to_agent",
            "agent_key_id": "agent-key-1",
            "client_ephemeral_public_key": "public",
            "nonce": "nonce",
            "ciphertext": "ciphertext"
        })
        .to_string();
        let response_body = serde_json::json!({
            "schema": "okx.mailbox.envelope/v1",
            "request_id": "req-1",
            "direction": "agent_to_client",
            "agent_key_id": "agent-key-1",
            "client_ephemeral_public_key": "public",
            "nonce": "nonce",
            "ciphertext": "ciphertext"
        })
        .to_string();
        let comments = vec![
            IssueComment {
                id: 10,
                body: request_body,
                user_id: OWNER_USER_ID,
                created_at: "2026-09-27T12:00:00Z".to_owned(),
            },
            IssueComment {
                id: 11,
                body: response_body,
                user_id: OWNER_USER_ID,
                created_at: "2026-09-27T12:00:01Z".to_owned(),
            },
        ];

        assert!(terminal_request_ids(&comments).contains("req-1"));
    }

    #[test]
    fn failed_batch_does_not_advance_cursor() {
        let comments = vec![IssueComment {
            id: 10,
            body: "{}".to_owned(),
            user_id: OWNER_USER_ID,
            created_at: "2026-09-27T12:00:00Z".to_owned(),
        }];

        assert_eq!(completed_batch_cursor(&comments, false), None);
        assert_eq!(
            completed_batch_cursor(&comments, true),
            Some(IssueCommentCursor {
                created_at: "2026-09-27T12:00:00Z".to_owned(),
                id: 10,
            })
        );
    }

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
