use std::{collections::BTreeSet, path::Path, time::Instant};

use chrono::{DateTime, Utc};
use okx_github::{
    GitHubClient, GitHubError, IssueCheckpoint, IssueComment, IssueCommentCursor, IssueCursorStore,
    IssuePollTelemetryStatus, IssuePollTelemetryStore, OWNER_USER_ID, REPOSITORY_ID,
};
use okx_protocol::{MailboxDirection, MailboxEnvelope};
use okx_runtime::{PrivateWsHandle, PublicWsHandle};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    identity::AgentIdentity,
    market_bootstrap::MarketBootstrapper,
    once::{ObservationQueryContext, process_once_now},
};

pub const GITHUB_MAILBOX_IDENTITY_SCHEMA_V1: &str = "okx.github-mailbox.identity/v1";

pub struct GitHubMailboxClient {
    github: GitHubClient,
    issue_number: u64,
    cursor_store: IssueCursorStore,
    telemetry_store: IssuePollTelemetryStore,
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

        let mailbox_root = state_root.join("github-mailbox");
        let cursor_path = mailbox_root.join(format!("issue-{issue_number}-cursor.json"));
        let telemetry_path = mailbox_root.join(format!("issue-{issue_number}-telemetry.json"));

        Ok(Self {
            github: GitHubClient::new(token, "iamaman11-okx-agent/0.1")?,
            issue_number,
            cursor_store: IssueCursorStore::new(cursor_path, issue_number)?,
            telemetry_store: IssuePollTelemetryStore::new(telemetry_path, issue_number)?,
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
        account: Option<&AccountBootstrapper>,
        private_ws: Option<&PrivateWsHandle>,
    ) -> AgentResult<usize> {
        let mut checkpoint = self.load_checkpoint_for_poll()?;
        let fetch_started = Instant::now();
        let comments = if checkpoint.ledger_initialized {
            self.github
                .issue_comments_after(self.issue_number, checkpoint.cursor.as_ref())
                .await?
        } else {
            let history = self.github.issue_comments(self.issue_number).await?;
            checkpoint
                .terminal_request_ids
                .extend(terminal_request_ids(&history));
            checkpoint.ledger_initialized = true;

            if checkpoint.cursor.is_none() {
                history
            } else {
                self.github
                    .issue_comments_after(self.issue_number, checkpoint.cursor.as_ref())
                    .await?
            }
        };
        let fetch_latency = fetch_started.elapsed();

        if comments.is_empty() {
            if let Some(cursor) = checkpoint.cursor.as_ref()
                && checkpoint.ledger_initialized
            {
                self.cursor_store.save_checkpoint(
                    cursor,
                    &checkpoint.terminal_request_ids,
                    true,
                )?;
            }
            self.record_poll_telemetry(fetch_latency, 0, checkpoint.cursor.as_ref(), None, None);
            return Ok(0);
        }

        let mut terminal_ids = checkpoint.terminal_request_ids.clone();
        terminal_ids.extend(terminal_request_ids(&comments));
        let mut processed = 0usize;
        let mut batch_complete = true;
        let mut last_terminal_request_id = last_terminal_request_id(&comments);
        let mut last_request_latency_ms = None;
        let mailbox_telemetry = self.telemetry_for_query();

        for comment in &comments {
            if comment.user_id != OWNER_USER_ID {
                continue;
            }

            let Ok(envelope) = serde_json::from_str::<MailboxEnvelope>(&comment.body) else {
                continue;
            };
            if envelope.direction != MailboxDirection::ClientToAgent
                || terminal_ids.contains(&envelope.request_id)
            {
                continue;
            }

            match process_once_now(
                &envelope,
                expected_key_id,
                agent_private_key,
                ObservationQueryContext::live_with_private(
                    public_ws,
                    market,
                    mailbox_telemetry.as_ref(),
                    account,
                    private_ws,
                ),
            )
            .await
            {
                Ok(response) => {
                    self.github
                        .post_issue_comment(self.issue_number, &serde_json::to_string(&response)?)
                        .await?;
                    last_terminal_request_id = Some(envelope.request_id.clone());
                    last_request_latency_ms = request_latency_ms(&comment.created_at);
                    terminal_ids.insert(envelope.request_id);
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

        let completed_cursor = completed_batch_cursor(&comments, batch_complete);
        if let Some(cursor) = completed_cursor.as_ref() {
            self.cursor_store
                .save_checkpoint(cursor, &terminal_ids, true)?;
        }
        let effective_cursor = completed_cursor.as_ref().or(checkpoint.cursor.as_ref());
        self.record_poll_telemetry(
            fetch_latency,
            comments.len(),
            effective_cursor,
            last_terminal_request_id.as_deref(),
            last_request_latency_ms,
        );

        Ok(processed)
    }

    fn telemetry_for_query(&self) -> Option<IssuePollTelemetryStatus> {
        match self.telemetry_store.status() {
            Ok(status) => status,
            Err(error) => {
                eprintln!("mailbox telemetry read failed: {error}");
                None
            }
        }
    }

    fn record_poll_telemetry(
        &self,
        fetch_latency: std::time::Duration,
        comments_scanned: usize,
        cursor: Option<&IssueCommentCursor>,
        last_terminal_request_id: Option<&str>,
        last_request_latency_ms: Option<u64>,
    ) {
        if let Err(error) = self.telemetry_store.record_success(
            fetch_latency,
            comments_scanned,
            cursor,
            last_terminal_request_id,
            last_request_latency_ms,
        ) {
            eprintln!("mailbox telemetry update failed: {error}");
        }
    }

    fn load_checkpoint_for_poll(&self) -> AgentResult<IssueCheckpoint> {
        match self.cursor_store.load_checkpoint() {
            Ok(checkpoint) => Ok(checkpoint),
            Err(GitHubError::CursorJson(error)) => {
                eprintln!(
                    "mailbox cursor JSON invalid at {}: {}; falling back to bounded bootstrap scan",
                    self.cursor_store.path().display(),
                    error
                );
                Ok(IssueCheckpoint::default())
            }
            Err(GitHubError::CursorStateMismatch) => {
                eprintln!(
                    "mailbox cursor state mismatch at {}; falling back to bounded bootstrap scan",
                    self.cursor_store.path().display()
                );
                Ok(IssueCheckpoint::default())
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn last_terminal_request_id(comments: &[IssueComment]) -> Option<String> {
    comments.iter().rev().find_map(|comment| {
        if comment.user_id != OWNER_USER_ID {
            return None;
        }
        let envelope = serde_json::from_str::<MailboxEnvelope>(&comment.body).ok()?;
        (envelope.direction == MailboxDirection::AgentToClient
            && envelope.validate(MailboxDirection::AgentToClient).is_ok())
        .then_some(envelope.request_id)
    })
}

fn request_latency_ms(created_at: &str) -> Option<u64> {
    let created_at = DateTime::parse_from_rfc3339(created_at).ok()?;
    let latency = Utc::now()
        .signed_duration_since(created_at)
        .num_milliseconds();
    Some(latency.max(0) as u64)
}

fn terminal_request_ids(comments: &[IssueComment]) -> BTreeSet<String> {
    comments
        .iter()
        .filter(|comment| comment.user_id == OWNER_USER_ID)
        .filter_map(|comment| serde_json::from_str::<MailboxEnvelope>(&comment.body).ok())
        .filter(|envelope| {
            envelope.direction == MailboxDirection::AgentToClient
                && envelope.validate(MailboxDirection::AgentToClient).is_ok()
        })
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
    fn persisted_terminal_ledger_rejects_late_duplicate_request_id() {
        let mut checkpoint = IssueCheckpoint::default();
        checkpoint
            .terminal_request_ids
            .insert("req_0123456789abcdef".to_owned());
        checkpoint.ledger_initialized = true;

        assert!(
            checkpoint
                .terminal_request_ids
                .contains("req_0123456789abcdef")
        );
    }

    #[test]
    fn terminal_response_in_same_replayed_batch_prevents_duplicate_request() {
        let request_body = serde_json::json!({
            "schema": "okx.mailbox.envelope/v1",
            "request_id": "req_0123456789abcdef",
            "direction": "client_to_agent",
            "agent_key_id": "agent-key-1",
            "client_ephemeral_public_key": "public",
            "nonce": "nonce",
            "ciphertext": "ciphertext"
        })
        .to_string();
        let response_body = serde_json::json!({
            "schema": "okx.mailbox.envelope/v1",
            "request_id": "req_0123456789abcdef",
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

        assert!(terminal_request_ids(&comments).contains("req_0123456789abcdef"));
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
    fn request_latency_is_non_negative_for_valid_timestamp() {
        assert!(request_latency_ms("2026-09-27T00:00:00Z").is_some());
        assert_eq!(request_latency_ms("not-a-timestamp"), None);
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
