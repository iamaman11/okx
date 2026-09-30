use std::{
    env,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use okx_github::{GitHubClient, IssueCommentCursor, OWNER_USER_ID, REPOSITORY_ID};
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponse,
    HOST_CONTROL_REQUEST_SCHEMA_V1, HostControlOperation, HostControlRequest, HostControlResult,
    InstrumentTypeFilter, MAILBOX_ENVELOPE_SCHEMA_V1, MailboxDirection, MailboxEnvelope,
    crypto::{
        decrypt, derive_directional_key, encrypt, public_key_from_private, shared_secret,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const DATA_ISSUE: u64 = 10;
const CONTROL_ISSUE: u64 = 12;
const IDENTITY_SCHEMA_V1: &str = "okx.github-mailbox.identity/v1";
const GITHUB_TOKEN_ENV: &str = "OKX_BRIDGE_GITHUB_TOKEN";
const WAIT_BUDGET: Duration = Duration::from_secs(20);
const WAIT_INTERVAL: Duration = Duration::from_secs(1);

pub const SERVER_NAME: &str = "okx-bridge";
pub const SERVER_VERSION: &str = "0.2.0";

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("missing or invalid OKX bridge GitHub credential")]
    MissingGithubCredential,
    #[error("GitHub transport failed: {0}")]
    Github(#[from] okx_github::GitHubError),
    #[error("protocol validation failed: {0}")]
    Protocol(#[from] okx_protocol::ProtocolError),
    #[error("JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("base64 failed: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("crypto failed: {0}")]
    Crypto(#[from] okx_protocol::crypto::CryptoError),
    #[error("agent mailbox identity was not found in the bounded tail")]
    AgentIdentityMissing,
    #[error("agent mailbox identity is invalid")]
    AgentIdentityInvalid,
    #[error("bounded transport wait expired")]
    Timeout,
    #[error("randomness source failed: {0}")]
    Random(String),
    #[error("unexpected terminal response")]
    UnexpectedResponse,
    #[error("unknown tool '{0}'")]
    UnknownTool(String),
    #[error("invalid tool arguments: {0}")]
    InvalidToolArguments(String),
}

#[derive(Clone)]
pub struct Bridge {
    github_token: Zeroizing<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FindInstrumentsParams {
    asset: String,
    #[serde(default)]
    settle_currency: Option<String>,
    #[serde(default)]
    instrument_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketOverviewParams {
    instrument: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PublishedIdentity {
    schema: String,
    repository_id: u64,
    owner_user_id: u64,
    issue_number: u64,
    key_id: String,
    public_key: String,
}

impl Bridge {
    pub fn from_env() -> Result<Self, BridgeError> {
        let token = env::var(GITHUB_TOKEN_ENV).map_err(|_| BridgeError::MissingGithubCredential)?;
        if token.is_empty()
            || token.len() > 1024
            || token
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(BridgeError::MissingGithubCredential);
        }
        Ok(Self {
            github_token: Zeroizing::new(token),
        })
    }

    fn github(&self) -> Result<GitHubClient, BridgeError> {
        Ok(GitHubClient::new(
            Zeroizing::new(self.github_token.as_str().to_owned()),
            "iamaman11-okx-bridge/0.2",
        )?)
    }

    pub async fn execute_tool(&self, name: &str, arguments: Value) -> Result<Value, BridgeError> {
        match name {
            "find_instruments" => {
                let params: FindInstrumentsParams = serde_json::from_value(arguments)
                    .map_err(|error| BridgeError::InvalidToolArguments(error.to_string()))?;
                let instrument_type = match params.instrument_type.as_deref() {
                    None => None,
                    Some("SWAP") => Some(InstrumentTypeFilter::Swap),
                    Some("FUTURES") => Some(InstrumentTypeFilter::Futures),
                    Some(value) => {
                        return Err(BridgeError::InvalidToolArguments(format!(
                            "instrument_type must be SWAP or FUTURES, got {value}"
                        )));
                    }
                };
                let response = self
                    .data_request(AgentOperation::FindInstruments {
                        asset: params.asset,
                        settle_currency: params.settle_currency,
                        instrument_type,
                    })
                    .await?;
                Ok(compact_agent_response(response))
            }
            "market_overview" => {
                let params: MarketOverviewParams = serde_json::from_value(arguments)
                    .map_err(|error| BridgeError::InvalidToolArguments(error.to_string()))?;
                let response = self
                    .data_request(AgentOperation::MarketOverview {
                        instrument: params.instrument,
                    })
                    .await?;
                Ok(compact_agent_response(response))
            }
            "control_status" => {
                if arguments
                    .as_object()
                    .is_some_and(|object| !object.is_empty())
                {
                    return Err(BridgeError::InvalidToolArguments(
                        "control_status accepts no arguments".to_owned(),
                    ));
                }
                let result = self.control_status_request().await?;
                Ok(json!({
                    "status": result.status,
                    "observed_at": result.observed_at,
                    "details": result.details,
                    "failure": result.failure
                }))
            }
            other => Err(BridgeError::UnknownTool(other.to_owned())),
        }
    }

    async fn verify(&self, github: &GitHubClient) -> Result<(), BridgeError> {
        github.verify_repository_identity().await?;
        Ok(())
    }

    async fn discover_agent_identity(
        &self,
        github: &GitHubClient,
    ) -> Result<(String, [u8; 32]), BridgeError> {
        let comments = github.recent_issue_comments(DATA_ISSUE).await?;
        let identity = comments
            .iter()
            .rev()
            .filter(|comment| comment.user_id == OWNER_USER_ID)
            .filter_map(|comment| serde_json::from_str::<PublishedIdentity>(&comment.body).ok())
            .find(|identity| {
                identity.schema == IDENTITY_SCHEMA_V1
                    && identity.repository_id == REPOSITORY_ID
                    && identity.owner_user_id == OWNER_USER_ID
                    && identity.issue_number == DATA_ISSUE
            })
            .ok_or(BridgeError::AgentIdentityMissing)?;

        okx_protocol::validate_agent_key_id(&identity.key_id)?;
        let public = STANDARD.decode(&identity.public_key)?;
        let public: [u8; 32] = public
            .try_into()
            .map_err(|_| BridgeError::AgentIdentityInvalid)?;
        Ok((identity.key_id, public))
    }

    async fn data_request(&self, operation: AgentOperation) -> Result<AgentResponse, BridgeError> {
        let github = self.github()?;
        self.verify(&github).await?;
        let (agent_key_id, agent_public_key) = self.discover_agent_identity(&github).await?;
        let cursor = tail_cursor(&github, DATA_ISSUE).await?;

        let request_id = request_id("req_bridge")?;
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: request_id.clone(),
            operation,
        };
        request.validate()?;

        let mut client_private = [0_u8; 32];
        getrandom::fill(&mut client_private)
            .map_err(|error| BridgeError::Random(error.to_string()))?;
        let client_public = public_key_from_private(client_private);
        let shared = shared_secret(client_private, agent_public_key)?;
        let key = derive_directional_key(
            &shared,
            &request_id,
            &agent_key_id,
            MailboxDirection::ClientToAgent,
        )?;
        let mut nonce = [0_u8; 12];
        getrandom::fill(&mut nonce).map_err(|error| BridgeError::Random(error.to_string()))?;

        let mut envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id.clone(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: agent_key_id.clone(),
            client_ephemeral_public_key: STANDARD.encode(client_public),
            nonce: STANDARD.encode(nonce),
            ciphertext: String::new(),
        };
        let aad = envelope.aad()?;
        envelope.ciphertext = STANDARD.encode(encrypt(
            &key,
            &nonce,
            aad.as_bytes(),
            &serde_json::to_vec(&request)?,
        )?);

        github
            .post_issue_comment(DATA_ISSUE, &serde_json::to_string(&envelope)?)
            .await?;

        let terminal = wait_for_mailbox_response(
            &github,
            cursor,
            &request_id,
            &agent_key_id,
            agent_public_key,
            client_private,
        )
        .await;
        client_private.zeroize();
        terminal
    }

    async fn control_status_request(&self) -> Result<HostControlResult, BridgeError> {
        let github = self.github()?;
        self.verify(&github).await?;
        let cursor = tail_cursor(&github, CONTROL_ISSUE).await?;

        let request = HostControlRequest {
            schema: HOST_CONTROL_REQUEST_SCHEMA_V1.to_owned(),
            request_id: request_id("ctl_bridge")?,
            operation: HostControlOperation::TransportStatus,
        };
        request.validate()?;
        github
            .post_issue_comment(CONTROL_ISSUE, &serde_json::to_string(&request)?)
            .await?;

        wait_for_control_result(&github, cursor, &request.request_id).await
    }
}

pub fn mcp_initialize_result() -> Value {
    json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        },
        "serverInfo": {
            "name": SERVER_NAME,
            "version": SERVER_VERSION
        }
    })
}

pub fn mcp_tools() -> Value {
    json!([
        {
            "name": "find_instruments",
            "title": "Find OKX instruments",
            "description": "Find current OKX derivative instruments through the encrypted Windows DATA channel. Returns bounded typed data only; transport internals stay hidden.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "asset": {"type": "string", "description": "Uppercase asset code, e.g. ADA"},
                    "settle_currency": {"type": ["string", "null"], "description": "Optional settlement currency, e.g. USDT or USD"},
                    "instrument_type": {"type": ["string", "null"], "enum": ["SWAP", "FUTURES", null]}
                },
                "required": ["asset"],
                "additionalProperties": false
            },
            "annotations": {"readOnlyHint": true}
        },
        {
            "name": "market_overview",
            "title": "Read OKX market overview",
            "description": "Read current rules, prices and market state for one exact OKX derivative instrument through the encrypted Windows DATA channel.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "instrument": {"type": "string", "description": "Exact OKX instrument id, e.g. ADA-USDT-SWAP"}
                },
                "required": ["instrument"],
                "additionalProperties": false
            },
            "annotations": {"readOnlyHint": true}
        },
        {
            "name": "control_status",
            "title": "Read OKX Windows runtime status",
            "description": "Read the current controller, launcher and agent transport status. Performs no product or trading mutation.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            },
            "annotations": {"readOnlyHint": true}
        }
    ])
}

fn compact_agent_response(response: AgentResponse) -> Value {
    json!({
        "status": response.status,
        "generated_at": response.generated_at,
        "quality": response.quality,
        "result_schema": response.result_schema,
        "result": response.result,
        "failure": response.failure,
        "warnings": response.warnings
    })
}

async fn tail_cursor(
    github: &GitHubClient,
    issue_number: u64,
) -> Result<Option<IssueCommentCursor>, BridgeError> {
    Ok(github
        .recent_issue_comments(issue_number)
        .await?
        .last()
        .map(|comment| comment.cursor()))
}

async fn wait_for_mailbox_response(
    github: &GitHubClient,
    mut cursor: Option<IssueCommentCursor>,
    request_id: &str,
    agent_key_id: &str,
    agent_public_key: [u8; 32],
    client_private_key: [u8; 32],
) -> Result<AgentResponse, BridgeError> {
    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        let comments = github.issue_comments_after(DATA_ISSUE, cursor.as_ref()).await?;
        for comment in &comments {
            if comment.user_id != OWNER_USER_ID {
                continue;
            }
            let Ok(envelope) = serde_json::from_str::<MailboxEnvelope>(&comment.body) else {
                continue;
            };
            if envelope.request_id != request_id
                || envelope.direction != MailboxDirection::AgentToClient
                || envelope.agent_key_id != agent_key_id
            {
                continue;
            }

            envelope.validate(MailboxDirection::AgentToClient)?;
            let nonce: [u8; 12] = STANDARD
                .decode(&envelope.nonce)?
                .try_into()
                .map_err(|_| BridgeError::UnexpectedResponse)?;
            let ciphertext = STANDARD.decode(&envelope.ciphertext)?;
            let shared = shared_secret(client_private_key, agent_public_key)?;
            let key = derive_directional_key(
                &shared,
                request_id,
                agent_key_id,
                MailboxDirection::AgentToClient,
            )?;
            let plaintext = decrypt(&key, &nonce, envelope.aad()?.as_bytes(), &ciphertext)?;
            let response: AgentResponse = serde_json::from_slice(&plaintext)?;
            response.validate()?;
            if response.request_id != request_id {
                return Err(BridgeError::UnexpectedResponse);
            }
            return Ok(response);
        }
        if let Some(last) = comments.last() {
            cursor = Some(last.cursor());
        }

        if Instant::now() >= deadline {
            return Err(BridgeError::Timeout);
        }
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

async fn wait_for_control_result(
    github: &GitHubClient,
    mut cursor: Option<IssueCommentCursor>,
    request_id: &str,
) -> Result<HostControlResult, BridgeError> {
    let deadline = Instant::now() + WAIT_BUDGET;
    loop {
        let comments = github.issue_comments_after(CONTROL_ISSUE, cursor.as_ref()).await?;
        for comment in &comments {
            if comment.user_id != OWNER_USER_ID {
                continue;
            }
            let Ok(result) = serde_json::from_str::<HostControlResult>(&comment.body) else {
                continue;
            };
            if result.request_id == request_id {
                result.validate()?;
                return Ok(result);
            }
        }
        if let Some(last) = comments.last() {
            cursor = Some(last.cursor());
        }

        if Instant::now() >= deadline {
            return Err(BridgeError::Timeout);
        }
        tokio::time::sleep(WAIT_INTERVAL).await;
    }
}

fn request_id(prefix: &str) -> Result<String, BridgeError> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random).map_err(|error| BridgeError::Random(error.to_string()))?;
    let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!("{prefix}_{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_ids_are_typed_and_bounded() {
        let data = request_id("req_bridge").expect("request id");
        let control = request_id("ctl_bridge").expect("request id");
        assert!(data.starts_with("req_bridge_"));
        assert!(control.starts_with("ctl_bridge_"));
        assert!((16..=128).contains(&data.len()));
        assert!((16..=128).contains(&control.len()));
    }

    #[test]
    fn tool_surface_is_read_only_and_bounded() {
        let tools = mcp_tools();
        let tools = tools.as_array().expect("tools");
        assert_eq!(tools.len(), 3);
        assert!(tools.iter().all(|tool| {
            tool.pointer("/annotations/readOnlyHint")
                .and_then(Value::as_bool)
                == Some(true)
        }));
    }

    #[test]
    fn tool_surface_does_not_publish_transport_secrets() {
        let serialized = serde_json::to_string(&mcp_tools()).expect("serialize");
        for forbidden in [
            "ciphertext",
            "nonce",
            "private_key",
            "github_token",
            "client_ephemeral_public_key",
        ] {
            assert!(!serialized.contains(forbidden));
        }
    }

    #[test]
    fn identity_contract_is_pinned() {
        assert_eq!(IDENTITY_SCHEMA_V1, "okx.github-mailbox.identity/v1");
        assert_eq!(DATA_ISSUE, 10);
        assert_eq!(CONTROL_ISSUE, 12);
    }
}
