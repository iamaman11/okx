use std::{
    env,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use okx_github::{
    GitHubClient, IssueCommentCursor, OWNER_USER_ID, REPOSITORY_ID,
};
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AgentOperation, AgentRequest, AgentResponse,
    HOST_CONTROL_REQUEST_SCHEMA_V1, HostControlOperation, HostControlRequest,
    HostControlResult, InstrumentTypeFilter, MAILBOX_ENVELOPE_SCHEMA_V1,
    MailboxDirection, MailboxEnvelope,
    crypto::{
        decrypt, derive_directional_key, encrypt, public_key_from_private,
        shared_secret,
    },
};
use rmcp::{
    ErrorData as McpError,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock},
    schemars,
    tool, tool_router,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

const DATA_ISSUE: u64 = 10;
const CONTROL_ISSUE: u64 = 12;
const IDENTITY_SCHEMA_V1: &str = "okx.github-mailbox.identity/v1";
const GITHUB_TOKEN_ENV: &str = "OKX_BRIDGE_GITHUB_TOKEN";
const WAIT_BUDGET: Duration = Duration::from_secs(20);
const WAIT_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error("missing OKX bridge GitHub credential")]
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
}

#[derive(Clone)]
pub struct Bridge {
    github_token: Zeroizing<String>,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct FindInstrumentsParams {
    /// Base asset code, for example ADA.
    pub asset: String,
    /// Optional settlement currency, for example USDT or USD.
    #[serde(default)]
    pub settle_currency: Option<String>,
    /// Optional derivative type: SWAP or FUTURES.
    #[serde(default)]
    pub instrument_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
pub struct MarketOverviewParams {
    /// Exact OKX derivative instrument id, for example ADA-USDT-SWAP.
    pub instrument: String,
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

#[derive(Debug, Serialize)]
struct ToolEnvelope<T: Serialize> {
    transport: &'static str,
    result: T,
}

impl Bridge {
    pub fn from_env() -> Result<Self, BridgeError> {
        let token = env::var(GITHUB_TOKEN_ENV)
            .map_err(|_| BridgeError::MissingGithubCredential)?;
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
            "iamaman11-okx-bridge/0.1",
        )?)
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
        getrandom::fill(&mut nonce)
            .map_err(|error| BridgeError::Random(error.to_string()))?;

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
            operation: HostControlOperation::Status,
        };
        request.validate()?;
        github
            .post_issue_comment(CONTROL_ISSUE, &serde_json::to_string(&request)?)
            .await?;

        wait_for_control_result(&github, cursor, &request.request_id).await
    }

    async fn tool_data(&self, operation: AgentOperation) -> Result<CallToolResult, McpError> {
        match self.data_request(operation).await {
            Ok(response) => tool_success(&response),
            Err(error) => Ok(CallToolResult::error(vec![ContentBlock::text(
                error.to_string(),
            )])),
        }
    }

    async fn tool_control_status(&self) -> Result<CallToolResult, McpError> {
        match self.control_status_request().await {
            Ok(result) => tool_success(&result),
            Err(error) => Ok(CallToolResult::error(vec![ContentBlock::text(
                error.to_string(),
            )])),
        }
    }
}

#[tool_router(server_handler)]
impl Bridge {
    #[tool(description = "Find current OKX ADA/crypto derivative instruments through the encrypted Windows DATA channel. Returns bounded decrypted typed JSON only.")]
    pub async fn find_instruments(
        &self,
        Parameters(params): Parameters<FindInstrumentsParams>,
    ) -> Result<CallToolResult, McpError> {
        let instrument_type = match params.instrument_type.as_deref() {
            None => None,
            Some("SWAP") => Some(InstrumentTypeFilter::Swap),
            Some("FUTURES") => Some(InstrumentTypeFilter::Futures),
            Some(_) => {
                return Err(McpError::invalid_params(
                    "instrument_type must be SWAP or FUTURES",
                    None,
                ));
            }
        };

        self.tool_data(AgentOperation::FindInstruments {
            asset: params.asset,
            settle_currency: params.settle_currency,
            instrument_type,
        })
        .await
    }

    #[tool(description = "Read the current OKX market overview, rules and price state for one exact derivative instrument through the encrypted Windows DATA channel.")]
    pub async fn market_overview(
        &self,
        Parameters(params): Parameters<MarketOverviewParams>,
    ) -> Result<CallToolResult, McpError> {
        self.tool_data(AgentOperation::MarketOverview {
            instrument: params.instrument,
        })
        .await
    }

    #[tool(description = "Read the current Windows OKX controller/agent status through the CONTROL channel. No product mutation is performed.")]
    pub async fn control_status(&self) -> Result<CallToolResult, McpError> {
        self.tool_control_status().await
    }
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
            let plaintext = decrypt(
                &key,
                &nonce,
                envelope.aad()?.as_bytes(),
                &ciphertext,
            )?;
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

fn tool_success<T: Serialize>(value: &T) -> Result<CallToolResult, McpError> {
    let body = serde_json::to_string(&ToolEnvelope {
        transport: "hidden",
        result: value,
    })
    .map_err(|error| McpError::internal_error(error.to_string(), None))?;
    Ok(CallToolResult::success(vec![ContentBlock::text(body)]))
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
    fn tool_surface_does_not_expose_transport_fields() {
        let source = include_str!("lib.rs");
        for forbidden in [
            "client_private_key:",
            "github_token:",
            "ciphertext: String",
        ] {
            assert!(
                !source.contains(&format!("pub {forbidden}")),
                "public tool surface leaked {forbidden}"
            );
        }
    }

    #[test]
    fn identity_contract_is_pinned() {
        assert_eq!(IDENTITY_SCHEMA_V1, "okx.github-mailbox.identity/v1");
        assert_eq!(DATA_ISSUE, 10);
        assert_eq!(CONTROL_ISSUE, 12);
    }
}
