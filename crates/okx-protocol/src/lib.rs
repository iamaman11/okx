pub mod account_catalog_proto;
pub mod account_governance;
pub mod accounts;
pub mod analytics;
pub mod crypto;
pub mod host_state_proto;
pub mod risk_authority;
pub mod trading;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAILBOX_ENVELOPE_SCHEMA_V1: &str = "okx.mailbox.envelope/v1";
pub const AGENT_REQUEST_SCHEMA_V1: &str = "okx.agent.request/v1";
pub const AGENT_RESPONSE_SCHEMA_V1: &str = "okx.agent.response/v1";
/// Authenticated by inherited parent/child pipe handles, NOT a remote API.
pub const NATIVE_CHILD_CONTROL_SCHEMA_V1: &str = "okx.native-child-control/v2";
pub const NATIVE_CHILD_CONTROL_PREFIX: &str = "@okx-child-control:";
pub const NATIVE_CHILD_CONTROL_MAX_BYTES: usize = 4096;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeChildOperation {
    Shutdown { drain_timeout_ms: u64 },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeChildRequest {
    pub schema: String,
    pub request_id: String,
    pub operation: NativeChildOperation,
}
impl NativeChildRequest {
    pub fn valid(&self) -> bool {
        self.schema == NATIVE_CHILD_CONTROL_SCHEMA_V1
            && self.request_id.len() == 64
            && self.request_id.bytes().all(|b| b.is_ascii_hexdigit())
            && matches!(
                self.operation,
                NativeChildOperation::Shutdown {
                    drain_timeout_ms: 10000
                }
            )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeChildReply {
    pub schema: String,
    pub request_id: String,
    pub outcome: String,
}
impl NativeChildReply {
    pub fn valid_for(&self, request_id: &str) -> bool {
        self.schema == NATIVE_CHILD_CONTROL_SCHEMA_V1
            && self.request_id == request_id
            && matches!(self.outcome.as_str(), "DRAINED" | "RECOVERY_REQUIRED")
    }
}

pub const HOST_CONTROL_REQUEST_SCHEMA_V1: &str = "okx.windows.control/v1";
pub const HOST_CONTROL_RESULT_SCHEMA_V1: &str = "okx.windows.control.result/v1";
pub const MAILBOX_REPOSITORY: &str = "iamaman11/okx";
pub const KDF_LABEL_CLIENT_TO_AGENT_V1: &str = "okx-mailbox-v1/client-to-agent";
pub const KDF_LABEL_AGENT_TO_CLIENT_V1: &str = "okx-mailbox-v1/agent-to-client";
pub const DIRECT_TRANSPORT_FRAME_SCHEMA_V2: &str = "okx.direct-transport.frame/v2";
pub const DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported schema '{0}'")]
    UnsupportedSchema(String),

    #[error("invalid request_id")]
    InvalidRequestId,

    #[error("invalid mutation_id")]
    InvalidMutationId,

    #[error("invalid agent key id")]
    InvalidAgentKeyId,

    #[error("invalid instrument")]
    InvalidInstrument,

    #[error("invalid asset code")]
    InvalidAssetCode,

    #[error("invalid history bar")]
    InvalidHistoryBar,

    #[error("invalid decimal input '{0}'")]
    InvalidDecimalInput(&'static str),

    #[error("invalid history limit")]
    InvalidHistoryLimit,

    #[error("market research requires 2..=8 unique instruments")]
    InvalidMarketResearchInstruments,

    #[error("invalid analytical query plan: {0}")]
    InvalidAnalyticalQuery(&'static str),

    #[error("invalid research request: {0}")]
    InvalidResearchRequest(&'static str),

    #[error("invalid authenticated governance or risk message")]
    InvalidGovernance,

    #[error("position scenario requires exactly one exit_price or entry_move_ratio")]
    InvalidPositionScenarioExit,

    #[error("mailbox direction mismatch")]
    DirectionMismatch,

    #[error("invalid host-control request_id")]
    InvalidHostControlRequestId,

    #[error("invalid Git source tree id")]
    InvalidSourceTree,

    #[error("invalid SHA-256")]
    InvalidSha256,

    #[error("invalid direct transport token")]
    InvalidDirectTransportToken,

    #[error("invalid connection generation")]
    InvalidConnectionGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MailboxDirection {
    ClientToAgent,
    AgentToClient,
}

impl MailboxDirection {
    pub const fn kdf_label(self) -> &'static str {
        match self {
            Self::ClientToAgent => KDF_LABEL_CLIENT_TO_AGENT_V1,
            Self::AgentToClient => KDF_LABEL_AGENT_TO_CLIENT_V1,
        }
    }

    const fn aad_direction(self) -> &'static str {
        match self {
            Self::ClientToAgent => "client_to_agent",
            Self::AgentToClient => "agent_to_client",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailboxEnvelope {
    pub schema: String,
    pub request_id: String,
    pub direction: MailboxDirection,
    pub agent_key_id: String,
    pub client_ephemeral_public_key: String,
    pub nonce: String,
    pub ciphertext: String,
}

impl MailboxEnvelope {
    pub fn validate(&self, expected_direction: MailboxDirection) -> Result<(), ProtocolError> {
        if self.schema != MAILBOX_ENVELOPE_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_request_id(&self.request_id)?;
        validate_agent_key_id(&self.agent_key_id)?;
        if self.direction != expected_direction {
            return Err(ProtocolError::DirectionMismatch);
        }
        Ok(())
    }

    pub fn aad(&self) -> Result<String, ProtocolError> {
        self.validate(self.direction)?;
        Ok(format!(
            "schema={};repo={};request_id={};direction={};agent_key_id={}",
            MAILBOX_ENVELOPE_SCHEMA_V1,
            MAILBOX_REPOSITORY,
            self.request_id,
            self.direction.aad_direction(),
            self.agent_key_id
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub schema: String,
    pub request_id: String,
    pub operation: AgentOperation,
}

impl AgentRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != AGENT_REQUEST_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_request_id(&self.request_id)?;
        if self
            .account
            .as_ref()
            .is_some_and(|a| a.is_empty() || a.len() > 128 || a.chars().any(char::is_control))
        {
            return Err(ProtocolError::InvalidRequestId);
        }
        self.operation.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InstrumentTypeFilter {
    Swap,
    Futures,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentOperation {
    ConnectAccount {
        credential_ref: String,
        alias: String,
        operation_id: String,
        request_digest: String,
    },
    Market {
        request: analytics::MarketRequest,
    },
    MarketCapabilities,
    MarketMicrostructure {
        instrument: String,
        size_contracts: Option<String>,
    },
    Research {
        request: analytics::ResearchRequest,
    },
    Trade {
        request: trading::TradeRequest,
    },
    Strategies {
        request: trading::StrategyRequest,
    },
    Portfolio {
        request: trading::PortfolioRequest,
    },
    PortfolioScenario {
        request: trading::PortfolioScenarioRequest,
    },
    Diagnostics {
        request: trading::DiagnosticsRequest,
    },
    FindInstruments {
        asset: String,
        settle_currency: Option<String>,
        instrument_type: Option<InstrumentTypeFilter>,
    },
    MarketOverview {
        instrument: String,
    },
    AccountSummary,
    MailboxTelemetry,
}
impl AgentOperation {
    pub fn action_class(&self) -> trading::ActionClass {
        use trading::{ActionClass as C, StrategyRequest as S, TradeRequest as T};
        match self {
            Self::Trade {
                request: T::Execute { operation, .. },
            } => match operation {
                trading::TradeOperation::Reduce | trading::TradeOperation::Close => C::ReduceRisk,
                _ => C::IncreaseRisk,
            },
            Self::Trade {
                request: T::Close { .. },
            } => C::ReduceRisk,
            Self::Trade {
                request: T::Cancel { .. },
            } => C::Maintenance,
            Self::Strategies {
                request: S::Create { .. } | S::Update { .. } | S::Start { .. } | S::Stop { .. },
            }
            | Self::ConnectAccount { .. } => C::Maintenance,
            _ => C::Read,
        }
    }
    pub fn requires_account(&self) -> bool {
        matches!(
            self,
            Self::Trade { .. } | Self::Strategies { .. } | Self::AccountSummary
        )
    }
    pub fn direct_transport_read_only(&self) -> bool {
        self.action_class() == trading::ActionClass::Read
    }
    pub fn validate(&self) -> Result<(), ProtocolError> {
        let valid = match self {
            Self::ConnectAccount {
                credential_ref,
                alias,
                operation_id,
                request_digest,
            } => {
                accounts::valid_reference(credential_ref)
                    && accounts::valid_reference(alias)
                    && validate_direct_token(operation_id, 8, 128).is_ok()
                    && request_digest.len() == 64
                    && request_digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            }
            Self::Market { request } => request.valid(),
            Self::MarketCapabilities => true,
            Self::MarketMicrostructure {
                instrument,
                size_contracts,
            } => {
                validate_instrument(instrument).is_ok()
                    && size_contracts.as_ref().is_none_or(|v| {
                        v.parse::<f64>()
                            .is_ok_and(|n| n.is_finite() && n > 0.0 && n <= 1_000_000.0)
                    })
            }
            Self::Research { request } => request.valid(),
            Self::Trade { request } => request.valid(),
            Self::Strategies { request } => request.valid(),
            Self::Portfolio { request } => request.valid(),
            Self::PortfolioScenario { request } => request.valid(),
            Self::Diagnostics { request } => request.valid(),
            Self::FindInstruments {
                asset,
                settle_currency,
                ..
            } => {
                validate_asset_code(asset).is_ok()
                    && settle_currency
                        .as_ref()
                        .is_none_or(|v| validate_asset_code(v).is_ok())
            }
            Self::MarketOverview { instrument } => validate_instrument(instrument).is_ok(),
            Self::AccountSummary | Self::MailboxTelemetry => true,
        };
        if valid {
            Ok(())
        } else {
            Err(ProtocolError::InvalidAnalyticalQuery(
                "invalid product request",
            ))
        }
    }
}

/// The authenticated one-owner Windows process profile, never an order-mutation grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectRuntimeProfile {
    Engine,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DirectTransportFrame {
    Hello {
        schema: String,
        runtime_id: String,
        connection_id: String,
        installation_id: String,
        contract: String,
        runtime_profile: DirectRuntimeProfile,
        agent_pid: u32,
        agent_binary_sha256: String,
        agent_incarnation: String,
    },
    HelloAck {
        schema: String,
        session_id: String,
        connection_generation: u64,
    },
    Ping {
        schema: String,
        session_id: String,
        connection_generation: u64,
        nonce: String,
    },
    Pong {
        schema: String,
        session_id: String,
        connection_generation: u64,
        nonce: String,
    },
    Request {
        schema: String,
        session_id: String,
        connection_generation: u64,
        request: AgentRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operator_governance: Option<account_governance::CentralGovernanceSnapshot>,
    },
    RiskControl {
        schema: String,
        session_id: String,
        connection_generation: u64,
        command: risk_authority::RiskCommand,
    },
    PolicySnapshot {
        schema: String,
        session_id: String,
        connection_generation: u64,
        snapshot: account_governance::CentralGovernanceSnapshot,
    },
    RiskControlResult {
        schema: String,
        session_id: String,
        connection_generation: u64,
        result: risk_authority::RiskResult,
    },
    DeliveryAck {
        schema: String,
        session_id: String,
        connection_generation: u64,
        request_id: String,
    },
    Response {
        schema: String,
        session_id: String,
        connection_generation: u64,
        response: AgentResponse,
    },
}

impl DirectTransportFrame {
    /// Validate the transport envelope before sending delivery ACK.
    /// Request *operation* validation belongs to the single Rust owner, which
    /// returns a typed rejection rather than tearing down the whole WS session.
    /// Malformed request identities and envelopes are still fatal.
    pub fn validate_incoming_envelope(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Request {
                schema,
                session_id,
                connection_generation,
                request,
                operator_governance: _,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                if request.schema != AGENT_REQUEST_SCHEMA_V1 {
                    return Err(ProtocolError::UnsupportedSchema(request.schema.clone()));
                }
                validate_request_id(&request.request_id)
            }
            _ => self.validate(),
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Hello {
                schema,
                runtime_id,
                connection_id,
                installation_id,
                contract,
                runtime_profile: _,
                agent_pid,
                agent_binary_sha256,
                agent_incarnation,
            } => {
                validate_direct_schema(schema)?;
                if *agent_pid == 0 {
                    return Err(ProtocolError::InvalidDirectTransportToken);
                }
                validate_sha256(agent_binary_sha256)?;
                if agent_incarnation.len() != 36 || !agent_incarnation.starts_with("ctl_") || !agent_incarnation[4..].bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
                    return Err(ProtocolError::InvalidDirectTransportToken);
                }
                if !accounts::valid_installation(installation_id)
                    || contract != accounts::PRODUCT_CONTRACT
                {
                    return Err(ProtocolError::InvalidDirectTransportToken);
                }
                validate_direct_token(runtime_id, 1, 64)?;
                validate_direct_token(connection_id, 16, 128)
            }
            Self::HelloAck {
                schema,
                session_id,
                connection_generation,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)
            }
            Self::Ping {
                schema,
                session_id,
                connection_generation,
                nonce,
            }
            | Self::Pong {
                schema,
                session_id,
                connection_generation,
                nonce,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                validate_direct_token(nonce, 16, 128)
            }
            Self::Request {
                schema,
                session_id,
                connection_generation,
                request,
                operator_governance,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                if operator_governance
                    .as_ref()
                    .is_some_and(|p| !p.valid() || p.session_generation != *connection_generation)
                {
                    return Err(ProtocolError::InvalidGovernance);
                }
                request.validate()
            }
            Self::PolicySnapshot {
                schema,
                session_id,
                connection_generation,
                snapshot,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                if snapshot.valid() && snapshot.session_generation == *connection_generation {
                    Ok(())
                } else {
                    Err(ProtocolError::InvalidGovernance)
                }
            }
            Self::RiskControl {
                schema,
                session_id,
                connection_generation,
                command,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                if command.valid() {
                    Ok(())
                } else {
                    Err(ProtocolError::InvalidGovernance)
                }
            }
            Self::RiskControlResult {
                schema,
                session_id,
                connection_generation,
                result,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                if result.valid() {
                    Ok(())
                } else {
                    Err(ProtocolError::InvalidGovernance)
                }
            }
            Self::DeliveryAck {
                schema,
                session_id,
                connection_generation,
                request_id,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                validate_request_id(request_id)
            }
            Self::Response {
                schema,
                session_id,
                connection_generation,
                response,
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                response.validate()
            }
        }
    }

    pub fn schema(&self) -> &str {
        match self {
            Self::Hello { schema, .. }
            | Self::HelloAck { schema, .. }
            | Self::Ping { schema, .. }
            | Self::Pong { schema, .. }
            | Self::Request { schema, .. }
            | Self::PolicySnapshot { schema, .. }
            | Self::RiskControl { schema, .. }
            | Self::RiskControlResult { schema, .. }
            | Self::DeliveryAck { schema, .. }
            | Self::Response { schema, .. } => schema,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlRequest {
    pub schema: String,
    pub request_id: String,
    pub operation: HostControlOperation,
}

impl HostControlRequest {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != HOST_CONTROL_REQUEST_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_host_control_request_id(&self.request_id)?;
        self.operation.validate()
    }
}

pub const ACCOUNT_CATALOG_ENTRY_SCHEMA_V1: &str = "okx.account-catalog-entry/v1";

/// Exchange identity is separate from account environment (Demo/Real).
/// A venue is selectable only after its native adapter has been installed;
/// recognizing the identifier never grants an exchange credential or POST.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingVenue {
    #[default]
    Okx,
    Bybit,
}

impl TradingVenue {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Okx => "okx",
            Self::Bybit => "bybit",
        }
    }
}

/// CONTROL owns installation and process lifecycle. Access and limits belong to governance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostControlOperation {
    Status,
    AccountCatalog,
    Sync,
    BuildAgent,
    DeployAgent {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    InstallLauncherRoot {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    UpgradeLauncherRoot {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
        expected_current_launcher_sha256: String,
    },
    StageControllerUpdate {
        run_id: u64,
        artifact_id: u64,
        expected_source_tree: String,
    },
    HandoffControllerUpdate,
    AbortControllerUpdate,
    ControllerUpdateStatus,
    WorkspaceStatus,
    TestWorkspace,
    InitAgentIdentity,
    AgentIdentity,
    BootstrapAgentGithubToken,
    ProvisionCloudflareRuntimeToken,
    StartAgent,
    StopAgent,
    RestartAgent,
    InitializeInstallation {
        installation_id: String,
        expected_release: String,
        operation_id: String,
    },
    InitializeAccount {
        account_key: String,
        installation_id: String,
        expected_release: String,
        operation_id: String,
    },
    BackupAccount {
        account_key: String,
    },
    RestoreAccount {
        account_key: String,
        snapshot_id: String,
        expected_release: String,
        operation_id: String,
    },
    InstallAutostart,
    AutostartStatus,
    HandoffToAutostart,
    AcceptanceKillAgent,
    AcceptanceCrashController,
    AcceptanceFailNextControllerActivation,
    TransportStatus,
}
impl HostControlOperation {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::InitializeInstallation {
                installation_id,
                expected_release,
                operation_id,
            }
            | Self::InitializeAccount {
                installation_id,
                expected_release,
                operation_id,
                ..
            } => {
                if !accounts::valid_installation(installation_id)
                    || !(8..=128).contains(&operation_id.len())
                    || !operation_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                {
                    return Err(ProtocolError::InvalidRequestId);
                }
                validate_source_tree(expected_release)?;
                if let Self::InitializeAccount { account_key, .. } = self {
                    validate_account_wire(account_key)?;
                }
                Ok(())
            }
            Self::BackupAccount { account_key } => validate_account_wire(account_key),
            Self::RestoreAccount {
                account_key,
                snapshot_id,
                expected_release,
                operation_id,
            } => {
                validate_account_wire(account_key)?;
                validate_source_tree(expected_release)?;
                if snapshot_id.is_empty()
                    || snapshot_id.len() > 128
                    || !snapshot_id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    || !(8..=128).contains(&operation_id.len())
                {
                    Err(ProtocolError::InvalidRequestId)
                } else {
                    Ok(())
                }
            }
            Self::DeployAgent {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::InstallLauncherRoot {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::StageControllerUpdate {
                run_id,
                artifact_id,
                expected_source_tree,
            }
            | Self::UpgradeLauncherRoot {
                run_id,
                artifact_id,
                expected_source_tree,
                ..
            } => {
                if *run_id == 0 || *artifact_id == 0 {
                    return Err(ProtocolError::InvalidRequestId);
                }
                validate_source_tree(expected_source_tree)?;
                if let Self::UpgradeLauncherRoot {
                    expected_current_launcher_sha256,
                    ..
                } = self
                {
                    validate_sha256(expected_current_launcher_sha256)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
fn validate_account_wire(value: &str) -> Result<(), ProtocolError> {
    let p = value.split(':').collect::<Vec<_>>();
    if p.len() != 3 || p[0] != "okx" || !matches!(p[1], "real" | "demo") {
        return Err(ProtocolError::InvalidRequestId);
    }
    validate_sha256(p[2])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HostControlStatus {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostControlResult {
    pub schema: String,
    pub request_id: String,
    pub operation: HostControlOperation,
    pub status: HostControlStatus,
    pub observed_at: String,
    pub details: Option<serde_json::Value>,
    pub failure: Option<HostControlFailure>,
}

impl HostControlResult {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != HOST_CONTROL_RESULT_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_host_control_request_id(&self.request_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DataQuality {
    NotReady,
    Fresh,
    Stale,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentResponseStatus {
    Completed,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResponse {
    pub schema: String,
    pub request_id: String,
    pub status: AgentResponseStatus,
    pub generated_at: String,
    pub quality: DataQuality,
    pub result_schema: Option<String>,
    pub result: Option<serde_json::Value>,
    pub failure: Option<AgentFailure>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

impl AgentResponse {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.schema != AGENT_RESPONSE_SCHEMA_V1 {
            return Err(ProtocolError::UnsupportedSchema(self.schema.clone()));
        }
        validate_request_id(&self.request_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

fn validate_direct_schema(value: &str) -> Result<(), ProtocolError> {
    if value == DIRECT_TRANSPORT_FRAME_SCHEMA_V2 {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedSchema(value.to_owned()))
    }
}

fn validate_connection_generation(value: u64) -> Result<(), ProtocolError> {
    if value == 0 {
        Err(ProtocolError::InvalidConnectionGeneration)
    } else {
        Ok(())
    }
}

fn validate_direct_token(value: &str, min: usize, max: usize) -> Result<(), ProtocolError> {
    if (min..=max).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidDirectTransportToken)
    }
}

fn validate_request_id(value: &str) -> Result<(), ProtocolError> {
    if (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidRequestId)
    }
}

fn validate_sha256(value: &str) -> Result<(), ProtocolError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidSha256)
    }
}

fn validate_source_tree(value: &str) -> Result<(), ProtocolError> {
    if value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidSourceTree)
    }
}

fn validate_host_control_request_id(value: &str) -> Result<(), ProtocolError> {
    if value.starts_with("ctl_") && validate_request_id(value).is_ok() {
        Ok(())
    } else {
        Err(ProtocolError::InvalidHostControlRequestId)
    }
}

pub fn validate_agent_key_id(value: &str) -> Result<(), ProtocolError> {
    if (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidAgentKeyId)
    }
}

fn validate_asset_code(value: &str) -> Result<(), ProtocolError> {
    if (2..=16).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidAssetCode)
    }
}

fn validate_instrument(value: &str) -> Result<(), ProtocolError> {
    if (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        Ok(())
    } else {
        Err(ProtocolError::InvalidInstrument)
    }
}

#[cfg(test)]
mod product_contract_tests {
    use super::*;
    #[test]
    fn removed_period_and_activation_operations_are_rejected() {
        for operation in [
            "begin_strategy_run",
            "finalize_strategy_run",
            "prepare_execution",
            "submit_prepared_execution",
        ] {
            let value = serde_json::json!({"type":operation,"intent_id":"intent_12345678"});
            assert!(serde_json::from_value::<AgentOperation>(value).is_err());
        }
    }
    #[test]
    fn worker_universe_only_request_decodes_through_native_agent_envelope() {
        let body = serde_json::json!({
            "schema": AGENT_REQUEST_SCHEMA_V1,
            "request_id": "a".repeat(64),
            "operation": {
                "type": "market",
                "request": {
                    "action": "screen",
                    "universe": { "instrument_type": "SWAP", "limit": 50 },
                    "fields": ["return_pct"],
                    "timeframe_ms": 60000,
                    "start_ms": 60000,
                    "end_ms": 180000,
                    "limit": 50
                }
            }
        });
        let decoded: AgentRequest = serde_json::from_value(body.clone()).unwrap();
        assert!(decoded.validate().is_ok());
        let AgentOperation::Market { request } = decoded.operation else {
            panic!("wrong action")
        };
        assert!(request.instruments.is_empty());
        assert!(request.universe.is_some());
        let mut ambiguous = body;
        ambiguous["operation"]["request"]["instruments"] = serde_json::json!(["BTC-USDT-SWAP"]);
        let parsed: AgentRequest = serde_json::from_value(ambiguous).unwrap();
        assert!(parsed.validate().is_err());
    }
    #[test]
    fn account_reference_is_preserved_in_transport() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "request_12345678".into(),
            account: Some("main-real".into()),
            operation: AgentOperation::AccountSummary,
        };
        let decoded: AgentRequest =
            serde_json::from_slice(&serde_json::to_vec(&request).unwrap()).unwrap();
        assert_eq!(decoded, request);
        assert!(decoded.validate().is_ok());
        let mut bad = serde_json::to_value(request).unwrap();
        bad["selected_profile"] = serde_json::json!("demo");
        assert!(serde_json::from_value::<AgentRequest>(bad).is_err());
    }
    #[test]
    fn account_connect_is_maintenance_and_body_bounded() {
        let command = AgentOperation::ConnectAccount {
            alias: "main-real".into(),
            credential_ref: "main-keys".into(),
            operation_id: "connect_command_001".into(),
            request_digest: "a".repeat(64),
        };
        assert!(command.validate().is_ok());
        assert!(!command.direct_transport_read_only());
        let invalid = AgentOperation::ConnectAccount {
            alias: "../escape".into(),
            credential_ref: "main-keys".into(),
            operation_id: "connect_command_001".into(),
            request_digest: "a".repeat(64),
        };
        assert!(invalid.validate().is_err());
    }

    /// Validates exact policy_snapshot frames emitted by the real Durable
    /// Object CAS route, before the native SQL account owner reads them.
    /// Not a physical WebSocket delivery or exchange approval.
    #[test]
    #[ignore = "requires OKX_A2_TRACE_PATH from composed Worker acceptance"]
    fn worker_do_policy_frames_decode_and_fail_closed_in_native_protocol() {
        let path = std::env::var("OKX_A2_TRACE_PATH")
            .expect("run actual Worker composition before this protocol check");
        let trace: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(trace["schema"], "okx.a2.worker-sql-cas-trace/v1");
        let events = trace["events"].as_array().unwrap();
        assert_eq!(events.len(), 8);
        let expected = [
            ("connect", 1_u64),
            ("pause", 2),
            ("stale_resume_rejected", 2),
            ("resume", 3),
            ("set_limits", 4),
            ("stale_limits_rejected", 4),
            ("set_limits_replay", 4),
            ("final", 4),
        ];
        for (event, (stage, revision)) in events.iter().zip(expected) {
            assert_eq!(event["stage"].as_str(), Some(stage));
            let raw = &event["frame"];
            assert_eq!(raw["type"], "policy_snapshot");
            assert_eq!(raw["schema"], DIRECT_TRANSPORT_FRAME_SCHEMA_V2);
            assert_eq!(
                raw["snapshot"]["state"]["revision"].as_u64(),
                Some(revision)
            );
            let frame: DirectTransportFrame = serde_json::from_value(raw.clone()).unwrap();
            frame.validate_incoming_envelope().unwrap();

            let mut wrong_generation = raw.clone();
            wrong_generation["connection_generation"] = serde_json::json!(5);
            let wrong_frame: DirectTransportFrame =
                serde_json::from_value(wrong_generation).unwrap();
            assert!(wrong_frame.validate_incoming_envelope().is_err());

            let mut wrong_account_key = raw.clone();
            wrong_account_key["snapshot"]["state"]["accounts"][0]["account_key"] =
                serde_json::json!(format!("okx:demo:{}", "b".repeat(64)));
            let wrong_frame: DirectTransportFrame =
                serde_json::from_value(wrong_account_key).unwrap();
            assert!(wrong_frame.validate_incoming_envelope().is_err());

            let mut unknown = raw.clone();
            unknown["snapshot"]["unexpected_policy_owner"] = serde_json::json!(true);
            assert!(serde_json::from_value::<DirectTransportFrame>(unknown).is_err());
        }
        println!(
            "a2_native_typed_policy_wss PASS 8 actual DO frames, generation and UID binding negatives; no physical network claim"
        );
    }
}
