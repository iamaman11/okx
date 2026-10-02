pub mod crypto;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAILBOX_ENVELOPE_SCHEMA_V1: &str = "okx.mailbox.envelope/v1";
pub const AGENT_REQUEST_SCHEMA_V1: &str = "okx.agent.request/v1";
pub const AGENT_RESPONSE_SCHEMA_V1: &str = "okx.agent.response/v1";
pub const HOST_CONTROL_REQUEST_SCHEMA_V1: &str = "okx.windows.control/v1";
pub const HOST_CONTROL_RESULT_SCHEMA_V1: &str = "okx.windows.control.result/v1";
pub const MAILBOX_REPOSITORY: &str = "iamaman11/okx";
pub const KDF_LABEL_CLIENT_TO_AGENT_V1: &str = "okx-mailbox-v1/client-to-agent";
pub const KDF_LABEL_AGENT_TO_CLIENT_V1: &str = "okx-mailbox-v1/agent-to-client";
pub const DIRECT_TRANSPORT_FRAME_SCHEMA_V1: &str = "okx.direct-transport.frame/v1";
pub const DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES: usize = 64 * 1024;
pub const ANALYTICAL_QUERY_CATALOG_VERSION_V1: &str = "okx.query.catalog/2026-10-02.2";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported schema '{0}'")]
    UnsupportedSchema(String),

    #[error("invalid request_id")]
    InvalidRequestId,

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRequest {
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
        self.operation.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InstrumentTypeFilter {
    Swap,
    Futures,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryField {
    InstrumentId,
    InstrumentType,
    SettleCurrency,
    State,
    Last,
    BestBid,
    BestAsk,
    #[serde(rename = "open_24h")]
    Open24h,
    #[serde(rename = "volume_24h")]
    Volume24h,
    #[serde(rename = "volume_currency_24h")]
    VolumeCurrency24h,
    ExchangeTimestampMs,
    #[serde(rename = "return_24h_pct")]
    Return24hPct,
    SpreadBps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryMetric {
    #[serde(rename = "return_24h_pct")]
    Return24hPct,
    SpreadBps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuerySortKey {
    #[serde(rename = "return_24h_pct")]
    Return24hPct,
    SpreadBps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuerySortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryInstrumentState {
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySort {
    pub key: QuerySortKey,
    pub direction: QuerySortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketQueryUniverse {
    pub instrument_types: Vec<InstrumentTypeFilter>,
    pub settle_currency: Option<String>,
    pub state: Option<QueryInstrumentState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticalQueryPlan {
    pub catalog_version: String,
    pub universe: MarketQueryUniverse,
    pub select: Vec<QueryField>,
    pub metric: Option<QueryMetric>,
    pub sort: Option<QuerySort>,
    pub limit: u16,
}

impl AnalyticalQueryPlan {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.catalog_version.is_empty()
            || self.catalog_version.len() > 64
            || self.catalog_version.bytes().any(|byte| {
                !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'/' | b'-' | b'_'))
            })
        {
            return Err(ProtocolError::InvalidAnalyticalQuery("catalog_version"));
        }
        if self.universe.instrument_types.is_empty()
            || self.universe.instrument_types.len() > 2
            || (self.universe.instrument_types.len() == 2
                && self.universe.instrument_types[0] == self.universe.instrument_types[1])
        {
            return Err(ProtocolError::InvalidAnalyticalQuery("instrument_types"));
        }
        if let Some(settle) = self.universe.settle_currency.as_deref() {
            validate_asset_code(settle)?;
        }
        if self.select.is_empty() || self.select.len() > 10 {
            return Err(ProtocolError::InvalidAnalyticalQuery("select"));
        }
        for (index, field) in self.select.iter().enumerate() {
            if self.select[..index].contains(field) {
                return Err(ProtocolError::InvalidAnalyticalQuery(
                    "duplicate select field",
                ));
            }
        }
        if !(1..=25).contains(&self.limit) {
            return Err(ProtocolError::InvalidAnalyticalQuery("limit"));
        }
        let selected_metric = match (
            self.select.contains(&QueryField::Return24hPct),
            self.select.contains(&QueryField::SpreadBps),
        ) {
            (true, true) => {
                return Err(ProtocolError::InvalidAnalyticalQuery(
                    "one derived metric per query plan",
                ));
            }
            (true, false) => Some(QueryMetric::Return24hPct),
            (false, true) => Some(QueryMetric::SpreadBps),
            (false, false) => None,
        };
        let sort_metric = self.sort.as_ref().map(|sort| match sort.key {
            QuerySortKey::Return24hPct => QueryMetric::Return24hPct,
            QuerySortKey::SpreadBps => QueryMetric::SpreadBps,
        });
        let required_metric = match (selected_metric, sort_metric) {
            (Some(selected), Some(sorted)) if selected != sorted => {
                return Err(ProtocolError::InvalidAnalyticalQuery(
                    "selected and sorted metrics must match",
                ));
            }
            (Some(metric), _) | (_, Some(metric)) => Some(metric),
            (None, None) => None,
        };
        if self.metric != required_metric {
            return Err(ProtocolError::InvalidAnalyticalQuery(
                "declared metric must exactly match the derived field/sort key",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTradeMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingMarginMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOrderType {
    Limit,
    PostOnly,
    Fok,
    Ioc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentOperation {
    MarketSnapshot {
        instrument: String,
    },
    InstrumentRules {
        instrument: String,
    },
    FindInstruments {
        asset: String,
        settle_currency: Option<String>,
        instrument_type: Option<InstrumentTypeFilter>,
    },
    MarketOverview {
        instrument: String,
    },
    MarketIntelligence {
        instrument: String,
        impact_contracts: String,
        depth_levels: u16,
    },
    MarketResearch {
        instruments: Vec<String>,
        bar: String,
        limit: Option<u16>,
    },
    QueryCapabilities,
    Query {
        plan: AnalyticalQueryPlan,
    },
    MarketHistory {
        instrument: String,
        bar: String,
        limit: Option<u16>,
    },
    HistoryBehavior {
        instrument: String,
        bar: String,
        limit: Option<u16>,
    },
    SnapshotQuality {
        instrument: String,
    },
    ExecutorPreflight,
    PrepareOpenExecution {
        intent_id: String,
        instrument: String,
        trade_mode: ExecutionTradeMode,
        position_side: PositionSide,
        order_type: ExecutionOrderType,
        entry_price: String,
        stop_price: String,
        max_settle_notional: String,
        max_loss_settle: String,
        target_rr: String,
        entry_liquidity_role: LiquidityRole,
        exit_liquidity_role: LiquidityRole,
    },
    PrepareCloseExecution {
        intent_id: String,
        instrument: String,
        trade_mode: ExecutionTradeMode,
        position_side: PositionSide,
        order_type: ExecutionOrderType,
        size: String,
        price: String,
    },
    SubmitPreparedExecution {
        intent_id: String,
    },
    ExecutionStatus {
        intent_id: String,
    },
    MailboxTelemetry,
    AccountSnapshot,
    AccountSummary,
    PortfolioRisk,
    TradingCapabilities {
        instrument: String,
        margin_mode: TradingMarginMode,
    },
    CurrentCost {
        instrument: String,
        contracts: String,
        side: PositionSide,
        liquidity_role: LiquidityRole,
    },
    PositionScenario {
        instrument: String,
        side: PositionSide,
        contracts: String,
        entry_price: String,
        exit_price: Option<String>,
        entry_move_ratio: Option<String>,
        entry_liquidity_role: LiquidityRole,
        exit_liquidity_role: LiquidityRole,
    },
    AnalyzeCandidateOrder {
        instrument: String,
        side: PositionSide,
        entry_price: String,
        stop_price: String,
        max_settle_notional: String,
        max_loss_settle: String,
        target_rr: String,
        entry_liquidity_role: LiquidityRole,
        exit_liquidity_role: LiquidityRole,
    },
}

impl AgentOperation {
    pub const fn direct_transport_read_only(&self) -> bool {
        !matches!(
            self,
            Self::PrepareOpenExecution { .. }
                | Self::PrepareCloseExecution { .. }
                | Self::SubmitPreparedExecution { .. }
        )
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::MarketSnapshot { instrument }
            | Self::InstrumentRules { instrument }
            | Self::MarketOverview { instrument }
            | Self::SnapshotQuality { instrument } => validate_instrument(instrument),
            Self::MarketIntelligence {
                instrument,
                impact_contracts,
                depth_levels,
            } => {
                validate_instrument(instrument)?;
                validate_positive_decimal_text(impact_contracts, "impact_contracts")?;
                if !(1..=50).contains(depth_levels) {
                    return Err(ProtocolError::InvalidAnalyticalQuery("depth_levels"));
                }
                Ok(())
            }
            Self::FindInstruments {
                asset,
                settle_currency,
                ..
            } => {
                validate_asset_code(asset)?;
                if let Some(settle_currency) = settle_currency {
                    validate_asset_code(settle_currency)?;
                }
                Ok(())
            }
            Self::MarketResearch {
                instruments,
                bar,
                limit,
            } => validate_market_research(instruments, bar, *limit),
            Self::QueryCapabilities => Ok(()),
            Self::Query { plan } => plan.validate(),
            Self::MarketHistory {
                instrument,
                bar,
                limit,
            }
            | Self::HistoryBehavior {
                instrument,
                bar,
                limit,
            } => validate_history_request(instrument, bar, *limit),
            Self::ExecutorPreflight
            | Self::MailboxTelemetry
            | Self::AccountSnapshot
            | Self::AccountSummary
            | Self::PortfolioRisk => Ok(()),
            Self::TradingCapabilities { instrument, .. } => validate_instrument(instrument),
            Self::PrepareOpenExecution {
                intent_id,
                instrument,
                entry_price,
                stop_price,
                max_settle_notional,
                max_loss_settle,
                target_rr,
                ..
            } => {
                validate_request_id(intent_id)?;
                validate_instrument(instrument)?;
                validate_decimal_text(entry_price, "entry_price")?;
                validate_decimal_text(stop_price, "stop_price")?;
                validate_decimal_text(max_settle_notional, "max_settle_notional")?;
                validate_decimal_text(max_loss_settle, "max_loss_settle")?;
                validate_decimal_text(target_rr, "target_rr")
            }
            Self::PrepareCloseExecution {
                intent_id,
                instrument,
                size,
                price,
                ..
            } => {
                validate_request_id(intent_id)?;
                validate_instrument(instrument)?;
                validate_decimal_text(size, "size")?;
                validate_decimal_text(price, "price")
            }
            Self::SubmitPreparedExecution { intent_id } | Self::ExecutionStatus { intent_id } => {
                validate_request_id(intent_id)
            }
            Self::CurrentCost {
                instrument,
                contracts,
                ..
            } => {
                validate_instrument(instrument)?;
                validate_decimal_text(contracts, "contracts")
            }
            Self::PositionScenario {
                instrument,
                contracts,
                entry_price,
                exit_price,
                entry_move_ratio,
                ..
            } => {
                validate_instrument(instrument)?;
                validate_decimal_text(contracts, "contracts")?;
                validate_decimal_text(entry_price, "entry_price")?;
                match (exit_price, entry_move_ratio) {
                    (Some(exit_price), None) => validate_decimal_text(exit_price, "exit_price"),
                    (None, Some(entry_move_ratio)) => {
                        validate_decimal_text(entry_move_ratio, "entry_move_ratio")
                    }
                    _ => Err(ProtocolError::InvalidPositionScenarioExit),
                }
            }
            Self::AnalyzeCandidateOrder {
                instrument,
                entry_price,
                stop_price,
                max_settle_notional,
                max_loss_settle,
                target_rr,
                ..
            } => {
                validate_instrument(instrument)?;
                validate_decimal_text(entry_price, "entry_price")?;
                validate_decimal_text(stop_price, "stop_price")?;
                validate_decimal_text(max_settle_notional, "max_settle_notional")?;
                validate_decimal_text(max_loss_settle, "max_loss_settle")?;
                validate_decimal_text(target_rr, "target_rr")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DirectTransportFrame {
    Hello {
        schema: String,
        runtime_id: String,
        connection_id: String,
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
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Hello {
                schema,
                runtime_id,
                connection_id,
            } => {
                validate_direct_schema(schema)?;
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
            } => {
                validate_direct_schema(schema)?;
                validate_direct_token(session_id, 16, 128)?;
                validate_connection_generation(*connection_generation)?;
                request.validate()
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostControlOperation {
    Status,
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
            } => {
                if *run_id == 0 || *artifact_id == 0 {
                    return Err(ProtocolError::InvalidRequestId);
                }
                validate_source_tree(expected_source_tree)
            }
            Self::UpgradeLauncherRoot {
                run_id,
                artifact_id,
                expected_source_tree,
                expected_current_launcher_sha256,
            } => {
                if *run_id == 0 || *artifact_id == 0 {
                    return Err(ProtocolError::InvalidRequestId);
                }
                validate_source_tree(expected_source_tree)?;
                validate_sha256(expected_current_launcher_sha256)
            }
            _ => Ok(()),
        }
    }
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
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRole {
    Maker,
    Taker,
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
    if value == DIRECT_TRANSPORT_FRAME_SCHEMA_V1 {
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

fn validate_market_research(
    instruments: &[String],
    bar: &str,
    limit: Option<u16>,
) -> Result<(), ProtocolError> {
    if !(2..=8).contains(&instruments.len()) {
        return Err(ProtocolError::InvalidMarketResearchInstruments);
    }
    for (index, instrument) in instruments.iter().enumerate() {
        validate_history_request(instrument, bar, limit)?;
        if instruments[..index].contains(instrument) {
            return Err(ProtocolError::InvalidMarketResearchInstruments);
        }
    }
    Ok(())
}

fn validate_history_request(
    instrument: &str,
    bar: &str,
    limit: Option<u16>,
) -> Result<(), ProtocolError> {
    validate_instrument(instrument)?;
    if !matches!(
        bar,
        "1s" | "1m"
            | "3m"
            | "5m"
            | "15m"
            | "30m"
            | "1H"
            | "2H"
            | "4H"
            | "6H"
            | "12H"
            | "1D"
            | "2D"
            | "3D"
            | "1W"
            | "1M"
            | "3M"
            | "6Hutc"
            | "12Hutc"
            | "1Dutc"
            | "2Dutc"
            | "3Dutc"
            | "1Wutc"
            | "1Mutc"
            | "3Mutc"
    ) {
        return Err(ProtocolError::InvalidHistoryBar);
    }
    if matches!(limit, Some(0 | 101..)) {
        return Err(ProtocolError::InvalidHistoryLimit);
    }
    Ok(())
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

fn validate_positive_decimal_text(
    value: &str,
    field: &'static str,
) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > 64 {
        return Err(ProtocolError::InvalidDecimalInput(field));
    }

    let mut dots = 0usize;
    let mut digits = 0usize;
    let mut non_zero_digits = 0usize;
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().copied().enumerate() {
        match byte {
            b'0'..=b'9' => {
                digits += 1;
                if byte != b'0' {
                    non_zero_digits += 1;
                }
            }
            b'.'
                if dots == 0
                    && index > 0
                    && index + 1 < bytes.len() =>
            {
                dots += 1;
            }
            _ => return Err(ProtocolError::InvalidDecimalInput(field)),
        }
    }

    if digits == 0 || non_zero_digits == 0 {
        return Err(ProtocolError::InvalidDecimalInput(field));
    }

    Ok(())
}

fn validate_decimal_text(value: &str, field: &'static str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > 64 {
        return Err(ProtocolError::InvalidDecimalInput(field));
    }

    let mut dots = 0usize;
    let mut digits = 0usize;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' if dots == 0 => dots += 1,
            b'+' | b'-' if index == 0 => {}
            _ => return Err(ProtocolError::InvalidDecimalInput(field)),
        }
    }

    if digits == 0 {
        return Err(ProtocolError::InvalidDecimalInput(field));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_transport_read_only_gate_rejects_execution_mutations() {
        let read = AgentOperation::MarketOverview {
            instrument: "DOGE-USDT-SWAP".to_owned(),
        };
        assert!(read.direct_transport_read_only());

        let prepare = AgentOperation::PrepareCloseExecution {
            intent_id: "intent_0123456789abcdef".to_owned(),
            instrument: "DOGE-USDT-SWAP".to_owned(),
            trade_mode: ExecutionTradeMode::Cross,
            position_side: PositionSide::Long,
            order_type: ExecutionOrderType::Limit,
            size: "1".to_owned(),
            price: "0.1".to_owned(),
        };
        assert!(!prepare.direct_transport_read_only());

        let submit = AgentOperation::SubmitPreparedExecution {
            intent_id: "intent_0123456789abcdef".to_owned(),
        };
        assert!(!submit.direct_transport_read_only());
    }

    #[test]
    fn analytical_query_plan_is_bounded_and_strict() {
        let plan = AnalyticalQueryPlan {
            catalog_version: ANALYTICAL_QUERY_CATALOG_VERSION_V1.to_owned(),
            universe: MarketQueryUniverse {
                instrument_types: vec![InstrumentTypeFilter::Swap, InstrumentTypeFilter::Futures],
                settle_currency: Some("USDT".to_owned()),
                state: Some(QueryInstrumentState::Live),
            },
            select: vec![
                QueryField::InstrumentId,
                QueryField::Last,
                QueryField::Return24hPct,
            ],
            metric: Some(QueryMetric::Return24hPct),
            sort: Some(QuerySort {
                key: QuerySortKey::Return24hPct,
                direction: QuerySortDirection::Desc,
            }),
            limit: 10,
        };
        plan.validate().expect("valid bounded query plan");

        let mut stale = plan.clone();
        stale.catalog_version = "okx.query.catalog/2026-09-01.1".to_owned();
        stale
            .validate()
            .expect("stale catalog is a product-level rejection, not malformed transport input");

        let mut duplicate = plan.clone();
        duplicate.select.push(QueryField::Last);
        assert!(matches!(
            duplicate.validate(),
            Err(ProtocolError::InvalidAnalyticalQuery(
                "duplicate select field"
            ))
        ));

        let mut unbounded = plan.clone();
        unbounded.limit = 26;
        assert!(matches!(
            unbounded.validate(),
            Err(ProtocolError::InvalidAnalyticalQuery("limit"))
        ));

        let mut undeclared_metric = plan;
        undeclared_metric.metric = None;
        assert!(matches!(
            undeclared_metric.validate(),
            Err(ProtocolError::InvalidAnalyticalQuery(
                "declared metric must exactly match the derived field/sort key"
            ))
        ));
    }

    #[test]
    fn analytical_query_json_contract_matches_mcp_field_ids_exactly() {
        let json = r#"{
            "schema":"okx.agent.request/v1",
            "request_id":"req_query_json_contract_0001",
            "operation":{
                "type":"query",
                "plan":{
                    "catalog_version":"okx.query.catalog/2026-10-02.2",
                    "universe":{
                        "instrument_types":["SWAP","FUTURES"],
                        "settle_currency":null,
                        "state":"live"
                    },
                    "select":[
                        "instrument_id",
                        "open_24h",
                        "volume_24h",
                        "volume_currency_24h",
                        "return_24h_pct"
                    ],
                    "metric":"return_24h_pct",
                    "sort":{"key":"return_24h_pct","direction":"desc"},
                    "limit":10
                }
            }
        }"#;

        let request: AgentRequest =
            serde_json::from_str(json).expect("MCP query contract must deserialize");
        request
            .validate()
            .expect("MCP query contract must validate");

        let encoded = serde_json::to_string(&request).expect("serialize query contract");
        assert!(encoded.contains(r#""open_24h""#));
        assert!(encoded.contains(r#""volume_24h""#));
        assert!(encoded.contains(r#""volume_currency_24h""#));
        assert!(encoded.contains(r#""return_24h_pct""#));
        assert!(!encoded.contains(r#""open24h""#));
        assert!(!encoded.contains(r#""return24h_pct""#));
    }

    #[test]
    fn spread_query_and_market_intelligence_contracts_are_strict_and_read_only() {
        let spread = AnalyticalQueryPlan {
            catalog_version: ANALYTICAL_QUERY_CATALOG_VERSION_V1.to_owned(),
            universe: MarketQueryUniverse {
                instrument_types: vec![InstrumentTypeFilter::Swap],
                settle_currency: Some("USDT".to_owned()),
                state: Some(QueryInstrumentState::Live),
            },
            select: vec![
                QueryField::InstrumentId,
                QueryField::BestBid,
                QueryField::BestAsk,
                QueryField::SpreadBps,
            ],
            metric: Some(QueryMetric::SpreadBps),
            sort: Some(QuerySort {
                key: QuerySortKey::SpreadBps,
                direction: QuerySortDirection::Asc,
            }),
            limit: 10,
        };
        spread.validate().expect("valid spread plan");

        let mut mixed = spread.clone();
        mixed.select.push(QueryField::Return24hPct);
        assert!(matches!(
            mixed.validate(),
            Err(ProtocolError::InvalidAnalyticalQuery(
                "one derived metric per query plan"
            ))
        ));

        let operation = AgentOperation::MarketIntelligence {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            impact_contracts: "10".to_owned(),
            depth_levels: 20,
        };
        operation.validate().expect("market intelligence contract");
        assert!(operation.direct_transport_read_only());

        let too_deep = AgentOperation::MarketIntelligence {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            impact_contracts: "10".to_owned(),
            depth_levels: 51,
        };
        assert!(matches!(
            too_deep.validate(),
            Err(ProtocolError::InvalidAnalyticalQuery("depth_levels"))
        ));

        for invalid_impact in ["0", "0.0", "-1", "+1", "1.", ".1"] {
            let invalid = AgentOperation::MarketIntelligence {
                instrument: "DOGE-USDT-SWAP".to_owned(),
                impact_contracts: invalid_impact.to_owned(),
                depth_levels: 20,
            };
            assert!(matches!(
                invalid.validate(),
                Err(ProtocolError::InvalidDecimalInput("impact_contracts"))
            ));
        }
    }

    #[test]
    fn analytical_query_operations_remain_read_only_on_direct_transport() {
        let capabilities = AgentOperation::QueryCapabilities;
        assert!(capabilities.direct_transport_read_only());

        let query = AgentOperation::Query {
            plan: AnalyticalQueryPlan {
                catalog_version: ANALYTICAL_QUERY_CATALOG_VERSION_V1.to_owned(),
                universe: MarketQueryUniverse {
                    instrument_types: vec![InstrumentTypeFilter::Swap],
                    settle_currency: None,
                    state: Some(QueryInstrumentState::Live),
                },
                select: vec![QueryField::InstrumentId],
                metric: None,
                sort: None,
                limit: 5,
            },
        };
        assert!(query.direct_transport_read_only());
        query.validate().expect("valid query operation");
    }

    fn request_id() -> String {
        "req_0123456789abcdef".to_owned()
    }

    #[test]
    fn trading_capabilities_request_is_strict_and_round_trips() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_caps_0123456789".to_owned(),
            operation: AgentOperation::TradingCapabilities {
                instrument: "DOGE-USDT-SWAP".to_owned(),
                margin_mode: TradingMarginMode::Isolated,
            },
        };

        request
            .validate()
            .expect("valid trading capabilities request");
        let json = serde_json::to_string(&request).expect("serialize");
        let decoded: AgentRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, request);
        decoded.validate().expect("valid decoded request");

        let invalid = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_caps_invalid_012345".to_owned(),
            operation: AgentOperation::TradingCapabilities {
                instrument: "doge/usdt".to_owned(),
                margin_mode: TradingMarginMode::Cross,
            },
        };
        assert_eq!(invalid.validate(), Err(ProtocolError::InvalidInstrument));
    }

    #[test]
    fn execution_operations_are_strict_and_bounded() {
        let open = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: request_id(),
            operation: AgentOperation::PrepareOpenExecution {
                intent_id: "intent_open_0123456789".to_owned(),
                instrument: "DOGE-USDT-SWAP".to_owned(),
                trade_mode: ExecutionTradeMode::Cross,
                position_side: PositionSide::Long,
                order_type: ExecutionOrderType::Limit,
                entry_price: "0.1".to_owned(),
                stop_price: "0.09".to_owned(),
                max_settle_notional: "100".to_owned(),
                max_loss_settle: "5".to_owned(),
                target_rr: "2".to_owned(),
                entry_liquidity_role: LiquidityRole::Taker,
                exit_liquidity_role: LiquidityRole::Taker,
            },
        };
        open.validate().expect("valid open execution request");

        let close = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_close_0123456789".to_owned(),
            operation: AgentOperation::PrepareCloseExecution {
                intent_id: "intent_close_0123456789".to_owned(),
                instrument: "DOGE-USDT-SWAP".to_owned(),
                trade_mode: ExecutionTradeMode::Cross,
                position_side: PositionSide::Long,
                order_type: ExecutionOrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
            },
        };
        close.validate().expect("valid close execution request");

        let submit = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_submit_0123456789".to_owned(),
            operation: AgentOperation::SubmitPreparedExecution {
                intent_id: "intent_open_0123456789".to_owned(),
            },
        };
        submit.validate().expect("valid submit request");

        let status = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_status_0123456789".to_owned(),
            operation: AgentOperation::ExecutionStatus {
                intent_id: "intent_open_0123456789".to_owned(),
            },
        };
        status.validate().expect("valid execution status request");

        let invalid = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_invalid_exec_012345".to_owned(),
            operation: AgentOperation::PrepareCloseExecution {
                intent_id: "short".to_owned(),
                instrument: "DOGE-USDT-SWAP".to_owned(),
                trade_mode: ExecutionTradeMode::Cross,
                position_side: PositionSide::Long,
                order_type: ExecutionOrderType::Limit,
                size: "1".to_owned(),
                price: "0.1".to_owned(),
            },
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn host_control_request_is_strict_and_round_trips() {
        let request = HostControlRequest {
            schema: HOST_CONTROL_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "ctl_0123456789abcdef".to_owned(),
            operation: HostControlOperation::Status,
        };

        request.validate().expect("valid control request");
        let json = serde_json::to_string(&request).expect("serialize");
        let decoded: HostControlRequest = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded, request);
        decoded.validate().expect("valid decoded control request");

        let arbitrary = r#"{"schema":"okx.windows.control/v1","request_id":"ctl_0123456789abcdef","operation":{"type":"run_shell","command":"whoami"}}"#;
        assert!(serde_json::from_str::<HostControlRequest>(arbitrary).is_err());
    }

    #[test]
    fn deploy_agent_requires_bounded_ids_and_exact_tree_shape() {
        let request = HostControlRequest {
            schema: HOST_CONTROL_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "ctl_0123456789abcdef".to_owned(),
            operation: HostControlOperation::DeployAgent {
                run_id: 123,
                artifact_id: 456,
                expected_source_tree: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            },
        };
        request.validate().expect("valid deploy request");

        let invalid = HostControlRequest {
            schema: HOST_CONTROL_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "ctl_0123456789abcdef".to_owned(),
            operation: HostControlOperation::DeployAgent {
                run_id: 0,
                artifact_id: 456,
                expected_source_tree: "not-a-tree".to_owned(),
            },
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn host_control_request_requires_ctl_prefix() {
        let request = HostControlRequest {
            schema: HOST_CONTROL_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_0123456789abcdef".to_owned(),
            operation: HostControlOperation::Status,
        };

        assert_eq!(
            request.validate(),
            Err(ProtocolError::InvalidHostControlRequestId)
        );
    }

    #[test]
    fn typed_request_round_trips_without_loss() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: request_id(),
            operation: AgentOperation::AnalyzeCandidateOrder {
                instrument: "DOGE-USDT-SWAP".to_owned(),
                side: PositionSide::Long,
                entry_price: "0.2".to_owned(),
                stop_price: "0.18".to_owned(),
                max_settle_notional: "500".to_owned(),
                max_loss_settle: "20".to_owned(),
                target_rr: "3".to_owned(),
                entry_liquidity_role: LiquidityRole::Taker,
                exit_liquidity_role: LiquidityRole::Taker,
            },
        };

        request.validate().expect("valid request");
        let json = serde_json::to_string(&request).expect("serialize");
        let decoded: AgentRequest = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded, request);
        decoded.validate().expect("valid decoded request");
    }

    #[test]
    fn discovery_and_overview_requests_are_typed_and_validated() {
        let discovery = AgentOperation::FindInstruments {
            asset: "DOGE".to_owned(),
            settle_currency: Some("USDT".to_owned()),
            instrument_type: Some(InstrumentTypeFilter::Swap),
        };
        assert!(discovery.validate().is_ok());

        let invalid = AgentOperation::FindInstruments {
            asset: "doge".to_owned(),
            settle_currency: None,
            instrument_type: None,
        };
        assert_eq!(invalid.validate(), Err(ProtocolError::InvalidAssetCode));

        let overview = AgentOperation::MarketOverview {
            instrument: "DOGE-USDT-SWAP".to_owned(),
        };
        assert!(overview.validate().is_ok());
    }

    #[test]
    fn history_behavior_reuses_market_history_bounds() {
        let valid = AgentOperation::HistoryBehavior {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            limit: Some(24),
        };
        assert!(valid.validate().is_ok());

        let bad_bar = AgentOperation::HistoryBehavior {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "7H".to_owned(),
            limit: Some(24),
        };
        assert_eq!(bad_bar.validate(), Err(ProtocolError::InvalidHistoryBar));

        let too_many = AgentOperation::HistoryBehavior {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            limit: Some(101),
        };
        assert_eq!(too_many.validate(), Err(ProtocolError::InvalidHistoryLimit));
    }

    #[test]
    fn market_history_is_bounded_and_uses_supported_bars() {
        let valid = AgentOperation::MarketHistory {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "4H".to_owned(),
            limit: Some(100),
        };
        assert!(valid.validate().is_ok());

        let utc = AgentOperation::MarketHistory {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "1Dutc".to_owned(),
            limit: None,
        };
        assert!(utc.validate().is_ok());

        let bad_bar = AgentOperation::MarketHistory {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "7H".to_owned(),
            limit: Some(50),
        };
        assert_eq!(bad_bar.validate(), Err(ProtocolError::InvalidHistoryBar));

        let too_many = AgentOperation::MarketHistory {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            limit: Some(101),
        };
        assert_eq!(too_many.validate(), Err(ProtocolError::InvalidHistoryLimit));
    }

    #[test]
    fn account_summary_is_typed_and_read_only() {
        let operation = AgentOperation::AccountSummary;
        assert!(operation.validate().is_ok());
        assert!(operation.direct_transport_read_only());
        assert_eq!(
            serde_json::to_string(&operation).expect("serialize"),
            r#"{"type":"account_summary"}"#
        );
    }

    #[test]
    fn mailbox_telemetry_request_is_typed() {
        let request = AgentOperation::MailboxTelemetry;
        assert!(request.validate().is_ok());
        let json = serde_json::to_string(&request).expect("serialize");
        assert_eq!(json, r#"{"type":"mailbox_telemetry"}"#);
    }

    #[test]
    fn launcher_root_upgrade_requires_exact_sha256() {
        let valid = HostControlOperation::UpgradeLauncherRoot {
            run_id: 1,
            artifact_id: 2,
            expected_source_tree: "a".repeat(40),
            expected_current_launcher_sha256: "b".repeat(64),
        };
        assert!(valid.validate().is_ok());

        let invalid = HostControlOperation::UpgradeLauncherRoot {
            run_id: 1,
            artifact_id: 2,
            expected_source_tree: "a".repeat(40),
            expected_current_launcher_sha256: "not-a-sha".to_owned(),
        };
        assert_eq!(invalid.validate(), Err(ProtocolError::InvalidSha256));
    }

    #[test]
    fn unknown_operation_is_rejected_by_deserialization() {
        let json = format!(
            r#"{{"schema":"{}","request_id":"{}","operation":{{"type":"run_shell","command":"whoami"}}}}"#,
            AGENT_REQUEST_SCHEMA_V1,
            request_id()
        );

        assert!(serde_json::from_str::<AgentRequest>(&json).is_err());
    }

    #[test]
    fn unknown_request_field_is_rejected() {
        let json = format!(
            r#"{{"schema":"{}","request_id":"{}","unexpected":true,"operation":{{"type":"account_snapshot"}}}}"#,
            AGENT_REQUEST_SCHEMA_V1,
            request_id()
        );

        assert!(serde_json::from_str::<AgentRequest>(&json).is_err());
    }

    #[test]
    fn malformed_request_id_fails_closed() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "../bad".to_owned(),
            operation: AgentOperation::AccountSnapshot,
        };

        assert_eq!(request.validate(), Err(ProtocolError::InvalidRequestId));
    }

    #[test]
    fn aad_is_stable_and_binds_repository_direction_and_key() {
        let envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: "AA==".to_owned(),
            nonce: "AA==".to_owned(),
            ciphertext: "AA==".to_owned(),
        };

        assert_eq!(
            envelope.aad().expect("aad"),
            "schema=okx.mailbox.envelope/v1;repo=iamaman11/okx;request_id=req_0123456789abcdef;direction=client_to_agent;agent_key_id=agent-key-1"
        );
        assert_eq!(
            envelope.direction.kdf_label(),
            "okx-mailbox-v1/client-to-agent"
        );
    }

    #[test]
    fn wrong_envelope_direction_is_rejected() {
        let envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: "AA==".to_owned(),
            nonce: "AA==".to_owned(),
            ciphertext: "AA==".to_owned(),
        };

        assert_eq!(
            envelope.validate(MailboxDirection::AgentToClient),
            Err(ProtocolError::DirectionMismatch)
        );
    }

    #[test]
    fn market_research_is_bounded_unique_and_round_trips() {
        let request = AgentOperation::MarketResearch {
            instruments: vec![
                "DOGE-USDT-SWAP".to_owned(),
                "QNT-USDT-SWAP".to_owned(),
                "BTC-USDT-SWAP".to_owned(),
            ],
            bar: "1H".to_owned(),
            limit: Some(48),
        };
        assert!(request.validate().is_ok());

        let json = serde_json::to_string(&request).expect("serialize");
        let decoded: AgentOperation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, request);

        let too_short = AgentOperation::MarketResearch {
            instruments: vec!["DOGE-USDT-SWAP".to_owned()],
            bar: "1H".to_owned(),
            limit: Some(48),
        };
        assert_eq!(
            too_short.validate(),
            Err(ProtocolError::InvalidMarketResearchInstruments)
        );

        let duplicate = AgentOperation::MarketResearch {
            instruments: vec!["DOGE-USDT-SWAP".to_owned(), "DOGE-USDT-SWAP".to_owned()],
            bar: "1H".to_owned(),
            limit: Some(48),
        };
        assert_eq!(
            duplicate.validate(),
            Err(ProtocolError::InvalidMarketResearchInstruments)
        );
    }

    #[test]
    fn current_cost_is_typed_and_keeps_decimal_contracts_as_text() {
        let request = AgentOperation::CurrentCost {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            contracts: "2.5".to_owned(),
            side: PositionSide::Long,
            liquidity_role: LiquidityRole::Taker,
        };
        assert!(request.validate().is_ok());

        let json = serde_json::to_string(&request).expect("serialize");
        let decoded: AgentOperation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, request);

        let invalid = AgentOperation::CurrentCost {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            contracts: "2e3".to_owned(),
            side: PositionSide::Short,
            liquidity_role: LiquidityRole::Maker,
        };
        assert_eq!(
            invalid.validate(),
            Err(ProtocolError::InvalidDecimalInput("contracts"))
        );
    }

    #[test]
    fn position_scenario_requires_exactly_one_typed_exit_assumption() {
        let explicit_price = AgentOperation::PositionScenario {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Long,
            contracts: "2".to_owned(),
            entry_price: "0.2".to_owned(),
            exit_price: Some("0.206".to_owned()),
            entry_move_ratio: None,
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Maker,
        };
        assert!(explicit_price.validate().is_ok());

        let signed_move = AgentOperation::PositionScenario {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Short,
            contracts: "2".to_owned(),
            entry_price: "0.2".to_owned(),
            exit_price: None,
            entry_move_ratio: Some("-0.03".to_owned()),
            entry_liquidity_role: LiquidityRole::Maker,
            exit_liquidity_role: LiquidityRole::Taker,
        };
        assert!(signed_move.validate().is_ok());

        let json = serde_json::to_string(&signed_move).expect("serialize");
        let decoded: AgentOperation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, signed_move);

        let missing_exit = AgentOperation::PositionScenario {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Long,
            contracts: "2".to_owned(),
            entry_price: "0.2".to_owned(),
            exit_price: None,
            entry_move_ratio: None,
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
        };
        assert_eq!(
            missing_exit.validate(),
            Err(ProtocolError::InvalidPositionScenarioExit)
        );

        let ambiguous_exit = AgentOperation::PositionScenario {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Long,
            contracts: "2".to_owned(),
            entry_price: "0.2".to_owned(),
            exit_price: Some("0.21".to_owned()),
            entry_move_ratio: Some("0.05".to_owned()),
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
        };
        assert_eq!(
            ambiguous_exit.validate(),
            Err(ProtocolError::InvalidPositionScenarioExit)
        );
    }

    #[test]
    fn decimal_inputs_remain_strings_and_are_syntax_checked() {
        let valid = AgentOperation::AnalyzeCandidateOrder {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Short,
            entry_price: "0.2".to_owned(),
            stop_price: "0.22".to_owned(),
            max_settle_notional: "500.25".to_owned(),
            max_loss_settle: "20.00".to_owned(),
            target_rr: "3.0".to_owned(),
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Maker,
        };
        assert!(valid.validate().is_ok());

        let invalid = AgentOperation::AnalyzeCandidateOrder {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Short,
            entry_price: "2e-1".to_owned(),
            stop_price: "0.22".to_owned(),
            max_settle_notional: "500".to_owned(),
            max_loss_settle: "20".to_owned(),
            target_rr: "3".to_owned(),
            entry_liquidity_role: LiquidityRole::Taker,
            exit_liquidity_role: LiquidityRole::Taker,
        };
        assert_eq!(
            invalid.validate(),
            Err(ProtocolError::InvalidDecimalInput("entry_price"))
        );
    }
}

#[cfg(test)]
mod direct_transport_tests {
    use super::*;

    fn request() -> AgentRequest {
        AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_direct_0123456789".to_owned(),
            operation: AgentOperation::MarketOverview {
                instrument: "ADA-USDT-SWAP".to_owned(),
            },
        }
    }

    #[test]
    fn direct_transport_request_round_trips() {
        let frame = DirectTransportFrame::Request {
            schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
            session_id: "session_0123456789".to_owned(),
            connection_generation: 7,
            request: request(),
        };
        frame.validate().expect("valid frame");
        let encoded = serde_json::to_string(&frame).expect("encode");
        assert!(encoded.len() < DIRECT_TRANSPORT_MAX_PAYLOAD_BYTES);
        let decoded: DirectTransportFrame = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, frame);
        decoded.validate().expect("valid decoded frame");
    }

    #[test]
    fn direct_transport_rejects_zero_generation_and_bad_schema() {
        let bad_generation = DirectTransportFrame::Request {
            schema: DIRECT_TRANSPORT_FRAME_SCHEMA_V1.to_owned(),
            session_id: "session_0123456789".to_owned(),
            connection_generation: 0,
            request: request(),
        };
        assert_eq!(
            bad_generation.validate(),
            Err(ProtocolError::InvalidConnectionGeneration)
        );

        let bad_schema = DirectTransportFrame::Hello {
            schema: "okx.direct-transport.frame/v0".to_owned(),
            runtime_id: "windows-primary".to_owned(),
            connection_id: "connection_0123456789".to_owned(),
        };
        assert!(matches!(
            bad_schema.validate(),
            Err(ProtocolError::UnsupportedSchema(_))
        ));
    }
}
