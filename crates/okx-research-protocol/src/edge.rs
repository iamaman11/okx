//! Edge wire shapes for the independently authenticated Research owner.

use crate::analytics::{MarketRequest, PortfolioScenarioRequest, ResearchRequest};
use crate::research_ipc::ResearchLane;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const SCHEMA: &str = "okx.direct-transport.frame/v2";
pub const FRAME_LIMIT: usize = 64 * 1024;
pub const PRODUCT_CONTRACT: &str = "okx.product/v1";
pub const AGENT_RESPONSE_SCHEMA_V1: &str = "okx.agent.response/v1";
const REQUEST_SCHEMA: &str = "okx.agent.request/v1";

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum IncomingFrame {
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
        request: Box<ResearchRequestEnvelope>,
        operator_governance: Option<Value>,
    },
}

impl IncomingFrame {
    pub fn validate_incoming_envelope(&self) -> Result<(), &'static str> {
        let (schema, session_id, generation) = match self {
            Self::HelloAck {
                schema,
                session_id,
                connection_generation,
            } => (schema, session_id, connection_generation),
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
                if !valid_token(nonce) {
                    return Err("INVALID_RESEARCH_EDGE_NONCE");
                }
                (schema, session_id, connection_generation)
            }
            Self::Request {
                schema,
                session_id,
                connection_generation,
                request,
                ..
            } => {
                if !request.valid_envelope() {
                    return Err("INVALID_RESEARCH_REQUEST_ENVELOPE");
                }
                (schema, session_id, connection_generation)
            }
        };
        if schema != SCHEMA || !valid_token(session_id) || *generation == 0 {
            return Err("INVALID_RESEARCH_EDGE_ENVELOPE");
        }
        Ok(())
    }
}

fn valid_token(value: &str) -> bool {
    (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchRequestEnvelope {
    pub schema: String,
    pub request_id: String,
    pub account: Option<String>,
    operation: ResearchOperation,
}

impl ResearchRequestEnvelope {
    fn valid_envelope(&self) -> bool {
        self.schema == REQUEST_SCHEMA && valid_token(&self.request_id)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ResearchOperation {
    Market {
        request: MarketRequest,
    },
    MarketCapabilities,
    Research {
        request: ResearchRequest,
    },
    PortfolioScenario {
        request: PortfolioScenarioRequest,
    },
    Diagnostics {
        request: ResearchDiagnostics,
    },
    MarketMicrostructure {
        instrument: String,
        size_contracts: Option<String>,
    },
    FindInstruments {
        asset: String,
        settle_currency: Option<String>,
        instrument_type: Option<InstrumentTypeFilter>,
    },
    MarketOverview {
        instrument: String,
    },
    #[serde(other)]
    Forbidden,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchDiagnostics {
    account: Option<String>,
    view: Option<String>,
    reference: Option<String>,
    #[serde(rename = "cycle_id")]
    _cycle_id: Option<u64>,
    #[serde(rename = "limit")]
    _limit: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InstrumentTypeFilter {
    Swap,
    Futures,
}

pub enum ResearchCommand {
    Market(MarketRequest),
    MarketCapabilities,
    Research(ResearchRequest),
    PortfolioScenario(PortfolioScenarioRequest),
    ExplainJob(Option<String>),
    MarketMicrostructure {
        instrument: String,
        size_contracts: Option<String>,
    },
    FindInstruments {
        asset: String,
        settle_currency: Option<String>,
        instrument_type: Option<InstrumentTypeFilter>,
    },
    MarketOverview {
        instrument: String,
    },
}

impl ResearchCommand {
    /// The Edge socket uses the same command priorities as local Research IPC.
    /// A flooded source/compute lane must not lock out cancellation or status.
    pub fn lane(&self) -> ResearchLane {
        match self {
            Self::Research(ResearchRequest::Cancel { .. }) => ResearchLane::Critical,
            Self::Research(
                ResearchRequest::Status { .. }
                | ResearchRequest::List { .. }
                | ResearchRequest::Capabilities {},
            )
            | Self::MarketCapabilities
            | Self::ExplainJob(_) => ResearchLane::Observation,
            _ => ResearchLane::Work,
        }
    }
}

#[cfg(test)]
mod edge_lanes_tests {
    use super::*;

    #[test]
    fn cancellation_and_diagnostics_have_independent_admission_lanes() {
        assert_eq!(
            ResearchCommand::Research(ResearchRequest::Cancel {
                job_id: "job".into(),
                idempotency_key: "cancel".into(),
            })
            .lane(),
            ResearchLane::Critical
        );
        assert_eq!(
            ResearchCommand::Research(ResearchRequest::Status {
                job_id: "job".into()
            })
            .lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            ResearchCommand::Research(ResearchRequest::List {
                limit: 10,
                cursor: None,
            })
            .lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            ResearchCommand::ExplainJob(Some("job".into())).lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            ResearchCommand::MarketCapabilities.lane(),
            ResearchLane::Observation
        );
        assert_eq!(
            ResearchCommand::Research(ResearchRequest::Benchmark {
                idempotency_key: "benchmark".into(),
            })
            .lane(),
            ResearchLane::Work
        );
    }
}

impl TryFrom<ResearchRequestEnvelope> for ResearchCommand {
    type Error = &'static str;

    fn try_from(envelope: ResearchRequestEnvelope) -> Result<Self, Self::Error> {
        if envelope.account.is_some() {
            return Err("RESEARCH_OPERATION_FORBIDDEN");
        }
        if !envelope.valid_envelope() {
            return Err("INVALID_RESEARCH_REQUEST");
        }
        let command = match envelope.operation {
            ResearchOperation::Market { request } if request.valid() => Self::Market(request),
            ResearchOperation::MarketCapabilities => Self::MarketCapabilities,
            ResearchOperation::Research { request } if request.valid() => Self::Research(request),
            ResearchOperation::PortfolioScenario { request } if request.valid() => {
                Self::PortfolioScenario(request)
            }
            ResearchOperation::Diagnostics { request } => {
                if request.account.is_some() || request.view.as_deref() != Some("research") {
                    return Err("RESEARCH_OPERATION_FORBIDDEN");
                }
                if !request.reference.as_ref().is_some_and(|v| {
                    (1..=128).contains(&v.len())
                        && v.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b':'))
                }) {
                    return Err("INVALID_RESEARCH_REQUEST");
                }
                Self::ExplainJob(request.reference)
            }
            ResearchOperation::MarketMicrostructure {
                instrument,
                size_contracts,
            } if valid_instrument(&instrument)
                && size_contracts.as_ref().is_none_or(|v| {
                    v.parse::<f64>()
                        .is_ok_and(|n| n.is_finite() && n > 0.0 && n <= 1_000_000.0)
                }) =>
            {
                Self::MarketMicrostructure {
                    instrument,
                    size_contracts,
                }
            }
            ResearchOperation::MarketOverview { instrument } if valid_instrument(&instrument) => {
                Self::MarketOverview { instrument }
            }
            ResearchOperation::FindInstruments {
                asset,
                settle_currency,
                instrument_type,
            } if valid_asset(&asset) && settle_currency.as_deref().is_none_or(valid_asset) => {
                Self::FindInstruments {
                    asset,
                    settle_currency,
                    instrument_type,
                }
            }
            ResearchOperation::Forbidden => return Err("RESEARCH_OPERATION_FORBIDDEN"),
            _ => return Err("INVALID_RESEARCH_REQUEST"),
        };
        Ok(command)
    }
}

fn valid_instrument(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_'))
}

fn valid_asset(value: &str) -> bool {
    (2..=16).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutgoingFrame {
    HelloResearch {
        schema: String,
        runtime_id: String,
        connection_id: String,
        host_scope: String,
        contract: String,
        runtime_profile: &'static str,
        agent_pid: u32,
        agent_binary_sha256: String,
        agent_incarnation: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentFailure {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}
