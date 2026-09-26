pub mod crypto;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAILBOX_ENVELOPE_SCHEMA_V1: &str = "okx.mailbox.envelope/v1";
pub const AGENT_REQUEST_SCHEMA_V1: &str = "okx.agent.request/v1";
pub const AGENT_RESPONSE_SCHEMA_V1: &str = "okx.agent.response/v1";
pub const MAILBOX_REPOSITORY: &str = "iamaman11/okx";
pub const KDF_LABEL_CLIENT_TO_AGENT_V1: &str = "okx-mailbox-v1/client-to-agent";
pub const KDF_LABEL_AGENT_TO_CLIENT_V1: &str = "okx-mailbox-v1/agent-to-client";

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

    #[error("invalid history bar")]
    InvalidHistoryBar,

    #[error("invalid decimal input '{0}'")]
    InvalidDecimalInput(&'static str),

    #[error("invalid history limit")]
    InvalidHistoryLimit,

    #[error("mailbox direction mismatch")]
    DirectionMismatch,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentOperation {
    MarketSnapshot {
        instrument: String,
    },
    InstrumentRules {
        instrument: String,
    },
    MarketHistory {
        instrument: String,
        bar: String,
        limit: Option<u16>,
    },
    SnapshotQuality {
        instrument: String,
    },
    AccountSnapshot,
    PortfolioRisk,
    AnalyzeCandidateOrder {
        instrument: String,
        side: PositionSide,
        notional_usd: String,
        max_risk_usd: String,
        target_rr: String,
    },
}

impl AgentOperation {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::MarketSnapshot { instrument }
            | Self::InstrumentRules { instrument }
            | Self::SnapshotQuality { instrument } => validate_instrument(instrument),
            Self::MarketHistory {
                instrument,
                bar,
                limit,
            } => {
                validate_instrument(instrument)?;
                if bar.is_empty()
                    || bar.len() > 16
                    || !bar.bytes().all(|b| b.is_ascii_alphanumeric())
                {
                    return Err(ProtocolError::InvalidHistoryBar);
                }
                if matches!(limit, Some(0)) {
                    return Err(ProtocolError::InvalidHistoryLimit);
                }
                Ok(())
            }
            Self::AccountSnapshot | Self::PortfolioRisk => Ok(()),
            Self::AnalyzeCandidateOrder {
                instrument,
                notional_usd,
                max_risk_usd,
                target_rr,
                ..
            } => {
                validate_instrument(instrument)?;
                validate_decimal_text(notional_usd, "notional_usd")?;
                validate_decimal_text(max_risk_usd, "max_risk_usd")?;
                validate_decimal_text(target_rr, "target_rr")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
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

fn validate_agent_key_id(value: &str) -> Result<(), ProtocolError> {
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

    fn request_id() -> String {
        "req_0123456789abcdef".to_owned()
    }

    #[test]
    fn typed_request_round_trips_without_loss() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: request_id(),
            operation: AgentOperation::AnalyzeCandidateOrder {
                instrument: "DOGE-USDT-SWAP".to_owned(),
                side: PositionSide::Long,
                notional_usd: "500".to_owned(),
                max_risk_usd: "20".to_owned(),
                target_rr: "3".to_owned(),
            },
        };

        request.validate().expect("valid request");
        let json = serde_json::to_string(&request).expect("serialize");
        let decoded: AgentRequest = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded, request);
        decoded.validate().expect("valid decoded request");
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
    fn decimal_inputs_remain_strings_and_are_syntax_checked() {
        let valid = AgentOperation::AnalyzeCandidateOrder {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Short,
            notional_usd: "500.25".to_owned(),
            max_risk_usd: "20.00".to_owned(),
            target_rr: "3.0".to_owned(),
        };
        assert!(valid.validate().is_ok());

        let invalid = AgentOperation::AnalyzeCandidateOrder {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            side: PositionSide::Short,
            notional_usd: "5e2".to_owned(),
            max_risk_usd: "20".to_owned(),
            target_rr: "3".to_owned(),
        };
        assert_eq!(
            invalid.validate(),
            Err(ProtocolError::InvalidDecimalInput("notional_usd"))
        );
    }
}
