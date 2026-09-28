use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{SecondsFormat, Utc};
use okx_github::MAX_COMMENT_BODY_BYTES;
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest,
    AgentResponse, AgentResponseStatus, DataQuality, MAILBOX_ENVELOPE_SCHEMA_V1, MailboxDirection,
    MailboxEnvelope,
    crypto::{decrypt, derive_directional_key, encrypt, shared_secret},
};

use crate::{AgentError, AgentResult, query::dispatch};

pub const INVALID_REQUEST_CODE: &str = "INVALID_REQUEST";
pub const RESPONSE_TOO_LARGE_CODE: &str = "RESPONSE_TOO_LARGE";

const GLOBAL_RESPONSE_PLAINTEXT_BYTES: usize = 40 * 1024;
const COMPACT_RESPONSE_PLAINTEXT_BYTES: usize = 8 * 1024;
const MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES: usize = 12 * 1024;
const STANDARD_RESPONSE_PLAINTEXT_BYTES: usize = 16 * 1024;
const LARGE_RESPONSE_PLAINTEXT_BYTES: usize = 32 * 1024;
const AEAD_TAG_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseBudget {
    plaintext_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseSize {
    plaintext_bytes: usize,
    predicted_comment_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseSizeTelemetry {
    pub plaintext_bytes: u64,
    pub plaintext_budget_bytes: u64,
    pub predicted_comment_bytes: u64,
    pub budget_exceeded: bool,
}

struct BoundedResponsePlaintext {
    bytes: Vec<u8>,
    telemetry: ResponseSizeTelemetry,
}

pub use crate::query::{
    ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE, ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
    ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE, ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
    ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE, ANALYSIS_INPUT_INCONSISTENT_CODE,
    MARKET_BOOTSTRAP_INCONSISTENT_CODE, MARKET_HISTORY_INCONSISTENT_CODE,
    MARKET_INSTRUMENT_NOT_LIVE_CODE, MARKET_OVERVIEW_INCONSISTENT_CODE, MARKET_OVERVIEW_SCHEMA_V1,
    MARKET_PUBLIC_API_UNAVAILABLE_CODE, MARKET_REFERENCE_INCOMPLETE_CODE, ObservationQueryContext,
    P1_NOT_AVAILABLE_CODE, PUBLIC_MARKET_MAX_AGE_MS, REFERENCE_INSTRUMENT_NOT_FOUND_CODE,
};

pub async fn process_once(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: ObservationQueryContext<'_>,
    response_nonce: [u8; 12],
    generated_at: &str,
) -> AgentResult<MailboxEnvelope> {
    let (response, _) = process_once_with_size_telemetry(
        envelope,
        expected_key_id,
        agent_private_key,
        context,
        response_nonce,
        generated_at,
    )
    .await?;
    Ok(response)
}

async fn process_once_with_size_telemetry(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: ObservationQueryContext<'_>,
    response_nonce: [u8; 12],
    generated_at: &str,
) -> AgentResult<(MailboxEnvelope, ResponseSizeTelemetry)> {
    envelope.validate(MailboxDirection::ClientToAgent)?;

    if envelope.agent_key_id != expected_key_id {
        return Err(AgentError::AgentKeyMismatch {
            expected: expected_key_id.to_owned(),
            actual: envelope.agent_key_id.clone(),
        });
    }

    let client_public_key = decode_fixed::<32>(&envelope.client_ephemeral_public_key)?;
    let request_nonce = decode_fixed::<12>(&envelope.nonce)?;
    let shared = shared_secret(*agent_private_key, client_public_key)?;
    let request_key = derive_directional_key(
        &shared,
        &envelope.request_id,
        expected_key_id,
        MailboxDirection::ClientToAgent,
    )?;
    let ciphertext = STANDARD.decode(&envelope.ciphertext)?;
    let aad = envelope.aad()?;
    let plaintext = decrypt(&request_key, &request_nonce, aad.as_bytes(), &ciphertext)?;

    let (response, budget) = match serde_json::from_slice::<AgentRequest>(&plaintext) {
        Ok(request) if request.validate().is_ok() && request.request_id == envelope.request_id => {
            debug_assert_eq!(request.schema, AGENT_REQUEST_SCHEMA_V1);
            let budget = response_budget(&request.operation);
            (dispatch(&request, context, generated_at).await?, budget)
        }
        Ok(_) | Err(_) => (
            invalid_request_response(&envelope.request_id, generated_at),
            ResponseBudget {
                plaintext_bytes: COMPACT_RESPONSE_PLAINTEXT_BYTES,
            },
        ),
    };
    let bounded = bounded_response_plaintext(
        response,
        budget,
        envelope,
        expected_key_id,
        response_nonce,
        generated_at,
    )?;
    let response_key = derive_directional_key(
        &shared,
        &envelope.request_id,
        expected_key_id,
        MailboxDirection::AgentToClient,
    )?;

    let mut response_envelope = MailboxEnvelope {
        schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
        request_id: envelope.request_id.clone(),
        direction: MailboxDirection::AgentToClient,
        agent_key_id: expected_key_id.to_owned(),
        client_ephemeral_public_key: envelope.client_ephemeral_public_key.clone(),
        nonce: STANDARD.encode(response_nonce),
        ciphertext: String::new(),
    };
    let response_aad = response_envelope.aad()?;
    let response_ciphertext = encrypt(
        &response_key,
        &response_nonce,
        response_aad.as_bytes(),
        &bounded.bytes,
    )?;
    response_envelope.ciphertext = STANDARD.encode(response_ciphertext);

    Ok((response_envelope, bounded.telemetry))
}

fn response_budget(operation: &AgentOperation) -> ResponseBudget {
    let plaintext_bytes = match operation {
        AgentOperation::InstrumentRules { .. }
        | AgentOperation::ExecutorPreflight
        | AgentOperation::SubmitPreparedExecution { .. }
        | AgentOperation::HistoryBehavior { .. }
        | AgentOperation::SnapshotQuality { .. }
        | AgentOperation::MailboxTelemetry
        | AgentOperation::CurrentCost { .. }
        | AgentOperation::PositionScenario { .. } => COMPACT_RESPONSE_PLAINTEXT_BYTES,
        AgentOperation::MarketResearch { .. } => MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES,
        AgentOperation::MarketSnapshot { .. }
        | AgentOperation::MarketOverview { .. }
        | AgentOperation::PrepareOpenExecution { .. }
        | AgentOperation::PrepareCloseExecution { .. }
        | AgentOperation::PortfolioRisk
        | AgentOperation::AnalyzeCandidateOrder { .. } => STANDARD_RESPONSE_PLAINTEXT_BYTES,
        AgentOperation::MarketHistory { .. }
        | AgentOperation::FindInstruments { .. }
        | AgentOperation::AccountSnapshot => LARGE_RESPONSE_PLAINTEXT_BYTES,
    };
    debug_assert!(plaintext_bytes <= GLOBAL_RESPONSE_PLAINTEXT_BYTES);
    ResponseBudget { plaintext_bytes }
}

fn bounded_response_plaintext(
    response: AgentResponse,
    budget: ResponseBudget,
    request_envelope: &MailboxEnvelope,
    expected_key_id: &str,
    response_nonce: [u8; 12],
    generated_at: &str,
) -> AgentResult<BoundedResponsePlaintext> {
    response.validate()?;
    let plaintext = serde_json::to_vec(&response)?;
    let size = response_size(
        request_envelope,
        expected_key_id,
        response_nonce,
        plaintext.len(),
    )?;
    let effective_budget = budget.plaintext_bytes.min(GLOBAL_RESPONSE_PLAINTEXT_BYTES);
    let within_budget = response_size_within_budget(size, budget);
    let telemetry = ResponseSizeTelemetry {
        plaintext_bytes: u64::try_from(size.plaintext_bytes)
            .map_err(|_| AgentError::ResponseBudgetInvariant)?,
        plaintext_budget_bytes: u64::try_from(effective_budget)
            .map_err(|_| AgentError::ResponseBudgetInvariant)?,
        predicted_comment_bytes: u64::try_from(size.predicted_comment_bytes)
            .map_err(|_| AgentError::ResponseBudgetInvariant)?,
        budget_exceeded: !within_budget,
    };

    if within_budget {
        return Ok(BoundedResponsePlaintext {
            bytes: plaintext,
            telemetry,
        });
    }

    let failure = response_too_large_response(&response.request_id, generated_at, size, budget);
    failure.validate()?;
    let failure_plaintext = serde_json::to_vec(&failure)?;
    let failure_size = response_size(
        request_envelope,
        expected_key_id,
        response_nonce,
        failure_plaintext.len(),
    )?;
    let failure_budget = ResponseBudget {
        plaintext_bytes: COMPACT_RESPONSE_PLAINTEXT_BYTES,
    };
    if !response_size_within_budget(failure_size, failure_budget) {
        return Err(AgentError::ResponseBudgetInvariant);
    }

    Ok(BoundedResponsePlaintext {
        bytes: failure_plaintext,
        telemetry,
    })
}

fn response_size(
    request_envelope: &MailboxEnvelope,
    expected_key_id: &str,
    response_nonce: [u8; 12],
    plaintext_bytes: usize,
) -> AgentResult<ResponseSize> {
    let ciphertext_bytes = plaintext_bytes
        .checked_add(AEAD_TAG_BYTES)
        .ok_or(AgentError::ResponseBudgetInvariant)?;
    let ciphertext_base64_bytes = ciphertext_bytes
        .checked_add(2)
        .and_then(|value| value.checked_div(3))
        .and_then(|value| value.checked_mul(4))
        .ok_or(AgentError::ResponseBudgetInvariant)?;

    let projected_envelope = MailboxEnvelope {
        schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
        request_id: request_envelope.request_id.clone(),
        direction: MailboxDirection::AgentToClient,
        agent_key_id: expected_key_id.to_owned(),
        client_ephemeral_public_key: request_envelope.client_ephemeral_public_key.clone(),
        nonce: STANDARD.encode(response_nonce),
        ciphertext: String::new(),
    };
    let envelope_without_ciphertext = serde_json::to_vec(&projected_envelope)?.len();
    let predicted_comment_bytes = envelope_without_ciphertext
        .checked_add(ciphertext_base64_bytes)
        .ok_or(AgentError::ResponseBudgetInvariant)?;

    Ok(ResponseSize {
        plaintext_bytes,
        predicted_comment_bytes,
    })
}

fn response_size_within_budget(size: ResponseSize, budget: ResponseBudget) -> bool {
    size.plaintext_bytes <= budget.plaintext_bytes
        && size.plaintext_bytes <= GLOBAL_RESPONSE_PLAINTEXT_BYTES
        && size.predicted_comment_bytes <= MAX_COMMENT_BODY_BYTES
}

fn response_too_large_response(
    request_id: &str,
    generated_at: &str,
    size: ResponseSize,
    budget: ResponseBudget,
) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request_id.to_owned(),
        status: AgentResponseStatus::Failed,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: RESPONSE_TOO_LARGE_CODE.to_owned(),
            message: format!(
                "serialized response exceeds bounded transport budget: plaintext={}B limit={}B predicted_comment={}B comment_limit={}B",
                size.plaintext_bytes,
                budget.plaintext_bytes.min(GLOBAL_RESPONSE_PLAINTEXT_BYTES),
                size.predicted_comment_bytes,
                MAX_COMMENT_BODY_BYTES
            ),
            retryable: false,
        }),
        warnings: Vec::new(),
    }
}

fn invalid_request_response(request_id: &str, generated_at: &str) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request_id.to_owned(),
        status: AgentResponseStatus::Rejected,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: INVALID_REQUEST_CODE.to_owned(),
            message:
                "authenticated request payload does not match the supported typed query contract"
                    .to_owned(),
            retryable: false,
        }),
        warnings: Vec::new(),
    }
}

pub async fn process_once_now(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: ObservationQueryContext<'_>,
) -> AgentResult<MailboxEnvelope> {
    let (response, _) =
        process_once_now_with_size_telemetry(envelope, expected_key_id, agent_private_key, context)
            .await?;
    Ok(response)
}

pub async fn process_once_now_with_size_telemetry(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: ObservationQueryContext<'_>,
) -> AgentResult<(MailboxEnvelope, ResponseSizeTelemetry)> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|error| AgentError::Random(error.to_string()))?;
    let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    process_once_with_size_telemetry(
        envelope,
        expected_key_id,
        agent_private_key,
        context,
        nonce,
        &generated_at,
    )
    .await
}

fn decode_fixed<const N: usize>(value: &str) -> AgentResult<[u8; N]> {
    let bytes = STANDARD.decode(value)?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| AgentError::InvalidPrivateKeyLength(bytes.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_protocol::{
        AgentOperation, AgentResponse, AgentResponseStatus, DataQuality,
        crypto::{derive_directional_key, encrypt, public_key_from_private, shared_secret},
    };

    async fn process_authenticated_raw_request(
        plaintext: &[u8],
        request_id: &str,
        request_nonce: [u8; 12],
        response_nonce: [u8; 12],
    ) -> Result<AgentResponse, AgentError> {
        let agent_private = [1_u8; 32];
        let client_private = [2_u8; 32];
        let agent_public = public_key_from_private(agent_private);
        let client_public = public_key_from_private(client_private);
        let shared = shared_secret(client_private, agent_public).expect("shared");
        let request_key = derive_directional_key(
            &shared,
            request_id,
            "agent-key-1",
            MailboxDirection::ClientToAgent,
        )
        .expect("request key");

        let mut request_envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: STANDARD.encode(client_public),
            nonce: STANDARD.encode(request_nonce),
            ciphertext: String::new(),
        };
        let aad = request_envelope.aad().expect("request aad");
        request_envelope.ciphertext = STANDARD.encode(
            encrypt(&request_key, &request_nonce, aad.as_bytes(), plaintext)
                .expect("encrypt request"),
        );

        let response_envelope = process_once(
            &request_envelope,
            "agent-key-1",
            &agent_private,
            ObservationQueryContext::unavailable(),
            response_nonce,
            "2026-09-28T00:00:00.000Z",
        )
        .await?;

        let response_key = derive_directional_key(
            &shared,
            request_id,
            "agent-key-1",
            MailboxDirection::AgentToClient,
        )
        .expect("response key");
        let response_aad = response_envelope.aad().expect("response aad");
        let response_ciphertext = STANDARD
            .decode(&response_envelope.ciphertext)
            .expect("response ciphertext");
        let response_plaintext = decrypt(
            &response_key,
            &response_nonce,
            response_aad.as_bytes(),
            &response_ciphertext,
        )
        .expect("decrypt response");
        Ok(serde_json::from_slice(&response_plaintext).expect("response json"))
    }

    #[tokio::test]
    async fn authenticated_invalid_typed_requests_return_terminal_rejection() {
        let cases = [
            (
                "req_invalid_enum_0123456789",
                br#"{"schema":"okx.agent.request/v1","request_id":"req_invalid_enum_0123456789","operation":{"type":"find_instruments","asset":"DOGE","settle_currency":"USDT","instrument_type":"swap"}}"#.as_slice(),
            ),
            (
                "req_unknown_op_0123456789",
                br#"{"schema":"okx.agent.request/v1","request_id":"req_unknown_op_0123456789","operation":{"type":"run_shell","command":"whoami"}}"#.as_slice(),
            ),
            (
                "req_extra_field_0123456789",
                br#"{"schema":"okx.agent.request/v1","request_id":"req_extra_field_0123456789","unexpected":true,"operation":{"type":"portfolio_risk"}}"#.as_slice(),
            ),
        ];

        for (index, (request_id, plaintext)) in cases.into_iter().enumerate() {
            let response = process_authenticated_raw_request(
                plaintext,
                request_id,
                [10 + index as u8; 12],
                [20 + index as u8; 12],
            )
            .await
            .expect("terminal rejection");

            assert_eq!(response.request_id, request_id);
            assert_eq!(response.status, AgentResponseStatus::Rejected);
            assert_eq!(response.quality, DataQuality::NotReady);
            let failure = response.failure.expect("failure");
            assert_eq!(failure.code, INVALID_REQUEST_CODE);
            assert!(!failure.retryable);
            assert!(!failure.message.contains("swap"));
            assert!(!failure.message.contains("whoami"));
        }
    }

    #[tokio::test]
    async fn authenticated_request_id_mismatch_returns_terminal_rejection() {
        let response = process_authenticated_raw_request(
            br#"{"schema":"okx.agent.request/v1","request_id":"req_other_0123456789","operation":{"type":"portfolio_risk"}}"#,
            "req_envelope_0123456789",
            [30_u8; 12],
            [31_u8; 12],
        )
        .await
        .expect("terminal rejection");

        assert_eq!(response.request_id, "req_envelope_0123456789");
        assert_eq!(
            response.failure.expect("failure").code,
            INVALID_REQUEST_CODE
        );
    }

    #[tokio::test]
    async fn tampered_ciphertext_still_fails_before_terminal_response() {
        let agent_private = [1_u8; 32];
        let client_private = [2_u8; 32];
        let agent_public = public_key_from_private(agent_private);
        let client_public = public_key_from_private(client_private);
        let shared = shared_secret(client_private, agent_public).expect("shared");
        let request_id = "req_tampered_0123456789";
        let request_nonce = [40_u8; 12];
        let request_key = derive_directional_key(
            &shared,
            request_id,
            "agent-key-1",
            MailboxDirection::ClientToAgent,
        )
        .expect("request key");
        let mut envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: STANDARD.encode(client_public),
            nonce: STANDARD.encode(request_nonce),
            ciphertext: String::new(),
        };
        let aad = envelope.aad().expect("aad");
        let mut ciphertext = encrypt(
            &request_key,
            &request_nonce,
            aad.as_bytes(),
            br#"{"schema":"okx.agent.request/v1","request_id":"req_tampered_0123456789","operation":{"type":"portfolio_risk"}}"#,
        )
        .expect("encrypt");
        ciphertext[0] ^= 1;
        envelope.ciphertext = STANDARD.encode(ciphertext);

        let error = process_once(
            &envelope,
            "agent-key-1",
            &agent_private,
            ObservationQueryContext::unavailable(),
            [41_u8; 12],
            "2026-09-28T00:00:00.000Z",
        )
        .await
        .expect_err("tampered ciphertext must fail before response");

        assert!(matches!(error, AgentError::Crypto(_)));
    }

    fn sizing_envelope(request_id: &str) -> MailboxEnvelope {
        MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: STANDARD.encode([7_u8; 32]),
            nonce: STANDARD.encode([8_u8; 12]),
            ciphertext: String::new(),
        }
    }

    #[test]
    fn response_budget_registry_is_explicit_and_bounded() {
        let compact = AgentOperation::MailboxTelemetry;
        let research = AgentOperation::MarketResearch {
            instruments: vec!["DOGE-USDT-SWAP".to_owned(), "BTC-USDT-SWAP".to_owned()],
            bar: "1H".to_owned(),
            limit: Some(12),
        };
        let standard = AgentOperation::MarketOverview {
            instrument: "DOGE-USDT-SWAP".to_owned(),
        };
        let history = AgentOperation::MarketHistory {
            instrument: "DOGE-USDT-SWAP".to_owned(),
            bar: "1H".to_owned(),
            limit: Some(100),
        };
        let large_raw = AgentOperation::AccountSnapshot;

        assert_eq!(
            response_budget(&compact).plaintext_bytes,
            COMPACT_RESPONSE_PLAINTEXT_BYTES
        );
        assert_eq!(
            response_budget(&research).plaintext_bytes,
            MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES
        );
        assert_eq!(
            response_budget(&standard).plaintext_bytes,
            STANDARD_RESPONSE_PLAINTEXT_BYTES
        );
        assert_eq!(
            response_budget(&history).plaintext_bytes,
            LARGE_RESPONSE_PLAINTEXT_BYTES
        );
        assert_eq!(
            response_budget(&large_raw).plaintext_bytes,
            LARGE_RESPONSE_PLAINTEXT_BYTES
        );
    }

    #[test]
    fn every_plaintext_budget_has_an_exact_boundary() {
        for limit in [
            COMPACT_RESPONSE_PLAINTEXT_BYTES,
            MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES,
            STANDARD_RESPONSE_PLAINTEXT_BYTES,
            LARGE_RESPONSE_PLAINTEXT_BYTES,
        ] {
            let budget = ResponseBudget {
                plaintext_bytes: limit,
            };
            assert!(response_size_within_budget(
                ResponseSize {
                    plaintext_bytes: limit,
                    predicted_comment_bytes: MAX_COMMENT_BODY_BYTES,
                },
                budget
            ));
            assert!(!response_size_within_budget(
                ResponseSize {
                    plaintext_bytes: limit + 1,
                    predicted_comment_bytes: MAX_COMMENT_BODY_BYTES,
                },
                budget
            ));
        }
    }

    #[test]
    fn global_plaintext_ceiling_predicts_a_publishable_envelope() {
        let envelope = sizing_envelope(&"r".repeat(128));
        let size = response_size(
            &envelope,
            &"k".repeat(64),
            [9_u8; 12],
            GLOBAL_RESPONSE_PLAINTEXT_BYTES,
        )
        .expect("size");
        assert!(size.predicted_comment_bytes < MAX_COMMENT_BODY_BYTES);
    }

    #[test]
    fn predicted_comment_size_matches_actual_encrypted_envelope_size() {
        let request_id = "req_response_size_prediction_20260928a";
        let request = sizing_envelope(request_id);
        let response_nonce = [11_u8; 12];
        let plaintext = vec![b'x'; 10_535];
        let predicted = response_size(&request, "agent-key-1", response_nonce, plaintext.len())
            .expect("predicted size");

        let mut response = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            direction: MailboxDirection::AgentToClient,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: request.client_ephemeral_public_key.clone(),
            nonce: STANDARD.encode(response_nonce),
            ciphertext: String::new(),
        };
        let aad = response.aad().expect("aad");
        let ciphertext =
            encrypt(&[5_u8; 32], &response_nonce, aad.as_bytes(), &plaintext).expect("encrypt");
        response.ciphertext = STANDARD.encode(ciphertext);
        let actual = serde_json::to_vec(&response).expect("serialize").len();

        assert_eq!(predicted.predicted_comment_bytes, actual);
    }

    #[test]
    fn oversized_response_becomes_small_terminal_failure_before_encryption() {
        let request_id = "req_response_budget_0123456789";
        let envelope = sizing_envelope(request_id);
        let response = AgentResponse {
            schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            status: AgentResponseStatus::Completed,
            generated_at: "2026-09-28T00:00:00.000Z".to_owned(),
            quality: DataQuality::Fresh,
            result_schema: Some("okx.test.large/v1".to_owned()),
            result: Some(serde_json::json!({
                "payload": "x".repeat(MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES)
            })),
            failure: None,
            warnings: Vec::new(),
        };
        let bounded = bounded_response_plaintext(
            response,
            ResponseBudget {
                plaintext_bytes: MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES,
            },
            &envelope,
            "agent-key-1",
            [10_u8; 12],
            "2026-09-28T00:00:00.000Z",
        )
        .expect("bounded failure");

        assert!(bounded.telemetry.budget_exceeded);
        assert_eq!(
            bounded.telemetry.plaintext_budget_bytes,
            MARKET_RESEARCH_RESPONSE_PLAINTEXT_BYTES as u64
        );
        let terminal: AgentResponse =
            serde_json::from_slice(&bounded.bytes).expect("terminal json");
        assert_eq!(terminal.status, AgentResponseStatus::Failed);
        assert_eq!(terminal.quality, DataQuality::NotReady);
        let failure = terminal.failure.expect("failure");
        assert_eq!(failure.code, RESPONSE_TOO_LARGE_CODE);
        assert!(!failure.retryable);
        assert!(bounded.bytes.len() < COMPACT_RESPONSE_PLAINTEXT_BYTES);

        let size = response_size(&envelope, "agent-key-1", [10_u8; 12], bounded.bytes.len())
            .expect("failure size");
        assert!(size.predicted_comment_bytes < MAX_COMMENT_BODY_BYTES);
    }

    #[tokio::test]
    async fn local_once_round_trip_returns_authenticated_terminal_response() {
        let agent_private = [1_u8; 32];
        let client_private = [2_u8; 32];
        let agent_public = public_key_from_private(agent_private);
        let client_public = public_key_from_private(client_private);
        let shared = shared_secret(client_private, agent_public).expect("shared");
        assert_eq!(
            shared_secret(agent_private, client_public).expect("shared"),
            shared
        );

        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_0123456789abcdef".to_owned(),
            operation: AgentOperation::MarketSnapshot {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let request_plaintext = serde_json::to_vec(&request).expect("request json");
        let request_nonce = [3_u8; 12];
        let request_key = derive_directional_key(
            &shared,
            &request.request_id,
            "agent-key-1",
            MailboxDirection::ClientToAgent,
        )
        .expect("request key");

        let mut request_envelope = MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: request.request_id.clone(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "agent-key-1".to_owned(),
            client_ephemeral_public_key: STANDARD.encode(client_public),
            nonce: STANDARD.encode(request_nonce),
            ciphertext: String::new(),
        };
        let aad = request_envelope.aad().expect("request aad");
        request_envelope.ciphertext = STANDARD.encode(
            encrypt(
                &request_key,
                &request_nonce,
                aad.as_bytes(),
                &request_plaintext,
            )
            .expect("encrypt request"),
        );

        let response_nonce = [4_u8; 12];
        let response_envelope = process_once(
            &request_envelope,
            "agent-key-1",
            &agent_private,
            ObservationQueryContext::unavailable(),
            response_nonce,
            "2026-09-26T18:00:00.000Z",
        )
        .await
        .expect("process once");

        let response_key = derive_directional_key(
            &shared,
            &request.request_id,
            "agent-key-1",
            MailboxDirection::AgentToClient,
        )
        .expect("response key");
        let response_aad = response_envelope.aad().expect("response aad");
        let response_ciphertext = STANDARD
            .decode(&response_envelope.ciphertext)
            .expect("response ciphertext");
        let response_plaintext = decrypt(
            &response_key,
            &response_nonce,
            response_aad.as_bytes(),
            &response_ciphertext,
        )
        .expect("decrypt response");
        let response: AgentResponse =
            serde_json::from_slice(&response_plaintext).expect("response json");

        assert_eq!(response.request_id, request.request_id);
        assert_eq!(response.status, AgentResponseStatus::Rejected);
        assert_eq!(response.quality, DataQuality::NotReady);
        assert_eq!(
            response.failure.expect("failure").code,
            P1_NOT_AVAILABLE_CODE
        );
    }
}
