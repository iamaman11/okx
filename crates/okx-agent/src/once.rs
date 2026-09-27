use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{SecondsFormat, Utc};
use okx_observation::{
    INSTRUMENT_RULES_SCHEMA_V1, MARKET_SNAPSHOT_SCHEMA_V1, MarketError, ReferenceRegistry,
    SNAPSHOT_QUALITY_SCHEMA_V1, SnapshotQualityReport,
};
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentOperation, AgentRequest,
    AgentResponse, AgentResponseStatus, DataQuality, MAILBOX_ENVELOPE_SCHEMA_V1, MailboxDirection,
    MailboxEnvelope,
    crypto::{decrypt, derive_directional_key, encrypt, shared_secret},
};

use crate::{
    AgentError, AgentResult,
    market_bootstrap::{MarketBootstrapError, MarketBootstrapper},
};

pub const P1_NOT_AVAILABLE_CODE: &str = "P1_OPERATION_NOT_AVAILABLE";
pub const REFERENCE_INSTRUMENT_NOT_FOUND_CODE: &str = "REFERENCE_INSTRUMENT_NOT_FOUND";
pub const MARKET_REFERENCE_INCOMPLETE_CODE: &str = "MARKET_REFERENCE_INCOMPLETE";
pub const MARKET_PUBLIC_API_UNAVAILABLE_CODE: &str = "MARKET_PUBLIC_API_UNAVAILABLE";
pub const MARKET_BOOTSTRAP_INCONSISTENT_CODE: &str = "MARKET_BOOTSTRAP_INCONSISTENT";
pub const MARKET_INSTRUMENT_NOT_LIVE_CODE: &str = "MARKET_INSTRUMENT_NOT_LIVE";

const REFERENCE_BOOTSTRAP_WARNING: &str =
    "reference data is REST-bootstrap only; live instruments continuity is not connected until M3";
const MARKET_REST_BOOTSTRAP_WARNING: &str =
    "market data is bounded public REST bootstrap; persistent WebSocket continuity is not connected until M3";

pub async fn process_once(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    reference: Option<&ReferenceRegistry>,
    market: Option<&MarketBootstrapper>,
    response_nonce: [u8; 12],
    generated_at: &str,
) -> AgentResult<MailboxEnvelope> {
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

    let request: AgentRequest = serde_json::from_slice(&plaintext)?;
    request.validate()?;
    if request.request_id != envelope.request_id {
        return Err(AgentError::RequestIdMismatch);
    }
    debug_assert_eq!(request.schema, AGENT_REQUEST_SCHEMA_V1);

    let response = response_for(&request, reference, market, generated_at).await?;
    response.validate()?;
    let response_plaintext = serde_json::to_vec(&response)?;
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
        &response_plaintext,
    )?;
    response_envelope.ciphertext = STANDARD.encode(response_ciphertext);

    Ok(response_envelope)
}

pub async fn process_once_now(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    reference: Option<&ReferenceRegistry>,
    market: Option<&MarketBootstrapper>,
) -> AgentResult<MailboxEnvelope> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|error| AgentError::Random(error.to_string()))?;
    let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    process_once(
        envelope,
        expected_key_id,
        agent_private_key,
        reference,
        market,
        nonce,
        &generated_at,
    )
    .await
}

async fn response_for(
    request: &AgentRequest,
    reference: Option<&ReferenceRegistry>,
    market: Option<&MarketBootstrapper>,
    generated_at: &str,
) -> AgentResult<AgentResponse> {
    match &request.operation {
        AgentOperation::MarketSnapshot { instrument } => {
            let (Some(reference), Some(market)) = (reference, market) else {
                return Ok(unavailable(request, generated_at));
            };

            match market.snapshot(reference, instrument).await {
                Ok(result) => Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Degraded,
                    result_schema: Some(MARKET_SNAPSHOT_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![MARKET_REST_BOOTSTRAP_WARNING.to_owned()],
                }),
                Err(error) => Ok(market_failure(request, generated_at, error)),
            }
        }
        AgentOperation::InstrumentRules { instrument } => {
            let Some(reference) = reference else {
                return Ok(unavailable(request, generated_at));
            };

            if let Some(result) = reference.instrument_rules(instrument) {
                return Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Degraded,
                    result_schema: Some(INSTRUMENT_RULES_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![REFERENCE_BOOTSTRAP_WARNING.to_owned()],
                });
            }

            Ok(reference_not_found(request, generated_at, instrument))
        }
        AgentOperation::SnapshotQuality { instrument } => {
            let Some(reference) = reference else {
                return Ok(unavailable(request, generated_at));
            };

            match SnapshotQualityReport::m2(reference, instrument) {
                Ok(result) => Ok(AgentResponse {
                    schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
                    request_id: request.request_id.clone(),
                    status: AgentResponseStatus::Completed,
                    generated_at: generated_at.to_owned(),
                    quality: DataQuality::Degraded,
                    result_schema: Some(SNAPSHOT_QUALITY_SCHEMA_V1.to_owned()),
                    result: Some(serde_json::to_value(result)?),
                    failure: None,
                    warnings: vec![MARKET_REST_BOOTSTRAP_WARNING.to_owned()],
                }),
                Err(MarketError::InstrumentNotFound(_)) => {
                    Ok(reference_not_found(request, generated_at, instrument))
                }
                Err(MarketError::InstrumentNotLive(_)) => Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Rejected,
                    MARKET_INSTRUMENT_NOT_LIVE_CODE,
                    format!("instrument '{instrument}' is not live"),
                    false,
                )),
                Err(error) => Ok(failure_response(
                    request,
                    generated_at,
                    AgentResponseStatus::Failed,
                    MARKET_BOOTSTRAP_INCONSISTENT_CODE,
                    error.to_string(),
                    false,
                )),
            }
        }
        _ => Ok(unavailable(request, generated_at)),
    }
}

fn market_failure(
    request: &AgentRequest,
    generated_at: &str,
    error: MarketBootstrapError,
) -> AgentResponse {
    match error {
        MarketBootstrapError::ReferenceInstrumentNotFound(instrument) => {
            reference_not_found(request, generated_at, &instrument)
        }
        MarketBootstrapError::MissingUnderlying(instrument) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Rejected,
            MARKET_REFERENCE_INCOMPLETE_CODE,
            format!("instrument '{instrument}' has no underlying/index id in reference data"),
            false,
        ),
        MarketBootstrapError::Api(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_PUBLIC_API_UNAVAILABLE_CODE,
            error.to_string(),
            true,
        ),
        MarketBootstrapError::Normalize(error) => failure_response(
            request,
            generated_at,
            AgentResponseStatus::Failed,
            MARKET_BOOTSTRAP_INCONSISTENT_CODE,
            error.to_string(),
            true,
        ),
    }
}

fn reference_not_found(
    request: &AgentRequest,
    generated_at: &str,
    instrument: &str,
) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        REFERENCE_INSTRUMENT_NOT_FOUND_CODE,
        format!("instrument '{instrument}' is not present in the reference registry"),
        false,
    )
}

fn failure_response(
    request: &AgentRequest,
    generated_at: &str,
    status: AgentResponseStatus,
    code: &str,
    message: String,
    retryable: bool,
) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request.request_id.clone(),
        status,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: code.to_owned(),
            message,
            retryable,
        }),
        warnings: Vec::new(),
    }
}

fn unavailable(request: &AgentRequest, generated_at: &str) -> AgentResponse {
    failure_response(
        request,
        generated_at,
        AgentResponseStatus::Rejected,
        P1_NOT_AVAILABLE_CODE,
        "typed request accepted by the P1 runtime shell; domain operation is not connected yet"
            .to_owned(),
        false,
    )
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
    use okx_api::PublicInstrument;
    use okx_protocol::crypto::{
        derive_directional_key, encrypt, public_key_from_private, shared_secret,
    };

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
            None,
            None,
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

    #[tokio::test]
    async fn instrument_rules_uses_reference_registry_and_reports_bootstrap_quality() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_rules_0123456789".to_owned(),
            operation: AgentOperation::InstrumentRules {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let registry = reference();

        let response = response_for(
            &request,
            Some(&registry),
            None,
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(INSTRUMENT_RULES_SCHEMA_V1)
        );
        assert!(response.failure.is_none());
        assert_eq!(response.warnings, vec![REFERENCE_BOOTSTRAP_WARNING]);
    }

    #[tokio::test]
    async fn snapshot_quality_explains_rest_only_m2_state() {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_quality_012345678".to_owned(),
            operation: AgentOperation::SnapshotQuality {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let registry = reference();

        let response = response_for(
            &request,
            Some(&registry),
            None,
            "2026-09-27T00:00:01.000Z",
        )
        .await
        .expect("response");

        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(response.quality, DataQuality::Degraded);
        assert_eq!(
            response.result_schema.as_deref(),
            Some(SNAPSHOT_QUALITY_SCHEMA_V1)
        );
        assert_eq!(
            response.result.expect("result")["reason"],
            "M2_REST_BOOTSTRAP_ONLY"
        );
    }

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public("2026-09-27T00:00:00.000Z", vec![swap()])
            .expect("registry")
    }

    fn swap() -> PublicInstrument {
        PublicInstrument {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            instrument_family: "DOGE-USDT".to_owned(),
            underlying: "DOGE-USDT".to_owned(),
            state: "live".to_owned(),
            rule_type: "normal".to_owned(),
            base_currency: String::new(),
            quote_currency: String::new(),
            settle_currency: "USDT".to_owned(),
            tick_size: "0.00001".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: "1000000".to_owned(),
            max_market_size: "100000".to_owned(),
            max_limit_amount: String::new(),
            max_market_amount: String::new(),
            contract_type: "linear".to_owned(),
            contract_value: "1000".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            fee_group_id: "4".to_owned(),
            lever: "50".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
        }
    }
}
