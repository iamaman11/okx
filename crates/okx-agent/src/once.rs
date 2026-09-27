use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{SecondsFormat, Utc};
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AGENT_RESPONSE_SCHEMA_V1, AgentFailure, AgentRequest, AgentResponse,
    AgentResponseStatus, DataQuality, MAILBOX_ENVELOPE_SCHEMA_V1, MailboxDirection,
    MailboxEnvelope,
    crypto::{decrypt, derive_directional_key, encrypt, shared_secret},
};

use crate::{AgentError, AgentResult, query::dispatch};

pub const INVALID_REQUEST_CODE: &str = "INVALID_REQUEST";

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

    let response = match serde_json::from_slice::<AgentRequest>(&plaintext) {
        Ok(request)
            if request.validate().is_ok() && request.request_id == envelope.request_id =>
        {
            debug_assert_eq!(request.schema, AGENT_REQUEST_SCHEMA_V1);
            dispatch(&request, context, generated_at).await?
        }
        Ok(_) | Err(_) => invalid_request_response(&envelope.request_id, generated_at),
    };
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
            message: "authenticated request payload does not match the supported typed query contract"
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
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|error| AgentError::Random(error.to_string()))?;
    let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    process_once(
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
            encrypt(
                &request_key,
                &request_nonce,
                aad.as_bytes(),
                plaintext,
            )
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
