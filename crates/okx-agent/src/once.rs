use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{SecondsFormat, Utc};
use okx_protocol::{
    AGENT_REQUEST_SCHEMA_V1, AgentRequest, MailboxDirection, MailboxEnvelope,
    crypto::{decrypt, derive_directional_key, encrypt, shared_secret},
};

use crate::{AgentError, AgentResult, query::dispatch};

pub use crate::query::{
    ACCOUNT_BOOTSTRAP_INCONSISTENT_CODE, ACCOUNT_OBSERVER_CREDENTIAL_UNAVAILABLE_CODE,
    ACCOUNT_OBSERVER_PERMISSION_REJECTED_CODE, ACCOUNT_PRIVATE_API_UNAVAILABLE_CODE,
    ANALYSIS_EXACT_FEE_UNAVAILABLE_CODE, ANALYSIS_INPUT_INCONSISTENT_CODE,
    MARKET_BOOTSTRAP_INCONSISTENT_CODE, MARKET_HISTORY_INCONSISTENT_CODE,
    MARKET_INSTRUMENT_NOT_LIVE_CODE, MARKET_OVERVIEW_INCONSISTENT_CODE,
    MARKET_OVERVIEW_SCHEMA_V1, MARKET_PUBLIC_API_UNAVAILABLE_CODE,
    MARKET_REFERENCE_INCOMPLETE_CODE, ObservationQueryContext, P1_NOT_AVAILABLE_CODE,
    PUBLIC_MARKET_MAX_AGE_MS, REFERENCE_INSTRUMENT_NOT_FOUND_CODE,
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

    let request: AgentRequest = serde_json::from_slice(&plaintext)?;
    request.validate()?;
    if request.request_id != envelope.request_id {
        return Err(AgentError::RequestIdMismatch);
    }
    debug_assert_eq!(request.schema, AGENT_REQUEST_SCHEMA_V1);

    let response = dispatch(&request, context, generated_at).await?;
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
