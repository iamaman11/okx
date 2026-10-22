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
pub const MAILBOX_EXECUTION_DISABLED_CODE: &str = "MAILBOX_EXECUTION_DISABLED";
pub const RESPONSE_TOO_LARGE_CODE: &str = "RESPONSE_TOO_LARGE";

const GLOBAL_RESPONSE_PLAINTEXT_BYTES: usize = 40 * 1024;
const COMPACT_RESPONSE_PLAINTEXT_BYTES: usize = 8 * 1024;
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

pub use crate::query::DispatchContext;

pub async fn process_once(
    envelope: &MailboxEnvelope,
    expected_key_id: &str,
    agent_private_key: &[u8; 32],
    context: &DispatchContext,
    response_nonce: [u8; 12],
    generated_at: &str,
) -> AgentResult<MailboxEnvelope> {
    let (response, _) = process_once_with_size_telemetry(
        envelope,
        expected_key_id,
        agent_private_key,
        Some(context),
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
    context: Option<&DispatchContext>,
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
            // Encrypted GitHub mailbox and local Once are strictly read-only.
            // Only the profile-verified Cloudflare direct transport calls the
            // shared query dispatcher with trading authority. Decrypting an
            // authenticated envelope does NOT authorize exchange mutation.
            let response = if request.operation.direct_transport_read_only() {
                if let Some(ctx) = context {
                    dispatch(ctx, &request).await
                } else {
                    invalid_request_response(&request.request_id, generated_at)
                }
            } else {
                mailbox_execution_disabled(&request.request_id, generated_at)
            };
            (response, budget)
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
    let plaintext_bytes = if matches!(operation, AgentOperation::Portfolio { .. }) {
        4 * 1024
    } else {
        COMPACT_RESPONSE_PLAINTEXT_BYTES
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

fn mailbox_execution_disabled(request_id: &str, generated_at: &str) -> AgentResponse {
    AgentResponse {
        schema: AGENT_RESPONSE_SCHEMA_V1.to_owned(),
        request_id: request_id.to_owned(),
        status: AgentResponseStatus::Rejected,
        generated_at: generated_at.to_owned(),
        quality: DataQuality::NotReady,
        result_schema: None,
        result: None,
        failure: Some(AgentFailure {
            code: MAILBOX_EXECUTION_DISABLED_CODE.to_owned(),
            message: "GitHub mailbox and local Once are read-only; trading writes require the authenticated primary Cloudflare direct transport and native account policy".to_owned(),
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
    context: &DispatchContext,
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
    context: &DispatchContext,
) -> AgentResult<(MailboxEnvelope, ResponseSizeTelemetry)> {
    let mut nonce = [0_u8; 12];
    getrandom::fill(&mut nonce).map_err(|error| AgentError::Random(error.to_string()))?;
    let generated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    process_once_with_size_telemetry(
        envelope,
        expected_key_id,
        agent_private_key,
        Some(context),
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
    fn envelope() -> MailboxEnvelope {
        MailboxEnvelope {
            schema: MAILBOX_ENVELOPE_SCHEMA_V1.into(),
            request_id: "request-398-fixture-001".into(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: "key-398".into(),
            client_ephemeral_public_key: STANDARD.encode([7u8; 32]),
            nonce: STANDARD.encode([8u8; 12]),
            ciphertext: String::new(),
        }
    }
    #[test]
    fn predicted_comment_size_matches_actual_encrypted_envelope_size() {
        let e = envelope();
        let size = response_size(&e, "key-398", [1; 12], 1024).unwrap();
        let mut actual = e.clone();
        actual.direction = MailboxDirection::AgentToClient;
        actual.nonce = STANDARD.encode([1; 12]);
        actual.ciphertext = STANDARD.encode(vec![0u8; 1040]);
        assert_eq!(
            size.predicted_comment_bytes,
            serde_json::to_vec(&actual).unwrap().len()
        );
    }
    #[test]
    fn oversized_response_becomes_small_terminal_failure_before_encryption() {
        let e = envelope();
        let mut r = invalid_request_response(&e.request_id, "2026-10-23T00:00:00Z");
        r.warnings = vec!["x".repeat(10000)];
        let bounded = bounded_response_plaintext(
            r,
            ResponseBudget {
                plaintext_bytes: 8192,
            },
            &e,
            "key-398",
            [1; 12],
            "2026-10-23T00:00:00Z",
        )
        .unwrap();
        let r: AgentResponse = serde_json::from_slice(&bounded.bytes).unwrap();
        assert_eq!(r.failure.unwrap().code, RESPONSE_TOO_LARGE_CODE);
        assert!(bounded.bytes.len() < 8192);
    }
    #[tokio::test]
    async fn encrypted_mailbox_authenticates_and_rejects_mutation_before_dispatch() {
        use okx_protocol::crypto::public_key_from_private;
        let agent = [3u8; 32];
        let client = [4u8; 32];
        let mut e = envelope();
        e.client_ephemeral_public_key = STANDARD.encode(public_key_from_private(client));
        let shared = shared_secret(agent, public_key_from_private(client)).unwrap();
        let key = derive_directional_key(
            &shared,
            &e.request_id,
            &e.agent_key_id,
            MailboxDirection::ClientToAgent,
        )
        .unwrap();
        let payload = serde_json::json!({"schema":AGENT_REQUEST_SCHEMA_V1,"request_id":e.request_id,"account":null,"operation":{"type":"connect_account","credential_ref":"fixture-reference","alias":"fixture","operation_id":"operation-398","request_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}});
        e.ciphertext = STANDARD.encode(
            encrypt(
                &key,
                &[8; 12],
                e.aad().unwrap().as_bytes(),
                &serde_json::to_vec(&payload).unwrap(),
            )
            .unwrap(),
        );
        let reply = process_once_with_size_telemetry(
            &e,
            "key-398",
            &agent,
            None,
            [1; 12],
            "2026-10-23T00:00:00Z",
        )
        .await
        .unwrap()
        .0;
        let key = derive_directional_key(
            &shared,
            &e.request_id,
            &e.agent_key_id,
            MailboxDirection::AgentToClient,
        )
        .unwrap();
        let plaintext = decrypt(
            &key,
            &[1; 12],
            reply.aad().unwrap().as_bytes(),
            &STANDARD.decode(&reply.ciphertext).unwrap(),
        )
        .unwrap();
        let response: AgentResponse = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(response.request_id, e.request_id);
        assert_eq!(response.status, AgentResponseStatus::Rejected);
        assert_eq!(
            response.failure.unwrap().code,
            MAILBOX_EXECUTION_DISABLED_CODE
        );
        let mut corrupted = e;
        corrupted.ciphertext = STANDARD.encode([0u8; 32]);
        assert!(
            process_once_with_size_telemetry(
                &corrupted,
                "key-398",
                &agent,
                None,
                [1; 12],
                "2026-10-23T00:00:00Z"
            )
            .await
            .is_err()
        );
    }
}
