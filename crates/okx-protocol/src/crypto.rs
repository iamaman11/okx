use base64::{Engine as _, engine::general_purpose::STANDARD};
use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::{Aead, KeyInit, Payload, array::Array},
};
use hkdf::Hkdf;
use sha2::Sha256;
use thiserror::Error;
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

use crate::{
    AgentRequest, MAILBOX_REPOSITORY, MailboxDirection, MailboxEnvelope, ProtocolError,
    validate_agent_key_id, validate_request_id,
};

pub const HKDF_SALT_V1: &[u8] = b"okx-mailbox-v1/hkdf-sha256";

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),

    #[error("X25519 peer public key produced a non-contributory shared secret")]
    NonContributoryPeerKey,

    #[error("HKDF expansion failed")]
    Kdf,

    #[error("AEAD encryption failed")]
    Encrypt,

    #[error("AEAD authentication/decryption failed")]
    Decrypt,
}

#[derive(Debug, Error)]
pub enum ClientEnvelopePreflightError {
    #[error("protocol error: {0}")]
    Protocol(ProtocolError),

    #[error("crypto error: {0}")]
    Crypto(CryptoError),

    #[error("base64 decode error: {0}")]
    Base64(base64::DecodeError),

    #[error("JSON decode error: {0}")]
    Json(serde_json::Error),

    #[error("client ephemeral public key must decode to exactly 32 bytes")]
    InvalidClientPublicKeyLength,

    #[error("nonce must decode to exactly 12 bytes")]
    InvalidNonceLength,

    #[error("client ephemeral public key does not match the supplied private key")]
    ClientPublicKeyMismatch,

    #[error("outer and inner request_id do not match")]
    RequestIdMismatch,

    #[error("decrypted request does not match the expected typed request")]
    RequestMismatch,
}

pub fn preflight_client_request_envelope(
    envelope: &MailboxEnvelope,
    request: &AgentRequest,
    client_private_key: [u8; 32],
    agent_public_key: [u8; 32],
) -> Result<(), ClientEnvelopePreflightError> {
    envelope
        .validate(MailboxDirection::ClientToAgent)
        .map_err(ClientEnvelopePreflightError::Protocol)?;
    request
        .validate()
        .map_err(ClientEnvelopePreflightError::Protocol)?;

    if envelope.request_id != request.request_id {
        return Err(ClientEnvelopePreflightError::RequestIdMismatch);
    }

    let decoded_client_public = STANDARD
        .decode(&envelope.client_ephemeral_public_key)
        .map_err(ClientEnvelopePreflightError::Base64)?;
    let decoded_client_public: [u8; 32] = decoded_client_public
        .try_into()
        .map_err(|_| ClientEnvelopePreflightError::InvalidClientPublicKeyLength)?;

    if decoded_client_public != public_key_from_private(client_private_key) {
        return Err(ClientEnvelopePreflightError::ClientPublicKeyMismatch);
    }

    let decoded_nonce = STANDARD
        .decode(&envelope.nonce)
        .map_err(ClientEnvelopePreflightError::Base64)?;
    let decoded_nonce: [u8; 12] = decoded_nonce
        .try_into()
        .map_err(|_| ClientEnvelopePreflightError::InvalidNonceLength)?;

    let ciphertext = STANDARD
        .decode(&envelope.ciphertext)
        .map_err(ClientEnvelopePreflightError::Base64)?;

    let shared = shared_secret(client_private_key, agent_public_key)
        .map_err(ClientEnvelopePreflightError::Crypto)?;
    let key = derive_directional_key(
        &shared,
        &envelope.request_id,
        &envelope.agent_key_id,
        MailboxDirection::ClientToAgent,
    )
    .map_err(ClientEnvelopePreflightError::Crypto)?;
    let aad = envelope
        .aad()
        .map_err(ClientEnvelopePreflightError::Protocol)?;
    let plaintext = decrypt(&key, &decoded_nonce, aad.as_bytes(), &ciphertext)
        .map_err(ClientEnvelopePreflightError::Crypto)?;
    let decoded_request: AgentRequest =
        serde_json::from_slice(&plaintext).map_err(ClientEnvelopePreflightError::Json)?;
    decoded_request
        .validate()
        .map_err(ClientEnvelopePreflightError::Protocol)?;

    if decoded_request.request_id != envelope.request_id {
        return Err(ClientEnvelopePreflightError::RequestIdMismatch);
    }
    if &decoded_request != request {
        return Err(ClientEnvelopePreflightError::RequestMismatch);
    }

    Ok(())
}

pub fn public_key_from_private(private_key: [u8; 32]) -> [u8; 32] {
    x25519(private_key, X25519_BASEPOINT_BYTES)
}

pub fn shared_secret(
    private_key: [u8; 32],
    peer_public_key: [u8; 32],
) -> Result<[u8; 32], CryptoError> {
    let shared = x25519(private_key, peer_public_key);
    if shared == [0_u8; 32] {
        return Err(CryptoError::NonContributoryPeerKey);
    }
    Ok(shared)
}

pub fn derive_directional_key(
    shared_secret: &[u8; 32],
    request_id: &str,
    agent_key_id: &str,
    direction: MailboxDirection,
) -> Result<[u8; 32], CryptoError> {
    validate_request_id(request_id)?;
    validate_agent_key_id(agent_key_id)?;

    let info = format!(
        "repo={MAILBOX_REPOSITORY};request_id={request_id};agent_key_id={agent_key_id};direction={}",
        direction.aad_direction()
    );

    let hkdf = Hkdf::<Sha256>::new(Some(HKDF_SALT_V1), shared_secret);
    let mut key = [0_u8; 32];
    hkdf.expand(info.as_bytes(), &mut key)
        .map_err(|_| CryptoError::Kdf)?;
    Ok(key)
}

pub fn encrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = ChaCha20Poly1305::new_from_slice(key).map_err(|_| CryptoError::Encrypt)?;
    cipher
        .encrypt(
            &Array(*nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptoError::Encrypt)
}

pub fn decrypt(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    let cipher = ChaCha20Poly1305::new_from_slice(key).map_err(|_| CryptoError::Decrypt)?;
    cipher
        .decrypt(
            &Array(*nonce),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| CryptoError::Decrypt)
}

#[cfg(test)]
mod tests {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    use super::*;

    const REQUEST_ID: &str = "req_0123456789abcdef";
    const AGENT_KEY_ID: &str = "agent-key-1";

    fn decode_32(value: &str) -> [u8; 32] {
        STANDARD
            .decode(value)
            .expect("base64")
            .try_into()
            .expect("32 bytes")
    }

    #[test]
    fn python_and_rust_match_fixed_x25519_hkdf_chacha20poly1305_vector() {
        let agent_private = decode_32("AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=");
        let expected_agent_public = decode_32("B6N8vBQgk8i3VdwbEOhstCY3StFqqFPtC9/AsrhtHHw=");
        let client_private = decode_32("ISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0A=");
        let expected_client_public = decode_32("WGmv9FBUlzLLqu1eXfmzCm2jHLDldCutWtShp2jxpns=");
        let expected_shared = decode_32("qE3Hw8jwWLGy3EzR6bXcCnmH+ItqlWTN4zkfxCEVnnc=");
        let expected_client_to_agent = decode_32("mxrr1AaS6LTdtcCPzTIjt7ftRrAXE/OI+ogwsZfFEj0=");
        let expected_agent_to_client = decode_32("W/SGxPP0+HTrkyoK98xwHEnAYqKbTsCjZlbo8fmr+d4=");

        assert_eq!(
            public_key_from_private(agent_private),
            expected_agent_public
        );
        assert_eq!(
            public_key_from_private(client_private),
            expected_client_public
        );

        let client_shared =
            shared_secret(client_private, expected_agent_public).expect("client shared secret");
        let agent_shared =
            shared_secret(agent_private, expected_client_public).expect("agent shared secret");
        assert_eq!(client_shared, expected_shared);
        assert_eq!(agent_shared, expected_shared);

        let client_to_agent = derive_directional_key(
            &client_shared,
            REQUEST_ID,
            AGENT_KEY_ID,
            MailboxDirection::ClientToAgent,
        )
        .expect("client-to-agent key");
        let agent_to_client = derive_directional_key(
            &client_shared,
            REQUEST_ID,
            AGENT_KEY_ID,
            MailboxDirection::AgentToClient,
        )
        .expect("agent-to-client key");

        assert_eq!(client_to_agent, expected_client_to_agent);
        assert_eq!(agent_to_client, expected_agent_to_client);
        assert_ne!(client_to_agent, agent_to_client);

        let envelope = crate::MailboxEnvelope {
            schema: crate::MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: REQUEST_ID.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: AGENT_KEY_ID.to_owned(),
            client_ephemeral_public_key: STANDARD.encode(expected_client_public),
            nonce: "AAECAwQFBgcICQoL".to_owned(),
            ciphertext: String::new(),
        };
        let aad = envelope.aad().expect("aad");
        let nonce: [u8; 12] = STANDARD
            .decode("AAECAwQFBgcICQoL")
            .expect("nonce base64")
            .try_into()
            .expect("12 byte nonce");
        let plaintext = br#"{"schema":"okx.agent.request/v1","request_id":"req_0123456789abcdef","operation":{"type":"market_snapshot","instrument":"DOGE-USDT-SWAP"}}"#;

        let ciphertext =
            encrypt(&client_to_agent, &nonce, aad.as_bytes(), plaintext).expect("encrypt");
        assert_eq!(
            STANDARD.encode(&ciphertext),
            "42cQjQ+x3qsct69xRscwOYvBJ7WEfLKME5e83cAnthCc6uUJGoPh1uQBIP5/tCgWPKkE1gLMtYSTy/U3Tiuse/RpvA3MZFRWAptiNwhDOLkRN3vcxzA7jecvBezBUUd0tAMlGosLlIuyVA5xIC4AztiKeiutYqK8nUAYjro7BsmYmgzvrIyyGoJ91TIolZavZh18ifCmDarlag=="
        );

        let decrypted =
            decrypt(&client_to_agent, &nonce, aad.as_bytes(), &ciphertext).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn client_envelope_preflight_accepts_exact_typed_request() {
        let agent_private = decode_32("AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=");
        let agent_public = public_key_from_private(agent_private);
        let client_private = decode_32("ISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0A=");
        let client_public = public_key_from_private(client_private);
        let nonce = *b"0123456789ab";
        let request = crate::AgentRequest {
            schema: crate::AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: REQUEST_ID.to_owned(),
            operation: crate::AgentOperation::MarketSnapshot {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };

        let shared = shared_secret(client_private, agent_public).expect("shared");
        let key = derive_directional_key(
            &shared,
            REQUEST_ID,
            AGENT_KEY_ID,
            MailboxDirection::ClientToAgent,
        )
        .expect("key");
        let mut envelope = crate::MailboxEnvelope {
            schema: crate::MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: REQUEST_ID.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: AGENT_KEY_ID.to_owned(),
            client_ephemeral_public_key: STANDARD.encode(client_public),
            nonce: STANDARD.encode(nonce),
            ciphertext: String::new(),
        };
        let aad = envelope.aad().expect("aad");
        let plaintext = serde_json::to_vec(&request).expect("request json");
        envelope.ciphertext =
            STANDARD.encode(encrypt(&key, &nonce, aad.as_bytes(), &plaintext).expect("encrypt"));

        preflight_client_request_envelope(&envelope, &request, client_private, agent_public)
            .expect("preflight");
    }

    #[test]
    fn client_envelope_preflight_rejects_malformed_transport_encoding() {
        let client_private = [7_u8; 32];
        let request = crate::AgentRequest {
            schema: crate::AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: REQUEST_ID.to_owned(),
            operation: crate::AgentOperation::MarketSnapshot {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let envelope = crate::MailboxEnvelope {
            schema: crate::MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: REQUEST_ID.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: AGENT_KEY_ID.to_owned(),
            client_ephemeral_public_key: STANDARD.encode(public_key_from_private(client_private)),
            nonce: STANDARD.encode([0_u8; 12]),
            ciphertext: "=".to_owned(),
        };

        assert!(matches!(
            preflight_client_request_envelope(&envelope, &request, client_private, [9_u8; 32],),
            Err(ClientEnvelopePreflightError::Base64(_))
        ));
    }

    #[test]
    fn client_envelope_preflight_rejects_request_id_mismatch_before_post() {
        let client_private = [11_u8; 32];
        let request = crate::AgentRequest {
            schema: crate::AGENT_REQUEST_SCHEMA_V1.to_owned(),
            request_id: "req_aaaaaaaaaaaaaaaa".to_owned(),
            operation: crate::AgentOperation::MarketSnapshot {
                instrument: "DOGE-USDT-SWAP".to_owned(),
            },
        };
        let envelope = crate::MailboxEnvelope {
            schema: crate::MAILBOX_ENVELOPE_SCHEMA_V1.to_owned(),
            request_id: REQUEST_ID.to_owned(),
            direction: MailboxDirection::ClientToAgent,
            agent_key_id: AGENT_KEY_ID.to_owned(),
            client_ephemeral_public_key: STANDARD.encode(public_key_from_private(client_private)),
            nonce: STANDARD.encode([0_u8; 12]),
            ciphertext: STANDARD.encode([0_u8; 16]),
        };

        assert!(matches!(
            preflight_client_request_envelope(&envelope, &request, client_private, [9_u8; 32],),
            Err(ClientEnvelopePreflightError::RequestIdMismatch)
        ));
    }

    #[test]
    fn tampering_fails_authentication() {
        let key = [7_u8; 32];
        let nonce = [9_u8; 12];
        let aad = b"bound-metadata";
        let plaintext = b"private account result";

        let mut ciphertext = encrypt(&key, &nonce, aad, plaintext).expect("encrypt");
        ciphertext[0] ^= 1;

        assert!(matches!(
            decrypt(&key, &nonce, aad, &ciphertext),
            Err(CryptoError::Decrypt)
        ));
    }

    #[test]
    fn direction_derives_distinct_keys() {
        let shared = [3_u8; 32];
        let c2a = derive_directional_key(
            &shared,
            REQUEST_ID,
            AGENT_KEY_ID,
            MailboxDirection::ClientToAgent,
        )
        .expect("c2a");
        let a2c = derive_directional_key(
            &shared,
            REQUEST_ID,
            AGENT_KEY_ID,
            MailboxDirection::AgentToClient,
        )
        .expect("a2c");

        assert_ne!(c2a, a2c);
    }
}
