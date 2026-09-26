use chacha20poly1305::{
    ChaCha20Poly1305,
    aead::{Aead, KeyInit, Payload, array::Array},
};
use hkdf::Hkdf;
use sha2::Sha256;
use thiserror::Error;
use x25519_dalek::{X25519_BASEPOINT_BYTES, x25519};

use crate::{
    MAILBOX_REPOSITORY, MailboxDirection, ProtocolError, validate_agent_key_id, validate_request_id,
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
            Array(*nonce),
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
            Array(*nonce),
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
