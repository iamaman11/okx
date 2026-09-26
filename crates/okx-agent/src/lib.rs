pub mod config;
pub mod identity;
pub mod once;
pub mod runtime;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("protocol error: {0}")]
    Protocol(#[from] okx_protocol::ProtocolError),

    #[error("mailbox cryptography error: {0}")]
    Crypto(#[from] okx_protocol::crypto::CryptoError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error("system random source failed: {0}")]
    Random(String),

    #[error("native secret store error: {0}")]
    SecretStore(String),

    #[error("agent identity '{0}' already exists")]
    IdentityAlreadyExists(String),

    #[error("agent identity '{0}' was not found")]
    IdentityNotFound(String),

    #[error("stored agent private key has invalid length {0}; expected 32 bytes")]
    InvalidPrivateKeyLength(usize),

    #[error("mailbox envelope targets agent key '{actual}', expected '{expected}'")]
    AgentKeyMismatch { expected: String, actual: String },

    #[error("decrypted request_id does not match its mailbox envelope")]
    RequestIdMismatch,

    #[error("native Windows secret storage is unavailable on this platform")]
    UnsupportedSecretStorePlatform,
}

pub type AgentResult<T> = Result<T, AgentError>;
