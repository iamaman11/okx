use thiserror::Error;

use crate::RateThrottleEvidence;

#[derive(Debug, Error)]
pub enum OkxError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("OKX API error {code}: {message}")]
    Api { code: String, message: String },

    #[error("OKX rate/backpressure defer: {evidence:?}")]
    RateLimited { evidence: Box<RateThrottleEvidence> },

    #[error("OKX response error: {0}")]
    Response(String),

    /// HTTP 401/403 is classified without echoing the untrusted response
    /// body or any request credential. The OKX numeric code may be absent.
    #[error("OKX private REST authentication rejected: HTTP {status}; code={okx_code}")]
    AuthenticationRejected { status: u16, okx_code: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("cryptographic error: {0}")]
    Crypto(String),

    #[error("OKX clock error: {0}")]
    Clock(String),
}

impl OkxError {
    pub fn rate_throttle_evidence(&self) -> Option<&RateThrottleEvidence> {
        match self {
            Self::RateLimited { evidence } => Some(evidence.as_ref()),
            _ => None,
        }
    }
}
