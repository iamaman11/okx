use thiserror::Error;

#[derive(Debug, Error)]
pub enum OkxError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("OKX API error {code}: {message}")]
    Api { code: String, message: String },

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("cryptographic error: {0}")]
    Crypto(String),
}
