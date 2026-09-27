use thiserror::Error;

#[derive(Debug, Error)]
pub enum OkxError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("OKX API error {code}: {message}")]
    Api { code: String, message: String },

    #[error("OKX response error: {0}")]
    Response(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("cryptographic error: {0}")]
    Crypto(String),
}
