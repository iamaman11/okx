mod coordinator;
mod state;

use okx_api::OkxError;
use okx_ws::PrivateWsError;
use thiserror::Error;

pub use coordinator::{
    PRIVATE_RECONNECT_BACKOFF_SECONDS, PrivateWsCoordinator, PrivateWsHandle,
    private_reconnect_delay,
};
pub use state::{
    PRIVATE_EVENT_JOURNAL_CAPACITY, PRIVATE_WS_STATUS_SCHEMA_V1, PrivateConnectionState,
    PrivateConvergenceCursor, PrivateConvergenceError, PrivateConvergenceWindow,
    PrivateRuntimeState, PrivateWsEvent, PrivateWsStatus, baseline_private_subscriptions,
    connection_fingerprint as private_connection_fingerprint,
};

#[derive(Debug, Error)]
pub enum PrivateRuntimeError {
    #[error("private websocket transport error: {0}")]
    WebSocket(PrivateWsError),

    #[error("private websocket authentication material error: {0}")]
    Api(#[from] OkxError),

    #[error("private websocket JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
