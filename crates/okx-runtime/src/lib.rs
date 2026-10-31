pub mod private;
pub mod public;

// Transport-only composition; runtime state machine remains the same.
pub use okx_ws::wire::{WsCall, WsReceive, WsWire};
pub use okx_ws::{LiveWsConnector, WsConnect, WsConnector};

pub use private::{
    PRIVATE_EVENT_JOURNAL_CAPACITY, PRIVATE_RECONNECT_BACKOFF_SECONDS, PRIVATE_WS_STATUS_SCHEMA_V1,
    PrivateConnectionState, PrivateConvergenceCursor, PrivateConvergenceError,
    PrivateConvergenceWindow, PrivateRuntimeError, PrivateRuntimeState, PrivateWsCoordinator,
    PrivateWsEvent, PrivateWsHandle, PrivateWsStatus, baseline_private_subscriptions,
    private_connection_fingerprint, private_reconnect_delay,
};
pub use public::{
    PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2, PublicConnectionState, PublicMarketWakeup,
    PublicQualitySnapshot, PublicRuntimeError, PublicRuntimeState, PublicWsCoordinator,
    PublicWsHandle, RECONNECT_BACKOFF_SECONDS, reconnect_delay,
};
