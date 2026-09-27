mod coordinator;
mod decode;
mod state;
mod subscriptions;

use okx_observation::{MarketStreamError, ReferenceError};
use okx_ws::{PublicChannel, PublicWsError};
use thiserror::Error;

pub use coordinator::{
    PublicWsCoordinator, PublicWsHandle, RECONNECT_BACKOFF_SECONDS, reconnect_delay,
};
pub use state::{
    PublicConnectionState, PublicMarketOverviewView, PublicQualitySnapshot, PublicRuntimeState,
};

pub const PUBLIC_SNAPSHOT_QUALITY_SCHEMA_V2: &str = "okx.snapshot-quality/v2";

#[derive(Debug, Error)]
pub enum PublicRuntimeError {
    #[error("public websocket transport error: {0}")]
    WebSocket(#[from] PublicWsError),

    #[error("public websocket JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("reference data error: {0}")]
    Reference(#[from] ReferenceError),

    #[error("streaming market state error: {0}")]
    Stream(#[from] MarketStreamError),

    #[error("public runtime command channel is closed")]
    CommandChannelClosed,

    #[error("public runtime command queue is full")]
    CommandQueueFull,

    #[error("system clock is before Unix epoch")]
    ClockBeforeEpoch,

    #[error("instrument '{0}' is not present in reference data")]
    InstrumentNotFound(String),

    #[error("instrument '{0}' has no underlying/index id in reference data")]
    MissingUnderlying(String),

    #[error("instrument '{instrument_id}' has unknown funding semantics")]
    UnknownFundingSemantics { instrument_id: String },

    #[error("books message action is missing or unsupported: {0:?}")]
    UnsupportedBookAction(Option<String>),

    #[error("websocket data for channel {channel:?} is missing instId")]
    MissingChannelInstrument { channel: PublicChannel },

    #[error("live market state for instrument '{0}' is not initialized")]
    MarketStateNotInitialized(String),

    #[error("current live state for instrument '{0}' is not FRESH")]
    MarketNotFresh(String),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use okx_api::{OkxEnvironment, PublicInstrument};
    use okx_observation::ReferenceRegistry;
    use okx_ws::{PublicChannel, Subscription};

    use super::{
        PublicRuntimeError, PublicWsCoordinator,
        decode::{RawBookData, decode_book},
        reconnect_delay,
        subscriptions::{baseline_subscriptions, connection_fingerprint, required_subscriptions},
    };

    fn instrument(id: &str, instrument_type: &str, rule_type: &str) -> PublicInstrument {
        PublicInstrument {
            instrument_type: instrument_type.to_owned(),
            instrument_id: id.to_owned(),
            instrument_family: "DOGE-USDT".to_owned(),
            underlying: "DOGE-USDT".to_owned(),
            state: "live".to_owned(),
            rule_type: rule_type.to_owned(),
            base_currency: String::new(),
            quote_currency: String::new(),
            settle_currency: "USDT".to_owned(),
            tick_size: "0.00001".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: "1000000".to_owned(),
            max_market_size: "100000".to_owned(),
            max_limit_amount: String::new(),
            max_market_amount: String::new(),
            contract_type: "linear".to_owned(),
            contract_value: "1000".to_owned(),
            contract_value_currency: "DOGE".to_owned(),
            fee_group_id: "4".to_owned(),
            lever: "50".to_owned(),
            list_time: "1700000000000".to_owned(),
            expiry_time: String::new(),
        }
    }

    fn reference(instrument: PublicInstrument) -> ReferenceRegistry {
        ReferenceRegistry::from_public("2026-09-27T00:00:00.000Z", vec![instrument])
            .expect("reference")
    }

    #[test]
    fn connection_fingerprint_is_stable_and_does_not_expose_conn_id() {
        let first = connection_fingerprint("conn-secret-value");
        let second = connection_fingerprint("conn-secret-value");
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert!(!first.contains("conn-secret-value"));
    }

    #[tokio::test]
    async fn demand_registration_is_local_idempotent_and_network_independent() {
        let reference = reference(instrument("DOGE-USDT-SWAP", "SWAP", "normal"));
        let (_coordinator, handle) = PublicWsCoordinator::new(
            OkxEnvironment::new(okx_api::Region::Global, false),
            reference,
        );

        handle
            .demand_instrument("DOGE-USDT-SWAP")
            .await
            .expect("first demand");
        handle
            .demand_instrument("DOGE-USDT-SWAP")
            .await
            .expect("repeat demand");

        let shared = handle.state();
        let state = shared.read().await;
        assert!(state.markets.contains_key("DOGE-USDT-SWAP"));
        assert_eq!(state.markets.len(), 1);
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_starts_at_one_second() {
        assert_eq!(reconnect_delay(0), Duration::from_secs(1));
        assert_eq!(reconnect_delay(1), Duration::from_secs(5));
        assert_eq!(reconnect_delay(2), Duration::from_secs(15));
        assert_eq!(reconnect_delay(3), Duration::from_secs(30));
        assert_eq!(reconnect_delay(4), Duration::from_secs(60));
        assert_eq!(reconnect_delay(99), Duration::from_secs(60));
    }

    #[test]
    fn baseline_and_demand_subscriptions_are_reference_driven() {
        let reference = reference(instrument("DOGE-USDT-SWAP", "SWAP", "normal"));
        let required = required_subscriptions(&reference, "DOGE-USDT-SWAP").expect("subscriptions");

        assert!(required.contains(&Subscription::instrument(
            PublicChannel::FundingRate,
            "DOGE-USDT-SWAP"
        )));
        assert!(required.contains(&Subscription::instrument(
            PublicChannel::IndexTickers,
            "DOGE-USDT"
        )));
        assert!(required.contains(&Subscription::instrument(
            PublicChannel::Books,
            "DOGE-USDT-SWAP"
        )));
        assert_eq!(baseline_subscriptions().len(), 2);
    }

    #[test]
    fn ordinary_future_does_not_subscribe_funding_but_xperp_does() {
        let ordinary = reference(instrument("DOGE-USDT-261225", "FUTURES", "normal"));
        let ordinary_required =
            required_subscriptions(&ordinary, "DOGE-USDT-261225").expect("ordinary");
        assert!(
            !ordinary_required
                .iter()
                .any(|subscription| subscription.channel == PublicChannel::FundingRate)
        );

        let xperp = reference(instrument("DOGE-USDT-XPERP", "FUTURES", "xperp"));
        let xperp_required = required_subscriptions(&xperp, "DOGE-USDT-XPERP").expect("xperp");
        assert!(
            xperp_required
                .iter()
                .any(|subscription| subscription.channel == PublicChannel::FundingRate)
        );
    }

    #[test]
    fn unknown_funding_semantics_fail_closed() {
        let reference = reference(instrument("DOGE-USDT-FUTURE", "FUTURES", "future_rule"));
        assert!(matches!(
            required_subscriptions(&reference, "DOGE-USDT-FUTURE"),
            Err(PublicRuntimeError::UnknownFundingSemantics { .. })
        ));
    }

    #[test]
    fn standard_books_array_maps_price_size_and_order_count_only() {
        let raw: RawBookData = serde_json::from_str(
            r#"{
                "asks":[["0.12346","10","0","3"]],
                "bids":[["0.12345","12","0","4"]],
                "ts":"1790467200127",
                "seqId":10,
                "prevSeqId":-1,
                "checksum":0
            }"#,
        )
        .expect("book");
        let book = decode_book(raw);

        assert_eq!(book.asks[0].price, "0.12346");
        assert_eq!(book.asks[0].size, "10");
        assert_eq!(book.asks[0].order_count.as_deref(), Some("3"));
        assert_eq!(book.bids[0].order_count.as_deref(), Some("4"));
        assert_eq!(book.prev_seq_id, -1);
    }
}
