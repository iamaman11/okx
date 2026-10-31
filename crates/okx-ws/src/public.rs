use okx_api::{OkxEnvironment, RateBudget, RateOperationClass, RateThrottleEvidence};
use thiserror::Error;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message};

use crate::protocol::{
    InboundMessage, Subscription, parse_text, subscribe_payload, unsubscribe_payload,
};
use crate::wire::{LiveWsConnector, WsConnector, WsWire};

const MAX_SUBSCRIPTION_PAYLOAD_BYTES: usize = 64 * 1024;

pub struct PublicWsConnection {
    socket: Box<dyn WsWire>,
    rate_budget: RateBudget,
    connection_scope: String,
}

#[derive(Debug, Error)]
pub enum PublicWsError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] TungsteniteError),

    #[error("websocket JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("subscription set must not be empty")]
    EmptySubscriptionSet,

    #[error("subscription payload is {0} bytes; OKX limit is 64 KiB")]
    SubscriptionPayloadTooLarge(usize),

    #[error("OKX public websocket sent an unexpected binary frame")]
    UnexpectedBinaryFrame,

    #[error("OKX websocket rate/backpressure defer: {evidence:?}")]
    RateLimited { evidence: Box<RateThrottleEvidence> },
}

impl PublicWsConnection {
    pub async fn connect(environment: OkxEnvironment) -> Result<Self, PublicWsError> {
        Self::connect_with_rate_budget(
            environment,
            RateBudget::new(),
            "public-standalone".to_owned(),
        )
        .await
    }

    pub async fn connect_with_rate_budget(
        environment: OkxEnvironment,
        rate_budget: RateBudget,
        connection_scope: String,
    ) -> Result<Self, PublicWsError> {
        Self::connect_with_connector(environment, rate_budget, connection_scope, &LiveWsConnector)
            .await
    }

    pub async fn connect_with_connector(
        environment: OkxEnvironment,
        rate_budget: RateBudget,
        connection_scope: String,
        connector: &dyn WsConnector,
    ) -> Result<Self, PublicWsError> {
        let socket = connector
            .connect(environment.public_ws_url().to_owned())
            .await?;
        Ok(Self {
            socket,
            rate_budget,
            connection_scope,
        })
    }

    pub async fn subscribe(&mut self, subscriptions: &[Subscription]) -> Result<(), PublicWsError> {
        let payload = subscribe_payload(subscriptions)?;
        validate_subscription_payload(subscriptions, &payload)?;
        self.admit_control(RateOperationClass::WsSubscribe)?;
        self.socket.send(Message::Text(payload.into())).await?;
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        subscriptions: &[Subscription],
    ) -> Result<(), PublicWsError> {
        let payload = unsubscribe_payload(subscriptions)?;
        validate_subscription_payload(subscriptions, &payload)?;
        self.admit_control(RateOperationClass::WsUnsubscribe)?;
        self.socket.send(Message::Text(payload.into())).await?;
        Ok(())
    }

    fn admit_control(&self, operation: RateOperationClass) -> Result<(), PublicWsError> {
        let plan = self
            .rate_budget
            .ws_control_plan(operation, self.connection_scope.clone());
        self.rate_budget
            .admit(&plan)
            .map_err(|evidence| PublicWsError::RateLimited { evidence })
    }

    pub async fn send_application_ping(&mut self) -> Result<(), PublicWsError> {
        self.socket.send(Message::Text("ping".into())).await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<InboundMessage>, PublicWsError> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(None);
            };

            match message? {
                Message::Text(text) => return Ok(Some(parse_text(text.as_str())?)),
                Message::Ping(_) => {
                    // Tungstenite queues the protocol Pong while reading Ping.
                    // Flush the queued protocol response; do not hand-roll RFC6455 frames.
                    self.socket.flush().await?;
                }
                Message::Pong(_) => {}
                Message::Close(_) => return Ok(None),
                Message::Binary(_) => return Err(PublicWsError::UnexpectedBinaryFrame),
                _ => {}
            }
        }
    }

    pub async fn close(&mut self) -> Result<(), PublicWsError> {
        self.socket.close().await?;
        Ok(())
    }
}

fn validate_subscription_payload(
    subscriptions: &[Subscription],
    payload: &str,
) -> Result<(), PublicWsError> {
    if subscriptions.is_empty() {
        return Err(PublicWsError::EmptySubscriptionSet);
    }
    if payload.len() > MAX_SUBSCRIPTION_PAYLOAD_BYTES {
        return Err(PublicWsError::SubscriptionPayloadTooLarge(payload.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{protocol::PublicChannel, wire::test_support::ScriptedConnector};

    #[tokio::test]
    async fn public_recorded_frames_use_real_subscription_and_parser() {
        let script = ScriptedConnector::default();
        script
            .incoming
            .lock()
            .unwrap()
            .push_back(Message::Text("pong".into()));
        let mut connection = PublicWsConnection::connect_with_connector(
            OkxEnvironment::new(okx_api::Region::Global, false),
            RateBudget::new(),
            "public-scripted".to_owned(),
            &script,
        )
        .await
        .unwrap();
        connection
            .subscribe(&[Subscription::instrument(
                PublicChannel::Tickers,
                "BTC-USDT-SWAP",
            )])
            .await
            .unwrap();
        assert!(matches!(
            connection.next().await.unwrap(),
            Some(InboundMessage::Pong)
        ));
        assert_eq!(script.sent.lock().unwrap().len(), 1);
        assert!(script.urls.lock().unwrap()[0].starts_with("wss://"));
    }

    #[tokio::test]
    async fn binary_frame_is_not_promoted_into_market_evidence() {
        let script = ScriptedConnector::default();
        script
            .incoming
            .lock()
            .unwrap()
            .push_back(Message::Binary(vec![0, 1].into()));
        let mut connection = PublicWsConnection::connect_with_connector(
            OkxEnvironment::new(okx_api::Region::Global, false),
            RateBudget::new(),
            "public-binary".into(),
            &script,
        )
        .await
        .unwrap();
        assert!(matches!(
            connection.next().await,
            Err(PublicWsError::UnexpectedBinaryFrame)
        ));
    }

    #[test]
    fn subscription_payload_validation_is_fail_closed() {
        assert!(matches!(
            validate_subscription_payload(&[], "{}"),
            Err(PublicWsError::EmptySubscriptionSet)
        ));

        let subscriptions = [Subscription::instrument(
            PublicChannel::Tickers,
            "DOGE-USDT-SWAP",
        )];
        let too_large = "x".repeat(MAX_SUBSCRIPTION_PAYLOAD_BYTES + 1);
        assert!(matches!(
            validate_subscription_payload(&subscriptions, &too_large),
            Err(PublicWsError::SubscriptionPayloadTooLarge(_))
        ));
    }
}
