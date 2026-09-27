use futures_util::{SinkExt, StreamExt};
use okx_api::OkxEnvironment;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as TungsteniteError, Message},
};

use crate::protocol::{
    InboundMessage, Subscription, parse_text, subscribe_payload, unsubscribe_payload,
};

const MAX_SUBSCRIPTION_PAYLOAD_BYTES: usize = 64 * 1024;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct PublicWsConnection {
    socket: Socket,
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
}

impl PublicWsConnection {
    pub async fn connect(environment: OkxEnvironment) -> Result<Self, PublicWsError> {
        let (socket, _response) = connect_async(environment.public_ws_url()).await?;
        Ok(Self { socket })
    }

    pub async fn subscribe(&mut self, subscriptions: &[Subscription]) -> Result<(), PublicWsError> {
        let payload = subscribe_payload(subscriptions)?;
        validate_subscription_payload(subscriptions, &payload)?;
        self.socket.send(Message::Text(payload.into())).await?;
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        subscriptions: &[Subscription],
    ) -> Result<(), PublicWsError> {
        let payload = unsubscribe_payload(subscriptions)?;
        validate_subscription_payload(subscriptions, &payload)?;
        self.socket.send(Message::Text(payload.into())).await?;
        Ok(())
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
        self.socket.close(None).await?;
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
    use crate::protocol::PublicChannel;

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
