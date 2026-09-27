use futures_util::{SinkExt, StreamExt};
use okx_api::{OkxEnvironment, WsLoginMaterial};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as TungsteniteError, Message},
};

use crate::private_protocol::{
    PrivateInboundMessage, PrivateSubscription, login_payload, parse_private_text,
    private_subscribe_payload, private_unsubscribe_payload,
};

const MAX_SUBSCRIPTION_PAYLOAD_BYTES: usize = 64 * 1024;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct PrivateWsConnection {
    socket: Socket,
}

#[derive(Debug, Error)]
pub enum PrivateWsError {
    #[error("websocket error: {0}")]
    WebSocket(#[from] TungsteniteError),

    #[error("websocket JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("subscription set must not be empty")]
    EmptySubscriptionSet,

    #[error("subscription payload is {0} bytes; OKX limit is 64 KiB")]
    SubscriptionPayloadTooLarge(usize),

    #[error("OKX private websocket sent an unexpected binary frame")]
    UnexpectedBinaryFrame,
}

impl PrivateWsConnection {
    pub async fn connect(environment: OkxEnvironment) -> Result<Self, PrivateWsError> {
        let (socket, _response) = connect_async(environment.private_ws_url()).await?;
        Ok(Self { socket })
    }

    pub async fn login(&mut self, material: &WsLoginMaterial) -> Result<(), PrivateWsError> {
        self.socket
            .send(Message::Text(login_payload(material)?.into()))
            .await?;
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        subscriptions: &[PrivateSubscription],
    ) -> Result<(), PrivateWsError> {
        let payload = private_subscribe_payload(subscriptions)?;
        validate_subscription_payload(subscriptions, &payload)?;
        self.socket.send(Message::Text(payload.into())).await?;
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        subscriptions: &[PrivateSubscription],
    ) -> Result<(), PrivateWsError> {
        let payload = private_unsubscribe_payload(subscriptions)?;
        validate_subscription_payload(subscriptions, &payload)?;
        self.socket.send(Message::Text(payload.into())).await?;
        Ok(())
    }

    pub async fn send_application_ping(&mut self) -> Result<(), PrivateWsError> {
        self.socket.send(Message::Text("ping".into())).await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<PrivateInboundMessage>, PrivateWsError> {
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(None);
            };

            match message? {
                Message::Text(text) => return Ok(Some(parse_private_text(text.as_str())?)),
                Message::Ping(_) => {
                    self.socket.flush().await?;
                }
                Message::Pong(_) => {}
                Message::Close(_) => return Ok(None),
                Message::Binary(_) => return Err(PrivateWsError::UnexpectedBinaryFrame),
                _ => {}
            }
        }
    }

    pub async fn close(&mut self) -> Result<(), PrivateWsError> {
        self.socket.close(None).await?;
        Ok(())
    }
}

fn validate_subscription_payload(
    subscriptions: &[PrivateSubscription],
    payload: &str,
) -> Result<(), PrivateWsError> {
    if subscriptions.is_empty() {
        return Err(PrivateWsError::EmptySubscriptionSet);
    }
    if payload.len() > MAX_SUBSCRIPTION_PAYLOAD_BYTES {
        return Err(PrivateWsError::SubscriptionPayloadTooLarge(payload.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PrivateSubscription;

    #[test]
    fn private_subscription_payload_validation_is_fail_closed() {
        assert!(matches!(
            validate_subscription_payload(&[], "{}"),
            Err(PrivateWsError::EmptySubscriptionSet)
        ));

        let subscriptions = [PrivateSubscription::orders_any()];
        let too_large = "x".repeat(MAX_SUBSCRIPTION_PAYLOAD_BYTES + 1);
        assert!(matches!(
            validate_subscription_payload(&subscriptions, &too_large),
            Err(PrivateWsError::SubscriptionPayloadTooLarge(_))
        ));
    }
}
