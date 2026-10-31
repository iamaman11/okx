//! Raw WebSocket delivery seam shared by public, private and business streams.
//! The existing protocol parser, login, subscriptions and generation checks remain
//! the only production interpretation of these frames.
use futures_util::{SinkExt, StreamExt};
use std::{future::Future, pin::Pin};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as TungsteniteError, Message},
};

type LiveSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsCall<'a> = Pin<Box<dyn Future<Output = Result<(), TungsteniteError>> + Send + 'a>>;
pub type WsReceive<'a> =
    Pin<Box<dyn Future<Output = Option<Result<Message, TungsteniteError>>> + Send + 'a>>;
pub type WsConnect<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn WsWire>, TungsteniteError>> + Send + 'a>>;

/// Transport only: never classifies account sources, risk or order outcomes.
pub trait WsWire: Send {
    fn send(&mut self, message: Message) -> WsCall<'_>;
    fn flush(&mut self) -> WsCall<'_>;
    fn next(&mut self) -> WsReceive<'_>;
    fn close(&mut self) -> WsCall<'_>;
}

impl WsWire for LiveSocket {
    fn send(&mut self, message: Message) -> WsCall<'_> {
        Box::pin(async move { SinkExt::send(self, message).await })
    }
    fn flush(&mut self) -> WsCall<'_> {
        Box::pin(async move { SinkExt::flush(self).await })
    }
    fn next(&mut self) -> WsReceive<'_> {
        Box::pin(async move { StreamExt::next(self).await })
    }
    fn close(&mut self) -> WsCall<'_> {
        Box::pin(async move { WebSocketStream::close(self, None).await })
    }
}

pub trait WsConnector: Send + Sync {
    fn connect(&self, url: String) -> WsConnect<'_>;
}

pub struct LiveWsConnector;
impl WsConnector for LiveWsConnector {
    fn connect(&self, url: String) -> WsConnect<'_> {
        Box::pin(async move {
            let (socket, _response) = connect_async(url).await?;
            Ok(Box::new(socket) as Box<dyn WsWire>)
        })
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };
    /// One-shot inbound recordings and outbound raw frame trace, no exchange state.
    #[derive(Clone, Default)]
    pub(crate) struct ScriptedConnector {
        pub incoming: Arc<Mutex<VecDeque<Message>>>,
        pub sent: Arc<Mutex<Vec<Message>>>,
        pub urls: Arc<Mutex<Vec<String>>>,
    }
    struct ScriptedWire {
        script: ScriptedConnector,
    }
    impl WsWire for ScriptedWire {
        fn send(&mut self, message: Message) -> WsCall<'_> {
            Box::pin(async move {
                self.script.sent.lock().unwrap().push(message);
                Ok(())
            })
        }
        fn flush(&mut self) -> WsCall<'_> {
            Box::pin(async { Ok(()) })
        }
        fn next(&mut self) -> WsReceive<'_> {
            Box::pin(async move { self.script.incoming.lock().unwrap().pop_front().map(Ok) })
        }
        fn close(&mut self) -> WsCall<'_> {
            Box::pin(async { Ok(()) })
        }
    }
    impl WsConnector for ScriptedConnector {
        fn connect(&self, url: String) -> WsConnect<'_> {
            Box::pin(async move {
                self.urls.lock().unwrap().push(url);
                Ok(Box::new(ScriptedWire {
                    script: self.clone(),
                }) as Box<dyn WsWire>)
            })
        }
    }
}
