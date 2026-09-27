use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
};

use crate::{
    InstrumentType, OkxEnvironment, OkxError, PublicFundingRate, PublicIndexTicker,
    PublicInstrument, PublicMarkPrice, PublicOpenInterest, PublicTicker,
};

pub const OKX_TEXT_PING: &str = "ping";
pub const OKX_TEXT_PONG: &str = "pong";
pub const OKX_SERVICE_UPGRADE_CODE: &str = "64008";
pub const OKX_SUBSCRIPTION_MESSAGE_MAX_BYTES: usize = 64 * 1024;

pub type PublicWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOperation {
    Subscribe,
    Unsubscribe,
}

impl WsOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Subscribe => "subscribe",
            Self::Unsubscribe => "unsubscribe",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicSubscription {
    Instruments {
        instrument_type: InstrumentType,
    },
    Tickers {
        instrument_id: String,
    },
    MarkPrice {
        instrument_id: String,
    },
    IndexTickers {
        index_id: String,
    },
    FundingRate {
        instrument_id: String,
    },
    OpenInterest {
        instrument_type: InstrumentType,
        instrument_id: String,
    },
    Books {
        instrument_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsArg {
    pub channel: String,
    #[serde(rename = "instType", default, skip_serializing_if = "Option::is_none")]
    pub instrument_type: Option<String>,
    #[serde(rename = "instFamily", default, skip_serializing_if = "Option::is_none")]
    pub instrument_family: Option<String>,
    #[serde(rename = "instId", default, skip_serializing_if = "Option::is_none")]
    pub instrument_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsEventKind {
    Subscribe,
    Unsubscribe,
    Error,
    Notice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsEvent {
    pub id: Option<String>,
    pub kind: WsEventKind,
    pub arg: Option<WsArg>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub connection_id: String,
}

impl WsEvent {
    pub fn is_service_upgrade_notice(&self) -> bool {
        self.kind == WsEventKind::Notice
            && self.code.as_deref() == Some(OKX_SERVICE_UPGRADE_CODE)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderBookAction {
    Snapshot,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct WsOrderBook {
    #[serde(default)]
    pub asks: Vec<[String; 4]>,
    #[serde(default)]
    pub bids: Vec<[String; 4]>,
    #[serde(default)]
    pub ts: String,
    #[serde(default)]
    pub checksum: i64,
    #[serde(rename = "prevSeqId")]
    pub previous_sequence_id: i64,
    #[serde(rename = "seqId")]
    pub sequence_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsPush {
    Instruments {
        arg: WsArg,
        data: Vec<PublicInstrument>,
    },
    Tickers {
        arg: WsArg,
        data: Vec<PublicTicker>,
    },
    MarkPrice {
        arg: WsArg,
        data: Vec<PublicMarkPrice>,
    },
    IndexTickers {
        arg: WsArg,
        data: Vec<PublicIndexTicker>,
    },
    FundingRate {
        arg: WsArg,
        data: Vec<PublicFundingRate>,
    },
    OpenInterest {
        arg: WsArg,
        data: Vec<PublicOpenInterest>,
    },
    Books {
        arg: WsArg,
        action: OrderBookAction,
        data: Vec<WsOrderBook>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsInbound {
    Pong,
    Event(WsEvent),
    Push(WsPush),
}

#[derive(Debug, Serialize)]
struct WireRequest {
    id: String,
    op: String,
    args: Vec<WsArg>,
}

#[derive(Debug, Deserialize)]
struct RawMessage {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    arg: Option<WsArg>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(rename = "connId", default)]
    connection_id: Option<String>,
    #[serde(default)]
    action: Option<OrderBookAction>,
    #[serde(default)]
    data: Value,
}

impl PublicSubscription {
    fn wire_arg(&self) -> WsArg {
        let empty = || WsArg {
            channel: String::new(),
            instrument_type: None,
            instrument_family: None,
            instrument_id: None,
        };

        match self {
            Self::Instruments { instrument_type } => WsArg {
                channel: "instruments".to_owned(),
                instrument_type: Some(instrument_type.to_string()),
                ..empty()
            },
            Self::Tickers { instrument_id } => WsArg {
                channel: "tickers".to_owned(),
                instrument_id: Some(instrument_id.clone()),
                ..empty()
            },
            Self::MarkPrice { instrument_id } => WsArg {
                channel: "mark-price".to_owned(),
                instrument_id: Some(instrument_id.clone()),
                ..empty()
            },
            Self::IndexTickers { index_id } => WsArg {
                channel: "index-tickers".to_owned(),
                instrument_id: Some(index_id.clone()),
                ..empty()
            },
            Self::FundingRate { instrument_id } => WsArg {
                channel: "funding-rate".to_owned(),
                instrument_id: Some(instrument_id.clone()),
                ..empty()
            },
            Self::OpenInterest {
                instrument_type,
                instrument_id,
            } => WsArg {
                channel: "open-interest".to_owned(),
                instrument_type: Some(instrument_type.to_string()),
                instrument_id: Some(instrument_id.clone()),
                ..empty()
            },
            Self::Books { instrument_id } => WsArg {
                channel: "books".to_owned(),
                instrument_id: Some(instrument_id.clone()),
                ..empty()
            },
        }
    }
}

pub fn encode_subscription_request(
    id: &str,
    operation: WsOperation,
    subscriptions: &[PublicSubscription],
) -> Result<String, OkxError> {
    if id.is_empty()
        || id.len() > 32
        || !id.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(OkxError::Config(
            "WebSocket request id must be 1..=32 ASCII alphanumeric characters".to_owned(),
        ));
    }
    if subscriptions.is_empty() {
        return Err(OkxError::Config(
            "WebSocket subscription request cannot be empty".to_owned(),
        ));
    }

    let encoded = serde_json::to_string(&WireRequest {
        id: id.to_owned(),
        op: operation.as_str().to_owned(),
        args: subscriptions
            .iter()
            .map(PublicSubscription::wire_arg)
            .collect(),
    })?;

    if encoded.len() > OKX_SUBSCRIPTION_MESSAGE_MAX_BYTES {
        return Err(OkxError::Config(
            "WebSocket subscription request exceeds OKX 64 KiB limit".to_owned(),
        ));
    }

    Ok(encoded)
}

pub fn decode_ws_text(text: &str) -> Result<WsInbound, OkxError> {
    if text == OKX_TEXT_PONG {
        return Ok(WsInbound::Pong);
    }

    let raw: RawMessage = serde_json::from_str(text)?;

    if let Some(event) = raw.event.as_deref() {
        let kind = match event {
            "subscribe" => WsEventKind::Subscribe,
            "unsubscribe" => WsEventKind::Unsubscribe,
            "error" => WsEventKind::Error,
            "notice" => WsEventKind::Notice,
            other => {
                return Err(OkxError::Response(format!(
                    "unsupported OKX WebSocket event '{other}'"
                )));
            }
        };
        return Ok(WsInbound::Event(WsEvent {
            id: raw.id,
            kind,
            arg: raw.arg,
            code: raw.code,
            message: raw.msg,
            connection_id: raw.connection_id.unwrap_or_default(),
        }));
    }

    let arg = raw
        .arg
        .ok_or_else(|| OkxError::Response("WebSocket data push is missing arg".to_owned()))?;

    let push = match arg.channel.as_str() {
        "instruments" => WsPush::Instruments {
            arg,
            data: decode_data(raw.data)?,
        },
        "tickers" => WsPush::Tickers {
            arg,
            data: decode_data(raw.data)?,
        },
        "mark-price" => WsPush::MarkPrice {
            arg,
            data: decode_data(raw.data)?,
        },
        "index-tickers" => WsPush::IndexTickers {
            arg,
            data: decode_data(raw.data)?,
        },
        "funding-rate" => WsPush::FundingRate {
            arg,
            data: decode_data(raw.data)?,
        },
        "open-interest" => WsPush::OpenInterest {
            arg,
            data: decode_data(raw.data)?,
        },
        "books" => WsPush::Books {
            arg,
            action: raw.action.ok_or_else(|| {
                OkxError::Response("books push is missing snapshot/update action".to_owned())
            })?,
            data: decode_data(raw.data)?,
        },
        other => {
            return Err(OkxError::Response(format!(
                "unsupported OKX WebSocket channel '{other}'"
            )));
        }
    };

    Ok(WsInbound::Push(push))
}

pub async fn connect_public_ws(
    environment: OkxEnvironment,
) -> Result<PublicWsStream, OkxError> {
    let (stream, _) = connect_async(environment.public_ws_url()).await?;
    Ok(stream)
}

fn decode_data<T>(value: Value) -> Result<Vec<T>, OkxError>
where
    T: for<'de> Deserialize<'de>,
{
    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_typed_subscriptions() {
        let encoded = encode_subscription_request(
            "m3a1",
            WsOperation::Subscribe,
            &[
                PublicSubscription::Instruments {
                    instrument_type: InstrumentType::Swap,
                },
                PublicSubscription::Books {
                    instrument_id: "DOGE-USDT-SWAP".to_owned(),
                },
            ],
        )
        .expect("request");

        let value: Value = serde_json::from_str(&encoded).expect("json");
        assert_eq!(value["op"], "subscribe");
        assert_eq!(value["args"][0]["channel"], "instruments");
        assert_eq!(value["args"][0]["instType"], "SWAP");
        assert_eq!(value["args"][1]["channel"], "books");
        assert_eq!(value["args"][1]["instId"], "DOGE-USDT-SWAP");
    }

    #[test]
    fn decodes_subscription_ack_and_upgrade_notice() {
        let ack = decode_ws_text(
            r#"{"id":"m3a1","event":"subscribe","arg":{"channel":"tickers","instId":"DOGE-USDT-SWAP"},"connId":"abc123"}"#,
        )
        .expect("ack");
        let WsInbound::Event(ack) = ack else {
            panic!("event expected");
        };
        assert_eq!(ack.kind, WsEventKind::Subscribe);
        assert_eq!(ack.connection_id, "abc123");

        let notice = decode_ws_text(
            r#"{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade. Please reconnect.","connId":"abc123"}"#,
        )
        .expect("notice");
        let WsInbound::Event(notice) = notice else {
            panic!("event expected");
        };
        assert!(notice.is_service_upgrade_notice());
    }

    #[test]
    fn decodes_text_pong() {
        assert_eq!(decode_ws_text("pong").expect("pong"), WsInbound::Pong);
    }

    #[test]
    fn routes_market_pushes_by_channel() {
        let push = decode_ws_text(
            r#"{"arg":{"channel":"tickers","instId":"DOGE-USDT-SWAP"},"data":[{"instType":"SWAP","instId":"DOGE-USDT-SWAP","last":"0.0962","lastSz":"1","askPx":"0.0963","askSz":"2","bidPx":"0.0961","bidSz":"3","open24h":"0.1","high24h":"0.11","low24h":"0.09","volCcy24h":"1000","vol24h":"2000","ts":"1790470000000"}]}"#,
        )
        .expect("push");

        let WsInbound::Push(WsPush::Tickers { data, .. }) = push else {
            panic!("ticker push expected");
        };
        assert_eq!(data[0].instrument_id, "DOGE-USDT-SWAP");
        assert_eq!(data[0].bid_price, "0.0961");
    }

    #[test]
    fn books_keep_sequence_ids_and_ignore_checksum_as_authority() {
        let push = decode_ws_text(
            r#"{"arg":{"channel":"books","instId":"DOGE-USDT-SWAP"},"action":"snapshot","data":[{"asks":[["0.0963","10","0","2"]],"bids":[["0.0962","20","0","3"]],"ts":"1790470000000","checksum":0,"prevSeqId":-1,"seqId":123456}]}"#,
        )
        .expect("books");

        let WsInbound::Push(WsPush::Books { action, data, .. }) = push else {
            panic!("books push expected");
        };
        assert_eq!(action, OrderBookAction::Snapshot);
        assert_eq!(data[0].previous_sequence_id, -1);
        assert_eq!(data[0].sequence_id, 123456);
        assert_eq!(data[0].checksum, 0);
        assert_eq!(data[0].bids[0][0], "0.0962");
    }

    #[test]
    fn rejects_unknown_channel() {
        let error = decode_ws_text(
            r#"{"arg":{"channel":"mystery","instId":"DOGE-USDT-SWAP"},"data":[]}"#,
        )
        .expect_err("unknown channel must fail closed");

        assert!(error.to_string().contains("unsupported OKX WebSocket channel"));
    }
}
