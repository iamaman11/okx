use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PublicChannel {
    Instruments,
    Tickers,
    MarkPrice,
    IndexTickers,
    FundingRate,
    OpenInterest,
    Books,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsArg {
    pub channel: PublicChannel,
    #[serde(rename = "instType", default, skip_serializing_if = "Option::is_none")]
    pub instrument_type: Option<String>,
    #[serde(
        rename = "instFamily",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub instrument_family: Option<String>,
    #[serde(rename = "instId", default, skip_serializing_if = "Option::is_none")]
    pub instrument_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Subscription {
    pub channel: PublicChannel,
    #[serde(rename = "instType", skip_serializing_if = "Option::is_none")]
    pub instrument_type: Option<String>,
    #[serde(rename = "instFamily", skip_serializing_if = "Option::is_none")]
    pub instrument_family: Option<String>,
    #[serde(rename = "instId", skip_serializing_if = "Option::is_none")]
    pub instrument_id: Option<String>,
}

impl Subscription {
    pub fn instrument(channel: PublicChannel, instrument_id: impl Into<String>) -> Self {
        Self {
            channel,
            instrument_type: None,
            instrument_family: None,
            instrument_id: Some(instrument_id.into()),
        }
    }

    pub fn instrument_type(channel: PublicChannel, instrument_type: impl Into<String>) -> Self {
        Self {
            channel,
            instrument_type: Some(instrument_type.into()),
            instrument_family: None,
            instrument_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundMessage {
    Pong,
    Subscribed {
        arg: WsArg,
        connection_id: Option<String>,
    },
    Unsubscribed {
        arg: WsArg,
        connection_id: Option<String>,
    },
    Error {
        code: Option<String>,
        message: Option<String>,
        arg: Option<WsArg>,
    },
    Notice {
        code: Option<String>,
        message: Option<String>,
        connection_id: Option<String>,
    },
    Data {
        arg: WsArg,
        action: Option<String>,
        data: Vec<Value>,
    },
    Other(Value),
}

#[derive(Debug, Serialize)]
struct SubscriptionRequest<'a> {
    id: &'static str,
    op: &'static str,
    args: &'a [Subscription],
}

pub fn subscribe_payload(subscriptions: &[Subscription]) -> Result<String, serde_json::Error> {
    subscription_payload("subscribe", subscriptions)
}

pub fn unsubscribe_payload(subscriptions: &[Subscription]) -> Result<String, serde_json::Error> {
    subscription_payload("unsubscribe", subscriptions)
}

fn subscription_payload(
    operation: &'static str,
    subscriptions: &[Subscription],
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&SubscriptionRequest {
        id: "m3-public",
        op: operation,
        args: subscriptions,
    })
}

pub fn parse_text(text: &str) -> Result<InboundMessage, serde_json::Error> {
    if text == "pong" {
        return Ok(InboundMessage::Pong);
    }

    let value: Value = serde_json::from_str(text)?;
    let event = value.get("event").and_then(Value::as_str);

    match event {
        Some("subscribe") => {
            let arg = serde_json::from_value(value.get("arg").cloned().unwrap_or(Value::Null))?;
            Ok(InboundMessage::Subscribed {
                arg,
                connection_id: string_field(&value, "connId"),
            })
        }
        Some("unsubscribe") => {
            let arg = serde_json::from_value(value.get("arg").cloned().unwrap_or(Value::Null))?;
            Ok(InboundMessage::Unsubscribed {
                arg,
                connection_id: string_field(&value, "connId"),
            })
        }
        Some("error") => Ok(InboundMessage::Error {
            code: string_field(&value, "code"),
            message: string_field(&value, "msg"),
            arg: value
                .get("arg")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
        }),
        Some("notice") => Ok(InboundMessage::Notice {
            code: string_field(&value, "code"),
            message: string_field(&value, "msg"),
            connection_id: string_field(&value, "connId"),
        }),
        _ if value.get("arg").is_some() && value.get("data").is_some() => {
            let arg = serde_json::from_value(value.get("arg").cloned().unwrap_or(Value::Null))?;
            let data = serde_json::from_value(value.get("data").cloned().unwrap_or(Value::Null))?;
            Ok(InboundMessage::Data {
                arg,
                action: string_field(&value, "action"),
                data,
            })
        }
        _ => Ok(InboundMessage::Other(value)),
    }
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_and_unsubscribe_are_typed() {
        let subscriptions = [
            Subscription::instrument(PublicChannel::Tickers, "DOGE-USDT-SWAP"),
            Subscription::instrument(PublicChannel::Books, "DOGE-USDT-SWAP"),
        ];

        let subscribe: Value =
            serde_json::from_str(&subscribe_payload(&subscriptions).expect("subscribe"))
                .expect("subscribe json");
        let unsubscribe: Value =
            serde_json::from_str(&unsubscribe_payload(&subscriptions).expect("unsubscribe"))
                .expect("unsubscribe json");

        assert_eq!(subscribe["op"], "subscribe");
        assert_eq!(unsubscribe["op"], "unsubscribe");
        assert_eq!(subscribe["args"][0]["channel"], "tickers");
        assert_eq!(subscribe["args"][1]["channel"], "books");
    }

    #[test]
    fn parses_known_channels_notice_and_data() {
        let subscribed = parse_text(
            r#"{"event":"subscribe","arg":{"channel":"tickers","instId":"DOGE-USDT-SWAP"},"connId":"abc"}"#,
        )
        .expect("subscribe");
        assert!(matches!(
            subscribed,
            InboundMessage::Subscribed {
                arg: WsArg {
                    channel: PublicChannel::Tickers,
                    ..
                },
                connection_id: Some(ref id),
            } if id == "abc"
        ));

        let notice =
            parse_text(r#"{"event":"notice","code":"64008","msg":"upgrade","connId":"abc"}"#)
                .expect("notice");
        assert!(matches!(
            notice,
            InboundMessage::Notice {
                code: Some(ref code),
                ..
            } if code == "64008"
        ));

        let data = parse_text(
            r#"{"arg":{"channel":"books","instId":"DOGE-USDT-SWAP"},"action":"snapshot","data":[{"seqId":10,"prevSeqId":-1}]}"#,
        )
        .expect("data");
        assert!(matches!(
            data,
            InboundMessage::Data {
                arg: WsArg {
                    channel: PublicChannel::Books,
                    ..
                },
                action: Some(ref action),
                ..
            } if action == "snapshot"
        ));
    }

    #[test]
    fn unknown_channel_fails_closed() {
        assert!(
            parse_text(
                r#"{"arg":{"channel":"future-unknown","instId":"DOGE-USDT-SWAP"},"data":[{}]}"#
            )
            .is_err()
        );
    }

    #[test]
    fn application_pong_is_not_json() {
        assert_eq!(parse_text("pong").expect("pong"), InboundMessage::Pong);
    }
}
