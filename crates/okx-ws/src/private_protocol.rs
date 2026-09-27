use okx_api::WsLoginMaterial;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivateChannel {
    Account,
    Positions,
    Orders,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateWsArg {
    pub channel: PrivateChannel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ccy: Option<String>,
    #[serde(rename = "instType", default, skip_serializing_if = "Option::is_none")]
    pub instrument_type: Option<String>,
    #[serde(rename = "instFamily", default, skip_serializing_if = "Option::is_none")]
    pub instrument_family: Option<String>,
    #[serde(rename = "instId", default, skip_serializing_if = "Option::is_none")]
    pub instrument_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct PrivateSubscription {
    pub channel: PrivateChannel,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ccy: Option<String>,
    #[serde(rename = "instType", skip_serializing_if = "Option::is_none")]
    pub instrument_type: Option<String>,
    #[serde(rename = "instFamily", skip_serializing_if = "Option::is_none")]
    pub instrument_family: Option<String>,
    #[serde(rename = "instId", skip_serializing_if = "Option::is_none")]
    pub instrument_id: Option<String>,
}

impl PrivateSubscription {
    pub fn account() -> Self {
        Self {
            channel: PrivateChannel::Account,
            ccy: None,
            instrument_type: None,
            instrument_family: None,
            instrument_id: None,
        }
    }

    pub fn positions_any() -> Self {
        Self {
            channel: PrivateChannel::Positions,
            ccy: None,
            instrument_type: Some("ANY".to_owned()),
            instrument_family: None,
            instrument_id: None,
        }
    }

    pub fn orders_any() -> Self {
        Self {
            channel: PrivateChannel::Orders,
            ccy: None,
            instrument_type: Some("ANY".to_owned()),
            instrument_family: None,
            instrument_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PrivateInboundMessage {
    Pong,
    Login {
        code: String,
        message: String,
        connection_id: Option<String>,
    },
    Subscribed {
        arg: PrivateWsArg,
        connection_id: Option<String>,
    },
    Unsubscribed {
        arg: PrivateWsArg,
        connection_id: Option<String>,
    },
    Error {
        code: Option<String>,
        message: Option<String>,
        arg: Option<PrivateWsArg>,
        connection_id: Option<String>,
    },
    Notice {
        code: Option<String>,
        message: Option<String>,
        connection_id: Option<String>,
    },
    Data {
        arg: PrivateWsArg,
        data: Vec<Value>,
    },
    Other(Value),
}

#[derive(Debug, Serialize)]
struct LoginRequest<'a> {
    op: &'static str,
    args: [&'a WsLoginMaterial; 1],
}

#[derive(Debug, Serialize)]
struct SubscriptionRequest<'a> {
    op: &'static str,
    args: &'a [PrivateSubscription],
}

pub fn login_payload(material: &WsLoginMaterial) -> Result<String, serde_json::Error> {
    serde_json::to_string(&LoginRequest {
        op: "login",
        args: [material],
    })
}

pub fn private_subscribe_payload(
    subscriptions: &[PrivateSubscription],
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&SubscriptionRequest {
        op: "subscribe",
        args: subscriptions,
    })
}

pub fn private_unsubscribe_payload(
    subscriptions: &[PrivateSubscription],
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&SubscriptionRequest {
        op: "unsubscribe",
        args: subscriptions,
    })
}

pub fn parse_private_text(text: &str) -> Result<PrivateInboundMessage, serde_json::Error> {
    if text == "pong" {
        return Ok(PrivateInboundMessage::Pong);
    }

    let value: Value = serde_json::from_str(text)?;
    let event = value.get("event").and_then(Value::as_str);

    match event {
        Some("login") => Ok(PrivateInboundMessage::Login {
            code: string_field(&value, "code").unwrap_or_default(),
            message: string_field(&value, "msg").unwrap_or_default(),
            connection_id: string_field(&value, "connId"),
        }),
        Some("subscribe") => Ok(PrivateInboundMessage::Subscribed {
            arg: serde_json::from_value(value.get("arg").cloned().unwrap_or(Value::Null))?,
            connection_id: string_field(&value, "connId"),
        }),
        Some("unsubscribe") => Ok(PrivateInboundMessage::Unsubscribed {
            arg: serde_json::from_value(value.get("arg").cloned().unwrap_or(Value::Null))?,
            connection_id: string_field(&value, "connId"),
        }),
        Some("error") => Ok(PrivateInboundMessage::Error {
            code: string_field(&value, "code"),
            message: string_field(&value, "msg"),
            arg: value
                .get("arg")
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
            connection_id: string_field(&value, "connId"),
        }),
        Some("notice") => Ok(PrivateInboundMessage::Notice {
            code: string_field(&value, "code"),
            message: string_field(&value, "msg"),
            connection_id: string_field(&value, "connId"),
        }),
        _ if value.get("arg").is_some() && value.get("data").is_some() => {
            Ok(PrivateInboundMessage::Data {
                arg: serde_json::from_value(value.get("arg").cloned().unwrap_or(Value::Null))?,
                data: serde_json::from_value(value.get("data").cloned().unwrap_or(Value::Null))?,
            })
        }
        _ => Ok(PrivateInboundMessage::Other(value)),
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
    fn login_payload_contains_no_secret_key_field() {
        let material = WsLoginMaterial {
            api_key: "api-key".to_owned(),
            passphrase: "pass".to_owned(),
            timestamp: "1538054050".to_owned(),
            sign: "signature".to_owned(),
        };
        let payload = login_payload(&material).expect("login");
        assert!(payload.contains("\"op\":\"login\""));
        assert!(payload.contains("\"apiKey\":\"api-key\""));
        assert!(!payload.contains("secret"));
    }

    #[test]
    fn baseline_private_subscriptions_are_typed() {
        let subscriptions = [
            PrivateSubscription::account(),
            PrivateSubscription::positions_any(),
            PrivateSubscription::orders_any(),
        ];
        let payload = private_subscribe_payload(&subscriptions).expect("subscribe");
        let value: Value = serde_json::from_str(&payload).expect("json");
        assert_eq!(value["args"][0]["channel"], "account");
        assert_eq!(value["args"][1]["channel"], "positions");
        assert_eq!(value["args"][1]["instType"], "ANY");
        assert_eq!(value["args"][2]["channel"], "orders");
        assert_eq!(value["args"][2]["instType"], "ANY");
    }

    #[test]
    fn parses_login_subscribe_data_and_upgrade_notice() {
        assert!(matches!(
            parse_private_text(r#"{"event":"login","code":"0","msg":"","connId":"abc"}"#)
                .expect("login"),
            PrivateInboundMessage::Login { code, .. } if code == "0"
        ));

        assert!(matches!(
            parse_private_text(
                r#"{"event":"subscribe","arg":{"channel":"positions","instType":"ANY"},"connId":"abc"}"#
            )
            .expect("subscribe"),
            PrivateInboundMessage::Subscribed {
                arg: PrivateWsArg {
                    channel: PrivateChannel::Positions,
                    ..
                },
                ..
            }
        ));

        assert!(matches!(
            parse_private_text(
                r#"{"arg":{"channel":"orders","instType":"ANY"},"data":[{"ordId":"1"}]}"#
            )
            .expect("data"),
            PrivateInboundMessage::Data {
                arg: PrivateWsArg {
                    channel: PrivateChannel::Orders,
                    ..
                },
                ..
            }
        ));

        assert!(matches!(
            parse_private_text(
                r#"{"event":"notice","code":"64008","msg":"upgrade","connId":"abc"}"#
            )
            .expect("notice"),
            PrivateInboundMessage::Notice { code: Some(code), .. } if code == "64008"
        ));
    }

    #[test]
    fn unknown_private_channel_fails_closed() {
        assert!(parse_private_text(
            r#"{"arg":{"channel":"future-private"},"data":[{}]}"#
        )
        .is_err());
    }
}
