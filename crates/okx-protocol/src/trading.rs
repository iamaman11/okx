//! Strict public trading, strategy and portfolio commands. Native owners classify risk.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum TradeRequest {
    Preview {
        operation: TradeOperation,
        strategy_instance: String,
        instrument: String,
        side: PositionSide,
        sizing: Sizing,
        order: OrderConstraints,
        protection: Option<Protection>,
    },
    Execute {
        idempotency_key: String,
        operation: TradeOperation,
        strategy_instance: String,
        instrument: String,
        side: PositionSide,
        sizing: Sizing,
        order: OrderConstraints,
        protection: Option<Protection>,
        expires_in_ms: u64,
    },
    Status {
        reference: String,
    },
    Cancel {
        idempotency_key: String,
        reference: String,
    },
    Close {
        idempotency_key: String,
        reference: String,
        expires_in_ms: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Sizing {
    Contracts { contracts: String },
    MaxLossUsd { max_loss_usd: String },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderConstraints {
    #[serde(rename = "type")]
    pub order_type: String,
    pub limit_price: Option<String>,
    pub reference: Option<String>,
    pub max_slippage_bps: u32,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum Protection {
    Price { stop_price: String },
    Distance { stop_distance_bps: u32 },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeOperation {
    Open,
    Add,
    Reduce,
    Close,
    Replace,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum StrategyRequest {
    List {},
    Status {
        instance_id: String,
    },
    Create {
        idempotency_key: String,
        instance_id: String,
        definition_id: String,
        version: u32,
        parameters: serde_json::Value,
        instruments: Vec<String>,
        timeframe_ms: u64,
        budget_usd: String,
    },
    Update {
        idempotency_key: String,
        instance_id: String,
        expected_revision: u64,
        parameters: serde_json::Value,
        budget_usd: String,
    },
    Start {
        idempotency_key: String,
        instance_id: String,
    },
    Stop {
        idempotency_key: String,
        instance_id: String,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioRequest {
    pub action: PortfolioAction,
    pub accounts: Vec<String>,
    pub start_ms: u64,
    pub end_ms: u64,
    pub strategies: Option<Vec<String>>,
    pub currencies: Option<Vec<String>>,
    pub cursor: Option<String>,
    pub limit: u16,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioAction {
    Summary,
    History,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsRequest {
    pub reference: Option<String>,
    pub account: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionClass {
    Read,
    IncreaseRisk,
    ReduceRisk,
    Maintenance,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandIdentity {
    pub account: String,
    pub idempotency_key: String,
    pub expires_in_ms: u64,
}
impl CommandIdentity {
    pub fn valid(&self) -> bool {
        !self.account.is_empty()
            && self.account.len() <= 128
            && (8..=128).contains(&self.idempotency_key.len())
            && self
                .idempotency_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            && (1..=30000).contains(&self.expires_in_ms)
    }
}
fn id(v: &str) -> bool {
    (8..=128).contains(&v.len())
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn name(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b':')
}
fn decimal(v: &str) -> bool {
    let Some((a, b)) = v
        .split_once('.')
        .map(|(a, b)| (a, Some(b)))
        .or(Some((v, None)))
    else {
        return false;
    };
    !a.is_empty()
        && a.len() <= 15
        && (a.len() == 1 || !a.starts_with('0'))
        && a.bytes().all(|c| c.is_ascii_digit())
        && b.is_none_or(|b| !b.is_empty() && b.len() <= 9 && b.bytes().all(|c| c.is_ascii_digit()))
        && v.bytes().any(|c| matches!(c, b'1'..=b'9'))
}
impl Sizing {
    pub fn valid(&self) -> bool {
        match self {
            Self::Contracts { contracts } => decimal(contracts),
            Self::MaxLossUsd { max_loss_usd } => decimal(max_loss_usd),
        }
    }
}
impl Protection {
    pub fn valid(&self) -> bool {
        match self {
            Self::Price { stop_price } => decimal(stop_price),
            Self::Distance { stop_distance_bps } => (1..=10000).contains(stop_distance_bps),
        }
    }
}
impl OrderConstraints {
    pub fn valid(&self) -> bool {
        matches!(
            self.order_type.as_str(),
            "limit" | "market" | "fok" | "ioc" | "post_only"
        ) && self.reference.as_ref().is_none_or(|v| name(v))
            && self.max_slippage_bps <= 10000
            && self.limit_price.as_ref().is_none_or(|v| decimal(v))
            && (!matches!(self.order_type.as_str(), "limit" | "post_only")
                || self.limit_price.is_some())
    }
}
impl TradeRequest {
    pub fn valid(&self) -> bool {
        match self {
            Self::Preview {
                operation,
                strategy_instance,
                instrument,
                sizing,
                order,
                protection,
                ..
            }
            | Self::Execute {
                operation,
                strategy_instance,
                instrument,
                sizing,
                order,
                protection,
                ..
            } => {
                let common = name(strategy_instance)
                    && name(instrument)
                    && instrument.len() >= 3
                    && sizing.valid()
                    && order.valid()
                    && protection.as_ref().is_none_or(Protection::valid)
                    && (!matches!(
                        operation,
                        TradeOperation::Open | TradeOperation::Add | TradeOperation::Replace
                    ) || protection.is_some());
                common
                    && match self {
                        Self::Execute {
                            idempotency_key,
                            expires_in_ms,
                            ..
                        } => id(idempotency_key) && (1..=30000).contains(expires_in_ms),
                        _ => true,
                    }
            }
            Self::Status { reference } => name(reference),
            Self::Cancel {
                idempotency_key,
                reference,
            } => id(idempotency_key) && name(reference),
            Self::Close {
                idempotency_key,
                reference,
                expires_in_ms,
            } => id(idempotency_key) && name(reference) && (1..=30000).contains(expires_in_ms),
        }
    }
}
impl StrategyRequest {
    pub fn valid(&self) -> bool {
        match self {
            Self::List {} => true,
            Self::Status { instance_id } => name(instance_id),
            Self::Start {
                idempotency_key,
                instance_id,
            }
            | Self::Stop {
                idempotency_key,
                instance_id,
            } => id(idempotency_key) && name(instance_id),
            Self::Create {
                idempotency_key,
                instance_id,
                definition_id,
                version,
                parameters,
                instruments,
                timeframe_ms,
                budget_usd,
            } => {
                id(idempotency_key)
                    && name(instance_id)
                    && name(definition_id)
                    && *version > 0
                    && parameters.is_object()
                    && parameters.to_string().len() <= 4096
                    && !instruments.is_empty()
                    && instruments.len() <= 32
                    && instruments.iter().all(|v| name(v))
                    && *timeframe_ms >= 1000
                    && decimal(budget_usd)
            }
            Self::Update {
                idempotency_key,
                instance_id,
                parameters,
                budget_usd,
                ..
            } => {
                id(idempotency_key)
                    && name(instance_id)
                    && parameters.is_object()
                    && parameters.to_string().len() <= 4096
                    && decimal(budget_usd)
            }
        }
    }
}
impl PortfolioRequest {
    pub fn valid(&self) -> bool {
        !self.accounts.is_empty()
            && self.accounts.len() <= 32
            && self.accounts.iter().all(|v| name(v))
            && self.end_ms > self.start_ms
            && (1..=50).contains(&self.limit)
            && self.cursor.as_ref().is_none_or(|v| v.len() <= 512)
            && self
                .strategies
                .as_ref()
                .is_none_or(|v| v.len() <= 32 && v.iter().all(|v| name(v)))
            && self
                .currencies
                .as_ref()
                .is_none_or(|v| v.len() <= 32 && v.iter().all(|v| name(v)))
    }
}
impl DiagnosticsRequest {
    pub fn valid(&self) -> bool {
        self.reference.as_ref().is_some_and(|v| name(v))
            || self.account.as_ref().is_some_and(|v| name(v))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trade_positive_negative_wire_fixtures() {
        let good = r#"{"action":"execute","operation":"open","strategy_instance":"manual","idempotency_key":"command_001","instrument":"BTC-USDT-SWAP","side":"long","sizing":{"max_loss_usd":"5.00"},"order":{"type":"fok","max_slippage_bps":5},"protection":{"stop_distance_bps":50},"expires_in_ms":10000}"#;
        let parsed: TradeRequest = serde_json::from_str(good).unwrap();
        assert!(parsed.valid());
        for bad in [
            good.replace("10000}", "30001}"),
            good.replace("\"5.00\"", "\"-5\""),
            good.replace("command_001", "x"),
        ] {
            assert!(!serde_json::from_str::<TradeRequest>(&bad).unwrap().valid());
        }
        let mut bad: serde_json::Value = serde_json::from_str(good).unwrap();
        bad["sizing"]["contracts"] = serde_json::json!("1");
        assert!(serde_json::from_value::<TradeRequest>(bad).is_err());
    }
}
