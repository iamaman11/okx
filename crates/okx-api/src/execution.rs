use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::{account::Instrument, client::OkxRestClient, error::OkxError};

const OKX_MAX_AUTH_SKEW_MS: i64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TradeMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OrderType {
    #[serde(rename = "market")]
    Market,
    #[serde(rename = "limit")]
    Limit,
    #[serde(rename = "post_only")]
    PostOnly,
    #[serde(rename = "fok")]
    FillOrKill,
    #[serde(rename = "ioc")]
    ImmediateOrCancel,
    #[serde(rename = "optimal_limit_ioc")]
    OptimalLimitIoc,
}

impl OrderType {
    fn requires_price(self) -> bool {
        matches!(
            self,
            Self::Limit | Self::PostOnly | Self::FillOrKill | Self::ImmediateOrCancel
        )
    }

    fn uses_market_size_limit(self) -> bool {
        matches!(self, Self::Market | Self::OptimalLimitIoc)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    pub fn new(value: impl Into<String>) -> Result<Self, OkxError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 32
            || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(OkxError::Config(
                "clOrdId must be 1-32 case-sensitive ASCII alphanumeric characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderIntent {
    #[serde(rename = "instId")]
    pub instrument_id: String,
    #[serde(rename = "tdMode")]
    pub trade_mode: TradeMode,
    #[serde(rename = "clOrdId")]
    pub client_order_id: ClientOrderId,
    pub side: Side,
    #[serde(rename = "posSide")]
    pub position_side: PositionSide,
    #[serde(rename = "ordType")]
    pub order_type: OrderType,
    #[serde(rename = "sz")]
    pub size: Decimal,
    #[serde(rename = "px", skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
    #[serde(rename = "reduceOnly")]
    pub reduce_only: bool,
}

impl OrderIntent {
    pub fn validate(&self, instrument: &Instrument) -> Result<(), OkxError> {
        if instrument.instrument_id != self.instrument_id {
            return Err(OkxError::Config(format!(
                "instrument metadata mismatch: intent={} metadata={}",
                self.instrument_id, instrument.instrument_id
            )));
        }
        if instrument.state != "live" {
            return Err(OkxError::Config(format!(
                "instrument {} is not live: state={}",
                instrument.instrument_id, instrument.state
            )));
        }

        let lot_size = required_decimal("lotSz", &instrument.lot_size)?;
        let min_size = required_decimal("minSz", &instrument.min_size)?;
        validate_positive("sz", self.size)?;
        validate_positive("lotSz", lot_size)?;
        validate_positive("minSz", min_size)?;

        if self.size < min_size {
            return Err(OkxError::Config(format!(
                "order size {} is below minSz {}",
                self.size, min_size
            )));
        }
        if self.size % lot_size != Decimal::ZERO {
            return Err(OkxError::Config(format!(
                "order size {} is not aligned to lotSz {}",
                self.size, lot_size
            )));
        }

        let max_size_raw = if self.order_type.uses_market_size_limit() {
            &instrument.max_market_size
        } else {
            &instrument.max_limit_size
        };
        if let Some(max_size) = optional_decimal(max_size_raw)? {
            validate_positive("maximum order size", max_size)?;
            if self.size > max_size {
                return Err(OkxError::Config(format!(
                    "order size {} exceeds instrument maximum {}",
                    self.size, max_size
                )));
            }
        }

        match (self.order_type.requires_price(), self.price) {
            (true, None) => {
                return Err(OkxError::Config(
                    "this order type requires an explicit price".to_owned(),
                ));
            }
            (false, Some(_)) => {
                return Err(OkxError::Config(
                    "this order type must not carry an explicit price".to_owned(),
                ));
            }
            _ => {}
        }

        if let Some(price) = self.price {
            let tick_size = required_decimal("tickSz", &instrument.tick_size)?;
            validate_positive("px", price)?;
            validate_positive("tickSz", tick_size)?;
            if price % tick_size != Decimal::ZERO {
                return Err(OkxError::Config(format!(
                    "order price {} is not aligned to tickSz {}",
                    price, tick_size
                )));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionOutcome {
    NotSubmitted,
    UnknownSubmission {
        client_order_id: ClientOrderId,
    },
    Acknowledged {
        client_order_id: ClientOrderId,
        order_id: String,
    },
    Rejected {
        client_order_id: ClientOrderId,
        code: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ClockCheck {
    pub server_time_ms: i64,
    pub local_midpoint_ms: i64,
    pub round_trip_ms: i64,
    pub absolute_skew_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
struct ServerTime {
    ts: String,
}

#[derive(Clone)]
pub struct ExecutionApi {
    client: OkxRestClient,
}

impl ExecutionApi {
    pub fn new(client: OkxRestClient) -> Self {
        Self { client }
    }

    pub async fn verify_server_time(
        &self,
        max_allowed_skew_ms: i64,
    ) -> Result<ClockCheck, OkxError> {
        if !(1..OKX_MAX_AUTH_SKEW_MS).contains(&max_allowed_skew_ms) {
            return Err(OkxError::Config(format!(
                "max_allowed_skew_ms must be between 1 and {}",
                OKX_MAX_AUTH_SKEW_MS - 1
            )));
        }

        let started_ms = Utc::now().timestamp_millis();
        let server = self
            .client
            .public_get::<ServerTime>("/api/v5/public/time", &[])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| OkxError::Config("OKX returned no server time".to_owned()))?;
        let completed_ms = Utc::now().timestamp_millis();

        let server_time_ms = server
            .ts
            .parse::<i64>()
            .map_err(|_| OkxError::Config("OKX returned an invalid server timestamp".to_owned()))?;
        let round_trip_ms = completed_ms.saturating_sub(started_ms);
        let local_midpoint_ms = started_ms.saturating_add(round_trip_ms / 2);
        let absolute_skew_ms = local_midpoint_ms.saturating_sub(server_time_ms).abs();

        if absolute_skew_ms > max_allowed_skew_ms {
            return Err(OkxError::Config(format!(
                "clock skew {}ms exceeds allowed {}ms",
                absolute_skew_ms, max_allowed_skew_ms
            )));
        }

        Ok(ClockCheck {
            server_time_ms,
            local_midpoint_ms,
            round_trip_ms,
            absolute_skew_ms,
        })
    }
}

fn required_decimal(name: &str, raw: &str) -> Result<Decimal, OkxError> {
    optional_decimal(raw)?.ok_or_else(|| {
        OkxError::Config(format!(
            "instrument metadata is missing required field {name}"
        ))
    })
}

fn optional_decimal(raw: &str) -> Result<Option<Decimal>, OkxError> {
    if raw.trim().is_empty() {
        return Ok(None);
    }
    Decimal::from_str(raw)
        .map(Some)
        .map_err(|_| OkxError::Config(format!("invalid decimal value '{raw}'")))
}

fn validate_positive(name: &str, value: Decimal) -> Result<(), OkxError> {
    if value <= Decimal::ZERO {
        return Err(OkxError::Config(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ClientOrderId, OrderIntent, OrderType, PositionSide, Side, TradeMode};
    use crate::account::Instrument;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn instrument() -> Instrument {
        Instrument {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            instrument_family: "BTC-USDT".to_owned(),
            state: "live".to_owned(),
            lever: "100".to_owned(),
            contract_type: "linear".to_owned(),
            fee_group_id: "4".to_owned(),
            tick_size: "0.1".to_owned(),
            lot_size: "1".to_owned(),
            min_size: "1".to_owned(),
            max_limit_size: "1000".to_owned(),
            max_market_size: "500".to_owned(),
            contract_value: "0.01".to_owned(),
            contract_value_currency: "BTC".to_owned(),
        }
    }

    fn decimal(value: &str) -> Decimal {
        Decimal::from_str(value).expect("valid decimal fixture")
    }

    fn limit_intent() -> OrderIntent {
        OrderIntent {
            instrument_id: "BTC-USDT-SWAP".to_owned(),
            trade_mode: TradeMode::Cross,
            client_order_id: ClientOrderId::new("Succession0001").expect("valid id"),
            side: Side::Buy,
            position_side: PositionSide::Long,
            order_type: OrderType::Limit,
            size: decimal("2"),
            price: Some(decimal("100000.1")),
            reduce_only: false,
        }
    }

    #[test]
    fn validates_client_order_id_contract() {
        assert!(ClientOrderId::new("Abc123").is_ok());
        assert!(ClientOrderId::new("").is_err());
        assert!(ClientOrderId::new("abc-123").is_err());
        assert!(ClientOrderId::new("a".repeat(33)).is_err());
    }

    #[test]
    fn accepts_aligned_limit_order() {
        assert!(limit_intent().validate(&instrument()).is_ok());
    }

    #[test]
    fn rejects_misaligned_price() {
        let mut intent = limit_intent();
        intent.price = Some(decimal("100000.15"));
        assert!(intent.validate(&instrument()).is_err());
    }

    #[test]
    fn rejects_misaligned_size() {
        let mut rules = instrument();
        rules.lot_size = "0.5".to_owned();
        let mut intent = limit_intent();
        intent.size = decimal("1.25");
        assert!(intent.validate(&rules).is_err());
    }

    #[test]
    fn rejects_market_order_with_price() {
        let mut intent = limit_intent();
        intent.order_type = OrderType::Market;
        assert!(intent.validate(&instrument()).is_err());
    }
}
