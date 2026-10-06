use std::collections::BTreeSet;

use okx_api::{PublicCandle, PublicFundingHistory, PublicOpenInterestHistory, PublicTrade};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{FundingRequirement, ReferenceRegistry};

pub const MARKET_HISTORY_SCHEMA_V1: &str = "okx.market-history/v1";
pub const MARKET_HISTORY_SOURCE_V1: &str = "okx_public_rest_history";
pub const MARKET_TRADES_SCHEMA_V1: &str = "okx.market-trades/v1";
pub const MARKET_TRADES_SOURCE_V1: &str = "okx_public_rest_trades";
pub const FUNDING_HISTORY_SCHEMA_V1: &str = "okx.funding-history/v1";
pub const FUNDING_HISTORY_SOURCE_V1: &str = "okx_public_rest_funding_history";
pub const OPEN_INTEREST_HISTORY_SCHEMA_V1: &str = "okx.open-interest-history/v1";
pub const OPEN_INTEREST_HISTORY_SOURCE_V1: &str = "okx_public_rest_contract_oi_history";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCandle {
    pub open_time_ms: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub volume_currency: String,
    pub volume_quote: Option<String>,
    pub confirmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub bar: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub history_generation: String,
    pub all_confirmed: bool,
    pub oldest_open_time_ms: String,
    pub newest_open_time_ms: String,
    pub candles: Vec<HistoryCandle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketTradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTrade {
    pub trade_id: String,
    pub price: String,
    pub size_contracts: String,
    pub side: MarketTradeSide,
    pub source: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTradesSnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub trades_generation: String,
    pub oldest_exchange_timestamp_ms: Option<String>,
    pub newest_exchange_timestamp_ms: Option<String>,
    pub trades: Vec<MarketTrade>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingHistoryEvent {
    pub funding_time_ms: String,
    pub funding_rate: String,
    pub realized_rate: Option<String>,
    pub formula_type: Option<String>,
    pub method: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub funding_generation: String,
    pub oldest_funding_time_ms: Option<String>,
    pub newest_funding_time_ms: Option<String>,
    pub events: Vec<FundingHistoryEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenInterestHistoryPoint {
    pub timestamp_ms: String,
    pub open_interest_contracts: String,
    pub open_interest_currency: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenInterestHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub period: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub open_interest_generation: String,
    pub oldest_timestamp_ms: Option<String>,
    pub newest_timestamp_ms: Option<String>,
    pub points: Vec<OpenInterestHistoryPoint>,
}

pub fn normalize_research_candles(
    rows: Vec<PublicCandle>,
    max_rows: usize,
) -> Result<Vec<HistoryCandle>, MarketHistoryError> {
    if rows.len() > max_rows {
        return Err(MarketHistoryError::TooManyRows);
    }

    let mut normalized = Vec::with_capacity(rows.len());
    let mut timestamps = BTreeSet::new();
    for row in rows {
        let timestamp = row
            .timestamp_ms
            .parse::<u64>()
            .map_err(|_| MarketHistoryError::InvalidTimestamp(row.timestamp_ms.clone()))?;
        if !timestamps.insert(timestamp) {
            return Err(MarketHistoryError::DuplicateTimestamp(row.timestamp_ms));
        }

        let confirmed = match row.confirm.as_str() {
            "0" => false,
            "1" => true,
            _ => return Err(MarketHistoryError::InvalidConfirm(row.confirm)),
        };

        normalized.push((
            timestamp,
            HistoryCandle {
                open_time_ms: required("ts", row.timestamp_ms)?,
                open: required("o", row.open)?,
                high: required("h", row.high)?,
                low: required("l", row.low)?,
                close: required("c", row.close)?,
                volume: required("vol", row.volume)?,
                volume_currency: required("volCcy", row.volume_currency)?,
                volume_quote: optional(row.volume_quote),
                confirmed,
            },
        ));
    }
    normalized.sort_by_key(|(timestamp, _)| *timestamp);
    Ok(normalized.into_iter().map(|(_, candle)| candle).collect())
}

pub fn normalize_research_trades(
    instrument_id: &str,
    rows: Vec<PublicTrade>,
    max_rows: usize,
) -> Result<Vec<MarketTrade>, MarketHistoryError> {
    if rows.len() > max_rows {
        return Err(MarketHistoryError::TooManyRows);
    }

    let mut trade_ids = BTreeSet::new();
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        if row.instrument_id != instrument_id {
            return Err(MarketHistoryError::InstrumentMismatch {
                expected: instrument_id.to_owned(),
                actual: row.instrument_id,
            });
        }
        if !trade_ids.insert(row.trade_id.clone()) {
            return Err(MarketHistoryError::DuplicateTradeId(row.trade_id));
        }
        let timestamp = row
            .timestamp_ms
            .parse::<u64>()
            .map_err(|_| MarketHistoryError::InvalidTimestamp(row.timestamp_ms.clone()))?;
        let side = match row.side.as_str() {
            "buy" => MarketTradeSide::Buy,
            "sell" => MarketTradeSide::Sell,
            _ => return Err(MarketHistoryError::InvalidTradeSide(row.side)),
        };
        normalized.push((
            timestamp,
            MarketTrade {
                trade_id: required("tradeId", row.trade_id)?,
                price: required("px", row.price)?,
                size_contracts: required("sz", row.size)?,
                side,
                source: optional_text(row.source),
                exchange_timestamp_ms: required("ts", row.timestamp_ms)?,
            },
        ));
    }
    normalized.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.trade_id.cmp(&right.1.trade_id))
    });
    Ok(normalized.into_iter().map(|(_, trade)| trade).collect())
}

pub fn normalize_research_funding(
    instrument_id: &str,
    rows: Vec<PublicFundingHistory>,
    max_rows: usize,
) -> Result<Vec<FundingHistoryEvent>, MarketHistoryError> {
    if rows.len() > max_rows {
        return Err(MarketHistoryError::TooManyRows);
    }

    let mut timestamps = BTreeSet::new();
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        if row.instrument_id != instrument_id {
            return Err(MarketHistoryError::InstrumentMismatch {
                expected: instrument_id.to_owned(),
                actual: row.instrument_id,
            });
        }
        let timestamp = row
            .funding_time_ms
            .parse::<u64>()
            .map_err(|_| MarketHistoryError::InvalidTimestamp(row.funding_time_ms.clone()))?;
        if !timestamps.insert(timestamp) {
            return Err(MarketHistoryError::DuplicateFundingTimestamp(
                row.funding_time_ms,
            ));
        }
        normalized.push((
            timestamp,
            FundingHistoryEvent {
                funding_time_ms: required("fundingTime", row.funding_time_ms)?,
                funding_rate: required("fundingRate", row.funding_rate)?,
                realized_rate: optional_text(row.realized_rate),
                formula_type: optional_text(row.formula_type),
                method: optional_text(row.method),
            },
        ));
    }
    normalized.sort_by_key(|(timestamp, _)| *timestamp);
    Ok(normalized.into_iter().map(|(_, event)| event).collect())
}

#[derive(Debug, Error)]
pub enum MarketHistoryError {
    #[error("history source receive timestamp is empty")]
    EmptySourceTimestamp,

    #[error("history limit must be between 1 and 100")]
    InvalidLimit,

    #[error("instrument '{0}' is not present in the reference registry")]
    InstrumentNotFound(String),

    #[error("instrument '{0}' is not live")]
    InstrumentNotLive(String),

    #[error("OKX returned no history candles")]
    EmptyHistory,

    #[error("OKX returned more history candles than requested")]
    TooManyRows,

    #[error("history candle timestamp '{0}' is invalid")]
    InvalidTimestamp(String),

    #[error("history contains duplicate candle timestamp '{0}'")]
    DuplicateTimestamp(String),

    #[error("history candle is missing required field '{0}'")]
    MissingField(&'static str),

    #[error("history candle confirm value '{0}' is invalid")]
    InvalidConfirm(String),

    #[error("history row instrument mismatch: expected '{expected}', got '{actual}'")]
    InstrumentMismatch { expected: String, actual: String },

    #[error("history contains duplicate trade id '{0}'")]
    DuplicateTradeId(String),

    #[error("trade side '{0}' is invalid")]
    InvalidTradeSide(String),

    #[error("history contains duplicate funding timestamp '{0}'")]
    DuplicateFundingTimestamp(String),

    #[error("history contains duplicate open-interest timestamp '{0}'")]
    DuplicateOpenInterestTimestamp(String),

    #[error("funding history is not applicable to instrument '{0}'")]
    FundingNotApplicable(String),

    #[error("failed to serialize normalized market history: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl MarketHistorySnapshot {
    pub fn from_public(
        reference: &ReferenceRegistry,
        instrument_id: &str,
        bar: &str,
        requested_limit: u16,
        source_received_at: impl Into<String>,
        rows: Vec<PublicCandle>,
    ) -> Result<Self, MarketHistoryError> {
        if !(1..=100).contains(&requested_limit) {
            return Err(MarketHistoryError::InvalidLimit);
        }

        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(MarketHistoryError::EmptySourceTimestamp);
        }

        let instrument = reference
            .get(instrument_id)
            .ok_or_else(|| MarketHistoryError::InstrumentNotFound(instrument_id.to_owned()))?;
        if instrument.state != "live" {
            return Err(MarketHistoryError::InstrumentNotLive(
                instrument_id.to_owned(),
            ));
        }

        if rows.is_empty() {
            return Err(MarketHistoryError::EmptyHistory);
        }
        if rows.len() > requested_limit as usize {
            return Err(MarketHistoryError::TooManyRows);
        }

        let candles = normalize_research_candles(rows, requested_limit as usize)?;
        if candles.is_empty() {
            return Err(MarketHistoryError::EmptyHistory);
        }
        let oldest_open_time_ms = candles
            .first()
            .expect("history is non-empty")
            .open_time_ms
            .clone();
        let newest_open_time_ms = candles
            .last()
            .expect("history is non-empty")
            .open_time_ms
            .clone();
        let all_confirmed = candles.iter().all(|candle| candle.confirmed);

        let mut snapshot = Self {
            schema: MARKET_HISTORY_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            bar: bar.to_owned(),
            requested_limit,
            reference_generation: reference.generation().as_str().to_owned(),
            source: MARKET_HISTORY_SOURCE_V1.to_owned(),
            source_received_at,
            history_generation: String::new(),
            all_confirmed,
            oldest_open_time_ms,
            newest_open_time_ms,
            candles,
        };
        snapshot.history_generation = generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

impl OpenInterestHistorySnapshot {
    pub fn from_public(
        reference: &ReferenceRegistry,
        instrument_id: &str,
        period: &str,
        requested_limit: u16,
        source_received_at: impl Into<String>,
        rows: Vec<PublicOpenInterestHistory>,
    ) -> Result<Self, MarketHistoryError> {
        validate_history_request(reference, instrument_id, requested_limit)?;
        let source_received_at = validate_source_timestamp(source_received_at.into())?;
        if rows.len() > requested_limit as usize {
            return Err(MarketHistoryError::TooManyRows);
        }

        let mut timestamps = BTreeSet::new();
        let mut normalized = Vec::with_capacity(rows.len());
        for row in rows {
            let timestamp = row
                .ts
                .parse::<u64>()
                .map_err(|_| MarketHistoryError::InvalidTimestamp(row.ts.clone()))?;
            if !timestamps.insert(timestamp) {
                return Err(MarketHistoryError::DuplicateOpenInterestTimestamp(row.ts));
            }
            normalized.push((
                timestamp,
                OpenInterestHistoryPoint {
                    timestamp_ms: required("ts", row.ts)?,
                    open_interest_contracts: required("oi", row.oi)?,
                    open_interest_currency: required("oiCcy", row.oi_currency)?,
                },
            ));
        }
        normalized.sort_by_key(|(timestamp, _)| *timestamp);
        let points = normalized
            .into_iter()
            .map(|(_, point)| point)
            .collect::<Vec<_>>();

        let mut snapshot = Self {
            schema: OPEN_INTEREST_HISTORY_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            period: period.to_owned(),
            requested_limit,
            reference_generation: reference.generation().as_str().to_owned(),
            source: OPEN_INTEREST_HISTORY_SOURCE_V1.to_owned(),
            source_received_at,
            open_interest_generation: String::new(),
            oldest_timestamp_ms: points.first().map(|point| point.timestamp_ms.clone()),
            newest_timestamp_ms: points.last().map(|point| point.timestamp_ms.clone()),
            points,
        };
        snapshot.open_interest_generation = open_interest_generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

impl MarketTradesSnapshot {
    pub fn from_public(
        reference: &ReferenceRegistry,
        instrument_id: &str,
        requested_limit: u16,
        source_received_at: impl Into<String>,
        rows: Vec<PublicTrade>,
    ) -> Result<Self, MarketHistoryError> {
        validate_history_request(reference, instrument_id, requested_limit)?;
        let source_received_at = validate_source_timestamp(source_received_at.into())?;
        if rows.len() > requested_limit as usize {
            return Err(MarketHistoryError::TooManyRows);
        }

        let trades = normalize_research_trades(instrument_id, rows, requested_limit as usize)?;

        let mut snapshot = Self {
            schema: MARKET_TRADES_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            requested_limit,
            reference_generation: reference.generation().as_str().to_owned(),
            source: MARKET_TRADES_SOURCE_V1.to_owned(),
            source_received_at,
            trades_generation: String::new(),
            oldest_exchange_timestamp_ms: trades
                .first()
                .map(|trade| trade.exchange_timestamp_ms.clone()),
            newest_exchange_timestamp_ms: trades
                .last()
                .map(|trade| trade.exchange_timestamp_ms.clone()),
            trades,
        };
        snapshot.trades_generation = trades_generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

impl FundingHistorySnapshot {
    pub fn from_public(
        reference: &ReferenceRegistry,
        instrument_id: &str,
        requested_limit: u16,
        source_received_at: impl Into<String>,
        rows: Vec<PublicFundingHistory>,
    ) -> Result<Self, MarketHistoryError> {
        validate_history_request(reference, instrument_id, requested_limit)?;
        let instrument = reference
            .get(instrument_id)
            .ok_or_else(|| MarketHistoryError::InstrumentNotFound(instrument_id.to_owned()))?;
        if instrument.funding_requirement != FundingRequirement::Required {
            return Err(MarketHistoryError::FundingNotApplicable(
                instrument_id.to_owned(),
            ));
        }
        let source_received_at = validate_source_timestamp(source_received_at.into())?;
        if rows.len() > requested_limit as usize {
            return Err(MarketHistoryError::TooManyRows);
        }

        let events = normalize_research_funding(instrument_id, rows, requested_limit as usize)?;

        let mut snapshot = Self {
            schema: FUNDING_HISTORY_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            requested_limit,
            reference_generation: reference.generation().as_str().to_owned(),
            source: FUNDING_HISTORY_SOURCE_V1.to_owned(),
            source_received_at,
            funding_generation: String::new(),
            oldest_funding_time_ms: events.first().map(|event| event.funding_time_ms.clone()),
            newest_funding_time_ms: events.last().map(|event| event.funding_time_ms.clone()),
            events,
        };
        snapshot.funding_generation = funding_generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

fn validate_history_request(
    reference: &ReferenceRegistry,
    instrument_id: &str,
    requested_limit: u16,
) -> Result<(), MarketHistoryError> {
    if !(1..=100).contains(&requested_limit) {
        return Err(MarketHistoryError::InvalidLimit);
    }
    let instrument = reference
        .get(instrument_id)
        .ok_or_else(|| MarketHistoryError::InstrumentNotFound(instrument_id.to_owned()))?;
    if instrument.state != "live" {
        return Err(MarketHistoryError::InstrumentNotLive(
            instrument_id.to_owned(),
        ));
    }
    Ok(())
}

fn validate_source_timestamp(value: String) -> Result<String, MarketHistoryError> {
    if value.trim().is_empty() {
        Err(MarketHistoryError::EmptySourceTimestamp)
    } else {
        Ok(value)
    }
}

fn open_interest_generation_for(
    snapshot: &OpenInterestHistorySnapshot,
) -> Result<String, MarketHistoryError> {
    let encoded = serde_json::to_vec(&(
        OPEN_INTEREST_HISTORY_SCHEMA_V1,
        &snapshot.instrument_id,
        &snapshot.period,
        snapshot.requested_limit,
        &snapshot.reference_generation,
        OPEN_INTEREST_HISTORY_SOURCE_V1,
        &snapshot.points,
    ))?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

fn trades_generation_for(snapshot: &MarketTradesSnapshot) -> Result<String, MarketHistoryError> {
    let encoded = serde_json::to_vec(&(
        MARKET_TRADES_SCHEMA_V1,
        &snapshot.instrument_id,
        snapshot.requested_limit,
        &snapshot.reference_generation,
        MARKET_TRADES_SOURCE_V1,
        &snapshot.trades,
    ))?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

fn funding_generation_for(snapshot: &FundingHistorySnapshot) -> Result<String, MarketHistoryError> {
    let encoded = serde_json::to_vec(&(
        FUNDING_HISTORY_SCHEMA_V1,
        &snapshot.instrument_id,
        snapshot.requested_limit,
        &snapshot.reference_generation,
        FUNDING_HISTORY_SOURCE_V1,
        &snapshot.events,
    ))?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

pub const MARKET_RESEARCH_SOURCE_GENERATION_SCHEMA_V1: &str =
    "okx.market-research-source-generation/v1";

pub fn market_research_source_generation(
    reference_generation: &str,
    market_generation: &str,
    history_generation: &str,
    trades_generation: &str,
    funding_generation: Option<&str>,
    open_interest_generation: Option<&str>,
) -> Result<String, MarketHistoryError> {
    let encoded = serde_json::to_vec(&(
        MARKET_RESEARCH_SOURCE_GENERATION_SCHEMA_V1,
        reference_generation,
        market_generation,
        history_generation,
        trades_generation,
        funding_generation,
        open_interest_generation,
    ))?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

fn required(field: &'static str, value: String) -> Result<String, MarketHistoryError> {
    if value.trim().is_empty() {
        Err(MarketHistoryError::MissingField(field))
    } else {
        Ok(value)
    }
}

fn optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| (!value.trim().is_empty()).then_some(value))
}

fn optional_text(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn generation_for(snapshot: &MarketHistorySnapshot) -> Result<String, MarketHistoryError> {
    #[derive(Serialize)]
    struct GenerationInput<'a> {
        schema: &'static str,
        instrument_id: &'a str,
        bar: &'a str,
        requested_limit: u16,
        reference_generation: &'a str,
        source: &'static str,
        candles: &'a [HistoryCandle],
    }

    let encoded = serde_json::to_vec(&GenerationInput {
        schema: MARKET_HISTORY_SCHEMA_V1,
        instrument_id: &snapshot.instrument_id,
        bar: &snapshot.bar,
        requested_limit: snapshot.requested_limit,
        reference_generation: &snapshot.reference_generation,
        source: MARKET_HISTORY_SOURCE_V1,
        candles: &snapshot.candles,
    })?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::{PublicFundingHistory, PublicInstrument, PublicOpenInterestHistory, PublicTrade};

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![PublicInstrument {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                instrument_family: "DOGE-USDT".to_owned(),
                underlying: "DOGE-USDT".to_owned(),
                state: "live".to_owned(),
                rule_type: "normal".to_owned(),
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
                initial_price_limit_pct: "0.05".to_owned(),
                floating_price_limit_pct: "0.03".to_owned(),
                maximum_price_limit_pct: "0.15".to_owned(),
                upcoming_parameter_changes: Vec::new(),
            }],
        )
        .expect("reference")
    }

    fn row(timestamp_ms: &str, close: &str, confirm: &str) -> PublicCandle {
        PublicCandle {
            timestamp_ms: timestamp_ms.to_owned(),
            open: "0.12".to_owned(),
            high: "0.13".to_owned(),
            low: "0.11".to_owned(),
            close: close.to_owned(),
            volume: "100".to_owned(),
            volume_currency: "12.5".to_owned(),
            volume_quote: Some("12.5".to_owned()),
            confirm: confirm.to_owned(),
        }
    }

    #[test]
    fn open_interest_history_is_chronological_and_content_addressed() {
        let rows = vec![
            PublicOpenInterestHistory {
                oi: "120".to_owned(),
                oi_currency: "12".to_owned(),
                ts: "1790470800000".to_owned(),
            },
            PublicOpenInterestHistory {
                oi: "100".to_owned(),
                oi_currency: "10".to_owned(),
                ts: "1790467200000".to_owned(),
            },
        ];

        let first = OpenInterestHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:00:00Z",
            rows.clone(),
        )
        .expect("oi history");
        let second = OpenInterestHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:01:00Z",
            rows,
        )
        .expect("oi history");

        assert_eq!(first.points[0].open_interest_contracts, "100");
        assert_eq!(first.points[1].open_interest_contracts, "120");
        assert_eq!(
            first.open_interest_generation,
            second.open_interest_generation
        );
        assert_ne!(first.source_received_at, second.source_received_at);
    }

    #[test]
    fn trades_are_chronological_content_addressed_and_typed() {
        let rows = vec![
            PublicTrade {
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                trade_id: "2".to_owned(),
                price: "0.126".to_owned(),
                size: "5".to_owned(),
                side: "sell".to_owned(),
                source: "0".to_owned(),
                timestamp_ms: "1790470800000".to_owned(),
            },
            PublicTrade {
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                trade_id: "1".to_owned(),
                price: "0.125".to_owned(),
                size: "7".to_owned(),
                side: "buy".to_owned(),
                source: "0".to_owned(),
                timestamp_ms: "1790467200000".to_owned(),
            },
        ];

        let first = MarketTradesSnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            2,
            "2026-09-27T14:00:00Z",
            rows.clone(),
        )
        .expect("trades");
        let second = MarketTradesSnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            2,
            "2026-09-27T14:01:00Z",
            rows,
        )
        .expect("trades");

        assert_eq!(first.trades[0].trade_id, "1");
        assert_eq!(first.trades[0].side, MarketTradeSide::Buy);
        assert_eq!(first.trades[1].side, MarketTradeSide::Sell);
        assert_eq!(first.trades_generation, second.trades_generation);
        assert_ne!(first.source_received_at, second.source_received_at);
    }

    #[test]
    fn research_normalization_does_not_consult_current_reference_state() {
        let candles = normalize_research_candles(
            vec![
                row("1790470800000", "0.13", "1"),
                row("1790467200000", "0.12", "1"),
            ],
            100,
        )
        .expect("research candles");
        assert_eq!(candles[0].open_time_ms, "1790467200000");
        assert_eq!(candles[1].open_time_ms, "1790470800000");

        let funding = normalize_research_funding(
            "DELISTED-USDT-SWAP",
            vec![PublicFundingHistory {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DELISTED-USDT-SWAP".to_owned(),
                funding_rate: "0.0001".to_owned(),
                funding_time_ms: "1790467200000".to_owned(),
                realized_rate: "0.00011".to_owned(),
                formula_type: "withRate".to_owned(),
                method: "current_period".to_owned(),
            }],
            400,
        )
        .expect("research funding");
        assert_eq!(funding.len(), 1);
    }

    #[test]
    fn funding_history_is_chronological_and_preserves_realized_rate() {
        let rows = vec![
            PublicFundingHistory {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                funding_rate: "-0.0002".to_owned(),
                funding_time_ms: "1790470800000".to_owned(),
                realized_rate: "-0.00019".to_owned(),
                formula_type: "withRate".to_owned(),
                method: "current_period".to_owned(),
            },
            PublicFundingHistory {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                funding_rate: "0.0001".to_owned(),
                funding_time_ms: "1790467200000".to_owned(),
                realized_rate: "0.00011".to_owned(),
                formula_type: "withRate".to_owned(),
                method: "current_period".to_owned(),
            },
        ];

        let snapshot = FundingHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            2,
            "2026-09-27T14:00:00Z",
            rows,
        )
        .expect("funding");

        assert_eq!(snapshot.events[0].funding_time_ms, "1790467200000");
        assert_eq!(snapshot.events[0].realized_rate.as_deref(), Some("0.00011"));
        assert_eq!(
            snapshot.events[1].realized_rate.as_deref(),
            Some("-0.00019")
        );
    }

    #[test]
    fn empty_trade_and_funding_windows_are_explicit_evidence() {
        let trades = MarketTradesSnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            10,
            "2026-09-27T14:00:00Z",
            Vec::new(),
        )
        .expect("trades");
        let funding = FundingHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            10,
            "2026-09-27T14:00:00Z",
            Vec::new(),
        )
        .expect("funding");

        assert!(trades.trades.is_empty());
        assert_eq!(trades.oldest_exchange_timestamp_ms, None);
        assert!(funding.events.is_empty());
        assert_eq!(funding.newest_funding_time_ms, None);
    }

    #[test]
    fn history_is_chronological_and_content_addressed() {
        let first = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:00:00.000Z",
            vec![
                row("1790470800000", "0.126", "1"),
                row("1790467200000", "0.125", "1"),
            ],
        )
        .expect("history");

        assert_eq!(first.candles[0].open_time_ms, "1790467200000");
        assert_eq!(first.candles[1].open_time_ms, "1790470800000");
        assert!(first.all_confirmed);

        let second = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:01:00.000Z",
            vec![
                row("1790470800000", "0.126", "1"),
                row("1790467200000", "0.125", "1"),
            ],
        )
        .expect("history");

        assert_eq!(first.history_generation, second.history_generation);
        assert_ne!(first.source_received_at, second.source_received_at);
    }

    #[test]
    fn history_rejects_duplicate_timestamp() {
        let error = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:00:00.000Z",
            vec![
                row("1790467200000", "0.125", "1"),
                row("1790467200000", "0.126", "1"),
            ],
        )
        .expect_err("duplicate");

        assert!(matches!(error, MarketHistoryError::DuplicateTimestamp(_)));
    }

    #[test]
    fn history_preserves_unconfirmed_state_explicitly() {
        let history = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            1,
            "2026-09-27T14:00:00.000Z",
            vec![row("1790467200000", "0.125", "0")],
        )
        .expect("history");

        assert!(!history.all_confirmed);
        assert!(!history.candles[0].confirmed);
    }
    #[test]
    fn market_research_source_generation_is_content_addressed() {
        let first = market_research_source_generation(
            "ref",
            "market",
            "history",
            "trades",
            Some("funding"),
            Some("oi"),
        )
        .expect("generation");
        let repeat = market_research_source_generation(
            "ref",
            "market",
            "history",
            "trades",
            Some("funding"),
            Some("oi"),
        )
        .expect("generation");
        let changed = market_research_source_generation(
            "ref",
            "market-2",
            "history",
            "trades",
            Some("funding"),
            Some("oi"),
        )
        .expect("generation");

        assert_eq!(first, repeat);
        assert_ne!(first, changed);
        assert!(first.starts_with("sha256:"));
    }
}
