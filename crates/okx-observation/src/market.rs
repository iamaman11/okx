use okx_api::{FundingRate, IndexTicker, InstrumentType, MarkPrice, OpenInterest, Ticker};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::reference::InstrumentSpec;

pub const MARKET_SNAPSHOT_SCHEMA_V1: &str = "okx.market-snapshot/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct MarketGeneration(String);

impl MarketGeneration {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TickerState {
    pub last_price: String,
    pub last_size: Option<String>,
    pub best_ask_price: String,
    pub best_ask_size: String,
    pub best_bid_price: String,
    pub best_bid_size: String,
    pub open_24h: Option<String>,
    pub high_24h: Option<String>,
    pub low_24h: Option<String>,
    pub volume_currency_24h: Option<String>,
    pub volume_24h: Option<String>,
    pub exchange_ts_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PricePoint {
    pub price: String,
    pub exchange_ts_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingState {
    pub funding_rate: String,
    pub funding_time_ms: String,
    pub next_funding_time_ms: String,
    pub method: Option<String>,
    pub formula_type: Option<String>,
    pub min_funding_rate: Option<String>,
    pub max_funding_rate: Option<String>,
    pub interest_rate: Option<String>,
    pub premium: Option<String>,
    pub exchange_ts_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OpenInterestState {
    pub contracts: String,
    pub currency_amount: Option<String>,
    pub usd_amount: Option<String>,
    pub exchange_ts_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketSnapshot {
    pub schema: &'static str,
    pub reference_generation: String,
    pub market_generation: String,
    pub source_received_at: String,
    pub instrument_id: String,
    pub instrument_type: InstrumentType,
    pub index_id: String,
    pub ticker: TickerState,
    pub mark: PricePoint,
    pub index: PricePoint,
    pub funding: Option<FundingState>,
    pub open_interest: OpenInterestState,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MarketError {
    #[error("market source receive timestamp is empty")]
    EmptySourceTimestamp,

    #[error("instrument '{0}' has no reference underlying/index id")]
    MissingIndexId(String),

    #[error("{source} returned instrument '{actual}', expected '{expected}'")]
    InstrumentMismatch {
        source: &'static str,
        expected: String,
        actual: String,
    },

    #[error("{source} returned instrument type '{actual}', expected '{expected}'")]
    InstrumentTypeMismatch {
        source: &'static str,
        expected: String,
        actual: String,
    },

    #[error("{source} is missing required field '{field}'")]
    MissingRequiredField {
        source: &'static str,
        field: &'static str,
    },

    #[error("funding data is required for SWAP instrument '{0}'")]
    MissingFunding(String),

    #[error("failed to serialize normalized market snapshot: {0}")]
    Serialization(String),
}

impl MarketSnapshot {
    pub fn from_public(
        reference_generation: &str,
        source_received_at: impl Into<String>,
        instrument: &InstrumentSpec,
        ticker: Ticker,
        mark: MarkPrice,
        index: IndexTicker,
        funding: Option<FundingRate>,
        open_interest: OpenInterest,
    ) -> Result<Self, MarketError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(MarketError::EmptySourceTimestamp);
        }

        let expected_type = instrument.instrument_type.to_string();
        require_source_id("ticker", &instrument.instrument_id, &ticker.instrument_id)?;
        require_source_type("ticker", &expected_type, &ticker.instrument_type)?;
        require_source_id("mark", &instrument.instrument_id, &mark.instrument_id)?;
        require_source_type("mark", &expected_type, &mark.instrument_type)?;
        require_source_id(
            "open_interest",
            &instrument.instrument_id,
            &open_interest.instrument_id,
        )?;
        require_source_type(
            "open_interest",
            &expected_type,
            &open_interest.instrument_type,
        )?;

        let index_id = instrument
            .underlying
            .as_deref()
            .or(instrument.instrument_family.as_deref())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| MarketError::MissingIndexId(instrument.instrument_id.clone()))?;
        require_source_id("index", index_id, &index.instrument_id)?;

        let ticker_state = TickerState {
            last_price: require("ticker", "last", ticker.last)?,
            last_size: optional(ticker.last_size),
            best_ask_price: require("ticker", "askPx", ticker.ask_price)?,
            best_ask_size: require("ticker", "askSz", ticker.ask_size)?,
            best_bid_price: require("ticker", "bidPx", ticker.bid_price)?,
            best_bid_size: require("ticker", "bidSz", ticker.bid_size)?,
            open_24h: optional(ticker.open_24h),
            high_24h: optional(ticker.high_24h),
            low_24h: optional(ticker.low_24h),
            volume_currency_24h: optional(ticker.volume_currency_24h),
            volume_24h: optional(ticker.volume_24h),
            exchange_ts_ms: require("ticker", "ts", ticker.ts)?,
        };

        let mark_state = PricePoint {
            price: require("mark", "markPx", mark.mark_price)?,
            exchange_ts_ms: require("mark", "ts", mark.ts)?,
        };

        let index_state = PricePoint {
            price: require("index", "idxPx", index.index_price)?,
            exchange_ts_ms: require("index", "ts", index.ts)?,
        };

        let funding_state = match instrument.instrument_type {
            InstrumentType::Swap => {
                let funding = funding
                    .ok_or_else(|| MarketError::MissingFunding(instrument.instrument_id.clone()))?;
                require_source_id("funding", &instrument.instrument_id, &funding.instrument_id)?;
                require_source_type("funding", &expected_type, &funding.instrument_type)?;
                Some(FundingState {
                    funding_rate: require("funding", "fundingRate", funding.funding_rate)?,
                    funding_time_ms: require("funding", "fundingTime", funding.funding_time)?,
                    next_funding_time_ms: require(
                        "funding",
                        "nextFundingTime",
                        funding.next_funding_time,
                    )?,
                    method: optional(funding.method),
                    formula_type: optional(funding.formula_type),
                    min_funding_rate: optional(funding.min_funding_rate),
                    max_funding_rate: optional(funding.max_funding_rate),
                    interest_rate: optional(funding.interest_rate),
                    premium: optional(funding.premium),
                    exchange_ts_ms: require("funding", "ts", funding.ts)?,
                })
            }
            InstrumentType::Futures => None,
        };

        let open_interest_state = OpenInterestState {
            contracts: require("open_interest", "oi", open_interest.oi)?,
            currency_amount: optional(open_interest.open_interest_currency),
            usd_amount: optional(open_interest.open_interest_usd),
            exchange_ts_ms: require("open_interest", "ts", open_interest.ts)?,
        };

        let market_generation = generation_for(
            reference_generation,
            &instrument.instrument_id,
            instrument.instrument_type,
            index_id,
            &ticker_state,
            &mark_state,
            &index_state,
            funding_state.as_ref(),
            &open_interest_state,
        )?;

        Ok(Self {
            schema: MARKET_SNAPSHOT_SCHEMA_V1,
            reference_generation: reference_generation.to_owned(),
            market_generation: market_generation.0,
            source_received_at,
            instrument_id: instrument.instrument_id.clone(),
            instrument_type: instrument.instrument_type,
            index_id: index_id.to_owned(),
            ticker: ticker_state,
            mark: mark_state,
            index: index_state,
            funding: funding_state,
            open_interest: open_interest_state,
        })
    }
}

fn require_source_id(
    source: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), MarketError> {
    if actual == expected {
        Ok(())
    } else {
        Err(MarketError::InstrumentMismatch {
            source,
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

fn require_source_type(
    source: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), MarketError> {
    if actual == expected {
        Ok(())
    } else {
        Err(MarketError::InstrumentTypeMismatch {
            source,
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

fn require(
    source: &'static str,
    field: &'static str,
    value: String,
) -> Result<String, MarketError> {
    if value.trim().is_empty() {
        Err(MarketError::MissingRequiredField { source, field })
    } else {
        Ok(value)
    }
}

fn optional(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

#[allow(clippy::too_many_arguments)]
fn generation_for(
    reference_generation: &str,
    instrument_id: &str,
    instrument_type: InstrumentType,
    index_id: &str,
    ticker: &TickerState,
    mark: &PricePoint,
    index: &PricePoint,
    funding: Option<&FundingState>,
    open_interest: &OpenInterestState,
) -> Result<MarketGeneration, MarketError> {
    #[derive(Serialize)]
    struct Input<'a> {
        schema: &'static str,
        reference_generation: &'a str,
        instrument_id: &'a str,
        instrument_type: InstrumentType,
        index_id: &'a str,
        ticker: &'a TickerState,
        mark: &'a PricePoint,
        index: &'a PricePoint,
        funding: Option<&'a FundingState>,
        open_interest: &'a OpenInterestState,
    }

    let bytes = serde_json::to_vec(&Input {
        schema: MARKET_SNAPSHOT_SCHEMA_V1,
        reference_generation,
        instrument_id,
        instrument_type,
        index_id,
        ticker,
        mark,
        index,
        funding,
        open_interest,
    })
    .map_err(|error| MarketError::Serialization(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(MarketGeneration(format!("sha256:{digest:x}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::InstrumentSpec;

    fn instrument() -> InstrumentSpec {
        InstrumentSpec {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            instrument_type: InstrumentType::Swap,
            instrument_family: Some("DOGE-USDT".to_owned()),
            underlying: Some("DOGE-USDT".to_owned()),
            state: "live".to_owned(),
            rule_type: Some("normal".to_owned()),
            base_currency: None,
            quote_currency: None,
            settle_currency: Some("USDT".to_owned()),
            tick_size: "0.00001".to_owned(),
            lot_size: "0.01".to_owned(),
            min_size: "0.01".to_owned(),
            max_limit_size: Some("100000000".to_owned()),
            max_market_size: Some("24000".to_owned()),
            max_limit_amount: Some("20000000".to_owned()),
            max_market_amount: None,
            contract_type: Some("linear".to_owned()),
            contract_value: Some("1000".to_owned()),
            contract_value_currency: Some("DOGE".to_owned()),
            fee_group_id: Some("4".to_owned()),
            max_leverage: Some("50".to_owned()),
            list_time_ms: Some("1587463971000".to_owned()),
            expiry_time_ms: None,
        }
    }

    fn ticker() -> Ticker {
        Ticker {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            last: "0.123456".to_owned(),
            last_size: "10".to_owned(),
            ask_price: "0.123457".to_owned(),
            ask_size: "11".to_owned(),
            bid_price: "0.123455".to_owned(),
            bid_size: "12".to_owned(),
            open_24h: "0.120000".to_owned(),
            high_24h: "0.130000".to_owned(),
            low_24h: "0.110000".to_owned(),
            volume_currency_24h: "123456".to_owned(),
            volume_24h: "987654".to_owned(),
            ts: "1790460000100".to_owned(),
        }
    }

    fn mark() -> MarkPrice {
        MarkPrice {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            instrument_family: "DOGE-USDT".to_owned(),
            mark_price: "0.123450".to_owned(),
            ts: "1790460000200".to_owned(),
        }
    }

    fn index() -> IndexTicker {
        IndexTicker {
            instrument_id: "DOGE-USDT".to_owned(),
            index_price: "0.123440".to_owned(),
            high_24h: "0.130000".to_owned(),
            low_24h: "0.110000".to_owned(),
            open_24h: "0.120000".to_owned(),
            ts: "1790460000300".to_owned(),
        }
    }

    fn funding() -> FundingRate {
        FundingRate {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            method: "current_period".to_owned(),
            formula_type: "withRate".to_owned(),
            funding_rate: "0.000123".to_owned(),
            funding_time: "1790467200000".to_owned(),
            next_funding_time: "1790496000000".to_owned(),
            min_funding_rate: "-0.00375".to_owned(),
            max_funding_rate: "0.00375".to_owned(),
            interest_rate: "0.0001".to_owned(),
            premium: "0.000023".to_owned(),
            ts: "1790460000400".to_owned(),
        }
    }

    fn open_interest() -> OpenInterest {
        OpenInterest {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            oi: "123456789".to_owned(),
            open_interest_currency: "123456789000".to_owned(),
            open_interest_usd: "15234567.89".to_owned(),
            ts: "1790460000500".to_owned(),
        }
    }

    fn snapshot(received_at: &str) -> MarketSnapshot {
        MarketSnapshot::from_public(
            "sha256:reference",
            received_at,
            &instrument(),
            ticker(),
            mark(),
            index(),
            Some(funding()),
            open_interest(),
        )
        .expect("snapshot")
    }

    #[test]
    fn normalizes_coherent_swap_market_sources() {
        let snapshot = snapshot("2026-09-27T00:00:00.000Z");

        assert_eq!(snapshot.instrument_id, "DOGE-USDT-SWAP");
        assert_eq!(snapshot.index_id, "DOGE-USDT");
        assert_eq!(snapshot.ticker.last_price, "0.123456");
        assert_eq!(snapshot.mark.price, "0.123450");
        assert_eq!(snapshot.index.price, "0.123440");
        assert_eq!(
            snapshot.funding.as_ref().expect("funding").funding_rate,
            "0.000123"
        );
        assert_eq!(snapshot.open_interest.contracts, "123456789");
        assert!(snapshot.market_generation.starts_with("sha256:"));
    }

    #[test]
    fn generation_ignores_local_receive_time_but_binds_exchange_content() {
        let first = snapshot("2026-09-27T00:00:00.000Z");
        let second = snapshot("2026-09-27T00:01:00.000Z");

        assert_eq!(first.market_generation, second.market_generation);

        let mut changed_ticker = ticker();
        changed_ticker.last = "0.123457".to_owned();
        let changed = MarketSnapshot::from_public(
            "sha256:reference",
            "2026-09-27T00:01:00.000Z",
            &instrument(),
            changed_ticker,
            mark(),
            index(),
            Some(funding()),
            open_interest(),
        )
        .expect("changed snapshot");

        assert_ne!(first.market_generation, changed.market_generation);
    }

    #[test]
    fn source_instrument_mismatch_fails_closed() {
        let mut wrong = mark();
        wrong.instrument_id = "BTC-USDT-SWAP".to_owned();

        let error = MarketSnapshot::from_public(
            "sha256:reference",
            "2026-09-27T00:00:00.000Z",
            &instrument(),
            ticker(),
            wrong,
            index(),
            Some(funding()),
            open_interest(),
        )
        .expect_err("mismatch");

        assert!(matches!(
            error,
            MarketError::InstrumentMismatch { source: "mark", .. }
        ));
    }

    #[test]
    fn swap_requires_funding_source() {
        let error = MarketSnapshot::from_public(
            "sha256:reference",
            "2026-09-27T00:00:00.000Z",
            &instrument(),
            ticker(),
            mark(),
            index(),
            None,
            open_interest(),
        )
        .expect_err("missing funding");

        assert!(matches!(error, MarketError::MissingFunding(_)));
    }

    #[test]
    fn required_bid_ask_and_timestamps_fail_closed() {
        let mut missing = ticker();
        missing.ask_price.clear();

        let error = MarketSnapshot::from_public(
            "sha256:reference",
            "2026-09-27T00:00:00.000Z",
            &instrument(),
            missing,
            mark(),
            index(),
            Some(funding()),
            open_interest(),
        )
        .expect_err("missing ask");

        assert_eq!(
            error,
            MarketError::MissingRequiredField {
                source: "ticker",
                field: "askPx"
            }
        );
    }
}
