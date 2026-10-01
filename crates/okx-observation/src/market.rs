use okx_api::{
    InstrumentType, PublicFundingRate, PublicIndexTicker, PublicMarkPrice, PublicOpenInterest,
    PublicTicker,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{FundingRequirement, ReferenceRegistry};

pub const MARKET_SNAPSHOT_SCHEMA_V1: &str = "okx.market-snapshot/v1";
pub const SNAPSHOT_QUALITY_SCHEMA_V1: &str = "okx.snapshot-quality/v1";
pub const M2_REST_BOOTSTRAP_REASON: &str = "M2_REST_BOOTSTRAP_ONLY";

#[derive(Debug, Clone)]
pub struct MarketBootstrap {
    pub ticker: PublicTicker,
    pub mark_price: PublicMarkPrice,
    pub index_ticker: PublicIndexTicker,
    pub funding_rate: Option<PublicFundingRate>,
    pub open_interest: PublicOpenInterest,
}

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
    pub last: String,
    pub last_size: String,
    pub best_ask: String,
    pub best_ask_size: String,
    pub best_bid: String,
    pub best_bid_size: String,
    pub open_24h: Option<String>,
    pub high_24h: Option<String>,
    pub low_24h: Option<String>,
    pub volume_currency_24h: Option<String>,
    pub volume_24h: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarkPriceState {
    pub price: String,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexPriceState {
    pub index_id: String,
    pub price: String,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingState {
    pub rate: String,
    pub funding_time_ms: String,
    pub next_funding_time_ms: String,
    pub premium: Option<String>,
    pub min_rate: Option<String>,
    pub max_rate: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OpenInterestState {
    pub contracts: String,
    pub currency: Option<String>,
    pub usd: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketSnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub instrument_type: InstrumentType,
    pub underlying: String,
    pub reference_generation: String,
    pub market_generation: String,
    pub source_received_at: String,
    pub ticker: TickerState,
    pub mark_price: MarkPriceState,
    pub index_price: IndexPriceState,
    pub funding: Option<FundingState>,
    pub open_interest: OpenInterestState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotQualityReport {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub reference_source_received_at: String,
    pub market_mode: String,
    pub persistent_ws_connected: bool,
    pub sequence_continuity_proven: bool,
    pub reason: String,
}

impl SnapshotQualityReport {
    pub fn m2(reference: &ReferenceRegistry, instrument_id: &str) -> Result<Self, MarketError> {
        let instrument = reference
            .get(instrument_id)
            .ok_or_else(|| MarketError::InstrumentNotFound(instrument_id.to_owned()))?;
        if instrument.state != "live" {
            return Err(MarketError::InstrumentNotLive(instrument_id.to_owned()));
        }

        Ok(Self {
            schema: SNAPSHOT_QUALITY_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            reference_generation: reference.generation().as_str().to_owned(),
            reference_source_received_at: reference.source_received_at().to_owned(),
            market_mode: "rest_bootstrap".to_owned(),
            persistent_ws_connected: false,
            sequence_continuity_proven: false,
            reason: M2_REST_BOOTSTRAP_REASON.to_owned(),
        })
    }
}

#[derive(Debug, Error)]
pub enum MarketError {
    #[error("market source receive timestamp is empty")]
    EmptySourceTimestamp,

    #[error("instrument '{0}' is not present in the reference registry")]
    InstrumentNotFound(String),

    #[error("instrument '{0}' is not live")]
    InstrumentNotLive(String),

    #[error("instrument '{0}' has no underlying/index id in reference data")]
    MissingUnderlying(String),

    #[error("{origin} instrument mismatch: expected '{expected}', got '{actual}'")]
    InstrumentMismatch {
        origin: &'static str,
        expected: String,
        actual: String,
    },

    #[error("index ticker mismatch: expected '{expected}', got '{actual}'")]
    IndexMismatch { expected: String, actual: String },

    #[error("{origin} is missing required field '{field}'")]
    MissingField {
        origin: &'static str,
        field: &'static str,
    },

    #[error("market snapshot requires funding-rate data for this instrument")]
    FundingRequired,

    #[error("market snapshot contains funding-rate data for a non-funding instrument")]
    UnexpectedFunding,

    #[error(
        "instrument '{instrument_id}' has unknown funding semantics for ruleType '{rule_type}'"
    )]
    UnknownFundingRequirement {
        instrument_id: String,
        rule_type: String,
    },

    #[error("failed to serialize normalized market snapshot: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl MarketSnapshot {
    pub fn from_bootstrap(
        reference: &ReferenceRegistry,
        instrument_id: &str,
        source_received_at: impl Into<String>,
        bootstrap: MarketBootstrap,
    ) -> Result<Self, MarketError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(MarketError::EmptySourceTimestamp);
        }

        let instrument = reference
            .get(instrument_id)
            .ok_or_else(|| MarketError::InstrumentNotFound(instrument_id.to_owned()))?;
        if instrument.state != "live" {
            return Err(MarketError::InstrumentNotLive(instrument_id.to_owned()));
        }

        let underlying = instrument
            .underlying
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| MarketError::MissingUnderlying(instrument_id.to_owned()))?
            .to_owned();

        require_instrument("ticker", instrument_id, &bootstrap.ticker.instrument_id)?;
        require_instrument(
            "mark price",
            instrument_id,
            &bootstrap.mark_price.instrument_id,
        )?;
        require_instrument(
            "open interest",
            instrument_id,
            &bootstrap.open_interest.instrument_id,
        )?;

        if bootstrap.index_ticker.instrument_id != underlying {
            return Err(MarketError::IndexMismatch {
                expected: underlying,
                actual: bootstrap.index_ticker.instrument_id,
            });
        }

        let ticker = TickerState {
            last: required("ticker", "last", bootstrap.ticker.last)?,
            last_size: required("ticker", "lastSz", bootstrap.ticker.last_size)?,
            best_ask: required("ticker", "askPx", bootstrap.ticker.ask_price)?,
            best_ask_size: required("ticker", "askSz", bootstrap.ticker.ask_size)?,
            best_bid: required("ticker", "bidPx", bootstrap.ticker.bid_price)?,
            best_bid_size: required("ticker", "bidSz", bootstrap.ticker.bid_size)?,
            open_24h: optional(bootstrap.ticker.open_24h),
            high_24h: optional(bootstrap.ticker.high_24h),
            low_24h: optional(bootstrap.ticker.low_24h),
            volume_currency_24h: optional(bootstrap.ticker.volume_currency_24h),
            volume_24h: optional(bootstrap.ticker.volume_24h),
            exchange_timestamp_ms: required("ticker", "ts", bootstrap.ticker.ts)?,
        };

        let mark_price = MarkPriceState {
            price: required("mark price", "markPx", bootstrap.mark_price.mark_price)?,
            exchange_timestamp_ms: required("mark price", "ts", bootstrap.mark_price.ts)?,
        };

        let index_price = IndexPriceState {
            index_id: underlying.clone(),
            price: required("index ticker", "idxPx", bootstrap.index_ticker.index_price)?,
            exchange_timestamp_ms: required("index ticker", "ts", bootstrap.index_ticker.ts)?,
        };

        let funding = match instrument.funding_requirement {
            FundingRequirement::Required => {
                let funding = bootstrap.funding_rate.ok_or(MarketError::FundingRequired)?;
                require_instrument("funding rate", instrument_id, &funding.instrument_id)?;
                Some(FundingState {
                    rate: required("funding rate", "fundingRate", funding.funding_rate)?,
                    funding_time_ms: required("funding rate", "fundingTime", funding.funding_time)?,
                    next_funding_time_ms: required(
                        "funding rate",
                        "nextFundingTime",
                        funding.next_funding_time,
                    )?,
                    premium: optional(funding.premium),
                    min_rate: optional(funding.min_funding_rate),
                    max_rate: optional(funding.max_funding_rate),
                    exchange_timestamp_ms: required("funding rate", "ts", funding.ts)?,
                })
            }
            FundingRequirement::NotApplicable => {
                if bootstrap.funding_rate.is_some() {
                    return Err(MarketError::UnexpectedFunding);
                }
                None
            }
            FundingRequirement::Unknown => {
                return Err(MarketError::UnknownFundingRequirement {
                    instrument_id: instrument_id.to_owned(),
                    rule_type: instrument
                        .rule_type
                        .clone()
                        .unwrap_or_else(|| "<missing>".to_owned()),
                });
            }
        };

        let open_interest = OpenInterestState {
            contracts: required("open interest", "oi", bootstrap.open_interest.oi)?,
            currency: optional(bootstrap.open_interest.oi_currency),
            usd: optional(bootstrap.open_interest.oi_usd),
            exchange_timestamp_ms: required("open interest", "ts", bootstrap.open_interest.ts)?,
        };

        let mut snapshot = Self {
            schema: MARKET_SNAPSHOT_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            instrument_type: instrument.instrument_type,
            underlying,
            reference_generation: reference.generation().as_str().to_owned(),
            market_generation: String::new(),
            source_received_at,
            ticker,
            mark_price,
            index_price,
            funding,
            open_interest,
        };
        snapshot.market_generation = generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

fn require_instrument(
    origin: &'static str,
    expected: &str,
    actual: &str,
) -> Result<(), MarketError> {
    if actual == expected {
        Ok(())
    } else {
        Err(MarketError::InstrumentMismatch {
            origin,
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        })
    }
}

fn required(
    origin: &'static str,
    field: &'static str,
    value: String,
) -> Result<String, MarketError> {
    if value.trim().is_empty() {
        Err(MarketError::MissingField { origin, field })
    } else {
        Ok(value)
    }
}

fn optional(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn generation_for(snapshot: &MarketSnapshot) -> Result<String, MarketError> {
    #[derive(Serialize)]
    struct GenerationInput<'a> {
        schema: &'static str,
        instrument_id: &'a str,
        instrument_type: InstrumentType,
        underlying: &'a str,
        reference_generation: &'a str,
        ticker: &'a TickerState,
        mark_price: &'a MarkPriceState,
        index_price: &'a IndexPriceState,
        funding: &'a Option<FundingState>,
        open_interest: &'a OpenInterestState,
    }

    let encoded = serde_json::to_vec(&GenerationInput {
        schema: MARKET_SNAPSHOT_SCHEMA_V1,
        instrument_id: &snapshot.instrument_id,
        instrument_type: snapshot.instrument_type,
        underlying: &snapshot.underlying,
        reference_generation: &snapshot.reference_generation,
        ticker: &snapshot.ticker,
        mark_price: &snapshot.mark_price,
        index_price: &snapshot.index_price,
        funding: &snapshot.funding,
        open_interest: &snapshot.open_interest,
    })?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;

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

    fn bootstrap() -> MarketBootstrap {
        MarketBootstrap {
            ticker: PublicTicker {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                last: "0.123456".to_owned(),
                last_size: "17".to_owned(),
                ask_price: "0.123457".to_owned(),
                ask_size: "25".to_owned(),
                bid_price: "0.123455".to_owned(),
                bid_size: "31".to_owned(),
                open_24h: "0.12".to_owned(),
                high_24h: "0.13".to_owned(),
                low_24h: "0.11".to_owned(),
                volume_currency_24h: "1234567.89".to_owned(),
                volume_24h: "7654321".to_owned(),
                sod_utc0: "0.121".to_owned(),
                sod_utc8: "0.122".to_owned(),
                ts: "1790467200123".to_owned(),
            },
            mark_price: PublicMarkPrice {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                mark_price: "0.123450".to_owned(),
                ts: "1790467200124".to_owned(),
            },
            index_ticker: PublicIndexTicker {
                instrument_id: "DOGE-USDT".to_owned(),
                index_price: "0.123440".to_owned(),
                open_24h: "0.12".to_owned(),
                high_24h: "0.13".to_owned(),
                low_24h: "0.11".to_owned(),
                sod_utc0: "0.121".to_owned(),
                sod_utc8: "0.122".to_owned(),
                ts: "1790467200125".to_owned(),
            },
            funding_rate: Some(PublicFundingRate {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                funding_rate: "0.00001234".to_owned(),
                funding_time: "1790467200000".to_owned(),
                next_funding_time: "1790496000000".to_owned(),
                impact_value: "10000".to_owned(),
                interest_rate: "0.0001".to_owned(),
                premium: "0.00000001".to_owned(),
                min_funding_rate: "-0.003".to_owned(),
                max_funding_rate: "0.003".to_owned(),
                method: "current_period".to_owned(),
                formula_type: "withRate".to_owned(),
                settlement_state: String::new(),
                settled_funding_rate: String::new(),
                ts: "1790467199000".to_owned(),
            }),
            open_interest: PublicOpenInterest {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                oi: "123456789".to_owned(),
                oi_currency: "1234567.89".to_owned(),
                oi_usd: "152345678.90".to_owned(),
                ts: "1790467200126".to_owned(),
            },
        }
    }

    #[test]
    fn snapshot_uses_reference_underlying_and_preserves_exact_values() {
        let reference = reference();
        let snapshot = MarketSnapshot::from_bootstrap(
            &reference,
            "DOGE-USDT-SWAP",
            "2026-09-27T00:00:01.000Z",
            bootstrap(),
        )
        .expect("snapshot");

        assert_eq!(snapshot.underlying, "DOGE-USDT");
        assert_eq!(snapshot.ticker.best_bid, "0.123455");
        assert_eq!(snapshot.ticker.best_ask, "0.123457");
        assert_eq!(snapshot.mark_price.price, "0.123450");
        assert_eq!(snapshot.index_price.price, "0.123440");
        assert_eq!(
            snapshot.funding.as_ref().expect("funding").rate,
            "0.00001234"
        );
        assert_eq!(snapshot.open_interest.contracts, "123456789");
    }

    #[test]
    fn generation_is_content_based_not_receive_time_based() {
        let reference = reference();
        let first = MarketSnapshot::from_bootstrap(
            &reference,
            "DOGE-USDT-SWAP",
            "2026-09-27T00:00:01.000Z",
            bootstrap(),
        )
        .expect("first");
        let second = MarketSnapshot::from_bootstrap(
            &reference,
            "DOGE-USDT-SWAP",
            "2026-09-27T00:00:02.000Z",
            bootstrap(),
        )
        .expect("second");

        assert_eq!(first.market_generation, second.market_generation);
        assert_ne!(first.source_received_at, second.source_received_at);
    }

    #[test]
    fn index_mismatch_fails_closed() {
        let reference = reference();
        let mut data = bootstrap();
        data.index_ticker.instrument_id = "BTC-USDT".to_owned();

        let error = MarketSnapshot::from_bootstrap(
            &reference,
            "DOGE-USDT-SWAP",
            "2026-09-27T00:00:01.000Z",
            data,
        )
        .expect_err("mismatch");

        assert!(matches!(error, MarketError::IndexMismatch { .. }));
    }

    #[test]
    fn snapshot_quality_reports_m2_rest_only_readiness() {
        let reference = reference();
        let report =
            SnapshotQualityReport::m2(&reference, "DOGE-USDT-SWAP").expect("quality report");

        assert_eq!(report.reason, M2_REST_BOOTSTRAP_REASON);
        assert_eq!(report.market_mode, "rest_bootstrap");
        assert!(!report.persistent_ws_connected);
        assert!(!report.sequence_continuity_proven);
    }

    #[test]
    fn swap_requires_funding() {
        let reference = reference();
        let mut data = bootstrap();
        data.funding_rate = None;

        let error = MarketSnapshot::from_bootstrap(
            &reference,
            "DOGE-USDT-SWAP",
            "2026-09-27T00:00:01.000Z",
            data,
        )
        .expect_err("funding");

        assert!(matches!(error, MarketError::FundingRequired));
    }
}
