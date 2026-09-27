use okx_api::{
    PublicFundingRate, PublicIndexTicker, PublicMarkPrice, PublicOpenInterest, PublicTicker,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    FundingRequirement, MarketBootstrap, MarketError, MarketSnapshot, OrderBookError,
    OrderBookMessage, OrderBookSnapshot, OrderBookState, OrderBookStatus, ReferenceRegistry,
};

#[derive(Debug, Clone)]
struct Observed<T> {
    generation: u64,
    received_at_ms: u64,
    value: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketReadiness {
    NotReady,
    Degraded,
    Fresh,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketReadinessReport {
    pub quality: MarketReadiness,
    pub reason: String,
    pub generation: u64,
    pub reference_generation: String,
    pub sequence_continuity_proven: bool,
    pub oldest_required_receive_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveMarketSnapshot {
    pub generation: u64,
    pub market: MarketSnapshot,
    pub order_book: OrderBookSnapshot,
}

#[derive(Debug)]
pub struct MarketStreamState {
    instrument_id: String,
    generation: u64,
    reference_generation: String,
    ticker: Option<Observed<PublicTicker>>,
    mark_price: Option<Observed<PublicMarkPrice>>,
    index_ticker: Option<Observed<PublicIndexTicker>>,
    funding_rate: Option<Observed<PublicFundingRate>>,
    open_interest: Option<Observed<PublicOpenInterest>>,
    order_book: OrderBookState,
    order_book_received_at_ms: Option<u64>,
}

#[derive(Debug, Error)]
pub enum MarketStreamError {
    #[error("stream update generation {actual} does not match current generation {expected}")]
    GenerationMismatch { expected: u64, actual: u64 },

    #[error("instrument '{0}' is not present in reference data")]
    InstrumentNotFound(String),

    #[error("reference generation changed from '{expected}' to '{actual}'")]
    ReferenceGenerationMismatch { expected: String, actual: String },

    #[error("streaming market dependencies are incomplete")]
    Incomplete,

    #[error("order book is not sequence-contiguous")]
    OrderBookNotContiguous,

    #[error("order-book error: {0}")]
    OrderBook(#[from] OrderBookError),

    #[error("market normalization error: {0}")]
    Market(#[from] MarketError),
}

impl MarketStreamState {
    pub fn new(
        instrument_id: impl Into<String>,
        generation: u64,
        reference_generation: impl Into<String>,
    ) -> Self {
        Self {
            instrument_id: instrument_id.into(),
            generation,
            reference_generation: reference_generation.into(),
            ticker: None,
            mark_price: None,
            index_ticker: None,
            funding_rate: None,
            open_interest: None,
            order_book: OrderBookState::waiting(generation),
            order_book_received_at_ms: None,
        }
    }

    pub fn reset_generation(
        &mut self,
        generation: u64,
        reference_generation: impl Into<String>,
    ) {
        self.generation = generation;
        self.reference_generation = reference_generation.into();
        self.ticker = None;
        self.mark_price = None;
        self.index_ticker = None;
        self.funding_rate = None;
        self.open_interest = None;
        self.order_book = OrderBookState::waiting(generation);
        self.order_book_received_at_ms = None;
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn apply_ticker(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: PublicTicker,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.ticker = Some(Observed {
            generation,
            received_at_ms,
            value,
        });
        Ok(())
    }

    pub fn apply_mark_price(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: PublicMarkPrice,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.mark_price = Some(Observed {
            generation,
            received_at_ms,
            value,
        });
        Ok(())
    }

    pub fn apply_index_ticker(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: PublicIndexTicker,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.index_ticker = Some(Observed {
            generation,
            received_at_ms,
            value,
        });
        Ok(())
    }

    pub fn apply_funding_rate(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: PublicFundingRate,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.funding_rate = Some(Observed {
            generation,
            received_at_ms,
            value,
        });
        Ok(())
    }

    pub fn apply_open_interest(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: PublicOpenInterest,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.open_interest = Some(Observed {
            generation,
            received_at_ms,
            value,
        });
        Ok(())
    }

    pub fn apply_book_snapshot(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: OrderBookMessage,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.order_book.apply_snapshot(generation, value)?;
        self.order_book_received_at_ms = Some(received_at_ms);
        Ok(())
    }

    pub fn apply_book_update(
        &mut self,
        generation: u64,
        received_at_ms: u64,
        value: OrderBookMessage,
    ) -> Result<(), MarketStreamError> {
        self.require_generation(generation)?;
        self.order_book.apply_update(generation, value)?;
        self.order_book_received_at_ms = Some(received_at_ms);
        Ok(())
    }

    pub fn readiness(
        &self,
        reference: &ReferenceRegistry,
        now_ms: u64,
        max_age_ms: u64,
        connection_active: bool,
        subscriptions_complete: bool,
        rest_fallback_available: bool,
    ) -> MarketReadinessReport {
        let fallback_quality = || {
            if rest_fallback_available {
                MarketReadiness::Degraded
            } else {
                MarketReadiness::NotReady
            }
        };

        if !connection_active {
            return self.report(fallback_quality(), "WS_CONNECTION_NOT_ACTIVE", None);
        }
        if !subscriptions_complete {
            return self.report(fallback_quality(), "WS_SUBSCRIPTIONS_INCOMPLETE", None);
        }
        if reference.generation().as_str() != self.reference_generation {
            return self.report(fallback_quality(), "REFERENCE_GENERATION_CHANGED", None);
        }

        let Some(instrument) = reference.get(&self.instrument_id) else {
            return self.report(fallback_quality(), "REFERENCE_INSTRUMENT_MISSING", None);
        };
        if instrument.state != "live" {
            return self.report(fallback_quality(), "REFERENCE_INSTRUMENT_NOT_LIVE", None);
        }
        if instrument.funding_requirement == FundingRequirement::Unknown {
            return self.report(fallback_quality(), "FUNDING_SEMANTICS_UNKNOWN", None);
        }

        let Some(received_times) = self.required_receive_times(instrument.funding_requirement) else {
            return self.report(fallback_quality(), "WS_DEPENDENCIES_INCOMPLETE", None);
        };
        if self.order_book.status() != OrderBookStatus::Contiguous {
            return self.report(fallback_quality(), "ORDER_BOOK_NOT_CONTIGUOUS", None);
        }

        let oldest = received_times.into_iter().min();
        let Some(oldest) = oldest else {
            return self.report(fallback_quality(), "WS_DEPENDENCIES_INCOMPLETE", None);
        };
        if now_ms.saturating_sub(oldest) > max_age_ms {
            return self.report(MarketReadiness::Stale, "WS_DEPENDENCY_STALE", Some(oldest));
        }

        if self
            .bootstrap_if_complete(reference, instrument.funding_requirement)
            .and_then(|bootstrap| {
                MarketSnapshot::from_bootstrap(
                    reference,
                    &self.instrument_id,
                    "stream-readiness",
                    bootstrap,
                )
                .map_err(MarketStreamError::from)
            })
            .is_err()
        {
            return self.report(fallback_quality(), "WS_DEPENDENCY_INCONSISTENT", Some(oldest));
        }

        self.report(MarketReadiness::Fresh, "WS_CURRENT_GENERATION_COMPLETE", Some(oldest))
    }

    pub fn publish(
        &self,
        reference: &ReferenceRegistry,
        source_received_at: impl Into<String>,
    ) -> Result<LiveMarketSnapshot, MarketStreamError> {
        if reference.generation().as_str() != self.reference_generation {
            return Err(MarketStreamError::ReferenceGenerationMismatch {
                expected: self.reference_generation.clone(),
                actual: reference.generation().as_str().to_owned(),
            });
        }
        let instrument = reference
            .get(&self.instrument_id)
            .ok_or_else(|| MarketStreamError::InstrumentNotFound(self.instrument_id.clone()))?;
        if self.order_book.status() != OrderBookStatus::Contiguous {
            return Err(MarketStreamError::OrderBookNotContiguous);
        }

        let bootstrap = self.bootstrap_if_complete(reference, instrument.funding_requirement)?;
        let market = MarketSnapshot::from_bootstrap(
            reference,
            &self.instrument_id,
            source_received_at,
            bootstrap,
        )?;

        Ok(LiveMarketSnapshot {
            generation: self.generation,
            market,
            order_book: self.order_book.snapshot(),
        })
    }

    fn require_generation(&self, generation: u64) -> Result<(), MarketStreamError> {
        if generation == self.generation {
            Ok(())
        } else {
            Err(MarketStreamError::GenerationMismatch {
                expected: self.generation,
                actual: generation,
            })
        }
    }

    fn bootstrap_if_complete(
        &self,
        _reference: &ReferenceRegistry,
        funding_requirement: FundingRequirement,
    ) -> Result<MarketBootstrap, MarketStreamError> {
        let ticker = self
            .current(&self.ticker)
            .ok_or(MarketStreamError::Incomplete)?
            .clone();
        let mark_price = self
            .current(&self.mark_price)
            .ok_or(MarketStreamError::Incomplete)?
            .clone();
        let index_ticker = self
            .current(&self.index_ticker)
            .ok_or(MarketStreamError::Incomplete)?
            .clone();
        let open_interest = self
            .current(&self.open_interest)
            .ok_or(MarketStreamError::Incomplete)?
            .clone();

        let funding_rate = match funding_requirement {
            FundingRequirement::Required => Some(
                self.current(&self.funding_rate)
                    .ok_or(MarketStreamError::Incomplete)?
                    .clone(),
            ),
            FundingRequirement::NotApplicable => None,
            FundingRequirement::Unknown => return Err(MarketStreamError::Incomplete),
        };

        Ok(MarketBootstrap {
            ticker,
            mark_price,
            index_ticker,
            funding_rate,
            open_interest,
        })
    }

    fn current<'a, T>(&self, value: &'a Option<Observed<T>>) -> Option<&'a T> {
        value
            .as_ref()
            .filter(|observed| observed.generation == self.generation)
            .map(|observed| &observed.value)
    }

    fn required_receive_times(
        &self,
        funding_requirement: FundingRequirement,
    ) -> Option<Vec<u64>> {
        let mut times = vec![
            self.ticker.as_ref()?.received_at_ms,
            self.mark_price.as_ref()?.received_at_ms,
            self.index_ticker.as_ref()?.received_at_ms,
            self.open_interest.as_ref()?.received_at_ms,
            self.order_book_received_at_ms?,
        ];
        match funding_requirement {
            FundingRequirement::Required => times.push(self.funding_rate.as_ref()?.received_at_ms),
            FundingRequirement::NotApplicable => {}
            FundingRequirement::Unknown => return None,
        }
        Some(times)
    }

    fn report(
        &self,
        quality: MarketReadiness,
        reason: &str,
        oldest_required_receive_ms: Option<u64>,
    ) -> MarketReadinessReport {
        MarketReadinessReport {
            quality,
            reason: reason.to_owned(),
            generation: self.generation,
            reference_generation: self.reference_generation.clone(),
            sequence_continuity_proven: self.order_book.status() == OrderBookStatus::Contiguous,
            oldest_required_receive_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::{InstrumentType, PublicInstrument};

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
            }],
        )
        .expect("reference")
    }

    fn ticker() -> PublicTicker {
        PublicTicker {
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
        }
    }

    fn mark() -> PublicMarkPrice {
        PublicMarkPrice {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            mark_price: "0.123450".to_owned(),
            ts: "1790467200124".to_owned(),
        }
    }

    fn index() -> PublicIndexTicker {
        PublicIndexTicker {
            instrument_id: "DOGE-USDT".to_owned(),
            index_price: "0.123440".to_owned(),
            open_24h: "0.12".to_owned(),
            high_24h: "0.13".to_owned(),
            low_24h: "0.11".to_owned(),
            sod_utc0: "0.121".to_owned(),
            sod_utc8: "0.122".to_owned(),
            ts: "1790467200125".to_owned(),
        }
    }

    fn funding() -> PublicFundingRate {
        PublicFundingRate {
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
        }
    }

    fn oi() -> PublicOpenInterest {
        PublicOpenInterest {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            oi: "123456789".to_owned(),
            oi_currency: "1234567.89".to_owned(),
            oi_usd: "152345678.90".to_owned(),
            ts: "1790467200126".to_owned(),
        }
    }

    fn book() -> OrderBookMessage {
        OrderBookMessage {
            asks: vec![crate::BookLevelUpdate {
                price: "0.123460".to_owned(),
                size: "10".to_owned(),
                order_count: Some("1".to_owned()),
            }],
            bids: vec![crate::BookLevelUpdate {
                price: "0.123450".to_owned(),
                size: "12".to_owned(),
                order_count: Some("2".to_owned()),
            }],
            exchange_timestamp_ms: "1790467200127".to_owned(),
            seq_id: 10,
            prev_seq_id: -1,
        }
    }

    fn complete(state: &mut MarketStreamState, generation: u64, received: u64) {
        state.apply_ticker(generation, received, ticker()).expect("ticker");
        state
            .apply_mark_price(generation, received, mark())
            .expect("mark");
        state
            .apply_index_ticker(generation, received, index())
            .expect("index");
        state
            .apply_funding_rate(generation, received, funding())
            .expect("funding");
        state
            .apply_open_interest(generation, received, oi())
            .expect("oi");
        state
            .apply_book_snapshot(generation, received, book())
            .expect("book");
    }

    #[test]
    fn complete_current_generation_becomes_fresh() {
        let reference = reference();
        let mut state = MarketStreamState::new(
            "DOGE-USDT-SWAP",
            7,
            reference.generation().as_str(),
        );
        complete(&mut state, 7, 1_000);

        let report = state.readiness(&reference, 1_500, 1_000, true, true, true);
        assert_eq!(report.quality, MarketReadiness::Fresh);
        assert!(report.sequence_continuity_proven);

        let snapshot = state
            .publish(&reference, "2026-09-27T00:00:01.500Z")
            .expect("snapshot");
        assert_eq!(snapshot.generation, 7);
        assert_eq!(snapshot.market.instrument_type, InstrumentType::Swap);
        assert_eq!(snapshot.order_book.seq_id, Some(10));
    }

    #[test]
    fn incomplete_ws_is_degraded_only_when_rest_fallback_exists() {
        let reference = reference();
        let state = MarketStreamState::new(
            "DOGE-USDT-SWAP",
            7,
            reference.generation().as_str(),
        );

        assert_eq!(
            state
                .readiness(&reference, 1_500, 1_000, true, true, true)
                .quality,
            MarketReadiness::Degraded
        );
        assert_eq!(
            state
                .readiness(&reference, 1_500, 1_000, true, true, false)
                .quality,
            MarketReadiness::NotReady
        );
    }

    #[test]
    fn reconnect_generation_revokes_fresh_until_rebuilt() {
        let reference = reference();
        let mut state = MarketStreamState::new(
            "DOGE-USDT-SWAP",
            7,
            reference.generation().as_str(),
        );
        complete(&mut state, 7, 1_000);
        assert_eq!(
            state
                .readiness(&reference, 1_100, 1_000, true, true, true)
                .quality,
            MarketReadiness::Fresh
        );

        state.reset_generation(8, reference.generation().as_str());
        assert_eq!(
            state
                .readiness(&reference, 1_100, 1_000, true, true, true)
                .quality,
            MarketReadiness::Degraded
        );
        assert!(state.publish(&reference, "now").is_err());
    }

    #[test]
    fn aged_required_dependency_is_stale() {
        let reference = reference();
        let mut state = MarketStreamState::new(
            "DOGE-USDT-SWAP",
            7,
            reference.generation().as_str(),
        );
        complete(&mut state, 7, 1_000);

        let report = state.readiness(&reference, 3_001, 2_000, true, true, true);
        assert_eq!(report.quality, MarketReadiness::Stale);
        assert_eq!(report.reason, "WS_DEPENDENCY_STALE");
    }
}
