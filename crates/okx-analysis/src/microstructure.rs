use std::{cmp::Ordering, str::FromStr};

use okx_observation::{MarketSnapshot, OrderBookSnapshot, OrderBookStatus};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::AnalysisError;

pub const MARKET_INTELLIGENCE_ANALYSIS_SCHEMA_V1: &str = "okx.market-intelligence-analysis/v1";
pub const SPREAD_BPS_METRIC_ID: &str = "spread_bps";
pub const SPREAD_BPS_METRIC_VERSION_V1: &str = "spread_bps/v1";
pub const SPREAD_BPS_UNIT: &str = "basis_points";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpreadBps {
    value: Decimal,
}

impl SpreadBps {
    pub fn value_text(&self) -> String {
        self.value.normalize().to_string()
    }
}

impl Ord for SpreadBps {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for SpreadBps {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookSweepAnalysis {
    pub requested_contracts: String,
    pub available_contracts: String,
    pub filled_contracts: String,
    pub complete: bool,
    pub vwap: Option<String>,
    pub worst_price: Option<String>,
    pub impact_bps_from_mid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketIntelligenceAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub market_generation: String,
    pub order_book_generation: u64,
    pub order_book_seq_id: i64,
    pub order_book_exchange_timestamp_ms: String,
    pub depth_levels: u16,
    pub best_bid: String,
    pub best_ask: String,
    pub mid_price: String,
    pub spread_price: String,
    pub spread_bps: String,
    pub bid_depth_contracts: String,
    pub ask_depth_contracts: String,
    pub buy_sweep: BookSweepAnalysis,
    pub sell_sweep: BookSweepAnalysis,
    pub last_price: String,
    pub mark_price: String,
    pub index_price: String,
    pub mark_index_basis_bps: String,
    pub last_mark_deviation_bps: String,
    pub funding_rate: Option<String>,
    pub next_funding_time_ms: Option<String>,
    pub open_interest_contracts: String,
    pub open_interest_usd: Option<String>,
}

pub fn analyze_spread_bps(best_bid: &str, best_ask: &str) -> Result<SpreadBps, AnalysisError> {
    let bid = positive_decimal("best_bid", best_bid)?;
    let ask = positive_decimal("best_ask", best_ask)?;
    if ask <= bid {
        return Err(AnalysisError::CrossedOrderBook {
            best_bid: best_bid.to_owned(),
            best_ask: best_ask.to_owned(),
        });
    }
    let mid = (bid + ask) / Decimal::from(2_u32);
    let value = ((ask - bid) / mid) * Decimal::from(10_000_u32);
    Ok(SpreadBps { value })
}

pub fn analyze_market_intelligence(
    instrument_id: &str,
    reference_generation: &str,
    market: &MarketSnapshot,
    order_book: &OrderBookSnapshot,
    impact_contracts: &str,
    depth_levels: u16,
) -> Result<MarketIntelligenceAnalysis, AnalysisError> {
    if instrument_id != market.instrument_id {
        return Err(AnalysisError::InstrumentMismatch);
    }
    if reference_generation != market.reference_generation {
        return Err(AnalysisError::ReferenceGenerationMismatch);
    }
    if order_book.status != OrderBookStatus::Contiguous {
        return Err(AnalysisError::OrderBookNotContiguous);
    }
    let seq_id = order_book
        .seq_id
        .ok_or(AnalysisError::OrderBookMissingSequence)?;
    let exchange_timestamp_ms = order_book
        .exchange_timestamp_ms
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or(AnalysisError::OrderBookMissingTimestamp)?;
    if !(1..=50).contains(&depth_levels) {
        return Err(AnalysisError::InvalidOrderBookDepth(depth_levels));
    }

    let requested = positive_decimal("impact_contracts", impact_contracts)?;
    let asks = normalized_side(&order_book.asks, depth_levels, true)?;
    let bids = normalized_side(&order_book.bids, depth_levels, false)?;
    let (best_ask, _) = asks
        .first()
        .copied()
        .ok_or(AnalysisError::OrderBookEmptyAsk)?;
    let (best_bid, _) = bids
        .first()
        .copied()
        .ok_or(AnalysisError::OrderBookEmptyBid)?;
    if best_ask <= best_bid {
        return Err(AnalysisError::CrossedOrderBook {
            best_bid: best_bid.normalize().to_string(),
            best_ask: best_ask.normalize().to_string(),
        });
    }

    let mid = (best_bid + best_ask) / Decimal::from(2_u32);
    let spread_price = best_ask - best_bid;
    let spread_bps = analyze_spread_bps(
        &best_bid.normalize().to_string(),
        &best_ask.normalize().to_string(),
    )?;

    let bid_depth = bids.iter().map(|(_, size)| *size).sum::<Decimal>();
    let ask_depth = asks.iter().map(|(_, size)| *size).sum::<Decimal>();
    let buy_sweep = sweep_book(&asks, requested, mid, true);
    let sell_sweep = sweep_book(&bids, requested, mid, false);

    let last = positive_decimal("last_price", &market.ticker.last)?;
    let mark = positive_decimal("mark_price", &market.mark_price.price)?;
    let index = positive_decimal("index_price", &market.index_price.price)?;
    let basis_bps = ((mark - index) / index) * Decimal::from(10_000_u32);
    let last_mark_bps = ((last - mark) / mark) * Decimal::from(10_000_u32);

    Ok(MarketIntelligenceAnalysis {
        schema: MARKET_INTELLIGENCE_ANALYSIS_SCHEMA_V1.to_owned(),
        instrument_id: market.instrument_id.clone(),
        reference_generation: market.reference_generation.clone(),
        market_generation: market.market_generation.clone(),
        order_book_generation: order_book.generation,
        order_book_seq_id: seq_id,
        order_book_exchange_timestamp_ms: exchange_timestamp_ms.to_owned(),
        depth_levels,
        best_bid: best_bid.normalize().to_string(),
        best_ask: best_ask.normalize().to_string(),
        mid_price: mid.normalize().to_string(),
        spread_price: spread_price.normalize().to_string(),
        spread_bps: spread_bps.value_text(),
        bid_depth_contracts: bid_depth.normalize().to_string(),
        ask_depth_contracts: ask_depth.normalize().to_string(),
        buy_sweep,
        sell_sweep,
        last_price: last.normalize().to_string(),
        mark_price: mark.normalize().to_string(),
        index_price: index.normalize().to_string(),
        mark_index_basis_bps: basis_bps.normalize().to_string(),
        last_mark_deviation_bps: last_mark_bps.normalize().to_string(),
        funding_rate: market.funding.as_ref().map(|value| value.rate.clone()),
        next_funding_time_ms: market
            .funding
            .as_ref()
            .map(|value| value.next_funding_time_ms.clone()),
        open_interest_contracts: market.open_interest.contracts.clone(),
        open_interest_usd: market.open_interest.usd.clone(),
    })
}

fn normalized_side(
    levels: &[okx_observation::BookLevel],
    depth_levels: u16,
    ascending: bool,
) -> Result<Vec<(Decimal, Decimal)>, AnalysisError> {
    let mut result = Vec::new();
    let mut previous: Option<Decimal> = None;
    for level in levels.iter().take(usize::from(depth_levels)) {
        let price = positive_decimal("order_book_price", &level.price)?;
        let size = positive_decimal("order_book_size", &level.size)?;
        if let Some(previous) = previous {
            let valid = if ascending {
                price > previous
            } else {
                price < previous
            };
            if !valid {
                return Err(AnalysisError::OrderBookNotStrictlySorted);
            }
        }
        previous = Some(price);
        result.push((price, size));
    }
    Ok(result)
}

fn sweep_book(
    levels: &[(Decimal, Decimal)],
    requested: Decimal,
    mid: Decimal,
    buy: bool,
) -> BookSweepAnalysis {
    let available = levels.iter().map(|(_, size)| *size).sum::<Decimal>();
    let mut remaining = requested;
    let mut filled = Decimal::ZERO;
    let mut weighted = Decimal::ZERO;
    let mut worst = None;

    for (price, size) in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let take = remaining.min(*size);
        if take <= Decimal::ZERO {
            continue;
        }
        filled += take;
        weighted += *price * take;
        remaining -= take;
        worst = Some(*price);
    }

    let vwap = if filled > Decimal::ZERO {
        Some(weighted / filled)
    } else {
        None
    };
    let impact = vwap.map(|price| {
        if buy {
            ((price - mid) / mid) * Decimal::from(10_000_u32)
        } else {
            ((mid - price) / mid) * Decimal::from(10_000_u32)
        }
    });

    BookSweepAnalysis {
        requested_contracts: requested.normalize().to_string(),
        available_contracts: available.normalize().to_string(),
        filled_contracts: filled.normalize().to_string(),
        complete: filled >= requested,
        vwap: vwap.map(|value| value.normalize().to_string()),
        worst_price: worst.map(|value| value.normalize().to_string()),
        impact_bps_from_mid: impact.map(|value| value.normalize().to_string()),
    }
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    if value.is_empty() || value.len() > 64 {
        return Err(AnalysisError::InvalidDecimal {
            field,
            value: value.to_owned(),
        });
    }
    let mut dots = 0_usize;
    let mut digits = 0_usize;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'0'..=b'9' => digits += 1,
            b'.' if dots == 0 => dots += 1,
            b'+' | b'-' if index == 0 => {}
            _ => {
                return Err(AnalysisError::InvalidDecimal {
                    field,
                    value: value.to_owned(),
                });
            }
        }
    }
    if digits == 0 {
        return Err(AnalysisError::InvalidDecimal {
            field,
            value: value.to_owned(),
        });
    }
    Decimal::from_str(value).map_err(|_| AnalysisError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn positive_decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    let value = decimal(field, value)?;
    if value <= Decimal::ZERO {
        return Err(AnalysisError::NonPositive(field));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_observation::{
        BookLevel, FundingState, IndexPriceState, MarkPriceState, OpenInterestState, TickerState,
    };

    fn market() -> MarketSnapshot {
        MarketSnapshot {
            schema: "okx.market-snapshot/v1".to_owned(),
            instrument_id: "AAA-USDT-SWAP".to_owned(),
            instrument_type: InstrumentType::Swap,
            underlying: "AAA-USDT".to_owned(),
            reference_generation: "ref-1".to_owned(),
            market_generation: "mkt-1".to_owned(),
            source_received_at: "2026-10-02T14:00:01Z".to_owned(),
            ticker: TickerState {
                last: "100.5".to_owned(),
                last_size: "1".to_owned(),
                best_ask: "101".to_owned(),
                best_ask_size: "5".to_owned(),
                best_bid: "100".to_owned(),
                best_bid_size: "5".to_owned(),
                open_24h: Some("99".to_owned()),
                high_24h: Some("102".to_owned()),
                low_24h: Some("98".to_owned()),
                volume_currency_24h: Some("1000".to_owned()),
                volume_24h: Some("1000".to_owned()),
                exchange_timestamp_ms: "1790950000000".to_owned(),
            },
            mark_price: MarkPriceState {
                price: "100.4".to_owned(),
                exchange_timestamp_ms: "1790950000001".to_owned(),
            },
            index_price: IndexPriceState {
                index_id: "AAA-USDT".to_owned(),
                price: "100".to_owned(),
                exchange_timestamp_ms: "1790950000002".to_owned(),
            },
            funding: Some(FundingState {
                rate: "0.0001".to_owned(),
                funding_time_ms: "1790956800000".to_owned(),
                next_funding_time_ms: "1790985600000".to_owned(),
                premium: Some("0.0001".to_owned()),
                min_rate: Some("-0.003".to_owned()),
                max_rate: Some("0.003".to_owned()),
                exchange_timestamp_ms: "1790950000003".to_owned(),
            }),
            open_interest: OpenInterestState {
                contracts: "10000".to_owned(),
                currency: Some("10000".to_owned()),
                usd: Some("1000000".to_owned()),
                exchange_timestamp_ms: "1790950000004".to_owned(),
            },
        }
    }

    fn book() -> OrderBookSnapshot {
        OrderBookSnapshot {
            generation: 7,
            status: OrderBookStatus::Contiguous,
            seq_id: Some(42),
            exchange_timestamp_ms: Some("1790950000005".to_owned()),
            asks: vec![
                BookLevel {
                    price: "101".to_owned(),
                    size: "2".to_owned(),
                    order_count: Some("1".to_owned()),
                },
                BookLevel {
                    price: "102".to_owned(),
                    size: "3".to_owned(),
                    order_count: Some("1".to_owned()),
                },
            ],
            bids: vec![
                BookLevel {
                    price: "100".to_owned(),
                    size: "2".to_owned(),
                    order_count: Some("1".to_owned()),
                },
                BookLevel {
                    price: "99".to_owned(),
                    size: "4".to_owned(),
                    order_count: Some("1".to_owned()),
                },
            ],
        }
    }

    #[test]
    fn spread_bps_is_exact() {
        assert_eq!(
            analyze_spread_bps("100", "101")
                .expect("spread")
                .value_text(),
            "99.5024875621890547263681592"
        );
    }

    #[test]
    fn microstructure_computes_depth_basis_and_symmetric_sweeps() {
        let result =
            analyze_market_intelligence("AAA-USDT-SWAP", "ref-1", &market(), &book(), "3", 2).expect("analysis");

        assert_eq!(result.best_bid, "100");
        assert_eq!(result.best_ask, "101");
        assert_eq!(result.bid_depth_contracts, "6");
        assert_eq!(result.ask_depth_contracts, "5");
        assert!(result.buy_sweep.complete);
        assert!(result.sell_sweep.complete);
        assert_eq!(result.buy_sweep.filled_contracts, "3");
        assert_eq!(result.sell_sweep.filled_contracts, "3");
        assert_eq!(result.mark_index_basis_bps, "40");
        assert_eq!(result.last_mark_deviation_bps, "9.960159362549800796812749");
    }

    #[test]
    fn insufficient_depth_is_evidence_not_an_analysis_failure() {
        let result =
            analyze_market_intelligence("AAA-USDT-SWAP", "ref-1", &market(), &book(), "10", 2).expect("analysis");
        assert!(!result.buy_sweep.complete);
        assert_eq!(result.buy_sweep.filled_contracts, "5");
        assert!(!result.sell_sweep.complete);
        assert_eq!(result.sell_sweep.filled_contracts, "6");
    }

    #[test]
    fn crossed_or_non_contiguous_book_fails_closed() {
        assert!(matches!(
            analyze_spread_bps("101", "100"),
            Err(AnalysisError::CrossedOrderBook { .. })
        ));

        let mut invalid = book();
        invalid.status = OrderBookStatus::Invalid;
        assert!(matches!(
            analyze_market_intelligence(&rules(), &market(), &invalid, "1", 2),
            Err(AnalysisError::OrderBookNotContiguous)
        ));
    }
}
