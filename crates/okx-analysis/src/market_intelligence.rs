use std::str::FromStr;

use okx_observation::{MarketHistorySnapshot, MarketSnapshot, OrderBookSnapshot, OrderBookStatus};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{AnalysisError, analyze_history_behavior};

pub const MARKET_MICROSTRUCTURE_SCHEMA_V1: &str = "okx.market-microstructure/v1";
pub const MARKET_MICROSTRUCTURE_FORMULA_V1: &str = "market-microstructure/2026-10-02.1";
pub const REALIZED_VOLATILITY_FORMULA_V1: &str = "realized-volatility-log-return/2026-10-02.1";
pub const TOP_DEPTH_LEVELS: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketMicrostructureAnalysis {
    pub schema: &'static str,
    pub formula_version: &'static str,
    pub instrument_id: String,
    pub market_generation: String,
    pub spread_quote: String,
    pub relative_spread_ratio: String,
    pub mid_price: String,
    pub microprice: Option<String>,
    pub best_level_imbalance_ratio: Option<String>,
    pub mark_index_basis_ratio: String,
    pub top5_bid_size_contracts: Option<String>,
    pub top5_ask_size_contracts: Option<String>,
    pub top5_depth_imbalance_ratio: Option<String>,
    pub current_event_min_timestamp_ms: String,
    pub current_event_max_timestamp_ms: String,
    pub current_event_skew_ms: u64,
    pub order_book_generation: Option<u64>,
    pub order_book_seq_id: Option<i64>,
    pub order_book_exchange_timestamp_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RealizedVolatilityAnalysis {
    pub formula_version: &'static str,
    pub confirmed_candle_count: usize,
    pub return_observation_count: usize,
    pub realized_volatility_log_return_ratio: String,
}

pub fn analyze_market_microstructure(
    market: &MarketSnapshot,
    order_book: Option<&OrderBookSnapshot>,
) -> Result<MarketMicrostructureAnalysis, AnalysisError> {
    let bid = positive("best_bid", &market.ticker.best_bid)?;
    let ask = positive("best_ask", &market.ticker.best_ask)?;
    if ask < bid {
        return Err(AnalysisError::CrossedTopOfBook {
            bid: market.ticker.best_bid.clone(),
            ask: market.ticker.best_ask.clone(),
        });
    }

    let bid_size = non_negative("best_bid_size", &market.ticker.best_bid_size)?;
    let ask_size = non_negative("best_ask_size", &market.ticker.best_ask_size)?;
    let spread = ask - bid;
    let mid = (ask + bid) / Decimal::from(2_u32);
    let relative_spread = spread / mid;

    let best_size_total = bid_size + ask_size;
    let (microprice, best_level_imbalance_ratio) = if best_size_total > Decimal::ZERO {
        (
            Some(
                ((ask * bid_size + bid * ask_size) / best_size_total)
                    .normalize()
                    .to_string(),
            ),
            Some(
                ((bid_size - ask_size) / best_size_total)
                    .normalize()
                    .to_string(),
            ),
        )
    } else {
        (None, None)
    };

    let mark = positive("mark_price", &market.mark_price.price)?;
    let index = positive("index_price", &market.index_price.price)?;
    let mark_index_basis_ratio = ((mark - index) / index).normalize().to_string();

    let mut timestamps = vec![
        timestamp("ticker_ts", &market.ticker.exchange_timestamp_ms)?,
        timestamp("mark_ts", &market.mark_price.exchange_timestamp_ms)?,
        timestamp("index_ts", &market.index_price.exchange_timestamp_ms)?,
        timestamp("open_interest_ts", &market.open_interest.exchange_timestamp_ms)?,
    ];
    if let Some(funding) = &market.funding {
        timestamps.push(timestamp("funding_ts", &funding.exchange_timestamp_ms)?);
    }

    let (
        top5_bid_size_contracts,
        top5_ask_size_contracts,
        top5_depth_imbalance_ratio,
        order_book_generation,
        order_book_seq_id,
        order_book_exchange_timestamp_ms,
    ) = if let Some(book) = order_book {
        if book.status != OrderBookStatus::Contiguous {
            return Err(AnalysisError::OrderBookNotContiguous);
        }
        let book_ts_text = book
            .exchange_timestamp_ms
            .as_deref()
            .ok_or(AnalysisError::OrderBookTimestampMissing)?;
        timestamps.push(timestamp("order_book_ts", book_ts_text)?);

        let bid_sum = sum_top_sizes(&book.bids, "book_bid_size")?;
        let ask_sum = sum_top_sizes(&book.asks, "book_ask_size")?;
        let total = bid_sum + ask_sum;
        let imbalance = (total > Decimal::ZERO)
            .then(|| ((bid_sum - ask_sum) / total).normalize().to_string());

        (
            Some(bid_sum.normalize().to_string()),
            Some(ask_sum.normalize().to_string()),
            imbalance,
            Some(book.generation),
            book.seq_id,
            book.exchange_timestamp_ms.clone(),
        )
    } else {
        (None, None, None, None, None, None)
    };

    let min_ts = *timestamps
        .iter()
        .min()
        .expect("market microstructure always has timestamp inputs");
    let max_ts = *timestamps
        .iter()
        .max()
        .expect("market microstructure always has timestamp inputs");

    Ok(MarketMicrostructureAnalysis {
        schema: MARKET_MICROSTRUCTURE_SCHEMA_V1,
        formula_version: MARKET_MICROSTRUCTURE_FORMULA_V1,
        instrument_id: market.instrument_id.clone(),
        market_generation: market.market_generation.clone(),
        spread_quote: spread.normalize().to_string(),
        relative_spread_ratio: relative_spread.normalize().to_string(),
        mid_price: mid.normalize().to_string(),
        microprice,
        best_level_imbalance_ratio,
        mark_index_basis_ratio,
        top5_bid_size_contracts,
        top5_ask_size_contracts,
        top5_depth_imbalance_ratio,
        current_event_min_timestamp_ms: min_ts.to_string(),
        current_event_max_timestamp_ms: max_ts.to_string(),
        current_event_skew_ms: max_ts.saturating_sub(min_ts),
        order_book_generation,
        order_book_seq_id,
        order_book_exchange_timestamp_ms,
    })
}

pub fn analyze_realized_volatility(
    history: &MarketHistorySnapshot,
) -> Result<RealizedVolatilityAnalysis, AnalysisError> {
    let behavior = analyze_history_behavior(history)?;
    let closes = history
        .candles
        .iter()
        .filter(|candle| candle.confirmed)
        .map(|candle| {
            positive("history_close", &candle.close)?;
            candle
                .close
                .parse::<f64>()
                .map_err(|_| AnalysisError::InvalidStatisticalValue("history_close"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut sum_squared_log_returns = 0.0_f64;
    for pair in closes.windows(2) {
        let ratio = pair[1] / pair[0];
        if !ratio.is_finite() || ratio <= 0.0 {
            return Err(AnalysisError::InvalidStatisticalValue("close_ratio"));
        }
        let value = ratio.ln();
        sum_squared_log_returns += value * value;
    }
    let realized = sum_squared_log_returns.sqrt();
    if !realized.is_finite() {
        return Err(AnalysisError::InvalidStatisticalValue(
            "realized_volatility",
        ));
    }

    Ok(RealizedVolatilityAnalysis {
        formula_version: REALIZED_VOLATILITY_FORMULA_V1,
        confirmed_candle_count: behavior.confirmed_candle_count,
        return_observation_count: behavior.confirmed_candle_count - 1,
        realized_volatility_log_return_ratio: stable_statistical_text(realized),
    })
}

fn sum_top_sizes(
    levels: &[okx_observation::BookLevel],
    field: &'static str,
) -> Result<Decimal, AnalysisError> {
    levels
        .iter()
        .take(TOP_DEPTH_LEVELS)
        .try_fold(Decimal::ZERO, |sum, level| {
            Ok(sum + non_negative(field, &level.size)?)
        })
}

fn timestamp(field: &'static str, value: &str) -> Result<u64, AnalysisError> {
    value
        .parse::<u64>()
        .map_err(|_| AnalysisError::InvalidMarketTimestamp {
            field,
            value: value.to_owned(),
        })
}

fn positive(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    let value = decimal(field, value)?;
    if value <= Decimal::ZERO {
        return Err(AnalysisError::NonPositive(field));
    }
    Ok(value)
}

fn non_negative(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    let value = decimal(field, value)?;
    if value < Decimal::ZERO {
        return Err(AnalysisError::Negative(field));
    }
    Ok(value)
}

fn decimal(field: &'static str, value: &str) -> Result<Decimal, AnalysisError> {
    Decimal::from_str(value).map_err(|_| AnalysisError::InvalidDecimal {
        field,
        value: value.to_owned(),
    })
}

fn stable_statistical_text(value: f64) -> String {
    let text = format!("{value:.12}");
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-0" {
        "0".to_owned()
    } else {
        trimmed.to_owned()
    }
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
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            instrument_type: okx_api::InstrumentType::Swap,
            underlying: "DOGE-USDT".to_owned(),
            reference_generation: "ref".to_owned(),
            market_generation: "market".to_owned(),
            source_received_at: "2026-10-02T13:00:00.000Z".to_owned(),
            ticker: TickerState {
                last: "101".to_owned(),
                last_size: "1".to_owned(),
                best_ask: "101".to_owned(),
                best_ask_size: "10".to_owned(),
                best_bid: "99".to_owned(),
                best_bid_size: "30".to_owned(),
                open_24h: Some("100".to_owned()),
                high_24h: None,
                low_24h: None,
                volume_currency_24h: None,
                volume_24h: None,
                exchange_timestamp_ms: "1004".to_owned(),
            },
            mark_price: MarkPriceState {
                price: "100.5".to_owned(),
                exchange_timestamp_ms: "1003".to_owned(),
            },
            index_price: IndexPriceState {
                index_id: "DOGE-USDT".to_owned(),
                price: "100".to_owned(),
                exchange_timestamp_ms: "1002".to_owned(),
            },
            funding: Some(FundingState {
                rate: "0.0001".to_owned(),
                funding_time_ms: "2000".to_owned(),
                next_funding_time_ms: "3000".to_owned(),
                premium: None,
                min_rate: None,
                max_rate: None,
                exchange_timestamp_ms: "1001".to_owned(),
            }),
            open_interest: OpenInterestState {
                contracts: "1000".to_owned(),
                currency: None,
                usd: Some("100000".to_owned()),
                exchange_timestamp_ms: "1005".to_owned(),
            },
        }
    }

    fn book() -> OrderBookSnapshot {
        OrderBookSnapshot {
            generation: 7,
            status: OrderBookStatus::Contiguous,
            seq_id: Some(42),
            exchange_timestamp_ms: Some("1006".to_owned()),
            asks: vec![
                BookLevel { price: "101".to_owned(), size: "10".to_owned(), order_count: Some("1".to_owned()) },
                BookLevel { price: "102".to_owned(), size: "20".to_owned(), order_count: Some("1".to_owned()) },
            ],
            bids: vec![
                BookLevel { price: "99".to_owned(), size: "30".to_owned(), order_count: Some("1".to_owned()) },
                BookLevel { price: "98".to_owned(), size: "40".to_owned(), order_count: Some("1".to_owned()) },
            ],
        }
    }

    #[test]
    fn microstructure_is_exact_and_uses_contiguous_depth() {
        let value = analyze_market_microstructure(&market(), Some(&book())).expect("analysis");
        assert_eq!(value.spread_quote, "2");
        assert_eq!(value.mid_price, "100");
        assert_eq!(value.relative_spread_ratio, "0.02");
        assert_eq!(value.microprice.as_deref(), Some("100.5"));
        assert_eq!(value.best_level_imbalance_ratio.as_deref(), Some("0.5"));
        assert_eq!(value.mark_index_basis_ratio, "0.005");
        assert_eq!(value.top5_bid_size_contracts.as_deref(), Some("70"));
        assert_eq!(value.top5_ask_size_contracts.as_deref(), Some("30"));
        assert_eq!(value.top5_depth_imbalance_ratio.as_deref(), Some("0.4"));
        assert_eq!(value.current_event_min_timestamp_ms, "1001");
        assert_eq!(value.current_event_max_timestamp_ms, "1006");
        assert_eq!(value.current_event_skew_ms, 5);
        assert_eq!(value.order_book_seq_id, Some(42));
    }

    #[test]
    fn rest_only_microstructure_keeps_top_of_book_but_marks_depth_unavailable() {
        let value = analyze_market_microstructure(&market(), None).expect("analysis");
        assert_eq!(value.spread_quote, "2");
        assert_eq!(value.top5_bid_size_contracts, None);
        assert_eq!(value.top5_ask_size_contracts, None);
        assert_eq!(value.order_book_seq_id, None);
        assert_eq!(value.current_event_max_timestamp_ms, "1005");
    }

    #[test]
    fn crossed_top_of_book_fails_closed() {
        let mut market = market();
        market.ticker.best_bid = "102".to_owned();
        assert!(matches!(
            analyze_market_microstructure(&market, None),
            Err(AnalysisError::CrossedTopOfBook { .. })
        ));
    }

    #[test]
    fn invalid_book_continuity_fails_closed() {
        let mut book = book();
        book.status = OrderBookStatus::Invalid;
        assert!(matches!(
            analyze_market_microstructure(&market(), Some(&book)),
            Err(AnalysisError::OrderBookNotContiguous)
        ));
    }

    #[test]
    fn stable_statistical_text_is_bounded() {
        assert_eq!(stable_statistical_text(0.0), "0");
        assert_eq!(stable_statistical_text(0.125), "0.125");
    }
}
