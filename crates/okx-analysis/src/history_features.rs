use std::collections::BTreeSet;

use okx_observation::{FundingHistorySnapshot, MarketTradeSide, MarketTradesSnapshot};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{AnalysisError, decimal, positive_decimal};

pub const TRADE_FLOW_ANALYSIS_SCHEMA_V1: &str = "okx.trade-flow-analysis/v1";
pub const FUNDING_REGIME_ANALYSIS_SCHEMA_V1: &str = "okx.funding-regime-analysis/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TradeFlowAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub trades_generation: String,
    pub trade_count: usize,
    pub oldest_exchange_timestamp_ms: Option<String>,
    pub newest_exchange_timestamp_ms: Option<String>,
    pub buy_contracts: String,
    pub sell_contracts: String,
    pub signed_taker_imbalance_ratio: Option<String>,
    pub vwap: Option<String>,
    pub buy_vwap: Option<String>,
    pub sell_vwap: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingRegime {
    NoEvents,
    Positive,
    Negative,
    Flat,
    Mixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingRegimeAnalysis {
    pub schema: String,
    pub instrument_id: String,
    pub funding_generation: String,
    pub event_count: usize,
    pub realized_rate_coverage_count: usize,
    pub regime_rate_basis: &'static str,
    pub oldest_funding_time_ms: Option<String>,
    pub newest_funding_time_ms: Option<String>,
    pub latest_rate: Option<String>,
    pub cumulative_rate_sum: Option<String>,
    pub mean_rate: Option<String>,
    pub min_rate: Option<String>,
    pub max_rate: Option<String>,
    pub positive_event_count: usize,
    pub negative_event_count: usize,
    pub zero_event_count: usize,
    pub regime: FundingRegime,
    pub formula_types: Vec<String>,
    pub methods: Vec<String>,
}

pub fn analyze_trade_flow(
    trades: &MarketTradesSnapshot,
) -> Result<TradeFlowAnalysis, AnalysisError> {
    let mut buy_size = Decimal::ZERO;
    let mut sell_size = Decimal::ZERO;
    let mut buy_notional = Decimal::ZERO;
    let mut sell_notional = Decimal::ZERO;

    for trade in &trades.trades {
        let price = positive_decimal("trade_price", &trade.price)?;
        let size = positive_decimal("trade_size", &trade.size_contracts)?;
        match trade.side {
            MarketTradeSide::Buy => {
                buy_size += size;
                buy_notional += price * size;
            }
            MarketTradeSide::Sell => {
                sell_size += size;
                sell_notional += price * size;
            }
        }
    }

    let total_size = buy_size + sell_size;
    let total_notional = buy_notional + sell_notional;
    let imbalance = (total_size > Decimal::ZERO).then(|| {
        ((buy_size - sell_size) / total_size)
            .normalize()
            .to_string()
    });
    let vwap =
        (total_size > Decimal::ZERO).then(|| (total_notional / total_size).normalize().to_string());
    let buy_vwap =
        (buy_size > Decimal::ZERO).then(|| (buy_notional / buy_size).normalize().to_string());
    let sell_vwap =
        (sell_size > Decimal::ZERO).then(|| (sell_notional / sell_size).normalize().to_string());

    Ok(TradeFlowAnalysis {
        schema: TRADE_FLOW_ANALYSIS_SCHEMA_V1.to_owned(),
        instrument_id: trades.instrument_id.clone(),
        trades_generation: trades.trades_generation.clone(),
        trade_count: trades.trades.len(),
        oldest_exchange_timestamp_ms: trades.oldest_exchange_timestamp_ms.clone(),
        newest_exchange_timestamp_ms: trades.newest_exchange_timestamp_ms.clone(),
        buy_contracts: buy_size.normalize().to_string(),
        sell_contracts: sell_size.normalize().to_string(),
        signed_taker_imbalance_ratio: imbalance,
        vwap,
        buy_vwap,
        sell_vwap,
    })
}

pub fn analyze_funding_regime(
    funding: &FundingHistorySnapshot,
) -> Result<FundingRegimeAnalysis, AnalysisError> {
    let event_count = funding.events.len();
    let realized_rate_coverage_count = funding
        .events
        .iter()
        .filter(|event| event.realized_rate.is_some())
        .count();
    let use_realized = event_count > 0 && realized_rate_coverage_count == event_count;

    let mut rates = Vec::with_capacity(event_count);
    let mut formula_types = BTreeSet::new();
    let mut methods = BTreeSet::new();
    for event in &funding.events {
        let rate_text = if use_realized {
            event
                .realized_rate
                .as_deref()
                .expect("complete realized-rate coverage")
        } else {
            &event.funding_rate
        };
        rates.push(decimal("funding_history_rate", rate_text)?);
        if let Some(value) = &event.formula_type {
            formula_types.insert(value.clone());
        }
        if let Some(value) = &event.method {
            methods.insert(value.clone());
        }
    }

    if rates.is_empty() {
        return Ok(FundingRegimeAnalysis {
            schema: FUNDING_REGIME_ANALYSIS_SCHEMA_V1.to_owned(),
            instrument_id: funding.instrument_id.clone(),
            funding_generation: funding.funding_generation.clone(),
            event_count,
            realized_rate_coverage_count,
            regime_rate_basis: "none",
            oldest_funding_time_ms: funding.oldest_funding_time_ms.clone(),
            newest_funding_time_ms: funding.newest_funding_time_ms.clone(),
            latest_rate: None,
            cumulative_rate_sum: None,
            mean_rate: None,
            min_rate: None,
            max_rate: None,
            positive_event_count: 0,
            negative_event_count: 0,
            zero_event_count: 0,
            regime: FundingRegime::NoEvents,
            formula_types: formula_types.into_iter().collect(),
            methods: methods.into_iter().collect(),
        });
    }

    let mut sum = Decimal::ZERO;
    let mut min = rates[0];
    let mut max = rates[0];
    let mut positive = 0usize;
    let mut negative = 0usize;
    let mut zero = 0usize;
    for rate in &rates {
        sum += *rate;
        min = min.min(*rate);
        max = max.max(*rate);
        match rate.cmp(&Decimal::ZERO) {
            std::cmp::Ordering::Greater => positive += 1,
            std::cmp::Ordering::Less => negative += 1,
            std::cmp::Ordering::Equal => zero += 1,
        }
    }

    let regime = match (positive > 0, negative > 0, zero > 0) {
        (true, false, false) => FundingRegime::Positive,
        (false, true, false) => FundingRegime::Negative,
        (false, false, true) => FundingRegime::Flat,
        _ => FundingRegime::Mixed,
    };
    let mean = sum / Decimal::from(rates.len() as u64);

    Ok(FundingRegimeAnalysis {
        schema: FUNDING_REGIME_ANALYSIS_SCHEMA_V1.to_owned(),
        instrument_id: funding.instrument_id.clone(),
        funding_generation: funding.funding_generation.clone(),
        event_count,
        realized_rate_coverage_count,
        regime_rate_basis: if use_realized {
            "realized_rate"
        } else {
            "funding_rate"
        },
        oldest_funding_time_ms: funding.oldest_funding_time_ms.clone(),
        newest_funding_time_ms: funding.newest_funding_time_ms.clone(),
        latest_rate: rates.last().map(|rate| rate.normalize().to_string()),
        cumulative_rate_sum: Some(sum.normalize().to_string()),
        mean_rate: Some(mean.normalize().to_string()),
        min_rate: Some(min.normalize().to_string()),
        max_rate: Some(max.normalize().to_string()),
        positive_event_count: positive,
        negative_event_count: negative,
        zero_event_count: zero,
        regime,
        formula_types: formula_types.into_iter().collect(),
        methods: methods.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_observation::{
        FundingHistoryEvent, FundingHistorySnapshot, MarketTrade, MarketTradeSide,
        MarketTradesSnapshot,
    };

    fn trades() -> MarketTradesSnapshot {
        MarketTradesSnapshot {
            schema: "okx.market-trades/v1".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            requested_limit: 3,
            reference_generation: "ref".to_owned(),
            source: "okx_public_rest_trades".to_owned(),
            source_received_at: "2026-10-02T18:00:00Z".to_owned(),
            trades_generation: "trades".to_owned(),
            oldest_exchange_timestamp_ms: Some("1".to_owned()),
            newest_exchange_timestamp_ms: Some("3".to_owned()),
            trades: vec![
                MarketTrade {
                    trade_id: "1".to_owned(),
                    price: "100".to_owned(),
                    size_contracts: "2".to_owned(),
                    side: MarketTradeSide::Buy,
                    source: Some("0".to_owned()),
                    exchange_timestamp_ms: "1".to_owned(),
                },
                MarketTrade {
                    trade_id: "2".to_owned(),
                    price: "102".to_owned(),
                    size_contracts: "1".to_owned(),
                    side: MarketTradeSide::Buy,
                    source: Some("0".to_owned()),
                    exchange_timestamp_ms: "2".to_owned(),
                },
                MarketTrade {
                    trade_id: "3".to_owned(),
                    price: "99".to_owned(),
                    size_contracts: "1".to_owned(),
                    side: MarketTradeSide::Sell,
                    source: Some("0".to_owned()),
                    exchange_timestamp_ms: "3".to_owned(),
                },
            ],
        }
    }

    fn funding(realized: bool) -> FundingHistorySnapshot {
        FundingHistorySnapshot {
            schema: "okx.funding-history/v1".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            requested_limit: 3,
            reference_generation: "ref".to_owned(),
            source: "okx_public_rest_funding_history".to_owned(),
            source_received_at: "2026-10-02T18:00:00Z".to_owned(),
            funding_generation: "funding".to_owned(),
            oldest_funding_time_ms: Some("1".to_owned()),
            newest_funding_time_ms: Some("3".to_owned()),
            events: vec![
                FundingHistoryEvent {
                    funding_time_ms: "1".to_owned(),
                    funding_rate: "0.0001".to_owned(),
                    realized_rate: realized.then(|| "0.00011".to_owned()),
                    formula_type: Some("withRate".to_owned()),
                    method: Some("current_period".to_owned()),
                },
                FundingHistoryEvent {
                    funding_time_ms: "2".to_owned(),
                    funding_rate: "-0.0002".to_owned(),
                    realized_rate: realized.then(|| "-0.00019".to_owned()),
                    formula_type: Some("withRate".to_owned()),
                    method: Some("current_period".to_owned()),
                },
                FundingHistoryEvent {
                    funding_time_ms: "3".to_owned(),
                    funding_rate: "0".to_owned(),
                    realized_rate: realized.then(|| "0".to_owned()),
                    formula_type: Some("withRate".to_owned()),
                    method: Some("current_period".to_owned()),
                },
            ],
        }
    }

    #[test]
    fn trade_flow_reports_signed_taker_imbalance_and_vwap() {
        let analysis = analyze_trade_flow(&trades()).expect("trade flow");

        assert_eq!(analysis.buy_contracts, "3");
        assert_eq!(analysis.sell_contracts, "1");
        assert_eq!(
            analysis.signed_taker_imbalance_ratio.as_deref(),
            Some("0.5")
        );
        assert_eq!(analysis.vwap.as_deref(), Some("100.25"));
        assert_eq!(
            analysis.buy_vwap.as_deref(),
            Some("100.66666666666666666666666667")
        );
        assert_eq!(analysis.sell_vwap.as_deref(), Some("99"));
    }

    #[test]
    fn complete_realized_rates_drive_mixed_funding_regime() {
        let analysis = analyze_funding_regime(&funding(true)).expect("funding");

        assert_eq!(analysis.regime_rate_basis, "realized_rate");
        assert_eq!(analysis.realized_rate_coverage_count, 3);
        assert_eq!(analysis.regime, FundingRegime::Mixed);
        assert_eq!(analysis.positive_event_count, 1);
        assert_eq!(analysis.negative_event_count, 1);
        assert_eq!(analysis.zero_event_count, 1);
        assert_eq!(analysis.cumulative_rate_sum.as_deref(), Some("-0.00008"));
    }

    #[test]
    fn incomplete_realized_coverage_uses_declared_rates_consistently() {
        let mut input = funding(true);
        input.events[1].realized_rate = None;

        let analysis = analyze_funding_regime(&input).expect("funding");
        assert_eq!(analysis.regime_rate_basis, "funding_rate");
        assert_eq!(analysis.realized_rate_coverage_count, 2);
        assert_eq!(analysis.cumulative_rate_sum.as_deref(), Some("-0.0001"));
    }

    #[test]
    fn empty_windows_are_explicit_not_ready_like_evidence_without_failure() {
        let mut trades = trades();
        trades.trades.clear();
        trades.oldest_exchange_timestamp_ms = None;
        trades.newest_exchange_timestamp_ms = None;
        let trade = analyze_trade_flow(&trades).expect("trade flow");
        assert_eq!(trade.trade_count, 0);
        assert_eq!(trade.signed_taker_imbalance_ratio, None);
        assert_eq!(trade.vwap, None);

        let mut funding = funding(true);
        funding.events.clear();
        funding.oldest_funding_time_ms = None;
        funding.newest_funding_time_ms = None;
        let analysis = analyze_funding_regime(&funding).expect("funding");
        assert_eq!(analysis.regime, FundingRegime::NoEvents);
        assert_eq!(analysis.mean_rate, None);
    }
}
