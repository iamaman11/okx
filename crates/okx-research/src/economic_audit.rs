//! Independent, bounded cash ledger for already-produced causal replay trades.
//! Reuses the replay's exact Decimal primitives but reconstructs positions,
//! cash, funding and every equity point from trade records and closed bars.
//! It does not claim venue mark prices, historical fees or trading permission.
use crate::economics::{QuoteAmount, SettlementCashflow};
use crate::strategy_registry::{
    Bar, Mechanics, Money, ReplayExecution, ReplayResult, ReplaySpec, dec, mul,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::Serialize;

pub const REPLAY_ECONOMIC_AUDIT_VERSION: &str = "okx.research.replay-economic-audit/v1";

#[derive(Debug, Clone, Serialize)]
pub struct ReplayEconomicAudit {
    pub schema: &'static str,
    pub result_hash: String,
    pub dataset_hash: String,
    pub settlement_currency: String,
    pub gross_exact: String,
    pub fees_exact: String,
    /// Positive means an expense; negative means received funding.
    pub funding_cost_exact: String,
    /// The legacy Replay Summary expresses funding as income instead.
    pub funding_income_exact: String,
    pub net_exact: String,
    pub trade_entries_checked: usize,
    pub equity_points_checked: usize,
    pub score_start_index: usize,
    pub score_end_index: usize,
    pub accounting_identity: &'static str,
    pub trade_price_basis: &'static str,
    pub funding_price_basis: &'static str,
    pub historical_rules_verified: bool,
    pub historical_fees_verified: bool,
    pub historical_publication_time_verified: bool,
}

fn approximately(left: f64, right: f64) -> bool {
    left.is_finite()
        && right.is_finite()
        && (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
}

pub fn audit_replay(
    bars: &[Bar],
    spec: &ReplaySpec,
    result: &ReplayResult,
    currency: &str,
    score_start: usize,
    score_end: usize,
    cancel: &impl Fn() -> bool,
) -> Result<ReplayEconomicAudit, String> {
    audit_graph_replay(
        bars,
        &ReplayExecution::from(spec),
        result,
        currency,
        score_start,
        score_end,
        cancel,
    )
}

/// The same independent money oracle for graph and registered strategies.
/// Only the producer of the position intents differs.
pub fn audit_graph_replay(
    bars: &[Bar],
    execution: &ReplayExecution,
    result: &ReplayResult,
    currency: &str,
    score_start: usize,
    score_end: usize,
    cancel: &impl Fn() -> bool,
) -> Result<ReplayEconomicAudit, String> {
    QuoteAmount::exact(currency, Decimal::ZERO)?;
    if score_start >= score_end
        || score_end > bars.len()
        || result.equity.len() != score_end - score_start
        || result.summary.trades != result.trades.len()
    {
        return Err("REPLAY_AUDIT_RANGE_OR_COUNT_MISMATCH".into());
    }
    let (multiplier, short_allowed) = match &execution.mechanics {
        Mechanics::Spot => (1.0, false),
        Mechanics::Linear {
            contract_multiplier,
        } if contract_multiplier.is_finite() && *contract_multiplier > 0.0 => {
            (*contract_multiplier, true)
        }
        _ => return Err("REPLAY_AUDIT_UNSUPPORTED_MECHANICS".into()),
    };
    let mut money = Money::new();
    let mut position = 0i32;
    let mut next_trade = 0usize;
    for (offset, bar) in bars[score_start..score_end].iter().enumerate() {
        if offset.is_multiple_of(4096) && cancel() {
            return Err("CANCELLED".into());
        }
        while let Some(trade) = result.trades.get(next_trade) {
            if trade.time < bar.time {
                return Err("REPLAY_AUDIT_TRADE_OFF_GRID".into());
            }
            if trade.time != bar.time {
                break;
            }
            if !matches!(trade.side, -1 | 1)
                || !trade.quantity.is_finite()
                || trade.quantity <= 0.0
                || !trade.price.is_finite()
                || trade.price <= 0.0
                || !trade.fee.is_finite()
                || trade.fee < 0.0
            {
                return Err("REPLAY_AUDIT_INVALID_TRADE".into());
            }
            let lots = trade.quantity / multiplier;
            let amount = if approximately(lots, 1.0) {
                1
            } else if approximately(lots, 2.0) {
                2
            } else {
                return Err("REPLAY_AUDIT_TRADE_QUANTITY_MISMATCH".into());
            };
            let next_position = position + i32::from(trade.side) * amount;
            if !(-1..=1).contains(&next_position) || (!short_allowed && next_position < 0) {
                return Err("REPLAY_AUDIT_INVALID_POSITION_TRANSITION".into());
            }
            let old_fees = money.fees;
            money.trade(
                f64::from(trade.side) * trade.quantity,
                trade.price,
                execution.costs.taker_fee_bps,
            )?;
            let recorded_fee = money
                .fees
                .checked_sub(old_fees)
                .and_then(|v| v.to_f64())
                .ok_or("MONEY_PRECISION")?;
            if !approximately(trade.fee, recorded_fee) {
                return Err("REPLAY_AUDIT_TRADE_FEE_MISMATCH".into());
            }
            position = next_position;
            next_trade += 1;
        }
        if execution.costs.observed_funding && bar.funding_rate.is_none() {
            return Err("HISTORICAL_FUNDING_MISSING".into());
        }
        let rate = bar.funding_rate.unwrap_or(0.0);
        let funding = f64::from(position) * multiplier * bar.close * rate;
        if funding != 0.0 {
            money.fund(f64::from(position) * multiplier, bar.close, rate)?;
        }
        let equity = money.cash.to_f64().ok_or("MONEY_PRECISION")?
            + f64::from(position) * multiplier * bar.close;
        let recorded = result.equity[offset];
        if recorded.0 != bar.time || !approximately(recorded.1, equity) {
            return Err("REPLAY_AUDIT_EQUITY_MISMATCH".into());
        }
    }
    if next_trade != result.trades.len() {
        return Err("REPLAY_AUDIT_TRADE_OUTSIDE_RANGE".into());
    }
    let net = money
        .cash
        .checked_add(mul(
            dec(f64::from(position) * multiplier)?,
            dec(bars[score_end - 1].close)?,
        )?)
        .and_then(|value| value.checked_sub(Decimal::from(10_000)))
        .ok_or("MONEY_OVERFLOW")?;
    let reported_net = QuoteAmount::parse_exact(currency, &result.summary.pnl_exact)?;
    let reported_fees = QuoteAmount::parse_exact(currency, &result.summary.fees_exact)?;
    let reported_income = QuoteAmount::parse_exact(currency, &result.summary.funding_exact)?;
    if net != reported_net.value
        || money.fees != reported_fees.value
        || money.funding != reported_income.value
        || !approximately(
            result.summary.pnl,
            result.equity.last().unwrap().1 - 10_000.0,
        )
        || !approximately(
            result.summary.fees,
            money.fees.to_f64().ok_or("MONEY_PRECISION")?,
        )
        || !approximately(
            result.summary.funding,
            money.funding.to_f64().ok_or("MONEY_PRECISION")?,
        )
        || !approximately(
            result.summary.turnover,
            money.turnover.to_f64().ok_or("MONEY_PRECISION")?,
        )
    {
        return Err("REPLAY_AUDIT_SUMMARY_MISMATCH".into());
    }
    // The recorded replay funding is signed *income*, while the shared
    // SettlementCashflow takes signed *cost*. Convert exactly once here.
    let funding_cost = Decimal::ZERO
        .checked_sub(money.funding)
        .ok_or("MONEY_OVERFLOW")?;
    let gross = net
        .checked_add(money.fees)
        .and_then(|v| v.checked_add(funding_cost))
        .ok_or("MONEY_OVERFLOW")?;
    let flow = SettlementCashflow::checked(
        QuoteAmount::exact(currency, gross)?,
        QuoteAmount::exact(currency, money.fees)?,
        QuoteAmount::exact(currency, funding_cost)?,
        reported_net,
    )?;
    Ok(ReplayEconomicAudit {
        schema: REPLAY_ECONOMIC_AUDIT_VERSION,
        result_hash: result.hash.clone(),
        dataset_hash: result.dataset_hash.clone(),
        settlement_currency: currency.to_owned(),
        gross_exact: flow.gross().value.normalize().to_string(),
        fees_exact: flow.fees().value.normalize().to_string(),
        funding_cost_exact: flow.funding().value.normalize().to_string(),
        funding_income_exact: money.funding.normalize().to_string(),
        net_exact: flow.net().value.normalize().to_string(),
        trade_entries_checked: result.trades.len(),
        equity_points_checked: result.equity.len(),
        score_start_index: score_start,
        score_end_index: score_end,
        accounting_identity: "gross_minus_fees_minus_signed_funding_cost_equals_net",
        trade_price_basis: "replay_fills_not_independently_attested_venue_fills",
        funding_price_basis: "recorded_closed_bar_price_not_venue_mark_price",
        historical_rules_verified: execution.historical_rules,
        historical_fees_verified: execution.historical_fees,
        historical_publication_time_verified: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy_registry::{Costs, Summary, Trade};

    fn fixture() -> (Vec<Bar>, ReplaySpec, ReplayResult) {
        let bar = |time: u64, close: f64, funding_rate: Option<f64>| Bar {
            time,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1.0,
            funding_rate,
        };
        let bars = vec![
            bar(0, 100.0, None),
            bar(60_000, 100.0, Some(0.01)),
            bar(120_000, 110.0, None),
        ];
        let spec = ReplaySpec {
            strategy_id: "close_momentum".into(),
            parameters: serde_json::from_value(serde_json::json!({"fast":2,"slow":3})).unwrap(),
            mechanics: Mechanics::Linear {
                contract_multiplier: 0.3,
            },
            costs: Costs {
                maker_fee_bps: 0.0,
                taker_fee_bps: 0.0,
                slippage_bps: 0.0,
                observed_funding: false,
            },
            timeframe_ms: 60_000,
            historical_observed: false,
            historical_rules: false,
            historical_fees: false,
            seed: 7,
            stop_fraction: None,
            take_profit_fraction: None,
        };
        let result = ReplayResult {
            dataset_hash: "sha256:dataset".into(),
            code_version: "test".into(),
            hash: "sha256:replay".into(),
            summary: Summary {
                pnl: 2.7,
                pnl_exact: "2.7".into(),
                fees_exact: "0".into(),
                funding_exact: "-0.3".into(),
                max_drawdown: 0.0,
                sharpe: 0.0,
                trades: 2,
                turnover: 63.0,
                fees: 0.0,
                funding: -0.3,
                benchmark: 0.1,
                assumptions: vec![],
            },
            trades: vec![
                Trade {
                    time: 60_000,
                    side: 1,
                    price: 100.0,
                    quantity: 0.3,
                    fee: 0.0,
                    reason: "next_bar_signal".into(),
                },
                Trade {
                    time: 120_000,
                    side: -1,
                    price: 110.0,
                    quantity: 0.3,
                    fee: 0.0,
                    reason: "next_bar_signal".into(),
                },
            ],
            equity: vec![(60_000, 9999.7), (120_000, 10002.7)],
        };
        (bars, spec, result)
    }

    #[test]
    fn t09_reconstructed_ledger_has_exact_signed_funding_cost_and_net() {
        let (bars, spec, result) = fixture();
        let audit = audit_replay(&bars, &spec, &result, "USDT", 1, 3, &|| false).unwrap();
        assert_eq!(audit.gross_exact, "3");
        assert_eq!(audit.fees_exact, "0");
        assert_eq!(audit.funding_cost_exact, "0.3");
        assert_eq!(audit.funding_income_exact, "-0.3");
        assert_eq!(audit.net_exact, "2.7");
        assert_eq!(audit.trade_entries_checked, 2);
        assert_eq!(audit.equity_points_checked, 2);
        assert!(!audit.historical_publication_time_verified);
    }

    #[test]
    fn t09_t13_reconstructed_ledger_rejects_modified_fills_funding_and_equity() {
        let (bars, spec, result) = fixture();
        let mut altered = result.clone();
        altered.trades[0].price = 101.0;
        assert_eq!(
            audit_replay(&bars, &spec, &altered, "USDT", 1, 3, &|| false).unwrap_err(),
            "REPLAY_AUDIT_EQUITY_MISMATCH"
        );
        let mut altered = result.clone();
        altered.summary.funding_exact = "0.3".into();
        assert_eq!(
            audit_replay(&bars, &spec, &altered, "USDT", 1, 3, &|| false).unwrap_err(),
            "REPLAY_AUDIT_SUMMARY_MISMATCH"
        );
        let mut altered = result.clone();
        altered.equity[0].1 += 1.0;
        assert_eq!(
            audit_replay(&bars, &spec, &altered, "USDT", 1, 3, &|| false).unwrap_err(),
            "REPLAY_AUDIT_EQUITY_MISMATCH"
        );
        let mut altered = result.clone();
        altered.trades[0].side = -1;
        assert_eq!(
            audit_replay(&bars, &spec, &altered, "USDT", 1, 3, &|| false).unwrap_err(),
            "REPLAY_AUDIT_EQUITY_MISMATCH"
        );
        let mut altered = result.clone();
        altered.trades[0].quantity = 0.6;
        assert_eq!(
            audit_replay(&bars, &spec, &altered, "USDT", 1, 3, &|| false).unwrap_err(),
            "REPLAY_AUDIT_INVALID_POSITION_TRANSITION"
        );
        assert_eq!(
            audit_replay(&bars, &spec, &result, "usd", 1, 3, &|| false).unwrap_err(),
            "INVALID_CURRENCY"
        );
    }

    #[test]
    fn t09_t13_reconstructed_ledger_cancellation_is_bounded() {
        let (bars, spec, result) = fixture();
        assert_eq!(
            audit_replay(&bars, &spec, &result, "USDT", 1, 3, &|| true).unwrap_err(),
            "CANCELLED"
        );
    }
}
