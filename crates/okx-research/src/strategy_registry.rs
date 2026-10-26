//! Causal EMA replay. Signals at close t execute at open t+1.
use okx_analysis::operators::{EmaParameters, StrategyKernel};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub const CAUSAL_REPLAY_VERSION: &str = "okx-causal-replay/448.1";
use std::str::FromStr;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub funding_rate: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Mechanics {
    Spot,
    Linear { contract_multiplier: f64 },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Costs {
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub slippage_bps: f64,
    pub observed_funding: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySpec {
    pub strategy_id: String,
    pub parameters: EmaParameters,
    pub mechanics: Mechanics,
    pub costs: Costs,
    pub timeframe_ms: u64,
    pub historical_observed: bool,
    pub historical_rules: bool,
    pub historical_fees: bool,
    pub seed: u64,
    pub stop_fraction: Option<f64>,
    pub take_profit_fraction: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub time: u64,
    pub side: i8,
    pub price: f64,
    pub quantity: f64,
    pub fee: f64,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub pnl: f64,
    pub pnl_exact: String,
    pub fees_exact: String,
    pub funding_exact: String,
    pub max_drawdown: f64,
    pub sharpe: f64,
    pub trades: usize,
    pub turnover: f64,
    pub fees: f64,
    pub funding: f64,
    pub benchmark: f64,
    pub assumptions: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub dataset_hash: String,
    pub code_version: String,
    pub summary: Summary,
    pub equity: Vec<(u64, f64)>,
    pub trades: Vec<Trade>,
    pub hash: String,
}
pub fn replay(
    bars: &[Bar],
    spec: &ReplaySpec,
    score_start: usize,
    score_end: usize,
    cancel: &impl Fn() -> bool,
) -> Result<ReplayResult, String> {
    replay_with_hash(bars, spec, score_start, score_end, cancel, None)
}
fn replay_with_hash(
    bars: &[Bar],
    spec: &ReplaySpec,
    score_start: usize,
    score_end: usize,
    cancel: &impl Fn() -> bool,
    known_dataset_hash: Option<&str>,
) -> Result<ReplayResult, String> {
    spec.parameters.validate()?;
    if bars.len() > 1_000_000 {
        return Err("LIMIT_BARS:1000000".into());
    }
    // Warm-up belongs to the strategy kernel, not to unrelated EMA
    // parameters that are also carried by a generic replay specification.
    let required_warmup = match spec.strategy_id.as_str() {
        "close_momentum" => 1,
        "two_bar_momentum" => 2,
        "no_trade" => 0,
        _ => spec.parameters.slow,
    };
    if spec.timeframe_ms == 0
        || score_end > bars.len()
        || score_start >= score_end
        || score_start < required_warmup
    {
        return Err("INVALID_SCORE_RANGE_OR_WARMUP".into());
    }
    if spec.historical_observed && (!spec.historical_rules || !spec.historical_fees) {
        return Err("HISTORICAL_RULES_OR_FEES_MISSING".into());
    }
    let multiplier = match spec.mechanics {
        Mechanics::Spot => 1.0,
        Mechanics::Linear {
            contract_multiplier,
        } if contract_multiplier.is_finite() && contract_multiplier > 0.0 => contract_multiplier,
        _ => return Err("UNSUPPORTED_CONTRACT_MECHANICS".into()),
    };
    for c in [
        spec.costs.maker_fee_bps,
        spec.costs.taker_fee_bps,
        spec.costs.slippage_bps,
    ] {
        if !c.is_finite() || c < 0.0 {
            return Err("INVALID_COST".into());
        }
    }
    let (mut kernel, mut desired, mut position, mut cash, mut entry) = (
        StrategyKernel::from_id(&spec.strategy_id, spec.parameters)?,
        0i8,
        0i8,
        10000.0,
        0.0,
    );
    let mut money = Money::new();
    let (mut trades, mut equity) = (Vec::new(), Vec::new());
    let (mut peak, mut maxdd) = (10000.0f64, 0.0f64);
    let mut previous = None;
    for (i, b) in bars.iter().take(score_end).enumerate() {
        if i % 4096 == 0 && cancel() {
            return Err("CANCELLED".into());
        }
        if [b.open, b.high, b.low, b.close]
            .iter()
            .any(|v| !v.is_finite() || *v <= 0.0)
            || b.high < b.low
        {
            return Err("INVALID_BAR".into());
        }
        if previous.is_some_and(|t| b.time != t + spec.timeframe_ms) {
            return Err("MISSING_OR_DUPLICATE_BAR".into());
        }
        previous = Some(b.time);
        if i >= score_start {
            if spec.costs.observed_funding && b.funding_rate.is_none() {
                return Err("HISTORICAL_FUNDING_MISSING".into());
            }
            if desired != position {
                let delta = desired - position;
                let price =
                    b.open * (1.0 + (delta.signum() as f64) * spec.costs.slippage_bps / 10000.0);
                let quantity = delta as f64 * multiplier;
                let fee = quantity.abs() * price * spec.costs.taker_fee_bps / 10000.0;
                money.trade(quantity, price, spec.costs.taker_fee_bps)?;
                cash = money.cash.to_f64().ok_or("MONEY_PRECISION")?;
                trades.push(Trade {
                    time: b.time,
                    side: delta.signum(),
                    price,
                    quantity: quantity.abs(),
                    fee,
                    reason: "next_bar_signal".into(),
                });
                position = desired;
                entry = price;
            }
            if position != 0 {
                let stop = spec
                    .stop_fraction
                    .map(|v| entry * (1.0 - position as f64 * v));
                let take = spec
                    .take_profit_fraction
                    .map(|v| entry * (1.0 + position as f64 * v));
                let stop_hit = stop.filter(|p| {
                    if position > 0 {
                        b.low <= *p
                    } else {
                        b.high >= *p
                    }
                });
                let take_hit = take.filter(|p| {
                    if position > 0 {
                        b.high >= *p
                    } else {
                        b.low <= *p
                    }
                });
                if let Some(trigger) = stop_hit.or(take_hit) {
                    let adverse = if stop_hit.is_some() {
                        if position > 0 {
                            trigger.min(b.open)
                        } else {
                            trigger.max(b.open)
                        }
                    } else {
                        trigger
                    };
                    let price =
                        adverse * (1.0 - position as f64 * spec.costs.slippage_bps / 10000.0);
                    let qty = position as f64 * multiplier;
                    let fee = qty.abs() * price * spec.costs.taker_fee_bps / 10000.0;
                    money.trade(-qty, price, spec.costs.taker_fee_bps)?;
                    cash = money.cash.to_f64().ok_or("MONEY_PRECISION")?;
                    trades.push(Trade {
                        time: b.time,
                        side: -position,
                        price,
                        quantity: qty.abs(),
                        fee,
                        reason: if stop_hit.is_some() {
                            "stop_before_take_profit"
                        } else {
                            "take_profit"
                        }
                        .into(),
                    });
                    position = 0;
                }
            }
            let f = position as f64 * multiplier * b.close * b.funding_rate.unwrap_or(0.0);
            if f != 0.0 {
                money.fund(
                    position as f64 * multiplier,
                    b.close,
                    b.funding_rate.unwrap_or(0.0),
                )?;
                cash = money.cash.to_f64().ok_or("MONEY_PRECISION")?;
            }
            let nav = cash + position as f64 * multiplier * b.close;
            peak = peak.max(nav);
            maxdd = maxdd.max((peak - nav) / peak);
            equity.push((b.time, nav));
        }
        desired = kernel.push_closed(b.close)?.unwrap_or(0);
        if matches!(spec.mechanics, Mechanics::Spot) {
            desired = desired.max(0)
        }
    }
    let (mut mean, mut m2, mut samples) = (0.0, 0.0, 0usize);
    for (i, w) in equity.windows(2).enumerate() {
        if i.is_multiple_of(4096) && cancel() {
            return Err("CANCELLED".into());
        }
        let value = w[1].1 / w[0].1 - 1.0;
        samples += 1;
        let delta = value - mean;
        mean += delta / samples as f64;
        m2 += delta * (value - mean);
    }
    let var = m2 / samples.max(1) as f64;
    let sharpe = if var > 0.0 {
        mean / var.sqrt() * (31_536_000_000.0 / spec.timeframe_ms as f64).sqrt()
    } else {
        0.0
    };
    let summary = Summary {
        pnl: equity.last().unwrap().1 - 10000.0,
        pnl_exact: money
            .cash
            .checked_add(mul(
                dec(position as f64 * multiplier)?,
                dec(bars[score_end - 1].close)?,
            )?)
            .and_then(|v| v.checked_sub(Decimal::from(10000)))
            .ok_or("MONEY_OVERFLOW")?
            .normalize()
            .to_string(),
        fees_exact: money.fees.normalize().to_string(),
        funding_exact: money.funding.normalize().to_string(),
        max_drawdown: maxdd,
        sharpe,
        trades: trades.len(),
        turnover: money.turnover.to_f64().ok_or("MONEY_PRECISION")?,
        fees: money.fees.to_f64().ok_or("MONEY_PRECISION")?,
        funding: money.funding.to_f64().ok_or("MONEY_PRECISION")?,
        benchmark: bars[score_end - 1].close / bars[score_start].open - 1.0,
        assumptions: if spec.historical_observed {
            vec!["historical_observed".into()]
        } else {
            vec!["COUNTERFACTUAL: explicit fees, slippage and contract rules; not historical evidence".into()]
        },
    };
    let dataset_hash = match known_dataset_hash {
        Some(hash) => hash.to_owned(),
        None => hash_chunks(bars, cancel)?,
    };
    let curve_hash = hash_chunks(&equity, cancel)?;
    let trades_hash = hash_chunks(&trades, cancel)?;
    let hash = crate::canonical_sha256(&(
        CAUSAL_REPLAY_VERSION,
        &dataset_hash,
        spec,
        &summary,
        curve_hash,
        trades_hash,
    ))
    .map_err(|e| e.to_string())?;
    Ok(ReplayResult {
        dataset_hash,
        code_version: CAUSAL_REPLAY_VERSION.into(),
        summary,
        equity,
        trades,
        hash,
    })
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridCandidate {
    pub parameters: EmaParameters,
    pub train: Summary,
    /// Stable train-only replay evidence; changes to the holdout cannot
    /// modify this hash or the candidate's selection score.
    pub train_result_hash: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridResult {
    pub variants: Vec<GridCandidate>,
    pub selected: EmaParameters,
    pub split_index: usize,
    pub train_score_start_index: usize,
    pub train_dataset_hash: String,
    pub train: Summary,
    pub test: ReplayResult,
    pub selection_warning: String,
}
pub fn grid(
    bars: &[Bar],
    spec: &ReplaySpec,
    variants: &[EmaParameters],
    cancel: &impl Fn() -> bool,
) -> Result<GridResult, String> {
    if variants.is_empty() || variants.len() > 100 {
        return Err("LIMIT_VARIANTS:1..100".into());
    }
    if bars.len() > 1_000_000 {
        return Err("LIMIT_BARS:1000000".into());
    }
    let split = bars.len() * 7 / 10;
    // All candidate scores use the same reporting window. Different EMA
    // warmups are loaded before it, never counted as reporting returns.
    let train_score_start = variants.iter().map(|p| p.slow).max().ok_or("EMPTY_GRID")?;
    if train_score_start >= split {
        return Err("INSUFFICIENT_TRAIN_BARS".into());
    }
    // Never feed holdout observations into training identities or scores.
    let train_dataset_hash = hash_chunks(&bars[..split], cancel)?;
    let dataset_hash = hash_chunks(bars, cancel)?;
    let mut best: Option<(EmaParameters, Summary)> = None;
    let mut candidates = Vec::with_capacity(variants.len());
    for p in variants {
        if cancel() {
            return Err("CANCELLED".into());
        }
        let mut s = spec.clone();
        s.parameters = *p;
        let r = replay_with_hash(
            bars,
            &s,
            train_score_start,
            split,
            cancel,
            Some(&train_dataset_hash),
        )?;
        candidates.push(GridCandidate {
            parameters: *p,
            train: r.summary.clone(),
            train_result_hash: r.hash.clone(),
        });
        // Stable tie-break independent of the user's variant iteration order.
        if best.as_ref().is_none_or(|(prior, b)| {
            r.summary.sharpe > b.sharpe
                || (r.summary.sharpe == b.sharpe
                    && (r.summary.turnover < b.turnover
                        || (r.summary.turnover == b.turnover
                            && (p.fast, p.slow) < (prior.fast, prior.slow))))
        }) {
            best = Some((*p, r.summary));
        }
    }
    let (p, train) = best.unwrap();
    let mut s = spec.clone();
    s.parameters = p;
    let test = replay_with_hash(bars, &s, split, bars.len(), cancel, Some(&dataset_hash))?;
    Ok(GridResult {
        variants: candidates,
        selected: p,
        split_index: split,
        train_score_start_index: train_score_start,
        train_dataset_hash,
        train,
        test,
        selection_warning:
            "Single chronological 70/30 split does not establish future profitability".into(),
    })
}

fn dec(v: f64) -> Result<Decimal, String> {
    if !v.is_finite() {
        return Err("MONEY_NONFINITE".into());
    }
    Decimal::from_str(&v.to_string()).map_err(|_| "MONEY_PRECISION".into())
}
fn mul(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_mul(b).ok_or("MONEY_OVERFLOW".into())
}
struct Money {
    cash: Decimal,
    fees: Decimal,
    funding: Decimal,
    turnover: Decimal,
}
impl Money {
    fn new() -> Self {
        Self {
            cash: Decimal::from(10000),
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            turnover: Decimal::ZERO,
        }
    }
    fn trade(&mut self, quantity: f64, price: f64, bps: f64) -> Result<(), String> {
        let notional = mul(dec(quantity)?, dec(price)?)?;
        let fee = mul(notional.abs(), dec(bps)?)?
            .checked_div(Decimal::from(10000))
            .ok_or("MONEY_OVERFLOW")?;
        self.cash = self
            .cash
            .checked_sub(notional)
            .and_then(|v| v.checked_sub(fee))
            .ok_or("MONEY_OVERFLOW")?;
        self.fees = self.fees.checked_add(fee).ok_or("MONEY_OVERFLOW")?;
        self.turnover = self
            .turnover
            .checked_add(notional.abs())
            .ok_or("MONEY_OVERFLOW")?;
        Ok(())
    }
    fn fund(&mut self, quantity: f64, price: f64, rate: f64) -> Result<(), String> {
        let funding = mul(mul(dec(quantity)?, dec(price)?)?, dec(rate)?)?;
        self.cash = self.cash.checked_sub(funding).ok_or("MONEY_OVERFLOW")?;
        self.funding = self.funding.checked_sub(funding).ok_or("MONEY_OVERFLOW")?;
        Ok(())
    }
}

fn hash_chunks<T: Serialize>(values: &[T], cancel: &impl Fn() -> bool) -> Result<String, String> {
    let mut hash = Sha256::new();
    hash.update(b"okx.typed.chunks/4096/v1");
    hash.update((values.len() as u64).to_le_bytes());
    for chunk in values.chunks(4096) {
        if cancel() {
            return Err("CANCELLED".into());
        }
        let bytes = serde_json::to_vec(chunk).map_err(|e| e.to_string())?;
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(&bytes);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn bars() -> Vec<Bar> {
        (0..100)
            .map(|i| Bar {
                time: i * 3600000,
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1.0,
                funding_rate: Some(0.0),
            })
            .collect()
    }
    fn spec() -> ReplaySpec {
        ReplaySpec {
            strategy_id: "ema_cross_v1".into(),
            parameters: EmaParameters { fast: 2, slow: 3 },
            mechanics: Mechanics::Linear {
                contract_multiplier: 1.0,
            },
            costs: Costs {
                maker_fee_bps: 0.0,
                taker_fee_bps: 0.0,
                slippage_bps: 0.0,
                observed_funding: true,
            },
            timeframe_ms: 3600000,
            historical_observed: false,
            historical_rules: false,
            historical_fees: false,
            seed: 42,
            stop_fraction: None,
            take_profit_fraction: None,
        }
    }
    #[test]
    fn ema_cross_next_bar_no_lookahead() {
        let b = bars();
        let r = replay(&b, &spec(), 3, 100, &|| false).unwrap();
        assert_eq!(r.trades[0].time, b[3].time);
        assert_eq!(r.trades[0].price, b[3].open);
        assert_eq!(r.trades.len(), 1);
    }
    #[test]
    fn same_dataset_same_result_hash() {
        assert_eq!(
            replay(&bars(), &spec(), 3, 100, &|| false).unwrap().hash,
            replay(&bars(), &spec(), 3, 100, &|| false).unwrap().hash
        );
    }
    #[test]
    fn historical_mode_rejects_missing_rules() {
        let mut s = spec();
        s.historical_observed = true;
        assert_eq!(
            replay(&bars(), &s, 3, 100, &|| false).unwrap_err(),
            "HISTORICAL_RULES_OR_FEES_MISSING"
        );
    }
    #[test]
    fn costs_change_net_return() {
        let s = spec();
        let a = replay(&bars(), &s, 3, 100, &|| false).unwrap();
        let mut s = s;
        s.costs.taker_fee_bps = 20.0;
        let b = replay(&bars(), &s, 3, 100, &|| false).unwrap();
        assert!(b.summary.pnl < a.summary.pnl);
        assert!(b.summary.fees > 0.0);
    }
    #[test]
    fn c14_four_bar_oracle_executes_only_on_next_open() {
        let opens = [100.0, 100.0, 101.0, 102.0];
        let closes = [100.0, 101.0, 102.0, 100.0];
        let bars = (0..4)
            .map(|i| Bar {
                time: i as u64 * 60_000,
                open: opens[i],
                high: opens[i].max(closes[i]) + 1.0,
                low: opens[i].min(closes[i]) - 1.0,
                close: closes[i],
                volume: 1.0,
                funding_rate: None,
            })
            .collect::<Vec<_>>();
        let mut s = spec();
        s.strategy_id = "close_momentum".into();
        s.mechanics = Mechanics::Spot;
        s.timeframe_ms = 60_000;
        s.costs.observed_funding = false;
        let first = replay(&bars, &s, 1, 4, &|| false).unwrap();
        assert_eq!(first.trades.len(), 1);
        assert_eq!(first.trades[0].time, bars[2].time);
        assert_eq!(first.trades[0].price, 101.0);
        assert_eq!(first.trades[0].quantity, 1.0);
        assert_eq!(first.trades[0].side, 1);
        assert_eq!(first.summary.pnl_exact, "-1");
        assert_eq!(first.summary.fees_exact, "0");
        assert_eq!(first.equity.last().unwrap().1, 9999.0);
        // The exit signal at bar 3 cannot execute without a subsequent open.
        let mut future = bars.clone();
        future[3].close = 90.0;
        future[3].low = 89.0;
        let later = replay(&future, &s, 1, 4, &|| false).unwrap();
        assert_eq!(later.trades.len(), 1);
        assert_eq!(later.trades[0].time, first.trades[0].time);
        assert_eq!(later.trades[0].price, first.trades[0].price);
        assert_eq!(later.trades[0].quantity, first.trades[0].quantity);
        future[3] = bars[3].clone();
        future.push(Bar {
            time: 4 * 60_000,
            open: 99.0,
            high: 101.0,
            low: 97.0,
            close: 98.0,
            volume: 1.0,
            funding_rate: None,
        });
        let extended = replay(&future, &s, 1, 5, &|| false).unwrap();
        assert_eq!(extended.trades.len(), 2);
        assert_eq!(extended.trades[1].time, future[4].time);
        assert_eq!(extended.trades[1].price, future[4].open);
        assert_eq!(extended.trades[1].side, -1);
    }

    #[test]
    fn c15_identical_seed_and_inputs_repeat_exact_financial_hashes() {
        let input = bars();
        let parameters = spec();
        let baseline = replay(&input, &parameters, 3, 100, &|| false).unwrap();
        for workers in [1, 2, 4, 8] {
            let tasks = (0..workers)
                .map(|_| {
                    let input = input.clone();
                    let spec = parameters.clone();
                    std::thread::spawn(move || replay(&input, &spec, 3, 100, &|| false).unwrap())
                })
                .collect::<Vec<_>>();
            for task in tasks {
                let result = task.join().unwrap();
                assert_eq!(baseline.hash, result.hash);
                assert_eq!(baseline.dataset_hash, result.dataset_hash);
                assert_eq!(baseline.summary.pnl_exact, result.summary.pnl_exact);
                assert_eq!(baseline.summary.fees_exact, result.summary.fees_exact);
                assert_eq!(baseline.summary.funding_exact, result.summary.funding_exact);
                assert_eq!(baseline.trades.len(), result.trades.len());
                assert_eq!(baseline.equity, result.equity);
            }
        }
    }

    #[test]
    fn c14_train_artifacts_ignore_holdout_and_ties_do_not_depend_on_order() {
        let mut s = spec();
        s.strategy_id = "no_trade".into();
        let variants = [
            EmaParameters { fast: 3, slow: 8 },
            EmaParameters { fast: 2, slow: 3 },
        ];
        let input = bars();
        let first = grid(&input, &s, &variants, &|| false).unwrap();
        let mut changed = input;
        for bar in &mut changed[70..] {
            bar.close += 10.0;
        }
        let changed_result = grid(&changed, &s, &variants, &|| false).unwrap();
        assert_eq!(first.split_index, 70);
        assert_eq!(first.train_score_start_index, 8);
        assert_eq!(first.train_score_start_index, changed_result.train_score_start_index);
        assert_eq!(first.train_dataset_hash, changed_result.train_dataset_hash);
        assert_eq!(first.selected, changed_result.selected);
        assert_eq!(first.variants.len(), changed_result.variants.len());
        for (left, right) in first.variants.iter().zip(&changed_result.variants) {
            assert_eq!(left.train_result_hash, right.train_result_hash);
            assert_eq!(left.train.pnl_exact, right.train.pnl_exact);
        }
        assert_ne!(first.test.dataset_hash, changed_result.test.dataset_hash);
        assert_eq!(first.selected, EmaParameters { fast: 2, slow: 3 });
        let reversed = [variants[1], variants[0]];
        let inverted = grid(&changed, &s, &reversed, &|| false).unwrap();
        assert_eq!(first.selected, inverted.selected);
    }

    #[test]
    fn test_partition_not_used_for_selection() {
        let b = bars();
        let v = [
            EmaParameters { fast: 2, slow: 3 },
            EmaParameters { fast: 3, slow: 8 },
        ];
        let a = grid(&b, &spec(), &v, &|| false).unwrap();
        let mut b = b;
        for row in &mut b[70..] {
            row.close *= 2.0;
        }
        let c = grid(&b, &spec(), &v, &|| false).unwrap();
        assert_eq!(a.selected, c.selected);
    }
    #[test]
    fn partial_missing_bars_rejected() {
        let mut b = bars();
        b.remove(5);
        assert!(
            replay(&b, &spec(), 3, b.len(), &|| false)
                .unwrap_err()
                .contains("MISSING")
        );
    }
}
#[cfg(test)]
mod mechanics_fixtures {
    use super::*;
    fn fixture(multiplier: f64) -> (Vec<Bar>, ReplaySpec) {
        let bars = (0..12)
            .map(|i| Bar {
                time: i * 60000,
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1.0,
                funding_rate: Some(0.001),
            })
            .collect();
        let spec = ReplaySpec {
            strategy_id: "ema_cross_v1".into(),
            parameters: EmaParameters { fast: 1, slow: 2 },
            mechanics: Mechanics::Linear {
                contract_multiplier: multiplier,
            },
            costs: Costs {
                maker_fee_bps: 1.0,
                taker_fee_bps: 5.0,
                slippage_bps: 2.0,
                observed_funding: true,
            },
            timeframe_ms: 60000,
            historical_observed: false,
            historical_rules: false,
            historical_fees: false,
            seed: 398,
            stop_fraction: None,
            take_profit_fraction: None,
        };
        (bars, spec)
    }
    #[test]
    fn funding_fee_signs_and_contract_multiplier() {
        let (b, s) = fixture(1.0);
        let a = replay(&b, &s, 2, b.len(), &|| false).unwrap();
        let mut s = s;
        s.mechanics = Mechanics::Linear {
            contract_multiplier: 2.0,
        };
        let c = replay(&b, &s, 2, b.len(), &|| false).unwrap();
        assert!(a.summary.funding < 0.0);
        assert!(a.summary.fees > 0.0);
        assert!((c.summary.funding - 2.0 * a.summary.funding).abs() < 1e-9);
        assert!((c.summary.fees - 2.0 * a.summary.fees).abs() < 1e-9);
        assert!(a.summary.fees_exact.parse::<Decimal>().unwrap() > Decimal::ZERO);
    }
    #[test]
    fn gap_through_stop_and_stop_before_take_profit() {
        let (mut b, mut s) = fixture(1.0);
        s.stop_fraction = Some(0.01);
        s.take_profit_fraction = Some(0.01);
        b[2].low = 50.0;
        b[2].high = 150.0;
        let a = replay(&b, &s, 2, b.len(), &|| false).unwrap();
        assert_eq!(a.trades[1].reason, "stop_before_take_profit");
        b[3].open = 50.0;
        b[3].low = 40.0;
        let a = replay(&b, &s, 2, b.len(), &|| false).unwrap();
        assert!(
            a.trades
                .iter()
                .filter(|t| t.reason == "stop_before_take_profit")
                .all(|t| t.price <= t.time as f64 / 60000.0 + 103.0)
        );
    }
    #[test]
    fn two_instances_no_shared_mutable_state() {
        let (b, s) = fixture(1.0);
        let a = replay(&b, &s, 2, b.len(), &|| false).unwrap();
        let mut other = s.clone();
        other.parameters = EmaParameters { fast: 3, slow: 5 };
        let _ = replay(&b, &other, 5, b.len(), &|| false).unwrap();
        let c = replay(&b, &s, 2, b.len(), &|| false).unwrap();
        assert_eq!(a.hash, c.hash);
    }
    #[test]
    fn cooperative_cancel_stops_at_chunk() {
        let (b, s) = fixture(1.0);
        assert_eq!(
            replay(&b, &s, 2, b.len(), &|| true).unwrap_err(),
            "CANCELLED"
        );
    }
}
