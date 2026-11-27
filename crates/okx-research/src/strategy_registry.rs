//! Causal replay for registered and composed research signals.
//! Signals at close t execute at open t+1 through one money ledger.
use crate::graph_signal::{GraphSignalSpec, compiled_graph};
#[cfg(test)]
use okx_analysis::operators::EmaParameters;
use okx_analysis::operators::{StrategyKernel, StrategyParameters};
use okx_analysis::query_plan::ClosedBar;
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
    pub parameters: StrategyParameters,
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
/// Execution assumptions shared by named and graph strategies. Graphs never
/// define a second money model or gain financial/trading authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayExecution {
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
impl From<&ReplaySpec> for ReplayExecution {
    fn from(spec: &ReplaySpec) -> Self {
        Self {
            mechanics: spec.mechanics.clone(),
            costs: spec.costs.clone(),
            timeframe_ms: spec.timeframe_ms,
            historical_observed: spec.historical_observed,
            historical_rules: spec.historical_rules,
            historical_fees: spec.historical_fees,
            seed: spec.seed,
            stop_fraction: spec.stop_fraction,
            take_profit_fraction: spec.take_profit_fraction,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphReplaySpec {
    pub graph: GraphSignalSpec,
    pub execution: ReplayExecution,
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
    let required_warmup = spec.parameters.warmup_for(&spec.strategy_id)?;
    let mut kernel = StrategyKernel::from_definition(&spec.strategy_id, spec.parameters)?;
    replay_core(
        bars,
        &ReplayExecution::from(spec),
        spec,
        score_start,
        score_end,
        required_warmup,
        cancel,
        known_dataset_hash,
        |bar| Ok(Some(kernel.push_closed(bar.close)?.unwrap_or(0))),
    )
}

/// Replay one typed causal market-composition hypothesis through the existing
/// next-open and exact-money engine, with no dynamically loaded code.
/// Only graphs supported by the incremental close-only evaluator are admitted;
/// OI/funding and non-incremental operators fail closed at compile time.
pub fn replay_graph(
    bars: &[Bar],
    spec: &GraphReplaySpec,
    score_start: usize,
    score_end: usize,
    cancel: &impl Fn() -> bool,
) -> Result<ReplayResult, String> {
    let (checked, mut stream) = compiled_graph(&spec.graph, bars.len() as u64)?;
    replay_core(
        bars,
        &spec.execution,
        spec,
        score_start,
        score_end,
        checked.warmup_bars(),
        cancel,
        None,
        |bar| {
            let values = stream.on_closed_bar(ClosedBar {
                timestamp_ms: i64::try_from(bar.time)
                    .map_err(|_| "graph_signal:TIMESTAMP_OVERFLOW")?,
                close: Some(bar.close),
            })?;
            checked.target(&values)
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn replay_core(
    bars: &[Bar],
    execution: &ReplayExecution,
    identity: &impl Serialize,
    score_start: usize,
    score_end: usize,
    required_warmup: usize,
    cancel: &impl Fn() -> bool,
    known_dataset_hash: Option<&str>,
    mut target_on_close: impl FnMut(&Bar) -> Result<Option<i8>, String>,
) -> Result<ReplayResult, String> {
    if bars.len() > 1_000_000 {
        return Err("LIMIT_BARS:1000000".into());
    }
    // Warm-up is a property of the selected, strictly typed definition.
    if execution.timeframe_ms == 0
        || score_end > bars.len()
        || score_start >= score_end
        || score_start < required_warmup
    {
        return Err("INVALID_SCORE_RANGE_OR_WARMUP".into());
    }
    if execution.historical_observed && (!execution.historical_rules || !execution.historical_fees)
    {
        return Err("HISTORICAL_RULES_OR_FEES_MISSING".into());
    }
    let multiplier = match execution.mechanics {
        Mechanics::Spot => 1.0,
        Mechanics::Linear {
            contract_multiplier,
        } if contract_multiplier.is_finite() && contract_multiplier > 0.0 => contract_multiplier,
        _ => return Err("UNSUPPORTED_CONTRACT_MECHANICS".into()),
    };
    for c in [
        execution.costs.maker_fee_bps,
        execution.costs.taker_fee_bps,
        execution.costs.slippage_bps,
    ] {
        if !c.is_finite() || c < 0.0 {
            return Err("INVALID_COST".into());
        }
    }
    let (mut desired, mut position, mut cash, mut entry) = (0i8, 0i8, 10000.0, 0.0);
    let mut money = Money::new();
    let (mut trades, mut equity) = (Vec::new(), Vec::new());
    let (mut peak, mut maxdd) = (10000.0f64, 0.0f64);
    let mut previous = None;
    for (i, b) in bars.iter().take(score_end).enumerate() {
        if i.is_multiple_of(4096) && cancel() {
            return Err("CANCELLED".into());
        }
        if [b.open, b.high, b.low, b.close]
            .iter()
            .any(|v| !v.is_finite() || *v <= 0.0)
            || b.high < b.low
        {
            return Err("INVALID_BAR".into());
        }
        if previous.is_some_and(|t| b.time != t + execution.timeframe_ms) {
            return Err("MISSING_OR_DUPLICATE_BAR".into());
        }
        previous = Some(b.time);
        if i >= score_start {
            if execution.costs.observed_funding && b.funding_rate.is_none() {
                return Err("HISTORICAL_FUNDING_MISSING".into());
            }
            if desired != position {
                let delta = desired - position;
                let price = b.open
                    * (1.0 + (delta.signum() as f64) * execution.costs.slippage_bps / 10000.0);
                let quantity = delta as f64 * multiplier;
                let fee = quantity.abs() * price * execution.costs.taker_fee_bps / 10000.0;
                money.trade(quantity, price, execution.costs.taker_fee_bps)?;
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
                let stop = execution
                    .stop_fraction
                    .map(|v| entry * (1.0 - position as f64 * v));
                let take = execution
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
                        adverse * (1.0 - position as f64 * execution.costs.slippage_bps / 10000.0);
                    let qty = position as f64 * multiplier;
                    let fee = qty.abs() * price * execution.costs.taker_fee_bps / 10000.0;
                    money.trade(-qty, price, execution.costs.taker_fee_bps)?;
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
        if let Some(target) = target_on_close(b)? {
            desired = target;
            if matches!(execution.mechanics, Mechanics::Spot) {
                desired = desired.max(0)
            }
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
        mean / var.sqrt() * (31_536_000_000.0 / execution.timeframe_ms as f64).sqrt()
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
        assumptions: if execution.historical_observed {
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
        identity,
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
    pub parameters: StrategyParameters,
    pub train: Summary,
    /// Stable train-only replay evidence; changes to the holdout cannot
    /// modify this hash or the candidate's selection score.
    pub train_result_hash: String,
    /// Report-only holdout evidence; never used by selection or row ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test: Option<CandidateEvaluation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_fees_x2: Option<CandidateEvaluation>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateEvaluation {
    pub summary: Summary,
    /// Fractional return on the replay's initial 10,000, minus the existing
    /// Summary::benchmark buy-and-hold return (not a USD amount).
    pub baseline_delta_return: f64,
    pub baseline_delta_unit: String,
    pub no_trades_reason: Option<String>,
    pub quality: String,
}
impl CandidateEvaluation {
    fn new(summary: Summary, bars: usize) -> Self {
        let no_trades = summary.trades == 0;
        Self {
            baseline_delta_return: summary.pnl / 10000.0 - summary.benchmark,
            baseline_delta_unit: "fraction_of_initial_capital".into(),
            summary,
            no_trades_reason: no_trades.then(|| "NO_EXECUTED_TRADES".into()),
            quality: if no_trades || bars < 2 {
                "partial"
            } else {
                "observed"
            }
            .into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridPolicy {
    pub split_index: usize,
    pub train_score_start_index: usize,
    pub selection_metric: String,
    pub tie_break: Vec<String>,
    pub baseline: String,
    pub sensitivity: String,
}
/// Shared preparation contract, persisted by the adapter before computation.
pub fn grid_policy(
    bar_count: usize,
    strategy_id: &str,
    variants: &[StrategyParameters],
) -> Result<GridPolicy, String> {
    // Internal stress/benchmark callers retain the accepted 100-candidate
    // performance fixture; the public ResearchRequest admission caps at 64.
    if variants.is_empty() || variants.len() > 100 {
        return Err("LIMIT_VARIANTS:1..100".into());
    }
    if bar_count > 1_000_000 {
        return Err("LIMIT_BARS:1000000".into());
    }
    let warmups = variants
        .iter()
        .map(|p| {
            p.warmup_for(strategy_id)?;
            // Legacy EMA-shaped grids score from the largest `slow` even when
            // the selected internal kernel needs less warmup (e.g. no_trade).
            // Preserve that accepted train/OOS boundary and all old hashes.
            Ok::<usize, String>(match p {
                StrategyParameters::Ema(ema) => ema.slow,
                StrategyParameters::Breakout(breakout) => breakout.lookback,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let split_index = bar_count * 7 / 10;
    let train_score_start_index = warmups.into_iter().max().ok_or("EMPTY_GRID")?;
    if train_score_start_index >= split_index {
        return Err("INSUFFICIENT_TRAIN_BARS".into());
    }
    Ok(GridPolicy {
        split_index,
        train_score_start_index,
        selection_metric: "train_sharpe_descending".into(),
        tie_break: if strategy_id == okx_analysis::operators::BREAKOUT_STRATEGY_ID {
            vec![
                "train_turnover_ascending".into(),
                "lookback_ascending".into(),
            ]
        } else {
            vec![
                "train_turnover_ascending".into(),
                "fast_ascending".into(),
                "slow_ascending".into(),
            ]
        },
        baseline: "buy_and_hold_return:summary.benchmark;initial_capital=10000".into(),
        sensitivity: "maker_and_taker_fees_x2;slippage_and_funding_unchanged;report_only".into(),
    })
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridResult {
    pub variants: Vec<GridCandidate>,
    pub selected: StrategyParameters,
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
    variants: &[StrategyParameters],
    cancel: &impl Fn() -> bool,
) -> Result<GridResult, String> {
    grid_with_observer(bars, spec, variants, cancel, &|_| {})
}

/// Successful replay units only: N train, N holdout, N fee sensitivity,
/// then the selected replay. A candidate completes after its sensitivity run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GridProgress {
    pub completed_work_units: u64,
    pub completed_candidates: u64,
}

/// Observation does not participate in identities, selection, or result data.
pub fn grid_with_observer(
    bars: &[Bar],
    spec: &ReplaySpec,
    variants: &[StrategyParameters],
    cancel: &impl Fn() -> bool,
    observe: &impl Fn(GridProgress),
) -> Result<GridResult, String> {
    let mut progress = GridProgress {
        completed_work_units: 0,
        completed_candidates: 0,
    };
    let policy = grid_policy(bars.len(), &spec.strategy_id, variants)?;
    let split = policy.split_index;
    // All candidate scores use the same reporting window. Different EMA
    // warmups are loaded before it, never counted as reporting returns.
    let train_score_start = policy.train_score_start_index;
    // Never feed holdout observations into training identities or scores.
    let train_dataset_hash = hash_chunks(&bars[..split], cancel)?;
    let dataset_hash = hash_chunks(bars, cancel)?;
    let mut best: Option<(StrategyParameters, Summary)> = None;
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
        progress.completed_work_units += 1;
        observe(progress);
        candidates.push(GridCandidate {
            parameters: *p,
            train: r.summary.clone(),
            train_result_hash: r.hash.clone(),
            test: None,
            test_fees_x2: None,
        });
        // Stable tie-break independent of the user's variant iteration order.
        if best.as_ref().is_none_or(|(prior, b)| {
            r.summary.sharpe > b.sharpe
                || (r.summary.sharpe == b.sharpe
                    && (r.summary.turnover < b.turnover
                        || (r.summary.turnover == b.turnover && p < prior)))
        }) {
            best = Some((*p, r.summary));
        }
    }
    let (p, train) = best.unwrap();
    // Freeze the winner before any holdout replay. Reporting preserves input
    // candidate order and cannot affect train-only selection or tie-breaking.
    for candidate in &mut candidates {
        if cancel() {
            return Err("CANCELLED".into());
        }
        let mut report_spec = spec.clone();
        report_spec.parameters = candidate.parameters;
        // Retain only the summary for non-selected variants. Drop their full
        // equity/trade vectors before starting sensitivity replay.
        let report_summary = replay_with_hash(
            bars,
            &report_spec,
            split,
            bars.len(),
            cancel,
            Some(&dataset_hash),
        )?
        .summary;
        progress.completed_work_units += 1;
        observe(progress);
        candidate.test = Some(CandidateEvaluation::new(report_summary, bars.len() - split));
        report_spec.costs.maker_fee_bps *= 2.0;
        report_spec.costs.taker_fee_bps *= 2.0;
        let sensitivity_summary = replay_with_hash(
            bars,
            &report_spec,
            split,
            bars.len(),
            cancel,
            Some(&dataset_hash),
        )?
        .summary;
        candidate.test_fees_x2 = Some(CandidateEvaluation::new(
            sensitivity_summary,
            bars.len() - split,
        ));
        progress.completed_work_units += 1;
        progress.completed_candidates += 1;
        observe(progress);
    }
    let mut s = spec.clone();
    s.parameters = p;
    let test = replay_with_hash(bars, &s, split, bars.len(), cancel, Some(&dataset_hash))?;
    progress.completed_work_units += 1;
    observe(progress);
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

pub(crate) fn dec(v: f64) -> Result<Decimal, String> {
    if !v.is_finite() {
        return Err("MONEY_NONFINITE".into());
    }
    Decimal::from_str(&v.to_string()).map_err(|_| "MONEY_PRECISION".into())
}
pub(crate) fn mul(a: Decimal, b: Decimal) -> Result<Decimal, String> {
    a.checked_mul(b).ok_or("MONEY_OVERFLOW".into())
}
pub(crate) struct Money {
    pub(crate) cash: Decimal,
    pub(crate) fees: Decimal,
    pub(crate) funding: Decimal,
    pub(crate) turnover: Decimal,
}
impl Money {
    pub(crate) fn new() -> Self {
        Self {
            cash: Decimal::from(10000),
            fees: Decimal::ZERO,
            funding: Decimal::ZERO,
            turnover: Decimal::ZERO,
        }
    }
    pub(crate) fn trade(&mut self, quantity: f64, price: f64, bps: f64) -> Result<(), String> {
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
    pub(crate) fn fund(&mut self, quantity: f64, price: f64, rate: f64) -> Result<(), String> {
        let funding = mul(mul(dec(quantity)?, dec(price)?)?, dec(rate)?)?;
        self.cash = self.cash.checked_sub(funding).ok_or("MONEY_OVERFLOW")?;
        self.funding = self.funding.checked_sub(funding).ok_or("MONEY_OVERFLOW")?;
        Ok(())
    }
}

/// The original replay dataset digest, incrementally verifiable from immutable
/// 4096-row pages. The writer and export verifier share exactly one hashing
/// contract; verification never has to allocate the complete million-bar set.
pub struct ChunkedDataHash {
    hash: Sha256,
    expected: usize,
    observed: usize,
}

impl ChunkedDataHash {
    pub fn new(expected: usize) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"okx.typed.chunks/4096/v1");
        hash.update((expected as u64).to_le_bytes());
        Self {
            hash,
            expected,
            observed: 0,
        }
    }

    pub fn update<T: Serialize>(&mut self, values: &[T]) -> Result<(), String> {
        if values.is_empty()
            || values.len() > 4096
            || (self.observed > 0 && !self.observed.is_multiple_of(4096))
            || self
                .observed
                .checked_add(values.len())
                .is_none_or(|n| n > self.expected)
        {
            return Err("DATASET_CHUNK_SHAPE_INVALID".into());
        }
        let bytes = serde_json::to_vec(values).map_err(|e| e.to_string())?;
        self.hash.update((bytes.len() as u64).to_le_bytes());
        self.hash.update(&bytes);
        self.observed += values.len();
        Ok(())
    }

    pub fn finish(self) -> Result<String, String> {
        if self.observed != self.expected {
            return Err("DATASET_ROW_COUNT_MISMATCH".into());
        }
        Ok(format!("sha256:{:x}", self.hash.finalize()))
    }
}

fn hash_chunks<T: Serialize>(values: &[T], cancel: &impl Fn() -> bool) -> Result<String, String> {
    let mut digest = ChunkedDataHash::new(values.len());
    for chunk in values.chunks(4096) {
        if cancel() {
            return Err("CANCELLED".into());
        }
        digest.update(chunk)?;
    }
    digest.finish()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::economic_audit::{audit_graph_replay, audit_replay};
    use crate::graph_signal::{GRAPH_SIGNAL_VERSION, MissingSignalPolicy};
    use okx_analysis::query_plan::COMPOSITION_VERSION;
    #[test]
    fn t13_streaming_dataset_hash_matches_original_replay_digest_at_page_boundaries() {
        for n in [0, 1, 4095, 4096, 4097, 8192] {
            let values = (0..n).map(|i| i as f64 / 10.0).collect::<Vec<_>>();
            let original = hash_chunks(&values, &|| false).unwrap();
            let mut streaming = ChunkedDataHash::new(n);
            for page in values.chunks(4096) {
                streaming.update(page).unwrap();
            }
            assert_eq!(streaming.finish().unwrap(), original);
        }
        let mut missing = ChunkedDataHash::new(2);
        missing.update(&[1u64]).unwrap();
        assert_eq!(missing.finish().unwrap_err(), "DATASET_ROW_COUNT_MISMATCH");
        let mut oversized = ChunkedDataHash::new(4097);
        assert_eq!(
            oversized.update(&vec![0u64; 4097]).unwrap_err(),
            "DATASET_CHUNK_SHAPE_INVALID"
        );
        let mut early_partial = ChunkedDataHash::new(3);
        early_partial.update(&[1u64]).unwrap();
        assert_eq!(
            early_partial.update(&[2u64, 3]).unwrap_err(),
            "DATASET_CHUNK_SHAPE_INVALID"
        );
    }
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
    pub(super) fn spec() -> ReplaySpec {
        ReplaySpec {
            strategy_id: "ema_cross_v1".into(),
            parameters: EmaParameters { fast: 2, slow: 3 }.into(),
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

    fn graph_spec() -> GraphReplaySpec {
        GraphReplaySpec {
            graph: GraphSignalSpec {
                version: GRAPH_SIGNAL_VERSION.into(),
                graph: serde_json::from_value(serde_json::json!({
                    "version": COMPOSITION_VERSION,
                    "steps": [
                        {"id":"r", "op":"returns_series", "input":"close"},
                        {"id":"positive", "op":"greater_than", "input":"r", "threshold":0.0}
                    ],
                    "result_steps": ["positive"]
                }))
                .unwrap(),
                signal_step: "positive".into(),
                target_if_true: 1,
                target_if_false: 0,
                missing: MissingSignalPolicy::Hold,
            },
            execution: ReplayExecution::from(&spec()),
        }
    }

    #[test]
    fn graph_replay_reuses_exact_money_and_next_open_of_registered_strategy() {
        let mut series = bars();
        series[4].funding_rate = Some(0.001);
        let mut named = spec();
        named.strategy_id = "close_momentum".into();
        named.costs.taker_fee_bps = 10.0;
        named.costs.slippage_bps = 5.0;
        let original = replay(&series, &named, 3, series.len(), &|| false).unwrap();
        let mut graph = graph_spec();
        graph.execution = ReplayExecution::from(&named);
        let composed = replay_graph(&series, &graph, 3, series.len(), &|| false).unwrap();
        assert_eq!(composed.equity, original.equity);
        assert_eq!(
            serde_json::to_value(&composed.trades).unwrap(),
            serde_json::to_value(&original.trades).unwrap()
        );
        assert_eq!(composed.summary.pnl_exact, original.summary.pnl_exact);
        assert_eq!(composed.summary.fees_exact, original.summary.fees_exact);
        assert_eq!(
            composed.summary.funding_exact,
            original.summary.funding_exact
        );
        assert_ne!(composed.summary.funding_exact, "0");
        assert_ne!(composed.hash, original.hash);
        assert_eq!(composed.trades[0].time, series[3].time);
        let checked_graph = audit_graph_replay(
            &series,
            &graph.execution,
            &composed,
            "USDT",
            3,
            series.len(),
            &|| false,
        )
        .unwrap();
        let checked_named =
            audit_replay(&series, &named, &original, "USDT", 3, series.len(), &|| {
                false
            })
            .unwrap();
        assert_eq!(checked_graph.net_exact, checked_named.net_exact);
        assert_eq!(checked_graph.fees_exact, checked_named.fees_exact);
    }

    #[test]
    fn graph_replay_missing_input_does_not_create_trade_and_hashes_are_stable() {
        let series = bars();
        let mut graph = graph_spec();
        graph.graph.target_if_false = -1;
        assert_eq!(
            replay_graph(&series, &graph, 1, series.len(), &|| false).unwrap_err(),
            "INVALID_SCORE_RANGE_OR_WARMUP"
        );
        let first = replay_graph(&series, &graph, 2, series.len(), &|| false).unwrap();
        let repeated = replay_graph(&series, &graph, 2, series.len(), &|| false).unwrap();
        assert_eq!(first.hash, repeated.hash);
        // The first close has no prior return. HOLD may not short at open 1.
        assert_eq!(first.trades[0].time, series[2].time);
        graph.graph.missing = MissingSignalPolicy::Flat;
        assert_ne!(
            first.hash,
            replay_graph(&series, &graph, 2, series.len(), &|| false)
                .unwrap()
                .hash
        );
    }

    #[test]
    fn graph_replay_rejects_unavailable_sources_and_non_incremental_ops() {
        let series = bars();
        let mut graph = graph_spec();
        graph.graph.graph.steps[0].input = "volume".into();
        assert_eq!(
            replay_graph(&series, &graph, 3, series.len(), &|| false).unwrap_err(),
            "composition.stream:SOURCE_NOT_SUPPORTED:volume"
        );
        graph.graph.graph.steps[0].input = "close".into();
        graph.graph.graph.steps[0].op = okx_analysis::query_plan::SeriesOp::RollingStddev;
        graph.graph.graph.steps[0].period = Some(2);
        assert_eq!(
            replay_graph(&series, &graph, 3, series.len(), &|| false).unwrap_err(),
            "composition.stream:OPERATOR_NOT_INCREMENTAL"
        );
        graph = graph_spec();
        graph.graph.graph.steps.push(
            serde_json::from_value(serde_json::json!({
                "id":"future", "op":"forward_return_label", "input":"close",
                "when":"positive", "period":2
            }))
            .unwrap(),
        );
        assert_eq!(
            replay_graph(&series, &graph, 3, series.len(), &|| false).unwrap_err(),
            "graph_signal:FORWARD_LABEL_FORBIDDEN"
        );
    }
    #[test]
    fn legacy_grid_candidates_remain_readable_without_holdout_reports() {
        let result = replay(&bars(), &spec(), 3, 100, &|| false).unwrap();
        let legacy = serde_json::json!({
            "parameters": { "fast": 2, "slow": 3 },
            "train": result.summary,
            "train_result_hash": result.hash
        });
        let restored: GridCandidate = serde_json::from_value(legacy.clone()).unwrap();
        assert!(restored.test.is_none());
        assert!(restored.test_fees_x2.is_none());
        assert_eq!(serde_json::to_value(restored).unwrap(), legacy);
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
    fn c20_registered_composition_replays_literal_next_open_money_oracle() {
        let opens: [f64; 6] = [100., 100., 103., 102., 101., 100.];
        let closes: [f64; 6] = [100., 102., 101., 101.05, 100., 101.];
        let bars = (0..6)
            .map(|index| Bar {
                time: index as u64 * 60000,
                open: opens[index],
                close: closes[index],
                high: opens[index].max(closes[index]) + 1.,
                low: opens[index].min(closes[index]) - 1.,
                volume: 1.,
                funding_rate: None,
            })
            .collect::<Vec<_>>();
        let mut parameters = spec();
        parameters.strategy_id = "ema_momentum_confirm_v1".into();
        parameters.parameters = EmaParameters { fast: 1, slow: 2 }.into();
        parameters.timeframe_ms = 60000;
        parameters.mechanics = Mechanics::Linear {
            contract_multiplier: 1.,
        };
        parameters.costs = Costs {
            maker_fee_bps: 0.,
            taker_fee_bps: 0.,
            slippage_bps: 0.,
            observed_funding: false,
        };
        let result = replay(&bars, &parameters, 2, 6, &|| false).unwrap();
        assert_eq!(
            result
                .trades
                .iter()
                .map(|trade| (trade.time / 60000, trade.side, trade.quantity, trade.price))
                .collect::<Vec<_>>(),
            vec![
                (2, 1, 1., 103.),
                (3, -1, 2., 102.),
                (4, 1, 1., 101.),
                (5, -1, 1., 100.)
            ]
        );
        assert_eq!(result.summary.pnl_exact, "-1");
        assert_eq!(result.summary.fees_exact, "0");
        assert_eq!(result.summary.funding_exact, "0");
        assert_eq!(result.equity.last().unwrap().1, 9999.);
        // Last positive signal has no next open; a future bar executes it,
        // preserving all earlier trades and scored equity.
        let mut future = bars.clone();
        future.push(Bar {
            time: 360000,
            open: 105.,
            high: 110.,
            low: 100.,
            close: 109.,
            volume: 1.,
            funding_rate: None,
        });
        let extended = replay(&future, &parameters, 2, 7, &|| false).unwrap();
        assert_eq!(
            serde_json::to_value(&extended.trades[..4]).unwrap(),
            serde_json::to_value(&result.trades).unwrap()
        );
        assert_eq!(extended.trades[4].time, 360000);
        assert_eq!(extended.trades[4].side, 1);
        assert_eq!(extended.trades[4].quantity, 2.);
        assert_eq!(extended.trades[4].price, 105.);
        let rescored = replay(&future, &parameters, 2, 6, &|| false).unwrap();
        assert_eq!(rescored.equity, result.equity);
        assert_eq!(rescored.summary.pnl_exact, result.summary.pnl_exact);
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
            EmaParameters { fast: 3, slow: 8 }.into(),
            EmaParameters { fast: 2, slow: 3 }.into(),
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
        assert_eq!(
            first.train_score_start_index,
            changed_result.train_score_start_index
        );
        assert_eq!(first.train_dataset_hash, changed_result.train_dataset_hash);
        assert_eq!(first.selected, changed_result.selected);
        assert_eq!(first.variants.len(), changed_result.variants.len());
        for (left, right) in first.variants.iter().zip(&changed_result.variants) {
            assert_eq!(left.train_result_hash, right.train_result_hash);
            assert_eq!(left.train.pnl_exact, right.train.pnl_exact);
            assert_ne!(
                left.test.as_ref().unwrap().baseline_delta_return,
                right.test.as_ref().unwrap().baseline_delta_return
            );
            assert_eq!(
                left.test.as_ref().unwrap().no_trades_reason.as_deref(),
                Some("NO_EXECUTED_TRADES")
            );
        }
        assert_ne!(first.test.dataset_hash, changed_result.test.dataset_hash);
        assert_eq!(first.selected, EmaParameters { fast: 2, slow: 3 }.into());
        let reversed = [variants[1], variants[0]];
        let inverted = grid(&changed, &s, &reversed, &|| false).unwrap();
        assert_eq!(first.selected, inverted.selected);
        assert_eq!(inverted.variants[0].parameters, reversed[0]);
    }

    #[test]
    fn grid_oos_and_fee_sensitivity_match_independent_one_trade_oracle() {
        let input = (0..10)
            .map(|i| Bar {
                time: i * 60_000,
                open: 100.0 + i as f64,
                close: 100.0 + i as f64,
                high: 101.0 + i as f64,
                low: 99.0 + i as f64,
                volume: 1.0,
                funding_rate: None,
            })
            .collect::<Vec<_>>();
        let mut s = spec();
        s.strategy_id = "close_momentum".into();
        s.mechanics = Mechanics::Spot;
        s.timeframe_ms = 60_000;
        s.costs = Costs {
            maker_fee_bps: 0.0,
            taker_fee_bps: 10.0,
            slippage_bps: 0.0,
            observed_funding: false,
        };
        let variants = [
            EmaParameters { fast: 1, slow: 2 }.into(),
            EmaParameters { fast: 2, slow: 3 }.into(),
        ];
        let result = grid(&input, &s, &variants, &|| false).unwrap();
        for candidate in result.variants {
            let report = candidate.test.unwrap();
            let sensitivity = candidate.test_fees_x2.unwrap();
            // Split is7. A close6 signal buys one unit at open7=107,
            // marked at close9=109. Fees107*10/10000=.107; doubled=.214.
            assert_eq!(report.summary.trades, 1);
            assert_eq!(report.summary.pnl_exact, "1.893");
            assert_eq!(report.summary.fees_exact, "0.107");
            assert_eq!(sensitivity.summary.pnl_exact, "1.786");
            assert_eq!(sensitivity.summary.fees_exact, "0.214");
            assert_eq!(report.no_trades_reason, None);
            assert_eq!(report.quality, "observed");
            assert_eq!(report.baseline_delta_unit, "fraction_of_initial_capital");
            assert!(
                (report.baseline_delta_return - (1.893 / 10000.0 - (109.0 / 107.0 - 1.0))).abs()
                    < 1e-12
            );
        }
    }

    #[test]
    fn test_partition_not_used_for_selection() {
        let b = bars();
        let v = [
            EmaParameters { fast: 2, slow: 3 }.into(),
            EmaParameters { fast: 3, slow: 8 }.into(),
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
            parameters: EmaParameters { fast: 1, slow: 2 }.into(),
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
    fn t09_three_linear_contracts_exact_quantity_and_usd_pnl() {
        // Independent decimal oracle: three linear contracts, each worth
        // 0.1 BTC, give a 0.3 BTC position. USD account and USD/BTC prices
        // are explicit assumptions; no inverse pricing or FX is implied.
        let contracts = Decimal::from(3);
        let btc_per_contract = Decimal::new(1, 1);
        let quantity_btc = contracts * btc_per_contract;
        assert_eq!(quantity_btc, Decimal::new(3, 1));
        let expected_gross_usd = quantity_btc * (Decimal::from(110) - Decimal::from(100));
        assert_eq!(expected_gross_usd, Decimal::from(3));

        let mut ledger = Money::new();
        ledger.trade(0.3, 100.0, 0.0).unwrap();
        ledger.trade(-0.3, 110.0, 0.0).unwrap();
        assert_eq!(ledger.cash - Decimal::from(10000), expected_gross_usd);
        assert_eq!(ledger.fees, Decimal::ZERO);
        assert_eq!(ledger.funding, Decimal::ZERO);
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
        other.parameters = EmaParameters { fast: 3, slow: 5 }.into();
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

#[cfg(test)]
mod t08_breakout_oos_tests {
    use super::*;
    use okx_analysis::operators::{BREAKOUT_STRATEGY_ID, BreakoutParameters};

    #[test]
    fn breakout_replay_executes_after_signal_and_keeps_future_out_of_decision() {
        let input = (0..30)
            .map(|i| Bar {
                time: i * 60_000,
                open: 100.0 + i as f64,
                high: 102.0 + i as f64,
                low: 99.0 + i as f64,
                close: 101.0 + i as f64,
                volume: 1.0,
                funding_rate: None,
            })
            .collect::<Vec<_>>();
        let mut spec = super::tests::spec();
        spec.strategy_id = BREAKOUT_STRATEGY_ID.into();
        spec.parameters = BreakoutParameters { lookback: 2 }.into();
        spec.mechanics = Mechanics::Spot;
        spec.timeframe_ms = 60_000;
        spec.costs.observed_funding = false;
        let result = replay(&input, &spec, 2, input.len(), &|| false).unwrap();
        assert_eq!(result.trades[0].time, input[3].time);
        assert_eq!(result.trades[0].price, input[3].open);

        // The same shared Rust kernel used by the live decision path produces
        // the signal that replay executes on the NEXT bar, not this close.
        let mut live =
            StrategyKernel::from_definition(BREAKOUT_STRATEGY_ID, spec.parameters).unwrap();
        let decisions = input
            .iter()
            .map(|bar| live.push_closed(bar.close).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(decisions[..3], [None, None, Some(1)]);
    }

    #[test]
    fn breakout_grid_sixty_four_train_only_candidates_and_oos_isolation() {
        let input = (0..200)
            .map(|i| Bar {
                time: i * 60_000,
                open: 100.0 + i as f64 * 0.2,
                high: 101.0 + i as f64 * 0.2,
                low: 99.0 + i as f64 * 0.2,
                close: 100.5 + i as f64 * 0.2,
                volume: 1.0,
                funding_rate: None,
            })
            .collect::<Vec<_>>();
        let mut spec = super::tests::spec();
        spec.strategy_id = BREAKOUT_STRATEGY_ID.into();
        spec.parameters = BreakoutParameters { lookback: 2 }.into();
        spec.mechanics = Mechanics::Spot;
        spec.timeframe_ms = 60_000;
        spec.costs.observed_funding = false;
        let variants = (2..66)
            .map(|lookback| BreakoutParameters { lookback }.into())
            .collect::<Vec<_>>();
        let before = grid(&input, &spec, &variants, &|| false).unwrap();
        assert_eq!(before.variants.len(), 64);
        assert_eq!(before.split_index, 140);
        assert_eq!(before.train_score_start_index, 65);
        assert_eq!(
            grid_policy(input.len(), BREAKOUT_STRATEGY_ID, &variants)
                .unwrap()
                .tie_break
                .last()
                .unwrap(),
            "lookback_ascending"
        );

        let mut changed = input;
        for bar in &mut changed[140..] {
            bar.open += 30.0;
            bar.high += 30.0;
            bar.low += 30.0;
            bar.close += 30.0;
        }
        let after = grid(&changed, &spec, &variants, &|| false).unwrap();
        assert_eq!(before.selected, after.selected);
        assert_eq!(before.train_dataset_hash, after.train_dataset_hash);
        assert_eq!(before.train.pnl_exact, after.train.pnl_exact);
        for (left, right) in before.variants.iter().zip(after.variants.iter()) {
            assert_eq!(left.train_result_hash, right.train_result_hash);
        }
        assert_ne!(before.test.dataset_hash, after.test.dataset_hash);
    }
}
