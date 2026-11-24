//! Typed, finite-only, causal operators for replay and live evaluation.
pub use crate::strategy_contract::{
    BREAKOUT_STRATEGY_ID, BreakoutParameters, EmaParameters, StrategyParameters,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct BreakoutKernel {
    lookback: usize,
    index: usize,
    maxima: VecDeque<(usize, f64)>,
    minima: VecDeque<(usize, f64)>,
}

impl BreakoutKernel {
    pub fn new(params: BreakoutParameters) -> Result<Self, String> {
        Ok(Self {
            lookback: params.validate()?.lookback,
            index: 0,
            maxima: VecDeque::with_capacity(params.lookback),
            minima: VecDeque::with_capacity(params.lookback),
        })
    }

    fn push_closed(&mut self, close: f64) -> Option<i8> {
        // Sliding extrema are amortized O(1), including for a million bars and
        // a thousand-bar window. Only PRIOR closed observations are compared.
        let earliest = self.index.saturating_sub(self.lookback);
        while self
            .maxima
            .front()
            .is_some_and(|(index, _)| *index < earliest)
        {
            self.maxima.pop_front();
        }
        while self
            .minima
            .front()
            .is_some_and(|(index, _)| *index < earliest)
        {
            self.minima.pop_front();
        }
        let result = (self.index >= self.lookback).then(|| {
            let highest = self.maxima.front().expect("warmed maximum").1;
            let lowest = self.minima.front().expect("warmed minimum").1;
            if close > highest {
                1
            } else if close < lowest {
                -1
            } else {
                0
            }
        });
        while self.maxima.back().is_some_and(|(_, value)| *value <= close) {
            self.maxima.pop_back();
        }
        while self.minima.back().is_some_and(|(_, value)| *value >= close) {
            self.minima.pop_back();
        }
        self.maxima.push_back((self.index, close));
        self.minima.push_back((self.index, close));
        self.index += 1;
        result
    }
}
#[derive(Debug, Clone)]
pub struct EmaKernel {
    params: EmaParameters,
    fast: Option<f64>,
    slow: Option<f64>,
    samples: usize,
}
impl EmaKernel {
    pub fn new(params: EmaParameters) -> Result<Self, String> {
        Ok(Self {
            params: params.validate()?,
            fast: None,
            slow: None,
            samples: 0,
        })
    }
    pub fn push_closed(&mut self, close: f64) -> Result<Option<i8>, String> {
        if !close.is_finite() || close <= 0.0 {
            return Err("invalid close".into());
        }
        self.fast = Some(self.fast.map_or(close, |v| {
            v + 2.0 / (self.params.fast + 1) as f64 * (close - v)
        }));
        self.slow = Some(self.slow.map_or(close, |v| {
            v + 2.0 / (self.params.slow + 1) as f64 * (close - v)
        }));
        self.samples += 1;
        Ok((self.samples >= self.params.slow).then(|| {
            if self.fast > self.slow {
                1
            } else if self.fast < self.slow {
                -1
            } else {
                0
            }
        }))
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    Returns,
    LogReturns,
    RealizedVolatility,
    DownsideVolatility,
    MaxDrawdown,
    PearsonCorrelation,
    Ema,
    RollingMean,
    Atr,
    Rsi,
    SpreadBps,
    VolumeChange,
    OiChange,
    FundingSum,
    PositionSize,
    CostEstimate,
}
/// The enumerable public DSL contract. Adding a typed operator requires listing it here.
pub const SUPPORTED_OPERATORS: &[Operator] = &[
    Operator::Returns,
    Operator::LogReturns,
    Operator::RealizedVolatility,
    Operator::DownsideVolatility,
    Operator::MaxDrawdown,
    Operator::PearsonCorrelation,
    Operator::Ema,
    Operator::RollingMean,
    Operator::Atr,
    Operator::Rsi,
    Operator::SpreadBps,
    Operator::VolumeChange,
    Operator::OiChange,
    Operator::FundingSum,
    Operator::PositionSize,
    Operator::CostEstimate,
];

#[derive(Debug, Clone, Copy)]
pub struct OperatorInfo {
    pub operator: Operator,
    pub inputs: &'static [&'static str],
    pub unit: &'static str,
}
pub fn registry(op: Operator) -> OperatorInfo {
    use Operator::*;
    let (inputs, unit): (&[_], _) = match op {
        Returns | LogReturns | MaxDrawdown => (&["close"], "ratio"),
        RealizedVolatility | DownsideVolatility => (&["close"], "annualized_ratio"),
        PearsonCorrelation => (&["close", "peer_close"], "coefficient"),
        Ema | RollingMean => (&["close"], "price"),
        Atr => (&["high", "low", "close"], "price"),
        Rsi => (&["close"], "index"),
        SpreadBps => (&["bid", "ask"], "bps"),
        VolumeChange => (&["volume"], "ratio"),
        OiChange => (&["open_interest"], "ratio"),
        FundingSum => (&["funding"], "ratio"),
        PositionSize => (&["budget", "price", "stop_distance"], "base_quantity"),
        CostEstimate => (&["notional", "fee_bps", "slippage_bps"], "quote_currency"),
    };
    OperatorInfo {
        operator: op,
        inputs,
        unit,
    }
}
pub fn returns(values: &[f64], log: bool) -> Vec<Option<f64>> {
    let mut out = vec![None; values.len()];
    for i in 1..values.len() {
        if values[i].is_finite()
            && values[i - 1].is_finite()
            && values[i] > 0.0
            && values[i - 1] > 0.0
        {
            let r = values[i] / values[i - 1];
            let estimate = if log { r.ln() } else { r - 1.0 };
            if estimate.is_finite() {
                out[i] = Some(estimate);
            }
        }
    }
    out
}
pub fn correlation(left: &[(u64, f64)], right: &[(u64, f64)]) -> Option<f64> {
    let (mut i, mut j) = (0, 0);
    let mut pairs = Vec::new();
    while i < left.len() && j < right.len() {
        match left[i].0.cmp(&right[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                if left[i].1.is_finite() && right[j].1.is_finite() {
                    pairs.push((left[i].1, right[j].1))
                }
                i += 1;
                j += 1;
            }
        }
    }
    correlation_pairs(&pairs)
}
/// Pearson over already timestamp-aligned finite pairs. The minimum sample
/// count and numerical estimator are identical to legacy correlation.
pub fn correlation_pairs(pairs: &[(f64, f64)]) -> Option<f64> {
    if pairs.len() < 30 || pairs.iter().any(|(a, b)| !a.is_finite() || !b.is_finite()) {
        return None;
    }
    let n = pairs.len() as f64;
    let a = pairs.iter().map(|p| p.0).sum::<f64>() / n;
    let b = pairs.iter().map(|p| p.1).sum::<f64>() / n;
    let c = pairs.iter().map(|p| (p.0 - a) * (p.1 - b)).sum::<f64>();
    let d = (pairs.iter().map(|p| (p.0 - a).powi(2)).sum::<f64>()
        * pairs.iter().map(|p| (p.1 - b).powi(2)).sum::<f64>())
    .sqrt();
    (d > 0.0).then_some(c / d)
}
pub fn annualization(timeframe_ms: u64) -> Result<f64, String> {
    if timeframe_ms == 0 {
        return Err("zero timeframe".into());
    }
    Ok(31_536_000_000.0 / timeframe_ms as f64)
}

/// Scalar projection over a closed, timestamp-aligned contiguous series.
pub fn scalar(
    op: Operator,
    close: &[f64],
    high: &[f64],
    low: &[f64],
    aux: &[f64],
    period: usize,
    timeframe_ms: u64,
) -> Result<Option<f64>, String> {
    use Operator::*;
    let auxiliary = matches!(
        op,
        VolumeChange | OiChange | FundingSum | SpreadBps | PositionSize | CostEstimate
    );
    let invalid = if auxiliary {
        aux.iter().any(|v| !v.is_finite())
    } else {
        close.iter().any(|v| !v.is_finite())
            || (op == Atr && high.iter().chain(low).any(|v| !v.is_finite()))
    };
    if invalid {
        return Err("NONFINITE_INPUT".into());
    }
    let n = close.len();
    if !auxiliary && (n == 0 || close.iter().any(|v| *v <= 0.0)) {
        return Ok(None);
    }
    let last = close.last().copied().unwrap_or(0.0);
    let first = close.first().copied().unwrap_or(0.0);
    let result = match op {
        Returns => (first > 0.0).then_some(last / first - 1.0),
        LogReturns => (first > 0.0 && last > 0.0).then(|| (last / first).ln()),
        RealizedVolatility => {
            let r = returns(close, true)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            if r.len() < 2 {
                None
            } else {
                let mean = r.iter().sum::<f64>() / r.len() as f64;
                Some(
                    (r.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (r.len() - 1) as f64
                        * annualization(timeframe_ms)?)
                    .sqrt(),
                )
            }
        }
        DownsideVolatility => {
            if n < 3 || close.iter().any(|value| *value <= 0.0) {
                None
            } else {
                let mut squares = 0.0;
                for pair in close.windows(2) {
                    let r = pair[1] / pair[0] - 1.0;
                    if !r.is_finite() {
                        return Ok(None);
                    }
                    squares += r.min(0.0).powi(2);
                }
                Some((squares / (n - 1) as f64 * annualization(timeframe_ms)?).sqrt())
            }
        }
        MaxDrawdown => {
            let (mut peak, mut dd) = (first, 0f64);
            for v in close {
                peak = peak.max(*v);
                if peak > 0.0 {
                    dd = dd.max((peak - v) / peak)
                }
            }
            Some(dd)
        }
        Ema => {
            if period == 0 {
                return Err("INVALID_PERIOD".into());
            }
            if n < period {
                None
            } else {
                let mut e = first;
                for v in &close[1..] {
                    e += 2.0 / (period + 1) as f64 * (v - e)
                }
                Some(e)
            }
        }
        Atr => {
            if period == 0 || high.len() != n || low.len() != n {
                return Err("ATR_INPUT_LENGTH_OR_PERIOD".into());
            }
            if n <= period {
                None
            } else {
                let tr = (1..n)
                    .map(|i| {
                        (high[i] - low[i])
                            .max((high[i] - close[i - 1]).abs())
                            .max((low[i] - close[i - 1]).abs())
                    })
                    .collect::<Vec<_>>();
                Some(tr[tr.len() - period..].iter().sum::<f64>() / period as f64)
            }
        }
        RollingMean => {
            if period == 0 || period > 1000 {
                return Err("INVALID_PERIOD".into());
            }
            if n < period {
                None
            } else {
                let tail = &close[n - period..];
                Some(tail.iter().sum::<f64>() / period as f64)
            }
        }
        Rsi => {
            if period == 0 {
                return Err("INVALID_PERIOD".into());
            }
            if n <= period {
                None
            } else {
                let (mut gain, mut loss) = (0f64, 0f64);
                for w in close[n - period - 1..].windows(2) {
                    let d = w[1] - w[0];
                    gain += d.max(0.0);
                    loss += (-d).max(0.0)
                }
                Some(if loss == 0.0 {
                    if gain == 0.0 { 50.0 } else { 100.0 }
                } else {
                    100.0 - 100.0 / (1.0 + gain / loss)
                })
            }
        }
        VolumeChange | OiChange => aux
            .first()
            .zip(aux.last())
            .filter(|(a, _)| **a > 0.0)
            .map(|(a, b)| b / a - 1.0),
        FundingSum => {
            if aux.is_empty() {
                None
            } else {
                Some(aux.iter().sum())
            }
        }
        SpreadBps => {
            if aux.len() == 2 && aux[0] > 0.0 && aux[1] >= aux[0] {
                Some((aux[1] - aux[0]) / ((aux[1] + aux[0]) / 2.0) * 10000.0)
            } else {
                None
            }
        }
        PositionSize => {
            if aux.len() == 3 && aux.iter().all(|v| *v > 0.0) {
                Some(aux[0] / (aux[1] * aux[2]))
            } else {
                None
            }
        }
        CostEstimate => {
            if aux.len() == 3 && aux.iter().all(|v| *v >= 0.0) {
                Some(aux[0] * (aux[1] + aux[2]) / 10000.0)
            } else {
                None
            }
        }
        PearsonCorrelation => return Err("CORRELATION_REQUIRES_TIMESTAMP_ALIGNMENT".into()),
    };
    Ok(result.filter(|v| v.is_finite()))
}
/// Existing close-to-close momentum state, shared by registered compositions.
#[derive(Debug, Clone, Default)]
pub struct MomentumKernel {
    previous: Option<f64>,
}
impl MomentumKernel {
    fn push_closed(&mut self, close: f64) -> Option<i8> {
        let direction = self.previous.map(|previous| {
            if close > previous {
                1
            } else if close < previous {
                -1
            } else {
                0
            }
        });
        self.previous = Some(close);
        direction
    }
}
/// One signal kernel for both execution consumers and research. State is per instance.
#[derive(Debug, Clone)]
pub enum StrategyKernel {
    Ema(EmaKernel),
    Breakout(BreakoutKernel),
    NoTrade,
    Momentum(MomentumKernel),
    EmaMomentumConfirm {
        ema: EmaKernel,
        momentum: MomentumKernel,
    },
    TwoBar {
        antecedent: Option<f64>,
        previous: Option<f64>,
    },
}
impl StrategyKernel {
    pub fn from_id(id: &str, p: EmaParameters) -> Result<Self, String> {
        // Preserve the original Engine entry point and its parameter policy.
        // The new Research definition route validates its own parameter type.
        match id {
            "no_trade" => Ok(Self::NoTrade),
            "close_momentum" => Ok(Self::Momentum(MomentumKernel::default())),
            "two_bar_momentum" => Ok(Self::TwoBar {
                antecedent: None,
                previous: None,
            }),
            _ => Self::from_definition(id, p.into()),
        }
    }

    pub fn from_definition(id: &str, parameters: StrategyParameters) -> Result<Self, String> {
        parameters.warmup_for(id)?;
        match (id, parameters) {
            ("ema_cross_v1", StrategyParameters::Ema(p)) => Ok(Self::Ema(EmaKernel::new(p)?)),
            (BREAKOUT_STRATEGY_ID, StrategyParameters::Breakout(p)) => {
                Ok(Self::Breakout(BreakoutKernel::new(p)?))
            }
            ("no_trade", _) => Ok(Self::NoTrade),
            ("close_momentum", _) => Ok(Self::Momentum(MomentumKernel::default())),
            ("ema_momentum_confirm_v1", StrategyParameters::Ema(p)) => {
                Ok(Self::EmaMomentumConfirm {
                    ema: EmaKernel::new(p)?,
                    momentum: MomentumKernel::default(),
                })
            }
            ("two_bar_momentum", _) => Ok(Self::TwoBar {
                antecedent: None,
                previous: None,
            }),
            _ => Err("UNSUPPORTED_STRATEGY".into()),
        }
    }
    pub fn push_closed(&mut self, close: f64) -> Result<Option<i8>, String> {
        if !close.is_finite() || close <= 0.0 {
            return Err("INVALID_CLOSE".into());
        }
        Ok(match self {
            Self::Ema(k) => return k.push_closed(close),
            Self::Breakout(k) => k.push_closed(close),
            Self::NoTrade => Some(0),
            Self::Momentum(kernel) => kernel.push_closed(close),
            Self::EmaMomentumConfirm { ema, momentum } => {
                match (ema.push_closed(close)?, momentum.push_closed(close)) {
                    (Some(trend), Some(direction)) => {
                        Some(if trend == direction { trend } else { 0 })
                    }
                    _ => None,
                }
            }
            Self::TwoBar {
                antecedent,
                previous,
            } => {
                let out = antecedent.zip(*previous).map(|(a, p)| {
                    if close > p && p > a {
                        1
                    } else if close < p && p < a {
                        -1
                    } else {
                        0
                    }
                });
                *antecedent = *previous;
                *previous = Some(close);
                out
            }
        })
    }
}

/// Contiguous numeric storage with one validity bit per sample. Missing/NaN never becomes a usable zero.
#[derive(Debug, Clone)]
pub struct NumericColumn {
    pub values: Vec<f64>,
    validity: Vec<u64>,
}
impl NumericColumn {
    pub fn from_values(values: impl IntoIterator<Item = f64>) -> Self {
        let mut out = Self {
            values: Vec::new(),
            validity: Vec::new(),
        };
        for value in values {
            let index = out.values.len();
            if index.is_multiple_of(64) {
                out.validity.push(0)
            }
            if value.is_finite() {
                out.validity[index / 64] |= 1u64 << (index % 64);
                out.values.push(value)
            } else {
                out.values.push(f64::NAN)
            }
        }
        out
    }
    pub fn get(&self, index: usize) -> Option<f64> {
        self.values
            .get(index)
            .copied()
            .filter(|_| self.validity[index / 64] & (1u64 << (index % 64)) != 0)
    }
    pub fn valid_count(&self) -> usize {
        self.validity.iter().map(|v| v.count_ones() as usize).sum()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn correlation_timestamp_alignment() {
        let a = (0..60).map(|i| (i, i as f64)).collect::<Vec<_>>();
        let b = (30..90).map(|i| (i, 2.0 * i as f64)).collect::<Vec<_>>();
        assert!((correlation(&a, &b).unwrap() - 1.0).abs() < 1e-12);
        assert!(correlation(&a[..29], &b).is_none());
    }
    #[test]
    fn metric_units_and_missing_values() {
        assert_eq!(registry(Operator::SpreadBps).unit, "bps");
        assert_eq!(
            returns(&[1.0, f64::NAN, 2.0, 4.0], false),
            vec![None, None, None, Some(1.0)]
        );
    }
    #[test]
    fn finite_input_overflow_cannot_publish_infinite_returns() {
        let extreme = [1e-300, 1e300, 1e-300];
        assert_eq!(returns(&extreme, false), vec![None, None, Some(-1.0)]);
        assert_eq!(returns(&extreme, true), vec![None, None, None]);
    }
    #[test]
    fn ema_warmup_and_instances() {
        let p = EmaParameters { fast: 2, slow: 3 };
        let mut a = EmaKernel::new(p).unwrap();
        let mut b = a.clone();
        assert_eq!(a.push_closed(1.0).unwrap(), None);
        a.push_closed(2.0).unwrap();
        assert_eq!(a.push_closed(3.0).unwrap(), Some(1));
        assert_eq!(b.push_closed(9.0).unwrap(), None);
    }
}

#[cfg(test)]
mod numeric_fixtures {
    use super::*;
    #[test]
    fn numeric_validity_bitmap_preserves_missing() {
        let c = NumericColumn::from_values([1.0, f64::NAN, f64::INFINITY, 0.0]);
        assert_eq!(c.valid_count(), 2);
        assert_eq!(c.get(1), None);
        assert_eq!(c.get(3), Some(0.0));
        assert_eq!(c.get(4), None);
    }
}
#[cfg(test)]
mod downside_fixtures {
    use super::*;
    #[test]
    fn downside_volatility_closed_returns_and_crypto_annualization() {
        let value = scalar(
            Operator::DownsideVolatility,
            &[100.0, 110.0, 99.0],
            &[],
            &[],
            &[],
            14,
            86400000,
        )
        .unwrap()
        .unwrap();
        assert!((value - (0.005f64 * 365.0).sqrt()).abs() < 1e-12);
        let info = registry(Operator::DownsideVolatility);
        assert_eq!(info.inputs, &["close"]);
        assert_eq!(info.unit, "annualized_ratio");
    }
    #[test]
    fn downside_volatility_finite_gaps_are_not_zero() {
        assert_eq!(
            scalar(
                Operator::DownsideVolatility,
                &[100.0],
                &[],
                &[],
                &[],
                14,
                60000
            )
            .unwrap(),
            None
        );
        assert_eq!(
            scalar(
                Operator::DownsideVolatility,
                &[100.0, 0.0, 90.0],
                &[],
                &[],
                &[],
                14,
                60000
            )
            .unwrap(),
            None
        );
        assert!(
            scalar(
                Operator::DownsideVolatility,
                &[100.0, f64::NAN, 90.0],
                &[],
                &[],
                &[],
                14,
                60000
            )
            .is_err()
        );
        assert_eq!(
            scalar(
                Operator::DownsideVolatility,
                &[100.0, 110.0, 120.0],
                &[],
                &[],
                &[],
                14,
                60000
            )
            .unwrap(),
            Some(0.0)
        );
    }
}

#[cfg(test)]
mod dependency_fixtures {
    use super::*;
    #[test]
    fn unused_numeric_columns_do_not_degrade_downside() {
        let value = scalar(
            Operator::DownsideVolatility,
            &[100.0, 110.0, 120.0],
            &[f64::NAN],
            &[f64::NAN],
            &[],
            14,
            60000,
        )
        .unwrap();
        assert_eq!(value, Some(0.0));
        assert_eq!(
            scalar(
                Operator::FundingSum,
                &[],
                &[],
                &[],
                &[-0.01, 0.02],
                14,
                60000
            )
            .unwrap(),
            Some(0.01)
        );
    }
}

#[cfg(test)]
mod c20_rolling_extension {
    use super::*;

    #[test]
    fn c20_rolling_mean_new_operator_finite_and_causal() {
        let candles = (1..=20).map(f64::from).collect::<Vec<_>>();
        assert_eq!(
            scalar(Operator::RollingMean, &candles, &[], &[], &[], 4, 60_000).unwrap(),
            Some(18.5)
        );
        assert_eq!(
            scalar(
                Operator::RollingMean,
                &candles[..3],
                &[],
                &[],
                &[],
                4,
                60_000
            )
            .unwrap(),
            None
        );
        assert_eq!(
            scalar(Operator::RollingMean, &candles, &[], &[], &[], 0, 60_000).unwrap_err(),
            "INVALID_PERIOD"
        );
        let mut invalid = candles.clone();
        invalid[17] = f64::NAN;
        assert_eq!(
            scalar(Operator::RollingMean, &invalid, &[], &[], &[], 4, 60_000).unwrap_err(),
            "NONFINITE_INPUT"
        );
    }

    #[test]
    fn c20_registered_composition_has_independent_signal_oracle_and_no_lookahead() {
        let parameters = EmaParameters { fast: 1, slow: 2 };
        let closes = [100., 102., 101., 101.05, 100., 101.];
        let signals = |input: &[f64]| {
            let mut kernel =
                StrategyKernel::from_id("ema_momentum_confirm_v1", parameters).unwrap();
            input
                .iter()
                .map(|close| kernel.push_closed(*close).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            signals(&closes),
            vec![None, Some(1), Some(-1), Some(0), Some(-1), Some(1)]
        );
        let mut extended = closes.to_vec();
        extended.extend([1000., 1., 500.]);
        assert_eq!(signals(&extended)[..closes.len()], signals(&closes));
        let mut composition =
            StrategyKernel::from_id("ema_momentum_confirm_v1", parameters).unwrap();
        let mut ema = StrategyKernel::from_id("ema_cross_v1", parameters).unwrap();
        let mut momentum = StrategyKernel::from_id("close_momentum", parameters).unwrap();
        for close in closes {
            let expected = match (
                ema.push_closed(close).unwrap(),
                momentum.push_closed(close).unwrap(),
            ) {
                (Some(trend), Some(direction)) => Some(if trend == direction { trend } else { 0 }),
                _ => None,
            };
            assert_eq!(composition.push_closed(close).unwrap(), expected);
        }
        assert_eq!(
            composition.push_closed(f64::NAN).unwrap_err(),
            "INVALID_CLOSE"
        );
        assert!(
            StrategyKernel::from_id(
                "ema_momentum_confirm_v1",
                EmaParameters { fast: 2, slow: 2 }
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod t08_strategy_definition_tests {
    use super::*;

    #[test]
    fn breakout_wire_is_disjoint_strict_and_has_own_warmup() {
        let breakout = StrategyParameters::Breakout(BreakoutParameters { lookback: 2 });
        assert_eq!(
            serde_json::to_value(breakout).unwrap(),
            serde_json::json!({"lookback":2})
        );
        assert_eq!(breakout.warmup_for(BREAKOUT_STRATEGY_ID).unwrap(), 2);
        let restored: StrategyParameters =
            serde_json::from_value(serde_json::json!({"fast":2,"slow":4})).unwrap();
        assert_eq!(
            restored,
            StrategyParameters::Ema(EmaParameters { fast: 2, slow: 4 })
        );
        for malformed in [
            serde_json::json!({"lookback":1}),
            serde_json::json!({"lookback":1001}),
            serde_json::json!({"lookback":2,"slow":4}),
            serde_json::json!({"lookback":"2"}),
        ] {
            if let Ok(parameters) = serde_json::from_value::<StrategyParameters>(malformed) {
                assert!(parameters.warmup_for(BREAKOUT_STRATEGY_ID).is_err());
            }
        }
        assert_eq!(
            breakout.warmup_for("ema_cross_v1").unwrap_err(),
            "STRATEGY_PARAMETERS_MISMATCH"
        );
        assert_eq!(
            StrategyParameters::Ema(EmaParameters { fast: 2, slow: 4 })
                .warmup_for(BREAKOUT_STRATEGY_ID)
                .unwrap_err(),
            "STRATEGY_PARAMETERS_MISMATCH"
        );
    }

    #[test]
    fn breakout_only_reads_prior_closed_bars_and_is_prefix_invariant() {
        let parameters = StrategyParameters::Breakout(BreakoutParameters { lookback: 2 });
        let signals = |closes: &[f64]| {
            let mut kernel =
                StrategyKernel::from_definition(BREAKOUT_STRATEGY_ID, parameters).unwrap();
            closes
                .iter()
                .map(|close| kernel.push_closed(*close).unwrap())
                .collect::<Vec<_>>()
        };
        let prefix = [100., 101., 102., 102., 98., 97., 103.];
        assert_eq!(
            signals(&prefix),
            vec![None, None, Some(1), Some(0), Some(-1), Some(-1), Some(1)]
        );
        let mut extended = prefix.to_vec();
        extended.extend([1000., 1., 2000.]);
        assert_eq!(&signals(&extended)[..prefix.len()], signals(&prefix));
        let mut invalid =
            StrategyKernel::from_definition(BREAKOUT_STRATEGY_ID, parameters).unwrap();
        assert_eq!(invalid.push_closed(f64::NAN).unwrap_err(), "INVALID_CLOSE");
    }

    #[test]
    fn existing_ema_signal_outputs_are_byte_for_byte_compatible() {
        let params = EmaParameters { fast: 2, slow: 4 };
        let sample = [100., 99., 101., 103., 96., 105., 109.];
        let mut legacy = StrategyKernel::from_id("ema_cross_v1", params).unwrap();
        let mut typed = StrategyKernel::from_definition("ema_cross_v1", params.into()).unwrap();
        for close in sample {
            assert_eq!(legacy.push_closed(close), typed.push_closed(close));
        }
    }

    #[test]
    fn amortized_breakout_extrema_match_independent_window_oracle() {
        let input = (0..1100)
            .map(|i| (i * 37 % 103 + 1) as f64)
            .collect::<Vec<_>>();
        for lookback in [2usize, 3, 32, 1000] {
            let mut actual = StrategyKernel::from_definition(
                BREAKOUT_STRATEGY_ID,
                BreakoutParameters { lookback }.into(),
            )
            .unwrap();
            for (index, close) in input.iter().enumerate() {
                let expected = (index >= lookback).then(|| {
                    let prior = &input[index - lookback..index];
                    let high = prior.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                    let low = prior.iter().copied().fold(f64::INFINITY, f64::min);
                    if *close > high {
                        1
                    } else if *close < low {
                        -1
                    } else {
                        0
                    }
                });
                assert_eq!(
                    actual.push_closed(*close).unwrap(),
                    expected,
                    "window={lookback},bar={index}"
                );
            }
        }
    }
}
