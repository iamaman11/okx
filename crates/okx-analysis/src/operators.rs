//! Typed, finite-only, causal operators shared by live and replay.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EmaParameters {
    pub fast: usize,
    pub slow: usize,
}
impl EmaParameters {
    pub fn validate(self) -> Result<Self, String> {
        if self.fast < 1 || self.fast >= self.slow || self.slow > 1000 {
            Err("EMA requires 1 <= fast < slow <= 1000".into())
        } else {
            Ok(self)
        }
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
    Atr,
    Rsi,
    SpreadBps,
    VolumeChange,
    OiChange,
    FundingSum,
    PositionSize,
    CostEstimate,
}
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
        Ema => (&["close"], "price"),
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
            out[i] = Some(if log { r.ln() } else { r - 1.0 })
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
    if pairs.len() < 30 {
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
/// One signal kernel for both execution consumers and research. State is per instance.
#[derive(Debug, Clone)]
pub enum StrategyKernel {
    Ema(EmaKernel),
    NoTrade,
    Momentum {
        previous: Option<f64>,
    },
    TwoBar {
        antecedent: Option<f64>,
        previous: Option<f64>,
    },
}
impl StrategyKernel {
    pub fn from_id(id: &str, p: EmaParameters) -> Result<Self, String> {
        match id {
            "ema_cross_v1" => Ok(Self::Ema(EmaKernel::new(p)?)),
            "no_trade" => Ok(Self::NoTrade),
            "close_momentum" => Ok(Self::Momentum { previous: None }),
            "two_bar_momentum" => Ok(Self::TwoBar {
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
            Self::NoTrade => Some(0),
            Self::Momentum { previous } => {
                let out = previous.map(|p| {
                    if close > p {
                        1
                    } else if close < p {
                        -1
                    } else {
                        0
                    }
                });
                *previous = Some(close);
                out
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
