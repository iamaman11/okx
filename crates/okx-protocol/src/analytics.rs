//! Public compact analytics contract. No transport or financial logic.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketRequest {
    pub action: MarketAction,
    pub instruments: Vec<String>,
    #[serde(default)]
    pub universe: Option<MarketUniverse>,
    pub fields: Vec<String>,
    pub timeframe_ms: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    #[serde(default = "page_limit")]
    pub limit: u16,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub parameters: std::collections::BTreeMap<String, f64>,
    #[serde(default)]
    pub filters: Vec<MarketFilter>,
    #[serde(default)]
    pub sort: Option<MarketSort>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketUniverse {
    pub instrument_type: String,
    #[serde(default)]
    pub settle_currency: Option<String>,
    #[serde(default)]
    pub after_instrument: Option<String>,
    #[serde(default = "universe_page_limit")]
    pub limit: u16,
}
fn universe_page_limit() -> u16 {
    100
}
impl MarketUniverse {
    pub fn valid(&self) -> bool {
        matches!(self.instrument_type.as_str(), "SPOT" | "SWAP" | "FUTURES")
            && (1..=100).contains(&self.limit)
            && self.settle_currency.as_ref().is_none_or(|s| {
                !s.is_empty()
                    && s.len() <= 16
                    && s.bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
            })
            && self.after_instrument.as_ref().is_none_or(|s| {
                !s.is_empty()
                    && s.len() <= 64
                    && s.bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
            })
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketFilter {
    pub field: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSort {
    pub field: String,
    #[serde(default)]
    pub descending: bool,
}
fn page_limit() -> u16 {
    50
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketAction {
    Screen,
    Compare,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaParameters {
    pub fast: usize,
    pub slow: usize,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchCosts {
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub slippage_bps: f64,
    pub funding_mode: FundingMode,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingMode {
    Observed,
    ZeroAssumption,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchDataMode {
    HistoricalObserved,
    Counterfactual,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchResultView {
    #[default]
    Trades,
    Summary,
    Variants,
    Equity,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchRequest {
    Capabilities {},
    Validate {
        instruments: Vec<String>,
        timeframe_ms: u64,
        start_ms: u64,
        end_ms: u64,
        strategy_id: String,
        parameters: EmaParameters,
        costs: ResearchCosts,
        data_mode: ResearchDataMode,
    },
    Backtest {
        idempotency_key: String,
        instruments: Vec<String>,
        timeframe_ms: u64,
        start_ms: u64,
        end_ms: u64,
        strategy_id: String,
        parameters: EmaParameters,
        costs: ResearchCosts,
        data_mode: ResearchDataMode,
        #[serde(default)]
        seed: u64,
    },
    Grid {
        idempotency_key: String,
        instruments: Vec<String>,
        timeframe_ms: u64,
        start_ms: u64,
        end_ms: u64,
        strategy_id: String,
        variants: Vec<EmaParameters>,
        costs: ResearchCosts,
        data_mode: ResearchDataMode,
        #[serde(default)]
        seed: u64,
    },
    Status {
        job_id: String,
    },
    Cancel {
        job_id: String,
        idempotency_key: String,
    },
    Result {
        job_id: String,
        #[serde(default)]
        cursor: Option<String>,
        #[serde(default = "page_limit")]
        limit: u16,
        #[serde(default)]
        view: ResearchResultView,
    },
}

impl MarketRequest {
    pub fn valid(&self) -> bool {
        ((self.universe.is_none() && !self.instruments.is_empty())
            || (matches!(self.action, MarketAction::Screen)
                && self.instruments.is_empty()
                && self.universe.as_ref().is_some_and(MarketUniverse::valid)))
            && self.instruments.len() <= 100
            && self
                .instruments
                .iter()
                .all(|s| !s.is_empty() && s.len() <= 64)
            && self.limit > 0
            && self.limit <= 50
            && market_options(self)
            && valid_range(self.timeframe_ms, self.start_ms, self.end_ms)
            && !self.fields.is_empty()
            && self.fields.len() <= 14
            && self.fields.iter().all(|f| {
                !f.is_empty()
                    && f.len() <= 64
                    && f.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            })
    }
}
fn market_options(r: &MarketRequest) -> bool {
    r.parameters.len() <= 9
        && r.parameters.iter().all(|(key, v)| {
            v.is_finite()
                && match key.as_str() {
                    "ema_period" | "atr_period" | "rsi_period" => {
                        *v >= 1.0 && *v <= 1000.0 && v.fract() == 0.0
                    }
                    "budget" | "price" => *v > 0.0,
                    "stop_distance" => *v > 0.0 && *v < 1.0,
                    "notional" => *v >= 0.0,
                    "fee_bps" | "slippage_bps" => *v >= 0.0 && *v <= 10000.0,
                    _ => false,
                }
        })
        && r.filters.len() <= 14
        && r.filters.iter().all(|f| {
            r.fields.contains(&f.field)
                && f.min.is_none_or(|v| v.is_finite())
                && f.max.is_none_or(|v| v.is_finite())
                && f.min.zip(f.max).is_none_or(|(min, max)| min <= max)
        })
        && r.sort.as_ref().is_none_or(|s| r.fields.contains(&s.field))
}
fn valid_range(ms: u64, start: u64, end: u64) -> bool {
    matches!(
        ms,
        60000 | 180000 | 300000 | 900000 | 1800000 | 3600000 | 14400000 | 86400000
    ) && end > start
        && (end - start) / ms <= 1_000_000
}
impl EmaParameters {
    fn valid(&self) -> bool {
        self.fast >= 1 && self.fast < self.slow && self.slow <= 1000
    }
}
impl ResearchCosts {
    fn valid(&self) -> bool {
        [self.maker_fee_bps, self.taker_fee_bps, self.slippage_bps]
            .iter()
            .all(|n| n.is_finite() && *n >= 0.0 && *n <= 10000.0)
    }
}
impl ResearchRequest {
    pub fn valid(&self) -> bool {
        match self {
            Self::Capabilities {} => true,
            Self::Validate {
                instruments,
                timeframe_ms,
                start_ms,
                end_ms,
                strategy_id,
                parameters,
                costs,
                data_mode,
            } => Self::Backtest {
                idempotency_key: "validation_only".into(),
                instruments: instruments.clone(),
                timeframe_ms: *timeframe_ms,
                start_ms: *start_ms,
                end_ms: *end_ms,
                strategy_id: strategy_id.clone(),
                parameters: parameters.clone(),
                costs: costs.clone(),
                data_mode: *data_mode,
                seed: 0,
            }
            .valid(),
            Self::Backtest {
                idempotency_key,
                instruments,
                timeframe_ms,
                start_ms,
                end_ms,
                strategy_id,
                parameters,
                costs,
                ..
            } => {
                !idempotency_key.is_empty()
                    && idempotency_key.len() <= 128
                    && !instruments.is_empty()
                    && instruments.len() <= 8
                    && instruments.iter().all(|s| !s.is_empty() && s.len() <= 64)
                    && instruments
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == instruments.len()
                    && valid_range(*timeframe_ms, *start_ms, *end_ms)
                    && ((end_ms - start_ms) / timeframe_ms).saturating_mul(instruments.len() as u64)
                        <= 1_000_000
                    && matches!(
                        strategy_id.as_str(),
                        "ema_cross_v1" | "no_trade" | "close_momentum" | "two_bar_momentum"
                    )
                    && parameters.valid()
                    && costs.valid()
            }
            Self::Grid {
                idempotency_key,
                instruments,
                timeframe_ms,
                start_ms,
                end_ms,
                strategy_id,
                variants,
                costs,
                ..
            } => {
                !idempotency_key.is_empty()
                    && idempotency_key.len() <= 128
                    && !instruments.is_empty()
                    && instruments.len() <= 8
                    && instruments.iter().all(|s| !s.is_empty() && s.len() <= 64)
                    && instruments
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == instruments.len()
                    && valid_range(*timeframe_ms, *start_ms, *end_ms)
                    && ((end_ms - start_ms) / timeframe_ms).saturating_mul(instruments.len() as u64)
                        <= 1_000_000
                    && matches!(
                        strategy_id.as_str(),
                        "ema_cross_v1" | "no_trade" | "close_momentum" | "two_bar_momentum"
                    )
                    && !variants.is_empty()
                    && variants.len() <= 100
                    && variants.iter().all(EmaParameters::valid)
                    && costs.valid()
            }
            Self::Status { job_id } => valid_job(job_id),
            Self::Cancel {
                job_id,
                idempotency_key,
            } => valid_job(job_id) && !idempotency_key.is_empty() && idempotency_key.len() <= 128,
            Self::Result { job_id, limit, .. } => valid_job(job_id) && *limit > 0 && *limit <= 50,
        }
    }
}
fn valid_job(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    #[test]
    fn research_capabilities_strict_read_only_wire() {
        let r: ResearchRequest = serde_json::from_str(r#"{"action":"capabilities"}"#).unwrap();
        assert!(matches!(r, ResearchRequest::Capabilities {}));
        assert!(r.valid());
        assert!(
            serde_json::from_str::<ResearchRequest>(
                r#"{"action":"capabilities","raw_rpc":"forbidden"}"#
            )
            .is_err()
        );
    }
    #[test]
    fn research_batch_wire_roundtrip_and_total_bar_budget() {
        let request = ResearchRequest::Backtest {
            idempotency_key: "batch".into(),
            instruments: vec!["BTC-USDT".into(), "ETH-USDT".into()],
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 30000000000,
            strategy_id: "ema_cross_v1".into(),
            parameters: EmaParameters { fast: 20, slow: 50 },
            costs: ResearchCosts {
                maker_fee_bps: 0.0,
                taker_fee_bps: 0.0,
                slippage_bps: 0.0,
                funding_mode: FundingMode::ZeroAssumption,
            },
            data_mode: ResearchDataMode::Counterfactual,
            seed: 1,
        };
        assert!(request.valid());
        assert_eq!(
            serde_json::from_value::<ResearchRequest>(serde_json::to_value(&request).unwrap())
                .unwrap(),
            request
        );
        let mut invalid = request.clone();
        if let ResearchRequest::Backtest { instruments, .. } = &mut invalid {
            instruments.push("SOL-USDT".into())
        }
        assert!(!invalid.valid());
        let mut invalid = request.clone();
        if let ResearchRequest::Backtest { instruments, .. } = &mut invalid {
            instruments[1] = instruments[0].clone()
        }
        assert!(!invalid.valid());
        let mut invalid = request;
        if let ResearchRequest::Backtest { instruments, .. } = &mut invalid {
            *instruments = (0..9).map(|i| format!("ASSET{i}-USDT")).collect()
        }
        assert!(!invalid.valid());
    }
}
