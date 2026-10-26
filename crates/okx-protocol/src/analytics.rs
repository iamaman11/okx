//! Public compact analytics contract. No transport or financial logic.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeMode {
    #[default]
    Strict,
    Normalize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketRequest {
    pub action: MarketAction,
    #[serde(default)]
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
    /// Opt-in: sort all eligible venue instruments, never just a name page.
    #[serde(default)]
    pub rank_globally: bool,
    #[serde(default)]
    pub settle_currency: Option<String>,
    #[serde(default)]
    pub after_instrument: Option<String>,
    #[serde(default)]
    pub reference_generation: Option<String>,
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
            && (!self.rank_globally || self.after_instrument.is_none())
            && self.after_instrument.as_ref().is_none_or(|s| {
                !s.is_empty()
                    && s.len() <= 64
                    && s.bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
            })
            && self.reference_generation.as_ref().is_none_or(|s| {
                !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_graphic())
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
        #[serde(default)]
        range_mode: RangeMode,
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
            && self.universe.as_ref().is_none_or(|u| {
                !u.rank_globally
                    || (self.sort.is_some()
                        && !self.fields.iter().any(|f| f == "pearson_correlation"))
            })
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
                ..
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
    fn market_universe_only_wire_decodes_and_rejects_ambiguous_modes() {
        let request = serde_json::json!({
            "action":"screen", "universe":{"instrument_type":"SWAP","limit":50},
            "fields":["return_pct"],"timeframe_ms":60000,
            "start_ms":60000,"end_ms":180000,"limit":50
        });
        let parsed: MarketRequest = serde_json::from_value(request.clone()).unwrap();
        assert!(parsed.instruments.is_empty());
        assert!(parsed.valid());
        let mut both = request.clone();
        both["instruments"] = serde_json::json!(["BTC-USDT-SWAP"]);
        assert!(
            !serde_json::from_value::<MarketRequest>(both)
                .unwrap()
                .valid()
        );
        let mut neither = request.clone();
        neither.as_object_mut().unwrap().remove("universe");
        assert!(
            !serde_json::from_value::<MarketRequest>(neither)
                .unwrap()
                .valid()
        );
        let mut compare = request.clone();
        compare["action"] = serde_json::json!("compare");
        assert!(
            !serde_json::from_value::<MarketRequest>(compare)
                .unwrap()
                .valid()
        );
        let mut wrong_generation = request;
        wrong_generation["universe"]["reference_generation"] = serde_json::json!(" ");
        assert!(
            !serde_json::from_value::<MarketRequest>(wrong_generation)
                .unwrap()
                .valid()
        );
    }

    #[test]
    fn global_rank_is_opt_in_and_rejects_name_cursor_without_score_sort() {
        let mut value = serde_json::json!({
            "action":"screen","fields":["returns"],"timeframe_ms":900000,
            "start_ms":900000,"end_ms":1800000,"limit":10,
            "universe":{"instrument_type":"SWAP","rank_globally":true}
        });
        let r: MarketRequest = serde_json::from_value(value.clone()).unwrap();
        assert!(!r.valid());
        value["sort"] = serde_json::json!({"field":"returns","descending":true});
        assert!(
            serde_json::from_value::<MarketRequest>(value.clone())
                .unwrap()
                .valid()
        );
        value["universe"]["after_instrument"] = serde_json::json!("BTC-USDT-SWAP");
        assert!(
            !serde_json::from_value::<MarketRequest>(value.clone())
                .unwrap()
                .valid()
        );
        value["universe"]
            .as_object_mut()
            .unwrap()
            .remove("after_instrument");
        value["fields"] = serde_json::json!(["pearson_correlation"]);
        value["sort"]["field"] = serde_json::json!("pearson_correlation");
        assert!(
            !serde_json::from_value::<MarketRequest>(value)
                .unwrap()
                .valid()
        );
    }
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
