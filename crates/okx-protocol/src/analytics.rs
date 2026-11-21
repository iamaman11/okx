//! Public compact analytics contract. No transport or financial logic.
pub use okx_kernel::strategy_contract::{BreakoutParameters, EmaParameters, StrategyParameters};
use okx_kernel::strategy_contract::CompositionRequest;
use crate::graph_wire::GraphEnvelope;
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
    /// Optional v1 typed composition; legacy fields retain their scalar meaning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composition: Option<Box<CompositionRequest>>,
    /// Extensible graph envelope: no mathematical operator enumeration in Agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Box<GraphEnvelope>>,
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
    Export,
}
/// A research annotation carries no trading authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchDecisionStatus {
    Accepted,
    Rejected,
    Deferred,
}

/// Explicit counterfactual position units. This request never conveys trading
/// authority or claims historical venue contract rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicProduct {
    Spot,
    LinearSwap,
    InverseSwap,
    Option,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EconomicSide {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EconomicAmount {
    pub currency: String,
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EconomicCase {
    pub instrument: String,
    pub product: EconomicProduct,
    pub side: EconomicSide,
    /// Spot: base asset; swaps: number of contracts. Exact decimal string.
    pub quantity: String,
    /// Price in explicit quote currency per base asset.
    pub entry_price: String,
    pub exit_price: String,
    /// A caller-supplied *assumption*, never a verified historical reference.
    #[serde(default)]
    pub contract_type: Option<String>,
    #[serde(default)]
    pub contract_value: Option<String>,
    #[serde(default)]
    pub contract_value_currency: Option<String>,
    #[serde(default)]
    pub settle_currency: Option<String>,
    pub fees: EconomicAmount,
    pub funding: EconomicAmount,
}

#[cfg(test)]
mod economic_case_contract_tests {
    use super::*;
    use serde_json::{Value, json};

    fn request() -> Value {
        json!({"action":"economics","data_mode":"counterfactual","cases":[{
            "instrument":"BTC-USDT-SWAP","product":"linear_swap","side":"long",
            "quantity":"3","entry_price":"100","exit_price":"110",
            "contract_type":"linear","contract_value":"0.1",
            "contract_value_currency":"BTC","settle_currency":"USDT",
            "fees":{"currency":"USDT","amount":"0"},
            "funding":{"currency":"USDT","amount":"0"}
        }]})
    }

    #[test]
    fn t02_t09_economics_is_strictly_typed_bounded_and_backwards_compatible() {
        let value = request();
        let typed: ResearchRequest = serde_json::from_value(value.clone()).unwrap();
        assert!(typed.valid());
        assert_eq!(serde_json::to_value(typed).unwrap(), value);
        for (pointer, bad) in [
            ("/cases/0/product", json!("token")),
            ("/cases/0/side", json!("buy")),
            ("/cases/0/quantity", json!(3)),
            ("/cases/0/fees/currency", json!(42)),
            ("/data_mode", json!("historical_data_maybe")),
        ] {
            let mut invalid = value.clone();
            *invalid.pointer_mut(pointer).unwrap() = bad;
            assert!(
                serde_json::from_value::<ResearchRequest>(invalid).is_err(),
                "{pointer}"
            );
        }
        let mut invalid = value.clone();
        invalid["cases"][0]["raw_rpc"] = json!("forbidden");
        assert!(serde_json::from_value::<ResearchRequest>(invalid).is_err());
        let mut too_many = value.clone();
        too_many["cases"] = json!(vec![value["cases"][0].clone(); 9]);
        assert!(
            !serde_json::from_value::<ResearchRequest>(too_many)
                .unwrap()
                .valid()
        );
        let mut empty = value.clone();
        empty["cases"] = json!([]);
        assert!(
            !serde_json::from_value::<ResearchRequest>(empty)
                .unwrap()
                .valid()
        );
        let mut long = value;
        long["cases"][0]["entry_price"] = json!("1".repeat(37));
        assert!(
            !serde_json::from_value::<ResearchRequest>(long)
                .unwrap()
                .valid()
        );
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResearchRequest {
    RecordDecision {
        job_id: String,
        result_id: String,
        #[serde(default)]
        expected_decision_id: Option<String>,
        name: String,
        hypothesis: String,
        status: ResearchDecisionStatus,
        reason: String,
    },
    Decision {
        job_id: String,
        #[serde(default)]
        decision_id: Option<String>,
    },
    Capabilities {},
    /// Pure, bounded economic oracle. No market read, job, account or order.
    Economics {
        cases: Vec<EconomicCase>,
        data_mode: ResearchDataMode,
    },
    /// Read-only, opt-in K9 acceptance performed by this same running Agent.
    Benchmark {
        idempotency_key: String,
    },
    Validate {
        instruments: Vec<String>,
        timeframe_ms: u64,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        range_mode: RangeMode,
        strategy_id: String,
        parameters: StrategyParameters,
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
        parameters: StrategyParameters,
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
        variants: Vec<StrategyParameters>,
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
            && self.composition.as_ref().is_none_or(|composition| {
                self.graph.is_none()
                    && matches!(self.action, MarketAction::Screen)
                    && self
                        .fields
                        .len()
                        .saturating_add(composition.result_steps.len())
                        <= 14
            })
            && self.graph.as_ref().is_none_or(|graph| {
                self.composition.is_none()
                    && matches!(self.action, MarketAction::Screen)
                    && graph.structurally_valid()
                    && self.fields.len().saturating_add(graph.result_steps.len()) <= 14
            })
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
            Self::Economics { cases, .. } => {
                !cases.is_empty()
                    && cases.len() <= 8
                    && cases.iter().all(|case| {
                        !case.instrument.is_empty()
                            && case.instrument.len() <= 64
                            && [
                                case.quantity.as_str(),
                                case.entry_price.as_str(),
                                case.exit_price.as_str(),
                                case.fees.amount.as_str(),
                                case.funding.amount.as_str(),
                            ]
                            .iter()
                            .all(|s| !s.is_empty() && s.len() <= 36)
                            && [case.fees.currency.as_str(), case.funding.currency.as_str()]
                                .iter()
                                .all(|s| !s.is_empty() && s.len() <= 16)
                            && [
                                case.contract_type.as_deref(),
                                case.contract_value.as_deref(),
                                case.contract_value_currency.as_deref(),
                                case.settle_currency.as_deref(),
                            ]
                            .iter()
                            .all(|v| v.is_none_or(|s| !s.is_empty() && s.len() <= 36))
                    })
            }
            Self::Benchmark { idempotency_key } => {
                (8..=128).contains(&idempotency_key.len())
                    && idempotency_key
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            }
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
                parameters: *parameters,
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
                        "ema_cross_v1"
                            | "ema_momentum_confirm_v1"
                            | okx_kernel::strategy_contract::BREAKOUT_STRATEGY_ID
                            | "no_trade"
                            | "close_momentum"
                            | "two_bar_momentum"
                    )
                    && parameters.warmup_for(strategy_id).is_ok()
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
                        "ema_cross_v1"
                            | "ema_momentum_confirm_v1"
                            | okx_kernel::strategy_contract::BREAKOUT_STRATEGY_ID
                            | "no_trade"
                            | "close_momentum"
                            | "two_bar_momentum"
                    )
                    && !variants.is_empty()
                    && variants.len() <= 64
                    && variants.iter().all(|p| p.warmup_for(strategy_id).is_ok())
                    && costs.valid()
            }
            Self::RecordDecision {
                job_id,
                result_id,
                expected_decision_id,
                name,
                hypothesis,
                reason,
                ..
            } => {
                valid_job(job_id)
                    && valid_artifact(result_id)
                    && expected_decision_id.as_deref().is_none_or(valid_artifact)
                    && valid_annotation(name, 128)
                    && valid_annotation(hypothesis, 4096)
                    && valid_annotation(reason, 2048)
                    && serde_json::to_vec(&(name, hypothesis, reason))
                        .is_ok_and(|bytes| bytes.len() <= 6144)
            }
            Self::Decision {
                job_id,
                decision_id,
            } => valid_job(job_id) && decision_id.as_deref().is_none_or(valid_artifact),
            Self::Status { job_id } => valid_job(job_id),
            Self::Cancel {
                job_id,
                idempotency_key,
            } => valid_job(job_id) && !idempotency_key.is_empty() && idempotency_key.len() <= 128,
            Self::Result { job_id, limit, .. } => valid_job(job_id) && *limit > 0 && *limit <= 50,
        }
    }
}
fn valid_artifact(id: &str) -> bool {
    id.strip_prefix("sha256:").is_some_and(valid_job)
}
fn valid_annotation(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max
}
fn valid_job(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    #[test]
    fn c13_research_validate_range_mode_is_typed_and_backward_compatible() {
        let mut request = serde_json::json!({
            "action":"validate","instruments":["BTC-USDT-SWAP"],
            "timeframe_ms":3600000,"start_ms":3600001,"end_ms":14400001,
            "strategy_id":"ema_cross_v1","parameters":{"fast":2,"slow":4},
            "costs":{"maker_fee_bps":1,"taker_fee_bps":2,"slippage_bps":2,
                "funding_mode":"zero_assumption"},
            "data_mode":"counterfactual"
        });
        let legacy: ResearchRequest = serde_json::from_value(request.clone()).unwrap();
        assert!(legacy.valid());
        assert!(matches!(
            legacy,
            ResearchRequest::Validate {
                range_mode: RangeMode::Strict,
                ..
            }
        ));
        request["range_mode"] = serde_json::json!("normalize");
        let normalized: ResearchRequest = serde_json::from_value(request.clone()).unwrap();
        assert!(normalized.valid());
        assert!(matches!(
            normalized,
            ResearchRequest::Validate {
                range_mode: RangeMode::Normalize,
                ..
            }
        ));
        request["range_mode"] = serde_json::json!("silent_truncation");
        assert!(serde_json::from_value::<ResearchRequest>(request).is_err());
    }

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
    fn market_sort_defaults_and_optional_filter_bounds_match_public_mcp() {
        let wire = serde_json::json!({
            "action":"screen","instruments":["BTC-USDT-SWAP"],
            "fields":["returns"],"timeframe_ms":60000,
            "start_ms":60000,"end_ms":180000,"limit":10,
            "filters":[{"field":"returns"}],
            "sort":{"field":"returns"}
        });
        let request: MarketRequest = serde_json::from_value(wire).unwrap();
        assert!(request.valid());
        assert_eq!(request.filters[0].min, None);
        assert_eq!(request.filters[0].max, None);
        assert!(!request.sort.unwrap().descending);
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
    fn k9_grid_variants_match_canonical_sixty_four_limit() {
        let parameters = EmaParameters { fast: 2, slow: 4 };
        let variants = (0..64).map(|_| parameters.into()).collect();
        let mut request = ResearchRequest::Grid {
            idempotency_key: "k9-grid".into(),
            instruments: vec!["BTC-USDT".into()],
            timeframe_ms: 60000,
            start_ms: 0,
            end_ms: 60000 * 100,
            strategy_id: "ema_cross_v1".into(),
            variants,
            costs: ResearchCosts {
                maker_fee_bps: 0.0,
                taker_fee_bps: 0.0,
                slippage_bps: 0.0,
                funding_mode: FundingMode::ZeroAssumption,
            },
            data_mode: ResearchDataMode::Counterfactual,
            seed: 448,
        };
        assert!(request.valid());
        if let ResearchRequest::Grid { variants, .. } = &mut request {
            variants.push(parameters.into());
        }
        assert!(!request.valid());
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
            parameters: EmaParameters { fast: 20, slow: 50 }.into(),
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

#[cfg(test)]
mod research_decision_contract_tests {
    use super::*;
    fn request() -> ResearchRequest {
        ResearchRequest::RecordDecision {
            job_id: "a".repeat(64),
            result_id: format!("sha256:{}", "b".repeat(64)),
            expected_decision_id: None,
            name: "Experiment".into(),
            hypothesis: "A testable hypothesis".into(),
            status: ResearchDecisionStatus::Deferred,
            reason: "Await evidence".into(),
        }
    }
    #[test]
    fn decision_payload_is_typed_bounded_and_json_escape_budgeted() {
        let request = request();
        assert!(request.valid());
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            serde_json::from_value::<ResearchRequest>(value.clone()).unwrap(),
            request
        );
        for (key, bad) in [
            ("name", serde_json::json!(" ")),
            ("hypothesis", serde_json::json!("x".repeat(4097))),
            ("reason", serde_json::json!("я".repeat(1025))),
            ("status", serde_json::json!("approved_to_trade")),
            ("result_id", serde_json::json!("b".repeat(64))),
            ("expected_decision_id", serde_json::json!("foreign")),
        ] {
            let mut bad_value = value.clone();
            bad_value[key] = bad;
            assert!(
                !serde_json::from_value::<ResearchRequest>(bad_value).is_ok_and(|r| r.valid()),
                "{key}"
            );
        }
        for text in ["\0".repeat(2000), "\"".repeat(4096), "\\".repeat(4096)] {
            let mut escaped = value.clone();
            escaped["hypothesis"] = serde_json::json!(text);
            assert!(
                !serde_json::from_value::<ResearchRequest>(escaped)
                    .unwrap()
                    .valid()
            );
        }
        let mut maximum = value.clone();
        maximum["hypothesis"] = serde_json::json!("я".repeat(2048));
        maximum["reason"] = serde_json::json!("r".repeat(1900));
        assert!(
            serde_json::from_value::<ResearchRequest>(maximum)
                .unwrap()
                .valid()
        );
        let mut foreign = value;
        foreign["account"] = serde_json::json!("a");
        assert!(serde_json::from_value::<ResearchRequest>(foreign).is_err());
    }
}

#[cfg(test)]
mod composition_contract_tests {
    use super::*;
    #[test]
    fn registered_composition_uses_existing_typed_parameters_without_arbitrary_code() {
        let wire = serde_json::json!({"action":"backtest","idempotency_key":"composition-contract",
            "instruments":["BTC-USDT"],"timeframe_ms":60000,"start_ms":0,"end_ms":1200000,
            "strategy_id":"ema_momentum_confirm_v1","parameters":{"fast":1,"slow":2},
            "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,"funding_mode":"zero_assumption"},"data_mode":"counterfactual","seed":7});
        assert!(
            serde_json::from_value::<ResearchRequest>(wire.clone())
                .unwrap()
                .valid()
        );
        let mut maximum = wire.clone();
        maximum["parameters"] = serde_json::json!({"fast":999,"slow":1000});
        assert!(
            serde_json::from_value::<ResearchRequest>(maximum.clone())
                .unwrap()
                .valid()
        );
        maximum["parameters"]["slow"] = serde_json::json!(1001);
        assert!(
            !serde_json::from_value::<ResearchRequest>(maximum)
                .unwrap()
                .valid()
        );
        let mut unknown = wire.clone();
        unknown["strategy_id"] = serde_json::json!("unregistered_source_code");
        assert!(
            !serde_json::from_value::<ResearchRequest>(unknown)
                .unwrap()
                .valid()
        );
        let mut invalid = wire;
        invalid["parameters"] = serde_json::json!({"fast":2,"slow":2});
        assert!(
            !serde_json::from_value::<ResearchRequest>(invalid)
                .unwrap()
                .valid()
        );
    }
}

#[cfg(test)]
mod t08_breakout_public_wire_tests {
    use super::*;

    fn request(action: &str) -> serde_json::Value {
        let mut value = serde_json::json!({
            "action":action,"instruments":["BTC-USDT"],"timeframe_ms":60000,
            "start_ms":0,"end_ms":12000000,"strategy_id":"close_breakout_v1",
            "costs":{"maker_fee_bps":0,"taker_fee_bps":0,"slippage_bps":0,"funding_mode":"zero_assumption"},
            "data_mode":"counterfactual"
        });
        if action != "validate" {
            value["idempotency_key"] = serde_json::json!("t08-breakout-wire");
        }
        if action == "grid" {
            value["variants"] = serde_json::json!(
                (2..66)
                    .map(|lookback| serde_json::json!({"lookback":lookback}))
                    .collect::<Vec<_>>()
            );
        } else {
            value["parameters"] = serde_json::json!({"lookback":20});
        }
        value
    }

    #[test]
    fn validate_backtest_grid_accept_breakout_and_reject_mismatched_parameters() {
        for action in ["validate", "backtest", "grid"] {
            let wire = request(action);
            let typed = serde_json::from_value::<ResearchRequest>(wire.clone()).unwrap();
            assert!(typed.valid(), "{action}");
            let roundtrip =
                serde_json::from_value::<ResearchRequest>(serde_json::to_value(&typed).unwrap())
                    .unwrap();
            assert_eq!(typed, roundtrip);
            let mut invalid = wire.clone();
            invalid["strategy_id"] = serde_json::json!("ema_cross_v1");
            assert!(
                !serde_json::from_value::<ResearchRequest>(invalid)
                    .unwrap()
                    .valid()
            );
            let mut invalid = wire;
            if action == "grid" {
                invalid["variants"][0] = serde_json::json!({"fast":2,"slow":4});
            } else {
                invalid["parameters"] = serde_json::json!({"fast":2,"slow":4});
            }
            assert!(
                !serde_json::from_value::<ResearchRequest>(invalid)
                    .unwrap()
                    .valid()
            );
        }
    }

    #[test]
    fn breakout_public_grid_allows_64_rejects_65_before_sources() {
        let mut wire = request("grid");
        assert!(
            serde_json::from_value::<ResearchRequest>(wire.clone())
                .unwrap()
                .valid()
        );
        wire["variants"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"lookback":66}));
        assert!(
            !serde_json::from_value::<ResearchRequest>(wire)
                .unwrap()
                .valid()
        );
        for lookback in [1, 1001] {
            let mut invalid = request("validate");
            invalid["parameters"] = serde_json::json!({"lookback":lookback});
            assert!(
                !serde_json::from_value::<ResearchRequest>(invalid)
                    .unwrap()
                    .valid()
            );
        }
    }
}
