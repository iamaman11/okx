//! Typed K7 analysis plan, independent of storage, transport and venue.
use crate::{
    operators::{Operator, registry},
    time_series::ClosedRange,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const QUERY_PLAN_VERSION: &str = "okx.query-plan/448.1";
pub const COMPOSITION_VERSION: &str = "okx.market.composition/v1";

/// A bounded sequence of typed series operations, evaluated by the existing
/// market worker. No general-purpose expressions, I/O or financial effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionRequest {
    pub version: String,
    pub steps: Vec<SeriesStep>,
    pub result_steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesStep {
    pub id: String,
    pub op: SeriesOp,
    /// "close" is the only source in v1; every other name is a prior step ID.
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesOp {
    ReturnsSeries,
    RollingMean,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckedSeriesStep {
    pub step: SeriesStep,
    pub unit: Unit,
    pub depth: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckedComposition {
    pub version: String,
    pub steps: Vec<CheckedSeriesStep>,
    pub result_steps: Vec<String>,
    pub estimated_cells: u64,
}

impl CheckedComposition {
    pub fn compile(request: CompositionRequest, total_bars: u64) -> Result<Self, String> {
        if request.version != COMPOSITION_VERSION {
            return Err("composition.version:UNSUPPORTED".into());
        }
        if request.steps.is_empty() || request.steps.len() > 32 {
            return Err("composition.steps:COUNT_LIMIT".into());
        }
        if request.result_steps.is_empty() || request.result_steps.len() > 14 {
            return Err("composition.result_steps:COUNT_LIMIT".into());
        }
        let cells = total_bars
            .checked_mul(request.steps.len() as u64)
            .ok_or("composition:CELLS_OVERFLOW")?;
        if cells > 8_000_000 {
            return Err("composition:CELLS_LIMIT".into());
        }
        let mut known = BTreeMap::<String, (Unit, u8)>::new();
        let mut steps = Vec::with_capacity(request.steps.len());
        for (idx, step) in request.steps.into_iter().enumerate() {
            let path = format!("composition.steps[{idx}]");
            if step.id.is_empty()
                || step.id.len() > 32
                || !step
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(format!("{path}.id:INVALID"));
            }
            if step.id == "close" || known.contains_key(&step.id) {
                return Err(format!("{path}.id:DUPLICATE"));
            }
            let (input_unit, parent_depth) = if step.input == "close" {
                (Unit::Price, 0)
            } else {
                known
                    .get(&step.input)
                    .copied()
                    .ok_or_else(|| format!("{path}.input:UNKNOWN_OR_FORWARD_STEP:{}", step.input))?
            };
            let depth = parent_depth + 1;
            if depth > 8 {
                return Err(format!("{path}:DEPTH_LIMIT"));
            }
            let output_unit = match step.op {
                SeriesOp::ReturnsSeries if step.period.is_none() && input_unit == Unit::Price => {
                    Unit::Ratio
                }
                SeriesOp::ReturnsSeries => {
                    return Err(format!("{path}:EXPECTED_PRICE_SERIES_NO_PERIOD"));
                }
                SeriesOp::RollingMean if matches!(step.period, Some(1..=1000)) => input_unit,
                SeriesOp::RollingMean => return Err(format!("{path}.period:INVALID")),
            };
            known.insert(step.id.clone(), (output_unit, depth));
            steps.push(CheckedSeriesStep {
                step,
                unit: output_unit,
                depth,
            });
        }
        let mut unique_outputs = BTreeSet::new();
        for (idx, id) in request.result_steps.iter().enumerate() {
            if !known.contains_key(id) || !unique_outputs.insert(id) {
                return Err(format!("composition.result_steps[{idx}]:INVALID_REFERENCE"));
            }
        }
        Ok(Self {
            version: request.version,
            steps,
            result_steps: request.result_steps,
            estimated_cells: cells,
        })
    }

    /// Returns only bounded scalar projections; intermediate series stay inside
    /// the existing worker and can be reused by multiple downstream steps.
    pub fn evaluate(&self, close: &[f64]) -> BTreeMap<String, (Option<f64>, Unit)> {
        let mut values = BTreeMap::<String, Vec<Option<f64>>>::new();
        for checked in &self.steps {
            let input = if checked.step.input == "close" {
                close.iter().map(|v| v.is_finite().then_some(*v)).collect()
            } else {
                values[&checked.step.input].clone()
            };
            let output = match checked.step.op {
                SeriesOp::ReturnsSeries => {
                    let mut result = vec![None; input.len()];
                    for i in 1..input.len() {
                        if let (Some(a), Some(b)) = (input[i - 1], input[i]) {
                            if a > 0.0 && b > 0.0 {
                                let value = b / a - 1.0;
                                result[i] = value.is_finite().then_some(value);
                            }
                        }
                    }
                    result
                }
                SeriesOp::RollingMean => {
                    let period = checked.step.period.unwrap_or(1) as usize;
                    let mut result = vec![None; input.len()];
                    for i in (period - 1)..input.len() {
                        let window = &input[i + 1 - period..=i];
                        if window.iter().all(Option::is_some) {
                            let mean = window.iter().flatten().sum::<f64>() / period as f64;
                            result[i] = mean.is_finite().then_some(mean);
                        }
                    }
                    result
                }
            };
            values.insert(checked.step.id.clone(), output);
        }
        self.result_steps
            .iter()
            .map(|id| {
                let checked = self
                    .steps
                    .iter()
                    .find(|s| &s.step.id == id)
                    .expect("checked projection");
                (
                    id.clone(),
                    (values[id].last().copied().flatten(), checked.unit),
                )
            })
            .collect()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Candles,
    Ticker,
    Funding,
    OpenInterest,
    Request,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    Price,
    Ratio,
    AnnualizedRatio,
    Coefficient,
    Index,
    Bps,
    Quantity,
    QuoteCurrency,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Input {
    pub name: String,
    pub source: Source,
    pub unit: Unit,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorDescriptor {
    pub id: Operator,
    pub version: u16,
    pub inputs: Vec<Input>,
    pub output_unit: Unit,
    pub warmup_bars: u64,
    pub null_policy: String,
    pub deterministic: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub operator: Operator,
    pub min: Option<f64>,
    pub max: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rank {
    pub operator: Operator,
    pub descending: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryPlan {
    pub version: String,
    pub universe: Vec<String>,
    pub dataset_refs: Vec<String>,
    pub range: ClosedRange,
    pub timeframe_ms: u64,
    pub warmup_bars: u64,
    pub sources: Vec<Source>,
    pub operators: Vec<OperatorDescriptor>,
    pub filters: Vec<Filter>,
    pub ranking: Option<Rank>,
    pub result_projection: Vec<Operator>,
    pub parameters: BTreeMap<String, f64>,
    pub assumptions: Vec<String>,
    pub resource_budget_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composition: Option<CheckedComposition>,
}
fn input(name: &str) -> Result<Input, String> {
    let (source, unit) = match name {
        "close" | "high" | "low" | "peer_close" => (Source::Candles, Unit::Price),
        "volume" => (Source::Candles, Unit::Quantity),
        "bid" | "ask" => (Source::Ticker, Unit::Price),
        "funding" => (Source::Funding, Unit::Ratio),
        "open_interest" => (Source::OpenInterest, Unit::Quantity),
        "budget" | "notional" => (Source::Request, Unit::QuoteCurrency),
        "price" => (Source::Request, Unit::Price),
        "stop_distance" => (Source::Request, Unit::Ratio),
        "fee_bps" | "slippage_bps" => (Source::Request, Unit::Bps),
        _ => return Err(format!("operators.inputs:UNKNOWN:{name}")),
    };
    Ok(Input {
        name: name.into(),
        source,
        unit,
    })
}
fn output_unit(op: Operator) -> Result<Unit, String> {
    Ok(match registry(op).unit {
        "price" => Unit::Price,
        "ratio" => Unit::Ratio,
        "annualized_ratio" => Unit::AnnualizedRatio,
        "coefficient" => Unit::Coefficient,
        "index" => Unit::Index,
        "bps" => Unit::Bps,
        "base_quantity" => Unit::Quantity,
        "quote_currency" => Unit::QuoteCurrency,
        other => return Err(format!("operators.output_unit:UNKNOWN:{other}")),
    })
}
impl QueryPlan {
    pub fn compile(
        universe: Vec<String>,
        range: ClosedRange,
        operators: Vec<Operator>,
        parameters: BTreeMap<String, f64>,
        filters: Vec<Filter>,
        ranking: Option<Rank>,
    ) -> Result<Self, String> {
        if operators.is_empty() || operators.len() > 14 {
            return Err("operators:COUNT_LIMIT".into());
        }
        if range.expected_bars > 1_000_000 {
            return Err("range:LIMIT_BARS".into());
        }
        if universe.len() > 100 || universe.iter().any(String::is_empty) {
            return Err("universe:INVALID".into());
        }
        let selected = operators.iter().copied().collect::<BTreeSet<_>>();
        if selected.len() != operators.len() {
            return Err("operators:DUPLICATE".into());
        }
        for (name, value) in &parameters {
            let valid = value.is_finite()
                && match name.as_str() {
                    "ema_period" | "atr_period" | "rsi_period" => {
                        *value >= 1.0 && *value <= 1000.0 && value.fract() == 0.0
                    }
                    "budget" | "price" => *value > 0.0,
                    "stop_distance" => *value > 0.0 && *value < 1.0,
                    "notional" => *value >= 0.0,
                    "fee_bps" | "slippage_bps" => *value >= 0.0 && *value <= 10000.0,
                    _ => false,
                };
            if !valid {
                return Err(format!("parameters.{name}:INVALID"));
            }
        }
        for (idx, filter) in filters.iter().enumerate() {
            if !selected.contains(&filter.operator) {
                return Err(format!("filters[{idx}].operator:NOT_PROJECTED"));
            }
            if filter.min.is_some_and(|v| !v.is_finite())
                || filter.max.is_some_and(|v| !v.is_finite())
                || filter.min.zip(filter.max).is_some_and(|(a, b)| a > b)
            {
                return Err(format!("filters[{idx}]:INVALID_BOUNDS"));
            }
        }
        if ranking
            .as_ref()
            .is_some_and(|r| !selected.contains(&r.operator))
        {
            return Err("ranking.operator:NOT_PROJECTED".into());
        }
        let mut sources = BTreeSet::new();
        let mut bindings = Vec::new();
        let mut warmup_bars = 0;
        for op in &operators {
            let inputs = registry(*op)
                .inputs
                .iter()
                .map(|s| input(s))
                .collect::<Result<Vec<_>, _>>()?;
            sources.extend(inputs.iter().map(|i| i.source));
            let warmup = match op {
                Operator::Ema => parameters.get("ema_period").copied().unwrap_or(14.0) as u64,
                Operator::RollingMean => 14,
                Operator::Atr => parameters.get("atr_period").copied().unwrap_or(14.0) as u64,
                Operator::Rsi => parameters.get("rsi_period").copied().unwrap_or(14.0) as u64,
                Operator::PearsonCorrelation => 30,
                Operator::RealizedVolatility | Operator::DownsideVolatility => 3,
                _ => 0,
            };
            warmup_bars = warmup_bars.max(warmup);
            bindings.push(OperatorDescriptor {
                id: *op,
                version: 1,
                inputs,
                output_unit: output_unit(*op)?,
                warmup_bars: warmup,
                null_policy: "propagate_missing".into(),
                deterministic: true,
            });
        }
        Ok(Self {
            version: QUERY_PLAN_VERSION.into(),
            universe,
            dataset_refs: Vec::new(),
            timeframe_ms: range.timeframe_ms,
            range,
            warmup_bars,
            sources: sources.into_iter().collect(),
            operators: bindings,
            result_projection: operators,
            parameters,
            filters,
            ranking,
            assumptions: vec!["closed_bars_half_open_utc".into(), "no_forward_fill".into()],
            resource_budget_ref: "research/default".into(),
            composition: None,
        })
    }

    pub fn with_composition(mut self, request: Option<CompositionRequest>) -> Result<Self, String> {
        if let Some(request) = request {
            self.composition = Some(CheckedComposition::compile(
                request,
                self.range.expected_bars,
            )?);
            if !self.sources.contains(&Source::Candles) {
                self.sources.push(Source::Candles);
                self.sources.sort();
            }
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn two_step_composition() -> CompositionRequest {
        CompositionRequest {
            version: COMPOSITION_VERSION.into(),
            steps: vec![
                SeriesStep {
                    id: "r1".into(),
                    op: SeriesOp::ReturnsSeries,
                    input: "close".into(),
                    period: None,
                },
                SeriesStep {
                    id: "w1".into(),
                    op: SeriesOp::RollingMean,
                    input: "r1".into(),
                    period: Some(2),
                },
            ],
            result_steps: vec!["w1".into()],
        }
    }
    #[test]
    fn t05_returns_series_can_feed_a_ratio_window_without_changing_scalar_returns() {
        let checked = CheckedComposition::compile(two_step_composition(), 3).unwrap();
        assert_eq!(checked.steps[0].unit, Unit::Ratio);
        assert_eq!(checked.steps[1].unit, Unit::Ratio);
        assert_eq!(checked.estimated_cells, 6);
        let result = checked.evaluate(&[100.0, 110.0, 99.0]);
        assert!(result["w1"].0.unwrap().abs() < 1e-12);
        assert_eq!(result["w1"].1, Unit::Ratio);
        assert!(
            (crate::operators::scalar(
                Operator::Returns,
                &[100.0, 110.0, 99.0],
                &[],
                &[],
                &[],
                14,
                60_000
            )
            .unwrap()
            .unwrap()
                + 0.01)
                .abs()
                < 1e-12
        );
    }
    #[test]
    fn t03_checked_composition_allows_branching_and_repeated_ops_but_rejects_bad_refs() {
        let mut spec = two_step_composition();
        spec.steps.push(SeriesStep {
            id: "w2".into(),
            op: SeriesOp::RollingMean,
            input: "r1".into(),
            period: Some(3),
        });
        spec.result_steps.push("w2".into());
        let checked = CheckedComposition::compile(spec.clone(), 20).unwrap();
        assert_eq!(checked.steps.len(), 3);
        let mut duplicate = spec.clone();
        duplicate.steps[2].id = "w1".into();
        assert!(
            CheckedComposition::compile(duplicate, 20)
                .unwrap_err()
                .contains("DUPLICATE")
        );
        let mut forward = spec.clone();
        forward.steps[0].input = "w2".into();
        assert!(
            CheckedComposition::compile(forward, 20)
                .unwrap_err()
                .contains("UNKNOWN_OR_FORWARD_STEP")
        );
        let mut wrong_type = spec.clone();
        wrong_type.steps[2].op = SeriesOp::ReturnsSeries;
        wrong_type.steps[2].period = None;
        assert!(
            CheckedComposition::compile(wrong_type, 20)
                .unwrap_err()
                .contains("EXPECTED_PRICE_SERIES")
        );
        let mut wrong_version = spec.clone();
        wrong_version.version = "v2".into();
        assert_eq!(
            CheckedComposition::compile(wrong_version, 20).unwrap_err(),
            "composition.version:UNSUPPORTED"
        );
        assert!(
            CheckedComposition::compile(spec, 3_000_000)
                .unwrap_err()
                .contains("CELLS_LIMIT")
        );
    }
    #[test]
    fn t04_missing_observations_and_warmup_are_not_converted_to_zero() {
        let checked = CheckedComposition::compile(two_step_composition(), 3).unwrap();
        assert_eq!(checked.evaluate(&[100.0, f64::NAN, 99.0])["w1"].0, None);
        assert_eq!(checked.evaluate(&[100.0, 110.0])["w1"].0, None);
        assert!(
            checked.evaluate(&[100.0, 110.0, 99.0])["w1"]
                .0
                .unwrap()
                .abs()
                < 1e-12
        );
    }
    fn fixture() -> (Vec<String>, ClosedRange, Vec<Operator>) {
        (
            vec!["BTC-USDT-SWAP".into()],
            ClosedRange::strict(0, 20 * 3_600_000, 3_600_000).unwrap(),
            vec![Operator::Returns, Operator::SpreadBps],
        )
    }
    #[test]
    fn k7_compiled_plan_has_typed_sources_units_and_deterministic_provenance() {
        let (universe, range, ops) = fixture();
        let plan = QueryPlan::compile(
            universe,
            range,
            ops,
            BTreeMap::new(),
            Vec::new(),
            Some(Rank {
                operator: Operator::Returns,
                descending: true,
            }),
        )
        .unwrap();
        assert_eq!(plan.sources, vec![Source::Candles, Source::Ticker]);
        assert_eq!(plan.operators[0].output_unit, Unit::Ratio);
        assert_eq!(plan.operators[1].output_unit, Unit::Bps);
        assert!(plan.operators.iter().all(|o| o.deterministic));
        assert_eq!(plan.range.expected_bars, 20);
    }
    #[test]
    fn k7_invalid_operator_bounds_or_parameter_fail_closed() {
        let (universe, range, ops) = fixture();
        let params = BTreeMap::from([("unrecognized_parameter".to_string(), 1.0)]);
        assert_eq!(
            QueryPlan::compile(
                universe.clone(),
                range,
                ops.clone(),
                params,
                Vec::new(),
                None
            )
            .unwrap_err(),
            "parameters.unrecognized_parameter:INVALID"
        );
        let filters = vec![Filter {
            operator: Operator::Rsi,
            min: None,
            max: Some(40.0),
        }];
        assert_eq!(
            QueryPlan::compile(
                universe.clone(),
                range,
                ops.clone(),
                BTreeMap::new(),
                filters,
                None
            )
            .unwrap_err(),
            "filters[0].operator:NOT_PROJECTED"
        );
        assert_eq!(
            QueryPlan::compile(
                universe,
                range,
                ops,
                BTreeMap::new(),
                Vec::new(),
                Some(Rank {
                    operator: Operator::Atr,
                    descending: false
                }),
            )
            .unwrap_err(),
            "ranking.operator:NOT_PROJECTED"
        );
    }
}

#[cfg(test)]
mod c20_query_plan_extension {
    use super::*;

    #[test]
    fn rolling_mean_descriptor_is_routed_without_finance_or_venue_dependency() {
        let plan = QueryPlan::compile(
            vec!["BTC-USDT-SWAP".into()],
            ClosedRange::strict(0, 20 * 3_600_000, 3_600_000).unwrap(),
            vec![Operator::RollingMean],
            BTreeMap::new(),
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(plan.sources, vec![Source::Candles]);
        assert_eq!(plan.operators[0].output_unit, Unit::Price);
        assert_eq!(plan.operators[0].warmup_bars, 14);
        assert_eq!(plan.operators[0].null_policy, "propagate_missing");
        assert!(plan.operators[0].deterministic);
    }
}
