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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositionRequest {
    pub version: String,
    pub steps: Vec<SeriesStep>,
    pub result_steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeriesStep {
    pub id: String,
    pub op: SeriesOp,
    /// "close" is the only source in v1; every other name is a prior step ID.
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    /// Only a prior rising_edge event may condition a future outcome label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesOp {
    ReturnsSeries,
    RollingMean,
    Lag,
    GreaterThan,
    RisingEdge,
    ForwardReturnLabel,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckedSeriesStep {
    pub step: SeriesStep,
    pub unit: Unit,
    pub depth: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
            if step.when.is_some() && step.op != SeriesOp::ForwardReturnLabel {
                return Err(format!("{path}.when:UNEXPECTED"));
            }
            let condition_depth = if let Some(id) = &step.when {
                let (unit, depth) = known
                    .get(id)
                    .copied()
                    .ok_or_else(|| format!("{path}.when:UNKNOWN_OR_FORWARD_STEP:{id}"))?;
                if unit != Unit::Event {
                    return Err(format!(
                        "{path}.when:UNIT_MISMATCH:expected=event:actual={}",
                        composition_unit_name(unit)
                    ));
                }
                depth
            } else {
                0
            };
            let depth = parent_depth.max(condition_depth) + 1;
            if depth > 8 {
                return Err(format!("{path}:DEPTH_LIMIT"));
            }
            let output_unit = match step.op {
                SeriesOp::ReturnsSeries => {
                    if input_unit != Unit::Price {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=price:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if step.period.is_some() {
                        return Err(format!("{path}.period:UNEXPECTED"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    Unit::Ratio
                }
                SeriesOp::RollingMean | SeriesOp::Lag => {
                    if !matches!(input_unit, Unit::Price | Unit::Ratio)
                        && !(step.op == SeriesOp::Lag
                            && matches!(input_unit, Unit::Boolean | Unit::Event))
                    {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=causal_series:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if !matches!(step.period, Some(1..=1000)) {
                        return Err(format!("{path}.period:INVALID"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    input_unit
                }
                SeriesOp::GreaterThan => {
                    if !matches!(input_unit, Unit::Price | Unit::Ratio) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=numeric_series:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if step.period.is_some() {
                        return Err(format!("{path}.period:UNEXPECTED"));
                    }
                    if !step.threshold.is_some_and(f64::is_finite) {
                        return Err(format!("{path}.threshold:INVALID"));
                    }
                    Unit::Boolean
                }
                SeriesOp::RisingEdge => {
                    if input_unit != Unit::Boolean {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=boolean:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if step.period.is_some() {
                        return Err(format!("{path}.period:UNEXPECTED"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    Unit::Event
                }
                SeriesOp::ForwardReturnLabel => {
                    if input_unit != Unit::Price {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=price:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if !matches!(step.period, Some(1..=1000)) {
                        return Err(format!("{path}.period:INVALID"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    if step.when.is_none() {
                        return Err(format!("{path}.when:REQUIRED_EVENT"));
                    }
                    // An outcome is deliberately not a numeric feature type.
                    // No causal operator accepts it as an input.
                    Unit::OutcomeRatio
                }
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

    /// Returns only bounded scalar projections for the latest closed bar.
    /// Outcome labels cannot be confirmed at this index without future bars.
    pub fn evaluate(&self, close: &[f64]) -> BTreeMap<String, (Option<f64>, Unit)> {
        self.evaluate_at(close, close.len().saturating_sub(1))
    }

    /// Project at a historical closed index using one deterministic graph pass.
    /// Causal outputs at index `at` must be invariant under changes after `at`.
    /// Outcome labels are explicitly non-causal and may use later bars.
    pub fn evaluate_at(&self, close: &[f64], at: usize) -> BTreeMap<String, (Option<f64>, Unit)> {
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
                        if let (Some(a), Some(b)) = (input[i - 1], input[i])
                            && a > 0.0
                            && b > 0.0
                        {
                            let value = b / a - 1.0;
                            result[i] = value.is_finite().then_some(value);
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
                SeriesOp::Lag => {
                    let period = checked.step.period.expect("validated lag") as usize;
                    let mut result = vec![None; input.len()];
                    for i in period..input.len() {
                        result[i] = input[i - period];
                    }
                    result
                }
                SeriesOp::GreaterThan => {
                    let threshold = checked.step.threshold.expect("validated threshold");
                    input
                        .iter()
                        .map(|value| value.map(|v| if v > threshold { 1.0 } else { 0.0 }))
                        .collect()
                }
                SeriesOp::RisingEdge => {
                    let mut result = vec![None; input.len()];
                    for i in 1..input.len() {
                        if let (Some(previous), Some(current)) = (input[i - 1], input[i]) {
                            result[i] = Some(if previous == 0.0 && current == 1.0 {
                                1.0
                            } else {
                                0.0
                            });
                        }
                    }
                    result
                }
                SeriesOp::ForwardReturnLabel => {
                    let horizon = checked.step.period.expect("validated horizon") as usize;
                    let condition = &values[checked.step.when.as_ref().expect("validated event")];
                    let mut result = vec![None; input.len()];
                    for i in 0..input.len().saturating_sub(horizon) {
                        if condition[i] == Some(1.0)
                            && let (Some(start), Some(end)) = (input[i], input[i + horizon])
                            && start > 0.0
                            && end > 0.0
                        {
                            let outcome = end / start - 1.0;
                            result[i] = outcome.is_finite().then_some(outcome);
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
                    (values[id].get(at).copied().flatten(), checked.unit),
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
    Boolean,
    Event,
    OutcomeRatio,
    AnnualizedRatio,
    Coefficient,
    Index,
    Bps,
    Quantity,
    QuoteCurrency,
}
fn composition_unit_name(unit: Unit) -> &'static str {
    match unit {
        Unit::Price => "price",
        Unit::Ratio => "ratio",
        Unit::Boolean => "boolean",
        Unit::Event => "event",
        Unit::OutcomeRatio => "outcome_ratio",
        Unit::AnnualizedRatio => "annualized_ratio",
        Unit::Coefficient => "coefficient",
        Unit::Index => "index",
        Unit::Bps => "bps",
        Unit::Quantity => "quantity",
        Unit::QuoteCurrency => "quote_currency",
    }
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
/// One source/type mapping for plan compilation and public capability discovery.
pub fn input(name: &str) -> Result<Input, String> {
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
                    threshold: None,
                    when: None,
                },
                SeriesStep {
                    id: "w1".into(),
                    op: SeriesOp::RollingMean,
                    input: "r1".into(),
                    period: Some(2),
                    threshold: None,
                    when: None,
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
            threshold: None,
            when: None,
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
        assert_eq!(
            CheckedComposition::compile(wrong_type, 20).unwrap_err(),
            "composition.steps[2].input:UNIT_MISMATCH:expected=price:actual=ratio"
        );
        let mut unexpected_period = spec.clone();
        unexpected_period.steps[0].period = Some(2);
        assert_eq!(
            CheckedComposition::compile(unexpected_period, 20).unwrap_err(),
            "composition.steps[0].period:UNEXPECTED"
        );
        let mut invalid_window = spec.clone();
        invalid_window.steps[1].period = Some(0);
        assert_eq!(
            CheckedComposition::compile(invalid_window, 20).unwrap_err(),
            "composition.steps[1].period:INVALID"
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
    fn t10_composition_checks_cells_steps_and_depth_at_the_boundary() {
        let parallel_steps = |count: usize| CompositionRequest {
            version: COMPOSITION_VERSION.into(),
            steps: (0..count)
                .map(|i| SeriesStep {
                    id: format!("w{i}"),
                    op: SeriesOp::RollingMean,
                    input: "close".into(),
                    period: Some(1),
                    threshold: None,
                    when: None,
                })
                .collect(),
            result_steps: vec!["w0".into()],
        };
        let eight = parallel_steps(8);
        assert_eq!(
            CheckedComposition::compile(eight.clone(), 1_000_000)
                .unwrap()
                .estimated_cells,
            8_000_000
        );
        assert_eq!(
            CheckedComposition::compile(eight, 1_000_001).unwrap_err(),
            "composition:CELLS_LIMIT"
        );
        assert_eq!(
            CheckedComposition::compile(parallel_steps(33), 1).unwrap_err(),
            "composition.steps:COUNT_LIMIT"
        );
        assert_eq!(
            CheckedComposition::compile(parallel_steps(8), u64::MAX).unwrap_err(),
            "composition:CELLS_OVERFLOW"
        );

        let mut chained = parallel_steps(9);
        for i in 1..9 {
            chained.steps[i].input = format!("w{}", i - 1);
        }
        assert_eq!(
            CheckedComposition::compile(chained.clone(), 1).unwrap_err(),
            "composition.steps[8]:DEPTH_LIMIT"
        );
        chained.steps.pop();
        assert!(CheckedComposition::compile(chained, 1).is_ok());
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

    fn causal_event_graph() -> CompositionRequest {
        let step = |id: &str, op, input: &str, period, threshold, when: Option<&str>| {
            SeriesStep {
                id: id.into(),
                op,
                input: input.into(),
                period,
                threshold,
                when: when.map(str::to_owned),
            }
        };
        CompositionRequest {
            version: COMPOSITION_VERSION.into(),
            steps: vec![
                step("return", SeriesOp::ReturnsSeries, "close", None, None, None),
                step("positive", SeriesOp::GreaterThan, "return", None, Some(0.0), None),
                step("entry", SeriesOp::RisingEdge, "positive", None, None, None),
                step("previous", SeriesOp::Lag, "close", Some(1), None, None),
                step(
                    "outcome",
                    SeriesOp::ForwardReturnLabel,
                    "close",
                    Some(2),
                    None,
                    Some("entry"),
                ),
            ],
            result_steps: vec![
                "return".into(),
                "positive".into(),
                "entry".into(),
                "previous".into(),
                "outcome".into(),
            ],
        }
    }

    #[test]
    fn t06_causal_graph_lag_boolean_edge_and_outcomes_have_separate_time_directions() {
        let graph = CheckedComposition::compile(causal_event_graph(), 7).unwrap();
        let prices = [100.0, 90.0, 95.0, 99.0, 120.0, 110.0, 108.0];
        let at_event = graph.evaluate_at(&prices, 2);
        assert_eq!(at_event["positive"], (Some(1.0), Unit::Boolean));
        assert_eq!(at_event["entry"], (Some(1.0), Unit::Event));
        assert_eq!(at_event["previous"], (Some(90.0), Unit::Price));
        assert!((at_event["outcome"].0.unwrap() - (120.0 / 95.0 - 1.0)).abs() < 1e-12);
        assert_eq!(at_event["outcome"].1, Unit::OutcomeRatio);
        assert_eq!(graph.evaluate_at(&prices, 3)["entry"].0, Some(0.0));
        assert_eq!(graph.evaluate_at(&prices, 1)["entry"].0, None);
        assert_eq!(graph.evaluate(&prices)["outcome"].0, None);

        let first_three = graph.evaluate_at(&prices[..3], 2);
        let altered_future = graph.evaluate_at(&[100.0, 90.0, 95.0, 99.0, 60.0, 200.0], 2);
        for causal in ["return", "positive", "entry", "previous"] {
            assert_eq!(first_three[causal], at_event[causal]);
            assert_eq!(altered_future[causal], at_event[causal]);
        }
        assert_eq!(first_three["outcome"].0, None);
        assert!((altered_future["outcome"].0.unwrap() - (60.0 / 95.0 - 1.0)).abs() < 1e-12);
    }

    #[test]
    fn t06_every_causal_projection_is_prefix_invariant_for_each_closed_index() {
        let graph = CheckedComposition::compile(causal_event_graph(), 7).unwrap();
        let closes = [100.0, 90.0, 95.0, 99.0, 120.0, 110.0, 108.0];
        for at in 0..closes.len() {
            let full = graph.evaluate_at(&closes, at);
            let prefix = graph.evaluate_at(&closes[..=at], at);
            let mut changed_future = closes;
            for later in &mut changed_future[at + 1..] {
                *later = 55.0;
            }
            let changed = graph.evaluate_at(&changed_future, at);
            for feature in ["return", "positive", "entry", "previous"] {
                assert_eq!(full[feature], prefix[feature], "prefix mismatch: {feature} at {at}");
                assert_eq!(full[feature], changed[feature], "future leak: {feature} at {at}");
            }
        }
    }

    #[test]
    fn t05_original_composition_wire_omits_new_optional_graph_fields() {
        let old = two_step_composition();
        let encoded = serde_json::to_value(&old).unwrap();
        assert_eq!(encoded["steps"][0]["op"], "returns_series");
        assert!(encoded["steps"][0].get("threshold").is_none());
        assert!(encoded["steps"][0].get("when").is_none());
        assert_eq!(serde_json::from_value::<CompositionRequest>(encoded).unwrap(), old);
    }

    #[test]
    fn t07_outcome_cannot_be_a_feature_or_event_condition() {
        let spec = causal_event_graph();
        let mut leakage = spec.clone();
        leakage.steps.push(SeriesStep {
            id: "leak".into(),
            op: SeriesOp::GreaterThan,
            input: "outcome".into(),
            period: None,
            threshold: Some(0.0),
            when: None,
        });
        assert_eq!(
            CheckedComposition::compile(leakage.clone(), 7).unwrap_err(),
            "composition.steps[5].input:UNIT_MISMATCH:expected=numeric_series:actual=outcome_ratio"
        );
        leakage.steps[5].op = SeriesOp::Lag;
        leakage.steps[5].period = Some(1);
        leakage.steps[5].threshold = None;
        assert_eq!(
            CheckedComposition::compile(leakage, 7).unwrap_err(),
            "composition.steps[5].input:UNIT_MISMATCH:expected=causal_series:actual=outcome_ratio"
        );

        let mut missing_event = spec.clone();
        missing_event.steps[4].when = Some("future".into());
        assert_eq!(
            CheckedComposition::compile(missing_event, 7).unwrap_err(),
            "composition.steps[4].when:UNKNOWN_OR_FORWARD_STEP:future"
        );
        let mut non_event = spec.clone();
        non_event.steps[4].when = Some("positive".into());
        assert_eq!(
            CheckedComposition::compile(non_event, 7).unwrap_err(),
            "composition.steps[4].when:UNIT_MISMATCH:expected=event:actual=boolean"
        );
        let mut no_event = spec.clone();
        no_event.steps[4].when = None;
        assert_eq!(
            CheckedComposition::compile(no_event, 7).unwrap_err(),
            "composition.steps[4].when:REQUIRED_EVENT"
        );
        let mut bad_threshold = spec.clone();
        bad_threshold.steps[1].threshold = Some(f64::NAN);
        assert_eq!(
            CheckedComposition::compile(bad_threshold, 7).unwrap_err(),
            "composition.steps[1].threshold:INVALID"
        );
        let mut bad_horizon = spec.clone();
        bad_horizon.steps[4].period = Some(0);
        assert_eq!(
            CheckedComposition::compile(bad_horizon, 7).unwrap_err(),
            "composition.steps[4].period:INVALID"
        );
        let mut unexpected_when = spec;
        unexpected_when.steps[0].when = Some("entry".into());
        assert_eq!(
            CheckedComposition::compile(unexpected_when, 7).unwrap_err(),
            "composition.steps[0].when:UNEXPECTED"
        );
    }

    #[test]
    fn t04_causal_graph_preserves_nulls_across_missing_and_warmup_bars() {
        let graph = CheckedComposition::compile(causal_event_graph(), 6).unwrap();
        let inputs = [100.0, f64::NAN, 95.0, 99.0, 120.0, 110.0];
        assert_eq!(graph.evaluate_at(&inputs, 2)["return"].0, None);
        assert_eq!(graph.evaluate_at(&inputs, 2)["positive"].0, None);
        assert_eq!(graph.evaluate_at(&inputs, 2)["entry"].0, None);
        assert_eq!(graph.evaluate_at(&inputs, 2)["previous"].0, None);
        assert_eq!(graph.evaluate_at(&inputs, 2)["outcome"].0, None);
        assert_eq!(graph.evaluate_at(&inputs, 0)["previous"].0, None);
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
