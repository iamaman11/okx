//! Typed K7 analysis plan, independent of storage, transport and venue.
use crate::{
    operators::{Operator, registry},
    time_series::ClosedRange,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const QUERY_PLAN_VERSION: &str = "okx.query-plan/448.1";
pub const COMPOSITION_VERSION: &str = "okx.market.composition/v1";
const MAX_COMPOSITION_WORK_UNITS: u64 = 64_000_000;

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
    /// "close" (price), "open_interest" (contracts), and sparse "funding_rate"
    /// (realized settlement ratio at event time) are sources; other names are prior steps.
    pub input: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    /// Only a prior rising_edge event may condition a future outcome label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// Second prior series for dimensionless arithmetic or Boolean logic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub other: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesOp {
    ReturnsSeries,
    RollingMean,
    RollingMax,
    RollingMin,
    RollingPearson,
    AddSeries,
    SubtractSeries,
    DivideSeries,
    And,
    Or,
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

/// A deterministic explanation for a projected null. Diagnosed from the
/// already-evaluated series, without storing another per-bar quality graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositionNullCause {
    pub reason: &'static str,
    pub origin_step: String,
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
        let mut estimated_work = 0u64;
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
            if matches!(step.id.as_str(), "close" | "open_interest" | "funding_rate")
                || known.contains_key(&step.id)
            {
                return Err(format!("{path}.id:DUPLICATE"));
            }
            let (input_unit, parent_depth) = match step.input.as_str() {
                "close" => (Unit::Price, 0),
                "open_interest" => (Unit::Contracts, 0),
                "funding_rate" => (Unit::Ratio, 0),
                _ => known.get(&step.input).copied().ok_or_else(|| {
                    format!("{path}.input:UNKNOWN_OR_FORWARD_STEP:{}", step.input)
                })?,
            };
            if step.when.is_some() && step.op != SeriesOp::ForwardReturnLabel {
                return Err(format!("{path}.when:UNEXPECTED"));
            }
            if step.other.is_some()
                && !matches!(
                    step.op,
                    SeriesOp::AddSeries
                        | SeriesOp::SubtractSeries
                        | SeriesOp::DivideSeries
                        | SeriesOp::RollingPearson
                        | SeriesOp::And
                        | SeriesOp::Or
                )
            {
                return Err(format!("{path}.other:UNEXPECTED"));
            }
            let other = if matches!(
                step.op,
                SeriesOp::AddSeries
                    | SeriesOp::SubtractSeries
                    | SeriesOp::DivideSeries
                    | SeriesOp::RollingPearson
                    | SeriesOp::And
                    | SeriesOp::Or
            ) {
                let name = step
                    .other
                    .as_deref()
                    .ok_or_else(|| format!("{path}.other:REQUIRED"))?;
                Some(match name {
                    "close" => (Unit::Price, 0),
                    "open_interest" => (Unit::Contracts, 0),
                    "funding_rate" => (Unit::Ratio, 0),
                    _ => known
                        .get(name)
                        .copied()
                        .ok_or_else(|| format!("{path}.other:UNKNOWN_OR_FORWARD_STEP:{name}"))?,
                })
            } else {
                None
            };
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
            let depth = parent_depth
                .max(condition_depth)
                .max(other.map_or(0, |(_, d)| d))
                + 1;
            if depth > 8 {
                return Err(format!("{path}:DEPTH_LIMIT"));
            }
            let output_unit = match step.op {
                SeriesOp::RollingPearson => {
                    if input_unit != Unit::Ratio {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let unit = other.expect("validated second input").0;
                    if unit != Unit::Ratio {
                        return Err(format!(
                            "{path}.other:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(unit)
                        ));
                    }
                    if !matches!(step.period, Some(30..=1000)) {
                        return Err(format!("{path}.period:INVALID:30..1000"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    Unit::Coefficient
                }
                SeriesOp::AddSeries | SeriesOp::SubtractSeries | SeriesOp::DivideSeries => {
                    if input_unit != Unit::Ratio {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let unit = other.expect("validated second input").0;
                    if unit != Unit::Ratio {
                        return Err(format!(
                            "{path}.other:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(unit)
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
                SeriesOp::And | SeriesOp::Or => {
                    if input_unit != Unit::Boolean {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=boolean:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let unit = other.expect("validated second condition").0;
                    if unit != Unit::Boolean {
                        return Err(format!(
                            "{path}.other:UNIT_MISMATCH:expected=boolean:actual={}",
                            composition_unit_name(unit)
                        ));
                    }
                    if step.period.is_some() {
                        return Err(format!("{path}.period:UNEXPECTED"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    Unit::Boolean
                }
                SeriesOp::ReturnsSeries => {
                    if !matches!(input_unit, Unit::Price | Unit::Contracts) {
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
                SeriesOp::RollingMean
                | SeriesOp::RollingMax
                | SeriesOp::RollingMin
                | SeriesOp::Lag => {
                    let numeric = matches!(
                        input_unit,
                        Unit::Price | Unit::Ratio | Unit::Contracts | Unit::Coefficient
                    );
                    let lagged_discrete = step.op == SeriesOp::Lag
                        && matches!(input_unit, Unit::Boolean | Unit::Event);
                    if !(numeric || lagged_discrete) {
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
                    if !matches!(input_unit, Unit::Price | Unit::Ratio | Unit::Coefficient) {
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
            // Rolling mean is a true sliding window: the cost does not grow
            // with period. Count add/remove/emit, not O(window) rescans.
            let cost_per_bar = series_cost_per_bar(step.op);
            estimated_work = estimated_work
                .checked_add(
                    total_bars
                        .checked_mul(cost_per_bar)
                        .ok_or("composition:WORK_OVERFLOW")?,
                )
                .ok_or("composition:WORK_OVERFLOW")?;
            if estimated_work > MAX_COMPOSITION_WORK_UNITS {
                return Err("composition:WORK_LIMIT".into());
            }
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
        if source_buffer_count(&steps) > 1
            && total_bars
                .checked_mul(steps.len() as u64 + source_buffer_count(&steps))
                .is_none_or(|cells| cells > 8_000_000)
        {
            return Err("composition:SOURCE_CELLS_LIMIT".into());
        }
        Ok(Self {
            version: request.version,
            steps,
            result_steps: request.result_steps,
            estimated_cells: cells,
        })
    }

    /// A fully type-checked graph can be rebound to a venue-wide instrument
    /// set without parsing or validating the user's step descriptions again.
    /// Check the actual global allocation and work bound before job admission.
    pub fn validate_global_budget(&self, total_bars: u64) -> Result<(), String> {
        let cells = total_bars
            .checked_mul(self.steps.len() as u64)
            .ok_or("composition:CELLS_OVERFLOW")?;
        if cells > 8_000_000 {
            return Err("composition:CELLS_LIMIT".into());
        }
        let mut work = 0u64;
        for checked in &self.steps {
            let cost = series_cost_per_bar(checked.step.op);
            work = work
                .checked_add(
                    total_bars
                        .checked_mul(cost)
                        .ok_or("composition:WORK_OVERFLOW")?,
                )
                .ok_or("composition:WORK_OVERFLOW")?;
            if work > MAX_COMPOSITION_WORK_UNITS {
                return Err("composition:WORK_LIMIT".into());
            }
        }
        if source_buffer_count(&self.steps) > 1
            && total_bars
                .checked_mul(self.steps.len() as u64 + source_buffer_count(&self.steps))
                .is_none_or(|source_cells| source_cells > 8_000_000)
        {
            return Err("composition:SOURCE_CELLS_LIMIT".into());
        }
        Ok(())
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
        self.evaluate_at_sources(close, None, None, at, false).0
    }

    /// Preserve the legacy value projection; add precise explanations only
    /// for projected nulls, calculated once at the requested closed index.
    pub fn evaluate_with_diagnostics(
        &self,
        close: &[f64],
    ) -> (
        BTreeMap<String, (Option<f64>, Unit)>,
        BTreeMap<String, CompositionNullCause>,
    ) {
        self.evaluate_at_sources(close, None, None, close.len().saturating_sub(1), true)
    }

    pub fn evaluate_with_open_interest(
        &self,
        close: &[f64],
        open_interest: &[Option<f64>],
    ) -> Result<BTreeMap<String, (Option<f64>, Unit)>, String> {
        self.check_open_interest(close, open_interest)?;
        Ok(self
            .evaluate_at_sources(
                close,
                Some(open_interest),
                None,
                close.len().saturating_sub(1),
                false,
            )
            .0)
    }

    pub fn evaluate_with_open_interest_diagnostics(
        &self,
        close: &[f64],
        open_interest: &[Option<f64>],
    ) -> Result<
        (
            BTreeMap<String, (Option<f64>, Unit)>,
            BTreeMap<String, CompositionNullCause>,
        ),
        String,
    > {
        self.check_open_interest(close, open_interest)?;
        Ok(self.evaluate_at_sources(
            close,
            Some(open_interest),
            None,
            close.len().saturating_sub(1),
            true,
        ))
    }

    /// Aligned source values must be event-time exact. A missing funding event
    /// stays `None` rather than becoming a zero-rate or forward-filled sample.
    /// The caller owns publication-time qualification of any derived output.
    pub fn evaluate_with_sources_diagnostics(
        &self,
        close: &[f64],
        open_interest: Option<&[Option<f64>]>,
        funding_rate: Option<&[Option<f64>]>,
    ) -> Result<
        (
            BTreeMap<String, (Option<f64>, Unit)>,
            BTreeMap<String, CompositionNullCause>,
        ),
        String,
    > {
        if let Some(oi) = open_interest {
            self.check_open_interest(close, oi)?;
        }
        if let Some(rates) = funding_rate {
            if rates.len() != close.len() {
                return Err("composition.sources:ALIGNMENT_LENGTH_MISMATCH".into());
            }
            if rates.iter().flatten().any(|rate| !rate.is_finite()) {
                return Err("composition.sources.funding_rate:NONFINITE_RATE".into());
            }
        }
        Ok(self.evaluate_at_sources(
            close,
            open_interest,
            funding_rate,
            close.len().saturating_sub(1),
            true,
        ))
    }

    fn check_open_interest(
        &self,
        close: &[f64],
        open_interest: &[Option<f64>],
    ) -> Result<(), String> {
        if close.len() != open_interest.len() {
            return Err("composition.sources:ALIGNMENT_LENGTH_MISMATCH".into());
        }
        if open_interest
            .iter()
            .flatten()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err("composition.sources.open_interest:INVALID_CONTRACT_COUNT".into());
        }
        Ok(())
    }

    fn evaluate_at_sources(
        &self,
        close: &[f64],
        open_interest: Option<&[Option<f64>]>,
        funding_rate: Option<&[Option<f64>]>,
        at: usize,
        explain_nulls: bool,
    ) -> (
        BTreeMap<String, (Option<f64>, Unit)>,
        BTreeMap<String, CompositionNullCause>,
    ) {
        let mut values = BTreeMap::<String, Vec<Option<f64>>>::new();
        values.insert(
            "close".into(),
            close.iter().map(|v| v.is_finite().then_some(*v)).collect(),
        );
        if self
            .steps
            .iter()
            .any(|step| step.step.input == "open_interest")
        {
            values.insert(
                "open_interest".into(),
                open_interest
                    .map(|source| source.to_vec())
                    .unwrap_or_else(|| vec![None; close.len()]),
            );
        }
        if self.steps.iter().any(|step| {
            step.step.input == "funding_rate" || step.step.other.as_deref() == Some("funding_rate")
        }) {
            values.insert(
                "funding_rate".into(),
                funding_rate
                    .map(|source| source.to_vec())
                    .unwrap_or_else(|| vec![None; close.len()]),
            );
        }
        for checked in &self.steps {
            // A shared upstream series is read by reference for all branches;
            // cloning it here multiplied peak memory and source work.
            let input = &values[&checked.step.input];
            let output = match checked.step.op {
                SeriesOp::RollingPearson => {
                    let other =
                        &values[checked.step.other.as_ref().expect("validated second input")];
                    rolling_pearson(
                        input,
                        other,
                        checked.step.period.expect("validated period") as usize,
                    )
                }
                SeriesOp::AddSeries | SeriesOp::SubtractSeries | SeriesOp::DivideSeries => {
                    let other =
                        &values[checked.step.other.as_ref().expect("validated second input")];
                    input
                        .iter()
                        .zip(other)
                        .map(|(left, right)| match (left, right) {
                            (Some(a), Some(b)) => {
                                let value = match checked.step.op {
                                    SeriesOp::AddSeries => *a + *b,
                                    SeriesOp::SubtractSeries => *a - *b,
                                    SeriesOp::DivideSeries if *b != 0.0 => *a / *b,
                                    SeriesOp::DivideSeries => return None,
                                    _ => unreachable!("checked binary arithmetic"),
                                };
                                value.is_finite().then_some(value)
                            }
                            _ => None,
                        })
                        .collect()
                }
                SeriesOp::And | SeriesOp::Or => {
                    let other = &values[checked
                        .step
                        .other
                        .as_ref()
                        .expect("validated second condition")];
                    input
                        .iter()
                        .zip(other)
                        .map(|(left, right)| match (left, right) {
                            (Some(a), Some(b)) => Some(if checked.step.op == SeriesOp::And {
                                f64::from(*a == 1.0 && *b == 1.0)
                            } else {
                                f64::from(*a == 1.0 || *b == 1.0)
                            }),
                            // A missing observation is unknown, never false.
                            _ => None,
                        })
                        .collect()
                }
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
                    // Accumulate normalized contributions to avoid overflow
                    // when finite prices sum beyond f64::MAX although their
                    // *mean* is representable.
                    let mut mean_sum = 0.0f64;
                    let mut correction = 0.0f64;
                    let divisor = period as f64;
                    let mut missing = 0usize;
                    for (i, current) in input.iter().copied().enumerate() {
                        if i >= period {
                            match input[i - period] {
                                Some(removed) => compensated_add(
                                    &mut mean_sum,
                                    &mut correction,
                                    -removed / divisor,
                                ),
                                None => missing -= 1,
                            }
                        }
                        match current {
                            Some(value) => {
                                compensated_add(&mut mean_sum, &mut correction, value / divisor)
                            }
                            None => missing += 1,
                        }
                        if i + 1 >= period && missing == 0 {
                            let mean = mean_sum + correction;
                            result[i] = mean.is_finite().then_some(mean);
                        }
                    }
                    result
                }
                SeriesOp::RollingMax | SeriesOp::RollingMin => rolling_extremum(
                    &input,
                    checked.step.period.expect("validated extrema period") as usize,
                    checked.step.op == SeriesOp::RollingMax,
                ),
                SeriesOp::Lag => {
                    let period = checked.step.period.expect("validated lag") as usize;
                    let mut result = vec![None; input.len()];
                    if period < input.len() {
                        result[period..].copy_from_slice(&input[..input.len() - period]);
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
        let diagnostics = if explain_nulls {
            self.result_steps
                .iter()
                .filter(|id| values[id.as_str()].get(at).copied().flatten().is_none())
                .map(|id| (id.clone(), self.null_cause_at(&values, id, at)))
                .collect()
        } else {
            BTreeMap::new()
        };
        let results = self
            .result_steps
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
            .collect();
        (results, diagnostics)
    }

    /// Only the projected nulls are traced. Graph depth <=8, windows <=1000,
    /// and projected results <=14 bound diagnostics independently of bar count.
    fn null_cause_at(
        &self,
        values: &BTreeMap<String, Vec<Option<f64>>>,
        id: &str,
        at: usize,
    ) -> CompositionNullCause {
        let cause = |reason| CompositionNullCause {
            reason,
            origin_step: id.to_owned(),
        };
        let Some(checked) = self.steps.iter().find(|item| item.step.id == id) else {
            return cause("SOURCE_OBSERVATION_MISSING");
        };
        let step = &checked.step;
        let value = |name: &str, index: usize| {
            values
                .get(name)
                .and_then(|series| series.get(index))
                .copied()
                .flatten()
        };
        let trace = |name: &str, index: usize| -> Option<CompositionNullCause> {
            value(name, index)
                .is_none()
                .then(|| self.null_cause_at(values, name, index))
        };
        match step.op {
            SeriesOp::AddSeries
            | SeriesOp::SubtractSeries
            | SeriesOp::DivideSeries
            | SeriesOp::And
            | SeriesOp::Or => {
                let other = step.other.as_deref().expect("validated second operand");
                if let Some(upstream) = trace(&step.input, at).or_else(|| trace(other, at)) {
                    return upstream;
                }
                if step.op == SeriesOp::DivideSeries && value(other, at) == Some(0.0) {
                    return cause("DIVISION_BY_ZERO");
                }
                cause("NON_FINITE_RESULT")
            }
            SeriesOp::RollingPearson => {
                let period = step.period.expect("validated correlation window") as usize;
                if at + 1 < period {
                    return cause("WARMUP");
                }
                let other = step.other.as_deref().expect("validated second input");
                for index in (at + 1 - period)..=at {
                    if let Some(upstream) =
                        trace(&step.input, index).or_else(|| trace(other, index))
                    {
                        return upstream;
                    }
                }
                let mut stats = SlidingPearson::default();
                for index in (at + 1 - period)..=at {
                    stats.add(
                        value(&step.input, index).unwrap(),
                        value(other, index).unwrap(),
                    );
                }
                if stats.m2_x <= 0.0 || stats.m2_y <= 0.0 {
                    return cause("ZERO_VARIANCE");
                }
                cause("NON_FINITE_RESULT")
            }
            SeriesOp::ReturnsSeries => {
                if at == 0 {
                    return cause("WARMUP");
                }
                if let Some(upstream) =
                    trace(&step.input, at - 1).or_else(|| trace(&step.input, at))
                {
                    return upstream;
                }
                if value(&step.input, at - 1).is_some_and(|v| v <= 0.0)
                    || value(&step.input, at).is_some_and(|v| v <= 0.0)
                {
                    return cause("NON_POSITIVE_RETURN_INPUT");
                }
                cause("NON_FINITE_RESULT")
            }
            SeriesOp::RollingMean | SeriesOp::RollingMax | SeriesOp::RollingMin => {
                let period = step.period.expect("validated window") as usize;
                if at + 1 < period {
                    return cause("WARMUP");
                }
                for index in (at + 1 - period)..=at {
                    if let Some(upstream) = trace(&step.input, index) {
                        return upstream;
                    }
                }
                cause("NON_FINITE_RESULT")
            }
            SeriesOp::Lag => {
                let period = step.period.expect("validated lag") as usize;
                if at < period {
                    return cause("WARMUP");
                }
                trace(&step.input, at - period).unwrap_or_else(|| cause("NON_FINITE_RESULT"))
            }
            SeriesOp::GreaterThan => {
                trace(&step.input, at).unwrap_or_else(|| cause("NON_FINITE_RESULT"))
            }
            SeriesOp::RisingEdge => {
                if at == 0 {
                    return cause("WARMUP");
                }
                trace(&step.input, at - 1)
                    .or_else(|| trace(&step.input, at))
                    .unwrap_or_else(|| cause("NON_FINITE_RESULT"))
            }
            SeriesOp::ForwardReturnLabel => {
                let horizon = step.period.expect("validated horizon") as usize;
                let Some(end) = at
                    .checked_add(horizon)
                    .filter(|end| *end < values[&step.input].len())
                else {
                    return cause("OUTCOME_UNCONFIRMED");
                };
                let condition = step.when.as_deref().expect("validated event");
                if let Some(upstream) = trace(condition, at) {
                    return upstream;
                }
                if value(condition, at) != Some(1.0) {
                    return cause("EVENT_NOT_TRIGGERED");
                }
                if let Some(upstream) = trace(&step.input, at).or_else(|| trace(&step.input, end)) {
                    return upstream;
                }
                if value(&step.input, at).is_some_and(|v| v <= 0.0)
                    || value(&step.input, end).is_some_and(|v| v <= 0.0)
                {
                    return cause("NON_POSITIVE_RETURN_INPUT");
                }
                cause("NON_FINITE_RESULT")
            }
        }
    }

    pub fn result_sources(&self) -> BTreeMap<String, BTreeSet<Source>> {
        let mut known = BTreeMap::<String, BTreeSet<Source>>::new();
        known.insert("close".into(), BTreeSet::from([Source::Candles]));
        known.insert(
            "open_interest".into(),
            BTreeSet::from([Source::OpenInterest]),
        );
        known.insert("funding_rate".into(), BTreeSet::from([Source::Funding]));
        for checked in &self.steps {
            let mut sources = known[&checked.step.input].clone();
            if let Some(condition) = &checked.step.when {
                sources.extend(known[condition].iter().copied());
            }
            if let Some(other) = &checked.step.other {
                sources.extend(known[other].iter().copied());
            }
            known.insert(checked.step.id.clone(), sources);
        }
        self.result_steps
            .iter()
            .map(|id| (id.clone(), known[id].clone()))
            .collect()
    }

    pub fn sources(&self) -> BTreeSet<Source> {
        self.result_sources().into_values().flatten().collect()
    }
}

fn source_buffer_count(steps: &[CheckedSeriesStep]) -> u64 {
    let uses = |name: &str| {
        steps.iter().any(|checked| {
            checked.step.input == name || checked.step.other.as_deref() == Some(name)
        })
    };
    1 + u64::from(uses("open_interest")) + u64::from(uses("funding_rate"))
}

fn series_cost_per_bar(op: SeriesOp) -> u64 {
    match op {
        SeriesOp::RollingMean => 3,
        SeriesOp::RollingPearson => 8,
        SeriesOp::AddSeries
        | SeriesOp::SubtractSeries
        | SeriesOp::DivideSeries
        | SeriesOp::And
        | SeriesOp::Or => 2,
        _ => 1,
    }
}

/// Online centered covariance with O(1) insertion and removal. This avoids
/// catastrophic cancellation of raw sums of squares for near-constant data.
#[derive(Default)]
struct SlidingPearson {
    n: usize,
    mean_x: f64,
    mean_y: f64,
    m2_x: f64,
    m2_y: f64,
    covariance: f64,
}

impl SlidingPearson {
    fn add(&mut self, x: f64, y: f64) {
        self.n += 1;
        let n = self.n as f64;
        let dx = x - self.mean_x;
        let dy = y - self.mean_y;
        self.mean_x += dx / n;
        self.mean_y += dy / n;
        self.m2_x += dx * (x - self.mean_x);
        self.m2_y += dy * (y - self.mean_y);
        self.covariance += dx * (y - self.mean_y);
    }

    fn remove(&mut self, x: f64, y: f64) {
        if self.n <= 1 {
            *self = Self::default();
            return;
        }
        let next_n = (self.n - 1) as f64;
        let dx = x - self.mean_x;
        let dy = y - self.mean_y;
        let next_mean_x = self.mean_x - dx / next_n;
        let next_mean_y = self.mean_y - dy / next_n;
        self.m2_x -= dx * (x - next_mean_x);
        self.m2_y -= dy * (y - next_mean_y);
        self.covariance -= dx * (y - next_mean_y);
        self.n -= 1;
        self.mean_x = next_mean_x;
        self.mean_y = next_mean_y;
    }

    fn coefficient(&self) -> Option<f64> {
        if self.m2_x <= 0.0 || self.m2_y <= 0.0 {
            return None;
        }
        let denominator = self.m2_x.sqrt() * self.m2_y.sqrt();
        let correlation = self.covariance / denominator;
        if correlation.is_finite() && correlation.abs() <= 1.000_001 {
            Some(correlation.clamp(-1.0, 1.0))
        } else {
            None
        }
    }
}

/// Exact-index join of two dimensionless return series. Every observation in
/// the specified window must be present; no as-of filling, future values or
/// exclusion of missing pairs. Periodic bounded rebasing limits numeric drift
/// without making the per-bar cost depend on window size.
fn rolling_pearson(left: &[Option<f64>], right: &[Option<f64>], period: usize) -> Vec<Option<f64>> {
    let mut output = vec![None; left.len()];
    let mut stats = SlidingPearson::default();
    let mut missing = 0usize;
    let pair = |index: usize| match (left[index], right[index]) {
        (Some(a), Some(b)) if a.is_finite() && b.is_finite() => Some((a, b)),
        _ => None,
    };
    for i in 0..left.len() {
        if i >= period {
            match pair(i - period) {
                Some((a, b)) => stats.remove(a, b),
                None => missing -= 1,
            }
        }
        // Exactly O(period) once per 4096 observations: the amortized number
        // of operations remains bounded independently of the window size.
        if i > 0 && i.is_multiple_of(4096) {
            stats = SlidingPearson::default();
            missing = 0;
            for old in i.saturating_sub(period - 1)..i {
                match pair(old) {
                    Some((a, b)) => stats.add(a, b),
                    None => missing += 1,
                }
            }
        }
        match pair(i) {
            Some((a, b)) => stats.add(a, b),
            None => missing += 1,
        }
        if i + 1 >= period && missing == 0 {
            output[i] = stats.coefficient();
        }
    }
    output
}

/// Compensated add/subtract for a sliding window, preserving small terms
/// when a large observation exits. Each update is constant time.
fn compensated_add(sum: &mut f64, correction: &mut f64, term: f64) {
    let next = *sum + term;
    if sum.abs() >= term.abs() {
        *correction += (*sum - next) + term;
    } else {
        *correction += (term - next) + *sum;
    }
    *sum = next;
}

/// Causal, closed-bar window extrema in O(bars) time and O(period) memory.
/// Every incomplete window, including one with an interior gap, stays None.
fn rolling_extremum(input: &[Option<f64>], period: usize, maximum: bool) -> Vec<Option<f64>> {
    let mut result = vec![None; input.len()];
    let mut deque = VecDeque::<(usize, f64)>::with_capacity(period);
    let mut missing = 0usize;
    for (index, value) in input.iter().copied().enumerate() {
        if index >= period && input[index - period].is_none() {
            missing -= 1;
        }
        match value {
            Some(value) => {
                while deque.back().is_some_and(|(_, prior)| {
                    if maximum {
                        *prior <= value
                    } else {
                        *prior >= value
                    }
                }) {
                    deque.pop_back();
                }
                deque.push_back((index, value));
            }
            None => missing += 1,
        }
        while deque
            .front()
            .is_some_and(|(prior, _)| prior.saturating_add(period) <= index)
        {
            deque.pop_front();
        }
        if index + 1 >= period && missing == 0 {
            result[index] = deque.front().map(|(_, value)| *value);
        }
    }
    result
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
    Contracts,
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
        Unit::Contracts => "contracts",
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
        "funding" | "funding_rate" => (Source::Funding, Unit::Ratio),
        "open_interest" => (Source::OpenInterest, Unit::Contracts),
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
        // Resolve optional operator defaults once, in the pure plan owner.
        // The worker must execute these values, not maintain another set of
        // independent defaults which could drift between validation and I/O.
        let mut parameters = parameters;
        for name in ["ema_period", "atr_period", "rsi_period"] {
            parameters.entry(name.into()).or_insert(14.0);
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
            let checked = CheckedComposition::compile(request, self.range.expected_bars)?;
            for source in checked.sources() {
                if !self.sources.contains(&source) {
                    self.sources.push(source);
                }
            }
            self.sources.sort();
            self.composition = Some(checked);
        }
        Ok(self)
    }

    /// Bind a resolved venue universe without recompiling operation IDs or
    /// silently applying the 100-explicit-instrument limit to a global scan.
    /// This check runs before source reads or durable job admission.
    pub fn with_resolved_universe(
        mut self,
        instruments: Vec<String>,
        globally_ranked: bool,
    ) -> Result<Self, String> {
        let cap = if globally_ranked { 512 } else { 100 };
        if instruments.is_empty()
            || instruments.len() > cap
            || instruments.iter().any(|id| id.is_empty() || id.len() > 64)
        {
            return Err(format!("universe:RESOLVED_INSTRUMENT_LIMIT:{cap}"));
        }
        let mut distinct = BTreeSet::new();
        if instruments.iter().any(|id| !distinct.insert(id)) {
            return Err("universe:DUPLICATE_INSTRUMENT".into());
        }
        let total_bars = self
            .range
            .expected_bars
            .checked_mul(instruments.len() as u64)
            .ok_or("LIMIT_TOTAL_BARS:OVERFLOW")?;
        if self.sources.iter().any(|source| {
            matches!(
                source,
                Source::Candles | Source::OpenInterest | Source::Funding
            )
        }) && total_bars > 1_000_000
        {
            return Err("LIMIT_TOTAL_BARS:1000000".into());
        }
        if let Some(composition) = &self.composition {
            composition.validate_global_budget(total_bars)?;
        }
        self.universe = instruments;
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
                    other: None,
                },
                SeriesStep {
                    id: "w1".into(),
                    op: SeriesOp::RollingMean,
                    input: "r1".into(),
                    period: Some(2),
                    threshold: None,
                    when: None,
                    other: None,
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
    fn t02_scalar_window_and_contract_currency_mix_fail_before_sources() {
        let scalar: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"wrong","op":"rolling_mean","input":"returns","period":2}],
            "result_steps":["wrong"]
        }))
        .unwrap();
        assert_eq!(
            CheckedComposition::compile(scalar, 100).unwrap_err(),
            "composition.steps[0].input:UNKNOWN_OR_FORWARD_STEP:returns"
        );
        let contracts: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"wrong","op":"add_series","input":"open_interest","other":"close"}],
            "result_steps":["wrong"]
        }))
        .unwrap();
        assert_eq!(
            CheckedComposition::compile(contracts, 100).unwrap_err(),
            "composition.steps[0].input:UNIT_MISMATCH:expected=ratio:actual=contracts"
        );
        let zero: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"return","op":"returns_series","input":"close"},
                {"id":"window","op":"rolling_mean","input":"return","period":1}
            ],"result_steps":["window"]
        }))
        .unwrap();
        let checked = CheckedComposition::compile(zero, 2).unwrap();
        assert_eq!(checked.evaluate_at(&[100.0, 100.0], 0)["window"].0, None);
        assert_eq!(
            checked.evaluate_at(&[100.0, 100.0], 1)["window"].0,
            Some(0.0)
        );
        let loss = checked.evaluate_at(&[100.0, 90.0], 1)["window"].0.unwrap();
        assert!((loss + 0.1).abs() < 1e-12);
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
            other: None,
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
    fn t03_t07_boolean_branches_are_typed_causal_and_unknown_is_not_false() {
        let graph: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"returns","op":"returns_series","input":"close"},
                {"id":"positive","op":"greater_than","input":"returns","threshold":0},
                {"id":"previous","op":"lag","input":"positive","period":1},
                {"id":"both","op":"and","input":"positive","other":"previous"},
                {"id":"either","op":"or","input":"positive","other":"previous"},
                {"id":"event","op":"rising_edge","input":"both"}
            ],
            "result_steps":["both","either","event"]
        }))
        .unwrap();
        let compiled = CheckedComposition::compile(graph.clone(), 6).unwrap();
        assert_eq!(compiled.steps[3].unit, Unit::Boolean);
        assert_eq!(compiled.steps[4].unit, Unit::Boolean);
        let series = [100.0, 110.0, 121.0, 108.9, 119.79, 131.769];
        assert_eq!(compiled.evaluate_at(&series, 0)["both"].0, None);
        assert_eq!(compiled.evaluate_at(&series, 1)["either"].0, None);
        assert_eq!(compiled.evaluate_at(&series, 3)["both"].0, Some(0.0));
        assert_eq!(compiled.evaluate_at(&series, 3)["either"].0, Some(1.0));
        assert_eq!(compiled.evaluate_at(&series, 5)["both"].0, Some(1.0));
        assert_eq!(compiled.evaluate_at(&series, 5)["event"].0, Some(1.0));
        let mut broken = series;
        broken[2] = f64::NAN;
        assert_eq!(compiled.evaluate_at(&broken, 2)["both"].0, None);
        assert_eq!(compiled.evaluate_at(&broken, 2)["either"].0, None);
        let mut future_changed = series;
        future_changed[4] = 1.0;
        future_changed[5] = 2.0;
        assert_eq!(
            compiled.evaluate_at(&series, 3),
            compiled.evaluate_at(&future_changed, 3)
        );
        let mut bad_input = graph.clone();
        bad_input.steps[3].input = "close".into();
        assert_eq!(
            CheckedComposition::compile(bad_input, 6).unwrap_err(),
            "composition.steps[3].input:UNIT_MISMATCH:expected=boolean:actual=price"
        );
        let mut bad_other = graph.clone();
        bad_other.steps[4].other = Some("returns".into());
        assert_eq!(
            CheckedComposition::compile(bad_other, 6).unwrap_err(),
            "composition.steps[4].other:UNIT_MISMATCH:expected=boolean:actual=ratio"
        );
        let mut absent_other = graph.clone();
        absent_other.steps[3].other = None;
        assert_eq!(
            CheckedComposition::compile(absent_other, 6).unwrap_err(),
            "composition.steps[3].other:REQUIRED"
        );
        let mut bad_extra = graph.clone();
        bad_extra.steps[4].period = Some(1);
        assert_eq!(
            CheckedComposition::compile(bad_extra, 6).unwrap_err(),
            "composition.steps[4].period:UNEXPECTED"
        );
        let mut future = graph;
        future.steps.push(SeriesStep {
            id: "outcome".into(),
            op: SeriesOp::ForwardReturnLabel,
            input: "close".into(),
            period: Some(2),
            threshold: None,
            when: Some("event".into()),
            other: None,
        });
        future.steps.push(SeriesStep {
            id: "leak".into(),
            op: SeriesOp::Or,
            input: "positive".into(),
            period: None,
            threshold: None,
            when: None,
            other: Some("outcome".into()),
        });
        assert_eq!(
            CheckedComposition::compile(future, 6).unwrap_err(),
            "composition.steps[7].other:UNIT_MISMATCH:expected=boolean:actual=outcome_ratio"
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
                    other: None,
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
        let step = |id: &str, op, input: &str, period, threshold, when: Option<&str>| SeriesStep {
            id: id.into(),
            op,
            input: input.into(),
            period,
            threshold,
            when: when.map(str::to_owned),
            other: None,
        };
        CompositionRequest {
            version: COMPOSITION_VERSION.into(),
            steps: vec![
                step("return", SeriesOp::ReturnsSeries, "close", None, None, None),
                step(
                    "positive",
                    SeriesOp::GreaterThan,
                    "return",
                    None,
                    Some(0.0),
                    None,
                ),
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
                assert_eq!(
                    full[feature], prefix[feature],
                    "prefix mismatch: {feature} at {at}"
                );
                assert_eq!(
                    full[feature], changed[feature],
                    "future leak: {feature} at {at}"
                );
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
        assert_eq!(
            serde_json::from_value::<CompositionRequest>(encoded).unwrap(),
            old
        );
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
            other: None,
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
    fn t02_t03_t10_plan_owns_defaults_and_binds_global_universe_once() {
        let range = ClosedRange::strict(0, 100 * 60_000, 60_000).unwrap();
        let query = QueryPlan::compile(
            vec![],
            range,
            vec![Operator::Ema, Operator::Returns],
            BTreeMap::new(),
            Vec::new(),
            None,
        )
        .unwrap();
        assert_eq!(query.parameters["ema_period"], 14.0);
        assert_eq!(query.parameters["atr_period"], 14.0);
        assert_eq!(query.parameters["rsi_period"], 14.0);
        assert_eq!(query.warmup_bars, 14);
        let symbols = (0..512)
            .map(|idx| format!("ASSET{idx}-USDT-SWAP"))
            .collect::<Vec<_>>();
        assert_eq!(
            query
                .clone()
                .with_resolved_universe(symbols.clone(), false)
                .unwrap_err(),
            "universe:RESOLVED_INSTRUMENT_LIMIT:100"
        );
        let bound = query
            .clone()
            .with_resolved_universe(symbols.clone(), true)
            .unwrap();
        assert_eq!(bound.universe.len(), 512);
        assert_eq!(bound.operators, query.operators);
        assert_eq!(bound.parameters, query.parameters);
        let mut excessive = symbols.clone();
        excessive.push("EXTRA-USDT-SWAP".into());
        assert_eq!(
            query
                .clone()
                .with_resolved_universe(excessive, true)
                .unwrap_err(),
            "universe:RESOLVED_INSTRUMENT_LIMIT:512"
        );
        let mut duplicate = symbols;
        duplicate[1] = duplicate[0].clone();
        assert_eq!(
            query.with_resolved_universe(duplicate, true).unwrap_err(),
            "universe:DUPLICATE_INSTRUMENT"
        );
    }
    #[test]
    fn t10_resolved_global_market_budget_is_checked_before_job_admission() {
        let names = (0..100)
            .map(|i| format!("ASSET{i}-USDT"))
            .collect::<Vec<_>>();
        let plan = |bars: u64| {
            QueryPlan::compile(
                Vec::new(),
                ClosedRange::strict(0, bars * 60_000, 60_000).unwrap(),
                vec![Operator::Returns],
                BTreeMap::new(),
                Vec::new(),
                None,
            )
            .unwrap()
        };
        assert_eq!(
            plan(10_000)
                .with_resolved_universe(names.clone(), true)
                .unwrap()
                .universe
                .len(),
            100
        );
        assert_eq!(
            plan(10_001)
                .with_resolved_universe(names.clone(), true)
                .unwrap_err(),
            "LIMIT_TOTAL_BARS:1000000"
        );
        let composition: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":(0..9).map(|i|serde_json::json!({
                "id":format!("r{i}"),"op":"returns_series","input":"close"
            })).collect::<Vec<_>>(),
            "result_steps":["r0"]
        }))
        .unwrap();
        let checked = plan(10_000).with_composition(Some(composition)).unwrap();
        assert_eq!(
            checked.with_resolved_universe(names, true).unwrap_err(),
            "composition:CELLS_LIMIT"
        );
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

#[cfg(test)]
mod t15_causal_window_extrema {
    use super::*;

    fn plan() -> CheckedComposition {
        CheckedComposition::compile(
            serde_json::from_value(serde_json::json!({
                "version":"okx.market.composition/v1",
                "steps":[
                    {"id":"upper","op":"rolling_max","input":"close","period":3},
                    {"id":"lower","op":"rolling_min","input":"close","period":3},
                    {"id":"gain","op":"returns_series","input":"close"},
                    {"id":"low_gain","op":"rolling_min","input":"gain","period":2}
                ],
                "result_steps":["upper","lower","low_gain"]
            }))
            .unwrap(),
            100,
        )
        .unwrap()
    }

    #[test]
    fn independent_oracle_handles_gaps_zero_negative_ratios_and_future_tail() {
        let plan = plan();
        let closes = [100., 105., 100., 101., f64::NAN, 99., 102., 98., 104.];
        for index in 0..closes.len() {
            let values = plan.evaluate_at(&closes, index);
            for (name, max) in [("upper", true), ("lower", false)] {
                let oracle = if index < 2 {
                    None
                } else {
                    let window = &closes[index - 2..=index];
                    window.iter().all(|v| v.is_finite()).then(|| {
                        window.iter().copied().fold(
                            if max {
                                f64::NEG_INFINITY
                            } else {
                                f64::INFINITY
                            },
                            |acc, val| if max { acc.max(val) } else { acc.min(val) },
                        )
                    })
                };
                assert_eq!(values[name].0, oracle, "{name}, index={index}");
                assert_eq!(values[name].1, Unit::Price);
            }
            assert_eq!(values["low_gain"].1, Unit::Ratio);
        }
        let first = plan.evaluate_at(&closes, 3);
        let mut future_changed = closes;
        future_changed[4..].copy_from_slice(&[5000., 1., 9000., 2., 8000.]);
        assert_eq!(first, plan.evaluate_at(&future_changed, 3));
        assert!((plan.evaluate_at(&closes, 3)["low_gain"].0.unwrap() + 5.0 / 105.0).abs() < 1e-12);
    }

    #[test]
    fn invalid_extrema_edges_fail_before_source_io_with_step_path() {
        let mut request = serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[{"id":"window","op":"rolling_max","input":"close","period":3}],
            "result_steps":["window"]
        });
        let checked = |value| {
            CheckedComposition::compile(serde_json::from_value(value).unwrap(), 100).unwrap_err()
        };
        request["steps"][0]["period"] = serde_json::json!(0);
        assert_eq!(
            checked(request.clone()),
            "composition.steps[0].period:INVALID"
        );
        request["steps"][0]["period"] = serde_json::json!(1001);
        assert_eq!(
            checked(request.clone()),
            "composition.steps[0].period:INVALID"
        );
        request["steps"][0]["period"] = serde_json::json!(2);
        request["steps"][0]["threshold"] = serde_json::json!(1.0);
        assert_eq!(
            checked(request),
            "composition.steps[0].threshold:UNEXPECTED"
        );
    }
}

#[cfg(test)]
mod t02_t06_multisource_regression {
    use super::*;

    #[test]
    fn t02_t06_sparse_realized_funding_joins_price_returns_by_event_time() {
        let spec: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"net_return","op":"subtract_series","input":"price_return","other":"funding_rate"},
                {"id":"signal","op":"greater_than","input":"net_return","threshold":0.0}
            ],"result_steps":["net_return","signal"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(spec.clone(), 4).unwrap();
        assert_eq!(plan.steps[1].unit, Unit::Ratio);
        assert_eq!(
            plan.result_sources()["net_return"],
            BTreeSet::from([Source::Candles, Source::Funding])
        );
        assert_eq!(
            plan.result_sources()["signal"],
            BTreeSet::from([Source::Candles, Source::Funding])
        );
        let prices = [100., 110., 121., 133.1];
        let events = [None, Some(0.001), None, Some(-0.002)];
        let at_event = plan
            .evaluate_with_sources_diagnostics(&prices[..2], None, Some(&events[..2]))
            .unwrap();
        assert!((at_event.0["net_return"].0.unwrap() - 0.099).abs() < 1e-12);
        let absent = plan
            .evaluate_with_sources_diagnostics(&prices[..3], None, Some(&events[..3]))
            .unwrap();
        assert!(absent.0["net_return"].0.is_none());
        assert_eq!(absent.1["net_return"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(absent.1["net_return"].origin_step, "funding_rate");
        let latest = plan
            .evaluate_with_sources_diagnostics(&prices, None, Some(&events))
            .unwrap();
        assert!((latest.0["net_return"].0.unwrap() - 0.102).abs() < 1e-12);
        assert_eq!(latest.0["signal"].0, Some(1.0));
        assert_eq!(
            plan.evaluate_with_sources_diagnostics(&prices, None, Some(&events[..3]))
                .unwrap_err(),
            "composition.sources:ALIGNMENT_LENGTH_MISMATCH"
        );
        assert_eq!(
            plan.evaluate_with_sources_diagnostics(
                &prices,
                None,
                Some(&[None, Some(f64::NAN), None, None])
            )
            .unwrap_err(),
            "composition.sources.funding_rate:NONFINITE_RATE"
        );
        let mut invalid = spec;
        invalid.steps[0].id = "funding_rate".into();
        assert_eq!(
            CheckedComposition::compile(invalid, 4).unwrap_err(),
            "composition.steps[0].id:DUPLICATE"
        );
    }

    #[test]
    fn t02_t06_funding_as_first_operand_and_oi_joint_budget() {
        let spec: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"funding_spread","op":"subtract_series","input":"funding_rate","other":"price_return"},
                {"id":"combined","op":"add_series","input":"funding_spread","other":"oi_return"}
            ],"result_steps":["combined"]
        })).unwrap();
        let graph = CheckedComposition::compile(spec, 100).unwrap();
        assert_eq!(
            graph.sources(),
            BTreeSet::from([Source::Candles, Source::OpenInterest, Source::Funding])
        );
        assert_eq!(source_buffer_count(&graph.steps), 3);
        let prices = [100., 110.];
        let oi = [Some(10.), Some(12.)];
        let funding = [None, Some(0.01)];
        let result = graph
            .evaluate_with_sources_diagnostics(&prices, Some(&oi), Some(&funding))
            .unwrap();
        assert!((result.0["combined"].0.unwrap() - 0.11).abs() < 1e-12);
        assert_eq!(
            graph.validate_global_budget(8_000_000 / 7 + 1).unwrap_err(),
            "composition:SOURCE_CELLS_LIMIT"
        );
    }

    #[test]
    fn t02_contract_count_and_type_checked_source_dependencies() {
        assert_eq!(input("open_interest").unwrap().unit, Unit::Contracts);
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"combined","op":"add_series","input":"price_return","other":"oi_return"}
            ],
            "result_steps":["price_return","oi_return","combined"]
        }))
        .unwrap();
        let checked = CheckedComposition::compile(request.clone(), 4).unwrap();
        assert_eq!(
            checked.result_sources()["combined"],
            BTreeSet::from([Source::Candles, Source::OpenInterest,])
        );
        assert_eq!(
            checked.result_sources()["oi_return"],
            BTreeSet::from([Source::OpenInterest])
        );
        let close = [100.0, 105.0, 110.0, 100.0];
        let oi = [Some(10.0), Some(12.0), None, Some(12.0)];
        let at_one = checked
            .evaluate_with_open_interest(&close[..2], &oi[..2])
            .unwrap();
        assert!((at_one["combined"].0.unwrap() - 0.25).abs() < 1e-12);
        assert!(
            checked.evaluate_with_open_interest(&close, &oi).unwrap()["combined"]
                .0
                .is_none()
        );
        assert_eq!(
            checked
                .evaluate_with_open_interest(&close, &oi[..3])
                .unwrap_err(),
            "composition.sources:ALIGNMENT_LENGTH_MISMATCH"
        );
        let mut invalid = request.clone();
        invalid.steps[2].other = Some("close".into());
        assert_eq!(
            CheckedComposition::compile(invalid, 4).unwrap_err(),
            "composition.steps[2].other:UNIT_MISMATCH:expected=ratio:actual=price"
        );
        let mut forward = request;
        forward.steps[2].other = Some("future".into());
        assert!(
            CheckedComposition::compile(forward, 4)
                .unwrap_err()
                .contains("other:UNKNOWN_OR_FORWARD_STEP")
        );
    }

    #[test]
    fn t02_t05_t06_cross_source_divergence_and_division_preserve_zero_and_missing() {
        let spec: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"difference","op":"subtract_series","input":"price_return","other":"oi_return"},
                {"id":"relative","op":"divide_series","input":"price_return","other":"oi_return"}
            ],
            "result_steps":["difference","relative"]
        }))
        .unwrap();
        let compiled = CheckedComposition::compile(spec.clone(), 3).unwrap();
        assert_eq!(compiled.steps[2].unit, Unit::Ratio);
        assert_eq!(compiled.steps[3].unit, Unit::Ratio);
        assert_eq!(
            compiled.result_sources()["difference"],
            BTreeSet::from([Source::Candles, Source::OpenInterest])
        );
        let prices = [100., 110., 99.];
        let oi = [Some(10.), Some(12.), Some(11.)];
        let first = compiled
            .evaluate_with_open_interest(&prices[..2], &oi[..2])
            .unwrap();
        // Independent fractions: (1/10 - 1/5) = -1/10,
        // and (1/10)/(1/5) = 1/2.
        assert!((first["difference"].0.unwrap() + 0.1).abs() < 1e-12);
        assert!((first["relative"].0.unwrap() - 0.5).abs() < 1e-12);
        let last = compiled.evaluate_with_open_interest(&prices, &oi).unwrap();
        // -1/10 - (-1/12) = -1/60; (-1/10)/(-1/12) = 6/5.
        assert!((last["difference"].0.unwrap() + 1. / 60.).abs() < 1e-12);
        assert!((last["relative"].0.unwrap() - 1.2).abs() < 1e-12);
        assert_eq!(
            compiled
                .evaluate_with_open_interest(&[100., 100.], &[Some(10.), Some(11.)])
                .unwrap()["relative"]
                .0,
            Some(0.0)
        );
        let zero_denominator = compiled
            .evaluate_with_open_interest(&[100., 110.], &[Some(10.), Some(10.)])
            .unwrap();
        assert!((zero_denominator["difference"].0.unwrap() - 0.1).abs() < 1e-12);
        assert_eq!(zero_denominator["relative"].0, None);
        let missing = compiled
            .evaluate_with_open_interest(&[100., 110.], &[Some(10.), None])
            .unwrap();
        assert_eq!(missing["difference"].0, None);
        assert_eq!(missing["relative"].0, None);
        let mut wrong_first = spec.clone();
        wrong_first.steps[2].input = "open_interest".into();
        assert_eq!(
            CheckedComposition::compile(wrong_first, 3).unwrap_err(),
            "composition.steps[2].input:UNIT_MISMATCH:expected=ratio:actual=contracts"
        );
        let mut wrong_second = spec.clone();
        wrong_second.steps[3].other = Some("close".into());
        assert_eq!(
            CheckedComposition::compile(wrong_second, 3).unwrap_err(),
            "composition.steps[3].other:UNIT_MISMATCH:expected=ratio:actual=price"
        );
        let mut unexpected = spec.clone();
        unexpected.steps[3].period = Some(2);
        assert_eq!(
            CheckedComposition::compile(unexpected, 3).unwrap_err(),
            "composition.steps[3].period:UNEXPECTED"
        );
        let mut forward = spec;
        forward.steps[2].other = Some("relative".into());
        assert_eq!(
            CheckedComposition::compile(forward, 3).unwrap_err(),
            "composition.steps[2].other:UNKNOWN_OR_FORWARD_STEP:relative"
        );
    }

    #[test]
    fn t05_t06_null_causes_distinguish_zero_missing_warmup_and_nonfinite_through_branches() {
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"relative","op":"divide_series","input":"price_return","other":"oi_return"},
                {"id":"delayed","op":"lag","input":"relative","period":1},
                {"id":"smoothed","op":"rolling_mean","input":"delayed","period":2}
            ],"result_steps":["price_return","oi_return","relative","delayed","smoothed"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(request, 4).unwrap();
        let prices = [100., 110., 121., 133.1];
        let flat_oi = [Some(10.), Some(10.), Some(10.), Some(10.)];
        let (values, causes) = plan
            .evaluate_with_open_interest_diagnostics(&prices, &flat_oi)
            .unwrap();
        assert_eq!(
            values,
            plan.evaluate_with_open_interest(&prices, &flat_oi).unwrap()
        );
        assert!((values["price_return"].0.unwrap() - 0.1).abs() < 1e-12);
        assert_eq!(values["oi_return"].0, Some(0.0));
        for id in ["relative", "delayed", "smoothed"] {
            assert_eq!(values[id].0, None);
            assert_eq!(
                causes[id],
                CompositionNullCause {
                    reason: "DIVISION_BY_ZERO",
                    origin_step: "relative".into()
                }
            );
        }
        assert!(!causes.contains_key("price_return"));
        let (_, warmup) = plan
            .evaluate_with_open_interest_diagnostics(&prices[..1], &flat_oi[..1])
            .unwrap();
        assert_eq!(warmup["relative"].reason, "WARMUP");
        assert_eq!(warmup["relative"].origin_step, "price_return");
        let mut incomplete_oi = flat_oi;
        incomplete_oi[3] = None;
        let (_, missing) = plan
            .evaluate_with_open_interest_diagnostics(&prices, &incomplete_oi)
            .unwrap();
        assert_eq!(missing["relative"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(missing["relative"].origin_step, "open_interest");

        let standalone: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"ret","op":"returns_series","input":"close"}],
            "result_steps":["ret"]
        }))
        .unwrap();
        let compiled = CheckedComposition::compile(standalone, 2).unwrap();
        let (_, invalid_domain) = compiled.evaluate_with_diagnostics(&[100., 0.]);
        assert_eq!(invalid_domain["ret"].reason, "NON_POSITIVE_RETURN_INPUT");
        let (_, overflow) = compiled.evaluate_with_diagnostics(&[f64::MIN_POSITIVE, f64::MAX]);
        assert_eq!(overflow["ret"].reason, "NON_FINITE_RESULT");

        let label: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"condition","op":"greater_than","input":"close","threshold":105.},
                {"id":"event","op":"rising_edge","input":"condition"},
                {"id":"label","op":"forward_return_label","input":"close","when":"event","period":1}
            ],"result_steps":["label"]
        }))
        .unwrap();
        let label_plan = CheckedComposition::compile(label, 2).unwrap();
        let (_, unconfirmed) = label_plan.evaluate_with_diagnostics(&[100., 110.]);
        assert_eq!(unconfirmed["label"].reason, "OUTCOME_UNCONFIRMED");
    }

    #[test]
    fn t02_t06_rolling_pearson_requires_thirty_exact_pairs_and_preserves_coefficient_type() {
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"joined","op":"rolling_pearson","input":"price_return","other":"oi_return","period":30},
                {"id":"strong","op":"greater_than","input":"joined","threshold":0.9}
            ],"result_steps":["joined","strong"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(request.clone(), 32).unwrap();
        assert_eq!(plan.steps[2].unit, Unit::Coefficient);
        assert_eq!(plan.steps[3].unit, Unit::Boolean);
        assert_eq!(
            plan.result_sources()["joined"],
            BTreeSet::from([Source::Candles, Source::OpenInterest])
        );
        let mut prices = vec![100.0];
        let mut oi = vec![Some(10.0)];
        for i in 1..32 {
            let change = (((i * 7) % 11) as f64 - 5.0) * 0.0005;
            prices.push(prices[i - 1] * (1.0 + change));
            oi.push(Some(oi[i - 1].unwrap() * (1.0 + 2.0 * change)));
        }
        let (early, warmup) = plan
            .evaluate_with_open_interest_diagnostics(&prices[..30], &oi[..30])
            .unwrap();
        assert_eq!(early["joined"].0, None);
        assert_eq!(warmup["joined"].reason, "WARMUP");
        assert_eq!(warmup["joined"].origin_step, "price_return");
        let full = plan
            .evaluate_with_open_interest(&prices[..31], &oi[..31])
            .unwrap();
        assert!((full["joined"].0.unwrap() - 1.0).abs() < 1e-8);
        assert_eq!(full["strong"].0, Some(1.0));
        let later = plan.evaluate_with_open_interest(&prices, &oi).unwrap();
        assert!((later["joined"].0.unwrap() - 1.0).abs() < 1e-8);

        let mut missing = oi.clone();
        missing[15] = None;
        let (partial, causes) = plan
            .evaluate_with_open_interest_diagnostics(&prices, &missing)
            .unwrap();
        assert_eq!(partial["joined"].0, None);
        assert_eq!(causes["joined"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(causes["joined"].origin_step, "open_interest");
        let (flat, causes) = plan
            .evaluate_with_open_interest_diagnostics(&vec![100.0; 31], &oi[..31])
            .unwrap();
        assert_eq!(flat["joined"].0, None);
        assert_eq!(causes["joined"].reason, "ZERO_VARIANCE");

        let mut invalid = request.clone();
        invalid.steps[2].input = "close".into();
        assert_eq!(
            CheckedComposition::compile(invalid, 32).unwrap_err(),
            "composition.steps[2].input:UNIT_MISMATCH:expected=ratio:actual=price"
        );
        let mut invalid = request.clone();
        invalid.steps[2].other = Some("open_interest".into());
        assert_eq!(
            CheckedComposition::compile(invalid, 32).unwrap_err(),
            "composition.steps[2].other:UNIT_MISMATCH:expected=ratio:actual=contracts"
        );
        for period in [None, Some(29), Some(1001)] {
            let mut invalid = request.clone();
            invalid.steps[2].period = period;
            assert_eq!(
                CheckedComposition::compile(invalid, 32).unwrap_err(),
                "composition.steps[2].period:INVALID:30..1000"
            );
        }
        let mut invalid = request;
        invalid.steps[2].other = None;
        assert_eq!(
            CheckedComposition::compile(invalid, 32).unwrap_err(),
            "composition.steps[2].other:REQUIRED"
        );
    }

    #[test]
    fn t10_rolling_pearson_is_bounded_linear_work_and_recovers_after_old_gap() {
        let compiled: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"returns","op":"returns_series","input":"close"},
                {"id":"compare","op":"rolling_pearson","input":"returns","other":"returns","period":1000}
            ],"result_steps":["compare"]
        }))
        .unwrap();
        assert!(CheckedComposition::compile(compiled, 1_000_000).is_ok());
        let left = (0..100_000)
            .map(|i| Some(((i * 7) % 191) as f64 * 0.000_001))
            .collect::<Vec<_>>();
        let right = left.iter().map(|v| v.map(|x| 2.0 * x)).collect::<Vec<_>>();
        let result = rolling_pearson(&left, &right, 1000);
        assert!(result[..999].iter().all(Option::is_none));
        assert!((result[999].unwrap() - 1.0).abs() < 1e-10);
        assert!((result[99_999].unwrap() - 1.0).abs() < 1e-7);
        let mut interrupted = left;
        interrupted[500] = None;
        let after = rolling_pearson(&interrupted, &right, 1000);
        assert!(after[999].is_none());
        assert!((after[99_999].unwrap() - 1.0).abs() < 1e-7);
    }

    #[test]
    fn t10_sliding_mean_cost_is_linear_and_all_other_work_still_bounded() {
        let mut request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[{"id":"window","op":"rolling_mean","input":"close","period":1000}],
            "result_steps":["window"]
        }))
        .unwrap();
        assert!(CheckedComposition::compile(request.clone(), 1_000_000).is_ok());
        request.steps[0].op = SeriesOp::RollingMax;
        assert!(CheckedComposition::compile(request, 1_000_000).is_ok());
    }
    #[test]
    fn t05_t10_sliding_mean_agrees_with_independent_window_oracle_and_propagates_gaps() {
        let plan: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[{"id":"window","op":"rolling_mean","input":"close","period":1000}],
            "result_steps":["window"]
        }))
        .unwrap();
        let compiled = CheckedComposition::compile(plan, 10_000).unwrap();
        let numbers = (0..10_000)
            .map(|i| 10_000.0 + (i % 97) as f64)
            .collect::<Vec<_>>();
        for at in [998, 999, 1000, 4999, 9999] {
            let value = compiled.evaluate_at(&numbers, at)["window"].0;
            if at < 999 {
                assert!(value.is_none());
            } else {
                let oracle = numbers[at + 1 - 1000..=at].iter().sum::<f64>() / 1000.0;
                assert!((value.unwrap() - oracle).abs() < 1e-9);
            }
        }
        let mut with_gap = numbers;
        with_gap[4999] = f64::NAN;
        assert!(compiled.evaluate_at(&with_gap, 5000)["window"].0.is_none());
        assert!(compiled.evaluate_at(&with_gap, 6000)["window"].0.is_some());
        let extremes: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":"okx.market.composition/v1",
            "steps":[{"id":"window","op":"rolling_mean","input":"close","period":2}],
            "result_steps":["window"]
        }))
        .unwrap();
        let extremes = CheckedComposition::compile(extremes, 4).unwrap();
        let near_max = f64::MAX / 2.0;
        let series = [near_max, near_max, 2.0, 4.0];
        assert_eq!(extremes.evaluate_at(&series, 1)["window"].0, Some(near_max));
        assert_eq!(extremes.evaluate_at(&series, 3)["window"].0, Some(3.0));
    }
}
