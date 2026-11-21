//! A bounded, incremental interpreter for a compiled, close-only causal graph.
//!
//! This module is pure: event-time admission, persistence, cancellation and
//! trading authority remain with the caller. Restoring means replaying closed
//! observations once, **without** replaying previously emitted orders.
use crate::query_plan::{CheckedComposition, CompositionValues, SeriesOp};
use std::collections::{BTreeMap, VecDeque};

/// One finalised observation. The timestamp identifies the closed candle,
/// never the time at which a late external source happened to arrive.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClosedBar {
    pub timestamp_ms: i64,
    pub close: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
enum Input {
    Close,
    Step(usize),
}

impl Input {
    fn value(self, close: Option<f64>, columns: &[Option<f64>]) -> Option<f64> {
        match self {
            Self::Close => close,
            Self::Step(index) => columns[index],
        }
    }
}

#[derive(Debug)]
enum Memory {
    Stateless,
    Previous(Option<f64>),
    Mean(RollingMean),
    Extremum(MonotoneWindow),
    Range(MonotoneWindow, MonotoneWindow),
    Lag(VecDeque<Option<f64>>),
}

#[derive(Debug)]
struct CompiledNode {
    input: Input,
    other: Option<Input>,
    memory: Memory,
}

/// A deterministic, finite state machine: O(steps) work per bar amortized,
/// O(sum of declared window lengths) state, and no retained price history.
/// Unsupported operators and non-close sources fail at construction.
#[derive(Debug)]
pub struct CausalGraphStream {
    plan: CheckedComposition,
    nodes: Vec<CompiledNode>,
    projection: Vec<usize>,
    capacity: u64,
    seen: u64,
    last_timestamp_ms: Option<i64>,
    /// A deterministic operation counter for complexity regression tests.
    operations: u64,
}

impl CausalGraphStream {
    pub fn new(plan: &CheckedComposition) -> Result<Self, String> {
        let mut bindings = BTreeMap::<&str, usize>::new();
        let mut nodes = Vec::with_capacity(plan.steps().len());
        for (index, checked) in plan.steps().iter().enumerate() {
            let step = &checked.step;
            let input = resolve_input(&bindings, &step.input)?;
            let other = step
                .other
                .as_deref()
                .map(|name| resolve_input(&bindings, name))
                .transpose()?;
            let memory = match step.op {
                SeriesOp::ForwardReturnLabel => {
                    return Err("composition.stream:NON_CAUSAL_OPERATOR".into());
                }
                SeriesOp::RollingPearson | SeriesOp::RollingStddev | SeriesOp::RollingZscore => {
                    return Err("composition.stream:OPERATOR_NOT_INCREMENTAL".into());
                }
                SeriesOp::RollingMean => Memory::Mean(RollingMean::new(
                    step.period.expect("checked period") as usize,
                )),
                SeriesOp::RollingMax | SeriesOp::RollingMin => {
                    Memory::Extremum(MonotoneWindow::new(
                        step.period.expect("checked period") as usize,
                        step.op == SeriesOp::RollingMax,
                    ))
                }
                SeriesOp::RollingRange => {
                    let period = step.period.expect("checked period") as usize;
                    Memory::Range(
                        MonotoneWindow::new(period, true),
                        MonotoneWindow::new(period, false),
                    )
                }
                SeriesOp::Lag => Memory::Lag(VecDeque::with_capacity(
                    step.period.expect("checked period") as usize + 1,
                )),
                SeriesOp::ReturnsSeries | SeriesOp::RisingEdge | SeriesOp::FallingEdge => {
                    Memory::Previous(None)
                }
                _ => Memory::Stateless,
            };
            nodes.push(CompiledNode {
                input,
                other,
                memory,
            });
            bindings.insert(&step.id, index);
        }
        let projection = plan
            .result_steps()
            .iter()
            .map(|id| bindings.get(id.as_str()).copied().expect("checked result"))
            .collect();
        Ok(Self {
            plan: plan.clone(),
            nodes,
            projection,
            capacity: plan.estimated_cells() / plan.steps().len() as u64,
            seen: 0,
            last_timestamp_ms: None,
            operations: 0,
        })
    }

    /// The first unseen closed bar is admitted exactly once; invalid or
    /// out-of-order bars leave all state unchanged. At the compiled budget
    /// boundary the caller must compile a new version/budget and restore.
    pub fn on_closed_bar(&mut self, bar: ClosedBar) -> Result<CompositionValues, String> {
        if self.seen >= self.capacity {
            return Err("composition.stream:COMPILED_CAPACITY_EXCEEDED".into());
        }
        if self
            .last_timestamp_ms
            .is_some_and(|last| bar.timestamp_ms <= last)
        {
            return Err("composition.stream:CLOSED_BAR_NOT_MONOTONIC".into());
        }
        let close = bar.close.filter(|value| value.is_finite());
        let mut columns = vec![None; self.nodes.len()];
        for (index, node) in self.nodes.iter_mut().enumerate() {
            let step = &self.plan.steps()[index].step;
            let input = node.input.value(close, &columns);
            let other = node
                .other
                .and_then(|binding| binding.value(close, &columns));
            columns[index] = match &mut node.memory {
                Memory::Mean(window) => window.push(input),
                Memory::Extremum(window) => window.push(input),
                Memory::Range(maximum, minimum) => {
                    match (maximum.push(input), minimum.push(input)) {
                        (Some(max), Some(min)) => {
                            let width = max - min;
                            width.is_finite().then_some(width)
                        }
                        _ => None,
                    }
                }
                Memory::Lag(history) => {
                    history.push_back(input);
                    if history.len() > step.period.expect("checked lag") as usize {
                        history.pop_front().flatten()
                    } else {
                        None
                    }
                }
                Memory::Previous(previous) => {
                    let result = scalar_value(step.op, input, other, *previous, step.threshold);
                    *previous = input;
                    result
                }
                Memory::Stateless => scalar_value(step.op, input, other, None, step.threshold),
            };
        }
        self.seen += 1;
        self.last_timestamp_ms = Some(bar.timestamp_ms);
        self.operations += self.nodes.len() as u64;
        Ok(self
            .projection
            .iter()
            .map(|&index| {
                let checked = &self.plan.steps()[index];
                (checked.step.id.clone(), (columns[index], checked.unit))
            })
            .collect())
    }

    /// Rebuilds pure mathematical state from an exact closed-bar prefix in
    /// linear work. Callers must persist/restore their order emission cursor
    /// separately; this method never issues an order or repeats an effect.
    pub fn restore_from_history(
        plan: &CheckedComposition,
        bars: impl IntoIterator<Item = ClosedBar>,
    ) -> Result<Self, String> {
        let mut stream = Self::new(plan)?;
        for bar in bars {
            stream.on_closed_bar(bar)?;
        }
        Ok(stream)
    }

    pub fn processed_bars(&self) -> u64 {
        self.seen
    }

    pub fn last_timestamp_ms(&self) -> Option<i64> {
        self.last_timestamp_ms
    }

    pub fn operation_count(&self) -> u64 {
        self.operations
    }
}

fn resolve_input(bindings: &BTreeMap<&str, usize>, name: &str) -> Result<Input, String> {
    if name == "close" {
        Ok(Input::Close)
    } else {
        bindings
            .get(name)
            .copied()
            .map(Input::Step)
            .ok_or_else(|| format!("composition.stream:SOURCE_NOT_SUPPORTED:{name}"))
    }
}

/// One pointwise oracle shared by the batch and streaming interpreters.
/// Windowed functions use the state machines below in both paths.
pub(crate) fn scalar_value(
    op: SeriesOp,
    input: Option<f64>,
    other: Option<f64>,
    previous: Option<f64>,
    threshold: Option<f64>,
) -> Option<f64> {
    match op {
        SeriesOp::ReturnsSeries => match (previous, input) {
            (Some(a), Some(b)) if a > 0.0 && b > 0.0 => {
                let result = b / a - 1.0;
                result.is_finite().then_some(result)
            }
            _ => None,
        },
        SeriesOp::RisingEdge | SeriesOp::FallingEdge => match (previous, input) {
            (Some(a), Some(b)) => Some(f64::from(if op == SeriesOp::RisingEdge {
                a == 0.0 && b == 1.0
            } else {
                a == 1.0 && b == 0.0
            })),
            _ => None,
        },
        SeriesOp::GreaterThan | SeriesOp::LessThan => input.map(|value| {
            f64::from(if op == SeriesOp::GreaterThan {
                value > threshold.expect("checked threshold")
            } else {
                value < threshold.expect("checked threshold")
            })
        }),
        SeriesOp::AddSeries
        | SeriesOp::SubtractSeries
        | SeriesOp::DivideSeries
        | SeriesOp::GreaterThanSeries
        | SeriesOp::LessThanSeries
        | SeriesOp::And
        | SeriesOp::Or => match (input, other) {
            (Some(a), Some(b)) => {
                let result = match op {
                    SeriesOp::AddSeries => a + b,
                    SeriesOp::SubtractSeries => a - b,
                    SeriesOp::DivideSeries if b != 0.0 => a / b,
                    SeriesOp::DivideSeries => return None,
                    SeriesOp::GreaterThanSeries => f64::from(a > b),
                    SeriesOp::LessThanSeries => f64::from(a < b),
                    SeriesOp::And => f64::from(a == 1.0 && b == 1.0),
                    SeriesOp::Or => f64::from(a == 1.0 || b == 1.0),
                    _ => unreachable!("pointwise binary"),
                };
                result.is_finite().then_some(result)
            }
            _ => None,
        },
        _ => unreachable!("stream state or batch interpreter handles {op:?}"),
    }
}

#[derive(Debug)]
pub(crate) struct RollingMean {
    period: usize,
    history: VecDeque<Option<f64>>,
    mean_sum: f64,
    correction: f64,
    missing: usize,
}

impl RollingMean {
    pub(crate) fn new(period: usize) -> Self {
        Self {
            period,
            history: VecDeque::with_capacity(period),
            mean_sum: 0.0,
            correction: 0.0,
            missing: 0,
        }
    }

    pub(crate) fn push(&mut self, current: Option<f64>) -> Option<f64> {
        let divisor = self.period as f64;
        if self.history.len() == self.period {
            match self.history.pop_front().expect("full window") {
                Some(old) => crate::query_plan::compensated_add(
                    &mut self.mean_sum,
                    &mut self.correction,
                    -old / divisor,
                ),
                None => self.missing -= 1,
            }
        }
        match current {
            Some(value) => crate::query_plan::compensated_add(
                &mut self.mean_sum,
                &mut self.correction,
                value / divisor,
            ),
            None => self.missing += 1,
        }
        self.history.push_back(current);
        if self.history.len() == self.period && self.missing == 0 {
            let mean = self.mean_sum + self.correction;
            mean.is_finite().then_some(mean)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(crate) struct MonotoneWindow {
    period: usize,
    maximum: bool,
    deque: VecDeque<(usize, f64)>,
    gaps: VecDeque<bool>,
    missing: usize,
    next_index: usize,
}

impl MonotoneWindow {
    pub(crate) fn new(period: usize, maximum: bool) -> Self {
        Self {
            period,
            maximum,
            deque: VecDeque::with_capacity(period),
            gaps: VecDeque::with_capacity(period),
            missing: 0,
            next_index: 0,
        }
    }

    pub(crate) fn push(&mut self, input: Option<f64>) -> Option<f64> {
        let index = self.next_index;
        self.next_index += 1;
        if self.gaps.len() == self.period && self.gaps.pop_front().expect("full gap window") {
            self.missing -= 1;
        }
        let value = input.filter(|value| value.is_finite());
        self.gaps.push_back(value.is_none());
        if let Some(value) = value {
            while self.deque.back().is_some_and(|(_, prior)| {
                if self.maximum {
                    *prior <= value
                } else {
                    *prior >= value
                }
            }) {
                self.deque.pop_back();
            }
            self.deque.push_back((index, value));
        } else {
            self.missing += 1;
        }
        while self
            .deque
            .front()
            .is_some_and(|(old, _)| old.saturating_add(self.period) <= index)
        {
            self.deque.pop_front();
        }
        if self.gaps.len() == self.period && self.missing == 0 {
            self.deque.front().map(|(_, value)| *value)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_plan::{COMPOSITION_VERSION, CompositionRequest};
    use serde_json::json;

    fn graph(bars: u64) -> CheckedComposition {
        let request: CompositionRequest = serde_json::from_value(json!({
            "version": COMPOSITION_VERSION,
            "steps": [
                {"id":"returns", "op":"returns_series", "input":"close"},
                {"id":"average", "op":"rolling_mean", "input":"close", "period":13},
                {"id":"maximum", "op":"rolling_max", "input":"close", "period":7},
                {"id":"minimum", "op":"rolling_min", "input":"close", "period":7},
                {"id":"range", "op":"rolling_range", "input":"close", "period":9},
                {"id":"previous", "op":"lag", "input":"close", "period":3},
                {"id":"positive", "op":"greater_than", "input":"returns", "threshold":0.0},
                {"id":"negative", "op":"less_than", "input":"returns", "threshold":0.0},
                {"id":"and_both", "op":"and", "input":"positive", "other":"negative"},
                {"id":"or_either", "op":"or", "input":"positive", "other":"negative"},
                {"id":"edge_up", "op":"rising_edge", "input":"positive"},
                {"id":"edge_down", "op":"falling_edge", "input":"negative"},
                {"id":"return_lag", "op":"lag", "input":"returns", "period":2},
                {"id":"add", "op":"add_series", "input":"returns", "other":"return_lag"},
                {"id":"sub", "op":"subtract_series", "input":"returns", "other":"return_lag"},
                {"id":"divide", "op":"divide_series", "input":"returns", "other":"return_lag"},
                {"id":"above_average", "op":"greater_than_series", "input":"close", "other":"average"},
                {"id":"below_previous", "op":"less_than_series", "input":"close", "other":"previous"}
            ],
            "result_steps": [
                "average", "maximum", "minimum", "range", "previous", "and_both",
                "or_either", "edge_up", "edge_down", "add", "sub", "divide",
                "above_average", "below_previous"
            ]
        }))
        .unwrap();
        CheckedComposition::compile(request, bars).unwrap()
    }

    fn prices(length: usize) -> Vec<f64> {
        (0..length)
            .map(|index| match index % 37 {
                0 => f64::NAN,
                1 => 0.0,
                2 => -5.0,
                3 => f64::MAX,
                _ => 100.0 + ((index * 31) % 47) as f64,
            })
            .collect()
    }

    #[test]
    fn streaming_replay_and_future_changes_match_exact_batch_projections() {
        let plan = graph(512);
        let closes = prices(257);
        let mut runtime = CausalGraphStream::new(&plan).unwrap();
        let mut historical = Vec::new();
        for (index, &price) in closes.iter().enumerate() {
            let bar = ClosedBar {
                timestamp_ms: 1000 + index as i64 * 60_000,
                close: Some(price),
            };
            let result = runtime.on_closed_bar(bar).unwrap();
            assert_eq!(
                result,
                plan.evaluate_at(&closes[..=index], index),
                "bar={index}"
            );
            historical.push(bar);
        }
        assert_eq!(runtime.processed_bars(), 257);
        assert_eq!(runtime.operation_count(), 257 * plan.steps().len() as u64);
        let restored = CausalGraphStream::restore_from_history(&plan, historical).unwrap();
        assert_eq!(restored.processed_bars(), runtime.processed_bars());
        assert_eq!(restored.operation_count(), runtime.operation_count());
        assert_eq!(restored.last_timestamp_ms(), runtime.last_timestamp_ms());

        let mut altered = closes.clone();
        altered[130..].fill(1_000_000.0);
        let mut altered_stream = CausalGraphStream::new(&plan).unwrap();
        for (index, &price) in altered.iter().enumerate() {
            let current = altered_stream
                .on_closed_bar(ClosedBar {
                    timestamp_ms: index as i64,
                    close: Some(price),
                })
                .unwrap();
            if index < 130 {
                assert_eq!(current, plan.evaluate_at(&closes, index));
            }
        }
    }

    #[test]
    fn operation_count_scales_linearly_and_windows_remain_bounded() {
        let plan = graph(2048);
        let mut counts = Vec::new();
        for length in [512, 1024, 2048] {
            let mut runner = CausalGraphStream::new(&plan).unwrap();
            for index in 0..length {
                runner
                    .on_closed_bar(ClosedBar {
                        timestamp_ms: index as i64,
                        close: Some(100.0 + (index % 11) as f64),
                    })
                    .unwrap();
                for node in &runner.nodes {
                    match &node.memory {
                        Memory::Mean(window) => assert!(window.history.len() <= window.period),
                        Memory::Extremum(window) => {
                            assert!(window.deque.len() <= window.period);
                            assert!(window.gaps.len() <= window.period);
                        }
                        Memory::Range(maximum, minimum) => {
                            for window in [maximum, minimum] {
                                assert!(window.deque.len() <= window.period);
                                assert!(window.gaps.len() <= window.period);
                            }
                        }
                        Memory::Lag(history) => assert!(history.len() <= 1001),
                        _ => {}
                    }
                }
            }
            counts.push(runner.operation_count());
        }
        assert_eq!(counts[1], 2 * counts[0]);
        assert_eq!(counts[2], 2 * counts[1]);
    }

    #[test]
    fn duplicate_out_of_order_and_over_capacity_bars_do_not_advance_state() {
        let plan = graph(2);
        let mut runner = CausalGraphStream::new(&plan).unwrap();
        let first = ClosedBar {
            timestamp_ms: 10,
            close: None,
        };
        runner.on_closed_bar(first).unwrap();
        let duplicate = runner.on_closed_bar(first).unwrap_err();
        assert_eq!(duplicate, "composition.stream:CLOSED_BAR_NOT_MONOTONIC");
        assert_eq!(runner.processed_bars(), 1);
        runner
            .on_closed_bar(ClosedBar {
                timestamp_ms: 11,
                close: Some(20.0),
            })
            .unwrap();
        assert_eq!(
            runner
                .on_closed_bar(ClosedBar {
                    timestamp_ms: 12,
                    close: Some(30.0)
                })
                .unwrap_err(),
            "composition.stream:COMPILED_CAPACITY_EXCEEDED"
        );
        assert_eq!(runner.processed_bars(), 2);
    }

    #[test]
    fn rejects_external_sources_lookahead_and_nonincremental_math_before_running() {
        let request = |op: &str, input: &str| {
            serde_json::from_value::<CompositionRequest>(json!({
                "version": COMPOSITION_VERSION,
                "steps": [{"id":"result", "op":op, "input":input, "period":2}],
                "result_steps": ["result"]
            }))
            .unwrap()
        };
        let external = CheckedComposition::compile(request("rolling_mean", "volume"), 10).unwrap();
        assert_eq!(
            CausalGraphStream::new(&external).unwrap_err(),
            "composition.stream:SOURCE_NOT_SUPPORTED:volume"
        );
        let nonincremental =
            CheckedComposition::compile(request("rolling_stddev", "close"), 10).unwrap();
        assert_eq!(
            CausalGraphStream::new(&nonincremental).unwrap_err(),
            "composition.stream:OPERATOR_NOT_INCREMENTAL"
        );
        let lookahead: CompositionRequest = serde_json::from_value(json!({
            "version": COMPOSITION_VERSION,
            "steps": [
                {"id":"condition", "op":"greater_than", "input":"close", "threshold":0.0},
                {"id":"label", "op":"forward_return_label", "input":"close", "when":"condition", "period":2}
            ],
            "result_steps": ["label"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(lookahead, 10).unwrap();
        assert_eq!(
            CausalGraphStream::new(&plan).unwrap_err(),
            "composition.stream:NON_CAUSAL_OPERATOR"
        );
    }
}
