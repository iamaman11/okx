//! Typed K7 analysis plan, independent of storage, transport and venue.
pub use crate::operation_contract::{DataAvailability, OperationEffect, OperationShape, TimeBasis};
use crate::operators::{Operator, registry};
use crate::stream::{MonotoneWindow, RollingMean, scalar_value};
use crate::time_series::ClosedRange;
pub use okx_research_protocol::strategy_contract::{
    CompositionRequest, GRAPH_SIGNAL_VERSION, GraphSignalSpec, MissingSignalPolicy, SeriesOp,
    SeriesStep,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub const QUERY_PLAN_VERSION: &str = "okx.query-plan/448.1";
pub const COMPOSITION_VERSION: &str = "okx.market.composition/v1";
const MAX_COMPOSITION_WORK_UNITS: u64 = 64_000_000;

#[cfg(test)]
mod t07_t10_projected_prefix_evaluation {
    use super::*;

    fn graph() -> CheckedComposition {
        let spec = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"price_return","op":"returns_series","input":"close"},
                {"id":"positive","op":"greater_than","input":"price_return","threshold":0.0},
                {"id":"entry","op":"rising_edge","input":"positive"},
                {"id":"smooth_price","op":"rolling_mean","input":"close","period":2},
                {"id":"outcome","op":"forward_return_label","input":"smooth_price","when":"entry","period":2},
                {"id":"risk","op":"rolling_stddev","input":"close","period":2},
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"divergence","op":"subtract_series","input":"price_return","other":"oi_return"},
                {"id":"volume_window","op":"rolling_mean","input":"volume","period":2}
            ],
            "result_steps":["entry","outcome","risk","divergence","volume_window"]
        }))
        .unwrap();
        CheckedComposition::compile(spec, 100_000).unwrap()
    }

    #[test]
    fn causal_branches_stop_at_decision_while_only_the_label_price_reads_future_horizon() {
        let plan = graph();
        let required = plan.required_prefixes(100_000, 2);
        assert_eq!(required["close"], 5);
        assert_eq!(required["smooth_price"], 5);
        assert_eq!(required["outcome"], 3);
        assert_eq!(required["price_return"], 3);
        assert_eq!(required["positive"], 3);
        assert_eq!(required["entry"], 3);
        assert_eq!(required["risk"], 3);
        assert_eq!(required["open_interest"], 3);
        assert_eq!(required["volume"], 3);
        assert!(!required.contains_key("funding_rate"));
        // A large future tail is not traversed or copied by a causal branch.
        assert_eq!(plan.required_prefixes(100_000, 0)["risk"], 1);
        assert_eq!(plan.required_prefixes(100_000, 0)["smooth_price"], 3);
        assert_eq!(plan.required_prefixes(100_000, 99_999)["close"], 100_000);
        assert_eq!(plan.required_prefixes(3, 2)["close"], 3);
        assert_eq!(plan.required_prefixes(3, usize::MAX)["entry"], 3);
    }

    #[test]
    fn multi_source_historical_projection_is_future_invariant_except_for_its_outcome() {
        let plan = graph();
        let mut closes = vec![100.0; 1000];
        closes[..7].copy_from_slice(&[100.0, 90.0, 95.0, 99.0, 120.0, 110.0, 108.0]);
        let oi = (0..closes.len())
            .map(|i| Some(10.0 + i as f64))
            .collect::<Vec<_>>();
        let volume = (0..closes.len())
            .map(|i| 1.0 + i as f64)
            .collect::<Vec<_>>();
        let evaluate = |close: &[f64], observations: &[Option<f64>], volumes: &[f64]| {
            plan.evaluate_at_with_candle_sources_diagnostics(
                close,
                CandlePriceInputs {
                    volume: Some(volumes),
                    ..Default::default()
                },
                Some(observations),
                None,
                2,
            )
            .unwrap()
        };
        let (first, causes) = evaluate(&closes, &oi, &volume);
        assert!(causes.is_empty());
        assert_eq!(first["entry"], (Some(1.0), Unit::Event));
        assert_eq!(first["risk"], (Some(2.5), Unit::Price));
        assert_eq!(first["volume_window"], (Some(2.5), Unit::NativeVolume));
        assert!((first["outcome"].0.unwrap() - (109.5 / 92.5 - 1.0)).abs() < 1e-12);

        let mut changed_tail = closes.clone();
        changed_tail[5..].fill(99_999.0);
        let mut changed_oi = oi.clone();
        changed_oi[3..].fill(None);
        let mut changed_volume = volume.clone();
        changed_volume[3..].fill(999.0);
        assert_eq!(
            first,
            evaluate(&changed_tail, &changed_oi, &changed_volume).0
        );

        // Validation is causal as well: corrupt later OI/volume must not
        // invalidate a historical decision, but latest-bar validation must
        // still reject the exact same malformed sources.
        let mut future_invalid_oi = oi.clone();
        future_invalid_oi[500] = Some(-1.0);
        let mut future_invalid_volume = volume.clone();
        future_invalid_volume[500] = -1.0;
        assert_eq!(
            first,
            evaluate(&closes, &future_invalid_oi, &future_invalid_volume).0
        );
        assert_eq!(
            plan.evaluate_with_candle_sources_diagnostics(
                &closes,
                CandlePriceInputs {
                    volume: Some(&volume),
                    ..Default::default()
                },
                Some(&future_invalid_oi),
                None,
            )
            .unwrap_err(),
            "composition.sources.open_interest:INVALID_CONTRACT_COUNT"
        );
        assert_eq!(
            plan.evaluate_with_candle_sources_diagnostics(
                &closes,
                CandlePriceInputs {
                    volume: Some(&future_invalid_volume),
                    ..Default::default()
                },
                Some(&oi),
                None,
            )
            .unwrap_err(),
            "composition.sources.volume:INVALID_NON_NEGATIVE_VOLUME"
        );

        let mut changed_outcome = closes.clone();
        changed_outcome[4] = 200.0;
        let after = evaluate(&changed_outcome, &oi, &volume).0;
        for feature in ["entry", "risk", "divergence", "volume_window"] {
            assert_eq!(
                first[feature], after[feature],
                "future leaked into {feature}"
            );
        }
        assert_ne!(first["outcome"], after["outcome"]);

        let mut missing_oi = oi.clone();
        missing_oi[2] = None;
        let (missing, causes) = evaluate(&closes, &missing_oi, &volume);
        assert_eq!(missing["divergence"].0, None);
        assert_eq!(causes["divergence"].origin_step, "open_interest");
        assert_eq!(causes["divergence"].origin_index, 2);
        assert_eq!(missing["outcome"], first["outcome"]);
        assert_eq!(
            plan.evaluate_at_with_candle_sources_diagnostics(
                &closes,
                CandlePriceInputs {
                    volume: Some(&volume),
                    ..Default::default()
                },
                Some(&oi[..999]),
                None,
                2,
            )
            .unwrap_err(),
            "composition.sources:ALIGNMENT_LENGTH_MISMATCH"
        );
    }

    #[test]
    fn projected_outcome_cannot_read_beyond_supplied_prefix_or_skip_unknown_event() {
        let plan = graph();
        let closes = [100.0, 90.0, 95.0, 99.0, 120.0];
        let oi = [Some(10.0), Some(11.0), Some(12.0), Some(13.0), Some(14.0)];
        let volumes = [1.0, 2.0, 3.0, 4.0, 5.0];
        let evaluate = |len| {
            plan.evaluate_at_with_candle_sources_diagnostics(
                &closes[..len],
                CandlePriceInputs {
                    volume: Some(&volumes[..len]),
                    ..Default::default()
                },
                Some(&oi[..len]),
                None,
                2,
            )
            .unwrap()
        };
        let (short, cause) = evaluate(4);
        assert_eq!(short["entry"].0, Some(1.0));
        assert_eq!(short["outcome"].0, None);
        assert_eq!(cause["outcome"].reason, "OUTCOME_UNCONFIRMED");
        let (confirmed, _) = evaluate(5);
        assert!(confirmed["outcome"].0.is_some());
        let latest = plan
            .evaluate_with_candle_sources_diagnostics(
                &closes,
                CandlePriceInputs {
                    volume: Some(&volumes),
                    ..Default::default()
                },
                Some(&oi),
                None,
            )
            .unwrap();
        let explicit = plan
            .evaluate_at_with_candle_sources_diagnostics(
                &closes,
                CandlePriceInputs {
                    volume: Some(&volumes),
                    ..Default::default()
                },
                Some(&oi),
                None,
                4,
            )
            .unwrap();
        assert_eq!(latest, explicit);
    }

    #[test]
    fn historical_funding_value_checks_stop_at_the_requested_source_prefix() {
        let request = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"mean_rate","op":"rolling_mean","input":"funding_rate","period":2}],
            "result_steps":["mean_rate"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(request, 1000).unwrap();
        let close = vec![f64::NAN; 1000];
        let mut rates = vec![None; 1000];
        rates[1] = Some(0.01);
        rates[2] = Some(-0.01);
        let (historical, causes) = plan
            .evaluate_at_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs::default(),
                None,
                Some(&rates),
                2,
            )
            .unwrap();
        assert_eq!(historical["mean_rate"].0, Some(0.0));
        assert!(causes.is_empty());
        rates[900] = Some(f64::INFINITY);
        let after = plan
            .evaluate_at_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs::default(),
                None,
                Some(&rates),
                2,
            )
            .unwrap();
        assert_eq!(historical, after.0);
        assert_eq!(
            plan.evaluate_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs::default(),
                None,
                Some(&rates),
            )
            .unwrap_err(),
            "composition.sources.funding_rate:NONFINITE_RATE"
        );
        // Sparse gaps remain unknown rather than becoming synthetic zeros.
        rates[2] = None;
        let (missing, cause) = plan
            .evaluate_at_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs::default(),
                None,
                Some(&rates),
                2,
            )
            .unwrap();
        assert_eq!(missing["mean_rate"].0, None);
        assert_eq!(cause["mean_rate"].origin_step, "funding_rate");
        assert_eq!(cause["mean_rate"].origin_index, 2);
    }
}

/// Unit constraints are finite Rust variants, not caller-supplied strings.
/// The compiler resolves each constraint against the actual `Unit` of every
/// parent. Serialized names are discovery data, never admission authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesUnitRule {
    Price,
    Ratio,
    Boolean,
    PriceOrContractsOrNativeVolume,
    PriceOrRatioOrContractsOrNativeVolumeOrCoefficient,
    PriceOrRatioOrNativeVolumeOrCoefficient,
    CausalSeries,
    SameAsInput,
    Coefficient,
    Event,
    OutcomeRatio,
}

impl SeriesUnitRule {
    /// The same finite unit predicate is used by admission and published in
    /// the operation contract. `SameAsInput` applies only to another input or
    /// to the output of an operation with a previously checked input.
    fn accepts(self, actual: Unit, input: Option<Unit>) -> bool {
        match self {
            Self::Price => actual == Unit::Price,
            Self::Ratio => actual == Unit::Ratio,
            Self::Boolean => actual == Unit::Boolean,
            Self::PriceOrContractsOrNativeVolume => {
                matches!(actual, Unit::Price | Unit::Contracts | Unit::NativeVolume)
            }
            Self::PriceOrRatioOrContractsOrNativeVolumeOrCoefficient => matches!(
                actual,
                Unit::Price
                    | Unit::Ratio
                    | Unit::Contracts
                    | Unit::NativeVolume
                    | Unit::Coefficient
            ),
            Self::PriceOrRatioOrNativeVolumeOrCoefficient => matches!(
                actual,
                Unit::Price | Unit::Ratio | Unit::NativeVolume | Unit::Coefficient
            ),
            Self::CausalSeries => matches!(
                actual,
                Unit::Price
                    | Unit::Ratio
                    | Unit::Contracts
                    | Unit::NativeVolume
                    | Unit::Coefficient
                    | Unit::Boolean
                    | Unit::Event
            ),
            Self::SameAsInput => input == Some(actual),
            Self::Coefficient => actual == Unit::Coefficient,
            Self::Event => actual == Unit::Event,
            Self::OutcomeRatio => actual == Unit::OutcomeRatio,
        }
    }
}

/// Finite, serializable contract semantics. `serde` preserves the v1 catalog
/// strings while Rust callers can no longer silently invent new policies.
/// Changing any variant's meaning requires a new operation contract version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupPolicy {
    None,
    PeriodClosedBars,
    OnePriorClosedBar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NullPolicy {
    PropagateMissingOrNonpositivePrice,
    StrictCompleteWindowNoForwardFill,
    StrictCompleteWindowNoSyntheticZero,
    StrictExactPairs,
    StrictWindowOrZeroVariance,
    StrictWindowZeroForConstant,
    PropagateMissingOrZeroDenominator,
    PropagateUnknown,
    UnconfirmedWithoutFutureBars,
    PropagateMissing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeCostModel {
    LinearInSeriesLength,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRequirement {
    DeclaredExactEventTimeNoForwardFill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterRequirement {
    FiniteThreshold,
    PriorEventStepInWhen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VariancePolicy {
    PopulationInclusiveWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NumericPolicy {
    FiniteOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventTrigger {
    FalseToTrue,
    TrueToFalse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum OperationOwner {
    #[serde(rename = "okx-strategy-core::query_plan")]
    StrategyCoreQueryPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationPolicy {
    CheckedCompositionUnitAndReplayTests,
}

/// One complete, immutable contract for every elemental series operation.
/// The pure compiler owns version, type/units and cost; Market and Research
/// serialize these descriptors directly instead of copying JSON catalogs.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SeriesOperationContract {
    pub op: SeriesOp,
    pub version: u16,
    pub input_unit: SeriesUnitRule,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub other_unit: Option<SeriesUnitRule>,
    pub output_unit: SeriesUnitRule,
    pub output_shape: OperationShape,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_min: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_max: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires: Option<ParameterRequirement>,
    pub warmup: WarmupPolicy,
    pub null_policy: NullPolicy,
    pub compute_cost: ComputeCostModel,
    pub cost_per_bar: u64,
    pub causal: bool,
    pub graph_backtest_supported: bool,
    pub time_basis: TimeBasis,
    pub source_requirement: SourceRequirement,
    pub effects: OperationEffect,
    pub owner: OperationOwner,
    pub verification: VerificationPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variance: Option<VariancePolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum_pairs: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numeric_policy: Option<NumericPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<EventTrigger>,
}

pub fn series_operation_contract(op: SeriesOp) -> SeriesOperationContract {
    use SeriesOp::*;
    use SeriesUnitRule as U;
    let input_unit = match op {
        ReturnsSeries => U::PriceOrContractsOrNativeVolume,
        RollingMean | RollingMax | RollingMin | RollingRange | RollingZscore | RollingStddev
        | GreaterThanSeries | LessThanSeries => {
            U::PriceOrRatioOrContractsOrNativeVolumeOrCoefficient
        }
        RollingPearson | AddSeries | SubtractSeries | DivideSeries => U::Ratio,
        And | Or | RisingEdge | FallingEdge => U::Boolean,
        Lag => U::CausalSeries,
        GreaterThan | LessThan => U::PriceOrRatioOrNativeVolumeOrCoefficient,
        ForwardReturnLabel => U::Price,
    };
    let other_unit = match op {
        RollingPearson | AddSeries | SubtractSeries | DivideSeries => Some(U::Ratio),
        And | Or => Some(U::Boolean),
        GreaterThanSeries | LessThanSeries => Some(U::SameAsInput),
        _ => None,
    };
    let output_unit = match op {
        ReturnsSeries | AddSeries | SubtractSeries | DivideSeries => U::Ratio,
        RollingMean | RollingMax | RollingMin | RollingRange | RollingStddev | Lag => {
            U::SameAsInput
        }
        RollingPearson | RollingZscore => U::Coefficient,
        And | Or | GreaterThan | GreaterThanSeries | LessThan | LessThanSeries => U::Boolean,
        RisingEdge | FallingEdge => U::Event,
        ForwardReturnLabel => U::OutcomeRatio,
    };
    let period = match op {
        RollingMean | RollingMax | RollingMin | Lag | ForwardReturnLabel => Some((1, 1000)),
        RollingRange | RollingStddev | RollingZscore => Some((2, 1000)),
        RollingPearson => Some((30, 1000)),
        _ => None,
    };
    let null_policy = match op {
        ReturnsSeries => NullPolicy::PropagateMissingOrNonpositivePrice,
        RollingMean | RollingMax | RollingMin => NullPolicy::StrictCompleteWindowNoForwardFill,
        RollingRange => NullPolicy::StrictCompleteWindowNoSyntheticZero,
        RollingPearson => NullPolicy::StrictExactPairs,
        RollingZscore => NullPolicy::StrictWindowOrZeroVariance,
        RollingStddev => NullPolicy::StrictWindowZeroForConstant,
        DivideSeries => NullPolicy::PropagateMissingOrZeroDenominator,
        And | Or | GreaterThan | GreaterThanSeries | LessThan | LessThanSeries | RisingEdge
        | FallingEdge => NullPolicy::PropagateUnknown,
        ForwardReturnLabel => NullPolicy::UnconfirmedWithoutFutureBars,
        AddSeries | SubtractSeries | Lag => NullPolicy::PropagateMissing,
    };
    let cost_per_bar = match op {
        RollingMean => 3,
        RollingZscore | RollingStddev => 6,
        RollingRange => 4,
        RollingPearson => 8,
        AddSeries | SubtractSeries | DivideSeries | GreaterThanSeries | LessThanSeries | And
        | Or => 2,
        _ => 1,
    };
    let causal = op != ForwardReturnLabel;
    SeriesOperationContract {
        op,
        version: 1,
        input_unit,
        other_unit,
        output_unit,
        output_shape: OperationShape::Series,
        period_min: period.map(|(min, _)| min),
        period_max: period.map(|(_, max)| max),
        requires: match op {
            GreaterThan | LessThan => Some(ParameterRequirement::FiniteThreshold),
            ForwardReturnLabel => Some(ParameterRequirement::PriorEventStepInWhen),
            _ => None,
        },
        warmup: if period.is_some() {
            WarmupPolicy::PeriodClosedBars
        } else if matches!(op, ReturnsSeries | RisingEdge | FallingEdge) {
            WarmupPolicy::OnePriorClosedBar
        } else {
            WarmupPolicy::None
        },
        null_policy,
        compute_cost: ComputeCostModel::LinearInSeriesLength,
        cost_per_bar,
        causal,
        graph_backtest_supported: causal,
        time_basis: if causal {
            TimeBasis::ClosedBar
        } else {
            TimeBasis::OutcomeFutureHorizon
        },
        source_requirement: SourceRequirement::DeclaredExactEventTimeNoForwardFill,
        effects: OperationEffect::PureNoIo,
        owner: OperationOwner::StrategyCoreQueryPlan,
        verification: VerificationPolicy::CheckedCompositionUnitAndReplayTests,
        variance: matches!(op, RollingZscore | RollingStddev)
            .then_some(VariancePolicy::PopulationInclusiveWindow),
        minimum_pairs: (op == RollingPearson).then_some(30),
        numeric_policy: matches!(op, SubtractSeries | DivideSeries)
            .then_some(NumericPolicy::FiniteOnly),
        trigger: match op {
            RisingEdge => Some(EventTrigger::FalseToTrue),
            FallingEdge => Some(EventTrigger::TrueToFalse),
            _ => None,
        },
    }
}

pub fn series_operation_contracts() -> Vec<SeriesOperationContract> {
    SeriesOp::ALL
        .into_iter()
        .map(series_operation_contract)
        .collect()
}

#[cfg(test)]
mod typed_operation_catalog_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn all_protocol_operations_have_unique_versioned_pure_contracts() {
        let contracts = series_operation_contracts();
        assert_eq!(contracts.len(), 21);
        assert_eq!(contracts.len(), SeriesOp::ALL.len());
        let mut ids = BTreeSet::new();
        for contract in contracts {
            let id = serde_json::to_string(&contract.op).unwrap();
            assert!(ids.insert(id), "duplicate operation id");
            assert_eq!(contract.version, 1);
            assert_eq!(contract.effects, OperationEffect::PureNoIo);
            assert_eq!(contract.output_shape, OperationShape::Series);
            assert!(contract.cost_per_bar > 0);
            assert_eq!(contract.period_min.is_some(), contract.period_max.is_some());
            assert_eq!(contract.graph_backtest_supported, contract.causal);
            assert_eq!(
                contract.source_requirement,
                SourceRequirement::DeclaredExactEventTimeNoForwardFill
            );
            if let (Some(min), Some(max)) = (contract.period_min, contract.period_max) {
                assert!((1..=max).contains(&min));
                assert!(max <= 1000);
            }
        }
        assert_eq!(ids.len(), 21);
    }

    #[test]
    fn typed_semantics_preserve_the_existing_version_one_capability_wire() {
        let expected_nulls = [
            ("returns_series", "propagate_missing_or_nonpositive_price"),
            ("rolling_mean", "strict_complete_window_no_forward_fill"),
            ("rolling_max", "strict_complete_window_no_forward_fill"),
            ("rolling_min", "strict_complete_window_no_forward_fill"),
            ("rolling_range", "strict_complete_window_no_synthetic_zero"),
            ("rolling_pearson", "strict_exact_pairs"),
            ("rolling_zscore", "strict_window_or_zero_variance"),
            ("rolling_stddev", "strict_window_zero_for_constant"),
            ("add_series", "propagate_missing"),
            ("subtract_series", "propagate_missing"),
            ("divide_series", "propagate_missing_or_zero_denominator"),
            ("and", "propagate_unknown"),
            ("or", "propagate_unknown"),
            ("lag", "propagate_missing"),
            ("greater_than", "propagate_unknown"),
            ("greater_than_series", "propagate_unknown"),
            ("less_than", "propagate_unknown"),
            ("less_than_series", "propagate_unknown"),
            ("rising_edge", "propagate_unknown"),
            ("falling_edge", "propagate_unknown"),
            ("forward_return_label", "unconfirmed_without_future_bars"),
        ];
        for (contract, (id, null_policy)) in
            series_operation_contracts().into_iter().zip(expected_nulls)
        {
            let published = serde_json::to_value(contract).unwrap();
            assert_eq!(published["op"], id);
            assert_eq!(published["null_policy"], null_policy);
            assert_eq!(published["output_shape"], "series");
            assert_eq!(published["effects"], "pure_no_io");
            assert_eq!(published["owner"], "okx-strategy-core::query_plan");
            assert_eq!(
                published["verification"],
                "checked_composition_unit_and_replay_tests"
            );
            assert_eq!(
                published["source_requirement"],
                "declared_exact_event_time_no_forward_fill"
            );
            assert_eq!(published["compute_cost"], "linear_in_series_length");
            assert_eq!(
                published["time_basis"],
                if contract.causal {
                    "closed_bar"
                } else {
                    "outcome_future_horizon"
                }
            );
        }
    }

    #[test]
    fn future_outcome_is_a_distinct_typed_contract_not_a_signal() {
        let contract = series_operation_contract(SeriesOp::ForwardReturnLabel);
        assert_eq!(contract.time_basis, TimeBasis::OutcomeFutureHorizon);
        assert_eq!(contract.output_unit, SeriesUnitRule::OutcomeRatio);
        assert_eq!(
            contract.requires,
            Some(ParameterRequirement::PriorEventStepInWhen)
        );
        assert_eq!(contract.warmup, WarmupPolicy::PeriodClosedBars);
        assert!(!contract.causal);
        assert!(!contract.graph_backtest_supported);
        let published = serde_json::to_value(contract).unwrap();
        assert_eq!(published["requires"], "prior_event_step_in_when");
        assert_eq!(published["output_unit"], "outcome_ratio");
        assert_eq!(published["warmup"], "period_closed_bars");
        assert_eq!(published["graph_backtest_supported"], false);
        assert_eq!(published["null_policy"], "unconfirmed_without_future_bars");
    }

    #[test]
    fn compiler_admits_exactly_the_input_units_declared_by_every_contract() {
        let sources = [
            ("close", Unit::Price),
            ("open_interest", Unit::Contracts),
            ("volume", Unit::NativeVolume),
            ("funding_rate", Unit::Ratio),
            ("condition", Unit::Boolean),
            ("event", Unit::Event),
            ("coefficient", Unit::Coefficient),
            ("outcome", Unit::OutcomeRatio),
        ];
        for op in SeriesOp::ALL {
            let contract = series_operation_contract(op);
            for (source, unit) in sources {
                let other = contract.other_unit.map(|rule| match rule {
                    SeriesUnitRule::Boolean => "condition",
                    SeriesUnitRule::SameAsInput => source,
                    _ => "returns",
                });
                let mut probe = json!({"id":"probe","op":op,"input":source});
                if let Some(other) = other {
                    probe["other"] = json!(other);
                }
                if let Some(period) = contract.period_min {
                    probe["period"] = json!(period);
                }
                if matches!(op, SeriesOp::GreaterThan | SeriesOp::LessThan) {
                    probe["threshold"] = json!(0.0);
                }
                if op == SeriesOp::ForwardReturnLabel {
                    probe["when"] = json!("event");
                }
                let request = serde_json::from_value(json!({
                    "version":COMPOSITION_VERSION,
                    "steps":[
                        {"id":"returns","op":"returns_series","input":"close"},
                        {"id":"condition","op":"greater_than","input":"close","threshold":0},
                        {"id":"event","op":"rising_edge","input":"condition"},
                        {"id":"coefficient","op":"rolling_zscore","input":"close","period":2},
                        {"id":"outcome","op":"forward_return_label","input":"close","when":"event","period":1},
                        probe
                    ],
                    "result_steps":["probe"]
                }))
                .unwrap();
                let outcome = CheckedComposition::compile(request, 80);
                assert_eq!(
                    outcome.is_ok(),
                    contract.input_unit.accepts(unit, None),
                    "operation={op:?}, input={source}, compile={outcome:?}"
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckedSeriesStep {
    pub step: SeriesStep,
    pub unit: Unit,
    pub depth: u8,
}

/// Only the compiler may create this value. A decoded snapshot must be
/// recompiled: JSON fields describing checked units, graph depth and budget
/// are evidence, never authority to bypass validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "StoredCheckedComposition")]
pub struct CheckedComposition {
    version: String,
    steps: Vec<CheckedSeriesStep>,
    result_steps: Vec<String>,
    estimated_cells: u64,
}

/// Keep the existing JSON shape for old immutable artifacts, but do not
/// deserialize a trusted plan from it. The serialized checked annotations
/// must agree exactly with a fresh compilation of the underlying requests.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCheckedComposition {
    version: String,
    steps: Vec<CheckedSeriesStep>,
    result_steps: Vec<String>,
    estimated_cells: u64,
}

impl TryFrom<StoredCheckedComposition> for CheckedComposition {
    type Error = String;

    fn try_from(stored: StoredCheckedComposition) -> Result<Self, Self::Error> {
        if stored.steps.is_empty() || stored.steps.len() > 32 {
            return Err("composition.checked:INVALID_STEP_COUNT".into());
        }
        let step_count = stored.steps.len() as u64;
        if !stored.estimated_cells.is_multiple_of(step_count) {
            return Err("composition.checked:INVALID_ESTIMATED_CELLS".into());
        }
        let total_bars = stored.estimated_cells / step_count;
        let checked = CheckedComposition::compile(
            CompositionRequest {
                version: stored.version,
                steps: stored.steps.iter().map(|step| step.step.clone()).collect(),
                result_steps: stored.result_steps,
            },
            total_bars,
        )?;
        if checked.steps != stored.steps || checked.estimated_cells != stored.estimated_cells {
            return Err("composition.checked:FORGED_BINDINGS_OR_BUDGET".into());
        }
        Ok(checked)
    }
}

/// A deterministic explanation for a projected null. Diagnosed from the
/// already-evaluated series, without storing another per-bar quality graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositionNullCause {
    pub reason: &'static str,
    pub origin_step: String,
    /// Exact aligned grid index where the missing source, warmup or failed
    /// operation originated. This is an event index, not a publication time.
    pub origin_index: usize,
}

/// Stable public projections; aliases keep the diagnostic API readable.
pub type CompositionValues = BTreeMap<String, (Option<f64>, Unit)>;
pub type CompositionDiagnostics = (CompositionValues, BTreeMap<String, CompositionNullCause>);

/// Optional aligned closed-candle columns (OHLCV). A missing observation is NaN;
/// the evaluation layer turns it into an unknown, never a synthetic zero.
/// An absent *required column* is an error before evaluation.
#[derive(Debug, Clone, Copy, Default)]
pub struct CandlePriceInputs<'a> {
    pub open: Option<&'a [f64]>,
    pub high: Option<&'a [f64]>,
    pub low: Option<&'a [f64]>,
    pub volume: Option<&'a [f64]>,
}

impl CheckedComposition {
    /// Read-only accessors preserve inspection without permitting the caller
    /// to mutate checked bindings after validation.
    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn steps(&self) -> &[CheckedSeriesStep] {
        &self.steps
    }

    pub fn result_steps(&self) -> &[String] {
        &self.result_steps
    }

    pub fn estimated_cells(&self) -> u64 {
        self.estimated_cells
    }

    /// Inspect only the transitive projection closure: unused declarations
    /// neither fetch sources nor allocate their columns at execution time.
    pub fn uses_source(&self, source: &str) -> bool {
        let reachable = self.reachable_steps();
        self.steps.iter().any(|checked| {
            reachable.contains(checked.step.id.as_str())
                && (checked.step.input == source || checked.step.other.as_deref() == Some(source))
        })
    }
    /// Resolve the transitive dependency closure of projected results. The
    /// complete declaration is still validated, fingerprinted and budgeted;
    /// only the work actually contributing to a result is evaluated. This
    /// also prevents dead OI/funding branches from becoming source reads.
    fn reachable_steps(&self) -> BTreeSet<&str> {
        let mut reachable = self
            .result_steps
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        for checked in self.steps.iter().rev() {
            let step = &checked.step;
            if reachable.contains(step.id.as_str()) {
                reachable.insert(&step.input);
                if let Some(other) = step.other.as_deref() {
                    reachable.insert(other);
                }
                if let Some(when) = step.when.as_deref() {
                    reachable.insert(when);
                }
            }
        }
        reachable
    }

    /// Compute the minimum prefix required by each reachable step and source
    /// for a historical projection. Ordinary features stop at the requested
    /// closed index. Only a forward outcome's *price input* may extend to its
    /// declared horizon; its event condition and independent feature branches
    /// remain at the decision cutoff. No value from an unused branch is read.
    fn required_prefixes(&self, available: usize, at: usize) -> BTreeMap<String, usize> {
        let decision_prefix = at.saturating_add(1).min(available);
        let mut required = self
            .result_steps
            .iter()
            .map(|id| (id.clone(), decision_prefix))
            .collect::<BTreeMap<_, _>>();
        for checked in self.steps.iter().rev() {
            let step = &checked.step;
            let Some(&end) = required.get(step.id.as_str()) else {
                continue;
            };
            let input_end = if step.op == SeriesOp::ForwardReturnLabel {
                end.saturating_add(step.period.expect("validated horizon") as usize)
                    .min(available)
            } else {
                end
            };
            let mut extend = |name: &str, length: usize| {
                required
                    .entry(name.to_owned())
                    .and_modify(|existing| *existing = (*existing).max(length))
                    .or_insert(length);
            };
            extend(step.input.as_str(), input_end);
            if let Some(other) = step.other.as_deref() {
                extend(other, end);
            }
            if let Some(condition) = step.when.as_deref() {
                extend(condition, end);
            }
        }
        required
    }

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
            if SeriesSource::parse(&step.id).is_some() || known.contains_key(&step.id) {
                return Err(format!("{path}.id:DUPLICATE"));
            }
            let (input_unit, parent_depth) = match SeriesSource::parse(&step.input) {
                Some(source) => (source.contract().unit, 0),
                None => known.get(&step.input).copied().ok_or_else(|| {
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
                        | SeriesOp::GreaterThanSeries
                        | SeriesOp::LessThanSeries
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
                    | SeriesOp::GreaterThanSeries
                    | SeriesOp::LessThanSeries
                    | SeriesOp::And
                    | SeriesOp::Or
            ) {
                let name = step
                    .other
                    .as_deref()
                    .ok_or_else(|| format!("{path}.other:REQUIRED"))?;
                Some(match SeriesSource::parse(name) {
                    Some(source) => (source.contract().unit, 0),
                    None => known
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
            let contract = series_operation_contract(step.op);
            let valid_period = step.period.is_some_and(|period| {
                contract
                    .period_min
                    .zip(contract.period_max)
                    .is_some_and(|(min, max)| (min..=max).contains(&period))
            });
            let output_unit = match step.op {
                SeriesOp::RollingZscore | SeriesOp::RollingStddev | SeriesOp::RollingRange => {
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=numeric_series:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if !valid_period {
                        return Err(format!("{path}.period:INVALID:2..1000"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    if step.op == SeriesOp::RollingZscore {
                        Unit::Coefficient
                    } else {
                        input_unit
                    }
                }
                SeriesOp::RollingPearson => {
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let unit = other.expect("validated second input").0;
                    if !contract
                        .other_unit
                        .expect("typed second input")
                        .accepts(unit, Some(input_unit))
                    {
                        return Err(format!(
                            "{path}.other:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(unit)
                        ));
                    }
                    if !valid_period {
                        return Err(format!("{path}.period:INVALID:30..1000"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    Unit::Coefficient
                }
                SeriesOp::AddSeries | SeriesOp::SubtractSeries | SeriesOp::DivideSeries => {
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=ratio:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let unit = other.expect("validated second input").0;
                    if !contract
                        .other_unit
                        .expect("typed second input")
                        .accepts(unit, Some(input_unit))
                    {
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
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=boolean:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let unit = other.expect("validated second condition").0;
                    if !contract
                        .other_unit
                        .expect("typed second input")
                        .accepts(unit, Some(input_unit))
                    {
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
                    if !contract.input_unit.accepts(input_unit, None) {
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
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=causal_series:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if !valid_period {
                        return Err(format!("{path}.period:INVALID"));
                    }
                    if step.threshold.is_some() {
                        return Err(format!("{path}.threshold:UNEXPECTED"));
                    }
                    input_unit
                }
                SeriesOp::GreaterThan | SeriesOp::LessThan => {
                    if !contract.input_unit.accepts(input_unit, None) {
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
                SeriesOp::GreaterThanSeries | SeriesOp::LessThanSeries => {
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=numeric_series:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    let other_unit = other.expect("validated second series").0;
                    if !contract
                        .other_unit
                        .expect("typed second input")
                        .accepts(other_unit, Some(input_unit))
                    {
                        return Err(format!(
                            "{path}.other:UNIT_MISMATCH:expected={}:actual={}",
                            composition_unit_name(input_unit),
                            composition_unit_name(other_unit)
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
                SeriesOp::RisingEdge | SeriesOp::FallingEdge => {
                    if !contract.input_unit.accepts(input_unit, None) {
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
                    if !contract.input_unit.accepts(input_unit, None) {
                        return Err(format!(
                            "{path}.input:UNIT_MISMATCH:expected=price:actual={}",
                            composition_unit_name(input_unit)
                        ));
                    }
                    if !valid_period {
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
            debug_assert!(contract.output_unit.accepts(output_unit, Some(input_unit)));
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
        self.evaluate_at_sources(close, CandlePriceInputs::default(), None, None, at, false)
            .0
    }

    /// Preserve the legacy value projection; add precise explanations only
    /// for projected nulls, calculated once at the requested closed index.
    pub fn evaluate_with_diagnostics(&self, close: &[f64]) -> CompositionDiagnostics {
        self.evaluate_at_sources(
            close,
            CandlePriceInputs::default(),
            None,
            None,
            close.len().saturating_sub(1),
            true,
        )
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
                CandlePriceInputs::default(),
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
    ) -> Result<CompositionDiagnostics, String> {
        self.check_open_interest(close, open_interest)?;
        Ok(self.evaluate_at_sources(
            close,
            CandlePriceInputs::default(),
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
    ) -> Result<CompositionDiagnostics, String> {
        self.evaluate_with_candle_sources_diagnostics(
            close,
            CandlePriceInputs::default(),
            open_interest,
            funding_rate,
        )
    }

    /// Evaluate exact timestamp-aligned OHLC and derivative source columns.
    /// The worker owns grid construction and source availability policy.
    pub fn evaluate_with_candle_sources_diagnostics(
        &self,
        close: &[f64],
        candles: CandlePriceInputs<'_>,
        open_interest: Option<&[Option<f64>]>,
        funding_rate: Option<&[Option<f64>]>,
    ) -> Result<CompositionDiagnostics, String> {
        self.evaluate_at_with_candle_sources_diagnostics(
            close,
            candles,
            open_interest,
            funding_rate,
            close.len().saturating_sub(1),
        )
    }

    /// Historical projection over aligned sources with bounded per-step
    /// prefixes. Event-time OI/funding columns are retrospective unless the
    /// caller separately attests their *publication* times; this method does
    /// not silently certify those observations for live decision-making.
    pub fn evaluate_at_with_candle_sources_diagnostics(
        &self,
        close: &[f64],
        candles: CandlePriceInputs<'_>,
        open_interest: Option<&[Option<f64>]>,
        funding_rate: Option<&[Option<f64>]>,
        at: usize,
    ) -> Result<CompositionDiagnostics, String> {
        let required = self.required_prefixes(close.len(), at);
        // Preserve the full legacy validation for a latest-bar projection.
        // Historical feature checks must not inspect a future source value:
        // a malformed observation recorded after `at` cannot retroactively
        // change whether an earlier closed-bar decision was computable.
        let inspected = |name: &str| {
            if at >= close.len().saturating_sub(1) {
                close.len()
            } else {
                required.get(name).copied().unwrap_or(0)
            }
        };
        for (name, series) in [
            ("open", candles.open),
            ("high", candles.high),
            ("low", candles.low),
            ("volume", candles.volume),
        ] {
            if self.uses_source(name) && series.is_none() {
                return Err(format!("composition.sources.{name}:REQUIRED"));
            }
            if series.is_some_and(|values| values.len() != close.len()) {
                return Err(format!(
                    "composition.sources.{name}:ALIGNMENT_LENGTH_MISMATCH"
                ));
            }
        }
        if candles.volume.is_some_and(|values| {
            values[..inspected("volume")]
                .iter()
                .any(|value| !value.is_nan() && (!value.is_finite() || *value < 0.0))
        }) {
            return Err("composition.sources.volume:INVALID_NON_NEGATIVE_VOLUME".into());
        }
        if let Some(oi) = open_interest {
            self.check_open_interest_prefix(close, oi, inspected("open_interest"))?;
        }
        if let Some(rates) = funding_rate {
            if rates.len() != close.len() {
                return Err("composition.sources:ALIGNMENT_LENGTH_MISMATCH".into());
            }
            if rates[..inspected("funding_rate")]
                .iter()
                .flatten()
                .any(|rate| !rate.is_finite())
            {
                return Err("composition.sources.funding_rate:NONFINITE_RATE".into());
            }
        }
        Ok(self.evaluate_at_sources(close, candles, open_interest, funding_rate, at, true))
    }

    fn check_open_interest(
        &self,
        close: &[f64],
        open_interest: &[Option<f64>],
    ) -> Result<(), String> {
        self.check_open_interest_prefix(close, open_interest, close.len())
    }

    fn check_open_interest_prefix(
        &self,
        close: &[f64],
        open_interest: &[Option<f64>],
        inspected: usize,
    ) -> Result<(), String> {
        if close.len() != open_interest.len() {
            return Err("composition.sources:ALIGNMENT_LENGTH_MISMATCH".into());
        }
        if open_interest[..inspected]
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
        candles: CandlePriceInputs<'_>,
        open_interest: Option<&[Option<f64>]>,
        funding_rate: Option<&[Option<f64>]>,
        at: usize,
        explain_nulls: bool,
    ) -> CompositionDiagnostics {
        let required = self.required_prefixes(close.len(), at);
        let mut values = BTreeMap::<String, Vec<Option<f64>>>::new();
        let close_end = required.get("close").copied().unwrap_or(0);
        values.insert(
            "close".into(),
            close[..close_end]
                .iter()
                .map(|v| v.is_finite().then_some(*v))
                .collect(),
        );
        for (name, source) in [
            ("open", candles.open),
            ("high", candles.high),
            ("low", candles.low),
            ("volume", candles.volume),
        ] {
            if let Some(&end) = required.get(name) {
                values.insert(
                    name.into(),
                    source
                        .map(|values| {
                            values[..end]
                                .iter()
                                .map(|v| v.is_finite().then_some(*v))
                                .collect()
                        })
                        .unwrap_or_else(|| vec![None; end]),
                );
            }
        }
        if let Some(&end) = required.get("open_interest") {
            values.insert(
                "open_interest".into(),
                open_interest
                    .map(|source| source[..end].to_vec())
                    .unwrap_or_else(|| vec![None; end]),
            );
        }
        if let Some(&end) = required.get("funding_rate") {
            values.insert(
                "funding_rate".into(),
                funding_rate
                    .map(|source| source[..end].to_vec())
                    .unwrap_or_else(|| vec![None; end]),
            );
        }
        for checked in &self.steps {
            let Some(&end) = required.get(checked.step.id.as_str()) else {
                continue;
            };
            // A shared upstream series is read by reference for all branches;
            // cloning it here multiplied peak memory and source work.
            let full_input = &values[&checked.step.input];
            let input = if checked.step.op == SeriesOp::ForwardReturnLabel {
                full_input.as_slice()
            } else {
                &full_input[..end]
            };
            let output = match checked.step.op {
                SeriesOp::RollingZscore => rolling_zscore(
                    input,
                    checked.step.period.expect("validated zscore period") as usize,
                ),
                SeriesOp::RollingStddev => rolling_stddev(
                    input,
                    checked.step.period.expect("validated stddev period") as usize,
                ),
                SeriesOp::RollingRange => rolling_range(
                    input,
                    checked.step.period.expect("validated range period") as usize,
                ),
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
                        .map(|(&left, &right)| {
                            scalar_value(checked.step.op, left, right, None, None)
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
                        .map(|(&left, &right)| {
                            scalar_value(checked.step.op, left, right, None, None)
                        })
                        .collect()
                }
                SeriesOp::ReturnsSeries => {
                    let mut result = vec![None; input.len()];
                    for i in 1..input.len() {
                        result[i] =
                            scalar_value(checked.step.op, input[i], None, input[i - 1], None);
                    }
                    result
                }
                SeriesOp::RollingMean => {
                    let period = checked.step.period.unwrap_or(1) as usize;
                    let mut window = RollingMean::new(period);
                    input.iter().copied().map(|v| window.push(v)).collect()
                }
                SeriesOp::RollingMax | SeriesOp::RollingMin => rolling_extremum(
                    input,
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
                SeriesOp::GreaterThan | SeriesOp::LessThan => {
                    let threshold = checked.step.threshold.expect("validated threshold");
                    input
                        .iter()
                        .map(|&v| scalar_value(checked.step.op, v, None, None, Some(threshold)))
                        .collect()
                }
                SeriesOp::GreaterThanSeries | SeriesOp::LessThanSeries => {
                    let other = &values[checked
                        .step
                        .other
                        .as_ref()
                        .expect("validated comparison input")];
                    input
                        .iter()
                        .zip(other)
                        .map(|(&a, &b)| scalar_value(checked.step.op, a, b, None, None))
                        .collect()
                }
                SeriesOp::RisingEdge | SeriesOp::FallingEdge => {
                    let mut result = vec![None; input.len()];
                    for i in 1..input.len() {
                        result[i] =
                            scalar_value(checked.step.op, input[i], None, input[i - 1], None);
                    }
                    result
                }
                SeriesOp::ForwardReturnLabel => {
                    let horizon = checked.step.period.expect("validated horizon") as usize;
                    let condition = &values[checked.step.when.as_ref().expect("validated event")];
                    let mut result = vec![None; end];
                    for i in 0..end.min(input.len().saturating_sub(horizon)) {
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
            origin_index: at,
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
            SeriesOp::RollingZscore | SeriesOp::RollingStddev => {
                let period = step.period.expect("validated dispersion window") as usize;
                if at + 1 < period {
                    return cause("WARMUP");
                }
                let mut stats = SlidingPearson::default();
                for index in (at + 1 - period)..=at {
                    if let Some(upstream) = trace(&step.input, index) {
                        return upstream;
                    }
                    let value = value(&step.input, index).expect("present after trace");
                    stats.add(value, value);
                }
                if stats.m2_x <= 0.0 && step.op == SeriesOp::RollingZscore {
                    return cause("ZERO_VARIANCE");
                }
                cause("NON_FINITE_RESULT")
            }
            SeriesOp::AddSeries
            | SeriesOp::SubtractSeries
            | SeriesOp::DivideSeries
            | SeriesOp::GreaterThanSeries
            | SeriesOp::LessThanSeries
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
            SeriesOp::RollingMean
            | SeriesOp::RollingMax
            | SeriesOp::RollingMin
            | SeriesOp::RollingRange => {
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
            SeriesOp::GreaterThan | SeriesOp::LessThan => {
                trace(&step.input, at).unwrap_or_else(|| cause("NON_FINITE_RESULT"))
            }
            SeriesOp::RisingEdge | SeriesOp::FallingEdge => {
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
        let mut known = SERIES_SOURCES
            .into_iter()
            .map(|source| {
                (
                    source.as_str().to_string(),
                    BTreeSet::from([source.contract().source]),
                )
            })
            .collect::<BTreeMap<_, _>>();
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
    // Close has one unconditional buffer in the existing evaluator. Other
    // sources add storage only if referenced by a compiled step.
    1 + SERIES_SOURCES
        .into_iter()
        .filter(|source| *source != SeriesSource::Close && uses(source.as_str()))
        .count() as u64
}

fn series_cost_per_bar(op: SeriesOp) -> u64 {
    series_operation_contract(op).cost_per_bar
}

/// Online centered covariance with O(1) insertion and removal. This avoids
/// catastrophic cancellation of raw sums of squares for near-constant data.
#[derive(Default, Debug)]
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
#[derive(Debug)]
pub(crate) struct RollingPearsonWindow {
    period: usize,
    history: VecDeque<Option<(f64, f64)>>,
    stats: SlidingPearson,
    missing: usize,
    seen: usize,
}

impl RollingPearsonWindow {
    pub(crate) fn new(period: usize) -> Self {
        Self {
            period,
            history: VecDeque::with_capacity(period),
            stats: SlidingPearson::default(),
            missing: 0,
            seen: 0,
        }
    }

    pub(crate) fn push(&mut self, left: Option<f64>, right: Option<f64>) -> Option<f64> {
        let pair = match (left, right) {
            (Some(a), Some(b)) if a.is_finite() && b.is_finite() => Some((a, b)),
            _ => None,
        };
        if self.history.len() == self.period {
            match self.history.pop_front().expect("full correlation window") {
                Some((a, b)) => self.stats.remove(a, b),
                None => self.missing -= 1,
            }
        }
        // Rebase the finite sliding state once every 4096 observations. This
        // is O(1) amortized, and the same implementation serves batch/stream.
        if self.seen > 0 && self.seen.is_multiple_of(4096) {
            self.stats = SlidingPearson::default();
            self.missing = 0;
            for sample in &self.history {
                match *sample {
                    Some((a, b)) => self.stats.add(a, b),
                    None => self.missing += 1,
                }
            }
        }
        match pair {
            Some((a, b)) => self.stats.add(a, b),
            None => self.missing += 1,
        }
        self.history.push_back(pair);
        self.seen += 1;
        if self.history.len() == self.period && self.missing == 0 {
            self.stats.coefficient()
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn buffered(&self) -> usize {
        self.history.len()
    }
}

fn rolling_pearson(left: &[Option<f64>], right: &[Option<f64>], period: usize) -> Vec<Option<f64>> {
    let mut window = RollingPearsonWindow::new(period);
    left.iter()
        .copied()
        .zip(right.iter().copied())
        .map(|(a, b)| window.push(a, b))
        .collect()
}

/// Causal population dispersion over an inclusive complete window. Standard
/// deviation preserves the input dimension and returns zero for a constant
/// window; z-score is dimensionless and marks zero variance as unknown.
/// Online centered variance avoids cancellation from raw squared sums;
/// periodic rebasing caps drift without an O(window) per-bar rescan.
#[derive(Debug)]
pub(crate) struct RollingDispersionWindow {
    period: usize,
    zscore: bool,
    history: VecDeque<Option<f64>>,
    stats: SlidingPearson,
    missing: usize,
    identical_run: usize,
    seen: usize,
}

impl RollingDispersionWindow {
    pub(crate) fn new(period: usize, zscore: bool) -> Self {
        Self {
            period,
            zscore,
            history: VecDeque::with_capacity(period),
            stats: SlidingPearson::default(),
            missing: 0,
            identical_run: 0,
            seen: 0,
        }
    }

    pub(crate) fn push(&mut self, input: Option<f64>) -> Option<f64> {
        let input = input.filter(|value| value.is_finite());
        self.identical_run = match (self.history.back().copied().flatten(), input) {
            (Some(previous), Some(current)) if previous == current => self.identical_run + 1,
            (_, Some(_)) => 1,
            _ => 0,
        };
        if self.history.len() == self.period {
            match self.history.pop_front().expect("full dispersion window") {
                Some(value) => self.stats.remove(value, value),
                None => self.missing -= 1,
            }
        }
        if self.seen > 0 && self.seen.is_multiple_of(4096) {
            self.stats = SlidingPearson::default();
            self.missing = 0;
            for sample in &self.history {
                match *sample {
                    Some(value) => self.stats.add(value, value),
                    None => self.missing += 1,
                }
            }
        }
        match input {
            Some(value) => self.stats.add(value, value),
            None => self.missing += 1,
        }
        self.history.push_back(input);
        self.seen += 1;
        if self.history.len() != self.period || self.missing != 0 {
            return None;
        }
        if self.identical_run >= self.period {
            return (!self.zscore).then_some(0.0);
        }
        if self.stats.m2_x <= 0.0 || !self.stats.m2_x.is_finite() {
            return None;
        }
        let deviation = (self.stats.m2_x / self.period as f64).sqrt();
        let projected = if self.zscore {
            (input.expect("complete window") - self.stats.mean_x) / deviation
        } else {
            deviation
        };
        projected.is_finite().then_some(projected)
    }

    #[cfg(test)]
    pub(crate) fn buffered(&self) -> usize {
        self.history.len()
    }
}

fn rolling_dispersion(input: &[Option<f64>], period: usize, zscore: bool) -> Vec<Option<f64>> {
    let mut window = RollingDispersionWindow::new(period, zscore);
    input
        .iter()
        .copied()
        .map(|value| window.push(value))
        .collect()
}

fn rolling_zscore(input: &[Option<f64>], period: usize) -> Vec<Option<f64>> {
    rolling_dispersion(input, period, true)
}

fn rolling_stddev(input: &[Option<f64>], period: usize) -> Vec<Option<f64>> {
    rolling_dispersion(input, period, false)
}

/// Compensated add/subtract for a sliding window, preserving small terms
/// when a large observation exits. Each update is constant time.
pub(crate) fn compensated_add(sum: &mut f64, correction: &mut f64, term: f64) {
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
    let mut window = MonotoneWindow::new(period, maximum);
    input.iter().copied().map(|v| window.push(v)).collect()
}

/// Inclusive closed-bar high-minus-low of an already typed numeric series.
/// Two monotone deques make both extrema O(1) amortized per bar. Missing or
/// nonfinite observations invalidate exactly the windows containing them;
/// constant *complete* windows produce the observed numeric zero.
pub fn rolling_range(input: &[Option<f64>], period: usize) -> Vec<Option<f64>> {
    if !(2..=1000).contains(&period) {
        return vec![None; input.len()];
    }
    let mut maxima = MonotoneWindow::new(period, true);
    let mut minima = MonotoneWindow::new(period, false);
    input
        .iter()
        .copied()
        .map(|value| match (maxima.push(value), minima.push(value)) {
            (Some(max), Some(min)) => {
                let width = max - min;
                width.is_finite().then_some(width)
            }
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod t15_extension_contract {
    use super::*;
    use serde_json::json;

    fn compile(
        steps: serde_json::Value,
        result_steps: &[&str],
    ) -> Result<CheckedComposition, String> {
        let request = serde_json::from_value(json!({
            "version": COMPOSITION_VERSION,
            "steps": steps,
            "result_steps": result_steps,
        }))
        .unwrap();
        CheckedComposition::compile(request, 20)
    }

    #[test]
    fn new_pure_operator_has_stable_serde_id_version_descriptor_and_numeric_oracle() {
        assert_eq!(
            serde_json::to_value(SeriesOp::RollingRange).unwrap(),
            "rolling_range"
        );
        let descriptor = series_operation_contract(SeriesOp::RollingRange);
        assert_eq!(
            serde_json::to_value(descriptor.op).unwrap(),
            "rolling_range"
        );
        assert_eq!(descriptor.version, 1);
        assert_eq!(descriptor.effects, OperationEffect::PureNoIo);
        assert_eq!(descriptor.output_unit, SeriesUnitRule::SameAsInput);
        assert_eq!(descriptor.period_min, Some(2));
        assert_eq!(descriptor.period_max, Some(1000));
        assert_eq!(
            rolling_range(&[Some(10.), Some(14.), Some(13.), Some(18.), Some(18.)], 3),
            vec![None, None, Some(4.), Some(5.), Some(5.)]
        );
        assert_eq!(
            rolling_range(&[Some(4.); 4], 2),
            vec![None, Some(0.), Some(0.), Some(0.)]
        );
        assert_eq!(
            rolling_range(&[Some(2.), None, Some(3.), Some(5.), Some(5.)], 2),
            vec![None, None, None, Some(2.), Some(0.)]
        );
        assert_eq!(
            rolling_range(&[Some(2.), Some(f64::NAN), Some(3.)], 2),
            vec![None, None, None]
        );
        assert_eq!(
            rolling_range(&[Some(f64::MAX), Some(-f64::MAX)], 2),
            vec![None, None]
        );

        // Deliberately independent O(period) reference, with holes and ties,
        // against the production O(bars) two-deque implementation.
        let observations = (0..2037)
            .map(|i| (i % 31 != 0).then_some((i * 37 % 101 - 50) as f64))
            .collect::<Vec<_>>();
        for period in [2usize, 3, 13, 31, 1000] {
            let actual = rolling_range(&observations, period);
            for (index, got) in actual.iter().copied().enumerate() {
                let expected = if index + 1 < period {
                    None
                } else {
                    let window = &observations[index + 1 - period..=index];
                    window
                        .iter()
                        .copied()
                        .collect::<Option<Vec<_>>>()
                        .map(|values| {
                            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
                            max - min
                        })
                };
                assert_eq!(got, expected, "period={period},bar={index}");
            }
        }
    }

    #[test]
    fn runtime_composes_new_operator_without_new_route_and_never_leaks_future() {
        let graph = json!([
            {"id":"width","op":"rolling_range","input":"close","period":3},
            {"id":"wide","op":"greater_than","input":"width","threshold":4.5},
            {"id":"first_wide","op":"rising_edge","input":"wide"},
            {"id":"smooth","op":"rolling_mean","input":"width","period":2},
            {"id":"relative","op":"returns_series","input":"close"},
            {"id":"return_span","op":"rolling_range","input":"relative","period":2}
        ]);
        let compiled = compile(
            graph.clone(),
            &["width", "wide", "first_wide", "smooth", "return_span"],
        )
        .unwrap();
        let close = [10., 14., 13., 18., 18., 12.];
        let at = compiled.evaluate_at(&close, 3);
        assert_eq!(at["width"], (Some(5.), Unit::Price));
        assert_eq!(at["wide"], (Some(1.), Unit::Boolean));
        assert_eq!(at["first_wide"], (Some(1.), Unit::Event));
        assert_eq!(at["smooth"], (Some(4.5), Unit::Price));
        assert_eq!(at["return_span"].1, Unit::Ratio);
        let mut future_changed = close.to_vec();
        future_changed[4..].fill(1000.);
        assert_eq!(at, compiled.evaluate_at(&future_changed, 3));

        // Users can change conditions and branches on the same installed
        // binary. No Rust strategy ID, routing entry or finance call is added.
        let mut altered = graph;
        altered[1]["threshold"] = json!(100.0);
        let different = compile(
            altered,
            &["width", "wide", "first_wide", "smooth", "return_span"],
        )
        .unwrap();
        assert_eq!(different.evaluate_at(&close, 3)["wide"].0, Some(0.));
        assert_eq!(different.evaluate_at(&close, 3)["first_wide"].0, Some(0.));
        assert_eq!(different.evaluate_at(&close, 3)["width"], at["width"]);

        let mut missing = close.to_vec();
        missing[2] = f64::NAN;
        let (values, causes) = compiled.evaluate_at_sources(
            &missing,
            CandlePriceInputs::default(),
            None,
            None,
            3,
            true,
        );
        assert_eq!(values["width"].0, None);
        assert_eq!(causes["width"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(causes["width"].origin_step, "close");
        assert_eq!(causes["width"].origin_index, 2);
    }

    #[test]
    fn compiler_rejects_malformed_extension_windows_units_and_future_outcomes() {
        for (steps, message) in [
            (
                json!([{"id":"range","op":"rolling_range","input":"close"}]),
                "composition.steps[0].period:INVALID:2..1000",
            ),
            (
                json!([{"id":"range","op":"rolling_range","input":"close","period":1}]),
                "composition.steps[0].period:INVALID:2..1000",
            ),
            (
                json!([{"id":"range","op":"rolling_range","input":"close","period":1001}]),
                "composition.steps[0].period:INVALID:2..1000",
            ),
            (
                json!([{"id":"range","op":"rolling_range","input":"close","period":2,"threshold":1}]),
                "composition.steps[0].threshold:UNEXPECTED",
            ),
            (
                json!([{"id":"range","op":"rolling_range","input":"close","period":2,"other":"open"}]),
                "composition.steps[0].other:UNEXPECTED",
            ),
            (
                json!([{"id":"flag","op":"greater_than","input":"close","threshold":1},
                    {"id":"range","op":"rolling_range","input":"flag","period":2}]),
                "composition.steps[1].input:UNIT_MISMATCH:expected=numeric_series:actual=boolean",
            ),
            (
                json!([{"id":"flag","op":"greater_than","input":"close","threshold":1},
                    {"id":"event","op":"rising_edge","input":"flag"},
                    {"id":"outcome","op":"forward_return_label","input":"close","when":"event","period":2},
                    {"id":"range","op":"rolling_range","input":"outcome","period":2}]),
                "composition.steps[3].input:UNIT_MISMATCH:expected=numeric_series:actual=outcome_ratio",
            ),
        ] {
            assert_eq!(compile(steps, &["range"]).unwrap_err(), message);
        }
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
    Contracts,
    /// Provider-native candle volume. Spot base quantity and derivative
    /// contract counts must not silently become interchangeable dimensions.
    NativeVolume,
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
/// The complete finite set of v1 Market composition input ports. These are
/// source contracts, not additional executable operators or source adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesSource {
    Open,
    High,
    Low,
    Close,
    Volume,
    OpenInterest,
    FundingRate,
}

/// Preserve v1 wire/discovery order while sharing admission semantics.
pub const SERIES_SOURCES: [SeriesSource; 7] = [
    SeriesSource::Open,
    SeriesSource::High,
    SeriesSource::Low,
    SeriesSource::Close,
    SeriesSource::Volume,
    SeriesSource::OpenInterest,
    SeriesSource::FundingRate,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesSourceAlignment {
    ClosedCandleGridNoForwardFill,
    ExactClosedBarTimestampNoForwardFill,
    SparseRealizedSettlementExactTimestampNoForwardFill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesSourceScope {
    SameInstrument,
    SwapOnlySameInstrument,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesSourceAccess {
    ReadOnlyMarketData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SeriesSourceOwner {
    #[serde(rename = "okx-research-service::analytics::market")]
    ResearchServiceMarket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesSourceVerification {
    RequestAlignmentAndBoundedArtifactIntegrity,
}

/// Denomination is not interchangeable with a numerical unit. In particular,
/// provider-native volume is not automatically a base-asset quantity, and a
/// swap contract count needs its own exchange-reference conversion evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesCurrencyBasis {
    QuotePerBase,
    NativeVolumeUnresolved,
    ContractsRequireReference,
    DimensionlessRate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesInstrumentProduct {
    Spot,
    Swap,
}

/// Derived from the exact request instrument, not a guessed settlement
/// currency. Inverse swap settlement remains unresolved without a verified
/// instrument reference; these fields alone confer no economic conversion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SeriesInstrumentBinding {
    pub instrument: String,
    pub base_asset: String,
    pub quote_currency: String,
    pub product: SeriesInstrumentProduct,
}

impl SeriesInstrumentBinding {
    fn parse(id: &str, index: usize) -> Result<Self, String> {
        let invalid = || format!("composition.bindings.instruments[{index}]:INVALID_INSTRUMENT");
        let parts = id.split('-').collect::<Vec<_>>();
        let product = match parts.as_slice() {
            [_, _] => SeriesInstrumentProduct::Spot,
            [_, _, suffix] if *suffix == "SWAP" => SeriesInstrumentProduct::Swap,
            _ => return Err(invalid()),
        };
        let valid_token = |token: &str| {
            !token.is_empty()
                && token.len() <= 32
                && token
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        };
        if !valid_token(parts[0]) || !valid_token(parts[1]) || parts[0] == parts[1] {
            return Err(invalid());
        }
        Ok(Self {
            instrument: id.into(),
            base_asset: parts[0].into(),
            quote_currency: parts[1].into(),
            product,
        })
    }
}

/// An ephemeral, checked source-port binding for one instrument and closed
/// range. No second graph state is persisted; a QueryPlan derives this value
/// from the existing finite source contracts before admission/source I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BoundSeriesSourcePort {
    pub source: SeriesSource,
    pub version: u16,
    pub instrument: SeriesInstrumentBinding,
    pub range: ClosedRange,
    pub unit: Unit,
    pub currency_basis: SeriesCurrencyBasis,
    pub time_basis: TimeBasis,
    pub availability: DataAvailability,
    pub alignment: SeriesSourceAlignment,
    pub publication_time_verified: bool,
}

/// Declarative input-port properties. `availability` is an obligation for
/// source admission, not proof that data was historically observable. A
/// single Market query binds one instrument/closed grid; this descriptor does
/// not grant cross-instrument or cross-currency algebra.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SeriesSourceContract {
    pub id: SeriesSource,
    pub version: u16,
    pub source: Source,
    pub unit: Unit,
    pub currency_basis: SeriesCurrencyBasis,
    pub output_shape: OperationShape,
    pub time_basis: TimeBasis,
    pub availability: DataAvailability,
    pub alignment: SeriesSourceAlignment,
    pub scope: SeriesSourceScope,
    pub access: SeriesSourceAccess,
    pub owner: SeriesSourceOwner,
    pub verification: SeriesSourceVerification,
    pub publication_time_verified: bool,
    pub graph_backtest_supported: bool,
}

impl SeriesSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::High => "high",
            Self::Low => "low",
            Self::Close => "close",
            Self::Volume => "volume",
            Self::OpenInterest => "open_interest",
            Self::FundingRate => "funding_rate",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        SERIES_SOURCES
            .into_iter()
            .find(|source| source.as_str() == name)
    }

    pub const fn contract(self) -> SeriesSourceContract {
        let (source, unit, time_basis, availability, alignment, scope) = match self {
            Self::Open | Self::High | Self::Low | Self::Close => (
                Source::Candles,
                Unit::Price,
                TimeBasis::ClosedBar,
                DataAvailability::CheckedPerRequest,
                SeriesSourceAlignment::ClosedCandleGridNoForwardFill,
                SeriesSourceScope::SameInstrument,
            ),
            Self::Volume => (
                Source::Candles,
                Unit::NativeVolume,
                TimeBasis::ClosedBar,
                DataAvailability::CheckedPerRequest,
                SeriesSourceAlignment::ClosedCandleGridNoForwardFill,
                SeriesSourceScope::SameInstrument,
            ),
            Self::OpenInterest => (
                Source::OpenInterest,
                Unit::Contracts,
                TimeBasis::OpenInterestEventTimeNotPublicationTime,
                DataAvailability::HistoricalPublicationTimeUnverified,
                SeriesSourceAlignment::ExactClosedBarTimestampNoForwardFill,
                SeriesSourceScope::SwapOnlySameInstrument,
            ),
            Self::FundingRate => (
                Source::Funding,
                Unit::Ratio,
                TimeBasis::RealizedFundingEventTimeNotPublicationTime,
                DataAvailability::HistoricalPublicationTimeUnverified,
                SeriesSourceAlignment::SparseRealizedSettlementExactTimestampNoForwardFill,
                SeriesSourceScope::SwapOnlySameInstrument,
            ),
        };
        SeriesSourceContract {
            id: self,
            version: 1,
            source,
            unit,
            currency_basis: match self {
                Self::Open | Self::High | Self::Low | Self::Close => {
                    SeriesCurrencyBasis::QuotePerBase
                }
                Self::Volume => SeriesCurrencyBasis::NativeVolumeUnresolved,
                Self::OpenInterest => SeriesCurrencyBasis::ContractsRequireReference,
                Self::FundingRate => SeriesCurrencyBasis::DimensionlessRate,
            },
            output_shape: OperationShape::Series,
            time_basis,
            availability,
            alignment,
            scope,
            access: SeriesSourceAccess::ReadOnlyMarketData,
            owner: SeriesSourceOwner::ResearchServiceMarket,
            verification: SeriesSourceVerification::RequestAlignmentAndBoundedArtifactIntegrity,
            // Closed bars are finalised observations, but this metadata does
            // not assert provider publication evidence for any input series.
            publication_time_verified: false,
            // Only finalised candle columns have physical ports in Research.
            // OI/funding event times do not establish historical publication.
            graph_backtest_supported: matches!(
                self,
                Self::Open | Self::High | Self::Low | Self::Close | Self::Volume
            ),
        }
    }

    /// Product, currency and temporal identity must all be bound for every
    /// referenced port. No historical publication claim is inferred here.
    pub fn bind(
        self,
        instrument: &str,
        range: ClosedRange,
        instrument_index: usize,
    ) -> Result<BoundSeriesSourcePort, String> {
        let identity = SeriesInstrumentBinding::parse(instrument, instrument_index)?;
        let contract = self.contract();
        if matches!(contract.scope, SeriesSourceScope::SwapOnlySameInstrument)
            && identity.product != SeriesInstrumentProduct::Swap
        {
            return Err(format!(
                "composition.sources.{}:UNSUPPORTED_PRODUCT",
                self.as_str()
            ));
        }
        Ok(BoundSeriesSourcePort {
            source: self,
            version: contract.version,
            instrument: identity,
            range,
            unit: contract.unit,
            currency_basis: contract.currency_basis,
            time_basis: contract.time_basis,
            availability: contract.availability,
            alignment: contract.alignment,
            publication_time_verified: contract.publication_time_verified,
        })
    }
}

pub fn series_source_contracts() -> Vec<SeriesSourceContract> {
    SERIES_SOURCES
        .into_iter()
        .map(SeriesSource::contract)
        .collect()
}

#[cfg(test)]
mod series_source_contract_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn finite_source_contracts_are_unique_typed_and_share_scalar_input_mapping() {
        let contracts = series_source_contracts();
        assert_eq!(contracts.len(), 7);
        assert_eq!(
            contracts
                .iter()
                .map(|contract| contract.id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "open",
                "high",
                "low",
                "close",
                "volume",
                "open_interest",
                "funding_rate"
            ]
        );
        let ids = contracts
            .iter()
            .map(|contract| contract.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), contracts.len());
        for contract in &contracts {
            assert_eq!(SeriesSource::parse(contract.id.as_str()), Some(contract.id));
            assert_eq!(contract.version, 1);
            assert_eq!(contract.output_shape, OperationShape::Series);
            assert_eq!(contract.access, SeriesSourceAccess::ReadOnlyMarketData);
            assert_eq!(contract.owner, SeriesSourceOwner::ResearchServiceMarket);
            assert_eq!(input(contract.id.as_str()).unwrap().unit, contract.unit);
            assert_eq!(input(contract.id.as_str()).unwrap().source, contract.source);
            assert!(!contract.publication_time_verified);
        }
        assert_eq!(SeriesSource::parse("peer_close"), None);
        assert_eq!(SeriesSource::Volume.contract().unit, Unit::NativeVolume);
        assert_eq!(SeriesSource::OpenInterest.contract().unit, Unit::Contracts);
        assert_eq!(SeriesSource::FundingRate.contract().unit, Unit::Ratio);
    }

    #[test]
    fn market_source_type_does_not_grant_unverified_graph_or_cross_product_access() {
        let eligible = series_source_contracts()
            .into_iter()
            .filter(|source| source.graph_backtest_supported)
            .map(|source| source.id)
            .collect::<Vec<_>>();
        assert_eq!(
            eligible,
            vec![
                SeriesSource::Open,
                SeriesSource::High,
                SeriesSource::Low,
                SeriesSource::Close,
                SeriesSource::Volume,
            ]
        );
        for id in [SeriesSource::OpenInterest, SeriesSource::FundingRate] {
            let source = id.contract();
            assert_eq!(
                source.availability,
                DataAvailability::HistoricalPublicationTimeUnverified
            );
            assert_eq!(source.scope, SeriesSourceScope::SwapOnlySameInstrument);
            assert!(!source.graph_backtest_supported);
        }
        assert_eq!(
            SeriesSource::OpenInterest.contract().alignment,
            SeriesSourceAlignment::ExactClosedBarTimestampNoForwardFill
        );
        assert_eq!(
            SeriesSource::FundingRate.contract().alignment,
            SeriesSourceAlignment::SparseRealizedSettlementExactTimestampNoForwardFill
        );
        let encoded = serde_json::to_value(SeriesSource::FundingRate.contract()).unwrap();
        assert_eq!(encoded["id"], "funding_rate");
        assert_eq!(
            encoded["time_basis"],
            "realized_funding_event_time_not_publication_time"
        );
        assert_eq!(encoded["currency_basis"], "dimensionless_rate");
    }

    #[test]
    fn source_port_binding_keeps_instrument_currency_units_and_time_explicit() {
        let range = ClosedRange::strict(0, 180_000, 60_000).unwrap();
        let spot = SeriesSource::Close.bind("BTC-USDT", range, 0).unwrap();
        let peer = SeriesSource::Close.bind("ETH-USDC", range, 1).unwrap();
        assert_eq!(spot.instrument.base_asset, "BTC");
        assert_eq!(spot.instrument.quote_currency, "USDT");
        assert_eq!(spot.instrument.product, SeriesInstrumentProduct::Spot);
        assert_eq!(spot.currency_basis, SeriesCurrencyBasis::QuotePerBase);
        assert_eq!(spot.time_basis, TimeBasis::ClosedBar);
        assert_eq!(spot.availability, DataAvailability::CheckedPerRequest);
        assert!(!spot.publication_time_verified);
        assert_ne!(spot.instrument, peer.instrument);
        assert_ne!(
            spot.instrument.quote_currency,
            peer.instrument.quote_currency
        );
        assert_eq!(spot.range, peer.range);

        let swap = "BTC-USD-SWAP";
        let volume = SeriesSource::Volume.bind(swap, range, 0).unwrap();
        let oi = SeriesSource::OpenInterest.bind(swap, range, 0).unwrap();
        let funding = SeriesSource::FundingRate.bind(swap, range, 0).unwrap();
        assert_eq!(
            volume.currency_basis,
            SeriesCurrencyBasis::NativeVolumeUnresolved
        );
        assert_eq!(
            oi.currency_basis,
            SeriesCurrencyBasis::ContractsRequireReference
        );
        assert_eq!(
            funding.currency_basis,
            SeriesCurrencyBasis::DimensionlessRate
        );
        assert_eq!(oi.instrument.quote_currency, "USD");
        assert_eq!(
            oi.availability,
            DataAvailability::HistoricalPublicationTimeUnverified
        );
        assert!(!oi.publication_time_verified);
        assert!(!funding.publication_time_verified);

        for source in [SeriesSource::OpenInterest, SeriesSource::FundingRate] {
            assert_eq!(
                source.bind("BTC-USDT", range, 0).unwrap_err(),
                format!(
                    "composition.sources.{}:UNSUPPORTED_PRODUCT",
                    source.as_str()
                )
            );
        }
        for invalid in ["btc-usdt", "BTC", "BTC-BTC", "BTC-USDT-DEC", "BTC--SWAP"] {
            assert_eq!(
                SeriesSource::Close.bind(invalid, range, 2).unwrap_err(),
                "composition.bindings.instruments[2]:INVALID_INSTRUMENT"
            );
        }
    }

    #[test]
    fn query_plan_binds_every_referenced_source_before_io_and_rechecks_snapshots() {
        let range = ClosedRange::strict(0, 300_000, 60_000).unwrap();
        let graph: CompositionRequest = serde_json::from_value(json!({
            "version": COMPOSITION_VERSION,
            "steps":[
                {"id":"oi_return","op":"returns_series","input":"open_interest"},
                {"id":"close_return","op":"returns_series","input":"close"}
            ],
            "result_steps":["oi_return","close_return"]
        }))
        .unwrap();
        let base = QueryPlan::compile(
            vec![],
            range,
            vec![Operator::Returns],
            BTreeMap::new(),
            vec![],
            None,
        )
        .unwrap()
        .with_composition(Some(graph.clone()))
        .unwrap();
        assert!(base.bound_series_source_ports().unwrap().is_empty());
        assert_eq!(
            base.clone()
                .with_resolved_universe(vec!["BTC-USDT".into()], false)
                .unwrap_err(),
            "composition.sources.open_interest:UNSUPPORTED_PRODUCT"
        );
        let checked = base
            .with_resolved_universe(vec!["BTC-USDT-SWAP".into(), "ETH-USDC-SWAP".into()], false)
            .unwrap();
        let ports = checked.bound_series_source_ports().unwrap();
        assert_eq!(ports.len(), 4);
        assert_eq!(ports[0].source, SeriesSource::Close);
        assert_eq!(ports[1].source, SeriesSource::OpenInterest);
        assert_eq!(ports[0].instrument.quote_currency, "USDT");
        assert_eq!(ports[2].instrument.quote_currency, "USDC");
        assert_eq!(ports[0].range, range);
        let mut forged = serde_json::to_value(&checked).unwrap();
        forged["universe"][0] = json!("BTC-USDT");
        assert!(
            serde_json::from_value::<QueryPlan>(forged)
                .unwrap_err()
                .to_string()
                .contains("composition.sources.open_interest:UNSUPPORTED_PRODUCT")
        );
        assert_eq!(
            QueryPlan::compile(
                vec!["BTC-USDT".into()],
                range,
                vec![Operator::Returns],
                BTreeMap::new(),
                vec![],
                None,
            )
            .unwrap()
            .with_composition(Some(graph))
            .unwrap_err(),
            "composition.sources.open_interest:UNSUPPORTED_PRODUCT"
        );
    }

    #[test]
    fn compiler_reserves_every_typed_source_and_resolves_its_owner_before_io() {
        for source in SERIES_SOURCES {
            let name = source.as_str();
            let duplicate: CompositionRequest = serde_json::from_value(json!({
                "version": COMPOSITION_VERSION,
                "steps": [{"id": name, "op": "rolling_mean", "input": "close", "period": 2}],
                "result_steps": [name]
            }))
            .unwrap();
            assert_eq!(
                CheckedComposition::compile(duplicate, 10).unwrap_err(),
                "composition.steps[0].id:DUPLICATE"
            );
            let request: CompositionRequest = serde_json::from_value(json!({
                "version": COMPOSITION_VERSION,
                "steps": [{"id": "mean", "op": "rolling_mean", "input": name, "period": 2}],
                "result_steps": ["mean"]
            }))
            .unwrap();
            let graph = CheckedComposition::compile(request, 10).unwrap();
            assert_eq!(graph.steps()[0].unit, source.contract().unit);
            assert_eq!(graph.sources(), BTreeSet::from([source.contract().source]));
        }
    }
}

fn composition_unit_name(unit: Unit) -> &'static str {
    match unit {
        Unit::Price => "price",
        Unit::Contracts => "contracts",
        Unit::NativeVolume => "native_volume",
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
#[serde(try_from = "StoredQueryPlan")]
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

/// Restore the existing artifact shape, then attest its cross-field bindings.
/// CheckedComposition separately validates every operator, input and unit;
/// this layer prevents a valid subgraph being paired with a forged cheap grid
/// or a source manifest that omits one of its dependencies.
#[derive(Deserialize)]
struct StoredQueryPlan {
    version: String,
    universe: Vec<String>,
    dataset_refs: Vec<String>,
    range: ClosedRange,
    timeframe_ms: u64,
    warmup_bars: u64,
    sources: Vec<Source>,
    operators: Vec<OperatorDescriptor>,
    filters: Vec<Filter>,
    ranking: Option<Rank>,
    result_projection: Vec<Operator>,
    parameters: BTreeMap<String, f64>,
    assumptions: Vec<String>,
    resource_budget_ref: String,
    #[serde(default)]
    composition: Option<CheckedComposition>,
}

impl TryFrom<StoredQueryPlan> for QueryPlan {
    type Error = String;

    fn try_from(snapshot: StoredQueryPlan) -> Result<Self, Self::Error> {
        let plan = Self {
            version: snapshot.version,
            universe: snapshot.universe,
            dataset_refs: snapshot.dataset_refs,
            range: snapshot.range,
            timeframe_ms: snapshot.timeframe_ms,
            warmup_bars: snapshot.warmup_bars,
            sources: snapshot.sources,
            operators: snapshot.operators,
            filters: snapshot.filters,
            ranking: snapshot.ranking,
            result_projection: snapshot.result_projection,
            parameters: snapshot.parameters,
            assumptions: snapshot.assumptions,
            resource_budget_ref: snapshot.resource_budget_ref,
            composition: snapshot.composition,
        };
        if let Some(graph) = &plan.composition {
            let cells = plan
                .range
                .expected_bars
                .checked_mul(graph.steps().len() as u64)
                .ok_or("query_plan.composition:CELLS_OVERFLOW")?;
            if cells != graph.estimated_cells() || plan.timeframe_ms != plan.range.timeframe_ms {
                return Err("query_plan.composition:RANGE_BINDING_MISMATCH".into());
            }
            let sources = graph.sources();
            if !sources.iter().all(|source| plan.sources.contains(source)) {
                return Err("query_plan.composition:SOURCE_BINDING_MISMATCH".into());
            }
            let total_bars = plan
                .range
                .expected_bars
                .checked_mul(plan.universe.len().max(1) as u64)
                .ok_or("query_plan.composition:GLOBAL_BARS_OVERFLOW")?;
            graph.validate_global_budget(total_bars)?;
            plan.bound_series_source_ports()?;
        }
        Ok(plan)
    }
}
/// One source/type mapping for plan compilation and public capability discovery.
pub fn input(name: &str) -> Result<Input, String> {
    if let Some(series_source) = SeriesSource::parse(name) {
        let contract = series_source.contract();
        return Ok(Input {
            name: name.into(),
            source: contract.source,
            unit: contract.unit,
        });
    }
    let (source, unit) = match name {
        "peer_close" => (Source::Candles, Unit::Price),
        "bid" | "ask" => (Source::Ticker, Unit::Price),
        "funding" => (Source::Funding, Unit::Ratio),
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
fn output_unit(op: Operator) -> Unit {
    use crate::operators::ScalarOutputUnit as U;
    match registry(op).output_unit {
        U::Price => Unit::Price,
        U::Ratio => Unit::Ratio,
        U::AnnualizedRatio => Unit::AnnualizedRatio,
        U::Coefficient => Unit::Coefficient,
        U::Index => Unit::Index,
        U::Bps => Unit::Bps,
        U::BaseQuantity => Unit::Quantity,
        U::QuoteCurrency => Unit::QuoteCurrency,
    }
}
impl QueryPlan {
    /// The single request-bound source-port schema for Market compositions.
    /// Every operation still runs in the existing per-instrument Rust owner;
    /// unlike a cross-instrument join, these independent bindings cannot
    /// silently exchange units, instruments or quote currencies.
    pub fn bound_series_source_ports(&self) -> Result<Vec<BoundSeriesSourcePort>, String> {
        let Some(composition) = &self.composition else {
            return Ok(Vec::new());
        };
        // Use the same projected dependency closure as source scheduling.
        // Dead branches are type-checked during compilation, but must not
        // cause source admission failures or trigger additional data reads.
        let sources = SERIES_SOURCES
            .into_iter()
            .filter(|source| composition.uses_source(source.as_str()))
            .collect::<Vec<_>>();
        let mut result = Vec::with_capacity(self.universe.len().saturating_mul(sources.len()));
        for (index, instrument) in self.universe.iter().enumerate() {
            for source in &sources {
                result.push(source.bind(instrument, self.range, index)?);
            }
        }
        Ok(result)
    }

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
                output_unit: output_unit(*op),
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
            // Explicit instruments must fail before any external universe or
            // market data read. Global universes are checked when resolved.
            self.bound_series_source_ports()?;
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
        self.bound_series_source_ports()?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t02_t03_t13_serialized_checked_plan_roundtrips_only_after_recompilation() {
        let original = CheckedComposition::compile(two_step_composition(), 7).unwrap();
        let snapshot = serde_json::to_value(&original).unwrap();
        let restored: CheckedComposition = serde_json::from_value(snapshot.clone()).unwrap();
        assert_eq!(restored, original);
        assert_eq!(restored.version(), COMPOSITION_VERSION);
        assert_eq!(restored.steps().len(), 2);
        assert_eq!(restored.result_steps(), &["w1".to_owned()]);
        assert_eq!(restored.estimated_cells(), 14);
        assert_eq!(
            restored.evaluate(&[100.0, 110.0, 99.0]),
            original.evaluate(&[100.0, 110.0, 99.0])
        );
        // A durable QueryPlan with the same checked graph remains readable.
        let range = ClosedRange::strict(0, 420_000, 60_000).unwrap();
        let query = QueryPlan::compile(
            vec!["BTC-USDT".into()],
            range,
            vec![Operator::Returns],
            BTreeMap::new(),
            vec![],
            None,
        )
        .unwrap()
        .with_composition(Some(two_step_composition()))
        .unwrap();
        let wire = serde_json::to_value(&query).unwrap();
        let decoded: QueryPlan = serde_json::from_value(wire).unwrap();
        assert_eq!(decoded, query);
    }

    #[test]
    fn t02_t03_checked_graph_restore_rejects_forged_units_edges_depth_and_limits() {
        let baseline =
            serde_json::to_value(CheckedComposition::compile(two_step_composition(), 7).unwrap())
                .unwrap();
        let mutations: Vec<(&str, serde_json::Value)> = vec![
            ("unit", {
                let mut v = baseline.clone();
                v["steps"][1]["unit"] = serde_json::json!("price");
                v
            }),
            ("depth", {
                let mut v = baseline.clone();
                v["steps"][1]["depth"] = serde_json::json!(1);
                v
            }),
            ("output", {
                let mut v = baseline.clone();
                v["result_steps"] = serde_json::json!(["not_a_step"]);
                v
            }),
            ("forward", {
                let mut v = baseline.clone();
                v["steps"][0]["step"]["input"] = serde_json::json!("w1");
                v
            }),
            ("unit_from_op", {
                let mut v = baseline.clone();
                v["steps"][1]["step"]["op"] = serde_json::json!("returns_series");
                v
            }),
            ("version", {
                let mut v = baseline.clone();
                v["version"] = serde_json::json!("future-version");
                v
            }),
            ("non_integral_cells", {
                let mut v = baseline.clone();
                v["estimated_cells"] = serde_json::json!(13);
                v
            }),
            ("over_budget", {
                let mut v = baseline.clone();
                v["estimated_cells"] = serde_json::json!(8_000_002);
                v
            }),
            ("undeclared", {
                let mut v = baseline.clone();
                v["authority"] = serde_json::json!("trade");
                v
            }),
        ];
        for (name, forged) in mutations {
            assert!(
                serde_json::from_value::<CheckedComposition>(forged).is_err(),
                "forged {name} cannot construct a checked plan"
            );
        }
        // This is the same decoding route as a persisted QueryPlan snapshot.
        let range = ClosedRange::strict(0, 420_000, 60_000).unwrap();
        let query = QueryPlan::compile(
            vec!["BTC-USDT".into()],
            range,
            vec![Operator::Returns],
            BTreeMap::new(),
            vec![],
            None,
        )
        .unwrap()
        .with_composition(Some(two_step_composition()))
        .unwrap();
        let mut wire = serde_json::to_value(query).unwrap();
        for (name, path, value) in [
            (
                "binding",
                vec!["composition", "steps", "0", "unit"],
                serde_json::json!("boolean"),
            ),
            (
                "timeframe",
                vec!["timeframe_ms"],
                serde_json::json!(120_000),
            ),
            (
                "range_budget",
                vec!["range", "expected_bars"],
                serde_json::json!(1),
            ),
            ("required_source", vec!["sources"], serde_json::json!([])),
            (
                "composition_cells",
                vec!["composition", "estimated_cells"],
                serde_json::json!(12),
            ),
        ] {
            let mut invalid = wire.clone();
            let mut target = &mut invalid;
            for segment in &path {
                target = if let Ok(index) = segment.parse::<usize>() {
                    &mut target[index]
                } else {
                    &mut target[*segment]
                };
            }
            *target = value;
            assert!(
                serde_json::from_value::<QueryPlan>(invalid).is_err(),
                "forged {name} must not restore a checked query plan"
            );
        }
        // Keep the original intact for legacy immutable artifact replay.
        let valid: QueryPlan = serde_json::from_value(wire.take()).unwrap();
        assert_eq!(valid.composition.unwrap().estimated_cells(), 14);
    }

    #[test]
    fn t02_t04_t07_native_volume_is_composable_but_never_an_implicit_price_or_contract() {
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"short","op":"rolling_mean","input":"volume","period":2},
                {"id":"long","op":"rolling_mean","input":"volume","period":3},
                {"id":"lift","op":"greater_than_series","input":"short","other":"long"},
                {"id":"entry","op":"rising_edge","input":"lift"},
                {"id":"change","op":"returns_series","input":"volume"}
            ],
            "result_steps":["short","lift","entry","change"]
        }))
        .unwrap();
        let checked = CheckedComposition::compile(request, 5).unwrap();
        assert_eq!(input("volume").unwrap().unit, Unit::NativeVolume);
        assert_eq!(checked.steps[0].unit, Unit::NativeVolume);
        assert_eq!(checked.steps[1].unit, Unit::NativeVolume);
        assert_eq!(checked.steps[2].unit, Unit::Boolean);
        assert_eq!(checked.steps[3].unit, Unit::Event);
        assert_eq!(checked.steps[4].unit, Unit::Ratio);
        assert_eq!(checked.sources(), BTreeSet::from([Source::Candles]));
        assert!(checked.uses_source("volume"));
        assert!(!checked.uses_source("open"));
        let close = [100.0; 5];
        let volume = [10.0, 10.0, 10.0, 10.0, 30.0];
        let candles = CandlePriceInputs {
            volume: Some(&volume),
            ..Default::default()
        };
        let (result, causes) = checked
            .evaluate_with_candle_sources_diagnostics(&close, candles, None, None)
            .unwrap();
        assert!(causes.is_empty());
        assert_eq!(result["short"], (Some(20.0), Unit::NativeVolume));
        assert_eq!(result["lift"], (Some(1.0), Unit::Boolean));
        assert_eq!(result["entry"], (Some(1.0), Unit::Event));
        assert_eq!(result["change"], (Some(2.0), Unit::Ratio));
        let mut missing = volume;
        missing[4] = f64::NAN;
        let (partial, causes) = checked
            .evaluate_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs {
                    volume: Some(&missing),
                    ..Default::default()
                },
                None,
                None,
            )
            .unwrap();
        assert_eq!(partial["lift"].0, None);
        assert_eq!(causes["lift"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(causes["lift"].origin_step, "volume");
        let mut negative = volume;
        negative[4] = -1.0;
        assert_eq!(
            checked
                .evaluate_with_candle_sources_diagnostics(
                    &close,
                    CandlePriceInputs {
                        volume: Some(&negative),
                        ..Default::default()
                    },
                    None,
                    None,
                )
                .unwrap_err(),
            "composition.sources.volume:INVALID_NON_NEGATIVE_VOLUME"
        );
        let mut zero = volume;
        zero[4] = 0.0;
        let (values, causes) = checked
            .evaluate_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs {
                    volume: Some(&zero),
                    ..Default::default()
                },
                None,
                None,
            )
            .unwrap();
        assert_eq!(values["short"].0, Some(5.0));
        assert_eq!(values["change"].0, None);
        assert_eq!(causes["change"].reason, "NON_POSITIVE_RETURN_INPUT");
    }

    #[test]
    fn t02_native_volume_static_mismatch_and_source_shadowing_fail_before_io() {
        let build = |input: &str, other: &str| {
            serde_json::from_value::<CompositionRequest>(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"comparison","op":"greater_than_series","input":input,"other":other}],
            "result_steps":["comparison"]
        })).unwrap()
        };
        assert_eq!(
            CheckedComposition::compile(build("volume", "close"), 5).unwrap_err(),
            "composition.steps[0].other:UNIT_MISMATCH:expected=native_volume:actual=price"
        );
        assert_eq!(
            CheckedComposition::compile(build("volume", "open_interest"), 5).unwrap_err(),
            "composition.steps[0].other:UNIT_MISMATCH:expected=native_volume:actual=contracts"
        );
        let shadow: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"volume","op":"rolling_mean","input":"close","period":2}],
            "result_steps":["volume"]
        }))
        .unwrap();
        assert_eq!(
            CheckedComposition::compile(shadow, 5).unwrap_err(),
            "composition.steps[0].id:DUPLICATE"
        );
    }

    #[test]
    fn t03_t07_t15_two_price_windows_compose_into_a_causal_crossing_event() {
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"fast","op":"rolling_mean","input":"close","period":1},
                {"id":"slow","op":"rolling_mean","input":"close","period":3},
                {"id":"above","op":"greater_than_series","input":"fast","other":"slow"},
                {"id":"cross","op":"rising_edge","input":"above"}
            ],
            "result_steps":["above","cross"]
        }))
        .unwrap();
        let checked = CheckedComposition::compile(request, 5).unwrap();
        assert_eq!(checked.sources(), BTreeSet::from([Source::Candles]));
        assert_eq!(checked.steps[2].unit, Unit::Boolean);
        assert_eq!(checked.steps[3].unit, Unit::Event);
        let close = [3.0, 2.0, 1.0, 2.0, 4.0];
        assert_eq!(checked.evaluate_at(&close, 2)["above"].0, Some(0.0));
        assert_eq!(checked.evaluate_at(&close, 3)["cross"].0, Some(1.0));
        assert_eq!(checked.evaluate(&close)["cross"].0, Some(0.0));
        let mut future_changed = close;
        future_changed[4] = 1000.0;
        assert_eq!(
            checked.evaluate_at(&future_changed, 3),
            checked.evaluate_at(&close, 3)
        );
    }

    #[test]
    fn t02_t03_t04_ohlc_composition_has_strict_units_and_missing_source_diagnostics() {
        let request: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"body","op":"greater_than_series","input":"close","other":"open"},
                {"id":"spread","op":"greater_than_series","input":"high","other":"low"},
                {"id":"signal","op":"and","input":"body","other":"spread"}
            ],
            "result_steps":["signal"]
        }))
        .unwrap();
        let checked = CheckedComposition::compile(request.clone(), 3).unwrap();
        assert_eq!(checked.sources(), BTreeSet::from([Source::Candles]));
        assert!(checked.uses_source("open"));
        assert!(checked.uses_source("high"));
        assert!(checked.uses_source("low"));
        let close = [100.0, 103.0, 110.0];
        let open = [99.0, 102.0, 105.0];
        let high = [102.0, 105.0, f64::NAN];
        let low = [98.0, 101.0, 104.0];
        let candles = CandlePriceInputs {
            open: Some(&open),
            high: Some(&high),
            low: Some(&low),
            volume: None,
        };
        let (values, causes) = checked
            .evaluate_with_candle_sources_diagnostics(&close, candles, None, None)
            .unwrap();
        assert_eq!(values["signal"].0, None);
        assert_eq!(causes["signal"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(causes["signal"].origin_step, "high");
        let mut valid_high = [102.0, 105.0, 111.0];
        let valid = checked
            .evaluate_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs {
                    high: Some(&valid_high),
                    ..candles
                },
                None,
                None,
            )
            .unwrap();
        assert_eq!(valid.0["signal"], (Some(1.0), Unit::Boolean));
        valid_high[2] = 104.0;
        let invalid = checked
            .evaluate_with_candle_sources_diagnostics(
                &close,
                CandlePriceInputs {
                    high: Some(&valid_high),
                    ..candles
                },
                None,
                None,
            )
            .unwrap();
        assert_eq!(invalid.0["signal"].0, Some(0.0));
        assert_eq!(
            checked
                .evaluate_with_candle_sources_diagnostics(
                    &close,
                    CandlePriceInputs {
                        high: None,
                        ..candles
                    },
                    None,
                    None,
                )
                .unwrap_err(),
            "composition.sources.high:REQUIRED"
        );
        assert_eq!(
            checked
                .evaluate_with_candle_sources_diagnostics(
                    &close,
                    CandlePriceInputs {
                        low: Some(&[1.0]),
                        ..candles
                    },
                    None,
                    None,
                )
                .unwrap_err(),
            "composition.sources.low:ALIGNMENT_LENGTH_MISMATCH"
        );

        let mut wrong_units = request.clone();
        wrong_units.steps.insert(
            0,
            SeriesStep {
                id: "r".into(),
                op: SeriesOp::ReturnsSeries,
                input: "close".into(),
                other: None,
                period: None,
                threshold: None,
                when: None,
            },
        );
        wrong_units.steps[1].other = Some("r".into());
        assert_eq!(
            CheckedComposition::compile(wrong_units, 3).unwrap_err(),
            "composition.steps[1].other:UNIT_MISMATCH:expected=price:actual=ratio"
        );
        let mut no_second = request.clone();
        no_second.steps[0].other = None;
        assert_eq!(
            CheckedComposition::compile(no_second, 3).unwrap_err(),
            "composition.steps[0].other:REQUIRED"
        );
        let mut bad_threshold = request.clone();
        bad_threshold.steps[0].threshold = Some(0.0);
        assert_eq!(
            CheckedComposition::compile(bad_threshold, 3).unwrap_err(),
            "composition.steps[0].threshold:UNEXPECTED"
        );
        let mut unprojected = request;
        unprojected.result_steps = vec!["body".into()];
        let checked = CheckedComposition::compile(unprojected, 3).unwrap();
        assert!(checked.uses_source("open"));
        assert!(!checked.uses_source("high"));
        assert!(!checked.uses_source("low"));
    }
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
    fn t10_only_projected_dependency_closure_executes_while_all_declarations_validate() {
        let baseline = two_step_composition();
        let mut request = baseline.clone();
        // These valid but unprojected branches must neither become extra
        // public data reads nor change the scalar value or its diagnostics.
        request.steps.extend([
            SeriesStep {
                id: "dead_oi".into(),
                op: SeriesOp::ReturnsSeries,
                input: "open_interest".into(),
                period: None,
                threshold: None,
                when: None,
                other: None,
            },
            SeriesStep {
                id: "dead_funding".into(),
                op: SeriesOp::RollingMean,
                input: "funding_rate".into(),
                period: Some(2),
                threshold: None,
                when: None,
                other: None,
            },
            SeriesStep {
                id: "dead_joint".into(),
                op: SeriesOp::And,
                input: "dead_oi".into(),
                period: None,
                threshold: None,
                when: None,
                other: Some("dead_funding".into()),
            },
        ]);
        // An unused invalid step remains a pre-I/O error. The graph cannot
        // hide a future-reference or an incompatible unit in a dead branch.
        assert!(
            CheckedComposition::compile(request.clone(), 3)
                .unwrap_err()
                .contains("UNIT_MISMATCH")
        );
        request.steps[4].op = SeriesOp::SubtractSeries;
        request.steps[4].input = "dead_funding".into();
        request.steps[4].other = Some("dead_funding".into());
        let checked = CheckedComposition::compile(request, 3).unwrap();
        let plain = CheckedComposition::compile(baseline, 3).unwrap();
        assert_eq!(checked.steps.len(), 5);
        assert_eq!(
            checked.reachable_steps(),
            BTreeSet::from(["close", "r1", "w1"])
        );
        assert_eq!(checked.sources(), BTreeSet::from([Source::Candles]));
        assert_eq!(checked.estimated_cells, 15); // conservative declared budget
        let prices = [100.0, 110.0, 99.0];
        assert_eq!(
            checked.evaluate_with_diagnostics(&prices),
            plain.evaluate_with_diagnostics(&prices)
        );
        let oi = [None; 3];
        let funding = [None; 3];
        assert_eq!(
            checked
                .evaluate_with_sources_diagnostics(&prices, Some(&oi), Some(&funding))
                .unwrap(),
            plain.evaluate_with_diagnostics(&prices)
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
        for (i, step) in chained.steps.iter_mut().enumerate().take(9).skip(1) {
            step.input = format!("w{}", i - 1);
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
mod t07_t15_symmetric_causal_signals {
    use super::*;
    use serde_json::json;

    fn compiled(steps: serde_json::Value, results: &[&str]) -> Result<CheckedComposition, String> {
        CheckedComposition::compile(
            serde_json::from_value(json!({
                "version": COMPOSITION_VERSION,
                "steps": steps,
                "result_steps": results
            }))
            .unwrap(),
            100,
        )
    }

    #[test]
    fn population_stddev_matches_independent_oracle_on_full_windows_and_gaps() {
        let input = [
            Some(-0.1),
            Some(0.0),
            Some(0.1),
            Some(0.1),
            None,
            Some(0.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
        ];
        let observed = rolling_stddev(&input, 3);
        for (i, value) in observed.iter().enumerate() {
            let expected = if i >= 2 {
                let window = &input[i - 2..=i];
                if window.iter().all(Option::is_some) {
                    let mean = window.iter().map(|n| n.unwrap()).sum::<f64>() / 3.0;
                    Some(
                        (window
                            .iter()
                            .map(|n| (n.unwrap() - mean).powi(2))
                            .sum::<f64>()
                            / 3.0)
                            .sqrt(),
                    )
                } else {
                    None
                }
            } else {
                None
            };
            assert_eq!(value.is_some(), expected.is_some(), "bar={i}");
            if let (Some(value), Some(expected)) = (value, expected) {
                assert!(
                    (value - expected).abs() < 1e-12,
                    "bar={i} actual={value} expected={expected}"
                );
            }
        }
        assert_eq!(observed[7], Some(0.0));
        assert_eq!(observed[8], Some(0.0));
        assert_eq!(rolling_zscore(&input, 3)[7], None);
        assert_eq!(rolling_zscore(&input, 3)[8], None);
        let offset = (0..200)
            .map(|i| Some(1e12 + (i % 5) as f64))
            .collect::<Vec<_>>();
        let stable = rolling_stddev(&offset, 5);
        for value in stable.iter().skip(4) {
            assert!((value.unwrap() - 2f64.sqrt()).abs() < 0.02);
        }
    }

    #[test]
    fn opposite_thresholds_and_falling_edge_are_typed_and_future_invariant() {
        let plan = compiled(json!([
            {"id":"average","op":"rolling_mean","input":"close","period":2},
            {"id":"risk","op":"rolling_stddev","input":"close","period":2},
            {"id":"lower","op":"less_than","input":"close","threshold":100.0},
            {"id":"below_avg","op":"less_than_series","input":"close","other":"average"},
            {"id":"exit","op":"falling_edge","input":"lower"},
            {"id":"outcome","op":"forward_return_label","input":"close","when":"exit","period":1}
        ]), &["risk", "below_avg", "lower", "exit", "outcome"]).unwrap();
        assert_eq!(plan.steps[1].unit, Unit::Price);
        assert_eq!(plan.steps[2].unit, Unit::Boolean);
        assert_eq!(plan.steps[4].unit, Unit::Event);
        assert_eq!(plan.steps[5].unit, Unit::OutcomeRatio);
        let prices = [100.0, 90.0, 80.0, 100.0, 110.0];
        assert_eq!(plan.evaluate_at(&prices, 2)["below_avg"].0, Some(1.0));
        assert_eq!(plan.evaluate_at(&prices, 2)["exit"].0, Some(0.0));
        let event = plan.evaluate_at(&prices, 3);
        assert_eq!(event["lower"].0, Some(0.0));
        assert_eq!(event["exit"].0, Some(1.0));
        assert_eq!(event["risk"].0, Some(10.0));
        assert!((event["outcome"].0.unwrap() - 0.1).abs() < 1e-12);
        let mut future = prices;
        future[4] = 200.0;
        let changed = plan.evaluate_at(&future, 3);
        for field in ["risk", "below_avg", "lower", "exit"] {
            assert_eq!(event[field], changed[field], "causal feature {field}");
        }
        assert_ne!(event["outcome"], changed["outcome"]);
        assert_eq!(plan.evaluate_at(&prices, 4)["exit"].0, Some(0.0));
        let (_, causes) = plan.evaluate_with_diagnostics(&prices);
        assert_eq!(causes["outcome"].reason, "OUTCOME_UNCONFIRMED");
        let gap = [100.0, 90.0, f64::NAN, 100.0];
        let (_, causes) = plan.evaluate_with_diagnostics(&gap);
        assert_eq!(causes["exit"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(causes["exit"].origin_step, "close");
        assert_eq!(causes["risk"].reason, "SOURCE_OBSERVATION_MISSING");
    }

    #[test]
    fn validation_rejects_untyped_lower_edges_and_future_label_reuse() {
        let reject = |steps, results: &[&str]| compiled(steps, results).unwrap_err();
        assert_eq!(
            reject(
                json!([{"id":"risk","op":"rolling_stddev","input":"close","period":1}]),
                &["risk"]
            ),
            "composition.steps[0].period:INVALID:2..1000"
        );
        assert_eq!(
            reject(
                json!([{"id":"risk","op":"rolling_stddev","input":"close","period":2,"threshold":1}]),
                &["risk"]
            ),
            "composition.steps[0].threshold:UNEXPECTED"
        );
        assert_eq!(
            reject(
                json!([{"id":"below","op":"less_than","input":"close"}]),
                &["below"]
            ),
            "composition.steps[0].threshold:INVALID"
        );
        assert_eq!(
            reject(
                json!([{"id":"edge","op":"falling_edge","input":"close"}]),
                &["edge"]
            ),
            "composition.steps[0].input:UNIT_MISMATCH:expected=boolean:actual=price"
        );
        assert_eq!(
            reject(
                json!([
                    {"id":"ret","op":"returns_series","input":"close"},
                    {"id":"below","op":"less_than_series","input":"close","other":"ret"}
                ]),
                &["below"]
            ),
            "composition.steps[1].other:UNIT_MISMATCH:expected=price:actual=ratio"
        );
        assert_eq!(
            reject(
                json!([
                    {"id":"cond","op":"less_than","input":"close","threshold":100},
                    {"id":"event","op":"falling_edge","input":"cond"},
                    {"id":"label","op":"forward_return_label","input":"close","when":"event","period":1},
                    {"id":"leak","op":"rolling_stddev","input":"label","period":2}
                ]),
                &["leak"]
            ),
            "composition.steps[3].input:UNIT_MISMATCH:expected=numeric_series:actual=outcome_ratio"
        );
        assert_eq!(
            reject(
                json!([
                    {"id":"v","op":"rolling_mean","input":"volume","period":2},
                    {"id":"wrong","op":"less_than_series","input":"v","other":"close"}
                ]),
                &["wrong"]
            ),
            "composition.steps[1].other:UNIT_MISMATCH:expected=native_volume:actual=price"
        );
    }
}

#[cfg(test)]
mod t02_t06_multisource_regression {
    use super::*;

    #[test]
    fn t15_rolling_zscore_is_typed_causal_and_matches_independent_oracle() {
        let spec: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"normalized","op":"rolling_zscore","input":"close","period":3},
                {"id":"above","op":"greater_than","input":"normalized","threshold":0.5},
                {"id":"rise","op":"rising_edge","input":"above"}
            ],"result_steps":["normalized","above","rise"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(spec.clone(), 100).unwrap();
        assert_eq!(plan.steps[0].unit, Unit::Coefficient);
        assert_eq!(plan.steps[1].unit, Unit::Boolean);
        assert_eq!(
            plan.result_sources()["rise"],
            BTreeSet::from([Source::Candles])
        );
        let prices = [100., 110., 99., 105., 115.];
        let first = plan.evaluate_at(&prices, 2);
        let oracle = (99. - 103.) / (74.0f64 / 3.0).sqrt();
        assert!((first["normalized"].0.unwrap() - oracle).abs() < 1e-12);
        let mut changed_future = prices;
        changed_future[3] *= 2.0;
        changed_future[4] *= 2.0;
        assert_eq!(
            plan.evaluate_at(&prices, 2),
            plan.evaluate_at(&changed_future, 2)
        );
        assert_eq!(
            plan.evaluate_with_diagnostics(&prices[..2]).1["normalized"].reason,
            "WARMUP"
        );

        let mut bad = spec.clone();
        bad.steps[0].period = Some(1);
        assert_eq!(
            CheckedComposition::compile(bad, 100).unwrap_err(),
            "composition.steps[0].period:INVALID:2..1000"
        );
        let mut bad = spec.clone();
        bad.steps[0].threshold = Some(1.0);
        assert_eq!(
            CheckedComposition::compile(bad, 100).unwrap_err(),
            "composition.steps[0].threshold:UNEXPECTED"
        );
        let mut bad = spec.clone();
        bad.steps[0].other = Some("close".into());
        assert_eq!(
            CheckedComposition::compile(bad, 100).unwrap_err(),
            "composition.steps[0].other:UNEXPECTED"
        );
        let mut bad = spec.clone();
        bad.steps.push(SeriesStep {
            id: "miscast".into(),
            op: SeriesOp::RollingZscore,
            input: "above".into(),
            period: Some(2),
            threshold: None,
            when: None,
            other: None,
        });
        assert_eq!(
            CheckedComposition::compile(bad, 100).unwrap_err(),
            "composition.steps[3].input:UNIT_MISMATCH:expected=numeric_series:actual=boolean"
        );
        let mut bad = spec;
        bad.steps[0].input = "above".into();
        assert!(
            CheckedComposition::compile(bad, 100)
                .unwrap_err()
                .contains("UNKNOWN_OR_FORWARD_STEP")
        );
    }

    #[test]
    fn t15_zscore_preserves_sparse_gaps_zero_variance_and_linear_budget() {
        let spec: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"standard","op":"rolling_zscore","input":"close","period":1000}],
            "result_steps":["standard"]
        }))
        .unwrap();
        let checked = CheckedComposition::compile(spec, 1_000_000).unwrap();
        assert!(checked.validate_global_budget(1_000_000).is_ok());
        assert_eq!(series_cost_per_bar(SeriesOp::RollingZscore), 6);
        let prices = (0..100_000)
            .map(|i| Some(1_000_000_000_000.0 + ((i * 7) % 191) as f64 * 0.25))
            .collect::<Vec<_>>();
        let output = rolling_zscore(&prices, 1000);
        assert!(output[..999].iter().all(Option::is_none));
        for index in [999usize, 4096, 9999, 99_999] {
            let window = &prices[index + 1 - 1000..=index];
            let mean = window
                .iter()
                .map(|v| (v.unwrap() - 1_000_000_000_000.0) / 1000.0)
                .sum::<f64>()
                + 1_000_000_000_000.0;
            let variance = window
                .iter()
                .map(|v| (v.unwrap() - mean).powi(2))
                .sum::<f64>()
                / 1000.0;
            let expected = (prices[index].unwrap() - mean) / variance.sqrt();
            assert!(
                (output[index].unwrap() - expected).abs() < 0.001,
                "index={index}"
            );
        }
        let mut interrupted = prices;
        interrupted[99_500] = None;
        let gaps = rolling_zscore(&interrupted, 1000);
        assert!(gaps[99_999].is_none());
        let (_out, causes) = checked.evaluate_with_diagnostics(&vec![5.0; 1000]);
        assert_eq!(causes["standard"].reason, "ZERO_VARIANCE");
        let (_, causes) = checked.evaluate_with_diagnostics(&[]);
        assert_eq!(causes["standard"].reason, "WARMUP");
        let mut interrupted = vec![5.0; 1000];
        interrupted[500] = f64::NAN;
        let (_, causes) = checked.evaluate_with_diagnostics(&interrupted);
        assert_eq!(causes["standard"].reason, "SOURCE_OBSERVATION_MISSING");
    }

    #[test]
    fn t15_zscore_of_funding_events_does_not_replace_missing_rates_with_zero() {
        let spec: CompositionRequest = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[{"id":"rate_deviation","op":"rolling_zscore","input":"funding_rate","period":2}],
            "result_steps":["rate_deviation"]
        })).unwrap();
        let plan = CheckedComposition::compile(spec, 4).unwrap();
        assert_eq!(plan.steps[0].unit, Unit::Coefficient);
        assert_eq!(
            plan.result_sources()["rate_deviation"],
            BTreeSet::from([Source::Funding])
        );
        let close = [f64::NAN; 4];
        let rates = [None, Some(0.01), None, Some(-0.01)];
        let (results, diagnostics) = plan
            .evaluate_with_sources_diagnostics(&close, None, Some(&rates))
            .unwrap();
        assert!(results["rate_deviation"].0.is_none());
        assert_eq!(
            diagnostics["rate_deviation"].reason,
            "SOURCE_OBSERVATION_MISSING"
        );
        assert_eq!(diagnostics["rate_deviation"].origin_step, "funding_rate");
        let dense = [None, Some(-0.01), Some(0.01), Some(0.03)];
        let (results, _) = plan
            .evaluate_with_sources_diagnostics(&close, None, Some(&dense))
            .unwrap();
        assert!((results["rate_deviation"].0.unwrap() - 1.0).abs() < 1e-12);
    }

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
                    origin_step: "relative".into(),
                    origin_index: if id == "relative" {
                        3
                    } else if id == "delayed" {
                        2
                    } else {
                        1
                    },
                }
            );
        }
        assert!(!causes.contains_key("price_return"));
        let (_, warmup) = plan
            .evaluate_with_open_interest_diagnostics(&prices[..1], &flat_oi[..1])
            .unwrap();
        assert_eq!(warmup["relative"].reason, "WARMUP");
        assert_eq!(warmup["relative"].origin_step, "price_return");
        assert_eq!(warmup["relative"].origin_index, 0);
        let mut incomplete_oi = flat_oi;
        incomplete_oi[3] = None;
        let (_, missing) = plan
            .evaluate_with_open_interest_diagnostics(&prices, &incomplete_oi)
            .unwrap();
        assert_eq!(missing["relative"].reason, "SOURCE_OBSERVATION_MISSING");
        assert_eq!(missing["relative"].origin_step, "open_interest");
        assert_eq!(missing["relative"].origin_index, 3);

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
        assert_eq!(unconfirmed["label"].origin_index, 1);
    }

    #[test]
    fn t07_out_of_range_historical_projection_is_null_not_a_panic_or_future_read() {
        let spec = serde_json::from_value(serde_json::json!({
            "version":COMPOSITION_VERSION,
            "steps":[
                {"id":"r1","op":"returns_series","input":"close"},
                {"id":"w1","op":"rolling_mean","input":"r1","period":2}
            ],
            "result_steps":["w1"]
        }))
        .unwrap();
        let plan = CheckedComposition::compile(spec, 3).unwrap();
        assert_eq!(
            plan.evaluate_at(&[100., 110., 105.], usize::MAX)["w1"].0,
            None
        );
        let (_, causes) = plan.evaluate_with_diagnostics(&[]);
        assert_eq!(causes["w1"].reason, "WARMUP");
        assert_eq!(causes["w1"].origin_step, "w1");
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
