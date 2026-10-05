use std::str::FromStr;

use okx_analysis::{
    BaselineStrategyKind, ValidationCostStress, ValidationSampleStatistics,
    analyze_validation_cost_stress, analyze_validation_pnl_samples,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    BUILD_SOURCE_TREE, PreHoldoutEvidence, ReplayEvidenceClass, ReplayMechanicsProvenance, ReplayStatus,
    ResearchArtifactStore, ResearchError, ResearchRange, VALIDATION_PROMOTION_CRITERIA_V2,
    VALIDATION_SPEC_SCHEMA_V2, ValidationEvidenceReadiness, ValidationPartitionRole,
    ValidationRobustnessEvidence, ValidationSpec, build_baseline_experiment, canonical_sha256,
    derive_validation_slice, replay_experiment,
};

pub const FINAL_OOS_CONSUMPTION_INTENT_SCHEMA_V1: &str =
    "okx.research.final-oos-consumption-intent/v1";
pub const PROMOTION_BUNDLE_SCHEMA_V1: &str = "okx.research.promotion-bundle/v1";
pub const FINAL_OOS_PROMOTION_ALGORITHM_V1: &str =
    "okx.research.final-oos-promotion/2026-10-05.1";


#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationFinalDecision {
    Reject,
    Backtested,
    InsufficientData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationPromotionCriteria {
    pub version: String,
    pub min_final_oos_trades: usize,
    pub min_net_pnl_quote_exclusive: String,
    pub min_profit_factor_exclusive: String,
    pub min_mean_to_sample_stddev_ratio_exclusive: String,
    pub require_cost_stress_monotonic: bool,
}

pub fn validation_promotion_criteria_v2() -> ValidationPromotionCriteria {
    ValidationPromotionCriteria {
        version: VALIDATION_PROMOTION_CRITERIA_V2.to_owned(),
        min_final_oos_trades: 30,
        min_net_pnl_quote_exclusive: "0".to_owned(),
        min_profit_factor_exclusive: "1".to_owned(),
        min_mean_to_sample_stddev_ratio_exclusive: "0".to_owned(),
        require_cost_stress_monotonic: true,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalOosConsumptionIntent {
    pub schema: String,
    pub consumption_intent_id: String,
    pub algorithm_version: String,
    pub validation_spec_id: String,
    pub validation_spec_artifact_id: String,
    pub robustness_id: String,
    pub robustness_artifact_id: String,
    pub parent_replay_dataset_artifact_id: String,
    pub final_oos_declared_range: ResearchRange,
    pub final_oos_dataset_id: String,
    pub hypothesis_id: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub promotion_criteria: ValidationPromotionCriteria,
    pub final_oos_status: String,
    pub source_tree: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalOosEvidence {
    pub replay_dataset_artifact_id: String,
    pub dataset_id: String,
    pub experiment_spec_artifact_id: String,
    pub experiment_result_artifact_id: String,
    pub experiment_id: String,
    pub replay_source_tree: String,
    pub status: String,
    pub evidence_class: String,
    pub candles_processed: usize,
    pub trade_count: usize,
    pub net_pnl_quote: String,
    pub statistics: Option<ValidationSampleStatistics>,
    pub cost_stress: ValidationCostStress,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionBundle {
    pub schema: String,
    pub promotion_bundle_id: String,
    pub algorithm_version: String,
    pub decision: ValidationFinalDecision,
    pub decision_blockers: Vec<String>,
    pub promotion_criteria: ValidationPromotionCriteria,
    pub validation_spec_id: String,
    pub validation_spec_artifact_id: String,
    pub research_family_id: String,
    pub research_family_artifact_id: String,
    pub hypothesis_id: String,
    pub strategy: BaselineStrategyKind,
    pub strategy_version: String,
    pub robustness_id: String,
    pub robustness_artifact_id: String,
    pub consumption_intent_id: String,
    pub consumption_intent_artifact_id: String,
    pub final_oos: FinalOosEvidence,
    pub final_oos_status: String,
    pub invalidation_conditions: Vec<String>,
    pub source_tree: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedValidationPromotion {
    pub consumption_intent: FinalOosConsumptionIntent,
    pub consumption_intent_artifact_id: String,
    pub bundle: PromotionBundle,
    pub bundle_artifact_id: String,
}

#[derive(Serialize)]
struct ConsumptionIntentIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    validation_spec_id: &'a str,
    validation_spec_artifact_id: &'a str,
    robustness_id: &'a str,
    robustness_artifact_id: &'a str,
    parent_replay_dataset_artifact_id: &'a str,
    final_oos_declared_range: &'a ResearchRange,
    final_oos_dataset_id: &'a str,
    hypothesis_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    promotion_criteria: &'a ValidationPromotionCriteria,
    final_oos_status: &'static str,
    source_tree: &'a str,
}

#[derive(Serialize)]
struct PromotionBundleIdentity<'a> {
    schema: &'static str,
    algorithm_version: &'static str,
    decision: ValidationFinalDecision,
    decision_blockers: &'a [String],
    promotion_criteria: &'a ValidationPromotionCriteria,
    validation_spec_id: &'a str,
    validation_spec_artifact_id: &'a str,
    research_family_id: &'a str,
    research_family_artifact_id: &'a str,
    hypothesis_id: &'a str,
    strategy: BaselineStrategyKind,
    strategy_version: &'a str,
    robustness_id: &'a str,
    robustness_artifact_id: &'a str,
    consumption_intent_id: &'a str,
    consumption_intent_artifact_id: &'a str,
    final_oos: &'a FinalOosEvidence,
    final_oos_status: &'static str,
    invalidation_conditions: &'a [String],
    source_tree: &'a str,
}

pub fn consume_final_oos(
    store: &ResearchArtifactStore,
    expected_instrument: &str,
    validation_spec_artifact_id: &str,
    robustness_artifact_id: &str,
) -> Result<PreparedValidationPromotion, ResearchError> {
    if BUILD_SOURCE_TREE == "UNAVAILABLE" {
        return Err(ResearchError::MissingField(
            "validation_promotion.source_tree",
        ));
    }

    let spec: ValidationSpec = store.read_evidence_json(validation_spec_artifact_id)?;
    if spec.schema != VALIDATION_SPEC_SCHEMA_V2
        || spec.validation_source_tree != BUILD_SOURCE_TREE
        || spec.promotion_criteria_version != VALIDATION_PROMOTION_CRITERIA_V2
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let robustness: ValidationRobustnessEvidence =
        store.read_evidence_json(robustness_artifact_id)?;
    if robustness.validation_spec_id != spec.validation_spec_id
        || robustness.validation_spec_artifact_id != validation_spec_artifact_id
        || robustness.readiness != ValidationEvidenceReadiness::ReadyForFinalOos
        || !robustness.blockers.is_empty()
        || robustness.final_oos_status != "SEALED_UNCONSUMED"
        || robustness.source_tree != BUILD_SOURCE_TREE
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let pre_holdout: PreHoldoutEvidence =
        store.read_evidence_json(&robustness.pre_holdout_evidence_artifact_id)?;
    if pre_holdout.evidence_id != robustness.pre_holdout_evidence_id
        || pre_holdout.validation_spec_id != spec.validation_spec_id
        || pre_holdout.validation_spec_artifact_id != validation_spec_artifact_id
        || pre_holdout.final_oos_status != "SEALED_UNCONSUMED"
        || pre_holdout.source_tree != BUILD_SOURCE_TREE
    {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let parent: crate::ReplayDatasetArtifact =
        store.read_evidence_json(&spec.parent_replay_dataset_artifact_id)?;
    parent.validate()?;
    if parent.manifest.instrument_id != expected_instrument
        || parent.manifest.dataset_id != spec.parent_dataset_id
    {
        return Err(ResearchError::InstrumentMismatch {
            expected: expected_instrument.to_owned(),
            actual: parent.manifest.instrument_id,
        });
    }

    let final_slice =
        derive_validation_slice(&parent, &spec, ValidationPartitionRole::FinalOos)?;
    let criteria = validation_promotion_criteria_v2();

    let (hypothesis, experiment) = build_baseline_experiment(
        &final_slice.replay_dataset,
        spec.strategy,
        ReplayMechanicsProvenance::DeclaredCounterfactual,
    )?;
    if hypothesis.hypothesis_id != pre_holdout.hypothesis_id {
        return Err(ResearchError::ArtifactIdentityMismatch);
    }

    let consumption_intent_id = canonical_sha256(&ConsumptionIntentIdentity {
        schema: FINAL_OOS_CONSUMPTION_INTENT_SCHEMA_V1,
        algorithm_version: FINAL_OOS_PROMOTION_ALGORITHM_V1,
        validation_spec_id: &spec.validation_spec_id,
        validation_spec_artifact_id,
        robustness_id: &robustness.robustness_id,
        robustness_artifact_id,
        parent_replay_dataset_artifact_id: &spec.parent_replay_dataset_artifact_id,
        final_oos_declared_range: &final_slice.declared_range,
        final_oos_dataset_id: &final_slice.replay_dataset.manifest.dataset_id,
        hypothesis_id: &hypothesis.hypothesis_id,
        strategy: spec.strategy,
        strategy_version: &spec.strategy_version,
        promotion_criteria: &criteria,
        final_oos_status: "CONSUMED",
        source_tree: BUILD_SOURCE_TREE,
    })?;
    let consumption_intent = FinalOosConsumptionIntent {
        schema: FINAL_OOS_CONSUMPTION_INTENT_SCHEMA_V1.to_owned(),
        consumption_intent_id,
        algorithm_version: FINAL_OOS_PROMOTION_ALGORITHM_V1.to_owned(),
        validation_spec_id: spec.validation_spec_id.clone(),
        validation_spec_artifact_id: validation_spec_artifact_id.to_owned(),
        robustness_id: robustness.robustness_id.clone(),
        robustness_artifact_id: robustness_artifact_id.to_owned(),
        parent_replay_dataset_artifact_id: spec.parent_replay_dataset_artifact_id.clone(),
        final_oos_declared_range: final_slice.declared_range.clone(),
        final_oos_dataset_id: final_slice.replay_dataset.manifest.dataset_id.clone(),
        hypothesis_id: hypothesis.hypothesis_id.clone(),
        strategy: spec.strategy,
        strategy_version: spec.strategy_version.clone(),
        promotion_criteria: criteria.clone(),
        final_oos_status: "CONSUMED".to_owned(),
        source_tree: BUILD_SOURCE_TREE.to_owned(),
    };

    // Publishing this immutable intent is the holdout-opening event. Everything below is
    // deterministic/idempotent so a retry after interruption resumes the same lineage.
    let (consumption_intent_artifact_id, _) = store.publish_evidence(&consumption_intent)?;
    let (final_oos_replay_dataset_artifact_id, _) =
        store.publish_evidence(&final_slice.replay_dataset)?;
    let (_hypothesis_artifact_id, _) = store.publish_evidence(&hypothesis)?;
    let (experiment_spec_artifact_id, _) = store.publish_evidence(&experiment)?;

    let result = replay_experiment(
        &final_slice.replay_dataset.manifest,
        &final_slice.replay_dataset.candles,
        &final_slice.replay_dataset.funding,
        &experiment,
    )?;
    let (experiment_result_artifact_id, _) = store.publish_evidence(&result)?;

    let trade_pnl = result
        .trades
        .iter()
        .map(|trade| trade.net_pnl_quote.clone())
        .collect::<Vec<_>>();
    let statistics = if trade_pnl.len() >= 2 {
        Some(analyze_validation_pnl_samples(&trade_pnl)?)
    } else {
        None
    };
    let cost_stress = analyze_validation_cost_stress(
        &result.gross_pnl_quote,
        &result.trading_cost_quote,
        &result.funding_cost_quote,
    )?;
    let (decision, decision_blockers) = decide_final_oos(
        result.status,
        result.trade_count,
        statistics.as_ref(),
        &cost_stress,
        &criteria,
    )?;

    let final_oos = FinalOosEvidence {
        replay_dataset_artifact_id: final_oos_replay_dataset_artifact_id,
        dataset_id: final_slice.replay_dataset.manifest.dataset_id,
        experiment_spec_artifact_id,
        experiment_result_artifact_id,
        experiment_id: result.experiment_id,
        replay_source_tree: result.replay_source_tree,
        status: replay_status_label(result.status).to_owned(),
        evidence_class: replay_evidence_class_label(result.evidence_class).to_owned(),
        candles_processed: result.candles_processed,
        trade_count: result.trade_count,
        net_pnl_quote: result.net_pnl_quote,
        statistics,
        cost_stress,
    };

    let invalidation_conditions = vec![
        "BOUND_ARTIFACT_INTEGRITY_FAILURE".to_owned(),
        "VALIDATION_SPEC_OR_PROMOTION_CRITERIA_CHANGE".to_owned(),
        "STRATEGY_VERSION_CHANGE".to_owned(),
        "DESCENDANT_REUSES_CONSUMED_FINAL_OOS_AS_PRISTINE".to_owned(),
        "REPLAY_OR_ACCOUNTING_SEMANTICS_CHANGE".to_owned(),
    ];
    let promotion_bundle_id = canonical_sha256(&PromotionBundleIdentity {
        schema: PROMOTION_BUNDLE_SCHEMA_V1,
        algorithm_version: FINAL_OOS_PROMOTION_ALGORITHM_V1,
        decision,
        decision_blockers: &decision_blockers,
        promotion_criteria: &criteria,
        validation_spec_id: &spec.validation_spec_id,
        validation_spec_artifact_id,
        research_family_id: &pre_holdout.research_family_id,
        research_family_artifact_id: &pre_holdout.research_family_artifact_id,
        hypothesis_id: &pre_holdout.hypothesis_id,
        strategy: spec.strategy,
        strategy_version: &spec.strategy_version,
        robustness_id: &robustness.robustness_id,
        robustness_artifact_id,
        consumption_intent_id: &consumption_intent.consumption_intent_id,
        consumption_intent_artifact_id: &consumption_intent_artifact_id,
        final_oos: &final_oos,
        final_oos_status: "CONSUMED",
        invalidation_conditions: &invalidation_conditions,
        source_tree: BUILD_SOURCE_TREE,
    })?;
    let bundle = PromotionBundle {
        schema: PROMOTION_BUNDLE_SCHEMA_V1.to_owned(),
        promotion_bundle_id,
        algorithm_version: FINAL_OOS_PROMOTION_ALGORITHM_V1.to_owned(),
        decision,
        decision_blockers,
        promotion_criteria: criteria,
        validation_spec_id: spec.validation_spec_id,
        validation_spec_artifact_id: validation_spec_artifact_id.to_owned(),
        research_family_id: pre_holdout.research_family_id,
        research_family_artifact_id: pre_holdout.research_family_artifact_id,
        hypothesis_id: pre_holdout.hypothesis_id,
        strategy: spec.strategy,
        strategy_version: spec.strategy_version,
        robustness_id: robustness.robustness_id,
        robustness_artifact_id: robustness_artifact_id.to_owned(),
        consumption_intent_id: consumption_intent.consumption_intent_id.clone(),
        consumption_intent_artifact_id: consumption_intent_artifact_id.clone(),
        final_oos,
        final_oos_status: "CONSUMED".to_owned(),
        invalidation_conditions,
        source_tree: BUILD_SOURCE_TREE.to_owned(),
    };
    let (bundle_artifact_id, _) = store.publish_evidence(&bundle)?;

    Ok(PreparedValidationPromotion {
        consumption_intent,
        consumption_intent_artifact_id,
        bundle,
        bundle_artifact_id,
    })
}

fn decide_final_oos(
    status: ReplayStatus,
    trade_count: usize,
    statistics: Option<&ValidationSampleStatistics>,
    cost_stress: &ValidationCostStress,
    criteria: &ValidationPromotionCriteria,
) -> Result<(ValidationFinalDecision, Vec<String>), ResearchError> {
    if status != ReplayStatus::Completed {
        return Ok((
            ValidationFinalDecision::InsufficientData,
            vec!["FINAL_OOS_REPLAY_NOT_COMPLETED".to_owned()],
        ));
    }
    if trade_count < criteria.min_final_oos_trades {
        return Ok((
            ValidationFinalDecision::InsufficientData,
            vec!["FINAL_OOS_SAMPLE_BELOW_POLICY_MINIMUM".to_owned()],
        ));
    }
    let Some(statistics) = statistics else {
        return Ok((
            ValidationFinalDecision::InsufficientData,
            vec!["FINAL_OOS_STATISTICS_UNAVAILABLE".to_owned()],
        ));
    };

    let mut blockers = Vec::new();
    if decimal(&statistics.total_net_pnl_quote)?
        <= decimal(&criteria.min_net_pnl_quote_exclusive)?
    {
        blockers.push("FINAL_OOS_NET_PNL_NOT_POSITIVE".to_owned());
    }

    let profit_factor_pass = match &statistics.profit_factor {
        Some(value) => decimal(value)? > decimal(&criteria.min_profit_factor_exclusive)?,
        None => {
            decimal(&statistics.gross_loss_abs_quote)? == Decimal::ZERO
                && decimal(&statistics.gross_profit_quote)? > Decimal::ZERO
        }
    };
    if !profit_factor_pass {
        blockers.push("FINAL_OOS_PROFIT_FACTOR_NOT_ABOVE_ONE".to_owned());
    }

    let mean_to_stddev_threshold =
        decimal(&criteria.min_mean_to_sample_stddev_ratio_exclusive)?;
    let mean_to_stddev_pass = statistics
        .mean_to_sample_stddev_ratio
        .as_deref()
        .map(decimal)
        .transpose()?
        .is_some_and(|value| value > mean_to_stddev_threshold);
    if !mean_to_stddev_pass {
        blockers.push("FINAL_OOS_MEAN_TO_STDDEV_NOT_POSITIVE".to_owned());
    }

    if criteria.require_cost_stress_monotonic && !cost_stress.monotonic_nonincreasing {
        blockers.push("FINAL_OOS_COST_STRESS_NOT_MONOTONIC".to_owned());
    }

    blockers.sort();
    blockers.dedup();
    Ok((
        if blockers.is_empty() {
            ValidationFinalDecision::Backtested
        } else {
            ValidationFinalDecision::Reject
        },
        blockers,
    ))
}

fn replay_status_label(status: ReplayStatus) -> &'static str {
    match status {
        ReplayStatus::Completed => "COMPLETED",
        ReplayStatus::InsufficientData => "INSUFFICIENT_DATA",
        ReplayStatus::InsufficientReferenceHistory => "INSUFFICIENT_REFERENCE_HISTORY",
    }
}

fn replay_evidence_class_label(evidence_class: ReplayEvidenceClass) -> &'static str {
    match evidence_class {
        ReplayEvidenceClass::DataOnly => "DATA_ONLY",
        ReplayEvidenceClass::HistoricalObserved => "HISTORICAL_OBSERVED",
        ReplayEvidenceClass::CounterfactualMechanics => "COUNTERFACTUAL_MECHANICS",
    }
}

fn decimal(value: &str) -> Result<Decimal, ResearchError> {
    Decimal::from_str(value).map_err(|_| ResearchError::ReplayInvalidDecimal {
        field: "validation_promotion_decimal",
        value: value.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statistics(samples: &[&str]) -> ValidationSampleStatistics {
        analyze_validation_pnl_samples(
            &samples.iter().map(|value| (*value).to_owned()).collect::<Vec<_>>(),
        )
        .expect("statistics")
    }

    #[test]
    fn final_oos_positive_sample_can_be_backtested() {
        let samples = (0..40)
            .map(|index| if index % 5 == 0 { "-0.25" } else { "0.5" })
            .collect::<Vec<_>>();
        let stats = statistics(&samples);
        let stress = analyze_validation_cost_stress("16", "2", "0").expect("stress");
        let (decision, blockers) =
            decide_final_oos(
                ReplayStatus::Completed,
                40,
                Some(&stats),
                &stress,
                &validation_promotion_criteria_v2(),
            )
                .expect("decision");
        assert_eq!(decision, ValidationFinalDecision::Backtested);
        assert!(blockers.is_empty());
    }

    #[test]
    fn final_oos_negative_edge_is_rejected() {
        let samples = (0..40)
            .map(|index| if index % 5 == 0 { "0.25" } else { "-0.5" })
            .collect::<Vec<_>>();
        let stats = statistics(&samples);
        let stress = analyze_validation_cost_stress("-16", "2", "0").expect("stress");
        let (decision, blockers) =
            decide_final_oos(
                ReplayStatus::Completed,
                40,
                Some(&stats),
                &stress,
                &validation_promotion_criteria_v2(),
            )
                .expect("decision");
        assert_eq!(decision, ValidationFinalDecision::Reject);
        assert!(blockers.contains(&"FINAL_OOS_NET_PNL_NOT_POSITIVE".to_owned()));
    }

    #[test]
    fn final_oos_small_sample_is_insufficient_data() {
        let stats = statistics(&["1", "-0.25"]);
        let stress = analyze_validation_cost_stress("1", "0.25", "0").expect("stress");
        let (decision, blockers) =
            decide_final_oos(
                ReplayStatus::Completed,
                2,
                Some(&stats),
                &stress,
                &validation_promotion_criteria_v2(),
            )
                .expect("decision");
        assert_eq!(decision, ValidationFinalDecision::InsufficientData);
        assert_eq!(blockers, vec!["FINAL_OOS_SAMPLE_BELOW_POLICY_MINIMUM"]);
    }
}
